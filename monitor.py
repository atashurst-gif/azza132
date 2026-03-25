"""
Gmail → Google Sheets Automation
Monitors inbox for emails from R.healey@arkleinsolvency.co.uk
with XLSX attachments and writes rows to UKDT Automation sheet.
"""

import os
import io
import re
import json
import time
import base64
import logging
import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ─────────────────────────────────────────────
# Config & Logging
# ─────────────────────────────────────────────

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("automation.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Constants (override via .env)
# ─────────────────────────────────────────────

SENDER_EMAIL      = os.getenv("SENDER_EMAIL", "R.healey@arkleinsolvency.co.uk")
SENDER_EMAIL_2    = os.getenv("SENDER_EMAIL_2", "d.yiu@arkleinsolvency.co.uk")
SENDER_EMAIL_3    = os.getenv("SENDER_EMAIL_3", "info@trust-link.co.uk")
SENDER_EMAIL_3    = os.getenv("SENDER_EMAIL_3", "info@trust-link.co.uk")
SENDER_EMAIL_3    = os.getenv("SENDER_EMAIL_3", "info@trust-link.co.uk")
GMAIL_ADDRESS     = os.getenv("GMAIL_ADDRESS", "regenmarketing26@gmail.com")
SHEET_NAME        = os.getenv("SHEET_NAME", "Sheet1")
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "60"))
PROCESSED_IDS_FILE = os.getenv("PROCESSED_IDS_FILE", "processed_ids.json")
CREDENTIALS_FILE  = os.getenv("CREDENTIALS_FILE", "credentials.json")
TOKEN_FILE        = os.getenv("TOKEN_FILE", "token.json")

# Gmail API needs these scopes
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

# ─────────────────────────────────────────────
# Duplicate Protection
# ─────────────────────────────────────────────

def load_processed_ids() -> set:
    """
    Load the set of already-processed Gmail message IDs from a local JSON file.
    This is the core duplicate-prevention mechanism — if the script restarts or
    re-runs, it will skip any message whose ID already appears in this file.
    """
    if Path(PROCESSED_IDS_FILE).exists():
        with open(PROCESSED_IDS_FILE, "r") as f:
            return set(json.load(f))
    return set()


def save_processed_id(msg_id: str, processed_ids: set) -> None:
    """Persist a newly processed message ID to disk immediately after processing."""
    processed_ids.add(msg_id)
    with open(PROCESSED_IDS_FILE, "w") as f:
        json.dump(list(processed_ids), f, indent=2)
    log.debug(f"Saved message ID {msg_id} to processed list.")


# ─────────────────────────────────────────────
# Google Auth (shared for Gmail + Sheets)
# ─────────────────────────────────────────────

def get_google_credentials() -> Credentials:
    """
    Authenticate with Google using OAuth2.
    - On first run: prints a URL for the user to open manually, saves token.json.
    - On subsequent runs: loads token.json and refreshes if expired.
    """
    creds = None

    if Path(TOKEN_FILE).exists():
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            log.info("Refreshing expired Google credentials...")
            creds.refresh(Request())
        else:
            log.info("Starting OAuth2 flow...")
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            # Use console flow - prints URL for user to open manually
            creds = flow.run_console()

        with open(TOKEN_FILE, "w") as token:
            token.write(creds.to_json())
        log.info("Credentials saved to token.json.")

    return creds


# ─────────────────────────────────────────────
# Gmail Helpers
# ─────────────────────────────────────────────

def search_unprocessed_emails(service, processed_ids: set) -> list:
    """
    Query Gmail for emails from the target sender that have attachments.
    Returns only messages not yet in processed_ids.
    """
    query = f"from:{SENDER_EMAIL} OR from:{SENDER_EMAIL_2} OR from:{SENDER_EMAIL_3}"
    try:
        result = service.users().messages().list(userId="me", q=query).execute()
        messages = result.get("messages", [])
        log.info(f"Found {len(messages)} total matching email(s) from approved senders.")

        unprocessed = [m for m in messages if m["id"] not in processed_ids]
        log.info(f"{len(unprocessed)} new (unprocessed) email(s) to handle.")
        return unprocessed

    except HttpError as e:
        log.error(f"Gmail search failed: {e}")
        return []


def get_xlsx_attachment(service, msg_id: str) -> tuple[str | None, bytes | None]:
    """
    Fetch the first XLSX attachment from a Gmail message.
    Returns (filename, raw_bytes) or (None, None) if not found.
    """
    try:
        msg = service.users().messages().get(userId="me", id=msg_id).execute()
        parts = msg.get("payload", {}).get("parts", [])

        for part in parts:
            filename = part.get("filename", "")
            mime = part.get("mimeType", "")

            is_xlsx = filename.lower().endswith(".xlsx") or \
                      mime in (
                          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                          "application/octet-stream",
                      )

            if is_xlsx and filename:
                body = part.get("body", {})
                attachment_id = body.get("attachmentId")

                if attachment_id:
                    att = service.users().messages().attachments().get(
                        userId="me", messageId=msg_id, id=attachment_id
                    ).execute()
                    data = base64.urlsafe_b64decode(att["data"])
                    log.info(f"Downloaded XLSX attachment: {filename} ({len(data):,} bytes)")
                    return filename, data

        log.info(f"No XLSX attachment found in message {msg_id}.")
        return None, None

    except HttpError as e:
        log.error(f"Failed to fetch attachment from message {msg_id}: {e}")
        return None, None


def get_email_body_table(service, msg_id: str):
    """
    Extract a table from the email body (HTML or plain text).
    Returns parsed rows or None.
    """
    try:
        msg = service.users().messages().get(
            userId="me", id=msg_id, format="full"
        ).execute()

        def extract_parts(payload):
            parts = []
            if payload.get("body", {}).get("data"):
                parts.append((payload.get("mimeType", ""), payload["body"]["data"]))
            for part in payload.get("parts", []):
                parts.extend(extract_parts(part))
            return parts

        body_parts = extract_parts(msg.get("payload", {}))
        html_body = ""
        text_body = ""

        for mime, data in body_parts:
            decoded = base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="ignore")
            if "html" in mime:
                html_body = decoded
            elif "plain" in mime or not html_body:
                text_body = decoded

        if html_body:
            rows = _parse_html_table(html_body)
            if rows:
                log.info(f"Found HTML table with {len(rows)} rows in email body.")
                return rows

        if text_body:
            rows = _parse_text_table(text_body)
            if rows:
                log.info(f"Found text table with {len(rows)} rows in email body.")
                return rows

        return None

    except Exception as e:
        log.error(f"Failed to extract email body for {msg_id}: {e}")
        return None


def _parse_html_table(html):
    try:
        row_pat  = re.compile(r'<tr[^>]*>(.*?)</tr>', re.DOTALL | re.IGNORECASE)
        cell_pat = re.compile(r'<t[dh][^>]*>(.*?)</t[dh]>', re.DOTALL | re.IGNORECASE)
        tag_pat  = re.compile(r'<[^>]+>')
        rows = []
        for rm in row_pat.finditer(html):
            cells = []
            for cm in cell_pat.finditer(rm.group(1)):
                text = tag_pat.sub('', cm.group(1))
                text = text.replace('&nbsp;', ' ').replace('&amp;', '&').strip()
                cells.append(text)
            if cells:
                rows.append(cells)
        return rows if len(rows) > 1 else None
    except Exception:
        return None


def _parse_text_table(text):
    try:
        lines = text.splitlines()
        header_idx = None
        for i, line in enumerate(lines):
            if "Reference" in line and "Customer" in line:
                header_idx = i
                break
        if header_idx is None:
            return None
        rows = []
        for line in lines[header_idx:]:
            line = line.strip()
            if not line:
                continue
            if '|' in line:
                cells = [c.strip() for c in line.split('|') if c.strip()]
            else:
                cells = line.split()
            if cells:
                rows.append(cells)
        return rows if len(rows) > 1 else None
    except Exception:
        return None


def process_body_rows(raw_rows):
    """Convert raw table rows from email body into standard format."""
    if not raw_rows:
        return []

    header = [h.lower().strip() for h in raw_rows[0]]

    def find_col(*names):
        for name in names:
            for i, h in enumerate(header):
                if name.lower() in h:
                    return i
        return None

    ref_idx    = find_col("reference")
    cust_idx   = find_col("customer")
    phone_idx  = find_col("mobile", "contact", "phone")
    source_idx = find_col("source", "campaign")
    stage_idx  = find_col("stage", "status")

    if ref_idx is None or cust_idx is None:
        log.warning(f"Could not find Reference/Customer in body table. Header: {header}")
        return []

    rows = []
    today = datetime.datetime.now().strftime("%d/%m/%Y")

    for row in raw_rows[1:]:
        def get(idx):
            if idx is None or idx >= len(row):
                return ""
            val = str(row[idx]).strip()
            return "" if val in ("nan", "None") else val

        reference = get(ref_idx)
        customer  = get(cust_idx)
        if not reference or not customer:
            continue

        rows.append([
            today,
            reference,
            extract_first_name(customer),
            get(phone_idx),
            get(source_idx),
            get(stage_idx),
        ])

    log.info(f"Body table processed: {len(rows)} valid row(s).")
    return rows


# ─────────────────────────────────────────────
# Name Cleaning
# ─────────────────────────────────────────────

def extract_first_name(raw: str) -> str:
    """
    Extract the first real name from a messy Customer field.

    Rules:
    - Strip email fragments (anything with @ or .com/.co.uk etc.)
    - Strip numeric-only tokens
    - Strip symbols and punctuation except hyphens inside words
    - Take the first remaining token
    - Convert to Title Case

    Examples:
      'john smith'          → 'John'
      'JOHN SMITH'          → 'John'
      'Smith, John'         → 'Smith'   (first token after cleaning)
      '###John123 Smith'    → 'John'
      'john@example.com'    → ''        (no usable name)
      'Mr. John P. Smith'   → 'Mr'      (salutation kept — acceptable)
    """
    if not isinstance(raw, str) or not raw.strip():
        return ""

    # Remove email addresses
    cleaned = re.sub(r'\S+@\S+', '', raw)

    # Remove anything that looks like a web domain fragment
    cleaned = re.sub(r'\b\S+\.(com|co\.uk|org|net|io|uk)\b', '', cleaned, flags=re.I)

    # Remove standalone numbers or tokens that are mostly digits
    cleaned = re.sub(r'\b\d+\w*\b', '', cleaned)

    # Remove special characters except hyphens between word characters
    cleaned = re.sub(r'[^a-zA-Z\s\-]', ' ', cleaned)

    # Collapse whitespace
    tokens = cleaned.split()

    if not tokens:
        return ""

    first = tokens[0].strip('-')  # remove leading/trailing hyphens
    return first.title() if first else ""


# ─────────────────────────────────────────────
# XLSX Processing
# ─────────────────────────────────────────────

def get_col(df_columns, *names):
    """Return the first column name from `names` that exists in df_columns."""
    for name in names:
        if name in df_columns:
            return name
    return None


def process_xlsx(raw_bytes: bytes) -> list[list]:
    """
    Read XLSX bytes with pandas, auto-detect column names,
    transform each row, and return a list of
    [Date, TL-REF, First Name, Phone Number, Campaign, Status] rows.

    Handles multiple column name variants:
    - Phone: 'Customer  Mobile', 'Customer Mobile', 'Contact'
    - Campaign: 'Source'
    - Status: 'Stage'
    """
    df = pd.read_excel(io.BytesIO(raw_bytes), dtype=str)

    # Normalise column names (strip whitespace)
    df.columns = df.columns.str.strip()
    cols = list(df.columns)

    # Auto-detect column names
    ref_col    = get_col(cols, "Reference")
    cust_col   = get_col(cols, "Customer")
    phone_col  = get_col(cols, "Customer  Mobile", "Customer Mobile", "Contact")
    source_col = get_col(cols, "Source")
    stage_col  = get_col(cols, "Stage")

    if not ref_col or not cust_col:
        raise ValueError(f"XLSX missing Reference or Customer column. Found: {cols}")

    log.info(f"Columns detected — ref:{ref_col} cust:{cust_col} phone:{phone_col} source:{source_col} stage:{stage_col}")

    def get_val(row, col):
        if col is None or col not in row.index:
            return ""
        val = str(row[col]).strip()
        return "" if val in ("nan", "None", "NaN") else val

    rows = []
    skipped = 0

    for _, row in df.iterrows():
        if row.isnull().all():
            skipped += 1
            continue

        reference       = get_val(row, ref_col)
        customer        = get_val(row, cust_col)
        customer_mobile = get_val(row, phone_col)
        source          = get_val(row, source_col)
        stage           = get_val(row, stage_col)

        if all(v == "" for v in [reference, customer]):
            skipped += 1
            continue

        first_name   = extract_first_name(customer)
        today        = datetime.datetime.now().strftime("%d/%m/%Y")

        rows.append([today, reference, first_name, customer_mobile, source, stage])

    log.info(f"XLSX processed: {len(rows)} valid row(s), {skipped} blank row(s) skipped.")
    return rows


# ─────────────────────────────────────────────
# Google Sheets Helper
# ─────────────────────────────────────────────

def find_sheet_id(sheets_service, spreadsheet_id: str, sheet_name: str) -> int | None:
    """Return the sheetId integer for a named tab, or None if not found."""
    meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == sheet_name:
            return s["properties"]["sheetId"]
    return None



def get_existing_refs(sheets_service, spreadsheet_id: str) -> set:
    """Fetch all TL-REF values already in Sheet1 to prevent duplicates."""
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"'{SHEET_NAME}'!B:B"
        ).execute()
        values = result.get('values', [])
        refs = {row[0].strip() for row in values if row and row[0].strip()}
        log.info(f'Found {len(refs)} existing TL-REFs in sheet.')
        return refs
    except Exception as e:
        log.warning(f'Could not fetch existing refs: {e}')
        return set()


def append_rows_to_sheet(sheets_service, spreadsheet_id: str, rows: list[list]) -> int:
    """
    Append rows to the UKDT Automation sheet.
    Uses SHEET_NAME from env. Creates the sheet tab if it doesn't exist.
    Returns the number of rows written.
    """
    # Ensure the target sheet tab exists
    sheet_id = find_sheet_id(sheets_service, spreadsheet_id, SHEET_NAME)

    if sheet_id is None:
        log.info(f"Sheet tab '{SHEET_NAME}' not found — creating it...")
        body = {
            "requests": [{
                "addSheet": {
                    "properties": {"title": SHEET_NAME}
                }
            }]
        }
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body=body
        ).execute()

        # Write header row
        header = [["Date", "TL-REF", "First Name", "Phone Number", "Campaign", "Status"]]
        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{SHEET_NAME}'!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": header},
        ).execute()
        log.info("Header row written.")

    # Filter out rows whose TL-REF already exists in the sheet
    existing_refs = get_existing_refs(sheets_service, spreadsheet_id)
    original_count = len(rows)
    rows = [r for r in rows if r[1] not in existing_refs]
    skipped = original_count - len(rows)
    if skipped:
        log.info(f'Skipped {skipped} duplicate TL-REF(s) already in sheet.')
    if not rows:
        log.info('All rows were duplicates — nothing to append.')
        return 0

    # Append data rows
    result = sheets_service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"'{SHEET_NAME}'!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()

    updated = result.get("updates", {}).get("updatedRows", len(rows))
    log.info(f"Appended {updated} row(s) to '{SHEET_NAME}'.")
    return updated


# ─────────────────────────────────────────────
# Core Processing Loop
# ─────────────────────────────────────────────

def process_email(gmail_service, sheets_service, spreadsheet_id: str,
                  msg_id: str, processed_ids: set) -> bool:
    """
    Full pipeline for a single email:
      1. Download XLSX attachment
      2. Parse & transform rows
      3. Write to Google Sheets
      4. Mark message ID as processed

    Returns True on success, False on any failure (so the ID is NOT
    marked processed and the email will be retried on the next poll).
    """
    log.info(f"Processing message ID: {msg_id}")

    # Step 1: Try XLSX attachment first, then fall back to email body table
    filename, raw_bytes = get_xlsx_attachment(gmail_service, msg_id)

    if filename and raw_bytes:
        # Step 2a: Parse XLSX
        try:
            rows = process_xlsx(raw_bytes)
        except Exception as e:
            log.error(f"Failed to parse XLSX from message {msg_id}: {e}")
            return False
    else:
        # Step 2b: No XLSX — try parsing table from email body
        log.info(f"No XLSX found for {msg_id} — trying email body table...")
        body_table = get_email_body_table(gmail_service, msg_id)
        if not body_table:
            log.warning(f"No usable data found in message {msg_id} — skipping.")
            save_processed_id(msg_id, processed_ids)
            return False
        try:
            rows = process_body_rows(body_table)
        except Exception as e:
            log.error(f"Failed to parse body table from message {msg_id}: {e}")
            return False

    if not rows:
        log.warning(f"XLSX in message {msg_id} contained no usable rows.")
        save_processed_id(msg_id, processed_ids)
        return True

    # Step 3: Write to Sheets
    try:
        append_rows_to_sheet(sheets_service, spreadsheet_id, rows)
    except HttpError as e:
        log.error(f"Google Sheets write failed for message {msg_id}: {e}")
        return False

    # Step 4: Mark as done
    save_processed_id(msg_id, processed_ids)
    log.info(f"✓ Message {msg_id} fully processed — {len(rows)} row(s) written.")
    return True


def run_poll_cycle(gmail_service, sheets_service,
                   spreadsheet_id: str, processed_ids: set) -> None:
    """Run one poll cycle: search inbox, process new emails."""
    log.info("─── Poll cycle started ───")
    messages = search_unprocessed_emails(gmail_service, processed_ids)

    for msg in messages:
        try:
            process_email(gmail_service, sheets_service,
                          spreadsheet_id, msg["id"], processed_ids)
        except Exception as e:
            log.exception(f"Unexpected error processing message {msg['id']}: {e}")

    log.info("─── Poll cycle complete ───")


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────

def main():
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    if not spreadsheet_id:
        raise EnvironmentError("SPREADSHEET_ID is not set in .env")

    log.info("Starting Gmail → Google Sheets automation...")
    log.info(f"Monitoring: {GMAIL_ADDRESS}")
    log.info(f"Sender filter: {SENDER_EMAIL}")
    log.info(f"Target sheet: {SHEET_NAME}")
    log.info(f"Poll interval: {POLL_INTERVAL_SEC}s")

    creds = get_google_credentials()
    gmail_service  = build("gmail", "v1", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)

    processed_ids = load_processed_ids()
    log.info(f"Loaded {len(processed_ids)} previously processed message ID(s).")

    while True:
        try:
            run_poll_cycle(gmail_service, sheets_service, spreadsheet_id, processed_ids)
        except Exception as e:
            log.exception(f"Fatal error in poll cycle — will retry: {e}")

        log.info(f"Sleeping {POLL_INTERVAL_SEC}s before next poll...")
        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
