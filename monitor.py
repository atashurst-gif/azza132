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

import base64, os
if _cb64 := os.getenv('GOOGLE_CREDENTIALS_B64'):
    open('credentials.json','wb').write(base64.b64decode(_cb64))
if _tb64 := os.getenv('GOOGLE_TOKEN_B64'):
    open('token.json','wb').write(base64.b64decode(_tb64))


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
    query = f"from:{SENDER_EMAIL} has:attachment"
    try:
        result = service.users().messages().list(userId="me", q=query).execute()
        messages = result.get("messages", [])
        log.info(f"Found {len(messages)} total matching email(s) from {SENDER_EMAIL}.")

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

REQUIRED_COLS = {"Reference", "Customer", "Source", "Stage", "Customer  Mobile"}


def process_xlsx(raw_bytes: bytes) -> list[list]:
    """
    Read XLSX bytes with pandas, validate required columns exist,
    transform each row per the field mapping, and return a list of
    [TL-REF, First Name, Phone Number, Campaign, Status] rows.

    Blank rows (all NaN) are silently skipped.
    """
    df = pd.read_excel(io.BytesIO(raw_bytes), dtype=str)

    # Normalise column names (strip whitespace)
    df.columns = df.columns.str.strip()

    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"XLSX is missing required columns: {missing}")

    rows = []
    skipped = 0

    for _, row in df.iterrows():
        # Skip fully blank rows
        if row.isnull().all():
            skipped += 1
            continue

        reference       = str(row.get("Reference", "")).strip()
        customer        = str(row.get("Customer", "")).strip()
        source          = str(row.get("Source", "")).strip()
        stage           = str(row.get("Stage", "")).strip()
        customer_mobile = str(row.get("Customer  Mobile", "")).strip()

        # Skip rows where all key fields are blank/nan
        if all(v in ("", "nan", "None") for v in [reference, customer, source, stage, customer_mobile]):
            skipped += 1
            continue

        # Clean 'nan' strings that pandas uses for empty cells
        def clean(val):
            return "" if val in ("nan", "None") else val

        first_name   = extract_first_name(customer)
        phone_number = clean(customer_mobile)  # kept exactly as-is
        campaign     = clean(source)            # kept exactly as-is
        status       = clean(stage)             # kept exactly as-is
        tl_ref       = clean(reference)

        today = datetime.datetime.now().strftime("%d/%m/%Y")
        rows.append([today, tl_ref, first_name, phone_number, campaign, status])

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

    # Step 1: Get attachment
    filename, raw_bytes = get_xlsx_attachment(gmail_service, msg_id)
    if not filename or not raw_bytes:
        log.warning(f"Skipping message {msg_id} — no XLSX attachment found.")
        # Mark as processed so we don't keep hitting it
        save_processed_id(msg_id, processed_ids)
        return False

    # Step 2: Parse XLSX
    try:
        rows = process_xlsx(raw_bytes)
    except Exception as e:
        log.error(f"Failed to parse XLSX from message {msg_id}: {e}")
        return False  # Don't mark processed — may be retried

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
