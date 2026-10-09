"""Publish the day's record so it can be read without screenshots.

    python -m mintel.ops.report_upload --config data/config.json            # today
    python -m mintel.ops.report_upload --config data/config.json --date 2026-09-17
    python -m mintel.ops.report_upload --config data/config.json --set-token

Writes, for the UTC day, into the repository's ``reports`` branch:

    reports/<date>/day_review.txt   the plain-English review
    reports/<date>/trades.csv       one line per trade: approach, regime,
                                    score, tier, how it exited, money, R,
                                    and its mode (LIVE, or PAPER for a
                                    Trend & Breakout paper trade)
    reports/<date>/status.json      the status page's data (periods, rules)
    reports/<date>/rules.json       the strategy rules that were in force,
                                    and Trend & Breakout's mode
    reports/<date>/tnb.json         Trend & Breakout's PAPER day (its paper
                                    deals and positions, practice, not
                                    money) and its real positions left from
                                    LIVE - while it is on PAPER, or on a
                                    day it had paper deals
    reports/<date>/uptime.log       the day's ten-minute pulses (UP/DOWN + why)
    reports/<date>/<bot>.json       each other bot's day: its status file and
                                    its closed trades, each with its mode
                                    (runner, bandbreaker, crowd, rider, ian;
                                    scalper while its records exist)
    reports/latest/...              a copy of the newest day

A file GitHub refuses because the branch moved under it (409 or 422: the
pulse or another upload wrote at the same moment, as on 8 Oct) is retried
with a fresh read, up to five times; a file that still fails does not stop
the others, and the run ends by naming every file that failed.

Run nightly by launchd after the broker's day closes, and on demand by the
"Send Report" desktop icon. The GitHub token comes from secrets.json
("github_token"); it is never written into source control.
"""
from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import io
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable, Optional

from ..clock import UTC, to_utc, utcnow
from ..config import STRATEGY_VERSION, Config

API = "https://api.github.com"


# ------------------------------------------------------------------ build --
def trades_csv(journal_rows: list[dict], positions: list[dict], paper_tickets=frozenset()) -> str:
    """One line per closed trade. Broker money where known (the paper
    record's for a PAPER trade), journal detail, and the trade's mode: PAPER
    when its ticket was born on Trend & Breakout's paper record, else LIVE."""
    money = {int(p["position"]): p for p in positions}
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["ticket", "symbol", "side", "opened_utc", "closed_utc", "minutes",
                "tactic", "regime", "score", "tier", "net_money", "commission",
                "realised_r", "mfe_r", "mae_r", "exit_reason", "final_state", "mode"])
    for r in sorted(journal_rows, key=lambda r: str(r.get("opened_utc") or "")):
        t = int(r.get("ticket") or 0)
        p = money.get(t, {})
        mode = "PAPER" if t in paper_tickets else str(p.get("mode") or "LIVE")
        w.writerow([t, r.get("symbol"), r.get("side"), r.get("opened_utc"),
                    r.get("closed_utc"), p.get("minutes"),
                    r.get("tactic"), r.get("regime"),
                    round(float(r.get("opportunity") or 0), 1), r.get("tier"),
                    p.get("net", r.get("pnl_money")), p.get("commission"),
                    r.get("realised_r"), r.get("mfe_r"), r.get("mae_r"),
                    r.get("exit_reason"), r.get("final_flow_state"), mode])
    return out.getvalue()


def tnb_json(cfg: Config, day: dt.datetime, positions: list[dict]) -> Optional[str]:
    """Trend & Breakout's PAPER day: its paper deals and positions from its
    paper record (practice, not money, never in the account) and its REAL
    positions left from LIVE (real money). None on LIVE with no paper deal
    that day."""
    from .standing import TnbPaperRecord, tnb_mode
    mode = tnb_mode(cfg)
    record = TnbPaperRecord(cfg.ops.data_dir, cfg.magic)
    end = day + dt.timedelta(days=1)
    deals = record.deals_in(day, end)
    if mode != "PAPER" and not deals and not record.error:
        return None
    paper = [p for p in positions if p.get("mode") == "PAPER"]
    real = [p for p in positions if p.get("mode") != "PAPER"]
    out = {"mode": mode,
           "note": ("Trend & Breakout is on PAPER: real prices, simulated orders; its practice is not money and is "
                    "never in the account. The Momentum Runner rides its index entries." if mode == "PAPER" else
                    "Trend & Breakout was on PAPER for part of the day: its paper trades are not money."),
           "paper": {"source": "the paper record (tnb_paper.sqlite), read-only", "deals": deals,
                     "positions": paper, "net": round(sum(float(p.get("net") or 0.0) for p in paper), 2)},
           "real": {"source": "the broker's records", "positions": real,
                    "net": round(sum(float(p.get("net") or 0.0) for p in real), 2)}}
    if record.error:
        out["paper"]["unavailable"] = record.error
    return json.dumps(out, indent=2, default=str)


def rules_json(cfg: Config) -> str:
    from ..version import RUNNING_STAMP
    from .standing import tnb_mode
    d = cfg.to_dict()
    return json.dumps({"strategy": cfg.tracking_strategy or STRATEGY_VERSION,
                       "code_strategy": STRATEGY_VERSION,
                       "build": RUNNING_STAMP,
                       "tracking_start_utc": cfg.tracking_start_utc,
                       "aggression": cfg.aggression,
                       "tnb_mode": tnb_mode(cfg),
                       "scan": d.get("scan"), "flowlock": d.get("flowlock"),
                       "risk": d.get("risk")}, indent=2, sort_keys=True, default=str)


def status_json(cfg: Config, fetch: Optional[Callable[[str], str]] = None) -> str:
    """The status page's own data, if the bot is up (else a note)."""
    url = f"http://{cfg.ops.dashboard_host}:{cfg.ops.dashboard_port}/api"
    try:
        if fetch is None:
            with urllib.request.urlopen(url, timeout=5) as r:      # noqa: S310
                body = r.read().decode("utf-8")
        else:
            body = fetch(url)
        json.loads(body)
        return body
    except Exception as exc:
        return json.dumps({"unavailable": str(exc)})


def scalper_json(cfg: Config, day: dt.datetime) -> Optional[str]:
    """The Rapid Scalper's day: its status file plus its closed trades.

    None when the scalper has never run (no journal file)."""
    data = Path(cfg.ops.data_dir)
    status_path = data / "scalper-status.json"
    journal_path = data / "scalper.sqlite"
    if not journal_path.exists() and not status_path.exists():
        return None
    out: dict = {"status": None, "trades": []}
    try:
        out["status"] = json.loads(status_path.read_text())
    except Exception as exc:
        out["status"] = {"unavailable": str(exc)}
    try:
        from ..scalper.journal import ScalperJournal
        j = ScalperJournal(journal_path)
        start = day.replace(hour=0, minute=0, second=0, microsecond=0)
        out["trades"] = [dict(r) for r in j.closed_since(start)]
        j.close()
    except Exception as exc:
        out["trades_unavailable"] = str(exc)
    return json.dumps(out, indent=2, default=str)


def runner_json(cfg: Config, day: dt.datetime) -> Optional[str]:
    """The Momentum Runner's day: its status file plus its closed shadows."""
    return paper_bot_json(cfg, day, "runner-status.json", "runner.sqlite")


def rider_json(cfg: Config, day: dt.datetime) -> Optional[str]:
    """The Rapid Momentum Rider's day: rider-status.json plus its closed
    trades (each with its mode, LIVE or PAPER, and where its money figure
    came from: the broker, a paper calculation or an estimate)."""
    return paper_bot_json(cfg, day, "rider-status.json", "rider.sqlite", events=True)


def ian_json(cfg: Config, day: dt.datetime) -> Optional[str]:
    """Financial Ian's day: ian-status.json plus its closed trades (each with
    its mode and its data source: a SYNTHETIC or REPLAY row is never a
    result) and the signals it produced."""
    return paper_bot_json(cfg, day, "ian-status.json", "ian.sqlite", events=True, signals=True)


def paper_bot_json(cfg: Config, day: dt.datetime, status_name: str, db_name: str,
                   events: bool = False, signals: bool = False) -> Optional[str]:
    data = Path(cfg.ops.data_dir)
    status_path = data / status_name
    journal_path = data / db_name
    if not journal_path.exists() and not status_path.exists():
        return None
    out: dict = {"status": None, "trades": []}
    try:
        out["status"] = json.loads(status_path.read_text())
    except Exception as exc:
        out["status"] = {"unavailable": str(exc)}
    try:
        import sqlite3
        start = day.replace(hour=0, minute=0, second=0, microsecond=0)
        db = sqlite3.connect(f"file:{journal_path}?mode=ro", uri=True)
        db.row_factory = sqlite3.Row
        end = to_utc(start + dt.timedelta(days=1)).isoformat()
        out["trades"] = [dict(r) for r in db.execute(
            "SELECT * FROM trades WHERE closed_utc IS NOT NULL AND closed_utc >= ? AND closed_utc < ? ORDER BY closed_utc",
            (to_utc(start).isoformat(), end))]
        try:
            out["checks"] = [dict(r) for r in db.execute(
                "SELECT * FROM checks WHERE ts_utc >= ? AND ts_utc < ? ORDER BY ts_utc", (to_utc(start).isoformat(), end))]
        except Exception:
            pass                                   # not every paper bot keeps a checks table
        for wanted, table, key in ((events, "events", "events"), (signals, "signals", "signals")):
            if not wanted:
                continue
            try:
                out[key] = [dict(r) for r in db.execute(
                    f"SELECT * FROM {table} WHERE ts_utc >= ? AND ts_utc < ? ORDER BY ts_utc LIMIT 5000",
                    (to_utc(start).isoformat(), end))]
            except Exception:
                pass
        db.close()
    except Exception as exc:
        out["trades_unavailable"] = str(exc)
    return json.dumps(out, indent=2, default=str)


def build_bundle(cfg: Config, broker, day: dt.datetime,
                 fetch: Optional[Callable[[str], str]] = None) -> dict[str, str]:
    from .day_review import build_day_review
    from .pulse import uptime_lines, uptime_summary
    text, positions, journal_rows = build_day_review(cfg, broker, day)
    key = day.strftime("%Y-%m-%d")
    pulses = uptime_lines(cfg, day)
    text += "\n\nBOT UPTIME\n" + uptime_summary(pulses)
    from .standing import TnbPaperRecord
    paper_tickets = (TnbPaperRecord(cfg.ops.data_dir, cfg.magic).tickets
                     | frozenset(int(p["position"]) for p in positions if p.get("mode") == "PAPER"))
    files = {
        f"reports/{key}/day_review.txt": text + "\n",
        f"reports/{key}/uptime.log": "\n".join(pulses) + "\n",
        f"reports/{key}/trades.csv": trades_csv(journal_rows, positions, paper_tickets),
        f"reports/{key}/status.json": status_json(cfg, fetch),
        f"reports/{key}/rules.json": rules_json(cfg),
    }
    tnb = tnb_json(cfg, day, positions)
    if tnb is not None:
        files[f"reports/{key}/tnb.json"] = tnb
    scalper = scalper_json(cfg, day)
    if scalper is not None:
        files[f"reports/{key}/scalper.json"] = scalper
    runner = runner_json(cfg, day)
    if runner is not None:
        files[f"reports/{key}/runner.json"] = runner
    bb = paper_bot_json(cfg, day, "bandbreaker-status.json", "bandbreaker.sqlite")
    if bb is not None:
        files[f"reports/{key}/bandbreaker.json"] = bb
    cf = paper_bot_json(cfg, day, "crowd-status.json", "crowd.sqlite")
    if cf is not None:
        files[f"reports/{key}/crowd.json"] = cf
    rider = rider_json(cfg, day)
    if rider is not None:
        files[f"reports/{key}/rider.json"] = rider
    ian = ian_json(cfg, day)
    if ian is not None:
        files[f"reports/{key}/ian.json"] = ian
    files.update({p.replace(f"reports/{key}/", "reports/latest/"): v
                  for p, v in list(files.items())})
    files["reports/latest/DATE"] = key + "\n"
    return files


# ----------------------------------------------------------------- upload --
class UploadIncomplete(RuntimeError):
    """Some files were published and some were not, after every retry."""

    def __init__(self, written: int, failed: list[tuple[str, str]]):
        self.written = written
        self.failed = failed
        names = "; ".join(f"{p} ({why})" for p, why in failed)
        super().__init__(f"{len(failed)} file{'' if len(failed) == 1 else 's'} could not be published: {names}")


CONFLICT = (409, 422)            # the branch moved between our read and our write
PUT_TRIES = 5


class GitHub:
    """The few calls needed to write files onto a branch."""

    def __init__(self, repo: str, token: str, opener: Optional[Callable] = None,
                 sleep: Callable[[float], None] = time.sleep):
        self.repo = repo
        self.token = token
        self._open = opener or self._default_open
        self._sleep = sleep

    def _default_open(self, method: str, url: str, body: Optional[dict]) -> tuple[int, dict]:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers={
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "mintel-report",
            "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:          # noqa: S310
                return r.status, json.loads(r.read().decode() or "{}")
        except urllib.error.HTTPError as exc:
            try:
                payload = json.loads(exc.read().decode() or "{}")
            except Exception:
                payload = {}
            return exc.code, payload

    def call(self, method: str, path: str, body: Optional[dict] = None) -> tuple[int, dict]:
        return self._open(method, f"{API}{path}", body)

    def ensure_branch(self, branch: str) -> None:
        code, _ = self.call("GET", f"/repos/{self.repo}/branches/{branch}")
        if code == 200:
            return
        code, repo = self.call("GET", f"/repos/{self.repo}")
        if code != 200:
            raise RuntimeError(f"repository {self.repo} not reachable ({code}): check the token")
        default = repo.get("default_branch") or "main"
        code, ref = self.call("GET", f"/repos/{self.repo}/git/ref/heads/{default}")
        if code != 200:
            raise RuntimeError(f"could not read branch {default} ({code})")
        sha = ref["object"]["sha"]
        code, _ = self.call("POST", f"/repos/{self.repo}/git/refs",
                            {"ref": f"refs/heads/{branch}", "sha": sha})
        if code not in (200, 201):
            raise RuntimeError(f"could not create branch {branch} ({code})")

    def put_file(self, branch: str, path: str, content: str, message: str) -> None:
        """Write one file. A conflict (409 or 422: something else wrote to
        the branch between our read and our write, or our sha is stale) is
        retried with a fresh read of the file's sha, up to PUT_TRIES times
        with a short, growing pause. Any other refusal is not retried."""
        encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
        code, resp = 0, {}
        for attempt in range(1, PUT_TRIES + 1):
            code, existing = self.call("GET", f"/repos/{self.repo}/contents/{path}?ref={branch}")
            body = {"message": message, "branch": branch, "content": encoded}
            if code == 200 and existing.get("sha"):
                if existing.get("content") and base64.b64decode(
                        existing["content"].replace("\n", "")).decode("utf-8", "replace") == content:
                    return                                   # unchanged
                body["sha"] = existing["sha"]
            code, resp = self.call("PUT", f"/repos/{self.repo}/contents/{path}", body)
            if code in (200, 201):
                return
            if code not in CONFLICT or attempt == PUT_TRIES:
                break
            self._sleep(min(8.0, 0.5 * 2 ** (attempt - 1)))   # 0.5, 1, 2, 4 s
        tries = f" after {PUT_TRIES} tries" if code in CONFLICT else ""
        raise RuntimeError(f"could not write {path} ({code}{tries}): {resp.get('message', '')}")


def upload(files: dict[str, str], repo: str, branch: str, token: str,
           opener: Optional[Callable] = None, day: str = "",
           sleep: Callable[[float], None] = time.sleep) -> int:
    """Publish every file; returns how many were written (or were already
    there unchanged). A file that still fails after its retries does not
    stop the rest: once every file has been tried, UploadIncomplete names
    each one that failed. A branch that cannot be reached at all (a bad
    token) stops everything at once, before any file is tried."""
    gh = GitHub(repo, token, opener, sleep=sleep)
    gh.ensure_branch(branch)
    failed: list[tuple[str, str]] = []
    for path, content in files.items():
        try:
            gh.put_file(branch, path, content, f"report {day}: {path.split('/')[-1]}")
        except Exception as exc:
            failed.append((path, str(exc)))
    written = len(files) - len(failed)
    if failed:
        raise UploadIncomplete(written, failed)
    return written


# ----------------------------------------------------------------- secrets --
def secrets_path(config_path: str) -> Path:
    return Path(config_path).parent / "secrets.json"


def read_token(config_path: str) -> str:
    try:
        return str(json.loads(secrets_path(config_path).read_text()).get("github_token") or "")
    except Exception:
        return ""


def store_token(config_path: str, token: str) -> None:
    p = secrets_path(config_path)
    try:
        data = json.loads(p.read_text()) if p.exists() else {}
    except Exception:
        data = {}
    data["github_token"] = token.strip()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2))
    try:
        os.chmod(p, 0o600)
    except Exception:
        pass


# ------------------------------------------------------------------- main --
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="publish the day's record to GitHub")
    ap.add_argument("--config", default="data/config.json")
    ap.add_argument("--date", default="", help="YYYY-MM-DD (UTC); default: the day that just ended")
    ap.add_argument("--set-token", action="store_true", help="store the GitHub token in secrets.json")
    args = ap.parse_args(argv)
    if args.set_token:
        import getpass
        tok = getpass.getpass("Paste the GitHub token (nothing is shown while you type): ")
        if not tok.strip():
            print("Nothing entered; token unchanged.")
            return 1
        store_token(args.config, tok)
        print("Token saved to secrets.json (not in the code, not in git).")
        return 0
    cfg = Config.load(args.config)
    token = read_token(args.config)
    if not token:
        print("No GitHub token yet. Run:  python -m mintel.ops.report_upload --config "
              f"{args.config} --set-token")
        return 2
    if args.date:
        day = dt.datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=UTC)
    else:
        now = to_utc(utcnow())
        day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if now.hour < 6:                 # just after midnight: the day that ended
            day -= dt.timedelta(days=1)
    from ..run import build_broker
    broker = build_broker(cfg)
    if not broker.connect():
        print("Could not connect to the bridge; is the bot running?")
        return 3
    try:
        files = build_bundle(cfg, broker, day)
    finally:
        try:
            broker.disconnect()
        except Exception:
            pass
    try:
        n = upload(files, cfg.report_repo, cfg.report_branch, token, day=day.strftime("%Y-%m-%d"))
    except UploadIncomplete as exc:
        print(f"Published {exc.written} of {len(files)} files for {day:%Y-%m-%d} to {cfg.report_repo} "
              f"({cfg.report_branch} branch). These failed:")
        for path, why in exc.failed:
            print(f"  {path}: {why}")
        return 1
    print(f"Published {n} files for {day:%Y-%m-%d} to {cfg.report_repo} ({cfg.report_branch} branch).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
