"""Publish the day's record so it can be read without screenshots.

    python -m mintel.ops.report_upload --config data/config.json            # today
    python -m mintel.ops.report_upload --config data/config.json --date 2026-09-17
    python -m mintel.ops.report_upload --config data/config.json --set-token

Writes, for the UTC day, into the repository's ``reports`` branch:

    reports/<date>/day_review.txt   the plain-English review
    reports/<date>/trades.csv       one line per trade: approach, regime,
                                    score, tier, how it exited, money, R
    reports/<date>/status.json      the status page's data (periods, rules)
    reports/<date>/rules.json       the strategy rules that were in force
    reports/<date>/uptime.log       the day's ten-minute pulses (UP/DOWN + why)
    reports/latest/...              a copy of the newest day

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
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable, Optional

from ..clock import UTC, to_utc, utcnow
from ..config import STRATEGY_VERSION, Config

API = "https://api.github.com"


# ------------------------------------------------------------------ build --
def trades_csv(journal_rows: list[dict], positions: list[dict]) -> str:
    """One line per closed trade. Broker money where known, journal detail."""
    money = {int(p["position"]): p for p in positions}
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["ticket", "symbol", "side", "opened_utc", "closed_utc", "minutes",
                "tactic", "regime", "score", "tier", "net_money", "commission",
                "realised_r", "mfe_r", "mae_r", "exit_reason", "final_state"])
    for r in sorted(journal_rows, key=lambda r: str(r.get("opened_utc") or "")):
        t = int(r.get("ticket") or 0)
        p = money.get(t, {})
        w.writerow([t, r.get("symbol"), r.get("side"), r.get("opened_utc"),
                    r.get("closed_utc"), p.get("minutes"),
                    r.get("tactic"), r.get("regime"),
                    round(float(r.get("opportunity") or 0), 1), r.get("tier"),
                    p.get("net", r.get("pnl_money")), p.get("commission"),
                    r.get("realised_r"), r.get("mfe_r"), r.get("mae_r"),
                    r.get("exit_reason"), r.get("final_flow_state")])
    return out.getvalue()


def rules_json(cfg: Config) -> str:
    from ..version import RUNNING_STAMP
    d = cfg.to_dict()
    return json.dumps({"strategy": cfg.tracking_strategy or STRATEGY_VERSION,
                       "code_strategy": STRATEGY_VERSION,
                       "build": RUNNING_STAMP,
                       "tracking_start_utc": cfg.tracking_start_utc,
                       "aggression": cfg.aggression,
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


def build_bundle(cfg: Config, broker, day: dt.datetime,
                 fetch: Optional[Callable[[str], str]] = None) -> dict[str, str]:
    from .day_review import build_day_review
    from .pulse import uptime_lines, uptime_summary
    text, positions, journal_rows = build_day_review(cfg, broker, day)
    key = day.strftime("%Y-%m-%d")
    pulses = uptime_lines(cfg, day)
    text += "\n\nBOT UPTIME\n" + uptime_summary(pulses)
    files = {
        f"reports/{key}/day_review.txt": text + "\n",
        f"reports/{key}/uptime.log": "\n".join(pulses) + "\n",
        f"reports/{key}/trades.csv": trades_csv(journal_rows, positions),
        f"reports/{key}/status.json": status_json(cfg, fetch),
        f"reports/{key}/rules.json": rules_json(cfg),
    }
    files.update({p.replace(f"reports/{key}/", "reports/latest/"): v
                  for p, v in list(files.items())})
    files["reports/latest/DATE"] = key + "\n"
    return files


# ----------------------------------------------------------------- upload --
class GitHub:
    """The few calls needed to write files onto a branch."""

    def __init__(self, repo: str, token: str, opener: Optional[Callable] = None):
        self.repo = repo
        self.token = token
        self._open = opener or self._default_open

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
        code, existing = self.call("GET", f"/repos/{self.repo}/contents/{path}?ref={branch}")
        body = {"message": message, "branch": branch,
                "content": base64.b64encode(content.encode("utf-8")).decode("ascii")}
        if code == 200 and existing.get("sha"):
            if existing.get("content") and base64.b64decode(
                    existing["content"].replace("\n", "")).decode("utf-8", "replace") == content:
                return                                   # unchanged
            body["sha"] = existing["sha"]
        code, resp = self.call("PUT", f"/repos/{self.repo}/contents/{path}", body)
        if code not in (200, 201):
            raise RuntimeError(f"could not write {path} ({code}): {resp.get('message', '')}")


def upload(files: dict[str, str], repo: str, branch: str, token: str,
           opener: Optional[Callable] = None, day: str = "") -> int:
    gh = GitHub(repo, token, opener)
    gh.ensure_branch(branch)
    for path, content in files.items():
        gh.put_file(branch, path, content, f"report {day}: {path.split('/')[-1]}")
    return len(files)


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
    n = upload(files, cfg.report_repo, cfg.report_branch, token, day=day.strftime("%Y-%m-%d"))
    print(f"Published {n} files for {day:%Y-%m-%d} to {cfg.report_repo} ({cfg.report_branch} branch).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
