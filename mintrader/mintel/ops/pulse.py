"""A pulse every few minutes: is the bot trading, and if not, why not?

    python -m mintel.ops.pulse --config data/config.json

Run by launchd every ten minutes, *independently* of the bot, so the record
is written even when the bot is dead.  Each run appends one line to
``data/uptime.log``::

    2026-09-24T09:30:12Z UP   equity=1012.40 open=1 today=+3.20
    2026-09-24T10:40:15Z DOWN trader not answering (last heartbeat 480s ago); ...

and publishes the latest state to the reports branch:

    reports/live/pulse.json     this pulse, with everything it could see
    reports/live/status.json    the status page's data, if the bot answered
    reports/live/uptime.log     the last few hundred pulse lines

The nightly report copies the day's pulse lines into
``reports/<date>/uptime.log`` and sums the gaps, so an outage is a number
in the day's review and not a surprise the next morning.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Callable, Optional

from ..clock import UTC, to_utc, utcnow
from ..config import Config

KEEP_LINES = 400
STALE_SECONDS = 120.0


# --------------------------------------------------------------- observe --
def take_pulse(cfg: Config, now: Optional[dt.datetime] = None,
               fetch: Optional[Callable[[str], str]] = None,
               heartbeats: Optional[dict] = None,
               bridge: Optional[Callable[[], Optional[dict]]] = None,
               port_is_open: Optional[Callable[[], bool]] = None,
               terminal_running: Optional[Callable[[], Optional[bool]]] = None,
               ) -> dict:
    """Everything an outside observer can tell about the bot right now.

    Every probe can be replaced (for tests, and because a pulse must never
    hang on one of them).
    """
    now = to_utc(now or utcnow()).replace(microsecond=0)
    out: dict = {"ts_utc": now.isoformat(), "up": False, "reason": "",
                 "trader": {}, "watchdog": {}, "bridge": {}, "mt5": {},
                 "status": None}

    # 1. The bot's own page: the best witness when it answers.
    from .report_upload import status_json
    body = status_json(cfg, fetch)
    try:
        status = json.loads(body)
    except Exception:
        status = {"unavailable": body}
    out["status"] = status
    st = (status.get("status") or {}) if "unavailable" not in status else {}
    health = (status.get("health") or {}) if "unavailable" not in status else {}

    # 2. Heartbeats on disk: written by the trader and the watchdog.
    if heartbeats is None:
        from .health import read_all_heartbeats
        heartbeats = read_all_heartbeats(Path(cfg.ops.data_dir) / "heartbeats", now)
    strat = heartbeats.get("strategy") or {}
    wd = heartbeats.get("watchdog") or {}
    out["trader"] = {"heartbeat_age": strat.get("age_seconds"),
                     "waiting": (strat.get("detail") or {}).get("waiting")}
    out["watchdog"] = {"heartbeat_age": wd.get("age_seconds"),
                       "last_action": _last_watchdog_action(cfg)}

    # 3. The bridge and MetaTrader, as seen from outside the trader.
    if port_is_open is None:
        from .watchdog import port_open
        port_is_open = lambda: port_open(cfg.bridge_host, cfg.bridge_port)   # noqa: E731
    if bridge is None:
        from .watchdog import bridge_status
        bridge = lambda: bridge_status(cfg.bridge_host, cfg.bridge_port,       # noqa: E731
                                       cfg.bridge_token_file)
    if terminal_running is None:
        from .watchdog import process_running
        terminal_running = lambda: process_running("terminal64.exe")          # noqa: E731
    if cfg.broker_mode == "bridge":
        answering = bool(port_is_open())
        info = bridge() if answering else None
        out["bridge"] = {"answering": answering,
                         "mt5_connected": (info or {}).get("mt5_connected"),
                         "connect_failures": (info or {}).get("connect_failures"),
                         "last_error": (info or {}).get("mt5_last_error", "")}
        out["mt5"] = {"terminal_running": terminal_running()}

    # 4. The verdict.
    out["up"], out["reason"] = _judge(out, st, health)
    if out["up"]:
        out["equity"] = st.get("equity")
        out["open_positions"] = st.get("open_positions")
        out["today_pnl"] = st.get("today_pnl")
        out["build"] = st.get("build")
    return out


def _judge(p: dict, st: dict, health: dict) -> tuple[bool, str]:
    bot = st.get("bot")
    if bot == "RUNNING" and not health.get("safe_mode"):
        if st.get("mt5_connected") is False or st.get("broker_connected") is False:
            return False, "trader running but MetaTrader is not connected"
        return True, ""
    if bot == "WAITING FOR METATRADER":
        why = str(st.get("waiting") or "waiting for MetaTrader")
        if st.get("detail"):
            why += f" ({st['detail']})"
        return False, "trader up, " + why + _mt5_note(p)
    if bot == "RUNNING" and health.get("safe_mode"):
        return False, "SAFE MODE: " + str(health.get("summary") or "see the page")
    if bot:
        return False, f"trader reports {bot}: {health.get('summary', '')}".strip()

    # The page did not answer: piece it together from the outside.
    parts = []
    age = p["trader"].get("heartbeat_age")
    if age is None:
        parts.append("trader has never reported")
    elif age > STALE_SECONDS:
        parts.append(f"trader not answering (last heartbeat {age:.0f}s ago)")
    else:
        parts.append(f"trader alive (heartbeat {age:.0f}s ago) but its page "
                     f"is not answering")
    wage = p["watchdog"].get("heartbeat_age")
    if wage is None:
        parts.append("watchdog has never reported")
    elif wage > STALE_SECONDS:
        parts.append(f"watchdog not running (last seen {wage:.0f}s ago)")
    else:
        parts.append("watchdog alive")
    last = p["watchdog"].get("last_action")
    if last:
        parts.append(f"watchdog last said: {last}")
    note = _mt5_note(p)
    return False, "; ".join(parts) + note


def _mt5_note(p: dict) -> str:
    b = p.get("bridge") or {}
    m = p.get("mt5") or {}
    if not b and not m:
        return ""
    bits = []
    if b.get("answering"):
        if b.get("mt5_connected"):
            bits.append("bridge answering, MetaTrader connected")
        else:
            bits.append("bridge answering but MetaTrader is not connected"
                        + (f" ({b['last_error']})" if b.get("last_error") else ""))
    else:
        bits.append("bridge not answering")
    tr = m.get("terminal_running")
    if tr is True:
        bits.append("MetaTrader terminal running")
    elif tr is False:
        bits.append("MetaTrader terminal NOT running")
    return "; " + "; ".join(bits)


def _last_watchdog_action(cfg: Config) -> str:
    path = Path(cfg.ops.log_dir) / "watchdog.log"
    try:
        lines = path.read_text().strip().splitlines()
    except Exception:
        return ""
    return lines[-1].strip()[:300] if lines else ""


# ---------------------------------------------------------------- record --
def pulse_line(p: dict) -> str:
    ts = p["ts_utc"].replace("+00:00", "Z")
    if p["up"]:
        eq = p.get("equity")
        pnl = p.get("today_pnl")
        return (f"{ts} UP   equity={eq if eq is None else f'{eq:.2f}'} "
                f"open={p.get('open_positions', '?')} "
                f"today={'?' if pnl is None else f'{pnl:+.2f}'}")
    return f"{ts} DOWN {p['reason']}"


def uptime_path(cfg: Config) -> Path:
    return Path(cfg.ops.data_dir) / "uptime.log"


def append_uptime(cfg: Config, line: str) -> None:
    path = uptime_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as fh:
        fh.write(line + "\n")


def uptime_lines(cfg: Config, day: Optional[dt.datetime] = None) -> list[str]:
    """Pulse lines, optionally only those on the UTC day starting at ``day``."""
    try:
        lines = uptime_path(cfg).read_text().splitlines()
    except Exception:
        return []
    if day is None:
        return lines
    key = to_utc(day).strftime("%Y-%m-%d")
    return [ln for ln in lines if ln.startswith(key)]


def uptime_summary(lines: list[str], interval_minutes: float = 10.0) -> str:
    """Plain words: how much of the day the bot was up, and the gaps."""
    if not lines:
        return "No pulse record for this day (the pulse job was not running)."
    ups = [ln for ln in lines if " UP " in ln]
    downs = [ln for ln in lines if " DOWN " in ln]
    total = len(lines)
    pct = 100.0 * len(ups) / total
    out = [f"Bot up for {pct:.0f}% of the day's pulses "
           f"({len(ups)} of {total}, one every ~{interval_minutes:.0f} min)."]
    # Group consecutive DOWN pulses into outages.
    outage: list[str] = []
    gaps: list[tuple[str, str, int, str]] = []
    for ln in lines:
        if " DOWN " in ln:
            outage.append(ln)
        elif outage:
            gaps.append(_gap(outage))
            outage = []
    if outage:
        gaps.append(_gap(outage, open_ended=True))
    for start, end, n, why in gaps:
        mins = n * interval_minutes
        out.append(f"  DOWN {start} to {end} (~{mins:.0f} min): {why}")
    if not downs:
        out.append("  No outages recorded.")
    return "\n".join(out)


def _gap(outage: list[str], open_ended: bool = False) -> tuple[str, str, int, str]:
    first, last = outage[0], outage[-1]
    start = first[11:16]
    end = "still down" if open_ended else last[11:16]
    why = first.split(" DOWN ", 1)[1] if " DOWN " in first else ""
    return start, end, len(outage), why[:220]


def pulse_files(p: dict, lines: list[str]) -> dict[str, str]:
    status = p.get("status")
    slim = {k: v for k, v in p.items() if k != "status"}
    return {
        "reports/live/pulse.json": json.dumps(slim, indent=2, default=str) + "\n",
        "reports/live/status.json": json.dumps(status, indent=2, default=str) + "\n",
        "reports/live/uptime.log": "\n".join(lines[-KEEP_LINES:]) + "\n",
    }


# ------------------------------------------------------------------ main --
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="record whether the bot is up, and publish it")
    ap.add_argument("--config", default="data/config.json")
    ap.add_argument("--no-upload", action="store_true", help="only write data/uptime.log")
    args = ap.parse_args(argv)
    cfg = Config.load(args.config)
    p = take_pulse(cfg)
    line = pulse_line(p)
    append_uptime(cfg, line)
    print(line)
    if args.no_upload:
        return 0
    from .report_upload import read_token, upload
    token = read_token(args.config)
    if not token:
        print("(not published: no GitHub token in secrets.json)")
        return 0
    try:
        n = upload(pulse_files(p, uptime_lines(cfg)), cfg.report_repo,
                   cfg.report_branch, token, day="live")
        print(f"published {n} files to {cfg.report_repo} ({cfg.report_branch})")
    except Exception as exc:
        print(f"(not published: {exc})")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
