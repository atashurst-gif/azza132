"""A pulse every few minutes: is the bot trading, and if not, why not?

    python -m mintel.ops.pulse --config data/config.json

Run by launchd every ten minutes, *independently* of the bot, so the record
is written even when the bot is dead.  Each run appends one line to
``data/uptime.log``::

    2026-09-24T09:30:12Z UP   equity=1012.40 open=1 today=+3.20
    2026-10-09T09:30:12Z UP   equity=1012.40 open=3 today=+3.20 tnb=PAPER tnb_practice=+1.10
    2026-09-24T10:40:15Z DOWN trader not answering (last heartbeat 480s ago); ...

``today`` is the ACCOUNT's real result for the UK day (from 00:00 UK, the
page's Today; not the broker's day, which starts at 22:00 UK), every bot,
after commission: the Today figure on the page's Account card. ``open`` is
every real open position on the account, ``equity`` the account's. While
Trend & Breakout is on PAPER its practice result for the same UK day
follows, labelled ``tnb_practice``: simulated, never money, never in
``today``.
(Lines written before this change, made on 9 Oct 2026, carry Trend &
Breakout's own day as ``today`` and only its own trades as ``open``.)

and publishes the latest state to the reports branch:

    reports/live/pulse.json     this pulse, with everything it could see
    reports/live/status.json    the status page's data, if the bot answered
    reports/live/uptime.log     the last few hundred pulse lines
    reports/live/bots.json      one entry per bot (``bots_report``): its mode,
                                its health line, today's trades and result
                                as the page's standing block gives them, its
                                open trades; the Rapid Momentum Rider's "why
                                no trade" block and its top scanner rows,
                                Financial Ian's feed, Trend & Breakout's
                                paper trades and thinking, the Momentum
                                Runner's rides. Read-only from each bot's
                                status file; under 60 KB; no secrets

The nightly report copies the day's pulse lines into
``reports/<date>/uptime.log`` and sums the gaps, so an outage is a number
in the day's review and not a surprise the next morning.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Callable, Optional

from ..clock import UTC, to_utc, utcnow
from ..config import Config

KEEP_LINES = 400
STALE_SECONDS = 120.0
BOTS_PATH = "reports/live/bots.json"
BOTS_MAX_BYTES = 60_000


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
    # The nightly review reads "up" and "reason", and, on an UP pulse, the
    # figures listed in _figures(): today_pnl, equity, open_positions.
    #   up      - True when the trader's page says RUNNING, MetaTrader is
    #             connected and safe mode is off; False otherwise.
    #   reason  - when not up: why, in plain words, from whatever could be
    #             seen (the page, heartbeats, watchdog, bridge, terminal);
    #             "" when up.
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
        out.update(_figures(cfg, status, st, now))
    return out


def _money(v) -> Optional[float]:
    try:
        return None if v is None else round(float(v), 2)
    except (TypeError, ValueError):
        return None


def _figures(cfg: Config, status: dict, st: dict, now: dt.datetime) -> dict:
    """The figures an UP pulse carries, each one for what it is. Read from
    the trader's own page (/api), which reads MetaTrader: nothing here is
    worked out from Trend & Breakout's own day, and a figure the page could
    not give is None (written "?"), never a guess."""
    sd = status.get("standing") if isinstance(status.get("standing"), dict) else {}
    open_live = status.get("open_live")
    mode = str(st.get("tnb_mode") or "").strip().upper()
    if mode not in ("LIVE", "PAPER"):                  # an older page: the setting the trader reads
        from .standing import tnb_mode
        mode = tnb_mode(cfg)
    today = _money(sd.get("today"))
    out: dict = {
        # today_pnl - the ACCOUNT's real result for the UK day so far (from
        #   00:00 UK, the page's Today): every bot's trades and any placed by
        #   hand, after commission and swap, from MetaTrader's own deal
        #   history. The same figure as "Today" on the page's Account card
        #   (mintel/ops/standing.py, account_standing "today"). Paper never
        #   reaches it. None when the page could not read MetaTrader's
        #   history (account_error says why).
        "today_pnl": today,
        # equity, balance - the account's, as the trader last read them from
        #   MetaTrader (its page's status block). Unchanged by PAPER: the
        #   PAPER wrapper passes the real account straight through.
        "equity": st.get("equity"),
        "balance": st.get("balance"),
        # open_positions - every REAL open position on the account: every
        #   bot's (Trend & Breakout's real ones, the Momentum Runner, the
        #   Rapid Momentum Rider, Financial Ian, ...) and any placed by hand,
        #   from MetaTrader. Paper trades are never in it. None when the
        #   page could not read them.
        "open_positions": len(open_live) if isinstance(open_live, list) else None,
        # tnb_mode - Trend & Breakout's mode as the running trader reports
        #   it: LIVE (its orders are real) or PAPER (simulated).
        "tnb_mode": mode,
        "build": st.get("build"),
    }
    if today is None:
        out["account_error"] = str(sd.get("error") or "the page has no account figure from MetaTrader yet")
    if mode == "PAPER":
        # tnb_practice_today - Trend & Breakout's PRACTICE result for the
        #   same UK day (from 00:00 UK): its paper trades that closed today,
        #   after their simulated commission, from its paper record
        #   (data/tnb_paper.sqlite, read-only), the practice figure on its
        #   card. Not money, never in today_pnl. None when it could not be
        #   read (tnb_practice_error).
        practice, why = _tnb_practice_today(cfg, sd, now)
        out["tnb_practice_today"] = practice
        if why:
            out["tnb_practice_error"] = why
    return out


def _tnb_practice_today(cfg: Config, sd: dict, now: dt.datetime) -> tuple[Optional[float], str]:
    """(practice money, "") for Trend & Breakout's paper trades that closed
    in the page's Today (the UK day, from 00:00 UK), or (None, why not)."""
    if not sd.get("start"):
        return None, "the page has not given its start date yet"
    try:
        from .standing import TnbPaperRecord, period_bounds
        start, end, _label, _key = period_bounds(sd, "today", now=now)
        record = TnbPaperRecord(cfg.ops.data_dir)          # read-only: never books or writes
        if record.error:
            return None, record.error
        return record.net(start, end), ""
    except Exception as exc:
        return None, f"the paper record could not be read: {exc}"


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
        pnl = p.get("today_pnl")                       # the ACCOUNT's UK day (from 00:00 UK), every bot
        n_open = p.get("open_positions")               # every real open position on the account
        line = (f"{ts} UP   equity={eq if eq is None else f'{eq:.2f}'} "
                f"open={'?' if n_open is None else n_open} "
                f"today={'?' if pnl is None else f'{pnl:+.2f}'}")
        if str(p.get("tnb_mode") or "").upper() == "PAPER":
            pr = p.get("tnb_practice_today")           # practice, never money
            line += f" tnb=PAPER tnb_practice={'?' if pr is None else f'{pr:+.2f}'}"
        return line
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


def pulse_files(p: dict, lines: list[str], bots: Optional[str] = None) -> dict[str, str]:
    """The files the pulse publishes; ``bots`` (bots_json's text) adds
    reports/live/bots.json."""
    status = p.get("status")
    slim = {k: v for k, v in p.items() if k != "status"}
    files = {
        "reports/live/pulse.json": json.dumps(slim, indent=2, default=str) + "\n",
        "reports/live/status.json": json.dumps(status, indent=2, default=str) + "\n",
        "reports/live/uptime.log": "\n".join(lines[-KEEP_LINES:]) + "\n",
    }
    if bots is not None:
        files[BOTS_PATH] = bots
    return files


# -------------------------------------------------------------- the bots --
# Each bot's own status file in the data folder (read-only). Trend & Breakout
# has none: it is the trader, whose page the pulse has already read.
STATUS_FILES = {"momentum_rider": "rider-status.json", "momentum_runner": "runner-status.json",
                "band_breaker": "bandbreaker-status.json", "crowd_fader": "crowd-status.json",
                "financial_ian": "ian-status.json"}
# Only these fields are copied out of a status file or the page (never a whole
# file), and a key that could hold a secret is dropped wherever it appears.
SCANNER_KEYS = ("market", "direction", "score", "state", "reason", "cost_ratio", "headroom_atr", "family")
RIDER_OPEN_KEYS = ("ticket", "market", "side", "volume", "entry", "stop", "pips", "money", "money_note",
                   "flowlock", "opened_utc")
RIDE_KEYS = ("ticket", "source_ticket", "symbol", "future", "side", "volume", "entry", "stop", "initial_stop",
             "r_now", "peak_r", "best_r", "pnl", "trailing", "trail", "opened", "mode", "not_seen_since", "note")
TNB_POSITION_KEYS = ("symbol", "side", "volume", "entry", "stop", "profit", "flow_state", "note")
OPEN_LIVE_KEYS = ("bot", "magic", "ticket", "symbol", "side", "volume", "entry", "stop", "target", "profit", "opened")
THINKING_KEYS = ("rank", "symbol", "direction", "score", "tier", "tactic", "regime", "reason", "blockers",
                 "reward_risk")
ACCOUNT_KEYS = ("currency", "start", "open", "equity", "balance", "made", "error", "source")
STANDING_ROW_KEYS = ("magic", "mode", "open", "open_trades")
TNB_PAPER_DB = "tnb_paper.sqlite"
_SECRET_KEY = re.compile(r"(?i)token|secret|passw|api_?key|credential|login|bearer|authori[sz]ation")
_SECRET_TEXT = (re.compile(r"(?i)\b(token|secret|password|passwd|api[_-]?key|login)\b\s*[=:]\s*\S+"),
                re.compile(r"(?i)\bbearer\s+\S+"),
                re.compile(r"\b(ghp|gho|ghs|ghu|github_pat)_[A-Za-z0-9_]{8,}"),
                re.compile(r"\bdb-[A-Za-z0-9]{12,}"))


def _scrub(x, depth: int = 0):
    """A copy with every secret-looking key dropped and secret-looking text
    blanked (tokens, keys, passwords, logins)."""
    if depth > 12:
        return None
    if isinstance(x, dict):
        return {str(k): _scrub(v, depth + 1) for k, v in x.items() if not _SECRET_KEY.search(str(k))}
    if isinstance(x, (list, tuple)):
        return [_scrub(v, depth + 1) for v in x]
    if isinstance(x, str):
        for pat in _SECRET_TEXT:
            x = pat.sub("[hidden]", x)
        return x[:800]
    if x is None or isinstance(x, (bool, int, float)):
        return x
    return str(x)[:200]


def _pick(d, keys) -> dict:
    return {k: d[k] for k in keys if isinstance(d, dict) and k in d}


def _picks(rows, keys, limit: int) -> list[dict]:
    return [_pick(r, keys) for r in (rows if isinstance(rows, list) else [])[:limit] if isinstance(r, dict)]


def _plain(v) -> bool:
    return v is None or isinstance(v, (bool, int, float, str))


def _scalars(d, limit: int = 24) -> dict:
    """The plain values of a dict (numbers, words, yes/no), nothing nested."""
    if not isinstance(d, dict):
        return {}
    out = {}
    for k, v in d.items():
        if _plain(v):
            out[k] = v
        if len(out) >= limit:
            break
    return out


def _today_fields(d: dict) -> dict:
    """Every plain "today..." or "day..." figure of a standing block or row,
    copied as it is (whatever day the page counts as today)."""
    return {k: v for k, v in d.items() if _plain(v) and (k.startswith("today") or k.startswith("day"))}


def _read_status_file(path: Path) -> tuple[Optional[dict], str]:
    """(status, "") or (None, why not) - never raises."""
    try:
        raw = path.read_text()
    except FileNotFoundError:
        return None, f"not running / not readable: no status file ({path.name}) - it has not reported yet"
    except Exception as exc:
        return None, f"not running / not readable: {path.name} could not be read ({exc})"
    try:
        st = json.loads(raw)
    except Exception as exc:
        return None, f"not running / not readable: {path.name} is not valid JSON ({exc})"
    if not isinstance(st, dict):
        return None, f"not running / not readable: {path.name} holds no status"
    return st, ""


def _age(st: dict, now: dt.datetime) -> Optional[float]:
    stamp = st.get("updated_utc") or st.get("updated")
    try:
        t = dt.datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
        return round((now - to_utc(t if t.tzinfo else t.replace(tzinfo=UTC))).total_seconds(), 1)
    except Exception:
        return None


def _rider_status_name(data: Path) -> str:
    """rider-status.json, unless rider.json names another file (as the Rider reads it)."""
    try:
        sn = json.loads((data / "rider.json").read_text()).get("status_file")
        return str(sn) if isinstance(sn, str) and sn.strip() else STATUS_FILES["momentum_rider"]
    except Exception:
        return STATUS_FILES["momentum_rider"]


def _money_today(row: Optional[dict], sd: dict, strat, page_ok: bool, why_down: str) -> dict:
    """Today's trades and result for one bot, copied from the page's standing
    block (MetaTrader's deals for its magic number: real money), and its
    practice figure from the page's strategy tab while it has paper trades.
    Read, never worked out here."""
    if not page_ok:
        return {"today": {"unavailable": "the trader's page did not answer, so the account's figures are not known"
                                         + (f" ({why_down})" if why_down else "")}}
    if not isinstance(row, dict):
        return {"today": {"unavailable": "the page's standing block has no row for this bot"}}
    more = {k: v for k, v in _today_fields(row).items() if k not in ("today", "today_trades")}
    today = {"result": row.get("today"), "trades": row.get("today_trades"), **more, **_pick(row, STANDING_ROW_KEYS),
             "from_utc": sd.get("day_start"),
             "source": "the page's standing block: MetaTrader's own record for this bot's magic number, real money, "
                       "for the page's today; paper trades are never in it"}
    out: dict = {"today": today}
    if isinstance(strat, dict) and (strat.get("paper_trades") or str(row.get("mode") or "").upper() == "PAPER"):
        out["practice_today"] = {"result": strat.get("paper_net"), "trades": strat.get("paper_trades"),
                                 "note": "PAPER trades from the bot's own records (the page's strategy tab for "
                                         "today): practice, not money, never in the account"}
    return out


def _file_bot(data: Path, bid: str, now: dt.datetime) -> dict:
    """What a bot's own status file says. A missing or broken file is said in
    plain words, never a crash."""
    name = _rider_status_name(data) if bid == "momentum_rider" else STATUS_FILES[bid]
    st, why = _read_status_file(data / name)
    if st is None:
        return {"running": False, "health": why}
    age = _age(st, now)
    running = age is not None and age <= STALE_SECONDS
    mode = str(st.get("mode") or "?")
    out: dict = {"mode": mode, "status_file": name, "updated_utc": st.get("updated_utc") or st.get("updated"),
                 "report_age_seconds": age, "running": running}
    if bid in ("momentum_rider", "financial_ian"):
        try:
            from .modes import health_line
            out["health"] = health_line(data, "rider" if bid == "momentum_rider" else "ian", now)[1]
        except Exception as exc:
            out["health"] = f"health line not readable: {exc}"
    elif running:
        out["health"] = f"{st.get('status') or 'RUNNING'}, {mode}, last report {age:.0f} s ago"
    else:
        when = "never" if age is None else f"{age / 60:.0f} min ago"
        out["health"] = f"NOT RUNNING (last report {when}); its file says {mode}"
    if st.get("executor_error"):
        out["executor_error"] = st.get("executor_error")
    if bid == "momentum_rider":
        h = st.get("health") if isinstance(st.get("health"), dict) else {}
        out["entries_allowed"] = h.get("entries_allowed")
        out["health_summary"] = h.get("summary")
        out["failing_checks"] = [_pick(c, ("name", "critical", "message")) for c in (h.get("checks") or [])
                                 if isinstance(c, dict) and not c.get("ok")]
        out["user_pip_value_gbp"] = st.get("user_pip_value_gbp")
        out["open_trades"] = _picks(st.get("open_positions"), RIDER_OPEN_KEYS, 20)
        out["own_record_today"] = _scalars(st.get("today"))
        why_block = st.get("why_no_trade")
        out["why_no_trade"] = why_block if isinstance(why_block, dict) else {
            "unavailable": "this Rider's status has no why_no_trade block (it runs an older build)"}
        out["scanner_top10"] = _picks(st.get("scanner"), SCANNER_KEYS, 10)
        out["notes"] = [str(n) for n in (st.get("notes") or [])[-8:]]
    elif bid == "financial_ian":
        feed = st.get("feed") if isinstance(st.get("feed"), dict) else {}
        out["status"] = st.get("status")
        out["feed"] = {"state": feed.get("state") or "UNKNOWN", "reason": feed.get("reason") or "",
                       "vendor": feed.get("vendor") or "", "synthetic": feed.get("synthetic"),
                       "events": feed.get("events")}
        out["data_label"] = st.get("data_label")
        mt5 = st.get("mt5") if isinstance(st.get("mt5"), dict) else {}
        out["metatrader_connected"] = mt5.get("connected")
        out["open_trades"] = _picks(st.get("open_positions"), RIDE_KEYS, 20)
        out["own_record_today"] = _scalars(st.get("today"))
    else:
        out["status"] = st.get("status")
        out["open_trades"] = _picks(st.get("open"), RIDE_KEYS, 20)
        if st.get("unmanaged"):
            out["unmanaged"] = _picks(st.get("unmanaged"), ("ticket", "symbol", "side", "mode", "note"), 10)
        if bid == "momentum_runner":
            out["note"] = st.get("note")
            out["refused_today"], out["skipped_today"] = st.get("refused"), st.get("skipped")
    return out


def _tnb_paper_open(data: Path) -> dict:
    """Trend & Breakout's open PAPER trades from its paper record
    (data/tnb_paper.sqlite), opened read-only: simulated, never money."""
    path = data / TNB_PAPER_DB
    if not path.exists():
        return {"trades": [], "note": f"no paper record yet ({TNB_PAPER_DB})"}
    try:
        con = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
        try:
            rows = con.execute("SELECT ticket, symbol, side, volume, entry, sl, open_utc FROM positions "
                               "ORDER BY open_utc").fetchall()
        finally:
            con.close()
    except Exception as exc:
        return {"unavailable": f"the paper record could not be read: {exc}"}
    trades = [{"ticket": r[0], "symbol": r[1], "side": r[2], "volume": r[3], "entry": r[4], "stop": r[5],
               "opened": r[6]} for r in rows[:20]]
    return {"trades": trades, "count": len(rows),
            "source": f"the paper record ({TNB_PAPER_DB}), read-only: simulated orders, not money"}


def _tnb_bot(cfg: Config, p: dict, status: dict, page_ok: bool) -> dict:
    """Trend & Breakout, from the trader's own page (it has no status file)
    and, on PAPER, its paper record."""
    st = status.get("status") if page_ok and isinstance(status.get("status"), dict) else {}
    mode = str(st.get("tnb_mode") or "").upper()
    if mode not in ("LIVE", "PAPER"):
        try:
            from .standing import tnb_mode
            mode = tnb_mode(cfg)
        except Exception:
            mode = "?"
    out: dict = {"mode": mode, "running": bool(p.get("up"))}
    if mode == "PAPER":
        out["open_paper_trades"] = _tnb_paper_open(Path(cfg.ops.data_dir))
    if not page_ok:
        out["health"] = f"the trader's page did not answer: {p.get('reason') or 'no reason given'}"
        return out
    health = status.get("health") if isinstance(status.get("health"), dict) else {}
    out["health"] = f"{st.get('bot') or '?'}: {health.get('summary') or 'no health summary on the page'}"
    out["build"] = st.get("build")
    out["last_scan"] = st.get("last_scan")
    out["last_trade"] = st.get("last_trade")
    out["not_trading_because"] = [str(x) for x in (st.get("not_trading_because") or [])][:10]
    out["positions_on_page"] = _picks(status.get("positions"), TNB_POSITION_KEYS, 20)
    out["positions_on_page_note"] = ("as its page lists them: on PAPER its paper book (simulated) plus any real "
                                     "position left from LIVE; the real ones are also under open_on_account")
    out["thinking"] = _picks(status.get("thinking"), THINKING_KEYS, 5)
    return out


def bots_report(cfg: Config, p: dict, now: Optional[dt.datetime] = None) -> dict:
    """Every bot in one small file, for reading the bots from GitHub: its
    mode, its health line, today's trades and result (from the page's
    standing block, never worked out here), its open trades, and what each
    bot needs said about it (see the module's notes). Read-only."""
    now = to_utc(now or utcnow()).replace(microsecond=0)
    data = Path(cfg.ops.data_dir)
    status = p.get("status") if isinstance(p.get("status"), dict) else {}
    page_ok = bool(status) and "unavailable" not in status
    sd = status.get("standing") if page_ok and isinstance(status.get("standing"), dict) else {}
    strategies = status.get("strategies") if page_ok and isinstance(status.get("strategies"), dict) else {}
    open_live = status.get("open_live") if page_ok and isinstance(status.get("open_live"), list) else None
    why_down = "" if page_ok else str(p.get("reason") or status.get("unavailable") or "")
    rows = {b.get("id"): b for b in (sd.get("bots") or []) if isinstance(b, dict)}
    from .standing import ACTIVE_BOTS, BOTS, bot_modes_and_magics
    try:
        modes_set = bot_modes_and_magics(cfg)               # what the settings files say (read-only)
    except Exception:
        modes_set = {}
    if sd:
        account = {**_pick(sd, ACCOUNT_KEYS), **_today_fields(sd),
                   "about": "the page's standing block (MetaTrader's deal history): real money, every bot"}
    else:
        account = {"unavailable": "the trader's page did not answer, so the account's figures are not known"
                                  + (f" ({why_down})" if why_down else "")}
    out: dict = {
        "ts_utc": now.isoformat(),
        "about": ("One entry per bot, written by the ten-minute pulse from each bot's own status file and the "
                  "trader's page (read-only). 'today' is copied from the page's standing block (MetaTrader's own "
                  "record for the bot's magic number, for the page's today) - real money. 'practice_today' is "
                  "paper: not money. Nothing here is worked out again."),
        "trader_up": bool(p.get("up")), "trader_reason": p.get("reason") or "",
        "account": account,
        "bots": [],
    }
    for bid, label in ACTIVE_BOTS:
        entry: dict = {"id": bid, "label": label}
        if bid in modes_set:
            entry["mode_set"] = modes_set[bid][1]
        try:
            entry.update(_tnb_bot(cfg, p, status, page_ok) if bid == "market_intelligence"
                         else _file_bot(data, bid, now))
        except Exception as exc:                            # one bot's trouble never stops the file
            entry.update({"running": False, "health": f"not running / not readable: {exc}"})
        entry.update(_money_today(rows.get(bid), sd, strategies.get(bid), page_ok, why_down))
        if open_live is not None:
            entry["open_on_account"] = _picks([r for r in open_live if isinstance(r, dict) and r.get("bot") == label],
                                              OPEN_LIVE_KEYS, 20)
        out["bots"].append(entry)
    for bid, label in BOTS[len(ACTIVE_BOTS):]:              # the retired bots: only while they have money to show
        row = rows.get(bid)
        if isinstance(row, dict) and (row.get("today_trades") or row.get("open_trades")):
            out["bots"].append({"id": bid, "label": label, "mode": "RETIRED",
                                **_money_today(row, sd, None, page_ok, why_down)})
    return _scrub(out)


def _trim_steps():
    """Applied in turn while bots.json is over its cap: the least needed first."""
    def rider(e: dict) -> dict:
        return e.get("why_no_trade") if isinstance(e.get("why_no_trade"), dict) else {}

    def per_bot(fn):
        def run(rep: dict) -> None:
            for e in rep.get("bots", []):
                fn(e)
        return run

    def markets_to_10(e):
        w = rider(e)
        m = w.get("markets")
        if isinstance(m, dict) and len(m) > 10:
            keep = sorted(m, key=lambda s: -float(((m[s] or {}).get("best") or {}).get("score") or 0.0))[:10]
            w["markets"] = {s: m[s] for s in sorted(keep)}
            w["markets_trimmed"] = "only the 10 markets with the best scores are listed (size cap)"

    def lists_to(n):
        def cut(e):
            for k in ("scanner_top10", "thinking", "open_trades", "open_on_account", "positions_on_page", "notes",
                      "failing_checks"):
                if isinstance(e.get(k), list) and len(e[k]) > n:
                    e[k] = e[k][:n]
            w = rider(e)
            if isinstance(w.get("near_misses"), list) and len(w["near_misses"]) > n:
                w["near_misses"] = w["near_misses"][-n:]
        return cut

    def drop_markets(e):
        w = rider(e)
        if "markets" in w:
            w.pop("markets", None)
            w["markets_trimmed"] = "the per-market rows were left out (size cap)"

    def bare(e):
        for k in [k for k in e if k not in ("id", "label", "mode", "mode_set", "running", "health", "today",
                                           "practice_today")]:
            e.pop(k, None)
        e["trimmed"] = "details left out to keep the file small"

    return [per_bot(markets_to_10), per_bot(lists_to(5)), per_bot(drop_markets), per_bot(lists_to(2)), per_bot(bare)]


def bots_json(cfg: Config, p: dict, now: Optional[dt.datetime] = None, max_bytes: int = BOTS_MAX_BYTES) -> str:
    """bots_report as text, kept under ``max_bytes``. Never raises: a report
    that cannot be built is a short file saying why."""
    try:
        rep = bots_report(cfg, p, now)
    except Exception as exc:
        rep = {"ts_utc": str(p.get("ts_utc") or ""), "unavailable": f"the bots' report could not be built: {exc}"}
    text = json.dumps(rep, indent=1, default=str) + "\n"
    for step in _trim_steps():
        if len(text.encode("utf-8")) <= max_bytes:
            break
        step(rep)
        rep["trimmed"] = "some details were left out to keep this file under its size cap"
        text = json.dumps(rep, indent=1, default=str) + "\n"
    if len(text.encode("utf-8")) > max_bytes:
        text = json.dumps({"ts_utc": rep.get("ts_utc"), "unavailable": "the bots' report was too big to publish"}) + "\n"
    return text


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
    from .report_upload import UploadIncomplete, read_token, upload
    token = read_token(args.config)
    if not token:
        print("(not published: no GitHub token in secrets.json)")
        return 0
    bots = bots_json(cfg, p)                       # never raises: a broken bot is an entry saying so
    files = pulse_files(p, uptime_lines(cfg), bots)
    try:
        # the same upload as the nightly report: a clash with another writer is retried with a fresh read
        n = upload(files, cfg.report_repo, cfg.report_branch, token, day="live")
        print(f"published {n} files to {cfg.report_repo} ({cfg.report_branch})")
    except UploadIncomplete as exc:
        print(f"(published {exc.written} of {len(files)} files; not published: "
              + "; ".join(f"{path} ({why})" for path, why in exc.failed) + ")")
        return 1
    except Exception as exc:
        print(f"(not published: {exc})")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
