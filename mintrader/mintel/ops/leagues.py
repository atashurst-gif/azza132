"""Three leagues and one career a strategy (Aaron, 14 Oct 2026).

* PAPER LEAGUE - every test, on its own paper (practice) record after costs: the squad's own figures
  (``squad.load_world``: the bots' PAPER rows, Trend & Breakout's parts, every lab idea's PRACTICE rows).
* DEMO LEAGUE - every strategy that has been on the IC Markets DEMO account, ranked by the BROKER's results after
  all costs: for the bots and Trend & Breakout's parts, MetaTrader's closed positions by magic number
  (``weekly.live_trades`` on ``weekly.live_positions_json``) for each stint on DEMO from the weekly history; for
  lab ideas, the lab-demo record (data/labdemo/labdemo.sqlite: the broker's closed-deal figures by ticket, by idea
  and version). Unknown (not zero) while MetaTrader - or that record - cannot be read.
* REAL MONEY LEAGUE - present, empty: "Not connected yet". No real account is connected anywhere in the code
  (``realmoney.REAL_TRADING_ENABLED`` is False).

THE THREE ARE NEVER ADDED TOGETHER. Each league carries its own total; nothing here returns a sum across them.

ONE CAREER A STRATEGY, by one stable id - the squad's test id ("lab:<idea id>", "bot:runner",
"tnb:<approach>:<kind of market>"), the same id the weekly pick, the history and the lab use. A career holds:
every trade by league (kept apart), the daily ranking history (data/leagues/YYYY-MM-DD.json), promotions and
demotions with their reasons (data/weekly/history.jsonl), versions and adjustments (the lab's parent and child
variants and its version; Trend & Breakout's rule-set name), the markets, sessions and conditions where it made
and lost money, the risk it took and its worst drawdown. Retired ideas keep their careers: nothing is deleted.

THE QUALITY behind the profit ranking (``quality``): profitability (net after costs), consistency (the share
of its trading days that finished up), risk-adjusted (net / its worst drawdown, and a Sharpe-like figure: the
mean trade / the spread of its trades) and frequency (trades a trading day). Four plain checks make the
stars: in profit; up on at least CONSISTENT_SHARE of its days; net at least RISK_ADJ_MIN x its worst
drawdown; at least ACTIVE_PER_DAY trades a day - and none before QUALITY_MIN_TRADES trades (too early to say). The
league stays sorted by profit; the stars are shown beside it.

THE DAILY SNAPSHOT (``write_snapshot``): one file a UK trading day, data/leagues/YYYY-MM-DD.json, created once
(exclusive create) and never rewritten or removed - the store only ever grows. It is written by the pulse
(``daily_snapshots``) for the latest finished trading days that have none yet, as each league stood at the end
of that UK day (worked out from the time-stamped records, so a late write says the same as an on-time one).
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import math
import os
from pathlib import Path
from statistics import median, pstdev
from typing import Iterable, Optional, Sequence

from ..clock import TZ_LONDON, to_utc, utcnow
from .standing import uk_date, uk_midnight

log = logging.getLogger("mintel.leagues")
UTC = dt.timezone.utc

PAPER, DEMO, REAL = "PAPER", "DEMO", "REAL"
LEAGUES = (PAPER, DEMO, REAL)
LEAGUE_WORDS = {PAPER: "PAPER LEAGUE", DEMO: "DEMO LEAGUE", REAL: "REAL MONEY LEAGUE"}
REAL_NOT_CONNECTED = "Not connected yet — no real money is ever traded without your approval"
SNAP_DIR = "leagues"
SNAP_SCHEMA = 1
SNAP_CATCH_UP_DAYS = 7          # the pulse fills in at most this many missing trading days back
CONSISTENT_SHARE = 0.55         # stars: up on at least this share of its trading days
RISK_ADJ_MIN = 2.0              # stars: net at least this many times its worst drawdown
ACTIVE_PER_DAY = 1.0            # stars: at least this many trades a trading day
QUALITY_MIN_TRADES = 10         # no stars before this many trades: too early to judge (shown as "early")
CONDITION_MIN_TRADES = 3        # best / worst conditions: only with this many trades behind them
SESSIONS = ((0, 7, "Asia"), (7, 12, "London"), (12, 16, "London–New York"), (16, 21, "New York"), (21, 24, "Late"))


# ------------------------------------------------------------------ small helpers --
def _when(v) -> Optional[dt.datetime]:
    if v is None or v == "":
        return None
    if isinstance(v, dt.datetime):
        return to_utc(v if v.tzinfo else v.replace(tzinfo=UTC))
    try:
        t = dt.datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return to_utc(t if t.tzinfo else t.replace(tzinfo=UTC))


def _f(v) -> Optional[float]:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def session_of(t: dict) -> str:
    """The session a trade was taken in (its open, else its close), by the UK clock."""
    w = _when(t.get("opened")) or _when(t.get("closed"))
    if w is None:
        return ""
    h = w.astimezone(TZ_LONDON).hour
    return next(name for a, b, name in SESSIONS if a <= h < b)


def trading_days(trades: Iterable[dict]) -> list[dt.date]:
    return sorted({uk_date(t["closed"]) for t in trades if t.get("closed") is not None})


def max_drawdown(trades: Sequence[dict]) -> float:
    """The worst fall of the running net after costs from a peak (a positive £ figure), trade by trade in the
    order they closed. Starting at zero counts: a first loss is a drawdown."""
    peak = run = dd = 0.0
    for t in sorted(trades, key=lambda x: x["closed"]):
        run += float(t["net"])
        peak = max(peak, run)
        dd = max(dd, peak - run)
    return round(dd, 2)


def quality(trades: Sequence[dict], until: Optional[dt.datetime] = None) -> dict:
    """The quality behind the profit: {net, trades, days (the UK days it traded), up_days, consistency, max_dd,
    net_to_dd, sharpe, per_day, mean, stars, checks}. Only the trades' own figures.

    ``per_day`` (and the "active" check) is its trades a UK TRADING DAY in the window, from its first trade here
    to ``until`` (the window's end, at most now; its last trade when not given, or when its last trade is later:
    a lab idea's last demo position closes after its demote) - the weekly pick's own measure - never its trades
    on the days it happened to trade, which would make a strategy that trades on one day a week look five times
    as busy."""
    from .weekly import trading_days_between
    trades = [t for t in trades if t.get("closed") is not None]
    nets = [float(t["net"]) for t in trades]
    n = len(nets)
    by_day: dict = {}
    for t in trades:
        d = uk_date(t["closed"])
        by_day[d] = by_day.get(d, 0.0) + float(t["net"])
    days = len(by_day)
    tdays = (trading_days_between(min(by_day), max(max(by_day), uk_date(until)) if until is not None
                                  else max(by_day))
             if by_day else None)
    up = sum(1 for v in by_day.values() if v > 0)
    net = round(sum(nets), 2)
    dd = max_drawdown(trades)
    sd = pstdev(nets) if n > 1 else 0.0
    q = {"net": net, "trades": n, "days": days, "up_days": up,
         "consistency": (up / days) if days else None, "max_dd": dd,
         "net_to_dd": (round(net / dd, 2) if dd > 0 else None),
         "sharpe": (round((net / n) / sd, 3) if n > 1 and sd > 0 else None),
         "per_day": (round(n / tdays, 2) if tdays else None), "mean": (round(net / n, 2) if n else None)}
    checks = {"profitable": net > 0,
              "consistent": bool(days) and up / days >= CONSISTENT_SHARE,
              "risk_adjusted": net > 0 and (dd == 0 or net / dd >= RISK_ADJ_MIN),
              "active": bool(tdays) and n / tdays >= ACTIVE_PER_DAY}
    q["checks"] = checks
    q["early"] = n < QUALITY_MIN_TRADES
    q["stars"] = 0 if q["early"] else sum(1 for v in checks.values() if v)
    return q


def stars_text(n: int) -> str:
    n = max(0, min(4, int(n or 0)))
    return "★" * n + "☆" * (4 - n)


# ------------------------------------------------------------------ DEMO: the stints --
def magic_of(item: dict) -> Optional[int]:
    """The magic number MetaTrader's closed positions are read under for a test's DEMO money: its own when the
    record holds one, the bot's for one of the four bots, Trend & Breakout's for one of its parts. None for a lab
    idea: its DEMO money is the lab-demo record (``lab_demo_trades``), the broker's own closed-deal figures by
    ticket - read once, never also by magic, so nothing is counted twice."""
    from . import weekly as W
    cid = str(item.get("id") or "")
    if cid.startswith("lab:"):
        return None                     # a lab idea's DEMO money is its own demo record (the broker's, by ticket)
    try:
        if item.get("magic"):
            return int(item["magic"])
    except (TypeError, ValueError):
        pass
    if cid.startswith("bot:"):
        b = W.BOTS.get(cid[4:]) or W.BOTS.get(str(item.get("bot") or ""))
        return int(b[2]) if b else None
    if cid.startswith("tnb:"):
        return W.TNB_MAGIC
    return None


def _item_fields(cid: str, src: dict) -> dict:
    """kind, bot, tactic, group of a test id (from the record when it says, else from the id itself)."""
    out = {"id": cid, "kind": src.get("kind") or cid.split(":", 1)[0]}
    if cid.startswith("bot:"):
        out["bot"] = src.get("bot") or cid[4:]
    elif cid.startswith("tnb:"):
        parts = cid.split(":")
        if len(parts) >= 3:
            out["tactic"], out["group"] = src.get("tactic") or parts[1], src.get("group") or parts[2]
    return out


def demo_stints(state: Optional[dict], hist: Sequence[dict]) -> list[dict]:
    """Every stint a test has had on DEMO, oldest first: {id, kind, bot/tactic/group, magic, label, part,
    since, until (None while it is there), why_in, why_out, risk_gbp, version}. From the weekly history
    (a promote opens a stint, a demote or a hand change closes it) and the weekly state (what is there now, with
    its magic number and risk)."""
    now_items = {str(p.get("id")): p for p in (state or {}).get("promoted") or () if p.get("id")}
    stints: list[dict] = []
    open_: dict = {}
    for e in sorted((e for e in hist if isinstance(e, dict)), key=lambda e: str(e.get("ts_utc") or "")):
        cid = str(e.get("id") or "")
        t = _when(e.get("ts_utc"))
        if not cid or t is None:
            continue
        ev = e.get("event")
        if ev == "promote":
            if cid in open_:
                continue
            s = dict(_item_fields(cid, e), label=e.get("label"), part=e.get("part") or "", since=t, until=None,
                     why_in=str(e.get("why") or ""), why_out="", risk_gbp=_f(e.get("risk_gbp")),
                     version=e.get("version"), magic=None)
            open_[cid] = s
            stints.append(s)
        elif ev in ("demote", "gone"):
            s = open_.pop(cid, None)
            if s is not None:
                s["until"], s["why_out"] = t, str(e.get("why") or "")
    for cid, p in now_items.items():                   # on DEMO now, whether or not the history says so
        since = _when(p.get("since_utc"))
        s = open_.get(cid)
        if s is None and since is not None:
            s = dict(_item_fields(cid, p), label=p.get("label"), part=p.get("part") or "", since=since, until=None,
                     why_in=str(p.get("why") or "on Demo"), why_out="", risk_gbp=None, version=p.get("version"),
                     magic=None)
            stints.append(s)
        if s is not None:
            for k in ("bot", "tactic", "group"):
                if p.get(k):
                    s[k] = p[k]
            s["magic"] = p.get("magic") or s.get("magic")
            s["risk_gbp"] = _f(p.get("risk_gbp")) or s.get("risk_gbp")
            s["version"] = p.get("version") or s.get("version")
    for s in stints:
        s["magic"] = magic_of(s)
    stints.sort(key=lambda s: s["since"])
    return stints


LABDEMO_DB = ("labdemo", "labdemo.sqlite")         # the lab-demo engine's permanent record (data/labdemo/)


def _ro_rows(path: Path, sql: str, args: tuple = ()) -> Optional[list]:
    """Rows read-only; [] when the file is not there; None when it is there but cannot be read."""
    import sqlite3
    if not path.exists():
        return []
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
        try:
            con.row_factory = sqlite3.Row
            return [dict(r) for r in con.execute(sql, args).fetchall()]
        finally:
            con.close()
    except sqlite3.Error:
        return None


def lab_demo_readable(data_dir) -> bool:
    """False when the lab-demo record is there but cannot be read (its figures are then unknown, never zero)."""
    if not data_dir:
        return True
    return _ro_rows(Path(data_dir).joinpath(*LABDEMO_DB), "SELECT 1 FROM sqlite_master LIMIT 1") is not None


def lab_demo_trades(data_dir) -> dict:
    """{"lab:<idea id>": [trades]} - the lab ideas' DEMO record, read-only and tolerant of how it is kept:
    data/labdemo/labdemo.sqlite ``demo_trades`` (the lab-demo engine's: the broker's closed-deal net by ticket,
    with the idea's id and version, its risk, its R and its £1,500 equivalent); and, should the lab keep one, any
    table in data/lab/lab.sqlite whose name says demo (or live) with an idea id, a net and a close time, or a
    practice row marked DEMO or LIVE. Each trade: {closed, opened, net, symbol, risk, r, version, ticket,
    equiv_1500, source}. Nothing for a made-up (synthetic) lab. A trade is taken once (by its ticket)."""
    if not data_dir:
        return {}
    from . import weekly as W
    data = Path(data_dir)
    summ = W._read(data / "lab" / "summary.json")
    if isinstance(summ, dict) and summ.get("synthetic"):
        return {}
    out: dict = {}
    seen: set = set()

    def add(cid, r, source):
        cid = cid[4:] if cid.startswith("lab:") else cid
        t = _when(r.get("closed_utc") or r.get("closed"))
        net = _f(r.get("net_gbp") if r.get("net_gbp") is not None else r.get("net"))
        if not cid or t is None or net is None:
            return
        key = (cid, r.get("ticket") or r.get("position") or (t.isoformat(), net))
        if key in seen:
            return
        seen.add(key)
        eq = next((_f(v) for k, v in r.items() if "1500" in str(k) and _f(v) is not None), None)
        out.setdefault(f"lab:{cid}", []).append(
            {"closed": t, "opened": _when(r.get("opened_utc") or r.get("opened")), "net": round(net, 2),
             "symbol": str(r.get("symbol") or ""), "risk": _f(r.get("risk_gbp") or r.get("risk")), "r": _f(r.get("r")),
             "volume": _f(r.get("volume")), "version": r.get("version") or r.get("idea_version"),
             "ticket": r.get("ticket") or r.get("position"), "equiv_1500": eq, "source": source,
             "costs": (abs(_f(r.get("commission_gbp"))) if _f(r.get("commission_gbp")) is not None else _f(r.get("costs"))),
             "tier_pct": _f(r.get("tier_pct"))})
    for r in _ro_rows(data.joinpath(*LABDEMO_DB), "SELECT * FROM demo_trades WHERE closed_utc IS NOT NULL "
                                                  "ORDER BY closed_utc") or ():
        if str(r.get("mode") or "DEMO").upper() in ("DEMO", "LIVE"):
            add(str(r.get("idea_id") or ""), r, "the broker, by ticket")
    db = data / "lab" / "lab.sqlite"
    if db.exists():
        tables = [str(r.get("name")) for r in W._rows(db, "SELECT name FROM sqlite_master WHERE type='table'")]
        for name in tables:
            low = name.lower()
            if ("demo" not in low and "live" not in low) or not name.replace("_", "").isalnum():
                continue
            cols = {str(r.get("name")) for r in W._rows(db, f"PRAGMA table_info({name})")}
            idc = next((c for c in ("candidate_id", "idea_id", "cid") if c in cols), None)
            if idc is None or not ({"closed_utc", "closed"} & cols) or not ({"net_gbp", "net"} & cols):
                continue
            for r in W._rows(db, f"SELECT * FROM {name}"):
                add(str(r.get(idc) or ""), r, "the lab's demo record")
        for r in W._rows(db, "SELECT * FROM practice_trades WHERE closed_utc IS NOT NULL AND mode IN ('DEMO','LIVE')"):
            add(str(r.get("candidate_id") or ""), r, "the lab's demo record")
    for v in out.values():
        v.sort(key=lambda t: t["closed"])
    return out


def demo_trades(stints: Sequence[dict], positions: Optional[list], data_dir, lab_rows: Optional[dict] = None
                ) -> tuple[dict, dict]:
    """({id: [its DEMO trades, every stint]}, {id: known}). A stint with a magic number reads MetaTrader's
    closed positions (``positions``, None while MetaTrader has not been read: then it is not known); a lab idea
    without one reads the lab's own demo record. Never both for the same id, so nothing is counted twice."""
    from . import weekly as W
    lab_rows = lab_demo_trades(data_dir) if lab_rows is None else lab_rows
    lab_ok = lab_demo_readable(data_dir)
    out: dict = {}
    known: dict = {}
    opened_at: dict = {}                              # MetaTrader's open time by position (ticket)
    for p in positions or ():
        try:
            tk = int(p.get("position") or 0)
        except (TypeError, ValueError, AttributeError):
            continue
        if tk > 0 and _when(p.get("opened")) is not None:
            opened_at[tk] = _when(p.get("opened"))
    for s in stints:
        cid = s["id"]
        out.setdefault(cid, [])
        if s.get("magic") is None:
            if cid.startswith("lab:") and not lab_ok:
                known[cid] = False
            continue
        known.setdefault(cid, True)
        if positions is None:
            known[cid] = False
            continue
        item = dict(s, since_utc=s["since"].isoformat(), magic=s["magic"])
        try:
            got = W.live_trades(item, positions, data_dir)[0]
        except Exception as exc:                          # one broken record never stops the rest
            log.warning("leagues: demo %s: %s", cid, exc)
            known[cid] = False
            continue
        lab_open = {int(x.get("ticket") or 0): x.get("opened") for x in lab_rows.get(cid) or ()
                    if str(x.get("ticket") or "").isdigit() and x.get("opened") is not None}
        for t in got:
            c = _when(t.get("closed"))
            if c is None or (s.get("until") is not None and c >= s["until"]):
                continue
            try:
                tk = int(t.get("ticket") or 0)
            except (TypeError, ValueError):
                tk = 0
            o = _when(t.get("opened")) or opened_at.get(tk) or _when(lab_open.get(tk))
            out[cid].append(dict(t, closed=c, opened=o, risk=None,
                                 stated_risk=s.get("risk_gbp"), source="MetaTrader"))
    magic_ids = {s["id"] for s in stints if s.get("magic") is not None}
    for cid, trades in lab_rows.items():
        if cid in magic_ids:
            continue
        out.setdefault(cid, []).extend(trades)
        known.setdefault(cid, lab_ok)
    for cid in out:
        known.setdefault(cid, True)
        out[cid].sort(key=lambda t: t["closed"])
    return out, known


# ------------------------------------------------------------------ the three leagues --
def _row(t: dict, trades: Sequence[dict], until: Optional[dt.datetime] = None,
         record: Optional[Sequence[dict]] = None, open_day: Optional[dt.date] = None) -> dict:
    from .squad import figures
    q = quality(trades, until)
    return dict({k: t.get(k) for k in ("id", "nick", "desc", "kind", "retired", "live", "ready")},
                **figures(trades, record, open_day), quality=q, stars=q["stars"])


def pace_end(stints: Optional[Sequence[dict]], at: dt.datetime) -> Optional[dt.datetime]:
    """Where a DEMO record's pace (its trades a trading day) stops: ``at`` while one of its stints that began by
    then is still running, else the end of its last such stint (never after ``at``); None (its last trade) with no
    stint on record. The league row and the career page both use it, so the two always agree."""
    ss = [s for s in stints or () if s.get("since") is None or s["since"] <= at]
    if not ss:
        return None
    ends = [s.get("until") for s in ss]
    return at if any(u is None for u in ends) else min(at, max(ends))


def league_rows(tests: dict, trades_by: dict, start: dt.datetime, end: dt.datetime, known: Optional[dict] = None,
                until_by: Optional[dict] = None, now: Optional[dt.datetime] = None) -> list[dict]:
    """One league's rows for [start, end): every strategy with a trade in it, best £ after costs first (then
    the better quality, then the name), with ``pos`` from 1. ``trades_by``: {id: that league's trades}.
    ``until_by``: {id: where its pace stops (None: its last trade)} for a strategy that has left (a retired idea,
    a DEMO record whose stints have all ended), capped at the window's end; the window's end otherwise. A
    streak is counted on the whole record in this league up to ``end`` (``now``: today, unfinished, never
    ends one)."""
    from .squad import open_day_of, record_until
    at = end - dt.timedelta(microseconds=1)
    od = open_day_of(end, to_utc(now) if now is not None else utcnow())
    rows = []
    for cid, trades in trades_by.items():
        tr = [x for x in trades if start <= x["closed"] < end]
        if not tr:
            continue
        t = tests.get(cid) or {"id": cid, "nick": cid, "desc": ""}
        u = (until_by or {}).get(cid, at)
        # its pace over its trading days here; its streak over its whole record in this league up to the end
        r = _row(t, tr, None if u is None else min(u, at), record_until(trades, end), od)
        r["known"] = (known or {}).get(cid, True)
        rows.append(r)
    rows.sort(key=lambda r: (-r["net"], -r["stars"], str(r.get("nick")), str(r["id"])))
    for i, r in enumerate(rows, 1):
        r["pos"] = i
    return rows


def _total(rows: Sequence[dict]) -> dict:
    trades = sum(int(r["trades"]) for r in rows)
    wins = sum(int(r["wins"]) for r in rows)
    return {"net": round(sum(float(r["net"]) for r in rows), 2), "trades": trades,
            "win_rate": (100.0 * wins / trades) if trades else None, "strategies": len(rows),
            "winning": sum(1 for r in rows if r["net"] > 0)}


def real_league() -> dict:
    """The REAL MONEY LEAGUE: empty, not connected."""
    from . import realmoney as RM
    return {"league": REAL, "title": LEAGUE_WORDS[REAL], "connected": RM.real_connected(), "rows": [],
            "total": None, "words": REAL_NOT_CONNECTED}


def build(world: dict, data_dir, positions: Optional[list], start: dt.datetime, end: dt.datetime,
          lab_demo: Optional[dict] = None, as_of: Optional[dt.datetime] = None) -> dict:
    """The three leagues for [start, end), each on its own: {PAPER, DEMO, REAL, stints, demo_trades, demo_known}.
    Each league: {league, title, rows, total}. The totals are never added together. ``as_of``: the moment the
    on-DEMO set is taken at (the stints running then); None: the stints open now (the live pages)."""
    from . import weekly as W
    tests = world.get("tests") or {}
    paper_by = {cid: t.get("trades") or [] for cid, t in tests.items()}
    stints = demo_stints({"promoted": world.get("promoted") or []}, world.get("history") or [])
    d_by, d_known = demo_trades(stints, positions, data_dir, lab_demo)
    # the pace stops where its career's does: a retired idea's at its last trade, a demo record at its last stint's end
    at = end - dt.timedelta(microseconds=1)
    stints_of: dict = {}
    for s in stints:
        stints_of.setdefault(s["id"], []).append(s)
    p_rows = league_rows(tests, paper_by, start, end,
                         until_by={cid: None for cid, t in tests.items() if t.get("retired")})
    d_rows = league_rows(tests, d_by, start, end, d_known,
                         until_by={cid: pace_end(stints_of.get(cid), at) for cid in d_by})
    on_demo = ({s["id"] for s in stints if s.get("until") is None} if as_of is None else
               {s["id"] for s in stints if s["since"] < as_of and (s.get("until") is None or s["until"] > as_of)})
    for r in d_rows:
        r["on_demo"] = r["id"] in on_demo
    for cid in on_demo:                               # on DEMO with no demo trade in the period yet
        if not any(r["id"] == cid for r in d_rows):
            t = tests.get(cid) or {"id": cid, "nick": W.short_name(next(s for s in stints if s["id"] == cid)),
                                   "desc": ""}
            r = _row(t, [])
            r.update(pos=None, known=d_known.get(cid, True), on_demo=True)
            d_rows.append(r)
    demo_total = _total([r for r in d_rows if r.get("known", True)])
    demo_total["active"] = len(on_demo)
    demo_total["known"] = all(d_known.get(cid, True) for cid in on_demo) if on_demo else True
    return {PAPER: {"league": PAPER, "title": LEAGUE_WORDS[PAPER], "rows": p_rows, "total": _total(p_rows)},
            DEMO: {"league": DEMO, "title": LEAGUE_WORDS[DEMO], "rows": d_rows, "total": demo_total},
            REAL: real_league(), "stints": stints, "demo_trades": d_by, "demo_known": d_known}


# ------------------------------------------------------------------ versions --
def version_of(test: dict, lab_row: Optional[dict] = None) -> str:
    """The exact version a career is on now: a lab idea's own version number (its id is its definition);
    Trend & Breakout's rule-set name for one of its parts; the bot's current rules otherwise."""
    kind = str(test.get("kind") or "")
    if kind == "lab":
        v = (lab_row or {}).get("version")
        return f"v{v}" if v not in (None, "") else "v1"
    if kind == "tnb":
        from ..config import STRATEGY_NAME
        return STRATEGY_NAME
    return "current rules"


def versions_of(test: dict, data_dir) -> list[dict]:
    """Versions and adjustments, newest last: a lab idea's parent (the idea it was made from) and its children
    (the variants made from it), each kept; Trend & Breakout's rule set; the bot's current rules."""
    out = []
    kind = str(test.get("kind") or "")
    if kind == "lab" and data_dir:
        from . import weekly as W
        db = Path(data_dir) / "lab" / "lab.sqlite"
        ref = str(test.get("ref") or str(test.get("id"))[4:])
        me = (W._rows(db, "SELECT id, version, parent_id, created_utc, retired_utc FROM candidates WHERE id=?",
                      (ref,)) or [{}])[0]
        if me.get("parent_id"):
            par = (W._rows(db, "SELECT id, name, version, created_utc FROM candidates WHERE id=?",
                           (me["parent_id"],)) or [{}])[0]
            out.append({"kind": "parent", "id": f"lab:{me['parent_id']}", "version": f"v{par.get('version') or 1}",
                        "when": _when(par.get("created_utc")), "words": "the idea it was made from (kept)"})
        out.append({"kind": "this", "id": test.get("id"), "version": version_of(test, me),
                    "when": _when(me.get("created_utc")),
                    "words": "this version" + (" (retired, kept on record)" if me.get("retired_utc") else "")})
        for k in W._rows(db, "SELECT id, version, created_utc FROM candidates WHERE parent_id=? ORDER BY created_utc",
                         (ref,)):
            out.append({"kind": "child", "id": f"lab:{k['id']}", "version": f"v{k.get('version') or 1}",
                        "when": _when(k.get("created_utc")), "words": "a variant made from it"})
    else:
        out.append({"kind": "this", "id": test.get("id"), "version": version_of(test), "when": None,
                    "words": "the rules it runs now"})
    return out


# ------------------------------------------------------------------ conditions --
def _conditions_of(trades: Sequence[dict]) -> list[dict]:
    """[{what, label, net, trades, mean}] by market, by session and by condition (where the record has one)."""
    groups: dict = {}
    for t in trades:
        keys = [("market", t.get("symbol") or ""), ("session", session_of(t)), ("condition", t.get("condition") or "")]
        for what, label in keys:
            if not label:
                continue
            g = groups.setdefault((what, label), [0.0, 0])
            g[0] += float(t["net"])
            g[1] += 1
    return [{"what": w, "label": lab, "net": round(v[0], 2), "trades": v[1], "mean": round(v[0] / v[1], 2)}
            for (w, lab), v in groups.items()]


def conditions(trades: Sequence[dict], top: int = 3) -> dict:
    """{best, worst, all}: the markets, sessions and conditions with CONDITION_MIN_TRADES+ trades, best and worst
    by their net after costs. Only what made money is "best" and only what lost is "worst"."""
    rows = [r for r in _conditions_of(trades) if r["trades"] >= CONDITION_MIN_TRADES]
    best = sorted((r for r in rows if r["net"] > 0), key=lambda r: (-r["net"], r["label"]))[:top]
    worst = sorted((r for r in rows if r["net"] < 0), key=lambda r: (r["net"], r["label"]))[:top]
    return {"best": best, "worst": worst, "all": sorted(rows, key=lambda r: (r["what"], -r["net"]))}


def tnb_conditions(data_dir, trades: Sequence[dict]) -> list[dict]:
    """Trend & Breakout's trades with the market condition its journal booked (its regime), by ticket."""
    if not data_dir or not trades:
        return list(trades)
    from . import weekly as W
    tickets = {int(t.get("ticket") or 0) for t in trades}
    j = {int(r.get("ticket") or 0): str(r.get("regime") or "") for r in W._rows(
        Path(data_dir) / "journal.sqlite", "SELECT ticket, regime FROM trades")} if tickets else {}
    return [dict(t, condition=j.get(int(t.get("ticket") or 0), "").replace("_", " ").lower()) for t in trades]


# ------------------------------------------------------------------ one career --
def timeline(cid: str, hist: Sequence[dict], data_dir=None) -> list[dict]:
    """A career's moments, oldest first: joined the pool, promoted to DEMO, kept, back to PAPER (each with its
    reason), retired by the Saturday refresh, and the lab's own stage changes."""
    out = []
    for e in hist:
        if str(e.get("id") or "") != cid:
            continue
        t = _when(e.get("ts_utc"))
        ev = str(e.get("event") or "")
        words = {"promote": "Promoted to Demo", "demote": "Back to Paper", "keep": "Kept on Demo",
                 "gone": "Taken off Demo by hand"}.get(ev)
        if t is None or words is None:
            continue
        out.append({"when": t, "kind": ev, "words": words, "why": str(e.get("why") or "")})
    if cid.startswith("lab:") and data_dir:
        from . import weekly as W
        db = Path(data_dir) / "lab" / "lab.sqlite"
        me = (W._rows(db, "SELECT created_utc, retired_utc, retired_reason FROM candidates WHERE id=?", (cid[4:],))
              or [{}])[0]
        if _when(me.get("created_utc")):
            out.append({"when": _when(me["created_utc"]), "kind": "joined", "words": "Joined the squad", "why": ""})
        if _when(me.get("retired_utc")):
            out.append({"when": _when(me["retired_utc"]), "kind": "retired", "words": "Benched by the Saturday refresh",
                        "why": str(me.get("retired_reason") or "")})
        for e in W._rows(db, "SELECT ts_utc, kind, from_stage, to_stage, why FROM events WHERE candidate_id=? "
                             "ORDER BY ts_utc", (cid[4:],)):
            t = _when(e.get("ts_utc"))
            if t is None:
                continue
            out.append({"when": t, "kind": "stage", "words": f"{str(e.get('from_stage') or '').title()} → "
                                                             f"{str(e.get('to_stage') or '').title()}".strip(" →"),
                        "why": str(e.get("why") or "")})
    out.sort(key=lambda x: x["when"])
    return out


def career(cid: str, world: dict, data_dir, built: Optional[dict] = None, positions: Optional[list] = None,
           snapshots: Optional[Sequence[dict]] = None, now: Optional[dt.datetime] = None) -> Optional[dict]:
    """One strategy's career across the three leagues, by its id. None when the id is not known anywhere.

    {id, nick, desc, kind, retired, version, versions, timeline, leagues: {PAPER, DEMO, REAL}, ranks}. Each
    league: {trades (its own), quality, conditions, risk: {median, max}, first, last}; DEMO None when it has
    never been on DEMO, unknown while MetaTrader has not been read; REAL not connected."""
    tests = world.get("tests") or {}
    t = tests.get(cid)
    hist = world.get("history") or []
    if built is None:
        far = dt.datetime(2000, 1, 1, tzinfo=UTC)
        built = build(world, data_dir, positions, far, to_utc(now or utcnow()) + dt.timedelta(days=1))
    d_trades = (built.get("demo_trades") or {}).get(cid)
    if t is None and d_trades is None and not any(str(e.get("id")) == cid for e in hist):
        return None
    if t is None:
        t = {"id": cid, "nick": cid, "desc": "", "kind": cid.split(":", 1)[0], "trades": []}
    paper = list(t.get("trades") or [])
    if t.get("kind") == "tnb":
        paper = tnb_conditions(data_dir, paper)
        d_trades = tnb_conditions(data_dir, d_trades or []) if d_trades is not None else None

    def league(trades, kind, until=None):
        if trades is None:
            return None
        risks = [x for x in (_f(x.get("risk") or x.get("stated_risk")) for x in trades) if x and x > 0]
        return {"league": kind, "trades": trades, "quality": quality(trades, until),
                "conditions": conditions(trades),
                "risk": {"median": round(median(risks), 2) if risks else None,
                         "max": round(max(risks), 2) if risks else None},
                "first": min((x["closed"] for x in trades), default=None),
                "last": max((x["closed"] for x in trades), default=None)}
    stints = [s for s in built.get("stints") or () if s["id"] == cid]
    # its pace runs to now while it is still there (paper: not retired; demo: a stint running), else to the end
    # of its time there (demo: its last stint's end; a retired idea: its last trade)
    at = to_utc(now or utcnow())
    d_until = pace_end(stints, at)                    # the same end as its DEMO league row
    demo = league(d_trades, DEMO, d_until) if (stints or d_trades) else None
    if demo is not None:
        demo["known"] = (built.get("demo_known") or {}).get(cid, True)
        demo["stints"] = stints
        demo["on_demo"] = any(s.get("until") is None for s in stints)
    lab_row = None
    if t.get("kind") == "lab" and data_dir:
        from . import weekly as W
        lab_row = (W._rows(Path(data_dir) / "lab" / "lab.sqlite", "SELECT version FROM candidates WHERE id=?",
                           (cid[4:],)) or [None])[0]
    ranks = []
    for s in snapshots or ():
        p = next((r.get("pos") for r in s.get("paper") or () if r.get("id") == cid), None)
        d = next((r.get("pos") for r in s.get("demo") or () if r.get("id") == cid), None)
        if p is not None or d is not None:
            ranks.append({"date": s.get("date"), "paper": p, "demo": d})
    return {"id": cid, "nick": t.get("nick") or cid, "desc": t.get("desc") or "", "kind": t.get("kind"),
            "retired": bool(t.get("retired")), "version": version_of(t, lab_row), "versions": versions_of(t, data_dir),
            "timeline": timeline(cid, hist, data_dir),
            "leagues": {PAPER: league(paper, PAPER, None if t.get("retired") else at), DEMO: demo,
                        REAL: {"league": REAL, "connected": False, "words": REAL_NOT_CONNECTED}},
            "ranks": ranks}


# ------------------------------------------------------------------ the daily snapshot --
def snap_dir(data_dir) -> Path:
    return Path(data_dir) / SNAP_DIR


def snapshot_path(data_dir, day: dt.date) -> Path:
    return snap_dir(data_dir) / f"{day.isoformat()}.json"


def write_snapshot(data_dir, day: dt.date, payload: dict) -> bool:
    """Write one day's snapshot ONCE: an exclusive create, so an existing day is never rewritten (False). The
    store only grows: nothing here removes or replaces a file."""
    p = snapshot_path(data_dir, day)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(dict(payload, schema=SNAP_SCHEMA, date=day.isoformat()), indent=1, default=str) + "\n"
    try:
        fd = os.open(str(p), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        return False
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(body)
        fh.flush()
        os.fsync(fh.fileno())
    return True


def read_snapshots(data_dir, limit: Optional[int] = None) -> list[dict]:
    """Every day's snapshot, oldest first (the latest ``limit``)."""
    d = snap_dir(data_dir) if data_dir else None
    if d is None or not d.is_dir():
        return []
    names = sorted(n for n in os.listdir(d) if len(n) == 15 and n.endswith(".json"))
    if limit:
        names = names[-limit:]
    out = []
    for n in names:
        try:
            s = json.loads((d / n).read_text())
        except Exception:
            continue
        if isinstance(s, dict):
            out.append(s)
    return out


def snapshot_of(world: dict, data_dir, positions: Optional[list], day: dt.date, reset: dt.datetime,
                lab_demo: Optional[dict] = None, now: Optional[dt.datetime] = None) -> Optional[dict]:
    """Each league as it stood at the end of UK ``day``: positions since the reset with that day's own result.
    None when a DEMO stint that was running then could not be read from MetaTrader (it waits for a later try,
    so a snapshot never freezes an unknown as a figure)."""
    end = uk_midnight(day + dt.timedelta(days=1))
    day_start = uk_midnight(day)
    built = build(world, data_dir, positions, reset, end, lab_demo, as_of=end)
    running = [s for s in built["stints"] if s["since"] < end and (s.get("until") is None or s["until"] > reset)]
    if any(not built["demo_known"].get(s["id"], True) for s in running):
        return None

    def rows(league: str, trades_by: dict) -> list[dict]:
        out = []
        for r in built[league]["rows"]:
            if r.get("pos") is None:
                continue
            today = [x for x in trades_by.get(r["id"]) or () if day_start <= x["closed"] < end]
            out.append({"id": r["id"], "nick": r.get("nick"), "pos": r["pos"], "net": r["net"], "trades": r["trades"],
                        "day_net": round(sum(float(x["net"]) for x in today), 2), "day_trades": len(today),
                        "stars": r.get("stars")})
        return out
    paper_by = {cid: t.get("trades") or [] for cid, t in (world.get("tests") or {}).items()}
    return {"written_utc": to_utc(now or utcnow()).replace(microsecond=0).isoformat(),
            "as_of_utc": end.isoformat(), "since_utc": reset.isoformat(),
            "paper": rows(PAPER, paper_by), "demo": rows(DEMO, built["demo_trades"]),
            "real": {"connected": False, "rows": [], "words": REAL_NOT_CONNECTED},
            "totals": {"paper": built[PAPER]["total"], "demo": built[DEMO]["total"]}}


def days_to_snapshot(data_dir, now: dt.datetime, reset: dt.datetime, back: int = SNAP_CATCH_UP_DAYS) -> list[dt.date]:
    """The finished UK trading days (Monday to Friday, before today, not before the reset) of the last ``back``
    trading days that have no snapshot yet, oldest first."""
    from .squad import trading_days_back
    today = uk_date(now)
    out = []
    for d in trading_days_back(today - dt.timedelta(days=1), back):
        if d < uk_date(reset):
            continue
        if not snapshot_path(data_dir, d).exists():
            out.append(d)
    return sorted(out)


def daily_snapshots(data_dir, config_path=None, now: Optional[dt.datetime] = None,
                    positions: Optional[list] = None) -> list[str]:
    """The pulse's daily job: write the snapshot of each finished trading day that has none yet (at most
    SNAP_CATCH_UP_DAYS back). Returns the days written. Never raises."""
    try:
        now = to_utc(now or utcnow())
        from .squad import load_world, reset_of
        reset = reset_of(None)
        todo = days_to_snapshot(data_dir, now, reset)
        if not todo:
            return []
        world = load_world(data_dir, config_path, now, reset, cache_seconds=0)
        lab_demo = lab_demo_trades(data_dir)
        done = []
        for d in todo:
            snap = snapshot_of(world, data_dir, positions, d, reset, lab_demo, now)
            if snap is not None and write_snapshot(data_dir, d, snap):
                done.append(d.isoformat())
        return done
    except Exception as exc:
        log.warning("leagues: the daily snapshot: %s", exc)
        return []
