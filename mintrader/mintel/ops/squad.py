"""TRADING SQUAD: the front page (Aaron, 13 Oct 2026, the full frontend redesign).

    "/"            the squad: TODAY / 5 DAYS / 1 MONTH / ALL TIME, the headline cards, the PAPER LEAGUE,
                   the DEMO SQUAD (only when it exists), LATEST ACTION and STRATEGY FACTORY
    "/bot?id=..."  one test in plain words, with VIEW ADVANCED DETAILS behind a click
    "/demo"        the first team on its own
    "/factory"     the strategy factory on its own

The spec is docs/MASTER_MISSION.md, "The dashboard". This module is the
PRESENTATION ONLY. It never writes a file, never changes a mode, never touches
an order path and decides nothing: every figure is read from the records the
rest of the system already keeps, read-only.

* PAPER (practice) money is each test's own record after costs, exactly as the
  weekly pick reads it: the bots' own PAPER rows (``verdicts.practice_trades``
  through ``weekly.bot_trades``), Trend & Breakout's paper record split into
  its parts (``weekly.tnb_part_trades``) and every lab idea's PRACTICE rows in
  data/lab/lab.sqlite (the same filters as ``weekly.lab_trades``, retired
  ideas included so a test benched this week still shows what it made).
* DEMO money is MetaTrader's: the account's own figure for the period
  (``standing.period_view``) and each promoted test's closed positions
  (``weekly.live_trades`` on ``weekly.live_positions_json``). Paper and demo
  are never added together anywhere on these pages.
* "Passes the bar" (CHAMPION) and "on DEMO" are the weekly pick's own
  verdicts (``weekly._pool``, the same cached pool Home reads).

Nothing here is stored. The previous day's league (for the rank arrows and
"enters the Top 10") is worked out again from the trades themselves; the
nicknames are worked out from each test's own definition (see ``nickname_base``
and ``assign_nicknames``), so they are the same on every refresh and every
machine.
"""
from __future__ import annotations

import datetime as dt
import html
import logging
from pathlib import Path
from typing import Iterable, Optional, Sequence
from urllib.parse import quote

from ..clock import to_utc, utcnow
from .standing import _months_back, uk_date, uk_midnight

log = logging.getLogger("mintel.squad")
UTC = dt.timezone.utc

# ------------------------------------------------------------------ the rules --
RESET_DEFAULT = "2026-10-05T11:30:42+00:00"     # the demo account's reset (config account_reset_utc)
TIMELINES = (("today", "TODAY"), ("5d", "5 DAYS"), ("month", "1 MONTH"), ("all", "ALL TIME"))
TIMELINE_KEYS = tuple(k for k, _ in TIMELINES)
DEFAULT_TIMELINE = "today"
FIVE_DAYS = 5                    # 5 DAYS: the last five UK trading days (Monday to Friday)
TOP_SHOWN = 20                   # the league shows the top 20, then "Show all N"
TOP_TEN = 10
TESTING_UNDER = 10               # TESTING: fewer trades than this in the period
BENCH_TRADES = 20                # BENCHED: retired, or this many trades in the period and losing
RISING_PLACES = 3                # RISING: up this many places on the previous day's league
HOT_STREAK = 3                   # HOT: a streak of this many profitable days
PROMISING_TRADES = 10            # PROMISING: in profit with this many trades in the period
NEW_DAYS = 7                     # NEW THIS WEEK: ideas added in the last 7 days

CHAMPION, RISING, HOT, TESTING, BENCHED = "CHAMPION", "RISING", "HOT", "TESTING", "BENCHED"
BADGES = {CHAMPION: ("🏆", "gold"), RISING: ("🚀", "teal"), HOT: ("🔥", "amber"), TESTING: ("🧪", "plain"),
          BENCHED: ("🪑", "muted")}
DEMO_READY = "DEMO READY"
ON_DEMO = "ON DEMO"


# ------------------------------------------------------------------- UK days --
def is_trading_day(d: dt.date) -> bool:
    """A UK trading day: Monday to Friday."""
    return d.weekday() < 5


def previous_trading_day(d: dt.date) -> dt.date:
    """The UK trading day before ``d`` (Friday for a Monday or a weekend)."""
    d -= dt.timedelta(days=1)
    while not is_trading_day(d):
        d -= dt.timedelta(days=1)
    return d


def trading_days_back(d: dt.date, n: int) -> list[dt.date]:
    """The last ``n`` UK trading days up to and including ``d``, latest first."""
    out, x = [], d
    while len(out) < n:
        if is_trading_day(x):
            out.append(x)
        x -= dt.timedelta(days=1)
    return out


def reset_of(sd: Optional[dict]) -> dt.datetime:
    """The account's reset: the standing's own start, else the configured one (5 Oct 2026, 12:30 UK)."""
    for txt in ((sd or {}).get("start"), RESET_DEFAULT):
        try:
            if txt:
                return to_utc(dt.datetime.fromisoformat(str(txt).replace("Z", "+00:00")))
        except (TypeError, ValueError):
            continue
    return to_utc(dt.datetime.fromisoformat(RESET_DEFAULT))


def timeline_of(v) -> str:
    v = str(v or "").strip().lower()
    return v if v in TIMELINE_KEYS else DEFAULT_TIMELINE


def bounds(key: str, now: dt.datetime, reset: dt.datetime, as_of: Optional[dt.date] = None
           ) -> tuple[dt.datetime, dt.datetime]:
    """[start, end) in UTC of a timeline as it stands at the end of UK day ``as_of`` (today by default):

    * TODAY: 00:00 UK to 24:00 UK that day;
    * 5 DAYS: from 00:00 UK on the fifth UK trading day back (that day counted), to the end of the day;
    * 1 MONTH: from the same date a month back, the day after it, to the end of the day;
    * ALL TIME: from the reset (5 Oct 2026).

    Never earlier than the reset."""
    d = as_of or uk_date(now)
    one = dt.timedelta(days=1)
    if key == "today":
        first = d
    elif key == "5d":
        first = trading_days_back(d, FIVE_DAYS)[-1]
    elif key == "month":
        first = _months_back(d, 1) + one
    else:
        first = None
    start = reset if first is None else max(uk_midnight(first), reset)
    end = max(uk_midnight(d + one), reset)
    return start, end


def _d(d: dt.date) -> str:
    return f"{d.strftime('%a')} {d.day} {d.strftime('%b')}"


def timeline_label(key: str, start: dt.datetime, end: dt.datetime) -> str:
    """The period in a few words: 'Tue 13 Oct', 'Wed 7 Oct – Tue 13 Oct', 'since the reset, Mon 5 Oct'."""
    a, b = uk_date(start), uk_date(end - dt.timedelta(seconds=1))
    if key == "today":
        return _d(b)
    if key == "all":
        return f"since the reset, {_d(a)}"
    return _d(b) if a == b else f"{_d(a)} – {_d(b)}"


# ------------------------------------------------------------------ the figures --
def in_window(trades: Iterable[dict], start: dt.datetime, end: dt.datetime) -> list[dict]:
    return [t for t in trades if start <= t["closed"] < end]


def day_nets(trades: Iterable[dict]) -> dict:
    """{UK date: £ after costs that day} - each trade on the UK day it closed."""
    out: dict = {}
    for t in trades:
        k = uk_date(t["closed"])
        out[k] = out.get(k, 0.0) + float(t["net"])
    return {k: round(v, 2) for k, v in out.items()}


def streak_of(trades: Iterable[dict]) -> int:
    """Consecutive profitable UK trading days up to its latest: counted back from the latest UK day on which
    it traded, every day it traded and finished in profit after costs; the first day it traded and did not
    (a loss or exactly level) ends it. A day it did not trade neither breaks nor extends it."""
    n = 0
    for _d_, net in sorted(day_nets(trades).items(), reverse=True):
        if net > 0:
            n += 1
        else:
            break
    return n


def figures(trades: Sequence[dict]) -> dict:
    """{net, trades, wins, win_rate, streak, avg_win, avg_loss} of some trades, after costs. A win made money
    after costs; every other trade counts against the win rate (as the pages have always counted)."""
    nets = [float(t["net"]) for t in trades]
    wins = [v for v in nets if v > 0]
    losses = [v for v in nets if v < 0]
    n = len(nets)
    return {"net": round(sum(nets), 2), "trades": n, "wins": len(wins),
            "win_rate": (100.0 * len(wins) / n) if n else None, "streak": streak_of(trades),
            "avg_win": round(sum(wins) / len(wins), 2) if wins else None,
            "avg_loss": round(-sum(losses) / len(losses), 2) if losses else None}


def badge_of(row: dict) -> str:
    """One badge a row, the highest that applies:

    1. CHAMPION - passes the promotion bar (the weekly pick's ``passes``);
    2. RISING   - up RISING_PLACES or more places on the previous day's league;
    3. HOT      - a streak of HOT_STREAK or more profitable days;
    4. TESTING  - fewer than TESTING_UNDER trades in the period;
    5. BENCHED  - retired by the weekly refresh, or BENCH_TRADES+ trades in the period and losing.

    A retired test is BENCHED before it can read as TESTING: "testing" would not be true of it."""
    if row.get("ready"):
        return CHAMPION
    if (row.get("move") or 0) >= RISING_PLACES:
        return RISING
    if int(row.get("streak") or 0) >= HOT_STREAK:
        return HOT
    if row.get("retired"):
        return BENCHED
    if int(row.get("trades") or 0) < TESTING_UNDER:
        return TESTING
    if int(row.get("trades") or 0) >= BENCH_TRADES and float(row.get("net") or 0.0) < 0:
        return BENCHED
    return ""


def league_of(tests: dict, start: dt.datetime, end: dt.datetime) -> list[dict]:
    """Every test with a paper trade closed in [start, end), best £ after costs first (then the better win
    rate, then the name), with its place ``pos`` from 1."""
    rows = []
    for t in tests.values():
        tr = in_window(t.get("trades") or (), start, end)
        if not tr:
            continue
        rows.append(dict({k: t.get(k) for k in ("id", "nick", "desc", "kind", "ref", "ready", "live", "retired")},
                         **figures(tr), last=max(x["closed"] for x in tr)))
    rows.sort(key=lambda r: (-r["net"], -(r["win_rate"] or 0.0), str(r["nick"]), str(r["id"])))
    for i, r in enumerate(rows, 1):
        r["pos"] = i
    return rows


def apply_moves(cur: list[dict], prev: Sequence[dict]) -> list[dict]:
    """Each row's place on the previous day's league (``prev``, None when it was not on it), how many places
    it moved (``move``: up is positive), its badge and DEMO READY (passes the bar, not on DEMO yet)."""
    was = {r["id"]: r["pos"] for r in prev}
    for r in cur:
        p = was.get(r["id"])
        r["prev"] = p
        r["move"] = (p - r["pos"]) if p is not None else None
        r["badge"] = badge_of(r)
        r["demo_ready"] = bool(r.get("ready")) and not r.get("live")
    return cur


def move_words(row: dict) -> tuple[str, str, str]:
    """(the arrow, its class, its title): '↑2' / '↓1' / '–' / 'NEW', and '#4 → #2'."""
    p, m = row.get("prev"), row.get("move")
    if p is None:
        return "NEW", "new", f"New on the league at #{row['pos']}"
    title = f"#{p} → #{row['pos']}"
    if m > 0:
        return f"↑{m}", "up", title
    if m < 0:
        return f"↓{-m}", "down", title
    return "–", "same", title


def headline(rows: Sequence[dict]) -> dict:
    """The headline cards from the league: PAPER PROFIT, WIN RATE, TRADES, WINNING BOTS (in profit / traded)
    and BEST STREAK (the longest; the better £ breaks a tie)."""
    trades = sum(int(r["trades"]) for r in rows)
    wins = sum(int(r["wins"]) for r in rows)
    best = max((r for r in rows if r["streak"] > 0), key=lambda r: (r["streak"], r["net"], r["id"]), default=None)
    return {"net": round(sum(float(r["net"]) for r in rows), 2), "trades": trades, "wins": wins,
            "win_rate": (100.0 * wins / trades) if trades else None,
            "winning": sum(1 for r in rows if r["net"] > 0), "traded": len(rows),
            "streak": ({"id": best["id"], "nick": best["nick"], "days": best["streak"]} if best else None)}


def factory_counts(pool: Sequence[dict], rows: Sequence[dict], lab_rows: Sequence[dict], now: dt.datetime) -> dict:
    """STRATEGY FACTORY: ACTIVE TESTS (the pool: every test practising, retired ideas left out), NEW THIS WEEK
    (lab ideas added in the last NEW_DAYS days), PROMISING (in profit with PROMISING_TRADES+ trades in the
    period) and CHAMPIONS (passing the bar)."""
    since = to_utc(now) - dt.timedelta(days=NEW_DAYS)
    new = 0
    for r in lab_rows:
        t = _when(r.get("created_utc"))
        if t is not None and since <= t <= to_utc(now):
            new += 1
    return {"active": len(pool), "new": new,
            "promising": sum(1 for r in rows if r["net"] > 0 and int(r["trades"]) >= PROMISING_TRADES),
            "champions": sum(1 for c in pool if c.get("passes"))}


def _when(v) -> Optional[dt.datetime]:
    if not v:
        return None
    try:
        t = v if isinstance(v, dt.datetime) else dt.datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return to_utc(t if t.tzinfo else t.replace(tzinfo=UTC))
    except (TypeError, ValueError):
        return None


def latest_action(rows: Sequence[dict], prev: Sequence[dict], hist: Sequence[dict], refresh: Optional[dict],
                  nick_of, now: dt.datetime) -> Optional[dict]:
    """The latest thing that happened, newest first, from:

    * the weekly history (data/weekly/history.jsonl): a promotion to DEMO or a move back to PAPER;
    * the lab's Saturday refresh (data/lab/refresh.json): fresh challengers added, losers benched;
    * the league against the previous day's: a new #1, or a new entry in the Top 10 (when it happened: the
      latest trade that put it there).

    {emoji, text, when} or None when nothing has happened yet."""
    now = to_utc(now)
    out = []
    for e in hist:
        t = _when(e.get("ts_utc"))
        if t is None or t > now:
            continue
        name = nick_of(str(e.get("id") or ""), e)
        if e.get("event") == "promote":
            out.append({"emoji": "🚀", "text": f"{name} promoted to Demo", "when": t, "kind": "promote"})
        elif e.get("event") == "demote":
            out.append({"emoji": "⬇️", "text": f"{name} back to Paper", "when": t, "kind": "demote"})
    if isinstance(refresh, dict):
        t = _when(refresh.get("written_utc"))
        added = _count(refresh, "new") + _count(refresh, "variants")
        benched = _count(refresh, "retired")
        if t is not None and t <= now and (added or benched):
            bits = []
            if added:
                bits.append(f"{added} fresh challenger{'' if added == 1 else 's'} entered testing")
            if benched:
                bits.append(f"{benched} benched")
            out.append({"emoji": "🧪", "text": ", ".join(bits), "when": t, "kind": "refresh"})
    if prev and rows:
        was = {r["id"]: r["pos"] for r in prev}
        top = rows[0]
        if was.get(top["id"]) != 1 and top["net"] > 0:
            out.append({"emoji": "🏆", "text": f"{top['nick']} has taken top spot", "when": top["last"],
                        "kind": "top"})
        else:
            for r in rows[1:TOP_TEN]:
                p = was.get(r["id"])
                if (p is None or p > TOP_TEN) and r["net"] > 0:
                    out.append({"emoji": "👀", "text": f"{r['nick']} enters the Top 10 at #{r['pos']}",
                                "when": r["last"], "kind": "top10"})
                    break
    if not out:
        return None
    return max(out, key=lambda x: x["when"])


def _count(rep: dict, key: str) -> int:
    v = rep.get(key)
    if isinstance(v, list):
        return len(v)
    try:
        return int(rep.get(f"{key}_count") or 0)
    except (TypeError, ValueError):
        return 0


# ------------------------------------------------------------------ nicknames --
# Each test gets a short name worked out from its own definition - never stored, never typed. The lab's ideas
# take a place (their session), a market and a noun for the family; Trend & Breakout's parts their kind of market
# and a noun for the approach; the four original bots keep their own names. Two ideas that come out the same are
# told apart by when they joined the pool: the first keeps the name, the next is "Mk2", then "Mk3" - so a name
# never changes when a newer idea arrives, and the lab never deletes a row (retired ideas keep their names too).
SESSION_WORD = {"LONDON": "London", "NEWYORK": "NY", "US_CASH": "Wall St", "ASIA": "Tokyo", "BOTH": "", "ALL": ""}
MARKET_WORD = {"FX_MAJOR": "Major", "FX_MINOR": "Minor", "INDEX": "Index", "GOLD": "Gold", "SILVER": "Silver",
               "FX_EXOTIC": "Exotic", "METAL": "Metal", "ENERGY": "Oil", "CRYPTO": "Crypto"}
APPROACH_NOUN = {"MOMENTUM_CONTINUATION": "Momentum", "TREND_PULLBACK": "Dipper", "BREAKOUT_ACCEPTANCE": "Breakaway",
                 "SESSION_EXPANSION": "Expander", "SQUEEZE_RELEASE": "Spring", "RANGE_REJECTION": "Bouncer",
                 "FAILED_BREAKOUT_RECLAIM": "Reclaimer", "LIQUIDITY_SWEEP_REVERSAL": "Sweeper",
                 "VWAP_REVERSION": "Magnet", "NEWS_CONTINUATION": "Headliner", "BREAKOUT_RETEST": "Retester"}
APPROACH_PLAIN = {"MOMENTUM_CONTINUATION": "joins a strong move that is already running",
                  "TREND_PULLBACK": "waits for a dip in a trend, then rejoins the trend",
                  "BREAKOUT_ACCEPTANCE": "trades a breakout once the price holds outside its range",
                  "SESSION_EXPANSION": "trades the burst when a new session opens up the range",
                  "SQUEEZE_RELEASE": "trades the burst after a quiet spell",
                  "RANGE_REJECTION": "bets the price turns back at the edge of its range",
                  "FAILED_BREAKOUT_RECLAIM": "bets against a breakout that fails and slips back",
                  "LIQUIDITY_SWEEP_REVERSAL": "trades the turn after a quick spike through an obvious level",
                  "VWAP_REVERSION": "bets a stretched price snaps back to the day's average",
                  "NEWS_CONTINUATION": "follows through on a strong move after news",
                  "BREAKOUT_RETEST": "trades a breakout when the price comes back, tests the level and holds"}
APPROACH_SHORT = {"MOMENTUM_CONTINUATION": "momentum", "TREND_PULLBACK": "trend dip", "BREAKOUT_ACCEPTANCE": "breakout",
                  "SESSION_EXPANSION": "session burst", "SQUEEZE_RELEASE": "squeeze", "RANGE_REJECTION": "range edge",
                  "FAILED_BREAKOUT_RECLAIM": "failed breakout", "LIQUIDITY_SWEEP_REVERSAL": "spike reversal",
                  "VWAP_REVERSION": "back to average", "NEWS_CONTINUATION": "news follow-through",
                  "BREAKOUT_RETEST": "breakout retest"}
BOT_NAMES = {"runner": "Momentum Runner", "bandbreaker": "Band Breaker", "crowd": "Crowd Fader",
             "ian": "Financial Ian"}
BOT_DESC = {"runner": "Original bot · rides index trends", "bandbreaker": "Original bot · index bands",
            "crowd": "Original bot · fades the crowd", "ian": "Original bot · reads the order book"}


def lab_noun(family: str, params: dict) -> str:
    """The family's noun, and its key setting where it has one ('Breakout 60', 'Opener 15')."""
    p = params or {}
    if family == "MOMENTUM_CONTINUATION":
        return "Surfer" if p.get("pullback") == "HALF" else "Rocket"
    if family == "RANGE_BREAK":
        return f"Breakout {p.get('lookback_min')}" if p.get("lookback_min") else "Breakout"
    if family == "OPENING_RANGE":
        return f"Opener {p.get('range_min')}" if p.get("range_min") else "Opener"
    if family == "PREV_DAY_LEVELS":
        return "Level Breaker" if p.get("mode") == "BREAK" else "Level Fader"
    if family == "SQUEEZE_RELEASE":
        return "Squeeze"
    if family == "TREND_PULLBACK":
        return "Trend Rider"
    if family == "VWAP_FADE":
        return "Snapback"
    if family == "NEWS_MOVE":
        return "News Chaser" if p.get("direction") == "WITH" else "News Fader"
    if family == "CONTROL_TIME_OF_DAY":
        return "Clockwork"
    words = str(family or "Idea").replace("_", " ").title().split()
    return words[0] if words else "Idea"


def nickname_base(t: dict) -> str:
    """A test's nickname before two the same are told apart."""
    kind = t.get("kind")
    if kind == "bot":
        return BOT_NAMES.get(str(t.get("bot")), str(t.get("label") or t.get("id")))
    if kind == "tnb":
        g, a = str(t.get("group") or ""), str(t.get("tactic") or "")
        return " ".join(x for x in (MARKET_WORD.get(g, g.title().replace("_", " ")),
                                    APPROACH_NOUN.get(a, a.title().replace("_", " "))) if x)
    if kind == "lab" and t.get("family"):
        words = (SESSION_WORD.get(str(t.get("session")), ""), MARKET_WORD.get(str(t.get("markets")), ""),
                 lab_noun(str(t.get("family")), t.get("params") or {}))
        return " ".join(w for w in words if w)
    return str(t.get("desc") or t.get("id") or "Test")[:28]


def assign_nicknames(items: Sequence[dict]) -> dict:
    """{id: nickname} for every test: its base name (``nickname_base``); when several share one, the first by
    ``order`` (when it joined the pool, then its id) keeps it and the next become 'Mk2', 'Mk3' ... Unique, and
    the same every time for the same tests."""
    groups: dict = {}
    for t in items:
        groups.setdefault(nickname_base(t), []).append(t)
    out: dict = {}
    taken = set(groups)
    for base, ts in groups.items():
        ts = sorted(ts, key=lambda t: (str(t.get("order") or "~"), str(t["id"])))
        k = 1
        for i, t in enumerate(ts):
            if i == 0:
                out[t["id"]] = base
                continue
            k += 1
            name = f"{base} Mk{k}"
            while name in taken:
                k += 1
                name = f"{base} Mk{k}"
            taken.add(name)
            out[t["id"]] = name
    return out


# ------------------------------------------------------------------ the records --
_WORLD: dict = {}


def _lab_tests(data: Path, start: dt.datetime, end: dt.datetime) -> tuple[list[dict], list[dict]]:
    """(every lab idea as a test with its PRACTICE trades in [start, end), every idea's row). Retired ideas are
    kept (their trades still happened). Nothing for a made-up (synthetic) lab, as the weekly pick reads it."""
    from . import weekly as W
    summ = W._read(data / "lab" / "summary.json")
    if isinstance(summ, dict) and summ.get("synthetic"):
        return [], []
    db = data / "lab" / "lab.sqlite"
    ideas = W._rows(db, "SELECT * FROM candidates")
    if not ideas:
        return [], []
    from ..coach.records import _is_test_row
    rows = W._rows(db, "SELECT * FROM practice_trades WHERE closed_utc IS NOT NULL AND closed_utc >= ? AND "
                       "closed_utc < ? AND (mode IS NULL OR mode='PRACTICE')",
                   (to_utc(start).isoformat(), to_utc(end).isoformat()))
    by: dict = {}
    for r in rows:
        t = _when(r.get("closed_utc"))
        if t is None or _is_test_row(r) or r.get("net_gbp") is None:
            continue
        by.setdefault(str(r.get("candidate_id")), []).append(
            {"closed": t, "opened": _when(r.get("opened_utc")), "net": round(float(r["net_gbp"]), 2),
             "symbol": str(r.get("symbol") or "")})
    out = []
    for i in ideas:
        fam, markets, session, exit_, params = str(i.get("family") or ""), "", "", "", {}
        try:
            from ..lab.stages import cand_of
            c = cand_of(i)
            fam, markets, session, exit_, params = c.family, c.markets, c.session, c.exit, c.pdict()
        except Exception:
            pass
        out.append({"id": f"lab:{i['id']}", "kind": "lab", "ref": str(i["id"]), "desc": W.lab_short_name(i),
                    "family": fam, "markets": markets, "session": session, "exit": exit_, "params": params,
                    "order": str(i.get("created_utc") or ""), "retired": bool(i.get("retired_utc")),
                    "stage": str(i.get("stage") or ""), "trades": by.get(str(i["id"]), [])})
    return out, ideas


def _bot_tests(data: Path, start: dt.datetime, end: dt.datetime) -> list[dict]:
    from . import weekly as W
    out = []
    for name, (vid, label, _magic, _p) in W.BOTS.items():
        try:
            trades = [{"closed": to_utc(t["closed"]), "net": float(t["net"]), "symbol": str(t.get("symbol") or "")}
                      for t in W.bot_trades(data, name, start, end)]
        except Exception as exc:                      # one broken record never stops the rest
            log.warning("squad: %s: %s", name, exc)
            trades = []
        out.append({"id": f"bot:{name}", "kind": "bot", "bot": name, "ref": vid, "label": label,
                    "desc": BOT_DESC.get(name, label), "order": "", "retired": False, "trades": trades})
    return out


def _tnb_tests(data: Path, start: dt.datetime, end: dt.datetime) -> list[dict]:
    from . import weekly as W
    out = []
    try:
        parts = W.tnb_part_trades(data, start, end)
    except Exception as exc:
        log.warning("squad: Trend & Breakout's parts: %s", exc)
        return out
    for (tactic, kind), trades in sorted(parts.items()):
        out.append({"id": f"tnb:{tactic}:{kind}", "kind": "tnb", "tactic": tactic, "group": kind,
                    "ref": "market_intelligence", "order": "", "retired": False,
                    "desc": f"Trend & Breakout · {APPROACH_SHORT.get(tactic, tactic.lower().replace('_', ' '))} · "
                            f"{W._kind_words(kind)}",
                    "trades": [{"closed": to_utc(t["closed"]), "net": float(t["net"]),
                                "symbol": str(t.get("symbol") or "")} for t in trades]})
    return out


def load_world(data_dir, config_path=None, now: Optional[dt.datetime] = None, reset: Optional[dt.datetime] = None,
               cache_seconds: float = 30.0) -> dict:
    """Every test with its paper trades since the reset, its nickname, and the weekly pick's verdicts:
    {tests: {id: test}, pool, lab_rows, history, refresh, nick}. Worked out at most once every
    ``cache_seconds``. Read-only; never raises (what cannot be read is left out and logged)."""
    now = to_utc(now or utcnow())
    reset = reset or reset_of(None)
    world = {"tests": {}, "pool": [], "lab_rows": [], "history": [], "refresh": None, "promoted": []}
    if not data_dir:
        return world
    key = (str(data_dir), reset.isoformat())
    hit = _WORLD.get(key)
    if hit and 0 <= (now - hit[0]).total_seconds() < cache_seconds:
        return hit[1]
    from . import weekly as W
    data = Path(data_dir)
    end = now + dt.timedelta(seconds=1)
    tests: list[dict] = []
    tests += _bot_tests(data, reset, end)
    tests += _tnb_tests(data, reset, end)
    try:
        lab, lab_rows = _lab_tests(data, reset, end)
    except Exception as exc:
        log.warning("squad: the lab: %s", exc)
        lab, lab_rows = [], []
    tests += lab
    try:
        pool = W._pool(data, config_path or data / "config.json", now, 60.0)
    except Exception as exc:
        log.warning("squad: the pool: %s", exc)
        pool = []
    verdict = {c["id"]: c for c in pool}
    try:
        promoted = W.load_state(data)["promoted"]
    except Exception as exc:
        log.warning("squad: the weekly state: %s", exc)
        promoted = []
    have = {t["id"] for t in tests}
    for it in promoted:                                  # on DEMO with no paper trade since the reset
        if it.get("id") and it["id"] not in have:
            k = str(it.get("kind") or "")
            extra = ({"bot": it.get("bot")} if k == "bot" else {"tactic": it.get("tactic"), "group": it.get("group")}
                     if k == "tnb" else {})
            tests.append(dict({"id": it["id"], "kind": k, "ref": "", "label": it.get("label"), "order": "",
                               "retired": False, "desc": W.short_name(it), "trades": []}, **extra))
    live_ids = {str(p.get("id")) for p in promoted}
    names = assign_nicknames(tests)
    for t in tests:
        c = verdict.get(t["id"]) or {}
        t["nick"] = names[t["id"]]
        t["ready"] = bool(c.get("passes"))
        t["live"] = bool(c.get("live")) or t["id"] in live_ids
        if t["kind"] == "bot" and not t.get("ref"):
            t["ref"] = W.BOTS.get(str(t.get("bot")), ("",))[0]
    world.update(tests={t["id"]: t for t in tests}, pool=pool, lab_rows=lab_rows, history=W.history(data),
                 refresh=_refresh(data), promoted=promoted)
    if len(_WORLD) > 16:
        _WORLD.clear()
    _WORLD[key] = (now, world)
    return world


def _refresh(data: Path) -> Optional[dict]:
    from . import weekly as W
    rep = W._read(data / "lab" / "refresh.json")
    if isinstance(rep, dict):
        return rep
    summ = W._read(data / "lab" / "summary.json")
    return summ.get("refresh") if isinstance(summ, dict) and isinstance(summ.get("refresh"), dict) else None


# ------------------------------------------------------------------ DEMO --
def demo_account(sd: Optional[dict], deals, key: str, start: dt.datetime, end: dt.datetime,
                 now: dt.datetime) -> Optional[dict]:
    """The DEMO account's own figures for the timeline, from MetaTrader's deal rows
    (``standing.period_view``, the one way every page works out money): {net, trades, wins, win_rate}.
    ALL TIME is the balance less the balance at the reset, as on the phone. None until MetaTrader has been
    read."""
    sd = sd or {}
    if not sd.get("start") or sd.get("made") is None or deals is None:
        return None
    from .standing import period_view
    if key == "today":
        v = period_view(sd, deals, "today", now=now)
    elif key == "all":
        v = period_view(sd, deals, "total", now=now)
    else:
        a, b = uk_date(start).isoformat(), uk_date(end - dt.timedelta(seconds=1)).isoformat()
        v = period_view(sd, deals, "custom", a, b, now=now)
    trades = int(v.get("trades") or 0)
    wins = int(v.get("wins") or 0)
    return {"net": round(float(v["made"] if key == "all" else v["trading"]), 2), "trades": trades, "wins": wins,
            "win_rate": (100.0 * wins / trades) if trades else None}


def demo_squad(world: dict, data_dir, deals, open_tickets, start: dt.datetime, end: dt.datetime) -> list[dict]:
    """The first team: one row per test the weekly pick has on DEMO, from MetaTrader's closed positions
    (``weekly.live_trades``) in the period - BOT, PROFIT, WIN RATE, TRADES, STREAK. Its figures are None
    while MetaTrader has not been read (never the bots' own records: those only ever take a pick back)."""
    from . import weekly as W
    positions = W.live_positions_json(deals, open_tickets)["positions"] if deals is not None else None
    out = []
    for it in world.get("promoted") or ():
        t = (world.get("tests") or {}).get(it.get("id")) or {}
        row = {"id": it.get("id"), "nick": t.get("nick") or W.short_name(it), "desc": t.get("desc") or "",
               "since": it.get("since_utc"), "known": positions is not None}
        if positions is not None:
            try:
                trades = W.live_trades(it, positions, data_dir)[0]
            except Exception as exc:
                log.warning("squad: demo %s: %s", it.get("id"), exc)
                trades, row["known"] = [], False
            trades = [dict(x, closed=to_utc(x["closed"])) for x in trades if x.get("closed")]
            row.update(figures(in_window(trades, start, end)))
        else:
            row.update({"net": None, "trades": None, "wins": None, "win_rate": None, "streak": None})
        out.append(row)
    out.sort(key=lambda r: (-(r["net"] or 0.0), str(r["nick"])))
    return out


# ------------------------------------------------------------------ one board --
def board(data_dir, timeline: str = DEFAULT_TIMELINE, now: Optional[dt.datetime] = None, sd: Optional[dict] = None,
          deals=None, config_path=None, cache_seconds: float = 30.0) -> dict:
    """Everything the squad pages show for one timeline, worked out once. Never raises: what cannot be read
    is shown as missing."""
    now = to_utc(now or utcnow())
    key = timeline_of(timeline)
    reset = reset_of(sd)
    start, end = bounds(key, now, reset)
    as_of_prev = previous_trading_day(uk_date(now))
    p_start, p_end = bounds(key, now, reset, as_of=as_of_prev)
    out = {"timeline": key, "label": timeline_label(key, start, end), "start": start, "end": end, "now": now,
           "reset": reset, "league": [], "prev": [], "head": headline([]), "demo": None, "demo_rows": [],
           "show_demo": False, "factory": {"active": 0, "new": 0, "promising": 0, "champions": 0},
           "latest": None, "tests": {}, "pick": None, "as_of_prev": as_of_prev}
    try:
        from . import weekly as W
        out["pick"] = W.pick_tile(None, now)
    except Exception:
        pass
    try:
        world = load_world(data_dir, config_path, now, reset, cache_seconds)
    except Exception as exc:
        log.warning("squad: %s", exc)
        return out
    tests = world["tests"]
    rows = league_of(tests, start, end)
    prev = league_of(tests, p_start, min(p_end, end))
    apply_moves(rows, prev)
    out.update(league=rows, prev=prev, head=headline(rows), tests=tests,
               factory=factory_counts(world["pool"], rows, world["lab_rows"], now))

    def nick_of(cid: str, e: dict) -> str:
        t = tests.get(cid)
        if t:
            return t["nick"]
        try:
            from . import weekly as W
            return W.short_name(e)
        except Exception:
            return cid
    try:
        out["latest"] = latest_action(rows, prev, world["history"], world["refresh"], nick_of, now)
    except Exception as exc:
        log.warning("squad: latest action: %s", exc)
    try:
        out["demo"] = demo_account(sd, deals, key, start, end, now)
        out["demo_rows"] = demo_squad(world, data_dir, deals, (sd or {}).get("open_tickets"), start, end)
    except Exception as exc:
        log.warning("squad: demo: %s", exc)
    out["show_demo"] = bool(out["demo_rows"]) or bool(out["demo"] and out["demo"]["trades"])
    try:
        from . import weekly as W
        latest = W._read(W.wdir(data_dir) / W.LATEST) if data_dir else None
        out["pick"] = W.pick_tile(latest if isinstance(latest, dict) else None, now)
    except Exception:
        pass
    return out


# ------------------------------------------------------------------ a test in plain words --
FAMILY_PLAIN = {
    "MOMENTUM_CONTINUATION": "Waits for a strong, steady push in one direction and jumps on board, aiming to ride "
                             "the move while it keeps going.",
    "RANGE_BREAK": "Watches the highest and lowest price of the last {lookback_min} minutes and joins the move when "
                   "the price breaks out.",
    "OPENING_RANGE": "Marks out the first {range_min} minutes after the market opens, then trades the break out of "
                     "that early range.",
    "PREV_DAY_LEVELS": "Uses yesterday's high and low as its markers: {mode_words}",
    "SQUEEZE_RELEASE": "Waits for the market to go unusually quiet, then trades the burst when it wakes up.",
    "TREND_PULLBACK": "Finds a market that is already trending, waits for a small dip against the trend, then "
                      "joins the trend again.",
    "VWAP_FADE": "When the price stretches a long way from the day's average price, it bets on it snapping back.",
    "NEWS_MOVE": "Trades the first move after big economic news: {news_words}",
    "CONTROL_TIME_OF_DAY": "A deliberate 'no idea' test: it trades at the same time every day, whatever the market "
                           "does. It is there to check the tests are fair.",
}
BOT_PLAIN = {
    "runner": "Follows Trend & Breakout's trades on the stock indices and holds on for longer: it leaves its stop "
              "alone until the trade is well ahead, then trails it behind the best price so a big move can run.",
    "bandbreaker": "Trades the big stock indices during the day. It draws a band of normal wobble around the "
                   "open and, when a half-hour bar closes outside that band, it joins the move. Flat before the close.",
    "crowd": "Watches how the crowd of traders is positioned. When the losing crowd leans hard one way and the "
             "proven winners don't agree, it bets the other way once the price turns.",
    "ian": "Reads who is buying and selling in size behind the market and trades the matching market early, with "
           "a stop that only ever tightens.",
}
BOT_WHEN = {"runner": "The big stock indices, while their markets are open.",
            "bandbreaker": "The big stock indices, during their trading day; never held overnight.",
            "crowd": "Whenever the crowd's positioning lines up with a turn in the price.",
            "ian": "Whenever the buying and selling behind the market shows a clear push."}
SESSION_PLAIN = {"LONDON": "the London session", "NEWYORK": "the New York session",
                 "US_CASH": "the US stock market's opening hours", "ASIA": "the Asian session",
                 "BOTH": "the London and New York sessions", "ALL": "any time of the trading day"}
HOUR_BUCKETS = ((0, 7, "the early Asian hours (before 7am UK)"), (7, 12, "the London morning"),
                (12, 16, "the London and New York overlap"), (16, 21, "the New York afternoon"),
                (21, 24, "the late evening"))


def _gbp(v, signed: bool = True) -> str:
    if v is None:
        return "—"
    f = float(v)
    s = ("+" if f > 0 else "-" if f < 0 else "") if signed else ("-" if f < 0 else "")
    return f"{s}£{abs(f):,.2f}"


def _pct(v) -> str:
    return "—" if v is None else f"{float(v):.0f}%"


def what_it_does(t: dict) -> str:
    """WHAT IT DOES: the family's or the bot's idea, templated in plain words, then its market and its exit."""
    kind = t.get("kind")
    if kind == "bot":
        return BOT_PLAIN.get(str(t.get("bot")), "One of the original bots.")
    if kind == "tnb":
        from . import weekly as W
        a = str(t.get("tactic") or "")
        return (f"One part of Trend & Breakout: it {APPROACH_PLAIN.get(a, a.lower().replace('_', ' '))}, on "
                f"{W._kind_words(str(t.get('group') or ''))}. Each part is judged on its own trades.")
    fam = str(t.get("family") or "")
    p = dict(t.get("params") or {})
    text = FAMILY_PLAIN.get(fam)
    if text is None:
        return f"An idea from the lab: {t.get('desc') or ''}."
    p.setdefault("lookback_min", "60")
    p.setdefault("range_min", "15")
    p["mode_words"] = ("it joins the move when the price breaks through one of them." if p.get("mode") == "BREAK"
                       else "it bets the price turns back when it reaches one of them.")
    p["news_words"] = ("it goes the same way as that move." if p.get("direction") == "WITH"
                       else "it bets against that move.")
    try:
        text = text.format(**p)
    except (KeyError, IndexError, ValueError):
        text = text.split("{")[0].strip()
    if fam == "MOMENTUM_CONTINUATION" and p.get("pullback") == "HALF":
        text = text.replace("jumps on board", "waits for a small pullback before it jumps on board")
    bits = [text]
    try:
        from ..lab.candidate import EXIT_SHORT, MARKET_NAMES
        if t.get("markets") in MARKET_NAMES:
            bits.append(f"It trades {MARKET_NAMES[t['markets']]}" + (
                f" and {EXIT_SHORT[t['exit']]}." if t.get("exit") in EXIT_SHORT else "."))
    except Exception:
        pass
    return " ".join(bits)


def _uk_hour(t: dt.datetime) -> int:
    from ..clock import TZ_LONDON
    return to_utc(t).astimezone(TZ_LONDON).hour


def _bucket(h: int) -> str:
    return next(w for a, b, w in HOUR_BUCKETS if a <= h < b)


def when_it_trades(t: dict, trades: Sequence[dict]) -> list[str]:
    """WHEN IT LIKES TO TRADE: its market and session, then the hours its trades in the period actually closed."""
    out = []
    kind = t.get("kind")
    if kind == "bot":
        out.append(BOT_WHEN.get(str(t.get("bot")), "Whenever its signal lines up."))
    elif kind == "tnb":
        from . import weekly as W
        out.append(f"{W._kind_words(str(t.get('group') or '')).capitalize()}, whenever its setup appears.")
    elif t.get("session"):
        try:
            from ..lab.candidate import MARKET_NAMES
            mk = MARKET_NAMES.get(str(t.get("markets")), "its markets")
        except Exception:
            mk = "its markets"
        out.append(f"{mk[0].upper() + mk[1:]}, during {SESSION_PLAIN.get(str(t.get('session')), 'the trading day')}.")
    if trades:
        hours = sorted(_uk_hour(x["closed"]) for x in trades)
        counts: dict = {}
        for h in hours:
            counts[h] = counts.get(h, 0) + 1
        busiest = max(counts, key=lambda h: (counts[h], -h))
        n = len(trades)
        out.append(f"In this period its {n} trade{'' if n == 1 else 's'} closed between {hours[0]:02d}:00 and "
                   f"{hours[-1]:02d}:59 UK" + (f", most often around {busiest:02d}:00." if n > 2 else "."))
        syms = sorted({x.get("symbol") for x in trades if x.get("symbol")})
        if syms:
            out.append("Markets it traded: " + ", ".join(syms[:6]) + (" and more." if len(syms) > 6 else "."))
    else:
        out.append("No trades in this period yet.")
    return out


def why_lines(f: dict, trades: Sequence[dict]) -> tuple[str, list[str]]:
    """(the heading, the lines) of WHY IT IS DOING WELL - only ever from its own figures. Under TESTING_UNDER
    trades it is "still early"; a test that is not in profit gets "HOW IT IS DOING" and the plain facts, never
    a reason to feel good that the numbers do not support."""
    n = int(f.get("trades") or 0)
    if not n:
        return "WHY IT IS DOING WELL", ["Still early: no trades in this period yet."]
    if n < TESTING_UNDER:
        side = (f"so far it is up {_gbp(f['net'])}" if f["net"] > 0 else
                f"so far it is at {_gbp(f['net'])}")
        return "WHY IT IS DOING WELL", [f"Still early: only {n} trade{'' if n == 1 else 's'} in this period, too few "
                                        f"to say why yet; {side} after costs."]
    wr, aw, al = f.get("win_rate") or 0.0, f.get("avg_win"), f.get("avg_loss")
    if f["net"] <= 0:
        lines = [f"Not in profit in this period: {_gbp(f['net'])} after costs over {n} trades, winning "
                 f"{wr:.0f}% of them."]
        if aw is not None and al is not None and al > aw:
            lines.append(f"Its average loss ({_gbp(al, False)}) is bigger than its average win ({_gbp(aw, False)}).")
        return "HOW IT IS DOING", lines
    lines = []
    if al is None:
        lines.append(f"It has not lost a trade in this period: {f['wins']} wins out of {n}.")
    elif aw is not None:
        ratio = aw / al if al else None
        if wr >= 50 and ratio is not None and ratio >= 1:
            lines.append(f"It wins {wr:.0f}% of its trades, and its average win ({_gbp(aw, False)}) is bigger than "
                         f"its average loss ({_gbp(al, False)}).")
        elif wr >= 50:
            lines.append(f"It wins {wr:.0f}% of its trades - often enough to cover an average loss "
                         f"({_gbp(al, False)}) that is bigger than its average win ({_gbp(aw, False)}).")
        elif ratio is not None and ratio > 1:
            lines.append(f"It wins only {wr:.0f}% of its trades, but its average win ({_gbp(aw, False)}) is "
                         f"{ratio:.1f}× its average loss ({_gbp(al, False)}), so the winners pay for the losers.")
    by_sym: dict = {}
    for x in trades:
        s = x.get("symbol") or ""
        if s:
            a = by_sym.setdefault(s, [0.0, 0])
            a[0] += float(x["net"])
            a[1] += 1
    if len(by_sym) > 1:
        s, (net, k) = max(by_sym.items(), key=lambda kv: (kv[1][0], kv[0]))
        if net > 0:
            lines.append(f"Its best market: {s} ({_gbp(net)} from {k} trade{'' if k == 1 else 's'}).")
    by_time: dict = {}
    for x in trades:
        w = _bucket(_uk_hour(x["closed"]))
        a = by_time.setdefault(w, [0.0, 0])
        a[0] += float(x["net"])
        a[1] += 1
    if by_time:
        w, (net, k) = max(by_time.items(), key=lambda kv: (kv[1][0], kv[0]))
        if net > 0:
            lead = "Its best time" if len(by_time) > 1 else "All its trades came in"
            lines.append(f"{lead}: {w} ({_gbp(net)} from {k} trade{'' if k == 1 else 's'})."
                         if len(by_time) > 1 else f"{lead} {w}, {_gbp(net)} after costs.")
    if int(f.get("streak") or 0) >= 2:
        lines.append(f"It is on a run of {f['streak']} profitable days in a row.")
    return "WHY IT IS DOING WELL", lines


def advanced_href(t: dict) -> str:
    """Where VIEW ADVANCED DETAILS leads: the bot's own page (/?bot=...), Trend & Breakout's for one of its
    parts, or the lab idea's own page (/lab?id=...)."""
    kind, ref = str(t.get("kind") or ""), str(t.get("ref") or "")
    if kind in ("bot", "tnb") and ref:
        return f"/?bot={quote(ref)}"
    if kind == "lab" and ref:
        return f"/lab?id={quote(ref)}"
    return "/advanced"


def bot_view(b: dict, test_id: str, data_dir=None) -> Optional[dict]:
    """One test for its own page, in the board's timeline: {test, row (its league row, None when it did not
    trade), figures, trades, does, when, why_title, why, demo (its DEMO figures when on DEMO), href}."""
    t = (b.get("tests") or {}).get(test_id)
    if t is None:
        return None
    trades = in_window(t.get("trades") or (), b["start"], b["end"])
    f = figures(trades)
    row = next((r for r in b.get("league") or () if r["id"] == test_id), None)
    title, why = why_lines(f, trades)
    demo = next((r for r in b.get("demo_rows") or () if r["id"] == test_id), None)
    return {"test": t, "row": row, "figures": f, "trades": trades, "does": what_it_does(t),
            "when": when_it_trades(t, trades), "why_title": title, "why": why, "demo": demo,
            "href": advanced_href(t)}


# =========================================================================== the page ==
# One stylesheet, inline (the page runs offline on 127.0.0.1): system fonts, colour tokens on :root with a dark
# set, a teal identity, green for profit, red for loss, gold for champions and streaks. No fixed width over 390px
# anywhere: the content is fluid with a max-width, and every grid collapses on a phone.
SQUAD_CSS = """
:root{--bg:#f2f6f5;--surface:#ffffff;--surface-2:#f6faf9;--line:#e1ebe9;--line-2:#d3e1de;--text:#0c1f1c;
--text-2:#3d5a55;--text-3:#7a938f;--accent:#0f9488;--accent-2:#14b8a6;--accent-soft:#e0f4f1;--on-accent:#ffffff;
--good:#04875f;--good-soft:#e2f5ec;--bad:#d42a2a;--bad-soft:#fcebeb;--gold:#a8710a;--gold-2:#d99e1e;
--gold-soft:#fbf1d9;--amber:#b45309;--amber-soft:#fdeedd;--muted-soft:#eef2f2;
--shadow:0 1px 2px rgba(12,31,28,.05),0 10px 30px -18px rgba(12,31,28,.25);
--shadow-hi:0 2px 4px rgba(12,31,28,.06),0 18px 40px -20px rgba(12,31,28,.35);
--team:#0b2e2b;--team-2:#103c38;--team-line:rgba(255,255,255,.09);--team-text:#eaf6f4;--team-dim:#93b9b3;
--radius:18px;color-scheme:light}
:root[data-theme="dark"]{--bg:#070d0e;--surface:#0e1718;--surface-2:#132022;--line:#1c2c2e;--line-2:#24393b;
--text:#e8f2f0;--text-2:#a7bdb9;--text-3:#6c8783;--accent:#2dd4bf;--accent-2:#5eead4;--accent-soft:rgba(45,212,191,.11);
--on-accent:#04201c;--good:#3ddc97;--good-soft:rgba(61,220,151,.1);--bad:#ff6b6b;--bad-soft:rgba(255,107,107,.1);
--gold:#f2c35b;--gold-2:#f6d68a;--gold-soft:rgba(242,195,91,.11);--amber:#f6a04d;--amber-soft:rgba(246,160,77,.11);
--muted-soft:rgba(255,255,255,.05);--shadow:0 1px 0 rgba(255,255,255,.03) inset,0 14px 34px -22px rgba(0,0,0,.9);
--shadow-hi:0 1px 0 rgba(255,255,255,.05) inset,0 22px 44px -22px rgba(0,0,0,1);--team:#0c1f1f;--team-2:#122a29;
--team-line:rgba(242,195,91,.16);--team-text:#f1f7f5;--team-dim:#8eaaa5;color-scheme:dark}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--bg:#070d0e;--surface:#0e1718;
--surface-2:#132022;--line:#1c2c2e;--line-2:#24393b;--text:#e8f2f0;--text-2:#a7bdb9;--text-3:#6c8783;
--accent:#2dd4bf;--accent-2:#5eead4;--accent-soft:rgba(45,212,191,.11);--on-accent:#04201c;--good:#3ddc97;
--good-soft:rgba(61,220,151,.1);--bad:#ff6b6b;--bad-soft:rgba(255,107,107,.1);--gold:#f2c35b;--gold-2:#f6d68a;
--gold-soft:rgba(242,195,91,.11);--amber:#f6a04d;--amber-soft:rgba(246,160,77,.11);--muted-soft:rgba(255,255,255,.05);
--shadow:0 1px 0 rgba(255,255,255,.03) inset,0 14px 34px -22px rgba(0,0,0,.9);
--shadow-hi:0 1px 0 rgba(255,255,255,.05) inset,0 22px 44px -22px rgba(0,0,0,1);--team:#0c1f1f;--team-2:#122a29;
--team-line:rgba(242,195,91,.16);--team-text:#f1f7f5;--team-dim:#8eaaa5;color-scheme:dark}}
*{box-sizing:border-box}
html,body{margin:0;padding:0;max-width:100%;overflow-x:hidden;-webkit-text-size-adjust:100%}
body{background:var(--bg);color:var(--text);font:15px/1.5 ui-sans-serif,-apple-system,BlinkMacSystemFont,
"SF Pro Text","Segoe UI Variable Text","Segoe UI",Inter,Roboto,"Helvetica Neue",Arial,sans-serif;
-webkit-font-smoothing:antialiased;text-rendering:optimizeLegibility}
a{color:inherit;text-decoration:none}
.num{font-variant-numeric:tabular-nums;letter-spacing:-.01em}
.pos{color:var(--good)}.neg{color:var(--bad)}.zero{color:var(--text-2)}
/* the top bar */
.sq-bar{position:sticky;top:0;z-index:30;background:color-mix(in srgb,var(--bg) 82%,transparent);
-webkit-backdrop-filter:saturate(1.4) blur(14px);backdrop-filter:saturate(1.4) blur(14px);
border-bottom:1px solid var(--line)}
.sq-bar-in{max-width:1180px;margin:0 auto;padding:12px 24px;display:flex;align-items:center;gap:18px}
.sq-brand{display:flex;align-items:center;gap:10px;font-weight:800;letter-spacing:.06em;font-size:14px;
white-space:nowrap}
.sq-mark{width:28px;height:28px;border-radius:9px;display:grid;place-items:center;background:var(--accent);
color:var(--on-accent);font-size:15px;box-shadow:0 6px 16px -8px var(--accent)}
.sq-nav{display:flex;gap:2px;flex:1;min-width:0;overflow-x:auto;scrollbar-width:none}
.sq-nav::-webkit-scrollbar{display:none}
.sq-nav a{padding:7px 12px;border-radius:10px;font-size:13.5px;font-weight:600;color:var(--text-2);
white-space:nowrap;transition:background .18s,color .18s}
.sq-nav a:hover{background:var(--muted-soft);color:var(--text)}
.sq-nav a.on{background:var(--accent-soft);color:var(--accent)}
.sq-nav a.adv{color:var(--text-3);font-weight:550}
.sq-tools{display:flex;align-items:center;gap:10px;white-space:nowrap}
.sq-health{display:inline-flex;align-items:center;gap:6px;font-size:12px;font-weight:600;color:var(--text-3)}
.sq-health i{width:8px;height:8px;border-radius:50%;background:var(--text-3);display:inline-block}
.sq-health.ok i{background:var(--good);box-shadow:0 0 0 3px var(--good-soft)}
.sq-health.warn i{background:var(--gold-2);box-shadow:0 0 0 3px var(--gold-soft)}
.sq-health.bad i{background:var(--bad);box-shadow:0 0 0 3px var(--bad-soft)}
.sq-theme{border:1px solid var(--line);background:var(--surface);color:var(--text-2);border-radius:10px;
padding:6px 10px;font:inherit;font-size:12.5px;font-weight:600;cursor:pointer;transition:border-color .18s}
.sq-theme:hover{border-color:var(--accent);color:var(--text)}
/* the page */
.sq-wrap{max-width:1180px;margin:0 auto;padding:28px 24px 72px}
.sq-hero{display:flex;align-items:flex-end;justify-content:space-between;gap:16px 24px;flex-wrap:wrap;
margin:6px 0 22px}
.sq-hero h1{margin:0;font-size:clamp(28px,4.2vw,44px);line-height:1.02;font-weight:850;letter-spacing:-.025em}
.sq-hero h1 span{color:var(--accent)}
.sq-hero p{margin:6px 0 0;color:var(--text-3);font-size:14px;font-weight:550}
.sq-tl{display:inline-grid;grid-auto-flow:column;gap:4px;padding:4px;border-radius:14px;background:var(--surface);
border:1px solid var(--line);box-shadow:var(--shadow)}
.sq-tl a{padding:8px 14px;border-radius:10px;font-size:12.5px;font-weight:750;letter-spacing:.06em;
color:var(--text-3);text-align:center;white-space:nowrap;transition:background .18s,color .18s}
.sq-tl a:hover{color:var(--text)}
.sq-tl a.on{background:var(--accent);color:var(--on-accent);box-shadow:0 6px 14px -8px var(--accent)}
.sq-h2{display:flex;align-items:baseline;justify-content:space-between;gap:10px;flex-wrap:wrap;margin:0 0 14px}
.sq-h2 h2{margin:0;font-size:13px;font-weight:800;letter-spacing:.12em;color:var(--text-2)}
.sq-h2 small{color:var(--text-3);font-size:12.5px;font-weight:550}
/* headline cards */
.sq-cards{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin-bottom:28px}
.sq-card.wide{grid-column:span 2}
.sq-card{position:relative;background:var(--surface);border:1px solid var(--line);border-radius:var(--radius);
padding:16px 18px 15px;box-shadow:var(--shadow);min-width:0;transition:transform .2s,box-shadow .2s}
.sq-card:hover{transform:translateY(-2px);box-shadow:var(--shadow-hi)}
.sq-k{font-size:11.5px;font-weight:800;letter-spacing:.1em;color:var(--text-3);white-space:nowrap}
.sq-v{font-size:clamp(24px,2.6vw,32px);font-weight:800;letter-spacing:-.03em;line-height:1.15;margin-top:8px;
font-variant-numeric:tabular-nums;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.sq-f{font-size:12.5px;color:var(--text-3);margin-top:4px;overflow-wrap:anywhere}
.sq-card.lead .sq-v{font-size:clamp(30px,3.6vw,42px)}
.sq-card.demo{background:var(--team);border-color:var(--team-line);color:var(--team-text)}
.sq-card.demo .sq-k,.sq-card.demo .sq-f{color:var(--team-dim)}
.sq-card.demo .sq-k::after{content:" · broker";font-weight:600;letter-spacing:.02em}
.sq-card.streak .sq-v{color:var(--gold)}
/* sections */
.sq-sec{margin-bottom:28px}
.sq-panel{background:var(--surface);border:1px solid var(--line);border-radius:var(--radius);box-shadow:var(--shadow);
overflow:hidden}
/* the league: one grid row a test, never a table */
.lg-head,.lg-row{display:grid;grid-template-columns:62px minmax(0,1fr) 116px 92px 70px 70px 150px;gap:0 14px;
align-items:center;padding:0 18px}
.lg-head{font-size:11px;font-weight:800;letter-spacing:.1em;color:var(--text-3);padding-top:12px;padding-bottom:10px;
border-bottom:1px solid var(--line);background:var(--surface-2)}
.lg-head .r,.lg-row .r{text-align:right}
.lg-row{min-height:64px;border-bottom:1px solid var(--line);transition:background .18s}
.lg-row:last-child{border-bottom:0}
.lg-row:hover{background:var(--surface-2)}
.lg-row:hover .lg-name{color:var(--accent)}
.lg-stats{display:contents}
.lg-pos{display:flex;flex-direction:column;align-items:flex-start;line-height:1.1}
.lg-pos b{font-size:19px;font-weight:850;font-variant-numeric:tabular-nums;letter-spacing:-.02em}
.lg-row.p1 .lg-pos b{color:var(--gold)}.lg-row.p2 .lg-pos b,.lg-row.p3 .lg-pos b{color:var(--accent)}
.mv{font-size:11px;font-weight:750;margin-top:3px;font-variant-numeric:tabular-nums}
.mv.up{color:var(--good)}.mv.down{color:var(--bad)}.mv.same{color:var(--text-3)}.mv.new{color:var(--accent);
letter-spacing:.06em}
.lg-bot{min-width:0;padding:10px 0}
.lg-name{font-weight:750;font-size:15px;letter-spacing:-.01em;transition:color .18s;white-space:nowrap;overflow:hidden;
text-overflow:ellipsis}
.lg-desc{font-size:12px;color:var(--text-3);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.lg-money{font-size:17px;font-weight:800;text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
.lg-c{font-size:14px;font-weight:650;text-align:right;font-variant-numeric:tabular-nums;color:var(--text-2);
white-space:nowrap}
.lg-c small{display:none}
.wr{display:block;height:3px;border-radius:3px;background:var(--line);margin:5px 0 0 auto;width:52px;overflow:hidden}
.wr i{display:block;height:100%;background:var(--accent);border-radius:3px}
.fire{color:var(--gold);font-weight:800}
.lg-status{display:flex;gap:6px;justify-content:flex-end;flex-wrap:wrap}
.bdg{display:inline-flex;align-items:center;gap:4px;font-size:10.5px;font-weight:800;letter-spacing:.06em;
padding:4px 8px;border-radius:999px;white-space:nowrap;background:var(--muted-soft);color:var(--text-2)}
.bdg.gold{background:var(--gold-soft);color:var(--gold)}.bdg.teal{background:var(--accent-soft);color:var(--accent)}
.bdg.amber{background:var(--amber-soft);color:var(--amber)}.bdg.muted{color:var(--text-3)}
.bdg.plain{background:var(--muted-soft);color:var(--text-2)}
.tag{display:inline-block;font-size:10px;font-weight:800;letter-spacing:.08em;padding:3px 7px;border-radius:6px;
white-space:nowrap;border:1px solid var(--gold);color:var(--gold)}
.tag.on{border-color:var(--accent);color:var(--accent)}
.lg-more{display:flex;justify-content:center;padding:14px;border-top:1px solid var(--line);background:var(--surface-2)}
.lg-more a{font-size:13px;font-weight:750;color:var(--accent);padding:6px 14px;border-radius:10px;
transition:background .18s}
.lg-more a:hover{background:var(--accent-soft)}
.sq-empty{padding:36px 22px;text-align:center;color:var(--text-3)}
.sq-empty b{display:block;color:var(--text);font-size:17px;margin-bottom:4px}
/* the first team */
.team{background:var(--team);border:1px solid var(--team-line);border-radius:var(--radius);color:var(--team-text);
box-shadow:var(--shadow-hi);overflow:hidden;position:relative}
.team::before{content:"";position:absolute;left:0;right:0;top:0;height:2px;background:var(--gold-2);opacity:.8}
.team-head{display:flex;align-items:baseline;justify-content:space-between;gap:10px;flex-wrap:wrap;padding:18px 20px 8px}
.team-head h2{margin:0;font-size:13px;font-weight:800;letter-spacing:.12em;color:var(--gold-2)}
.team-head small{color:var(--team-dim);font-size:12.5px}
.tm-row{display:grid;grid-template-columns:minmax(0,1fr) 120px 90px 70px 70px;gap:0 14px;align-items:center;
padding:14px 20px;border-top:1px solid var(--team-line);transition:background .18s}
.tm-row:hover{background:rgba(255,255,255,.04)}
.tm-row.head{font-size:11px;font-weight:800;letter-spacing:.1em;color:var(--team-dim);padding-top:8px;padding-bottom:8px}
.tm-row .r{text-align:right}
.tm-name{font-weight:750;font-size:16px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tm-desc{font-size:12px;color:var(--team-dim);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tm-money{font-size:18px;font-weight:800;text-align:right;font-variant-numeric:tabular-nums}
.team .pos{color:#5fe3a8}.team .neg{color:#ff8a8a}
.tm-c{text-align:right;font-weight:650;font-variant-numeric:tabular-nums;color:var(--team-text)}
.tm-c small{display:none}
.team .fire{color:var(--gold-2)}
/* the two small cards */
.sq-duo{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1.25fr);gap:14px;margin-bottom:28px}
.mini{background:var(--surface);border:1px solid var(--line);border-radius:var(--radius);box-shadow:var(--shadow);
padding:18px 20px;min-width:0}
.mini h2{margin:0 0 12px;font-size:12px;font-weight:800;letter-spacing:.12em;color:var(--text-3)}
.act{display:flex;gap:12px;align-items:flex-start}
.act-e{font-size:24px;line-height:1;width:44px;height:44px;border-radius:13px;display:grid;place-items:center;
background:var(--accent-soft);flex:none}
.act b{display:block;font-size:16px;font-weight:750;letter-spacing:-.01em}
.act span{font-size:12.5px;color:var(--text-3)}
.fac{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}
.fac div{background:var(--surface-2);border-radius:12px;padding:10px 12px;min-width:0}
.fac b{display:block;font-size:24px;font-weight:800;letter-spacing:-.02em;font-variant-numeric:tabular-nums}
.fac span{font-size:10.5px;font-weight:800;letter-spacing:.08em;color:var(--text-3);white-space:nowrap}
.fac .gold b{color:var(--gold)}
.mini-go{display:inline-block;margin-top:12px;font-size:12.5px;font-weight:700;color:var(--accent)}
.sq-foot{display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;color:var(--text-3);font-size:12px;
border-top:1px solid var(--line);padding-top:16px}
.sq-foot a{color:var(--text-3);font-weight:600}.sq-foot a:hover{color:var(--accent)}
/* a test's own page */
.bp-back{display:inline-block;font-size:13px;font-weight:700;color:var(--text-3);margin-bottom:12px}
.bp-back:hover{color:var(--accent)}
.bp-hero{display:flex;justify-content:space-between;align-items:flex-end;gap:16px;flex-wrap:wrap;margin-bottom:22px}
.bp-hero h1{margin:0;font-size:clamp(30px,4.6vw,48px);font-weight:850;letter-spacing:-.03em;line-height:1.02}
.bp-hero p{margin:8px 0 0;color:var(--text-3);font-size:13.5px}
.bp-pills{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}
.bp-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;margin-bottom:22px}
.bp-card{background:var(--surface);border:1px solid var(--line);border-radius:var(--radius);box-shadow:var(--shadow);
padding:20px 22px;min-width:0}
.bp-card h2{margin:0 0 10px;font-size:12px;font-weight:800;letter-spacing:.12em;color:var(--accent)}
.bp-card p{margin:0 0 8px;color:var(--text-2)}
.bp-card p:last-child{margin-bottom:0}
.bp-made{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.bp-made>div{background:var(--surface-2);border-radius:14px;padding:12px 14px;min-width:0}
.bp-made>div.demo{background:var(--team);color:var(--team-text)}
.bp-made>div.demo .sq-k,.bp-made>div.demo .sq-f{color:var(--team-dim)}
.bp-made .sq-v{font-size:26px;margin-top:4px}
.bp-btn{display:inline-flex;align-items:center;gap:8px;padding:13px 20px;border-radius:14px;background:var(--text);
color:var(--bg);font-size:13px;font-weight:800;letter-spacing:.1em;transition:transform .18s,box-shadow .18s}
.bp-btn:hover{transform:translateY(-1px);box-shadow:var(--shadow-hi)}
/* a moment worth a smile: once per event */
.glow{animation:glow 2.6s ease-out 1}
@keyframes glow{0%{box-shadow:0 0 0 0 rgba(217,158,30,.0)}20%{box-shadow:0 0 0 3px rgba(217,158,30,.55),
0 0 28px rgba(217,158,30,.35)}100%{box-shadow:0 0 0 0 rgba(217,158,30,0)}}
#sq-toast{position:fixed;left:50%;bottom:22px;transform:translate(-50%,20px);opacity:0;z-index:50;
background:var(--team);color:var(--team-text);border:1px solid var(--team-line);border-radius:999px;
padding:11px 18px;font-weight:700;font-size:14px;box-shadow:var(--shadow-hi);transition:opacity .35s,transform .35s;
max-width:calc(100vw - 32px);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;pointer-events:none}
#sq-toast.show{opacity:1;transform:translate(-50%,0)}
/* first paint only: the 10-second refresh swaps the content in place and never replays this */
@keyframes rise{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}
html:not(.settled) .rise{animation:rise .5s cubic-bezier(.2,.7,.2,1) both}
html:not(.settled) .rise.d1{animation-delay:.05s}html:not(.settled) .rise.d2{animation-delay:.1s}
html:not(.settled) .rise.d3{animation-delay:.15s}html:not(.settled) .rise.d4{animation-delay:.2s}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
@media (max-width:980px){.lg-head,.lg-row{grid-template-columns:52px minmax(0,1fr) 104px 70px 56px 60px 120px;
gap:0 10px;padding:0 14px}.sq-duo{grid-template-columns:minmax(0,1fr)}}
/* phones: the bar wraps, the cards go two by two, the league row folds its numbers under the name */
@media (max-width:720px){
.sq-bar-in{padding:10px 16px;flex-wrap:wrap;gap:8px 12px}
.sq-nav{order:3;flex-basis:100%;margin:0 -4px}
.sq-tools{margin-left:auto}
.sq-wrap{padding:18px 16px 56px}
.sq-hero{margin:0 0 18px}
.sq-tl{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));width:100%}
.sq-tl a{padding:9px 4px;font-size:11.5px;letter-spacing:.03em}
.sq-cards{grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.sq-card{padding:13px 14px}
.sq-v{font-size:22px}.sq-card.wide{grid-column:1/-1}.sq-card.lead .sq-v{font-size:30px}
.lg-head{display:none}
.lg-row{grid-template-columns:40px minmax(0,1fr) auto;grid-template-areas:"pos bot money" "pos stats stats";
gap:0 10px;padding:12px 14px;align-items:start}
.lg-pos{grid-area:pos;padding-top:2px}.lg-bot{grid-area:bot;padding:0}.lg-name{white-space:normal;overflow-wrap:anywhere}.lg-money{grid-area:money;font-size:16px}
.lg-stats{grid-area:stats;display:flex;flex-wrap:wrap;align-items:center;gap:4px 12px;margin-top:6px}
.lg-c{text-align:left;font-size:12.5px}.lg-c small{display:inline;color:var(--text-3);font-weight:600}
.wr{display:none}.lg-status{justify-content:flex-start;margin-left:auto}
.tm-row{grid-template-columns:minmax(0,1fr) auto;gap:4px 10px;padding:12px 16px}
.tm-row.head{display:none}
.tm-row .tm-c{grid-column:auto;text-align:left;font-size:12.5px;display:inline}
.tm-row .tm-c small{display:inline;color:var(--team-dim)}
.tm-row .tm-stats{grid-column:1/-1;display:flex;gap:12px;flex-wrap:wrap}
.fac{grid-template-columns:repeat(2,minmax(0,1fr))}
.bp-grid,.bp-made{grid-template-columns:minmax(0,1fr)}
}
@media (min-width:721px){.tm-stats{display:contents}}
"""

SQUAD_THEME_JS = """<script>
(function(){var t=null;try{t=localStorage.getItem('mintel-theme')}catch(e){}
if(t!=='dark'&&t!=='light'){try{t=window.matchMedia&&matchMedia('(prefers-color-scheme: dark)').matches?'dark':'light'}
catch(e){t='light'}}document.documentElement.setAttribute('data-theme',t);})();
</script>"""

# The 10-second refresh: the page asks for itself and swaps the new content in place - no reload, so nothing
# animates again and the scroll stays where it is. Success moments: each [data-moment] key is shown once, the
# first new one as a small toast and a gold glow, remembered in localStorage (in memory when that is blocked).
# The best streak and the record five days are remembered as numbers: a new best is celebrated, the first visit
# only notes it.
SQUAD_JS = """<script>
(function(){
var R=document.documentElement,mem={};
setTimeout(function(){R.classList.add('settled')},1500);
function get(k){try{var v=localStorage.getItem(k);return v===null?(mem[k]||null):v}catch(e){return mem[k]||null}}
function put(k,v){mem[k]=v;try{localStorage.setItem(k,v)}catch(e){}}
function toast(text,el){var t=document.getElementById('sq-toast');if(!t){t=document.createElement('div');
t.id='sq-toast';t.setAttribute('role','status');document.body.appendChild(t);}t.textContent=text;
t.classList.add('show');setTimeout(function(){t.classList.remove('show')},4200);
if(el){el.classList.remove('glow');void el.offsetWidth;el.classList.add('glow');}}
function moments(){
var seen=(get('squad-seen')||'').split('|'),has={},first=null,i;for(i=0;i<seen.length;i++)has[seen[i]]=1;
document.querySelectorAll('[data-moment]').forEach(function(el){var k=el.getAttribute('data-moment');
if(!has[k]){has[k]=1;seen.push(k);if(!first)first=el;}});
put('squad-seen',seen.filter(Boolean).slice(-300).join('|'));
[['data-streak','squad-best-streak'],['data-record5','squad-best-5d']].forEach(function(p){
var el=document.querySelector('['+p[0]+']');if(!el)return;var v=parseFloat(el.getAttribute(p[0]));
if(!(v>0))return;var old=parseFloat(get(p[1]));if(isNaN(old)){put(p[1],String(v));return;}
if(v>old){put(p[1],String(v));if(!first)first=el;}});
if(first)toast(first.getAttribute('data-moment-text')||'',first);}
function swap(){if(document.hidden)return;
fetch(location.href,{cache:'no-store',credentials:'same-origin'}).then(function(r){return r.ok?r.text():null})
.then(function(t){if(!t)return;var d=new DOMParser().parseFromString(t,'text/html');
['sq-main','sq-health'].forEach(function(id){var n=d.getElementById(id),o=document.getElementById(id);
if(n&&o&&n.innerHTML!==o.innerHTML)o.innerHTML=n.innerHTML;});moments();}).catch(function(){});}
var b=document.getElementById('theme-btn');if(b){var s=function(){b.textContent=
R.getAttribute('data-theme')==='dark'?'Light':'Dark'};s();b.addEventListener('click',function(){
var t=R.getAttribute('data-theme')==='dark'?'light':'dark';R.setAttribute('data-theme',t);
try{localStorage.setItem('mintel-theme',t)}catch(e){}s();});}
moments();setInterval(swap,10000);
})();
</script>"""

NAV = (("squad", "Squad", "/"), ("demo", "Demo", "/demo"), ("factory", "Factory", "/factory"))
ADVANCED_HREF = "/advanced"


def health_of(snap: Optional[dict]) -> tuple[str, str]:
    """(its class, its words) for the one small health dot: running, waiting, or needs a look."""
    st = (snap or {}).get("status") or {}
    health = (snap or {}).get("health") or {}
    state = str(st.get("bot") or "")
    if state == "RUNNING" and not health.get("safe_mode"):
        return "ok", "running"
    if state == "WAITING FOR METATRADER":
        return "warn", "waiting for MetaTrader"
    if state in ("", "STARTING"):
        return "warn", "starting"
    return "bad", "needs a look"


def _e(v) -> str:
    return html.escape(str(v if v is not None else ""))


def _money(v, signed: bool = True) -> str:
    """A money figure, coloured: green above zero, red below."""
    if v is None:
        return '<span class="zero">—</span>'
    f = float(v)
    cls = "pos" if f > 0 else "neg" if f < 0 else "zero"
    return f'<span class="{cls}">{_e(_gbp(f, signed))}</span>'


def _tq(timeline: str, extra: str = "") -> str:
    q = f"t={timeline}" if timeline != DEFAULT_TIMELINE else ""
    q = "&".join(x for x in (q, extra) if x)
    return f"?{q}" if q else ""


def shell(title: str, body: str, active: str, health: tuple[str, str], stamp: str = "") -> str:
    """A squad page: the top bar (brand, Squad · Demo · Factory · Advanced, the health dot, the theme), the
    content in #sq-main (what the refresh swaps), the script."""
    nav = "".join(f'<a href="{h}"{" class=on" if k == active else ""}>{_e(w)}</a>' for k, w, h in NAV)
    nav += f'<a class="adv" href="{ADVANCED_HREF}">Advanced</a>'
    cls, words = health
    return (f'<!doctype html><html lang="en-GB"><head><meta charset="utf-8">'
            f'<meta name="viewport" content="width=device-width,initial-scale=1">'
            f'<noscript><meta http-equiv="refresh" content="10"></noscript><title>{_e(title)}</title>'
            f'{SQUAD_THEME_JS}<style>{SQUAD_CSS}</style></head><body>'
            f'<header class="sq-bar"><div class="sq-bar-in"><a class="sq-brand" href="/"><span class="sq-mark">🔥'
            f'</span>TRADING SQUAD</a><nav class="sq-nav">{nav}</nav><div class="sq-tools">'
            f'<span id="sq-health"><span class="sq-health {cls}" title="The trader"><i></i>{_e(words)}</span></span>'
            f'<button class="sq-theme" id="theme-btn" type="button">Dark</button></div></div></header>'
            f'<main class="sq-wrap" id="sq-main">{body}'
            f'<footer class="sq-foot"><span>Paper is practice money after costs. Demo is MetaTrader\'s own figures. '
            f'They are never added together.</span><span>{_e(stamp)} · <a href="{ADVANCED_HREF}">Advanced</a>'
            f'</span></footer></main>{SQUAD_JS}</body></html>')


def timeline_bar(timeline: str, base: str = "/", keep: str = "") -> str:
    return '<nav class="sq-tl" aria-label="Timeline">' + "".join(
        f'<a href="{base}{_tq(k, keep)}"{" class=on" if k == timeline else ""} data-t="{k}">{_e(w)}</a>'
        for k, w in TIMELINES) + "</nav>"


def _card(key: str, value: str, foot: str = "", cls: str = "", attrs: str = "") -> str:
    return (f'<div class="sq-card {cls} rise" data-card="{key}"{attrs}><div class="sq-k">{_e(key)}</div>'
            f'<div class="sq-v num">{value}</div>' + (f'<div class="sq-f">{_e(foot)}</div>' if foot else "")
            + "</div>")


def render_cards(b: dict) -> str:
    """PAPER PROFIT, WIN RATE, TRADES, WINNING BOTS, BEST STREAK; DEMO PROFIT (the broker's) beside them only
    when a test is on DEMO or the account traded in the period - never added to the paper figure."""
    h = b["head"]
    pair = "" if b.get("show_demo") else " wide"          # four columns: every row full, with or without DEMO
    rec = f' data-record5="{h["net"]:.2f}" data-moment-text="🚀 Record 5 days: {_e(_gbp(h["net"]))} paper"' \
        if b["timeline"] == "5d" and h["net"] > 0 else ""
    out = [_card("PAPER PROFIT", _money(h["net"]) if h["trades"] else '<span class="zero">£0.00</span>',
                 "practice money, after costs", "lead wide", rec),
           _card("WIN RATE", _e(_pct(h["win_rate"])), f"{h['wins']:,} winning trade{'' if h['wins'] == 1 else 's'}" if h["trades"] else "no trades yet"),
           _card("TRADES", _e(f"{h['trades']:,}"), "paper trades closed"),
           _card("WINNING BOTS", f'{h["winning"]}<span class="zero"> / {h["traded"]}</span>', "in profit / traded", pair.strip())]
    s = h.get("streak")
    if s:
        out.append(_card("BEST STREAK", f'🔥 {s["days"]} day{"" if s["days"] == 1 else "s"}', s["nick"], "streak" + pair,
                         f' data-streak="{s["days"]}" data-moment-text="🔥 New best streak: {_e(s["nick"])} — '
                         f'{s["days"]} profitable days"'))
    else:
        out.append(_card("BEST STREAK", '<span class="zero">—</span>', "no profitable day yet", "streak" + pair))
    if b.get("show_demo"):
        d = b.get("demo")
        out.append(_card("DEMO PROFIT", _money(d["net"]) if d else '<span class="zero">—</span>',
                         (f"{d['trades']} demo trade{'' if d['trades'] == 1 else 's'} · {_pct(d['win_rate'])} won"
                          if d else "waiting for MetaTrader"), "demo wide"))
    return f'<section class="sq-cards" id="cards">{"".join(out)}</section>'


def _badge(code: str) -> str:
    if not code:
        return ""
    emoji, cls = BADGES[code]
    return f'<span class="bdg {cls}" data-badge="{code}">{emoji} {code}</span>'


def _streak(n) -> str:
    n = int(n or 0)
    return f'<span class="fire">🔥 {n}</span>' if n else '<span class="zero">–</span>'


def league_row(r: dict, timeline: str, prev_top: Optional[str]) -> str:
    arrow, mcls, mtitle = move_words(r)
    tags = ""
    if r.get("live"):
        tags += f' <span class="tag on">{ON_DEMO}</span>'
    elif r.get("demo_ready"):
        tags += f' <span class="tag" data-tag="demo-ready">{DEMO_READY}</span>'
    moment = ""
    if r["pos"] == 1 and prev_top is not None and prev_top != r["id"] and r["net"] > 0:
        moment = (f' data-moment="top:{_e(r["id"])}:{_e(uk_date(r["last"]).isoformat())}" '
                  f'data-moment-text="🏆 {_e(r["nick"])} has taken top spot"')
    elif r.get("badge") == CHAMPION:
        moment = f' data-moment="champ:{_e(r["id"])}" data-moment-text="🏆 New champion: {_e(r["nick"])}"'
    wr = r.get("win_rate")
    pcls = f" p{r['pos']}" if r["pos"] <= 3 else ""
    return (f'<a class="lg-row{pcls}" href="/bot?id={quote(str(r["id"]))}{_tq(timeline).replace("?", "&")}" '
            f'data-id="{_e(r["id"])}" data-pos="{r["pos"]}"{moment}>'
            f'<div class="lg-pos"><b>{r["pos"]}</b><span class="mv {mcls}" title="{_e(mtitle)}">{_e(arrow)}</span></div>'
            f'<div class="lg-bot"><div class="lg-name">{_e(r["nick"])}{tags}</div>'
            f'<div class="lg-desc">{_e(r["desc"])}</div></div>'
            f'<div class="lg-money">{_money(r["net"])}</div>'
            f'<div class="lg-stats"><div class="lg-c" data-col="win">{_e(_pct(wr))}<small> won</small>'
            f'<span class="wr"><i style="width:{0 if wr is None else round(wr)}%"></i></span></div>'
            f'<div class="lg-c" data-col="trades">{r["trades"]}<small> trades</small></div>'
            f'<div class="lg-c" data-col="streak">{_streak(r["streak"])}</div>'
            f'<div class="lg-status">{_badge(r.get("badge") or "")}</div></div></a>')


def render_league(b: dict, show_all: bool = False) -> str:
    """PAPER LEAGUE: POS, BOT, PROFIT, WIN RATE, TRADES, STREAK, STATUS - every test that traded in the period,
    best profit first; the top TOP_SHOWN, and "Show all N" (the same address with ?all=1) for the rest."""
    rows = b["league"]
    n = len(rows)
    shown = rows if show_all else rows[:TOP_SHOWN]
    prev_top = b["prev"][0]["id"] if b.get("prev") else None
    head = ('<div class="lg-head"><span>POS</span><span>BOT</span><span class="r">PROFIT</span>'
            '<span class="r">WIN RATE</span><span class="r">TRADES</span><span class="r">STREAK</span>'
            '<span class="r">STATUS</span></div>')
    if rows:
        body = "".join(league_row(r, b["timeline"], prev_top) for r in shown)
        more = ""
        if n > TOP_SHOWN:
            more = (f'<div class="lg-more"><a href="/{_tq(b["timeline"], "all=1")}#league">Show all {n}</a></div>'
                    if not show_all else
                    f'<div class="lg-more"><a href="/{_tq(b["timeline"])}#league">Show the top {TOP_SHOWN}</a></div>')
        inner = head + body + more
    else:
        inner = ('<div class="sq-empty"><b>No paper trades in this period yet</b>Every test starts on paper. The '
                 'league fills up as their trades close.</div>')
    sub = f"{n} test{'' if n == 1 else 's'} traded · practice money after costs · arrows vs the previous day"
    return (f'<section class="sq-sec rise d2" id="league"><div class="sq-h2"><h2>PAPER LEAGUE</h2>'
            f'<small>{_e(sub)}</small></div><div class="sq-panel">{inner}</div></section>')


def render_team(b: dict, title: str = "DEMO SQUAD") -> str:
    """DEMO SQUAD, "the first team": BOT, PROFIT, WIN RATE, TRADES, STREAK, from MetaTrader's figures. Only
    when a test is on DEMO."""
    rows = b.get("demo_rows") or []
    if not rows:
        return ""
    lines = []
    for r in rows:
        since = _when(r.get("since"))
        moment = ""
        if since is not None and (b["now"] - since) <= dt.timedelta(days=NEW_DAYS):
            moment = (f' data-moment="promo:{_e(r["id"])}:{_e(r.get("since"))}" '
                      f'data-moment-text="🚀 {_e(r["nick"])} promoted to Demo"')
        known = r.get("known")
        lines.append(
            f'<a class="tm-row" href="/bot?id={quote(str(r["id"]))}{_tq(b["timeline"]).replace("?", "&")}" '
            f'data-id="{_e(r["id"])}"{moment}><div><div class="tm-name">{_e(r["nick"])}</div>'
            f'<div class="tm-desc">{_e(r["desc"])}' + (f' · on Demo since {_e(_d(uk_date(since)))}' if since else "")
            + f'</div></div><div class="tm-money">{_money(r["net"]) if known else _money(None)}</div>'
            f'<div class="tm-stats"><div class="tm-c">{_e(_pct(r["win_rate"]))}<small> won</small></div>'
            f'<div class="tm-c">{_e(r["trades"] if known else "—")}<small> trades</small></div>'
            f'<div class="tm-c">{_streak(r["streak"]) if known else "—"}</div></div></a>')
    any_known = any(r.get("known") for r in rows)
    sub = "MetaTrader's figures, after costs" if any_known else "waiting for MetaTrader's figures"
    return (f'<section class="sq-sec rise d3" id="demo-squad"><div class="team"><div class="team-head">'
            f'<h2>{_e(title)} · THE FIRST TEAM</h2><small>{_e(sub)}</small></div>'
            f'<div class="tm-row head"><span>BOT</span><span class="r">PROFIT</span><span class="r">WIN RATE</span>'
            f'<span class="r">TRADES</span><span class="r">STREAK</span></div>{"".join(lines)}</div></section>')


def render_latest(b: dict) -> str:
    a = b.get("latest")
    if a:
        when = uk_date(a["when"])
        inner = (f'<div class="act" data-kind="{_e(a["kind"])}"><div class="act-e">{_e(a["emoji"])}</div><div>'
                 f'<b>{_e(a["text"])}</b><span>{_e(_d(when))}</span></div></div>')
    else:
        when = (b.get("pick") or {}).get("when") or ""
        inner = (f'<div class="act" data-kind="none"><div class="act-e">⏳</div><div><b>All quiet so far</b>'
                 f'<span>{_e(when)}</span></div></div>')
    return f'<div class="mini rise d3" id="latest"><h2>LATEST ACTION</h2>{inner}</div>'


def render_factory_card(b: dict, link: bool = True) -> str:
    f = b["factory"]
    cells = (("active", "ACTIVE TESTS", ""), ("new", "NEW THIS WEEK", ""), ("promising", "PROMISING", ""),
             ("champions", "CHAMPIONS", "gold"))
    inner = "".join(f'<div class="{c}" data-count="{k}"><b>{int(f.get(k) or 0):,}</b><span>{_e(w)}</span></div>'
                    for k, w, c in cells)
    go = '<a class="mini-go" href="/factory">Open the factory →</a>' if link else ""
    return f'<div class="mini rise d4" id="factory"><h2>STRATEGY FACTORY</h2><div class="fac">{inner}</div>{go}</div>'


def _stamp(now: dt.datetime) -> str:
    from ..clock import TZ_LONDON
    t = to_utc(now).astimezone(TZ_LONDON)
    return f"Updated {t.strftime('%a')} {t.day} {t.strftime('%b, %H:%M')} UK"


def _hero(title_html: str, b: dict, base: str = "/") -> str:
    return (f'<section class="sq-hero rise"><div><h1>{title_html}</h1><p>{_e(b["label"])}</p></div>'
            f'{timeline_bar(b["timeline"], base)}</section>')


def render_squad(b: dict, health: tuple[str, str] = ("ok", "running"), show_all: bool = False) -> str:
    """"/": TRADING SQUAD and the timeline, the headline cards, the PAPER LEAGUE, the DEMO SQUAD (only when it
    exists), LATEST ACTION and STRATEGY FACTORY. Nothing else."""
    body = (_hero("TRADING <span>SQUAD</span>", b) + render_cards(b) + render_league(b, show_all)
            + render_team(b) + f'<section class="sq-duo">{render_latest(b)}{render_factory_card(b)}</section>')
    return shell("Trading Squad", body, "squad", health, _stamp(b["now"]))


def render_bot_page(b: dict, test_id: str, health: tuple[str, str] = ("ok", "running"), data_dir=None) -> str:
    """"/bot?id=...": one test in plain words - WHAT IT DOES, WHEN IT LIKES TO TRADE, WHAT IT HAS MADE (paper
    and demo apart), WHY IT IS DOING WELL - then VIEW ADVANCED DETAILS (the existing detailed page)."""
    v = bot_view(b, test_id, data_dir)
    back = f'<a class="bp-back" href="/{_tq(b["timeline"])}">← Squad</a>'
    if v is None:
        body = back + ('<div class="sq-panel sq-empty"><b>No such test</b>It may have a new name, or it has not '
                       'traded since the reset.</div>')
        return shell("Trading Squad", body, "squad", health, _stamp(b["now"]))
    t, row, f = v["test"], v["row"], v["figures"]
    pills = ""
    if row is not None:
        arrow, mcls, mtitle = move_words(row)
        pills += f'<span class="bdg teal">#{row["pos"]} on the league</span>'
        pills += f'<span class="bdg plain" title="{_e(mtitle)}">{_e(arrow)}</span>'
        pills += _badge(row.get("badge") or "")
    if t.get("live"):
        pills += f'<span class="tag on">{ON_DEMO}</span>'
    elif t.get("ready"):
        pills += f'<span class="tag">{DEMO_READY}</span>'
    paper = (f'<div><div class="sq-k">PAPER · {_e(b["label"]).upper()}</div><div class="sq-v num">{_money(f["net"])}'
             f'</div><div class="sq-f">{f["trades"]} trade{"" if f["trades"] == 1 else "s"} · {_e(_pct(f["win_rate"]))}'
             f' won · practice money after costs</div></div>')
    d = v["demo"]
    if d is not None:
        demo = (f'<div class="demo"><div class="sq-k">DEMO · MetaTrader</div><div class="sq-v num">'
                f'{_money(d["net"]) if d.get("known") else _money(None)}</div><div class="sq-f">'
                + (f'{d["trades"]} demo trade{"" if d["trades"] == 1 else "s"} · {_e(_pct(d["win_rate"]))} won'
                   if d.get("known") else "waiting for MetaTrader's figures") + '</div></div>')
    else:
        demo = ('<div><div class="sq-k">DEMO</div><div class="sq-v num"><span class="zero">Not on Demo</span></div>'
                '<div class="sq-f">' + ("passes the bar: in line for Friday's pick" if t.get("ready") else
                                         "it moves up only once it passes the bar") + '</div></div>')
    when = "".join(f"<p>{_e(x)}</p>" for x in v["when"])
    why = "".join(f"<p>{_e(x)}</p>" for x in v["why"]) or "<p>Nothing stands out yet.</p>"
    body = (back + f'<section class="bp-hero rise"><div><h1>{_e(t["nick"])}</h1><p>{_e(t.get("desc") or "")}</p>'
            f'<div class="bp-pills">{pills}</div></div>{timeline_bar(b["timeline"], "/bot", "id=" + quote(test_id))}'
            f'</section><section class="bp-grid">'
            f'<div class="bp-card rise d1" data-section="does"><h2>WHAT IT DOES</h2><p>{_e(v["does"])}</p></div>'
            f'<div class="bp-card rise d2" data-section="when"><h2>WHEN IT LIKES TO TRADE</h2>{when}</div>'
            f'<div class="bp-card rise d3" data-section="made"><h2>WHAT IT HAS MADE</h2><div class="bp-made">{paper}'
            f'{demo}</div></div>'
            f'<div class="bp-card rise d4" data-section="why"><h2>{_e(v["why_title"])}</h2>{why}</div></section>'
            f'<p><a class="bp-btn" href="{_e(v["href"])}" data-advanced>VIEW ADVANCED DETAILS →</a></p>')
    return shell(f"{t['nick']} · Trading Squad", body, "squad", health, _stamp(b["now"]))


def render_demo_page(b: dict, health: tuple[str, str] = ("ok", "running")) -> str:
    """"/demo": DEMO PROFIT (the broker's), the first team, and who is next in line (passing the bar)."""
    d = b.get("demo")
    cards = (_card("DEMO PROFIT", _money(d["net"]) if d else _money(None),
                   "MetaTrader's figures, after costs" if d else "waiting for MetaTrader", "demo lead wide")
             + _card("DEMO TRADES", _e(d["trades"] if d else "—"), _pct(d["win_rate"]) + " won" if d else "")
             + _card("ON DEMO", _e(len(b.get("demo_rows") or [])), "tests in the first team"))
    team = render_team(b)
    if not team:
        team = ('<section class="sq-sec"><div class="sq-panel sq-empty"><b>Nobody on Demo yet</b>Every Friday at '
                '22:30 UK, up to three tests that pass the bar move up from paper. '
                + _e((b.get("pick") or {}).get("when") or "") + '</div></section>')
    ready = [r for r in b["league"] if r.get("demo_ready")]
    nxt = ""
    if ready:
        nxt = (f'<section class="sq-sec"><div class="sq-h2"><h2>DEMO READY</h2><small>pass the bar, in line for '
               f'Friday\'s pick</small></div><div class="sq-panel">'
               + "".join(league_row(r, b["timeline"], None) for r in ready[:TOP_SHOWN]) + '</div></section>')
    body = (_hero("DEMO <span>· THE FIRST TEAM</span>", b, "/demo") + f'<section class="sq-cards">{cards}</section>'
            + team + nxt)
    return shell("Demo · Trading Squad", body, "demo", health, _stamp(b["now"]))


def render_factory_page(b: dict, health: tuple[str, str] = ("ok", "running")) -> str:
    """"/factory": the strategy factory in numbers, the latest action, the champions and the most promising
    tests; the lab in full stays one click away under Advanced."""
    prom = [r for r in b["league"] if r["net"] > 0 and int(r["trades"]) >= PROMISING_TRADES][:TOP_SHOWN]
    champs = [r for r in b["league"] if r.get("ready")]
    sec = ""
    for title, rows, sub in (("CHAMPIONS", champs, "passing the bar"),
                             ("PROMISING", prom, f"in profit with {PROMISING_TRADES}+ trades")):
        if rows:
            sec += (f'<section class="sq-sec"><div class="sq-h2"><h2>{title}</h2><small>{_e(sub)}</small></div>'
                    f'<div class="sq-panel">' + "".join(league_row(r, b["timeline"], None) for r in rows)
                    + '</div></section>')
    body = (_hero("STRATEGY <span>FACTORY</span>", b, "/factory")
            + f'<section class="sq-duo">{render_latest(b)}{render_factory_card(b, link=False)}</section>' + sec
            + '<p><a class="bp-btn" href="/lab">VIEW THE LAB IN FULL →</a></p>')
    return shell("Factory · Trading Squad", body, "factory", health, _stamp(b["now"]))
