"""The showcase: the bots and the lab's candidates on one ladder, in plain words.

Aaron, 10 Oct 2026: "Make sure everything is presented in the bot showcase
really simply for me and not over the top, put it all nice and clear in
layman terms and change the ui to suit this" and "We can then just add in the
current ones to that and have them all together so it's simpler, each day the
winners stand out and we watch them closely like scouts".

This module works out WHAT the main page says about the ladder; the page
itself (mintel/ops/dashboard.py) only draws it. Everything here is read-only:

* The current bots keep every figure they have today (``verdicts.all_verdicts``,
  ``standing.period_view``, ``standing.practice_figures``). Their place on the
  ladder is DERIVED, never stored (``bot_stage``), and their five checks are
  worked out from their own records with the same helpers (``verdicts.stats``,
  ``verdicts.real_trades``, ``verdicts.practice_trades``; Formula 1 from
  MetaTrader's deal rows exactly as ``verdicts.formula1_live`` counts them).
* The lab's candidates come from ONE file, data/lab/summary.json (and the
  practice process's data/lab-status.json for /lab). The page never opens
  data/lab/lab.sqlite and never writes anything.
* Never a guess: a missing lab says "The lab has not run yet.", a broken
  summary says why it could not be read, a short price store says how many
  days it has, and a summary built from made-up prices shows "TEST DATA - not
  a result" in place of every lab figure.

Today's standouts (``standouts``) is a fixed rule, never today's money alone:
see its docstring.
"""
from __future__ import annotations

import datetime as dt
import json
import re
from typing import Optional, Sequence

from ..clock import TZ_LONDON, to_utc, utcnow
from ..lab import (BINNED, CHECK_COSTS, CHECK_FRESH, CHECK_KEYS, CHECK_LABELS, CHECK_LUCK, CHECK_STATES,
                   CHECK_STEADY, CHECK_TRADES, CHIP_COLOURS, FAILED, FRESH_TEST, IDEA, LIVE_TRIAL,
                   LIVE_TRIAL_TRADES, NEEDED_DAYS, NOT_TESTED, NOT_YET, ON_THE_BENCH, PASSED, PAST_TEST,
                   PRACTICE, PRACTICE_P, PROVEN, READY_FOR_LIVE, RUNNING, SHORT_HISTORY_DAYS, STAGES,
                   SUMMARY_SCHEMA, LabPaths)
from . import verdicts as V
from .standing import _money, uk_date, uk_midnight

# ------------------------------------------------------------- the words --
TEST_DATA = "TEST DATA - not a result"
NOT_RUN = "The lab has not run yet."
NO_STANDOUTS = "No standouts today. Nothing has earned it yet - that is the test working."
NOTHING_MOVED = "Nothing moved."
NO_SUCH_IDEA = "No such idea."
STAGE_NOTE = ("Past test = tried on old prices. Fresh test = tried once on prices it had never seen. On the bench = "
              "passed both, waiting for a practice place. Practice = live prices, no money. Ready for live = the "
              "lab's work is done; Aaron's call. Live trial = a little real money, judged after 20 trades. Proven = "
              "keep and grow. Binned = out, not retried.")
LADDER_TITLE = "The bots and the lab's candidates - tap one for what happened, what to do and the evidence"
SYNTHETIC_WHAT_TO_DO = "Nothing - this is test data made up to check the lab works, not a result."
UNREADABLE = "MetaTrader's records could not be read just now - nothing is shown rather than a guess."

# the three groups of the ladder on "/", in order, and the stages each holds (in order)
REAL_MONEY, PRACTISING, BINNED_GROUP = "Real money", "Practising", "Binned"
GROUPS = ((REAL_MONEY, (PROVEN, LIVE_TRIAL)), (PRACTISING, (READY_FOR_LIVE, PRACTICE, ON_THE_BENCH)),
          (BINNED_GROUP, (BINNED,)))
NOT_KNOWN = "Mode not known yet"          # a bot whose mode the page has not been told (MetaTrader not read yet)

# the pill classes of the five checks
CHECK_CLASS = {PASSED: "ok", FAILED: "no", RUNNING: "run", NOT_YET: "ny", NOT_TESTED: "ny"}
CHIP_CLASSES = {"blue", "sky", "sand", "amber", "grey"}

# the lab's state on the page
LAB_NONE, LAB_BROKEN, LAB_COLLECTING, LAB_READY = "none", "broken", "collecting", "ready"

# ------------------------------------------------------------ the rule ---
WINDOW_DAYS = 10                    # standouts look at the last 10 UK trading days ...
WINDOW_MIN_TRADES = 10              # ... and need at least 10 closed trades in them
STANDOUTS_MAX = 3                   # never more, never padded
BINNED_SHOWN_DAYS = 7               # candidates binned this recently sit in the Binned group on "/"
CHANGES_HOURS = 24                  # "What changed since yesterday"
CHANGES_MAX = 5
STANDOUT_STAGES = (PRACTICE, READY_FOR_LIVE, LIVE_TRIAL, PROVEN)
STANDOUT_VERDICTS = (V.PROVEN, V.PROMISING)     # a bot also needs its own verdict to say one of these

# where each bot's idea sits in the lab (spec section 6)
FRESH_WORDS = {V.TNB: "it went straight to practice; its idea is in the lab as 'Momentum burst'",
               "band_breaker": "its idea is in the lab as 'Back to the average'"}
BOTS_RULE = (f"Steady means PROVEN by the bots' rule: {V.MIN_TRADES}+ trades over {V.MIN_DAYS}+ UK days, positive "
             f"after costs, £{V.PROVEN_PROFIT_FACTOR:.2f}+ won for every £1 lost and more up days than down.")
HAND_PICKED = "picked by hand, not from a grid, so it is counted on its own"


# ----------------------------------------------------------------- helpers --
def money(v) -> str:
    """'+£7.13', '-£9.36' (the page's way)."""
    try:
        return _money(float(v), "GBP")
    except (TypeError, ValueError):
        return "-"


def pounds(text) -> str:
    """The pound sign in place of 'GBP ' in any text the lab wrote ('+GBP 38' -> '+£38')."""
    return re.sub(r"GBP\s?(?=[0-9])", "£", str(text or ""))


def _plural(n: int, one: str, many: str = "") -> str:
    return f"{n:,} {one if n == 1 else (many or one + 's')}"


def _when(v) -> Optional[dt.datetime]:
    """A UTC datetime from an ISO stamp or date ('' and nonsense: None)."""
    if v is None or v == "":
        return None
    try:
        if isinstance(v, dt.datetime):
            t = v
        else:
            s = str(v).strip().replace("Z", "+00:00")
            if len(s) == 10:
                return uk_midnight(dt.date.fromisoformat(s))
            t = dt.datetime.fromisoformat(s)
        return to_utc(t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc))
    except (TypeError, ValueError):
        return None


def _date(v) -> Optional[dt.date]:
    """The UK date of an ISO stamp, or the date itself."""
    if isinstance(v, str) and len(v.strip()) == 10:
        try:
            return dt.date.fromisoformat(v.strip())
        except ValueError:
            return None
    t = _when(v)
    return uk_date(t) if t is not None else None


def day_words(v, year: bool = False) -> str:
    """'12 Oct' ('12 Oct 2026' with ``year``); '' when it is not a date."""
    d = _date(v)
    if d is None:
        return ""
    return f"{d.day} {d.strftime('%b')}" + (f" {d.year}" if year else "")


def span_words(a, b) -> str:
    """'14 Sep-9 Oct 2026' (the year once when both ends share it)."""
    da, db = _date(a), _date(b)
    if da is None or db is None:
        return ""
    if da == db:
        return day_words(da, True)
    left = day_words(da, da.year != db.year)
    return f"{left}-{day_words(db, True)}"


def uk_time(t: Optional[dt.datetime], fmt: str = "%a %d %b %H:%M") -> str:
    return "" if t is None else t.astimezone(TZ_LONDON).strftime(fmt)


def _num(v) -> Optional[float]:
    try:
        if v is None or isinstance(v, bool):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _int(v) -> Optional[int]:
    f = _num(v)
    return None if f is None else int(round(f))


def _count(v) -> str:
    """18 -> '18', 18.0 -> '18', 2.5 -> '2.5'."""
    f = _num(v)
    if f is None:
        return "-"
    return f"{int(f):,}" if f == int(f) else f"{f:g}"


def make_check(key: str, state: str, text: str, n=None, of=None, p=None, of_words: str = "") -> dict:
    out = {"key": key, "label": CHECK_LABELS[key], "state": state, "text": text, "n": n, "of": of}
    if p is not None:
        out["p"] = p
    if of_words:
        out["of_words"] = of_words          # what the count is toward: 'Running (2 of 20 for the trial)'
    return out


def ordered_checks(checks) -> list[dict]:
    """The five checks in the page's order and wording; any the record does not
    have reads "Not yet" (never a guessed pass); an unknown state word reads
    "Not yet" too."""
    by_key = {}
    for c in checks or ():
        if isinstance(c, dict) and c.get("key") in CHECK_KEYS and c.get("key") not in by_key:
            by_key[c["key"]] = c
    out = []
    for key in CHECK_KEYS:
        c = by_key.get(key) or {}
        state = c.get("state") if c.get("state") in CHECK_STATES else NOT_YET
        out.append({"key": key, "label": CHECK_LABELS[key], "state": state,
                    "text": str(c.get("text") or ("" if c else "Not worked out yet.")),
                    "n": c.get("n"), "of": c.get("of"), "p": c.get("p"),
                    "of_words": str(c.get("of_words") or "")})
    return out


def check_word(c: dict, synthetic: bool = False) -> str:
    """The pill's words: 'Passed', 'Running (18 of 30)', 'Not tested this way'."""
    state = c.get("state") or NOT_YET
    if state == RUNNING and not synthetic and _num(c.get("n")) is not None and _num(c.get("of")) is not None:
        tail = f" {c['of_words']}" if c.get("of_words") else ""
        return f"Running ({_count(c['n'])} of {_count(c['of'])}{tail})"
    return state


def luck_computed(c: Optional[dict]) -> bool:
    """Has the luck check been worked out? A row without one can never stand out."""
    if not c:
        return False
    return c.get("state") in (PASSED, FAILED, RUNNING) or _num(c.get("p")) is not None


# ------------------------------------------------------ the 10-day window --
def window_start(now: dt.datetime, days: int = WINDOW_DAYS) -> dt.datetime:
    """00:00 UK on the first of the last ``days`` UK trading days (weekdays),
    today counted when it is one."""
    d = uk_date(to_utc(now))
    seen = 0
    while True:
        if d.weekday() < 5:
            seen += 1
            if seen >= days:
                return uk_midnight(d)
        d -= dt.timedelta(days=1)


def window_of(trades: Sequence[dict], now: dt.datetime) -> dict:
    """{trades, net, up_days, down_days, pf} of the trades that closed in the last
    10 UK trading days (``verdicts.stats``: each after its own costs)."""
    start = window_start(now)
    end = to_utc(now)
    s = V.stats([t for t in trades if t.get("closed") is not None and start <= to_utc(t["closed"]) <= end])
    return {"trades": s["trades"], "net": s["net"], "up_days": s["up_days"], "down_days": s["down_days"],
            "pf": s["profit_factor"], "from": uk_date(start).isoformat(), "to": uk_date(end).isoformat()}


def daily_nets(trades: Sequence[dict], now: dt.datetime) -> list[float]:
    """Net per UK day from the first day with a trade to today: every weekday
    (0 on a day without a trade) and any other day it traded on."""
    by_day: dict = {}
    for t in trades:
        if t.get("closed") is None:
            continue
        d = uk_date(to_utc(t["closed"]))
        by_day[d] = by_day.get(d, 0.0) + float(t.get("net") or 0.0)
    if not by_day:
        return []
    d, last = min(by_day), max(max(by_day), uk_date(to_utc(now)))
    out = []
    while d <= last:
        if d.weekday() < 5 or d in by_day:
            out.append(round(by_day.get(d, 0.0), 2))
        d += dt.timedelta(days=1)
    return out


_LUCK_CACHE: dict = {}


def luck_of(daily: Sequence[float], ident: str) -> dict:
    """``mintel.lab.stats.luck_of_daily`` with m = 1 (the bot was picked by
    hand, not from a grid); remembered, so the 10-second refresh does not redo
    2,000 resamples for the same days."""
    from ..lab.stats import luck_of_daily
    key = (ident, tuple(daily))
    got = _LUCK_CACHE.get(key)
    if got is None:
        if len(_LUCK_CACHE) > 256:
            _LUCK_CACHE.clear()
        got = _LUCK_CACHE[key] = luck_of_daily(list(daily), ident, 1)
    return got


# ------------------------------------------------------------- the bots --
def bot_stage(verdict: Optional[dict]) -> str:
    """Where a current bot sits on the ladder, from its mode and verdict (never
    stored): OFF -> BINNED (the Rider); LIVE -> LIVE TRIAL, or PROVEN when its
    own record says PROVEN; Trend & Breakout part LIVE (Formula 1) -> LIVE
    TRIAL; PAPER -> PRACTICE. '' while its mode is not known."""
    v = verdict or {}
    mode = str(v.get("mode") or "").strip().upper()
    if mode == "OFF":
        return BINNED
    if mode == "LIVE":
        return PROVEN if v.get("code") == V.PROVEN else LIVE_TRIAL
    if v.get("live_groups"):
        return LIVE_TRIAL
    if mode == "PAPER":
        return PRACTICE
    return ""


def formula1_trades(deals, open_tickets, since: Optional[dt.datetime], magic: int = V.TNB_MAGIC) -> list[dict]:
    """Formula 1's closed LIVE trades, one by one, chosen exactly as
    ``verdicts.formula1_live`` chooses them (its magic, minor currency pairs,
    opened at or after tnb.live_since_utc, closed): its luck check and its
    last 10 days need the trades, not only their tally."""
    from ..contracts import infer_group
    from .standing import positions_of
    out = []
    if since is None:
        return out
    since = to_utc(since)
    for p in positions_of(deals or (), open_tickets):
        if p.get("bot_magic") != magic or p.get("opened") is None or p["opened"] < since:
            continue
        if infer_group(str(p.get("symbol") or "")) != V.FORMULA1_GROUP or not p.get("is_closed"):
            continue
        out.append({"closed": p["closed"], "net": round(float(p["net"]), 2),
                    "costs": round(float(p.get("commission") or 0.0), 2), "symbol": str(p.get("symbol") or ""),
                    "ticket": int(p.get("position") or 0), "kind": "real"})
    return out


def bot_record(bid: str, v: dict, stage: str, *, sd: Optional[dict], top: Optional[dict], data_dir, now: dt.datetime,
               deals=None) -> tuple[str, Optional[list], str]:
    """(kind, trades, why not) - the record a bot's checks read: its REAL trades
    since the reset (a LIVE or switched-off bot: MetaTrader's, as
    ``verdicts.real_trades``), its PRACTICE trades since the reset (a PAPER bot:
    its own paper record, as ``verdicts.practice_trades``), or Formula 1's live
    trades (Trend & Breakout part LIVE). ``trades`` is None when the record
    cannot be read; ``why not`` then says so."""
    sd, top = sd or {}, top or {}
    b = next((x for x in sd.get("bots") or () if isinstance(x, dict) and x.get("id") == bid), {})
    from .dashboard import DEFAULT_MAGICS
    magic = int(b.get("magic") or v.get("magic") or DEFAULT_MAGICS.get(bid, 0))
    readable = bool(b) and magic not in set(sd.get("unknown_magics") or ()) and b.get("made") is not None
    if v.get("live_groups") and str(v.get("mode") or "").upper() != "LIVE":
        f1 = v.get("formula1") if isinstance(v.get("formula1"), dict) else None
        if f1 is None:
            return "live", None, "Formula 1's live trades are not counted on this computer."
        if not f1.get("readable", True) or deals is None:
            return "live", None, UNREADABLE
        if f1.get("since") is None:
            return "live", None, "its live start time is not recorded, so its live trades are not counted yet."
        trades = formula1_trades(deals, sd.get("open_tickets"), f1["since"], magic)
        if len(trades) != int(((f1.get("stats") or {}).get("trades")) or 0):
            return "live", None, "Formula 1's live trades could not be matched to its count just now."
        return "live", trades, ""
    if stage in (LIVE_TRIAL, PROVEN, BINNED):
        if not readable or not top.get("tot"):
            return "real", None, UNREADABLE
        return "real", V.real_trades(top["tot"], magic), ""
    reset = _when(sd.get("start")) or _when((top.get("tot") or {}).get("start")) or uk_midnight(uk_date(now))
    tomorrow = uk_midnight(uk_date(now) + dt.timedelta(days=1))
    practice = V.practice_trades(data_dir, bid, reset, tomorrow)
    # its verdict is judged on its real trades since the reset AND its practice (verdicts.bot_verdict):
    # the pills read the same trades, so they can never disagree with the chip
    real = V.real_trades(top["tot"], magic) if readable and top.get("tot") else []
    if real:
        return "mixed", real + practice, ""
    return "practice", practice, ""


def _had(kind: str, n: int, days: int) -> str:
    """'2 real trades over 1 UK day', 'No practice trades yet'."""
    if not n:
        return f"No {_kind_trades(kind, 2).split(' ', 1)[1]} yet"
    return f"{_kind_trades(kind, n)} over {_plural(days, 'UK day')}"


def _kind_trades(kind: str, n: int) -> str:
    if kind == "mixed":
        return _plural(n, "trade (real and practice)", "trades (real and practice)")
    return _plural(n, {"real": "real trade", "practice": "practice trade", "live": "live trade"}.get(kind, "trade"))


def bot_checks(bid: str, v: dict, stage: str, kind: str, trades: Optional[list], why_not: str, *,
               now: dt.datetime, since_words: str = "", judge_after: int = V.FORMULA1_JUDGE_AFTER) -> list[dict]:
    """A current bot's five checks, from its own record (spec sections 5 and 6):
    Enough trades, Steady and Costs from its trades (``verdicts.stats``) and its
    verdict (``verdicts.decide``'s rule); Fresh prices "Not tested this way"
    (or Failed when its own research on real past prices found no edge); the
    Luck check from its own daily net, counted once (picked by hand)."""
    v = v or {}
    code = str(v.get("code") or "")
    out = []
    s = V.stats(trades) if trades is not None else None
    n = int(s["trades"]) if s else 0
    since = f" since {since_words}" if since_words else ""
    # the count at which this bot is judged: before it, Costs and Luck show how they stand
    # ("Running"), never a pass or a fail on a handful of trades
    if kind == "live":
        judge_n, judged = judge_after, n >= judge_after
    elif stage in (LIVE_TRIAL, PROVEN):
        judge_n, judged = LIVE_TRIAL_TRADES, n >= LIVE_TRIAL_TRADES or stage == PROVEN
    else:
        judge_n = V.MIN_TRADES
        judged = bool(s) and n >= V.MIN_TRADES and int(s["days"]) >= V.MIN_DAYS
    early = f" So far only: it is judged after {judge_n} trades ({n} of {judge_n})."
    # Enough trades
    if s is None:
        out.append(make_check(CHECK_TRADES, NOT_YET, why_not or UNREADABLE))
    elif kind == "live":
        st = RUNNING if n < judge_after else PASSED
        out.append(make_check(CHECK_TRADES, st, f"{_kind_trades('live', n)} for Formula 1{since} (MetaTrader's "
                                                f"records); it is judged after {judge_after}.", n, judge_after))
    elif stage in (LIVE_TRIAL, PROVEN):
        enough = n >= LIVE_TRIAL_TRADES or stage == PROVEN
        out.append(make_check(CHECK_TRADES, PASSED if enough else RUNNING,
                              f"{_had(kind, n, int(s['days']))}{since}; a live trial is judged after "
                              f"{LIVE_TRIAL_TRADES} (PROVEN needs {V.MIN_TRADES} over {V.MIN_DAYS} UK days).",
                              n, LIVE_TRIAL_TRADES, of_words="for the trial"))
    else:
        enough = n >= V.MIN_TRADES and int(s["days"]) >= V.MIN_DAYS
        if not n:
            st = NOT_YET
        elif stage == BINNED:
            st = PASSED if enough else NOT_YET
        else:
            st = PASSED if enough else RUNNING
        out.append(make_check(CHECK_TRADES, st, f"{_had(kind, n, int(s['days']))}{since}; the bots' rule needs "
                                                f"{V.MIN_TRADES} over {V.MIN_DAYS} UK days.", n, V.MIN_TRADES))
    # Steady
    if kind == "live":
        if s is None:
            out.append(make_check(CHECK_STEADY, NOT_YET, why_not or UNREADABLE))
        elif n < judge_after:
            out.append(make_check(CHECK_STEADY, NOT_YET, f"Judged after {judge_after} live trades: it must be positive "
                                                         f"after costs with more up days than down."))
        else:
            ok = float(s["net"]) > 0 and int(s["up_days"]) > int(s["down_days"])
            out.append(make_check(CHECK_STEADY, PASSED if ok else FAILED,
                                  f"{money(s['net'])} after costs over {_kind_trades('live', n)}, {s['up_days']} days "
                                  f"up and {s['down_days']} down: "
                                  + ("positive with more up days than down." if ok else
                                     "it needs to be positive after costs with more up days than down.")))
    else:
        st = {V.PROVEN: PASSED, V.NO_EDGE: FAILED, V.BREAK_EVEN: FAILED}.get(code, NOT_YET)
        why = str(v.get("why") or "").rstrip(".")
        said = f"Its record says {code}" + (f": {why}" if why else "") + "." if code else "No verdict just now."
        out.append(make_check(CHECK_STEADY, st, f"{said} {BOTS_RULE}"))
    # Fresh prices
    r = v.get("research") if isinstance(v.get("research"), dict) else None
    if (r and V.research_counts(r) and r.get("code") == V.NO_EDGE
            and v.get("why") == "its own test on real past prices found no edge"):
        out.append(make_check(CHECK_FRESH, FAILED, f"Failed - its own test on real past prices found no edge over "
                                                   f"{_plural(int(r.get('trades') or 0), 'trade')} "
                                                   f"({r.get('source') or 'its research'})."))
    else:
        tail = FRESH_WORDS.get(bid, "")
        out.append(make_check(CHECK_FRESH, NOT_TESTED, "Not tested this way" + (f" - {tail}" if tail else "")
                              + ". It was never tried once on prices it had not seen, as the lab's ideas are."))
    # Costs
    if s is None:
        out.append(make_check(CHECK_COSTS, NOT_YET, why_not or UNREADABLE))
    elif not n:
        out.append(make_check(CHECK_COSTS, NOT_YET, "No trades yet, so no costs to test."))
    elif s.get("costs") is None:
        out.append(make_check(CHECK_COSTS, NOT_TESTED, "Not tested this way: its record does not hold each trade's "
                                                       "costs."))
    else:
        c, net = float(s["costs"]), float(s["net"])
        n15, n20 = round(net - 0.5 * c, 2), round(net - c, 2)
        if not judged:
            out.append(make_check(CHECK_COSTS, RUNNING, f"{money(n15)} if its costs ({money(c).lstrip('+')} on "
                                                        f"{_kind_trades(kind, n)}) were half as big again."
                                                        + early, n, judge_n))
        elif n15 > 0:
            out.append(make_check(CHECK_COSTS, PASSED, f"Still {money(n15)} if its costs ({money(c).lstrip('+')} on "
                                                       f"{_kind_trades(kind, n)}) were half as big again; "
                                                       f"{money(n20)} at twice the costs."))
        else:
            out.append(make_check(CHECK_COSTS, FAILED, f"At 1.5 times its costs ({money(c).lstrip('+')} on "
                                                       f"{_kind_trades(kind, n)}) it would be {money(n15)}: it must "
                                                       f"stay above nothing."))
    # Luck check
    if trades is None:
        out.append(make_check(CHECK_LUCK, NOT_YET, why_not or UNREADABLE))
    else:
        daily = daily_nets(trades, now)
        traded = len({uk_date(to_utc(x["closed"])) for x in trades if x.get("closed") is not None})
        if traded < 2:
            out.append(make_check(CHECK_LUCK, NOT_YET, "Not yet: it needs trades on at least 2 days."))
        else:
            luck = luck_of(daily, bid)
            ok = luck["p"] <= PRACTICE_P and judged
            what = "real and practice" if kind == "mixed" else kind
            out.append(make_check(CHECK_LUCK, PASSED if ok else RUNNING,
                                  f"{luck['text']}, from {_plural(len(daily), 'UK day')} of its {what} net "
                                  f"({HAND_PICKED}). It passes at better than 1 in {round(1 / PRACTICE_P)}."
                                  + ("" if judged else early),
                                  p=round(float(luck["p"]), 4)))
    return out


def bot_ladder(bid: str, v: Optional[dict], *, sd: Optional[dict] = None, top: Optional[dict] = None, data_dir="",
               now: Optional[dt.datetime] = None, deals=None, judge_after: int = V.FORMULA1_JUDGE_AFTER) -> dict:
    """A current bot on the ladder: {kind: "bot", id, name, stage, checks,
    what_to_do, verdict, trades (its record, for the standouts), trade_kind}.
    Its figures are not here: the row keeps the ones it has today."""
    v = v or {}
    now = to_utc(now or utcnow())
    stage = bot_stage(v)
    kind, trades, why_not = (bot_record(bid, v, stage, sd=sd, top=top, data_dir=data_dir, now=now, deals=deals)
                             if stage else ("", None, "Its mode is not known yet."))
    since = ""
    if kind == "live":
        f1 = v.get("formula1") or {}
        if f1.get("since") is not None:
            since = f"{day_words(f1['since'])} {uk_time(_when(f1['since']), '%H:%M')} UK"
    elif (sd or {}).get("start"):
        since = day_words((sd or {}).get("start"))
    checks = bot_checks(bid, v, stage, kind, trades, why_not, now=now, since_words=since, judge_after=judge_after)
    rec = str(v.get("recommendation") or "") or "Nothing until its records can be read - they come back on their own."
    return {"kind": "bot", "id": bid, "name": str(v.get("label") or bid), "stage": stage, "checks": checks,
            "what_to_do": rec, "verdict": str(v.get("code") or ""), "trades": trades, "trade_kind": kind,
            "synthetic": False}


# --------------------------------------------------------- the lab's file --
def load_lab(data_dir, now: Optional[dt.datetime] = None) -> dict:
    """What the page knows of the lab, from data/lab/summary.json only (and
    data/lab-status.json, read for /lab): {state, line, summary, status,
    status_error, synthetic, error}. ``state``: "none" (no data/lab/, or no
    summary yet), "broken" (the summary could not be read: ``line`` says why),
    "collecting" (fewer complete days of past prices than it needs) or
    "ready". Never raises."""
    now = to_utc(now or utcnow())
    out = {"state": LAB_NONE, "line": NOT_RUN, "summary": None, "status": None, "status_error": "",
           "synthetic": False, "error": "", "now": now}
    if not data_dir:
        return out
    paths = LabPaths(data_dir)
    try:
        status = json.loads(paths.status.read_text(encoding="utf-8"))
        if isinstance(status, dict):
            out["status"] = status
        else:
            out["status_error"] = "it does not hold a status"
    except FileNotFoundError:
        out["status_error"] = "missing"
    except (OSError, ValueError) as exc:
        out["status_error"] = f"it could not be read ({exc})"
    if not paths.root.is_dir() or not paths.summary.exists():
        return out
    try:
        raw = json.loads(paths.summary.read_text(encoding="utf-8"))
    except ValueError:
        return _broken(out, "it is not complete or not valid - the next nightly run writes it again")
    except OSError as exc:
        return _broken(out, f"the computer would not open it: {getattr(exc, 'strerror', None) or 'unknown reason'}")
    if not isinstance(raw, dict):
        return _broken(out, "it does not hold a summary")
    if raw.get("schema") != SUMMARY_SCHEMA:
        return _broken(out, f"it is written in format {raw.get('schema')!r}; this page reads format {SUMMARY_SCHEMA}")
    out["summary"] = raw
    out["synthetic"] = bool(raw.get("synthetic"))
    store = raw.get("store") if isinstance(raw.get("store"), dict) else {}
    have, need = _int(store.get("complete_days")), _int(store.get("needed_days")) or NEEDED_DAYS
    if have is not None and have < need:
        out["state"] = LAB_COLLECTING
        out["line"] = (f"The lab is collecting past prices: {TEST_DATA}." if out["synthetic"] else
                       f"The lab is collecting past prices: {have} of {need} days needed.")
        return out
    out["state"] = LAB_READY
    out["line"] = funnel_line(raw, now)
    return out


def _broken(out: dict, why: str) -> dict:
    out.update(state=LAB_BROKEN, error=why, line=f"The lab's summary could not be read ({why}).")
    return out


def expected_run(now: dt.datetime) -> dt.date:
    """The date of the latest nightly run that should have finished by now: it
    starts at 23:10 Monday to Saturday and must be done by 06:00 next morning."""
    d = uk_date(to_utc(now))
    for _ in range(10):
        if d.weekday() < 6 and uk_midnight(d + dt.timedelta(days=1)) + dt.timedelta(hours=6) <= to_utc(now):
            return d
        d -= dt.timedelta(days=1)
    return d


def run_missed(nightly: Optional[dict], now: dt.datetime) -> bool:
    """Did last night's run not happen? (From the summary's own record of the
    run that wrote it; False when the summary says nothing about runs.)"""
    if not isinstance(nightly, dict) or not nightly:
        return False
    want = expected_run(now)
    ran = _date(nightly.get("date"))
    if ran is not None and ran >= want:
        return False
    started = _when(nightly.get("started"))
    return not (started is not None and started >= uk_midnight(want) + dt.timedelta(hours=12))


def funnel_line(sm: dict, now: dt.datetime, prices: bool = True) -> str:
    """The lab in one line: "The lab: 632 ideas tried on past prices - 14
    passed; 9 of 30 fresh looks used, 3 passed; 1 on the bench, 2 practising,
    617 binned. Last night: 632 tested in 38 min. Prices: 24 markets, 72
    complete trading days." TEST DATA in place of it all for a made-up run."""
    if sm.get("synthetic"):
        return f"The lab: {TEST_DATA} - it ran on made-up prices to check that it works."
    f = sm.get("funnel") if isinstance(sm.get("funnel"), dict) else {}
    st = sm.get("store") if isinstance(sm.get("store"), dict) else {}
    if not f:
        return " ".join(b for b in ("The lab: nothing tried yet.", last_night_words(sm.get("nightly"), now)) if b)
    tried = _int(f.get("past_tested"))
    tried = tried if tried is not None else _int(f.get("ideas"))
    bits = []
    head = "The lab: "
    if tried is not None:
        head += f"{_plural(tried, 'idea')} tried on past prices - {_count(f.get('past_passed'))} passed"
    else:
        head += "nothing tried on past prices yet"
    looks = ""
    if _int(st.get("fresh_used")) is not None:
        looks = f"{_count(st.get('fresh_used'))} of {_count(st.get('fresh_cap'))} fresh looks used"
        if _int(f.get("fresh_passed")) is not None:
            looks += f", {_count(f.get('fresh_passed'))} passed"
    where = [f"{_count(f.get('bench'))} on the bench", f"{_count(f.get('practice'))} practising"]
    for key, words in (("ready", "ready for live"), ("live", "on a live trial"), ("proven", "proven")):
        if (_int(f.get(key)) or 0) > 0:
            where.append(f"{_count(f.get(key))} {words}")
    where.append(f"{_count(f.get('binned'))} binned")
    bits.append(head + (f"; {looks}" if looks else "") + "; " + ", ".join(where) + ".")
    bits.append(last_night_words(sm.get("nightly"), now))
    if prices and _int(st.get("symbols")) is not None:
        bits.append(f"Prices: {_count(st.get('symbols'))} markets, {_count(st.get('complete_days'))} complete "
                    f"trading days.")
    return " ".join(b for b in bits if b)


def took_words(secs) -> str:
    """'38 min', 'under a minute', or '' when the run's seconds are not recorded (or 0)."""
    secs = _num(secs)
    if secs is None or secs <= 0:
        return ""
    return "under a minute" if secs < 60 else f"{round(secs / 60)} min"


def last_night_words(nightly, now: dt.datetime) -> str:
    """'Last night: 632 tested in 38 min.'; "Last night's run: missed." when it did not run."""
    if not isinstance(nightly, dict) or not nightly:
        return ""
    if run_missed(nightly, now):
        return "Last night's run: missed."
    tested, secs = _int(nightly.get("tested")), _num(nightly.get("seconds"))
    took = took_words(secs)
    took = f" in {took}" if took else ""
    if nightly.get("deadline_hit"):
        return f"Last night: ran out of time at 06:00 - {_count(tested)} tested{took}; the rest tonight."
    if nightly.get("ok") is False:
        return f"Last night's run stopped at its '{nightly.get('step') or '?'}' step - {_count(tested)} tested."
    return f"Last night: {_count(tested)} tested{took}." if tested is not None else ""


# ------------------------------------------------------ the candidates --
def candidates_of(lab: dict) -> list[dict]:
    sm = lab.get("summary") or {}
    return [c for c in sm.get("candidates") or () if isinstance(c, dict) and c.get("id")]


def find_candidate(lab: dict, cid: str) -> Optional[dict]:
    for c in candidates_of(lab):
        if str(c.get("id")) == cid:
            return c
    for b in (lab.get("summary") or {}).get("binned_recent") or ():
        if isinstance(b, dict) and str(b.get("id")) == cid:
            return {"id": b.get("id"), "name": b.get("name"), "stage": BINNED, "stage_since": b.get("when"),
                    "headline": b.get("reason") or "", "what_to_do": "Nothing - it stays binned and is not retried.",
                    "chip": {"code": "FAILED", "colour": CHIP_COLOURS["FAILED"]}}
    return None


def candidate_window(c: dict) -> Optional[dict]:
    """A candidate's last 10 UK trading days, as the lab wrote them
    (figures.last10: {trades, net_gbp, up_days, down_days, pf}); else its whole
    practice record when that record is no longer than the window and carries a
    profit factor. None when neither says it - never a guess."""
    f = c.get("figures") if isinstance(c.get("figures"), dict) else {}
    w = f.get("last10") if isinstance(f.get("last10"), dict) else None
    if w is None:
        p = c.get("practice") if isinstance(c.get("practice"), dict) else {}
        pf = f.get("pf", f.get("profit_factor"))
        if _int(p.get("day")) is not None and _int(p.get("day")) <= WINDOW_DAYS and _num(pf) is not None:
            w = {"trades": f.get("trades"), "net_gbp": f.get("overall_gbp"), "up_days": f.get("up_days"),
                 "down_days": f.get("down_days"), "pf": pf}
    if w is None:
        return None
    vals = {k: _num(w.get(k)) for k in ("trades", "net_gbp", "up_days", "down_days")}
    if any(x is None for x in vals.values()):
        return None
    pf = _num(w.get("pf", w.get("profit_factor")))
    return {"trades": int(vals["trades"]), "net": vals["net_gbp"], "up_days": int(vals["up_days"]),
            "down_days": int(vals["down_days"]), "pf": pf}


def what_to_do_of(c: dict, synthetic: bool = False) -> str:
    if synthetic:
        return SYNTHETIC_WHAT_TO_DO
    got = pounds(c.get("what_to_do") or "").strip()
    if got:
        return got
    return {ON_THE_BENCH: "Nothing - it starts practising when a place frees.",
            PRACTICE: "Watch - it is judged on its look dates.",
            READY_FOR_LIVE: "Your call: say the word and it goes live, judged after 20 live trades - or leave it "
                            "practising.",
            LIVE_TRIAL: "Keep it at its trial size - judged after 20 live trades.",
            PROVEN: "Keep and grow - sizes are your call.",
            BINNED: "Nothing - it stays binned and is not retried."}.get(str(c.get("stage") or ""), "Nothing.")


def candidate_ladder(c: dict, synthetic: bool = False) -> dict:
    """A lab candidate on the ladder (the same shape as ``bot_ladder``). Its money
    is always practice: the lab has no live path, its trades are rows in
    lab.sqlite with mode PRACTICE (a candidate given real money is wired by hand
    as a bot, and its real trades are that bot's row). Test data when the
    summary, or the idea's own entry, says so."""
    stage = str(c.get("stage") or "")
    synthetic = bool(synthetic or c.get("synthetic"))
    return {"kind": "candidate", "id": str(c.get("id")), "name": str(c.get("name") or c.get("id")), "stage": stage,
            "checks": ordered_checks(c.get("checks")), "what_to_do": what_to_do_of(c, synthetic),
            "verdict": "", "window": candidate_window(c), "trades": None, "trade_kind": "practice",
            "synthetic": synthetic, "candidate": c}


def binned_lately(c: dict, now: dt.datetime, days: int = BINNED_SHOWN_DAYS) -> bool:
    t = _when(c.get("binned_utc") or c.get("stage_since"))
    return t is not None and to_utc(now) - t <= dt.timedelta(days=days)


def ladder_candidates(lab: dict, now: dt.datetime) -> list[dict]:
    """The candidates that sit on "/": bench and beyond, and those binned in
    the last 7 days, in the summary's order."""
    if lab.get("state") != LAB_READY:
        return []
    out = []
    for c in candidates_of(lab):
        stage = str(c.get("stage") or "")
        if stage not in STAGES or stage in (IDEA, PAST_TEST, FRESH_TEST):
            continue
        if stage == BINNED and not binned_lately(c, now):
            continue
        out.append(candidate_ladder(c, lab.get("synthetic", False)))
    return out


def grouped(rows: Sequence[dict]) -> list[tuple[str, list[dict]]]:
    """[(group, rows)] in the page's order: Real money (PROVEN then LIVE TRIAL),
    Practising (READY FOR LIVE, PRACTICE, ON THE BENCH), Binned - within a
    stage the bots first (in the page's order), then the candidates. Rows
    whose stage is not known come first, on their own."""
    out = []
    unknown = [r for r in rows if not r.get("stage")]
    if unknown:
        out.append((NOT_KNOWN, unknown))
    for name, stages in GROUPS:
        mine = []
        for stage in stages:
            mine += [r for r in rows if r.get("stage") == stage and r.get("kind") == "bot"]
            mine += [r for r in rows if r.get("stage") == stage and r.get("kind") != "bot"]
        out.append((name, mine))
    return out


# --------------------------------------------------------- the standouts --
def standouts(rows: Sequence[dict], now: dt.datetime) -> list[dict]:
    """Today's standouts - the rule (deterministic; never from today's money):

    a row is eligible only when ALL of these hold - its stage is PRACTICE,
    READY FOR LIVE, LIVE TRIAL or PROVEN; no check is Failed; its luck check
    has been worked out (a row without one never stands out); it closed at
    least 10 trades in the last 10 UK trading days, net above nothing after
    costs over them, with more days up than down; and, for a bot, its own
    verdict says PROMISING or PROVEN. Never test data. The eligible are ranked
    by profit factor over those 10 days, then by trades, then by name and id
    (so the order never depends on the order they came in); at most 3, never
    padded.

    Each pick: {row, window, why, next}."""
    now = to_utc(now)
    picks = []
    for r in rows:
        if r.get("synthetic") or r.get("stage") not in STANDOUT_STAGES:
            continue
        checks = ordered_checks(r.get("checks"))
        if any(c["state"] == FAILED for c in checks):
            continue
        if not luck_computed(next((c for c in checks if c["key"] == CHECK_LUCK), None)):
            continue
        if r.get("kind") == "bot" and r.get("verdict") not in STANDOUT_VERDICTS:
            continue
        w = r.get("window")
        if w is None and r.get("trades") is not None:
            w = window_of(r["trades"], now)
        if not w:
            continue
        n, net = int(w.get("trades") or 0), _num(w.get("net"))
        up, down = int(w.get("up_days") or 0), int(w.get("down_days") or 0)
        if n < WINDOW_MIN_TRADES or net is None or net <= 0 or up <= down:
            continue
        pf = _num(w.get("pf"))
        if pf is None and r.get("kind") == "candidate":
            continue                                # the lab did not say: never ranked on a guess
        picks.append((r, w, float("inf") if pf is None else pf))
    picks.sort(key=lambda x: (-x[2], -int(x[1].get("trades") or 0), str(x[0].get("name") or ""),
                              str(x[0].get("id") or "")))
    out = []
    for r, w, _pf in picks[:STANDOUTS_MAX]:
        kind = r.get("trade_kind") or ""
        luck = next((c for c in ordered_checks(r.get("checks")) if c["key"] == CHECK_LUCK), {})
        # the rule lets in a luck check that is worked out but not passed yet; the words never
        # say it passed
        tail = ("every check it has reached passed." if luck.get("state") == PASSED else
                "no check has failed; its luck check is worked out but not passed yet.")
        out.append({"row": r, "window": w, "next": r.get("what_to_do") or "",
                    "why": (f"Ahead {money(w['net'])} after costs over the last {WINDOW_DAYS} trading days: "
                            f"{_kind_trades(kind, int(w['trades']))}, {_plural(int(w['up_days']), 'day')} up, "
                            f"{int(w['down_days'])} down; {tail}")})
    return out


# ------------------------------------------------------ what changed --
def changes(lab: dict, data_dir, now: dt.datetime) -> list[dict]:
    """What changed in the last 24 hours, newest first, at most 5: the lab's
    own events (summary.json "changes_24h") and the bots' mode changes that the
    modes files date - Formula 1 going live (config.json tnb.live_since_utc).
    The other modes files hold no time, so a change to them is never claimed.
    [{when, text}]."""
    now = to_utc(now)
    out = []
    sm = lab.get("summary") or {}
    if lab.get("state") in (LAB_READY, LAB_COLLECTING):
        if lab.get("synthetic"):
            if sm.get("changes_24h"):
                out.append({"when": _when(sm.get("written_utc")) or now, "text": f"The lab: {TEST_DATA}."})
        else:
            for c in sm.get("changes_24h") or ():
                if not isinstance(c, dict):
                    continue
                when = _when(c.get("when"))
                if when is not None and now - when > dt.timedelta(hours=CHANGES_HOURS):
                    continue
                name = str(c.get("name") or c.get("id") or "The lab")
                out.append({"when": when, "text": f"{name}: {pounds(c.get('text') or '')}".rstrip(": ")})
    try:
        live = V.tnb_live_setting(data_dir) if data_dir else {"groups": (), "since": None}
    except Exception:                                    # never a reason to fail the page
        live = {"groups": (), "since": None}
    since = live.get("since")
    if since is not None and dt.timedelta(0) <= now - since <= dt.timedelta(hours=CHANGES_HOURS):
        out.append({"when": since, "text": f"Trend & Breakout: its {V.live_kinds(live['groups'], live.get('tactics'))} "
                                           f"went LIVE with real money (config.json tnb.live_since_utc)."})
    out.sort(key=lambda x: x["when"] or dt.datetime.min.replace(tzinfo=dt.timezone.utc), reverse=True)
    return out[:CHANGES_MAX]


# ------------------------------------------------- one candidate in words --
def what_happened(c: dict, synthetic: bool = False) -> list[str]:
    """What happened to a candidate, a sentence per step, from the figures the
    lab wrote (its past test, its one fresh look, its practice)."""
    if synthetic:
        return [TEST_DATA]
    out = []
    tests = c.get("tests") if isinstance(c.get("tests"), dict) else {}
    for kind, lead in (("past", "On old prices (computer test"), ("fresh", "Once on prices it had never seen (one go")):
        t = tests.get(kind) if isinstance(tests.get(kind), dict) else None
        if not t or _num(t.get("trades")) is None or _num(t.get("net_gbp")) is None:
            continue
        when = span_words(t.get("window_from"), t.get("window_to"))
        head = f"{lead}, {when})" if when else f"{lead})"
        bits = [f"{money(t['net_gbp'])} after costs over {_plural(int(_num(t['trades'])), 'trade')}"]
        if _int(t.get("up_days")) is not None and _int(t.get("down_days")) is not None:
            bits.append(f"{_int(t['up_days'])} days up, {_int(t['down_days'])} down")
        if _num(t.get("pf")) is not None:
            bits.append(f"won £{float(t['pf']):.2f} for every £1 lost")
        out.append(f"{head}: " + "; ".join(bits) + ".")
    p = c.get("practice") if isinstance(c.get("practice"), dict) else {}
    if p:
        n = _int(p.get("trades"))
        since = day_words(p.get("since"))
        if n:
            out.append(f"In practice (live prices, no money) since {since or 'it started'}: "
                       f"{money(p.get('net_gbp'))} after costs over {_plural(n, 'practice trade')}"
                       + (f", day {_count(p.get('day'))} of practice" if _int(p.get("day")) else "") + ".")
        elif since:
            out.append(f"Practising since {since}: no practice trades yet.")
        looks = [f"look {k} on {day_words(p.get(f'look{k}_due'))}" for k in (1, 2) if day_words(p.get(f"look{k}_due"))]
        if looks:
            out.append("It is judged at " + " and ".join(looks) + "; nothing is judged on any other day.")
    if not out:
        out.append(pounds(c.get("headline") or "") or "Nothing on record yet.")
    return out


def family_note(f: dict) -> str:
    """The family's note on /lab: dormant, no number to tune, settings at the edge."""
    bits = []
    if f.get("dormant"):
        bits.append("waiting for enough news history")
    if f.get("no_numeric_setting"):
        bits.append("no number to tune: it relies on the fresh test alone")
    if f.get("extend_grid"):
        bits.append(f"{len(f['extend_grid'])} at the edge of the range tried (held back)")
    return "; ".join(bits) or "-"


# ------------------------------------------------------- /lab in words --
SMALL_EDGE = ("An idea with a small edge, or one that trades rarely, shows 'too few trades' rather than a pass; "
              "it gets one more try on the next stretch.")


def coverage_line(sm: dict) -> str:
    """'Prices: 24 markets, 72 complete trading days (15 Jun-8 Oct 2026); the lab needs 40 to start.'"""
    st = sm.get("store") if isinstance(sm.get("store"), dict) else {}
    if _int(st.get("symbols")) is None and _int(st.get("complete_days")) is None:
        return "Prices: the lab has not said what it holds."
    when = span_words(st.get("first_day"), st.get("last_day"))
    need = _int(st.get("needed_days")) or NEEDED_DAYS
    out = (f"Prices: {_count(st.get('symbols'))} markets, {_count(st.get('complete_days'))} complete trading days"
           + (f" ({when})" if when else "") + f"; the lab needs {need} to start.")
    have = _int(st.get("complete_days"))
    if have is not None and need <= have < SHORT_HISTORY_DAYS:
        out += f" Short history: results are weaker (fewer than {SHORT_HISTORY_DAYS} days of past prices)."
    return out


def _ends(v) -> tuple:
    if isinstance(v, (list, tuple)) and v:
        return v[0], v[-1]
    return None, None


def fresh_line(sm: dict) -> str:
    """'14 Sep-9 Oct 2026 is kept back and never used for choosing; 9 of 30 looks used.'"""
    st = sm.get("store") if isinstance(sm.get("store"), dict) else {}
    fresh = span_words(*_ends(st.get("fresh")))
    past = span_words(*_ends(st.get("past")))
    out = []
    if fresh:
        out.append(f"{fresh} is kept back and never used for choosing; {_count(st.get('fresh_used'))} of "
                   f"{_count(st.get('fresh_cap'))} looks used (each idea gets one look, ever).")
    if past:
        out.append(f"Ideas are tried and chosen on {past} only.")
    if st.get("news_from"):
        out.append(f"News history from {day_words(st.get('news_from'), True)}.")
    return " ".join(out)


def honest_line(sm: dict) -> str:
    """'632 ideas tried; the best of 632 always looks good, so every result is judged against that.'"""
    tr = sm.get("trials") if isinstance(sm.get("trials"), dict) else {}
    f = sm.get("funnel") if isinstance(sm.get("funnel"), dict) else {}
    n = _int(tr.get("lab"))
    n = n if n is not None else _int(f.get("ideas"))
    if n is None:
        return ""
    return f"{n:,} ideas tried; the best of {n:,} always looks good, so every result is judged against that."


CONTROL = "CONTROL_TIME_OF_DAY"


def control_line(sm: dict) -> str:
    """How often the clock-trade control (no idea behind it) passed: the gates' own check."""
    tr = sm.get("trials") if isinstance(sm.get("trials"), dict) else {}
    ctl = tr.get("control") if isinstance(tr.get("control"), dict) else {}
    fam = next((x for x in sm.get("families") or () if isinstance(x, dict) and x.get("code") == CONTROL), {})
    n, k = _int(ctl.get("past_tested")), _int(ctl.get("past_passed"))
    rate = _num(ctl.get("past_pass_rate"))
    if n is None or k is None:                          # only the rate: of the family's grid
        n = _int(fam.get("n")) or _int((tr.get("by_family") or {}).get(CONTROL)
                                       if isinstance(tr.get("by_family"), dict) else None)
        k = int(round(rate * n)) if rate is not None and n else None
    if not n or k is None:
        return "The clock-trade control (buys or sells at a fixed hour, no idea behind it) has not been tested yet."
    out = f"The clock-trade control passed the past test {k} times in {n}: "
    out += ("the gates are not letting noise through." if k == 0 else
            "noise gets through the past test now and then, which is why only the fresh test counts.")
    fr = _num(ctl.get("fresh_pass_rate"))
    if fr is not None:
        out += f" On fresh prices it passed {fr:.0%} of its looks."
    return out


def practice_now_line(lab: dict, now: dt.datetime) -> str:
    """The practice process right now, from data/lab-status.json."""
    st = lab.get("status")
    if (lab.get("summary") or {}).get("paused"):
        return "The lab is paused: its mode is OFF (data/lab.json)."
    if not isinstance(st, dict):
        why = lab.get("status_error") or "missing"
        return f"The practice process has not reported (data/lab-status.json: {why})."
    if str(st.get("mode") or "").upper() == "OFF":
        return "The lab is paused: its mode is OFF (data/lab.json)."
    roster = [r for r in st.get("roster") or () if isinstance(r, dict)]
    age = None
    t = _when(st.get("updated_utc"))
    if t is not None:
        age = max(0, int((to_utc(now) - t).total_seconds()))
    out = f"Practice right now: {_plural(len(roster), 'idea')} on live prices, no money"
    if st.get("feed_ok") is False:
        out += "; its prices are NOT coming in"
    elif st.get("feed_ok"):
        out += "; prices are coming in"
    out += "."
    if age is not None:
        out += f" Reported {age:,} seconds ago" + (" - stale: the watchdog restarts it." if age > 180 else ".")
    if st.get("last_error"):
        out += f" Last problem: {st.get('last_error')}."
    return out
