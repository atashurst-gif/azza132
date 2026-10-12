"""Filling the store from the broker's M1 history, gently.

The lab asks the price feed for old minute bars, never for anything else, and
only in the quiet hours (Europe/London): weekday nights 23:10 to 06:00 (a
30 minute budget) and Saturday 00:00 to Sunday 20:00 (90 minutes), so the
bridge the live bots depend on is never slowed in the day. One page a second at
most (``pace_s``).

Each run is:

1. A forward top-up per symbol: re-fetch the last three UTC days so any gap
   heals, and everything since. A top-up that is cut short (or a month dropped as
   damaged) is remembered in the symbol's index and reaches back that far next time,
   so a hole never becomes permanent.
2. A backward backfill, a page (3,000 bars) at a time, round-robin across the
   symbols so they all deepen together, until ``history_days`` complete trading
   days are held, an empty page comes back (``history_exhausted``: the broker
   has no more), the budget is spent, the window closes, or the store is full.

The feed is duck-typed: ``bars(symbol, tf, count, end=None)``, ``symbols()``,
``spec(symbol)`` and ``ping()``. M1 depth at IC Markets is UNVERIFIED, so the
first run logs ``depth_days`` for every symbol: that is where it becomes a
number. One timeout is retried once after 8 seconds, then the night stops.

Nothing a page says is allowed to overwrite good prices with prices that disagree
with them. Every two-hour chunk of a page that overlaps what is stored is compared
with it first; if more than 2% of the overlapping minutes differ (a broker moving its
clock by an hour moves every stamp, and the bridge converts old bars with the CURRENT
clock offset, so the first top-up after a change brings the last three days back an
hour out), that chunk is left out, counted in the report, and the night carries on
with the rest.

Sleep and clock are injectable, so the tests drive it with a fake clock.
"""
from __future__ import annotations

import datetime as dt
import time
from dataclasses import dataclass, field
from typing import Callable, Optional, Sequence

from ..broker.base import TF
from ..clock import TZ_LONDON
from . import HISTORY_DAYS, add_days, day_min, epoch_min, min_to_day, minute_to_dt
from .store import BarStore, StoreError, bar_row

NIGHT_BUDGET_S = 30 * 60
WEEKEND_BUDGET_S = 90 * 60
RETRY_WAIT_S = 8.0
FORWARD_OVERLAP_DAYS = 3
SIZE_CHECK_EVERY = 25                 # calls between looks at the store's size
CHUNK_MIN = 120                       # a page is checked against the store in chunks of this many minutes
CLASH_MIN_COMPARED = 30               # fewer overlapping minutes than this prove nothing
CLASH_SHARE = 0.02                    # more than this share differing = the page disagrees
CLASH_MIN_DIFFERING = 3               # ... and at least this many minutes (a still-forming bar)
CLASH_MIN_MOVING = 5                  # an echo needs this many minutes with a real range (not a flat line)


def window_kind(now: dt.datetime) -> Optional[str]:
    """'weeknight', 'weekend' or None for a moment in Europe/London.

    Weekday nights run from 23:10 to 06:00 the next morning (Tuesday to Friday
    mornings belong to the night before); Saturday from 00:00 to Sunday 20:00
    is open all the time. Sunday evening and Monday morning are not."""
    local = now.astimezone(TZ_LONDON)
    wd, hm = local.weekday(), local.hour * 60 + local.minute
    if wd == 5 or (wd == 6 and hm < 20 * 60):
        return "weekend"
    if wd <= 4 and hm >= 23 * 60 + 10:
        return "weeknight"
    if 1 <= wd <= 4 and hm < 6 * 60:
        return "weeknight"
    return None


def in_window(now: dt.datetime) -> bool:
    return window_kind(now) is not None


def window_budget_s(now: dt.datetime) -> int:
    k = window_kind(now)
    return WEEKEND_BUDGET_S if k == "weekend" else NIGHT_BUDGET_S if k else 0


def is_synthetic_feed(feed, ping_info=None) -> bool:
    """A feed that says it is made up (``synthetic`` attribute), is the simulated
    broker itself, or answers ``ping()`` with a simulated backend (a bridge run with
    ``--backend sim`` names it there and nowhere else): whatever it fills is
    stamped SYNTHETIC."""
    if getattr(feed, "synthetic", False):
        return True
    if isinstance(ping_info, dict) and "sim" in str(ping_info.get("backend", "")).lower():
        return True
    for obj in (feed, getattr(feed, "broker", None), getattr(feed, "inner", None),
                getattr(feed, "_broker", None)):
        if obj is not None and type(obj).__name__ == "SimBroker":
            return True
    return False


@dataclass
class FetchReport:
    skipped: str = ""                 # why nothing was fetched at all
    stopped: str = ""                 # why the run ended early
    window: str = ""
    pages: int = 0
    bars_added: int = 0
    seconds: float = 0.0
    topped_up: list = field(default_factory=list)
    exhausted: list = field(default_factory=list)
    depth_days: dict = field(default_factory=dict)
    errors: list = field(default_factory=list)
    repaired: dict = field(default_factory=dict)
    clashes: dict = field(default_factory=dict)   # symbol -> {from, to, minutes, note}: prices left out

    @property
    def ok(self) -> bool:
        return not self.skipped and not self.errors


class _Stop(Exception):
    def __init__(self, why: str):
        super().__init__(why)
        self.why = why


def _guarded(store: BarStore, sym: str, cols, rep: "FetchReport", say):
    """Which rows of a page may be merged: ``True`` for all of them, else the list of
    their positions. The page is cut into two-hour chunks and each is held up against
    the stored prices.

    * A chunk whose minutes match the stored ones an hour later or earlier (30 or more
      of them, 9 in 10 identical, some of them moving) is an ECHO: the broker's clock
      changed, and the bridge converts old bars with the current offset. The echoed
      minutes are dropped (they would overwrite good prices, or, past the last stored
      bar, add a stretch of mislabelled ones); minutes in the same chunk that are new
      are kept.
    * Any other chunk where at least 3 overlapping minutes and more than 2% of them
      differ is left out whole.

    Both are counted in the report."""
    t, o, h, l, c = cols[:5]
    n = len(t)
    if n == 0:
        return []
    if not store.months(sym):
        return True                                   # nothing stored: nothing to disagree with
    now_ = store.overlap(sym, t, o, h, l, c)
    echo = {s: store.overlap(sym, t, o, h, l, c, shift=s) for s in (60, -60)}
    chunks: dict = {}
    for i in range(n):
        chunks.setdefault(int(t[i]) // CHUNK_MIN, []).append(i)
    drop: set = set()

    def note_clash(rows, why):
        d0, d1 = min_to_day(min(t[i] for i in rows)), min_to_day(max(t[i] for i in rows))
        prev = rep.clashes.get(sym)
        if prev is None:
            rep.clashes[sym] = {"from": d0, "to": d1, "minutes": len(rows), "note": why}
        else:
            prev["from"], prev["to"] = min(prev["from"], d0), max(prev["to"], d1)
            prev["minutes"] += len(rows)
            if why not in prev["note"]:
                prev["note"] += "; " + why
        say(f"fetch: {sym}: {len(rows)} minutes ({d0} to {d1}) left out ({why})")

    for key in sorted(chunks):
        idx = chunks[key]
        found = False
        for shift in (60, -60):
            e = echo[shift]
            seen = [i for i in idx if e[i] is not None]
            same = [i for i in seen if e[i]]
            moving = sum(1 for i in same if h[i] > l[i])
            if (len(seen) >= CLASH_MIN_COMPARED and len(same) >= 0.9 * len(seen)
                    and moving >= CLASH_MIN_MOVING):
                rows = [i for i in idx if e[i] or now_[i] is False]
                drop.update(rows)
                note_clash(rows, "every minute is an hour " + ("late" if shift > 0 else "early")
                           + ": the broker's clock changed")
                found = True
                break
        if found:
            continue
        compared = sum(1 for i in idx if now_[i] is not None)
        differing = sum(1 for i in idx if now_[i] is False)
        if (compared >= CLASH_MIN_COMPARED and differing >= CLASH_MIN_DIFFERING
                and differing > CLASH_SHARE * compared):
            drop.update(idx)
            note_clash(idx, f"{differing} of {compared} overlapping minutes differ from the stored ones")
    if not drop:
        return True
    return [i for i in range(n) if i not in drop]


def update(feed, store: BarStore, *, now: dt.datetime, budget_s: Optional[float] = None,
           pace_s: float = 1.0, page: int = 3000, log: Optional[Callable] = None,
           sleep: Callable = time.sleep, clock: Callable = time.monotonic,
           symbols: Optional[Sequence[str]] = None, history_days: int = HISTORY_DAYS,
           max_mb: int = 300, enforce_window: bool = True,
           source: Optional[str] = None) -> FetchReport:
    """One run of the fetch (nightly step 2). Returns a ``FetchReport``."""
    rep = FetchReport()
    say = log or (lambda *_: None)
    t0 = clock()
    if enforce_window and not in_window(now):
        rep.skipped = "outside the fetch window"
        return rep
    rep.window = window_kind(now) or "any time"
    budget = float(budget_s if budget_s is not None else (window_budget_s(now) or NIGHT_BUDGET_S))
    try:
        info = feed.ping()
    except Exception as e:                            # a feed that cannot even ping
        say(f"fetch: ping failed: {e}")
        info = None
    if not info:
        rep.skipped = "the price feed is not connected"
        return rep
    src = source or ("SYNTHETIC" if is_synthetic_feed(feed, info) else "bridge")
    # the symbols to fill: the universe the caller names, else the ones already held
    # (never "everything the broker lists")
    names = list(symbols) if symbols is not None else list(store.symbols())
    last_call = [None]
    calls = [0]

    def wall() -> dt.datetime:
        return now + dt.timedelta(seconds=clock() - t0)

    def check() -> None:
        if clock() - t0 >= budget:
            raise _Stop("the time budget is spent")
        if enforce_window and not in_window(wall()):
            raise _Stop("the fetch window closed")

    def fetch_page(symbol: str, end: Optional[dt.datetime]):
        """One paced call; a timeout is retried once after 8 seconds."""
        check()
        for attempt in (1, 2):
            if last_call[0] is not None:
                gap = pace_s - (clock() - last_call[0])
                if gap > 0:
                    sleep(gap)
                    check()                  # waiting can use up the budget or close the window
            last_call[0] = clock()
            calls[0] += 1
            try:
                rows = feed.bars(symbol, TF.M1, page, end)
                rep.pages += 1
                return rows
            except Exception as e:
                say(f"fetch: {symbol}: {type(e).__name__}: {e}")
                if attempt == 2:
                    raise _Stop("the feed timed out twice")
                sleep(RETRY_WAIT_S)
                check()
        raise _Stop("unreachable")

    ctx: dict = {}
    for sym in names:
        try:
            spec = feed.spec(sym)
        except Exception as e:
            rep.errors.append(f"{sym}: no specification ({e})")
            continue
        if spec is None or not float(getattr(spec, "point", 0) or 0):
            rep.errors.append(f"{sym}: no specification")
            continue
        torn = store.repair(sym)
        if torn:
            rep.repaired[sym] = torn
            say(f"fetch: {sym}: dropped damaged months {torn}")
        ctx[sym] = (float(spec.point), int(spec.digits))

    def merge(sym: str, rows) -> int:
        """Merge a page, except the stretches that disagree with what is stored."""
        point, digits = ctx[sym]
        cols = ([], [], [], [], [], [], [])
        for b in rows:
            row = bar_row(b, point)
            if row is None:                           # a corrupt bar is never data
                continue
            for col, x in zip(cols, row):
                col.append(x)
        keep = _guarded(store, sym, cols, rep, say)
        if not keep:
            return 0
        t, o, h, l, c, s_, v = (col if keep is True else [col[i] for i in keep] for col in cols)
        r = store.merge_columns(sym, t, o, h, l, c, s_, v, point=point, digits=digits,
                                source=src, now=wall())
        rep.bars_added += r.added
        return r.added

    try:
        # 1. forward top-up: the newest page, then back until the 3-day overlap is covered
        for sym in names:
            if sym not in ctx:
                continue
            last = store.last_min(sym)
            need_from = None if last is None else day_min(add_days(min_to_day(last),
                                                                   -(FORWARD_OVERLAP_DAYS - 1)))
            if need_from is not None:
                # A top-up cut short by a timeout or the budget, or a month dropped as damaged,
                # must still be reached next time: the newest page moves last_min on, so the
                # old anchor is kept in the index until a top-up gets all the way back to it.
                idx = store.index(sym)
                for mark in (idx.get("topup_from"), idx.get("heal_from")):
                    if mark is not None:
                        need_from = min(need_from, int(mark))
                store.set_flag(sym, topup_from=need_from)
            end = None
            while True:
                rows = fetch_page(sym, end)
                if not rows:
                    break
                merge(sym, rows)
                oldest = epoch_min(rows[0].time)
                if need_from is None or oldest <= need_from or len(rows) < 2:
                    break
                end = minute_to_dt(oldest - 1)
            if need_from is not None or store.index(sym).get("heal_from") is not None:
                # an empty store has just been given its newest page; the backfill refills the rest
                store.set_flag(sym, topup_from=None, heal_from=None)
            rep.topped_up.append(sym)
        # 2. backward backfill, round-robin, until deep enough or told to stop
        active = [s for s in names if s in ctx and not store.index(s).get("history_exhausted")
                  and len(store.complete_days(s)) < history_days]
        while active:
            still = []
            for sym in active:
                if calls[0] % SIZE_CHECK_EVERY == 0 and store.bytes_used() > max_mb * 1024 * 1024:
                    raise _Stop("the store is full")
                first = store.first_min(sym)
                end = minute_to_dt(first - 1) if first is not None else None
                rows = fetch_page(sym, end)
                added = merge(sym, rows) if rows else 0
                now_first = store.first_min(sym)
                if not rows or added == 0 or (first is not None and now_first >= first):
                    store.set_flag(sym, history_exhausted=True)
                    rep.exhausted.append(sym)
                    continue
                if len(store.complete_days(sym)) >= history_days:
                    continue
                still.append(sym)
            active = still
    except _Stop as stop:
        rep.stopped = stop.why
        say(f"fetch: stopped: {stop.why}")
    except StoreError as e:
        rep.errors.append(str(e))
        rep.stopped = "the store refused a page"
    for sym, cl in sorted(rep.clashes.items()):
        rep.errors.append(
            f"{sym}: {cl['minutes']} minutes of prices ({cl['from']} to {cl['to']}) disagree with "
            f"the stored ones ({cl['note']}) and were left out, so the stored prices are untouched")
    for sym in ctx:
        n = len(store.complete_days(sym))
        rep.depth_days[sym] = n
        store.set_flag(sym, depth_days=n)
        say(f"fetch: depth_days {sym} {n}")
    rep.seconds = clock() - t0
    return rep
