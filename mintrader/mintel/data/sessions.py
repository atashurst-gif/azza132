"""Session intelligence and opening ranges.

A session is *context*, never a trade trigger.  What this module produces is a
set of levels and behavioural facts (which session we are in, what Asia did,
where London's high sits, whether the opening range has been accepted or
rejected) that the tactic layer then interprets.

All windows come from :mod:`mintel.clock`, which uses real exchange timezones,
so daylight-saving transitions are handled by the tz database.  The operator's
machine clock is never consulted.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Optional, Sequence

from ..broker.base import Bar
from ..clock import SESSIONS, active_sessions, to_utc
from .series import atr


@dataclass
class SessionRange:
    name: str
    start: dt.datetime
    end: dt.datetime
    high: float
    low: float
    open_price: float
    close_price: float
    bars: int
    complete: bool

    @property
    def width(self) -> float:
        return self.high - self.low

    @property
    def direction(self) -> int:
        if self.close_price > self.open_price:
            return 1
        if self.close_price < self.open_price:
            return -1
        return 0


@dataclass
class OpeningRange:
    minutes: int
    start: dt.datetime
    high: float
    low: float
    complete: bool
    broke_up: bool = False
    broke_down: bool = False
    accepted: bool = False       # closed and held beyond
    reclaimed: bool = False      # broke then closed back inside -> failed break

    @property
    def width(self) -> float:
        return self.high - self.low


@dataclass
class SessionReading:
    as_of: dt.datetime
    active: tuple[str, ...]
    ranges: dict[str, SessionRange]
    opening_ranges: dict[int, OpeningRange]
    prev_day_high: Optional[float]
    prev_day_low: Optional[float]
    prev_session_high: Optional[float]
    prev_session_low: Optional[float]
    day_open: Optional[float]
    travelled_since_london_open: Optional[float]
    is_overlap: bool

    def levels(self) -> list[tuple[str, float]]:
        out: list[tuple[str, float]] = []
        for name, r in self.ranges.items():
            out.append((f"{name}_HIGH", r.high))
            out.append((f"{name}_LOW", r.low))
        if self.prev_day_high is not None:
            out.append(("PDH", self.prev_day_high))
        if self.prev_day_low is not None:
            out.append(("PDL", self.prev_day_low))
        for m, orr in self.opening_ranges.items():
            out.append((f"OR{m}_HIGH", orr.high))
            out.append((f"OR{m}_LOW", orr.low))
        return out

    def liquidity_score(self) -> float:
        """How much participation the current clock implies, 0..100.

        Used to scale opportunity, not to gate it: a genuine catalyst in a thin
        Asian session is still tradable, just with a wider execution allowance.
        """
        if "LONDON_NY_OVERLAP" in self.active:
            return 100.0
        if "US_CASH" in self.active:
            return 92.0
        if "LONDON" in self.active:
            return 85.0
        if "NEWYORK" in self.active:
            return 80.0
        if "ASIA" in self.active:
            return 55.0
        return 30.0


def _slice(bars: Sequence[Bar], start: dt.datetime,
           end: dt.datetime) -> list[Bar]:
    return [b for b in bars if start <= b.time < end]


def build_session_range(bars: Sequence[Bar], name: str, as_of: dt.datetime,
                        previous: bool = False) -> Optional[SessionRange]:
    win = SESSIONS.get(name)
    if win is None:
        return None
    o, c = win.bounds_for(as_of)
    if previous:
        o -= dt.timedelta(days=1)
        c -= dt.timedelta(days=1)
        # Skip the weekend hole so "previous session" means the previous
        # *trading* session, not an empty Sunday.
        for _ in range(3):
            if _slice(bars, o, c):
                break
            o -= dt.timedelta(days=1)
            c -= dt.timedelta(days=1)
    seg = _slice(bars, o, c)
    if not seg:
        return None
    return SessionRange(
        name=name, start=o, end=c,
        high=max(b.high for b in seg), low=min(b.low for b in seg),
        open_price=seg[0].open, close_price=seg[-1].close, bars=len(seg),
        complete=as_of >= c)


def build_opening_range(bars: Sequence[Bar], session: str, minutes: int,
                        as_of: dt.datetime) -> Optional[OpeningRange]:
    """Opening range for ``session``, then how price has treated it.

    ``accepted`` requires a *close* beyond the range plus the following bar
    holding beyond it.  A single wick outside is neither a breakout nor a
    failure - it is noise, and is reported as such.
    """
    win = SESSIONS.get(session)
    if win is None:
        return None
    o, _ = win.bounds_for(as_of)
    end = o + dt.timedelta(minutes=minutes)
    seg = _slice(bars, o, end)
    if not seg:
        return None
    hi = max(b.high for b in seg)
    lo = min(b.low for b in seg)
    after = _slice(bars, end, as_of + dt.timedelta(seconds=1))
    orr = OpeningRange(minutes=minutes, start=o, high=hi, low=lo,
                       complete=as_of >= end)
    if not after:
        return orr
    closes_above = [b for b in after if b.close > hi]
    closes_below = [b for b in after if b.close < lo]
    orr.broke_up = bool(closes_above)
    orr.broke_down = bool(closes_below)
    last = after[-1]
    if closes_above and last.close > hi:
        idx = after.index(closes_above[0])
        held = all(b.close > lo for b in after[idx:])
        orr.accepted = held
    elif closes_below and last.close < lo:
        idx = after.index(closes_below[0])
        held = all(b.close < hi for b in after[idx:])
        orr.accepted = held
    if (closes_above or closes_below) and lo <= last.close <= hi:
        orr.reclaimed = True      # broke out, came back in -> failed break
    return orr


def previous_day_levels(bars: Sequence[Bar],
                        as_of: dt.datetime) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """(prev day high, prev day low, today's open) using UTC calendar days."""
    as_of = to_utc(as_of)
    today = as_of.date()
    by_day: dict[dt.date, list[Bar]] = {}
    for b in bars:
        by_day.setdefault(b.time.date(), []).append(b)
    days = sorted(d for d in by_day if d < today)
    prev = by_day[days[-1]] if days else None
    today_bars = by_day.get(today)
    return (max(b.high for b in prev) if prev else None,
            min(b.low for b in prev) if prev else None,
            today_bars[0].open if today_bars else None)


def read(bars_m15: Sequence[Bar], bars_m1: Sequence[Bar],
         as_of: dt.datetime,
         opening_minutes: Sequence[int] = (1, 3, 5, 15, 30)) -> SessionReading:
    """Assemble the full session picture.

    ``bars_m1`` is used for opening ranges (a 1-minute opening range cannot be
    measured on 15-minute bars); ``bars_m15`` for session ranges and day levels.
    """
    as_of = to_utc(as_of)
    act = active_sessions(as_of)
    ranges: dict[str, SessionRange] = {}
    for name in ("ASIA", "LONDON", "NEWYORK", "US_CASH"):
        r = build_session_range(bars_m15, name, as_of)
        if r:
            ranges[name] = r

    current = next((n for n in ("LONDON_NY_OVERLAP", "US_CASH", "NEWYORK",
                                "LONDON", "ASIA") if n in act), "LONDON")
    base_session = "LONDON" if current == "LONDON_NY_OVERLAP" else current
    prev = build_session_range(bars_m15, base_session, as_of, previous=True)

    ors: dict[int, OpeningRange] = {}
    src = bars_m1 or bars_m15
    for m in opening_minutes:
        orr = build_opening_range(src, base_session, m, as_of)
        if orr:
            ors[m] = orr

    pdh, pdl, day_open = previous_day_levels(bars_m15, as_of)
    travelled = None
    lon = ranges.get("LONDON")
    if lon is not None:
        travelled = abs(lon.close_price - lon.open_price)

    return SessionReading(
        as_of=as_of, active=act, ranges=ranges, opening_ranges=ors,
        prev_day_high=pdh, prev_day_low=pdl,
        prev_session_high=prev.high if prev else None,
        prev_session_low=prev.low if prev else None,
        day_open=day_open, travelled_since_london_open=travelled,
        is_overlap="LONDON_NY_OVERLAP" in act)
