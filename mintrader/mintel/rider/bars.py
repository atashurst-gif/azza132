"""Bars and session levels built from ticks, so a replay needs only ticks.

* :class:`BarSeries` - M1 / M5 (any whole-second timeframe) OHLC bars of the
  MID price, with tick volume = the number of ticks and the mean spread.
  Live may seed it with the broker's bars before the first tick (MetaTrader
  bars are bid bars: a half-spread offset that rolls out within hours).
* :class:`SessionTracker` - the Asian, London and New York session ranges,
  the London and New York opening ranges (1/3/5/15/30 minutes) and the
  previous trading day's high and low (the FX day rolls at 17:00 New York).

No look-ahead: a bar is "closed" only once its end time has passed, and a
level is only known from prices already seen.
"""
from __future__ import annotations

import datetime as dt
from collections import deque
from dataclasses import dataclass, field
from typing import Iterable, Optional

from ..broker.base import Bar
from ..clock import SESSIONS, TZ_NEWYORK

UTC = dt.timezone.utc
OPENING_MINUTES = (1, 3, 5, 15, 30)
TRACKED_SESSIONS = ("ASIA", "LONDON", "NEWYORK")
OPENING_SESSIONS = ("LONDON", "NEWYORK")


def _epoch(ts: dt.datetime) -> float:
    return ts.timestamp()


class BarSeries:
    """Bars of one timeframe for one symbol."""

    def __init__(self, seconds: int, keep: int = 400):
        self.seconds = int(seconds)
        self.closed_bars: deque[Bar] = deque(maxlen=int(keep))
        self._cur: Optional[list] = None            # [start_epoch, o, h, l, c, n, spread_sum]
        self.version = 0                            # bumps whenever a bar closes (cache key)

    def _close_current(self) -> None:
        c = self._cur
        if c is None:
            return
        n = max(1, int(c[5]))
        self.closed_bars.append(Bar(dt.datetime.fromtimestamp(c[0], UTC), c[1], c[2], c[3], c[4],
                                    float(c[5]), 0.0, c[6] / n))
        self._cur = None
        self.version += 1

    def add(self, t_epoch: float, price: float, spread: float = 0.0) -> None:
        start = (int(t_epoch) // self.seconds) * self.seconds
        c = self._cur
        if c is not None and start != c[0]:
            if start < c[0]:
                return                               # out of order: ignored (the core never sends these)
            self._close_current()
            c = None
        if c is None:
            if self.closed_bars and start <= _epoch(self.closed_bars[-1].time):
                return                               # inside a seeded bar already closed
            self._cur = [start, price, price, price, price, 1, spread]
            return
        if price > c[2]:
            c[2] = price
        if price < c[3]:
            c[3] = price
        c[4] = price
        c[5] += 1
        c[6] += spread

    def seed(self, bars: Iterable[Bar], before_epoch: Optional[float] = None) -> int:
        """Closed broker bars before any tick (live only). Returns how many."""
        n = 0
        last = _epoch(self.closed_bars[-1].time) if self.closed_bars else -1e18
        for b in sorted(bars, key=lambda x: x.time):
            t0 = _epoch(b.time)
            if t0 <= last:
                continue
            if before_epoch is not None and t0 + self.seconds > before_epoch:
                continue                             # still forming at the broker: never seeded
            if self._cur is not None and t0 >= self._cur[0]:
                continue
            self.closed_bars.append(b)
            last = t0
            n += 1
        if n:
            self.version += 1
        return n

    def closed(self, now_epoch: float) -> list[Bar]:
        """Every bar whose end time has passed by ``now``."""
        out = list(self.closed_bars)
        c = self._cur
        if c is not None and c[0] + self.seconds <= now_epoch:
            n = max(1, int(c[5]))
            out.append(Bar(dt.datetime.fromtimestamp(c[0], UTC), c[1], c[2], c[3], c[4], float(c[5]), 0.0, c[6] / n))
        return out

    def forming(self, now_epoch: float) -> Optional[Bar]:
        c = self._cur
        if c is None or c[0] + self.seconds <= now_epoch:
            return None
        return Bar(dt.datetime.fromtimestamp(c[0], UTC), c[1], c[2], c[3], c[4], float(c[5]), 0.0, c[6] / max(1, c[5]))

    def key(self, now_epoch: float) -> tuple:
        c = self._cur
        done = c is not None and c[0] + self.seconds <= now_epoch
        return (self.version, done, len(self.closed_bars))


@dataclass
class _Range:
    open_epoch: float
    close_epoch: float
    hi: float = float("-inf")
    lo: float = float("inf")

    def add(self, hi: float, lo: float) -> None:
        if hi > self.hi:
            self.hi = hi
        if lo < self.lo:
            self.lo = lo

    @property
    def known(self) -> bool:
        return self.hi > float("-inf") and self.lo < float("inf")


@dataclass
class SessionLevels:
    """What the session tracker knows at a moment, for the features."""
    active: tuple[str, ...] = ()
    ranges: dict = field(default_factory=dict)        # name -> (hi, lo, complete)
    opening: dict = field(default_factory=dict)       # (name, minutes) -> (hi, lo, complete, open_epoch)
    prev_day: Optional[tuple[float, float]] = None
    day: Optional[tuple[float, float]] = None
    minutes_since_open: dict = field(default_factory=dict)


class SessionTracker:
    def __init__(self):
        self._bounds_cache: dict[tuple[str, int], tuple[float, float]] = {}
        self._day_cache: dict[int, dt.date] = {}
        self.current: dict[str, _Range] = {}
        self.previous: dict[str, _Range] = {}
        self.opening: dict[tuple[str, int], _Range] = {}
        self.day_key: Optional[dt.date] = None
        self.day = _Range(0.0, 0.0)
        self.prev_day: Optional[_Range] = None

    def _bounds(self, name: str, t: float) -> tuple[float, float]:
        hour = int(t // 3600)
        k = (name, hour)
        b = self._bounds_cache.get(k)
        if b is None:
            o, c = SESSIONS[name].bounds_for(dt.datetime.fromtimestamp(hour * 3600 + 1800, UTC))
            b = (o.timestamp(), c.timestamp())
            if len(self._bounds_cache) > 2000:
                self._bounds_cache.clear()
            self._bounds_cache[k] = b
        return b

    def _trading_day(self, t: float) -> dt.date:
        hour = int(t // 3600)
        d = self._day_cache.get(hour)
        if d is None:
            ny = dt.datetime.fromtimestamp(hour * 3600 + 1800, UTC).astimezone(TZ_NEWYORK)
            d = (ny + dt.timedelta(hours=7)).date()        # 17:00 New York starts the next FX day
            if len(self._day_cache) > 2000:
                self._day_cache.clear()
            self._day_cache[hour] = d
        return d

    def update(self, t: float, hi: float, lo: float, span: float = 0.0) -> None:
        day = self._trading_day(t)
        if day != self.day_key:
            if self.day_key is not None and self.day.known:
                self.prev_day = self.day
            self.day_key = day
            self.day = _Range(t, t)
        self.day.add(hi, lo)
        for name in TRACKED_SESSIONS:
            o, c = self._bounds(name, t)
            if not (o <= t < c):
                continue
            cur = self.current.get(name)
            if cur is None or cur.open_epoch != o:
                if cur is not None and cur.known:
                    self.previous[name] = cur
                cur = _Range(o, c)
                self.current[name] = cur
            cur.add(hi, lo)
            if name in OPENING_SESSIONS:
                for m in OPENING_MINUTES:
                    end = o + 60.0 * m
                    if t + span <= end + 1e-9:
                        k = (name, m)
                        r = self.opening.get(k)
                        if r is None or r.open_epoch != o:
                            r = _Range(o, end)
                            self.opening[k] = r
                        r.add(hi, lo)

    def levels(self, now: float) -> SessionLevels:
        out = SessionLevels()
        active = []
        for name in TRACKED_SESSIONS:
            o, c = self._bounds(name, now)
            if o <= now < c:
                active.append(name)
                out.minutes_since_open[name] = (now - o) / 60.0
            cur = self.current.get(name)
            if cur is not None and cur.known and cur.open_epoch > now - 86400:
                out.ranges[name] = (cur.hi, cur.lo, now >= cur.close_epoch)
            elif name in self.previous and self.previous[name].known and self.previous[name].open_epoch > now - 86400:
                p = self.previous[name]
                out.ranges[name] = (p.hi, p.lo, True)
        out.active = tuple(active)
        for (name, m), r in self.opening.items():
            if r.known and r.open_epoch > now - 12 * 3600:
                out.opening[(name, m)] = (r.hi, r.lo, now >= r.close_epoch, r.open_epoch)
        if self.prev_day is not None and self.prev_day.known:
            out.prev_day = (self.prev_day.hi, self.prev_day.lo)
        if self.day.known:
            out.day = (self.day.hi, self.day.lo)
        return out
