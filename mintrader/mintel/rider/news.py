"""News from the MetaTrader calendar: the catalyst (family 20) and the
post-news momentum read (family 21).

News is not a trade by itself. What is measured, from price only:

* the FIRST IMPULSE - the move in the first ``impulse_seconds`` after the
  release, against the price just before it;
* the SECOND WAVE / CONTINUATION - after the impulse window, price pushing
  beyond the impulse extreme in the impulse direction;
* REJECTION - price giving back most of the impulse;
* whether the SPREAD has recovered to near its pre-release level.

Nothing is read before ``settle_seconds`` have passed: the first tick after
a release is never chased. Just BEFORE a high-impact event for one of the
pair's currencies, new entries wait (spreads blow out at the release).
"""
from __future__ import annotations

import statistics as st
from collections import deque
from dataclasses import dataclass
from typing import Iterable, Optional


@dataclass
class _Anchor:
    event_id: str
    release: float                 # epoch
    price: float                   # mid just before the release
    pre_spread: float
    hi: float
    lo: float
    imp_hi: float
    imp_lo: float
    name: str = ""
    currency: str = ""
    importance: int = 0
    actual: Optional[float] = None
    forecast: Optional[float] = None
    previous: Optional[float] = None
    revised_previous: Optional[float] = None


@dataclass
class NewsContext:
    upcoming: Optional[dict] = None        # the next relevant event inside the block window
    upcoming_seconds: float = 0.0
    recent: Optional[dict] = None          # the latest release inside the window
    seconds_since: float = 0.0
    settled: bool = False
    impulse_dir: int = 0
    impulse_atr: float = 0.0
    continuation: bool = False
    rejection: bool = False
    spread_recovered: bool = True
    retrace_fraction: float = 0.0


def _ev_dict(e) -> dict:
    return {"event_id": str(getattr(e, "event_id", "")), "name": str(getattr(e, "name", "")),
            "currency": str(getattr(e, "currency", "")), "importance": int(getattr(e, "importance", 0) or 0),
            "time_utc": getattr(e, "time_utc").isoformat() if getattr(e, "time_utc", None) else "",
            "actual": getattr(e, "actual", None), "forecast": getattr(e, "forecast", None),
            "previous": getattr(e, "previous", None), "revised_previous": getattr(e, "revised_previous", None)}


class NewsTracker:
    def __init__(self, min_importance: int = 2, window_minutes: float = 15.0, settle_seconds: float = 20.0,
                 pre_block_seconds: float = 120.0, impulse_seconds: float = 30.0):
        self.min_importance = int(min_importance)
        self.window = float(window_minutes) * 60.0
        self.settle = float(settle_seconds)
        self.pre_block = float(pre_block_seconds)
        self.impulse_seconds = float(impulse_seconds)
        self.events: list = []                         # sorted by time, importance filtered
        self._times: list[float] = []
        self.anchors: dict[tuple[str, str], _Anchor] = {}
        self._last_mid: dict[str, float] = {}
        self._spreads: dict[str, deque] = {}

    def set_events(self, events: Iterable) -> None:
        evs = [e for e in (events or []) if int(getattr(e, "importance", 0) or 0) >= self.min_importance
               and getattr(e, "time_utc", None) is not None]
        evs.sort(key=lambda e: e.time_utc)
        self.events = evs
        self._times = [e.time_utc.timestamp() for e in evs]

    def _relevant(self, ccys: tuple[str, ...], t0: float, t1: float) -> list:
        import bisect
        i = bisect.bisect_left(self._times, t0)
        j = bisect.bisect_right(self._times, t1)
        return [e for e in self.events[i:j] if str(getattr(e, "currency", "")).upper() in ccys]

    def on_tick(self, symbol: str, ccys: tuple[str, ...], t: float, mid: float, spread: float) -> None:
        sp = self._spreads.get(symbol)
        if sp is None:
            sp = self._spreads[symbol] = deque(maxlen=60)
        if self.events:
            for e in self._relevant(ccys, t - self.window, t):
                k = (symbol, str(e.event_id))
                a = self.anchors.get(k)
                rel = e.time_utc.timestamp()
                if a is None:
                    before = self._last_mid.get(symbol, mid)
                    pre = st.median(sp) if sp else spread
                    a = _Anchor(str(e.event_id), rel, before, pre, mid, mid, mid, mid, str(e.name),
                                str(e.currency), int(e.importance), e.actual, e.forecast, e.previous,
                                e.revised_previous)
                    self.anchors[k] = a
                if mid > a.hi:
                    a.hi = mid
                if mid < a.lo:
                    a.lo = mid
                if t - rel <= self.impulse_seconds:
                    if mid > a.imp_hi:
                        a.imp_hi = mid
                    if mid < a.imp_lo:
                        a.imp_lo = mid
            if len(self.anchors) > 500:
                cut = t - 2 * self.window
                self.anchors = {k: a for k, a in self.anchors.items() if a.release >= cut}
        sp.append(spread)
        self._last_mid[symbol] = mid

    def context(self, symbol: str, ccys: tuple[str, ...], now: float, mid: float, spread: float,
                atr: float) -> NewsContext:
        ctx = NewsContext()
        if not self.events:
            return ctx
        ahead = self._relevant(ccys, now, now + self.pre_block)
        high = [e for e in ahead if int(e.importance) >= 3]
        if high:
            e = high[0]
            ctx.upcoming = _ev_dict(e)
            ctx.upcoming_seconds = e.time_utc.timestamp() - now
        recent = [self.anchors.get((symbol, str(e.event_id))) for e in self._relevant(ccys, now - self.window, now)]
        recent = [a for a in recent if a is not None]
        if not recent:
            return ctx
        a = max(recent, key=lambda x: (x.importance, x.release))
        ctx.recent = {"event_id": a.event_id, "name": a.name, "currency": a.currency, "importance": a.importance,
                      "actual": a.actual, "forecast": a.forecast, "previous": a.previous,
                      "revised_previous": a.revised_previous}
        ctx.seconds_since = now - a.release
        ctx.settled = ctx.seconds_since >= self.settle
        up, down = a.imp_hi - a.price, a.price - a.imp_lo
        if atr > 0:
            if up > down and up > 0:
                ctx.impulse_dir, ctx.impulse_atr = 1, up / atr
            elif down > 0:
                ctx.impulse_dir, ctx.impulse_atr = -1, down / atr
        ctx.spread_recovered = spread <= max(a.pre_spread, 1e-12) * 1.5
        if ctx.impulse_dir and ctx.settled:
            imp = (a.imp_hi - a.price) if ctx.impulse_dir > 0 else (a.price - a.imp_lo)
            if imp > 0:
                ctx.retrace_fraction = ((a.imp_hi - mid) if ctx.impulse_dir > 0 else (mid - a.imp_lo)) / imp
            beyond = mid > a.imp_hi if ctx.impulse_dir > 0 else mid < a.imp_lo
            ctx.continuation = bool(beyond and ctx.seconds_since > self.impulse_seconds)
            ctx.rejection = ctx.retrace_fraction >= 0.6
        return ctx
