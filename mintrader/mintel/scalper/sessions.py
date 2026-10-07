"""Session windows for the Rapid Scalper, in London local time (DST-aware).

Each major activation (London 08:00, New York FX 13:00, the US cash open
14:30) has an observation window before it, an opening window and a
post-open window. The opening window is the aggressive one, but only if the
session is actually moving: the engine measures that rather than assuming
it (see ``SessionTracker``).
"""
from __future__ import annotations

import datetime as dt
import statistics as st
from dataclasses import dataclass
from typing import Optional, Sequence

from ..clock import TZ_LONDON, to_utc

PHASES = ("PRE_OPEN", "OPENING", "POST_OPEN", "QUIET")


@dataclass(frozen=True)
class Window:
    name: str            # "London", "New York", "US cash"
    phase: str           # PRE_OPEN / OPENING / POST_OPEN
    start: str           # "HH:MM" London time
    end: str


def default_windows() -> tuple[Window, ...]:
    return (
        Window("London", "PRE_OPEN", "07:30", "08:00"), Window("London", "OPENING", "08:00", "08:45"),
        Window("London", "POST_OPEN", "08:45", "10:00"),
        Window("New York", "PRE_OPEN", "12:30", "13:00"), Window("New York", "OPENING", "13:00", "13:45"),
        Window("New York", "POST_OPEN", "13:45", "14:30"),
        Window("US cash", "PRE_OPEN", "14:15", "14:30"), Window("US cash", "OPENING", "14:30", "15:15"),
        Window("US cash", "POST_OPEN", "15:15", "16:30"),
    )


def _t(s: str) -> dt.time:
    h, m = s.split(":")
    return dt.time(int(h), int(m))


def current_window(now: dt.datetime, windows: Sequence[Window]) -> Optional[Window]:
    local = to_utc(now).astimezone(TZ_LONDON)
    if local.weekday() >= 5:
        return None
    t = local.time()
    for w in windows:
        if _t(w.start) <= t < _t(w.end):
            return w
    return None


class SessionTracker:
    """Is this session actually moving? Compares the one-minute ranges of the
    last few minutes with the typical minute of the last hour, per market,
    and keeps a simple memory per (session, market) of whether the last
    opening delivered."""

    def __init__(self, recent_minutes: int = 5, baseline_minutes: int = 60, moving_ratio: float = 1.3):
        self.recent_minutes = recent_minutes
        self.baseline_minutes = baseline_minutes
        self.moving_ratio = moving_ratio
        self.ratio: dict[str, float] = {}
        self.delivered: dict[tuple[str, str], bool] = {}

    def update(self, symbol: str, bars: Sequence, point: float) -> float:
        rngs = [(b.high - b.low) / point for b in bars[-self.baseline_minutes:] if point > 0]
        if len(rngs) < self.recent_minutes + 5:
            self.ratio[symbol] = 1.0
            return 1.0
        recent = st.mean(rngs[-self.recent_minutes:])
        base = st.median(rngs[:-self.recent_minutes]) or 1e-9
        r = recent / base
        self.ratio[symbol] = r
        return r

    def moving(self, symbol: str) -> bool:
        return self.ratio.get(symbol, 1.0) >= self.moving_ratio

    def note(self, window: Optional[Window], symbol: str, net: float) -> None:
        if window is not None and window.phase == "OPENING":
            self.delivered[(window.name, symbol)] = net > 0


def threshold_for(window: Optional[Window], base: float, opening_bonus: float, quiet_penalty: float,
                  moving: bool) -> tuple[float, str]:
    """The momentum score required right now, and why."""
    if window is None:
        return base + quiet_penalty, "quiet hours: a higher bar"
    if window.phase == "OPENING":
        if moving:
            return base - opening_bonus, f"{window.name} opening and the market is moving: aggressive"
        return base, f"{window.name} opening but the market is not moving yet: normal bar"
    if window.phase == "POST_OPEN":
        return base, f"{window.name} post-open: normal bar"
    return base + quiet_penalty / 2.0, f"{window.name} pre-open: observing, higher bar"
