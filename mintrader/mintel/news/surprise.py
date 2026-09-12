"""Surprise measurement and interpretation.

Two deliberate design choices:

1. A surprise is only meaningful **relative to how much that series normally
   surprises**.  A 0.1pp miss on core CPI is a large surprise; a 0.1pp miss on
   a regional business survey is noise.  So the raw gap is divided by the
   historical dispersion of gaps for the *same* series.

2. "Better number = buy the currency" is refused.  Direction is looked up per
   event family (an inflation beat is hawkish, an unemployment-rate beat is
   bearish for the currency, and so on), and even then the resulting bias is
   only a *hypothesis* that price must confirm - see
   :mod:`mintel.news.engine`.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Optional, Sequence

from ..broker.base import CalendarEvent
from ..data.series import stdev

# Event families and whether a HIGHER actual is currency-positive (+1),
# currency-negative (-1), or genuinely ambiguous (0).
FAMILY_SIGN: tuple[tuple[str, int], ...] = (
    (r"\bunemployment rate\b", -1),
    (r"\bjobless claims\b", -1),
    (r"\bcontinuing claims\b", -1),
    (r"\bnon-?farm|payrolls?\b", +1),
    (r"\bemployment change\b", +1),
    (r"\bcpi\b|\binflation\b|\bppi\b|\bhicp\b", +1),
    (r"\bgdp\b", +1),
    (r"\bretail sales\b", +1),
    (r"\bpmi\b|\bism\b", +1),
    (r"\binterest rate|rate decision|bank rate|refi rate|funds rate\b", +1),
    (r"\btrade balance|current account\b", +1),
    (r"\bindustrial production|manufacturing production\b", +1),
    (r"\bconsumer confidence|sentiment\b", +1),
    (r"\bdurable goods\b", +1),
    (r"\bbuilding permits|housing starts|home sales\b", +1),
    (r"\bcrude oil inventories\b", 0),
    (r"\bbudget balance|deficit\b", 0),
)


def family_sign(name: str) -> int:
    low = (name or "").lower()
    for pattern, sign in FAMILY_SIGN:
        if re.search(pattern, low):
            return sign
    return 0


@dataclass
class SurpriseReading:
    event: CalendarEvent
    gap_vs_forecast: Optional[float]
    gap_vs_previous: Optional[float]
    gap_pct_of_forecast: Optional[float]
    z_vs_history: Optional[float]      # normalised by this series' own history
    samples: int
    revision: Optional[float]
    hypothesis_sign: int               # +1 currency-bullish, -1 bearish, 0 none
    magnitude: float                   # 0..1, how big a surprise this is
    interpretable: bool
    note: str = ""

    @property
    def is_surprise(self) -> bool:
        return self.interpretable and self.magnitude >= 0.25

    def describe(self) -> str:
        e = self.event
        if self.gap_vs_forecast is None:
            return f"{e.currency} {e.name}: no forecast to compare against"
        word = "above" if self.gap_vs_forecast > 0 else (
            "below" if self.gap_vs_forecast < 0 else "in line with")
        z = f", {abs(self.z_vs_history):.1f} sigma" if self.z_vs_history else ""
        return (f"{e.currency} {e.name} came in {word} forecast "
                f"({_fmt(e.actual)} vs {_fmt(e.forecast)}{z})")


def _fmt(x: Optional[float]) -> str:
    return "n/a" if x is None else (f"{x:g}")


def measure(event: CalendarEvent,
            history: Sequence[CalendarEvent] = ()) -> SurpriseReading:
    """Quantify one release against its forecast, previous and own history."""
    gap_f = (event.actual - event.forecast
             if event.actual is not None and event.forecast is not None else None)
    prev = event.revised_previous if event.revised_previous is not None else event.previous
    gap_p = (event.actual - prev
             if event.actual is not None and prev is not None else None)
    revision = (event.revised_previous - event.previous
                if event.revised_previous is not None and event.previous is not None
                else None)
    gap_pct = None
    if gap_f is not None and event.forecast not in (None, 0):
        gap_pct = gap_f / abs(event.forecast) * 100.0

    hist_gaps = [h.actual - h.forecast for h in history
                 if h.actual is not None and h.forecast is not None]
    z = None
    samples = len(hist_gaps)
    if gap_f is not None and samples >= 6:
        sd = stdev(hist_gaps)
        if sd > 0:
            z = gap_f / sd

    sign = family_sign(event.name)
    interpretable = gap_f is not None and sign != 0

    # Magnitude: prefer the properly normalised z-score.  Without enough
    # history, fall back to a percentage gap but cap the magnitude so a
    # poorly-understood series can never look like a five-sigma shock.
    if z is not None:
        magnitude = min(1.0, abs(z) / 2.5)
        note = f"normalised over {samples} prior releases"
    elif gap_pct is not None:
        magnitude = min(0.6, abs(gap_pct) / 25.0)
        note = "no release history yet; magnitude capped"
    else:
        magnitude = 0.0
        note = "no forecast available"

    hypothesis = 0
    if interpretable and gap_f != 0:
        hypothesis = sign * (1 if gap_f > 0 else -1)

    return SurpriseReading(
        event=event, gap_vs_forecast=gap_f, gap_vs_previous=gap_p,
        gap_pct_of_forecast=gap_pct, z_vs_history=z, samples=samples,
        revision=revision, hypothesis_sign=hypothesis,
        magnitude=round(magnitude, 3), interpretable=interpretable, note=note)
