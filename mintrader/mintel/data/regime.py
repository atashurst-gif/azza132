"""Regime classification.

This is the first decision the engine makes about an instrument, and it gates
which tactics are even allowed to compete.  Running every strategy all the time
is how a bot ends up mean-reverting into a trend day.

Regimes are deliberately few and observable:

``TREND``       directional structure with efficient movement
``RANGE``       two-sided, contained, mean-reverting
``SQUEEZE``     volatility compressed below its own history, coiled
``HIGH_VOL``    volatility in the top decile, expansion under way
``LOW_VOL``     volatility in the bottom decile without compression structure
``NEWS``        inside a live reaction window to a scheduled release
``UNCLEAR``     honest answer when the data does not support a label
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from .momentum import MomentumReading
from .structure import StructureReading, Trend


class Regime(str, Enum):
    TREND = "TREND"
    RANGE = "RANGE"
    SQUEEZE = "SQUEEZE"
    HIGH_VOL = "HIGH_VOL"
    LOW_VOL = "LOW_VOL"
    NEWS = "NEWS"
    UNCLEAR = "UNCLEAR"


@dataclass
class RegimeReading:
    regime: Regime
    confidence: float          # 0..1
    vol_state: str             # LOW | NORMAL | HIGH
    direction: int             # structural bias, may be 0
    reasons: tuple[str, ...] = ()

    @property
    def label(self) -> str:
        return f"{self.regime.value} / {self.vol_state} VOL"


def classify(structure: Optional[StructureReading],
             momentum: Optional[MomentumReading],
             *, in_news_window: bool = False,
             news_importance: int = 0) -> RegimeReading:
    reasons: list[str] = []
    if structure is None or momentum is None:
        return RegimeReading(Regime.UNCLEAR, 0.0, "NORMAL", 0,
                             ("insufficient data",))

    vp = momentum.vol_percentile
    if vp >= 0.85:
        vol_state = "HIGH"
    elif vp <= 0.15:
        vol_state = "LOW"
    else:
        vol_state = "NORMAL"

    direction = structure.direction_bias or momentum.direction

    # A live reaction to a high-importance release dominates: the usual
    # structural reading is temporarily unreliable while the market repriced.
    if in_news_window and news_importance >= 3:
        reasons.append("inside high-importance news reaction window")
        return RegimeReading(Regime.NEWS, 0.9, vol_state, direction,
                             tuple(reasons))

    trend_evidence = 0.0
    if structure.trend in (Trend.UP, Trend.DOWN):
        trend_evidence += 0.4
        reasons.append(f"structure {structure.trend.value}")
    if momentum.adx >= 22:
        trend_evidence += 0.25
        reasons.append(f"ADX {momentum.adx:.0f}")
    if momentum.efficiency >= 0.30:
        trend_evidence += 0.25
        reasons.append(f"efficiency {momentum.efficiency:.2f}")
    if abs(momentum.ema_separation_atr) >= 0.5:
        trend_evidence += 0.10

    range_evidence = 0.0
    if structure.in_range:
        range_evidence += 0.45
        reasons.append("contained range")
    if momentum.adx < 18:
        range_evidence += 0.25
    if momentum.efficiency < 0.18:
        range_evidence += 0.30
        reasons.append(f"low efficiency {momentum.efficiency:.2f}")

    squeeze_evidence = 0.0
    if structure.compression >= 0.45:
        squeeze_evidence += 0.5
        reasons.append(f"compression {structure.compression:.2f}")
    if vp <= 0.25:
        squeeze_evidence += 0.35
    if momentum.vol_expansion < 0.85:
        squeeze_evidence += 0.15

    scores = {
        Regime.TREND: trend_evidence,
        Regime.RANGE: range_evidence,
        Regime.SQUEEZE: squeeze_evidence,
    }
    best = max(scores, key=lambda k: scores[k])
    best_score = scores[best]

    # Volatility state can override the structural label, because what to *do*
    # in a high-vol expansion differs from a calm trend even when structure is
    # identical.
    if vol_state == "HIGH" and momentum.vol_expansion >= 1.5 and best is not Regime.RANGE:
        reasons.append(f"vol expansion {momentum.vol_expansion:.2f}x ATR")
        return RegimeReading(Regime.HIGH_VOL, min(1.0, 0.55 + best_score / 3),
                             vol_state, direction, tuple(reasons))
    if vol_state == "LOW" and squeeze_evidence < 0.5:
        return RegimeReading(Regime.LOW_VOL, 0.6, vol_state, direction,
                             tuple(reasons) + ("volatility in bottom decile",))
    if best_score < 0.45:
        return RegimeReading(Regime.UNCLEAR, best_score, vol_state, direction,
                             tuple(reasons or ("no dominant character",)))
    return RegimeReading(best, min(1.0, best_score), vol_state, direction,
                         tuple(reasons))
