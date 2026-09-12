"""Cross-market confirmation.

Correlations are *measured*, never assumed.  Each relationship carries a
rolling-stability figure, and a relationship that has stopped working is
ignored rather than trusted out of habit.

Explicit non-feature: there is no order-book or depth-of-market analysis here.
Retail MT5 CFD feeds do not provide genuine market depth, and inventing it would
be fabrication.  :attr:`CrossMarketReading.depth_available` is always ``False``
for such feeds and the engine uses legitimate tick-volume and spread proxies
instead.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional, Sequence

from ..broker.base import Bar
from ..contracts import classify_fx
from .series import stdev


@dataclass
class Relationship:
    other: str
    correlation: float          # -1..1 over the recent window
    stability: float            # 0..1, agreement across sub-windows
    agrees: Optional[bool]      # None when the relationship is unusable
    other_move_z: float

    @property
    def usable(self) -> bool:
        return abs(self.correlation) >= 0.35 and self.stability >= 0.5


@dataclass
class CrossMarketReading:
    symbol: str
    direction: int
    relationships: list[Relationship] = field(default_factory=list)
    depth_available: bool = False
    note: str = ""

    @property
    def usable_relationships(self) -> list[Relationship]:
        return [r for r in self.relationships if r.usable]

    def score_0_100(self) -> float:
        """Agreement among usable relationships, weighted by |correlation|.

        With nothing usable the score is a neutral 50 - absence of confirmation
        is not evidence against the trade.
        """
        rel = [r for r in self.usable_relationships if r.agrees is not None]
        if not rel:
            return 50.0
        num = sum(abs(r.correlation) * (1.0 if r.agrees else -1.0) for r in rel)
        den = sum(abs(r.correlation) for r in rel) or 1.0
        return round((num / den * 0.5 + 0.5) * 100.0, 1)

    def breadth(self) -> int:
        return len(self.usable_relationships)


def _returns(bars: Sequence[Bar], n: int) -> list[float]:
    closes = [b.close for b in bars[-(n + 1):]]
    return [closes[i] / closes[i - 1] - 1.0
            for i in range(1, len(closes)) if closes[i - 1]]


def _corr(a: Sequence[float], b: Sequence[float]) -> float:
    n = min(len(a), len(b))
    if n < 12:
        return 0.0
    a, b = list(a[-n:]), list(b[-n:])
    ma, mb = sum(a) / n, sum(b) / n
    sa, sb = stdev(a), stdev(b)
    if sa <= 0 or sb <= 0:
        return 0.0
    cov = sum((x - ma) * (y - mb) for x, y in zip(a, b)) / (n - 1)
    return max(-1.0, min(1.0, cov / (sa * sb)))


def _move_z(bars: Sequence[Bar], lookback: int) -> float:
    rets = _returns(bars, 60)
    sd = stdev(rets)
    if sd <= 0 or len(bars) < lookback + 2:
        return 0.0
    total = bars[-1].close / bars[-1 - lookback].close - 1.0
    return total / (sd * math.sqrt(lookback))


def analyse(symbol: str, direction: int, bars: Sequence[Bar],
            others: dict[str, Sequence[Bar]], *, lookback: int = 20,
            window: int = 120, depth_available: bool = False,
            max_relationships: int = 8) -> CrossMarketReading:
    """Score how much related markets agree with ``direction`` on ``symbol``."""
    mine = _returns(bars, window)
    rels: list[Relationship] = []
    for other, obars in others.items():
        if other == symbol or len(obars) < window + 2:
            continue
        theirs = _returns(obars, window)
        corr = _corr(mine, theirs)
        if abs(corr) < 0.2:
            continue
        # Stability: does the sign of the correlation survive when the window
        # is cut into thirds?  A relationship that only exists in aggregate is
        # a coincidence.
        third = max(20, window // 3)
        subs = [_corr(mine[-third * k:len(mine) - third * (k - 1) or None],
                      theirs[-third * k:len(theirs) - third * (k - 1) or None])
                for k in (1, 2, 3)]
        same_sign = sum(1 for s in subs if s != 0 and (s > 0) == (corr > 0))
        stability = same_sign / 3.0
        z = _move_z(obars, lookback)
        agrees: Optional[bool] = None
        if abs(z) > 0.25:
            implied = (1 if z > 0 else -1) * (1 if corr > 0 else -1)
            agrees = implied == (1 if direction > 0 else -1)
        rels.append(Relationship(other=other, correlation=round(corr, 3),
                                 stability=stability, agrees=agrees,
                                 other_move_z=round(z, 3)))
    rels.sort(key=lambda r: -abs(r.correlation))
    note = ("no genuine market depth on this feed; using tick-volume and "
            "spread proxies" if not depth_available else "")
    return CrossMarketReading(symbol=symbol, direction=direction,
                              relationships=rels[:max_relationships],
                              depth_available=depth_available, note=note)
