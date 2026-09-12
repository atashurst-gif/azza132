"""Currency strength.

Derived from *many* pairs, never one.  The point is to separate "the dollar is
strong everywhere" from "sterling specifically is being sold", because those two
situations call for different trades even when GBPUSD looks identical on a
chart.

Method: for each pair, compute a normalised return (return divided by that
pair's own volatility, so a 30-pip move in GBPJPY is not treated as three times
more meaningful than a 10-pip move in EURCHF).  Credit the base currency and
debit the quote currency.  Average per currency, then rank 0-100.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional, Sequence

from ..broker.base import Bar
from ..contracts import FX_CURRENCIES, classify_fx
from .series import stdev


@dataclass
class StrengthReading:
    scores: dict[str, float]           # currency -> 0..100
    raw: dict[str, float]              # currency -> mean normalised return
    contributions: dict[str, int]      # currency -> number of pairs used
    pairs_used: int
    dispersion: float                  # spread of raw scores; low = no theme

    def score(self, ccy: str) -> float:
        return self.scores.get(ccy.upper(), 50.0)

    def divergence(self, base: str, quote: str) -> float:
        """Signed base-minus-quote strength, -100..+100."""
        return self.score(base) - self.score(quote)

    def confidence(self, ccy: str) -> float:
        """How much to trust one currency's reading, 0..1.

        Two pairs is an anecdote; five is a theme.
        """
        n = self.contributions.get(ccy.upper(), 0)
        return max(0.0, min(1.0, n / 5.0))

    def agrees_with(self, base: str, quote: str, side_sign: int,
                    min_divergence: float = 12.0) -> Optional[bool]:
        """Does the FX breadth picture support this direction?

        ``None`` means "no opinion" (insufficient pairs or no dispersion), which
        is different from disagreement and is scored as neutral.
        """
        if self.pairs_used < 4 or self.dispersion < 3.0:
            return None
        div = self.divergence(base, quote)
        if abs(div) < min_divergence:
            return None
        return (div > 0) == (side_sign > 0)


def _normalised_return(bars: Sequence[Bar], lookback: int) -> Optional[float]:
    if len(bars) < lookback + 12:
        return None
    closes = [b.close for b in bars]
    rets = [closes[i] / closes[i - 1] - 1.0
            for i in range(len(closes) - 60, len(closes)) if closes[i - 1]]
    sd = stdev(rets)
    if sd <= 0:
        return None
    total = closes[-1] / closes[-1 - lookback] - 1.0
    # z-score of the move against the pair's own recent per-bar volatility
    return total / (sd * math.sqrt(lookback))


def compute(pair_bars: dict[str, Sequence[Bar]], *,
            lookback: int = 20) -> StrengthReading:
    """``pair_bars`` maps symbol -> bars.  Non-FX symbols are ignored."""
    acc: dict[str, list[float]] = {c: [] for c in FX_CURRENCIES}
    used = 0
    for symbol, bars in pair_bars.items():
        base, quote, _ = classify_fx(symbol)
        if not base or not quote:
            continue
        z = _normalised_return(bars, lookback)
        if z is None:
            continue
        used += 1
        acc[base].append(z)
        acc[quote].append(-z)

    raw = {c: (sum(v) / len(v) if v else 0.0) for c, v in acc.items()}
    contributions = {c: len(v) for c, v in acc.items()}

    # Bounded squash rather than min-max: min-max lets one thin cross with an
    # outlier move define the whole scale, so a currency seen in a single pair
    # could print 100/100.  tanh keeps the scale stable over time and
    # comparable between scans.
    SCALE = 0.9
    scores: dict[str, float] = {}
    for c, v in raw.items():
        n = contributions[c]
        if n == 0:
            scores[c] = 50.0
            continue
        s_raw = 50.0 + 50.0 * math.tanh(v / SCALE)
        # Shrink toward neutral in proportion to how little evidence there is.
        w = min(1.0, n / 4.0)
        scores[c] = round(50.0 + (s_raw - 50.0) * w, 1)

    seen = [scores[c] for c in raw if contributions[c] > 0]
    dispersion = (max(seen) - min(seen)) if seen else 0.0

    return StrengthReading(scores=scores, raw=raw, contributions=contributions,
                           pairs_used=used, dispersion=dispersion)
