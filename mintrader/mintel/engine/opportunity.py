"""Opportunity scoring and confidence tiers.

The raw evidence aggregate is not a probability.  Turning a 0-100 score into a
position size demands calibration, so this module keeps the two things
separate:

* :func:`score` applies tactic quality and the multiplicative *reality checks*
  (headroom, exhaustion, execution feasibility) to produce the final 0-100
  opportunity number that markets compete on.
* :class:`Calibrator` learns, from the trade journal, what a given score band
  has *actually* delivered, and exposes that as a win probability and an
  expectancy multiplier.  Uncalibrated scores never size up aggressively.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional, Sequence

from ..config import ScanConfig
from ..data.momentum import MomentumReading, exhaustion_discount
from ..data.headroom import HeadroomReading


TIERS = ("NO_TRADE", "NORMAL", "STRONG", "EXCEPTIONAL")


@dataclass
class ScoreBreakdown:
    raw_evidence: float
    tactic_quality: float
    headroom_multiplier: float
    exhaustion_multiplier: float
    execution_multiplier: float
    liquidity_multiplier: float
    final: float
    notes: tuple[str, ...] = ()


def score(raw_evidence: float, tactic_quality: float,
          headroom: Optional[HeadroomReading],
          momentum: Optional[MomentumReading],
          range_consumed: float,
          execution_score: float,
          liquidity_score: float) -> ScoreBreakdown:
    """Combine evidence with tactic fit, then apply reality checks.

    Evidence and tactic quality are blended (a strong market with a mediocre
    setup is a mediocre trade, and vice versa).  The multipliers can only ever
    *reduce* the score except for generous headroom, which earns a small bonus.
    """
    notes: list[str] = []
    blended = 0.62 * raw_evidence + 0.38 * tactic_quality

    hm = headroom.multiplier() if headroom else 1.0
    if headroom and headroom.nearest and hm < 0.8:
        notes.append(f"only {headroom.headroom_pips:.0f} pips to "
                     f"{headroom.nearest.kind.lower().replace('_', ' ')}")
    xm = exhaustion_discount(range_consumed, momentum)
    if xm < 0.95:
        notes.append(f"{range_consumed:.1f}x average range already travelled")
    em = 0.55 + 0.45 * max(0.0, min(1.0, execution_score / 100.0))
    lm = 0.80 + 0.20 * max(0.0, min(1.0, liquidity_score / 100.0))

    final = blended * hm * xm * em * lm
    return ScoreBreakdown(raw_evidence=round(raw_evidence, 2),
                          tactic_quality=round(tactic_quality, 2),
                          headroom_multiplier=round(hm, 3),
                          exhaustion_multiplier=round(xm, 3),
                          execution_multiplier=round(em, 3),
                          liquidity_multiplier=round(lm, 3),
                          final=round(max(0.0, min(100.0, final)), 2),
                          notes=tuple(notes))


def tier_for(final_score: float, cfg: ScanConfig) -> str:
    if final_score >= cfg.tier_exceptional:
        return "EXCEPTIONAL"
    if final_score >= cfg.tier_strong:
        return "STRONG"
    if final_score >= cfg.tier_normal:
        return "NORMAL"
    return "NO_TRADE"


# ------------------------------------------------------------ calibration ----

@dataclass
class Bucket:
    lo: float
    hi: float
    trades: int = 0
    wins: int = 0
    sum_r: float = 0.0

    @property
    def win_rate(self) -> Optional[float]:
        return self.wins / self.trades if self.trades else None

    @property
    def expectancy_r(self) -> Optional[float]:
        return self.sum_r / self.trades if self.trades else None


class Calibrator:
    """Maps opportunity score -> realised performance, learned from the journal.

    Deliberately crude and transparent: score bands, observed win rate, observed
    expectancy in R.  A band with few samples reports low ``reliability``, and
    the risk layer refuses to size aggressively on an unreliable band.  This is
    what stops a 97/100 score from becoming a large position simply because the
    number is large.
    """

    def __init__(self, edges: Sequence[float] = (0, 50, 58, 64, 70, 76, 82, 88, 100),
                 min_samples: int = 25):
        self.buckets = [Bucket(edges[i], edges[i + 1])
                        for i in range(len(edges) - 1)]
        self.min_samples = min_samples

    def _bucket(self, s: float) -> Bucket:
        for b in self.buckets:
            if b.lo <= s < b.hi:
                return b
        return self.buckets[-1]

    def observe(self, opportunity: float, r_multiple: float) -> None:
        b = self._bucket(opportunity)
        b.trades += 1
        b.wins += 1 if r_multiple > 0 else 0
        b.sum_r += r_multiple

    def load(self, rows: Sequence[tuple[float, float]]) -> None:
        for b in self.buckets:
            b.trades = b.wins = 0
            b.sum_r = 0.0
        for opp, r in rows:
            self.observe(opp, r)

    def reliability(self, opportunity: float) -> float:
        """0..1 - how much the score band's history can be trusted."""
        b = self._bucket(opportunity)
        return max(0.0, min(1.0, b.trades / max(self.min_samples, 1)))

    def expected_r(self, opportunity: float) -> Optional[float]:
        b = self._bucket(opportunity)
        return b.expectancy_r if b.trades >= 5 else None

    def win_probability(self, opportunity: float) -> Optional[float]:
        b = self._bucket(opportunity)
        return b.win_rate if b.trades >= 5 else None

    def calibrated_confidence(self, opportunity: float) -> float:
        """Blend the raw score with observed performance, in 0..1.

        With no history this returns the raw score scaled down - an uncalibrated
        score is treated as an opinion, not a measurement.  As evidence
        accumulates, the observed expectancy takes over.
        """
        raw = max(0.0, min(1.0, (opportunity - 45.0) / 45.0))
        rel = self.reliability(opportunity)
        er = self.expected_r(opportunity)
        if er is None or rel <= 0:
            return raw * 0.75
        observed = max(0.0, min(1.0, 0.5 + er / 1.2))
        return raw * (1.0 - rel) * 0.75 + observed * rel

    def report(self) -> list[dict]:
        return [{"band": f"{b.lo:.0f}-{b.hi:.0f}", "trades": b.trades,
                 "win_rate": None if b.win_rate is None else round(b.win_rate, 3),
                 "expectancy_r": None if b.expectancy_r is None
                 else round(b.expectancy_r, 3)}
                for b in self.buckets]

    def monotonic_violation(self) -> Optional[str]:
        """Sanity check: higher score bands should not perform worse.

        Reported honestly rather than hidden - if the top band underperforms the
        middle one, the scoring model is miscalibrated and should be retuned,
        not trusted harder.
        """
        pts = [(b, b.expectancy_r) for b in self.buckets
               if b.trades >= self.min_samples and b.expectancy_r is not None]
        for (b1, e1), (b2, e2) in zip(pts, pts[1:]):
            if e2 < e1 - 0.25:
                return (f"band {b2.lo:.0f}-{b2.hi:.0f} (E={e2:.2f}R) "
                        f"underperforms {b1.lo:.0f}-{b1.hi:.0f} (E={e1:.2f}R)")
        return None
