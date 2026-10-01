"""The Rapid Scalp Opportunity Score: 0-100, built from measured parts.

Every component's contribution is returned and logged for every attempt,
taken or rejected, so the record can later say what was predictive.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .config import ScalperConfig, ScoreWeights
from .features import Features, chop_score


@dataclass
class Opportunity:
    symbol: str
    direction: int                      # +1 long, -1 short, 0 nothing
    confidence: float                   # 0..100
    contributions: dict = field(default_factory=dict)
    penalties: dict = field(default_factory=dict)
    notes: list = field(default_factory=list)
    blockers: list = field(default_factory=list)
    chop: float = 0.0
    expected_move_points: float = 0.0
    features: Optional[Features] = None

    @property
    def tradable(self) -> bool:
        return self.direction != 0 and not self.blockers

    def explain(self) -> str:
        parts = [f"Rapid Scalper opportunity: {self.confidence:.0f}/100"]
        for k, v in self.contributions.items():
            parts.append(f"{k}: {v:+.0f}")
        for k, v in self.penalties.items():
            parts.append(f"{k}: {v:+.0f}")
        return "  ".join(parts)


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def score(f: Features, cfg: ScalperConfig) -> Opportunity:
    w: ScoreWeights = cfg.weights
    opp = Opportunity(symbol=f.symbol, direction=0, confidence=0.0, features=f)
    if not f.ok:
        opp.blockers.append(f.reason or "no features")
        return opp
    d = f.direction()
    if d == 0:
        opp.blockers.append("no immediate directional move")
        return opp
    opp.direction = d
    vol = max(f.realised_vol_points, 0.1)

    # A. immediate momentum: movement beginning NOW
    v = f.velocity_5s * d
    vq = _clamp(v / (vol * 0.6))                         # a 5s velocity worth ~0.6 vol/s is full marks
    acc = _clamp(0.5 + (f.acceleration * d) / (vol * 0.4), 0, 1)
    run = _clamp(abs(f.consecutive_ticks) / 6.0) if f.consecutive_ticks * d > 0 else 0.0
    exp = _clamp((f.bar_range_ratio - 1.0) / 1.5)
    mom = (0.4 * vq + 0.25 * acc + 0.15 * run + 0.1 * exp + 0.1 * f.persistence)
    opp.contributions["Momentum"] = round(w.momentum * mom, 1)

    # B. trend alignment
    if f.trend_bias == d:
        align = 1.0
    elif f.trend_bias == 0:
        align = 0.4
    else:
        align = 0.0
    vwap_side = 1 if f.mid > f.vwap else -1
    align = 0.7 * align + 0.3 * (1.0 if vwap_side == d else 0.0)
    opp.contributions["Trend alignment"] = round(w.trend_alignment * align, 1)

    # C. entry quality: retest / not too late
    ext = abs(f.dist_from_fast_ema_points) / max(f.atr_points, 1.0)
    late = _clamp((ext - 0.8) / 1.2)                         # > ~2 ATR from the fast EMA is chasing
    retest = 1.0 - late
    opp.contributions["Retest quality"] = round(w.entry_quality * retest, 1)
    if late > 0:
        opp.penalties["Late entry"] = -round(cfg.late_entry_penalty_max * late, 1)
        if late > 0.85:
            opp.blockers.append("too late - price is already extended from the fast EMA")

    # D. volume
    volq = _clamp((f.volume_ratio - 0.8) / 1.7) * 0.6 + _clamp((f.volume_accel - 0.9) / 1.1) * 0.4
    opp.contributions["Volume"] = round(w.volume * volq, 1)

    # E. spread
    expected = max(2.0 * vol * 3.0, f.atr_points * 0.5)      # points a scalp can reasonably aim for
    opp.expected_move_points = expected
    frac = f.spread_points / expected if expected > 0 else 1.0
    sq = _clamp(1.0 - frac / cfg.max_spread_fraction_of_expected_move)
    if f.spread_expansion > cfg.spread_expansion_limit:
        sq *= 0.3
        opp.blockers.append(f"spread has expanded ({f.spread_expansion:.1f}x normal)")
    if f.spread_points > cfg.max_spread_points:
        opp.blockers.append(f"spread too wide ({f.spread_points:.1f} points)")
    if frac > cfg.max_spread_fraction_of_expected_move:
        opp.blockers.append(f"spread is {frac:.0%} of the expected move (limit "
                            f"{cfg.max_spread_fraction_of_expected_move:.0%})")
    opp.contributions["Spread"] = round(w.spread * sq, 1)

    # F. liquidity
    lq = _clamp((f.ticks_per_minute - cfg.min_ticks_per_minute) / (3 * cfg.min_ticks_per_minute))
    if f.ticks_per_minute < cfg.min_ticks_per_minute:
        opp.blockers.append(f"too thin ({f.ticks_per_minute:.0f} ticks/min)")
    opp.contributions["Liquidity"] = round(w.liquidity * lq, 1)

    # volatility suitability: enough to pay, not violent chaos
    vs = _clamp(1.0 - abs(f.bar_range_ratio - 1.6) / 2.0)
    opp.contributions["Volatility fit"] = round(w.volatility * vs, 1)

    # nearby structure: room to the micro extreme in our direction
    room = ((f.micro_high - f.mid) if d > 0 else (f.mid - f.micro_low)) / max(f.point, 1e-9)
    sr = _clamp(room / max(expected, 1.0))
    opp.contributions["Structure room"] = round(w.structure * sr, 1)

    # G. order flow - explicitly disabled
    opp.contributions["Order flow (disabled)"] = 0.0

    # regime / chop
    opp.chop, chop_reasons = chop_score(f)
    if opp.chop >= 0.5:
        opp.penalties["Chop"] = -round(30.0 * opp.chop, 1)
        opp.notes.extend(chop_reasons)
        if cfg.chop_block and opp.chop >= 0.6:
            opp.blockers.append("choppy market: " + "; ".join(chop_reasons[:2]))

    total = sum(opp.contributions.values()) + sum(opp.penalties.values())
    opp.confidence = round(max(0.0, min(100.0, total)), 1)
    if opp.confidence < cfg.min_confidence:
        opp.blockers.append(f"confidence {opp.confidence:.0f} below {cfg.min_confidence:.0f}")
    return opp
