"""Unified market state from weighted evidence families.

The anti-pattern this module exists to avoid: forty indicators each shouting
BUY, producing 97/100 confidence that is really one signal counted forty times.

So evidence is organised into **families**.  A family contributes exactly one
score no matter how many measurements feed it: EMA slope, MACD-like separation
and rate-of-change all live inside MOMENTUM and cannot vote three times.
Weights belong to families, not to indicators, and the weights change with the
regime because a breakout score means something different in a squeeze than in
a range.

Each family also carries a ``confidence`` in [0, 1].  Low confidence shrinks
that family's score toward the neutral 50 rather than dropping it, so thin
evidence dilutes rather than distorts.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from ..broker.base import Bar, Position, Side, Tick, TF
from ..contracts import SymbolSpec
from ..data.crossmarket import CrossMarketReading
from ..data.headroom import HeadroomReading
from ..data.momentum import MomentumReading
from ..data.regime import Regime, RegimeReading
from ..data.sessions import SessionReading
from ..data.series import MultiTFData
from ..data.strength import StrengthReading
from ..data.structure import StructureReading
from ..news.engine import NewsVerdict


class Family(str, Enum):
    STRUCTURE = "STRUCTURE"
    MOMENTUM = "MOMENTUM"
    VOLATILITY = "VOLATILITY"
    PARTICIPATION = "PARTICIPATION"
    SESSION = "SESSION"
    MULTI_TF = "MULTI_TF"
    CROSS_MARKET = "CROSS_MARKET"
    NEWS = "NEWS"
    EXECUTION = "EXECUTION"
    HISTORICAL = "HISTORICAL"
    REGIME = "REGIME"
    HEADROOM = "HEADROOM"


@dataclass
class Evidence:
    family: Family
    score: float                 # 0..100, 50 = neutral
    confidence: float = 1.0      # 0..1
    note: str = ""

    @property
    def effective(self) -> float:
        """Score shrunk toward neutral by its own confidence."""
        c = max(0.0, min(1.0, self.confidence))
        return 50.0 + (self.score - 50.0) * c


# Regime-specific family weights.  They sum to 1.0 per regime.  Note how
# HEADROOM matters more in a range (targets are close) and MOMENTUM more in a
# trend, and how NEWS dominates in the NEWS regime.
WEIGHTS: dict[Regime, dict[Family, float]] = {
    Regime.TREND: {
        Family.STRUCTURE: 0.20, Family.MOMENTUM: 0.20, Family.MULTI_TF: 0.12,
        Family.VOLATILITY: 0.06, Family.PARTICIPATION: 0.05,
        Family.SESSION: 0.05, Family.CROSS_MARKET: 0.09, Family.NEWS: 0.06,
        Family.EXECUTION: 0.06, Family.HISTORICAL: 0.05, Family.HEADROOM: 0.06,
    },
    Regime.RANGE: {
        Family.STRUCTURE: 0.22, Family.MOMENTUM: 0.08, Family.MULTI_TF: 0.08,
        Family.VOLATILITY: 0.08, Family.PARTICIPATION: 0.05,
        Family.SESSION: 0.06, Family.CROSS_MARKET: 0.06, Family.NEWS: 0.05,
        Family.EXECUTION: 0.09, Family.HISTORICAL: 0.05, Family.HEADROOM: 0.18,
    },
    Regime.SQUEEZE: {
        Family.STRUCTURE: 0.18, Family.MOMENTUM: 0.14, Family.MULTI_TF: 0.10,
        Family.VOLATILITY: 0.16, Family.PARTICIPATION: 0.08,
        Family.SESSION: 0.07, Family.CROSS_MARKET: 0.06, Family.NEWS: 0.05,
        Family.EXECUTION: 0.06, Family.HISTORICAL: 0.04, Family.HEADROOM: 0.06,
    },
    Regime.HIGH_VOL: {
        Family.STRUCTURE: 0.15, Family.MOMENTUM: 0.22, Family.MULTI_TF: 0.09,
        Family.VOLATILITY: 0.09, Family.PARTICIPATION: 0.08,
        Family.SESSION: 0.05, Family.CROSS_MARKET: 0.08, Family.NEWS: 0.07,
        Family.EXECUTION: 0.10, Family.HISTORICAL: 0.03, Family.HEADROOM: 0.04,
    },
    Regime.LOW_VOL: {
        Family.STRUCTURE: 0.20, Family.MOMENTUM: 0.10, Family.MULTI_TF: 0.10,
        Family.VOLATILITY: 0.06, Family.PARTICIPATION: 0.06,
        Family.SESSION: 0.08, Family.CROSS_MARKET: 0.07, Family.NEWS: 0.05,
        Family.EXECUTION: 0.12, Family.HISTORICAL: 0.04, Family.HEADROOM: 0.12,
    },
    Regime.NEWS: {
        Family.STRUCTURE: 0.12, Family.MOMENTUM: 0.16, Family.MULTI_TF: 0.06,
        Family.VOLATILITY: 0.06, Family.PARTICIPATION: 0.08,
        Family.SESSION: 0.04, Family.CROSS_MARKET: 0.12, Family.NEWS: 0.20,
        Family.EXECUTION: 0.10, Family.HISTORICAL: 0.02, Family.HEADROOM: 0.04,
    },
    Regime.UNCLEAR: {
        Family.STRUCTURE: 0.20, Family.MOMENTUM: 0.16, Family.MULTI_TF: 0.12,
        Family.VOLATILITY: 0.07, Family.PARTICIPATION: 0.06,
        Family.SESSION: 0.06, Family.CROSS_MARKET: 0.08, Family.NEWS: 0.06,
        Family.EXECUTION: 0.09, Family.HISTORICAL: 0.05, Family.HEADROOM: 0.05,
    },
}


@dataclass
class ExecutionQuality:
    """Costs and feasibility.  Part of the strategy, not an afterthought."""
    spread_price: float
    spread_pips: float
    typical_spread_pips: float
    spread_ratio: float
    spread_percentile: float
    tick_age_seconds: float
    min_stop_distance_pips: float
    expected_slippage_pips: float
    tradable: bool
    reason: str = ""

    @property
    def cost_pips(self) -> float:
        """Round-trip cost estimate: spread plus expected slippage."""
        return self.spread_pips + self.expected_slippage_pips

    def score_0_100(self) -> float:
        if not self.tradable:
            return 0.0
        # 1.0x typical spread scores ~85; 3x scores ~25.
        ratio_score = max(0.0, min(100.0, 110.0 - 30.0 * self.spread_ratio))
        fresh = max(0.0, min(1.0, 1.0 - self.tick_age_seconds / 20.0))
        return round(0.75 * ratio_score + 25.0 * fresh, 1)


@dataclass
class SymbolContext:
    """Everything measured for one instrument at one moment.

    Built once per scan per symbol and shared by every tactic, so an expensive
    analysis is never repeated and every tactic sees an identical world.
    """
    symbol: str
    spec: SymbolSpec
    as_of: dt.datetime
    tick: Optional[Tick]
    data: MultiTFData
    structure: dict[TF, StructureReading] = field(default_factory=dict)
    momentum: dict[TF, MomentumReading] = field(default_factory=dict)
    regime: Optional[RegimeReading] = None
    session: Optional[SessionReading] = None
    strength: Optional[StrengthReading] = None
    cross: dict[int, CrossMarketReading] = field(default_factory=dict)
    news: Optional[NewsVerdict] = None
    execution: Optional[ExecutionQuality] = None
    vwap: Optional[float] = None
    atr_ref: float = 0.0            # ATR on the decision timeframe
    daily_atr: float = 0.0
    range_consumed: float = 0.0
    open_position: Optional[Position] = None
    decision_tf: TF = TF.M5

    # --- convenience ---------------------------------------------------------
    @property
    def price(self) -> float:
        if self.tick:
            return self.tick.mid
        c = self.data.last_close(self.decision_tf)
        return c or 0.0

    def st(self, tf: TF) -> Optional[StructureReading]:
        return self.structure.get(tf)

    def mo(self, tf: TF) -> Optional[MomentumReading]:
        return self.momentum.get(tf)

    def entry_price(self, side: Side) -> float:
        if self.tick:
            return self.tick.ask if side is Side.BUY else self.tick.bid
        return self.price


@dataclass
class MarketState:
    """The unified view the decision layer consumes."""
    symbol: str
    as_of: dt.datetime
    regime: Regime
    regime_label: str
    direction: int                     # +1 long, -1 short
    tactic: str
    evidence: dict[Family, Evidence]
    raw_score: float                   # weighted evidence, 0..100
    opportunity: float                 # after tactic/headroom/exhaustion mods
    tier: str
    entry: float
    stop: float
    target: Optional[float]
    reward_risk: Optional[float]
    headroom_pips: float
    cost_pips: float
    notes: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()

    @property
    def side(self) -> Side:
        return Side.BUY if self.direction > 0 else Side.SELL

    @property
    def tradable(self) -> bool:
        return not self.blockers and self.tier != "NO_TRADE"

    @property
    def stop_distance(self) -> float:
        return abs(self.entry - self.stop)

    def family(self, f: Family) -> float:
        e = self.evidence.get(f)
        return e.effective if e else 50.0

    def summary(self) -> str:
        d = "LONG" if self.direction > 0 else "SHORT"
        return (f"{self.symbol} {d} {self.opportunity:.0f} "
                f"[{self.regime_label}] via {self.tactic}")

    def explain(self) -> str:
        """Plain-English reasoning, used by the dashboard and the journal."""
        parts = [f"{self.symbol}: {self.regime_label} market. "
                 f"Best idea is {'buy' if self.direction > 0 else 'sell'} "
                 f"using {self.tactic.replace('_', ' ').lower()}."]
        ranked = sorted(self.evidence.items(),
                        key=lambda kv: -abs(kv[1].effective - 50.0))
        for fam, ev in ranked[:4]:
            word = "supports" if ev.effective >= 50 else "argues against"
            parts.append(f"{fam.value.replace('_', ' ').title()} {word} it "
                         f"({ev.effective:.0f}/100"
                         + (f"; {ev.note}" if ev.note else "") + ").")
        if self.reward_risk:
            parts.append(f"Room to the first obstacle is "
                         f"{self.headroom_pips:.0f} pips, giving about "
                         f"{self.reward_risk:.1f} to 1 after "
                         f"{self.cost_pips:.1f} pips of cost.")
        if self.blockers:
            parts.append("Blocked because: " + "; ".join(self.blockers) + ".")
        return " ".join(parts)

    def to_row(self) -> dict:
        return {
            "symbol": self.symbol, "as_of": self.as_of.isoformat(),
            "regime": self.regime.value, "direction": self.direction,
            "tactic": self.tactic, "raw_score": self.raw_score,
            "opportunity": self.opportunity, "tier": self.tier,
            "entry": self.entry, "stop": self.stop, "target": self.target,
            "reward_risk": self.reward_risk,
            "headroom_pips": self.headroom_pips, "cost_pips": self.cost_pips,
            "evidence": {f.value: round(e.effective, 2)
                         for f, e in self.evidence.items()},
            "evidence_confidence": {f.value: round(e.confidence, 3)
                                    for f, e in self.evidence.items()},
            "notes": list(self.notes), "blockers": list(self.blockers),
        }


def aggregate(evidence: dict[Family, Evidence], regime: Regime) -> float:
    """Weighted mean of family scores using regime-specific weights.

    Missing families are dropped and the remaining weights renormalised, so an
    unavailable data source does not silently drag the score toward zero.
    """
    weights = WEIGHTS.get(regime, WEIGHTS[Regime.UNCLEAR])
    num = den = 0.0
    for fam, ev in evidence.items():
        w = weights.get(fam, 0.0)
        if w <= 0:
            continue
        num += w * ev.effective
        den += w
    if den <= 0:
        return 50.0
    return round(num / den, 2)
