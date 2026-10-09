"""Market regime from the order flow, and the tactic that goes with it.

Deterministic rules on the features of :mod:`mintel.ian.features`, checked
in this order (the first that matches wins):

1. NEWS SHOCK       a scheduled high-impact release within the last few
                   minutes and the book disturbed (spread >= 2x normal, depth
                   <= 50%, trade intensity >= 4x or volatility >= 3x), or an
                   unscheduled shock (intensity >= 6x with the spread >= 2.5x
                   or depth <= 40%, and a move of >= 4 ticks in 5 s).
2. LIQUIDITY VACUUM depth <= 35% of normal and the spread >= 2x normal.
3. LOW LIQUIDITY   depth <= 60% of normal or the spread >= 1.6x normal.
4. BREAKOUT        a real sweep in the last 15 s, none the other way in the
                   last 30 s, with the medium-window aggression on the same
                   side (>= 0.25), or a move of >= 3
                   ticks in 5 s at >= 2x intensity with >= 0.5 aggression the
                   same way.
5. HIGH VOLATILITY realised volatility >= 2.5x normal.
6. REVERSAL        an absorption episode in the last 90 s, and price now
                   moving the absorber's way (>= 2 ticks in 30 s, the last 5 s
                   of aggression agreeing); or a delta divergence with the
                   aggression and the price already turned.
7. ABSORPTION      ABSORPTION_SCORE >= 0.6.
8. TRENDING        >= 8 ticks over 120 s, aggression the same way (>= 0.2),
                   the VWAP slope agreeing.
9. BALANCED        none of the above.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .features import FlowSnapshot

BALANCED = "BALANCED"
TRENDING = "TRENDING"
BREAKOUT = "BREAKOUT"
HIGH_VOLATILITY = "HIGH VOLATILITY"
LOW_LIQUIDITY = "LOW LIQUIDITY"
NEWS_SHOCK = "NEWS SHOCK"
ABSORPTION = "ABSORPTION"
LIQUIDITY_VACUUM = "LIQUIDITY VACUUM"
REVERSAL = "REVERSAL"
UNKNOWN = "UNKNOWN"

REGIMES = (BALANCED, TRENDING, BREAKOUT, HIGH_VOLATILITY, LOW_LIQUIDITY, NEWS_SHOCK, ABSORPTION,
           LIQUIDITY_VACUUM, REVERSAL)

TACTICS = {
    BALANCED: "Two-sided market: only a clearly one-sided book and tape is worth a trade (a higher score is needed).",
    TRENDING: "Trend: trade with the side doing the taking; against the trend only after absorption or a reversal.",
    BREAKOUT: "Breakout: join the side that swept the book while the swept liquidity does not come back.",
    HIGH_VOLATILITY: "Volatile: a higher score and a wider stop, smaller in effect.",
    LOW_LIQUIDITY: "Thin book: confidence is cut; signals in a thin book are less reliable.",
    NEWS_SHOCK: "News shock: never chase the first print; wait for the spread and depth to recover and the flow to settle.",
    ABSORPTION: "Absorption: the passive side is soaking up the aggressor; lean with the absorber once price turns.",
    LIQUIDITY_VACUUM: "Liquidity vacuum: no new trades; nothing in the book can be trusted until depth returns.",
    REVERSAL: "Reversal: the side that was absorbed has given up; follow the new flow.",
    UNKNOWN: "No reliable book yet.",
}


@dataclass
class RegimeRead:
    regime: str
    reasons: list = field(default_factory=list)
    direction: int = 0              # the regime's own direction where it has one (breakout, trend, reversal, absorption)
    allow_entry: bool = True
    confidence_mult: float = 1.0
    extra_score: float = 0.0        # added to the entry threshold in this regime

    @property
    def tactic(self) -> str:
        return TACTICS.get(self.regime, "")

    def to_dict(self) -> dict:
        return {"regime": self.regime, "reasons": list(self.reasons), "direction": self.direction,
                "allow_entry": self.allow_entry, "confidence_mult": self.confidence_mult,
                "extra_score": self.extra_score, "tactic": self.tactic}


@dataclass
class NewsContext:
    """What the MT5 calendar says about the currencies of one future (RETAIL source)."""
    seconds_since_release: Optional[float] = None    # the latest high-impact release for either currency
    seconds_to_next: Optional[float] = None          # the next one
    event: str = ""
    futures_effect: int = 0                          # +1 / -1: what the surprise means for the FUTURE's price
    surprise: Optional[float] = None


def _s(x: float) -> int:
    return (x > 0) - (x < 0)


def classify(fs: FlowSnapshot, news: Optional[NewsContext] = None, absorption_episodes=(),
             news_window_s: float = 300.0) -> RegimeRead:
    if not fs.ok:
        return RegimeRead(UNKNOWN, [fs.reason or "no reliable book"], allow_entry=False, confidence_mult=0.0)
    news = news or NewsContext()

    recent_release = news.seconds_since_release is not None and 0 <= news.seconds_since_release <= news_window_s
    disturbed = (fs.spread_ratio >= 2.0 or fs.depth_ratio <= 0.5 or fs.intensity >= 4.0 or fs.vol_ratio >= 3.0)
    if recent_release and disturbed:
        return RegimeRead(NEWS_SHOCK, [f"{news.event or 'a high-impact release'} {news.seconds_since_release:.0f} s ago; "
                                       f"spread {fs.spread_ratio:.1f}x, depth {fs.depth_ratio:.0%}, intensity {fs.intensity:.1f}x"],
                          direction=_s(fs.move_short_ticks), allow_entry=False, confidence_mult=0.0)
    if fs.intensity >= 6.0 and (fs.spread_ratio >= 2.5 or fs.depth_ratio <= 0.4) and abs(fs.move_short_ticks) >= 4:
        return RegimeRead(NEWS_SHOCK, [f"unscheduled shock: intensity {fs.intensity:.1f}x, spread {fs.spread_ratio:.1f}x, "
                                       f"{fs.move_short_ticks:+.0f} ticks in 5 s"],
                          direction=_s(fs.move_short_ticks), allow_entry=False, confidence_mult=0.0)
    if fs.depth_ratio <= 0.35 and fs.spread_ratio >= 2.0:
        return RegimeRead(LIQUIDITY_VACUUM, [f"depth {fs.depth_ratio:.0%} of normal, spread {fs.spread_ratio:.1f}x"],
                          allow_entry=False, confidence_mult=0.0)
    if fs.depth_ratio <= 0.6 or fs.spread_ratio >= 1.6:
        return RegimeRead(LOW_LIQUIDITY, [f"depth {fs.depth_ratio:.0%} of normal, spread {fs.spread_ratio:.1f}x"],
                          confidence_mult=0.7, extra_score=5.0)
    one_way = (fs.sweeps_down == 0) if fs.sweep_dir > 0 else (fs.sweeps_up == 0)
    if fs.sweep_dir and fs.sweep_age_s is not None and fs.sweep_age_s <= 15 and one_way and \
            fs.aggr_medium * fs.sweep_dir >= 0.25:
        return RegimeRead(BREAKOUT, [f"a {'buyer' if fs.sweep_dir > 0 else 'seller'} swept {fs.sweep_levels} levels "
                                     f"{fs.sweep_age_s:.0f} s ago"], direction=fs.sweep_dir)
    if abs(fs.move_short_ticks) >= 3 and fs.intensity >= 2.0 and fs.aggr_short * _s(fs.move_short_ticks) >= 0.5:
        return RegimeRead(BREAKOUT, [f"{fs.move_short_ticks:+.0f} ticks in 5 s at {fs.intensity:.1f}x the normal pace"],
                          direction=_s(fs.move_short_ticks))
    if fs.vol_ratio >= 2.5:
        return RegimeRead(HIGH_VOLATILITY, [f"volatility {fs.vol_ratio:.1f}x normal"], confidence_mult=0.8,
                          extra_score=10.0)
    for ep in reversed(list(absorption_episodes)):
        age = (fs.ts - ep["ts"]).total_seconds()
        if age > 90:
            break
        d = ep["dir"]
        if fs.move_medium_ticks * d >= 2 and fs.aggr_short * d >= 0.2 and fs.absorption_score < 0.6:
            return RegimeRead(REVERSAL, [f"absorption {age:.0f} s ago, price now {fs.move_medium_ticks:+.0f} ticks the absorber's way"],
                              direction=d)
    if fs.delta_divergence and fs.aggr_short * fs.delta_divergence >= 0.2 and \
            fs.move_short_ticks * fs.delta_divergence >= 1:
        return RegimeRead(REVERSAL, [f"delta divergence with the aggression turned ({fs.aggr_medium:+.2f})"],
                          direction=fs.delta_divergence)
    if fs.absorption_score >= 0.6:
        who = "sellers absorbing buyers" if fs.absorption_dir < 0 else "buyers absorbing sellers"
        return RegimeRead(ABSORPTION, [f"{who} (score {fs.absorption_score:.2f})"], direction=fs.absorption_dir)
    if abs(fs.move_long_ticks) >= 8 and fs.aggr_medium * _s(fs.move_long_ticks) >= 0.2 and \
            _s(fs.vwap_slope) in (0, _s(fs.move_long_ticks)):
        return RegimeRead(TRENDING, [f"{fs.move_long_ticks:+.0f} ticks in 2 min with the aggression the same way"],
                          direction=_s(fs.move_long_ticks))
    return RegimeRead(BALANCED, ["no one-sided pressure, normal depth and spread"], extra_score=10.0)
