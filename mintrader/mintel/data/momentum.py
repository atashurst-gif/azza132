"""Momentum engine.

The central idea: **displacement is not the same as movement.**  A market that
travels 20 pips in a straight line and one that thrashes 80 pips to end 20 pips
up have identical net change and completely different tradability.  The
efficiency measure separates them, and it is the single most load-bearing
number in this module.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, asdict
from typing import Optional, Sequence

from ..broker.base import Bar
from ..contracts import SymbolSpec
from .series import (adx_dmi, atr, atr_series, ema, linreg_slope,
                     percentile_rank, stdev, true_ranges)


@dataclass
class MomentumReading:
    # --- displacement & path -------------------------------------------------
    net_displacement: float          # signed, price units
    path_distance: float             # sum of |bar-to-bar| moves
    efficiency: float                # |net| / path, 0..1
    direction: int                   # +1 / -1 / 0
    # --- rates ---------------------------------------------------------------
    velocity_atr: float              # net displacement per bar, in ATR
    acceleration_atr: float          # change in velocity, in ATR
    roc_pct: float
    ema_slope_atr: float
    ema_separation_atr: float
    # --- trend quality -------------------------------------------------------
    adx: float
    plus_di: float
    minus_di: float
    body_strength: float             # mean body/range of aligned bars, 0..1
    wick_rejection: float            # adverse wick pressure, 0..1
    # --- rhythm --------------------------------------------------------------
    bars_since_extreme: int
    pullback_depth_atr: float
    pullback_bars: int
    # --- volatility & participation -----------------------------------------
    atr: float
    realised_vol: float
    vol_percentile: float            # 0..1 vs own history
    vol_expansion: float             # current TR vs ATR
    tick_participation: float        # current volume vs median
    tick_acceleration: float

    def score_0_100(self) -> float:
        """Compress the reading into one directional-quality score.

        Weighted toward *efficiency* and *ADX* because they are what separate a
        tradable impulse from noise; capped contributions stop any single
        component from dominating.
        """
        if self.direction == 0:
            return 0.0
        eff = _clamp01(self.efficiency)
        adx_n = _clamp01(self.adx / 45.0)
        vel = _clamp01(abs(self.velocity_atr) / 0.55)
        acc = _clamp01(0.5 + self.acceleration_atr * self.direction / 0.6)
        body = _clamp01(self.body_strength)
        di_align = _clamp01(
            ((self.plus_di - self.minus_di) * self.direction) / 30.0 * 0.5 + 0.5)
        slope = _clamp01(abs(self.ema_slope_atr) / 0.35)
        part = _clamp01(self.tick_participation / 1.8)
        penalty = 0.6 * _clamp01(self.wick_rejection)
        raw = (0.26 * eff + 0.18 * adx_n + 0.14 * vel + 0.10 * acc
               + 0.10 * body + 0.08 * di_align + 0.08 * slope + 0.06 * part)
        return round(max(0.0, min(1.0, raw - 0.12 * penalty)) * 100.0, 2)

    def as_dict(self) -> dict:
        return asdict(self)


def _clamp01(x: float) -> float:
    return 0.0 if x < 0 else (1.0 if x > 1 else x)


def measure(bars: Sequence[Bar], *, lookback: int = 20,
            atr_period: int = 14,
            vol_history: int = 200) -> Optional[MomentumReading]:
    """Compute a momentum reading over the last ``lookback`` bars."""
    if len(bars) < max(lookback + 2, atr_period + 2, 30):
        return None
    window = list(bars[-lookback:])
    a = atr(bars[-(atr_period * 4):], atr_period) or 1e-12

    net = window[-1].close - window[0].open
    path = sum(abs(window[i].close - window[i - 1].close)
               for i in range(1, len(window))) + abs(window[0].close - window[0].open)
    efficiency = abs(net) / path if path > 0 else 0.0
    direction = 1 if net > 0 else (-1 if net < 0 else 0)

    velocity = (net / len(window)) / a
    half = len(window) // 2
    v1 = (window[half - 1].close - window[0].open) / max(half, 1) / a
    v2 = (window[-1].close - window[half - 1].close) / max(len(window) - half, 1) / a
    acceleration = v2 - v1

    closes = [b.close for b in bars]
    roc = ((closes[-1] / closes[-lookback] - 1.0) * 100.0
           if closes[-lookback] else 0.0)
    e_fast, e_slow = ema(closes, 12), ema(closes, 34)
    ema_slope = linreg_slope(e_fast[-min(10, len(e_fast)):]) / a
    ema_sep = (e_fast[-1] - e_slow[-1]) / a

    adx, pdi, mdi = adx_dmi(bars[-min(len(bars), 120):], atr_period)

    aligned = [b for b in window if (b.is_up and direction > 0)
               or ((not b.is_up) and direction < 0)]
    body_strength = (sum(b.body_pct for b in aligned) / len(aligned)
                     if aligned else 0.0)
    adverse = sum((b.upper_wick if direction > 0 else b.lower_wick)
                  for b in window)
    total_rng = sum(b.range for b in window) or 1e-12
    wick_rejection = adverse / total_rng

    if direction >= 0:
        extreme_i = max(range(len(window)), key=lambda i: window[i].high)
        extreme = window[extreme_i].high
        trough = min(b.low for b in window[extreme_i:]) if extreme_i < len(window) else extreme
        pullback = (extreme - window[-1].close)
        pullback_depth = max(0.0, extreme - trough) / a
    else:
        extreme_i = min(range(len(window)), key=lambda i: window[i].low)
        extreme = window[extreme_i].low
        peak = max(b.high for b in window[extreme_i:]) if extreme_i < len(window) else extreme
        pullback = (window[-1].close - extreme)
        pullback_depth = max(0.0, peak - extreme) / a
    bars_since_extreme = len(window) - 1 - extreme_i
    pullback_bars = bars_since_extreme

    rets = [(closes[i] / closes[i - 1] - 1.0) for i in range(-lookback, 0)
            if closes[i - 1]]
    realised = stdev(rets) * math.sqrt(len(rets)) if rets else 0.0
    tr = true_ranges(bars[-min(len(bars), vol_history):])
    vol_pct = percentile_rank(tr[-1], tr) if tr else 0.5
    vol_expansion = (tr[-1] / a) if a else 0.0

    vols = [max(b.tick_volume, 0.0) for b in bars[-min(len(bars), 100):]]
    med = sorted(vols)[len(vols) // 2] if vols else 0.0
    participation = (window[-1].tick_volume / med) if med > 0 else 1.0
    recent = vols[-5:] if len(vols) >= 10 else vols
    prior = vols[-10:-5] if len(vols) >= 10 else vols
    tick_accel = ((sum(recent) / max(len(recent), 1))
                  / max(sum(prior) / max(len(prior), 1), 1e-9) - 1.0)

    return MomentumReading(
        net_displacement=net, path_distance=path, efficiency=efficiency,
        direction=direction, velocity_atr=velocity,
        acceleration_atr=acceleration, roc_pct=roc, ema_slope_atr=ema_slope,
        ema_separation_atr=ema_sep, adx=adx, plus_di=pdi, minus_di=mdi,
        body_strength=body_strength, wick_rejection=wick_rejection,
        bars_since_extreme=bars_since_extreme,
        pullback_depth_atr=pullback_depth, pullback_bars=pullback_bars,
        atr=a, realised_vol=realised, vol_percentile=vol_pct,
        vol_expansion=vol_expansion, tick_participation=participation,
        tick_acceleration=tick_accel)


def range_consumed(bars: Sequence[Bar], reference_atr: float) -> float:
    """How much of a typical period's range this period has already used.

    Returned as a ratio, so 1.0 means "an average day's range is already done".
    Used to *discount* late entries, never as a hard veto: a genuine trend day
    routinely prints 2-3x ATR and refusing to trade it would be the expensive
    mistake.
    """
    if not bars or reference_atr <= 0:
        return 0.0
    hi = max(b.high for b in bars)
    lo = min(b.low for b in bars)
    return (hi - lo) / reference_atr


def exhaustion_discount(consumed: float, momentum: Optional[MomentumReading]) -> float:
    """Multiplier in [0.45, 1.0] applied to a continuation opportunity.

    Exhaustion alone is not disqualifying.  A market 2.5x through its ATR with
    *decaying* momentum is a late trade; the same extension with accelerating,
    highly efficient momentum is a trend day worth joining, so strong momentum
    recovers most of the discount.
    """
    if consumed <= 1.0:
        return 1.0
    base = max(0.45, 1.0 - 0.22 * (consumed - 1.0))
    if momentum is None:
        return base
    quality = _clamp01(0.6 * momentum.efficiency
                       + 0.4 * _clamp01(momentum.adx / 40.0))
    # The test is whether momentum is DECAYING, not whether it is still
    # accelerating.  A steady, highly efficient trend travelling at constant
    # speed is exactly the move worth joining late, and penalising it for
    # failing to accelerate would be the wrong discipline.
    decaying = (momentum.acceleration_atr * momentum.direction) < -0.05
    recovery = quality * (0.5 if decaying else 0.85)
    return min(1.0, base + (1.0 - base) * recovery)
