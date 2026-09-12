"""Tactic library.

Each tactic is a hypothesis about *how* to make money in a particular kind of
market.  A tactic is only allowed to compete when the regime it belongs to is
the one actually present, which is the whole point: the same instrument gets
traded as a breakout on a squeeze-release morning and as a range rejection in a
dead afternoon.

A tactic returns a :class:`TacticSignal` with:

* a direction,
* a *quality* in 0..100 - how well this specific setup matches its template,
* an entry reference and, crucially, a **structural invalidation price** - the
  level at which the idea is simply wrong.  Stops are derived from that, not
  from an arbitrary fixed distance.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional, Sequence

from ..broker.base import Bar, Side, TF
from ..data.momentum import MomentumReading
from ..data.regime import Regime
from ..data.structure import (StructureReading, Trend, assess_breakout,
                              tight_invalidation)
from .evidence import SymbolContext


@dataclass
class TacticSignal:
    name: str
    direction: int
    quality: float                  # 0..100
    invalidation: float             # price at which the idea is wrong
    entry_hint: float
    note: str = ""
    target_hint: Optional[float] = None
    requires_confirmation: bool = False

    @property
    def valid(self) -> bool:
        return self.direction != 0 and self.quality > 0 and self.invalidation > 0


@dataclass
class Tactic:
    name: str
    regimes: tuple[Regime, ...]
    evaluate: Callable[[SymbolContext, int], Optional[TacticSignal]]
    # Tactics that fade a move need tighter evidence than ones that join it,
    # because being early and being wrong look identical for a while.
    counter_trend: bool = False


def _median_volume(bars: Sequence[Bar]) -> float:
    vols = sorted(b.tick_volume for b in bars[-100:])
    return vols[len(vols) // 2] if vols else 0.0


# --------------------------------------------------------------- TREND -------

def momentum_continuation(ctx: SymbolContext, side: int) -> Optional[TacticSignal]:
    """Join an efficient, accelerating move that is still making new extremes."""
    tf = ctx.decision_tf
    mo, st = ctx.mo(tf), ctx.st(tf)
    bars = ctx.data.get(tf, 40)
    if not (mo and st and bars):
        return None
    if mo.direction != side:
        return None
    if mo.efficiency < 0.22 or mo.adx < 18:
        return None
    if mo.bars_since_extreme > 6:
        return None      # the impulse has stalled; that is a pullback, not this
    q = 0.45 * min(1.0, mo.efficiency / 0.55) + 0.25 * min(1.0, mo.adx / 35.0)
    q += 0.15 * min(1.0, max(0.0, mo.acceleration_atr * side) / 0.35)
    q += 0.15 * min(1.0, mo.body_strength / 0.7)
    # The base of the current impulse, not the last confirmed swing: a momentum
    # trade is wrong the moment price trades back through the leg it is riding.
    inval = tight_invalidation(bars, side, mo.atr)
    return TacticSignal("MOMENTUM_CONTINUATION", side, round(q * 100, 1),
                        inval, ctx.price,
                        f"efficiency {mo.efficiency:.2f}, ADX {mo.adx:.0f}, "
                        f"{mo.bars_since_extreme} bars since the extreme")


def trend_pullback(ctx: SymbolContext, side: int) -> Optional[TacticSignal]:
    """Buy the dip in an uptrend / sell the rally in a downtrend.

    Requires the pullback to be shallow enough to still be a pullback: past
    ~1.6 ATR of retracement it is a possible reversal, not a discount.
    """
    tf = ctx.decision_tf
    mo, st = ctx.mo(tf), ctx.st(tf)
    bars = ctx.data.get(tf, 40)
    if not (mo and st and bars):
        return None
    if st.direction_bias != side:
        return None
    if not (0.25 <= mo.pullback_depth_atr <= 1.6):
        return None
    if mo.pullback_bars < 2 or mo.pullback_bars > 14:
        return None
    last = bars[-1]
    # Want the pullback to be showing first signs of ending, not still falling.
    turning = (last.is_up if side > 0 else not last.is_up)
    q = 0.35 * min(1.0, mo.adx / 30.0)
    q += 0.25 * (1.0 - min(1.0, mo.pullback_depth_atr / 1.6))
    q += 0.20 * (1.0 if turning else 0.0)
    q += 0.20 * min(1.0, mo.efficiency / 0.45)
    # The pullback low is the natural invalidation, but only while it is close
    # enough to be a stop rather than a donation.
    swing = st.last_swing_low if side > 0 else st.last_swing_high
    inval = tight_invalidation(bars, side, mo.atr, max_atr=2.4)
    if swing is not None:
        swing_dist = (bars[-1].close - swing.price) * side
        if 0.4 * mo.atr <= swing_dist <= 2.4 * mo.atr:
            inval = swing.price
    return TacticSignal("TREND_PULLBACK", side, round(q * 100, 1), inval,
                        ctx.price,
                        f"{mo.pullback_depth_atr:.2f} ATR pullback over "
                        f"{mo.pullback_bars} bars into a "
                        f"{st.trend.value.lower()} structure",
                        requires_confirmation=not turning)


def breakout_acceptance(ctx: SymbolContext, side: int) -> Optional[TacticSignal]:
    """A confirmed close beyond a meaningful level, with participation.

    A wick through the level is explicitly rejected here - the level list
    includes session and previous-day extremes, not just recent swings.
    """
    tf = ctx.decision_tf
    st, mo = ctx.st(tf), ctx.mo(tf)
    bars = ctx.data.get(tf, 40)
    if not (st and mo and bars):
        return None
    candidates: list[tuple[str, float]] = []
    if side > 0:
        candidates.append(("range high", st.range_high))
    else:
        candidates.append(("range low", st.range_low))
    if ctx.session:
        for name, price in ctx.session.levels():
            is_high = name.endswith("_HIGH") or name == "PDH"
            if (side > 0) == is_high:
                candidates.append((name.lower().replace("_", " "), price))
    best: Optional[TacticSignal] = None
    med = _median_volume(bars)
    for label, level in candidates:
        if level <= 0:
            continue
        bq = assess_breakout(bars, level, side, atr_value=st.atr,
                             median_volume=med)
        if not bq.confirmed:
            continue
        # Do not buy a breakout 3 ATR after it broke; that is a chase, and the
        # stop would have to sit an unusable distance away.
        if (ctx.price - level) * side > 2.0 * st.atr:
            continue
        q = bq.score / 100.0
        q *= 0.75 + 0.25 * min(1.0, mo.efficiency / 0.45)
        if bq.retested:
            q = min(1.0, q * 1.12)      # break, retest, hold is the best version
        inval = level - side * 0.35 * st.atr
        sig = TacticSignal(
            "BREAKOUT_RETEST" if bq.retested else "BREAKOUT_ACCEPTANCE",
            side, round(q * 100, 1), inval, ctx.price,
            f"closed {bq.close_beyond_atr:.2f} ATR beyond the {label} "
            f"with {bq.body_pct * 100:.0f}% body and "
            f"{bq.participation:.1f}x median participation")
        if best is None or sig.quality > best.quality:
            best = sig
    return best


def session_expansion(ctx: SymbolContext, side: int) -> Optional[TacticSignal]:
    """Opening-range expansion: the session breaks and accepts its own range."""
    if not ctx.session or not ctx.session.opening_ranges:
        return None
    tf = ctx.decision_tf
    st, mo = ctx.st(tf), ctx.mo(tf)
    if not (st and mo):
        return None
    best: Optional[TacticSignal] = None
    for minutes, orr in sorted(ctx.session.opening_ranges.items()):
        if not orr.complete or orr.width <= 0:
            continue
        broke = orr.broke_up if side > 0 else orr.broke_down
        if not broke or not orr.accepted or orr.reclaimed:
            continue
        level = orr.high if side > 0 else orr.low
        beyond = (ctx.price - level) * side
        # A range far narrower than ATR is noise; far wider is already a move.
        width_atr = orr.width / max(st.atr, 1e-12)
        if width_atr < 0.35 or width_atr > 4.0:
            continue
        # Already far beyond the level: the expansion happened without us.
        if beyond > max(1.2 * orr.width, 2.0 * st.atr):
            continue
        q = 0.40 * min(1.0, beyond / max(0.5 * orr.width, 1e-12))
        q += 0.25 * min(1.0, mo.efficiency / 0.45)
        q += 0.20 * min(1.0, ctx.session.liquidity_score() / 100.0)
        q += 0.15 * min(1.0, mo.tick_participation / 1.5)
        inval = level - side * 0.25 * st.atr
        sig = TacticSignal("SESSION_EXPANSION", side, round(q * 100, 1), inval,
                           ctx.price,
                           f"accepted beyond the {minutes}-minute opening "
                           f"range ({orr.width / max(ctx.spec.pip_size,1e-12):.0f} pips wide)")
        if best is None or sig.quality > best.quality:
            best = sig
    return best


# ------------------------------------------------------------- SQUEEZE -------

def squeeze_release(ctx: SymbolContext, side: int) -> Optional[TacticSignal]:
    """Compressed volatility beginning to expand in one direction."""
    tf = ctx.decision_tf
    st, mo = ctx.st(tf), ctx.mo(tf)
    bars = ctx.data.get(tf, 40)
    if not (st and mo and bars):
        return None
    if st.compression < 0.35:
        return None
    if mo.vol_expansion < 1.15:
        return None          # still coiled; nothing to trade yet
    if mo.direction != side:
        return None
    q = 0.35 * min(1.0, st.compression / 0.7)
    q += 0.30 * min(1.0, (mo.vol_expansion - 1.0) / 1.0)
    q += 0.20 * min(1.0, mo.efficiency / 0.45)
    q += 0.15 * min(1.0, mo.tick_participation / 1.6)
    inval = tight_invalidation(bars, side, mo.atr, min_atr=0.5, max_atr=2.0)
    return TacticSignal("SQUEEZE_RELEASE", side, round(q * 100, 1), inval,
                        ctx.price,
                        f"compression {st.compression:.2f} releasing at "
                        f"{mo.vol_expansion:.2f}x ATR")


# --------------------------------------------------------------- RANGE -------

def range_rejection(ctx: SymbolContext, side: int) -> Optional[TacticSignal]:
    """Fade the edge of a proven range after an actual rejection wick."""
    tf = ctx.decision_tf
    st, mo = ctx.st(tf), ctx.mo(tf)
    bars = ctx.data.get(tf, 40)
    if not (st and mo and bars):
        return None
    if not st.in_range:
        return None
    span = st.range_high - st.range_low
    if span <= 0:
        return None
    pos = (ctx.price - st.range_low) / span      # 0 at low, 1 at high
    last = bars[-1]
    if side > 0:
        if pos > 0.28:
            return None
        rejected = last.lower_wick > 0.35 * max(last.range, 1e-12) and last.is_up
        edge = st.range_low
    else:
        if pos < 0.72:
            return None
        rejected = last.upper_wick > 0.35 * max(last.range, 1e-12) and not last.is_up
        edge = st.range_high
    if not rejected:
        return None
    touches = sum(1 for lv in st.levels if abs(lv.price - edge) < 0.4 * st.atr)
    q = 0.35 * (1.0 - min(1.0, abs(pos - (0.0 if side > 0 else 1.0)) / 0.28))
    q += 0.25 * min(1.0, touches / 3.0)
    q += 0.20 * min(1.0, span / (3.0 * st.atr))
    q += 0.20 * (1.0 - min(1.0, mo.adx / 30.0))   # a range needs a LOW ADX
    inval = edge - side * 0.4 * st.atr
    return TacticSignal("RANGE_REJECTION", side, round(q * 100, 1), inval,
                        ctx.price,
                        f"rejected the range {'low' if side > 0 else 'high'} "
                        f"with ADX only {mo.adx:.0f}",
                        target_hint=st.range_high if side > 0 else st.range_low)


def failed_breakout(ctx: SymbolContext, side: int) -> Optional[TacticSignal]:
    """Price broke a level, failed, and closed back inside.

    One of the highest-quality patterns available because the invalidation is
    unusually tight and obvious: the failed extreme itself.
    """
    tf = ctx.decision_tf
    st = ctx.st(tf)
    bars = ctx.data.get(tf, 40)
    if not (st and bars):
        return None
    reclaim = st.reclaim_up if side > 0 else st.reclaim_down
    or_failed = None
    if ctx.session:
        for minutes, orr in sorted(ctx.session.opening_ranges.items()):
            if not orr.reclaimed:
                continue
            if side > 0 and orr.broke_down:
                or_failed = (minutes, orr.low)
            elif side < 0 and orr.broke_up:
                or_failed = (minutes, orr.high)
    if not reclaim and not or_failed:
        return None
    last = bars[-1]
    if side > 0 and not last.is_up:
        return None
    if side < 0 and last.is_up:
        return None
    if or_failed:
        minutes, level = or_failed
        extreme = (min(b.low for b in bars[-10:]) if side > 0
                   else max(b.high for b in bars[-10:]))
        note = (f"broke the {minutes}-minute opening range "
                f"{'low' if side > 0 else 'high'} and reclaimed it")
    else:
        level = st.sweep_level or (st.range_low if side > 0 else st.range_high)
        extreme = (min(b.low for b in bars[-8:]) if side > 0
                   else max(b.high for b in bars[-8:]))
        note = (f"swept the {'low' if side > 0 else 'high'} at "
                f"{level:.{ctx.spec.digits}f} and closed back through it")
    overshoot = abs(level - extreme) / max(st.atr, 1e-12)
    if abs(bars[-1].close - extreme) > 2.6 * st.atr:
        extreme = tight_invalidation(bars, side, st.atr, max_atr=2.6)
    q = 0.40 * min(1.0, overshoot / 0.8)          # a real sweep, not a graze
    q += 0.25 * last.body_pct
    q += 0.20 * (1.0 if st.in_range else 0.6)
    q += 0.15 * min(1.0, last.tick_volume / max(_median_volume(bars), 1e-9) / 1.5)
    inval = extreme - side * 0.2 * st.atr
    return TacticSignal("FAILED_BREAKOUT_RECLAIM", side, round(q * 100, 1),
                        inval, ctx.price, note)


def liquidity_sweep_reversal(ctx: SymbolContext, side: int) -> Optional[TacticSignal]:
    """A sweep of an obvious level followed by a structural break the other way.

    Distinct from ``failed_breakout``: this one demands the reversal be
    *confirmed* by a break of structure, which makes it later but stronger.
    """
    tf = ctx.decision_tf
    st = ctx.st(tf)
    if not st:
        return None
    reclaim = st.reclaim_up if side > 0 else st.reclaim_down
    bos = st.bos_up if side > 0 else st.bos_down
    if not (reclaim and bos):
        return None
    if st.bos_bars_ago is None or st.bos_bars_ago > 6:
        return None
    bars = ctx.data.get(tf, 30)
    extreme = (min(b.low for b in bars[-10:]) if side > 0
               else max(b.high for b in bars[-10:]))
    # The swept extreme is the true invalidation, but if the sweep was violent
    # the stop it implies can be unusable; fall back to a tighter anchor.
    if abs(bars[-1].close - extreme) > 2.4 * st.atr:
        extreme = tight_invalidation(bars, side, st.atr, max_atr=2.4)
    q = 0.5 + 0.3 * (1.0 - min(1.0, st.bos_bars_ago / 6.0))
    q += 0.2 * min(1.0, st.expansion / 1.6)
    return TacticSignal("LIQUIDITY_SWEEP_REVERSAL", side,
                        round(min(1.0, q) * 100, 1),
                        extreme - side * 0.2 * st.atr, ctx.price,
                        f"swept {st.sweep_level:.{ctx.spec.digits}f} then broke "
                        f"structure {st.bos_bars_ago} bars ago"
                        if st.sweep_level else "sweep then structural break")


def vwap_reversion(ctx: SymbolContext, side: int) -> Optional[TacticSignal]:
    """Stretched from session VWAP in a non-trending market.

    Only offered where VWAP is actually available and the market is not
    trending - reverting to VWAP inside a trend is how accounts die.
    """
    tf = ctx.decision_tf
    st, mo = ctx.st(tf), ctx.mo(tf)
    if not (st and mo and ctx.vwap):
        return None
    if mo.adx >= 22 or st.trend in (Trend.UP, Trend.DOWN):
        return None
    dist = (ctx.vwap - ctx.price) / max(st.atr, 1e-12)
    if abs(dist) < 1.0:
        return None
    if (dist > 0) != (side > 0):
        return None
    bars = ctx.data.get(tf, 20)
    q = 0.5 * min(1.0, abs(dist) / 2.5)
    q += 0.25 * (1.0 - min(1.0, mo.adx / 22.0))
    q += 0.25 * (1.0 - min(1.0, mo.efficiency / 0.3))
    inval = tight_invalidation(bars, side, st.atr, min_atr=0.5, max_atr=1.8)
    return TacticSignal("VWAP_REVERSION", side, round(q * 100, 1), inval,
                        ctx.price,
                        f"{abs(dist):.1f} ATR from session VWAP with ADX "
                        f"{mo.adx:.0f}", target_hint=ctx.vwap)


# ---------------------------------------------------------------- NEWS -------

def news_continuation(ctx: SymbolContext, side: int) -> Optional[TacticSignal]:
    """Trade the direction the market chose after a release.

    Never the headline's direction - the news verdict has already resolved that
    conflict in favour of price.
    """
    nv = ctx.news
    tf = ctx.decision_tf
    st = ctx.st(tf)
    if not (nv and st and nv.reaction):
        return None
    if nv.bias != side or nv.blackout:
        return None
    from ..news.engine import Wave
    if nv.wave not in (Wave.FIRST_WAVE, Wave.SECOND_WAVE, Wave.REJECTED):
        return None
    r = nv.reaction
    q = 0.35 * min(1.0, r.jump_atr / 1.2)
    q += 0.25 * min(1.0, r.persistence)
    q += 0.20 * (1.0 if r.spread_normalised else 0.0)
    q += 0.20 * min(1.0, r.participation / 1.8)
    if nv.wave is Wave.SECOND_WAVE:
        q = min(1.0, q * 1.1)
    bars = ctx.data.get(tf, 20)
    inval = tight_invalidation(bars, side, st.atr, min_atr=0.6, max_atr=2.4)
    return TacticSignal("NEWS_CONTINUATION", side, round(q * 100, 1), inval,
                        ctx.price,
                        f"{nv.wave.value.replace('_', ' ').lower()} after "
                        f"{nv.event.currency} {nv.event.name}"
                        if nv.event else "post-release continuation")


# ------------------------------------------------------------- registry ------

TACTICS: tuple[Tactic, ...] = (
    Tactic("MOMENTUM_CONTINUATION",
           (Regime.TREND, Regime.HIGH_VOL, Regime.NEWS), momentum_continuation),
    Tactic("TREND_PULLBACK", (Regime.TREND, Regime.LOW_VOL), trend_pullback),
    Tactic("BREAKOUT_ACCEPTANCE",
           (Regime.TREND, Regime.SQUEEZE, Regime.HIGH_VOL, Regime.UNCLEAR),
           breakout_acceptance),
    Tactic("SESSION_EXPANSION",
           (Regime.TREND, Regime.HIGH_VOL, Regime.SQUEEZE, Regime.UNCLEAR),
           session_expansion),
    Tactic("SQUEEZE_RELEASE", (Regime.SQUEEZE, Regime.LOW_VOL), squeeze_release),
    Tactic("RANGE_REJECTION", (Regime.RANGE, Regime.LOW_VOL), range_rejection,
           counter_trend=True),
    Tactic("FAILED_BREAKOUT_RECLAIM",
           (Regime.RANGE, Regime.HIGH_VOL, Regime.UNCLEAR, Regime.TREND),
           failed_breakout, counter_trend=True),
    Tactic("LIQUIDITY_SWEEP_REVERSAL",
           (Regime.RANGE, Regime.TREND, Regime.HIGH_VOL, Regime.UNCLEAR),
           liquidity_sweep_reversal, counter_trend=True),
    Tactic("VWAP_REVERSION", (Regime.RANGE, Regime.LOW_VOL), vwap_reversion,
           counter_trend=True),
    Tactic("NEWS_CONTINUATION", (Regime.NEWS, Regime.HIGH_VOL, Regime.TREND),
           news_continuation),
)


def candidates_for(regime: Regime) -> tuple[Tactic, ...]:
    return tuple(t for t in TACTICS if regime in t.regimes)


def best_signal(ctx: SymbolContext, side: int,
                regime: Regime) -> Optional[TacticSignal]:
    """Highest-quality applicable tactic for one direction.

    Counter-trend tactics are held to a higher bar because fading a move looks
    identical to being wrong right up until it works.
    """
    best: Optional[TacticSignal] = None
    for tactic in candidates_for(regime):
        try:
            sig = tactic.evaluate(ctx, side)
        except Exception:
            continue
        if sig is None or not sig.valid:
            continue
        if tactic.counter_trend and sig.quality < 45.0:
            continue
        if sig.quality < 25.0:
            continue
        if best is None or sig.quality > best.quality:
            best = sig
    return best
