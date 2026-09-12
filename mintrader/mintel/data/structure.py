"""Price-action structure.

This module is deliberately the first thing the engine consults, before any
indicator.  It answers questions a discretionary trader would ask by eye:

* Is this making higher highs and higher lows, or lower highs and lower lows?
* Has structure actually broken, or did a wick poke through and get rejected?
* Where are the levels price will collide with?
* Is the market compressed or expanding?
* Did a level get swept and then reclaimed (liquidity sweep -> reversal)?
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Sequence

from ..broker.base import Bar
from .series import atr


class Trend(str, Enum):
    UP = "UP"
    DOWN = "DOWN"
    RANGE = "RANGE"
    UNCLEAR = "UNCLEAR"


@dataclass(frozen=True)
class Swing:
    index: int
    price: float
    is_high: bool
    bar_time: object = None


@dataclass
class Level:
    price: float
    kind: str             # SWING_HIGH, SWING_LOW, SESSION_HIGH, PDH, ROUND, ...
    touches: int = 1
    strength: float = 0.0     # 0..1, how much respect price has shown it
    source_tf: str = ""

    def distance(self, price: float) -> float:
        return abs(self.price - price)


@dataclass
class StructureReading:
    trend: Trend
    swings: list[Swing]
    last_swing_high: Optional[Swing]
    last_swing_low: Optional[Swing]
    higher_highs: bool
    higher_lows: bool
    lower_highs: bool
    lower_lows: bool
    bos_up: bool                 # break of structure upward, by close
    bos_down: bool
    bos_bars_ago: Optional[int]
    reclaim_up: bool             # swept below then closed back above
    reclaim_down: bool
    sweep_level: Optional[float]
    compression: float           # 0..1, 1 = extremely compressed
    expansion: float             # current range vs recent mean
    range_high: float
    range_low: float
    in_range: bool
    levels: list[Level] = field(default_factory=list)
    atr: float = 0.0

    @property
    def direction_bias(self) -> int:
        if self.trend is Trend.UP:
            return 1
        if self.trend is Trend.DOWN:
            return -1
        return 0

    def score_0_100(self, side_sign: int) -> float:
        """Structural support for trading in ``side_sign`` direction."""
        s = 50.0
        if side_sign > 0:
            s += 18 if self.higher_highs else -10
            s += 14 if self.higher_lows else -8
            s += 16 if self.bos_up else 0
            s += 10 if self.reclaim_up else 0
            if self.trend is Trend.DOWN:
                s -= 16
        elif side_sign < 0:
            s += 18 if self.lower_lows else -10
            s += 14 if self.lower_highs else -8
            s += 16 if self.bos_down else 0
            s += 10 if self.reclaim_down else 0
            if self.trend is Trend.UP:
                s -= 16
        if self.bos_bars_ago is not None and self.bos_bars_ago > 14:
            s -= 8          # a stale break is not a fresh break
        return max(0.0, min(100.0, s))


def find_swings(bars: Sequence[Bar], left: int = 2, right: int = 2) -> list[Swing]:
    """Fractal swing detection.

    ``right`` bars of confirmation are required, so the most recent bars cannot
    produce a swing yet.  That lag is intentional: an unconfirmed swing is a
    guess, and trading off guessed structure is how false breaks get bought.
    """
    out: list[Swing] = []
    n = len(bars)
    for i in range(left, n - right):
        hi = bars[i].high
        lo = bars[i].low
        if (all(bars[j].high <= hi for j in range(i - left, i))
                and all(bars[j].high < hi for j in range(i + 1, i + right + 1))):
            out.append(Swing(i, hi, True, bars[i].time))
        if (all(bars[j].low >= lo for j in range(i - left, i))
                and all(bars[j].low > lo for j in range(i + 1, i + right + 1))):
            out.append(Swing(i, lo, False, bars[i].time))
    out.sort(key=lambda s: s.index)
    return out


def cluster_levels(swings: Sequence[Swing], tolerance: float,
                   tf_name: str = "") -> list[Level]:
    """Merge nearby swings into levels; repeated touches raise strength."""
    levels: list[Level] = []
    for sw in swings:
        hit = None
        for lv in levels:
            if abs(lv.price - sw.price) <= tolerance:
                hit = lv
                break
        if hit:
            hit.touches += 1
            hit.price = (hit.price * (hit.touches - 1) + sw.price) / hit.touches
            hit.strength = min(1.0, 0.3 + 0.2 * hit.touches)
        else:
            levels.append(Level(price=sw.price,
                                kind="SWING_HIGH" if sw.is_high else "SWING_LOW",
                                touches=1, strength=0.3, source_tf=tf_name))
    return levels


def analyse(bars: Sequence[Bar], *, lookback: int = 120, left: int = 2,
            right: int = 2, tf_name: str = "") -> Optional[StructureReading]:
    if len(bars) < 40:
        return None
    win = list(bars[-lookback:])
    a = atr(bars[-min(len(bars), 200):], 14) or 1e-12
    swings = find_swings(win, left, right)
    highs = [s for s in swings if s.is_high]
    lows = [s for s in swings if not s.is_high]
    lsh = highs[-1] if highs else None
    lsl = lows[-1] if lows else None

    hh = len(highs) >= 2 and highs[-1].price > highs[-2].price
    lh = len(highs) >= 2 and highs[-1].price < highs[-2].price
    hl = len(lows) >= 2 and lows[-1].price > lows[-2].price
    ll = len(lows) >= 2 and lows[-1].price < lows[-2].price

    # --- break of structure, confirmed by a CLOSE beyond the swing -----------
    # For each recent bar we compare its close against the most recent swing
    # that was already *confirmed* at that time (index < k - right).  Using a
    # swing the bar itself helped create would let every impulse leg count as a
    # structural break.
    def _bos_scan(want_high: bool) -> Optional[int]:
        pool = highs if want_high else lows
        for k in range(len(win) - 1, max(len(win) - 26, 0) - 1, -1):
            prior = [s for s in pool if s.index <= k - right - 1]
            if not prior:
                continue
            ref = prior[-1].price
            if (win[k].close > ref) if want_high else (win[k].close < ref):
                return len(win) - 1 - k
        return None

    up_ago = _bos_scan(True)
    down_ago = _bos_scan(False)
    bos_up = bos_down = False
    bos_bars_ago: Optional[int] = None
    if up_ago is not None and (down_ago is None or up_ago <= down_ago):
        bos_up, bos_bars_ago = True, up_ago
    elif down_ago is not None:
        bos_down, bos_bars_ago = True, down_ago

    # --- liquidity sweep then reclaim ---------------------------------------
    reclaim_up = reclaim_down = False
    sweep_level: Optional[float] = None
    tail = win[-8:]
    if lsl is not None:
        pierced = any(b.low < lsl.price for b in tail)
        if pierced and tail[-1].close > lsl.price:
            reclaim_up = True
            sweep_level = lsl.price
    if lsh is not None:
        pierced = any(b.high > lsh.price for b in tail)
        if pierced and tail[-1].close < lsh.price:
            reclaim_down = True
            sweep_level = lsh.price

    # --- trend classification ------------------------------------------------
    # A trend needs agreement from BOTH extremes plus a structural break in the
    # same direction.  Requiring the break is what stops a market that is
    # merely drifting inside a range from being labelled a trend.
    if hh and hl and not bos_down:
        trend = Trend.UP
    elif ll and lh and not bos_up:
        trend = Trend.DOWN
    elif (hh and ll) or (lh and hl):
        trend = Trend.RANGE       # expanding or contracting, no directional edge
    else:
        trend = Trend.UNCLEAR

    # --- compression / expansion --------------------------------------------
    recent_rng = max(b.high for b in win[-20:]) - min(b.low for b in win[-20:])
    prior_rng = max(b.high for b in win[-60:-20]) - min(b.low for b in win[-60:-20]) \
        if len(win) >= 60 else recent_rng
    compression = 0.0
    if prior_rng > 0:
        ratio = recent_rng / prior_rng
        compression = max(0.0, min(1.0, 1.0 - ratio))
    mean_tr = sum(b.range for b in win[-40:]) / min(40, len(win))
    expansion = (win[-1].range / mean_tr) if mean_tr > 0 else 1.0

    range_high = max(b.high for b in win[-40:])
    range_low = min(b.low for b in win[-40:])
    span = range_high - range_low
    in_range = trend in (Trend.RANGE, Trend.UNCLEAR) and span < 4.0 * a

    levels = cluster_levels(swings, tolerance=0.35 * a, tf_name=tf_name)
    levels.sort(key=lambda lv: -lv.strength)

    return StructureReading(
        trend=trend, swings=swings, last_swing_high=lsh, last_swing_low=lsl,
        higher_highs=hh, higher_lows=hl, lower_highs=lh, lower_lows=ll,
        bos_up=bos_up, bos_down=bos_down, bos_bars_ago=bos_bars_ago,
        reclaim_up=reclaim_up, reclaim_down=reclaim_down,
        sweep_level=sweep_level, compression=compression, expansion=expansion,
        range_high=range_high, range_low=range_low, in_range=in_range,
        levels=levels[:14], atr=a)


# ------------------------------------------------------- breakout quality ----

@dataclass
class BreakoutQuality:
    confirmed: bool
    direction: int
    level: float
    close_beyond_atr: float
    body_pct: float
    wick_pct: float
    participation: float
    retested: bool
    rejected: bool
    follow_through_atr: float
    score: float               # 0..100

    @property
    def failed(self) -> bool:
        return self.rejected and not self.confirmed


def assess_breakout(bars: Sequence[Bar], level: float, direction: int, *,
                    atr_value: float, median_volume: float = 0.0,
                    confirm_bars: int = 3) -> BreakoutQuality:
    """Judge a break of ``level``.

    A wick outside a level is explicitly **not** a breakout.  Confirmation
    requires a body close beyond the level by a meaningful fraction of ATR with
    participation; a poke-and-reclaim is reported as ``rejected`` instead, which
    is what feeds the failed-breakout tactic.
    """
    if not bars or atr_value <= 0:
        return BreakoutQuality(False, direction, level, 0, 0, 0, 0, False,
                               False, 0, 0.0)
    win = list(bars[-confirm_bars:])
    last = win[-1]
    sign = 1 if direction > 0 else -1
    beyond = (last.close - level) * sign
    close_beyond_atr = beyond / atr_value
    pierced = any((b.high - level) > 0 if sign > 0 else (level - b.low) > 0
                  for b in bars[-8:])
    adverse_wick = (last.upper_wick if sign > 0 else last.lower_wick)
    wick_pct = adverse_wick / last.range if last.range > 0 else 0.0
    part = (last.tick_volume / median_volume) if median_volume > 0 else 1.0

    confirmed = (beyond > 0 and close_beyond_atr >= 0.12
                 and last.body_pct >= 0.45 and wick_pct <= 0.45)
    rejected = pierced and beyond <= 0

    follow = 0.0
    if confirmed and len(bars) >= 2:
        follow = max(0.0, ((bars[-1].close - bars[-2].close) * sign) / atr_value)

    if confirmed:
        score = (100.0 * min(1.0, 0.30 * min(close_beyond_atr / 0.5, 1.0)
                             + 0.24 * last.body_pct
                             + 0.18 * min(part / 1.6, 1.0)
                             + 0.16 * min(follow / 0.3, 1.0)
                             + 0.12 * (1.0 - wick_pct)))
    elif rejected:
        score = 0.0
    else:
        score = 25.0 * max(0.0, min(1.0, close_beyond_atr / 0.12))

    retested = False
    if confirmed:
        for b in bars[-6:-1]:
            touched = (b.low <= level) if sign > 0 else (b.high >= level)
            if touched:
                retested = True
                break

    return BreakoutQuality(confirmed=confirmed, direction=sign, level=level,
                           close_beyond_atr=close_beyond_atr,
                           body_pct=last.body_pct, wick_pct=wick_pct,
                           participation=part, retested=retested,
                           rejected=rejected, follow_through_atr=follow,
                           score=round(score, 2))


def tight_invalidation(bars: Sequence[Bar], side: int, atr_value: float, *,
                       min_atr: float = 0.55, max_atr: float = 2.2,
                       max_bars: int = 24) -> float:
    """Nearest local extreme whose violation genuinely disproves a continuation.

    Using "the last confirmed swing" as the invalidation sounds rigorous but in
    a trending market that swing is routinely four or five ATR away, which
    produces a stop so wide the trade can no longer pay for itself.  What
    actually disproves an impulse is a move back through the base of the impulse
    itself, so this walks outward from the current bar and takes the first local
    extreme that is far enough away to not be noise (``min_atr``) while staying
    inside a usable stop width (``max_atr``).
    """
    if not bars or atr_value <= 0:
        return 0.0
    price = bars[-1].close
    floor_dist = min_atr * atr_value
    cap_dist = max_atr * atr_value
    chosen = None
    for k in range(2, min(max_bars, len(bars)) + 1):
        window = bars[-k:]
        extreme = min(b.low for b in window) if side > 0 else max(b.high for b in window)
        dist = (price - extreme) if side > 0 else (extreme - price)
        if dist >= floor_dist:
            chosen = extreme if dist <= cap_dist else None
            break
    if chosen is None:
        chosen = price - side * min(max(floor_dist, 0.0), cap_dist)
    return chosen
