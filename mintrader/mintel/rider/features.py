"""The evidence FAMILIES of the spec, measured from ticks and tick-built bars.

Every family returns an :class:`Evidence`: a normalised strength 0..1, a
signed direction (+1 up, -1 down, 0 directionless or unclear), a short
plain-English phrase and the raw measurements. Distances are ATR-normalised
(the 14-bar ATR of one-minute bars, "ATR" below) so markets compare fairly.

Honest limits, stated once:

* Retail MetaTrader FX has NO genuine order flow and NO depth of market.
  Family 17 (order flow) is therefore ABSENT - it is never faked from
  candles or tick counts. ``ORDER_FLOW_AVAILABLE`` is False and the score
  has no order-flow category.
* Spot FX "volume" is the broker's tick count (quote updates), not traded
  volume. Family 11 uses it for what it is: participation / activity.

No look-ahead: every measurement uses ticks at or before ``now`` and bars
whose end time has passed (plus the live price itself where a family says
"current"). The same code runs live and in replay.

Families (spec numbering): 1 volatility regime, 2 directional efficiency,
3 velocity / acceleration / jerk, 4 momentum measures, 5 breakouts,
6 structure break -> FIRST retest, 7 momentum pullback, 8 opening ranges,
9 session breakout, 10 squeeze release, 11 participation, 12 VWAP,
13 EMA geometry, 14 ADX/DMI, 15 candle quality, 16 microstructure,
(17 order flow: absent), 18 currency strength and 19 cross-market
(strength.py), 20 news catalyst and 21 post-news momentum (news.py read
here), 22 failed breakout, 23 liquidity sweep / reclaim, 24 structural
headroom, 25 ATR normalisation (everywhere), 26 COST_RATIO, 27 anti-chop.
"""
from __future__ import annotations

import bisect
import math
from dataclasses import dataclass, field
from typing import Optional, Sequence

from ..broker.base import Bar

ORDER_FLOW_AVAILABLE = False      # family 17: retail MT5 FX has no order flow or DOM. Never faked.

VELOCITY_WINDOWS = (1, 3, 5, 10, 15, 30, 60, 180)
VELOCITY_WEIGHTS = {1: 0.02, 3: 0.04, 5: 0.06, 10: 0.12, 15: 0.14, 30: 0.24, 60: 0.24, 180: 0.14}


def clip(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return lo if x < lo else hi if x > hi else x


def sign(x: float, eps: float = 0.0) -> int:
    return 1 if x > eps else -1 if x < -eps else 0


@dataclass
class Evidence:
    family: str
    strength: float = 0.0
    direction: int = 0
    phrase: str = ""
    values: dict = field(default_factory=dict)

    def toward(self, d: int) -> float:
        """Strength counted for direction ``d``: directionless evidence counts,
        evidence pointing the other way counts nothing."""
        if self.direction == 0 or d == 0 or self.direction == d:
            return self.strength
        return 0.0

    def to_dict(self) -> dict:
        return {"strength": round(self.strength, 3), "direction": self.direction, "phrase": self.phrase,
                "values": {k: (round(v, 4) if isinstance(v, float) else v) for k, v in self.values.items()}}


# ----------------------------------------------------------- indicators --

def ema_series(values: Sequence[float], n: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (n + 1.0)
    out = [float(values[0])]
    for v in values[1:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out


def true_ranges(bars: Sequence[Bar]) -> list[float]:
    out = []
    prev = None
    for b in bars:
        if prev is None:
            out.append(b.high - b.low)
        else:
            out.append(max(b.high - b.low, abs(b.high - prev), abs(b.low - prev)))
        prev = b.close
    return out


def wilder(values: Sequence[float], n: int) -> list[float]:
    """Wilder smoothing (the ATR/ADX average), seeded with a simple mean."""
    if len(values) < n or n <= 0:
        return []
    out = [sum(values[:n]) / n]
    for v in values[n:]:
        out.append(out[-1] + (v - out[-1]) / n)
    return out


def adx_series(bars: Sequence[Bar], n: int = 14) -> tuple[list[float], list[float], list[float]]:
    if len(bars) < 2 * n + 2:
        return [], [], []
    pdm, mdm, tr = [], [], []
    for i in range(1, len(bars)):
        up = bars[i].high - bars[i - 1].high
        dn = bars[i - 1].low - bars[i].low
        pdm.append(up if (up > dn and up > 0) else 0.0)
        mdm.append(dn if (dn > up and dn > 0) else 0.0)
        tr.append(max(bars[i].high - bars[i].low, abs(bars[i].high - bars[i - 1].close),
                      abs(bars[i].low - bars[i - 1].close)))
    atr = wilder(tr, n)
    sp = wilder(pdm, n)
    sm = wilder(mdm, n)
    pdi, mdi, dx = [], [], []
    for a, p, m in zip(atr, sp, sm):
        pi = 100.0 * p / a if a > 0 else 0.0
        mi = 100.0 * m / a if a > 0 else 0.0
        pdi.append(pi)
        mdi.append(mi)
        dx.append(100.0 * abs(pi - mi) / (pi + mi) if (pi + mi) > 0 else 0.0)
    adx = wilder(dx, n)
    return adx, pdi[-len(adx):] if adx else [], mdi[-len(adx):] if adx else []


def rsi_series(closes: Sequence[float], n: int = 14) -> list[float]:
    if len(closes) < n + 2:
        return []
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    ag, al = wilder(gains, n), wilder(losses, n)
    return [100.0 - 100.0 / (1.0 + g / l) if l > 0 else 100.0 for g, l in zip(ag, al)]


def regression(values: Sequence[float]) -> tuple[float, float]:
    """(slope per step, r squared)."""
    n = len(values)
    if n < 3:
        return 0.0, 0.0
    mx = (n - 1) / 2.0
    my = sum(values) / n
    sxx = sum((i - mx) ** 2 for i in range(n))
    sxy = sum((i - mx) * (v - my) for i, v in enumerate(values))
    syy = sum((v - my) ** 2 for v in values)
    slope = sxy / sxx if sxx > 0 else 0.0
    r2 = (sxy * sxy) / (sxx * syy) if sxx > 0 and syy > 0 else 0.0
    return slope, r2


def stdev(xs: Sequence[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    m = sum(xs) / n
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))


def median(xs: Sequence[float]) -> float:
    s = sorted(xs)
    n = len(s)
    if not n:
        return 0.0
    return s[n // 2] if n % 2 else 0.5 * (s[n // 2 - 1] + s[n // 2])


def fractal_swings(bars: Sequence[Bar], wing: int = 2) -> list[tuple[int, float, int]]:
    """Confirmed swing points: (index, price, +1 high / -1 low). A swing is
    confirmed only once ``wing`` bars on each side have closed."""
    out = []
    for i in range(wing, len(bars) - wing):
        h, lo = bars[i].high, bars[i].low
        if all(h > bars[i - k].high for k in range(1, wing + 1)) and all(h >= bars[i + k].high for k in range(1, wing + 1)):
            out.append((i, h, 1))
        if all(lo < bars[i - k].low for k in range(1, wing + 1)) and all(lo <= bars[i + k].low for k in range(1, wing + 1)):
            out.append((i, lo, -1))
    return out


def efficiency(prices: Sequence[float]) -> tuple[float, int]:
    """TREND_EFFICIENCY = |net move| / total path travelled, and its direction."""
    if len(prices) < 3:
        return 0.0, 0
    path = 0.0
    for a, b in zip(prices, prices[1:]):
        path += abs(b - a)
    net = prices[-1] - prices[0]
    return (abs(net) / path if path > 0 else 0.0), sign(net)


def zigzag(prices: Sequence[float], threshold: float) -> list[tuple[int, float, int]]:
    """Micro swings: turning points where price reversed by ``threshold``.
    Returns confirmed pivots (index, price, +1 high / -1 low)."""
    if len(prices) < 3 or threshold <= 0:
        return []
    pivots: list[tuple[int, float, int]] = []
    trend = 0
    ext_i, ext_p = 0, prices[0]
    lo_i, lo_p, hi_i, hi_p = 0, prices[0], 0, prices[0]
    for i, p in enumerate(prices):
        if trend == 0:
            if p > hi_p:
                hi_i, hi_p = i, p
            if p < lo_p:
                lo_i, lo_p = i, p
            if hi_p - lo_p >= threshold:
                if hi_i > lo_i:
                    pivots.append((lo_i, lo_p, -1))
                    trend, ext_i, ext_p = 1, hi_i, hi_p
                else:
                    pivots.append((hi_i, hi_p, 1))
                    trend, ext_i, ext_p = -1, lo_i, lo_p
        elif trend == 1:
            if p > ext_p:
                ext_i, ext_p = i, p
            elif ext_p - p >= threshold:
                pivots.append((ext_i, ext_p, 1))
                trend, ext_i, ext_p = -1, i, p
        else:
            if p < ext_p:
                ext_i, ext_p = i, p
            elif p - ext_p >= threshold:
                pivots.append((ext_i, ext_p, -1))
                trend, ext_i, ext_p = 1, i, p
    return pivots


# ------------------------------------------------------------ bar stats --

@dataclass
class BarStats:
    """Everything that changes only when a one-minute (or five-minute) bar
    closes. Cached by the core per symbol and bar version."""
    ok: bool = False
    n: int = 0
    atr: float = 0.0
    atr_fast: float = 0.0
    atr_slow: float = 0.0
    atr_prev: float = 0.0
    median_range: float = 0.0
    last_range: float = 0.0
    range_pct: float = 0.5
    squeeze_now: bool = False
    squeeze_bars: int = 0
    squeeze_hi: float = 0.0
    squeeze_lo: float = 0.0
    squeeze_range: float = 0.0
    bb_kc_ratio: float = 1.0
    don_hi: float = 0.0
    don_lo: float = 0.0
    ema9: float = 0.0
    ema21: float = 0.0
    ema50: float = 0.0
    ema21_slope: float = 0.0          # per bar, in ATR
    ema9_slope: float = 0.0
    sep: float = 0.0                  # (ema9 - ema21) / ATR
    sep_prev: float = 0.0
    adx: float = 0.0
    pdi: float = 0.0
    mdi: float = 0.0
    adx_change: float = 0.0
    macd_hist: float = 0.0
    macd_accel: float = 0.0           # hist change over the last bar, ATR
    rsi: float = 50.0
    rsi_prev: float = 50.0
    reg_slope: float = 0.0            # per bar, ATR
    reg_r2: float = 0.0
    roc5: float = 0.0                 # ATR
    roc15: float = 0.0
    consec: int = 0                   # signed run of same-direction closes
    displacement: float = 0.0         # last 5 bars, ATR
    vwap: float = 0.0
    vwap_slope: float = 0.0           # over 10 bars, ATR
    vwap_crossings: int = 0
    ema_crossings: int = 0
    vol_baseline: float = 0.0         # median ticks per minute
    wick_ratio: float = 0.0
    failed_breakouts: int = 0
    swings: list = field(default_factory=list)       # (time_epoch, price, kind) confirmed M1 swings
    bars_eff: float = 0.0
    bars_eff_dir: int = 0
    # five-minute picture
    m5_ok: bool = False
    atr5: float = 0.0
    m5_slope: float = 0.0             # ema9 of M5 closes, per bar, in M5 ATR
    m5_roc3: float = 0.0
    m5_swings: list = field(default_factory=list)
    last_close: float = 0.0
    last_bar_end: float = 0.0
    breakout: Optional[dict] = None   # the latest bar-close breakout and its retests
    pull_bars: list = field(default_factory=list)    # the last 15 bars (for the pullback family)


def compute_bar_stats(m1: Sequence[Bar], m5: Sequence[Bar], min_bars: int = 30) -> BarStats:
    s = BarStats()
    s.n = len(m1)
    if len(m1) < max(min_bars, 16):
        return s
    bars = list(m1[-240:])
    tr = true_ranges(bars)
    a14 = wilder(tr, 14)
    a5 = wilder(tr, 5)
    a30 = wilder(tr, 30) if len(tr) >= 30 else a14
    if not a14:
        return s
    s.atr = a14[-1]
    s.atr_prev = a14[-4] if len(a14) >= 4 else a14[0]
    s.atr_fast = a5[-1] if a5 else s.atr
    s.atr_slow = a30[-1] if a30 else s.atr
    atr = max(s.atr, 1e-12)
    ranges = [b.high - b.low for b in bars]
    s.median_range = median(ranges[-30:])
    s.last_range = ranges[-1]
    last60 = ranges[-60:]
    s.range_pct = sum(1 for r in last60 if r <= s.last_range) / len(last60)
    closes = [b.close for b in bars]
    s.last_close = closes[-1]
    s.last_bar_end = bars[-1].time.timestamp() + 60.0
    # squeeze: Bollinger(20, 2) inside Keltner(20, 1.5 ATR)
    sq_flags = []
    for end in range(max(20, len(bars) - 14), len(bars) + 1):
        win = closes[end - 20:end]
        if len(win) < 20:
            continue
        sd = stdev(win)
        kc_atr = sum(tr[end - 20:end]) / 20.0
        bbw, kcw = 2.0 * sd, 1.5 * kc_atr
        sq_flags.append((end - 1, bbw < kcw, (bbw / kcw) if kcw > 0 else 1.0))
    if sq_flags:
        s.squeeze_now = sq_flags[-1][1]
        s.bb_kc_ratio = sq_flags[-1][2]
        recent = [f for f in sq_flags if f[0] <= len(bars) - 2][-12:]
        sq_idx = [f[0] for f in recent if f[1]]
        s.squeeze_bars = len(sq_idx)
        if sq_idx:
            lo_i = min(sq_idx)
            box = bars[lo_i:max(sq_idx) + 1]
            s.squeeze_hi = max(b.high for b in box)
            s.squeeze_lo = min(b.low for b in box)
            s.squeeze_range = median([b.high - b.low for b in box])
    don = bars[-20:]
    s.don_hi = max(b.high for b in don)
    s.don_lo = min(b.low for b in don)
    e9, e21, e50 = ema_series(closes, 9), ema_series(closes, 21), ema_series(closes, 50)
    s.ema9, s.ema21, s.ema50 = e9[-1], e21[-1], e50[-1]
    s.ema21_slope = (e21[-1] - e21[-4]) / 3.0 / atr
    s.ema9_slope = (e9[-1] - e9[-4]) / 3.0 / atr
    s.sep = (e9[-1] - e21[-1]) / atr
    s.sep_prev = (e9[-4] - e21[-4]) / atr
    adx, pdi, mdi = adx_series(bars[-120:], 14)
    if adx:
        s.adx, s.pdi, s.mdi = adx[-1], pdi[-1], mdi[-1]
        s.adx_change = adx[-1] - (adx[-4] if len(adx) >= 4 else adx[0])
    e12, e26 = ema_series(closes, 12), ema_series(closes, 26)
    macd = [a - b for a, b in zip(e12, e26)]
    sig = ema_series(macd, 9)
    hist = [m - g for m, g in zip(macd, sig)]
    s.macd_hist = hist[-1] / atr
    s.macd_accel = (hist[-1] - hist[-2]) / atr if len(hist) >= 2 else 0.0
    rs = rsi_series(closes[-100:], 14)
    if rs:
        s.rsi = rs[-1]
        s.rsi_prev = rs[-4] if len(rs) >= 4 else rs[0]
    slope, r2 = regression(closes[-10:])
    s.reg_slope, s.reg_r2 = slope / atr, r2
    s.roc5 = (closes[-1] - closes[-6]) / atr
    s.roc15 = (closes[-1] - closes[-16]) / atr
    run = 0
    for i in range(len(closes) - 1, 0, -1):
        d = sign(closes[i] - closes[i - 1])
        if d == 0:
            break
        if run == 0 or sign(run) == d:
            run += d
        else:
            break
    s.consec = run
    s.displacement = (closes[-1] - bars[-5].open) / atr
    # VWAP over the last hour (tick-volume weighted typical price)
    vb = bars[-60:]
    num = den = 0.0
    vw_series = []
    for b in vb:
        w = max(b.tick_volume, 1.0)
        num += w * (b.high + b.low + b.close) / 3.0
        den += w
        vw_series.append(num / den)
    s.vwap = vw_series[-1]
    s.vwap_slope = (vw_series[-1] - vw_series[-11]) / atr if len(vw_series) >= 11 else 0.0
    last20 = bars[-20:]
    vw20 = vw_series[-20:]
    s.vwap_crossings = sum(1 for i in range(1, len(last20))
                           if sign(last20[i].close - vw20[i]) != sign(last20[i - 1].close - vw20[i - 1]))
    e21_20 = e21[-20:]
    s.ema_crossings = sum(1 for i in range(1, len(last20))
                          if sign(last20[i].close - e21_20[i]) != sign(last20[i - 1].close - e21_20[i - 1]))
    s.vol_baseline = median([b.tick_volume for b in bars[-30:]])
    wk = []
    for b in bars[-10:]:
        r = b.high - b.low
        if r > 0:
            wk.append((r - abs(b.close - b.open)) / r)
    s.wick_ratio = sum(wk) / len(wk) if wk else 0.0
    # failed breakouts: a close beyond the prior 20-bar extreme, back inside within 3 bars
    fb = 0
    for i in range(max(21, len(bars) - 30), len(bars) - 1):
        ph = max(b.high for b in bars[i - 20:i])
        pl = min(b.low for b in bars[i - 20:i])
        nxt = bars[i + 1:i + 4]
        if bars[i].close > ph and any(b.close < ph for b in nxt):
            fb += 1
        elif bars[i].close < pl and any(b.close > pl for b in nxt):
            fb += 1
    s.failed_breakouts = fb
    sw = fractal_swings(bars[-90:], 2)
    base = len(bars) - len(bars[-90:])
    s.swings = [(bars[base + i].time.timestamp(), p, k) for i, p, k in sw]
    s.bars_eff, s.bars_eff_dir = efficiency(closes[-11:])
    # the latest bar-close breakout (last 15 bars) and how price has treated the level since
    s.breakout = None
    for i in range(len(bars) - 1, max(21, len(bars) - 16) - 1, -1):
        ph = max(b.high for b in bars[i - 20:i])
        pl = min(b.low for b in bars[i - 20:i])
        d = 1 if bars[i].close > ph else -1 if bars[i].close < pl else 0
        if d:
            level = ph if d > 0 else pl
            after = bars[i + 1:]
            retests, touching, held = 0, False, True
            for b in after:
                touch = (b.low <= level + 0.3 * atr) if d > 0 else (b.high >= level - 0.3 * atr)
                if touch and not touching:
                    retests += 1
                touching = touch
                if (b.close < level - 0.25 * atr) if d > 0 else (b.close > level + 0.25 * atr):
                    held = False
            ext = max([b.high for b in bars[i:]]) if d > 0 else min([b.low for b in bars[i:]])
            last_touch_high = None
            for b in reversed(after):
                touch = (b.low <= level + 0.3 * atr) if d > 0 else (b.high >= level - 0.3 * atr)
                if touch:
                    last_touch_high = b.high if d > 0 else b.low
                    break
            s.breakout = {"dir": d, "level": level, "bars_ago": len(bars) - 1 - i, "retests": retests,
                          "held": held, "extreme": ext, "touching": touching, "retest_turn": last_touch_high}
            break
    s.pull_bars = bars[-15:]
    # five-minute picture
    if len(m5) >= 20:
        b5 = list(m5[-120:])
        a5_ = wilder(true_ranges(b5), 14)
        if a5_:
            s.m5_ok = True
            s.atr5 = a5_[-1]
            c5 = [b.close for b in b5]
            e5 = ema_series(c5, 9)
            s.m5_slope = (e5[-1] - e5[-3]) / 2.0 / max(s.atr5, 1e-12)
            s.m5_roc3 = (c5[-1] - c5[-4]) / max(s.atr5, 1e-12)
            sw5 = fractal_swings(b5[-60:], 2)
            base5 = len(b5) - len(b5[-60:])
            s.m5_swings = [(b5[base5 + i].time.timestamp(), p, k) for i, p, k in sw5]
    s.ok = True
    return s


# ----------------------------------------------------------- tick stats --

@dataclass
class TickStats:
    ok: bool = False
    now: float = 0.0
    mid: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    spread: float = 0.0
    spread_median: float = 0.0
    times: list = field(default_factory=list)
    mids: list = field(default_factory=list)
    moves: dict = field(default_factory=dict)       # window -> signed price move
    z: dict = field(default_factory=dict)           # window -> move / (ATR * sqrt(w/60))
    velocity: float = 0.0                           # weighted z, signed
    accel: float = 0.0                              # ATR per minute, signed
    jerk: float = 0.0
    eff60: float = 0.0
    eff60_dir: int = 0
    eff120: float = 0.0
    eff120_dir: int = 0
    tick_rate10: float = 0.0                        # ticks per minute
    tick_rate30: float = 0.0
    tick_rate60: float = 0.0
    upticks30: float = 0.5
    pivots: list = field(default_factory=list)      # micro swings (time, price, kind)
    range60: float = 0.0
    rv60: float = 0.0
    rv180: float = 0.0
    mean_crossings: int = 0
    range120: float = 0.0
    net120: float = 0.0
    big_reversals: int = 0


def price_at(times: Sequence[float], mids: Sequence[float], t: float) -> Optional[float]:
    """The last mid at or before time t (None when nothing is that old)."""
    i = bisect.bisect_right(times, t) - 1
    if i < 0:
        return None
    return mids[i]


def sampled(times: Sequence[float], mids: Sequence[float], start: float, end: float, step: float) -> list[float]:
    out = []
    t = start
    i = bisect.bisect_right(times, start) - 1
    n = len(times)
    while t <= end + 1e-9:
        while i + 1 < n and times[i + 1] <= t:
            i += 1
        if i >= 0:
            out.append(mids[i])
        t += step
    return out


def compute_tick_stats(ticks: Sequence, now: float, atr: float) -> TickStats:
    """``ticks`` are Tick objects in time order, all at or before ``now``."""
    s = TickStats(now=now)
    if len(ticks) < 5 or atr <= 0:
        return s
    times = [t.time.timestamp() for t in ticks]
    mids = [(t.bid + t.ask) * 0.5 for t in ticks]
    last = ticks[-1]
    s.times, s.mids = times, mids
    s.mid, s.bid, s.ask = mids[-1], float(last.bid), float(last.ask)
    s.spread = float(last.ask - last.bid)
    i120 = bisect.bisect_left(times, now - 120.0)
    s.spread_median = median([t.ask - t.bid for t in ticks[i120:]]) if i120 < len(ticks) else s.spread
    vel = 0.0
    wsum = 0.0
    for w in VELOCITY_WINDOWS:
        p0 = price_at(times, mids, now - w)
        if p0 is None:
            continue
        mv = s.mid - p0
        s.moves[w] = mv
        z = mv / (atr * math.sqrt(w / 60.0))
        s.z[w] = z
        vel += VELOCITY_WEIGHTS[w] * clip(z, -6.0, 6.0)
        wsum += VELOCITY_WEIGHTS[w]
    s.velocity = vel / wsum if wsum > 0 else 0.0

    def speed(t_end: float, w: float) -> Optional[float]:
        a, b = price_at(times, mids, t_end - w), price_at(times, mids, t_end)
        if a is None or b is None:
            return None
        return (b - a) / w

    unit = atr / 60.0
    f_now, l_now = speed(now, 15.0), speed(now, 120.0)
    f_prev, l_prev = speed(now - 15.0, 15.0), speed(now - 15.0, 120.0)
    if f_now is not None and l_now is not None:
        s.accel = (f_now - l_now) / unit
        if f_prev is not None and l_prev is not None:
            s.jerk = s.accel - (f_prev - l_prev) / unit
    p60 = sampled(times, mids, now - 60.0, now, 2.0)
    p120 = sampled(times, mids, now - 120.0, now, 2.0)
    s.eff60, s.eff60_dir = efficiency(p60)
    s.eff120, s.eff120_dir = efficiency(p120)
    i10 = bisect.bisect_left(times, now - 10.0)
    i30 = bisect.bisect_left(times, now - 30.0)
    i60 = bisect.bisect_left(times, now - 60.0)
    span0 = times[0]
    s.tick_rate10 = (len(times) - i10) * 60.0 / min(10.0, max(now - span0, 1.0))
    s.tick_rate30 = (len(times) - i30) * 60.0 / min(30.0, max(now - span0, 1.0))
    s.tick_rate60 = (len(times) - i60) * 60.0 / min(60.0, max(now - span0, 1.0))
    ups = downs = 0
    for a, b in zip(mids[max(i30 - 1, 0):], mids[max(i30, 1):]):
        if b > a:
            ups += 1
        elif b < a:
            downs += 1
    s.upticks30 = ups / (ups + downs) if (ups + downs) else 0.5
    seg = mids[i60:] if i60 < len(mids) else mids[-1:]
    s.range60 = max(seg) - min(seg)
    r60 = [b - a for a, b in zip(p60, p60[1:])]
    p180 = sampled(times, mids, now - 180.0, now, 2.0)
    r180 = [b - a for a, b in zip(p180, p180[1:])]
    s.rv60, s.rv180 = stdev(r60), stdev(r180)
    if p120:
        m = sum(p120) / len(p120)
        s.mean_crossings = sum(1 for a, b in zip(p120, p120[1:]) if sign(a - m) != sign(b - m) and sign(b - m) != 0)
    i_piv = bisect.bisect_left(times, now - 150.0)
    piv = zigzag(mids[i_piv:], 0.2 * atr)
    s.pivots = [(times[i_piv + i], p, k) for i, p, k in piv]
    i120 = bisect.bisect_left(times, now - 120.0)
    seg120 = mids[i120:] or mids[-1:]
    s.range120 = max(seg120) - min(seg120)
    p120_0 = price_at(times, mids, now - 120.0)
    s.net120 = s.mid - (p120_0 if p120_0 is not None else mids[0])
    s.big_reversals = len(zigzag(mids[i120:], 1.5 * atr))
    s.ok = True
    return s


# ------------------------------------------------------------- the view --

@dataclass
class MarketView:
    symbol: str
    now: float
    pip: float
    base: str
    quote: str
    bars: BarStats
    ticks: TickStats
    sessions: object = None            # bars.SessionLevels
    news: object = None                # news.NewsContext
    forming: Optional[Bar] = None
    cost_price: float = 0.0            # commission (round trip) + slippage (both sides), price units
    expected_run_atr: float = 3.0      # a run is expected to reach this many one-minute ATRs ...
    expected_run_atr5: float = 1.5     # ... or this many five-minute ATRs ...
    expected_run_impulse: float = 1.0  # ... or this many times the last minute's move, whichever is largest

    @property
    def atr(self) -> float:
        return self.bars.atr

    @property
    def ok(self) -> bool:
        return self.bars.ok and self.ticks.ok and self.bars.atr > 0


def _late(dist_atr: float) -> float:
    """Early entries are the point: a break already far through is late."""
    return clip(1.0 - (dist_atr - 2.0) / 2.5, 0.2, 1.0)


# --------------------------------------------------------------- families --

def fam_volatility(v: MarketView) -> Evidence:
    b, t = v.bars, v.ticks
    atr_ratio = b.atr_fast / b.atr_slow if b.atr_slow > 0 else 1.0
    range_now = t.range60 / b.median_range if b.median_range > 0 else 1.0
    rv_ratio = t.rv60 / t.rv180 if t.rv180 > 0 else 1.0
    vol_change = (b.atr - b.atr_prev) / b.atr_prev if b.atr_prev > 0 else 0.0
    expansion = (0.3 * clip((atr_ratio - 0.95) / 0.8) + 0.35 * clip((range_now - 1.0) / 1.5)
                 + 0.2 * clip((rv_ratio - 1.0) / 1.0) + 0.15 * clip((b.range_pct - 0.5) / 0.45))
    if b.squeeze_now:
        regime = "squeeze"
    elif expansion >= 0.5:
        regime = "expanding"
    elif atr_ratio < 0.8 and range_now < 0.8:
        regime = "quiet"
    else:
        regime = "normal"
    phrase = {"squeeze": "volatility squeezed tight", "expanding": f"volatility expanding ({range_now:.1f}x a normal minute)",
              "quiet": "quiet market", "normal": "normal volatility"}[regime]
    return Evidence("volatility", clip(expansion), 0, phrase,
                    {"atr_ratio": atr_ratio, "range_now_ratio": range_now, "rv_ratio": rv_ratio,
                     "range_pct": b.range_pct, "bb_kc": b.bb_kc_ratio, "vol_change": vol_change,
                     "donchian_atr": (b.don_hi - b.don_lo) / b.atr if b.atr > 0 else 0.0, "regime": regime})


def fam_efficiency(v: MarketView) -> Evidence:
    t, b = v.ticks, v.bars
    d = t.eff120_dir or t.eff60_dir
    agree = t.eff60_dir == t.eff120_dir or 0 in (t.eff60_dir, t.eff120_dir)
    eff = 0.5 * t.eff60 + 0.3 * t.eff120 + 0.2 * (b.bars_eff if b.bars_eff_dir in (d, 0) else 0.0)
    if not agree:
        eff *= 0.4
        d = t.eff60_dir
    strength = clip((eff - 0.15) / 0.45)
    word = "very clean" if strength >= 0.8 else "clean" if strength >= 0.5 else "ragged" if strength >= 0.2 else "choppy"
    return Evidence("efficiency", strength, d if strength > 0 else 0, f"{word} price path (efficiency {eff:.2f})",
                    {"efficiency": eff, "eff60": t.eff60, "eff120": t.eff120, "bars_eff": b.bars_eff})


def fam_velocity(v: MarketView) -> Evidence:
    t = v.ticks
    sp = abs(t.velocity)
    strength = clip((sp - 0.6) / 1.5)
    d = sign(t.velocity, 0.3)
    word = "very fast" if strength >= 0.75 else "fast" if strength >= 0.45 else "moving" if strength >= 0.15 else "slow"
    vals = {f"z{w}": t.z.get(w, 0.0) for w in VELOCITY_WINDOWS}
    vals["velocity"] = t.velocity
    vals["pips_60s"] = t.moves.get(60, 0.0) / v.pip if v.pip else 0.0
    return Evidence("velocity", strength, d, f"{word} {'up' if d > 0 else 'down' if d < 0 else 'sideways'}", vals)


def fam_acceleration(v: MarketView, d: int) -> Evidence:
    t = v.ticks
    acc = t.accel * d if d else abs(t.accel)
    jerk = t.jerk * d if d else 0.0
    strength = 0.8 * clip((acc - 0.5) / 2.5) + 0.2 * clip(jerk / 2.5)
    phrase = "speeding up" if strength >= 0.4 else "steady speed" if acc > -0.5 else "slowing down"
    return Evidence("acceleration", clip(strength), sign(t.accel, 0.5), phrase,
                    {"accel": t.accel, "jerk": t.jerk})


def fam_momentum_measures(v: MarketView) -> Evidence:
    """Family 4 as MEASUREMENTS (never "RSI crossed 50")."""
    b = v.bars
    parts = [
        clip(b.roc5 / 2.0, -1, 1),
        clip(b.ema9_slope / 0.3, -1, 1),
        clip(b.macd_accel / 0.15, -1, 1),
        clip((b.rsi - b.rsi_prev) / 15.0, -1, 1) * (1.0 if 30 < b.rsi < 85 else 0.5),
        clip(b.reg_slope / 0.3, -1, 1) * b.reg_r2,
        clip(b.displacement / 2.5, -1, 1),
        clip(b.consec / 4.0, -1, 1),
    ]
    m = sum(parts) / len(parts)
    return Evidence("momentum", clip(abs(m) / 0.6), sign(m, 0.05), f"one-minute momentum {'up' if m > 0 else 'down'}",
                    {"roc5": b.roc5, "ema9_slope": b.ema9_slope, "macd_accel": b.macd_accel, "rsi": b.rsi,
                     "reg_slope": b.reg_slope, "reg_r2": b.reg_r2, "displacement": b.displacement, "consec": b.consec,
                     "mean": m})


def _break_of(level_hi: float, level_lo: float, v: MarketView) -> tuple[int, float, float]:
    """(direction, distance through in ATR, seconds beyond) of the live mid
    against a range; 0 when inside."""
    t = v.ticks
    atr = v.atr
    if t.mid > level_hi:
        d, dist, lvl = 1, (t.mid - level_hi) / atr, level_hi
    elif t.mid < level_lo:
        d, dist, lvl = -1, (level_lo - t.mid) / atr, level_lo
    else:
        return 0, 0.0, 0.0
    since = 0.0
    for i in range(len(t.mids) - 1, -1, -1):
        if (t.mids[i] - lvl) * d <= 0:
            since = t.now - t.times[i]
            break
    else:
        since = t.now - t.times[0] if t.times else 0.0
    return d, dist, since


def fam_breakout(v: MarketView) -> Evidence:
    b, t = v.bars, v.ticks
    d, dist, since = _break_of(b.don_hi, b.don_lo, v)
    width = (b.don_hi - b.don_lo) / v.atr
    if d == 0:
        return Evidence("breakout", 0.0, 0, "inside its recent range", {"donchian_atr": width})
    seg = t.mids[bisect.bisect_left(t.times, t.now - since):] if since > 0 else [t.mid]
    level = b.don_hi if d > 0 else b.don_lo
    follow = ((max(seg) - level) if d > 0 else (level - min(seg))) / v.atr if seg else dist
    f = v.forming
    body = 0.5
    if f is not None and f.high > f.low:
        body = clip(((t.mid - f.open) * d) / (f.high - f.low))
    activity = clip((t.tick_rate30 / b.vol_baseline - 0.8) / 1.2) if b.vol_baseline > 0 else 0.5
    coil = clip((6.0 - width) / 4.0)
    held = 1.0 if dist >= 0.5 * follow else 0.5
    strength = (0.35 + 0.2 * clip(dist / 0.6) + 0.15 * body + 0.1 * activity + 0.1 * coil + 0.1 * held) * _late(dist)
    return Evidence("breakout", clip(strength), d,
                    f"breaking {'above' if d > 0 else 'below'} its 20-minute range",
                    {"dist_atr": dist, "seconds_beyond": since, "follow_atr": follow, "body": body,
                     "activity": activity, "donchian_atr": width})


def fam_first_retest(v: MarketView) -> Evidence:
    br = v.bars.breakout
    t = v.ticks
    if not br or br["retests"] < 1 or not br["held"]:
        return Evidence("first_retest", 0.0, 0, "no retest", {})
    d = br["dir"]
    turn = br.get("retest_turn")
    resuming = turn is not None and (t.mid - turn) * d > 0 and (t.moves.get(10, 0.0) * d) > 0
    fresh = {1: 1.0, 2: 0.55, 3: 0.3}.get(br["retests"], 0.1)
    strength = fresh * (1.0 if resuming else 0.35)
    return Evidence("first_retest", clip(strength), d,
                    f"{'first' if br['retests'] == 1 else 'repeated'} retest of the {'high' if d > 0 else 'low'} held"
                    + (" and price is moving on" if resuming else ""),
                    {"retests": br["retests"], "bars_ago": br["bars_ago"], "resuming": resuming})


def fam_pullback(v: MarketView) -> Evidence:
    bars = v.bars.pull_bars
    t = v.ticks
    atr = v.atr
    if len(bars) < 8:
        return Evidence("pullback", 0.0, 0, "no pullback", {})
    hi_i = max(range(len(bars)), key=lambda i: bars[i].high)
    lo_i = min(range(len(bars)), key=lambda i: bars[i].low)
    best = Evidence("pullback", 0.0, 0, "no pullback", {})
    for d in (1, -1):
        ext_i, start_i = (hi_i, lo_i) if d > 0 else (lo_i, hi_i)
        if not (start_i < ext_i < len(bars) - 1):
            continue
        ext = bars[ext_i].high if d > 0 else bars[ext_i].low
        start = bars[start_i].low if d > 0 else bars[start_i].high
        leg = (ext - start) * d
        if leg < 2.0 * atr:
            continue
        after = bars[ext_i + 1:]
        back = min(b.low for b in after) if d > 0 else max(b.high for b in after)
        back = min(back, t.mid) if d > 0 else max(back, t.mid)
        depth = ((ext - back) * d) / leg
        if depth < 0.15 or depth > 0.75:
            continue
        leg_speed = leg / max(ext_i - start_i, 1)
        pb_speed = ((ext - back) * d) / max(len(after), 1)
        leg_vol = sum(b.tick_volume for b in bars[start_i:ext_i + 1]) / max(ext_i - start_i + 1, 1)
        pb_vol = sum(b.tick_volume for b in after) / max(len(after), 1)
        resuming = (t.moves.get(15, 0.0) * d) > 0.15 * atr and (t.mid - back) * d > 0.25 * atr
        strength = (0.35 * clip(1.0 - depth / 0.618) + 0.2 * clip(1.0 - pb_speed / max(leg_speed, 1e-12))
                    + 0.15 * clip(1.0 - pb_vol / max(leg_vol, 1e-12)) + (0.3 if resuming else 0.0))
        if not resuming:
            strength *= 0.5
        if strength > best.strength:
            best = Evidence("pullback", clip(strength), d,
                            f"shallow pullback in a {'rising' if d > 0 else 'falling'} move"
                            + (" now turning back" if resuming else ""),
                            {"depth": depth, "leg_atr": leg / atr, "speed_ratio": pb_speed / max(leg_speed, 1e-12),
                             "volume_ratio": pb_vol / max(leg_vol, 1e-12), "resuming": resuming})
    return best


def fam_opening_range(v: MarketView) -> Evidence:
    lv = v.sessions
    if lv is None:
        return Evidence("opening_range", 0.0, 0, "", {})
    best = Evidence("opening_range", 0.0, 0, "no opening-range break", {})
    for (name, m), (hi, lo, complete, open_epoch) in getattr(lv, "opening", {}).items():
        if not complete or m < 3 or v.now - open_epoch > 3 * 3600:
            continue
        d, dist, since = _break_of(hi, lo, v)
        if d == 0:
            continue
        strength = (0.5 + 0.3 * clip(dist / 0.6) + 0.2 * clip(m / 15.0)) * _late(dist)
        if strength > best.strength:
            label = "London" if name == "LONDON" else "New York"
            best = Evidence("opening_range", clip(strength), d,
                            f"breaking {'above' if d > 0 else 'below'} the {label} {m}-minute opening range",
                            {"session": name, "minutes": m, "dist_atr": dist, "seconds_beyond": since})
    return best


def fam_session_breakout(v: MarketView) -> Evidence:
    lv = v.sessions
    if lv is None:
        return Evidence("session_breakout", 0.0, 0, "", {})
    cands = []
    ranges = getattr(lv, "ranges", {})
    active = getattr(lv, "active", ())
    if "LONDON" in active and "ASIA" in ranges and ranges["ASIA"][2]:
        cands.append(("the Asian session", ranges["ASIA"][0], ranges["ASIA"][1]))
    if "NEWYORK" in active and "LONDON" in ranges:
        cands.append(("the London session", ranges["LONDON"][0], ranges["LONDON"][1]))
    if getattr(lv, "prev_day", None):
        cands.append(("yesterday", lv.prev_day[0], lv.prev_day[1]))
    best = Evidence("session_breakout", 0.0, 0, "inside the session ranges", {})
    for label, hi, lo in cands:
        d, dist, since = _break_of(hi, lo, v)
        if d == 0:
            continue
        strength = (0.55 + 0.45 * clip(dist / 0.6)) * _late(dist)
        if strength > best.strength:
            best = Evidence("session_breakout", clip(strength), d,
                            f"breaking {'above' if d > 0 else 'below'} {label}'s {'high' if d > 0 else 'low'}",
                            {"level": label, "dist_atr": dist, "seconds_beyond": since})
    return best


def fam_squeeze_release(v: MarketView) -> Evidence:
    b, t = v.bars, v.ticks
    if b.squeeze_bars < 4 or b.squeeze_hi <= b.squeeze_lo:
        return Evidence("squeeze_release", 0.0, 0, "no squeeze", {"squeeze_bars": b.squeeze_bars})
    unit = max(b.squeeze_range, 1e-12)
    d, dist, since = _break_of(b.squeeze_hi, b.squeeze_lo, v)
    if d == 0:
        return Evidence("squeeze_release", 0.0, 0, "squeezed, waiting for the release",
                        {"squeeze_bars": b.squeeze_bars, "box_atr": (b.squeeze_hi - b.squeeze_lo) / v.atr})
    expand = t.range60 / unit
    strength = 0.45 + 0.3 * clip(dist / 0.8) + 0.25 * clip((expand - 1.0) / 2.0)
    return Evidence("squeeze_release", clip(strength) * _late(dist), d,
                    f"squeeze releasing {'upward' if d > 0 else 'downward'}",
                    {"squeeze_bars": b.squeeze_bars, "dist_atr": dist, "expansion": expand, "seconds_beyond": since})


def fam_participation(v: MarketView) -> Evidence:
    b, t = v.bars, v.ticks
    base = b.vol_baseline
    if base <= 0:
        return Evidence("participation", 0.5, 0, "activity unknown", {})
    r10, r30 = t.tick_rate10 / base, t.tick_rate30 / base
    accel = t.tick_rate10 / t.tick_rate60 if t.tick_rate60 > 0 else 1.0
    strength = 0.6 * clip((r30 - 0.9) / 1.2) + 0.25 * clip((r10 - 1.0) / 1.5) + 0.15 * clip((accel - 1.0) / 1.0)
    phrase = "busy - many more price updates than usual" if strength >= 0.5 else "normal activity" if strength >= 0.15 else "thin activity"
    return Evidence("participation", clip(strength), 0, phrase,
                    {"rate30_vs_base": r30, "rate10_vs_base": r10, "rate_accel": accel, "upticks30": t.upticks30})


def fam_vwap(v: MarketView) -> Evidence:
    b, t = v.bars, v.ticks
    side = sign(t.mid - b.vwap)
    slope_d = sign(b.vwap_slope, 0.02)
    spiral = b.vwap_crossings >= 5
    d = slope_d if slope_d == side else 0
    strength = 0.0 if spiral or d == 0 else 0.4 + 0.6 * clip(abs(b.vwap_slope) / 0.6)
    phrase = ("price spiralling through a flat VWAP" if spiral else
              f"price {'above a rising' if d > 0 else 'below a falling'} VWAP" if d else "VWAP flat")
    return Evidence("vwap", clip(strength), d, phrase,
                    {"dist_atr": (t.mid - b.vwap) / v.atr, "slope": b.vwap_slope, "crossings": b.vwap_crossings})


def fam_ema_geometry(v: MarketView) -> Evidence:
    b, t = v.bars, v.ticks
    d = sign(b.ema21_slope, 0.01)
    if sign(b.sep, 0.02) != d:
        d = 0
    expanding = (abs(b.sep) - abs(b.sep_prev))
    stretch = (t.mid - b.ema9) / v.atr
    strength = 0.0
    if d:
        strength = (0.4 * clip(abs(b.ema21_slope) / 0.25) + 0.3 * clip(abs(b.sep) / 1.5)
                    + 0.3 * clip(expanding / 0.5)) * clip(1.0 - (abs(stretch) - 3.0) / 3.0, 0.3, 1.0)
    return Evidence("ema_geometry", clip(strength), d,
                    "averages fanning out" if strength >= 0.5 else "averages flat" if d == 0 else "averages sloping",
                    {"ema21_slope": b.ema21_slope, "sep": b.sep, "expanding": expanding, "stretch_atr": stretch})


def fam_adx(v: MarketView) -> Evidence:
    b = v.bars
    d = sign(b.pdi - b.mdi, 2.0)
    strength = 0.6 * clip((b.adx - 15.0) / 20.0) + 0.4 * clip(b.adx_change / 4.0) if d else 0.0
    phrase = (f"trend strength rising (ADX {b.adx:.0f}, up {b.adx_change:+.1f})" if b.adx_change > 1 else
              f"ADX {b.adx:.0f}")
    return Evidence("adx", clip(strength), d, phrase, {"adx": b.adx, "pdi": b.pdi, "mdi": b.mdi, "adx_change": b.adx_change})


def fam_candles(v: MarketView, bars_tail: Sequence[Bar]) -> Evidence:
    b, t = v.bars, v.ticks
    cs = list(bars_tail[-3:])
    if v.forming is not None:
        f = v.forming
        cs.append(Bar(f.time, f.open, max(f.high, t.mid), min(f.low, t.mid), t.mid, f.tick_volume))
    if not cs:
        return Evidence("candles", 0.0, 0, "", {})
    net = sum(c.close - c.open for c in cs)
    d = sign(net)
    if d == 0:
        return Evidence("candles", 0.0, 0, "mixed candles", {})
    bodies, locs, against = [], [], []
    for c in cs:
        r = c.high - c.low
        if r <= 0:
            continue
        bodies.append(abs(c.close - c.open) / r)
        locs.append(((c.close - c.low) / r) if d > 0 else ((c.high - c.close) / r))
        against.append(((c.high - max(c.open, c.close)) / r) if d > 0 else ((min(c.open, c.close) - c.low) / r))
    if not bodies:
        return Evidence("candles", 0.0, 0, "", {})
    same = sum(1 for c in cs if sign(c.close - c.open) == d) / len(cs)
    expand = cs[-1].high - cs[-1].low
    rel = expand / b.median_range if b.median_range > 0 else 1.0
    strength = (0.3 * (sum(bodies) / len(bodies)) + 0.25 * (sum(locs) / len(locs)) + 0.15 * (1 - sum(against) / len(against))
                + 0.15 * same + 0.15 * clip((rel - 0.8) / 1.5))
    return Evidence("candles", clip(strength), d,
                    "strong candles closing near their " + ("highs" if d > 0 else "lows") if strength >= 0.6 else "ordinary candles",
                    {"body": sum(bodies) / len(bodies), "close_loc": sum(locs) / len(locs), "same": same, "rel_size": rel})


def fam_microstructure(v: MarketView) -> Evidence:
    t = v.ticks
    piv = t.pivots[-6:]
    highs = [p for _, p, k in piv if k > 0]
    lows = [p for _, p, k in piv if k < 0]
    hh = sum(1 for a, b in zip(highs, highs[1:]) if b > a)
    hl = sum(1 for a, b in zip(lows, lows[1:]) if b > a)
    lh = sum(1 for a, b in zip(highs, highs[1:]) if b < a)
    ll = sum(1 for a, b in zip(lows, lows[1:]) if b < a)
    up, dn = hh + hl, lh + ll
    pairs = max(len(highs) - 1, 0) + max(len(lows) - 1, 0)
    # a straight run has few pivots: judge it by where the live price is against the last pivots
    last_hi = highs[-1] if highs else None
    last_lo = lows[-1] if lows else None
    if up > dn:
        d = 1
    elif dn > up:
        d = -1
    else:
        d = sign(t.moves.get(30, 0.0), 0.1 * v.atr)
    struct = (max(up, dn) / pairs) if pairs else 0.6
    beyond = 0.0
    if d > 0 and last_hi is not None and t.mid > last_hi:
        beyond = 1.0
    elif d < 0 and last_lo is not None and t.mid < last_lo:
        beyond = 1.0
    elif not piv and d:
        beyond = 1.0
    spread_ok = 1.0 if t.spread_median <= 0 else clip(1.5 - t.spread / t.spread_median)
    ticks_dir = (t.upticks30 - 0.5) * 2.0 * d if d else 0.0
    strength = 0.45 * struct + 0.25 * beyond + 0.15 * clip(ticks_dir / 0.4) + 0.15 * spread_ok if d else 0.0
    return Evidence("microstructure", clip(strength), d,
                    ("higher highs and higher lows" if d > 0 else "lower highs and lower lows") if strength >= 0.5 else "messy micro structure",
                    {"hh": hh, "hl": hl, "lh": lh, "ll": ll, "pivots": len(piv), "upticks30": t.upticks30,
                     "spread_vs_median": (t.spread / t.spread_median) if t.spread_median > 0 else 1.0,
                     "tick_rate10": t.tick_rate10})


def fam_sweep(v: MarketView, bars_tail: Sequence[Bar]) -> Evidence:
    """Family 23, defined mathematically: within the last 5 closed bars price
    traded at least 0.1 ATR beyond a confirmed swing (from before those bars)
    and the live price is now back at least 0.1 ATR on the other side of it.
    The trade direction is AWAY from the sweep."""
    b, t = v.bars, v.ticks
    atr = v.atr
    tail = list(bars_tail[-5:])
    if len(tail) < 3 or not b.swings:
        return Evidence("sweep", 0.0, 0, "", {})
    start = tail[0].time.timestamp()
    old = [(tt, p, k) for tt, p, k in b.swings if tt < start - 120.0]
    best = Evidence("sweep", 0.0, 0, "no sweep", {})
    for tt, p, k in old[-6:]:
        if k < 0:      # swing low swept, then reclaimed -> long
            ext = min(x.low for x in tail)
            if ext < p - 0.1 * atr and t.mid > p + 0.1 * atr:
                s = 0.5 + 0.5 * clip((t.mid - p) / atr)
                if s > best.strength:
                    best = Evidence("sweep", clip(s), 1, "swept the lows and snapped back above",
                                    {"level": p, "depth_atr": (p - ext) / atr, "reclaim_atr": (t.mid - p) / atr})
        else:
            ext = max(x.high for x in tail)
            if ext > p + 0.1 * atr and t.mid < p - 0.1 * atr:
                s = 0.5 + 0.5 * clip((p - t.mid) / atr)
                if s > best.strength:
                    best = Evidence("sweep", clip(s), -1, "swept the highs and snapped back below",
                                    {"level": p, "depth_atr": (ext - p) / atr, "reclaim_atr": (p - t.mid) / atr})
    return best


def round_levels(price: float, quote: str) -> tuple[float, float]:
    step = 0.50 if quote == "JPY" else 0.0050
    lo = math.floor(price / step) * step
    return lo, lo + step


def headroom(v: MarketView, d: int) -> tuple[float, str]:
    """Distance (price) to the next obstacle in direction d and its name."""
    t, b = v.ticks, v.bars
    eps = 0.05 * v.atr
    p = t.mid
    obstacles: list[tuple[float, str]] = []
    for _, lvl, k in b.swings[-12:]:
        obstacles.append((lvl, "a recent swing " + ("high" if k > 0 else "low")))
    for _, lvl, k in b.m5_swings[-12:]:
        obstacles.append((lvl, "a five-minute swing " + ("high" if k > 0 else "low")))
    lv = v.sessions
    if lv is not None:
        for name, (hi, lo, complete) in getattr(lv, "ranges", {}).items():
            if not complete:
                continue                     # a running session's extreme is where price is, not an obstacle
            label = {"ASIA": "the Asian", "LONDON": "the London", "NEWYORK": "the New York"}.get(name, name)
            obstacles += [(hi, f"{label} high"), (lo, f"{label} low")]
        if getattr(lv, "prev_day", None):
            obstacles += [(lv.prev_day[0], "yesterday's high"), (lv.prev_day[1], "yesterday's low")]
    lo_r, hi_r = round_levels(p, v.quote)
    obstacles += [(lo_r, "a round number"), (hi_r, "a round number"),
                  (hi_r + (hi_r - lo_r), "a round number"), (lo_r - (hi_r - lo_r), "a round number")]
    best, name = float("inf"), ""
    for lvl, nm in obstacles:
        dist = (lvl - p) * d
        if dist > eps and dist < best:
            best, name = dist, nm
    return best, name


def fam_headroom(v: MarketView, d: int, min_atr: float) -> Evidence:
    if d == 0:
        return Evidence("headroom", 0.5, 0, "", {})
    room, name = headroom(v, d)
    room_atr = room / v.atr if v.atr > 0 else 0.0
    strength = clip((room_atr - 1.0) / 4.0) if room != float("inf") else 1.0
    ok = room_atr >= min_atr
    phrase = (f"open space ahead ({room_atr:.1f} ATR to {name})" if ok and room != float("inf") else
              "open space ahead" if ok else f"running into {name} just ahead")
    return Evidence("headroom", strength, d, phrase,
                    {"headroom_atr": room_atr if room != float("inf") else 99.0, "obstacle": name, "ok": ok})


def cost_ratio(v: MarketView, d: int) -> tuple[float, float, float]:
    """COST_RATIO = (spread + commission + slippage) / expected available move.
    Returns (ratio, cost in price, expected move in price)."""
    t = v.ticks
    cost = t.spread + v.cost_price
    room, _ = headroom(v, d) if d else (float("inf"), "")
    run = max(v.expected_run_atr * v.atr, v.expected_run_atr5 * v.bars.atr5,
              v.expected_run_impulse * abs(t.moves.get(60, 0.0)))
    expected = min(room, run)
    if expected <= 0:
        return float("inf"), cost, 0.0
    return cost / expected, cost, expected


def fam_cost(v: MarketView, d: int, max_ratio: float) -> Evidence:
    r, cost, exp = cost_ratio(v, d)
    pip = v.pip or 1.0
    strength = clip(1.0 - r / (1.6 * max(max_ratio, 1e-9))) if r != float("inf") else 0.0
    phrase = (f"costs {cost / pip:.1f} pips against a {exp / pip:.1f}-pip move ({r:.0%})" if r != float("inf")
              else "no room for the move")
    return Evidence("cost", strength, 0, phrase,
                    {"cost_ratio": r if r != float("inf") else 99.0, "cost_pips": cost / pip,
                     "expected_pips": exp / pip, "spread_pips": v.ticks.spread / pip,
                     "spread_atr": v.ticks.spread / v.atr if v.atr > 0 else 0.0})


def fam_news(v: MarketView, d: int) -> Evidence:
    n = v.news
    if n is None or (n.upcoming is None and n.recent is None):
        return Evidence("news", 0.5, 0, "no news nearby", {})
    if n.upcoming is not None:
        return Evidence("news", 0.0, 0, f"{n.upcoming['currency']} {n.upcoming['name']} due in "
                                        f"{max(n.upcoming_seconds, 0) / 60:.0f} min - waiting",
                        {"blocked": True, "event": n.upcoming})
    ev = n.recent or {}
    label = f"{ev.get('currency', '')} {ev.get('name', '')}".strip()
    vals = {"event": ev, "seconds_since": n.seconds_since, "impulse_dir": n.impulse_dir, "impulse_atr": n.impulse_atr,
            "continuation": n.continuation, "rejection": n.rejection, "spread_recovered": n.spread_recovered,
            "retrace": n.retrace_fraction, "settled": n.settled}
    if ev.get("actual") is not None and ev.get("forecast") is not None:
        vals["surprise"] = float(ev["actual"]) - float(ev["forecast"])
    if not n.settled:
        vals["blocked"] = True
        return Evidence("news", 0.1, 0, f"{label} just out - not chasing the first move", vals)
    if n.rejection:
        return Evidence("news", 0.1, -n.impulse_dir, f"the move after {label} has been rejected", vals)
    if n.impulse_dir and n.continuation and n.spread_recovered:
        return Evidence("news", 1.0, n.impulse_dir, f"second wave after {label}", vals)
    if n.impulse_dir and n.spread_recovered:
        return Evidence("news", 0.7, n.impulse_dir, f"holding the move after {label}", vals)
    return Evidence("news", 0.3, n.impulse_dir, f"after {label}, spread not yet back to normal", vals)


def anti_chop(v: MarketView) -> Evidence:
    """Family 27. Mostly the last two to three minutes of ticks (so a clean
    break out of a flat range is NOT called chop): low two-minute efficiency,
    a big range with little net progress (back and forth), repeated big
    reversals, recent failed breakouts. The bar history (flat VWAP and
    averages, weak ADX, repeated mean crossing, wicks) adds weight only when
    the ticks are also choppy."""
    t, b = v.ticks, v.bars
    atr = v.atr
    low_eff120 = clip((0.30 - t.eff120) / 0.20)
    low_eff60 = clip((0.30 - t.eff60) / 0.20)
    rng = t.range120 / atr if atr > 0 else 0.0
    prog = abs(t.net120) / t.range120 if t.range120 > 0 else 1.0
    rng60 = t.range60 / atr if atr > 0 else 0.0
    prog60 = abs(t.moves.get(60, 0.0)) / t.range60 if t.range60 > 0 else 1.0
    back_forth = max(clip((rng - 2.0) / 3.0) * clip(1.0 - prog / 0.5),
                     clip((rng60 - 2.0) / 3.0) * clip(1.0 - prog60 / 0.5))
    reversals = clip((t.big_reversals - 1) / 3.0)
    failed = clip(b.failed_breakouts / 3.0)
    wicks = clip((b.wick_ratio - 0.45) / 0.3)
    tick_chop = 0.2 * low_eff120 + 0.15 * low_eff60 + 0.25 * back_forth + 0.3 * reversals + 0.1 * failed
    flat = clip(1.0 - abs(b.ema21_slope) / 0.12) * clip(1.0 - abs(b.vwap_slope) / 0.3)
    weak_adx = clip((20.0 - b.adx) / 10.0)
    bar_cross = clip((max(b.vwap_crossings, b.ema_crossings) - 3) / 5.0)
    bar_chop = (flat + weak_adx + bar_cross + wicks) / 4.0
    # four or more reversals of 1.5 ATR inside two minutes cannot happen in a
    # clean run: on its own that is chop
    rev_chop = clip((t.big_reversals - 2) / 2.5)
    chop = max(tick_chop + 0.2 * bar_chop * clip(tick_chop / 0.45), rev_chop)
    phrase = "choppy - back and forth" if chop >= 0.6 else "some back-and-forth" if chop >= 0.35 else "not choppy"
    return Evidence("chop", clip(chop), 0, phrase,
                    {"low_eff120": low_eff120, "low_eff60": low_eff60, "back_forth": back_forth,
                     "reversals": t.big_reversals, "failed": failed, "wicks": wicks, "flat": flat,
                     "weak_adx": weak_adx, "bar_cross": bar_cross, "tick_chop": tick_chop, "bar_chop": bar_chop})


def price_confirmation(v: MarketView, d: int, break_s: float, vel_s: float, min_break_atr: float) -> tuple[bool, float, str]:
    """The TRIGGER's price confirmation: the live price has just taken out the
    micro high (low) of the last ``break_s`` seconds and the last ``vel_s``
    seconds moved the same way. Returns (confirmed, level broken, why)."""
    t = v.ticks
    if d == 0 or not t.ok:
        return False, 0.0, "no direction"
    i0 = bisect.bisect_left(t.times, t.now - break_s)
    i1 = bisect.bisect_right(t.times, t.now - vel_s)
    seg = t.mids[i0:i1]
    if not seg:
        return False, 0.0, "not enough ticks"
    level = max(seg) if d > 0 else min(seg)
    through = (t.mid - level) * d
    p0 = price_at(t.times, t.mids, t.now - vel_s)
    recent = (t.mid - p0) * d if p0 is not None else 0.0
    ok = through >= min_break_atr * v.atr and recent > 0
    why = (f"price just took out the {int(break_s)}-second {'high' if d > 0 else 'low'}" if ok else
           f"waiting for price to take out the {int(break_s)}-second {'high' if d > 0 else 'low'}")
    return ok, level, why


def compute_families(v: MarketView, bars_tail: Sequence[Bar]) -> dict[str, Evidence]:
    """Every directional/contextual family that does not need the scan's
    chosen direction. Direction-dependent ones (acceleration, headroom,
    cost, news) are computed in score.py once the direction is known."""
    return {
        "volatility": fam_volatility(v),
        "efficiency": fam_efficiency(v),
        "velocity": fam_velocity(v),
        "momentum": fam_momentum_measures(v),
        "breakout": fam_breakout(v),
        "first_retest": fam_first_retest(v),
        "pullback": fam_pullback(v),
        "opening_range": fam_opening_range(v),
        "session_breakout": fam_session_breakout(v),
        "squeeze_release": fam_squeeze_release(v),
        "participation": fam_participation(v),
        "vwap": fam_vwap(v),
        "ema_geometry": fam_ema_geometry(v),
        "adx": fam_adx(v),
        "candles": fam_candles(v, bars_tail),
        "microstructure": fam_microstructure(v),
        "sweep": fam_sweep(v, bars_tail),
        "chop": anti_chop(v),
    }
