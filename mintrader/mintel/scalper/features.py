"""Tick-level and bar-level features for the Rapid Scalper.

Everything measurable is measured; nothing is pretended. Order flow (DOM,
Level 2) is NOT available over this feed, so that component is explicitly
disabled rather than manufactured from candles.
"""
from __future__ import annotations

import datetime as dt
import math
import statistics as st
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, Sequence

from ..broker.base import Bar, Tick


def ema(values: Sequence[float], period: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (period + 1.0)
    out = [values[0]]
    for v in values[1:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out


@dataclass
class TickBuffer:
    """The last few minutes of ticks for one symbol."""
    symbol: str
    seconds: float = 180.0
    ticks: deque = field(default_factory=deque)

    def add(self, t: Tick) -> bool:
        """Append if newer than the last one. Returns True when new."""
        if self.ticks and t.time <= self.ticks[-1].time and \
                abs(t.bid - self.ticks[-1].bid) < 1e-12 and abs(t.ask - self.ticks[-1].ask) < 1e-12:
            return False
        self.ticks.append(t)
        cutoff = t.time - dt.timedelta(seconds=self.seconds)
        while self.ticks and self.ticks[0].time < cutoff:
            self.ticks.popleft()
        return True

    def since(self, seconds: float, now: Optional[dt.datetime] = None) -> list[Tick]:
        if not self.ticks:
            return []
        now = now or self.ticks[-1].time
        cutoff = now - dt.timedelta(seconds=seconds)
        return [t for t in self.ticks if t.time >= cutoff]

    @property
    def last(self) -> Optional[Tick]:
        return self.ticks[-1] if self.ticks else None


@dataclass
class Features:
    symbol: str
    ok: bool
    reason: str = ""
    point: float = 0.0
    # spread
    spread_points: float = 0.0
    spread_avg_points: float = 0.0
    spread_expansion: float = 1.0
    # momentum (points per second, signed)
    velocity_5s: float = 0.0
    velocity_20s: float = 0.0
    acceleration: float = 0.0
    consecutive_ticks: int = 0           # signed run of same-direction ticks
    rate_of_change_30s_points: float = 0.0
    persistence: float = 0.0             # fraction of 5s windows agreeing with 30s direction
    # liquidity / activity
    ticks_per_minute: float = 0.0
    realised_vol_points: float = 0.0     # std of tick-to-tick mid changes over 60s
    # trend
    ema_fast: float = 0.0
    ema_slow: float = 0.0
    vwap: float = 0.0
    mid: float = 0.0
    trend_bias: int = 0                  # +1 long bias, -1 short, 0 none
    # bars
    bar_range_ratio: float = 1.0         # last bar range / median
    volume_ratio: float = 1.0            # last bar volume / median
    volume_accel: float = 1.0            # last 3 bars / prior 10
    efficiency: float = 0.0              # directional efficiency 0..1 over 20 bars
    vwap_crossings: int = 0              # crossings in the last 20 bars
    ema_compression_points: float = 0.0
    failed_breakouts: int = 0
    atr_points: float = 0.0
    # structure
    micro_low: float = 0.0
    micro_high: float = 0.0
    # the bigger picture, from one-minute bars (points, signed: + is up)
    move_1m_points: float = 0.0
    move_5m_points: float = 0.0
    move_20m_points: float = 0.0
    bars_ok: bool = False
    # the last two COMPLETED one-minute bars: where a trend's pullbacks hold
    bar_low_2: float = 0.0
    bar_high_2: float = 0.0
    dist_from_fast_ema_points: float = 0.0
    # pullback structure, from COMPLETED one-minute bars (prices)
    last_bar_high: float = 0.0           # the last completed bar
    last_bar_low: float = 0.0
    swing_low_3: float = 0.0             # lowest low / highest high of the last three
    swing_high_3: float = 0.0
    high_20: float = 0.0                 # the last twenty: where the move last turned
    low_20: float = 0.0
    # order flow - not available over this feed
    order_flow_available: bool = False

    def timeframes(self) -> tuple[int, int, int]:
        """Direction (+1/-1/0) of the last 1, 5 and 20 minutes."""
        sg = lambda x: 1 if x > 0 else (-1 if x < 0 else 0)    # noqa: E731
        return sg(self.move_1m_points), sg(self.move_5m_points), sg(self.move_20m_points)

    def direction(self) -> int:
        """Where the immediate move is going, if anywhere."""
        if self.velocity_5s > 0 and self.rate_of_change_30s_points > 0:
            return 1
        if self.velocity_5s < 0 and self.rate_of_change_30s_points < 0:
            return -1
        return 0


def compute(symbol: str, buf: TickBuffer, bars: Sequence[Bar], point: float,
            now: Optional[dt.datetime] = None) -> Features:
    f = Features(symbol=symbol, ok=False, point=point)
    ticks = list(buf.ticks)
    if len(ticks) < 8 or point <= 0:
        f.reason = "not enough ticks yet"
        return f
    now = now or ticks[-1].time
    last = ticks[-1]
    f.mid = last.mid
    # ---- spread
    spreads = [(t.ask - t.bid) / point for t in ticks]
    f.spread_points = spreads[-1]
    f.spread_avg_points = st.median(spreads) if spreads else spreads[-1]
    f.spread_expansion = (f.spread_points / f.spread_avg_points) if f.spread_avg_points > 0 else 1.0
    # ---- momentum
    def vel(seconds: float) -> float:
        w = buf.since(seconds, now)
        if len(w) < 2:
            return 0.0
        span = (w[-1].time - w[0].time).total_seconds()
        if span <= 0:
            return 0.0
        return ((w[-1].mid - w[0].mid) / point) / span
    f.velocity_5s = vel(5.0)
    f.velocity_20s = vel(20.0)
    w10 = buf.since(10.0, now)
    w5 = buf.since(5.0, now)
    prior = [t for t in w10 if t not in w5]
    if len(prior) >= 2 and len(w5) >= 2:
        sp = (prior[-1].time - prior[0].time).total_seconds() or 1.0
        v_prior = ((prior[-1].mid - prior[0].mid) / point) / sp
        f.acceleration = f.velocity_5s - v_prior
    run = 0
    for a, b in zip(ticks[-30:-1], ticks[-29:]):
        d = 1 if b.mid > a.mid else (-1 if b.mid < a.mid else 0)
        if d == 0:
            continue
        if run == 0 or (run > 0) == (d > 0):
            run += d
        else:
            run = d
    f.consecutive_ticks = run
    w30 = buf.since(30.0, now)
    if len(w30) >= 2:
        f.rate_of_change_30s_points = (w30[-1].mid - w30[0].mid) / point
        sign30 = 1 if f.rate_of_change_30s_points > 0 else -1
        agree = tot = 0
        step = 5.0
        for k in range(6):
            end = now - dt.timedelta(seconds=k * step)
            start = end - dt.timedelta(seconds=step)
            w = [t for t in w30 if start <= t.time <= end]
            if len(w) >= 2:
                tot += 1
                if (w[-1].mid - w[0].mid) * sign30 > 0:
                    agree += 1
        f.persistence = agree / tot if tot else 0.0
    # ---- liquidity / activity
    w60 = buf.since(60.0, now)
    # The bot polls the latest price, so its own buffer can never hold more
    # ticks than polls per minute (about nine on a full 15-market pass). The
    # market's real activity is the broker's tick count per one-minute bar;
    # use the median of the last three completed bars when we have them.
    # (5 Oct: every market read "too thin (9 ticks/min)" overnight.)
    polled = float(len(w60))
    done = [b for b in bars[-4:-1] if getattr(b, "tick_volume", 0)] if len(bars) >= 4 else []
    if done:
        vols = sorted(float(b.tick_volume) for b in done)
        f.ticks_per_minute = max(polled, vols[len(vols) // 2])
    else:
        f.ticks_per_minute = polled
    if len(w60) >= 3:
        diffs = [(b.mid - a.mid) / point for a, b in zip(w60[:-1], w60[1:])]
        f.realised_vol_points = st.pstdev(diffs) if len(diffs) > 1 else 0.0
    # ---- bars
    closes = [b.close for b in bars]
    if len(closes) >= 25:
        e9 = ema(closes, 9)
        e21 = ema(closes, 21)
        f.ema_fast, f.ema_slow = e9[-1], e21[-1]
        f.ema_compression_points = abs(f.ema_fast - f.ema_slow) / point
        vols = [max(b.tick_volume, 0.0) for b in bars]
        tp = [(b.high + b.low + b.close) / 3.0 for b in bars]
        vsum = sum(vols[-60:]) or 1.0
        f.vwap = sum(p * v for p, v in zip(tp[-60:], vols[-60:])) / vsum if vsum else closes[-1]
        rng = [b.high - b.low for b in bars[-30:]]
        med_rng = st.median(rng) if rng else 0.0
        f.bar_range_ratio = (rng[-1] / med_rng) if med_rng > 0 else 1.0
        f.atr_points = (st.mean(rng[-14:]) / point) if rng else 0.0
        med_vol = st.median(vols[-30:]) if vols else 0.0
        f.volume_ratio = (vols[-1] / med_vol) if med_vol > 0 else 1.0
        recent = st.mean(vols[-3:]) if vols else 0.0
        before = st.mean(vols[-13:-3]) if len(vols) >= 13 else recent
        f.volume_accel = (recent / before) if before > 0 else 1.0
        c20 = closes[-20:]
        path = sum(abs(b - a) for a, b in zip(c20[:-1], c20[1:]))
        f.efficiency = abs(c20[-1] - c20[0]) / path if path > 0 else 0.0
        side = [1 if c > f.vwap else -1 for c in c20]
        f.vwap_crossings = sum(1 for a, b in zip(side[:-1], side[1:]) if a != b)
        hi20 = max(b.high for b in bars[-21:-1]); lo20 = min(b.low for b in bars[-21:-1])
        fails = 0
        for b in bars[-10:]:
            if (b.high > hi20 and b.close < hi20) or (b.low < lo20 and b.close > lo20):
                fails += 1
        f.failed_breakouts = fails
    if len(bars) >= 23:
        # now = the live price (bars are bid prices); the forming bar in the
        # cache can be several seconds old
        now_px = last.bid if last.bid else closes[-1]
        f.move_1m_points = (now_px - closes[-2]) / point
        f.move_5m_points = (now_px - closes[-6]) / point
        f.move_20m_points = (now_px - closes[-21]) / point
        done2 = bars[-3:-1]
        f.bar_low_2 = min(b.low for b in done2)
        f.bar_high_2 = max(b.high for b in done2)
        f.last_bar_high, f.last_bar_low = bars[-2].high, bars[-2].low
        done3 = bars[-4:-1]
        f.swing_low_3 = min(b.low for b in done3)
        f.swing_high_3 = max(b.high for b in done3)
        done20 = bars[-21:-1]
        f.high_20 = max(b.high for b in done20)
        f.low_20 = min(b.low for b in done20)
        f.bars_ok = True
        if f.mid > f.ema_fast > f.ema_slow:
            f.trend_bias = 1
        elif f.mid < f.ema_fast < f.ema_slow:
            f.trend_bias = -1
        f.dist_from_fast_ema_points = (f.mid - f.ema_fast) / point
    else:
        f.reason = "not enough bars yet"
        return f
    # ---- micro structure from the last 60 s of ticks
    w = w60 if len(w60) >= 5 else ticks[-20:]
    f.micro_low = min(t.bid for t in w)
    f.micro_high = max(t.ask for t in w)
    f.ok = True
    return f


def chop_score(f: Features) -> tuple[float, list[str]]:
    """0 (clean) .. 1 (pure chop), with the reasons."""
    reasons: list[str] = []
    s = 0.0
    if f.efficiency < 0.25:
        s += 0.3; reasons.append(f"low directional efficiency ({f.efficiency:.2f})")
    if f.vwap_crossings >= 6:
        s += 0.25; reasons.append(f"price crossing VWAP repeatedly ({f.vwap_crossings}x)")
    if f.atr_points > 0 and f.ema_compression_points < 0.25 * f.atr_points:
        s += 0.2; reasons.append("fast and slow EMA compressed")
    if f.failed_breakouts >= 2:
        s += 0.2; reasons.append(f"{f.failed_breakouts} failed breakouts recently")
    if f.persistence < 0.4:
        s += 0.15; reasons.append("tick momentum alternating")
    if f.realised_vol_points > 0 and f.spread_points > 0.6 * f.realised_vol_points * 3:
        s += 0.15; reasons.append("spread eats the expected movement")
    return min(1.0, s), reasons
