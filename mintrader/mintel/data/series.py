"""Bar series container and supporting measurements.

Indicators live here but hold no authority.  They are *measurements* that feed
evidence families in :mod:`mintel.engine.evidence`; they never vote directly.
Deliberately, EMA slope / MACD / ROC are grouped as one "momentum" measurement
family rather than three independent votes.
"""
from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

from ..broker.base import Bar, TF


def ema(values: Sequence[float], period: int) -> list[float]:
    if not values or period <= 0:
        return []
    k = 2.0 / (period + 1.0)
    out = [float(values[0])]
    for v in values[1:]:
        out.append(out[-1] + k * (float(v) - out[-1]))
    return out


def sma(values: Sequence[float], period: int) -> list[float]:
    out, run = [], 0.0
    for i, v in enumerate(values):
        run += v
        if i >= period:
            run -= values[i - period]
        out.append(run / min(i + 1, period))
    return out


def stdev(values: Sequence[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    m = sum(values) / n
    return math.sqrt(sum((v - m) ** 2 for v in values) / (n - 1))


def true_ranges(bars: Sequence[Bar]) -> list[float]:
    out = []
    for i, b in enumerate(bars):
        if i == 0:
            out.append(b.high - b.low)
        else:
            pc = bars[i - 1].close
            out.append(max(b.high - b.low, abs(b.high - pc), abs(b.low - pc)))
    return out


def atr(bars: Sequence[Bar], period: int = 14) -> float:
    """Wilder ATR of the completed series (price units)."""
    if len(bars) < 2:
        return 0.0
    tr = true_ranges(bars)
    period = min(period, len(tr))
    val = sum(tr[:period]) / period
    for x in tr[period:]:
        val = (val * (period - 1) + x) / period
    return val


def atr_series(bars: Sequence[Bar], period: int = 14) -> list[float]:
    tr = true_ranges(bars)
    if not tr:
        return []
    out, val = [], tr[0]
    for i, x in enumerate(tr):
        p = min(period, i + 1)
        val = (val * (p - 1) + x) / p
        out.append(val)
    return out


def rsi(closes: Sequence[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains = losses = 0.0
    for i in range(len(closes) - period, len(closes)):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    if losses == 0:
        return 100.0
    rs = (gains / period) / (losses / period)
    return 100.0 - 100.0 / (1.0 + rs)


def adx_dmi(bars: Sequence[Bar], period: int = 14) -> tuple[float, float, float]:
    """Return (ADX, +DI, -DI).  ADX measures trend *strength*, not direction."""
    if len(bars) < period * 2:
        return 0.0, 0.0, 0.0
    plus_dm, minus_dm, trs = [], [], []
    for i in range(1, len(bars)):
        up = bars[i].high - bars[i - 1].high
        dn = bars[i - 1].low - bars[i].low
        plus_dm.append(up if (up > dn and up > 0) else 0.0)
        minus_dm.append(dn if (dn > up and dn > 0) else 0.0)
        pc = bars[i - 1].close
        trs.append(max(bars[i].high - bars[i].low,
                       abs(bars[i].high - pc), abs(bars[i].low - pc)))

    def wilder(xs: Sequence[float]) -> list[float]:
        v = sum(xs[:period])
        out = [v]
        for x in xs[period:]:
            v = v - v / period + x
            out.append(v)
        return out

    tr_s, p_s, m_s = wilder(trs), wilder(plus_dm), wilder(minus_dm)
    dxs = []
    for tr_v, p_v, m_v in zip(tr_s, p_s, m_s):
        if tr_v <= 0:
            continue
        pdi, mdi = 100.0 * p_v / tr_v, 100.0 * m_v / tr_v
        s = pdi + mdi
        dxs.append(100.0 * abs(pdi - mdi) / s if s else 0.0)
    if not dxs:
        return 0.0, 0.0, 0.0
    tail = dxs[-period:]
    adx = sum(tail) / len(tail)
    tr_v, p_v, m_v = tr_s[-1], p_s[-1], m_s[-1]
    pdi = 100.0 * p_v / tr_v if tr_v else 0.0
    mdi = 100.0 * m_v / tr_v if tr_v else 0.0
    return adx, pdi, mdi


def percentile_rank(value: float, sample: Sequence[float]) -> float:
    """Fraction of ``sample`` at or below ``value``, in [0, 1]."""
    if not sample:
        return 0.5
    below = sum(1 for s in sample if s <= value)
    return below / len(sample)


def session_vwap(bars: Sequence[Bar], since: dt.datetime) -> Optional[float]:
    """Tick-volume-weighted average price since ``since``.

    Brokers rarely publish true traded volume on CFDs, so this is a tick-volume
    VWAP.  It is treated as a reference level, never as exchange VWAP.
    """
    num = den = 0.0
    for b in bars:
        if b.time < since:
            continue
        typical = (b.high + b.low + b.close) / 3.0
        w = max(b.tick_volume, 1.0)
        num += typical * w
        den += w
    return num / den if den else None


def linreg_slope(values: Sequence[float]) -> float:
    """Least-squares slope per bar."""
    n = len(values)
    if n < 2:
        return 0.0
    xm = (n - 1) / 2.0
    ym = sum(values) / n
    num = sum((i - xm) * (v - ym) for i, v in enumerate(values))
    den = sum((i - xm) ** 2 for i in range(n))
    return num / den if den else 0.0


@dataclass
class MultiTFData:
    """All timeframe views of one instrument at one moment.

    The engine always reads bars through this object so that a missing or short
    timeframe degrades the analysis gracefully instead of raising.
    """
    symbol: str
    frames: dict[TF, list[Bar]]
    as_of: dt.datetime

    def get(self, tf: TF, min_bars: int = 1) -> list[Bar]:
        bars = self.frames.get(tf, [])
        return bars if len(bars) >= min_bars else []

    def closes(self, tf: TF) -> list[float]:
        return [b.close for b in self.frames.get(tf, [])]

    def has(self, tf: TF, min_bars: int = 30) -> bool:
        return len(self.frames.get(tf, [])) >= min_bars

    @property
    def available(self) -> tuple[TF, ...]:
        return tuple(tf for tf, bars in self.frames.items() if bars)

    def last_close(self, tf: TF) -> Optional[float]:
        bars = self.frames.get(tf)
        return bars[-1].close if bars else None
