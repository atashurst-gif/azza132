"""SYNTHETIC tick paths for the Rapid Momentum Rider's self-tests.

Everything here is made-up data with a fixed seed. It exists so the tests
can show the machinery behaves as designed (a clean trend triggers, violent
chop waits, a squeeze release triggers the right way). Nothing produced
from these paths is a result, and nothing here may ever be reported as
performance - real-tick research lives in mintel/rider/research/.
"""
from __future__ import annotations

import datetime as dt
import math
import random
from typing import Iterable, Optional

from ..broker.base import Tick
from ..contracts import SymbolSpec, classify_fx

UTC = dt.timezone.utc
DATA_SOURCE = "SYNTHETIC (seeded random paths, not market data)"

# realistic MT5 values for a GBP account (tick_value = GBP per tick per 1.0 lot)
SPECS = {
    "EURUSD": (5, 0.74, 1.0850), "GBPUSD": (5, 0.74, 1.2700), "AUDUSD": (5, 0.74, 0.6550),
    "NZDUSD": (5, 0.74, 0.6050), "USDJPY": (3, 0.4926, 151.20), "USDCHF": (5, 0.83, 0.8900),
    "USDCAD": (5, 0.54, 1.3600), "EURJPY": (3, 0.4926, 164.05), "GBPJPY": (3, 0.4926, 192.00),
    "EURGBP": (5, 1.00, 0.8540), "AUDNZD": (5, 0.43, 1.0830), "EURCHF": (5, 0.83, 0.9650),
}


def make_spec(name: str, digits: Optional[int] = None, tick_value: Optional[float] = None,
              volume_min: float = 0.01, volume_max: float = 50.0, volume_step: float = 0.01) -> SymbolSpec:
    d0, tv0, _ = SPECS.get(name, (5, 0.74, 1.0))
    digits = d0 if digits is None else digits
    point = 10.0 ** -digits
    base, quote, group = classify_fx(name)
    return SymbolSpec(name=name, digits=digits, point=point, tick_size=point,
                      tick_value=tv0 if tick_value is None else tick_value, contract_size=100_000.0,
                      volume_min=volume_min, volume_max=volume_max, volume_step=volume_step,
                      stops_level_points=0.0, freeze_level_points=0.0, base_currency=base, profit_currency=quote,
                      margin_currency=base, asset_group=group or "FX_MAJOR", typical_spread_points=2.0)


def pip_of(name: str) -> float:
    return 0.01 if name.endswith("JPY") else 0.0001


def ticks_from_path(symbol: str, start: dt.datetime, mids_pips: Iterable[tuple[float, float]], base_price: float,
                    spread_pips: float = 0.15, digits: int = 5) -> list[Tick]:
    """(seconds from start, mid in pips from base) -> Ticks with bid/ask."""
    pip = pip_of(symbol)
    out = []
    for sec, mp in mids_pips:
        mid = base_price + mp * pip
        half = spread_pips * pip / 2.0
        out.append(Tick(symbol, start + dt.timedelta(seconds=sec), round(mid - half, digits), round(mid + half, digits)))
    return out


def _times(rng: random.Random, seconds: float, mean_gap: float) -> list[float]:
    t, out = 0.0, []
    while t < seconds:
        t += rng.expovariate(1.0 / mean_gap)
        out.append(t)
    return out


def quiet(rng: random.Random, seconds: float, start_pips: float, noise: float = 0.12, mean_gap: float = 0.5,
          pull: float = 0.002, anchor: Optional[float] = None) -> list[tuple[float, float]]:
    """A mean-reverting quiet market (noise in pips per tick)."""
    x = start_pips
    a = start_pips if anchor is None else anchor
    out = []
    for t in _times(rng, seconds, mean_gap):
        x += rng.gauss(0.0, noise) - pull * (x - a)
        out.append((t, x))
    return out


def trend(rng: random.Random, seconds: float, start_pips: float, pips_per_minute: float, noise: float = 0.12,
          mean_gap: float = 0.4) -> list[tuple[float, float]]:
    x = start_pips
    out = []
    step = pips_per_minute / 60.0
    last = 0.0
    for t in _times(rng, seconds, mean_gap):
        x += step * (t - last) + rng.gauss(0.0, noise)
        last = t
        out.append((t, x))
    return out


def chop(rng: random.Random, seconds: float, start_pips: float, amplitude: float = 6.0, period: float = 40.0,
         noise: float = 0.25, mean_gap: float = 0.4) -> list[tuple[float, float]]:
    """Violent chop: big fast swings back and forth around one level."""
    out = []
    phase = rng.random() * 2 * math.pi
    for t in _times(rng, seconds, mean_gap):
        x = start_pips + amplitude * math.sin(2 * math.pi * t / period + phase) + rng.gauss(0.0, noise)
        out.append((t, x))
    return out


def join(*segments: list[tuple[float, float]]) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    offset = 0.0
    for seg in segments:
        for t, x in seg:
            out.append((offset + t, x))
        if seg:
            offset += seg[-1][0]
    return out


def scenario(kind: str, symbol: str = "EURUSD", seed: int = 11, start: Optional[dt.datetime] = None,
             direction: int = 1, warm_minutes: float = 60.0, run_minutes: float = 5.0,
             pips_per_minute: float = 4.0) -> tuple[list[Tick], dt.datetime]:
    """Returns (ticks, the moment the interesting part starts)."""
    rng = random.Random(seed)
    start = start or dt.datetime(2026, 10, 6, 6, 30, tzinfo=UTC)      # London morning (BST)
    base = SPECS.get(symbol, (5, 0.74, 1.0))[2]
    digits = SPECS.get(symbol, (5, 0.74, 1.0))[0]
    warm = quiet(rng, warm_minutes * 60.0, 0.0)
    x0 = warm[-1][1] if warm else 0.0
    if kind == "trend":
        segs = [warm, trend(rng, run_minutes * 60.0, x0, direction * pips_per_minute)]
    elif kind == "chop":
        segs = [warm, chop(rng, run_minutes * 60.0, x0)]
    elif kind == "squeeze":
        sq = quiet(rng, 25 * 60.0, x0, noise=0.04, pull=0.02)
        x1 = sq[-1][1]
        segs = [warm, sq, trend(rng, run_minutes * 60.0, x1, direction * pips_per_minute * 1.25)]
    elif kind == "quiet":
        segs = [warm, quiet(rng, run_minutes * 60.0, x0)]
    else:
        raise ValueError(kind)
    path = join(*segs)
    pivot = start + dt.timedelta(seconds=sum(s[-1][0] for s in segs[:-1] if s))
    return ticks_from_path(symbol, start, path, base, digits=digits), pivot


def currency_paths(strength_pips_per_min: dict, symbols: Iterable[str], seed: int = 5,
                   start: Optional[dt.datetime] = None, warm_minutes: float = 60.0,
                   run_minutes: float = 4.0, noise: float = 0.1) -> tuple[dict[str, list[Tick]], dt.datetime]:
    """A factor model: each currency drifts at its own speed during the run;
    a pair moves as base minus quote, plus its own small noise."""
    rng = random.Random(seed)
    start = start or dt.datetime(2026, 10, 6, 12, 30, tzinfo=UTC)
    warm_s, run_s = warm_minutes * 60.0, run_minutes * 60.0
    out: dict[str, list[Tick]] = {}
    for sym in symbols:
        base, quote, _ = classify_fx(sym)
        drift = (strength_pips_per_min.get(base, 0.0) - strength_pips_per_min.get(quote, 0.0)) / 60.0
        w = quiet(rng, warm_s, 0.0, noise=noise)
        x = w[-1][1]
        seg = []
        last = 0.0
        for t in _times(rng, run_s, 0.4):
            x += drift * (t - last) + rng.gauss(0.0, noise)
            last = t
            seg.append((t, x))
        d0, _tv, px = SPECS.get(sym, (5, 0.74, 1.0))
        out[sym] = ticks_from_path(sym, start, join(w, seg), px, digits=d0)
    return out, start + dt.timedelta(seconds=warm_s)
