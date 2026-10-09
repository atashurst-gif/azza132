"""SYNTHETIC - NOT PERFORMANCE: a seeded generator of made-up FX ticks.

It exists only to prove that the research pipeline runs end to end (store
-> labels -> event study -> replay -> cost stress -> walk-forward ->
registry -> report). Nothing it produces is a result and every report built
from it says so on every page.

The model, so the pipeline has something to find:

* each of USD EUR GBP JPY CHF CAD AUD NZD has its own latent price (in
  pips); a pair moves as base minus quote, plus a little noise of its own -
  so currency strength and cross-market confirmation have real structure;
* the noise level and the tick arrival rate follow the sessions (quiet Asia,
  busy London and New York, a dead rollover with wide spreads);
* TRENDS: bursts of 2-15 minutes at 1.5-6 pips a minute for one currency;
* CHOP: violent back-and-forth episodes of a few minutes;
* SQUEEZES: 20-40 very quiet minutes, then a burst;
* NEWS-LIKE JUMPS: scheduled high-impact "releases" (also returned as
  CalendarEvents): a jump of 6-20 pips in a few seconds with spreads blown
  out, then a second wave (60%) or a rejection (40%).
"""
from __future__ import annotations

import datetime as dt
import math
import random
import zlib
from array import array
from typing import Callable, Optional, Sequence

from ...broker.base import CalendarEvent
from ...contracts import FX_CURRENCIES, classify_fx
from .. import synth
from .tickstore import DAY_MS, TickStore, day_start_ms

UTC = dt.timezone.utc
DEFAULT_SYMBOLS = ("EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "EURJPY", "GBPJPY")

# (from hour, to hour): (ticks per second, noise in pips per sqrt(second), spread factor)
SESSION_PROFILE = (
    ((0, 7), (0.8, 0.07, 1.0)),
    ((7, 12), (2.5, 0.15, 1.0)),
    ((12, 16), (3.0, 0.17, 1.0)),
    ((16, 21), (1.5, 0.11, 1.0)),
    ((21, 22), (0.3, 0.05, 3.0)),
    ((22, 24), (0.8, 0.07, 1.3)),
)
BASE_SPREAD_PIPS = {"EURUSD": 0.12, "GBPUSD": 0.25, "USDJPY": 0.15, "AUDUSD": 0.2, "USDCAD": 0.25, "USDCHF": 0.25,
                    "NZDUSD": 0.3, "EURJPY": 0.35, "GBPJPY": 0.6, "EURGBP": 0.3, "AUDNZD": 0.6, "EURCHF": 0.4}
NEWS_SLOTS = ((8, 30), (12, 30), (14, 0), (15, 0), (9, 30))
NEWS_NAMES = {"USD": "Synthetic US release", "EUR": "Synthetic euro-area release", "GBP": "Synthetic UK release",
              "JPY": "Synthetic Japan release", "AUD": "Synthetic Australia release", "CAD": "Synthetic Canada release",
              "CHF": "Synthetic Swiss release", "NZD": "Synthetic NZ release"}


def _profile(hour: int) -> tuple[float, float, float]:
    for (a, b), v in SESSION_PROFILE:
        if a <= hour < b:
            return v
    return SESSION_PROFILE[0][1]


def weekdays(n: int, last: dt.date) -> list[str]:
    out, d = [], last
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d -= dt.timedelta(days=1)
    return sorted(out)


class _Day:
    """One synthetic day's latent currency paths, one value per second."""

    def __init__(self, rng: random.Random, day: str, start_levels: dict, hours: tuple[int, int]):
        self.day = day
        self.hours = hours
        n = 86_400
        self.x = {c: array("d", [0.0]) * (n + 1) for c in FX_CURRENCIES}
        self.spread_mult = array("d", [1.0]) * (n + 1)
        self.events: list[CalendarEvent] = []
        drift = {c: array("d", [0.0]) * (n + 1) for c in FX_CURRENCIES}
        quiet = {c: array("d", [1.0]) * (n + 1) for c in FX_CURRENCIES}
        add = {c: array("d", [0.0]) * (n + 1) for c in FX_CURRENCIES}
        h0, h1 = hours
        busy0, busy1 = max(h0, 7) * 3600, min(h1, 20) * 3600
        if busy1 <= busy0:
            busy0, busy1 = h0 * 3600, h1 * 3600
        for c in FX_CURRENCIES:
            # trend bursts
            for _ in range(rng.randint(3, 7)):
                s0 = rng.randrange(busy0, max(busy0 + 1, busy1 - 900))
                dur = rng.randint(120, 900)
                speed = rng.choice((-1, 1)) * rng.uniform(1.5, 6.0) / 60.0
                for s in range(s0, min(n, s0 + dur)):
                    drift[c][s] += speed
            # chop episodes
            for _ in range(rng.randint(2, 5)):
                s0 = rng.randrange(busy0, max(busy0 + 1, busy1 - 600))
                dur = rng.randint(180, 600)
                amp, period, ph = rng.uniform(2.0, 6.0), rng.uniform(30.0, 90.0), rng.uniform(0, 2 * math.pi)
                for s in range(s0, min(n, s0 + dur)):
                    fade = min(1.0, (s - s0) / 30.0, (s0 + dur - s) / 30.0)
                    add[c][s] += fade * amp * math.sin(2 * math.pi * (s - s0) / period + ph)
            # a squeeze, then a burst
            if rng.random() < 0.6:
                s0 = rng.randrange(busy0, max(busy0 + 1, busy1 - 3600))
                dur = rng.randint(1200, 2400)
                for s in range(s0, min(n, s0 + dur)):
                    quiet[c][s] = 0.3
                speed = rng.choice((-1, 1)) * rng.uniform(3.0, 6.0) / 60.0
                for s in range(s0 + dur, min(n, s0 + dur + rng.randint(180, 600))):
                    drift[c][s] += speed
        # news-like releases
        slots = [sl for sl in NEWS_SLOTS if h0 * 60 <= sl[0] * 60 + sl[1] < h1 * 60 - 30]
        rng.shuffle(slots)
        d0 = dt.datetime.fromisoformat(day).replace(tzinfo=UTC)
        for k, (hh, mm) in enumerate(sorted(slots[:rng.randint(1, 3)])):
            c = rng.choice(("USD", "EUR", "GBP", "JPY", "AUD"))
            s0 = hh * 3600 + mm * 60
            jump = rng.choice((-1, 1)) * rng.uniform(6.0, 20.0)
            jump_s = rng.randint(2, 5)
            for s in range(s0, s0 + jump_s):
                drift[c][s] += jump / jump_s
            follow = rng.random() < 0.6
            speed = (1 if follow else -1) * (jump / abs(jump)) * rng.uniform(2.0, 4.0) / 60.0
            for s in range(s0 + 30, min(n, s0 + 30 + rng.randint(180, 420))):
                drift[c][s] += speed
            for s in range(s0, min(n, s0 + 60)):
                self.spread_mult[s] = max(self.spread_mult[s], 1.0 + 4.0 * max(0.0, 1.0 - (s - s0) / 20.0))
            forecast = round(rng.uniform(0.1, 3.0), 1)
            actual = round(forecast + (0.3 if jump > 0 else -0.3) * (1 if c != "JPY" else -1), 1)
            self.events.append(CalendarEvent(f"SYN-{day}-{k}", d0 + dt.timedelta(seconds=s0), c, c[:2], NEWS_NAMES[c],
                                             3, actual, forecast, round(forecast - 0.1, 1), None, "", "synthetic"))
        for c in FX_CURRENCIES:
            x = start_levels.get(c, 0.0)
            xc, dc, qc, ac = self.x[c], drift[c], quiet[c], add[c]
            for s in range(n + 1):
                _rate, noise, _spm = _profile(s // 3600 if s < n else 23)
                if h0 * 3600 <= s < h1 * 3600:
                    x += dc[s] + rng.gauss(0.0, noise * qc[s] * 0.7)
                xc[s] = x + ac[s]
            start_levels[c] = x


def generate(store: TickStore, days: Sequence[str], symbols: Sequence[str] = DEFAULT_SYMBOLS, seed: int = 7,
             tick_scale: float = 1.0, hours: tuple[int, int] = (0, 24),
             progress: Optional[Callable[[str], None]] = None) -> tuple[dict, list]:
    """Writes one SYNTHETIC tick file per (symbol, day) into ``store``.
    Returns (specs by symbol, calendar events)."""
    specs = {s: synth.make_spec(s) for s in symbols}
    levels: dict[str, float] = {}
    idio: dict[str, float] = {s: 0.0 for s in symbols}
    events: list[CalendarEvent] = []
    for day in sorted(days):
        rng = random.Random(seed * 1_000_003 + zlib.crc32(day.encode()))
        D = _Day(rng, day, levels, hours)
        events.extend(D.events)
        d0 = day_start_ms(day)
        for sym in symbols:
            r2 = random.Random(seed * 7919 + zlib.crc32(f"{sym}{day}".encode()))
            base, quote, _ = classify_fx(sym)
            digits, _tv, price0 = synth.SPECS.get(sym, (5, 0.74, 1.0))
            pip = synth.pip_of(sym)
            point = 10.0 ** -digits
            sp_base = BASE_SPREAD_PIPS.get(sym, 0.4)
            rate_mult = 1.0 if "USD" in (base, quote) else 0.7
            xb, xq, sm = D.x[base], D.x[quote], D.spread_mult
            times, bids, asks = array("q"), array("d"), array("d")
            t = float(hours[0] * 3600)
            idi = idio[sym]
            while True:
                hour = int(t // 3600)
                rate, _noise, spf = _profile(min(hour, 23))
                lam = max(rate * rate_mult * tick_scale, 1e-3)
                t += r2.expovariate(lam)
                if t >= hours[1] * 3600 or t >= 86_400:
                    break
                s = int(t)
                f = t - s
                mid_p = ((xb[s] - xq[s]) * (1 - f) + (xb[s + 1] - xq[s + 1]) * f)
                idi += r2.gauss(0.0, 0.02)
                mid = price0 + (mid_p + idi + r2.gauss(0.0, 0.04)) * pip
                spread = sp_base * spf * sm[s] * (1.0 + 0.25 * abs(r2.gauss(0.0, 1.0))) * pip
                bid = round(mid - spread / 2.0, digits)
                ask = round(max(bid + point, mid + spread / 2.0), digits)
                ms = d0 + int(t * 1000.0)
                if times and ms <= times[-1]:
                    ms = times[-1] + 1
                if ms >= d0 + DAY_MS:
                    break
                times.append(ms)
                bids.append(bid)
                asks.append(ask)
            idio[sym] = idi
            store.write_day(sym, day, times, bids, asks, complete=True, source="SYNTHETIC")
            if progress:
                progress(f"SYNTHETIC {sym} {day}: {len(times):,} made-up ticks")
    return specs, events
