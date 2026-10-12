"""Made-up prices for tests and for dry runs. Never real, always stamped.

Everything here is SYNTHETIC: a store filled from this module (or from the
simulated broker) carries ``source: "SYNTHETIC"`` and every result built from
it says ``synthetic: true`` all the way to the page. Nothing from here is ever
a result; it exists so the machinery can be proved on prices whose truth is
known (a market with no edge, or one with a planted edge).

``universe(n)`` gives up to 24 symbols across the lab's four kinds with
plausible contract specifications; ``fill_store`` writes any number of
weekday days of M1 bars for them, with a day-shaped volatility profile, a
spread that widens at the rollover, and tick volume.
"""
from __future__ import annotations

import datetime as dt
import math
import random
from typing import Optional, Sequence

from ..contracts import SymbolSpec
from . import UTC, day_min, add_days
from .store import BarStore, UniverseEntry, lab_kind

# name, digits, point, tick_value (GBP per tick per lot), contract, spread points,
# start price, per-minute sigma in points
_FX = [
    ("EURUSD", 5, 1e-5, 0.79, 100000, 2, 1.0850, 12.0),
    ("GBPUSD", 5, 1e-5, 0.79, 100000, 3, 1.2700, 14.0),
    ("USDJPY", 3, 1e-3, 0.53, 100000, 3, 151.20, 13.0),
    ("AUDUSD", 5, 1e-5, 0.79, 100000, 3, 0.6550, 10.0),
    ("USDCHF", 5, 1e-5, 0.88, 100000, 4, 0.8900, 11.0),
    ("USDCAD", 5, 1e-5, 0.58, 100000, 4, 1.3600, 11.0),
    ("NZDUSD", 5, 1e-5, 0.79, 100000, 5, 0.6050, 10.0),
    ("EURGBP", 5, 1e-5, 1.00, 100000, 4, 0.8540, 8.0),
    ("EURJPY", 3, 1e-3, 0.53, 100000, 5, 164.05, 16.0),
    ("GBPJPY", 3, 1e-3, 0.53, 100000, 7, 192.00, 22.0),
    ("EURCHF", 5, 1e-5, 0.88, 100000, 5, 0.9400, 7.0),
    ("EURAUD", 5, 1e-5, 0.52, 100000, 8, 1.6500, 15.0),
    ("EURCAD", 5, 1e-5, 0.58, 100000, 8, 1.4800, 13.0),
    ("GBPAUD", 5, 1e-5, 0.52, 100000, 9, 1.9300, 18.0),
    ("GBPCAD", 5, 1e-5, 0.58, 100000, 9, 1.7300, 16.0),
    ("AUDJPY", 3, 1e-3, 0.53, 100000, 6, 99.00, 14.0),
    ("CADJPY", 3, 1e-3, 0.53, 100000, 7, 111.00, 13.0),
    ("AUDNZD", 5, 1e-5, 0.48, 100000, 9, 1.0800, 8.0),
    ("AUDCAD", 5, 1e-5, 0.58, 100000, 8, 0.8900, 8.0),
    ("GBPCHF", 5, 1e-5, 0.88, 100000, 8, 1.1300, 12.0),
]
_INDEX = [
    ("US500", 1, 0.1, 0.079, 1, 5, 5200.0, 5.0),
    ("GER40", 1, 0.1, 0.085, 1, 8, 18200.0, 9.0),
    ("UK100", 1, 0.1, 0.100, 1, 9, 8200.0, 7.0),
    ("US30", 1, 0.1, 0.079, 1, 20, 39000.0, 14.0),
]
_GOLD = [("XAUUSD", 2, 0.01, 0.79, 100, 15, 2320.0, 25.0)]

# UTC hour -> relative volatility
_HOUR_VOL = [0.6] * 6 + [0.8, 1.2, 1.5, 1.5, 1.5, 1.3, 1.3, 1.5, 1.6, 1.6, 1.4, 1.2, 0.9, 0.9,
                         0.9, 0.6, 0.5, 0.4, 0.4]
_HOUR_VOL = (_HOUR_VOL + [0.4] * 24)[:24]


def _spec(name, digits, point, tick_value, contract, spread, group) -> SymbolSpec:
    from ..contracts import classify_fx
    base, quote, _ = classify_fx(name)
    return SymbolSpec(
        name=name, digits=digits, point=point, tick_size=point, tick_value=tick_value,
        contract_size=contract, volume_min=0.01, volume_max=100.0, volume_step=0.01,
        stops_level_points=10.0 if group.startswith("FX") else 30.0, freeze_level_points=0.0,
        base_currency=base, profit_currency=quote or "USD", margin_currency=base or "USD",
        asset_group=group, typical_spread_points=float(spread))


def universe(n: int = 24, kinds: Optional[Sequence[str]] = None) -> list:
    """Up to ``n`` synthetic universe entries (majors first, then gold, indices,
    minors), optionally only some kinds."""
    rows = []
    for r in _FX + _INDEX + _GOLD:
        name = r[0]
        kind = lab_kind(name)
        if not kind or (kinds and kind not in kinds):
            continue
        group = {"GOLD": "GOLD", "INDEX": "INDEX"}.get(kind, kind)
        rows.append((kind, r, _spec(name, r[1], r[2], r[3], r[4], r[5], group)))
    order = {"FX_MAJOR": 0, "GOLD": 1, "INDEX": 2, "FX_MINOR": 3}
    rows.sort(key=lambda x: (order[x[0]], x[1][0]))
    return [UniverseEntry(name=r[0], kind=k, spec=sp, spec_taken_utc="2026-01-01T00:00:00+00:00")
            for k, r, sp in rows[:n]]


def weekdays(first: str, count: int) -> list:
    """``count`` consecutive weekdays (ISO dates) starting on or after ``first``."""
    out, d = [], dt.date.fromisoformat(first)
    while len(out) < count:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += dt.timedelta(days=1)
    return out


def _row(name: str):
    for r in _FX + _INDEX + _GOLD:
        if r[0] == name:
            return r
    raise KeyError(name)


def gen_columns(entry: UniverseEntry, days: Sequence[str], *, seed: int = 1,
                zero_spread_share: float = 0.0, or_drift: float = 0.0,
                drift: float = 0.0, start_price_points: Optional[float] = None):
    """M1 columns (t, o, h, l, c, s, v) for the given UTC days, all 1440 minutes
    each, as plain lists of ints. ``or_drift`` plants a real edge: from 30
    minutes after the London open, the price keeps going in the direction it
    broke the opening range for 90 minutes, at ``or_drift`` x sigma a minute."""
    r = _row(entry.name)
    point, spread0, start, sigma = r[2], r[5], r[6], r[7]
    rng = random.Random(f"{seed}:{entry.name}")
    gauss = rng.gauss
    px = start_price_points if start_price_points is not None else start / point
    t, o, h, l, c, s, v = [], [], [], [], [], [], []
    from .store import session_bounds
    for day in days:
        base = day_min(day)
        dayvol = math.exp(rng.gauss(0.0, 0.25))
        london_lo = session_bounds("LONDON", day)[0]
        or_end = london_lo + 30
        hi = lo = None
        direction = 0
        for k in range(1440):
            m = base + k
            hr = k // 60
            sg = sigma * _HOUR_VOL[hr] * dayvol
            step = gauss(drift * sg, sg)
            if or_drift:
                if london_lo <= m < or_end:
                    pass
                elif or_end <= m < or_end + 90 and direction:
                    step += direction * or_drift * sg
            op = px
            cl = op + step
            hh = max(op, cl) + abs(gauss(0.0, sg * 0.5))
            ll = min(op, cl) - abs(gauss(0.0, sg * 0.5))
            if or_drift and london_lo <= m < or_end:
                hi = hh if hi is None else max(hi, hh)
                lo = ll if lo is None else min(lo, ll)
            if or_drift and m == or_end and hi is not None:
                direction = 1 if cl >= (hi + lo) / 2 else -1
            px = cl
            sp_mult = 3.0 if hr in (21, 22) else (1.5 if hr in (23, 0, 1) else 1.0)
            sp = max(1, int(round(spread0 * sp_mult * math.exp(rng.gauss(0.0, 0.15)))))
            if zero_spread_share and rng.random() < zero_spread_share:
                sp = 0
            t.append(m)
            o.append(int(round(op))); h.append(int(round(hh)))
            l.append(int(round(ll))); c.append(int(round(cl)))
            s.append(sp)
            v.append(max(1, int(rng.gauss(300 * _HOUR_VOL[hr], 60))))
    # make high/low enclose the rounded open and close
    for i in range(len(t)):
        if h[i] < max(o[i], c[i]):
            h[i] = max(o[i], c[i])
        if l[i] > min(o[i], c[i]):
            l[i] = min(o[i], c[i])
    return t, o, h, l, c, s, v


def fill_store(store: BarStore, entries: Sequence[UniverseEntry], days: Sequence[str], *,
               seed: int = 1, now: Optional[dt.datetime] = None, source: str = "SYNTHETIC",
               zero_spread_share: float = 0.0, or_drift: float = 0.0) -> None:
    """Write synthetic M1 bars for ``days`` (weekday ISO dates) for every entry.
    ``now`` should fall after the last day so that the days count as complete."""
    if now is None:
        now = dt.datetime.fromisoformat(add_days(days[-1], 2)).replace(tzinfo=UTC)
    for e in entries:
        t, o, h, l, c, s, v = gen_columns(e, days, seed=seed, zero_spread_share=zero_spread_share,
                                          or_drift=or_drift)
        sp = e.spec
        store.merge_columns(e.name, t, o, h, l, c, s, v, point=sp.point, digits=sp.digits,
                            source=source, now=now)


# ---------------------------------------------------- hand-made test series ---
def flat_columns(day: str, price: int = 110000, spread: int = 10, volume: int = 100):
    """1,440 identical bars for one UTC day (open = high = low = close = ``price``
    points): the blank canvas for crafting an exact scenario in a test."""
    base = day_min(day)
    t = list(range(base, base + 1440))
    n = len(t)
    return t, [price] * n, [price] * n, [price] * n, [price] * n, [spread] * n, [volume] * n


def entry_for(symbol: str = "EURUSD") -> UniverseEntry:
    """The synthetic universe entry for one symbol."""
    for e in universe(30):
        if e.name == symbol:
            return e
    raise KeyError(symbol)


def series_from_columns(entry: UniverseEntry, cols, *, first_i: int = 0, hours=None):
    from .store import SymbolSeries
    return SymbolSeries.from_columns(entry, *cols, first_i=first_i, hours=hours)


def window_of(series_list, days, *, kind: str = "past", cut_day: str = "2099-01-01",
              stretch_id: str = "test"):
    """A Window around hand-made series (stamped synthetic)."""
    from .store import Window
    w = Window(kind=kind, days=list(days), cut_day=cut_day, stretch_id=stretch_id)
    for s in series_list:
        w.series[s.symbol] = s
        w.kinds[s.symbol] = s.kind
    w.synthetic = True
    return w
