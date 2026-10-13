"""Features: the numbers the ideas look at, worked out once and kept.

Every feature is a small state machine that is fed one M1 bar at a time and
says what it knows at that bar's CLOSE. The same machines are used three ways:

* BATCH, over a whole month of bars, cached under ``data/lab/cache/features/``
  per (symbol, month, feature set), keyed by a stamp of this file's code, the
  month's bars and the warm-up bars before it. Change the code or a price and
  the cache refuses itself.
* STREAMING (``Streaming`` and ``LiveSeries``), for the practice process, one
  live bar at a time.
* IN MEMORY, over any series (the tests, synthetic data).

Because it is the same code, ``Streaming`` equals batch (a test pins it).
Nothing looks ahead: a value at bar i uses bars up to and including i, and a
higher-timeframe bar (M5, M15, H1) only counts once its last minute has
passed. That is also why a PAST window cut from the middle of a month is safe.

Feature sets (the names are the arrays on the M1 index; 0 or -1 means "not
known yet"):

    atr    atr5, atr15                          ATR(14) of the last finished M5 / M15 bar, points
    ema    ema{20,50,200}_{M15,H1}              EMAs of the last finished M15 / H1 close, points
    hl     hh{60,120,240}, ll{...}              high / low of the PREVIOUS N minutes, points
    sess   sess, ukday, fxday, or_*, asia_*     session bit mask (1 Asia, 2 London, 4 New York,
                                                8 US cash, 16 overlap); UK day number; FX day
                                                (rolls 22:00 UTC); finished opening ranges
    vwap   vwap_/sig_{ASIA,LONDON,US_CASH}      session VWAP and its sigma, points
    pdl    pdh, pdl                             previous UK day's high and low, points
    bb     bb_pct                               Bollinger(20,2) width rank among 100 M15 bars
    news   news_min                             minutes to the next high-importance release on
                                                either currency; -1 where the calendar has no
                                                coverage

Sessions come from ``mintel/clock.py`` and UK days from the page's own calendar
(``mintel/ops/standing.py``). The New York opening range is anchored at the US
cash open, 09:30 New York (14:30 UK for all but about three weeks a year).
"""
from __future__ import annotations

import bisect
import datetime as dt
import hashlib
import json
import math
import os
import sys
from array import array
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

from ..contracts import classify_fx
from ..ops.standing import uk_date
from . import UTC, LabPaths, add_days, atomic_write, day_min, minute_to_dt, uk_day_min
from .store import COLS, MINUTES_PER_DAY, BarStore, Cols, SymbolSeries, session_bounds

FEATURE_VERSION = 1
_BIG = sys.byteorder == "big"

OUTPUTS: dict = {
    "atr": (("atr5", "f"), ("atr15", "f")),
    "ema": tuple((f"ema{p}_{tf}", "f") for tf in ("M15", "H1") for p in (20, 50, 200)),
    "hl": tuple((f"{k}{n}", "i") for n in (60, 120, 240) for k in ("hh", "ll")),
    "sess": (("sess", "B"), ("ukday", "i"), ("fxday", "i"),
             ("or_L15_hi", "i"), ("or_L15_lo", "i"), ("or_L30_hi", "i"), ("or_L30_lo", "i"),
             ("or_N15_hi", "i"), ("or_N15_lo", "i"), ("or_N30_hi", "i"), ("or_N30_lo", "i"),
             ("asia_hi", "i"), ("asia_lo", "i")),
    "vwap": tuple((f"{k}_{s}", "f") for s in ("ASIA", "LONDON", "US_CASH") for k in ("vwap", "sig")),
    "pdl": (("pdh", "i"), ("pdl", "i")),
    "bb": (("bb_pct", "f"),),
    "news": (("news_min", "i"),),
}
SETS = tuple(OUTPUTS)
NAME_TO_SET = {name: s for s, outs in OUTPUTS.items() for name, _ in outs}
# bars of the previous month fed in first so a month's early values are warm
TAIL_BARS = {"atr": 4000, "ema": 20000, "hl": 4000, "sess": 4000, "vwap": 4000,
             "pdl": 4000, "bb": 4000, "news": 0}
SESS_ASIA, SESS_LONDON, SESS_NEWYORK, SESS_US_CASH, SESS_OVERLAP = 1, 2, 4, 8, 16
NEWS_FAR = 32000                      # minutes: "no release in sight"
OR_MIN_SHARE = 0.6                    # an opening range needs this share of its minutes

_f32cell = array("f", [0.0])


def f32(x: float) -> float:
    """x as the float32 the arrays store (so streaming equals batch exactly)."""
    _f32cell[0] = x
    return _f32cell[0]


def set_of(name: str) -> str:
    try:
        return NAME_TO_SET[name]
    except KeyError:
        raise KeyError(f"unknown feature {name!r}") from None


def _code_stamp() -> str:
    try:
        blob = Path(__file__).read_bytes()
    except OSError:
        blob = b""
    return hashlib.sha1(blob + str(FEATURE_VERSION).encode()).hexdigest()[:12]


CODE_STAMP = _code_stamp()


# ------------------------------------------------------------ the pieces ---
class TfBuilder:
    """M1 bars -> ``minutes``-minute bars, emitted only once final.

    A bar is final when its last minute has passed (the M1 bar that closes the
    bucket) or when a bar from a later bucket arrives. ``push`` returns a tuple
    of 0, 1 or 2 finished bars ``(open_minute, o, h, l, c)``."""
    __slots__ = ("m", "key", "o", "h", "l", "c")

    def __init__(self, minutes: int):
        self.m = minutes
        self.key = None
        self.o = self.h = self.l = self.c = 0

    def push(self, t, o, h, l, c):
        m = self.m
        key = t // m
        out = ()
        if self.key is not None and key != self.key:
            out = ((self.key * m, self.o, self.h, self.l, self.c),)
            self.key = None
        if self.key is None:
            self.key = key
            self.o, self.h, self.l, self.c = o, h, l, c
        else:
            if h > self.h:
                self.h = h
            if l < self.l:
                self.l = l
            self.c = c
        if (t + 1) % m == 0:
            out = out + ((self.key * m, self.o, self.h, self.l, self.c),)
            self.key = None
        return out


class _Atr:
    __slots__ = ("n", "prev", "cnt", "sum", "val")

    def __init__(self, n: int = 14):
        self.n, self.prev, self.cnt, self.sum, self.val = n, None, 0, 0.0, 0.0

    def update(self, h, l, c):
        pc = self.prev
        tr = (h - l) if pc is None else max(h - l, abs(h - pc), abs(l - pc))
        self.prev = c
        if self.cnt < self.n:
            self.sum += tr
            self.cnt += 1
            if self.cnt == self.n:
                self.val = self.sum / self.n
        else:
            self.val += (tr - self.val) / self.n


class _Ema:
    __slots__ = ("p", "a", "cnt", "sum", "val")

    def __init__(self, p: int):
        self.p, self.a, self.cnt, self.sum, self.val = p, 2.0 / (p + 1), 0, 0.0, 0.0

    def update(self, x):
        if self.cnt < self.p:
            self.sum += x
            self.cnt += 1
            if self.cnt == self.p:
                self.val = self.sum / self.p
        else:
            self.val += self.a * (x - self.val)


class _DayClock:
    """Tracks the UK calendar day of the bars it is fed."""
    __slots__ = ("lo", "hi", "iso")

    def __init__(self):
        self.lo = self.hi = 0
        self.iso = ""

    def update(self, t: int) -> bool:
        if self.lo <= t < self.hi:
            return False
        iso = uk_date(minute_to_dt(t)).isoformat()
        self.iso = iso
        self.lo = uk_day_min(iso)
        self.hi = uk_day_min(add_days(iso, 1))
        return True


# ---------------------------------------------------------------- engines ---
class AtrEngine:
    names = tuple(n for n, _ in OUTPUTS["atr"])

    def __init__(self, news=None):
        self.b5, self.b15 = TfBuilder(5), TfBuilder(15)
        self.a5, self.a15 = _Atr(14), _Atr(14)

    def push(self, t, o, h, l, c, s, v):
        for bar in self.b5.push(t, o, h, l, c):
            self.a5.update(bar[2], bar[3], bar[4])
        for bar in self.b15.push(t, o, h, l, c):
            self.a15.update(bar[2], bar[3], bar[4])
        return (self.a5.val, self.a15.val)


class EmaEngine:
    names = tuple(n for n, _ in OUTPUTS["ema"])

    def __init__(self, news=None):
        self.b = (TfBuilder(15), TfBuilder(60))
        self.e = tuple(tuple(_Ema(p) for p in (20, 50, 200)) for _ in range(2))

    def push(self, t, o, h, l, c, s, v):
        for k in (0, 1):
            for bar in self.b[k].push(t, o, h, l, c):
                for e in self.e[k]:
                    e.update(bar[4])
        e0, e1 = self.e
        return (e0[0].val, e0[1].val, e0[2].val, e1[0].val, e1[1].val, e1[2].val)


class HlEngine:
    names = tuple(n for n, _ in OUTPUTS["hl"])
    WINDOWS = (60, 120, 240)

    def __init__(self, news=None):
        self.mx = [deque() for _ in self.WINDOWS]       # (t, high), decreasing highs
        self.mn = [deque() for _ in self.WINDOWS]       # (t, low), increasing lows

    def push(self, t, o, h, l, c, s, v):
        out = []
        for k, n in enumerate(self.WINDOWS):
            mx, mn = self.mx[k], self.mn[k]
            lim = t - n
            while mx and mx[0][0] < lim:
                mx.popleft()
            while mn and mn[0][0] < lim:
                mn.popleft()
            out.append(mx[0][1] if mx else 0)
            out.append(mn[0][1] if mn else 0)
            while mx and mx[-1][1] <= h:
                mx.pop()
            mx.append((t, h))
            while mn and mn[-1][1] >= l:
                mn.pop()
            mn.append((t, l))
        return tuple(out)


class SessEngine:
    names = tuple(n for n, _ in OUTPUTS["sess"])

    def __init__(self, news=None):
        self.clock = _DayClock()
        self.b = None

    def _new_day(self):
        iso = self.clock.iso
        asia = session_bounds("ASIA", iso)
        lon = session_bounds("LONDON", iso)
        ny = session_bounds("NEWYORK", iso)
        us = session_bounds("US_CASH", iso)
        self.b = (asia, lon, ny, us)
        # [start, end, hi, lo, count, min_count]
        self.w = [[lon[0], lon[0] + 15, 0, 0, 0, math.ceil(OR_MIN_SHARE * 15)],
                  [lon[0], lon[0] + 30, 0, 0, 0, math.ceil(OR_MIN_SHARE * 30)],
                  [us[0], us[0] + 15, 0, 0, 0, math.ceil(OR_MIN_SHARE * 15)],
                  [us[0], us[0] + 30, 0, 0, 0, math.ceil(OR_MIN_SHARE * 30)],
                  [asia[0], asia[1], 0, 0, 0, math.ceil(OR_MIN_SHARE * (asia[1] - asia[0]))]]
        self.ord = dt.date.fromisoformat(iso).toordinal()

    def push(self, t, o, h, l, c, s, v):
        if self.clock.update(t):
            self._new_day()
        asia, lon, ny, us = self.b
        mask = 0
        if asia[0] <= t < asia[1]:
            mask |= SESS_ASIA
        in_lon = lon[0] <= t < lon[1]
        in_ny = ny[0] <= t < ny[1]
        if in_lon:
            mask |= SESS_LONDON
        if in_ny:
            mask |= SESS_NEWYORK
        if us[0] <= t < us[1]:
            mask |= SESS_US_CASH
        if in_lon and in_ny:
            mask |= SESS_OVERLAP
        vals = []
        for w in self.w:
            if t < w[0]:
                vals.append(0)
                vals.append(0)
            elif t < w[1]:
                if w[4] == 0:
                    w[2], w[3] = h, l
                else:
                    if h > w[2]:
                        w[2] = h
                    if l < w[3]:
                        w[3] = l
                w[4] += 1
                vals.append(0)
                vals.append(0)
            elif w[4] >= w[5]:
                vals.append(w[2])
                vals.append(w[3])
            else:
                vals.append(0)
                vals.append(0)
        return (mask, self.ord, (t + 120) // MINUTES_PER_DAY, *vals)


class VwapEngine:
    names = tuple(n for n, _ in OUTPUTS["vwap"])
    SESSIONS = ("ASIA", "LONDON", "US_CASH")
    MIN_BARS = 10

    def __init__(self, news=None):
        self.clock = _DayClock()
        self.st = None

    def _new_day(self):
        self.st = []
        for name in self.SESSIONS:
            lo, hi = session_bounds(name, self.clock.iso)
            self.st.append([lo, hi, 0.0, 0.0, 0.0, 0])         # sum w, sum w*p, sum w*p*p, n

    def push(self, t, o, h, l, c, s, v):
        if self.clock.update(t):
            self._new_day()
        out = []
        tp = (h + l + c) / 3.0
        w = v if v > 1 else 1
        for st in self.st:
            if st[0] <= t < st[1]:
                st[2] += w
                st[3] += w * tp
                st[4] += w * tp * tp
                st[5] += 1
                if st[5] >= self.MIN_BARS:
                    vw = st[3] / st[2]
                    var = st[4] / st[2] - vw * vw
                    out.append(vw)
                    out.append(math.sqrt(var) if var > 0.0 else 0.0)
                    continue
            out.append(0.0)
            out.append(0.0)
        return tuple(out)


def _uk_weekday(iso: str) -> bool:
    return bool(iso) and dt.date.fromisoformat(iso).weekday() < 5


class PdlEngine:
    """Yesterday's high and low: the previous UK WEEKDAY's. FX opens on a Sunday
    evening, so the UK Sunday holds a sliver of exactly two hours (120 bars); that
    sliver is not a day, whatever its bar count, and Monday's "yesterday" is Friday."""
    names = tuple(n for n, _ in OUTPUTS["pdl"])
    MIN_BARS = 120                    # a holiday-thin weekday is not a day either

    def __init__(self, news=None):
        self.clock = _DayClock()
        self.hi = self.lo = 0
        self.n = 0
        self.ph = self.pl = 0

    def push(self, t, o, h, l, c, s, v):
        ended = self.clock.iso            # the UK day being built, before this bar moves the clock
        if self.clock.update(t):
            if self.n >= self.MIN_BARS and _uk_weekday(ended):
                self.ph, self.pl = self.hi, self.lo
            self.n = 0
        if self.n == 0:
            self.hi, self.lo = h, l
        else:
            if h > self.hi:
                self.hi = h
            if l < self.lo:
                self.lo = l
        self.n += 1
        return (self.ph, self.pl)


class BbEngine:
    names = ("bb_pct",)

    def __init__(self, news=None):
        self.b = TfBuilder(15)
        self.closes = deque(maxlen=20)
        self.widths = deque(maxlen=100)
        self.pct = -1.0

    def push(self, t, o, h, l, c, s, v):
        for bar in self.b.push(t, o, h, l, c):
            self.closes.append(bar[4])
            if len(self.closes) == 20:
                xs = self.closes
                mean = sum(xs) / 20.0
                var = sum((x - mean) ** 2 for x in xs) / 20.0
                width = 4.0 * math.sqrt(var) / mean if mean > 0 else 0.0
                self.widths.append(width)
                if len(self.widths) == 100:
                    less = 0
                    for w in self.widths:
                        if w < width:
                            less += 1
                    self.pct = 100.0 * less / 99.0
        return (self.pct,)


@dataclass(frozen=True)
class NewsInputs:
    """What the news feature needs: high-importance release minutes (sorted,
    epoch minutes) for the symbol's currencies, and the span the calendar covers."""
    events: tuple = ()
    cov_lo: int = 0                   # first covered minute
    cov_hi: int = 0                   # one past the last covered minute

    def stamp(self) -> str:
        h = hashlib.sha1()
        h.update(f"{self.cov_lo}:{self.cov_hi}:{len(self.events)}".encode())
        h.update(array("i", self.events).tobytes())
        return h.hexdigest()[:12]


class NewsEngine:
    names = ("news_min",)

    def __init__(self, news: Optional[NewsInputs] = None):
        self.n = news
        self.p = 0

    def push(self, t, o, h, l, c, s, v):
        n = self.n
        if n is None or t < n.cov_lo or t >= n.cov_hi:
            return (-1,)
        ev = n.events
        p = self.p
        if p and ev[p - 1] >= t:                       # time went back: start over
            p = bisect.bisect_left(ev, t)
        while p < len(ev) and ev[p] < t:
            p += 1
        self.p = p
        return (min(ev[p] - t, NEWS_FAR) if p < len(ev) else NEWS_FAR,)


ENGINES = {"atr": AtrEngine, "ema": EmaEngine, "hl": HlEngine, "sess": SessEngine,
           "vwap": VwapEngine, "pdl": PdlEngine, "bb": BbEngine, "news": NewsEngine}


def make_engine(set_name: str, news: Optional[NewsInputs] = None):
    return ENGINES[set_name](news)


# ------------------------------------------------------------------ batch ---
def compute_set(set_name: str, t, o, h, l, c, s, v, *, news: Optional[NewsInputs] = None,
                skip: int = 0) -> dict:
    """Run a feature set over bars (equal-length sequences). The first ``skip``
    bars warm the engines up and produce no output. Returns {name: array}."""
    n = len(t)
    outs = [array(tc) for _, tc in OUTPUTS[set_name]]
    if set_name == "news" and (news is None or not news.events and news.cov_hi <= news.cov_lo):
        for a in outs:
            a.extend([-1] * (n - skip))
        return {name: a for (name, _), a in zip(OUTPUTS[set_name], outs)}
    push = make_engine(set_name, news).push
    for i in range(skip):
        push(t[i], o[i], h[i], l[i], c[i], s[i], v[i])
    k = len(outs)
    if k == 1:
        a0 = outs[0].append
        for i in range(skip, n):
            a0(push(t[i], o[i], h[i], l[i], c[i], s[i], v[i])[0])
    else:
        apps = [a.append for a in outs]
        for i in range(skip, n):
            vals = push(t[i], o[i], h[i], l[i], c[i], s[i], v[i])
            for j in range(k):
                apps[j](vals[j])
    return {name: a for (name, _), a in zip(OUTPUTS[set_name], outs)}


def _prev_ym(ym: str) -> str:
    y, m = int(ym[:4]), int(ym[5:7])
    return f"{y - (m == 1):04d}-{12 if m == 1 else m - 1:02d}"


def _cache_paths(paths: LabPaths, symbol: str, ym: str, set_name: str):
    d = paths.feature_cache / symbol
    return d / f"{ym}.{set_name}.bin", d / f"{ym}.{set_name}.json"


def month_features(store: BarStore, symbol: str, ym: str, set_name: str,
                   news: Optional[NewsInputs] = None, *, use_cache: bool = True) -> dict:
    """One symbol-month of one feature set, from the cache when its stamp still
    matches, else computed (with the previous month's tail as warm-up) and kept."""
    idx = store.index(symbol)
    meta = idx["months"][ym]
    cols = store.read_month(symbol, ym)
    tail_n = TAIL_BARS[set_name]
    tail = None
    prev = _prev_ym(ym)
    if tail_n and prev in idx["months"]:
        pc = store.read_month(symbol, prev)
        take = min(tail_n, pc.n)
        tail = Cols()
        for c in COLS:
            getattr(tail, c).extend(getattr(pc, c)[pc.n - take:])
    stamp = hashlib.sha1("|".join([
        CODE_STAMP, set_name, symbol, ym, str(meta["crc"]), str(meta["n"]),
        str(tail.crc() if tail else 0), str(tail.n if tail else 0),
        news.stamp() if (news is not None and set_name == "news") else "-"]).encode()
    ).hexdigest()[:20]
    binp, jsonp = _cache_paths(store.paths, symbol, ym, set_name)
    names = OUTPUTS[set_name]
    cacheable = use_cache and (set_name != "news" or news is not None)
    if cacheable:
        try:
            head = json.loads(jsonp.read_text(encoding="utf-8"))
            if head.get("stamp") == stamp and head.get("n") == cols.n:
                raw = binp.read_bytes()
                out, pos = {}, 0
                for (name, tc) in names:
                    a = array(tc)
                    nbytes = cols.n * a.itemsize
                    a.frombytes(raw[pos:pos + nbytes])
                    if _BIG:
                        a.byteswap()
                    out[name] = a
                    pos += nbytes
                if pos == len(raw):
                    return out
        except (OSError, ValueError, KeyError):
            pass
    if tail is not None:
        t = tail.t + cols.t
        o, h, l, c = tail.o + cols.o, tail.h + cols.h, tail.l + cols.l, tail.c + cols.c
        s, v = tail.s + cols.s, tail.v + cols.v
        skip = tail.n
    else:
        t, o, h, l, c, s, v = cols.t, cols.o, cols.h, cols.l, cols.c, cols.s, cols.v
        skip = 0
    out = compute_set(set_name, t, o, h, l, c, s, v, news=news, skip=skip)
    if cacheable:
        try:
            chunks = []
            for (name, _tc) in names:
                a = out[name]
                if _BIG:
                    a = array(a.typecode, a)
                    a.byteswap()
                chunks.append(a.tobytes())
            binp.parent.mkdir(parents=True, exist_ok=True)
            tmp = binp.with_name(f".{binp.name}.{os.getpid()}.tmp")
            tmp.write_bytes(b"".join(chunks))
            os.replace(tmp, binp)
            atomic_write(jsonp, json.dumps({"stamp": stamp, "n": cols.n, "set": set_name,
                                            "names": [n for n, _ in names],
                                            "code": CODE_STAMP}))
        except OSError:
            pass
    return out


def ensure_months(store: BarStore, symbols: Sequence[str], sets: Sequence[str] = SETS,
                  news_by_symbol: Optional[dict] = None, log=None) -> int:
    """Nightly step 4: build any missing symbol-month feature caches. Returns the
    number built (the cached ones are only checked)."""
    built = 0
    for sym in symbols:
        for ym in store.months(sym):
            for st in sets:
                news = (news_by_symbol or {}).get(sym) if st == "news" else None
                binp, jsonp = _cache_paths(store.paths, sym, ym, st)
                before = jsonp.stat().st_mtime_ns if jsonp.exists() else None
                month_features(store, sym, ym, st, news)
                after = jsonp.stat().st_mtime_ns if jsonp.exists() else None
                if after != before:
                    built += 1
        if log:
            log(f"features {sym} done")
    return built


def cache_bytes(paths: LabPaths) -> int:
    """Disk used by the feature cache (about 0.6 GB for 24 symbols x 130 days)."""
    total = 0
    if paths.feature_cache.exists():
        for dirpath, _dirs, files in os.walk(paths.feature_cache):
            for f in files:
                try:
                    total += os.path.getsize(os.path.join(dirpath, f))
                except OSError:
                    pass
    return total


def prune_cache(store: BarStore) -> int:
    """Delete cache files that can never be used again: written by other code
    (a changed file stamp), for a month the store no longer holds, or left
    half-written. Returns the bytes freed. Safe to run any time."""
    freed = 0
    root = store.paths.feature_cache
    if not root.exists():
        return 0
    for sym_dir in [d for d in root.iterdir() if d.is_dir()]:
        have = set(store.months(sym_dir.name)) if sym_dir.name in store.symbols() else set()
        for f in list(sym_dir.iterdir()):
            keep = False
            if f.suffix in (".bin", ".json") and not f.name.startswith("."):
                ym = f.name.split(".")[0]
                if ym in have:
                    keep = True
                    if f.suffix == ".json":
                        try:
                            keep = json.loads(f.read_text(encoding="utf-8")).get("code") == CODE_STAMP
                        except (OSError, ValueError):
                            keep = False
            if not keep:
                try:
                    freed += f.stat().st_size
                    f.unlink()
                except OSError:
                    pass
        # a .bin whose .json went is useless
        for f in list(sym_dir.glob("*.bin")):
            if not f.with_suffix(".json").exists():
                try:
                    freed += f.stat().st_size
                    f.unlink()
                except OSError:
                    pass
    return freed


def attach(series: SymbolSeries, set_name: str) -> None:
    """Make a series' feature arrays for one set available as ``series.f(name)``."""
    if series.has_feature(OUTPUTS[set_name][0][0]):
        return
    loader = series.feature_loader
    if loader is not None:
        got = loader(series, set_name)
    elif series.store is not None and series.parts:
        news = series.news_inputs if set_name == "news" else None
        got = {name: array(tc) for name, tc in OUTPUTS[set_name]}
        cache: dict = {}
        for ym, a, b in series.parts:
            if ym not in cache:
                cache[ym] = month_features(series.store, series.symbol, ym, set_name, news)
            for name, _tc in OUTPUTS[set_name]:
                got[name].extend(cache[ym][name][a:b])
    else:
        got = compute_set(set_name, series.t, series.o, series.h, series.l, series.c,
                          series.s, series.v,
                          news=series.news_inputs if set_name == "news" else None)
    for name, a in got.items():
        series._feat[name] = a


# -------------------------------------------------------------- the calendar --
INDEX_CURRENCY = (("GER", "EUR"), ("DAX", "EUR"), ("DE40", "EUR"), ("DE30", "EUR"),
                  ("EU50", "EUR"), ("STOXX", "EUR"), ("FRA", "EUR"), ("ES35", "EUR"),
                  ("UK100", "GBP"), ("FTSE", "GBP"), ("JP225", "JPY"), ("NIK", "JPY"),
                  ("AUS200", "AUD"), ("SWI", "CHF"))


def currencies_of(symbol: str, spec=None) -> tuple:
    """The currencies whose releases move a symbol."""
    up = symbol.upper()
    base, quote, _ = classify_fx(symbol)
    if base and quote:
        return (base, quote)
    for key, ccy in INDEX_CURRENCY:
        if key in up:
            return (ccy,)
    if spec is not None and getattr(spec, "profit_currency", "") and up.startswith("XAU"):
        return ("USD",)
    return ("USD",)


def known_days(calendar) -> set:
    """The UTC dates on which the calendar recorded at least one release: the days
    on which news is KNOWN. The calendar's first-to-last span is not enough (a
    calendar holding two days in October and two in December spans two months and
    knows about four of its days); a day counts only if something was recorded on it.
    A trading day with nothing recorded at all (a thin holiday, a collector that was
    switched off) is unknown, and AVOID_15M candidates skip it rather than run it as
    if the news were quiet."""
    cov = calendar.coverage()
    if cov.start is None or cov.end is None:
        return set()
    lo = dt.datetime(cov.start.year, cov.start.month, cov.start.day, tzinfo=UTC)
    hi = dt.datetime(cov.end.year, cov.end.month, cov.end.day, tzinfo=UTC) + dt.timedelta(days=2)
    return {ev.time_utc.astimezone(UTC).date().isoformat() for ev in calendar.query(lo, hi)}


def covered_days(calendar, days: Sequence[str]) -> list:
    """The given trading days on which news is known (``known_days``): the days an
    AVOID_15M candidate may be judged on and the days that count toward waking
    NEWS_MOVE."""
    known = known_days(calendar)
    return [d for d in days if d in known]


def news_inputs(calendar, symbol: str, spec=None) -> Optional[NewsInputs]:
    """What the news feature needs for one symbol, read (only read) from the
    calendar: high-importance release minutes on the symbol's currencies and
    the span covered. None when the calendar has no coverage at all."""
    cov = calendar.coverage()
    if cov.start is None or cov.end is None:
        return None
    s, e = cov.start.isoformat(), cov.end.isoformat()
    lo = dt.datetime(cov.start.year, cov.start.month, cov.start.day, tzinfo=UTC)
    hi = dt.datetime(cov.end.year, cov.end.month, cov.end.day, tzinfo=UTC) + dt.timedelta(days=2)
    evs = calendar.query(lo, hi, currencies_of(symbol, spec), min_importance=3)
    mins = sorted({int(ev.time_utc.timestamp()) // 60 for ev in evs})
    return NewsInputs(tuple(mins), day_min(s), day_min(e) + MINUTES_PER_DAY)


def wire_calendar(window, calendar) -> None:
    """Give a window the calendar's coverage and each series its release minutes.

    ``calendar`` is a ``CalendarStore`` (read only: ``coverage()`` and
    ``query()``). A day is news-known when the calendar recorded a release on it."""
    window.news_days = set(covered_days(calendar, window.days))
    for ser in window.series.values():
        ser.news_inputs = news_inputs(calendar, ser.symbol, ser.spec)
        ser._feat.pop("news_min", None)


# ----------------------------------------------------------------- streaming --
class Streaming:
    """The same engines, one live M1 bar at a time (the practice process).

    ``push`` takes a finished bar's values (time in epoch minutes, prices in
    points) and returns {feature name: value} for that bar, exactly what the
    batch arrays hold at that index."""

    def __init__(self, sets: Sequence[str] = SETS, news: Optional[NewsInputs] = None):
        self.sets = tuple(sets)
        self.engines = [(st, make_engine(st, news if st == "news" else None)) for st in self.sets]
        self._types = {name: tc for st in self.sets for name, tc in OUTPUTS[st]}
        self.last: dict = {}

    def push(self, t, o, h, l, c, s, v) -> dict:
        out = {}
        for st, eng in self.engines:
            vals = eng.push(t, o, h, l, c, s, v)
            for (name, tc), x in zip(OUTPUTS[st], vals):
                out[name] = f32(x) if tc == "f" else x
        self.last = out
        return out


class LiveSeries(SymbolSeries):
    """A growing series for the practice process: bars are pushed one at a time,
    the engines run, and the families see ordinary arrays (``f(name)``,
    ``dayviews()``, ``tf()``) exactly as in a test. Prime it with recent stored
    bars (a few days) before the first live bar.

    Everything the ideas read is worked out ONCE per market per bar, here, and
    shared by every idea that watches the market (13 Oct: every idea practises,
    about 630 of them on up to 24 markets): the feature engines run on ``push``;
    the higher-timeframe bars (``tf``) and the effective spreads
    (``spread_arrays``) are kept and grown by one bar, never rebuilt from the
    whole series as the batch class does (each would be tens of thousands of
    steps a market a minute). ``pushes`` counts the bars pushed (the tests check
    it is one a market a bar, however many ideas there are)."""

    def __init__(self, entry, sets: Sequence[str] = SETS, news: Optional[NewsInputs] = None,
                 hours: Optional[list] = None):
        super().__init__(entry.name, entry)
        self.point = float(entry.spec.point)
        self.hours = hours or [0] * 24
        self.source = "bridge"
        self.stream = Streaming(sets, news)
        self._tfs: dict = {}                      # minutes -> (TfBars, its TfBuilder), grown per push
        self._spread = None                       # (eff, assumed), grown per push once asked for
        self.pushes = 0
        for st in sets:
            for name, tc in OUTPUTS[st]:
                self._feat[name] = array(tc)

    def push(self, t, o, h, l, c, s, v) -> None:
        self.t.append(t); self.o.append(o); self.h.append(h); self.l.append(l)
        self.c.append(c); self.s.append(s); self.v.append(v)
        vals = self.stream.push(t, o, h, l, c, s, v)
        for name, x in vals.items():
            self._feat[name].append(x)
        i = self.n - 1
        for tb, b in self._tfs.values():          # the same builder the batch tf() runs, one bar on
            for bar in b.push(t, o, h, l, c):
                tb.add(bar, i)
        if self._spread is not None:
            eff, assumed = self._spread
            med = self.hours[(t // 60) % 24]
            s = self.s[i]                          # as stored, exactly what the batch reads
            if s <= 0:
                dflt = self._dflt_spread()
                eff.append(dflt if dflt > med else med)
                assumed.append(1)
            else:
                eff.append(s if s > med else med)
                assumed.append(0)
        self.pushes += 1
        self._memo.clear()

    def _dflt_spread(self) -> float:
        got = self.__dict__.get("_dflt")
        if got is None:
            from .sim import default_spread_points
            got = self._dflt = default_spread_points(self.symbol, self.kind)
        return got

    def tf(self, minutes: int):
        got = self._tfs.get(minutes)
        if got is None:                            # first asked: built over what is held, then grown
            from .store import TfBars
            tb, b = TfBars(), TfBuilder(minutes)
            t, o, h, l, c = self.t, self.o, self.h, self.l, self.c
            for i in range(self.n):
                for bar in b.push(t[i], o[i], h[i], l[i], c[i]):
                    tb.add(bar, i)
            got = self._tfs[minutes] = (tb, b)
        return got[0]

    def spread_arrays(self):
        if self._spread is None:
            self._spread = super().spread_arrays()
        return self._spread

    def prime(self, cols) -> None:
        for i in range(len(cols.t)):
            self.push(cols.t[i], cols.o[i], cols.h[i], cols.l[i], cols.c[i], cols.s[i], cols.v[i])
