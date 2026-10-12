"""The lab's price store: M1 bars in integer points, sliced by a date cut.

One store of one-minute bars per symbol, kept as little binary column files
(nothing but the standard library reads them). Prices are whole POINTS (the
broker's smallest step) so a price is an exact integer and nothing drifts.

    data/lab/bars/<SYMBOL>/<YYYY-MM>.t.bin   int32  bar open time, UTC epoch minutes
                           .o .h .l .c .bin  int32  bid price in points
                           .s.bin            uint16 the bar's recorded spread, points
                           .v.bin            uint16 tick volume, capped at 65535
    data/lab/bars/<SYMBOL>/index.json         point, digits, source, per-day counts and
                                              the complete flag, per-month length and crc

A month is rewritten whole (tmp files, then replaces, then the index last), and
the index carries a checksum of the month, so a crash half-way is NOTICED (a
"torn" month is dropped and fetched again) and never read as data.

The date cut (Aaron: "ensure that consistency stays, luck doesn't"):

* PAST = complete trading days before the cut. A PAST window never holds a bar
  at or after the cut: the loader slices ``t < cut 00:00 UTC`` and checks it.
* FRESH = complete trading days from the cut to yesterday. The ONLY way to read
  them is ``fresh_once``: in one sqlite transaction it checks the candidate
  passed the past test, has never had a fresh look, and the stretch still has
  looks left, then commits a ``running`` row and only then returns the window.

A store filled from the simulated broker is stamped SYNTHETIC, and every window
and result built from it says so, all the way to the page.
"""
from __future__ import annotations

import bisect
import datetime as dt
import json
import os
import statistics
import sys
import zlib
from array import array
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Callable, Optional, Sequence

from ..clock import SESSIONS
from ..contracts import SymbolSpec, infer_group
from . import (COMPLETE_SHARE, FLAT_UK, FLAT_UK_FRIDAY, KIND_FRESH, KIND_PAST, KINDS,
               NEEDED_DAYS, SPREAD_MAX, STORE_FORMAT, TRADING_DAY_SHARE, UNIVERSE_CAP,
               UTC, WARMUP_BARS, LabPaths, LabSettings, WindowInfo, atomic_write_json,
               day_min, epoch_min, min_to_day, uk_day_min, utc_stamp)
from .cut import CutState, load_cut

assert array("i").itemsize == 4 and array("H").itemsize == 2, "unexpected array sizes"
_BIG = sys.byteorder == "big"
COLS = ("t", "o", "h", "l", "c", "s", "v")
_TYPE = {"t": "i", "o": "i", "h": "i", "l": "i", "c": "i", "s": "H", "v": "H"}
_WRITE_ORDER = ("o", "h", "l", "c", "s", "v", "t")      # the time file goes last
MINUTES_PER_DAY = 1440
SPREAD_CAP = 65535
VOLUME_CAP = 65535


class StoreError(RuntimeError):
    """The store refuses something (a bad request or a bad file)."""


class TornMonth(StoreError):
    """A month whose files do not match the index (a crash mid-write, or a
    damaged file). It is never read as data; ``repair`` drops it."""


# ---------------------------------------------------------------- columns ---
class Cols:
    """Seven equal-length columns of one symbol (a month, or a merge result)."""
    __slots__ = COLS

    def __init__(self):
        for c in COLS:
            setattr(self, c, array(_TYPE[c]))

    @property
    def n(self) -> int:
        return len(self.t)

    def append_row(self, t, o, h, l, c, s, v) -> None:
        self.t.append(t); self.o.append(o); self.h.append(h); self.l.append(l)
        self.c.append(c); self.s.append(s); self.v.append(v)

    def crc(self) -> int:
        x = 0
        for c in COLS:
            a = getattr(self, c)
            if _BIG:
                a = array(a.typecode, a)
                a.byteswap()
            x = zlib.crc32(a.tobytes(), x)
        return x


def _merge_cols(old: Cols, new: Cols) -> Cols:
    """``new`` rows win on equal times. Result strictly ascending."""
    if not old.n:
        return new
    if not new.n:
        return old
    out = Cols()
    if new.t[0] > old.t[-1]:                     # a forward top-up: append
        for c in COLS:
            a = getattr(out, c)
            a.extend(getattr(old, c))
            a.extend(getattr(new, c))
        return out
    if new.t[-1] < old.t[0]:                     # a backfill: prepend
        for c in COLS:
            a = getattr(out, c)
            a.extend(getattr(new, c))
            a.extend(getattr(old, c))
        return out
    npos = {t: i for i, t in enumerate(new.t)}
    opos = {t: i for i, t in enumerate(old.t)}
    for t in sorted(set(npos) | set(opos)):
        if t in npos:
            i = npos[t]
            out.append_row(t, new.o[i], new.h[i], new.l[i], new.c[i], new.s[i], new.v[i])
        else:
            i = opos[t]
            out.append_row(t, old.o[i], old.h[i], old.l[i], old.c[i], old.s[i], old.v[i])
    return out


def _ym_of_day_ordinal(dord: int) -> str:
    d = dt.date.fromordinal(dord)
    return f"{d.year:04d}-{d.month:02d}"


def ym_of_min(m: int) -> str:
    return _ym_of_day_ordinal(719163 + m // MINUTES_PER_DAY)


def ym_of_day(day: str) -> str:
    return day[:7]


def _next_ym(ym: str) -> str:
    y, m = int(ym[:4]), int(ym[5:7])
    return f"{y + (m == 12):04d}-{1 if m == 12 else m + 1:02d}"


# --------------------------------------------------------------- the store ---
@dataclass
class MergeReport:
    symbol: str
    added: int = 0                    # rows not seen before
    rewritten: int = 0                # rows that replaced an old row with other values
    dropped: int = 0                  # corrupt rows refused (a price of zero, a high under a low)
    months: list = field(default_factory=list)
    first_t: Optional[int] = None
    last_t: Optional[int] = None


class BarStore:
    """Reads and writes ``data/lab/bars``. One writer at a time (the nightly)."""

    def __init__(self, paths):
        self.paths = paths if isinstance(paths, LabPaths) else LabPaths(paths)
        self.root = self.paths.bars
        self._idx: dict[str, dict] = {}

    # ------------------------------------------------------------- listing --
    def symbols(self) -> list:
        if not self.root.exists():
            return []
        return sorted(p.name for p in self.root.iterdir()
                      if p.is_dir() and (p / "index.json").exists())

    def _dir(self, symbol: str) -> Path:
        return self.root / symbol

    def _index_path(self, symbol: str) -> Path:
        return self._dir(symbol) / "index.json"

    def index(self, symbol: str) -> dict:
        """The symbol's index.json (a fresh empty one when there is none)."""
        if symbol in self._idx:
            return self._idx[symbol]
        try:
            idx = json.loads(self._index_path(symbol).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            idx = {"format": STORE_FORMAT, "symbol": symbol, "point": 0.0, "digits": 0,
                   "source": "bridge", "days": {}, "months": {}, "first_day": None,
                   "last_day": None, "history_exhausted": False, "fetched_utc": None}
        idx.setdefault("months", {})
        idx.setdefault("days", {})
        self._idx[symbol] = idx
        return idx

    def refresh(self) -> None:
        """Forget cached indexes (another process may have written)."""
        self._idx.clear()

    def months(self, symbol: str) -> list:
        return sorted(self.index(symbol)["months"])

    def bars_in(self, symbol: str) -> int:
        return sum(m["n"] for m in self.index(symbol)["months"].values())

    def bytes_used(self) -> int:
        total = 0
        if not self.root.exists():
            return 0
        for dirpath, _dirs, files in os.walk(self.root):
            for f in files:
                try:
                    total += os.path.getsize(os.path.join(dirpath, f))
                except OSError:
                    pass
        return total

    def is_synthetic(self, symbols: Optional[Sequence[str]] = None) -> bool:
        """True when any of the symbols' bars came from the simulated broker."""
        syms = symbols if symbols is not None else self.symbols()
        return any(self.index(s).get("source") == "SYNTHETIC" for s in syms)

    # ---------------------------------------------------------------- read --
    def read_month(self, symbol: str, ym: str, *, verify: bool = True) -> Cols:
        meta = self.index(symbol)["months"].get(ym)
        if meta is None:
            raise StoreError(f"{symbol} {ym}: no such month")
        cols = Cols()
        n = int(meta["n"])
        for c in COLS:
            a = getattr(cols, c)
            p = self._dir(symbol) / f"{ym}.{c}.bin"
            try:
                raw = p.read_bytes()
            except OSError as e:
                raise TornMonth(f"{symbol} {ym}.{c}: {e}")
            if len(raw) != n * a.itemsize:
                raise TornMonth(f"{symbol} {ym}.{c}: {len(raw)} bytes, expected {n * a.itemsize}")
            a.frombytes(raw)
            if _BIG:
                a.byteswap()
        if verify and cols.crc() != int(meta.get("crc", -1)):
            raise TornMonth(f"{symbol} {ym}: checksum does not match the index")
        if n and (cols.t[0] != meta.get("t0") or cols.t[-1] != meta.get("t1")):
            raise TornMonth(f"{symbol} {ym}: time range does not match the index")
        return cols

    def _month_or_empty(self, symbol: str, ym: str) -> Cols:
        if ym in self.index(symbol)["months"]:
            return self.read_month(symbol, ym)
        return Cols()

    def verify(self, symbol: str) -> list:
        """The months of a symbol that do not match their index (torn)."""
        bad = []
        for ym in self.months(symbol):
            try:
                self.read_month(symbol, ym)
            except TornMonth:
                bad.append(ym)
        return bad

    def repair(self, symbol: str) -> list:
        """Drop torn months (files and index entries) so they are fetched again."""
        bad = self.verify(symbol)
        if not bad:
            return []
        idx = self.index(symbol)
        for ym in bad:
            idx["months"].pop(ym, None)
            for c in COLS:
                try:
                    (self._dir(symbol) / f"{ym}.{c}.bin").unlink()
                except OSError:
                    pass
        # A dropped month is a hole the backward backfill never revisits (it only extends the
        # oldest edge), so remember how far back the next forward top-up has to reach.
        start = min(day_min(f"{ym}-01") for ym in bad)
        old = idx.get("heal_from")
        idx["heal_from"] = start if old is None else min(int(old), start)
        self._rebuild_days(symbol)
        self._write_index(symbol)
        return bad

    def overlap(self, symbol: str, t, o, h, l, c, *, shift: int = 0) -> list:
        """Hold new rows up against the stored ones. For each new row stamped T, the stored
        row stamped T - ``shift`` minutes is compared on open, high, low and close. The
        answer has one entry per new row: ``None`` when nothing is stored there, else
        ``True`` (identical) or ``False`` (different). The fetch uses it to notice a page
        whose prices no longer line up with what is held (a broker clock change moves every
        stamp by an hour) before that page can overwrite good rows."""
        idx = self.index(symbol)
        cache: dict = {}
        out: list = []
        for k in range(len(t)):
            tm = int(t[k]) - shift
            ym = ym_of_min(tm)
            if ym not in idx["months"]:
                out.append(None)
                continue
            cols = cache.get(ym)
            if cols is None:
                try:
                    cols = cache[ym] = self.read_month(symbol, ym, verify=False)
                except TornMonth:                      # repair() deals with it; nothing to compare
                    cols = cache[ym] = Cols()
            i = bisect.bisect_left(cols.t, tm)
            if i < cols.n and cols.t[i] == tm:
                out.append((cols.o[i], cols.h[i], cols.l[i], cols.c[i]) == (o[k], h[k], l[k], c[k]))
            else:
                out.append(None)
        return out

    def first_min(self, symbol: str) -> Optional[int]:
        ms = self.index(symbol)["months"]
        return min((m["t0"] for m in ms.values()), default=None) if ms else None

    def last_min(self, symbol: str) -> Optional[int]:
        ms = self.index(symbol)["months"]
        return max((m["t1"] for m in ms.values()), default=None) if ms else None

    # --------------------------------------------------------------- write --
    def _write_month(self, symbol: str, ym: str, cols: Cols) -> None:
        d = self._dir(symbol)
        d.mkdir(parents=True, exist_ok=True)
        tmps = {}
        pid = os.getpid()
        try:
            for c in COLS:
                a = getattr(cols, c)
                if _BIG:
                    a = array(a.typecode, a)
                    a.byteswap()
                tmp = d / f".{ym}.{c}.bin.{pid}.tmp"
                with open(tmp, "wb") as fh:
                    fh.write(a.tobytes())
                    fh.flush()
                    try:
                        os.fsync(fh.fileno())
                    except OSError:
                        pass
                tmps[c] = tmp
            for c in _WRITE_ORDER:
                os.replace(tmps[c], d / f"{ym}.{c}.bin")
        finally:
            for tmp in tmps.values():
                try:
                    tmp.unlink()
                except OSError:
                    pass
        self.index(symbol)["months"][ym] = {
            "n": cols.n, "t0": cols.t[0] if cols.n else None,
            "t1": cols.t[-1] if cols.n else None, "crc": cols.crc()}

    def _write_index(self, symbol: str) -> None:
        atomic_write_json(self._index_path(symbol), self.index(symbol))

    def _rebuild_days(self, symbol: str, now: Optional[dt.datetime] = None,
                      touched: Optional[dict] = None) -> None:
        """Bars per UTC day and the complete flags. With ``touched`` ({month:
        Cols just written}) only those months are recounted; otherwise every
        month file is read."""
        idx = self.index(symbol)
        old = idx.get("days", {})
        counts: dict[str, int] = {}
        if touched is not None:
            skip = set(touched)
            counts = {d: v["bars"] for d, v in old.items() if ym_of_day(d) not in skip}
            sources = list(touched.items())
        else:
            sources = []
            for ym in sorted(idx["months"]):
                try:
                    sources.append((ym, self.read_month(symbol, ym, verify=False)))
                except TornMonth:
                    continue
        for _ym, cols in sources:
            t = cols.t
            prev, run = None, 0
            for m in t:
                dd = m // MINUTES_PER_DAY
                if dd != prev:
                    if prev is not None:
                        counts[dt.date.fromordinal(719163 + prev).isoformat()] = run
                    prev, run = dd, 0
                run += 1
            if prev is not None:
                counts[dt.date.fromordinal(719163 + prev).isoformat()] = run
        today = (now or dt.datetime.now(UTC)).astimezone(UTC).date().isoformat()
        weekday = {d: n for d, n in counts.items()
                   if dt.date.fromisoformat(d).weekday() < 5 and d < today}
        med = statistics.median(weekday.values()) if weekday else 0
        days = {}
        for d in sorted(counts):
            n = counts[d]
            ok = (dt.date.fromisoformat(d).weekday() < 5 and d < today and med > 0
                  and n >= COMPLETE_SHARE * med)
            days[d] = {"bars": n, "complete": bool(ok)}
        idx["days"] = days
        idx["first_day"] = min(days) if days else None
        idx["last_day"] = max(days) if days else None

    def merge_columns(self, symbol: str, t, o, h, l, c, s, v, *, point: float, digits: int,
                      source: str = "bridge", now: Optional[dt.datetime] = None,
                      keep_flags: bool = True) -> MergeReport:
        """Merge rows (equal-length sequences of integers; points for prices) into
        the symbol's months. New rows win on equal times. Times need not be
        sorted; duplicates inside the batch keep the last. Writes each touched
        month whole, then the index."""
        rep = MergeReport(symbol)
        n = len(t)
        if n == 0:
            return rep
        order = sorted(range(n), key=t.__getitem__) if any(
            t[i] > t[i + 1] for i in range(n - 1)) else range(n)
        by_month: dict[str, Cols] = {}
        last_t = None
        cur_ym, cur = None, None
        for i in order:
            tm = int(t[i])
            if tm == last_t:                       # duplicate inside the batch: keep the last
                for col in COLS:
                    getattr(cur, col).pop()
            last_t = tm
            ym = ym_of_min(tm)
            if ym != cur_ym:
                cur_ym = ym
                cur = by_month.setdefault(ym, Cols())
            cur.append_row(tm, int(o[i]), int(h[i]), int(l[i]), int(c[i]),
                           min(max(int(s[i]), 0), SPREAD_CAP),
                           min(max(int(v[i]), 0), VOLUME_CAP))
        idx = self.index(symbol)
        old_src = idx.get("source")
        if idx["months"] and idx.get("point") and abs(idx["point"] - point) > 1e-12 * max(1.0, point):
            raise StoreError(f"{symbol}: point changed from {idx['point']} to {point}; "
                             f"refusing to mix price units")
        idx["point"], idx["digits"] = float(point), int(digits)
        # a SYNTHETIC store stays SYNTHETIC, whatever is added to it later
        idx["source"] = "SYNTHETIC" if "SYNTHETIC" in (old_src if idx["months"] else None, source) else source
        written: dict = {}
        for ym in sorted(by_month):
            new = by_month[ym]
            old = self._month_or_empty(symbol, ym)
            before = old.n
            merged = _merge_cols(old, new)
            rep.added += merged.n - before
            if before:
                rep.rewritten += _count_changed(old, new)
            self._write_month(symbol, ym, merged)
            written[ym] = merged
            rep.months.append(ym)
        rep.first_t = min(by_month[m].t[0] for m in by_month)
        rep.last_t = max(by_month[m].t[-1] for m in by_month)
        self._rebuild_days(symbol, now, touched=written)
        idx["fetched_utc"] = utc_stamp(now)
        self._write_index(symbol)
        return rep

    def merge_bars(self, symbol: str, bars: Sequence, *, point: float, digits: int,
                   source: str = "bridge", now: Optional[dt.datetime] = None) -> MergeReport:
        """Merge ``Bar`` objects (prices as the broker quotes them)."""
        t, o, h, l, c, s, v = [], [], [], [], [], [], []
        dropped = 0
        for b in bars:
            row = bar_row(b, point)
            if row is None:
                dropped += 1                      # a corrupt row is never data
                continue
            t.append(row[0]); o.append(row[1]); h.append(row[2]); l.append(row[3])
            c.append(row[4]); s.append(row[5]); v.append(row[6])
        rep = self.merge_columns(symbol, t, o, h, l, c, s, v, point=point, digits=digits,
                                 source=source, now=now)
        rep.dropped = dropped
        return rep

    def set_flag(self, symbol: str, **flags) -> None:
        """Store small facts about a symbol (history_exhausted, depth_days ...)."""
        idx = self.index(symbol)
        idx.update(flags)
        self._write_index(symbol)

    # ----------------------------------------------------------------- days --
    def day_info(self, symbol: str) -> dict:
        return self.index(symbol)["days"]

    def complete_days(self, symbol: str) -> list:
        return sorted(d for d, v in self.index(symbol)["days"].items() if v["complete"])

    def trading_days(self, symbols: Optional[Sequence[str]] = None,
                     share: float = TRADING_DAY_SHARE) -> list:
        """UTC weekdays that are complete for at least ``share`` of the symbols."""
        syms = list(symbols) if symbols is not None else self.symbols()
        if not syms:
            return []
        tally: dict[str, int] = {}
        for s in syms:
            for d in self.complete_days(s):
                tally[d] = tally.get(d, 0) + 1
        need = share * len(syms)
        return sorted(d for d, n in tally.items() if n >= need - 1e-9)

    def coverage(self, symbols: Optional[Sequence[str]] = None) -> dict:
        """What the page's 'collecting past prices: D of 40 days' line needs."""
        syms = list(symbols) if symbols is not None else self.symbols()
        days = self.trading_days(syms)
        firsts = [self.index(s)["first_day"] for s in syms if self.index(s)["first_day"]]
        lasts = [self.index(s)["last_day"] for s in syms if self.index(s)["last_day"]]
        return {"symbols": len(syms), "complete_days": len(days),
                "needed_days": NEEDED_DAYS, "first_day": min(firsts) if firsts else None,
                "last_day": max(lasts) if lasts else None,
                "first_complete": days[0] if days else None,
                "last_complete": days[-1] if days else None,
                "synthetic": self.is_synthetic(syms),
                "ready": len(days) >= NEEDED_DAYS}

    # --------------------------------------------------------------- tables --
    def hour_table(self, symbol: str, before_min: int) -> list:
        """Median recorded spread (points) for each UTC hour, over the bars
        before ``before_min`` (the past stretch). 0 where an hour has too few
        bars with a recorded spread. Cached per (symbol, cut) on disk, so the
        floor stays the same for every candidate of a stretch."""
        idx = self.index(symbol)
        since_min = 0
        key = f"{symbol}.{before_min}"
        cache = self.paths.cache / "spread" / f"{symbol}.json"
        try:
            blob = json.loads(cache.read_text(encoding="utf-8"))
            if blob.get("key") == key:
                return [int(x) for x in blob["table"]]
        except (OSError, ValueError, KeyError):
            pass
        hist = [dict() for _ in range(24)]
        for ym in sorted(idx["months"]):
            meta = idx["months"][ym]
            if meta["t0"] >= before_min or meta["t1"] < since_min:
                continue
            cols = self.read_month(symbol, ym, verify=False)
            t, s = cols.t, cols.s
            hi = bisect.bisect_left(t, before_min)
            lo = bisect.bisect_left(t, since_min)
            for i in range(lo, hi):
                sp = s[i]
                if sp:
                    d = hist[(t[i] // 60) % 24]
                    d[sp] = d.get(sp, 0) + 1
        table = []
        for d in hist:
            tot = sum(d.values())
            if tot < 30:
                table.append(0)
                continue
            half, acc = tot / 2.0, 0
            for sp in sorted(d):
                acc += d[sp]
                if acc >= half:
                    table.append(int(sp))
                    break
        try:
            atomic_write_json(cache, {"key": key, "table": table})
        except OSError:
            pass
        return table

    # -------------------------------------------------------------- windows --
    def _series_for(self, symbol: str, entry: "UniverseEntry", days: Sequence[str],
                    *, warm: int, cut_min: Optional[int], hours: list) -> Optional["SymbolSeries"]:
        """One symbol's bars for the given UTC days (plus ``warm`` bars before
        the first). None when the symbol has none of those days."""
        idx = self.index(symbol)
        info = idx["days"]
        inc = [d for d in days if info.get(d, {}).get("complete")]
        if not inc:
            return None
        months_needed = sorted({ym_of_day(d) for d in inc})
        first_ym = months_needed[0]
        have = self.months(symbol)
        # warm-up may reach into earlier months
        prior = [m for m in have if m < first_ym]
        cache: dict[str, Cols] = {}

        def month(ym: str) -> Cols:
            if ym not in cache:
                cache[ym] = self.read_month(symbol, ym)
            return cache[ym]

        parts: list = []                                  # (ym, a, b)
        first_day_min = day_min(inc[0])
        c0 = month(first_ym) if first_ym in idx["months"] else Cols()
        a0 = bisect.bisect_left(c0.t, first_day_min) if c0.n else 0
        need = warm
        warm_parts: list = []
        if first_ym in idx["months"]:
            take = min(need, a0)
            if take:
                warm_parts.append((first_ym, a0 - take, a0))
                need -= take
        for ym in reversed(prior):
            if need <= 0:
                break
            cm = month(ym)
            take = min(need, cm.n)
            if take:
                warm_parts.append((ym, cm.n - take, cm.n))
                need -= take
        parts.extend(reversed(warm_parts))
        warm_n = sum(b - a for _, a, b in warm_parts)
        # the included days, one slice each (merged when adjacent)
        for d in inc:
            ym = ym_of_day(d)
            if ym not in idx["months"]:
                continue
            cm = month(ym)
            lo = bisect.bisect_left(cm.t, day_min(d))
            hi = bisect.bisect_left(cm.t, day_min(d) + MINUTES_PER_DAY, lo)
            if hi <= lo:
                continue
            if parts and parts[-1][0] == ym and parts[-1][2] == lo:
                parts[-1] = (ym, parts[-1][1], hi)
            else:
                parts.append((ym, lo, hi))
        ser = SymbolSeries(symbol, entry)
        ser.point = float(idx["point"])
        for ym, a, b in parts:
            cm = month(ym)
            for c in COLS:
                getattr(ser, c).extend(getattr(cm, c)[a:b])
        ser.parts = parts
        ser.first_i = warm_n
        ser.utc_days = list(inc)
        ser.hours = hours
        ser.source = idx.get("source", "bridge")
        ser.store = self
        if cut_min is not None and ser.n and ser.t[-1] >= cut_min:
            raise StoreError(f"{symbol}: a PAST window must not hold a bar at or after the cut")
        return ser

    def _build_window(self, kind: str, days: Sequence[str], universe: Sequence["UniverseEntry"],
                      *, cut_day: str, stretch_id: str, warm: int = WARMUP_BARS,
                      calendar=None) -> "Window":
        cut_min = day_min(cut_day) if cut_day else None
        win = Window(kind=kind, days=list(days), cut_day=cut_day, stretch_id=stretch_id)
        win.store = self
        for entry in universe:
            name = entry.name
            if not self.months(name):
                continue
            hours = self.hour_table(name, cut_min) if cut_min is not None else [0] * 24
            ser = self._series_for(name, entry, days, warm=warm,
                                   cut_min=cut_min if kind == KIND_PAST else None, hours=hours)
            if ser is None:
                continue
            win.series[name] = ser
            win.kinds[name] = entry.kind
        win.synthetic = self.is_synthetic(list(win.series))
        if calendar is not None:
            win.set_calendar(calendar)
        if kind == KIND_PAST and cut_min is not None:
            for ser in win.series.values():
                if ser.n and ser.t[-1] >= cut_min:
                    raise StoreError("a PAST window must not hold a bar at or after the cut")
        return win

    def past_window(self, universe: Sequence["UniverseEntry"], *, cut: Optional[CutState] = None,
                    cut_day: Optional[str] = None, stretch_id: Optional[str] = None,
                    max_days: Optional[int] = None, calendar=None,
                    warm: int = WARMUP_BARS) -> "Window":
        """The PAST window: complete trading days BEFORE the cut. The cut is the
        registered one (cut.json) or, if a cut or a day is passed, no later than it:
        whichever way a later cut arrives (``cut_day=`` or a ``CutState``), the
        registered one in cut.json is the limit, so a past test can never be handed a
        fresh day."""
        registered = load_cut(self.paths)
        reg = cut or registered
        if cut_day is None:
            if reg is None:
                raise StoreError("no cut has been set yet")
            cut_day = reg.cut_day
        if registered is not None and cut_day > registered.cut_day:
            raise StoreError(f"{cut_day} is later than the registered cut {registered.cut_day}: "
                             f"that would show fresh prices to a past test")
        stretch = stretch_id or (reg.stretch_id if reg else "")
        syms = [e.name for e in universe]
        days = [d for d in self.trading_days(syms) if d < cut_day]
        if max_days:
            days = days[-int(max_days):]
        return self._build_window(KIND_PAST, days, universe, cut_day=cut_day,
                                  stretch_id=stretch, warm=warm, calendar=calendar)

    def fresh_days(self, universe: Sequence["UniverseEntry"], cut_day: str) -> list:
        return [d for d in self.trading_days([e.name for e in universe]) if d >= cut_day]


def bar_row(bar, point: float):
    """A ``Bar`` as the store holds it: (epoch minute, o, h, l, c in whole points,
    spread points, tick volume), or None when the bar is corrupt (a price of zero
    or less, a high under a low, an open or close outside the high-low range).
    The practice process uses this to turn live bars into the same integers."""
    inv = 1.0 / point
    po, ph, pl, pc = (round(bar.open * inv), round(bar.high * inv),
                      round(bar.low * inv), round(bar.close * inv))
    if min(po, ph, pl, pc) <= 0 or ph < pl or ph < max(po, pc) or pl > min(po, pc):
        return None
    return (epoch_min(bar.time), po, ph, pl, pc,
            min(max(int(round(bar.spread_points or 0)), 0), SPREAD_CAP),
            min(max(int(bar.tick_volume or 0), 0), VOLUME_CAP))


def _count_changed(old: Cols, new: Cols) -> int:
    """How many of ``new``'s rows replaced an old row that had other values."""
    opos = {t: i for i, t in enumerate(old.t)}
    changed = 0
    for j, t in enumerate(new.t):
        i = opos.get(t)
        if i is not None and (old.o[i], old.h[i], old.l[i], old.c[i], old.s[i]) != (
                new.o[j], new.h[j], new.l[j], new.c[j], new.s[j]):
            changed += 1
    return changed


# --------------------------------------------------------------- universe ---
@dataclass
class UniverseEntry:
    name: str
    kind: str
    spec: SymbolSpec
    spec_taken_utc: str = ""

    def to_dict(self) -> dict:
        return {"name": self.name, "kind": self.kind, "spec": self.spec.to_dict(),
                "spec_taken_utc": self.spec_taken_utc}


def spec_from_dict(d: dict) -> SymbolSpec:
    fields = SymbolSpec.__dataclass_fields__
    kw = {k: (tuple(v) if isinstance(v, list) else v) for k, v in d.items() if k in fields}
    return SymbolSpec(**kw)


def _full_spec(spec: Optional[SymbolSpec]) -> bool:
    if spec is None:
        return False
    return all(float(getattr(spec, f, 0) or 0) > 0 for f in
               ("point", "tick_value", "tick_size", "volume_min", "volume_step", "contract_size"))


def lab_kind(name: str, spec: Optional[SymbolSpec] = None) -> str:
    """The lab's kind of market: FX_MAJOR, FX_MINOR, INDEX or GOLD ('' if none)."""
    group = infer_group(name)
    if group == "GOLD":
        return "GOLD" if name.upper().startswith("XAUUSD") else ""
    return group if group in KINDS else ""


def build_universe(feed, *, exclude_patterns: Optional[Sequence[str]] = None,
                   cap: int = UNIVERSE_CAP, now: Optional[dt.datetime] = None,
                   only: Optional[Sequence[str]] = None) -> list:
    """The lab's markets, from the feed's own list (spec section 2):

    kinds FX_MAJOR, FX_MINOR, INDEX, GOLD (XAUUSD only); names matching the
    trader's ``exclude_patterns`` and any symbol without a full spec are
    dropped; all the majors, the cheapest minors (spread <= 12 points) and
    indices (spread <= 4 index points), gold; capped at 24. The trader's
    ``excluded_symbols`` are KEPT: another rule may win where Trend & Breakout
    lost. Room is reserved so minors cannot crowd out indices and gold.
    ``only`` restricts to named symbols (the settings override)."""
    if exclude_patterns is None:
        from ..config import UniverseConfig
        exclude_patterns = UniverseConfig().exclude_patterns
    pats = [p.upper() for p in exclude_patterns]
    taken = utc_stamp(now)
    want = {s.upper() for s in only} if only else None
    pools: dict[str, list] = {k: [] for k in KINDS}
    for name in feed.symbols():
        up = name.upper()
        if want is not None and up not in want:
            continue
        if any(p in up for p in pats):
            continue
        kind = lab_kind(name)
        if not kind:
            continue
        spec = feed.spec(name)
        if not _full_spec(spec):
            continue
        spread = float(getattr(spec, "typical_spread_points", 0.0) or 0.0)
        if want is None:
            if kind == "FX_MINOR" and spread > SPREAD_MAX["FX_MINOR"] > 0:
                continue
            if kind == "INDEX" and spread * spec.point > SPREAD_MAX["INDEX"]:
                continue
        pools[kind].append((spread if spread > 0 else 1e9, name, spec))
    for k in pools:
        pools[k].sort(key=lambda r: (r[0], r[1]))
    chosen: list = []
    chosen += [("FX_MAJOR", r) for r in pools["FX_MAJOR"]]
    chosen += [("GOLD", r) for r in pools["GOLD"][:1]]
    chosen += [("INDEX", r) for r in pools["INDEX"][:6]]
    chosen = chosen[:cap]
    room = cap - len(chosen)
    chosen += [("FX_MINOR", r) for r in pools["FX_MINOR"][:max(room, 0)]]
    room = cap - len(chosen)
    if room > 0:                       # still room: more indices, cheapest first
        chosen += [("INDEX", r) for r in pools["INDEX"][6:6 + room]]
    order = {k: i for i, k in enumerate(KINDS)}
    chosen.sort(key=lambda kr: (order[kr[0]], kr[1][0], kr[1][1]))
    return [UniverseEntry(name=r[1], kind=k, spec=r[2], spec_taken_utc=taken)
            for k, r in chosen[:cap]]


def write_universe(paths: LabPaths, entries: Sequence[UniverseEntry], *, note: str = "",
                   now: Optional[dt.datetime] = None, overwrite: bool = False) -> bool:
    """Fixed once written: False (and nothing changes) when the file exists."""
    if paths.universe.exists() and not overwrite:
        return False
    atomic_write_json(paths.universe, {
        "written_utc": utc_stamp(now),
        "symbols": [e.to_dict() for e in entries],
        "note": note or "chosen once from the broker's own list; settings.symbols overrides"})
    return True


def load_universe(paths: LabPaths, settings: Optional[LabSettings] = None) -> list:
    """The universe file as entries (``settings.symbols`` narrows it). []
    when there is no file."""
    try:
        raw = json.loads(paths.universe.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    out = []
    for r in raw.get("symbols", []):
        try:
            out.append(UniverseEntry(r["name"], r.get("kind") or lab_kind(r["name"]),
                                     spec_from_dict(r["spec"]), r.get("spec_taken_utc", "")))
        except (KeyError, TypeError):
            continue
    if settings and settings.symbols:
        keep = {s.upper() for s in settings.symbols}
        out = [e for e in out if e.name.upper() in keep]
    return out


def ensure_universe(paths: LabPaths, feed, settings: Optional[LabSettings] = None, *,
                    now: Optional[dt.datetime] = None) -> tuple:
    """(entries, created). Builds and writes the universe on the first run only;
    after that the file is fixed. ``settings.symbols`` overrides it for the run:
    only those names are returned, and any the file does not hold are built from
    the feed's own specification (the file is left as it was)."""
    entries = load_universe(paths)
    created = False
    if not entries:
        entries = build_universe(feed, now=now, only=settings.symbols if settings else None)
        if not entries:
            return [], False
        write_universe(paths, entries, now=now)
        created = True
    if settings and settings.symbols:
        want = {s.upper() for s in settings.symbols}
        missing = want - {e.name.upper() for e in entries}
        if missing:
            entries = entries + build_universe(feed, now=now, only=sorted(missing))
        entries = [e for e in entries if e.name.upper() in want]
    return entries, created


# ------------------------------------------------------------ UK sessions ---
@lru_cache(maxsize=8192)
def session_bounds(name: str, day: str) -> tuple:
    """[open, close) epoch minutes of a session for a UK calendar date.

    The date is the session's own local date (London 08:00-16:30 on that date,
    New York 08:00-17:00 and the 09:30-16:00 cash session, Tokyo 09:00-15:00),
    from ``mintel/clock.py`` SESSIONS, so BST, GMT and US daylight saving come
    from the time-zone database. BOTH is London open to New York close; ALL is
    the whole UK day up to the flat time."""
    d = dt.date.fromisoformat(day)
    if name == "BOTH":
        lo, _ = session_bounds("LONDON", day)
        _, hi = session_bounds("NEWYORK", day)
        return lo, hi
    if name == "ALL":
        return uk_day_min(day), flat_min(day)
    w = SESSIONS[name]

    def at(hm):
        local = dt.datetime.combine(d, dt.time(*hm), tzinfo=w.tz)
        return int(local.astimezone(UTC).timestamp()) // 60
    return at(w.open_hm), at(w.close_hm)


@lru_cache(maxsize=8192)
def flat_min(day: str) -> int:
    """Epoch minute by which everything is flat on a UK date: 21:55 UK, or
    21:00 UK on a Friday (in BST and GMT alike)."""
    hh, mm = FLAT_UK_FRIDAY if dt.date.fromisoformat(day).weekday() == 4 else FLAT_UK
    return uk_day_min(day) + hh * 60 + mm


@lru_cache(maxsize=8192)
def or_anchor(session: str, day: str) -> int:
    """The opening bell the opening-range families measure from: London 08:00
    London time; New York 09:30 New York time (14:30 UK for most of the year)."""
    return session_bounds("LONDON" if session == "LONDON" else "US_CASH", day)[0]


# ------------------------------------------------------------- the series ---
class DayView:
    """One UK day of one symbol, as the families and the simulator see it.

    ``i0..i1`` are the bar indices of the day inside the tradable region;
    ``flat_i`` is the last bar of the day that a position may be held through
    (the one that closes at the flat time)."""
    __slots__ = ("series", "uk", "i0", "i1", "flat_i", "ord", "open_ended")

    def __init__(self, series, uk, i0, i1, flat_i, ordinal):
        self.series, self.uk, self.i0, self.i1, self.flat_i, self.ord = (
            series, uk, i0, i1, flat_i, ordinal)
        # the data stops before the flat time (a day still being traded live): the
        # last bar is a perfectly good place for a signal, its entry bar is yet to come
        self.open_ended = series.t[i1 - 1] < flat_min(uk) - 1

    def span(self, session: str) -> tuple:
        """[lo, hi) bar indices of a session within this day (signals only here).
        For a finished day ``hi`` stops at the last bar before the flat time,
        because the entry bar i+1 must still be inside the day."""
        a, b = session_bounds(session, self.uk)
        t = self.series.t
        lo = bisect.bisect_left(t, a, self.i0, self.i1)
        hi = bisect.bisect_left(t, b, lo, self.i1)
        return lo, (min(hi, self.i1) if self.open_ended else min(hi, self.flat_i))

    def index_at(self, minute: int) -> int:
        """First bar index at or after an epoch minute (i1 when none)."""
        return bisect.bisect_left(self.series.t, minute, self.i0, self.i1)


class SymbolSeries:
    """One symbol's bars for a window: columns, features (loaded on demand),
    UK-day views, higher-timeframe bars. Plain arrays, no per-bar objects."""

    def __init__(self, symbol: str, entry: "UniverseEntry"):
        self.symbol = symbol
        self.entry = entry
        self.spec: SymbolSpec = entry.spec
        self.kind = entry.kind
        self.point = float(entry.spec.point)
        for c in COLS:
            setattr(self, c, array(_TYPE[c]))
        self.parts: list = []
        self.first_i = 0
        self.utc_days: list = []
        self.hours: list = [0] * 24
        self.source = "bridge"
        self.store: Optional[BarStore] = None
        self._feat: dict = {}
        self._memo: dict = {}
        self.feature_loader: Optional[Callable] = None
        self.news_inputs = None

    @property
    def n(self) -> int:
        return len(self.t)

    @classmethod
    def from_columns(cls, entry: "UniverseEntry", t, o, h, l, c, s, v, *, first_i: int = 0,
                     hours: Optional[list] = None, source: str = "SYNTHETIC") -> "SymbolSeries":
        """A series built in memory (tests, synthetic data). Features are
        computed from the arrays themselves."""
        ser = cls(entry.name, entry)
        for name, data in zip(COLS, (t, o, h, l, c, s, v)):
            getattr(ser, name).extend(data)
        ser.first_i = first_i
        ser.hours = hours or [0] * 24
        ser.source = source
        ser.utc_days = sorted(min_to_day(dd * MINUTES_PER_DAY)
                              for dd in {m // MINUTES_PER_DAY for m in ser.t[first_i:]})
        return ser

    # ---- features
    def f(self, name: str):
        """A feature array on the M1 index (loaded or computed on first use)."""
        arr = self._feat.get(name)
        if arr is not None:
            return arr
        from . import features
        features.attach(self, features.set_of(name))
        return self._feat[name]

    def has_feature(self, name: str) -> bool:
        return name in self._feat

    # ---- UK days
    def dayviews(self) -> list:
        dv = self._memo.get("days")
        if dv is not None:
            return dv
        out = []
        if self.n > self.first_i:
            t = self.t
            first = dt.datetime.fromtimestamp(t[self.first_i] * 60, tz=UTC)
            from ..ops.standing import uk_date
            d = uk_date(first)
            last_day = uk_date(dt.datetime.fromtimestamp(t[self.n - 1] * 60, tz=UTC))
            one = dt.timedelta(days=1)
            while d <= last_day:
                iso = d.isoformat()
                lo_m = uk_day_min(iso)
                hi_m = uk_day_min((d + one).isoformat())
                i0 = bisect.bisect_left(t, lo_m, self.first_i, self.n)
                i1 = bisect.bisect_left(t, hi_m, i0, self.n)
                if i1 > i0:
                    fi = bisect.bisect_left(t, flat_min(iso), i0, i1) - 1
                    if fi >= i0:
                        out.append(DayView(self, iso, i0, i1, fi, d.toordinal()))
                d += one
        self._memo["days"] = out
        return out

    # ---- higher timeframes
    def tf(self, minutes: int) -> "TfBars":
        """The M1 bars grouped into ``minutes``-minute bars, each emitted only
        once it can no longer change (same builder the features use)."""
        key = ("tf", minutes)
        got = self._memo.get(key)
        if got is None:
            from .features import TfBuilder
            tb = TfBars()
            b = TfBuilder(minutes)
            t, o, h, l, c = self.t, self.o, self.h, self.l, self.c
            for i in range(self.n):
                for bar in b.push(t[i], o[i], h[i], l[i], c[i]):
                    tb.add(bar, i)
            got = tb
            self._memo[key] = got
        return got

    def spread_arrays(self):
        """(effective spread per bar in points as floats, assumed flags): the
        bar's recorded spread, floored at that UTC hour's median over the past
        stretch; a bar with no recorded spread takes the default for its kind
        and is flagged assumed."""
        got = self._memo.get("spread")
        if got is None:
            from .sim import default_spread_points
            dflt = default_spread_points(self.symbol, self.kind)
            hours = self.hours
            n = self.n
            eff = array("f", bytes(4 * n))
            assumed = bytearray(n)
            t, s = self.t, self.s
            for i in range(n):
                sp = s[i]
                med = hours[(t[i] // 60) % 24]
                if sp <= 0:
                    eff[i] = dflt if dflt > med else med
                    assumed[i] = 1
                else:
                    eff[i] = sp if sp > med else med
            got = (eff, assumed)
            self._memo["spread"] = got
        return got

    def day_median_spread(self, dv: "DayView") -> float:
        """Median effective spread of a UK day (the skip rule's reference)."""
        key = ("dms", dv.uk)
        got = self._memo.get(key)
        if got is None:
            eff, _ = self.spread_arrays()
            vals = sorted(eff[dv.i0:dv.i1])
            got = vals[len(vals) // 2] if vals else 0.0
            self._memo[key] = got
        return got


class TfBars:
    """Higher-timeframe bars of one series. ``end_i[k]`` is the M1 index at
    which bar ``k`` became known (its last minute passed, or the next bucket began)."""
    __slots__ = ("t", "o", "h", "l", "c", "end_i")

    def __init__(self):
        self.t, self.o, self.h, self.l, self.c = (array("i") for _ in range(5))
        self.end_i = array("i")

    def add(self, bar, end_i: int) -> None:
        self.t.append(bar[0]); self.o.append(bar[1]); self.h.append(bar[2])
        self.l.append(bar[3]); self.c.append(bar[4]); self.end_i.append(end_i)

    def __len__(self) -> int:
        return len(self.t)

    def range_for(self, i0: int, i1: int) -> tuple:
        """Bars that became known in M1 index range [i0, i1)."""
        return (bisect.bisect_left(self.end_i, i0), bisect.bisect_left(self.end_i, i1))


class Window:
    """The prices one test runs on: a set of symbol series for a set of days."""

    def __init__(self, *, kind: str, days: Sequence[str], cut_day: str, stretch_id: str):
        self.kind = kind
        self.days = list(days)
        self.cut_day = cut_day
        self.stretch_id = stretch_id
        self.series: dict[str, SymbolSeries] = {}
        self.kinds: dict[str, str] = {}
        self.synthetic = False
        self.store: Optional[BarStore] = None
        self.test_id: Optional[int] = None         # a fresh window carries its running row
        self.news_days: Optional[set] = None        # UK days with a known calendar

    def set_calendar(self, calendar) -> None:
        """Wire the read-only calendar in: which days are news-known, and the
        events each symbol's news feature is computed from."""
        from . import features
        features.wire_calendar(self, calendar)

    def news_known(self, day: str) -> bool:
        return self.news_days is not None and day in self.news_days

    def symbols_of(self, kind: str) -> list:
        return [s for s in self.series if self.kinds.get(s) == kind]

    @property
    def info(self) -> WindowInfo:
        return WindowInfo(kind=self.kind, day_from=self.days[0] if self.days else "",
                          day_to=self.days[-1] if self.days else "", days=len(self.days),
                          cut_day=self.cut_day, stretch_id=self.stretch_id,
                          symbols=len(self.series))

    def uk_days(self) -> list:
        """The window's trading days as UK dates (the page's calendar)."""
        return list(self.days)

    def drop_features(self, keep: Sequence[str] = ()) -> None:
        """Free feature arrays (and derived higher-timeframe bars) to save memory
        between families. ``keep`` names feature sets to leave loaded."""
        from .features import NAME_TO_SET
        for ser in self.series.values():
            for name in [n for n in ser._feat if NAME_TO_SET.get(n) not in keep]:
                del ser._feat[name]
            for k in [k for k in ser._memo if isinstance(k, tuple) and k[0] == "tf"]:
                del ser._memo[k]


# ------------------------------------------------------------ fresh_once ---
def fresh_once(candidate_id: str, ledger, stretch: CutState, store: BarStore,
               universe: Sequence[UniverseEntry], *, calendar=None,
               now: Optional[dt.datetime] = None, warm: int = WARMUP_BARS) -> Window:
    """THE ONLY WAY TO READ FRESH BARS (a source-grep test enforces that nothing
    but this module and ``stages.py`` mentions it).

    In ONE sqlite transaction the ledger checks that a PAST row with passed=1
    exists for this stretch, that no fresh row exists for the id in any
    stretch (except one TOO_FEW_TRADES retry), and that the stretch has looks
    left (``stretch.fresh_cap``); then it inserts the ``running`` fresh row and
    commits. Only then is the window built and returned, carrying ``test_id``.
    Raises ``ledger.FreshRefused`` (codes NO_PAST_PASS, ALREADY_LOOKED,
    CAP_REACHED) and builds nothing. If building then fails the look is still
    spent: the row is marked crashed and the error propagates."""
    test_id = ledger.claim_fresh(candidate_id, stretch.stretch_id, stretch.cut_day,
                                 stretch.fresh_cap, now=now)
    try:
        days = store.fresh_days(universe, stretch.cut_day)
        win = store._build_window(KIND_FRESH, days, universe, cut_day=stretch.cut_day,
                                  stretch_id=stretch.stretch_id, warm=warm, calendar=calendar)
    except Exception as e:
        ledger.crash_test(test_id, f"window failed: {e}", now=now)
        raise
    win.test_id = test_id
    return win
