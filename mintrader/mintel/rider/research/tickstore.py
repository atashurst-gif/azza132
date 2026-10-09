"""Real broker ticks on disk: fetch, store, load, and say how much there is.

Layout under the research directory::

    ticks/<SYMBOL>/<YYYY-MM-DD>.csv.gz          one UTC day, header "time_ms,bid,ask"
    ticks/<SYMBOL>/.partial/<YYYY-MM-DD>/HHMM.csv.gz   hour chunks while a day is being fetched
    ticks/index.json                            per day: ticks, first/last, gaps, bytes

Fetching (``fetch_ticks``) asks ``broker.ticks_range`` for one chunk (an
hour by default) at a time. A chunk that arrived is written at once, so an
interrupted fetch RESUMES where it stopped; a day is merged into its file
only when every chunk is in. Only COMPLETE UTC days are fetched (never
today). A size budget stops the fetch before the disk fills up. Progress is
printed per day.

Coverage (``TickStore.coverage``) says per market and day how many ticks
there are and where the gaps are, and says plainly when the history is not
enough to judge anything.
"""
from __future__ import annotations

import datetime as dt
import gzip
import heapq
import json
import os
import time
from array import array
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterator, Optional, Sequence

from ...broker.base import Tick

UTC = dt.timezone.utc
HEADER = "time_ms,bid,ask"
DAY_MS = 86_400_000
SESSION_HOURS = (7, 20)              # UTC hours in which a weekday gap counts against the data
GAP_REPORT_SECONDS = 120.0


def day_start_ms(day: str) -> int:
    d = dt.date.fromisoformat(day)
    return int(dt.datetime(d.year, d.month, d.day, tzinfo=UTC).timestamp() * 1000)


def ms_to_dt(ms: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(ms / 1000.0, UTC)


def dt_to_ms(t: dt.datetime) -> int:
    if t.tzinfo is None:
        t = t.replace(tzinfo=UTC)
    return int(round(t.timestamp() * 1000.0))


def add_days(day: str, n: int) -> str:
    return (dt.date.fromisoformat(day) + dt.timedelta(days=n)).isoformat()


def weekdays_back(last: dt.date, n: int) -> list[str]:
    """The last ``n`` weekdays (Mon-Fri) ending at ``last`` (inclusive), oldest first."""
    out: list[str] = []
    d = last
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d -= dt.timedelta(days=1)
    return sorted(out)


@dataclass
class DayTicks:
    """One market's ticks, compact: epoch milliseconds, bid, ask."""
    symbol: str
    times: array = field(default_factory=lambda: array("q"))
    bids: array = field(default_factory=lambda: array("d"))
    asks: array = field(default_factory=lambda: array("d"))

    def __len__(self) -> int:
        return len(self.times)

    def extend(self, other: "DayTicks") -> None:
        if len(other) == 0:
            return
        if len(self) and other.times[0] < self.times[-1]:
            raise ValueError("ticks must be appended in time order")
        self.times.extend(other.times)
        self.bids.extend(other.bids)
        self.asks.extend(other.asks)

    def ticks(self) -> Iterator[Tick]:
        s = self.symbol
        for t, b, a in zip(self.times, self.bids, self.asks):
            yield Tick(s, ms_to_dt(t), b, a)


def merged(days: dict[str, DayTicks], order: Sequence[str]) -> Iterator[tuple[int, int, float, float]]:
    """Every market's ticks in one time-ordered stream of (time_ms, symbol
    index, bid, ask); equal times come in ``order``."""
    def gen(k: int, d: DayTicks):
        for t, b, a in zip(d.times, d.bids, d.asks):
            yield (t, k, b, a)
    streams = [gen(k, days[s]) for k, s in enumerate(order) if s in days and len(days[s])]
    return heapq.merge(*streams)


def day_stats(times: Sequence[int], day: str) -> dict:
    """Tick count, first/last, and the gaps (the largest overall and inside
    the busy hours of a weekday)."""
    n = len(times)
    out = {"ticks": n, "first_ms": times[0] if n else None, "last_ms": times[-1] if n else None,
           "max_gap_s": 0.0, "max_session_gap_s": 0.0, "gaps": []}
    d0 = day_start_ms(day)
    if not n:
        if dt.date.fromisoformat(day).weekday() < 5:
            out["max_session_gap_s"] = (SESSION_HOURS[1] - SESSION_HOURS[0]) * 3600.0
        out["max_gap_s"] = DAY_MS / 1000.0
        return out
    weekday = dt.date.fromisoformat(day).weekday() < 5
    s0, s1 = d0 + SESSION_HOURS[0] * 3_600_000, d0 + SESSION_HOURS[1] * 3_600_000
    gaps: list[tuple[float, int]] = []
    prev = None
    edges = [d0] + list(times) + [d0 + DAY_MS]
    max_sess = 0.0
    for t in edges:
        if prev is not None:
            g = (t - prev) / 1000.0
            if g >= GAP_REPORT_SECONDS:
                gaps.append((g, prev))
            if weekday:
                lo, hi = max(prev, s0), min(t, s1)
                if hi > lo:
                    max_sess = max(max_sess, (hi - lo) / 1000.0)
        prev = t
    inner = [(times[i + 1] - times[i]) / 1000.0 for i in range(n - 1)]
    out["max_gap_s"] = max(inner) if inner else 0.0
    out["max_session_gap_s"] = max_sess if weekday else 0.0
    gaps.sort(reverse=True)
    out["gaps"] = [{"start_utc": ms_to_dt(p).strftime("%H:%M:%S"), "seconds": round(g, 1)} for g, p in gaps[:5]]
    return out


@dataclass
class DayCoverage:
    symbol: str
    day: str
    ticks: int
    max_session_gap_s: float
    usable: bool
    why: str = ""

    def to_dict(self) -> dict:
        return {"symbol": self.symbol, "day": self.day, "ticks": self.ticks,
                "max_session_gap_min": round(self.max_session_gap_s / 60.0, 1), "usable": self.usable, "why": self.why}


@dataclass
class Coverage:
    symbols: list
    days: list
    rows: list
    usable_days: list
    min_days: int
    min_ticks: int
    max_gap_minutes: float

    @property
    def sufficient(self) -> bool:
        return len(self.usable_days) >= self.min_days

    def ticks_total(self) -> int:
        return sum(r.ticks for r in self.rows)

    def statement(self) -> str:
        n_req, n_ok = len(self.days), len(self.usable_days)
        if not self.sufficient:
            return (f"History insufficient: {n_ok} usable day(s) of ticks out of the {n_req} requested; at least "
                    f"{self.min_days} are needed before any result can be judged.")
        missing = [r for r in self.rows if not r.usable and r.day in self.days]
        tail = f" {len(missing)} market-days were not usable and were left out." if missing else ""
        return f"{n_ok} of {n_req} requested days had usable ticks.{tail}"

    def to_dict(self) -> dict:
        per_day = {}
        for r in self.rows:
            per_day.setdefault(r.day, {})[r.symbol] = r.to_dict()
        return {"symbols": list(self.symbols), "days_requested": list(self.days), "days_usable": list(self.usable_days),
                "sufficient": self.sufficient, "statement": self.statement(), "ticks_total": self.ticks_total(),
                "rules": {"min_days": self.min_days, "min_ticks_per_day": self.min_ticks,
                          "max_gap_minutes_in_busy_hours": self.max_gap_minutes},
                "per_day": per_day}


class TickStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._index: Optional[dict] = None

    # ------------------------------------------------------------- paths --
    def day_path(self, symbol: str, day: str) -> Path:
        return self.root / symbol / f"{day}.csv.gz"

    def partial_dir(self, symbol: str, day: str) -> Path:
        return self.root / symbol / ".partial" / day

    @property
    def index_path(self) -> Path:
        return self.root / "index.json"

    # ------------------------------------------------------------- index --
    def index(self) -> dict:
        if self._index is None:
            try:
                self._index = json.loads(self.index_path.read_text())
                if not isinstance(self._index, dict):
                    self._index = {}
            except Exception:
                self._index = {}
        return self._index

    def _save_index(self) -> None:
        tmp = self.index_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.index(), indent=1, sort_keys=True))
        tmp.replace(self.index_path)

    def entry(self, symbol: str, day: str) -> Optional[dict]:
        e = self.index().get(symbol, {}).get(day)
        if e is not None and not self.day_path(symbol, day).exists():
            return None
        return e

    def complete(self, symbol: str, day: str) -> bool:
        e = self.entry(symbol, day)
        return bool(e and e.get("complete"))

    def total_bytes(self) -> int:
        n = 0
        for p in self.root.rglob("*.csv.gz"):
            try:
                n += p.stat().st_size
            except OSError:
                pass
        return n

    def days(self, symbol: str) -> list[str]:
        d = self.root / symbol
        if not d.exists():
            return []
        return sorted(p.name[:10] for p in d.glob("*.csv.gz"))

    # ------------------------------------------------------------ writing --
    @staticmethod
    def _write_csv(path: Path, times: Sequence[int], bids: Sequence[float], asks: Sequence[float]) -> int:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        with gzip.open(tmp, "wt", encoding="ascii", newline="\n") as f:
            f.write(HEADER + "\n")
            f.writelines(f"{t},{b!r},{a!r}\n" for t, b, a in zip(times, bids, asks))
        tmp.replace(path)
        return path.stat().st_size

    def write_day(self, symbol: str, day: str, times: Sequence[int], bids: Sequence[float], asks: Sequence[float],
                  complete: bool = True, source: str = "") -> dict:
        size = self._write_csv(self.day_path(symbol, day), times, bids, asks)
        st = day_stats(times, day)
        st.update({"bytes": size, "complete": bool(complete), "source": source,
                   "written_utc": dt.datetime.now(UTC).replace(microsecond=0).isoformat()})
        self.index().setdefault(symbol, {})[day] = st
        self._save_index()
        return st

    # ------------------------------------------------------------ reading --
    @staticmethod
    def _read_csv(path: Path, symbol: str, start_ms: Optional[int] = None, end_ms: Optional[int] = None) -> DayTicks:
        out = DayTicks(symbol)
        if not path.exists():
            return out
        ta, ba, aa = out.times.append, out.bids.append, out.asks.append
        with gzip.open(path, "rt", encoding="ascii") as f:
            first = f.readline()
            if not first.startswith("time_ms"):
                raise ValueError(f"{path}: not a tick file")
            for line in f:
                p = line.split(",")
                if len(p) < 3:
                    continue
                t = int(p[0])
                if start_ms is not None and t < start_ms:
                    continue
                if end_ms is not None and t >= end_ms:
                    break
                ta(t)
                ba(float(p[1]))
                aa(float(p[2]))
        return out

    def load(self, symbol: str, day: str, start_ms: Optional[int] = None, end_ms: Optional[int] = None) -> DayTicks:
        return self._read_csv(self.day_path(symbol, day), symbol, start_ms, end_ms)

    def load_span(self, symbol: str, start_ms: int, end_ms: int) -> DayTicks:
        """Every stored tick in [start_ms, end_ms), across day files."""
        out = DayTicks(symbol)
        day = ms_to_dt(start_ms).date()
        last = ms_to_dt(max(end_ms - 1, start_ms)).date()
        while day <= last:
            out.extend(self.load(symbol, day.isoformat(), start_ms, end_ms))
            day += dt.timedelta(days=1)
        return out

    # ----------------------------------------------------------- coverage --
    def coverage(self, symbols: Sequence[str], days: Sequence[str], min_days: int = 5, min_ticks: int = 5000,
                 max_gap_minutes: float = 30.0, min_symbol_fraction: float = 0.8) -> Coverage:
        rows: list[DayCoverage] = []
        usable_days = []
        for day in days:
            ok_syms = 0
            for s in symbols:
                e = self.entry(s, day)
                if e is None:
                    rows.append(DayCoverage(s, day, 0, 0.0, False, "no ticks stored"))
                    continue
                n = int(e.get("ticks") or 0)
                gap = float(e.get("max_session_gap_s") or 0.0)
                why = ""
                if not e.get("complete"):
                    why = "the fetch for this day did not finish"
                elif n < min_ticks:
                    why = f"only {n:,} ticks (need {min_ticks:,})"
                elif gap > max_gap_minutes * 60.0:
                    why = f"a {gap / 60.0:.0f}-minute hole in the busy hours"
                rows.append(DayCoverage(s, day, n, gap, not why, why))
                ok_syms += 0 if why else 1
            if symbols and ok_syms >= max(1, int(round(min_symbol_fraction * len(symbols)))):
                usable_days.append(day)
        return Coverage(list(symbols), list(days), rows, usable_days, min_days, min_ticks, max_gap_minutes)


# --------------------------------------------------------------- fetching --

@dataclass
class FetchReport:
    days_fetched: int = 0
    days_already_stored: int = 0
    chunks_fetched: int = 0
    chunks_resumed: int = 0
    empty_retried: int = 0
    failed_chunks: list = field(default_factory=list)
    ticks_fetched: int = 0
    bytes_total: int = 0
    budget_hit: bool = False
    notes: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return dict(self.__dict__)


def _chunk_name(start_ms: int, day: str) -> str:
    m = (start_ms - day_start_ms(day)) // 60_000
    return f"{m // 60:02d}{m % 60:02d}.csv.gz"


def _ask(broker, sym: str, c0: int, c1: int, retries: int, sleep: Callable[[float], None]):
    err = ""
    for attempt in range(max(1, retries)):
        try:
            return list(broker.ticks_range(sym, ms_to_dt(c0), ms_to_dt(c1)) or []), ""
        except Exception as exc:                           # the bridge hiccuped: try again shortly
            err = str(exc)
            sleep(min(2.0 * (attempt + 1), 10.0))
    return None, err


def fetch_ticks(broker, store: TickStore, symbols: Sequence[str], days: Sequence[str], *,
                chunk_minutes: int = 60, max_bytes: float = 3e9, retries: int = 3,
                now: Optional[dt.datetime] = None, progress: Callable[[str], None] = print,
                sleep: Optional[Callable[[float], None]] = None) -> FetchReport:
    """Fetch every (symbol, day) not already stored, an hour chunk at a time."""
    sleep = sleep or time.sleep
    rep = FetchReport()
    now = now or dt.datetime.now(UTC)
    now_ms = dt_to_ms(now)
    chunk_ms = int(chunk_minutes) * 60_000
    if chunk_ms <= 0 or DAY_MS % chunk_ms:
        raise ValueError("chunk_minutes must divide a day")
    used = store.total_bytes()
    jobs = [(s, d) for s in symbols for d in days]
    for k, (sym, day) in enumerate(jobs, 1):
        d0 = day_start_ms(day)
        if d0 + DAY_MS > now_ms:
            rep.notes.append(f"{sym} {day}: not a complete day yet - not fetched")
            continue
        if store.complete(sym, day):
            rep.days_already_stored += 1
            continue
        pdir = store.partial_dir(sym, day)
        weekday = dt.date.fromisoformat(day).weekday() < 5
        whole = DayTicks(sym)
        failed = False
        for c0 in range(d0, d0 + DAY_MS, chunk_ms):
            c1 = c0 + chunk_ms
            path = pdir / _chunk_name(c0, day)
            if path.exists():
                try:
                    whole.extend(TickStore._read_csv(path, sym))
                    rep.chunks_resumed += 1
                    continue
                except Exception:
                    path.unlink(missing_ok=True)          # a damaged chunk is fetched again
            if used >= max_bytes:
                rep.budget_hit = True
                rep.notes.append(f"stopped: the tick store reached its size budget "
                                 f"({used / 1e9:.2f} GB of {max_bytes / 1e9:.2f} GB)")
                progress(rep.notes[-1])
                rep.bytes_total = used
                return rep
            got, err = _ask(broker, sym, c0, c1, retries, sleep)
            if got is None and c1 - c0 > 4 * 60_000:
                # a very busy chunk can time out over the bridge: try it in quarters before giving up
                got = []
                q = (c1 - c0) // 4
                for k in range(4):
                    part, err = _ask(broker, sym, c0 + k * q, c1 if k == 3 else c0 + (k + 1) * q, retries, sleep)
                    if part is None:
                        got = None
                        break
                    got.extend(part)
            hour = (c0 - d0) // 3_600_000
            if got == [] and weekday and 1 <= hour < 20:
                # MetaTrader can answer "nothing" while it is still syncing tick history: ask once more
                sleep(2.0)
                again, _e = _ask(broker, sym, c0, c1, 1, sleep)
                if again:
                    got = again
                    rep.empty_retried += 1
            if got is None:
                failed = True
                rep.failed_chunks.append({"symbol": sym, "day": day, "start_utc": ms_to_dt(c0).isoformat(),
                                          "error": err})
                continue
            chunk = DayTicks(sym)
            rows = sorted(((dt_to_ms(t.time), float(t.bid), float(t.ask)) for t in got), key=lambda r: r[0])
            last = None
            for t, b, a in rows:
                if not (c0 <= t < c1) or b <= 0 or a <= 0 or a < b:
                    continue
                if last is not None and (t, b, a) == last:
                    continue                               # chunk edges can repeat a tick
                last = (t, b, a)
                chunk.times.append(t)
                chunk.bids.append(b)
                chunk.asks.append(a)
            used += TickStore._write_csv(path, chunk.times, chunk.bids, chunk.asks)
            rep.chunks_fetched += 1
            rep.ticks_fetched += len(chunk)
            whole.extend(chunk)
        if failed:
            progress(f"{sym} {day}: some hours could not be fetched - will retry next run "
                     f"({k} of {len(jobs)} market-days)")
            continue
        st = store.write_day(sym, day, whole.times, whole.bids, whole.asks, complete=True, source="broker.ticks_range")
        for p in pdir.glob("*.csv.gz"):
            p.unlink(missing_ok=True)
        try:
            pdir.rmdir()
        except OSError:
            pass
        rep.days_fetched += 1
        used = store.total_bytes()
        progress(f"{sym} {day}: {st['ticks']:,} ticks ({st['bytes'] / 1e6:.1f} MB) - {k} of {len(jobs)} market-days, "
                 f"store {used / 1e6:.0f} MB")
    rep.bytes_total = used
    return rep


def save_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=1, sort_keys=True, default=str))
    os.replace(tmp, path)
