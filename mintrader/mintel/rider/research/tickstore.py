"""Real broker ticks on disk: fetch, store, load, and say how much there is.

Layout under the research directory::

    ticks/<SYMBOL>/<YYYY-MM-DD>.csv.gz          one UTC day, header "time_ms,bid,ask"
    ticks/<SYMBOL>/.partial/<YYYY-MM-DD>/HHMM.csv.gz   hour chunks while a day is being fetched
    ticks/index.json                            per day: ticks, first/last, gaps, bytes

Fetching (``fetch_ticks``) asks ``broker.ticks_range`` for one chunk (an
hour by default) at a time (``fetch_chunk``). A chunk that arrived is
written at once, so an interrupted fetch RESUMES where it stopped; a day is
merged into its file only when every chunk is in. Only COMPLETE UTC days
are fetched (never today). A size budget stops the fetch before the disk
fills up. Progress is printed per day.

Two things MetaTrader does are handled (9 Oct: 3,456 hour chunks of 10 past
weekdays came back with ONE tick in total and no error):

* an EMPTY answer for a past period it has not loaded from the server yet.
  An empty hour in busy hours is never stored as final: MetaTrader is asked
  to load the period (the symbol selected, its one-minute bars read) and
  the hour is asked again after short waits; still empty, it is left out
  so a later run asks again;
* an answer for ANOTHER time than asked (a bridge still running code from
  before the 9 Oct fix in mintel/broker/mt5_adapter.py hands MetaTrader
  times without a time zone, and MetaTrader can read them as local time on
  its machine, so an hour's answer comes back an hour early - and the ticks
  outside the hour are dropped). The lag is measured from the answer and
  every request is asked that much later from then on, with a note saying
  to update mintel where MetaTrader runs and restart the bridge.

``probe`` (``python -m mintel.rider.research --probe``) fetches one past
hour of EURUSD this same way and says plainly whether past ticks can be
read.

Days the fetch stored before 9 Oct as complete with busy hours that hold
no tick (the empty answers taken as final) are fetched again by any later
fetch, on any machine; the stored day is kept until the new one is in.

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

from ...broker.base import TF, Tick

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
                  complete: bool = True, source: str = "", extra: Optional[dict] = None) -> dict:
        size = self._write_csv(self.day_path(symbol, day), times, bids, asks)
        st = day_stats(times, day)
        st.update({"bytes": size, "complete": bool(complete), "source": source,
                   "written_utc": dt.datetime.now(UTC).replace(microsecond=0).isoformat()})
        st.update(extra or {})
        self.index().setdefault(symbol, {})[day] = st
        self._save_index()
        return st

    def busy_hours_empty(self, symbol: str, day: str) -> int:
        """How many busy hours (``BUSY_HOURS`` UTC, weekdays only) of a stored
        day hold no tick. Kept in the index; worked out from the day file
        (once) for an entry written before the index kept it."""
        e = self.entry(symbol, day)
        if e is None or dt.date.fromisoformat(day).weekday() >= 5:
            return 0
        n = e.get("busy_hours_empty")
        if isinstance(n, int) and not isinstance(n, bool):
            return n
        times = self.load(symbol, day).times if int(e.get("ticks") or 0) else []
        n = empty_busy_hours(times, day)
        e["busy_hours_empty"] = n
        self._save_index()
        return n

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

BUSY_HOURS = (1, 20)                 # UTC hours of a weekday in which the FX market always trades
EMPTY_BACKOFF_SECONDS = (1.0, 2.0, 4.0, 8.0)   # an empty busy hour is asked again after each of these waits
DRY_DAYS_STOP = 3                    # market-days in a row with no busy-hour tick at all: the fetch stops
SHIFT_QUANTUM_MS = 15 * 60_000       # time zones sit on whole quarter hours
SHIFT_TOLERANCE_MS = 1_000           # an answer may hold a tick a moment either side of the window
MAX_SHIFT_MS = 14 * 3_600_000
PROBE_HOUR = 10                      # the probe's past hour: 10:00-11:00 UTC on the last complete weekday


def busy_hour(day: str, hour: int) -> bool:
    """An hour in which the FX market always trades: an empty answer there
    means MetaTrader has not sent the history, never that there was none."""
    return dt.date.fromisoformat(day).weekday() < 5 and BUSY_HOURS[0] <= hour < BUSY_HOURS[1]


def empty_busy_hours(times: Sequence[int], day: str) -> int:
    """How many busy hours of ``day`` hold none of ``times`` (0 on a weekend)."""
    if dt.date.fromisoformat(day).weekday() >= 5:
        return 0
    d0 = day_start_ms(day)
    seen = {int((t - d0) // 3_600_000) for t in times}
    return sum(1 for h in range(*BUSY_HOURS) if h not in seen)


def _stored_with_empty_busy_hours(store: "TickStore", sym: str, day: str) -> int:
    """A day an EARLIER fetcher stored as complete although some of its busy
    hours hold no tick: how many such hours (0: the stored day stands).
    Until 9 Oct the fetcher took MetaTrader's empty answers as final, and
    stored whole weekdays with about no tick in them; this fetcher never
    stores a busy-hour chunk that came back empty, so a day it wrote
    (``busy_hours_checked``) is trusted as it is."""
    e = store.entry(sym, day)
    if not e or e.get("busy_hours_checked") or e.get("source") != "broker.ticks_range":
        return 0
    return store.busy_hours_empty(sym, day)


@dataclass
class FetchReport:
    days_fetched: int = 0
    days_already_stored: int = 0
    chunks_fetched: int = 0
    chunks_resumed: int = 0
    empty_retried: int = 0           # busy hours that came back empty, then had ticks when asked again
    empty_busy_chunks: list = field(default_factory=list)   # busy hours still empty: not stored, asked next run
    failed_chunks: list = field(default_factory=list)
    ticks_fetched: int = 0
    bytes_total: int = 0
    budget_hit: bool = False
    days_refetched: int = 0          # stored as complete by an earlier fetch with empty busy hours: asked again
    time_shift_minutes: Optional[float] = None   # MetaTrader answered this much earlier than asked (corrected)
    stopped_no_history: bool = False
    notes: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return dict(self.__dict__)


@dataclass
class FetchState:
    """What one fetch has learnt about MetaTrader's answers so far."""
    shift_ms: int = 0                # every window is asked this much later (MetaTrader answers this much early)
    dry_days: int = 0                # market-days in a row with no busy-hour tick at all


@dataclass
class ChunkAnswer:
    """One window as fetched, and what MetaTrader actually returned."""
    chunk: Optional[DayTicks]        # the ticks kept for the window; None: it could not be fetched (``error``)
    error: str = ""
    raw: int = 0                     # ticks MetaTrader returned on the last ask
    raw_first_ms: Optional[int] = None
    raw_last_ms: Optional[int] = None
    asked: int = 0                   # times the window was asked for
    retried: int = 0                 # ... of them because it came back empty
    empty_busy: bool = False         # still empty in busy hours after MetaTrader was asked to load it: never final
    bars_with_prices: Optional[int] = None   # its one-minute bars in the window with prices (None: not read)
    notes: list = field(default_factory=list)


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


def _ask_window(broker, sym: str, a: int, b: int, retries: int, sleep: Callable[[float], None]):
    """One window [a, b]; one that fails (a very busy hour can time out over
    the bridge) is tried in quarters before giving up."""
    got, err = _ask(broker, sym, a, b, retries, sleep)
    if got is None and b - a > 4 * 60_000:
        got = []
        q = (b - a) // 4
        for k in range(4):
            part, err = _ask(broker, sym, a + k * q, b if k == 3 else a + (k + 1) * q, retries, sleep)
            if part is None:
                return None, err
            got.extend(part)
    return got, err


def _rows(got) -> list[tuple[int, float, float]]:
    rows = []
    for t in got:
        try:
            rows.append((dt_to_ms(t.time), float(t.bid), float(t.ask)))
        except (AttributeError, TypeError, ValueError):
            continue
    rows.sort(key=lambda r: r[0])
    return rows


def _off(rows: list, c0: int, c1: int) -> bool:
    """The answer holds more ticks outside [c0, c1] than a right answer ever does."""
    out = sum(1 for t, _b, _a in rows if t < c0 - SHIFT_TOLERANCE_MS or t > c1 + SHIFT_TOLERANCE_MS)
    return out > max(2, len(rows) // 100)


def _lag_ms(rows: list, a: int, b: int) -> int:
    """How much earlier than the window [a, b] asked for the answer came back, to the quarter hour."""
    lag = ((a - rows[0][0]) + (b - rows[-1][0])) / 2.0
    return int(round(lag / SHIFT_QUANTUM_MS)) * SHIFT_QUANTUM_MS


def _keep(sym: str, rows: list, c0: int, c1: int) -> DayTicks:
    """The valid ticks inside [c0, c1), a tick repeated at a chunk edge once."""
    chunk = DayTicks(sym)
    last = None
    for t, b, a in rows:
        if not (c0 <= t < c1) or b <= 0 or a <= 0 or a < b:
            continue
        if last is not None and (t, b, a) == last:
            continue                                   # chunk edges can repeat a tick
        last = (t, b, a)
        chunk.times.append(t)
        chunk.bids.append(b)
        chunk.asks.append(a)
    return chunk


def _hm(ms: int, seconds: bool = False) -> str:
    return ms_to_dt(ms).strftime("%H:%M:%S.%f")[:12] if seconds else ms_to_dt(ms).strftime("%H:%M")


def _load(broker, sym: str, c0: int, c1: int) -> Optional[int]:
    """Ask MetaTrader to load a past period: the symbol selected and its
    one-minute bars over the window read (MetaTrader fetches a period's
    history from the server when it is asked for it). Returns how many of
    those bars fall in the window, or None when they could not be read.
    Only what mintel.broker already offers is used."""
    sel = getattr(broker, "ensure_selected", None)
    if callable(sel):
        try:
            sel(sym)
        except Exception:
            pass
    bars = getattr(broker, "bars", None)
    if not callable(bars):
        return None
    try:
        got = bars(sym, TF.M1, int((c1 - c0) // 60_000) + 5, ms_to_dt(c1)) or []
    except Exception:
        return None
    n = 0
    for bar in got:
        try:
            if c0 <= dt_to_ms(bar.time) < c1:
                n += 1
        except (AttributeError, TypeError, ValueError):
            continue
    return n


def fetch_chunk(broker, sym: str, day: str, c0: int, c1: int, state: FetchState, *, retries: int = 3,
                sleep: Callable[[float], None] = time.sleep,
                backoff: Sequence[float] = EMPTY_BACKOFF_SECONDS) -> ChunkAnswer:
    """Fetch one window [c0, c1) of ``sym`` on ``day``.

    * The answer is checked against the window. When it holds the ticks of
      another time, the lag is measured, every window is asked that much
      later from then on (``state.shift_ms``) and this one is asked again;
      an answer that still does not fit is an error (nothing stored).
    * An EMPTY answer in busy hours is never taken as "no ticks": MetaTrader
      is asked to load the period (``_load``) and the window is asked again
      after each wait in ``backoff``. Still empty, ``empty_busy`` is set and
      the caller stores nothing for it, so a later run asks again.
    """
    ans = ChunkAnswer(None)
    hour = int((c0 - day_start_ms(day)) // 3_600_000)

    def attempt() -> Optional[list]:
        a, b = c0 + state.shift_ms, c1 + state.shift_ms
        got, err = _ask_window(broker, sym, a, b, retries, sleep)
        ans.asked += 1
        if got is None:
            ans.error = err or "no answer"
            return None
        rows = _rows(got)
        ans.raw = len(rows)
        ans.raw_first_ms, ans.raw_last_ms = (rows[0][0], rows[-1][0]) if rows else (None, None)
        if not _off(rows, c0, c1):
            return rows
        lag = _lag_ms(rows, a, b)
        if lag != state.shift_ms and abs(lag) <= MAX_SHIFT_MS:
            again, err = _ask_window(broker, sym, c0 + lag, c1 + lag, retries, sleep)
            ans.asked += 1
            rows2 = _rows(again) if again is not None else None
            if rows2 is not None and not _off(rows2, c0, c1):
                state.shift_ms = lag
                mins = lag / 60_000.0
                ans.notes.append(
                    f"MetaTrader answered the {sym} {_hm(c0)}-{_hm(c1)} UTC request with ticks from about "
                    f"{abs(mins):g} minutes {'earlier' if mins > 0 else 'later'} than asked (from "
                    f"{_hm(rows[0][0])} to {_hm(rows[-1][0])} UTC) - most likely it reads the times it "
                    f"is given as local time on the machine it runs on. The fix for this is already in "
                    f"mintel/broker/mt5_adapter.py (9 Oct: times handed over as whole seconds, only the ticks "
                    f"inside the window kept), so the program that talks to MetaTrader (the bridge, on a Mac "
                    f"the one running under Wine) is still running code from before it: update mintel on the "
                    f"machine that runs MetaTrader and restart the bridge (Stop, then Start). No code needs "
                    f"to change. Until then every request is asked {abs(mins):g} minutes "
                    f"{'later' if mins > 0 else 'earlier'}, which returns the hour wanted.")
                ans.raw = len(rows2)
                ans.raw_first_ms, ans.raw_last_ms = (rows2[0][0], rows2[-1][0]) if rows2 else (None, None)
                return rows2
        ans.error = (f"MetaTrader's answer for {_hm(c0)}-{_hm(c1)} UTC held ticks from {_hm(rows[0][0])} to "
                     f"{_hm(rows[-1][0])} UTC and could not be matched to the hour asked; nothing stored")
        return None

    rows = attempt()
    if rows is None:
        return ans
    kept = _keep(sym, rows, c0, c1)
    if len(kept) or not busy_hour(day, hour):
        ans.chunk = kept
        return ans
    # MetaTrader often answers "nothing" for a past period until it has loaded it: ask it to, then ask again
    ans.bars_with_prices = _load(broker, sym, c0, c1)
    for wait in backoff:
        sleep(wait)
        ans.retried += 1
        rows = attempt()
        if rows is None:
            return ans
        kept = _keep(sym, rows, c0, c1)
        if len(kept):
            ans.chunk = kept
            return ans
    ans.chunk = kept
    ans.empty_busy = True
    return ans


def fetch_ticks(broker, store: TickStore, symbols: Sequence[str], days: Sequence[str], *,
                chunk_minutes: int = 60, max_bytes: float = 3e9, retries: int = 3,
                now: Optional[dt.datetime] = None, progress: Callable[[str], None] = print,
                sleep: Optional[Callable[[float], None]] = None,
                backoff: Sequence[float] = EMPTY_BACKOFF_SECONDS) -> FetchReport:
    """Fetch every (symbol, day) not already stored, an hour chunk at a time
    (``fetch_chunk``). A day is stored only once every hour is in. A busy
    hour still empty after MetaTrader was asked to load it is never stored,
    so a later run asks again (the rest of that day gets one quick retry
    each). When no busy hour of ``DRY_DAYS_STOP`` market-days in a row gives
    a single tick, the fetch stops and says so (``--probe`` shows what
    MetaTrader answers).

    A day an earlier version of this fetch (until 9 Oct) stored as complete
    although busy hours in it hold no tick - it took MetaTrader's empty
    answers as final - is fetched again, as is an empty busy-hour chunk it
    left half-way; the stored day is kept until the new one is complete.
    This repairs any store, whichever machine or launcher wrote it."""
    sleep = sleep or time.sleep
    rep = FetchReport()
    state = FetchState()
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
            holes = _stored_with_empty_busy_hours(store, sym, day)
            if not holes:
                rep.days_already_stored += 1
                continue
            rep.days_refetched += 1
            progress(f"{sym} {day}: stored as complete by an earlier version of this fetch, but {holes} of its "
                     f"{BUSY_HOURS[1] - BUSY_HOURS[0]} busy hours hold no tick - asking MetaTrader again (the "
                     f"stored day is kept until the new one is complete)")
        pdir = store.partial_dir(sym, day)
        whole = DayTicks(sym)
        failed = False
        empty = 0
        busy_asked = busy_ticks = 0                    # busy hours asked this run; busy-hour ticks (or resumed)
        quick = False                                  # after a busy hour stayed empty: one quick retry each
        for c0 in range(d0, d0 + DAY_MS, chunk_ms):
            c1 = c0 + chunk_ms
            busy = busy_hour(day, int((c0 - d0) // 3_600_000))
            path = pdir / _chunk_name(c0, day)
            if path.exists():
                try:
                    part = TickStore._read_csv(path, sym)
                    if busy and not len(part):
                        raise ValueError("an empty busy hour")    # only a fetch before 9 Oct stored one
                    whole.extend(part)
                    rep.chunks_resumed += 1
                    busy_ticks += len(part) if busy else 0
                    continue
                except Exception:
                    path.unlink(missing_ok=True)          # a damaged (or empty busy) chunk is fetched again
            if used >= max_bytes:
                rep.budget_hit = True
                rep.notes.append(f"stopped: the tick store reached its size budget "
                                 f"({used / 1e9:.2f} GB of {max_bytes / 1e9:.2f} GB)")
                progress(rep.notes[-1])
                rep.bytes_total = used
                return rep
            ans = fetch_chunk(broker, sym, day, c0, c1, state, retries=retries, sleep=sleep,
                              backoff=tuple(backoff)[:1] if quick else backoff)
            for n in ans.notes:
                rep.notes.append(n)
                progress(n)
            if state.shift_ms:
                rep.time_shift_minutes = state.shift_ms / 60_000.0
            if ans.chunk is None:
                failed = True
                rep.failed_chunks.append({"symbol": sym, "day": day, "start_utc": ms_to_dt(c0).isoformat(),
                                          "error": ans.error})
                continue
            if busy:
                busy_asked += 1
                busy_ticks += len(ans.chunk)
            if ans.retried and len(ans.chunk):
                rep.empty_retried += 1
            if ans.empty_busy:
                # never stored as final: the day stays incomplete and a later run asks for this hour again
                empty += 1
                quick = True
                rep.empty_busy_chunks.append({"symbol": sym, "day": day, "start_utc": ms_to_dt(c0).isoformat(),
                                              "asked": ans.asked, "bars_in_hour": ans.bars_with_prices})
                continue
            if len(ans.chunk):
                quick = False
            used += TickStore._write_csv(path, ans.chunk.times, ans.chunk.bids, ans.chunk.asks)
            rep.chunks_fetched += 1
            rep.ticks_fetched += len(ans.chunk)
            whole.extend(ans.chunk)
        if busy_ticks:
            state.dry_days = 0
        elif busy_asked:
            state.dry_days += 1
        if failed or empty:
            why = []
            if empty:
                why.append(f"{empty} busy hour{'' if empty == 1 else 's'} came back empty even after asking "
                           f"MetaTrader to load {'it' if empty == 1 else 'them'}")
            if failed:
                why.append("some hours could not be fetched")
            progress(f"{sym} {day}: {' and '.join(why)} - not stored as final; the next run asks again "
                     f"({k} of {len(jobs)} market-days)")
            if state.dry_days >= DRY_DAYS_STOP:
                rep.stopped_no_history = True
                rep.notes.append(f"stopped: MetaTrader gave no tick at all for the busy hours of {DRY_DAYS_STOP} "
                                 f"market-days in a row, even after being asked to load them. Nothing empty was "
                                 f"stored, so the next run asks again. To see what it answers, run: python -m "
                                 f"mintel.rider.research --config data/config.json --probe")
                progress(rep.notes[-1])
                break
            continue
        st = store.write_day(sym, day, whole.times, whole.bids, whole.asks, complete=True, source="broker.ticks_range",
                             extra={"busy_hours_checked": True})
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
    if rep.days_refetched:
        rep.notes.append(f"{rep.days_refetched} market-day{'' if rep.days_refetched == 1 else 's'} stored as complete "
                         f"by an earlier version of this fetch had busy hours with no tick and "
                         f"{'was' if rep.days_refetched == 1 else 'were'} asked for again")
    rep.bytes_total = used
    return rep


def probe(broker, symbol: str = "EURUSD", now: Optional[dt.datetime] = None, *,
          sleep: Optional[Callable[[float], None]] = None,
          backoff: Sequence[float] = EMPTY_BACKOFF_SECONDS) -> dict:
    """Can past ticks be read? Fetches, exactly as the research does
    (``fetch_chunk``), one busy hour of ``symbol`` from the last complete
    weekday (PROBE_HOUR UTC: history, as the research reads it) and, to
    compare, the last complete hour when that was a busy one (recent: still
    in MetaTrader's memory). Returns {"reachable": bool, "lines": [plain
    English], "windows": [...], "time_shift_minutes": float or None}."""
    sleep = sleep or time.sleep
    now = now or dt.datetime.now(UTC)
    now = now.replace(tzinfo=UTC) if now.tzinfo is None else now.astimezone(UTC)
    past_day = weekdays_back(now.date() - dt.timedelta(days=1), 1)[0]
    past = day_start_ms(past_day) + PROBE_HOUR * 3_600_000
    windows = [("past", past_day, past)]
    recent = (dt_to_ms(now) // 3_600_000 - 1) * 3_600_000
    rday = ms_to_dt(recent).date().isoformat()
    if recent > past and busy_hour(rday, ms_to_dt(recent).hour):
        windows.insert(0, ("recent", rday, recent))
    state = FetchState()
    lines: list[str] = []
    out: list[dict] = []
    reachable = False
    for kind, day, c0 in windows:
        c1 = c0 + 3_600_000
        ans = fetch_chunk(broker, symbol, day, c0, c1, state, retries=2, sleep=sleep, backoff=backoff)
        kept = len(ans.chunk) if ans.chunk is not None else 0
        what = ("the last complete hour (recent: still in MetaTrader's memory)" if kind == "recent"
                else "past history, as the research reads it")
        lines.append(f"{symbol} {day} {_hm(c0)}-{_hm(c1)} UTC - {what}:")
        lines.extend(f"  NOTE: {n}" for n in ans.notes)
        if ans.chunk is None:
            lines.append(f"  could not be read: {ans.error}")
        else:
            got = f"  MetaTrader returned {ans.raw:,} tick{'' if ans.raw == 1 else 's'}"
            if ans.raw:
                got += f" (first {_hm(ans.raw_first_ms, True)}, last {_hm(ans.raw_last_ms, True)} UTC)"
            lines.append(got + (f", asked {ans.asked} times" if ans.asked > 1 else ""))
            if kept:
                lines.append(f"  kept for the hour: {kept:,} ticks, first {_hm(ans.chunk.times[0], True)}, "
                             f"last {_hm(ans.chunk.times[-1], True)} UTC")
            else:
                lines.append("  kept for the hour: none")
                if ans.bars_with_prices:
                    lines.append(f"  MetaTrader's own one-minute bars show {ans.bars_with_prices} minutes with prices "
                                 f"in that hour: the prices exist at the broker, but no ticks were sent")
                elif ans.bars_with_prices == 0:
                    lines.append("  MetaTrader's one-minute bars for that hour came back empty too")
        if kind == "past" and kept:
            reachable = True
        out.append({"window": kind, "day": day, "start_utc": ms_to_dt(c0).isoformat(), "returned": ans.raw,
                    "kept": kept, "asked": ans.asked, "error": ans.error, "bars_in_hour": ans.bars_with_prices})
    shift = state.shift_ms / 60_000.0 if state.shift_ms else None
    if reachable:
        lines.append("VERDICT: past ticks ARE reachable" + (
            f" - with every request asked {abs(shift):g} minutes {'later' if shift > 0 else 'earlier'}, as the "
            f"note above explains (the research does this by itself)" if shift else "") + ".")
    else:
        recent_ok = any(o["window"] == "recent" and o["kept"] for o in out)
        lines.append("VERDICT: past ticks are NOT reachable from here right now" + (
            ": the recent hour works and the past hour does not, so MetaTrader has not sent that history "
            "(it may still be loading it - run the probe again in a minute)" if recent_ok else "") + ".")
    return {"reachable": reachable, "lines": lines, "windows": out, "time_shift_minutes": shift}


def save_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=1, sort_keys=True, default=str))
    os.replace(tmp, path)
