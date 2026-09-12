"""Deterministic calendar storage.

Why this exists: MT5's live economic calendar is **not available inside the
Strategy Tester**, and it is not available at all on some builds.  A backtest
that silently runs with an empty calendar while the live bot runs with a full
one is not a news-aware backtest - it is two different systems wearing the same
name.

So every calendar event the bot ever sees is written here (SQLite, one row per
event *revision*), and the backtester reads only from here.  A backtest whose
window is not covered by stored data refuses to claim news awareness - see
:meth:`CalendarStore.coverage`.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence

from ..broker.base import CalendarEvent
from ..clock import UTC, to_utc

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id        TEXT NOT NULL,
    time_utc        TEXT NOT NULL,
    currency        TEXT NOT NULL,
    country         TEXT,
    name            TEXT,
    importance      INTEGER NOT NULL DEFAULT 0,
    actual          REAL,
    forecast        REAL,
    previous        REAL,
    revised_previous REAL,
    unit            TEXT,
    source          TEXT NOT NULL DEFAULT 'mt5',
    first_seen_utc  TEXT NOT NULL,
    last_seen_utc   TEXT NOT NULL,
    PRIMARY KEY (event_id, time_utc, source)
);
CREATE INDEX IF NOT EXISTS ix_events_time ON events(time_utc);
CREATE INDEX IF NOT EXISTS ix_events_ccy_time ON events(currency, time_utc);

CREATE TABLE IF NOT EXISTS coverage (
    source       TEXT NOT NULL,
    day          TEXT NOT NULL,
    events       INTEGER NOT NULL,
    recorded_utc TEXT NOT NULL,
    PRIMARY KEY (source, day)
);
"""


@dataclass
class Coverage:
    start: Optional[dt.date]
    end: Optional[dt.date]
    days: int
    events: int

    def covers(self, start: dt.datetime, end: dt.datetime) -> bool:
        if self.start is None or self.end is None:
            return False
        return self.start <= to_utc(start).date() and self.end >= to_utc(end).date()


class CalendarStore:
    """Thread-safe SQLite calendar archive.

    Uses WAL so the news worker can write while the strategy reads, and so a
    hard kill leaves a recoverable database rather than a corrupt one.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None
        self.connect()

    # ----------------------------------------------------------- lifecycle --
    def connect(self) -> None:
        with self._lock:
            self._conn = sqlite3.connect(str(self.path), check_same_thread=False,
                                         timeout=15.0)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def healthy(self) -> bool:
        """True when the file is present and readable.

        Called by the health supervisor; a failure here triggers a reconnect
        rather than taking the trader down.
        """
        try:
            with self._lock:
                if self._conn is None:
                    return False
                self._conn.execute("SELECT 1 FROM events LIMIT 1").fetchone()
                return True
        except Exception:
            return False

    def reconnect(self) -> bool:
        try:
            self.close()
        except Exception:
            pass
        try:
            self.connect()
            return True
        except Exception:
            return False

    # --------------------------------------------------------------- write --
    def upsert(self, events: Iterable[CalendarEvent]) -> int:
        """Insert or refresh events.

        Revisions matter: a release arrives with ``actual=None``, then gets a
        value, then sometimes gets revised.  The row is updated in place while
        ``first_seen_utc`` is preserved, so the history of what we *knew when*
        stays reconstructable.
        """
        now = dt.datetime.now(UTC).isoformat()
        rows = []
        days: dict[tuple[str, str], int] = {}
        for e in events:
            t = to_utc(e.time_utc).isoformat()
            rows.append((e.event_id, t, e.currency, e.country, e.name,
                         int(e.importance), e.actual, e.forecast, e.previous,
                         e.revised_previous, e.unit, e.source, now, now))
            key = (e.source, to_utc(e.time_utc).date().isoformat())
            days[key] = days.get(key, 0) + 1
        if not rows:
            return 0
        with self._lock:
            cur = self._conn.cursor()
            cur.executemany("""
                INSERT INTO events (event_id, time_utc, currency, country, name,
                    importance, actual, forecast, previous, revised_previous,
                    unit, source, first_seen_utc, last_seen_utc)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(event_id, time_utc, source) DO UPDATE SET
                    actual=COALESCE(excluded.actual, events.actual),
                    forecast=COALESCE(excluded.forecast, events.forecast),
                    previous=COALESCE(excluded.previous, events.previous),
                    revised_previous=COALESCE(excluded.revised_previous,
                                              events.revised_previous),
                    importance=excluded.importance,
                    name=excluded.name,
                    last_seen_utc=excluded.last_seen_utc
            """, rows)
            for (source, day), n in days.items():
                cur.execute("""
                    INSERT INTO coverage (source, day, events, recorded_utc)
                    VALUES (?,?,?,?)
                    ON CONFLICT(source, day) DO UPDATE SET
                        events=MAX(coverage.events, excluded.events),
                        recorded_utc=excluded.recorded_utc
                """, (source, day, n, now))
            self._conn.commit()
            return cur.rowcount

    # ---------------------------------------------------------------- read --
    def query(self, start: dt.datetime, end: dt.datetime,
              currencies: Sequence[str] = (),
              min_importance: int = 0) -> list[CalendarEvent]:
        sql = ("SELECT * FROM events WHERE time_utc >= ? AND time_utc <= ? "
               "AND importance >= ?")
        args: list = [to_utc(start).isoformat(), to_utc(end).isoformat(),
                      int(min_importance)]
        if currencies:
            sql += f" AND currency IN ({','.join('?' * len(currencies))})"
            args += [c.upper() for c in currencies]
        sql += " ORDER BY time_utc ASC"
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [_row_to_event(r) for r in rows]

    def coverage(self, source: str = "") -> Coverage:
        sql = "SELECT day, events FROM coverage"
        args: list = []
        if source:
            sql += " WHERE source = ?"
            args.append(source)
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        if not rows:
            return Coverage(None, None, 0, 0)
        days = sorted(dt.date.fromisoformat(r["day"]) for r in rows)
        return Coverage(days[0], days[-1], len(days),
                        sum(int(r["events"]) for r in rows))

    def historical_values(self, event_name: str, currency: str,
                          before: dt.datetime, limit: int = 40) -> list[CalendarEvent]:
        """Past releases of the same series, used to size a surprise."""
        with self._lock:
            rows = self._conn.execute("""
                SELECT * FROM events
                WHERE name = ? AND currency = ? AND time_utc < ?
                      AND actual IS NOT NULL
                ORDER BY time_utc DESC LIMIT ?
            """, (event_name, currency.upper(), to_utc(before).isoformat(),
                  int(limit))).fetchall()
        return [_row_to_event(r) for r in rows]

    # ------------------------------------------------------------- csv i/o --
    def export_csv(self, path: str | Path) -> int:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events ORDER BY time_utc").fetchall()
        cols = ["event_id", "time_utc", "currency", "country", "name",
                "importance", "actual", "forecast", "previous",
                "revised_previous", "unit", "source"]
        with path.open("w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(cols)
            for r in rows:
                w.writerow([r[c] for c in cols])
        return len(rows)

    def import_csv(self, path: str | Path) -> int:
        """Load a CSV archive, e.g. to seed a fresh VPS or the tester."""
        events = []
        with Path(path).open(newline="") as fh:
            for row in csv.DictReader(fh):
                try:
                    events.append(CalendarEvent(
                        event_id=row["event_id"],
                        time_utc=dt.datetime.fromisoformat(row["time_utc"]),
                        currency=row["currency"].upper(),
                        country=row.get("country", ""),
                        name=row.get("name", ""),
                        importance=int(row.get("importance") or 0),
                        actual=_f(row.get("actual")),
                        forecast=_f(row.get("forecast")),
                        previous=_f(row.get("previous")),
                        revised_previous=_f(row.get("revised_previous")),
                        unit=row.get("unit", ""),
                        source=row.get("source", "csv")))
                except Exception:
                    continue
        return self.upsert(events)


def _f(x) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except ValueError:
        return None


def _row_to_event(r: sqlite3.Row) -> CalendarEvent:
    return CalendarEvent(
        event_id=r["event_id"],
        time_utc=dt.datetime.fromisoformat(r["time_utc"]),
        currency=r["currency"], country=r["country"] or "",
        name=r["name"] or "", importance=int(r["importance"]),
        actual=r["actual"], forecast=r["forecast"], previous=r["previous"],
        revised_previous=r["revised_previous"], unit=r["unit"] or "",
        source=r["source"])
