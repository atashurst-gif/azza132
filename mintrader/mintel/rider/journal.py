"""The Rapid Momentum Rider's own records: data/rider.sqlite.

* ``trades``        - every trade with the FULL pre-entry market state, the
  entry family and score, MFE/MAE in pips and money, realised pips,
  MFE_CAPTURE, the FlowLock X states it went through, the exit reason, the
  latency (signal -> send -> broker acknowledgement -> fill), the mode, where
  the money figure came from (the broker's deal, a paper calculation, or an
  estimate marked '?') and an explanation written for a child.
* ``stop_changes``  - every stop FlowLock X moved (and the broker accepted).
* ``scans``         - scanner snapshots (MARKET DIRECTION SCORE STATE REASON).
* ``events``        - starts, refusals, orphans, health changes.

The open trade's FlowLock state is stored as JSON so a restart carries on
exactly where it was.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
import threading
from pathlib import Path
from typing import Iterable, Optional

from ..clock import to_utc, utcnow

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    ticket INTEGER PRIMARY KEY, mode TEXT, symbol TEXT, side TEXT, volume REAL, gbp_per_pip REAL, pip REAL,
    signal_utc TEXT, send_utc TEXT, ack_utc TEXT, opened_utc TEXT, closed_utc TEXT,
    reference_price REAL, entry REAL, initial_stop REAL, stop REAL, exit_price REAL,
    score REAL, entry_family TEXT, entry_reason TEXT, trigger_id INTEGER,
    state_json TEXT, flow_json TEXT, flow_states TEXT, buckets_json TEXT,
    exit_reason TEXT, exit_detail TEXT,
    mfe_pips REAL, mae_pips REAL, mfe_money REAL, mae_money REAL,
    realised_pips REAL, net_money REAL, money_source TEXT, mfe_capture REAL,
    latency_signal_send_ms REAL, latency_send_ack_ms REAL, latency_signal_fill_ms REAL, fill_slippage_pips REAL,
    duration_seconds REAL, explanation TEXT, session_date TEXT, commission REAL
);
CREATE INDEX IF NOT EXISTS ix_rider_closed ON trades(closed_utc);
CREATE TABLE IF NOT EXISTS stop_changes (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ticket INTEGER, ts_utc TEXT, old_stop REAL, new_stop REAL,
    state TEXT, why TEXT
);
CREATE TABLE IF NOT EXISTS scans (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, symbol TEXT, direction TEXT, score REAL, state TEXT,
    reason TEXT, family TEXT, chop REAL, cost_ratio REAL
);
CREATE INDEX IF NOT EXISTS ix_rider_scans ON scans(ts_utc);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, kind TEXT, message TEXT, detail_json TEXT
);
"""


def _iso(x: Optional[dt.datetime]) -> Optional[str]:
    return to_utc(x).isoformat() if x is not None else None


def _json(x) -> str:
    def default(o):
        if isinstance(o, dt.datetime):
            return o.isoformat()
        if isinstance(o, float) and o != o:
            return None
        return str(o)
    return json.dumps(x, default=default, separators=(",", ":"))


class RiderJournal:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self.db = sqlite3.connect(str(self.path), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self.db.commit()
        self.last_write_ok = True
        self.last_error = ""

    def _exec(self, sql: str, args: tuple = ()) -> None:
        with self._lock:
            try:
                self.db.execute(sql, args)
                self.db.commit()
                self.last_write_ok = True
            except sqlite3.Error as exc:
                self.last_write_ok = False
                self.last_error = str(exc)
                raise

    def close(self) -> None:
        with self._lock:
            try:
                self.db.close()
            except Exception:
                pass

    # ------------------------------------------------------------- trades --
    def insert_trade(self, row: dict) -> None:
        cols = list(row)
        self._exec(f"INSERT INTO trades ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
                   tuple(row[c] for c in cols))

    def update_trade(self, ticket: int, fields: dict) -> None:
        if not fields:
            return
        sets = ", ".join(f"{k}=?" for k in fields)
        self._exec(f"UPDATE trades SET {sets} WHERE ticket=?", tuple(fields.values()) + (int(ticket),))

    def trade(self, ticket: int) -> Optional[dict]:
        with self._lock:
            r = self.db.execute("SELECT * FROM trades WHERE ticket=?", (int(ticket),)).fetchone()
        return dict(r) if r else None

    def open_rows(self) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self.db.execute(
                "SELECT * FROM trades WHERE closed_utc IS NULL ORDER BY opened_utc")]

    def closed_since(self, since: dt.datetime, mode: Optional[str] = None) -> list[dict]:
        q = "SELECT * FROM trades WHERE closed_utc IS NOT NULL AND closed_utc >= ?"
        args: tuple = (_iso(since),)
        if mode:
            q += " AND mode = ?"
            args += (mode,)
        with self._lock:
            return [dict(r) for r in self.db.execute(q + " ORDER BY closed_utc", args)]

    def recent_closed(self, n: int = 10) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self.db.execute(
                "SELECT * FROM trades WHERE closed_utc IS NOT NULL ORDER BY closed_utc DESC LIMIT ?", (int(n),))]

    def all_closed(self, limit: int = 5000) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self.db.execute(
                "SELECT * FROM trades WHERE closed_utc IS NOT NULL ORDER BY closed_utc DESC LIMIT ?", (int(limit),))]

    def estimates_since(self, since: dt.datetime) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self.db.execute(
                "SELECT * FROM trades WHERE mode='LIVE' AND money_source='estimate' AND closed_utc >= ?",
                (_iso(since),))]

    def max_ticket(self, default: int) -> int:
        with self._lock:
            r = self.db.execute("SELECT MAX(ticket) FROM trades").fetchone()
        return int(r[0]) if r and r[0] is not None else int(default)

    # -------------------------------------------------------------- other --
    def record_stop_change(self, ticket: int, old: float, new: float, state: str, why: str,
                           now: Optional[dt.datetime] = None) -> None:
        self._exec("INSERT INTO stop_changes (ticket, ts_utc, old_stop, new_stop, state, why) VALUES (?,?,?,?,?,?)",
                   (int(ticket), _iso(now or utcnow()), float(old), float(new), state, why))

    def stop_changes(self, ticket: int) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self.db.execute(
                "SELECT * FROM stop_changes WHERE ticket=? ORDER BY id", (int(ticket),))]

    def record_scan(self, rows: Iterable, now: dt.datetime) -> None:
        ts = _iso(now)
        with self._lock:
            try:
                self.db.executemany(
                    "INSERT INTO scans (ts_utc, symbol, direction, score, state, reason, family, chop, cost_ratio) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    [(ts, r.symbol, r.direction, float(r.score), r.state, r.reason, r.family, float(r.chop),
                      float(min(r.cost_ratio, 99.0))) for r in rows])
                self.db.commit()
                self.last_write_ok = True
            except sqlite3.Error as exc:
                self.last_write_ok = False
                self.last_error = str(exc)

    def log_event(self, kind: str, message: str, detail: Optional[dict] = None,
                  now: Optional[dt.datetime] = None) -> None:
        try:
            self._exec("INSERT INTO events (ts_utc, kind, message, detail_json) VALUES (?,?,?,?)",
                       (_iso(now or utcnow()), kind, message, _json(detail or {})))
        except sqlite3.Error:
            pass

    def events(self, kind: Optional[str] = None) -> list[dict]:
        q = "SELECT * FROM events"
        args: tuple = ()
        if kind:
            q += " WHERE kind=?"
            args = (kind,)
        with self._lock:
            return [dict(r) for r in self.db.execute(q + " ORDER BY id", args)]

    def probe(self) -> bool:
        """Is the database writable right now?"""
        try:
            with self._lock:
                self.db.execute("CREATE TABLE IF NOT EXISTS probe (x INTEGER)")
                self.db.execute("DELETE FROM probe")
                self.db.execute("INSERT INTO probe (x) VALUES (1)")
                self.db.commit()
            self.last_write_ok = True
            return True
        except sqlite3.Error as exc:
            self.last_write_ok = False
            self.last_error = str(exc)
            return False


def to_json(x) -> str:
    return _json(x)
