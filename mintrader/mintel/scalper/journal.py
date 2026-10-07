"""The Rapid Scalper's own ledger: data/scalper.sqlite. Every trade with
every number that explains it, every rejected opportunity, every stop
change, every fill's slippage."""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
import threading
from pathlib import Path
from typing import Optional, Sequence

from ..clock import to_utc, utcnow

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    ticket INTEGER PRIMARY KEY, strategy_id TEXT, mode TEXT, symbol TEXT, side TEXT,
    volume REAL, opened_utc TEXT, closed_utc TEXT,
    entry_requested REAL, entry_filled REAL, exit_requested REAL, exit_filled REAL,
    planned_loss REAL, initial_stop REAL, final_stop REAL,
    spread_at_entry REAL, spread_at_exit REAL, volatility_points REAL,
    confidence REAL, components_json TEXT, entry_reason TEXT, context_json TEXT,
    peak_profit REAL, peak_r REAL, mae_r REAL, final_state TEXT,
    stop_changes_json TEXT, runner_json TEXT, max_runner_probability REAL,
    exit_reason TEXT, exit_detail TEXT, duration_seconds REAL,
    gross_pnl REAL, commission REAL, spread_cost REAL, slippage_cost REAL, net_pnl REAL,
    entry_slippage_points REAL, exit_slippage_points REAL, entry_latency_ms REAL, exit_latency_ms REAL,
    reached_protected INTEGER, reached_runner INTEGER
);
CREATE INDEX IF NOT EXISTS ix_rs_closed ON trades(closed_utc);
CREATE TABLE IF NOT EXISTS rejected (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, symbol TEXT, direction INTEGER,
    confidence REAL, components_json TEXT, blockers_json TEXT, spread_points REAL
);
CREATE TABLE IF NOT EXISTS stop_changes (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ticket INTEGER, ts_utc TEXT,
    old_stop REAL, new_stop REAL, state TEXT, why TEXT
);
CREATE TABLE IF NOT EXISTS slippage (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, ticket INTEGER, kind TEXT,
    requested REAL, filled REAL, points REAL, latency_ms REAL, spread_points REAL
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, kind TEXT, message TEXT, detail_json TEXT
);
"""


# Version 3 (the rider) records more about every trade; older files gain the
# columns on first open, so nothing is lost or rewritten.
V3_COLUMNS = (("setup", "TEXT"), ("session", "TEXT"), ("momentum_at_entry", "REAL"),
              ("velocity_at_entry", "REAL"), ("acceleration_at_entry", "REAL"),
              ("protected_floor", "REAL"), ("trailing_distance_points", "REAL"),
              ("surrendered_from_peak", "REAL"), ("state_reason", "TEXT"), ("scalpability", "REAL"))


class ScalperJournal:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()
            have = {r[1] for r in self._conn.execute("PRAGMA table_info(trades)")}
            for col, typ in V3_COLUMNS:
                if col not in have:
                    self._conn.execute(f"ALTER TABLE trades ADD COLUMN {col} {typ}")
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass

    def _exec(self, sql: str, args: Sequence = ()) -> sqlite3.Cursor:
        with self._lock:
            cur = self._conn.execute(sql, args)
            self._conn.commit()
            return cur

    # ---------------------------------------------------------------- trades --
    def open_trade(self, trade, mode: str, spread_points: float, vol_points: float,
                   context: dict, entry_slip: float, entry_latency: float) -> None:
        self._exec("""INSERT OR REPLACE INTO trades (ticket, strategy_id, mode, symbol, side, volume,
            opened_utc, entry_requested, entry_filled, planned_loss, initial_stop, final_stop,
            spread_at_entry, volatility_points, confidence, components_json, entry_reason, context_json,
            entry_slippage_points, entry_latency_ms, peak_profit, peak_r, mae_r, final_state,
            reached_protected, reached_runner)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0,?,0,0)""",
                   (trade.ticket, "rapid_scalper", mode, trade.symbol, trade.side.value, trade.volume,
                    to_utc(trade.opened_at).isoformat(), trade.entry_requested, trade.entry_filled,
                    trade.planned_loss, trade.initial_stop, trade.stop, spread_points, vol_points,
                    trade.confidence, json.dumps(trade.components), trade.entry_reason,
                    json.dumps(context, default=str), entry_slip, entry_latency, trade.state))

    def update_trade(self, trade) -> None:
        self._exec("""UPDATE trades SET final_stop=?, peak_profit=?, peak_r=?, mae_r=?, final_state=?,
            stop_changes_json=?, runner_json=?, max_runner_probability=?,
            reached_protected=?, reached_runner=?, setup=?, session=?, momentum_at_entry=?,
            velocity_at_entry=?, acceleration_at_entry=?, protected_floor=?, trailing_distance_points=?,
            state_reason=? WHERE ticket=?""",
                   (trade.stop, trade.high_water_money, trade.peak_r, trade.mae_r, trade.state,
                    json.dumps(trade.stop_changes), json.dumps(trade.runner_history[-120:]),
                    max([p for _, p in trade.runner_history], default=0.0),
                    1 if trade.state in ("PROFIT_PROTECTED", "RUNNER", "PROFIT_PROTECTION", "MOMENTUM_RIDE", "MAXIMUM_RIDE") else 0,
                    1 if trade.state in ("RUNNER", "MOMENTUM_RIDE", "MAXIMUM_RIDE") else 0,
                    getattr(trade, "setup", ""), getattr(trade, "session", ""), getattr(trade, "momentum_at_entry", 0.0),
                    getattr(trade, "velocity_at_entry", 0.0), getattr(trade, "acceleration_at_entry", 0.0),
                    getattr(trade, "protected_floor_money", 0.0), getattr(trade, "trail_points", 0.0),
                    getattr(trade, "state_reason", ""), trade.ticket))

    def close_trade(self, trade, *, closed_at: dt.datetime, exit_requested: float, exit_filled: float,
                    exit_reason: str, exit_detail: str, spread_at_exit: float, gross: float,
                    commission: float, spread_cost: float, slippage_cost: float, net: float,
                    exit_slip: float, exit_latency: float) -> None:
        self.update_trade(trade)
        self._exec("""UPDATE trades SET closed_utc=?, exit_requested=?, exit_filled=?, exit_reason=?,
            exit_detail=?, spread_at_exit=?, duration_seconds=?, gross_pnl=?, commission=?, spread_cost=?,
            slippage_cost=?, net_pnl=?, exit_slippage_points=?, exit_latency_ms=? WHERE ticket=?""",
                   (to_utc(closed_at).isoformat(), exit_requested, exit_filled, exit_reason, exit_detail,
                    spread_at_exit, trade.duration(closed_at), gross, commission, spread_cost, slippage_cost,
                    net, exit_slip, exit_latency, trade.ticket))
        self._exec("UPDATE trades SET surrendered_from_peak=? WHERE ticket=?",
                   (round(max(0.0, float(trade.high_water_money) - float(net)), 2), trade.ticket))

    def rejection_counts(self, since: dt.datetime) -> dict:
        """How many rejected looks, by the first reason given, since ``since``."""
        out: dict = {}
        cur = self._exec("SELECT blockers_json FROM rejected WHERE ts_utc >= ?", (to_utc(since).isoformat(),))
        for (raw,) in cur.fetchall():
            try:
                reasons = json.loads(raw or "[]")
            except Exception:
                reasons = []
            key = (reasons[0] if reasons else "?")
            for tag, label in (("spread", "spread too wide"), ("momentum", "momentum too low"), ("scalpable", "not scalpable"),
                               ("costs", "costs too high"), ("thin", "market too thin"), ("moved", "chasing"),
                               ("stop", "stop placement"), ("news", "news blackout")):
                if tag in key:
                    key = label
                    break
            out[key] = out.get(key, 0) + 1
        return out

    def record_stop_change(self, ticket: int, old: float, new: float, state: str, why: str,
                           now: Optional[dt.datetime] = None) -> None:
        self._exec("INSERT INTO stop_changes (ticket, ts_utc, old_stop, new_stop, state, why) VALUES (?,?,?,?,?,?)",
                   (ticket, to_utc(now or utcnow()).isoformat(), old, new, state, why))

    def record_rejected(self, opp, spread_points: float, now: Optional[dt.datetime] = None,
                        every_seconds: float = 0.0) -> None:
        """One row per market per set of reasons per ``every_seconds``."""
        when = to_utc(now or utcnow())
        if every_seconds > 0:
            key = (opp.symbol, tuple(opp.blockers))
            seen = getattr(self, "_rejected_seen", None)
            if seen is None:
                seen = self._rejected_seen = {}
            last = seen.get(key)
            if last is not None and (when - last).total_seconds() < every_seconds:
                return
            seen[key] = when
            if len(seen) > 5000:
                seen.clear()
        self._exec("""INSERT INTO rejected (ts_utc, symbol, direction, confidence, components_json, blockers_json, spread_points)
                      VALUES (?,?,?,?,?,?,?)""",
                   (to_utc(now or utcnow()).isoformat(), opp.symbol, opp.direction, opp.confidence,
                    json.dumps({**opp.contributions, **opp.penalties}), json.dumps(opp.blockers), spread_points))

    def record_slippage(self, ticket: int, kind: str, requested: float, filled: float, points: float,
                        latency_ms: float, spread_points: float, now: Optional[dt.datetime] = None) -> None:
        self._exec("""INSERT INTO slippage (ts_utc, ticket, kind, requested, filled, points, latency_ms, spread_points)
                      VALUES (?,?,?,?,?,?,?,?)""",
                   (to_utc(now or utcnow()).isoformat(), ticket, kind, requested, filled, points, latency_ms, spread_points))

    def log_event(self, kind: str, message: str, detail: Optional[dict] = None,
                  now: Optional[dt.datetime] = None) -> None:
        self._exec("INSERT INTO events (ts_utc, kind, message, detail_json) VALUES (?,?,?,?)",
                   (to_utc(now or utcnow()).isoformat(), kind, message, json.dumps(detail or {}, default=str)))

    # --------------------------------------------------------------- queries --
    def closed_since(self, since: dt.datetime, limit: int = 2000) -> list[dict]:
        cur = self._exec("SELECT * FROM trades WHERE closed_utc IS NOT NULL AND closed_utc >= ? "
                         "ORDER BY closed_utc ASC LIMIT ?", (to_utc(since).isoformat(), limit))
        return [dict(r) for r in cur.fetchall()]

    def open_rows(self) -> list[dict]:
        cur = self._exec("SELECT * FROM trades WHERE closed_utc IS NULL")
        return [dict(r) for r in cur.fetchall()]

    def recent_events(self, limit: int = 30) -> list[dict]:
        cur = self._exec("SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,))
        return [dict(r) for r in cur.fetchall()]

    def rejected_since(self, since: dt.datetime) -> int:
        cur = self._exec("SELECT COUNT(*) AS n FROM rejected WHERE ts_utc >= ?", (to_utc(since).isoformat(),))
        row = cur.fetchone()
        return int(row["n"]) if row else 0

    def consecutive_losses(self, since: Optional[dt.datetime] = None, mode: str = "") -> int:
        sql = "SELECT net_pnl FROM trades WHERE closed_utc IS NOT NULL"
        args: list = []
        if since is not None:
            sql += " AND closed_utc >= ?"; args.append(to_utc(since).isoformat())
        if mode:
            sql += " AND mode = ?"; args.append(mode)
        cur = self._exec(sql + " ORDER BY closed_utc DESC LIMIT 20", args)
        n = 0
        for r in cur.fetchall():
            if (r["net_pnl"] or 0) < 0:
                n += 1
            else:
                break
        return n
