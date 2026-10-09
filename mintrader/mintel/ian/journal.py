"""Financial Ian's own records: data/ian.sqlite.

* signals - every signal produced, its score, components, reasoning, what the
  bridge did with it and the latency at every hop;
* orders  - the persisted DEDUPE STORE: one row per signal id that was ever
  (about to be) sent. A second order for the same id is impossible because
  the row is claimed with an atomic INSERT before anything is sent;
* trades  - every position with the full order-book state at entry (features,
  ladder, regime, score components, chart and news context), MFE, MAE, MFE
  capture and the exit explained in plain English;
* events  - starts, feed state changes, recoveries, refusals.

Every row says its mode (PAPER / LIVE) and its data source (LIVE FEED /
REPLAY / SYNTHETIC) so a synthetic or replayed figure can never be mistaken
for a real one.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
import threading
from pathlib import Path
from typing import Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    signal_id TEXT PRIMARY KEY, ts_utc TEXT, trigger_ts TEXT, instrument TEXT, root TEXT, spot_symbol TEXT,
    direction TEXT, futures_direction INTEGER, score REAL, confidence REAL, regime TEXT, reasoning TEXT,
    intended_stop REAL, stop_distance REAL, stop_basis TEXT, components_json TEXT, status TEXT, message TEXT,
    report_json TEXT, latency_json TEXT, data_source TEXT, mode TEXT, seen INTEGER DEFAULT 1
);
CREATE INDEX IF NOT EXISTS ix_ian_signals_ts ON signals(ts_utc);
CREATE TABLE IF NOT EXISTS orders (
    signal_id TEXT PRIMARY KEY, state TEXT, created_utc TEXT, updated_utc TEXT, attempts INTEGER DEFAULT 1,
    symbol TEXT, side TEXT, volume REAL, stop REAL, ticket INTEGER, price REAL, message TEXT, mode TEXT
);
CREATE TABLE IF NOT EXISTS trades (
    ticket INTEGER PRIMARY KEY, signal_id TEXT, instrument TEXT, root TEXT, spot_symbol TEXT, side TEXT,
    volume REAL, entry REAL, initial_stop REAL, stop REAL, opened_utc TEXT, closed_utc TEXT, exit_price REAL,
    net_pnl REAL, realised_r REAL, mfe_r REAL, mae_r REAL, capture REAL, regime TEXT, score REAL,
    confidence REAL, reasoning TEXT, exit_reason TEXT, exit_explanation TEXT, book_json TEXT, latency_json TEXT,
    mode TEXT, data_source TEXT, session_date TEXT, risk_money REAL, duration_seconds REAL
);
CREATE INDEX IF NOT EXISTS ix_ian_trades_closed ON trades(closed_utc);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, kind TEXT, instrument TEXT, message TEXT
);
"""

# order states in the dedupe store
SENDING = "SENDING"          # claimed, the order is being sent now
FILLED = "FILLED"
REJECTED = "REJECTED"        # the broker (or the executor's fail-closed check) said no: nothing is open
UNKNOWN = "UNKNOWN"          # the send may or may not have reached the broker: NEVER sent again
ADOPTED = "ADOPTED"          # an UNKNOWN that turned out to be at the broker, now managed
ABSENT = "ABSENT"            # an UNKNOWN that repeated positions reads and the deal history show is not open at the
                             # broker: it no longer holds up new orders on its pair (and is still never sent again)


def _j(x) -> str:
    return json.dumps(x, default=str, separators=(",", ":"))


class Journal:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.db = sqlite3.connect(str(self.path), check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)

    # ---------------------------------------------------------- dedupe store --
    def claim(self, signal_id: str, symbol: str, side: str, volume: float, stop: float, mode: str,
              now: dt.datetime) -> tuple[bool, Optional[dict]]:
        """Atomically claim a signal id for sending. (True, None) the first time; (False, the existing
        row) for every later attempt - which must never send."""
        with self._lock:
            cur = self.db.execute(
                "INSERT OR IGNORE INTO orders (signal_id, state, created_utc, updated_utc, attempts, symbol, side, volume,"
                " stop, mode) VALUES (?,?,?,?,1,?,?,?,?,?)",
                (signal_id, SENDING, now.isoformat(), now.isoformat(), symbol, side, volume, stop, mode))
            if cur.rowcount == 1:
                return True, None
            self.db.execute("UPDATE orders SET attempts = attempts + 1, updated_utc=? WHERE signal_id=?",
                            (now.isoformat(), signal_id))
            row = self.db.execute("SELECT * FROM orders WHERE signal_id=?", (signal_id,)).fetchone()
            return False, dict(row) if row is not None else None

    def order_update(self, signal_id: str, state: str, now: dt.datetime, **fields) -> None:
        cols = ["state=?", "updated_utc=?"]
        vals: list = [state, now.isoformat()]
        for k in ("ticket", "price", "volume", "message", "stop"):
            if k in fields:
                cols.append(f"{k}=?")
                vals.append(fields[k])
        vals.append(signal_id)
        with self._lock:
            self.db.execute(f"UPDATE orders SET {', '.join(cols)} WHERE signal_id=?", vals)

    def order(self, signal_id: str) -> Optional[dict]:
        with self._lock:
            row = self.db.execute("SELECT * FROM orders WHERE signal_id=?", (signal_id,)).fetchone()
        return dict(row) if row is not None else None

    def orders_in(self, *states: str) -> list[dict]:
        q = "SELECT * FROM orders WHERE state IN (%s) ORDER BY created_utc" % ",".join("?" * len(states))
        with self._lock:
            return [dict(r) for r in self.db.execute(q, states)]

    # --------------------------------------------------------------- signals --
    def record_signal(self, sig, status: str, message: str = "", report: Optional[dict] = None, mode: str = "") -> None:
        d = sig.to_dict()
        with self._lock:
            cur = self.db.execute("UPDATE signals SET seen = seen + 1 WHERE signal_id=?", (sig.signal_id,))
            if cur.rowcount:
                if report is not None:
                    self.db.execute("UPDATE signals SET report_json=?, latency_json=? WHERE signal_id=? AND status != 'FILLED'",
                                    (_j(report), _j(d.get("latency")), sig.signal_id))
                return
            self.db.execute(
                "INSERT INTO signals (signal_id, ts_utc, trigger_ts, instrument, root, spot_symbol, direction, futures_direction,"
                " score, confidence, regime, reasoning, intended_stop, stop_distance, stop_basis, components_json, status,"
                " message, report_json, latency_json, data_source, mode) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (sig.signal_id, d["created_ts"], d["trigger_ts"], sig.instrument, sig.root, sig.spot_symbol, sig.direction,
                 sig.futures_direction, sig.score, sig.confidence, sig.regime, sig.reasoning, sig.intended_stop,
                 sig.stop_distance, sig.stop_basis, _j(d["components"]), status, message, _j(report or {}),
                 _j(d.get("latency")), sig.data_source, mode))

    def signal_update(self, signal_id: str, status: str, message: str = "", report: Optional[dict] = None,
                      latency: Optional[dict] = None) -> None:
        with self._lock:
            self.db.execute("UPDATE signals SET status=?, message=?, report_json=COALESCE(?, report_json),"
                            " latency_json=COALESCE(?, latency_json) WHERE signal_id=?",
                            (status, message, _j(report) if report is not None else None,
                             _j(latency) if latency is not None else None, signal_id))

    def signal(self, signal_id: str) -> Optional[dict]:
        with self._lock:
            row = self.db.execute("SELECT * FROM signals WHERE signal_id=?", (signal_id,)).fetchone()
        return dict(row) if row is not None else None

    def signals(self, limit: int = 100) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self.db.execute("SELECT * FROM signals ORDER BY ts_utc DESC LIMIT ?", (limit,))]

    # ---------------------------------------------------------------- trades --
    def open_trade(self, row: dict) -> None:
        cols = ("ticket", "signal_id", "instrument", "root", "spot_symbol", "side", "volume", "entry", "initial_stop",
                "stop", "opened_utc", "regime", "score", "confidence", "reasoning", "book_json", "latency_json", "mode",
                "data_source", "session_date", "risk_money")
        vals = [row.get(c) for c in cols]
        with self._lock:
            self.db.execute(f"INSERT INTO trades ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})", vals)

    def trade_stop(self, ticket: int, stop: float) -> None:
        with self._lock:
            self.db.execute("UPDATE trades SET stop=? WHERE ticket=?", (stop, ticket))

    def close_trade(self, ticket: int, **f) -> None:
        cols = ("closed_utc", "exit_price", "net_pnl", "realised_r", "mfe_r", "mae_r", "capture", "exit_reason",
                "exit_explanation", "stop", "duration_seconds")
        sets = [f"{c}=?" for c in cols if c in f]
        vals = [f[c] for c in cols if c in f] + [ticket]
        with self._lock:
            self.db.execute(f"UPDATE trades SET {', '.join(sets)} WHERE ticket=?", vals)

    def reopen_trade(self, ticket: int) -> None:
        """A trade booked as closed that the broker still holds open: its close is taken back (the real close
        is booked later with the broker's own figure)."""
        with self._lock:
            self.db.execute("UPDATE trades SET closed_utc=NULL, exit_price=NULL, net_pnl=NULL, realised_r=NULL,"
                            " mfe_r=NULL, mae_r=NULL, capture=NULL, exit_reason=NULL, exit_explanation=NULL,"
                            " duration_seconds=NULL WHERE ticket=?", (ticket,))

    def trade(self, ticket: int) -> Optional[dict]:
        with self._lock:
            row = self.db.execute("SELECT * FROM trades WHERE ticket=?", (ticket,)).fetchone()
        return dict(row) if row is not None else None

    def last_closed(self) -> Optional[dict]:
        with self._lock:
            row = self.db.execute("SELECT * FROM trades WHERE closed_utc IS NOT NULL ORDER BY closed_utc DESC LIMIT 1").fetchone()
        return dict(row) if row is not None else None

    def open_trades(self) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self.db.execute("SELECT * FROM trades WHERE closed_utc IS NULL ORDER BY opened_utc")]

    def closed_since(self, since: dt.datetime, mode: Optional[str] = None) -> list[dict]:
        q = "SELECT * FROM trades WHERE closed_utc IS NOT NULL AND closed_utc >= ?"
        args: list = [since.isoformat()]
        if mode:
            q += " AND mode=?"
            args.append(mode)
        with self._lock:
            return [dict(r) for r in self.db.execute(q + " ORDER BY closed_utc", args)]

    def max_ticket(self, default: int) -> int:
        with self._lock:
            return int(self.db.execute("SELECT COALESCE(MAX(ticket), ?) FROM trades", (default,)).fetchone()[0])

    def entries_on(self, day: str, root: str) -> int:
        with self._lock:
            return int(self.db.execute("SELECT COUNT(*) FROM trades WHERE session_date=? AND root=?", (day, root)).fetchone()[0])

    # ---------------------------------------------------------------- events --
    def event(self, kind: str, message: str, instrument: str = "", now: Optional[dt.datetime] = None) -> None:
        ts = (now or dt.datetime.now(dt.timezone.utc)).isoformat()
        with self._lock:
            self.db.execute("INSERT INTO events (ts_utc, kind, instrument, message) VALUES (?,?,?,?)",
                            (ts, kind, instrument, message[:2000]))

    def events(self, limit: int = 50) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self.db.execute("SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,))]

    # ----------------------------------------------------------------- stats --
    def history(self, root: str, regime: str, futures_direction: int, min_trades: int = 20,
                data_source: str = "LIVE FEED") -> tuple[float, str]:
        """(score multiplier, note) from this bot's own closed trades of the same market, regime and
        direction, after costs (the broker's net in LIVE). Neutral until min_trades exist."""
        side_rows = []
        with self._lock:
            rows = [dict(r) for r in self.db.execute(
                "SELECT realised_r, side, root, regime, net_pnl FROM trades WHERE closed_utc IS NOT NULL AND root=? AND regime=?"
                " AND data_source=?", (root, regime, data_source))]
        from .mapping import futures_direction as fdir
        for r in rows:
            try:
                d = fdir(root, 1 if r["side"] == "BUY" else -1)
            except Exception:
                continue
            if d == futures_direction and r["realised_r"] is not None:
                side_rows.append(float(r["realised_r"]))
        n = len(side_rows)
        if n < min_trades:
            return 1.0, ""
        exp = sum(side_rows) / n
        mult = 1.0 + max(-0.3, min(0.2, exp))
        return mult, f"History: {n} such trades, {exp:+.2f} R each after costs."

    def stats(self, rows: list[dict]) -> dict:
        """Trades, win rate, net, expectancy, profit factor, average win/loss, drawdown, MFE, MAE, capture."""
        n = len(rows)
        pnl = [float(r.get("net_pnl") or 0.0) for r in rows]
        wins = [x for x in pnl if x > 0]
        losses = [x for x in pnl if x < 0]
        eq = peak = dd = 0.0
        for x in pnl:
            eq += x
            peak = max(peak, eq)
            dd = min(dd, eq - peak)
        rr = [float(r["realised_r"]) for r in rows if r.get("realised_r") is not None]
        mfe = [float(r["mfe_r"]) for r in rows if r.get("mfe_r") is not None]
        mae = [float(r["mae_r"]) for r in rows if r.get("mae_r") is not None]
        pos_mfe = sum(float(r["mfe_r"]) for r in rows if r.get("mfe_r") is not None and float(r["mfe_r"]) > 0)
        avg = lambda xs: (sum(xs) / len(xs)) if xs else None
        return {"trades": n, "wins": len(wins), "losses": len(losses),
                "win_rate": round(len(wins) / n, 3) if n else None, "net": round(sum(pnl), 2),
                "expectancy": round(sum(pnl) / n, 2) if n else None,
                "expectancy_r": round(avg(rr), 3) if rr else None,
                "profit_factor": round(sum(wins) / -sum(losses), 2) if losses else (None if not wins else float("inf")),
                "avg_win": round(avg(wins), 2) if wins else None, "avg_loss": round(avg(losses), 2) if losses else None,
                "max_drawdown": round(dd, 2), "mfe_r": round(avg(mfe), 3) if mfe else None,
                "mae_r": round(avg(mae), 3) if mae else None,
                "mfe_capture": round(sum(rr) / pos_mfe, 3) if pos_mfe > 0 and rr else None}

    def close(self) -> None:
        try:
            self.db.close()
        except Exception:
            pass
