"""Trade journal and decision archive.

Everything needed to reproduce a decision later is stored, not a summary of it:
the complete evidence vector at entry, the regime, the session, the news state,
execution conditions, the FlowLock state history, and MFE/MAE/capture at exit.

That archive serves three consumers:

* the operator, through the plain-English results page;
* the calibrator, which learns what each score band actually delivers;
* the historical-match scorer, which tells the engine whether this *kind* of
  setup has worked before.

SQLite with WAL is used deliberately: it survives a hard kill, needs no server
on the VPS, and can be copied off for offline research.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import sqlite3
import threading
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from ..broker.base import Position, Side
from ..clock import UTC, to_utc, utcnow
from .evidence import MarketState
from .execution import Intent, IntentState
from .flowlock import FlowState, TradeTracker

log = logging.getLogger("mintel.journal")

SCHEMA = """
CREATE TABLE IF NOT EXISTS intents (
    key TEXT PRIMARY KEY, symbol TEXT, side TEXT, volume REAL, sl REAL,
    tp REAL, created_utc TEXT, state TEXT, attempts INTEGER, ticket INTEGER,
    deal INTEGER, filled_volume REAL, fill_price REAL, last_retcode INTEGER,
    last_comment TEXT, opportunity REAL, tactic TEXT, updated_utc TEXT
);
CREATE INDEX IF NOT EXISTS ix_intents_state ON intents(state);

CREATE TABLE IF NOT EXISTS trades (
    ticket INTEGER PRIMARY KEY, intent_key TEXT, symbol TEXT, side TEXT,
    volume REAL, entry REAL, stop REAL, target REAL,
    opened_utc TEXT, closed_utc TEXT, exit_price REAL,
    pnl_money REAL, pnl_pips REAL, realised_r REAL,
    mfe REAL, mae REAL, mfe_r REAL, mae_r REAL, mfe_capture_pct REAL,
    exit_reason TEXT, final_flow_state TEXT,
    regime TEXT, tactic TEXT, opportunity REAL, raw_score REAL, tier TEXT,
    risk_pct REAL, risk_money REAL, session TEXT, news_state TEXT,
    spread_pips REAL, cost_pips REAL, reward_risk REAL,
    entry_state_json TEXT, exit_state_json TEXT, flow_json TEXT,
    account_currency TEXT, equity_at_entry REAL, aggression TEXT,
    mode TEXT, learned TEXT
);
CREATE INDEX IF NOT EXISTS ix_trades_closed ON trades(closed_utc);
CREATE INDEX IF NOT EXISTS ix_trades_symbol ON trades(symbol);
CREATE INDEX IF NOT EXISTS ix_trades_match ON trades(symbol, regime, tactic, side);

CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT, as_of_utc TEXT, symbol TEXT,
    direction INTEGER, tactic TEXT, regime TEXT, opportunity REAL,
    tier TEXT, acted INTEGER, blockers TEXT, state_json TEXT
);
CREATE INDEX IF NOT EXISTS ix_decisions_time ON decisions(as_of_utc);

CREATE TABLE IF NOT EXISTS flow_state (
    ticket INTEGER PRIMARY KEY, tracker_json TEXT, updated_utc TEXT
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, kind TEXT,
    severity TEXT, message TEXT, detail_json TEXT
);
CREATE INDEX IF NOT EXISTS ix_events_time ON events(ts_utc);

CREATE TABLE IF NOT EXISTS equity (
    ts_utc TEXT PRIMARY KEY, balance REAL, equity REAL, open_positions INTEGER,
    open_risk_pct REAL
);
"""


class Journal:
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
                                         timeout=20.0)
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
        try:
            with self._lock:
                if self._conn is None:
                    return False
                self._conn.execute("SELECT 1 FROM trades LIMIT 1").fetchone()
            return True
        except Exception:
            return False

    def reconnect(self) -> bool:
        """Recover from a dropped or corrupted connection.

        Called by the health supervisor; the trader stays up while this happens
        and simply loses journalling for a cycle.
        """
        try:
            self.close()
        except Exception:
            pass
        try:
            self.connect()
            return True
        except Exception as exc:
            log.error("journal reconnect failed: %s", exc)
            return False

    def _exec(self, sql: str, args: Sequence = ()) -> Optional[sqlite3.Cursor]:
        with self._lock:
            if self._conn is None and not self.reconnect():
                return None
            try:
                cur = self._conn.execute(sql, args)
                self._conn.commit()
                return cur
            except sqlite3.Error as exc:
                log.warning("journal write failed (%s); reconnecting", exc)
                if self.reconnect():
                    try:
                        cur = self._conn.execute(sql, args)
                        self._conn.commit()
                        return cur
                    except sqlite3.Error:
                        return None
                return None

    # -------------------------------------------------------------- intents --
    def save_intent(self, intent: Intent) -> None:
        self._exec("""
            INSERT INTO intents (key, symbol, side, volume, sl, tp,
                created_utc, state, attempts, ticket, deal, filled_volume,
                fill_price, last_retcode, last_comment, opportunity, tactic,
                updated_utc)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(key) DO UPDATE SET
                state=excluded.state, attempts=excluded.attempts,
                ticket=excluded.ticket, deal=excluded.deal,
                filled_volume=excluded.filled_volume,
                fill_price=excluded.fill_price,
                last_retcode=excluded.last_retcode,
                last_comment=excluded.last_comment,
                updated_utc=excluded.updated_utc
        """, (intent.key, intent.symbol, intent.side.value, intent.volume,
              intent.sl, intent.tp, to_utc(intent.created_utc).isoformat(),
              intent.state.value, intent.attempts, intent.ticket, intent.deal,
              intent.filled_volume, intent.fill_price, intent.last_retcode,
              intent.last_comment, intent.opportunity, intent.tactic,
              utcnow().isoformat()))

    def find_intent(self, key: str) -> Optional[Intent]:
        cur = self._exec("SELECT * FROM intents WHERE key = ?", (key,))
        row = cur.fetchone() if cur else None
        return _row_to_intent(row) if row else None

    def unresolved_intents(self) -> list[Intent]:
        """Intents that were sent but never confirmed - the recovery worklist."""
        cur = self._exec(
            "SELECT * FROM intents WHERE state IN ('UNKNOWN','SENT','CREATED')")
        return [_row_to_intent(r) for r in (cur.fetchall() if cur else [])]

    # --------------------------------------------------------------- trades --
    def open_trade(self, position: Position, state: MarketState,
                   intent_key: str, risk_pct: float, risk_money: float,
                   equity: float, currency: str, aggression: str,
                   mode: str) -> None:
        self._exec("""
            INSERT INTO trades (ticket, intent_key, symbol, side, volume,
                entry, stop, target, opened_utc, regime, tactic, opportunity,
                raw_score, tier, risk_pct, risk_money, session, news_state,
                spread_pips, cost_pips, reward_risk, entry_state_json,
                account_currency, equity_at_entry, aggression, mode)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(ticket) DO NOTHING
        """, (position.ticket, intent_key, position.symbol,
              position.side.value, position.volume, position.entry_price,
              state.stop, state.target or 0.0,
              to_utc(position.open_time).isoformat(), state.regime.value,
              state.tactic, state.opportunity, state.raw_score, state.tier,
              risk_pct, risk_money,
              ",".join(_sessions_from(state)), _news_from(state),
              _spread_from(state), state.cost_pips, state.reward_risk or 0.0,
              json.dumps(state.to_row()), currency, equity, aggression, mode))

    def close_trade(self, ticket: int, *, exit_price: float,
                    closed_utc: dt.datetime, pnl_money: float,
                    pnl_pips: float, exit_reason: str,
                    stats: dict, tracker: Optional[TradeTracker] = None,
                    exit_state: Optional[MarketState] = None,
                    learned: str = "") -> None:
        self._exec("""
            UPDATE trades SET closed_utc=?, exit_price=?, pnl_money=?,
                pnl_pips=?, realised_r=?, mfe=?, mae=?, mfe_r=?, mae_r=?,
                mfe_capture_pct=?, exit_reason=?, final_flow_state=?,
                exit_state_json=?, flow_json=?, learned=?
            WHERE ticket=?
        """, (to_utc(closed_utc).isoformat(), exit_price, pnl_money, pnl_pips,
              stats.get("realised_r", 0.0), stats.get("mfe", 0.0),
              stats.get("mae", 0.0), stats.get("mfe_r", 0.0),
              stats.get("mae_r", 0.0), stats.get("mfe_capture_pct", 0.0),
              exit_reason, stats.get("final_state", ""),
              json.dumps(exit_state.to_row()) if exit_state else None,
              json.dumps(tracker.to_row()) if tracker else None,
              learned, ticket))

    def closed_trades(self, limit: int = 500,
                      since: Optional[dt.datetime] = None) -> list[dict]:
        sql = "SELECT * FROM trades WHERE closed_utc IS NOT NULL"
        args: list = []
        if since is not None:
            sql += " AND closed_utc >= ?"
            args.append(to_utc(since).isoformat())
        sql += " ORDER BY closed_utc DESC LIMIT ?"
        args.append(int(limit))
        cur = self._exec(sql, args)
        return [dict(r) for r in (cur.fetchall() if cur else [])]

    def opened_since(self, since: dt.datetime) -> int:
        """How many trades were OPENED at or after ``since`` (open or closed)."""
        cur = self._exec("SELECT COUNT(*) FROM trades WHERE opened_utc >= ?",
                         (to_utc(since).isoformat(),))
        row = cur.fetchone() if cur else None
        return int(row[0]) if row else 0

    def open_trades(self) -> list[dict]:
        cur = self._exec("SELECT * FROM trades WHERE closed_utc IS NULL")
        return [dict(r) for r in (cur.fetchall() if cur else [])]

    def calibration_rows(self, limit: int = 3000) -> list[tuple[float, float]]:
        cur = self._exec("""
            SELECT opportunity, realised_r FROM trades
            WHERE closed_utc IS NOT NULL AND realised_r IS NOT NULL
            ORDER BY closed_utc DESC LIMIT ?
        """, (int(limit),))
        return [(float(r[0] or 0.0), float(r[1] or 0.0))
                for r in (cur.fetchall() if cur else [])]

    def match_stats(self, symbol: str, regime: str, tactic: str,
                    side: Side) -> dict:
        """Performance of comparable past trades, at three widths.

        Deliberately returns all three so the caller can prefer the tightest
        match that has enough samples, instead of being forced to choose
        between a precise-but-empty bucket and a broad-but-meaningless one.
        """
        out = {}
        queries = {
            "exact": ("SELECT COUNT(*), AVG(realised_r), "
                      "AVG(CASE WHEN realised_r>0 THEN 1.0 ELSE 0.0 END) "
                      "FROM trades WHERE closed_utc IS NOT NULL AND symbol=? "
                      "AND regime=? AND tactic=? AND side=?",
                      (symbol, regime, tactic, side.value)),
            "tactic_regime": ("SELECT COUNT(*), AVG(realised_r), "
                              "AVG(CASE WHEN realised_r>0 THEN 1.0 ELSE 0.0 END) "
                              "FROM trades WHERE closed_utc IS NOT NULL "
                              "AND regime=? AND tactic=?", (regime, tactic)),
            "tactic": ("SELECT COUNT(*), AVG(realised_r), "
                       "AVG(CASE WHEN realised_r>0 THEN 1.0 ELSE 0.0 END) "
                       "FROM trades WHERE closed_utc IS NOT NULL AND tactic=?",
                       (tactic,)),
        }
        for name, (sql, args) in queries.items():
            cur = self._exec(sql, args)
            row = cur.fetchone() if cur else None
            n = int(row[0] or 0) if row else 0
            out[name] = {"n": n,
                         "expectancy_r": float(row[1]) if row and row[1] is not None else None,
                         "win_rate": float(row[2]) if row and row[2] is not None else None}
        return out

    def kelly_stats(self, tactic: str, regime: str) -> Optional[tuple[int, float, float]]:
        """(samples, win_rate, payoff_ratio) for a fractional-Kelly cap."""
        cur = self._exec("""
            SELECT realised_r FROM trades
            WHERE closed_utc IS NOT NULL AND tactic=? AND regime=?
                  AND realised_r IS NOT NULL
        """, (tactic, regime))
        rs = [float(r[0]) for r in (cur.fetchall() if cur else [])]
        if len(rs) < 10:
            return None
        wins = [r for r in rs if r > 0]
        losses = [-r for r in rs if r < 0]
        if not wins or not losses:
            return None
        avg_w = sum(wins) / len(wins)
        avg_l = sum(losses) / len(losses)
        return (len(rs), len(wins) / len(rs), avg_w / max(avg_l, 1e-9))

    # ------------------------------------------------------------ decisions --
    def record_decision(self, state: MarketState, acted: bool) -> None:
        self._exec("""
            INSERT INTO decisions (as_of_utc, symbol, direction, tactic,
                regime, opportunity, tier, acted, blockers, state_json)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (to_utc(state.as_of).isoformat(), state.symbol, state.direction,
              state.tactic, state.regime.value, state.opportunity, state.tier,
              1 if acted else 0, "; ".join(state.blockers),
              json.dumps(state.to_row())))

    def prune_decisions(self, keep_days: int = 21) -> None:
        cutoff = (utcnow() - dt.timedelta(days=keep_days)).isoformat()
        self._exec("DELETE FROM decisions WHERE as_of_utc < ?", (cutoff,))

    # ----------------------------------------------------------- flow state --
    def save_tracker(self, tracker: TradeTracker) -> None:
        self._exec("""
            INSERT INTO flow_state (ticket, tracker_json, updated_utc)
            VALUES (?,?,?)
            ON CONFLICT(ticket) DO UPDATE SET
                tracker_json=excluded.tracker_json,
                updated_utc=excluded.updated_utc
        """, (tracker.ticket, json.dumps(_tracker_payload(tracker)),
              utcnow().isoformat()))

    def load_trackers(self) -> list[TradeTracker]:
        cur = self._exec("SELECT tracker_json FROM flow_state")
        out = []
        for row in (cur.fetchall() if cur else []):
            try:
                out.append(_tracker_from(json.loads(row[0])))
            except Exception:
                continue
        return out

    def drop_tracker(self, ticket: int) -> None:
        self._exec("DELETE FROM flow_state WHERE ticket = ?", (ticket,))

    # --------------------------------------------------------------- events --
    def log_event(self, kind: str, message: str, severity: str = "INFO",
                  detail: Optional[dict] = None) -> None:
        self._exec("""
            INSERT INTO events (ts_utc, kind, severity, message, detail_json)
            VALUES (?,?,?,?,?)
        """, (utcnow().isoformat(), kind, severity, message,
              json.dumps(detail) if detail else None))

    def recent_events(self, limit: int = 100,
                      min_severity: str = "") -> list[dict]:
        order = {"INFO": 0, "WARNING": 1, "ERROR": 2, "CRITICAL": 3}
        sql = "SELECT * FROM events"
        args: list = []
        if min_severity:
            allowed = [k for k, v in order.items()
                       if v >= order.get(min_severity, 0)]
            sql += f" WHERE severity IN ({','.join('?' * len(allowed))})"
            args += allowed
        sql += " ORDER BY id DESC LIMIT ?"
        args.append(int(limit))
        cur = self._exec(sql, args)
        return [dict(r) for r in (cur.fetchall() if cur else [])]

    def record_equity(self, balance: float, equity: float,
                      open_positions: int, open_risk_pct: float) -> None:
        self._exec("""
            INSERT INTO equity (ts_utc, balance, equity, open_positions,
                                open_risk_pct)
            VALUES (?,?,?,?,?)
            ON CONFLICT(ts_utc) DO NOTHING
        """, (utcnow().isoformat(), balance, equity, open_positions,
              open_risk_pct))

    def equity_curve(self, limit: int = 5000) -> list[dict]:
        cur = self._exec("SELECT * FROM equity ORDER BY ts_utc ASC LIMIT ?",
                         (int(limit),))
        return [dict(r) for r in (cur.fetchall() if cur else [])]


# --------------------------------------------------------------- helpers ------

def _sessions_from(state: MarketState) -> tuple[str, ...]:
    ev = state.evidence.get(__import__("mintel.engine.evidence",
                                       fromlist=["Family"]).Family.SESSION)
    if ev and "session:" in ev.note:
        return tuple(ev.note.split("session:")[-1].strip().split(","))
    return ()


def _news_from(state: MarketState) -> str:
    from .evidence import Family
    ev = state.evidence.get(Family.NEWS)
    return (ev.note[:200] if ev else "")


def _spread_from(state: MarketState) -> float:
    from .evidence import Family
    ev = state.evidence.get(Family.EXECUTION)
    if not ev or "spread" not in ev.note:
        return 0.0
    try:
        return float(ev.note.split("spread")[1].split("pips")[0].strip())
    except Exception:
        return 0.0


def _row_to_intent(row: sqlite3.Row) -> Intent:
    return Intent(
        key=row["key"], symbol=row["symbol"], side=Side(row["side"]),
        volume=row["volume"] or 0.0, sl=row["sl"] or 0.0, tp=row["tp"] or 0.0,
        created_utc=dt.datetime.fromisoformat(row["created_utc"]),
        state=IntentState(row["state"]), attempts=int(row["attempts"] or 0),
        ticket=int(row["ticket"] or 0), deal=int(row["deal"] or 0),
        filled_volume=row["filled_volume"] or 0.0,
        fill_price=row["fill_price"] or 0.0,
        last_retcode=int(row["last_retcode"] or 0),
        last_comment=row["last_comment"] or "",
        opportunity=row["opportunity"] or 0.0, tactic=row["tactic"] or "")


def _tracker_payload(t: TradeTracker) -> dict:
    d = t.to_row()
    d.update({"opened_utc": to_utc(t.opened_utc).isoformat(),
              "best_price": t.best_price, "worst_price": t.worst_price,
              "bars_since_extension": t.bars_since_extension,
              "partial_volume": t.partial_volume,
              "breakeven_done": t.breakeven_done})
    return d


def _tracker_from(d: dict) -> TradeTracker:
    t = TradeTracker(
        ticket=int(d["ticket"]), symbol=d["symbol"], side=Side(d["side"]),
        volume=float(d.get("volume") or 0.0), entry=float(d["entry"]), initial_stop=float(d["initial_stop"]),
        initial_risk=float(d["initial_risk"]),
        opened_utc=dt.datetime.fromisoformat(d["opened_utc"]),
        state=FlowState(d.get("state", "INITIAL")),
        stop=float(d.get("stop") or 0.0), mfe=float(d.get("mfe") or 0.0),
        mae=float(d.get("mae") or 0.0), mfe_r=float(d.get("mfe_r") or 0.0),
        mae_r=float(d.get("mae_r") or 0.0),
        best_price=float(d.get("best_price") or 0.0),
        worst_price=float(d.get("worst_price") or 0.0),
        bars_since_extension=int(d.get("bars_since_extension") or 0),
        partial_done=bool(d.get("partial_done")),
        partial_volume=float(d.get("partial_volume") or 0.0),
        thesis_strength=float(d.get("thesis_strength") or 60.0),
        updates=int(d.get("updates") or 0),
        breakeven_done=bool(d.get("breakeven_done")))
    t.state_history = tuple(d.get("state_history") or ())
    return t
