"""The permanent record of every lab-demo trade: ``data/labdemo/labdemo.sqlite``.

``demo_trades`` holds one row per closed DEMO position, keyed by the broker's
ticket, with the idea's id and version. Its money is the BROKER's: ``net_gbp``
is MetaTrader's own closed-deal figure (profit + commission + swap, the entry
commission included: ``LiveExecutor.closed_deal``), never a simulator's;
``entry_commission_gbp`` is the entry half it added (the exit half is inside
the closing deals' figure and not split out). A
position whose deal has not come back yet stays in ``demo_open`` (marked
``closing``) until it does: nothing is ever booked on a guess.

Beside the demo money, never mixed into it, each row carries the realistic
small-account comparison Aaron asked for on 14 Oct ("realistic account-size
comparisons"): ``equiv_1500_gbp`` = the trade's R multiple x 0.5% of £1,500 =
R x £7.50, where R = the broker's net / ``risk_gbp``, the money really at its
stop (the broker's fill, the stop, the volume). ``risk_allowed_gbp`` is what its
tier allowed (equity x tier); a position is cut below it to fit the account's
risk cap or the margin, never made larger.

``demo_open`` holds the open positions with the simulator's own state (stop,
peak, trail, hold), so a restart re-adopts them exactly. Paper (the lab's
PRACTICE rows in data/lab/lab.sqlite) is never written here, and nothing here is
written there: PAPER and DEMO are never mixed.

Other readers (the weekly pick, the careers and leagues pages) open this file
read-only.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

from .roster import DB, ldir

EQUIV_ACCOUNT_GBP = 1500.0
EQUIV_RISK_SHARE = 0.005                  # 0.5% of the £1,500 account a trade
EQUIV_PER_R = EQUIV_ACCOUNT_GBP * EQUIV_RISK_SHARE      # £7.50 for each R

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS demo_trades(
    ticket INTEGER PRIMARY KEY, idea_id TEXT NOT NULL, version INT NOT NULL, magic INT NOT NULL,
    symbol TEXT, side INT, volume REAL, entry REAL, stop REAL, target REAL,
    opened_utc TEXT, closed_utc TEXT, exit_price REAL, exit_reason TEXT,
    net_gbp REAL NOT NULL, entry_commission_gbp REAL, risk_gbp REAL NOT NULL, risk_allowed_gbp REAL,
    tier_pct REAL,
    equity_at_entry REAL, r REAL, equiv_1500_gbp REAL, deal_source TEXT, plan TEXT, recorded_utc TEXT,
    mode TEXT DEFAULT 'DEMO');
CREATE INDEX IF NOT EXISTS ix_demo_trades_idea ON demo_trades(idea_id, version);
CREATE TABLE IF NOT EXISTS demo_open(
    ticket INTEGER PRIMARY KEY, idea_id TEXT NOT NULL, version INT NOT NULL, magic INT NOT NULL,
    symbol TEXT, side INT, volume REAL, entry REAL, stop REAL, target REAL, opened_utc TEXT,
    risk_gbp REAL, risk_allowed_gbp REAL, tier_pct REAL, equity_at_entry REAL, day TEXT, signal_bar_utc TEXT,
    pos_json TEXT, last_t INT, closing TEXT, exit_hint TEXT, plan TEXT);
CREATE TABLE IF NOT EXISTS demo_skips(
    ts_utc TEXT, idea_id TEXT, symbol TEXT, why TEXT);
"""


def equiv_1500(r: Optional[float]) -> Optional[float]:
    """The £1,500-account equivalent of a trade of ``r`` R: R x 0.5% x £1,500."""
    return None if r is None else round(float(r) * EQUIV_PER_R, 2)


def db_path(data_dir) -> Path:
    return ldir(data_dir) / DB


class DemoRecord:
    """The engine's own handle (read and write)."""

    def __init__(self, data_dir):
        p = db_path(data_dir)
        p.parent.mkdir(parents=True, exist_ok=True)
        self.path = p
        self.conn = sqlite3.connect(str(p), timeout=10.0)
        self.conn.row_factory = sqlite3.Row
        try:
            self.conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.DatabaseError:
            pass
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()

    def query(self, sql: str, args: tuple = ()) -> list[dict]:
        return [dict(r) for r in self.conn.execute(sql, args).fetchall()]

    def execute(self, sql: str, args: tuple = ()) -> None:
        with self.conn:
            self.conn.execute(sql, args)

    def open_rows(self) -> list[dict]:
        return self.query("SELECT * FROM demo_open ORDER BY ticket")

    def put_open(self, row: dict) -> None:
        cols = ",".join(row)
        q = ",".join("?" for _ in row)
        self.execute(f"INSERT OR REPLACE INTO demo_open({cols}) VALUES ({q})", tuple(row.values()))

    def update_open(self, ticket: int, **fields) -> None:
        if not fields:
            return
        sets = ",".join(f"{k}=?" for k in fields)
        self.execute(f"UPDATE demo_open SET {sets} WHERE ticket=?", tuple(fields.values()) + (int(ticket),))

    def book(self, trade: dict) -> None:
        """One closed trade into demo_trades and its open row gone, in one transaction."""
        cols = ",".join(trade)
        q = ",".join("?" for _ in trade)
        with self.conn:
            self.conn.execute(f"INSERT OR REPLACE INTO demo_trades({cols}) VALUES ({q})", tuple(trade.values()))
            self.conn.execute("DELETE FROM demo_open WHERE ticket=?", (int(trade["ticket"]),))

    def skip(self, ts: str, idea_id: str, symbol: str, why: str) -> None:
        with self.conn:
            self.conn.execute("INSERT INTO demo_skips VALUES (?,?,?,?)", (ts, idea_id, symbol, why))
            self.conn.execute("DELETE FROM demo_skips WHERE rowid IN (SELECT rowid FROM demo_skips ORDER BY rowid "
                              "DESC LIMIT -1 OFFSET 5000)")

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


def read_trades(data_dir, idea_id: Optional[str] = None, since_utc: Optional[str] = None) -> Optional[list[dict]]:
    """The closed demo trades (read-only), oldest first; None when the file is there
    but cannot be read (a reader must not take that for "no trades")."""
    p = db_path(data_dir)
    if not p.exists():
        return []
    try:
        con = sqlite3.connect(f"file:{p}?mode=ro", uri=True, timeout=5.0)
        con.row_factory = sqlite3.Row
        try:
            sql, args = "SELECT * FROM demo_trades WHERE 1=1", []
            if idea_id:
                sql += " AND idea_id=?"
                args.append(idea_id)
            if since_utc:
                sql += " AND opened_utc>=?"
                args.append(since_utc)
            return [dict(r) for r in con.execute(sql + " ORDER BY closed_utc, ticket", tuple(args)).fetchall()]
        finally:
            con.close()
    except sqlite3.Error:
        return None
