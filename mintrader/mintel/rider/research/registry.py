"""The RESEARCH REGISTRY: every variation tested, and every time it was run.

    <research dir>/registry.sqlite

``methods``  one row per METHOD_ID: DESCRIPTION, HYPOTHESIS, the settings.
``runs``     one row per run of a method: MARKETS, PERIOD, TRADES, WIN RATE,
             EXPECTANCY (net pips a trade after every cost), PROFIT FACTOR,
             DRAWDOWN (pips), MFE CAPTURE, COST SENSITIVITY,
             OUT-OF-SAMPLE RESULT - and where the data came from (REAL or
             SYNTHETIC), so a synthetic check can never pass for a result.
``registry`` a view with exactly the brief's column names.

Being in the registry is not a recommendation: most variants are tested to
be rejected. Nothing here is deployed automatically.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path
from typing import Optional, Sequence

SCHEMA = """
CREATE TABLE IF NOT EXISTS methods (
    method_id TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    hypothesis TEXT NOT NULL,
    params_json TEXT NOT NULL DEFAULT '{}',
    first_seen_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_utc TEXT NOT NULL,
    method_id TEXT NOT NULL REFERENCES methods(method_id),
    data_source TEXT NOT NULL,
    markets TEXT NOT NULL,
    period TEXT NOT NULL,
    trades INTEGER NOT NULL,
    win_rate REAL,
    expectancy REAL,
    profit_factor REAL,
    drawdown REAL,
    mfe_capture REAL,
    cost_sensitivity TEXT,
    oos_result TEXT,
    net_pips REAL,
    net_money REAL,
    fragile INTEGER NOT NULL DEFAULT 0,
    report_dir TEXT,
    code_stamp TEXT,
    notes TEXT
);
CREATE INDEX IF NOT EXISTS runs_method ON runs(method_id, run_utc);
CREATE VIEW IF NOT EXISTS registry AS
SELECT r.id AS RUN_ID, r.run_utc AS RUN_UTC, r.data_source AS DATA_SOURCE,
       m.method_id AS METHOD_ID, m.description AS DESCRIPTION, m.hypothesis AS HYPOTHESIS,
       r.markets AS MARKETS, r.period AS PERIOD, r.trades AS TRADES, r.win_rate AS "WIN RATE",
       r.expectancy AS EXPECTANCY, r.profit_factor AS "PROFIT FACTOR", r.drawdown AS DRAWDOWN,
       r.mfe_capture AS "MFE CAPTURE", r.cost_sensitivity AS "COST SENSITIVITY",
       r.oos_result AS "OUT-OF-SAMPLE RESULT"
FROM runs r JOIN methods m ON m.method_id = r.method_id;
"""

COLUMNS = ("METHOD_ID", "DESCRIPTION", "HYPOTHESIS", "MARKETS", "PERIOD", "TRADES", "WIN RATE", "EXPECTANCY",
           "PROFIT FACTOR", "DRAWDOWN", "MFE CAPTURE", "COST SENSITIVITY", "OUT-OF-SAMPLE RESULT")


class Registry:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(self.path))
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    def seed(self, methods: Sequence[dict]) -> int:
        """Upsert methods: {method_id, description, hypothesis, params}."""
        now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
        n = 0
        for m in methods:
            self.db.execute(
                "INSERT INTO methods(method_id, description, hypothesis, params_json, first_seen_utc) "
                "VALUES(?,?,?,?,?) ON CONFLICT(method_id) DO UPDATE SET description=excluded.description, "
                "hypothesis=excluded.hypothesis, params_json=excluded.params_json",
                (m["method_id"], m["description"], m["hypothesis"], json.dumps(m.get("params", {}), sort_keys=True,
                                                                                default=str), now))
            n += 1
        self.db.commit()
        return n

    def record(self, run: dict) -> int:
        """One run of one method. Unknown method ids are refused."""
        if self.db.execute("SELECT 1 FROM methods WHERE method_id=?", (run["method_id"],)).fetchone() is None:
            raise KeyError(f"unknown method {run['method_id']!r}: seed it first")
        cols = ("run_utc", "method_id", "data_source", "markets", "period", "trades", "win_rate", "expectancy",
                "profit_factor", "drawdown", "mfe_capture", "cost_sensitivity", "oos_result", "net_pips",
                "net_money", "fragile", "report_dir", "code_stamp", "notes")
        row = dict(run)
        row.setdefault("run_utc", dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat())
        row["fragile"] = 1 if row.get("fragile") else 0
        cur = self.db.execute(f"INSERT INTO runs({', '.join(cols)}) VALUES({', '.join('?' for _ in cols)})",
                              tuple(row.get(c) for c in cols))
        self.db.commit()
        return int(cur.lastrowid)

    def rows(self, method_id: Optional[str] = None, data_source: Optional[str] = None) -> list[dict]:
        q = "SELECT * FROM registry"
        where, args = [], []
        if method_id:
            where.append("METHOD_ID = ?")
            args.append(method_id)
        if data_source:
            where.append("DATA_SOURCE = ?")
            args.append(data_source)
        if where:
            q += " WHERE " + " AND ".join(where)
        q += " ORDER BY RUN_ID"
        return [dict(r) for r in self.db.execute(q, args)]

    def methods(self) -> list[dict]:
        return [dict(r) for r in self.db.execute("SELECT * FROM methods ORDER BY method_id")]

    def run_count(self) -> int:
        return int(self.db.execute("SELECT COUNT(*) FROM runs").fetchone()[0])
