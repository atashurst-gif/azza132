"""Research trial ledger.

The purpose is to make backtest self-deception *arithmetically visible*.

Every meaningful parameter or model experiment is recorded here - including the
ones that failed.  Reporting the best of 300 silently-discarded variants as
though it were a single honest test is the most common way an automated trading
system convinces its owner of an edge that does not exist.  With the ledger, the
number of trials is known, so the Probability of Backtest Overfitting and the
deflated Sharpe ratio can be computed against the *real* search effort.

A report that omits the trial count is refused by
:meth:`TrialLedger.honest_report`.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import sqlite3
import threading
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Optional, Sequence

from ..clock import utcnow

SCHEMA = """
CREATE TABLE IF NOT EXISTS trials (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_utc TEXT NOT NULL,
    study TEXT NOT NULL,
    param_hash TEXT NOT NULL,
    params_json TEXT NOT NULL,
    dataset TEXT,
    period_start TEXT,
    period_end TEXT,
    trades INTEGER,
    sharpe REAL,
    expectancy_r REAL,
    profit_factor REAL,
    max_drawdown_pct REAL,
    net_money REAL,
    is_oos INTEGER DEFAULT 0,
    notes TEXT
);
CREATE INDEX IF NOT EXISTS ix_trials_study ON trials(study);
"""


@dataclass
class Trial:
    study: str
    params: dict
    dataset: str = ""
    period_start: str = ""
    period_end: str = ""
    trades: int = 0
    sharpe: Optional[float] = None
    expectancy_r: Optional[float] = None
    profit_factor: Optional[float] = None
    max_drawdown_pct: Optional[float] = None
    net_money: Optional[float] = None
    is_oos: bool = False
    notes: str = ""

    @property
    def param_hash(self) -> str:
        blob = json.dumps(self.params, sort_keys=True)
        return hashlib.sha256(blob.encode()).hexdigest()[:16]


class TrialLedger:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def record(self, trial: Trial) -> int:
        with self._lock:
            cur = self._conn.execute("""
                INSERT INTO trials (ts_utc, study, param_hash, params_json,
                    dataset, period_start, period_end, trades, sharpe,
                    expectancy_r, profit_factor, max_drawdown_pct, net_money,
                    is_oos, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (utcnow().isoformat(), trial.study, trial.param_hash,
                  json.dumps(trial.params, sort_keys=True), trial.dataset,
                  trial.period_start, trial.period_end, trial.trades,
                  trial.sharpe, trial.expectancy_r, trial.profit_factor,
                  trial.max_drawdown_pct, trial.net_money,
                  1 if trial.is_oos else 0, trial.notes))
            self._conn.commit()
            return int(cur.lastrowid)

    def count(self, study: str = "") -> int:
        sql = "SELECT COUNT(*) FROM trials"
        args: list = []
        if study:
            sql += " WHERE study = ?"
            args.append(study)
        with self._lock:
            return int(self._conn.execute(sql, args).fetchone()[0])

    def all(self, study: str = "") -> list[dict]:
        sql = "SELECT * FROM trials"
        args: list = []
        if study:
            sql += " WHERE study = ?"
            args.append(study)
        sql += " ORDER BY id"
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql, args).fetchall()]

    def sharpes(self, study: str = "") -> list[float]:
        return [float(r["sharpe"]) for r in self.all(study)
                if r["sharpe"] is not None]

    def best(self, study: str = "", metric: str = "sharpe") -> Optional[dict]:
        rows = [r for r in self.all(study) if r.get(metric) is not None]
        return max(rows, key=lambda r: r[metric]) if rows else None

    def honest_report(self, study: str = "") -> dict:
        """A results summary that cannot hide the search effort.

        Always carries the number of trials and an explicit warning when the
        headline figure is the maximum of many.
        """
        rows = self.all(study)
        n = len(rows)
        best = self.best(study)
        sharpes = self.sharpes(study)
        report = {
            "study": study or "ALL",
            "trials_run": n,
            "trials_with_sharpe": len(sharpes),
            "best": best,
            "median_sharpe": (sorted(sharpes)[len(sharpes) // 2]
                              if sharpes else None),
            "oos_trials": sum(1 for r in rows if r["is_oos"]),
        }
        if n > 1 and best is not None:
            report["warning"] = (
                f"the headline result is the best of {n} trials; judge it "
                f"against the deflated Sharpe ratio and the probability of "
                f"backtest overfitting, not on its own")
        if report["oos_trials"] == 0 and n > 0:
            report["warning_oos"] = (
                "no out-of-sample trial has been recorded for this study, so "
                "nothing here should be treated as validated")
        return report

    def close(self) -> None:
        with self._lock:
            self._conn.close()
