"""The lab's book of record: ``data/lab/lab.sqlite``.

This module owns the table layout (spec section 7) and every rule about
*when a row is written*, because those rules are what keep luck out:

* A ``tests`` row with status ``running`` is committed BEFORE a simulation
  starts. The count of ``tests`` rows (running, crashed and done) is N, the
  number of things tried, and it is what every "chance this is luck" figure is
  judged against. A crash cannot make a trial disappear.
* A candidate gets exactly one fresh look, ever (one allowed retry after
  TOO_FEW_TRADES, on a later stretch). ``claim_fresh`` checks that, the past
  pass, and the 30-looks cap (never more than 30, whatever it is told), and
  inserts the row, all in ONE sqlite transaction. The past pass must also have
  been through the family-end gate (``regrade_past``) and not been held back by
  it: its plateau known and not at the edge of the range tried, its family not
  fitting noise. A trigger then makes a finished fresh result final: no column
  of a done fresh row can change (the one write allowed is the trial-ledger id,
  once).
* Every finished test also goes to the shared research ``TrialLedger``
  (study ``lab:<FAMILY>``, ``is_oos`` for fresh) so the deflated Sharpe and
  the overfitting figures are computed against the real search effort.

The page never writes here. The practice process writes only its own
``practice_*`` tables, ``looks`` and ``roster`` are the nightly's.

Aaron, 10 Oct 2026: "ensure that consistency stays, luck doesn't".
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Iterable, Optional

from . import (BINNED, HOLD_BACK_REASONS, IDEA, KIND_FRESH, KIND_PAST, LEDGER_SCHEMA, PAST_TEST,
               PBO_MAX, STAGES, STRETCH_GAP_RETEST, Candidate, clamp_fresh_cap, utc_stamp)

SCHEMA = """
CREATE TABLE IF NOT EXISTS candidates(
    id TEXT PRIMARY KEY, family TEXT, version INT, def_json TEXT, name TEXT,
    stage TEXT, stage_since_utc TEXT, created_utc TEXT, parent_id TEXT,
    retest_due INT DEFAULT 0, bin_stage TEXT, bin_reason TEXT, binned_utc TEXT,
    rank_score REAL);
CREATE INDEX IF NOT EXISTS ix_candidates_stage ON candidates(stage);
CREATE INDEX IF NOT EXISTS ix_candidates_family ON candidates(family);

CREATE TABLE IF NOT EXISTS tests(
    id INTEGER PRIMARY KEY, candidate_id TEXT,
    kind TEXT CHECK(kind IN ('past','fresh')), stretch_id TEXT, cut_day TEXT,
    status TEXT CHECK(status IN ('running','done','crashed')),
    started_utc TEXT, finished_utc TEXT, seconds REAL,
    window_from TEXT, window_to TEXT, days INT, trades INT, net_gbp REAL,
    cost_gbp REAL, pf REAL, win_rate REAL, up_days INT, down_days INT,
    best_day_gbp REAL, p REAL, folds_up INT, folds_worst REAL,
    plateau TEXT CHECK(plateau IN ('ok','spike','extend','na')),
    cost15_net REAL, cost20_net REAL, dsr REAL, max_dd_r REAL, passed INT,
    reasons_json TEXT, skipped_json TEXT, synthetic INT, ledger_row_id INT,
    assumed_share REAL, gated INT DEFAULT 0,
    UNIQUE(candidate_id, kind, stretch_id));
CREATE INDEX IF NOT EXISTS ix_tests_candidate ON tests(candidate_id);

CREATE TABLE IF NOT EXISTS daily(
    test_id INT, day TEXT, net_gbp REAL, trades INT, PRIMARY KEY(test_id, day));

CREATE TABLE IF NOT EXISTS families(
    code TEXT, stretch_id TEXT, n INT, past_passed INT, pbo REAL,
    extend_grid_json TEXT, wf_ok INT, control_pass_json TEXT,
    PRIMARY KEY(code, stretch_id));

CREATE TABLE IF NOT EXISTS practice_trades(
    ticket INTEGER PRIMARY KEY, candidate_id TEXT, symbol TEXT, side INT,
    volume REAL, entry REAL, stop REAL, target REAL, opened_utc TEXT,
    closed_utc TEXT, exit_price REAL, exit_reason TEXT, gross_gbp REAL,
    commission_gbp REAL, spread_points INT, slippage_points INT, net_gbp REAL,
    risk_gbp REAL, r REAL, signal_bar_utc TEXT, tick_at_signal_json TEXT,
    fill_source TEXT, mode TEXT DEFAULT 'PRACTICE', source TEXT DEFAULT 'bridge');
CREATE TABLE IF NOT EXISTS practice_open(
    candidate_id TEXT, symbol TEXT, side INT, volume REAL, entry REAL,
    stop REAL, target REAL, peak REAL, opened_utc TEXT,
    PRIMARY KEY(candidate_id, symbol));
CREATE TABLE IF NOT EXISTS practice_days(
    candidate_id TEXT, day TEXT, net_gbp REAL, trades INT,
    PRIMARY KEY(candidate_id, day));
CREATE TABLE IF NOT EXISTS looks(
    candidate_id TEXT, look INT, due_day TEXT, done_utc TEXT, trades INT,
    net_gbp REAL, p REAL, pf REAL, up_days INT, down_days INT, cost15_net REAL,
    passed INT, PRIMARY KEY(candidate_id, look));
CREATE TABLE IF NOT EXISTS roster(
    candidate_id TEXT PRIMARY KEY, slot INT, since_utc TEXT, symbols_json TEXT);
CREATE TABLE IF NOT EXISTS events(
    id INTEGER PRIMARY KEY, ts_utc TEXT, candidate_id TEXT, kind TEXT,
    from_stage TEXT, to_stage TEXT, why TEXT, by TEXT DEFAULT 'rule');
CREATE INDEX IF NOT EXISTS ix_events_ts ON events(ts_utc);
CREATE TABLE IF NOT EXISTS runs(
    date TEXT PRIMARY KEY, started_utc TEXT, finished_utc TEXT, ok INT,
    step TEXT, tested INT, fresh INT, seconds REAL, seconds_per_candidate REAL,
    deadline_hit INT, note TEXT);

"""

# Columns added to ``tests`` after the spec's own list (migrated into older files):
#   assumed_share  the share of trades that had to assume a spread (the past test's Costs
#                  rule), so a re-grade from the ledger judges exactly what the first one did
#   gated          1 once the family-end gate (plateau, family PBO) has been applied to a
#                  PAST row by ``regrade_past``; the fresh door will not open before that
TEST_EXTRA_COLUMNS = (("assumed_share", "REAL"), ("gated", "INT DEFAULT 0"))

_SEALED_COLUMNS = ("id", "candidate_id", "kind", "stretch_id", "cut_day", "status", "started_utc",
                   "finished_utc", "seconds", "window_from", "window_to", "days", "trades",
                   "net_gbp", "cost_gbp", "pf", "win_rate", "up_days", "down_days",
                   "best_day_gbp", "p", "folds_up", "folds_worst", "plateau", "cost15_net",
                   "cost20_net", "dsr", "max_dd_r", "passed", "reasons_json", "skipped_json",
                   "synthetic", "assumed_share", "gated")

# A finished fresh look is final. Besides the older triggers that guard the headline figures,
# this one forbids changing ANY column of a done fresh row; the only write allowed is the one
# finish_test makes after the shared trial ledger answers (ledger_row_id, once, from empty).
TRIGGERS = """
CREATE TRIGGER IF NOT EXISTS fresh_result_is_final
BEFORE UPDATE OF net_gbp ON tests
WHEN OLD.kind='fresh' AND OLD.status='done'
BEGIN SELECT RAISE(ABORT,'fresh result is final'); END;

CREATE TRIGGER IF NOT EXISTS fresh_row_is_final
BEFORE UPDATE OF status, trades, pf, p, passed, cost_gbp, days, window_from,
                 window_to, win_rate, up_days, down_days ON tests
WHEN OLD.kind='fresh' AND OLD.status='done'
BEGIN SELECT RAISE(ABORT,'fresh result is final'); END;

CREATE TRIGGER IF NOT EXISTS fresh_row_is_kept
BEFORE DELETE ON tests
WHEN OLD.kind='fresh'
BEGIN SELECT RAISE(ABORT,'a fresh look is never forgotten'); END;

CREATE TRIGGER IF NOT EXISTS fresh_row_is_sealed
BEFORE UPDATE ON tests
WHEN OLD.kind='fresh' AND OLD.status='done' AND (
""" + "\n     OR ".join(f"NEW.{c} IS NOT OLD.{c}" for c in _SEALED_COLUMNS) + """
     OR (NEW.ledger_row_id IS NOT OLD.ledger_row_id AND OLD.ledger_row_id IS NOT NULL))
BEGIN SELECT RAISE(ABORT,'fresh result is final'); END;
"""

# the columns of a finished test that finish_test() writes from ``cols``
TEST_COLUMNS = ("window_from", "window_to", "days", "trades", "net_gbp",
                "cost_gbp", "pf", "win_rate", "up_days", "down_days",
                "best_day_gbp", "p", "folds_up", "folds_worst", "plateau",
                "cost15_net", "cost20_net", "dsr", "max_dd_r", "assumed_share", "passed")


class LedgerError(RuntimeError):
    """Something the ledger refuses to do."""


class FreshRefused(LedgerError):
    """A fresh look was not granted. ``code`` says why:
    NO_PAST_PASS, ALREADY_LOOKED, CAP_REACHED."""

    def __init__(self, code: str, message: str = ""):
        super().__init__(message or code)
        self.code = code


class AlreadyTested(LedgerError):
    """A finished test already exists for this candidate, kind and stretch."""


def _now(now: Optional[dt.datetime]) -> str:
    return utc_stamp(now)


class LabLedger:
    """One connection to ``lab.sqlite`` (WAL, autocommit, explicit transactions)."""

    def __init__(self, path, trials_path=None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.trials_path = Path(trials_path) if trials_path else None
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(str(self.path), check_same_thread=False,
                                    timeout=30.0, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)
        self._migrate()
        self.conn.executescript(TRIGGERS)
        self.conn.execute(f"PRAGMA user_version={LEDGER_SCHEMA}")
        self._trials = None

    # ------------------------------------------------------------ plumbing --
    def _migrate(self) -> None:
        """Add the columns a file made by an earlier version of this module lacks."""
        have = {r[1] for r in self.conn.execute("PRAGMA table_info(tests)").fetchall()}
        for name, decl in TEST_EXTRA_COLUMNS:
            if name not in have:
                self.conn.execute(f"ALTER TABLE tests ADD COLUMN {name} {decl}")

    def close(self) -> None:
        with self._lock:
            try:
                self.conn.close()
            finally:
                if self._trials is not None:
                    self._trials.close()
                    self._trials = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def trial_ledger(self):
        """The shared research ledger (data/research_trials.sqlite), or None."""
        if self.trials_path is None:
            return None
        if self._trials is None:
            from ..research.trial_ledger import TrialLedger
            self._trials = TrialLedger(self.trials_path)
        return self._trials

    def _tx(self):
        return _Tx(self)

    def query(self, sql: str, args: Iterable = ()) -> list:
        with self._lock:
            return list(self.conn.execute(sql, tuple(args)).fetchall())

    def one(self, sql: str, args: Iterable = ()):
        with self._lock:
            return self.conn.execute(sql, tuple(args)).fetchone()

    def execute(self, sql: str, args: Iterable = ()) -> sqlite3.Cursor:
        with self._lock:
            return self.conn.execute(sql, tuple(args))

    # ---------------------------------------------------------- candidates --
    def register(self, cand: Candidate, name: str, *, parent_id: Optional[str] = None,
                 now: Optional[dt.datetime] = None) -> bool:
        """Add a definition at stage IDEA. False when the id is already known
        (BINNED ones included: a binned id is never regenerated)."""
        stamp = _now(now)
        with self._lock:
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO candidates(id, family, version, def_json, name,"
                " stage, stage_since_utc, created_utc, parent_id, retest_due)"
                " VALUES (?,?,?,?,?,?,?,?,?,0)",
                (cand.id, cand.family, cand.version,
                 json.dumps(cand.definition(), sort_keys=True), name, IDEA, stamp,
                 stamp, parent_id))
            return cur.rowcount == 1

    def known_ids(self) -> set:
        return {r[0] for r in self.query("SELECT id FROM candidates")}

    def candidate(self, cid: str) -> Optional[dict]:
        r = self.one("SELECT * FROM candidates WHERE id=?", (cid,))
        return dict(r) if r else None

    def candidates(self, stage: Optional[str] = None, family: Optional[str] = None) -> list:
        sql, args = "SELECT * FROM candidates WHERE 1=1", []
        if stage:
            sql += " AND stage=?"
            args.append(stage)
        if family:
            sql += " AND family=?"
            args.append(family)
        return [dict(r) for r in self.query(sql + " ORDER BY id", args)]

    def stage_counts(self) -> dict:
        out = {s: 0 for s in STAGES}
        for r in self.query("SELECT stage, COUNT(*) FROM candidates GROUP BY stage"):
            out[r[0]] = r[1]
        return out

    def add_event(self, candidate_id: str, kind: str, from_stage: str = "",
                  to_stage: str = "", why: str = "", *, by: str = "rule",
                  now: Optional[dt.datetime] = None) -> int:
        with self._lock:
            cur = self.conn.execute(
                "INSERT INTO events(ts_utc, candidate_id, kind, from_stage, to_stage, why, by)"
                " VALUES (?,?,?,?,?,?,?)",
                (_now(now), candidate_id, kind, from_stage, to_stage, why, by))
            return int(cur.lastrowid)

    def move(self, cid: str, to_stage: str, why: str, *, by: str = "rule",
             bin_reason: Optional[str] = None, rank_score: Optional[float] = None,
             now: Optional[dt.datetime] = None) -> None:
        """Move a candidate to a stage and write the events row, together.
        BINNED records bin_stage (the stage it left), the reason and the date.
        Moving a BINNED candidate anywhere else is refused: the only ways back
        are ``retry_too_few`` and ``reopen``."""
        if to_stage not in STAGES:
            raise LedgerError(f"unknown stage {to_stage!r}")
        stamp = _now(now)
        with self._tx():
            row = self.conn.execute("SELECT stage FROM candidates WHERE id=?", (cid,)).fetchone()
            if row is None:
                raise LedgerError(f"unknown candidate {cid}")
            frm = row[0]
            if frm == BINNED and to_stage != BINNED:
                raise LedgerError(f"{cid} is binned; it can only come back through "
                                  f"reopen() or the one TOO_FEW_TRADES retry")
            if to_stage == BINNED:
                self.conn.execute(
                    "UPDATE candidates SET stage=?, stage_since_utc=?, bin_stage=?,"
                    " bin_reason=?, binned_utc=?, retest_due=0 WHERE id=?",
                    (BINNED, stamp, frm, bin_reason or why, stamp, cid))
            else:
                self.conn.execute(
                    "UPDATE candidates SET stage=?, stage_since_utc=? WHERE id=?",
                    (to_stage, stamp, cid))
            if rank_score is not None:
                self.conn.execute("UPDATE candidates SET rank_score=? WHERE id=?",
                                  (float(rank_score), cid))
            self.conn.execute(
                "INSERT INTO events(ts_utc, candidate_id, kind, from_stage, to_stage, why, by)"
                " VALUES (?,?,?,?,?,?,?)", (stamp, cid, "stage", frm, to_stage, why, by))

    def set_rank(self, cid: str, score: Optional[float]) -> None:
        self.execute("UPDATE candidates SET rank_score=? WHERE id=?", (score, cid))

    def reopen(self, cid: str, reason: str, *, by: str = "claude",
               now: Optional[dt.datetime] = None) -> None:
        """Claude's paper-side call: put a binned idea back at IDEA, due a
        retest on the next stretch. Logged on the events table (and /lab)."""
        stamp = _now(now)
        with self._tx():
            row = self.conn.execute("SELECT stage FROM candidates WHERE id=?", (cid,)).fetchone()
            if row is None:
                raise LedgerError(f"unknown candidate {cid}")
            self.conn.execute(
                "UPDATE candidates SET stage=?, stage_since_utc=?, retest_due=1 WHERE id=?",
                (IDEA, stamp, cid))
            self.conn.execute(
                "INSERT INTO events(ts_utc, candidate_id, kind, from_stage, to_stage, why, by)"
                " VALUES (?,?,?,?,?,?,?)", (stamp, cid, "reopen", row[0], IDEA, reason, by))

    def mark_retest_due(self) -> int:
        """A new stretch: every candidate still waiting at PAST TEST (passed the
        past, not yet looked at fresh) is due a past retest on the new cut."""
        with self._lock:
            cur = self.conn.execute(
                "UPDATE candidates SET retest_due=1 WHERE stage=?", (PAST_TEST,))
            return cur.rowcount

    def clear_retest_due(self, cid: str) -> None:
        self.execute("UPDATE candidates SET retest_due=0 WHERE id=?", (cid,))

    def retest_due_ids(self) -> list:
        return [r[0] for r in self.query(
            "SELECT id FROM candidates WHERE retest_due=1 AND stage!=? ORDER BY id", (BINNED,))]

    def retry_too_few(self, cid: str, *, now: Optional[dt.datetime] = None) -> None:
        """The one way back for a TOO_FEW_TRADES fresh look: a BINNED (or waiting)
        candidate goes back to PAST TEST, due a past retest on the new stretch."""
        stamp = _now(now)
        with self._tx():
            row = self.conn.execute("SELECT stage FROM candidates WHERE id=?", (cid,)).fetchone()
            if row is None:
                raise LedgerError(f"unknown candidate {cid}")
            self.conn.execute(
                "UPDATE candidates SET stage=?, stage_since_utc=?, retest_due=1 WHERE id=?",
                (PAST_TEST, stamp, cid))
            self.conn.execute(
                "INSERT INTO events(ts_utc, candidate_id, kind, from_stage, to_stage, why, by)"
                " VALUES (?,?,?,?,?,?,?)",
                (stamp, cid, "retry", row[0], PAST_TEST,
                 "too few trades on its one fresh look; one retry on this stretch", "rule"))

    # --------------------------------------------------------------- tests --
    def begin_test(self, cid: str, kind: str, stretch_id: str, cut_day: str, *,
                   now: Optional[dt.datetime] = None) -> int:
        """Commit a ``running`` row, then return its id. Call this BEFORE the
        simulation starts. A crashed (or abandoned running) PAST row for the same
        stretch is revived rather than duplicated, so it still counts once.
        Fresh looks go through ``claim_fresh``."""
        if kind != KIND_PAST:
            raise ValueError("fresh looks are claimed with claim_fresh()")
        stamp = _now(now)
        with self._tx():
            if self.conn.execute("SELECT 1 FROM candidates WHERE id=?", (cid,)).fetchone() is None:
                # a trial of an idea nobody registered could not be filed under a family, and
                # N must be the count of everything tried
                raise LedgerError(f"unknown candidate {cid}: register() it before testing it")
            row = self.conn.execute(
                "SELECT id, status FROM tests WHERE candidate_id=? AND kind=? AND stretch_id=?",
                (cid, kind, stretch_id)).fetchone()
            if row is None:
                cur = self.conn.execute(
                    "INSERT INTO tests(candidate_id, kind, stretch_id, cut_day, status, started_utc)"
                    " VALUES (?,?,?,?,?,?)", (cid, kind, stretch_id, cut_day, "running", stamp))
                return int(cur.lastrowid)
            if row["status"] == "done":
                raise AlreadyTested(f"{cid} already has a finished {kind} test in {stretch_id}")
            self.conn.execute(
                "UPDATE tests SET status='running', started_utc=?, finished_utc=NULL, cut_day=?,"
                " gated=0 WHERE id=?", (stamp, cut_day, row["id"]))
            return int(row["id"])

    def claim_fresh(self, cid: str, stretch_id: str, cut_day: str, fresh_cap: int, *,
                    now: Optional[dt.datetime] = None) -> int:
        """The fresh look, granted at most once. One transaction: a PAST row with
        passed=1 exists in this stretch AND the family-end gate has been applied to
        it (``regrade_past``) without holding it back; no fresh row exists for the
        id in any stretch (except one TOO_FEW_TRADES retry); fewer than ``fresh_cap``
        fresh rows exist in this stretch (the cap is never more than 30); then the
        ``running`` row is inserted and committed. Returns the test id. Raises
        FreshRefused otherwise.

        Held back (code NO_PAST_PASS, the message says why): a past pass the family
        gate has not looked at yet, one whose settings sit at the edge of the range
        tried (plateau 'extend'), one still waiting for a neighbouring setting to be
        tested (plateau unknown), one whose family looks to be fitting noise, or one
        whose family row says PBO is over its limit."""
        stamp = _now(now)
        cap = clamp_fresh_cap(fresh_cap)
        with self._tx():
            past = self.conn.execute(
                "SELECT t.passed, t.plateau, t.reasons_json, t.gated, c.family FROM tests t"
                " LEFT JOIN candidates c ON c.id=t.candidate_id"
                " WHERE t.candidate_id=? AND t.kind='past' AND t.stretch_id=?"
                " AND t.status='done'", (cid, stretch_id)).fetchone()
            if past is None or not past["passed"]:
                raise FreshRefused("NO_PAST_PASS",
                                   f"{cid} has no passed past test in {stretch_id}")
            held = self._held_back(past, stretch_id)
            if held:
                raise FreshRefused("NO_PAST_PASS", f"{cid} passed its past test but {held}")
            looks = self.conn.execute(
                "SELECT stretch_id, status, reasons_json FROM tests WHERE candidate_id=?"
                " AND kind='fresh'", (cid,)).fetchall()
            if looks:
                only = looks[0]
                reasons = []
                try:
                    reasons = json.loads(only["reasons_json"] or "[]")
                except ValueError:
                    pass
                retry_ok = (len(looks) == 1 and only["status"] == "done"
                            and STRETCH_GAP_RETEST in reasons
                            and only["stretch_id"] != stretch_id)
                if not retry_ok:
                    raise FreshRefused("ALREADY_LOOKED",
                                       f"{cid} has already had its fresh look")
            used = self.conn.execute(
                "SELECT COUNT(*) FROM tests WHERE kind='fresh' AND stretch_id=?",
                (stretch_id,)).fetchone()[0]
            if used >= cap:
                raise FreshRefused("CAP_REACHED",
                                   f"{used} of {cap} fresh looks used in {stretch_id}")
            cur = self.conn.execute(
                "INSERT INTO tests(candidate_id, kind, stretch_id, cut_day, status, started_utc)"
                " VALUES (?,?,?,?,?,?)", (cid, KIND_FRESH, stretch_id, cut_day, "running", stamp))
            return int(cur.lastrowid)

    def _held_back(self, past, stretch_id: str) -> str:
        """Why a PAST pass may not go to the fresh look yet ('' when it may)."""
        if not past["gated"]:
            return "the family's end-of-grid checks have not been applied to it yet"
        if past["plateau"] is None:
            return "a neighbouring setting has not been tested yet, so its plateau is unknown"
        if past["plateau"] == "extend":
            return "its settings sit at the edge of the range tried (the range must be extended)"
        try:
            reasons = json.loads(past["reasons_json"] or "[]")
        except ValueError:
            reasons = []
        for code in HOLD_BACK_REASONS:
            if code in reasons:
                return f"it is held back ({code})"
        fam = self.conn.execute("SELECT pbo FROM families WHERE code=? AND stretch_id=?",
                                (past["family"], stretch_id)).fetchone()
        if fam is not None and fam["pbo"] is not None and fam["pbo"] > PBO_MAX:
            return f"its family looks to be fitting noise (PBO {fam['pbo']:.2f})"
        return ""

    def finish_test(self, test_id: int, cols: dict, daily: dict, *, reasons: list,
                    skipped: dict, synthetic: bool, seconds: float,
                    trial: Optional[dict] = None,
                    now: Optional[dt.datetime] = None) -> Optional[int]:
        """Write the result of a test that was begun (``running`` -> ``done``),
        its per-day rows, and its TrialLedger row. ``cols`` holds TEST_COLUMNS;
        ``daily`` maps UK day -> (net_gbp, trades). ``trial`` (optional) holds
        sharpe, expectancy_r and max_drawdown_pct for the shared ledger.
        Returns the TrialLedger row id (or None)."""
        stamp = _now(now)
        unknown = set(cols) - set(TEST_COLUMNS)
        if unknown:
            raise LedgerError(f"unknown test columns {sorted(unknown)}")
        row = self.one("SELECT t.kind, t.candidate_id, c.family, c.def_json FROM tests t"
                       " LEFT JOIN candidates c ON c.id=t.candidate_id WHERE t.id=?", (test_id,))
        if row is None:
            raise LedgerError(f"no test {test_id}")
        sets = ", ".join(f"{k}=?" for k in cols)
        args = [cols[k] for k in cols]
        with self._tx():
            cur = self.conn.execute(
                f"UPDATE tests SET {sets}{', ' if sets else ''}status='done', finished_utc=?,"
                " seconds=?, reasons_json=?, skipped_json=?, synthetic=?, gated=0"
                " WHERE id=? AND status='running'",
                args + [stamp, float(seconds), json.dumps(list(reasons)),
                        json.dumps(skipped, sort_keys=True), 1 if synthetic else 0, test_id])
            if cur.rowcount != 1:
                raise LedgerError(f"test {test_id} is not running")
            self.conn.execute("DELETE FROM daily WHERE test_id=?", (test_id,))
            self.conn.executemany(
                "INSERT INTO daily(test_id, day, net_gbp, trades) VALUES (?,?,?,?)",
                [(test_id, d, float(v[0]), int(v[1])) for d, v in sorted(daily.items())])
        trial_id = None
        tl = self.trial_ledger()
        if tl is not None and row["family"]:
            from ..research.trial_ledger import Trial
            info = trial or {}
            tr = Trial(
                study=f"lab:{row['family']}",
                params=json.loads(row["def_json"]) if row["def_json"] else {"id": row["candidate_id"]},
                dataset="SYNTHETIC" if synthetic else "bridge",
                period_start=str(cols.get("window_from") or ""),
                period_end=str(cols.get("window_to") or ""),
                trades=int(cols.get("trades") or 0),
                sharpe=info.get("sharpe"), expectancy_r=info.get("expectancy_r"),
                profit_factor=cols.get("pf"),
                max_drawdown_pct=info.get("max_drawdown_pct"),
                net_money=cols.get("net_gbp"),
                is_oos=(row["kind"] == KIND_FRESH),
                notes=f"lab test {test_id} {row['candidate_id']} {row['kind']}")
            trial_id = tl.record(tr)
            self.execute("UPDATE tests SET ledger_row_id=? WHERE id=?", (trial_id, test_id))
        return trial_id

    def regrade_past(self, test_id: int, *, plateau: Optional[str], passed: bool,
                     reasons: list, dsr: Optional[float] = None) -> None:
        """After a family's grid is complete: write the plateau state, the final
        pass/fail and (optionally) the DSR onto a finished PAST row, and mark the row
        as gated (the fresh door will not open for a past pass that has not been
        through here). Call it for every past row that passed, even when nothing
        changed. ``plateau`` None means "a neighbouring setting is not tested yet":
        the row stays at PAST TEST and is not forwarded. ``reasons`` should carry
        the hold-back codes (EXTEND_GRID, FAMILY_FITS_NOISE, PLATEAU_PENDING) that
        ``past_verdict`` gave.

        A family-end gate can only TIGHTEN a result: a row that failed its own
        checks stays failed whatever ``passed`` says here (the reasons it failed
        for are kept). Only PAST rows can be re-graded; a fresh row is final (a
        trigger enforces it)."""
        with self._tx():
            row = self.conn.execute(
                "SELECT passed, reasons_json FROM tests WHERE id=? AND kind='past'"
                " AND status='done'", (test_id,)).fetchone()
            if row is None:
                raise LedgerError(f"test {test_id} is not a finished past test")
            reasons = list(reasons)
            if passed and row["passed"] is not None and not row["passed"]:
                passed = False
                try:
                    before = json.loads(row["reasons_json"] or "[]")
                except ValueError:
                    before = []
                reasons = list(before) + [r for r in reasons if r not in before]
            self.conn.execute(
                "UPDATE tests SET plateau=?, passed=?, reasons_json=?, dsr=COALESCE(?, dsr),"
                " gated=1 WHERE id=?",
                (plateau, 1 if passed else 0, json.dumps(reasons), dsr, test_id))

    def crash_test(self, test_id: int, note: str = "", *,
                   now: Optional[dt.datetime] = None) -> None:
        """A run that did not finish. The row stays and still counts toward N."""
        self.execute(
            "UPDATE tests SET status='crashed', finished_utc=?, reasons_json=? "
            "WHERE id=? AND status='running'",
            (_now(now), json.dumps(["CRASHED", note] if note else ["CRASHED"]), test_id))

    def crash_all_running(self, note: str = "a run was cut short", *,
                          now: Optional[dt.datetime] = None) -> int:
        """At the start of a run: anything still ``running`` belongs to a
        process that is gone."""
        with self._lock:
            cur = self.conn.execute(
                "UPDATE tests SET status='crashed', finished_utc=?, reasons_json=? "
                "WHERE status='running'", (_now(now), json.dumps(["CRASHED", note])))
            return cur.rowcount

    # ---- reading tests
    def test_row(self, test_id: int) -> Optional[dict]:
        r = self.one("SELECT * FROM tests WHERE id=?", (test_id,))
        return dict(r) if r else None

    def past_row(self, cid: str, stretch_id: str) -> Optional[dict]:
        r = self.one("SELECT * FROM tests WHERE candidate_id=? AND kind='past' AND stretch_id=?",
                     (cid, stretch_id))
        return dict(r) if r else None

    def latest_past(self, cid: str) -> Optional[dict]:
        r = self.one("SELECT * FROM tests WHERE candidate_id=? AND kind='past' "
                     "ORDER BY id DESC LIMIT 1", (cid,))
        return dict(r) if r else None

    def fresh_rows(self, cid: str) -> list:
        return [dict(r) for r in self.query(
            "SELECT * FROM tests WHERE candidate_id=? AND kind='fresh' ORDER BY id", (cid,))]

    def daily_of(self, test_id: int) -> dict:
        return {r["day"]: (r["net_gbp"], r["trades"]) for r in self.query(
            "SELECT day, net_gbp, trades FROM daily WHERE test_id=? ORDER BY day", (test_id,))}

    def past_rows_for_stretch(self, stretch_id: str, family: Optional[str] = None) -> list:
        sql = ("SELECT t.* FROM tests t JOIN candidates c ON c.id=t.candidate_id "
               "WHERE t.kind='past' AND t.stretch_id=? AND t.status='done'")
        args: list = [stretch_id]
        if family:
            sql += " AND c.family=?"
            args.append(family)
        return [dict(r) for r in self.query(sql + " ORDER BY t.candidate_id", args)]

    def n_tests(self, *, kind: Optional[str] = None, family: Optional[str] = None,
                stretch_id: Optional[str] = None) -> int:
        """N: the count of ``tests`` rows (running, crashed and done). Counted from
        the rows themselves, so a row whose candidate was never registered still
        counts (it is only left out when a family is asked for)."""
        sql = "SELECT COUNT(*) FROM tests t WHERE 1=1"
        args: list = []
        if kind:
            sql += " AND t.kind=?"
            args.append(kind)
        if family:
            sql += " AND t.candidate_id IN (SELECT id FROM candidates WHERE family=?)"
            args.append(family)
        if stretch_id:
            sql += " AND t.stretch_id=?"
            args.append(stretch_id)
        return int(self.one(sql, args)[0])

    def n_by_family(self) -> dict:
        """N per family; a row with no registered candidate is counted under ''."""
        return {r[0]: r[1] for r in self.query(
            "SELECT COALESCE(c.family, ''), COUNT(*) FROM tests t"
            " LEFT JOIN candidates c ON c.id=t.candidate_id GROUP BY COALESCE(c.family, '')")}

    def fresh_used(self, stretch_id: str) -> int:
        return int(self.one("SELECT COUNT(*) FROM tests WHERE kind='fresh' AND stretch_id=?",
                            (stretch_id,))[0])

    def family_sharpes(self, family: str) -> list:
        """Sharpe of every past trial of a family in the shared ledger: the real
        search effort that the deflated Sharpe is measured against. These are
        ANNUALISED Sharpes (``TestStats.sharpe``); ``stats.dsr_of`` converts them."""
        tl = self.trial_ledger()
        if tl is None:
            return []
        return [float(r["sharpe"]) for r in tl.all(f"lab:{family}")
                if r["sharpe"] is not None and not r["is_oos"]]

    # -------------------------------------------------------------- families --
    def upsert_family(self, code: str, stretch_id: str, *, n: int, past_passed: int,
                      pbo: Optional[float], extend_grid: list, wf_ok: Optional[int] = None,
                      control_pass: Optional[dict] = None) -> None:
        self.execute(
            "INSERT INTO families(code, stretch_id, n, past_passed, pbo, extend_grid_json,"
            " wf_ok, control_pass_json) VALUES (?,?,?,?,?,?,?,?)"
            " ON CONFLICT(code, stretch_id) DO UPDATE SET n=excluded.n,"
            " past_passed=excluded.past_passed, pbo=excluded.pbo,"
            " extend_grid_json=excluded.extend_grid_json, wf_ok=excluded.wf_ok,"
            " control_pass_json=excluded.control_pass_json",
            (code, stretch_id, n, past_passed, pbo, json.dumps(sorted(extend_grid)),
             wf_ok, json.dumps(control_pass or {}, sort_keys=True)))

    def family_row(self, code: str, stretch_id: str) -> Optional[dict]:
        r = self.one("SELECT * FROM families WHERE code=? AND stretch_id=?", (code, stretch_id))
        return dict(r) if r else None

    # ------------------------------------------------------------------ runs --
    def upsert_run(self, date: str, **fields: Any) -> None:
        cols = ("started_utc", "finished_utc", "ok", "step", "tested", "fresh", "seconds",
                "seconds_per_candidate", "deadline_hit", "note")
        bad = set(fields) - set(cols)
        if bad:
            raise LedgerError(f"unknown run columns {sorted(bad)}")
        with self._tx():
            self.conn.execute("INSERT OR IGNORE INTO runs(date) VALUES (?)", (date,))
            if fields:
                sets = ", ".join(f"{k}=?" for k in fields)
                self.conn.execute(f"UPDATE runs SET {sets} WHERE date=?",
                                  list(fields.values()) + [date])


class _Tx:
    """BEGIN IMMEDIATE ... COMMIT (ROLLBACK on any error), under the ledger lock."""

    def __init__(self, ledger: LabLedger):
        self.l = ledger

    def __enter__(self):
        self.l._lock.acquire()
        try:
            self.l.conn.execute("BEGIN IMMEDIATE")
        except Exception:
            self.l._lock.release()
            raise
        return self

    def __exit__(self, et, ev, tb):
        try:
            if et is None:
                self.l.conn.execute("COMMIT")
            else:
                try:
                    self.l.conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
        finally:
            self.l._lock.release()
        return False
