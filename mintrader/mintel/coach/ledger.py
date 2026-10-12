"""The coach's ledger: ``data/coach/ledger.sqlite`` (spec section 14).

Append-only where it matters: a trigger refuses every UPDATE or DELETE on
``changes``, and ``policies`` may only have ``valid`` moved from 0 to 1 (rows
older than 14 days are pruned). Every change carries a sentence of at least
20 characters (a CHECK, so a change without words cannot be stored) and who
made it (coach, aaron or installer).

Tables (the spec's, plus three small ones it implies, listed last):

    changes     one row per difference between two policies, and per decision
    segments    each key's state machine (state, step, cool-off, pauses, ...)
    policies    every policy written, sealed (json kept 14 days, when it changed)
    ticks       one row a tick: ok, how long, the error
    peaks       the coach's own saved equity peak (survives a restart)
    daily       the page's real figure per UK day (NULL: could not be counted)
    luck        the nightly luck check
    plans       tomorrow's game plan, when it was released and what was vetoed
    scorecard   the coach's own effect, nightly
    shadow      section 21: every coach-sized real trade at real-money (v1) size
    floors      tighten-only floors in force (section 13)
    meta        small facts: the WATCH-day self-check, the coach's own GUARD

Opened in WAL mode; only the coach's run loop writes it. Never raises past
its caller on a read (an unreadable ledger is a failed tick, which B9 counts).
"""
from __future__ import annotations

import datetime as dt
import gzip
import json
import sqlite3
import threading
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from . import UTC, as_utc
from .words import MIN_WORDS_CHARS

POLICY_KEEP_DAYS = 14
BY = ("coach", "aaron", "installer")

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS changes(
    id INTEGER PRIMARY KEY, ts_utc TEXT NOT NULL, policy_seq INT, kind TEXT NOT NULL, target TEXT NOT NULL,
    from_state TEXT, to_state TEXT, rule TEXT, numbers_json TEXT,
    words TEXT NOT NULL CHECK(length(words) >= {MIN_WORDS_CHARS}),
    by TEXT NOT NULL CHECK(by IN ('coach','aaron','installer')));
CREATE INDEX IF NOT EXISTS ix_changes_ts ON changes(ts_utc);
CREATE INDEX IF NOT EXISTS ix_changes_target ON changes(target);
CREATE TABLE IF NOT EXISTS segments(
    key TEXT PRIMARY KEY, registered_utc TEXT, epoch TEXT, state TEXT, step TEXT, since_utc TEXT,
    cooloff_until TEXT, live_since_utc TEXT, trial_pnl_gbp REAL, peak_r REAL, cusum REAL, real_n INT,
    paused_until TEXT, hold_until TEXT, g2_failures INT, banned_epoch TEXT, rule TEXT);
CREATE TABLE IF NOT EXISTS policies(
    seq INTEGER PRIMARY KEY, written_utc TEXT, sha256 TEXT, valid INT DEFAULT 0, json_gz BLOB);
CREATE TABLE IF NOT EXISTS ticks(ts_utc TEXT, ok INT, ms REAL, err TEXT);
CREATE INDEX IF NOT EXISTS ix_ticks_ts ON ticks(ts_utc);
CREATE TABLE IF NOT EXISTS peaks(ts_utc TEXT, equity_peak REAL);
CREATE TABLE IF NOT EXISTS daily(day TEXT PRIMARY KEY, real_net REAL, source TEXT, est_lo REAL, est_mid REAL,
    est_hi REAL);
CREATE TABLE IF NOT EXISTS luck(date TEXT PRIMARY KEY, shuffles INT, pass_rate REAL, blocked INT);
CREATE TABLE IF NOT EXISTS plans(date TEXT PRIMARY KEY, json TEXT, released_utc TEXT, vetoed_json TEXT);
CREATE TABLE IF NOT EXISTS scorecard(date TEXT PRIMARY KEY, saved_gbp REAL, added_gbp REAL, trades INT);
CREATE TABLE IF NOT EXISTS shadow(ticket INTEGER PRIMARY KEY, key TEXT, closed_utc TEXT, step TEXT,
    money_gbp REAL, risk_gbp REAL, net_gbp REAL, real_money_gbp REAL, real_money_net_gbp REAL);
CREATE TABLE IF NOT EXISTS floors(key TEXT PRIMARY KEY, min_score REAL, min_rr REAL, since_utc TEXT);
CREATE TABLE IF NOT EXISTS meta(name TEXT PRIMARY KEY, value TEXT, ts_utc TEXT);
CREATE TRIGGER IF NOT EXISTS changes_no_update BEFORE UPDATE ON changes
    BEGIN SELECT RAISE(ABORT, 'the coach''s changes are append-only'); END;
CREATE TRIGGER IF NOT EXISTS changes_no_delete BEFORE DELETE ON changes
    BEGIN SELECT RAISE(ABORT, 'the coach''s changes are append-only'); END;
CREATE TRIGGER IF NOT EXISTS policies_valid_only BEFORE UPDATE ON policies
    WHEN NOT (OLD.valid = 0 AND NEW.valid = 1 AND OLD.seq = NEW.seq AND OLD.sha256 IS NEW.sha256
              AND OLD.written_utc IS NEW.written_utc AND OLD.json_gz IS NEW.json_gz)
    BEGIN SELECT RAISE(ABORT, 'a policy row may only be marked valid'); END;
"""


class LedgerError(ValueError):
    pass


@dataclass
class Change:
    """One row of ``changes``."""
    kind: str
    target: str
    words: str
    from_state: Optional[str] = None
    to_state: Optional[str] = None
    rule: Optional[str] = None
    numbers: dict = field(default_factory=dict)
    by: str = "coach"
    ts_utc: Optional[str] = None
    policy_seq: Optional[int] = None
    id: Optional[int] = None


@dataclass
class Segment:
    """One key's state machine (``segments``)."""
    key: str
    registered_utc: str = ""
    epoch: str = ""
    state: str = "PRACTICE"
    step: Optional[str] = None
    since_utc: str = ""                # the last change of state or step
    cooloff_until: str = ""            # UK date: no re-promotion before it
    live_since_utc: str = ""           # when it last went real under the coach
    trial_pnl_gbp: float = 0.0
    peak_r: float = 0.0
    cusum: float = 0.0
    real_n: int = 0
    paused_until: str = ""             # PAUSED until (UTC)
    hold_until: str = ""               # UK date: no step up before it (after a step down)
    g2_failures: int = 0
    banned_epoch: str = ""             # BANNED_REAL for this epoch
    rule: str = ""                     # the rule behind the state

    def copy(self) -> "Segment":
        return Segment(**{f.name: getattr(self, f.name) for f in fields(self)})


_SEG_COLS = [f.name for f in fields(Segment)]


def _now_iso() -> str:
    return dt.datetime.now(tz=UTC).isoformat()


class Ledger:
    """``data/coach/ledger.sqlite``; ``path=":memory:"`` for a replay."""

    def __init__(self, path: "str | Path"):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.con = sqlite3.connect(self.path, timeout=10.0, check_same_thread=False, isolation_level=None)
        self.con.row_factory = sqlite3.Row
        if self.path != ":memory:":
            self.con.execute("PRAGMA journal_mode=WAL")
        self.con.execute("PRAGMA synchronous=FULL")
        self.con.executescript(SCHEMA)

    def close(self) -> None:
        try:
            self.con.close()
        except Exception:
            pass

    # ------------------------------------------------------------- changes --
    def check_change(self, c: Change) -> None:
        if not isinstance(c.words, str) or len(c.words.strip()) < MIN_WORDS_CHARS:
            raise LedgerError(f"a change to {c.target} has no sentence: refused")
        if c.by not in BY:
            raise LedgerError(f"a change by {c.by!r}: refused")

    def add_changes(self, changes: Sequence[Change], *, policy_seq: Optional[int] = None,
                    now: Optional[dt.datetime] = None) -> list[int]:
        """Commit the rows in one transaction (all or none); returns their ids. A change without a sentence
        refuses the whole batch."""
        for c in changes:
            self.check_change(c)
        ts = as_utc(now).isoformat() if now is not None else _now_iso()
        ids = []
        with self._lock:
            self.con.execute("BEGIN IMMEDIATE")
            try:
                for c in changes:
                    cur = self.con.execute(
                        "INSERT INTO changes(ts_utc, policy_seq, kind, target, from_state, to_state, rule, "
                        "numbers_json, words, by) VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (c.ts_utc or ts, c.policy_seq if c.policy_seq is not None else policy_seq, c.kind,
                         c.target, c.from_state, c.to_state, c.rule,
                         json.dumps(c.numbers or {}, sort_keys=True, default=str), c.words, c.by))
                    ids.append(int(cur.lastrowid))
                self.con.execute("COMMIT")
            except Exception:
                self.con.execute("ROLLBACK")
                raise
        return ids

    def commit(self, changes: Sequence[Change], segments: Iterable[Segment] = (), meta: Optional[dict] = None, *,
               policy_seq: Optional[int] = None, now: Optional[dt.datetime] = None) -> list[int]:
        """The writer's step 4 (section 10.2): every change row, the segments and the small facts in ONE
        transaction, before the policy file is replaced. A change without a sentence refuses all of it."""
        for c in changes:
            self.check_change(c)
        ts = as_utc(now).isoformat() if now is not None else _now_iso()
        cols = ",".join(_SEG_COLS)
        qs = ",".join("?" * len(_SEG_COLS))
        ids = []
        with self._lock:
            self.con.execute("BEGIN IMMEDIATE")
            try:
                for c in changes:
                    cur = self.con.execute(
                        "INSERT INTO changes(ts_utc, policy_seq, kind, target, from_state, to_state, rule, "
                        "numbers_json, words, by) VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (c.ts_utc or ts, c.policy_seq if c.policy_seq is not None else policy_seq, c.kind,
                         c.target, c.from_state, c.to_state, c.rule,
                         json.dumps(c.numbers or {}, sort_keys=True, default=str), c.words, c.by))
                    ids.append(int(cur.lastrowid))
                for s in segments:
                    self.con.execute(f"INSERT OR REPLACE INTO segments({cols}) VALUES ({qs})",
                                     tuple(getattr(s, c) for c in _SEG_COLS))
                for name, value in (meta or {}).items():
                    self.con.execute("INSERT OR REPLACE INTO meta VALUES (?,?,?)",
                                     (name, json.dumps(value, sort_keys=True, default=str), ts))
                self.con.execute("COMMIT")
            except Exception:
                self.con.execute("ROLLBACK")
                raise
        return ids

    def changes(self, since: Optional[dt.datetime] = None, limit: int = 500, kinds: Optional[Iterable[str]] = None,
                target: Optional[str] = None) -> list[Change]:
        q = "SELECT * FROM changes WHERE 1=1"
        args: list = []
        if since is not None:
            q += " AND ts_utc >= ?"
            args.append(as_utc(since).isoformat())
        if kinds is not None:
            ks = list(kinds)
            q += f" AND kind IN ({','.join('?' * len(ks))})"
            args += ks
        if target is not None:
            q += " AND target = ?"
            args.append(target)
        q += " ORDER BY id DESC LIMIT ?"
        args.append(int(limit))
        with self._lock:
            rows = self.con.execute(q, args).fetchall()
        out = []
        for r in rows:
            try:
                nums = json.loads(r["numbers_json"] or "{}")
            except Exception:
                nums = {}
            out.append(Change(r["kind"], r["target"], r["words"], r["from_state"], r["to_state"], r["rule"], nums,
                              r["by"], r["ts_utc"], r["policy_seq"], r["id"]))
        return out

    def last_change_id(self, target: str) -> Optional[int]:
        with self._lock:
            r = self.con.execute("SELECT max(id) FROM changes WHERE target = ?", (target,)).fetchone()
        return int(r[0]) if r and r[0] is not None else None

    # ------------------------------------------------------------ segments --
    def segments(self) -> dict[str, Segment]:
        with self._lock:
            rows = self.con.execute("SELECT * FROM segments").fetchall()
        out = {}
        for r in rows:
            d = {c: r[c] for c in _SEG_COLS}
            for c in ("registered_utc", "epoch", "since_utc", "cooloff_until", "live_since_utc", "paused_until",
                      "hold_until", "banned_epoch", "rule"):
                d[c] = d[c] or ""
            for c in ("trial_pnl_gbp", "peak_r", "cusum"):
                d[c] = float(d[c] or 0.0)
            for c in ("real_n", "g2_failures"):
                d[c] = int(d[c] or 0)
            out[d["key"]] = Segment(**d)
        return out

    def put_segments(self, segs: Iterable[Segment]) -> None:
        segs = list(segs)
        if not segs:
            return
        cols = ",".join(_SEG_COLS)
        qs = ",".join("?" * len(_SEG_COLS))
        with self._lock:
            self.con.execute("BEGIN IMMEDIATE")
            try:
                for s in segs:
                    self.con.execute(f"INSERT OR REPLACE INTO segments({cols}) VALUES ({qs})",
                                     tuple(getattr(s, c) for c in _SEG_COLS))
                self.con.execute("COMMIT")
            except Exception:
                self.con.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------ policies --
    def next_seq(self) -> int:
        with self._lock:
            r = self.con.execute("SELECT max(seq) FROM policies").fetchone()
        return int(r[0]) + 1 if r and r[0] is not None else 1

    def add_policy(self, seq: int, written_utc: str, sha256: str, body: Optional[bytes]) -> None:
        blob = gzip.compress(body) if body is not None else None
        with self._lock:
            self.con.execute("INSERT INTO policies(seq, written_utc, sha256, valid, json_gz) VALUES (?,?,?,0,?)",
                             (int(seq), written_utc, sha256, blob))

    def mark_valid(self, seq: int) -> None:
        with self._lock:
            self.con.execute("UPDATE policies SET valid = 1 WHERE seq = ?", (int(seq),))

    def policy_json(self, seq: int) -> Optional[dict]:
        with self._lock:
            r = self.con.execute("SELECT json_gz FROM policies WHERE seq = ?", (int(seq),)).fetchone()
        if r is None or r[0] is None:
            return None
        return json.loads(gzip.decompress(r[0]).decode("ascii"))

    def prune_policies(self, now: dt.datetime) -> int:
        cut = (as_utc(now) - dt.timedelta(days=POLICY_KEEP_DAYS)).isoformat()
        with self._lock:
            keep = self.con.execute("SELECT max(seq) FROM policies").fetchone()[0] or 0
            cur = self.con.execute("DELETE FROM policies WHERE written_utc < ? AND seq < ?", (cut, keep))
        return int(cur.rowcount or 0)

    def policy_changes_since(self, since: dt.datetime) -> int:
        """Policies that changed an instruction to the bots since ``since`` (B9 counts them)."""
        with self._lock:
            r = self.con.execute(
                "SELECT count(DISTINCT policy_seq) FROM changes WHERE by = 'coach' AND policy_seq IS NOT NULL "
                "AND kind IN ('entry','block','floor','demo','judged') AND ts_utc >= ?",
                (as_utc(since).isoformat(),)).fetchone()
        return int(r[0] or 0)

    # --------------------------------------------------------------- ticks --
    def add_tick(self, now: dt.datetime, ok: bool, ms: float, err: str = "") -> None:
        with self._lock:
            self.con.execute("INSERT INTO ticks VALUES (?,?,?,?)", (as_utc(now).isoformat(), int(bool(ok)),
                                                                    float(ms), (err or "")[:500]))
            self.con.execute("DELETE FROM ticks WHERE ts_utc < ?",
                             ((as_utc(now) - dt.timedelta(days=30)).isoformat(),))

    def errors_in_a_row(self) -> int:
        with self._lock:
            rows = self.con.execute("SELECT ok FROM ticks ORDER BY rowid DESC LIMIT 10").fetchall()
        n = 0
        for r in rows:
            if r[0]:
                break
            n += 1
        return n

    def ticks_since(self, since: dt.datetime) -> list[tuple[str, int, str]]:
        with self._lock:
            rows = self.con.execute("SELECT ts_utc, ok, err FROM ticks WHERE ts_utc >= ? ORDER BY ts_utc",
                                    (as_utc(since).isoformat(),)).fetchall()
        return [(r[0], int(r[1]), r[2] or "") for r in rows]

    # --------------------------------------------------------------- peaks --
    def peak(self) -> Optional[float]:
        with self._lock:
            r = self.con.execute("SELECT equity_peak FROM peaks ORDER BY rowid DESC LIMIT 1").fetchone()
        return float(r[0]) if r and r[0] is not None else None

    def set_peak(self, now: dt.datetime, value: float) -> None:
        with self._lock:
            self.con.execute("INSERT INTO peaks VALUES (?,?)", (as_utc(now).isoformat(), float(value)))

    # --------------------------------------------------------------- daily --
    def put_daily(self, day: str, real_net: Optional[float], source: str, est: Optional[tuple] = None) -> None:
        lo, mid, hi = est if est else (None, None, None)
        with self._lock:
            self.con.execute("INSERT OR REPLACE INTO daily VALUES (?,?,?,?,?,?)", (day, real_net, source, lo, mid, hi))

    def daily(self) -> list[dict]:
        with self._lock:
            rows = self.con.execute("SELECT * FROM daily ORDER BY day").fetchall()
        return [dict(r) for r in rows]

    # ---------------------------------------------------------------- luck --
    def put_luck(self, date: str, shuffles: int, rate: float, blocked: bool) -> None:
        with self._lock:
            self.con.execute("INSERT OR REPLACE INTO luck VALUES (?,?,?,?)", (date, int(shuffles), float(rate),
                                                                            int(bool(blocked))))

    def luck(self, date: Optional[str] = None) -> Optional[dict]:
        with self._lock:
            if date is None:
                r = self.con.execute("SELECT * FROM luck ORDER BY date DESC LIMIT 1").fetchone()
            else:
                r = self.con.execute("SELECT * FROM luck WHERE date = ?", (date,)).fetchone()
        return dict(r) if r else None

    # --------------------------------------------------------------- plans --
    def put_plan(self, date: str, plan: dict) -> None:
        with self._lock:
            self.con.execute("INSERT OR REPLACE INTO plans(date, json, released_utc, vetoed_json) VALUES (?,?,?,?)",
                             (date, json.dumps(plan, sort_keys=True, default=str), None, None))

    def plan(self, date: str) -> Optional[dict]:
        with self._lock:
            r = self.con.execute("SELECT * FROM plans WHERE date = ?", (date,)).fetchone()
        return self._plan_row(r)

    def latest_plan(self) -> Optional[dict]:
        with self._lock:
            r = self.con.execute("SELECT * FROM plans ORDER BY date DESC LIMIT 1").fetchone()
        return self._plan_row(r)

    @staticmethod
    def _plan_row(r) -> Optional[dict]:
        if r is None:
            return None
        try:
            body = json.loads(r["json"])
        except Exception:
            body = {}
        return {"date": r["date"], "plan": body, "released_utc": r["released_utc"],
                "vetoed": json.loads(r["vetoed_json"]) if r["vetoed_json"] else []}

    def mark_released(self, date: str, now: dt.datetime, vetoed: Sequence[str]) -> None:
        with self._lock:
            self.con.execute("UPDATE plans SET released_utc = ?, vetoed_json = ? WHERE date = ?",
                             (as_utc(now).isoformat(), json.dumps(sorted(vetoed)), date))

    # ----------------------------------------------------------- scorecard --
    def put_scorecard(self, date: str, saved: float, added: float, trades: int) -> None:
        with self._lock:
            self.con.execute("INSERT OR REPLACE INTO scorecard VALUES (?,?,?,?)", (date, float(saved), float(added),
                                                                                 int(trades)))

    def scorecard(self) -> list[dict]:
        with self._lock:
            rows = self.con.execute("SELECT * FROM scorecard ORDER BY date").fetchall()
        return [dict(r) for r in rows]

    # -------------------------------------------------------------- shadow --
    def put_shadow(self, rows: Iterable[dict]) -> None:
        with self._lock:
            for r in rows:
                self.con.execute(
                    "INSERT OR REPLACE INTO shadow VALUES (?,?,?,?,?,?,?,?,?)",
                    (int(r["ticket"]), r["key"], r["closed_utc"], r.get("step"), r["money_gbp"], r["risk_gbp"],
                     r["net_gbp"], r["real_money_gbp"], r["real_money_net_gbp"]))

    def shadow(self, since: Optional[dt.datetime] = None) -> list[dict]:
        with self._lock:
            if since is None:
                rows = self.con.execute("SELECT * FROM shadow ORDER BY closed_utc").fetchall()
            else:
                rows = self.con.execute("SELECT * FROM shadow WHERE closed_utc >= ? ORDER BY closed_utc",
                                        (as_utc(since).isoformat(),)).fetchall()
        return [dict(r) for r in rows]

    # -------------------------------------------------------------- floors --
    def floors(self) -> dict[str, dict]:
        with self._lock:
            rows = self.con.execute("SELECT * FROM floors").fetchall()
        return {r["key"]: dict(r) for r in rows}

    def set_floor(self, key: str, min_score: Optional[float], min_rr: Optional[float], since: str) -> None:
        with self._lock:
            if min_score is None and min_rr is None:
                self.con.execute("DELETE FROM floors WHERE key = ?", (key,))
            else:
                self.con.execute("INSERT OR REPLACE INTO floors VALUES (?,?,?,?)", (key, min_score, min_rr, since))

    # ---------------------------------------------------------------- meta --
    def get(self, name: str, default: Any = None) -> Any:
        with self._lock:
            r = self.con.execute("SELECT value FROM meta WHERE name = ?", (name,)).fetchone()
        if r is None:
            return default
        try:
            return json.loads(r[0])
        except Exception:
            return default

    def set(self, name: str, value: Any, now: Optional[dt.datetime] = None) -> None:
        with self._lock:
            self.con.execute("INSERT OR REPLACE INTO meta VALUES (?,?,?)",
                             (name, json.dumps(value, sort_keys=True, default=str),
                              as_utc(now).isoformat() if now else _now_iso()))
