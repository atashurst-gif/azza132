"""The lab's night: ten steps, every one idempotent, the whole run resumable.

    python -m mintel.lab.nightly --config data/config.json --upload --workers 1
                                 [--deadline 06:00] [--max-candidates N] [--no-fetch]

Run by the LaunchAgent ``com.mintel.lab`` at 23:10 on Monday to Saturday, under
``nice -n 19``, one process; the launcher starts a missed night between 23:10
and 06:00. Its lines go to ``logs/lab-nightly.log``.

Guards, before anything is touched: ``runs/<night>/done`` (the night is done:
exit 0); ``data/lab/nightly.lock`` holding the pid of a lab night still running
(exit 0; a dead pid's lock is removed); ``data/lab/STOP`` (stops cleanly, here
and between steps); less than 1 GB free on the disk (nothing is run); the lab
switched OFF in ``data/lab.json`` (nothing is run).

The steps, each with a line in ``runs/<night>/state.json`` and its seconds in
the log:

     1 universe   the lab's markets, chosen once from the broker's own list
     2 fetch      old minute prices, gently (skipped when MetaTrader is not
                  connected, outside the quiet hours, or with --no-fetch)
     3 cut        the date that splits past from fresh (the first night of a month)
     4 features   the worked-out numbers for any month that lacks them
     5 practice   the practice looks due, the drawdown kill, the bench, the roster
     6 generate   the registered ideas; the past tests due tonight get their
                  ``running`` rows BEFORE anything is simulated (N is known first)
     7 past       each past test, then each complete family's end-of-grid checks.
                  Self-sizing: after the first 10 it measures the seconds per idea
                  and takes no more than 60% of the time left; the deadline (06:00)
                  stops it between ideas and the rest wait for the next night
     8 fresh      at most 3 fresh looks, best first
     9 board      data/lab/summary.json, runs/<night>/report.md and tests.csv
    10 upload     those three to the reports branch (reports/lab/<night>/), the
                  GitHub token read by the report uploader's own reader

Aaron, 10 Oct 2026: "the quicker we can find winning trade functions the sooner
we can go live with real money" - and "ensure that consistency stays, luck
doesn't": the computer part is fast, the calendar part is never rushed.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import shutil
import signal
import subprocess
import sys
import traceback
from pathlib import Path
from typing import Callable, Optional, Sequence

from ..clock import TZ_LONDON
from . import (UTC, LabPaths, LabSettings, atomic_write, atomic_write_json, utc_stamp)
from . import features as FEAT
from . import report as REPORT
from . import stages as ST
from .cut import ensure_cut, load_cut, plan_roll, sync_fresh_used
from .families import all_families
from .fetch import update as fetch_update
from .fetch import window_budget_s
from .ledger import LabLedger
from .status import pid_alive
from .store import BarStore, ensure_universe, load_universe

STEPS = ("universe", "fetch", "cut", "features", "practice", "generate", "past", "fresh", "refresh", "board",
         "upload")
REFRESH_WEEKDAY = 5                   # Saturday's night: the week has closed and Friday's weekly pick is made
MIN_FREE_BYTES = 1024 ** 3            # 1 GB
LOCK_STALE_S = 12 * 3600              # a lock older than this is a dead night's, unless its pid
                                      # is still a mintel.lab.nightly process
LONGEST_RUN_S = LOCK_STALE_S - 3600   # a night's own deadline is never later than this after its start
PRACTICE_DAY = ((6, 0), (23, 10))     # London, Monday to Friday: the day practice is still trading
BATCH_SHARE = 0.6                     # of the time left, after the first ten are measured
MEASURE_FIRST = 10


class Stopped(Exception):
    """data/lab/STOP was found: the night stops cleanly where it is."""


def _london_deadline(start: dt.datetime, hhmm: str) -> dt.datetime:
    """The first ``hhmm`` (London time) after ``start``."""
    hh, mm = (int(x) for x in hhmm.split(":"))
    local = start.astimezone(TZ_LONDON)
    target = local.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target <= local:
        target = (local + dt.timedelta(days=1)).replace(hour=hh, minute=mm, second=0, microsecond=0)
    return target.astimezone(UTC)


def practice_day_hours(now: dt.datetime) -> bool:
    """Monday to Friday, 06:00 to 23:10 London: the day is not over, so the night's
    run would judge a part day and fetch nothing. The lab does not run then."""
    local = now.astimezone(TZ_LONDON)
    return local.weekday() < 5 and PRACTICE_DAY[0] <= (local.hour, local.minute) < PRACTICE_DAY[1]


def _pid_command(pid: int) -> Optional[str]:
    """The command line of a process (``ps``: the Mac has no /proc), or None if unknown."""
    try:
        out = subprocess.run(["ps", "-p", str(int(pid)), "-o", "command="], capture_output=True,
                             text=True, timeout=10)
    except (OSError, ValueError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() if out.returncode == 0 else None


def is_lab_night(pid: int) -> bool:
    """Is ``pid`` a running ``mintel.lab.nightly`` (or ``mintel.lab nightly``)?"""
    cmd = _pid_command(pid) or ""
    return "mintel.lab" in cmd and "nightly" in cmd


def _lock_held(pid: int, since: Optional[dt.datetime], now: dt.datetime) -> bool:
    if not pid or not pid_alive(pid):
        return False
    if since is not None and (now - since).total_seconds() < LOCK_STALE_S:
        return True
    return is_lab_night(pid)          # an old lock whose night is still alive is never taken over


# ------------------------------------------------------------------ the lock --
def acquire_lock(paths: LabPaths, now: dt.datetime) -> tuple:
    """(True, "") with the lock taken, or (False, why) when a lab night is running."""
    paths.root.mkdir(parents=True, exist_ok=True)
    for _attempt in (1, 2):
        try:
            fd = os.open(str(paths.lock), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            try:
                held = json.loads(paths.lock.read_text(encoding="utf-8"))
                pid = int(held.get("pid") or 0)
                since = dt.datetime.fromisoformat(held.get("started_utc"))
            except (OSError, ValueError, TypeError):
                pid, since = 0, None
            if pid and pid != os.getpid() and _lock_held(pid, since, now):
                return False, f"another lab night is running (pid {pid})"
            try:
                paths.lock.unlink()               # a dead (or stale) night's lock
            except OSError:
                pass
            continue
        with os.fdopen(fd, "w") as fh:
            json.dump({"pid": os.getpid(), "started_utc": utc_stamp(now)}, fh)
        return True, ""
    return False, "the lock could not be taken"


def release_lock(paths: LabPaths) -> None:
    try:
        held = json.loads(paths.lock.read_text(encoding="utf-8"))
        if int(held.get("pid") or 0) == os.getpid():
            paths.lock.unlink()
    except (OSError, ValueError, TypeError):
        pass


def nightly_running(paths: LabPaths, now: Optional[dt.datetime] = None) -> bool:
    """Is a lab night running now (a live pid in a recent lock)?"""
    now = now or dt.datetime.now(UTC)
    try:
        held = json.loads(paths.lock.read_text(encoding="utf-8"))
        since = dt.datetime.fromisoformat(held.get("started_utc"))
        return _lock_held(int(held.get("pid") or 0), since, now)
    except (OSError, ValueError, TypeError):
        return False


def night_done(paths: LabPaths, night: str) -> bool:
    return (paths.run_dir(night) / "done").exists()


def due(data_dir, now: Optional[dt.datetime] = None) -> tuple:
    """(True, night) when tonight's run has not finished and none is running."""
    now = now or dt.datetime.now(UTC)
    paths = LabPaths(data_dir)
    night = ST.night_of(now)
    if night_done(paths, night):
        return False, night
    if nightly_running(paths, now):
        return False, night
    return True, night


# ------------------------------------------------------------------ the run --
class Night:
    """One night's run. Every outside thing is injectable for the tests: the wall
    clock (``now_fn``), the price feed (``feed_factory``), the uploader and the
    token reader, and the log."""

    def __init__(self, cfg, config_path: str, *, deadline: Optional[str] = None,
                 max_candidates: Optional[int] = None, no_fetch: bool = False, upload: bool = False,
                 workers: int = 1, now_fn: Optional[Callable[[], dt.datetime]] = None,
                 feed_factory: Optional[Callable] = None, upload_fn: Optional[Callable] = None,
                 token_fn: Optional[Callable] = None, log: Optional[Callable[[str], None]] = None,
                 disk_free: Optional[Callable[[Path], int]] = None, fetch_options: Optional[dict] = None):
        self.cfg = cfg
        self.config_path = config_path
        self.paths = LabPaths(cfg.ops.data_dir, cfg.ops.log_dir)
        self.settings = LabSettings.load(self.paths.data_dir)
        self.now_fn = now_fn or (lambda: dt.datetime.now(UTC))
        self.max_candidates = max_candidates
        self.no_fetch = no_fetch
        self.upload = upload
        self.workers = workers
        self.feed_factory = feed_factory
        self.upload_fn = upload_fn
        self.token_fn = token_fn
        self._log = log or (lambda msg: print(msg, flush=True))
        self.disk_free = disk_free or (lambda p: shutil.disk_usage(str(p)).free)
        self.fetch_options = dict(fetch_options or {})       # the tests' fake sleep and clock
        self.started = self.now_fn()
        self.night = ST.night_of(self.started)
        self.deadline = min(_london_deadline(self.started, deadline or self.settings.deadline_local),
                            self.started + dt.timedelta(seconds=LONGEST_RUN_S))
        self.notes: list = []
        self.ledger: Optional[LabLedger] = None
        self.store: Optional[BarStore] = None
        self.universe: list = []
        self.cut = None
        self.calendar = None
        self._feed = None
        self.window = None
        self.tested = 0
        self.durations: list = []
        self.deadline_hit = False
        self.fresh_tonight: list = []
        self.practice_tonight: Optional[dict] = None
        self.waiting = 0
        self.state: dict = {}

    # ------------------------------------------------------------- helpers --
    def log(self, msg: str) -> None:
        self._log(f"{utc_stamp(self.now_fn())} lab: {msg}")

    def note(self, msg: str) -> None:
        self.notes.append(msg)
        self.log(f"note: {msg}")

    def stop_requested(self) -> bool:
        return self.paths.stop.exists()

    def time_left(self) -> float:
        return (self.deadline - self.now_fn()).total_seconds()

    def out_of_time(self) -> bool:
        return self.time_left() <= 0

    def feed(self):
        if self._feed is None:
            if self.feed_factory is not None:
                self._feed = self.feed_factory()
            else:
                from .prices import from_config
                self._feed = from_config(self.cfg)
        return self._feed

    def _feed_ok(self) -> tuple:
        """(ping info, "") when MetaTrader answers through the bridge, else (None, why)."""
        try:
            info = self.feed().ping()
        except Exception as exc:
            return None, f"the price feed could not be asked ({exc})"
        if not info:
            return None, "the price feed is not connected"
        if isinstance(info, dict) and info.get("mt5_connected") is False:
            return None, "MetaTrader is not connected"
        return info, ""

    def _calendar(self):
        p = Path(self.cfg.news.store_path)
        path = p if p.is_absolute() else self.paths.data_dir / p.name
        if not path.exists():
            return None
        try:
            from ..news.calendar_store import CalendarStore
            return CalendarStore(path)
        except Exception as exc:
            self.note(f"the news calendar could not be read ({exc}); news-aware ideas wait")
            return None

    def names(self) -> list:
        return [e.name for e in self.universe]

    # --------------------------------------------------------------- state --
    def _state_path(self) -> Path:
        return self.paths.run_dir(self.night) / "state.json"

    def _load_state(self) -> None:
        try:
            self.state = json.loads(self._state_path().read_text(encoding="utf-8"))
        except (OSError, ValueError):
            self.state = {}
        self.state.setdefault("night", self.night)
        self.state.setdefault("started_utc", utc_stamp(self.started))
        self.state.setdefault("steps", {})

    def _save_state(self) -> None:
        atomic_write_json(self._state_path(), self.state)

    def _done(self, step: str) -> bool:
        return bool((self.state.get("steps", {}).get(step) or {}).get("done"))

    # ----------------------------------------------------------------- run --
    def run(self) -> int:
        paths = self.paths
        if practice_day_hours(self.started) and not os.environ.get("MINTEL_LAB_ANY_HOUR"):   # the tests' own
            self.log("the lab runs between 23:10 and 06:00 on weekdays (any time at the weekend): "
                     "today's practice is not over yet, so nothing is run now")
            return 0
        if night_done(paths, self.night):
            self.log(f"the night of {self.night} is already done")
            return 0
        if self.settings.mode == "OFF":
            self.log("the lab is paused (data/lab.json says OFF): nothing run tonight")
            return 0
        if self.stop_requested():
            self.log("data/lab/STOP is there: nothing run (delete it to let the lab run)")
            return 0
        took, why = acquire_lock(paths, self.started)
        if not took:
            self.log(f"{why}: this one leaves it to finish")
            return 0
        try:
            return self._run_locked()
        finally:
            release_lock(paths)

    def _run_locked(self) -> int:
        paths = self.paths
        free = self.disk_free(paths.root)
        if free < MIN_FREE_BYTES:
            self.log(f"only {free / 1024 ** 3:.2f} GB free on the disk (needs 1 GB): nothing run tonight")
            return 1
        if self.workers != 1:
            self.note(f"--workers {self.workers} asked for; the lab runs one worker on this Mac")
        self._load_state()
        self.ledger = LabLedger(paths.sqlite, paths.trials)
        self.store = BarStore(paths)
        crashed = self.ledger.crash_all_running("a lab night was cut short", now=self.now_fn())
        if crashed:
            self.log(f"{crashed} test(s) left running by an earlier run are marked crashed (they still count)")
        spent = ST.bin_spent_looks(self.ledger, now=self.now_fn(), log=self.log)
        if spent:
            self.note(f"{len(spent)} idea(s) binned: a fresh look was cut short, so the one look is spent")
        self.ledger.upsert_run(self.night, started_utc=self.state["started_utc"], ok=None, step="start")
        self.log(f"the night of {self.night}: deadline {self.deadline.astimezone(TZ_LONDON):%H:%M} London")
        self.calendar = self._calendar()
        stopped = completed = False
        try:
            for name in STEPS:
                if self.stop_requested():
                    raise Stopped()
                self._step(name)
            completed = True
        except Stopped:
            stopped = True
            self.log("data/lab/STOP found: stopping cleanly (the night is not finished; it resumes next time)")
        finally:
            if completed or stopped:
                self._finish(stopped)
            else:                                   # interrupted (a signal, a fault): resumes next time
                try:
                    self._record_run(finished=False, ok=False, step="interrupted")
                except Exception:
                    pass
            try:
                self.ledger.close()
            except Exception:
                pass
            if self.calendar is not None:
                try:
                    self.calendar.close()
                except Exception:
                    pass
        return 0

    def _step(self, name: str) -> None:
        # the universe, the cut and the practice roster are cheap and needed by later steps,
        # so they are read again on a resumed night even when their step is done
        if self._done(name) and name not in ("universe", "cut"):
            self.log(f"step {name}: already done tonight")
            return
        t0 = self.now_fn()
        self.ledger.upsert_run(self.night, step=name)
        ok, note = True, ""
        try:
            note = getattr(self, f"step_{name}")() or ""
        except Stopped:
            raise
        except Exception as exc:
            ok = False
            note = f"failed: {type(exc).__name__}: {exc}"
            self.note(f"step {name} {note}")
            self.log(traceback.format_exc().rstrip())
        secs = (self.now_fn() - t0).total_seconds()
        self.log(f"step {name}: {note or 'done'} ({secs:.1f} s)")
        self.state["steps"][name] = {"done": ok, "seconds": round(secs, 2), "note": note,
                                     "at_utc": utc_stamp(self.now_fn())}
        self._save_state()

    # ------------------------------------------------------------- steps --
    def step_universe(self) -> str:
        entries = load_universe(self.paths, self.settings)
        if entries:
            self.universe = entries
            return f"{len(entries)} markets (fixed once written)"
        info, why = self._feed_ok()
        if info is None:
            raise RuntimeError(f"no universe yet: {why}")
        entries, created = ensure_universe(self.paths, self.feed(), self.settings, now=self.now_fn())
        self.universe = entries
        if not entries:
            raise RuntimeError("the broker's list held none of the lab's kinds of market")
        return f"{len(entries)} markets" + (" (chosen tonight, fixed from now on)" if created else "")

    def step_fetch(self) -> str:
        if self.no_fetch:
            return "skipped (--no-fetch)"
        if not self.universe:
            return "skipped: no universe"
        info, why = self._feed_ok()
        if info is None:
            return f"skipped: {why}"
        now = self.now_fn()
        budget = min(float(window_budget_s(now) or 0.0), max(0.0, self.time_left()))
        rep = fetch_update(self.feed(), self.store, now=now, budget_s=budget, log=self.log,
                           symbols=self.names(), history_days=self.settings.history_days,
                           max_mb=self.settings.max_mb, **self.fetch_options)
        if rep.skipped:
            return f"skipped: {rep.skipped}"
        for err in rep.errors:
            self.note(f"fetch warning: {err}")
        self.store.refresh()
        return (f"{rep.pages} pages, {rep.bars_added} bars added"
                + (f"; stopped: {rep.stopped}" if rep.stopped else "")
                + (f"; {len(rep.errors)} warning(s)" if rep.errors else ""))

    def step_cut(self) -> str:
        if not self.universe:
            self.cut = load_cut(self.paths)
            return "skipped: no universe"
        days = self.store.trading_days(self.names())
        _target, awake, ids = plan_roll(days, self.calendar)
        state, moved = ensure_cut(self.paths, days, ids, self.now_fn(), ledger=self.ledger,
                                  fresh_cap=self.settings.fresh_cap_per_stretch, news_awake=awake)
        self.cut = sync_fresh_used(self.paths, self.ledger) or state
        if self.cut is None:
            return f"collecting past prices: {len(days)} of 40 days"
        back = ST.retry_too_few(self.ledger, self.cut, now=self.now_fn(), log=self.log)
        return (f"{'moved to ' if moved else ''}cut {self.cut.cut_day}, stretch {self.cut.stretch_id}, "
                f"{self.cut.fresh_used} of {self.cut.fresh_cap} fresh looks used"
                + (f"; {len(back)} back for their one more go" if back else ""))

    def step_features(self) -> str:
        if not self.universe or self.cut is None:
            return "skipped: nothing to test yet"
        built = 0
        for e in self.universe:
            if self.stop_requested():
                raise Stopped()
            if self.out_of_time():
                return f"{built} month-sets built; stopped at the deadline"
            news = FEAT.news_inputs(self.calendar, e.name, e.spec) if self.calendar is not None else None
            # without a calendar the news numbers cannot be kept (they would only be worked out again)
            sets = FEAT.SETS if news is not None else tuple(x for x in FEAT.SETS if x != "news")
            built += FEAT.ensure_months(self.store, [e.name], sets, news_by_symbol={e.name: news})
        freed = FEAT.prune_cache(self.store)
        return f"{built} month-sets built" + (f", {freed // 1024} KB of old cache freed" if freed else "")

    def step_practice(self) -> str:
        out = ST.grade_practice(self.ledger, self.universe, self.settings, now=self.now_fn(), log=self.log)
        self.practice_tonight = out
        return (f"judged up to {out['judge_day']}: {len(out['looks'])} look(s), {len(out['ready'])} ready, "
                f"{len(out['binned'])} binned, {len(out['bench_binned'])} off the bench, "
                f"{len(out['promoted'])} promoted")

    def step_refresh(self) -> str:
        """Saturday: the weekly refresh of the practice pool (``refresh.py``)."""
        if dt.date.fromisoformat(self.night).weekday() != REFRESH_WEEKDAY:
            return "skipped: the pool is refreshed on Saturday nights"
        from .refresh import refresh_pool
        rep = refresh_pool(self.ledger, self.paths, self.settings, night=self.night, now=self.now_fn(),
                           news_awake=bool(self.cut.news_awake) if self.cut else False, log=self.log)
        return (f"{len(rep['retired'])} retired, {len(rep['variants'])} variants, {len(rep['new'])} new; "
                f"pool {rep['pool']} of {rep['target']}")

    def _covered_past_days(self) -> int:
        if self.calendar is None or self.cut is None:
            return 0
        days = [d for d in self.store.trading_days(self.names()) if d < self.cut.cut_day]
        try:
            return len(FEAT.covered_days(self.calendar, days))
        except Exception:
            return 0

    def step_generate(self) -> str:
        if self.cut is None:
            return "skipped: no cut yet (still collecting past prices)"
        gen = ST.generate(self.ledger, self.cut, covered_past_days=self._covered_past_days(),
                          now=self.now_fn(), limit=self.max_candidates, log=self.log)
        self.waiting = len(gen.waiting)
        self.state["waiting_for_calendar"] = self.waiting
        if gen.grid_note:
            self.note(gen.grid_note)
        return (f"{gen.registered} new idea(s); {len(gen.begun)} past test(s) committed for tonight "
                f"({gen.due} due); {len(gen.waiting)} waiting for the calendar")

    def _past_window(self):
        if self.window is None:
            self.window = self.store.past_window(self.universe, calendar=self.calendar)
        return self.window

    def step_past(self) -> str:
        if self.cut is None:
            return "skipped: no cut yet"
        pending = ST.pending_past(self.ledger, self.cut)
        total = len(pending)
        covered = self._covered_past_days()
        ended: set = set()
        allowed: Optional[int] = None
        prev_family = None

        def end_family(code: str) -> None:
            if code in ended:
                return
            if ST.family_complete(self.ledger, code, self.cut, covered_past_days=covered):
                ST.family_end(self.ledger, code, self.cut, window=self._past_window(), now=self.now_fn(),
                              log=self.log, should_stop=lambda: self.out_of_time() or self.stop_requested())
                ended.add(code)

        reason = ""
        for test_id, row in pending:
            if self.stop_requested():
                raise Stopped()
            if self.out_of_time():
                self.deadline_hit = True
                reason = "deadline"
                break
            if allowed is not None and self.tested >= allowed:
                reason = "batch"
                break
            if prev_family is not None and row["family"] != prev_family:
                end_family(prev_family)
                if self.window is not None:
                    self.window.drop_features()
            prev_family = row["family"]
            t0 = self.now_fn()
            ST.run_past(self.ledger, row, test_id, self._past_window(), self.cut, now=self.now_fn(),
                        log=self.log)
            self.durations.append((self.now_fn() - t0).total_seconds())
            self.tested += 1
            if allowed is None and self.tested >= MEASURE_FIRST:
                spc = sum(self.durations) / len(self.durations)
                room = BATCH_SHARE * max(0.0, self.time_left())
                allowed = self.tested + (int(math.floor(room / spc)) if spc > 0 else total)
                self.log(f"past: {spc:.2f} s per idea measured; tonight's batch is {min(allowed, total)} "
                         f"of {total}")
        if prev_family is not None and not reason:
            end_family(prev_family)
        # families with nothing to run tonight still get their end-of-grid checks once complete
        if not self.stop_requested():
            for fam in all_families(bool(self.cut.news_awake)):
                end_family(fam.code)
        if reason == "deadline":
            local = self.deadline.astimezone(TZ_LONDON)
            msg = (f"ran out of time at {local:%H:%M}: {self.tested} of {total} tested; the rest tonight")
            self.note(msg)
            return msg
        if reason == "batch":
            msg = f"{self.tested} of {total} tested (the night's measured batch); the rest tomorrow night"
            self.note(msg)
            return msg
        return f"{self.tested} of {total} tested"

    def step_fresh(self) -> str:
        if self.cut is None:
            return "skipped: no cut yet"
        self.fresh_tonight = ST.fresh_looks(
            self.ledger, self.cut, self.store, self.universe, self.settings, calendar=self.calendar,
            now=self.now_fn(), log=self.log, should_stop=self.stop_requested)
        self.cut = sync_fresh_used(self.paths, self.ledger) or self.cut
        passed = sum(1 for f in self.fresh_tonight if f.get("passed"))
        return f"{len(self.fresh_tonight)} fresh look(s), {passed} passed"

    def _news_from(self) -> Optional[str]:
        if self.calendar is None:
            return None
        try:
            cov = self.calendar.coverage()
            return cov.start.isoformat() if cov.start else None
        except Exception:
            return None

    def _record_run(self, *, finished: bool, ok: bool, step: str) -> None:
        secs = (self.now_fn() - self.started).total_seconds()
        spc = (sum(self.durations) / len(self.durations)) if self.durations else None
        self.ledger.upsert_run(self.night, finished_utc=utc_stamp(self.now_fn()) if finished else None,
                               ok=int(ok) if finished else None, step=step, tested=self.tested,
                               fresh=len(self.fresh_tonight), seconds=round(secs, 1),
                               seconds_per_candidate=None if spc is None else round(spc, 3),
                               deadline_hit=int(self.deadline_hit), note="; ".join(self.notes)[:2000])

    def step_board(self) -> str:
        if self.cut is None:
            self.cut = load_cut(self.paths)
        failed = [k for k, v in self.state.get("steps", {}).items() if not v.get("done")]
        self._record_run(finished=True, ok=not failed, step="board")      # final figures follow at the end
        written = REPORT.write_board(
            self.paths, self.ledger, night=self.night, settings=self.settings, universe=self.universe,
            store=self.store, cut=self.cut, now=self.now_fn(), run_started=self.state["started_utc"],
            waiting=int(self.state.get("waiting_for_calendar") or self.waiting) + self._dormant_waiting(),
            news_from=self._news_from(), notes=self.notes,
            fresh_tonight=self.fresh_tonight, practice_tonight=self.practice_tonight)
        return f"wrote {', '.join(str(Path(p).name) for p in written.values())}"

    def _dormant_waiting(self) -> int:
        awake = bool(self.cut.news_awake) if self.cut else False
        return sum(f.GRID_SIZE for f in all_families(True) if f.dormant and not awake)

    def step_upload(self) -> str:
        if not self.upload:
            return "skipped (no --upload)"
        from ..ops.report_upload import UploadIncomplete, read_token, upload
        token = (self.token_fn or read_token)(self.config_path)
        if not token:
            return "not published: no GitHub token saved"
        files = REPORT.upload_files(self.paths, self.night)
        try:
            n = (self.upload_fn or upload)(files, self.cfg.report_repo, self.cfg.report_branch, token,
                                           day=f"lab/{self.night}")
        except UploadIncomplete as exc:
            self.note(f"published {exc.written} of {len(files)} files")
            return f"published {exc.written} of {len(files)}"
        except Exception as exc:
            self.note(f"not published: {exc}")
            return f"not published: {exc}"
        return f"published {n} file(s) to {self.cfg.report_repo} ({self.cfg.report_branch})"

    # ------------------------------------------------------------ the end --
    def _finish(self, stopped: bool) -> None:
        failed = [k for k, v in self.state.get("steps", {}).items() if not v.get("done")]
        ok = not failed and not stopped
        if stopped:
            self._record_run(finished=False, ok=False, step="stopped")
            return
        self._record_run(finished=True, ok=ok, step="done")
        atomic_write(self.paths.run_dir(self.night) / "done", utc_stamp(self.now_fn()) + "\n")
        took = (self.now_fn() - self.started).total_seconds()
        self.log(f"the night of {self.night} is done in {took / 60:.1f} min"
                 + (f" ({len(failed)} step(s) failed: {', '.join(failed)})" if failed else ""))


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="the lab's night: tests ideas on past, then fresh, prices")
    ap.add_argument("--config", default="data/config.json")
    ap.add_argument("--upload", action="store_true", help="publish the board to the reports branch")
    ap.add_argument("--workers", type=int, default=1, help="1 (the only value used on the Mac)")
    ap.add_argument("--deadline", default=None, help="HH:MM London time (default: data/lab.json, 06:00)")
    ap.add_argument("--max-candidates", type=int, default=None, help="at most this many past tests tonight")
    ap.add_argument("--no-fetch", action="store_true", help="do not fetch prices tonight")
    args = ap.parse_args(argv)
    from ..config import Config
    cfg = Config.load(args.config)
    if args.deadline:
        try:
            hh, mm = args.deadline.split(":")
            assert 0 <= int(hh) <= 23 and 0 <= int(mm) <= 59
        except Exception:
            print(f"--deadline must be HH:MM, not {args.deadline!r}", file=sys.stderr)
            return 2
    night = Night(cfg, args.config, deadline=args.deadline, max_candidates=args.max_candidates,
                  no_fetch=args.no_fetch, upload=args.upload, workers=args.workers)

    def _terminated(_signum, _frame):
        raise SystemExit(143)               # the lock is released and the night resumes next time

    try:
        signal.signal(signal.SIGTERM, _terminated)
    except (ValueError, OSError):
        pass
    return night.run()


if __name__ == "__main__":
    raise SystemExit(main())
