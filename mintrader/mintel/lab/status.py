"""The lab's two small public files, and the one line that sums the lab up.

``data/lab-status.json`` is written by the practice process every 30 seconds
(atomically). It is how the watchdog knows the practice process is alive: the
lab writes NO heartbeat file (a stale heartbeat the trader does not know would
stop its entries), so the watchdog asks ``status_fresh`` instead, exactly as it
asks the bridge whether it answers.

``data/lab/summary.json`` is written by the nightly's board step; the practice
process refreshes only its practice figures. Both writers take the same small
lock so neither can overwrite the other's newer copy.

``one_line`` is the lab in a sentence, for the launcher's health check, the
desktop icon and ``python -m mintel.lab show --one-line``:

    The lab: practising 2 ideas; last night 632 tested, 3 fresh looks, 0 passed

Aaron, 10 Oct 2026: "put it all nice and clear in layman terms".
"""
from __future__ import annotations

import contextlib
import datetime as dt
import json
import os
from pathlib import Path
from typing import Callable, Optional

from . import NEEDED_DAYS, UTC, LabPaths, LabSettings, atomic_write_json, utc_stamp

STATUS_MAX_AGE_S = 180.0              # the watchdog's limit: older than this, it is not answering
STATUS_EVERY_S = 30.0                 # how often the practice process writes it
TEST_DATA = "TEST DATA - not a result"

# only these keys of an open practice position leave the process
OPEN_KEYS = ("id", "name", "symbol", "side", "volume", "entry", "stop", "target", "opened_utc",
             "r_now")


# ------------------------------------------------------------ the status file --
def write_status(paths: LabPaths, payload: dict, now: Optional[dt.datetime] = None) -> dict:
    """Write ``data/lab-status.json`` whole (tmp + replace), stamped now."""
    out = dict(payload)
    out["updated_utc"] = utc_stamp(now)
    out["open_positions"] = [{k: p[k] for k in OPEN_KEYS if k in p}
                             for p in out.get("open_positions") or [] if isinstance(p, dict)]
    atomic_write_json(paths.status, out)
    return out


def read_json(path: Path) -> tuple:
    """(dict, "") or (None, why not). Never raises."""
    try:
        raw = Path(path).read_text(encoding="utf-8")
    except FileNotFoundError:
        return None, f"{Path(path).name} does not exist yet"
    except OSError as exc:
        return None, f"{Path(path).name} could not be read ({exc})"
    try:
        data = json.loads(raw)
    except ValueError as exc:
        return None, f"{Path(path).name} is not valid JSON ({exc})"
    if not isinstance(data, dict):
        return None, f"{Path(path).name} holds no object"
    return data, ""


def status_age(path: Path, now: Optional[dt.datetime] = None) -> Optional[float]:
    """Seconds since the status file was written, by its own stamp (None if unreadable)."""
    data, _why = read_json(path)
    if data is None:
        return None
    try:
        t = dt.datetime.fromisoformat(str(data.get("updated_utc")).replace("Z", "+00:00"))
    except ValueError:
        return None
    if t.tzinfo is None:
        t = t.replace(tzinfo=UTC)
    now = now or dt.datetime.now(UTC)
    return (now - t).total_seconds()


def status_fresh(path, max_age_s: float = STATUS_MAX_AGE_S,
                 clock: Optional[Callable[[], dt.datetime]] = None) -> Callable[[], Optional[bool]]:
    """The watchdog's probe for the practice process: True while the status
    file was written within ``max_age_s``; False when it is older, missing or
    unreadable (the watchdog only acts on that after its grace period, so a
    process that has just started is given time to write its first one)."""
    p = Path(path)

    def probe() -> Optional[bool]:
        age = status_age(p, clock() if clock else None)
        if age is None:
            return False
        return age <= max_age_s

    return probe


# ---------------------------------------------------------------- the lock --
@contextlib.contextmanager
def summary_lock(paths: LabPaths):
    """One writer of summary.json at a time (the nightly's board, the practice
    refresh). A no-op where the system has no file locks."""
    try:
        import fcntl
    except ImportError:                      # pragma: no cover - Windows
        yield
        return
    paths.root.mkdir(parents=True, exist_ok=True)
    fh = open(paths.root / ".summary.lock", "a+")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        finally:
            fh.close()


# --------------------------------------------------------------- one line --
def one_line(data_dir, now: Optional[dt.datetime] = None) -> str:
    """The lab in one sentence, from its own files (never a guess)."""
    paths = LabPaths(data_dir)
    settings = LabSettings.load(paths.data_dir)
    if not paths.root.exists():
        return "The lab: has not run yet."
    summary, why = read_json(paths.summary)
    paused = " (paused: data/lab.json says OFF)" if settings.mode == "OFF" else ""
    if summary is None:
        if why.endswith("does not exist yet"):
            return f"The lab: has not finished a night yet{paused}."
        return f"The lab's summary could not be read ({why}){paused}."
    if summary.get("synthetic"):
        return f"The lab: {TEST_DATA}{paused}."
    store = summary.get("store") or {}
    funnel = summary.get("funnel") or {}
    night = summary.get("nightly") or {}
    if not store.get("ready", True) and int(store.get("complete_days") or 0) < NEEDED_DAYS:
        return (f"The lab: collecting past prices: {int(store.get('complete_days') or 0)} of "
                f"{int(store.get('needed_days') or NEEDED_DAYS)} days{paused}.")
    practising = int(funnel.get("practice") or 0) + int(funnel.get("ready") or 0)
    head = f"practising {practising} idea{'' if practising == 1 else 's'}"
    if night.get("date"):
        fp = int(night.get("fresh_passed") or 0)
        tail = (f"last night {int(night.get('tested') or 0)} tested, {int(night.get('fresh') or 0)} "
                f"fresh look{'' if int(night.get('fresh') or 0) == 1 else 's'}, {fp} passed")
        if night.get("missed"):
            tail = "last night's run: missed"
    else:
        tail = "no night has run yet"
    return f"The lab: {head}; {tail}{paused}"


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
