"""The cut: the one date that splits PAST prices from FRESH ones.

Everything before the cut day may be used to choose and tune; everything from
it is kept back and looked at once. ``data/lab/cut.json`` holds the cut, the
stretch it opens and the hash of the grid registered for that stretch.

The rules (Aaron, 10 Oct 2026: "ensure that consistency stays, luck doesn't"):

* The cut only ever moves FORWARD, and only once a month (the first nightly of
  each month, and the first run ever), and only when the store holds at least
  40 complete trading days. ``set_cut`` refuses an earlier date.
* fresh days = clamp(round(0.25 x D), 20, 45) of the D complete trading days;
  the cut day is that many days back from the latest.
* A move opens a NEW stretch id, resets the fresh looks used, records the hash
  of every registered definition id (so N is known before anything runs and
  cannot grow mid-stretch), and marks every candidate still waiting at PAST
  TEST as due a retest on the new cut. Fresh verdicts already given stand.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Iterable, Optional, Sequence

from . import (FRESH_DAYS_MAX, FRESH_DAYS_MIN, FRESH_DAYS_SHARE, FRESH_LOOKS_PER_STRETCH,
                NEEDED_DAYS, SHORT_HISTORY_DAYS, LabPaths, atomic_write_json, clamp_fresh_cap,
                utc_stamp)


class CutError(RuntimeError):
    """The cut refuses to move that way."""


class GridChanged(CutError):
    """The registered grid is not the one this stretch was opened with."""


@dataclass
class CutState:
    cut_day: str
    stretch_id: str
    set_utc: str
    grid_hash: str
    fresh_used: int = 0                # mirrored from the ledger (the ledger is the truth)
    fresh_cap: int = FRESH_LOOKS_PER_STRETCH
    history: list = field(default_factory=list)
    news_awake: bool = False           # was NEWS_MOVE registered when this stretch opened
    grid_n: int = 0
    complete_days: int = 0             # D when the cut was set
    fresh_days: int = 0
    short_history: bool = False        # fewer than 90 complete days: results are weaker

    def __post_init__(self):
        # cut.json can be edited by hand; the cap is never allowed above 30 (see clamp_fresh_cap)
        self.fresh_cap = clamp_fresh_cap(self.fresh_cap)

    @property
    def fresh_left(self) -> int:
        return max(0, self.fresh_cap - self.fresh_used)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "CutState":
        known = cls.__dataclass_fields__
        return cls(**{k: v for k, v in d.items() if k in known})


def load_cut(paths: LabPaths) -> Optional[CutState]:
    try:
        raw = json.loads(paths.cut.read_text(encoding="utf-8"))
        return CutState.from_dict(raw)
    except (OSError, ValueError, TypeError):
        return None


def save_cut(paths: LabPaths, state: CutState) -> None:
    atomic_write_json(paths.cut, state.to_dict())


def fresh_days_for(d: int) -> int:
    """How many of the D complete trading days are kept back as fresh."""
    return int(min(FRESH_DAYS_MAX, max(FRESH_DAYS_MIN, round(FRESH_DAYS_SHARE * d))))


def cut_day_for(days: Sequence[str]) -> Optional[str]:
    """The cut day for a sorted list of complete trading days: the fresh_days-th
    one counted back from the latest. None while the lab is still collecting
    (fewer than 40 days)."""
    d = len(days)
    if d < NEEDED_DAYS:
        return None
    return days[-fresh_days_for(d)]


def grid_hash(ids: Iterable[str]) -> str:
    """sha1 of every registered definition id (sorted): the identity of a grid."""
    h = hashlib.sha1()
    for i in sorted(set(ids)):
        h.update(i.encode("ascii"))
        h.update(b"\n")
    return h.hexdigest()


def plan_roll(trading_days: Sequence[str], calendar=None) -> tuple:
    """What a roll would register: (target cut day or None, news_awake, grid ids).

    NEWS_MOVE joins the grid only at a roll, and only when at least 40 trading
    days of the PAST stretch it would open lie inside the calendar's covered
    span. Hand the ids and the flag to ``ensure_cut``."""
    from .candidate import news_ready, registered_ids
    days = sorted(trading_days)
    target = cut_day_for(days)
    awake = False
    if target is not None:
        awake = news_ready(calendar, [d for d in days if d < target])
    return target, awake, registered_ids(awake)


def roll_due(state: Optional[CutState], now: dt.datetime) -> bool:
    """True on the first nightly of a calendar month (or the first run ever)."""
    if state is None:
        return True
    try:
        then = dt.datetime.fromisoformat(state.set_utc)
    except ValueError:
        return True
    return (then.year, then.month) != (now.year, now.month)


def _new_stretch_id(now: dt.datetime, taken: Iterable[str]) -> str:
    base = f"F-{now.year:04d}-{now.month:02d}"
    taken = set(taken)
    if base not in taken:
        return base
    for suffix in "bcdefghijklmnopqrstuvwxyz":
        if base + suffix not in taken:
            return base + suffix
    raise CutError("too many stretches in one month")


def set_cut(paths: LabPaths, new_cut_day: str, *, grid_ids: Iterable[str], now: dt.datetime,
            fresh_cap: int = FRESH_LOOKS_PER_STRETCH, ledger=None, complete_days: int = 0,
            fresh_days: int = 0, news_awake: bool = False) -> CutState:
    """Move the cut to ``new_cut_day``. Refuses an earlier date; the same day is
    no move (the current state is returned untouched). A real move opens a new
    stretch: resets the looks used, records the grid hash, and marks the
    candidates still waiting at PAST TEST as due a retest."""
    old = load_cut(paths)
    if old is not None:
        if new_cut_day < old.cut_day:
            raise CutError(f"the cut only moves forward: {new_cut_day} is earlier than "
                           f"{old.cut_day}")
        if new_cut_day == old.cut_day:
            return old
    ids = list(grid_ids)
    taken = [h["stretch_id"] for h in (old.history if old else [])]
    if old:
        taken.append(old.stretch_id)
    sid = _new_stretch_id(now, taken)
    history = list(old.history) if old else []
    if old:
        history.append({"stretch_id": old.stretch_id, "cut_day": old.cut_day,
                        "set_utc": old.set_utc, "grid_hash": old.grid_hash,
                        "fresh_used": (ledger.fresh_used(old.stretch_id) if ledger is not None
                                       else old.fresh_used)})
    state = CutState(cut_day=new_cut_day, stretch_id=sid, set_utc=utc_stamp(now),
                     grid_hash=grid_hash(ids), fresh_used=0, fresh_cap=clamp_fresh_cap(fresh_cap),
                     history=history, news_awake=bool(news_awake), grid_n=len(set(ids)),
                     complete_days=int(complete_days), fresh_days=int(fresh_days),
                     short_history=bool(complete_days and complete_days < SHORT_HISTORY_DAYS))
    save_cut(paths, state)
    if ledger is not None:
        ledger.mark_retest_due()
    return state


def ensure_cut(paths: LabPaths, trading_days: Sequence[str], grid_ids: Iterable[str],
               now: dt.datetime, *, ledger=None, fresh_cap: int = FRESH_LOOKS_PER_STRETCH,
               news_awake: bool = False) -> tuple:
    """Nightly step 3. Returns (state_or_None, moved).

    Sets the first cut once the store holds 40 complete trading days, and rolls
    it forward on the first nightly of each month. Never backwards, never twice
    a month, never with fewer than 40 days."""
    days = sorted(trading_days)
    state = load_cut(paths)
    if len(days) < NEEDED_DAYS or not roll_due(state, now):
        return state, False
    target = cut_day_for(days)
    if target is None or (state is not None and target <= state.cut_day):
        return state, False
    moved = set_cut(paths, target, grid_ids=grid_ids, now=now, fresh_cap=fresh_cap,
                    ledger=ledger, complete_days=len(days),
                    fresh_days=fresh_days_for(len(days)), news_awake=news_awake)
    return moved, True


def check_grid(state: CutState, grid_ids: Iterable[str]) -> None:
    """The registered set must be the one the stretch opened with. A changed
    grid mid-stretch is refused: N would grow under the luck rules' feet."""
    if grid_hash(grid_ids) != state.grid_hash:
        raise GridChanged(
            f"the registered grid differs from stretch {state.stretch_id}'s; grid "
            f"extensions are only registered at a stretch roll")


def sync_fresh_used(paths: LabPaths, ledger) -> Optional[CutState]:
    """Mirror the ledger's count of fresh looks into cut.json (the page reads it)."""
    state = load_cut(paths)
    if state is None:
        return None
    used = ledger.fresh_used(state.stretch_id)
    if used != state.fresh_used:
        state.fresh_used = used
        save_cut(paths, state)
    return state
