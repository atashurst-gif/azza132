"""The lab-demo roster: which promoted lab ideas trade on the DEMO account.

Two small files in ``data/labdemo/``, both written whole (temporary name, then
renamed into place) and only ever by the weekly pick (``mintel/ops/weekly.py``,
inside the watchdog). The lab-demo engine inside the trader only READS them, and
re-reads the roster whenever it changes, so a promotion or a demotion needs no
restart.

* ``roster.json``: ``{"schema": 1, "written_utc": ..., "ideas": {idea id:
  {"version", "magic", "tier_pct", "since_utc", "plan"}}}``. An idea in it may
  trade; an idea taken out of it has its demo positions closed at once by the
  engine (the clean cut of the bots' LIVE -> PAPER switch).
* ``magics.json``: ``{"next": 992002, "by_key": {"lab-0123456789@v1": 992001}}``.
  Every idea version gets ONE magic number from the reserved range
  ``MAGIC_FIRST..MAGIC_LAST`` (992001-992999), which clashes with no bot's
  (Trend & Breakout 990311, the Runner 990511, Band Breaker 990611, Crowd Fader
  990711, the Rider 990811, Financial Ian 990911, the retired scalper 990411).
  A number is never handed to another idea, so a restart, a demotion and a
  later re-promotion all find the same positions under the same number.

Nothing here talks to a broker. Aaron, 14 Oct 2026: "lab ideas really trade on
the demo".
"""
from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import Optional

SCHEMA = 1
DIR = "labdemo"
ROSTER, MAGICS = "roster.json", "magics.json"
DB = "labdemo.sqlite"
STATUS = "status.json"
MAGIC_FIRST, MAGIC_LAST = 992_001, 992_999
# every other magic number in use in this codebase: the reserved range must never meet one
OTHER_MAGICS = frozenset({990_311, 990_411, 990_511, 990_611, 990_711, 990_811, 990_911})
# the demo risk tiers, % of equity a trade (Aaron, 14 Oct: "aggressive experimentation with different trade
# sizes"): each promotion gets the next one in turn, so the tiers can be compared
TIERS = (0.5, 1.0, 2.0)


def next_tier(n_before: int) -> float:
    """The tier of a lab idea's promotion after ``n_before`` earlier ones: 0.5%, 1%, 2%, 0.5% ..."""
    return TIERS[int(n_before) % len(TIERS)]


class RosterUnreadable(Exception):
    """roster.json or magics.json is there but cannot be read: nothing is decided on a guess."""


def ldir(data_dir) -> Path:
    return Path(data_dir) / DIR


def roster_path(data_dir) -> Path:
    return ldir(data_dir) / ROSTER


def magics_path(data_dir) -> Path:
    return ldir(data_dir) / MAGICS


def key_of(idea_id: str, version) -> str:
    return f"{idea_id}@v{int(version or 1)}"


def _read_obj(path: Path, strict: bool) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        got = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        if strict:
            raise RosterUnreadable(f"{path} could not be read ({exc})") from exc
        return None
    if not isinstance(got, dict):
        if strict:
            raise RosterUnreadable(f"{path} does not hold a JSON object")
        return None
    return got


def read_roster(data_dir, strict: bool = False) -> dict:
    """{idea id: entry}. Missing: nothing on the demo. Broken: ``strict`` raises,
    otherwise empty (a reader that only displays)."""
    got = _read_obj(roster_path(data_dir), strict)
    ideas = (got or {}).get("ideas")
    if ideas is None:
        return {}
    if not isinstance(ideas, dict):
        if strict:
            raise RosterUnreadable(f"{roster_path(data_dir)}: 'ideas' is not an object")
        return {}
    out = {}
    for k, v in ideas.items():
        if not isinstance(v, dict):
            if strict:
                raise RosterUnreadable(f"{roster_path(data_dir)}: the entry for {k} is not an object")
            continue
        try:
            magic = int(v.get("magic") or 0)
        except (TypeError, ValueError):
            magic = 0
        if not (MAGIC_FIRST <= magic <= MAGIC_LAST):
            if strict:
                raise RosterUnreadable(f"{roster_path(data_dir)}: {k} has no magic number in the reserved range")
            continue
        out[str(k)] = dict(v, magic=magic)
    return out


def read_magics(data_dir, strict: bool = False) -> dict:
    got = _read_obj(magics_path(data_dir), strict) or {}
    by = got.get("by_key") if isinstance(got.get("by_key"), dict) else {}
    try:
        nxt = int(got.get("next") or MAGIC_FIRST)
    except (TypeError, ValueError):
        if strict:
            raise RosterUnreadable(f"{magics_path(data_dir)}: 'next' is not a number")
        nxt = MAGIC_FIRST
    clean = {}
    for k, v in by.items():
        try:
            clean[str(k)] = int(v)
        except (TypeError, ValueError):
            if strict:
                raise RosterUnreadable(f"{magics_path(data_dir)}: {k} has no number")
    nxt = max([nxt, MAGIC_FIRST] + [m + 1 for m in clean.values()])
    return {"next": nxt, "by_key": clean}


def all_magics(data_dir) -> dict:
    """{magic: idea key} of every number ever handed out (for the engine's
    orphan sweep and the daily-loss stop): never strict."""
    try:
        return {m: k for k, m in read_magics(data_dir)["by_key"].items()}
    except Exception:
        return {}


def magic_for(magics: dict, idea_id: str, version) -> tuple[int, dict]:
    """(the idea version's magic, the mapping with it added when new). Raises when
    the reserved range is used up or the number would clash."""
    k = key_of(idea_id, version)
    by = dict(magics.get("by_key") or {})
    if k in by:
        return int(by[k]), {"next": int(magics.get("next") or MAGIC_FIRST), "by_key": by}
    m = max(int(magics.get("next") or MAGIC_FIRST), MAGIC_FIRST)
    used = set(by.values())
    while m in used or m in OTHER_MAGICS:
        m += 1
    if m > MAGIC_LAST:
        raise ValueError("the lab-demo magic numbers (992001-992999) are used up")
    by[k] = m
    return m, {"next": m + 1, "by_key": by}


def _write_atomic(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name("." + path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, indent=1, sort_keys=True, default=str) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def write_roster(data_dir, ideas: dict, magics: Optional[dict] = None, now: Optional[dt.datetime] = None) -> None:
    """Write magics.json first (a number handed out is never lost), then roster.json."""
    if magics is not None:
        _write_atomic(magics_path(data_dir), {"schema": SCHEMA, "next": int(magics["next"]),
                                              "by_key": dict(magics["by_key"])})
    when = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc).replace(microsecond=0)
    _write_atomic(roster_path(data_dir), {"schema": SCHEMA, "written_utc": when.isoformat(), "ideas": ideas})


def stamp(data_dir) -> Optional[tuple]:
    """(mtime_ns, size) of roster.json, for a cheap "has it changed" check; None when missing."""
    try:
        st = roster_path(data_dir).stat()
        return (st.st_mtime_ns, st.st_size)
    except OSError:
        return None
