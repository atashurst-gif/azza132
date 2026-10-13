"""The families, and the one registry of them.

Night-1 registered grid sizes (pinned by a test): OPENING_RANGE 144,
RANGE_BREAK 108, MOMENTUM_CONTINUATION 108, TREND_PULLBACK 48, VWAP_FADE 108,
PREV_DAY_LEVELS 48, SQUEEZE_RELEASE 36, NEWS_MOVE 32 (dormant until the
calendar has 40 covered trading days), CONTROL_TIME_OF_DAY 32. Total 632, or
664 when NEWS_MOVE wakes. Not expressible from bars, so present only as bots
with their own records: Crowd Fader, Financial Ian, the Rider.

A NEW FAMILY IS ONE NEW FILE HERE (13 Oct: "we add in new strategies to the mix
to replace and continue this cycle"). Every module in this folder is read when
the lab starts; each class in it that subclasses ``candidate.FamilyBase`` and
sets a ``code`` joins the registry (the nine above first, in their order, then
the others by file name). Nothing else needs editing. A module that fails to
import is left out with a warning; it never stops the lab. The contract:

* ``code`` - a unique UPPER_CASE word (it is in every idea's id, so never
  rename it); ``label`` - the family in a few plain words; ``version`` = 1;
* ``AXES`` - an ordered dict of axis -> values. ``markets`` (FX_MAJOR,
  FX_MINOR, INDEX, GOLD), ``session``, ``news`` (IGNORE, AVOID_15M) and
  ``exit`` (one of ``lab.EXITS``) are the idea's own fields, every other axis
  is a setting; ``GRID_SIZE`` - the product of the axes' lengths (checked);
* ``features`` - the feature sets it reads (``features.OUTPUTS``);
  ``sensitive`` - numeric settings the plateau check wiggles; ``TUNABLE`` -
  {setting: (lowest, highest)}: numeric settings that may take ANY value in
  that range, which the weekly refresh nudges between grid steps for a
  winner's variants (leave it empty when a setting picks a precomputed
  feature, like a 60/120/240-minute high);
* ``signals(day, p, span, news)`` - the signals of one UK day as
  ``lab.Signal``s at bar indices inside ``span``, known at that bar's close and
  never looking ahead. It reads only its own settings and ``p["session"]``,
  never the exit, markets or news rule (practice shares one call among ideas
  that differ only in those; a test reads the source), and the ``news`` array
  only when it sets ``reads_news = True``;
* ``subject(c)`` and ``rule_words(c)`` - plain words; ``key_words(c)`` - its
  settings in a few words for a one-line name (``subject`` when not given);
* optional: ``dormant = True`` (registered only once the news calendar wakes
  it), ``news_axis = False`` (no news field in its name), ``DEFAULTS``.

A new family's ideas join the practice pool at the next weekly refresh (as
many as there is room for) and its grid joins the past tests at the next
stretch roll.
"""
from __future__ import annotations

import importlib
import inspect
import logging
import pkgutil

from .control import ControlTimeOfDay
from .momentum import MomentumContinuation
from .news_move import NewsMove
from .opening_range import OpeningRange
from .prev_day_levels import PrevDayLevels
from .range_break import RangeBreak
from .squeeze_release import SqueezeRelease
from .trend_pullback import TrendPullback
from .vwap_fade import VwapFade

_log = logging.getLogger("mintel.lab.families")
KNOWN = (OpeningRange, RangeBreak, MomentumContinuation, TrendPullback, VwapFade, PrevDayLevels, SqueezeRelease,
         NewsMove, ControlTimeOfDay)


def discover(path=None, package: str = __name__) -> list:
    """Every family class in the modules of ``path`` (this folder by default),
    by file name: the subclasses of FamilyBase with a ``code`` that are defined
    in that module. A module that fails to import is skipped with a warning."""
    from ..candidate import FamilyBase
    out = []
    for info in sorted(pkgutil.iter_modules(path if path is not None else __path__), key=lambda m: m.name):
        if info.name.startswith("_"):
            continue
        try:
            mod = importlib.import_module(f"{package}.{info.name}")
        except Exception as exc:                        # a half-written family never stops the lab
            _log.warning("lab family module %s left out: %s", info.name, exc)
            continue
        for _name, cls in inspect.getmembers(mod, inspect.isclass):
            if (issubclass(cls, FamilyBase) and cls is not FamilyBase and getattr(cls, "code", "")
                    and cls.__module__ == mod.__name__):
                out.append(cls)
    return out


def _registry() -> dict:
    fams = {}
    for cls in list(KNOWN) + [c for c in discover() if c not in KNOWN]:
        if cls.code in fams:
            _log.warning("lab family %s defined twice: the first is kept", cls.code)
            continue
        fams[cls.code] = cls()
    return fams


FAMILIES = _registry()

GRID_SIZES = {code: f.GRID_SIZE for code, f in FAMILIES.items()}
CONTROL = "CONTROL_TIME_OF_DAY"


def get(code: str):
    try:
        return FAMILIES[code]
    except KeyError:
        raise KeyError(f"unknown family {code!r}") from None


def all_families(news_awake: bool = False) -> list:
    """Registered families in registry order; NEWS_MOVE only once it is awake."""
    return [f for f in FAMILIES.values() if news_awake or not f.dormant]


def is_control(code: str) -> bool:
    return code == CONTROL
