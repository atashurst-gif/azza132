"""The nine families, and the one registry of them.

Night-1 registered grid sizes (pinned by a test): OPENING_RANGE 144,
RANGE_BREAK 108, MOMENTUM_CONTINUATION 108, TREND_PULLBACK 48, VWAP_FADE 108,
PREV_DAY_LEVELS 48, SQUEEZE_RELEASE 36, NEWS_MOVE 32 (dormant until the
calendar has 40 covered trading days), CONTROL_TIME_OF_DAY 32. Total 632, or
664 when NEWS_MOVE wakes. Not expressible from bars, so present only as bots
with their own records: Crowd Fader, Financial Ian, the Rider.
"""
from __future__ import annotations

from .control import ControlTimeOfDay
from .momentum import MomentumContinuation
from .news_move import NewsMove
from .opening_range import OpeningRange
from .prev_day_levels import PrevDayLevels
from .range_break import RangeBreak
from .squeeze_release import SqueezeRelease
from .trend_pullback import TrendPullback
from .vwap_fade import VwapFade

FAMILIES = {f.code: f for f in (
    OpeningRange(), RangeBreak(), MomentumContinuation(), TrendPullback(), VwapFade(),
    PrevDayLevels(), SqueezeRelease(), NewsMove(), ControlTimeOfDay())}

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
