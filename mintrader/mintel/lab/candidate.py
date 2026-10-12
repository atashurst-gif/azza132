"""Candidates: frozen trading ideas, and the families that generate them.

A *candidate* (``mintel.lab.Candidate``) is a family, a version and a handful
of fixed settings. It is never fitted: every candidate comes from a
pre-registered grid, so how many were tried (N) is known before anything runs
and cannot grow in the middle of a stretch. Its id is a hash of the
definition alone, so the same idea is the same id on every machine and every
night, and a binned id is recognised for ever.

A *family* knows its grid, how to turn a day of bars into signals, the
numeric settings a plateau check should wiggle (``sensitive``), and how to
describe each candidate in plain words. Names and "rules in plain words" are
GENERATED from the definition, never typed, so they cannot drift from what
the candidate really does.

Aaron, 10 Oct 2026: "put it all nice and clear in layman terms".
"""
from __future__ import annotations

import itertools
from typing import Protocol

from . import NEWS_AVOID_MINUTES, RISK_GBP, Candidate, Result, Signal, Trade, candidate_id

__all__ = ["Candidate", "Signal", "Trade", "Result", "candidate_id", "Family", "FamilyBase",
           "registered", "registered_ids", "news_ready", "waiting_for_calendar", "family_of", "name_of", "words_of", "siblings",
           "MARKET_NAMES", "EXIT_SHORT", "EXIT_LONG", "SWING_NOTE", "swings"]

MARKET_NAMES = {"FX_MAJOR": "the major pairs", "FX_MINOR": "the minor pairs",
                "INDEX": "the stock indices", "GOLD": "gold"}
SESSION_NAMES = {"LONDON": "London", "NEWYORK": "New York", "US_CASH": "the US stock market",
                 "ASIA": "Asia", "BOTH": "London and New York", "ALL": "all day"}
EXIT_SHORT = {"t1": "aims for 1x its risk", "t2": "aims for 2x its risk",
              "t15w": "wider stop, aims for 1.5x its risk", "tr1": "trails 1 swing behind",
              "tr2": "trails 2x its risk behind", "time4h": "out after 4 hours",
              "time1h": "out after 1 hour", "vwap": "aims for the average price"}
EXIT_LONG = {
    "t1": "Takes its profit when the trade is up by what it risked (1 to 1).",
    "t2": "Takes its profit when the trade is up by twice what it risked (2 to 1).",
    "t15w": "Uses a wider stop (1.5 swings) and takes its profit at 1.5 times what it risked.",
    "tr1": ("No fixed target: once the trade is up by what it risked, the stop follows one "
            "swing behind the best price, so a big move can run."),
    "tr2": ("No fixed target: once the trade is up by what it risked, the stop follows "
            "twice the risk behind the best price, so a big move can run (the Momentum "
            "Runner's idea)."),
    "time4h": "No fixed target: it closes after 4 hours.",
    "time1h": "No fixed target: it closes after 1 hour.",
    "vwap": "Takes its profit when the price gets back to the session's average price.",
}
ATR_STOP_WORDS = {"t15w": "Stops 1.5 swings away", }
SWING_NOTE = "a swing = how far the price has typically moved in a recent 5-minute stretch"


def swings(x) -> str:
    """'1 swing', '1.5 swings', '0.25 swings': the lab's unit of distance in plain words."""
    x = float(x)
    return f"{x:g} swing" if x == 1 else f"{x:g} swings"


class Family(Protocol):
    code: str
    label: str
    version: int
    features: tuple
    sensitive: tuple
    dormant: bool

    def grid(self) -> list: ...
    def signals(self, day, p: dict, span: tuple, news) -> list: ...
    def name(self, c: Candidate) -> str: ...
    def words(self, c: Candidate) -> list: ...


class FamilyBase:
    """Shared machinery: grid from axes, siblings, names and words.

    Subclasses set ``AXES`` (an ordered dict of axis -> values; ``markets``,
    ``session``, ``news`` and ``exit`` are candidate fields, everything else
    is a parameter), ``GRID_SIZE`` (pinned by a test), ``features`` (the
    feature sets they read) and ``sensitive`` (numeric parameters a plateau
    check varies), and implement ``signals``, ``subject`` and ``rule_words``."""

    code = ""
    label = ""
    version = 1
    features: tuple = ()
    sensitive: tuple = ()
    dormant = False
    AXES: dict = {}
    GRID_SIZE = 0
    news_axis = True
    DEFAULTS = {"session": "ALL", "news": "IGNORE"}

    # ---- the grid
    def _build(self, values: dict) -> Candidate:
        v = dict(self.DEFAULTS)
        v.update(values)
        top = {k: v.pop(k) for k in ("markets", "session", "news", "exit")}
        return Candidate(family=self.code, version=self.version, markets=top["markets"],
                         session=top["session"], news=top["news"], exit=top["exit"],
                         params=tuple(sorted(v.items())), risk_gbp=RISK_GBP)

    def grid(self) -> list:
        keys = list(self.AXES)
        out = [self._build(dict(zip(keys, combo)))
               for combo in itertools.product(*(self.AXES[k] for k in keys))]
        if len(out) != self.GRID_SIZE:
            raise AssertionError(f"{self.code}: grid has {len(out)} candidates, "
                                 f"registered as {self.GRID_SIZE}")
        return out

    def axis_values(self, param: str) -> list:
        return list(self.AXES.get(param, ()))

    def siblings(self, c: Candidate, param: str) -> list:
        """Candidates identical to ``c`` except for ``param`` (the other values
        of that axis, in order, ``c`` itself included)."""
        out = []
        for val in self.axis_values(param):
            if param in ("markets", "session", "news", "exit"):
                out.append(c.replace(**{param: val}))
            else:
                out.append(c.replace(params={param: val}))
        return out

    def signals(self, day, p: dict, span: tuple, news) -> list:       # pragma: no cover
        raise NotImplementedError

    # ---- words
    def market_word(self, c: Candidate) -> str:
        return MARKET_NAMES[c.markets]

    def news_name(self, c: Candidate) -> str:
        if not self.news_axis:
            return ""
        return (f", avoiding news ({NEWS_AVOID_MINUTES} min)" if c.news == "AVOID_15M"
                else ", ignoring news")

    def subject(self, c: Candidate) -> str:                           # pragma: no cover
        raise NotImplementedError

    def name(self, c: Candidate) -> str:
        return f"{self.subject(c)} on {self.market_word(c)}{self.news_name(c)}; {EXIT_SHORT[c.exit]}"

    def rule_words(self, c: Candidate) -> list:                       # pragma: no cover
        raise NotImplementedError

    def stop_words(self, c: Candidate) -> str:
        k = swings(1.5 if c.exit == "t15w" else 1)
        return f"Stops {k} away ({SWING_NOTE})."

    def words(self, c: Candidate) -> list:
        out = [f"Trades {self.market_word(c)}."]
        out += self.rule_words(c)
        out.append(EXIT_LONG[c.exit])
        if self.news_axis:
            out.append(f"Does not enter within {NEWS_AVOID_MINUTES} minutes before a big news release."
                       if c.news == "AVOID_15M" else "Does not look at the news.")
        out.append("Flat by 9:55pm UK time (9pm on Fridays), never held overnight; one trade at a "
                   "time in a market, at most 3 open at once.")
        out.append(f"Risks about £{c.risk_gbp:g} a trade before costs; spread, slippage and "
                   f"commission are charged on every trade.")
        return out


# ---------------------------------------------------------------- registry ---
def _families():
    from . import families
    return families


def registered(news_awake: bool = False) -> list:
    """Every registered definition, in a fixed order. NEWS_MOVE is left out until
    the calendar has enough history (``news_awake``)."""
    out = []
    for fam in _families().all_families(news_awake):
        out.extend(fam.grid())
    return out


def news_ready(calendar, past_days) -> bool:
    """NEWS_MOVE wakes (at a stretch roll) once at least 40 trading days of the
    PAST stretch are days the calendar actually recorded news for."""
    from . import NEWS_MOVE_MIN_DAYS
    from .features import covered_days
    if calendar is None:
        return False
    return len(covered_days(calendar, list(past_days))) >= NEWS_MOVE_MIN_DAYS


def waiting_for_calendar(c: Candidate, covered_past_days: int) -> bool:
    """True for an AVOID_15M candidate while the PAST stretch holds fewer covered
    days than a past test needs (20). Such a candidate is judged only on days
    whose news is known, so run now it would fail "enough trades" for want of
    calendar history and be binned for good: hold it back and count it as
    'waiting for the calendar' instead."""
    from . import PAST_MIN_DAYS
    return c.news == "AVOID_15M" and covered_past_days < PAST_MIN_DAYS


def registered_ids(news_awake: bool = False) -> list:
    return [c.id for c in registered(news_awake)]


def family_of(c: Candidate):
    return _families().get(c.family)


def name_of(c: Candidate) -> str:
    return family_of(c).name(c)


def words_of(c: Candidate) -> list:
    return family_of(c).words(c)


def siblings(c: Candidate, param: str) -> list:
    return family_of(c).siblings(c, param)
