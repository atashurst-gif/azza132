"""Into the close: the US stock market's first half hour, followed into the 4pm close.

The idea is "market intraday momentum". Which way the US market moved in its first half
hour (the 9:30am New York open to 10am) is traded in the same direction at a fixed time
later in the afternoon, for the run into (and sometimes past) the close.

Evidence:

* Gao, Han, Li and Zhou (2018), "Market intraday momentum", Journal of Financial
  Economics 129(2). The S&P 500's first half-hour return (from the previous close to 10:00
  New York) predicts its last half-hour return (15:30-16:00), more strongly on
  high-volatility and high-volume days. The twelfth half hour (15:00-15:30) adds to it.
* Baltussen, Da, Lammers and Martens (2021), "Hedging demand and market intraday
  momentum", Journal of Financial Economics 142(1). The effect holds across 60+ futures
  markets, and they tie it to option (gamma) hedging into the close.
* The lab's own week (to 17 Oct 2026): the New York index ideas led practice, and Band
  Breaker's index shorts held to the 15:55 New York flat paid about +2.3 R.

No other family trades the end of the US session on the direction of its start.

Differences from the papers (the third forced by the exits the lab has):

* The first half hour is measured from 9:30am New York (the papers use the previous
  close): from the price at 9:30am New York (the close of the minute ending then) to the
  price at 10am.
* The lab trades CFDs, not futures, and this family only the US stock-index CFDs among
  the lab's markets (rule 0 below): on this account the Dow (US30) and the S&P 500
  (US500), so at most 2 trades at once. The lab's markets have no Nasdaq: IC Markets
  calls it USTEC, which ``contracts.INDEX_HINTS`` does not list, so
  ``store.lab_kind('USTEC')`` is '' and it is never stored (adding it there would change
  Trend & Breakout's own market classification). The study it follows is on the S&P
  500, the close it trades into is the US one, and every index would signal at the same
  minute: with practice's cap of 3 open trades an idea, the order of the market list
  would otherwise pick which indices trade.
* The lab has no 30-minute exit. With the 1-hour exit, a 3pm entry closes at the 4pm
  close, and a 3:30pm entry closes half an hour after it (at the 4pm close on a Friday,
  when the 9pm UK flat comes first). The 2:1 and trail exits have no time limit of their
  own and can hold past the close, until the 9:55pm UK flat (4:55pm New York for most of
  the year; on a Friday the 9pm UK flat, which usually closes them at the 4pm close).

The rule, at most one signal a UK day:

0. Only a US stock index: a series whose profit currency is USD, or, when its spec has
   no profit currency, whose name (upper case, letters and digits only) starts with
   US30, US500, USTEC, US100, NAS, SPX or DJ. Any other series gets no signal. This
   reads the series' own symbol and spec, never the idea's markets field.
1. The direction is the sign of (price at 10am New York - price at 9:30am New York). A
   price "at" a minute is the close of the minute that ends then. It may be up to two
   minutes stale (``LATE_MIN``) if minutes are missing. A zero move is always skipped.
2. The move's strength, |that move| / (or_N30_hi - or_N30_lo), must be at least the
   ``strength`` setting (0, 0.5 or 0.75; 0 is any move). The divisor is the high-to-low
   range of the first 30 minutes from the 9:30 New York open, read at the signal bar: it
   is fixed from 10:00, so it is known there. The ratio is scale-free, unlike a multiple of
   atr15 at 10am, which is mostly a pre-market scale. A range that is not known (fewer
   than 60% of its minutes stored) or is zero passes only ``strength`` 0.
3. ``day_agrees`` YES also needs the signal bar's close beyond the 10am price, in that
   direction (a trend day).
4. The signal is on the minute that closes at ``entry_ny`` New York time (up to two
   minutes late if that minute is missing). The fill is at the next minute's open. New
   York time follows US daylight saving: 3pm New York is 8pm UK, or 7pm UK in the weeks
   when only one country has changed its clocks (two or three weeks in March and about a
   week around the end of October; in 2026, 9-27 March and 26-30 October).
5. The stop is one atr15 from the signal close (``stop_points``). That is wider than the
   lab's 5-minute swing: the trade holds into the close, and on the stock indices a
   5-minute stop is mostly ordinary noise, with costs a bigger share of it. A signal bar
   whose atr15 is not known yet gives no signal (no stop could be set).

News: IGNORE only (``news_axis`` False). An AVOID_15M twin would double the ideas for a
rule that rarely acts: releases between 3pm and 3:45pm New York are rare.

The axes are ordered exit, markets, session, news, then the signal settings, because
the weekly refresh adds a new family's ideas in grid order, only as far as there is room,
and the grid varies the last axis fastest: its first 12 ideas are every signal setting
with the first exit (time1h).

The session is US_CASH, so the span is 9:30am-4pm New York and holds both entry times.
The prices before the span (the 9:29am minute) are only read, never signalled on.
Nothing here reads a price after the signal bar. The two anchors, the signal bar, the
first half hour's range and the atr15 value are all at or before it, so a past test and
live practice see the same.
"""
from __future__ import annotations

import bisect
import datetime as dt
import re
from functools import lru_cache
from math import ceil

from ...clock import TZ_NEWYORK
from .. import Signal, epoch_min
from ..candidate import EXIT_SHORT, EXIT_TAG, SESSION_NAMES, SWING_NOTE, FamilyBase

NOTE15 = "the usual move = how far the price has typically moved in a recent 15-minute stretch"
US_WORDS = "the US stock indices among the lab's markets (on this account the Dow and the S&P 500)"
US_NAMES = ("US30", "US500", "USTEC", "US100", "NAS", "SPX", "DJ")


@lru_cache(maxsize=8192)
def ny_minute(day: str, hh: int, mm: int) -> int:
    """Epoch minute of hh:mm New York time on a (UK) calendar date, by the time-zone
    database (US daylight saving, so 15:00 New York is 19:00 or 20:00 UTC)."""
    local = dt.datetime.combine(dt.date.fromisoformat(day), dt.time(hh, mm), tzinfo=TZ_NEWYORK)
    return epoch_min(local)


def is_us_index(ser) -> bool:
    """A series of a US stock index: its profit currency is USD or, when its spec gives
    none, its name (upper case, letters and digits only) starts with a US index's."""
    spec = getattr(ser, "spec", None)
    ccy = str(getattr(spec, "profit_currency", "") or "").strip().upper()
    if ccy:
        return ccy == "USD"
    return re.sub(r"[^A-Z0-9]", "", str(getattr(ser, "symbol", "") or "").upper()).startswith(US_NAMES)


def clock_words(hhmm: str) -> str:
    """'15:00' -> '3pm', '15:30' -> '3:30pm'."""
    h, m = (int(x) for x in str(hhmm).split(":"))
    half = "am" if h < 12 else "pm"
    h12 = h % 12 or 12
    return f"{h12}{half}" if m == 0 else f"{h12}:{m:02d}{half}"


def _later(hhmm: str, minutes: int) -> str:
    h, m = (int(x) for x in str(hhmm).split(":"))
    m += minutes
    return f"{(h + m // 60) % 24:02d}:{m % 60:02d}"


def close_run_words(entry_ny, exit_code) -> str:
    """'into the 4pm close' for an idea that is always out by then (an entry an hour or more before it, with the
    1-hour exit); 'into (and sometimes past) the 4pm close' for the rest (a later entry with the 1-hour exit,
    or the 2:1 and trail exits, which can hold past it until the flat)."""
    if exit_code == "time1h" and _later(entry_ny, 60) <= "16:00":
        return "into the 4pm close"
    return "into (and sometimes past) the 4pm close"


def share_words(x) -> str:
    """0.5 -> 'half', 0.75 -> 'three quarters', 0.25 -> 'a quarter', 0.6 -> '60%'."""
    x = float(x)
    named = {0.5: "half", 0.75: "three quarters", 0.25: "a quarter"}
    return named.get(x) or f"{round(x * 100, 2):g}%"


def _share_short(x) -> str:
    x = float(x)
    named = {0.5: "half", 0.75: "3/4", 0.25: "1/4"}
    return named.get(x) or f"{round(x * 100, 2):g}%"


class IntradayMomentum(FamilyBase):
    code = "INTRADAY_MOMENTUM"
    label = "Into the close"
    version = 1
    features = ("atr", "sess")
    sensitive = ("strength",)
    GRID_SIZE = 36
    TUNABLE = {"strength": (0.0, 0.9)}
    news_axis = False
    # exit first and the signal settings last: the grid varies the last axis fastest, and the weekly
    # refresh adds new ideas in grid order, so its first 12 are every signal setting with time1h
    AXES = {"exit": ["time1h", "tr1", "t2"], "markets": ["INDEX"], "session": ["US_CASH"], "news": ["IGNORE"],
            "entry_ny": ["15:00", "15:30"], "day_agrees": ["NO", "YES"], "strength": [0.0, 0.5, 0.75]}
    BELL = (9, 30)                   # the US cash open, New York time
    FIRST_END = (10, 0)              # the end of its first half hour
    LATE_MIN = 2                     # a price (or the signal minute) may be this many minutes off
    STOP_USUAL = 1.0                 # the stop: one usual 15-minute move from the signal close

    def signals(self, day, p, span, news):
        ser = day.series
        lo, hi = span
        if hi <= lo or not is_us_index(ser):          # the US stock indices only (its own symbol and spec)
            return []
        hh, mm = (int(x) for x in p["entry_ny"].split(":"))
        uk = day.uk
        want = ny_minute(uk, hh, mm) - 1                 # the minute that closes at the entry time
        t, c = ser.t, ser.c
        # the signal bar: that minute, or up to LATE_MIN minutes later if it is missing. Live,
        # before it has come, the search runs off the bars so far and nothing is said.
        i = bisect.bisect_left(t, want, lo, hi)
        if i >= hi or t[i] - want > self.LATE_MIN:
            return []
        # the price at 9:30am and at 10am: closes of minutes BEFORE the signal bar
        j0 = self._close_at(t, ny_minute(uk, *self.BELL), day.i0, i)
        j1 = self._close_at(t, ny_minute(uk, *self.FIRST_END), day.i0, i)
        if j0 < 0 or j1 <= j0:
            return []
        move = c[j1] - c[j0]
        if move == 0:
            return []
        x = p["strength"]
        if x > 0:
            # the first 30 minutes' high-to-low range from the 9:30 open, as known at the signal bar
            # (fixed from 10:00; 0 while unknown, or when too few of its minutes were stored)
            width = ser.f("or_N30_hi")[i] - ser.f("or_N30_lo")[i]
            if width <= 0 or abs(move) / width < x:
                return []
        usual = ser.f("atr15")[i]
        if usual <= 0:
            return []
        side = 1 if move > 0 else -1
        if p["day_agrees"] == "YES" and (c[i] - c[j1]) * side <= 0:
            return []
        return [Signal(i, side, ceil(self.STOP_USUAL * usual),
                       "first half hour up" if side > 0 else "first half hour down")]

    @classmethod
    def _close_at(cls, t, minute: int, a: int, b: int) -> int:
        """The bar whose close is the price at ``minute``: the last bar in [a, b) that opened
        before it. It must have opened no more than LATE_MIN minutes before the minute ending
        then. Returns -1 when there is none."""
        j = bisect.bisect_left(t, minute, a, b) - 1
        if j < a or t[j] < minute - 1 - cls.LATE_MIN:
            return -1
        return j

    # ---- words
    def market_word(self, c):
        return US_WORDS

    @staticmethod
    def _strength_name(x) -> str:
        if not x:
            return "any move"
        share = share_words(x)
        return f"moved at least {share}{'' if share == 'half' else ' of'} its range"

    def name(self, c):
        # the one-line name says the markets briefly; the words say which ones in full
        return f"{self.subject(c)} on the US stock indices; {EXIT_SHORT[c.exit]}"

    def short_name(self, c):
        return f"{self.label} · US indices · {self.key_words(c)} · {EXIT_TAG.get(c.exit, c.exit)}"

    def key_words(self, c):
        x = c.p("strength")
        move = "any move" if not x else f"{_share_short(x)}-range move"
        return (f"{clock_words(c.p('entry_ny'))} New York, {move}"
                f"{', agrees' if c.p('day_agrees') == 'YES' else ''}")

    def subject(self, c):
        agree = ", day agrees" if c.p("day_agrees") == "YES" else ""
        return (f"First half hour, in at {clock_words(c.p('entry_ny'))} New York "
                f"({self._strength_name(c.p('strength'))}{agree})")

    def stop_words(self, c):
        out = (f"Stops one usual move away ({NOTE15}). That is wider than the 5-minute swing most lab ideas "
               f"use, so an ordinary wobble on the way to the close does not knock it out.")
        if c.exit == "tr1":
            # the trail is the simulator's: one atr5 (a swing) behind the best price, not one atr15
            out += (f" Once the trail starts, it follows one swing behind the best price, not one usual move "
                    f"({SWING_NOTE}).")
        return out

    def rule_words(self, c):
        when = clock_words(c.p("entry_ny"))
        x = float(c.p("strength"))
        out = [f"Watches {SESSION_NAMES[c.session]}'s first half hour, from the 9:30am New York open to "
               f"10am, and notes which way the price went (its 10am price against its 9:30am price)."]
        if x:
            out.append(f"Only a clear first half hour counts: the move had to cover at least {share_words(x)} of "
                       f"the first half hour's high-to-low range (from its highest price to its lowest). A day "
                       f"whose range cannot be measured (too many of those minutes missing) is skipped.")
        else:
            out.append("Any rise or fall counts, however small; a first half hour that ends exactly "
                       "where it began is skipped.")
        if c.p("day_agrees") == "YES":
            out.append(f"The rest of the day must agree too: at {when} the price must have carried on "
                       f"past its 10am price in the same direction.")
        out.append(f"At {when} New York time it trades the same way as that first half hour, buying "
                   f"after a rise and selling after a fall, for the run {close_run_words(c.p('entry_ny'), c.exit)}. "
                   f"One trade a day at most in each of the US indices (so up to 2 at once). New York "
                   f"time follows US daylight saving: {when} New York is "
                   f"{clock_words(_later(c.p('entry_ny'), 300))} UK time for most of the year "
                   f"({clock_words(_later(c.p('entry_ny'), 240))} for two or three weeks in March and about a "
                   f"week around the end of October).")
        if c.exit == "time1h":
            end = _later(c.p("entry_ny"), 60)
            h, m = (int(v) for v in end.split(":"))
            after = h * 60 + m - 16 * 60
            if after == 0:
                out.append(f"With its 1-hour exit it closes at the {clock_words(end)} New York close.")
            else:
                out.append(f"With its 1-hour exit it closes at {clock_words(end)} New York time"
                           + (f", {after} minutes after the close (on a Friday the 9pm UK flat usually "
                              f"closes it at the 4pm close)." if after > 0 else "."))
        else:
            ender = "its target" if c.exit == "t2" else "the trail"
            out.append(f"If its stop or {ender} has not ended the trade by the 4pm close, it can stay open "
                       f"after the close, until the 9:55pm UK flat (4:55pm New York for most of the year). On "
                       f"a Friday the 9pm UK flat usually closes it at the 4pm close.")
        out.append("Research on the US market (2018) found that the first half hour's direction tends to "
                   "carry on into the last half hour before the close, most of all on busy, volatile days; a "
                   "2021 study of more than 60 futures markets found that the day's move so far carries on into "
                   "the close too. It trades only the US indices: the 2018 study is on the S&P 500, and the "
                   "close it trades into is the US one.")
        out.append(self.stop_words(c))
        return out
