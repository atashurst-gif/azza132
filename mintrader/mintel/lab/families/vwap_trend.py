"""Bounce off the average: join a trend when the price dips back to the session's average.

The opposite of VWAP_FADE ("Back to the average"), which bets a stretched price
snaps back to the session's volume-weighted average price (VWAP). Here the
average is a floor (or a ceiling) under a trend: when the price has stayed on
one side of it for a while and the slower trend agrees, a dip back to the
average is bought (a rally back to it sold) as the price moves away again.

The evidence it was added on (17 Oct 2026):

* Zarattini and Aziz (2023), "Volume Weighted Average Price (VWAP): The Holy
  Grail for Day Trading Systems" (SSRN 4631351): long above the VWAP and short
  below it, on one-minute bars of QQQ/TQQQ, was profitable after costs from
  2018 to 2023.
* The VWAP pullback in a trend is a staple of practitioners' day trading.
* The squad's own week: trend pullback ideas on the indices made +£208 and
  +£182 in practice on 16 Oct. That family (and VWAP_FADE) "fits noise" on past
  prices, so this one is kept simple: one average, one trend check, one dip.

The rule, on the minutes of the idea's session (US_CASH or LONDON, each with an
average of its own from its own open):

1. The trend is set when every one-minute close for the last ``side_min``
   minutes has been on the same side of the session's average (a close on the
   other side, or on it, starts the count again), and the 20-bar average of the
   15-minute or 1-hour closes (``trend_tf``) is above the 50-bar one for an
   uptrend (below for a downtrend).
2. The zone reaches ``touch_sd`` times the session's usual distance from the
   average (``sig_<session>``) either side of the average. Only a FRESH dip
   counts: a minute whose low (a high, for a sell) reaches into the zone
   straight after a minute that closed outside it (more than that distance
   from the average, in this run of closes), with the trend set and any wait
   after a signal (rule 5) over. So the whole dip starts at or after the
   moment it may count. A dip already under way when the trend comes on, the
   ``side_min`` count completes or the wait ends is spent: it needs a close
   back outside the zone, then a new dip. Going through the average is fine,
   as long as no minute closes on the other side (or on it). A deeper low (a
   higher high) before the close back out moves the stop with it.
3. The trade is taken on the first close back out of the zone, in the trend's
   direction, provided the trend is still on at that close (a dip that is one
   minute's wick, closing outside the zone, is taken on that minute). If the
   trend is not on, that dip is spent: it waits for the next fresh dip.
4. The stop sits half a swing (0.5 x atr5) beyond the average or the dip's
   extreme (from its first minute to the close back out), whichever is
   farther (``stop_points``, from the signal close).
5. At most two a session a day (``MAX_A_DAY``); after a signal, no dip counts
   until ``REARM_MIN`` (30) minutes after the signal's minute (a close through
   the average in between does not end the wait), so the second needs a fresh
   dip that starts at least half an hour after the first signal.

Before its first run (17 Oct, after the review) rule 2 was changed to count
only a fresh dip; before, a dip already under way was bought from its middle,
with a stop that ignored its earlier part. The ids stay: an id hashes the
definition (family, version, settings), not the code, and the family had never
run, so no result was made under the old rule.

Everything is known at the close of the signal bar: the average, its spread,
the trend averages and the swing are features of bars up to and including it,
and the counts above only ever look back. One position at a time in a market
(the simulator's rule): it never adds to a trade, let alone a losing one.

The grid (64): 30 or 60 minutes on one side, a zone of 0.25 or 0.5 of the usual
distance, the 15-minute or 1-hour trend, the US cash or London session, a 2:1
target or a 2R trail, on the indices and the major pairs, news ignored. Why
those and no more (decided after the review, 17 Oct):

* US_CASH and LONDON: the busy hours of the markets it trades. The Asian
  session has an average too, but is quiet for the indices and the majors.
* INDEX and FX_MAJOR: the evidence is on a stock index (QQQ) and the squad's
  index pullbacks; the majors are the liquid pairs. Gold and the minors are
  left out to keep the count of ideas tried (N) small. On the indices the
  0.25 zone's stops are usually tighter than the 0.5 zone's, so it pays
  proportionally more in costs (spread and commission against the risk) and
  has more trades refused for costs (``costs_vs_stop``): a weak result there
  is partly costs.
* News IGNORE only (``news_axis`` False): an AVOID_15M half would wait for
  calendar coverage before any past test (``waiting_for_calendar``) and would
  double N.
* Exits t2 and tr2: tr1 (a one-swing trail) overlaps tr2 for a rule that joins
  a trend, both letting the move run, so the 2:1 target is the contrast. The
  "back to the average" exit is not offered: its target would sit behind the
  entry of a trade that is leaving the average.

The axes are in this order on purpose: markets, exit, session, then trend_tf,
touch_sd and side_min last (news, with one value, changes nothing where it
sits). ``itertools.product`` varies the last axis
fastest, and the weekly refresh adds new ideas in grid order only as far as
there is room, so its first 16 are every signal setting on the indices with
the 2:1 target, then the same 16 trailed, then the majors. (An id is the hash
of the definition alone, so the order changes no id.)
"""
from __future__ import annotations

from math import ceil

from .. import Signal
from ..candidate import SESSION_NAMES, SWING_NOTE, FamilyBase


class VwapTrend(FamilyBase):
    code = "VWAP_TREND"
    label = "Bounce off the average"
    version = 1
    features = ("vwap", "ema", "atr")
    sensitive = ("side_min", "touch_sd")
    GRID_SIZE = 64
    TUNABLE = {"side_min": (10, 120), "touch_sd": (0.05, 1.5)}
    news_axis = False
    # the order matters (the module's words): the refresh takes ideas in grid order, the last axis varies fastest
    AXES = {"markets": ["INDEX", "FX_MAJOR"], "exit": ["t2", "tr2"], "news": ["IGNORE"],
            "session": ["US_CASH", "LONDON"], "trend_tf": ["M15", "H1"], "touch_sd": [0.25, 0.5],
            "side_min": [30, 60]}
    MAX_A_DAY = 2                   # signals a session a day
    REARM_MIN = 30                  # minutes after a signal before a new dip counts
    STOP_PAST = 0.5                 # swings beyond the average (or the dip) for the stop

    def signals(self, day, p, span, news):
        ser = day.series
        lo, hi = span
        s = p["session"]
        tf = p["trend_tf"]
        vw, sg = ser.f(f"vwap_{s}"), ser.f(f"sig_{s}")
        ef, es = ser.f(f"ema20_{tf}"), ser.f(f"ema50_{tf}")
        atr = ser.f("atr5")
        t, h, l, c = ser.t, ser.h, ser.l, ser.c
        need = p["side_min"]
        k = p["touch_sd"]
        out = []
        side = 0                        # the side of the average the closes are on (+1 above, -1 below)
        t0 = 0                          # open minute of the first close of this run on that side
        was_out = False                 # the previous bar, in this run, closed outside the zone
        armed = False                   # a fresh dip into the zone with the trend set
        ext = 0                         # the dip's extreme while armed (lowest low / highest high)
        rearm_t = None                  # no dip counts before this minute (after a signal)
        for i in range(lo, hi):
            v = vw[i]
            if v <= 0:                  # the average is not known yet (the session's first minutes)
                side, was_out = 0, False
                continue
            ci = c[i]
            now = 1 if ci > v else (-1 if ci < v else 0)
            if now != side:             # a close on the other side (or on the average): a new count
                side, t0, was_out, armed = now, t[i], False, False
            if side == 0:
                continue
            tol = k * sg[i]
            away = (ci - v) * side      # how far the close is from the average, on the trend's side
            reach = (l[i] - v) if side > 0 else (v - h[i])     # the bar's extreme, from the average
            f1, s1 = ef[i], es[i]
            trend = (t[i] + 1 - t0 >= need and f1 > 0 and s1 > 0
                     and (f1 - s1) * side > 0 and atr[i] > 0)
            # a FRESH dip only: this bar reaches into the zone straight after a close outside it, so the whole
            # dip is at or after the moment it may count (the trend set, the wait after a signal over). A dip
            # already under way then is spent: it needs a close back outside the zone, then a new dip.
            dip = (trend and was_out and reach <= tol
                   and (rearm_t is None or t[i] >= rearm_t))
            if dip and not armed:
                armed = True
                ext = l[i] if side > 0 else h[i]
            elif armed:
                ext = min(ext, l[i]) if side > 0 else max(ext, h[i])
            if armed and away > tol:    # the first close back out of the zone after the dip
                armed = False
                if trend:               # ... with the trend still set: the trade
                    ref = min(v, ext) if side > 0 else max(v, ext)
                    stop = ceil((ci - ref) * side + self.STOP_PAST * atr[i])
                    out.append(Signal(i, side, stop, "back off the average with the trend"))
                    if len(out) >= self.MAX_A_DAY:
                        break
                    rearm_t = t[i] + self.REARM_MIN
            was_out = away > tol
        return out

    # ---- words (Aaron, 10 Oct: "put it all nice and clear in layman terms")
    BARS = {"M15": "15-minute", "H1": "1-hour"}
    TREND_SHORT = {"M15": "15-min trend", "H1": "1-hour trend"}
    SHORT = {"US_CASH": "US", "LONDON": "London"}
    OPENED = {"US_CASH": "the US stock market", "LONDON": "the London session"}

    def _session(self, c):
        return self.SHORT.get(c.session, SESSION_NAMES.get(c.session, c.session))

    @staticmethod
    def _usual(k) -> str:
        """'0.25 times that usual distance'; 'that usual distance' at exactly 1."""
        return "that usual distance" if float(k) == 1 else f"{float(k):g} times that usual distance"

    def key_words(self, c):
        return (f"{self._session(c)} {c.p('side_min'):g} min, {c.p('touch_sd'):g}x dip, "
                f"{self.TREND_SHORT[c.p('trend_tf')]}")

    def subject(self, c):
        return (f"Bounce off the average, {self._session(c)} session ({c.p('side_min'):g} min one side, "
                f"dip to {c.p('touch_sd'):g}x the usual, {self.BARS[c.p('trend_tf')]} trend)")

    def rule_words(self, c):
        n = f"{c.p('side_min'):g}"
        bars = self.BARS[c.p("trend_tf")]
        opened = self.OPENED.get(c.session, SESSION_NAMES.get(c.session, c.session))
        return [f"Tracks the average price since {opened} opened that day (weighted by how much was traded) "
                f"and how far prices usually stray from it, and looks for trades only while that session is "
                f"open.",
                f"Calls an uptrend when every one-minute close for the last {n} minutes has been above that "
                f"average and the {bars} trend agrees (the 20-bar moving average of the {bars} closing prices "
                f"above the 50-bar one). A downtrend is the mirror image.",
                f"Its zone is the prices within {self._usual(c.p('touch_sd'))} of the average. In an uptrend it "
                f"waits for a close more than that distance from the average, then a fresh dip back into the "
                f"zone (going through the average is fine, as long as no minute closes on or below it), and "
                f"buys on the first one-minute close back out of the zone, provided the trend is still on (if "
                f"not, it waits for the next fresh dip). In a downtrend it sells the same way.",
                f"A fresh dip is one that starts once the trend is on, straight after a close outside the zone. "
                f"A dip already under way when the trend comes on (or when its wait after a signal ends) is let "
                f"go: it waits for a close back outside the zone, then a new dip.",
                f"At most {self.MAX_A_DAY} signals a session a day (one skipped for its costs still counts); the "
                f"second needs a fresh dip at least {self.REARM_MIN} minutes after the first.",
                f"Stops half a swing beyond the average price or the dip's low (its high, for a sell), "
                f"whichever is farther ({SWING_NOTE})."]
