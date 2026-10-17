"""Into the close (INTRADAY_MOMENTUM): the US stock market's first half hour, followed into the 4pm close.

Pinned here:

* the grid: 36 ideas, found by the registry from its file alone, after the nine, with every one of the
  nine's ids untouched. News IGNORE only. The exit is the first axis, so the weekly refresh (which adds a
  new family's ideas in grid order, as far as there is room) starts with every signal setting on the
  1-hour exit;
* the US stock indices only: made-up DE40 and UK100 series give no signal and no trade, US500 and US30 do.
  The rule reads the series' own profit currency, or its name when the spec gives none;
* the signal on hand-made days: its direction, and its New York minute in summer (BST), in winter (GMT)
  and in both weeks when only one country has changed its clocks. At most one a day;
* the strength filter (the move against the first half hour's high-to-low range) at, just under and just
  over 0.5 and 0.75, and a range that is not known (the 60%-of-minutes rule) or is zero;
* nothing before the bars it needs: the 9:30am and 10am prices up to LATE_MIN minutes stale and no more,
  the signal minute up to LATE_MIN minutes late, and no stop (so no signal) while the usual move is unknown;
  the "day agrees" filter;
* a stop on every signal and every simulated trade, the 1-hour exit of a 3pm entry at the 4pm close, and the
  2:1 and trail exits holding past the close up to the flat;
* nothing looks ahead. A day's signals are the same when every later bar is changed or removed, and the
  same bar by bar as the day grows live, for every setting (the strength filter included);
* the names, words, short names and dashboard nickname.

EVERY PRICE HERE IS MADE UP.
"""
from __future__ import annotations

import collections
import dataclasses
import datetime as dt
import hashlib
import re
from math import ceil

import pytest

from mintel.lab import day_min, features as F, sim, synth
from mintel.lab import candidate as C
from mintel.lab import families as FAM
from mintel.lab.families.intraday_momentum import IntradayMomentum, clock_words, is_us_index, ny_minute
from mintel.lab.store import flat_min, session_bounds
from tests.test_lab_families import JARGON, set_bar, wiggle_columns

CODE = "INTRADAY_MOMENTUM"
US500 = synth.entry_for("US500")
UTC = dt.timezone.utc
NINE_DIGEST = "21ba9a386edb1b961834c1f692a039ab8b300ff1"         # the nine families' 664 ids, before this one
AMP = 34                                                          # the made-up day's wiggle
RANGE = AMP + 6                                                   # so its first half hour's high-to-low is 40 points
US_WORDS = "the US stock indices among the lab's markets (on this account the Dow and the S&P 500)"

# (UK date, its clocks, 9:30am New York as minutes after 00:00 UTC, the UK clock's hours ahead of UTC)
SEASONS = [("2026-07-01", "BST", 13 * 60 + 30, 1),                # New York EDT: 9:30am = 13:30 UTC = 14:30 UK
           ("2026-01-14", "GMT", 14 * 60 + 30, 0),                # New York EST: 9:30am = 14:30 UTC = 14:30 UK
           ("2026-10-28", "autumn, UK back on GMT first", 13 * 60 + 30, 0),   # = 13:30 UK
           ("2026-03-18", "spring, US on summer time first", 13 * 60 + 30, 0)]
SETTINGS = [dict(entry_ny=e, day_agrees=a, strength=s)
            for e in ("15:00", "15:30") for a in ("NO", "YES") for s in (0.0, 0.5, 0.75)]
OFF_GRID = [dict(entry_ny="15:00", day_agrees="NO", strength=0.25),     # the refresh's tweaks: any value in range
            dict(entry_ny="15:30", day_agrees="YES", strength=0.9)]


def fam():
    return FAM.get(CODE)


def params(**kw) -> dict:
    p = dict(entry_ny="15:00", day_agrees="NO", strength=0.0, session="US_CASH", markets="INDEX",
             exit="time1h", news="IGNORE")
    p.update(kw)
    return p


def idea(**kw):
    """The grid idea with these settings (fields included)."""
    for c in fam().grid():
        d = dict(c.params)
        d.update(markets=c.markets, session=c.session, news=c.news, exit=c.exit)
        if all(d.get(k) == v for k, v in kw.items()):
            return c
    raise KeyError(kw)


def entry_as(name, ccy, like=US500):
    """A made-up index entry under another name and profit currency (synth gives every index USD)."""
    return dataclasses.replace(like, name=name, spec=dataclasses.replace(like.spec, name=name, profit_currency=ccy))


DE40 = entry_as("DE40", "EUR", synth.entry_for("GER40"))
UK100 = entry_as("UK100", "GBP", synth.entry_for("UK100"))


def shift(cols, k0, k1, d):
    """Move bars k0..k1-1 by d points (open, high, low and close)."""
    for arr in cols[1:5]:
        for k in range(k0, min(k1, len(arr))):
            arr[k] += d


def made_day(day, bell, first=0, later=0, amp=AMP):
    """A wiggle day (closes alternate base and base+amp, every minute's high and low 3 points beyond, so the
    first half hour's high-to-low range and the usual 15-minute move are both amp+6 points) whose price steps
    by ``first`` at the 9:30am New York open (so 9:30am to 10am moves by exactly that) and by ``later`` more
    at 1pm New York (so 10am to the afternoon moves by that)."""
    cols = wiggle_columns(day, amp=amp)
    shift(cols, bell, 1440, first)
    shift(cols, bell + 210, 1440, later)
    return cols


def without(cols, drop):
    drop = set(drop)
    return [[x for k, x in enumerate(col) if k not in drop] for col in cols]


def sigs(ser, p, day_index=0):
    dv = ser.dayviews()[day_index]
    return fam().signals(dv, p, dv.span(p["session"]), None)


def day_of(ser, uk):
    return next(dv for dv in ser.dayviews() if dv.uk == uk)


def triples(sg):
    return [(s.i, s.side, s.stop_points) for s in sg]


def width_at(ser, i):
    return ser.f("or_N30_hi")[i] - ser.f("or_N30_lo")[i]


# ------------------------------------------------------------------ the grid ---
def test_the_registry_finds_the_family_from_its_file_and_puts_it_after_the_nine():
    f = fam()
    assert isinstance(f, IntradayMomentum) and IntradayMomentum not in FAM.KNOWN
    assert IntradayMomentum in FAM.discover()
    codes = list(FAM.FAMILIES)
    assert codes[:9] == [k.code for k in FAM.KNOWN] and codes.index(CODE) >= 9
    assert f.label == "Into the close" and f.version == 1 and not f.dormant and not f.reads_news
    assert not f.news_axis
    assert f.features == ("atr", "sess") and f.sensitive == ("strength",) and f.TUNABLE == {"strength": (0.0, 0.9)}
    assert {F.set_of(n) for n in ("atr15", "or_N30_hi", "or_N30_lo")} == set(f.features)   # what signals reads
    grid = f.grid()
    assert len(grid) == f.GRID_SIZE == FAM.GRID_SIZES[CODE] == 36
    assert list(f.AXES.items()) == [("exit", ["time1h", "tr1", "t2"]), ("markets", ["INDEX"]),
                                    ("session", ["US_CASH"]), ("news", ["IGNORE"]),
                                    ("entry_ny", ["15:00", "15:30"]), ("day_agrees", ["NO", "YES"]),
                                    ("strength", [0.0, 0.5, 0.75])]
    assert {c.markets for c in grid} == {"INDEX"} and {c.session for c in grid} == {"US_CASH"}
    assert {c.exit for c in grid} == {"time1h", "tr1", "t2"} and {c.news for c in grid} == {"IGNORE"}
    assert len({c.id for c in grid}) == 36 and all(c.family == CODE and c.version == 1 for c in grid)
    assert "NEWS" not in CODE                                            # the simulator would hand it the news
    # the grid varies the last axis fastest: 12 ideas a exit, the strength changing first
    def setting(c):
        return c.p("entry_ny"), c.p("day_agrees"), c.p("strength")
    assert [c.exit for c in grid] == ["time1h"] * 12 + ["tr1"] * 12 + ["t2"] * 12
    assert [setting(c) for c in grid[:12]] == [(e, a, s) for e in ("15:00", "15:30") for a in ("NO", "YES")
                                               for s in (0.0, 0.5, 0.75)]
    assert [setting(c) for c in grid[12:24]] == [setting(c) for c in grid[24:]] == [setting(c) for c in grid[:12]]
    # registered after every one of the nine's ideas, and not one of their ids changed
    reg = C.registered(True)
    mine = [k for k, c in enumerate(reg) if c.family == CODE]
    known = {k.code for k in FAM.KNOWN}
    assert len(mine) == 36 and all(reg[k].family not in known for k in range(mine[0], len(reg)))
    nine = sorted(c.id for c in reg if c.family in known)
    assert len(nine) == 664 and hashlib.sha1("\n".join(nine).encode()).hexdigest() == NINE_DIGEST
    # the weekly refresh adds new ideas in this order, as far as there is room: with room for 12, every
    # signal setting practises, each on the 1-hour exit
    from mintel.lab.refresh import waiting_ideas
    others = {c.id for c in reg if c.family != CODE}
    assert [c.id for c in waiting_ideas(others, True)[:12]] == [c.id for c in grid[:12]]


def test_practice_shares_one_signals_call_per_setting_and_session():
    from mintel.lab.practice import signal_key
    keys = {signal_key(c, fam()) for c in fam().grid()}
    assert len(keys) == 12                                               # exits, markets and news are free
    assert {signal_key(c, fam()) for c in fam().grid()[:12]} == keys      # the first 12 already hold every one


# ------------------------------------------------------- the US indices only ---
def test_only_the_us_stock_indices_get_a_signal():
    day, bell = "2026-07-01", 13 * 60 + 30
    cols = made_day(day, bell, first=60, later=25)                       # every setting signals on a US index
    us = [synth.entry_for("US500"), synth.entry_for("US30")]
    assert all(e.spec.profit_currency == "USD" for e in us)
    assert (DE40.spec.profit_currency, UK100.spec.profit_currency) == ("EUR", "GBP")
    for p in SETTINGS + OFF_GRID:
        p = params(**p)
        for e in us:
            assert [s.side for s in sigs(synth.series_from_columns(e, cols), p)] == [1], (e.name, p)
        for e in (DE40, UK100):
            assert sigs(synth.series_from_columns(e, cols), p) == [], (e.name, p)
    # a spec with no profit currency: the name decides (upper case, letters and digits only)
    names = {"US500": True, "US30": True, "USTEC": True, "US100": True, "NAS100": True, "SPX500": True,
             "DJ30": True, "us_30.cash": True, "US500.f": True, "Us-Tec": True,
             "DE40": False, "UK100": False, "GER40": False, "JP225": False, "AUS200": False, "STOXX50": False,
             "HK50": False, "XUS500": False, "": False}
    for name, want in names.items():
        ser = synth.series_from_columns(entry_as(name, ""), cols)
        assert is_us_index(ser) is want, name
        assert len(sigs(ser, params())) == (1 if want else 0), name
    # a profit currency, when given, decides it whatever the name
    assert not is_us_index(synth.series_from_columns(entry_as("US500", "EUR"), cols))
    assert is_us_index(synth.series_from_columns(entry_as("WALLST", "USD"), cols))


def test_the_lab_stores_the_dow_and_the_s_and_p_500_and_no_nasdaq():
    """The words say the US indices among the lab's markets are, on this account, the Dow and the S&P 500, so at
    most 2 trades at once: IC Markets calls the Nasdaq USTEC, a name the lab's market kinds do not know (it is not
    in contracts.INDEX_HINTS), so it is never stored. (USTEC is not added there: that would change Trend &
    Breakout's own market classification.)"""
    from mintel import contracts
    from mintel.lab.store import lab_kind
    assert lab_kind("US30") == lab_kind("US500") == "INDEX"
    assert lab_kind("USTEC") == "" and not any(h in "USTEC" for h in contracts.INDEX_HINTS)
    assert "the Dow and the S&P 500" in US_WORDS and "Nasdaq" not in US_WORDS


# -------------------------------------------------------- hand-made days ---
@pytest.mark.parametrize("day,clocks,bell,uk_ahead", SEASONS)
def test_it_trades_the_first_half_hours_way_at_the_new_york_minute_in_every_season(day, clocks, bell, uk_ahead):
    assert ny_minute(day, 9, 30) - day_min(day) == bell == session_bounds("US_CASH", day)[0] - day_min(day)
    for first, side in ((60, 1), (-60, -1)):
        ser = synth.series_from_columns(US500, made_day(day, bell, first=first))
        for entry, after_bell in (("15:00", 5 * 60 + 29), ("15:30", 5 * 60 + 59)):
            got = sigs(ser, params(entry_ny=entry))
            assert len(got) == 1, (day, entry)
            s = got[0]
            assert (s.i, s.side) == (bell + after_bell, side), (day, clocks, entry)
            assert width_at(ser, s.i) == RANGE
            # the signal minute ends at the entry time, New York time: 3pm New York is 7pm or 8pm UK
            ny_behind_utc = 5 if clocks == "GMT" else 4                # EST in winter, EDT otherwise
            ends = dt.datetime.fromtimestamp((ser.t[s.i] + 1) * 60, tz=UTC)
            assert ends.strftime("%H:%M") == f"{int(entry[:2]) + ny_behind_utc:02d}:{entry[3:]}"
            uk_behind = 4 if clocks.startswith(("autumn", "spring")) else 5  # New York's hours behind the UK
            assert ((ends.hour + uk_ahead) % 24, ends.minute) == (int(entry[:2]) + uk_behind, int(entry[3:]))
            assert s.stop_points == ceil(ser.f("atr15")[s.i]) and s.stop_points > 0
            assert s.ref == ("first half hour up" if side > 0 else "first half hour down")


def test_one_signal_a_day_at_most_and_each_day_has_its_own():
    days = ("2026-01-14", "2026-01-15")
    bell = 14 * 60 + 30
    cols = [a + b for a, b in zip(made_day(days[0], bell, first=60), made_day(days[1], bell, first=-60))]
    ser = synth.series_from_columns(US500, cols)
    for p in SETTINGS:
        p = params(**p)
        for k, (uk, side) in enumerate(zip(days, (1, -1))):
            dv = day_of(ser, uk)
            got = fam().signals(dv, p, dv.span("US_CASH"), None)
            assert len(got) <= 1
            if p["day_agrees"] == "NO":                                  # (a flat afternoon never agrees)
                assert [(s.i, s.side) for s in got] == [(k * 1440 + bell + (329 if p["entry_ny"] == "15:00" else 359),
                                                        side)], p
    # a second, bigger move later in the day changes nothing: the idea has had its one signal
    cols = made_day(days[0], bell, first=60)
    shift(cols, bell + 360, 1440, -400)                                 # 3:30pm: a crash after the 3pm signal
    got = sigs(synth.series_from_columns(US500, cols), params())
    assert [(s.i, s.side) for s in got] == [(bell + 329, 1)]


def test_a_flat_first_half_hour_is_skipped_whatever_the_strength():
    day, bell = "2026-07-01", 13 * 60 + 30
    flat = synth.series_from_columns(US500, made_day(day, bell))       # 9:30am and 10am prices are equal
    assert width_at(flat, bell + 329) == RANGE
    for p in SETTINGS + OFF_GRID:
        assert sigs(flat, params(**p)) == [], p                          # not even "any move": zero is no move


@pytest.mark.parametrize("step", [0.5, 0.75])
def test_the_strength_filter_at_just_under_and_just_over_each_step(step):
    """strength = |10am price - 9:30am price| / the first half hour's high-to-low range, read at the signal
    bar. The made-up range is 40 points, so a 20-point move is exactly 0.5 and a 30-point one exactly 0.75."""
    day, bell = "2026-07-01", 13 * 60 + 30
    at = round(step * RANGE)
    assert at / RANGE == step
    for sign in (1, -1):
        for move, passes in ((at - 1, False), (at, True), (at + 1, True)):
            ser = synth.series_from_columns(US500, made_day(day, bell, first=sign * move))
            assert ser.c[bell + 29] - ser.c[bell - 1] == sign * move
            for entry in ("15:00", "15:30"):
                i = bell + (329 if entry == "15:00" else 359)
                assert width_at(ser, i) == RANGE                           # known at the signal bar
                got = sigs(ser, params(entry_ny=entry, strength=step))
                assert [(s.i, s.side) for s in got] == ([(i, sign)] if passes else []), (move, step, entry)
                # every other strength: the move passes exactly the ones it reaches
                for x in (0.0, 0.25, 0.5, 0.75, 0.9):
                    n = len(sigs(ser, params(entry_ny=entry, strength=x)))
                    assert n == (1 if move / RANGE >= x else 0), (move, x)


def test_a_range_that_is_not_known_or_is_zero_passes_only_strength_0():
    day, bell = "2026-07-01", 13 * 60 + 30
    cols = made_day(day, bell, first=60)                                 # a 1.5 strength move
    i = bell + 329

    def series(drop):
        return synth.series_from_columns(US500, without(cols, drop))
    # the opening range needs 60% of its 30 minutes (18): 18 stored is known, 17 is not
    known, unknown = series(range(bell + 1, bell + 13)), series(range(bell + 1, bell + 14))
    for ser, width in ((known, RANGE), (unknown, 0)):
        k = ser.t.index(ny_minute(day, 14, 59))
        assert width_at(ser, k) == width and ser.c[ser.t.index(ny_minute(day, 9, 59))] - ser.c[bell - 1] == 60
    for x in (0.0, 0.25, 0.5, 0.75, 0.9):
        assert len(sigs(known, params(strength=x))) == 1, x
        assert len(sigs(unknown, params(strength=x))) == (1 if x == 0 else 0), x
    # no opening range at all: only the 9:29am and 9:59am minutes (the two prices) stored around it
    bare = series(range(bell, bell + 29))
    k = bare.t.index(ny_minute(day, 14, 59))
    assert width_at(bare, k) == 0 and bare.f("or_N30_hi")[k] == 0
    assert [len(sigs(bare, params(strength=x))) for x in (0.0, 0.25, 0.5, 0.75)] == [1, 0, 0, 0]
    # a zero range: the 30 minutes all at one price, 60 points above the 9:29am close
    flat = made_day(day, bell)
    px = flat[4][bell - 1] + 60
    for k in range(bell, bell + 30):
        set_bar(flat, k, o=px, h=px, l=px, c=px)
    ser = synth.series_from_columns(US500, flat)
    assert ser.f("or_N30_hi")[i] == px and width_at(ser, i) == 0
    assert [s.side for s in sigs(ser, params(strength=0.0))] == [1]
    assert [len(sigs(ser, params(strength=x))) for x in (0.25, 0.5, 0.75, 0.9)] == [0, 0, 0, 0]


def test_the_day_agrees_filter_needs_the_price_on_past_its_10am_price():
    day, bell = "2026-10-28", 13 * 60 + 30
    for first in (60, -60):
        side = 1 if first > 0 else -1
        for later, agrees in ((25 * side, True), (0, False), (-25 * side, False)):
            ser = synth.series_from_columns(US500, made_day(day, bell, first=first, later=later))
            for entry in ("15:00", "15:30"):
                yes = sigs(ser, params(entry_ny=entry, day_agrees="YES"))
                no = sigs(ser, params(entry_ny=entry, day_agrees="NO"))
                assert [s.side for s in no] == [side]                    # without the filter: always
                assert [s.side for s in yes] == ([side] if agrees else []), (first, later, entry)


def test_nothing_before_the_bars_it_needs(monkeypatch):
    day, bell = "2026-07-01", 13 * 60 + 30
    cols = made_day(day, bell, first=60)
    p = params()
    assert IntradayMomentum.LATE_MIN == 2

    def at(drop=(), upto=None, base=None):
        c = without(base or cols, drop)
        if upto is not None:
            c = [col[:upto] for col in c]
        ser = synth.series_from_columns(US500, c)
        return ser, sigs(ser, p)

    ser, got = at()
    assert [(s.i, s.side) for s in got] == [(bell + 329, 1)]
    # the 9:29am minute missing: its price is the 9:28am close, one minute stale (allowed)
    ser, got = at(drop=[bell - 1])
    assert [s.side for s in got] == [1]
    # the stale 9:30am price, at the boundary. The 9:27am close is made 50 points ABOVE the 10am price, so a
    # signal measured from it is a sell
    stale = [list(x) for x in cols]
    top = stale[4][bell + 29] + 50
    set_bar(stale, bell - 3, h=top + 3, c=top)
    assert [s.side for s in at(base=stale)[1]] == [1]                  # 9:28 and 9:29am stored: 9:27am unused
    assert [s.side for s in at(drop=[bell - 2, bell - 1], base=stale)[1]] == [-1]   # LATE_MIN (2) stale: used
    assert at(drop=range(bell - 3, bell), base=stale)[1] == []         # 9:26am: 3 stale, beyond it: refused
    monkeypatch.setattr(IntradayMomentum, "LATE_MIN", 1)
    assert at(drop=[bell - 2, bell - 1], base=stale)[1] == []          # the same 9:27am close, beyond a LATE_MIN of 1
    assert [s.side for s in at(drop=[bell - 1], base=stale)[1]] == [1]
    monkeypatch.undo()
    # the 10am price: 9:58 and 9:59am missing, the 9:57am close (2 stale) is used; 9:57 to 9:59am, none
    assert [s.side for s in at(drop=[bell + 28, bell + 29])[1]] == [1]
    assert at(drop=range(bell + 27, bell + 30))[1] == []
    # the 2:59pm minute missing: the signal is on the 3pm minute, one late
    ser, got = at(drop=[bell + 329])
    assert len(got) == 1 and ser.t[got[0].i] == ny_minute(day, 15, 0)
    # 2:59 and 3pm missing: on the 3:01pm minute, LATE_MIN (2) late, its stop from that minute's usual move
    ser, got = at(drop=[bell + 329, bell + 330])
    assert len(got) == 1 and ser.t[got[0].i] == ny_minute(day, 15, 1) and got[0].side == 1
    assert got[0].stop_points == ceil(ser.f("atr15")[got[0].i])
    # three missing: none
    assert at(drop=range(bell + 329, bell + 332))[1] == []
    # the day's prices stop at 2:58pm New York (a day still being traded): nothing yet
    assert at(upto=bell + 329)[1] == []
    # the usual move unknown at the signal bar (too few 15-minute bars stored to know it): no stop, no signal.
    # Only 9:20 to 10:05am and 2:50 to 3:05pm New York are stored; the first half hour is all there
    keep = set(range(bell - 10, bell + 35)) | set(range(bell + 320, bell + 335))
    ser, got = at(drop=[k for k in range(1440) if k not in keep])
    k = ser.t.index(ny_minute(day, 14, 59))
    assert ser.f("atr15")[k] == 0 and width_at(ser, k) == RANGE and got == []
    # prices that start at 9am New York: no usual move at 10am, but the stop is set at the signal bar's
    late = synth.series_from_columns(US500, [col[bell - 30:] for col in cols])
    assert late.f("atr15")[29 + 30] == 0 and late.f("atr15")[30 + 329] > 0
    assert [(s.i, s.side) for s in sigs(late, p)] == [(30 + 329, 1)]
    # a session span that does not hold the entry minute (London) says nothing
    ser = synth.series_from_columns(US500, cols)
    dv = ser.dayviews()[0]
    assert fam().signals(dv, p, dv.span("LONDON"), None) == []


# ----------------------------------------------------- stops and the exits ---
def random_index_days(n=10, seed=3, entry=US500):
    days = synth.weekdays("2026-07-01", n)
    return synth.series_from_columns(entry, list(synth.gen_columns(entry, days, seed=seed))), entry, days


def test_every_signal_and_every_trade_has_a_stop():
    ser, ent, days = random_index_days()
    n = 0
    for p in SETTINGS + OFF_GRID:
        p = params(**p)
        for dv in ser.dayviews():
            got = fam().signals(dv, p, dv.span("US_CASH"), None)
            assert len(got) <= 1
            for s in got:
                n += 1
                assert isinstance(s.stop_points, int) and s.stop_points >= 1
                assert s.stop_points == ceil(ser.f("atr15")[s.i]) and s.side in (1, -1)
    assert n >= 40, n
    # two US indices and two others, the others on made-up prices of their own
    made_from = {"US500": "US500", "US30": "US30", "DE40": "GER40", "UK100": "UK100"}
    ents = [synth.entry_for("US500"), synth.entry_for("US30"), DE40, UK100]
    series = [synth.series_from_columns(e, list(synth.gen_columns(synth.entry_for(made_from[e.name]), days, seed=3)))
              for e in ents]
    w = synth.window_of(series, days)
    trades, past_close = 0, 0
    traded = set()
    for c in fam().grid():
        if c.p("day_agrees") != "NO":
            continue
        res = sim.run(c, w)
        for t in res.trades:
            trades += 1
            traded.add(t.symbol)
            assert (t.stop < t.entry) if t.side > 0 else (t.stop > t.entry), (c.id, t)
            assert t.exit_reason in ("STOP", "TARGET", "TRAIL", "TIME", "SESSION_END")
            opened, closed = dt.datetime.fromisoformat(t.opened_utc), dt.datetime.fromisoformat(t.closed_utc)
            ny = dt.timezone(dt.timedelta(hours=-4))                     # July: New York on EDT
            assert opened.astimezone(ny).strftime("%H:%M") == c.p("entry_ny")
            assert closed <= dt.datetime.fromtimestamp(flat_min(t.day) * 60, tz=UTC)
            if c.exit == "time1h":
                assert closed - opened <= dt.timedelta(hours=1)
            elif closed.astimezone(ny).strftime("%H:%M") > "16:00":
                past_close += 1                                          # 2:1 and trail: on past the close
    assert traded == {"US500", "US30"}                                    # never DE40 or UK100
    assert trades > 100 and past_close > 0, (trades, past_close)


@pytest.mark.parametrize("day,friday", [("2026-07-01", False), ("2026-01-14", False), ("2026-07-03", True)])
def test_a_3pm_entry_with_the_one_hour_exit_closes_at_the_4pm_close(day, friday):
    bell = (14 if day.startswith("2026-01") else 13) * 60 + 30
    ser = synth.series_from_columns(US500, made_day(day, bell, first=60))
    w = synth.window_of([ser], [day])
    close_utc = dt.datetime.fromisoformat(day).replace(tzinfo=UTC) + dt.timedelta(minutes=bell + 390)
    for entry, held in (("15:00", 60), ("15:30", 30 if friday else 60)):
        res = sim.run(idea(entry_ny=entry, strength=0.5, day_agrees="NO", exit="time1h"), w)
        assert len(res.trades) == 1, res.skipped
        t = res.trades[0]
        opened, closed = dt.datetime.fromisoformat(t.opened_utc), dt.datetime.fromisoformat(t.closed_utc)
        assert t.side == 1 and opened == close_utc - dt.timedelta(minutes=(16 * 60) - int(entry[:2]) * 60 - int(entry[3:]))
        assert closed - opened == dt.timedelta(minutes=held)
        if entry == "15:00":
            assert closed == close_utc and t.exit_reason == "TIME"          # 4pm New York, the close
        elif friday:                                                    # Friday's flat (9pm UK) is the 4pm close
            assert closed == close_utc and t.exit_reason == "SESSION_END"
            assert closed == dt.datetime.fromtimestamp(flat_min(day) * 60, tz=UTC)


def test_live_practice_opens_exactly_the_trades_the_past_test_opens(tmp_path):
    """The practice engine (the DEMO engine's own code) on a made-up live day of two indices, minute by minute
    through a made-up bridge: every idea of the family enters on the same minute, the same way and with the
    same stop as the past test on the same bars. On the US500 it trades; on the UK100 it never does."""
    from mintel.lab import LabPaths
    from mintel.lab.ledger import LabLedger
    from mintel.lab.practice import Practice
    from mintel.lab.prices import ALLOWED, PriceFeed
    from mintel.lab.store import BarStore, write_universe
    from tests.test_lab_practice import Clock, FakeBridge, _register, run_minutes, trades_of
    live_day = "2026-10-13"                                            # a Tuesday: BST in London, EDT in New York
    paths = LabPaths(tmp_path / "data", tmp_path / "logs")
    store = BarStore(paths)
    ents = {"US500": US500, "UK100": UK100}
    cols = {s: synth.gen_columns(synth.entry_for(s), ["2026-10-09", "2026-10-12", live_day], seed=31) for s in ents}
    for s, e in ents.items():
        k = cols[s][0].index(day_min(live_day))
        store.merge_columns(s, *(x[:k] for x in cols[s]), point=e.spec.point, digits=e.spec.digits,
                            source="SYNTHETIC", now=dt.datetime(2026, 10, 13, 6, tzinfo=UTC))
    write_universe(paths, list(ents.values()))
    ideas = fam().grid()
    led = LabLedger(paths.sqlite)
    _register(led, ideas, "IDEA")
    start = dt.datetime(2026, 10, 13, 12, 0, 3, tzinfo=UTC)
    clock = Clock(start)
    bridge = FakeBridge(clock, cols, ents)
    feed = PriceFeed(factory=bridge.factory)
    feed.ping()
    eng = Practice(paths, feed, ledger=led, now_fn=clock, log=lambda m: None, prime_bars=3000)
    run_minutes(eng, clock, start, dt.datetime(2026, 10, 13, 21, 0, 3, tzinfo=UTC), every_status=600)
    rows = trades_of(led)
    live = {c.id: [(r["symbol"], r["opened_utc"][11:16], r["side"], round(r["stop"], 2)) for r in rows
                   if r["candidate_id"] == c.id] for c in ideas}
    win = synth.window_of([synth.series_from_columns(e, [list(x) for x in cols[s]]) for s, e in ents.items()],
                          [live_day])
    past = {c.id: [(t.symbol, t.opened_utc[11:16], t.side, round(t.stop, 2)) for t in sim.run(c, win).trades]
            for c in ideas}
    assert live == past
    opened = {x[1] for v in past.values() for x in v}
    assert opened == {"19:00", "19:30"}                                 # 3pm and 3:30pm New York, both taken
    assert {x[0] for v in past.values() for x in v} == {"US500"}         # never the UK100
    assert sum(len(v) for v in past.values()) >= 12
    assert all(r["stop"] is not None and r["closed_utc"] <= "2026-10-13T20:55:00+00:00" for r in rows)
    assert set(bridge.calls) <= ALLOWED                                 # prices only
    led.close()


# ------------------------------------------------------- nothing looks ahead ---
def changed_after(cols, k):
    """The same bars up to k-1; every bar from k on replaced by a very different made-up path."""
    out = [list(col) for col in cols]
    t, o, h, l, c = out[:5]
    px = c[k - 1] if k else 110000
    for j in range(k, len(t)):
        step = ((j * 7919) % 61) - 30
        op = px + 900 if j == k else px                                # a gap, then a different walk
        px = op + step
        o[j], c[j] = op, px
        h[j], l[j] = max(op, px) + 4 + j % 5, min(op, px) - 4 - j % 3
        out[5][j] = 3 + j % 4
        out[6][j] = 50 + j % 90
    return out


def cut_points(ser):
    """Minutes to cut a day at: just after each 9:30am and 10am price, inside the first half hour (while its
    range is still forming), mid-afternoon, around each entry."""
    pts = []
    for dv in ser.dayviews():
        for hh, mm in ((9, 30), (9, 45), (10, 0), (10, 1), (12, 41), (14, 59), (15, 0), (15, 1), (15, 29),
                       (15, 30), (15, 31)):
            k = dv.index_at(ny_minute(dv.uk, hh, mm))
            if dv.i0 < k < dv.i1:
                pts.append(k)
    return sorted(set(pts))


def by_strength(found, settings):
    """Signals counted by strength setting (to show the filter really took some out)."""
    out = collections.Counter()
    for k, p in enumerate(settings):
        out[p["strength"]] += len(found[k])
    return out


def test_a_days_signals_are_the_same_when_every_later_bar_is_changed_or_removed():
    ser, ent, days = random_index_days(6, seed=8)
    cols = [list(getattr(ser, x)) for x in ("t", "o", "h", "l", "c", "s", "v")]
    cuts = cut_points(ser)
    settings = [params(**p) for p in SETTINGS + OFF_GRID]
    full = {k: [] for k in range(len(settings))}
    for k, p in enumerate(settings):
        for dv in ser.dayviews():
            for s in fam().signals(dv, p, dv.span("US_CASH"), None):
                full[k].append((dv.uk, s.i, s.side, s.stop_points))
                cuts.append(s.i + 1)                                    # right after every signal bar
    assert sum(len(v) for v in full.values()) >= 25
    counts = by_strength(full, settings)
    assert counts[0.0] > counts[0.5] > 0 and counts[0.5] >= counts[0.75] > 0, counts   # the filter bites
    checked = 0
    for cut in sorted(set(cuts)):
        for kind, made in (("removed", [col[:cut] for col in cols]), ("changed", changed_after(cols, cut))):
            other = synth.series_from_columns(ent, made)
            if kind == "changed":
                assert list(other.c[:cut]) == list(ser.c[:cut]) and list(other.c[cut:]) != list(ser.c[cut:])
            for k, p in enumerate(settings):
                want = [x for x in full[k] if x[1] < cut]
                got = []
                for dv in other.dayviews():
                    lo, hi = dv.span("US_CASH")
                    got += [(dv.uk, s.i, s.side, s.stop_points)
                            for s in fam().signals(dv, p, (lo, hi), None) if s.i < cut]
                assert got == want, (kind, cut, p)
                checked += 1
    assert checked > 500


@pytest.mark.parametrize("first", [-60, -20, 30])
def test_the_hand_made_days_signal_is_the_same_when_every_later_bar_is_changed_or_removed(first):
    """A clear move (strength 1.5) and moves exactly at the 0.5 and 0.75 steps, where one point of range or of
    move either way would change the answer."""
    for day, clocks, bell, _ in SEASONS:
        cols = made_day(day, bell, first=first, later=-20 if first < 0 else 20)
        ser = synth.series_from_columns(US500, cols)
        for p in SETTINGS + OFF_GRID:
            p = params(**p)
            whole = triples(sigs(ser, p))
            assert whole == [] or len(whole) == 1
            assert (whole != []) == (abs(first) / RANGE >= p["strength"]), (day, first, p)
            for i, _side, _stop in whole:
                for made in ([col[:i + 1] for col in cols], changed_after(cols, i + 1)):
                    assert triples(sigs(synth.series_from_columns(US500, made), p)) == whole, (day, p)
            # and before the signal minute, nothing (removed or changed alike), cut inside the first half
            # hour too, while its range is still forming
            for cut in (bell, bell + 15, bell + 30, bell + 328):
                for made in ([col[:cut] for col in cols], changed_after(cols, cut)):
                    got = triples(sigs(synth.series_from_columns(US500, made), p))
                    assert [x for x in got if x[0] < cut] == [], (day, cut, p)


def as_the_day_grows(cols, entry, settings):
    """The family on a whole series, and on that series growing a bar at a time as live practice sees it
    (features.LiveSeries). Returns per setting: the whole series' signals, the signals ON each newly finished
    bar (all practice takes), and the bars at which a growing day's signals were not the whole day's so far."""
    f = fam()
    full = synth.series_from_columns(entry, [list(x) for x in cols])
    whole = [{dv.uk: triples(f.signals(dv, p, dv.span("US_CASH"), None)) for dv in full.dayviews()}
             for p in settings]
    live = F.LiveSeries(entry)
    on_the_bar = [[] for _ in settings]
    differ = [[] for _ in settings]
    in_span = 0
    for i in range(len(cols[0])):
        live.push(*(x[i] for x in cols))
        dvs = live.dayviews()
        if not dvs or not (dvs[-1].i0 <= i < dvs[-1].i1):
            continue
        dv = dvs[-1]
        lo, hi = dv.span("US_CASH")
        if not (lo <= i < hi):
            continue
        in_span += 1
        for k, p in enumerate(settings):
            got = triples(f.signals(dv, p, (lo, hi), None))
            on_the_bar[k] += [x for x in got if x[0] == i]
            if got != [x for x in whole[k][dv.uk] if x[0] <= i]:
                differ[k].append(i)
    return whole, on_the_bar, differ, in_span


@pytest.mark.parametrize("symbol,seed", [("US500", 5), ("US30", 13)])
def test_signals_found_bar_by_bar_as_the_day_grows_are_exactly_the_whole_days(symbol, seed):
    """For every setting (and two off the grid): at every bar of the session a growing day's signals so far
    are the whole day's up to that bar, and the signals on each newly finished bar, taken together, are the
    whole series' list, none twice."""
    ent = synth.entry_for(symbol)
    days = synth.weekdays("2026-10-26", 6)                       # the autumn week the clocks disagree, and after
    cols = synth.gen_columns(ent, days, seed=seed)
    settings = [params(**p) for p in SETTINGS + OFF_GRID]
    whole, on_the_bar, differ, in_span = as_the_day_grows(cols, ent, settings)
    assert in_span == 6 * 390                                           # every minute of six US sessions
    total = 0
    for k, p in enumerate(settings):
        everything = sorted(x for v in whole[k].values() for x in v)
        assert sorted(on_the_bar[k]) == everything, p
        assert len(set(on_the_bar[k])) == len(on_the_bar[k]), p
        assert differ[k] == [], (p, differ[k][:5])
        total += len(everything)
    assert total >= 30, "the check proved nothing: too few signals"
    counts = by_strength([[x for v in whole[k].values() for x in v] for k in range(len(settings))], settings)
    assert counts[0.0] > counts[0.75] > 0, counts                       # the strength filter took some out


def test_the_family_keeps_no_state_between_calls():
    ser, ent, days = random_index_days(4, seed=2)
    f = fam()
    before = dict(vars(f))
    first = [[triples(f.signals(dv, params(**p), dv.span("US_CASH"), None)) for dv in ser.dayviews()]
             for p in SETTINGS]
    again = [[triples(f.signals(dv, params(**p), dv.span("US_CASH"), None)) for dv in ser.dayviews()]
             for p in reversed(SETTINGS)]
    assert first == list(reversed(again)) and vars(f) == before == {}


# ---------------------------------------------------------- names and words ---
def variants():
    """The grid, and the kind of off-grid neighbours the weekly refresh makes for a winner."""
    from mintel.lab.refresh import neighbour_values
    out = {c.id: c for c in fam().grid()}
    for c in fam().grid()[:12]:
        x = c.p("strength")
        for v in neighbour_values(fam().AXES["strength"], x, *fam().TUNABLE["strength"]):
            kid = c.replace(params={"strength": v})
            out.setdefault(kid.id, kid)
    for v in (0.3125, 0.875):
        kid = fam().grid()[-1].replace(params={"strength": v})
        out.setdefault(kid.id, kid)
    return list(out.values())


def test_names_and_words_are_generated_unique_and_plain():
    cs = variants()
    assert len({c.id for c in cs}) == len(cs) > 36
    assert {0.25, 0.625}.issubset({c.p("strength") for c in cs})          # the refresh's own neighbours
    names = [C.name_of(c) for c in cs]
    assert len(set(names)) == len(cs)
    words = [tuple(C.words_of(c)) for c in cs]
    assert len(set(words)) == len(cs)
    for c, n, w in zip(cs, names, words):
        assert n == fam().name(c) and n.startswith(fam().subject(c)) and len(n) < 150, n
        assert n.endswith(f" on the US stock indices; {C.EXIT_SHORT[c.exit]}") and "news" not in n, n
        assert "_" not in n and not re.search(r"\b(IGNORE|AVOID|RANGE|ATR|t15w|tr1|tr2|BOTH)\b", n), n
        text = " ".join((n,) + w)
        assert not JARGON.search(text), (c.id, JARGON.search(text).group(0))
        assert not re.search(r"\b(entry_ny|day_agrees|min_move|strength|US_CASH|INDEX|YES|NO)\b", text), text
        assert not re.search(r"\b1(\.0)? swings\b|\b\d+\.0 swing|\b0(\.0)? swings\b", text), text
        assert "  " not in text and "( " not in text and " )" not in text
        assert "color" not in text.lower() and "favor" not in text.lower()
        assert "US or not" not in text and "every stored index" not in text
        when = clock_words(c.p("entry_ny"))
        assert f"in at {when} New York" in n and any(f"At {when} New York time" in x for x in w)
        assert w[0] == f"Trades {US_WORDS}." and "£13" in text and "9:55pm UK" in text
        assert "One trade a day at most in each of the US indices (so up to 2 at once)." in text
        assert "Nasdaq" not in text and "up to 3 at once" not in text and "a week or two" not in text
        assert ("UK time for most of the year (7pm for two or three weeks in March and about a week around the end "
                "of October)." if when == "3pm" else
                "UK time for most of the year (7:30pm for two or three weeks in March and about a week around the "
                "end of October).") in text
        # past the close: every idea but a 3pm entry with the 1-hour exit (out at the 4pm close) can be
        always_out = c.exit == "time1h" and when == "3pm"
        assert ("for the run into the 4pm close." in text) == always_out
        assert ("for the run into (and sometimes past) the 4pm close." in text) == (not always_out)
        friday = "On a Friday the 9pm UK flat usually closes it at the 4pm close."
        assert (friday in text) == (c.exit in ("tr1", "t2"))
        if c.exit == "time1h" and when == "3:30pm":
            assert "(on a Friday the 9pm UK flat usually closes it at the 4pm close)" in text
        # the trail's size: one swing (atr5), defined once, while the stop is one usual move (atr15)
        trail = ("Once the trail starts, it follows one swing behind the best price, not one usual move (a swing = "
                 "how far the price has typically moved in a recent 5-minute stretch).")
        assert (trail in text) == (c.exit == "tr1") and text.count("(a swing = ") == (c.exit == "tr1")
        assert w[-4].startswith("Stops one usual move away")             # its own stop, then the three shared lines
        assert not any("news" in x for x in w)                            # it does not look at the news at all
        assert ("day agrees" in n) == (c.p("day_agrees") == "YES")
        x = c.p("strength")
        assert ("any move" in n) == (not x) and ("Any rise or fall counts" in text) == (not x)
        assert ("of the first half hour's high-to-low range" in text) == bool(x)
        assert text.count("(the usual move = ") == 1                     # explained once
        held = "it can stay open after the close, until the 9:55pm UK flat (4:55pm New York for most of the year)"
        assert (held in text) == (c.exit in ("tr1", "t2")), (c.exit, text)
        assert ("With its 1-hour exit it closes at" in text) == (c.exit == "time1h")
    one = idea(entry_ny="15:00", strength=0.5, day_agrees="YES", exit="time1h")
    assert C.name_of(one) == ("First half hour, in at 3pm New York (moved at least half its range, day agrees) on "
                              "the US stock indices; out after 1 hour")
    w = " ".join(C.words_of(one))
    assert "closes at the 4pm New York close" in w and "8pm UK time for most of the year" in w
    assert "the move had to cover at least half of the first half hour's high-to-low range" in w
    assert "carried on past its 10am price" in w
    late = idea(entry_ny="15:30", strength=0.75, day_agrees="NO", exit="time1h")
    w = " ".join(C.words_of(late))
    assert "closes at 4:30pm New York time, 30 minutes after the close" in w
    assert "the move had to cover at least three quarters of the first half hour's high-to-low range" in w
    assert C.name_of(late) == ("First half hour, in at 3:30pm New York (moved at least three quarters of its range) "
                               "on the US stock indices; out after 1 hour")
    trail = " ".join(C.words_of(idea(entry_ny="15:00", strength=0.0, day_agrees="NO", exit="tr1")))
    assert "If its stop or the trail has not ended the trade by the 4pm close" in trail
    two = " ".join(C.words_of(idea(entry_ny="15:30", strength=0.5, day_agrees="YES", exit="t2")))
    assert "If its stop or its target has not ended the trade by the 4pm close" in two
    odd = fam().grid()[0].replace(params={"strength": 0.625})
    assert "moved at least 62.5% of its range" in C.name_of(odd)
    assert "at least 62.5% of the first half hour's high-to-low range" in " ".join(C.words_of(odd))
    assert clock_words("15:00") == "3pm" and clock_words("15:30") == "3:30pm" and clock_words("09:30") == "9:30am"


def test_the_short_names_are_unique_across_the_whole_registry():
    cands = C.registered(news_awake=True)
    short = [C.short_name_of(c) for c in cands]
    assert len(set(short)) == len(cands) and max(len(s) for s in short) <= 90
    mine = [s for c, s in zip(cands, short) if c.family == CODE]
    assert len(mine) == 36 and all(s.startswith("Into the close · US indices · ") for s in mine)
    assert C.short_name_of(idea(entry_ny="15:30", strength=0.5, day_agrees="YES", exit="t2")) == \
        "Into the close · US indices · 3:30pm New York, half-range move, agrees · 2:1"
    assert C.short_name_of(idea(entry_ny="15:00", strength=0.75, day_agrees="NO", exit="tr1")) == \
        "Into the close · US indices · 3pm New York, 3/4-range move · trail 1"
    assert C.short_name_of(idea(entry_ny="15:00", strength=0.0, day_agrees="NO", exit="time1h")) == \
        "Into the close · US indices · 3pm New York, any move · 1h"
    assert all(len(C.short_name_of(c)) <= 90 for c in variants())
    assert len({C.short_name_of(c) for c in variants()}) == len(variants())


def test_the_dashboard_nickname_and_what_it_does():
    from mintel.ops import squad as SQ
    c = idea(entry_ny="15:00", strength=0.5, day_agrees="NO", exit="time1h")
    t = {"id": f"lab:{c.id}", "kind": "lab", "family": c.family, "markets": c.markets, "session": c.session,
         "params": c.pdict(), "exit": c.exit}
    assert SQ.lab_noun(CODE, c.pdict()) == "Closer 3pm" and SQ.lab_noun(CODE, {}) == "Closer"
    assert SQ.lab_noun(CODE, {"entry_ny": "15:30"}) == "Closer 3:30pm"
    assert SQ.nickname_base(t) == "Wall St Index Closer 3pm"
    text = SQ.what_it_does(t)
    assert not text.startswith("An idea from the lab")
    assert "first half hour" in text and "15:00 New York time" in text
    assert "for the run into the 4pm close." in text                       # a 3pm entry out after 1 hour
    assert f"It trades {US_WORDS} and out after 1 hour." in text
    assert "Nasdaq" not in text and "It trades the stock indices" not in text   # no contradiction left
    assert SQ.when_it_trades(t, [])[0] == f"{US_WORDS[0].upper()}{US_WORDS[1:]}, during the US stock market's " \
        "opening hours."
    for other in (idea(entry_ny="15:30", strength=0.5, day_agrees="NO", exit="time1h"),
                  idea(entry_ny="15:00", strength=0.5, day_agrees="NO", exit="tr1"),
                  idea(entry_ny="15:00", strength=0.5, day_agrees="NO", exit="t2")):
        t2 = dict(t, params=other.pdict(), exit=other.exit)
        assert "for the run into (and sometimes past) the 4pm close." in SQ.what_it_does(t2), other.exit
    assert SQ.nickname_base(dict(t, params={"entry_ny": "15:30"})) == "Wall St Index Closer 3:30pm"
    items = [{"id": f"lab:{x.id}", "kind": "lab", "family": x.family, "markets": x.markets, "session": x.session,
              "params": x.pdict(), "order": "2026-10-18T22:30:00+00:00"} for x in fam().grid()]
    names = SQ.assign_nicknames(items)
    assert len(set(names.values())) == 36 and max(len(n) for n in names.values()) <= 34
