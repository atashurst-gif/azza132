"""VWAP_TREND, "Bounce off the average": join a trend when the price dips back to the session's average.

Pinned here: the grid (64 ideas), its order (every signal setting first, for the weekly refresh) and that the
registry finds the family by itself, after the nine and without touching an existing idea's id; the signals on
hand-made days (direction, minute, stop, at most two a day) at the London and US cash opens in BST, GMT and the weeks
the clocks disagree; that nothing fires before the bars it needs; that each filter works, each rule on a day of its
own (a close more than the zone's distance from the average before the dip, a deeper low moving the stop, the
half-hour wait before a second dip, the trend still on at the close back out, the two-a-day cap); that only a FRESH
dip counts (one already under way when the count comes up, the slower trend comes back or the half-hour wait ends is
not bought), the count's exact minute, and a close on the average or through it; that every signal
leaves a stop on the far side of the average; that NOTHING LOOKS AHEAD
(the signals up to a bar are the same when every later bar is changed or removed, and when the day is fed one bar
at a time as live practice sees it); and that the names and words are generated, unique and plain. MADE-UP prices.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import re
from math import ceil

import pytest

from mintel.lab import day_min, features as F, sim, synth
from mintel.lab import candidate as C
from mintel.lab import families as FAM
from mintel.lab.families.vwap_trend import VwapTrend
from mintel.lab.store import session_bounds
from tests.test_lab_families import JARGON, cand_of, random_series

CODE = "VWAP_TREND"
FAMV = FAM.get(CODE)
EUR = synth.entry_for("EURUSD")
B = 110000
# every combination of the settings the signals read (exit, markets and news never change them)
SETTINGS = [dict(side_min=n, touch_sd=k, trend_tf=tf, session=s)
            for n in (30, 60) for k in (0.25, 0.5) for tf in ("M15", "H1") for s in ("US_CASH", "LONDON")]
# the session opens in UTC minutes of the day: BST, GMT, and the weeks the US is on summer time and the UK is not
OPENS = {("2026-07-01", "US_CASH"): 13 * 60 + 30, ("2026-07-01", "LONDON"): 7 * 60,
         ("2026-01-14", "US_CASH"): 14 * 60 + 30, ("2026-01-14", "LONDON"): 8 * 60,
         ("2026-03-10", "US_CASH"): 13 * 60 + 30, ("2026-03-10", "LONDON"): 8 * 60,
         ("2026-10-27", "US_CASH"): 13 * 60 + 30, ("2026-10-27", "LONDON"): 8 * 60}


def p_of(**kw):
    p = dict(side_min=30, touch_sd=0.25, trend_tf="M15", session="US_CASH")
    p.update(kw)
    return sim.family_params(cand_of(CODE, **p))


# ------------------------------------------------------------------- the grid ---
def test_the_grid_is_64_ideas_and_the_registry_finds_the_family_by_itself():
    assert isinstance(FAMV, VwapTrend) and VwapTrend not in FAM.KNOWN          # a new file, not one of the nine
    assert VwapTrend in FAM.discover()                                          # found by its file name alone
    order = list(FAM.FAMILIES)
    known = [k.code for k in FAM.KNOWN]
    assert order[:len(known)] == known and CODE in order[len(known):]          # discovered families come after the nine
    grid = FAMV.grid()
    assert FAMV.GRID_SIZE == len(grid) == 64 == FAM.GRID_SIZES[CODE]
    assert FAMV.label == "Bounce off the average" and FAMV.version == 1
    reg = C.registered()
    where = [k for k, c in enumerate(reg) if c.family == CODE]
    assert [reg[k].id for k in where] == [c.id for c in grid]
    assert where == list(range(where[0], where[0] + 64))                       # one block ...
    assert all(c.family not in known for c in reg[where[0]:])                  # ... after every idea of the nine
    assert len({c.id for c in grid}) == 64
    assert {c.session for c in grid} == {"US_CASH", "LONDON"}
    assert {c.exit for c in grid} == {"t2", "tr2"} and {c.markets for c in grid} == {"INDEX", "FX_MAJOR"}
    assert {c.news for c in grid} == {"IGNORE"} and not FAMV.news_axis and not FAMV.reads_news
    assert not FAMV.dormant and "NEWS" not in CODE
    for s in {c.session for c in grid}:                                         # each session has an average of its own
        assert F.set_of(f"vwap_{s}") == "vwap" and F.set_of(f"sig_{s}") == "vwap"
    assert set(FAMV.features) <= set(F.OUTPUTS)
    for param in FAMV.sensitive:
        assert all(isinstance(v, (int, float)) for v in FAMV.axis_values(param))
        lo, hi = FAMV.TUNABLE[param]
        assert all(lo <= v <= hi for v in FAMV.axis_values(param))


def test_the_first_ideas_cover_every_signal_setting_for_the_weekly_refresh(tmp_path):
    """The refresh adds new ideas in grid order only as far as there is room, and the last axis varies fastest:
    so the axes run markets, exit, session, trend, zone and minutes, and the first 16 ideas are every setting the
    signals read on the indices with the 2:1 target; then the same 16 trailed; then the majors."""
    from mintel.lab import LabPaths, LabSettings
    from mintel.lab import refresh as RF
    from mintel.lab.ledger import LabLedger
    assert list(FAMV.AXES) == ["markets", "exit", "news", "session", "trend_tf", "touch_sd", "side_min"]
    grid = FAMV.grid()
    signal = lambda c: (c.session, c.p("trend_tf"), c.p("touch_sd"), c.p("side_min"))
    every = sorted((s["session"], s["trend_tf"], s["touch_sd"], s["side_min"]) for s in SETTINGS)
    blocks = [grid[k:k + 16] for k in range(0, 64, 16)]
    assert [{(c.markets, c.exit) for c in b} for b in blocks] == [{("INDEX", "t2")}, {("INDEX", "tr2")},
                                                                  {("FX_MAJOR", "t2")}, {("FX_MAJOR", "tr2")}]
    assert all(sorted(signal(c) for c in b) == every for b in blocks)
    assert all([signal(c) for c in b] == [signal(c) for c in blocks[0]] for b in blocks)
    # a refresh with room for 16 takes exactly the first block
    paths = LabPaths(tmp_path / "data")
    paths.root.mkdir(parents=True)
    led = LabLedger(paths.sqlite)
    held = [c for c in C.registered() if c.family != CODE]
    for c in held:
        led.register(c, FAM.get(c.family).name(c))
    rep = RF.refresh_pool(led, paths, LabSettings.from_dict({"pool_target": len(held) + 16}), night="2026-10-17",
                          now=dt.datetime(2026, 10, 17, 22, 30, tzinfo=dt.timezone.utc))
    assert [x["id"] for x in rep["new"]] == [c.id for c in blocks[0]]
    led.close()


def test_adding_the_family_changes_no_existing_ideas_id():
    """The nine families' 664 ideas (NEWS_MOVE awake) are exactly the ones pinned before this family arrived."""
    known = {k.code for k in FAM.KNOWN}
    nine = sorted(c.id for c in C.registered(True) if c.family in known)
    assert len(nine) == 664
    assert hashlib.sha1("\n".join(nine).encode()).hexdigest() == "21ba9a386edb1b961834c1f692a039ab8b300ff1"
    assert not {c.id for c in FAMV.grid()} & {c.id for c in C.registered(True) if c.family != CODE}


# ------------------------------------------------------------ hand-made days ---
def rising_columns(days, slope=1.0, base=B, amp=3, pre=None):
    """Made-up M1 bars for whole UTC days: each close ``slope`` points on from the last (``pre`` = the slope
    up to that bar index, if different), highs and lows ``amp`` points outside the open and close."""
    t, o, h, l, c, s, v = [], [], [], [], [], [], []
    px, prev, k = float(base), base, 0
    for d in days:
        m0 = day_min(d)
        for j in range(1440):
            px += pre[1] if pre is not None and k < pre[0] else slope
            cl = int(round(px))
            t.append(m0 + j), o.append(prev), c.append(cl), s.append(4), v.append(100)
            h.append(max(prev, cl) + amp), l.append(min(prev, cl) - amp)
            prev, k = cl, k + 1
    return [t, o, h, l, c, s, v]


def series_of(cols, entry=EUR):
    return synth.series_from_columns(entry, [list(x) for x in cols])


def day_of(ser, uk):
    return next(dv for dv in ser.dayviews() if dv.uk == uk)


def sigs(ser, p, uk):
    dv = day_of(ser, uk)
    return [(s.i, s.side, s.stop_points) for s in FAMV.signals(dv, p, dv.span(p["session"]), None)]


def plant_dip(cols, at, session, low=-2, close=1, gap=False):
    """Bar ``at`` falls back to the session's average: its low ``low`` points from the average as it stood a bar
    earlier, its close ``close`` points from it; the bar after opens there and closes back where the rise was (with
    ``gap``, it is left as it was: it opens back up where the rise was, its low well clear of the zone)."""
    return plant_bars(cols, at, session, [(low, close)], gap=gap)


def plant_bars(cols, at, session, bars, gap=False):
    """``plant_dip`` over several bars: bar ``at + j`` has the low and close ``bars[j]``, in points from the
    average as it stood the bar before ``at``."""
    v = ceil(series_of(cols).f(f"vwap_{session}")[at - 1])
    t, o, h, l, c = cols[:5]
    for j, (low, close) in enumerate(bars):
        k = at + j
        o[k], c[k], l[k] = c[k - 1], v + close, v + low
        h[k] = max(o[k], c[k]) + 1
    end = at + len(bars)
    if not gap:
        o[end] = c[end - 1]
        h[end], l[end] = max(h[end], o[end]), min(l[end], o[end])
    return cols


def legs_columns(days, legs, base=B, amp=3):
    """``rising_columns`` with the slope changing: ``legs`` is [(up to this bar index, slope), ...], the last
    one's index None (to the end)."""
    t, o, h, l, c, s, v = [], [], [], [], [], [], []
    px, prev, k = float(base), base, 0
    for d in days:
        m0 = day_min(d)
        for j in range(1440):
            px += next(sl for until, sl in legs if until is None or k < until)
            cl = int(round(px))
            t.append(m0 + j), o.append(prev), c.append(cl), s.append(4), v.append(100)
            h.append(max(prev, cl) + amp), l.append(min(prev, cl) - amp)
            prev, k = cl, k + 1
    return [t, o, h, l, c, s, v]


def mirror(cols, base=B):
    """The same day upside down: every rise a fall, every dip a rally."""
    t, o, h, l, c, s, v = cols
    f = lambda xs: [2 * base - x for x in xs]
    return [list(t), f(o), f(l), f(h), f(c), list(s), list(v)]


def zone(ser, i, session, k):
    vw, sg = ser.f(f"vwap_{session}")[i], ser.f(f"sig_{session}")[i]
    return vw, k * sg


def expected_stop(ser, i, dip, session, side=1):
    vw = ser.f(f"vwap_{session}")[i]
    ref = min(vw, ser.l[dip]) if side > 0 else max(vw, ser.h[dip])
    return ceil((ser.c[i] - ref) * side + 0.5 * ser.f("atr5")[i])


@pytest.mark.parametrize("day,session", sorted(OPENS))
def test_buys_the_first_close_back_out_after_a_dip_to_the_average_at_the_right_clock_time(day, session):
    """Two days of a steady rise (so the 15-minute trend averages are warm by the London open); on the second
    the price dips back to the session's average an hour after the open, then closes back up. The clock: London
    opens at 08:00 London time and the US cash session at 09:30 New York time, by the time-zone database."""
    prev = {"2026-07-01": "2026-06-30", "2026-01-14": "2026-01-13", "2026-03-10": "2026-03-09",
            "2026-10-27": "2026-10-26"}[day]
    opn = OPENS[(day, session)]
    assert session_bounds(session, day)[0] == day_min(day) + opn
    cols = rising_columns([prev, day])
    dip = 1440 + opn + 60
    plant_dip(cols, dip, session)
    ser = series_of(cols)
    vw, tol = zone(ser, dip, session, 0.25)
    assert vw < ser.c[dip] <= vw + tol and ser.l[dip] <= vw + tol                # the dip: in the zone, above the average
    assert ser.c[dip + 1] - zone(ser, dip + 1, session, 0.25)[0] > tol              # ... then a close back out of it
    assert ser.f("ema20_M15")[dip] > ser.f("ema50_M15")[dip] > 0
    p = p_of(session=session, side_min=30, touch_sd=0.25, trend_tf="M15")
    got = sigs(ser, p, day)
    assert got == [(dip + 1, 1, expected_stop(ser, dip + 1, dip, session))]
    assert ser.t[got[0][0]] == day_min(day) + opn + 61                               # 61 minutes after the open, UTC
    # the hour-long count is not up yet at that dip: nothing at all (the trend's run started at the 10th minute,
    # once the average is known)
    assert sigs(ser, dict(p, side_min=60), day) == []
    # a dip after the hour is bought by both
    cols = rising_columns([prev, day])
    plant_dip(cols, 1440 + opn + 75, session)
    ser = series_of(cols)
    for n in (30, 60):
        assert [g[:2] for g in sigs(ser, dict(p, side_min=n), day)] == [(1440 + opn + 76, 1)], n


def test_a_downtrend_is_sold_the_same_way_upside_down():
    day = "2026-07-01"
    cols = rising_columns(["2026-06-29", "2026-06-30", day])                       # three days: the hourly averages warm
    dip = 2 * 1440 + 810 + 60
    plant_dip(cols, dip, "US_CASH")
    up, down = series_of(cols), series_of(mirror(cols))
    for p in (p_of(), p_of(touch_sd=0.5), p_of(trend_tf="H1")):
        a, b = sigs(up, p, day), sigs(down, p, day)
        assert a and [(i, -s) for i, s, _ in a] == [(i, s) for i, s, _ in b], p
        assert all(abs(x[2] - y[2]) <= 1 for x, y in zip(a, b))
    (i, side, stop), = sigs(down, p_of(), day)
    assert side == -1 and stop == expected_stop(down, i, dip, "US_CASH", side=-1)
    assert down.c[i] + stop > down.f("vwap_US_CASH")[i]                            # the stop is above the average


def test_nothing_fires_before_the_average_is_known_and_the_count_is_up():
    """The average needs the session's first 10 minutes, then every close for ``side_min`` minutes on one side:
    the earliest signal follows the first dip after that, here on a session that dips every three minutes from
    its 12th minute on."""
    day = "2026-07-01"
    for n in (30, 60):
        cols = rising_columns(["2026-06-30", day])
        opn = 1440 + 810
        dips = list(range(opn + 12, opn + 9 + n + 6, 3))                            # a dip every 3 minutes
        for at in dips:
            plant_dip(cols, at, "US_CASH")
        ser = series_of(cols)
        p = p_of(side_min=n)
        assert ser.f("vwap_US_CASH")[opn + 8] == 0 and ser.f("vwap_US_CASH")[opn + 9] > 0
        got = sigs(ser, p, day)
        first = next(at for at in dips if ser.t[at] + 1 - ser.t[opn + 9] >= n)    # the first dip once the count is up
        assert got and got[0][:2] == (first + 1, 1), (n, got)
        assert all(ser.t[i] >= ser.t[opn + 9] + n - 1 for i, _, _ in got), (n, got)
    # and nothing at all on the first day, before the 15-minute averages are warm (the London open, BST)
    ser = series_of(plant_dip(rising_columns(["2026-07-01"]), 420 + 60, "LONDON"))
    assert ser.f("ema50_M15")[480] == 0 and sigs(ser, p_of(session="LONDON"), "2026-07-01") == []


def test_the_slower_trend_must_agree():
    """A morning falling hard, then a rise from the US open with a dip to its average: the closes are above the
    average, but the 15-minute trend is still down, so nothing is bought (and nothing sold: the closes are not
    below the average)."""
    day = "2026-07-01"
    opn = 810
    cols = rising_columns([day], slope=1.0, pre=(opn, -1.5))
    dip = opn + 60
    plant_dip(cols, dip, "US_CASH")
    ser = series_of(cols)
    vw, tol = zone(ser, dip, "US_CASH", 0.25)
    assert vw < ser.c[dip] and ser.l[dip] <= vw + tol
    assert 0 < ser.f("ema20_M15")[dip] < ser.f("ema50_M15")[dip]
    for p in (p_of(), p_of(touch_sd=0.5)):
        assert sigs(ser, p, day) == []
    # the same session after a rising morning is bought
    cols = rising_columns([day])
    plant_dip(cols, dip, "US_CASH")
    assert [g[:2] for g in sigs(series_of(cols), p_of(), day)] == [(dip + 1, 1)]


def test_the_one_hour_trend_needs_two_days_of_hours_and_then_agrees():
    days = ["2026-06-29", "2026-06-30", "2026-07-01"]
    cols = rising_columns(days)
    dip = 2 * 1440 + 810 + 60
    plant_dip(cols, dip, "US_CASH")
    ser = series_of(cols)
    assert ser.f("ema50_H1")[1440 + 810 + 60] == 0 < ser.f("ema50_H1")[dip] < ser.f("ema20_H1")[dip]
    assert [g[:2] for g in sigs(ser, p_of(trend_tf="H1"), days[-1])] == [(dip + 1, 1)]
    # the second day: the same dip, but 50 hours have not passed yet
    cols = rising_columns(days[:2])
    plant_dip(cols, 1440 + 810 + 60, "US_CASH")
    assert sigs(series_of(cols), p_of(trend_tf="H1"), days[1]) == []


def test_a_dip_that_stops_short_of_the_zone_is_not_bought():
    """The dip's low stays between a quarter and a half of the usual distance above the average: the half-way
    idea buys the close back out of its zone, the quarter idea never saw a dip."""
    day = "2026-07-01"
    cols = rising_columns(["2026-06-30", day])
    dip = 1440 + 810 + 60
    plant_dip(cols, dip, "US_CASH", low=6, close=7)
    ser = series_of(cols)
    vw, q = zone(ser, dip, "US_CASH", 0.25)
    _, half = zone(ser, dip, "US_CASH", 0.5)
    assert vw + q < ser.l[dip] <= vw + half and ser.c[dip] <= vw + half
    assert sigs(ser, p_of(touch_sd=0.25), day) == []
    assert [g[:2] for g in sigs(ser, p_of(touch_sd=0.5), day)] == [(dip + 1, 1)]


def test_no_dip_no_trade_and_a_close_through_the_average_starts_the_count_again():
    day = "2026-07-01"
    cols = rising_columns(["2026-06-30", day])
    ser = series_of(cols)
    assert all(sigs(ser, p_of(**s), day) == [] for s in SETTINGS)                 # a straight rise never dips
    # the dip closes BELOW the average: the uptrend's count starts again from the next close above it
    opn = 1440 + 810
    dip = opn + 45
    plant_dip(cols, dip, "US_CASH", low=-6, close=-3)
    ser = series_of(cols)
    assert ser.c[dip] < ser.f("vwap_US_CASH")[dip]
    assert sigs(ser, p_of(), day) == []
    plant_dip(cols, dip + 20, "US_CASH")                                           # 20 minutes into the new count
    assert sigs(series_of(cols), p_of(), day) == []
    plant_dip(cols, dip + 40, "US_CASH")                                           # 40 minutes in: bought
    assert [g[:2] for g in sigs(series_of(cols), p_of(), day)] == [(dip + 41, 1)]


def test_at_most_two_a_day_and_the_second_needs_a_fresh_dip_half_an_hour_later():
    day = "2026-07-01"
    opn = 1440 + 810
    cols = rising_columns(["2026-06-30", day])
    for at in (opn + 60, opn + 70, opn + 100, opn + 140, opn + 180):
        plant_dip(cols, at, "US_CASH")
    ser = series_of(cols)
    got = sigs(ser, p_of(), day)
    # opn+70 is 9 minutes after the first signal: not counted; opn+100 is; then the day's two are used up
    assert [g[:2] for g in got] == [(opn + 61, 1), (opn + 101, 1)]
    for s in SETTINGS:
        assert len(sigs(ser, p_of(**s), day)) <= VwapTrend.MAX_A_DAY


def test_the_stop_sits_half_a_swing_beyond_the_average_or_the_dip_whichever_is_farther():
    day = "2026-07-01"
    dip = 1440 + 810 + 60
    for low, close in ((-9, 1), (1, 2)):                                           # through the average; just above it
        cols = rising_columns(["2026-06-30", day])
        plant_dip(cols, dip, "US_CASH", low=low, close=close)
        ser = series_of(cols)
        vw = ser.f("vwap_US_CASH")[dip + 1]
        (i, side, stop), = sigs(ser, p_of(touch_sd=0.5), day)
        ref = min(vw, ser.l[dip])
        assert (ref == ser.l[dip]) == (low < 0)
        assert i == dip + 1 and side == 1 and stop == ceil(ser.c[i] - ref + 0.5 * ser.f("atr5")[i])
        assert ser.c[i] - stop < min(vw, ser.l[dip])                               # below the average and the dip


def test_no_trade_when_the_run_never_closed_more_than_the_zone_from_the_average_before_the_dip():
    """A swing in the session's first ten minutes (so its usual distance is wide), then the closes hug the
    average, a point or two above it and inside the zone, for 75 minutes: the trend is set and every bar is "in
    the zone", but the run has never closed clear of it, so the jump back out is not a trade. A real dip later
    in the same run (after that clear close) is."""
    day = "2026-07-01"
    lo = 1440 + 810
    cols = rising_columns(["2026-06-30", day])
    t, o, h, l, c = cols[:5]
    c0 = c[lo - 1]
    for j, d in enumerate((-15, -30, -45, -60, -75, -60, -45, -30, -15)):           # the first nine minutes
        k = lo + j
        o[k], c[k] = c[k - 1], c0 + d
        h[k], l[k] = max(o[k], c[k]) + 3, min(o[k], c[k]) - 3
    hug, end = lo + 9, lo + 9 + 75
    tp = lambda k: (h[k] + l[k] + c[k]) / 3.0                                      # equal volumes: a plain mean
    total = sum(tp(k) for k in range(lo, hug))
    for k in range(len(c))[hug:]:
        o[k] = c[k - 1]
        c[k] = (int(total // (k - lo)) + 2 if k < end                                # 2 points over the average
                else c[k - 1] + (40 if k == end else 1))                             # then a jump, and the rise
        h[k], l[k] = max(o[k], c[k]) + 3, min(o[k], c[k]) - 3
        total += tp(k)
    ser = series_of(cols)
    vw, sg = ser.f("vwap_US_CASH"), ser.f("sig_US_CASH")
    assert vw[hug - 1] == 0 < vw[hug]                                                # the run starts here
    for k in (0.25, 0.5):
        assert all(0 < ser.c[i] - vw[i] <= k * sg[i] for i in range(hug, end)), k  # inside the zone, every close
        assert ser.l[end] - vw[end] <= k * sg[end] < ser.c[end] - vw[end]           # the jump: from it, out of it
    assert all(ser.f("ema20_M15")[i] > ser.f("ema50_M15")[i] > 0 for i in range(hug, end + 1))
    grid = [p_of(side_min=n, touch_sd=k) for n in (30, 60) for k in (0.25, 0.5)]
    for p in grid:
        assert sigs(ser, p, day) == [], p
    plant_dip(cols, end + 40, "US_CASH")
    ser = series_of(cols)
    for p in grid:
        assert [g[:2] for g in sigs(ser, p, day)] == [(end + 41, 1)], p
    assert [(i, -s) for i, s, _ in sigs(ser, p_of(), day)] == \
        [(i, s) for i, s, _ in sigs(series_of(mirror(cols)), p_of(), day)]           # and upside down


def test_a_deeper_low_before_the_close_back_out_moves_the_stop():
    """The dip's first bar stays above the average; the next one's low goes 8 points through it (its close back
    above it, in the zone): the stop is measured from that lower low. A low on the close-out bar itself counts
    too. Upside down, a higher high moves a sell's stop the same way."""
    day = "2026-07-01"
    dip = 1440 + 810 + 60
    p = p_of()
    cols = rising_columns(["2026-06-30", day])
    plant_bars(cols, dip, "US_CASH", [(1, 2)])
    ser = series_of(cols)
    (i, side, shallow), = sigs(ser, p, day)
    assert i == dip + 1 and ser.l[dip] > ser.f("vwap_US_CASH")[i]
    assert shallow == ceil(ser.c[i] - ser.f("vwap_US_CASH")[i] + 0.5 * ser.f("atr5")[i])   # from the average
    for bars, deep in (([(1, 2), (-8, 2)], dip + 1), ([(1, 2)], dip + 1)):
        cols = rising_columns(["2026-06-30", day])
        plant_bars(cols, dip, "US_CASH", bars)
        if len(bars) == 1:
            cols[3][dip + 1] = cols[4][dip] - 11                                    # the close-out bar's own low
        for up, s2 in ((1, series_of(cols)), (-1, series_of(mirror(cols)))):
            vw = s2.f("vwap_US_CASH")
            close_out = dip + len(bars)
            assert all(0 < (s2.c[k] - vw[k]) * up for k in range(dip, close_out + 1))  # never closes through
            ext = s2.l[deep] if up > 0 else s2.h[deep]
            assert (ext - vw[close_out]) * up < 0                                    # beyond the average
            (i, side, stop), = sigs(s2, p, day)
            assert i == close_out and side == up
            assert stop == ceil((s2.c[i] - ext) * up + 0.5 * s2.f("atr5")[i]), (bars, up)
            assert stop >= shallow + 8 and (s2.c[i] - up * stop - ext) * up < 0       # past the deeper extreme


def test_a_second_dip_counts_only_from_half_an_hour_after_the_first_signal():
    """After a signal no dip counts until REARM_MIN (30) minutes after the signal's minute: a dip 29 minutes
    after it is not bought, one 30 minutes after it is (the bar after each opens back up, clear of the zone, so
    it is no dip of its own)."""
    assert VwapTrend.REARM_MIN == 30
    day = "2026-07-01"
    opn = 1440 + 810
    first = opn + 61                                                               # the dip at opn + 60's signal
    for p in (p_of(), p_of(touch_sd=0.5)):
        for late, want in ((29, []), (30, [(first + 31, 1)])):
            cols = rising_columns(["2026-06-30", day])
            plant_dip(cols, opn + 60, "US_CASH")
            plant_dip(cols, first + late, "US_CASH", gap=True)
            ser = series_of(cols)
            vw, tol = zone(ser, first + late, "US_CASH", p["touch_sd"])
            assert vw < ser.c[first + late] and ser.l[first + late] - vw <= tol     # a dip into the zone ...
            assert ser.l[first + late + 1] - vw > tol                               # ... and the bar after is not
            assert ser.t[first + late] - ser.t[first] == late
            assert [g[:2] for g in sigs(ser, p, day)] == [(first, 1)] + want, (p, late)


# ------------------------------------------------------------- fresh dips only ---
def plant_wick(cols, at, session, low=-2):
    """Bar ``at``'s low falls back to ``low`` points from the session's average as it stood a bar earlier; its
    open and close are left where the rise was: a dip of one minute's wick, closed back out of the zone."""
    v = ceil(series_of(cols).f(f"vwap_{session}")[at - 1])
    cols[3][at] = v + low
    return cols


def run_start(ser, lo, session):
    """The bar the first run of closes on one side of the session's average starts at (the average is known)."""
    return next(i for i in range(lo, ser.n) if ser.f(f"vwap_{session}")[i] > 0)


def in_zone(ser, i, session, k):
    """(bar i's close inside the zone, bar i's low reaching into it), for an uptrend."""
    vw, tol = zone(ser, i, session, k)
    return ser.c[i] - vw <= tol, ser.l[i] - vw <= tol


@pytest.mark.parametrize("n", [30, 60])
def test_a_dip_under_way_when_the_count_comes_up_is_spent_and_the_next_fresh_dip_is_bought(n):
    """The count of closes above the average comes up at minute ``n`` of the run. A dip that began two minutes
    before that, still in the zone when the count comes up and closing back out a minute after, is not bought:
    it began before the idea could count it. The next fresh dip (its minute before closed outside the zone) is."""
    day = "2026-07-01"
    opn = 1440 + 810
    for k in (0.25, 0.5):
        p = p_of(side_min=n, touch_sd=k)
        cols = rising_columns(["2026-06-30", day])
        r0 = run_start(series_of(cols), opn, "US_CASH")
        up = r0 + n - 1                                                               # the count is up at this bar
        plant_bars(cols, up - 2, "US_CASH", [(-2, 1), (-1, 1), (-2, 1)])              # in the zone: up-2 .. up
        ser = series_of(cols)
        assert run_start(ser, opn, "US_CASH") == r0
        assert ser.t[up] + 1 - ser.t[r0] == n and ser.t[up - 1] + 1 - ser.t[r0] == n - 1
        assert all(ser.c[i] > ser.f("vwap_US_CASH")[i] for i in range(r0, up + 30))     # one run all along
        assert not in_zone(ser, up - 3, "US_CASH", k)[0]                                 # it closed out before ...
        assert all(in_zone(ser, i, "US_CASH", k) == (True, True) for i in range(up - 2, up + 1))   # ... the dip
        assert in_zone(ser, up + 1, "US_CASH", k) == (False, True)                       # then the close back out
        assert sigs(ser, p, day) == [], (n, k)
        plant_dip(cols, up + 15, "US_CASH")                                              # a fresh dip: bought
        ser = series_of(cols)
        assert not in_zone(ser, up + 14, "US_CASH", k)[0] and in_zone(ser, up + 15, "US_CASH", k)[1]
        assert [g[:2] for g in sigs(ser, p, day)] == [(up + 16, 1)], (n, k)
        # the same dip starting on the bar the count comes up: fresh, so bought at its close back out
        cols = rising_columns(["2026-06-30", day])
        plant_bars(cols, up, "US_CASH", [(-2, 1), (-1, 1), (-2, 1)])
        ser = series_of(cols)
        assert [g[:2] for g in sigs(ser, p, day)] == [(up + 3, 1)], (n, k)


@pytest.mark.parametrize("n", [30, 60])
def test_a_one_minute_dip_exactly_when_the_count_comes_up_is_bought_and_a_minute_earlier_is_not(n):
    """A dip of one minute's wick (its low in the zone, its close back out of it) is the dip and its close back out
    at once: bought on the bar the count of closes above the average reaches ``side_min`` minutes, and not on the
    bar a minute before (the count is not up; nothing follows)."""
    day = "2026-07-01"
    opn = 1440 + 810
    for k in (0.25, 0.5):
        p = p_of(side_min=n, touch_sd=k)
        r0 = run_start(series_of(rising_columns(["2026-06-30", day])), opn, "US_CASH")
        for at, want in ((r0 + n - 1, True), (r0 + n - 2, False)):
            cols = plant_wick(rising_columns(["2026-06-30", day]), at, "US_CASH")
            ser = series_of(cols)
            assert ser.t[at] + 1 - ser.t[r0] == (n if want else n - 1)
            assert in_zone(ser, at, "US_CASH", k) == (False, True)                       # a wick into the zone
            assert not in_zone(ser, at - 1, "US_CASH", k)[0]                              # after a close out of it
            got = sigs(ser, p, day)
            assert got == ([(at, 1, expected_stop(ser, at, at, "US_CASH"))] if want else []), (n, k, at)


def test_a_dip_under_way_when_the_half_hour_wait_ends_is_spent():
    """The dip starts 29 minutes after the first signal, a minute before the wait is over, and the bar after it
    (30 minutes after the signal) opens inside the zone and closes back out: not bought, because the dip began
    inside the wait. A fresh dip later is."""
    day = "2026-07-01"
    opn = 1440 + 810
    first = opn + 61
    for p in (p_of(), p_of(touch_sd=0.5)):
        k = p["touch_sd"]
        cols = rising_columns(["2026-06-30", day])
        plant_dip(cols, opn + 60, "US_CASH")
        plant_dip(cols, first + 29, "US_CASH", gap=False)
        ser = series_of(cols)
        assert ser.t[first + 30] - ser.t[first] == VwapTrend.REARM_MIN
        assert in_zone(ser, first + 29, "US_CASH", k) == (True, True)                    # the dip, inside the wait
        assert in_zone(ser, first + 30, "US_CASH", k) == (False, True)                   # its last minute, after it
        assert [g[:2] for g in sigs(ser, p, day)] == [(first, 1)], p
        plant_dip(cols, first + 45, "US_CASH")
        assert [g[:2] for g in sigs(series_of(cols), p, day)] == [(first, 1), (first + 46, 1)], p


def test_a_dip_begun_before_the_slower_trend_comes_back_is_not_fresh():
    """The day of the test below: the 15-minute averages cross back up at ``back``. A dip into the zone two minutes
    before, its close back out on that bar, with the trend on, is not bought: it began before the trend was on.
    The next fresh dip is."""
    day = "2026-07-01"
    opn = 1440 + 810
    days = ["2026-06-30", day]
    lg = [(1440 + 650, 1.0), (opn, -4.0), (opn + 60, 0.5), (None, 4.0)]
    plain = series_of(legs_columns(days, lg))
    ef, es = plain.f("ema20_M15"), plain.f("ema50_M15")
    gone = next(i for i in range(opn, opn + 120) if ef[i] < es[i])
    back = next(i for i in range(gone, opn + 300) if ef[i] > es[i])
    for p in (p_of(), p_of(touch_sd=0.5)):
        k = p["touch_sd"]
        cols = plant_bars(legs_columns(days, lg), back - 2, "US_CASH", [(-2, 1), (-1, 1)])
        ser = series_of(cols)
        f1, s1 = ser.f("ema20_M15"), ser.f("ema50_M15")
        assert all(ser.c[i] > ser.f("vwap_US_CASH")[i] for i in range(opn + 9, opn + 300))   # one run all along
        assert f1[back - 1] < s1[back - 1] and f1[back] > s1[back]                         # the trend comes on
        assert not in_zone(ser, back - 3, "US_CASH", k)[0]
        assert in_zone(ser, back - 2, "US_CASH", k) == in_zone(ser, back - 1, "US_CASH", k) == (True, True)
        assert in_zone(ser, back, "US_CASH", k) == (False, True)                         # out, with the trend on
        assert sigs(ser, p, day) == [], p
        plant_dip(cols, back + 20, "US_CASH")
        assert [g[:2] for g in sigs(series_of(cols), p, day)] == [(back + 21, 1)], p


def test_a_close_through_the_average_after_a_signal_does_not_end_the_half_hour_wait(monkeypatch):
    """After the first signal a minute closes below the average, so the count of closes above it starts again;
    with a 10-minute count (the refresh's lowest), it is up again well before a dip 20 minutes after the signal.
    That dip still waits: the half hour runs from the signal, whatever the closes did. Without the wait, it would
    have been bought."""
    day = "2026-07-01"
    opn = 1440 + 810
    first = opn + 61
    cols = rising_columns(["2026-06-30", day])
    plant_dip(cols, opn + 60, "US_CASH")
    plant_dip(cols, first + 5, "US_CASH", low=-6, close=-3)                                # a close below it
    plant_dip(cols, first + 20, "US_CASH")
    ser = series_of(cols)
    vw = ser.f("vwap_US_CASH")
    assert ser.c[first + 5] < vw[first + 5] and all(ser.c[i] > vw[i] for i in range(first + 6, first + 40))
    for k in (0.25, 0.5):
        p = dict(p_of(touch_sd=k), side_min=10)
        assert ser.t[first + 20] + 1 - ser.t[first + 6] >= 10                           # the count is up again
        assert [g[:2] for g in sigs(ser, p, day)] == [(first, 1)], k
        monkeypatch.setattr(VwapTrend, "REARM_MIN", 0)
        assert [g[:2] for g in sigs(ser, p, day)] == [(first, 1), (first + 21, 1)], k
        monkeypatch.undo()


def test_a_close_exactly_on_the_average_starts_the_count_again():
    """A close exactly ON the average is on neither side: the count of closes above it starts again from the next
    close above (as a close below it does). Here the average at one bar is set to that bar's close (a made-up
    average, to hit it exactly): a dip 20 minutes later is not bought with a 30-minute count, one 40 minutes later
    is. Without it, the 20-minute dip is bought."""
    day = "2026-07-01"
    opn = 1440 + 810
    on = opn + 45
    for late, want in ((20, []), (40, [(on + 41, 1)])):
        cols = rising_columns(["2026-06-30", day])
        plant_dip(cols, on + late, "US_CASH")
        ser = series_of(cols)
        if late == 20:
            assert [g[:2] for g in sigs(ser, p_of(), day)] == [(on + 21, 1)]             # the average left alone
        ser.f("vwap_US_CASH")[on] = float(ser.c[on])
        assert ser.c[on] == ser.f("vwap_US_CASH")[on]
        assert [g[:2] for g in sigs(ser, p_of(), day)] == want, late


def test_no_trade_when_the_trend_has_gone_by_the_close_back_out_and_it_waits_for_the_next_dip():
    """A long rise, then a fall into the US open that leaves the 15-minute averages about to cross, then a slow
    rise in the session: every close stays above the session's average, but the 20-bar 15-minute average drops
    under the 50-bar one at a 15-minute close. A dip just before that, closed back out just after, is not bought.
    Then the price climbs hard, the trend comes back, and nothing is bought until a fresh dip."""
    day = "2026-07-01"
    opn = 1440 + 810
    days = ["2026-06-30", day]
    lg = [(1440 + 650, 1.0), (opn, -4.0), (opn + 60, 0.5), (None, 4.0)]
    ser = series_of(legs_columns(days, lg))
    ef, es = ser.f("ema20_M15"), ser.f("ema50_M15")
    assert ef[opn] > es[opn] > 0
    gone = next(i for i in range(opn, opn + 120) if ef[i] < es[i])                 # the trend goes ...
    back = next(i for i in range(gone, opn + 300) if ef[i] > es[i])               # ... and comes back
    assert gone == opn + 59 and back == opn + 149 and (ser.t[gone] + 1) % 15 == 0
    for p in (p_of(), p_of(touch_sd=0.5)):
        assert sigs(ser, p, day) == []                                             # no dip, no trade
        # the same dip a quarter of an hour earlier, the trend still on at its close back out: bought
        cols = plant_bars(legs_columns(days, lg), gone - 17, "US_CASH", [(-2, 1), (-1, 1)])
        assert [g[:2] for g in sigs(series_of(cols), p, day)] == [(gone - 15, 1)], p
        # the dip just before the trend goes, its close back out just after: not bought
        cols = plant_bars(legs_columns(days, lg), gone - 2, "US_CASH", [(-2, 1), (-1, 1)])
        s2 = series_of(cols)
        vw, tol = zone(s2, gone, "US_CASH", p["touch_sd"])
        assert all(s2.c[k] > s2.f("vwap_US_CASH")[k] for k in range(opn + 9, opn + 300))  # one run all along
        assert s2.c[gone] - vw > tol and s2.l[gone - 2] - zone(s2, gone - 2, "US_CASH", 0)[0] < 0
        assert s2.f("ema20_M15")[gone - 1] > s2.f("ema50_M15")[gone - 1]
        assert s2.f("ema20_M15")[gone] < s2.f("ema50_M15")[gone]
        assert sigs(s2, p, day) == [], p
        # the trend back (every close far out of the zone): still nothing until a fresh dip, which is bought
        plant_dip(cols, back + 20, "US_CASH")
        s2 = series_of(cols)
        assert s2.f("ema20_M15")[back + 21] > s2.f("ema50_M15")[back + 21]
        assert [g[:2] for g in sigs(s2, p, day)] == [(back + 21, 1)], p
        assert [(i, -s) for i, s, _ in sigs(s2, p, day)] == \
            [(i, s) for i, s, _ in sigs(series_of(mirror(cols)), p, day)]           # and upside down


def test_at_most_two_a_session_a_day_however_many_fresh_dips(monkeypatch):
    """Four dips, each well over half an hour after the last signal, on each of two days: two bought a day (the
    count starts again each day). Allowed four, all four are bought: the cap alone stops the third and fourth."""
    days = ["2026-06-30", "2026-07-01"]
    cols = rising_columns(days)
    dips = {}
    for k, d in enumerate(days):
        opn = k * 1440 + 810
        dips[d] = [opn + 75, opn + 115, opn + 155, opn + 195]                     # after the hour-long count
        for at in dips[d]:
            plant_dip(cols, at, "US_CASH")
    ser = series_of(cols)
    assert ser.f("ema50_M15")[810 + 59] > 0                                        # warm on the first day too
    for s in SETTINGS:
        if s["session"] == "US_CASH" and s["trend_tf"] == "M15":
            for d in days:
                assert [g[:2] for g in sigs(ser, p_of(**s), d)] == [(at + 1, 1) for at in dips[d][:2]], (s, d)
    monkeypatch.setattr(VwapTrend, "MAX_A_DAY", 4)
    for d in days:
        assert [g[:2] for g in sigs(ser, p_of(), d)] == [(at + 1, 1) for at in dips[d]], d


# ------------------------------------------------------ on random made-up prices ---
@pytest.fixture(scope="module")
def random_days():
    out = []
    for sym, seed in (("EURUSD", 21), ("US500", 5), ("GER40", 8)):
        ser, ent, days = random_series(6, seed=seed, symbol=sym)
        out.append((ser, ent))
    return out


def test_every_signal_leaves_a_stop_beyond_the_average_and_the_simulator_trades_it(random_days):
    n = 0
    for ser, ent in random_days:
        for s in SETTINGS:
            p = p_of(**s)
            vw, sg = ser.f(f"vwap_{s['session']}"), ser.f(f"sig_{s['session']}")
            ef, es = ser.f(f"ema20_{s['trend_tf']}"), ser.f(f"ema50_{s['trend_tf']}")
            for dv in ser.dayviews():
                lo, hi = dv.span(s["session"])
                got = FAMV.signals(dv, p, (lo, hi), None)
                assert len(got) <= 2 and [g.i for g in got] == sorted({g.i for g in got})
                for g in got:
                    n += 1
                    assert lo <= g.i < hi and g.side in (1, -1)
                    assert isinstance(g.stop_points, int) and g.stop_points >= 1
                    assert (ser.c[g.i] - vw[g.i]) * g.side > s["touch_sd"] * sg[g.i]      # closed out of the zone
                    assert (ef[g.i] - es[g.i]) * g.side > 0                              # with the slower trend
                    assert (ser.c[g.i] - g.side * g.stop_points - vw[g.i]) * g.side < 0  # the stop: past the average
    assert n > 150, n
    # the simulator takes them, every trade with its stop on the losing side of the entry
    store_w = synth.window_of([ser for ser, _ in random_days], synth.weekdays("2026-07-01", 6))
    trades = 0
    for c in FAMV.grid():
        if c.p("side_min") != 30 or c.p("touch_sd") != 0.5:
            continue
        res = sim.run(c, store_w)
        for t in res.trades:
            trades += 1
            assert (t.entry - t.stop) * t.side > 0 and t.r > -4.0
    assert trades > 20, trades


# ----------------------------------------------------------- nothing looks ahead ---
def _cols(ser):
    return [list(getattr(ser, n)) for n in ("t", "o", "h", "l", "c", "s", "v")]


def _all_signals(ser, ent):
    """{(setting index, UK day): [(i, side, stop)]} for every setting the signals read."""
    out = {}
    for k, s in enumerate(SETTINGS):
        p = p_of(**s)
        for dv in ser.dayviews():
            out[(k, dv.uk)] = [(g.i, g.side, g.stop_points) for g in FAMV.signals(dv, p, dv.span(s["session"]), None)]
    return out


@pytest.mark.parametrize("symbol,seed", [("EURUSD", 21), ("US500", 3)])
def test_the_signals_up_to_a_bar_are_the_same_when_every_later_bar_is_changed_or_removed(symbol, seed):
    ser, ent, days = random_series(6, seed=seed, symbol=symbol)
    whole = _all_signals(ser, ent)
    bars = sorted({x[0] for v in whole.values() for x in v})
    assert len(bars) > 25
    cuts = bars[::max(1, len(bars) // 8)][:8]
    cols = _cols(ser)
    other = synth.gen_columns(ent, days, seed=seed + 100)
    for i in cuts:
        cut = series_of([x[:i + 1] for x in cols], ent)                                # every later bar removed
        changed = [x[:i + 1] + [y + (700 if n in (1, 2, 3, 4) else 0) for y in z[i + 1:]]
                   for n, (x, z) in enumerate(zip(cols, other))]
        changed[0] = list(cols[0])                                                    # same clock, other prices
        alt = series_of(changed, ent)
        assert alt.c[i + 1] != ser.c[i + 1] and alt.c[i] == ser.c[i]
        for name, s2 in (("removed", cut), ("changed", alt)):
            got = _all_signals(s2, ent)
            for key, want in whole.items():
                want = [x for x in want if x[0] <= i]
                assert [x for x in got.get(key, []) if x[0] <= i] == want, (name, i, SETTINGS[key[0]], key[1])


def as_the_day_grows(cols, ent, s):
    """The family on a whole series, and on that series fed one bar at a time to a live series as practice sees
    it: (signals on the whole series by UK day, the signals ON each newly finished bar - all that practice takes -
    and the bars at which a growing day's signals so far differ from the whole day's up to that bar)."""
    p = p_of(**s)
    full = series_of(cols, ent)
    whole = {dv.uk: [(g.i, g.side, g.stop_points) for g in FAMV.signals(dv, p, dv.span(s["session"]), None)]
             for dv in full.dayviews()}
    live = F.LiveSeries(ent, sets=FAMV.features)
    on_the_bar, differ = [], []
    for i in range(len(cols[0])):
        live.push(*(x[i] for x in cols))
        dvs = live.dayviews()
        if not dvs or not (dvs[-1].i0 <= i < dvs[-1].i1):
            continue
        dv = dvs[-1]
        lo, hi = dv.span(s["session"])
        if not (lo <= i < hi):
            continue
        assert hi == i + 1                                                             # a live day: the bar is the newest
        got = [(g.i, g.side, g.stop_points) for g in FAMV.signals(dv, p, (lo, hi), None)]
        on_the_bar += [x for x in got if x[0] == i]
        if got != [x for x in whole[dv.uk] if x[0] <= i]:
            differ.append(i)
    return whole, on_the_bar, differ


@pytest.mark.parametrize("symbol,seed", [("EURUSD", 7), ("GER40", 11)])
def test_a_whole_days_signals_are_those_found_bar_by_bar_as_the_day_grows(symbol, seed):
    """For every setting, the past test's signals on whole days are exactly the signals live practice finds on
    each bar as it finishes, none twice, and at every bar a growing day's signals so far are the whole day's up to
    that bar."""
    ent = synth.entry_for(symbol)
    cols = synth.gen_columns(ent, synth.weekdays("2026-07-01", 4), seed=seed)
    n = 0
    for s in SETTINGS:
        whole, on_the_bar, differ = as_the_day_grows(cols, ent, s)
        everything = sorted(x for v in whole.values() for x in v)
        assert sorted(on_the_bar) == everything, s
        assert len(set(on_the_bar)) == len(on_the_bar), s
        assert differ == [], (s, differ[:5])
        n += len(everything)
    assert n > 25, "the check proved nothing: too few signals"


# ------------------------------------------------------------ names and words ---
def test_names_and_words_are_generated_unique_and_plain():
    grid = FAMV.grid()
    names = [C.name_of(c) for c in grid]
    assert len(set(names)) == 64
    everything = C.registered(True)
    assert len({C.name_of(c) for c in everything}) == len(everything)
    for c, nm in zip(grid, names):
        assert nm == FAMV.name(c) and len(nm) < 150, nm
        assert "_" not in nm and not re.search(r"\b(IGNORE|AVOID|RANGE|ATR|t15w|tr1|tr2|BOTH)\b", nm), nm
        words = C.words_of(c)
        text = " ".join([nm] + words)
        assert not JARGON.search(text), (JARGON.search(text).group(0), text)
        assert not re.search(r"\b1(\.0)? swings\b|\b\d+\.0 swing|\b0(\.0)? swings\b", text), text
        assert "  " not in text and "( " not in text and " )" not in text and "the the" not in text
        assert not re.search(r"\b(side_min|touch_sd|trend_tf|US_CASH|LONDON)\b", text), text
        assert words[0] == "Trades " + C.MARKET_NAMES[c.markets] + "." and "£13" in text and "9:55pm UK" in text
        assert f"the last {c.p('side_min')} minutes" in text and f"within {c.p('touch_sd'):g} times" in text
        assert ("1-hour" if c.p("trend_tf") == "H1" else "15-minute") in nm
        assert ("since the US stock market opened" if c.session == "US_CASH"
                else "since the London session opened") in text
        assert any(w.startswith("Stops half a swing beyond the average price") for w in words)
        assert "waits for a close more than that distance from the average" in text and " clear of " not in text
        assert "close back out of the zone, provided the trend is still on" in text
        # the numbers, as the code has them: the cap, the wait, the trend's two averages; and only a fresh dip
        assert (VwapTrend.MAX_A_DAY, VwapTrend.REARM_MIN) == (2, 30)
        assert ("At most 2 signals a session a day (one skipped for its costs still counts); the second needs a "
                "fresh dip at least 30 minutes after the first.") in words
        tf = "1-hour" if c.p("trend_tf") == "H1" else "15-minute"
        assert f"the 20-bar moving average of the {tf} closing prices above the 50-bar one" in text
        assert "then a fresh dip back into the zone" in text and "waits for the next fresh dip" in text
        assert ("A fresh dip is one that starts once the trend is on, straight after a close outside the zone. A dip "
                "already under way when the trend comes on (or when its wait after a signal ends) is let go: it waits "
                "for a close back outside the zone, then a new dip.") in words
        assert "as long as no minute closes on or below it" in text
        assert "color" not in text.lower() and "favor" not in text.lower()
    one = cand_of(CODE, side_min=30, touch_sd=0.25, trend_tf="M15", session="US_CASH", markets="INDEX", exit="t2")
    assert C.name_of(one) == ("Bounce off the average, US session (30 min one side, dip to 0.25x the usual, "
                              "15-minute trend) on the stock indices; aims for 2x its risk")


def test_short_names_are_unique_across_the_whole_registry():
    everything = C.registered(True)
    short = [C.short_name_of(c) for c in everything]
    assert len(set(short)) == len(everything) and max(len(x) for x in short) <= 90
    one = cand_of(CODE, side_min=60, touch_sd=0.5, trend_tf="H1", session="LONDON", markets="FX_MAJOR", exit="tr2")
    assert C.short_name_of(one) == "Bounce off the average · majors · London 60 min, 0.5x dip, 1-hour trend · trail 2R"


def test_off_grid_settings_from_the_refresh_still_trade_and_read_well():
    """The weekly refresh nudges a winner's numeric settings between grid steps (TUNABLE): the signals, names and
    words must take any value in range."""
    from mintel.lab.refresh import variants_of
    base = cand_of(CODE, side_min=60, touch_sd=0.5, trend_tf="M15", session="US_CASH", markets="INDEX", exit="t2")
    kids = variants_of(base, set())
    assert {(k.p("side_min"), k.p("touch_sd")) for k in kids} >= {(45, 0.5), (90, 0.5)}
    ser, ent, days = random_series(5, seed=21)
    for c in kids + [base.replace(params={"side_min": 10, "touch_sd": 1.0}),
                     base.replace(params={"side_min": 120, "touch_sd": 0.1875})]:
        nm, words = C.name_of(c), C.words_of(c)
        assert len(nm) < 150 and len(C.short_name_of(c)) <= 90 and not JARGON.search(" ".join([nm] + words))
        p = sim.family_params(c)
        for dv in ser.dayviews():
            for g in FAMV.signals(dv, p, dv.span(c.session), None):
                assert g.stop_points >= 1
    assert "within that usual distance of the average" in " ".join(C.words_of(base.replace(params={"touch_sd": 1.0})))


def test_the_dashboard_gives_it_a_nickname_and_plain_words():
    from mintel.ops import squad as SQ
    t = {"kind": "lab", "family": CODE, "markets": "INDEX", "session": "US_CASH",
         "params": {"side_min": 30, "touch_sd": 0.25, "trend_tf": "M15"}, "exit": "t2"}
    assert SQ.nickname_base(t) == "Wall St Index Cruiser 30"
    assert SQ.nickname_base(dict(t, session="LONDON", markets="FX_MAJOR", params={"side_min": 60})) == \
        "London Major Cruiser 60"
    text = SQ.what_it_does(t)
    assert text.startswith("Finds a market that has stayed on one side of the session's average price for 30 "
                           "minutes")
    assert "a dip back to near the session's average price" in text and "provided the trend is still on" in text
    assert "day's average" not in text and "the average" not in text.replace("the session's average price", "")
    assert "{" not in text and "the stock indices" in text and "aims for 2x its risk" in text
