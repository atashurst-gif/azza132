"""The nine families, their grids, their names, their signals, and the features
they read.

Pinned here: the registered grid sizes (144/108/108/48/108/48/36/32/32), ids
that are stable across runs and machines, generated plain-word names, the
sibling candidates a plateau check needs, the control family, the dormant news
family, that nothing looks ahead (signals and features at bar i are the same
whether or not the bars after i exist), and that streaming equals batch.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import sys
from array import array
from pathlib import Path

import pytest

from mintel.broker.base import CalendarEvent
from mintel.lab import (Candidate, LabPaths, Signal, candidate_id, day_min, features as F, make_params,
                        sim, synth)
from mintel.lab import candidate as C
from mintel.lab import families as FAM
from mintel.lab.store import BarStore, SymbolSeries
from mintel.news.calendar_store import CalendarStore

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 9, 28, 12, 0, tzinfo=UTC)
SIZES = {"OPENING_RANGE": 144, "RANGE_BREAK": 108, "MOMENTUM_CONTINUATION": 108, "TREND_PULLBACK": 48,
         "VWAP_FADE": 108, "PREV_DAY_LEVELS": 48, "SQUEEZE_RELEASE": 36, "NEWS_MOVE": 32,
         "CONTROL_TIME_OF_DAY": 32}


# ------------------------------------------------------------------- the grids ---
def test_grid_sizes_are_exactly_the_registered_ones():
    assert {code: len(f.grid()) for code, f in FAM.FAMILIES.items()} == SIZES
    assert FAM.GRID_SIZES == SIZES
    assert list(FAM.FAMILIES) == list(SIZES)
    assert len(C.registered(False)) == 632 and len(C.registered(True)) == 664


def test_the_dormant_news_family_wakes_only_when_asked():
    assert FAM.get("NEWS_MOVE").dormant
    asleep = {c.family for c in C.registered(False)}
    awake = {c.family for c in C.registered(True)}
    assert "NEWS_MOVE" not in asleep and "NEWS_MOVE" in awake and awake - asleep == {"NEWS_MOVE"}
    assert all(not f.dormant for f in FAM.all_families(False) if f.code != "NEWS_MOVE")


def test_each_axis_is_as_registered():
    ax = {code: f.AXES for code, f in FAM.FAMILIES.items()}
    assert ax["OPENING_RANGE"] == {"session": ["LONDON", "NEWYORK"], "range_min": [15, 30],
                                   "buffer_atr": [0.1], "stop": ["RANGE", "ATR"],
                                   "exit": ["t1", "t2", "tr1"], "markets": ["FX_MAJOR", "FX_MINOR", "INDEX"],
                                   "news": ["IGNORE", "AVOID_15M"]}
    assert ax["RANGE_BREAK"]["lookback_min"] == [60, 120, 240] and ax["RANGE_BREAK"]["exit"] == ["t2", "tr1", "tr2"]
    assert ax["MOMENTUM_CONTINUATION"]["impulse_atr"] == [1.5, 2.0, 2.5]
    assert ax["MOMENTUM_CONTINUATION"]["pullback"] == ["NONE", "HALF"] and ax["MOMENTUM_CONTINUATION"]["session"] == ["BOTH"]
    assert ax["TREND_PULLBACK"]["tf"] == ["M15", "H1"] and ax["TREND_PULLBACK"]["emas"] == ["20/50", "50/200"]
    assert ax["TREND_PULLBACK"]["touch_atr"] == [0.25, 0.5] and ax["TREND_PULLBACK"]["news"] == ["IGNORE"]
    assert ax["VWAP_FADE"]["sigma"] == [2.0, 2.5, 3.0] and ax["VWAP_FADE"]["session"] == ["ASIA", "LONDON", "US_CASH"]
    assert ax["VWAP_FADE"]["exit"] == ["vwap"] and ax["VWAP_FADE"]["markets"] == ["FX_MAJOR", "INDEX", "GOLD"]
    assert ax["PREV_DAY_LEVELS"]["mode"] == ["BREAK", "FADE"] and ax["PREV_DAY_LEVELS"]["confirm"] == ["M5_CLOSE", "M15_CLOSE"]
    assert ax["SQUEEZE_RELEASE"]["width_pct"] == [10, 20] and ax["SQUEEZE_RELEASE"]["buffer_atr"] == [0.0, 0.25]
    assert ax["NEWS_MOVE"]["direction"] == ["WITH", "AGAINST"] and ax["NEWS_MOVE"]["delay_min"] == [5, 15]
    assert ax["NEWS_MOVE"]["markets"] == ["FX_MAJOR", "FX_MINOR"]
    assert ax["CONTROL_TIME_OF_DAY"]["hour_uk"] == [8, 10, 13, 15] and ax["CONTROL_TIME_OF_DAY"]["side"] == ["BUY", "SELL"]
    assert ax["CONTROL_TIME_OF_DAY"]["markets"] == ["FX_MAJOR", "FX_MINOR"]


def test_the_control_family_is_the_clock_trade():
    f = FAM.get("CONTROL_TIME_OF_DAY")
    assert f.label == "Clock trade (control)" and getattr(f, "is_control", False) and FAM.is_control(f.code)
    grid = f.grid()
    assert {c.exit for c in grid} == {"time1h", "time4h"} and {c.p("hour_uk") for c in grid} == {8, 10, 13, 15}
    assert {c.p("side") for c in grid} == {"BUY", "SELL"} and {c.markets for c in grid} == {"FX_MAJOR", "FX_MINOR"}
    assert f.sensitive == ()
    assert "EXPECTED to fail" in " ".join(C.words_of(grid[0]))


def test_aarons_names_for_the_families():
    labels = {code: f.label for code, f in FAM.FAMILIES.items()}
    assert labels == {"OPENING_RANGE": "Opening range", "RANGE_BREAK": "Breakout",
                      "MOMENTUM_CONTINUATION": "Momentum burst", "TREND_PULLBACK": "Trend pullback",
                      "VWAP_FADE": "Back to the average", "PREV_DAY_LEVELS": "Yesterday's high and low",
                      "SQUEEZE_RELEASE": "Squeeze release", "NEWS_MOVE": "After the news",
                      "CONTROL_TIME_OF_DAY": "Clock trade (control)"}


# ------------------------------------------------------------------------ ids ---
def test_ids_are_the_hash_of_the_definition_alone():
    c = Candidate("OPENING_RANGE", 1, "FX_MAJOR", "LONDON", "IGNORE", "t1", make_params(range_min=15, buffer_atr=0.1))
    blob = json.dumps({"family": "OPENING_RANGE", "version": 1, "markets": "FX_MAJOR", "session": "LONDON",
                       "news": "IGNORE", "exit": "t1", "params": [["buffer_atr", 0.1], ["range_min", 15]],
                       "risk_gbp": 13.0}, sort_keys=True, separators=(",", ":"))
    assert c.id == "lab-" + hashlib.sha1(blob.encode()).hexdigest()[:10]
    assert candidate_id(c) == c.id and re.fullmatch(r"lab-[0-9a-f]{10}", c.id)
    # parameter order does not matter once sorted; any change changes the id
    assert c.id == Candidate("OPENING_RANGE", 1, "FX_MAJOR", "LONDON", "IGNORE", "t1",
                             make_params(buffer_atr=0.1, range_min=15)).id
    assert c.replace(exit="t2").id != c.id and c.replace(params={"range_min": 30}).id != c.id
    assert c.replace(version=2).id != c.id and c.replace(risk_gbp=14.0).id != c.id
    assert set(c.definition()) == {"family", "version", "markets", "session", "news", "exit", "params", "risk_gbp"}


def test_all_registered_ids_are_distinct_and_pinned():
    ids = [c.id for c in C.registered(True)]
    assert len(set(ids)) == 664
    digest = hashlib.sha1("\n".join(sorted(ids)).encode()).hexdigest()
    # if this changes, a definition changed: bump the family's version on purpose, never by accident
    assert digest == PINNED_GRID_DIGEST, digest


PINNED_GRID_DIGEST = "21ba9a386edb1b961834c1f692a039ab8b300ff1"


def test_ids_are_identical_in_another_process_with_another_hash_seed():
    code = ("from mintel.lab.candidate import registered; "
            "print(','.join(c.id for c in registered(True)))")
    root = Path(__file__).resolve().parents[1]
    outs = []
    for seed in ("0", "12345"):
        env = dict(os.environ, PYTHONHASHSEED=seed, PYTHONPATH=str(root))
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env,
                             cwd=str(root), check=True).stdout.strip()
        outs.append(out)
    assert outs[0] == outs[1] == ",".join(c.id for c in C.registered(True))


def test_a_float_that_is_really_a_whole_number_would_change_the_id():
    a = Candidate("X", 1, "FX_MAJOR", "ALL", "IGNORE", "t1", make_params(k=0))
    b = Candidate("X", 1, "FX_MAJOR", "ALL", "IGNORE", "t1", make_params(k=0.0))
    assert a.id != b.id          # so grids spell their numbers once and consistently
    for fam in FAM.FAMILIES.values():
        for param, vals in fam.AXES.items():
            kinds = {type(v) for v in vals}
            assert len(kinds) == 1, (fam.code, param)


# --------------------------------------------------------------------- names ---
def test_names_are_generated_unique_and_in_plain_words():
    cs = C.registered(True)
    names = [C.name_of(c) for c in cs]
    assert len(set(names)) == len(cs)
    for c, n in zip(cs, names):
        assert n == FAM.get(c.family).name(c)
        assert "_" not in n and not re.search(r"\b(IGNORE|AVOID|RANGE|ATR|t15w|tr1|tr2|BOTH)\b", n), n
        assert len(n) < 150, n
    assert any("London opening range (30 min)" in n and "the major pairs" in n for n in names)
    assert any("avoiding news (15 min)" in n for n in names) and any("ignoring news" in n for n in names)


def test_words_say_what_the_candidate_does_in_plain_english():
    for c in C.registered(True)[::7]:
        w = C.words_of(c)
        text = " ".join(w)
        assert len(w) >= 5 and w[0].startswith("Trades ")
        assert "£13" in text and "9:55pm UK" in text and "overnight" in text
        assert not re.search(r"\b(sigma_atr|stop_atr|range_min|lookback_min|impulse_atr)\b", text)
        assert "color" not in text.lower() and "favor" not in text.lower()          # UK English
    asked = C.words_of(FAM.get("PREV_DAY_LEVELS").grid()[0])
    assert any("fresh test alone" in x for x in asked)                              # no number to tune: says so


JARGON = re.compile(r"\b(ATR|VWAP|sigma|Bollinger|EMA|M1|M5|M15|H1|PBO|DSR|bootstrap|tf)\b", re.I)


def test_nothing_aaron_reads_about_a_candidate_is_jargon_or_ungrammatical():
    """'ATR' was in 468 candidates' words, 'VWAP' and 'sigma' in 108 each, 'Bollinger' in 36 and
    'M15'/'H1' in the names of 24 each, and a one-swing stop read '1.0 swings'."""
    for c in C.registered(True):
        text = " ".join([C.name_of(c)] + C.words_of(c))
        assert not JARGON.search(text), (c.id, JARGON.search(text).group(0), text)
        assert not re.search(r"\b1(\.0)? swings\b|\b\d+\.0 swing|\b0(\.0)? swings\b", text), (c.id, text)
        assert "  " not in text and "( " not in text and " )" not in text
    one = cand_of("VWAP_FADE", stop_atr=1.0, sigma=2.5, session="LONDON")
    assert "stop 1 swing)" in C.name_of(one) and "strays 2.5x the usual" in C.name_of(one)
    assert any("Stops 1 swing away" in w for w in C.words_of(one))
    assert any("more than 2.5 times that usual distance" in w for w in C.words_of(one))
    assert C.swings(1) == "1 swing" and C.swings(1.0) == "1 swing" and C.swings(1.5) == "1.5 swings" \
        and C.swings(2.0) == "2 swings" and C.swings(0.25) == "0.25 swings"
    h1 = cand_of("TREND_PULLBACK", tf="H1", emas="50/200", touch_atr=0.25)
    assert "1-hour bars (50 and 200 bar averages)" in C.name_of(h1)
    sq = cand_of("SQUEEZE_RELEASE", width_pct=10, buffer_atr=0.0)
    assert "no buffer" in C.name_of(sq) and not any("minus" in w or "plus" in w for w in C.words_of(sq))


def test_family_and_name_helpers_round_trip():
    for fam in FAM.FAMILIES.values():
        c = fam.grid()[0]
        assert C.family_of(c) is fam and C.name_of(c) == fam.name(c)


# ------------------------------------------------------------------ plateau ---
def test_siblings_exist_in_the_grid_and_differ_only_in_that_setting():
    for fam in FAM.FAMILIES.values():
        ids = {c.id for c in fam.grid()}
        for c in fam.grid():
            for param in fam.sensitive:
                sibs = fam.siblings(c, param)
                assert [s.p(param) for s in sibs] == fam.axis_values(param)
                assert c.id in {s.id for s in sibs} and {s.id for s in sibs} <= ids
                for s in sibs:
                    assert {k: v for k, v in s.params if k != param} == {k: v for k, v in c.params if k != param}
                    assert (s.markets, s.session, s.news, s.exit) == (c.markets, c.session, c.news, c.exit)


def test_the_sensitive_settings_are_the_numeric_ones():
    assert FAM.get("OPENING_RANGE").sensitive == ("range_min",)
    assert FAM.get("RANGE_BREAK").sensitive == ("lookback_min",)
    assert FAM.get("MOMENTUM_CONTINUATION").sensitive == ("impulse_atr",)
    assert FAM.get("VWAP_FADE").sensitive == ("sigma", "stop_atr")
    assert FAM.get("SQUEEZE_RELEASE").sensitive == ("width_pct", "buffer_atr")
    assert FAM.get("PREV_DAY_LEVELS").sensitive == () and FAM.get("CONTROL_TIME_OF_DAY").sensitive == ()
    for fam in FAM.FAMILIES.values():
        for p in fam.sensitive:
            assert all(isinstance(v, (int, float)) for v in fam.axis_values(p)), (fam.code, p)


# ----------------------------------------------------------- hand-made days ---
EUR = synth.entry_for("EURUSD")


def wiggle_columns(day, base=110000, amp=10):
    """A busy-but-going-nowhere day: price alternates, so ATR is well above zero."""
    t, o, h, l, c, s, v = synth.flat_columns(day, base, 4)
    prev = base
    for k in range(1440):
        cl = base + (amp if k % 2 else 0)
        o[k], c[k] = prev, cl
        h[k], l[k] = max(prev, cl) + 3, min(prev, cl) - 3
        prev = cl
    return [t, o, h, l, c, s, v]


def set_bar(cols, minute_of_day, o=None, h=None, l=None, c=None):
    k = minute_of_day
    for arr, val in zip(cols[1:5], (o, h, l, c)):
        if val is not None:
            arr[k] = val


def series_of(cols, entry=EUR):
    return synth.series_from_columns(entry, cols)


def signals_for(ser, cand, day_index=0):
    fam = FAM.get(cand.family)
    dv = ser.dayviews()[day_index]
    news = ser.f("news_min") if cand.family == "NEWS_MOVE" else None
    return fam.signals(dv, sim.family_params(cand), dv.span(cand.session), news)


def cand_of(code, **kw):
    """The first grid candidate of a family matching the given settings."""
    for c in FAM.get(code).grid():
        d = dict(c.params)
        d.update(markets=c.markets, session=c.session, news=c.news, exit=c.exit)
        if all(d.get(k) == v for k, v in kw.items()):
            return c
    raise KeyError(kw)


DAY = "2026-07-01"          # BST: London opens at 07:00 UTC, minute 420


def test_opening_range_takes_the_first_break_of_the_first_15_minutes_with_the_far_side_stop():
    cols = wiggle_columns(DAY)
    # break upwards at 07:40 UTC (minute 460); a second, later break must not fire
    set_bar(cols, 460, o=110010, h=110105, l=110005, c=110100)
    set_bar(cols, 480, o=110010, h=110125, l=110005, c=110120)
    ser = series_of(cols)
    c = cand_of("OPENING_RANGE", session="LONDON", range_min=15, stop="RANGE", markets="FX_MAJOR", exit="t1", news="IGNORE")
    # the finished range is on the feature arrays from 07:15 on, zero before
    assert ser.f("or_L15_hi")[434] == 0 and ser.f("or_L15_hi")[435] > 0
    rng_hi = max(cols[2][420:435])
    rng_lo = min(cols[3][420:435])
    assert ser.f("or_L15_hi")[435] == rng_hi and ser.f("or_L15_lo")[435] == rng_lo
    sigs = signals_for(ser, c)
    assert len(sigs) == 1
    s = sigs[0]
    assert (s.i, s.side) == (460, 1) and s.stop_points == 110100 - rng_lo
    # ATR stop: no stop_points, the exit's own rule applies
    c2 = cand_of("OPENING_RANGE", session="LONDON", range_min=15, stop="ATR", markets="FX_MAJOR", exit="t1", news="IGNORE")
    assert signals_for(ser, c2)[0].stop_points is None
    # a downward break
    cols = wiggle_columns(DAY)
    set_bar(cols, 450, o=110000, h=110002, l=109880, c=109890)
    s2 = signals_for(series_of(cols), c)[0]
    assert (s2.i, s2.side) == (450, -1) and s2.stop_points == max(cols[2][420:435]) - 109890


def test_opening_range_new_york_is_the_us_cash_open_not_a_fixed_uk_clock_time():
    cols = wiggle_columns(DAY)
    ny_open = 13 * 60 + 30                         # 09:30 New York (EDT) = 13:30 UTC
    set_bar(cols, ny_open + 40, o=110010, h=110120, l=110005, c=110110)
    ser = series_of(cols)
    assert ser.f("or_N30_hi")[ny_open + 29] == 0 and ser.f("or_N30_hi")[ny_open + 30] > 0
    c = cand_of("OPENING_RANGE", session="NEWYORK", range_min=30, stop="ATR", markets="FX_MAJOR", exit="t1", news="IGNORE")
    assert [(s.i, s.side) for s in signals_for(ser, c)] == [(ny_open + 40, 1)]
    # in winter the same open is an hour later in UTC
    winter = "2026-01-14"
    cols = wiggle_columns(winter)
    assert series_of(cols).f("or_N15_hi")[14 * 60 + 30 + 14] == 0 and series_of(cols).f("or_N15_hi")[14 * 60 + 30 + 15] > 0


def test_range_break_fires_on_the_first_close_beyond_the_last_n_minutes_only():
    cols = wiggle_columns(DAY)
    set_bar(cols, 500, o=110010, h=110090, l=110005, c=110080)       # beyond the 60-minute high
    set_bar(cols, 501, o=110080, h=110085, l=110070, c=110082)       # still above, but no longer a fresh break
    set_bar(cols, 520, o=110010, h=110015, l=109900, c=109910)       # and a break downwards
    ser = series_of(cols)
    c = cand_of("RANGE_BREAK", lookback_min=60, session="LONDON", exit="t2", markets="FX_MAJOR", news="IGNORE")
    got = [(s.i, s.side) for s in signals_for(ser, c)]
    assert (500, 1) in got and (501, 1) not in got and (520, -1) in got
    # the high is of the PREVIOUS 60 minutes: the bar being judged is not in it
    assert ser.f("hh60")[500] == max(cols[2][440:500])
    # it only looks inside its session (London 07:00-15:30 UTC)
    cols = wiggle_columns(DAY)
    set_bar(cols, 1000, o=110010, h=110200, l=110005, c=110190)
    assert signals_for(series_of(cols), c) == []


def test_control_fires_at_the_uk_hour_in_bst_and_gmt():
    c = cand_of("CONTROL_TIME_OF_DAY", hour_uk=8, side="BUY", exit="time1h", markets="FX_MAJOR")
    assert signals_for(series_of(wiggle_columns("2026-07-01")), c)[0].i == 7 * 60        # 08:00 BST
    assert signals_for(series_of(wiggle_columns("2026-01-14")), c)[0].i == 8 * 60        # 08:00 GMT
    c2 = cand_of("CONTROL_TIME_OF_DAY", hour_uk=15, side="SELL", exit="time4h", markets="FX_MAJOR")
    s = signals_for(series_of(wiggle_columns("2026-07-01")), c2)[0]
    assert (s.i, s.side) == (14 * 60, -1)


def test_momentum_burst_needs_a_steady_push_then_a_break_of_its_extreme():
    def day_with_push(dip=0, break_at=634):
        cols = wiggle_columns(DAY)
        px = 110000
        for k in range(600, 630):                      # six 5-minute bars, +5 a minute: +150 points
            px += 5
            set_bar(cols, k, o=px - 5, h=px + 1, l=px - 6, c=px)
        top = px
        for k in range(630, break_at):                 # a pause (or a dip) under the push's high
            level = top - dip
            set_bar(cols, k, o=level, h=min(level + 2, top), l=level - 2, c=level)
        set_bar(cols, break_at, o=top - dip, h=top + 12, l=top - dip, c=top + 10)
        return series_of(cols), top
    none_c = cand_of("MOMENTUM_CONTINUATION", impulse_atr=1.5, pullback="NONE", exit="t15w", markets="FX_MAJOR", news="IGNORE")
    half_c = cand_of("MOMENTUM_CONTINUATION", impulse_atr=1.5, pullback="HALF", exit="t15w", markets="FX_MAJOR", news="IGNORE")
    ser, top = day_with_push(dip=0)
    got = [(s.i, s.side) for s in signals_for(ser, none_c)]
    assert (634, 1) in got
    assert signals_for(ser, half_c) == []                                  # no pullback happened: the half-pullback idea waits in vain
    ser, top = day_with_push(dip=50)                                       # a dip of a third of the push: shallow
    assert (634, 1) in [(s.i, s.side) for s in signals_for(ser, half_c)]
    ser, top = day_with_push(dip=90)                                       # more than half: the push has failed
    assert signals_for(ser, half_c) == []
    # a push too small for 2.5 swings is ignored
    big = cand_of("MOMENTUM_CONTINUATION", impulse_atr=2.5, pullback="NONE", exit="t15w", markets="FX_MAJOR", news="IGNORE")
    cols = wiggle_columns(DAY)
    px = 110000
    for k in range(600, 630):
        px += 1
        set_bar(cols, k, o=px - 1, h=px + 1, l=px - 2, c=px)
    set_bar(cols, 634, o=110030, h=110050, l=110030, c=110045)
    assert signals_for(series_of(cols), big) == []


# ------------------------- momentum burst: the past test sees what practice sees (14 Oct) ---
MOMENTUM_SETTINGS = [dict(impulse_atr=a, pullback=pb) for a in (1.5, 2.0, 2.5) for pb in ("NONE", "HALF")]


def momentum_as_the_day_grows(cols, entry, p, session="BOTH"):
    """The momentum family on a whole series and on that series growing a bar at a time, as live practice
    sees it: (signals on the full series by UK day, the signals ON each newly finished bar - all that practice
    takes - and the bars at which a growing day's signals so far were not the full day's up to that bar)."""
    fam = FAM.get("MOMENTUM_CONTINUATION")
    full = synth.series_from_columns(entry, [list(x) for x in cols])
    whole = {dv.uk: [(s.i, s.side) for s in fam.signals(dv, p, dv.span(session), None)] for dv in full.dayviews()}
    live = F.LiveSeries(entry)
    on_the_bar, differ = [], []
    for i in range(len(cols[0])):
        live.push(*(x[i] for x in cols))
        dvs = live.dayviews()
        if not dvs or not (dvs[-1].i0 <= i < dvs[-1].i1):
            continue
        dv = dvs[-1]
        lo, hi = dv.span(session)
        if not (lo <= i < hi):
            continue
        got = [(s.i, s.side) for s in fam.signals(dv, p, (lo, hi), None)]
        on_the_bar += [x for x in got if x[0] == i]
        if got != [x for x in whole[dv.uk] if x[0] <= i]:
            differ.append(i)
    return whole, on_the_bar, differ


def test_momentum_burst_takes_a_later_push_that_breaks_first_just_as_live_practice_does():
    """Push A (10:00-10:30 UTC) has a spike high on its first minute, and so does every push whose six bars
    hold that minute; push B, the same six bars five minutes later, does not, so its extreme is lower. B breaks
    at 10:40, the spike at 10:50. Live practice sees B's break at 10:40 and takes it (A has not broken yet). The
    past test read push by push: A first, it broke at 10:50, so B was skipped - on bars practice could not yet
    see. Now both take 10:40 (spending A), and 10:50 (a push that ended after 10:40). MADE-UP prices."""
    cols = wiggle_columns(DAY)
    px = 110000
    for k in range(600, 630):                                   # six 5-minute bars, +5 a minute: +150 points
        px += 5
        set_bar(cols, k, o=px - 5, h=px + 1, l=px - 6, c=px)
    set_bar(cols, 600, h=110200)                                # the spike: push A's extreme, not push B's
    for k in range(630, 640):
        set_bar(cols, k, o=px, h=px + 2, l=px - 2, c=px)
    set_bar(cols, 640, o=px, h=px + 12, l=px, c=px + 10)        # above B's extreme, below A's spike
    for k in range(641, 650):
        set_bar(cols, k, o=px + 5, h=px + 7, l=px + 3, c=px + 5)
    set_bar(cols, 650, o=px + 5, h=px + 62, l=px + 5, c=px + 60)  # above A's spike
    for k in range(651, 700):
        set_bar(cols, k, o=px + 60, h=px + 62, l=px + 58, c=px + 60)
    for p in MOMENTUM_SETTINGS:
        whole, on_the_bar, differ = momentum_as_the_day_grows(cols, EUR, p)
        want = [(640, 1), (650, 1)] if p["pullback"] == "NONE" else [(650, 1)]   # (B never pulled back)
        assert whole[DAY] == want, p                            # was: [(650, 1)] for the first-break ideas
        assert on_the_bar == want and differ == [], p


@pytest.mark.parametrize("symbol,seed", [("EURUSD", 7), ("EURJPY", 21)])
def test_momentum_burst_signals_on_a_whole_day_are_those_found_bar_by_bar_as_the_day_grows(symbol, seed):
    """For every setting, the past test's signals on whole days are exactly the union of the signals live
    practice finds on each bar as it finishes, and at every bar a growing day's signals so far are the whole
    day's up to that bar (each signal depends only on the bars up to it). MADE-UP prices; these two symbol and
    seed pairs hold later pushes breaking before earlier ones (the old reading failed both)."""
    ent = synth.entry_for(symbol)
    cols = synth.gen_columns(ent, synth.weekdays("2026-07-01", 5), seed=seed)
    n = 0
    for p in MOMENTUM_SETTINGS:
        whole, on_the_bar, differ = momentum_as_the_day_grows(cols, ent, p)
        everything = sorted(x for sigs in whole.values() for x in sigs)
        assert sorted(on_the_bar) == everything, p
        assert len(set(on_the_bar)) == len(on_the_bar), p       # no bar signalled twice
        assert differ == [], (p, differ[:5])
        n += len(everything)
    assert n > 100, "the check proved nothing: too few signals"


def quiet_hour(cols, k0, k1, base=110000):
    """Perfectly level bars: the session's price has no spread at all yet."""
    for k in range(k0, k1):
        set_bar(cols, k, o=base, h=base, l=base, c=base)


def test_vwap_fade_sells_a_stretch_above_the_session_average_and_aims_back_for_it():
    cols = wiggle_columns(DAY)
    # busy until London opens (07:00 UTC) so the average true range is warm, then a very quiet
    # hour around 110000, then a sudden jump far above the session's average
    quiet_hour(cols, 420, 480)
    for k in range(480, 495):
        set_bar(cols, k, o=110200, h=110205, l=110195, c=110200)
    ser = series_of(cols)
    c = cand_of("VWAP_FADE", sigma=2.0, session="LONDON", stop_atr=1.0, markets="FX_MAJOR", news="IGNORE")
    sigs = signals_for(ser, c)
    assert sigs and sigs[0].side == -1 and sigs[0].i == 480
    assert sigs[0].stop_points >= 1
    vw, sg = ser.f("vwap_LONDON")[480], ser.f("sig_LONDON")[480]
    assert ser.c[480] - vw >= 2.0 * sg > 0
    assert c.exit == "vwap"
    # no signal in the first half hour of the session, whatever the price does
    assert all(s.i >= 420 + 30 for s in sigs)
    # a jump DOWN is bought
    cols = wiggle_columns(DAY)
    quiet_hour(cols, 420, 480)
    set_bar(cols, 480, o=110000, h=110001, l=109795, c=109800)
    assert signals_for(series_of(cols), c)[0].side == 1


def test_previous_day_levels_break_and_rejection():
    d1, d2 = "2026-06-30", "2026-07-01"
    cols1, cols2 = wiggle_columns(d1), wiggle_columns(d2)
    set_bar(cols1, 800, h=110200)                                          # yesterday's high 110200
    set_bar(cols1, 801, l=109800)                                          # and low 109800
    for k in range(7 * 60 + 30, 7 * 60 + 40):                              # today: a 5-minute bar closing above it
        set_bar(cols2, k, o=110190, h=110195, l=110185, c=110190)
    set_bar(cols2, 7 * 60 + 40, o=110190, h=110250, l=110188, c=110240)
    set_bar(cols2, 7 * 60 + 44, o=110240, h=110255, l=110230, c=110245)
    joined = [a + b for a, b in zip(cols1, cols2)]
    ser = series_of(joined)
    yesterday = 23 * 60                                                    # 23:00 UTC is already the next UK day in BST
    assert ser.f("pdh")[1440 + 100] == max(cols1[2][:yesterday]) == 110200
    assert ser.f("pdl")[1440 + 100] == min(cols1[3][:yesterday]) <= 109800
    brk = cand_of("PREV_DAY_LEVELS", mode="BREAK", confirm="M5_CLOSE", session="LONDON", exit="t2", markets="FX_MAJOR")
    got = signals_for(ser, brk, day_index=1)
    assert got and got[0].side == 1
    # a rejection: poke above yesterday's high and close back under it
    cols2 = wiggle_columns(d2)
    set_bar(cols2, 7 * 60 + 60, o=110190, h=110230, l=110185, c=110195)
    set_bar(cols2, 7 * 60 + 61, o=110195, h=110196, l=110190, c=110194)
    set_bar(cols2, 7 * 60 + 64, o=110194, h=110198, l=110190, c=110193)
    ser = series_of([a + b for a, b in zip(cols1, cols2)])
    fade = cand_of("PREV_DAY_LEVELS", mode="FADE", confirm="M5_CLOSE", session="LONDON", exit="t2", markets="FX_MAJOR")
    got = signals_for(ser, fade, day_index=1)
    assert any(s.side == -1 for s in got)


def test_news_move_trades_with_or_against_the_first_five_minutes():
    cols = wiggle_columns(DAY)
    ev = 10 * 60
    for k in range(5):                                                     # the spike: +120 in five minutes
        set_bar(cols, ev + k, o=110000 + 24 * k, h=110000 + 24 * k + 26, l=110000 + 24 * k - 2, c=110000 + 24 * (k + 1))
    ser = series_of(cols)
    nm = array("i", [20] * ser.n)
    nm[ev - 1] = 1
    nm[ev] = 0
    ser._feat["news_min"] = nm
    with_c = cand_of("NEWS_MOVE", direction="WITH", delay_min=5, stop="ATR", exit="t15w", markets="FX_MAJOR")
    against = cand_of("NEWS_MOVE", direction="AGAINST", delay_min=5, stop="SPIKE", exit="t15w", markets="FX_MAJOR")
    w = signals_for(ser, with_c)
    a = signals_for(ser, against)
    assert [(s.i, s.side) for s in w] == [(ev + 4, 1)] and w[0].stop_points is None
    assert [(s.i, s.side) for s in a] == [(ev + 4, -1)]
    assert a[0].stop_points > 0                                            # beyond the top of the spike
    later = cand_of("NEWS_MOVE", direction="WITH", delay_min=15, stop="ATR", exit="t15w", markets="FX_MAJOR")
    assert [s.i for s in signals_for(ser, later)] == [ev + 14]
    # without a calendar nothing happens
    ser2 = series_of(cols)
    assert FAM.get("NEWS_MOVE").signals(ser2.dayviews()[0], sim.family_params(with_c), (0, 1000), None) == []


# ----------------------------------------------- semantics on random prices ---
def random_series(ndays=6, seed=5, symbol="EURUSD"):
    days = synth.weekdays("2026-07-01", ndays)
    ent = synth.entry_for(symbol)
    cols = synth.gen_columns(ent, days, seed=seed)
    return synth.series_from_columns(ent, list(cols)), ent, days


def test_trend_pullback_buys_only_in_an_uptrend_and_after_a_bar_closing_back_up():
    ser, ent, days = random_series(8, seed=2)
    n = 0
    for tf, emas in (("M15", "20/50"), ("H1", "20/50"), ("M15", "50/200")):
        c = cand_of("TREND_PULLBACK", tf=tf, emas=emas, touch_atr=0.5, exit="t2", markets="FX_MAJOR")
        fast, slow = emas.split("/")
        ef, es = ser.f(f"ema{fast}_{tf}"), ser.f(f"ema{slow}_{tf}")
        tb = ser.tf(15 if tf == "M15" else 60)
        for di in range(len(ser.dayviews())):
            for s in signals_for(ser, c, di):
                n += 1
                assert ef[s.i] > 0 and es[s.i] > 0
                assert (ef[s.i] > es[s.i]) == (s.side == 1)
                k = bisect_end(tb, s.i)
                assert (tb.c[k] > tb.o[k]) == (s.side == 1)
    assert n > 20


def bisect_end(tb, i):
    import bisect
    k = bisect.bisect_left(tb.end_i, i)
    assert tb.end_i[k] == i
    return k


def test_squeeze_release_only_after_a_quiet_spell_and_through_the_four_hour_box():
    ser, ent, days = random_series(10, seed=9)
    n = 0
    for w in (10, 20):
        c = cand_of("SQUEEZE_RELEASE", width_pct=w, buffer_atr=0.0, exit="t2", markets="FX_MAJOR")
        for di in range(len(ser.dayviews())):
            for s in signals_for(ser, c, di):
                n += 1
                assert 0 <= ser.f("bb_pct")[s.i] <= w
                if s.side == 1:
                    assert ser.c[s.i] > ser.f("hh240")[s.i]
                else:
                    assert ser.c[s.i] < ser.f("ll240")[s.i]
    assert n > 20


# ------------------------------------------------------- nothing looks ahead ---
def truncated(ser, ent, k):
    cols = [list(getattr(ser, nme)[:k]) for nme in ("t", "o", "h", "l", "c", "s", "v")]
    return synth.series_from_columns(ent, cols)


@pytest.mark.parametrize("code", list(SIZES))
def test_signals_do_not_look_ahead(code):
    """Signals at bars before a cut are the same whether or not later bars exist."""
    ser, ent, days = random_series(7, seed=21)
    fam = FAM.get(code)
    grid = fam.grid()
    step = max(1, len(grid) // 6)
    sample = grid[::step][:8]
    if code == "NEWS_MOVE":
        sample = grid[:4]
    k = 5 * 1440 + 11 * 60 + 17                       # a point in the middle of the sixth day
    short = truncated(ser, ent, k)
    news_full = array("i", [-1] * ser.n)
    news_short = array("i", [-1] * short.n)
    for j in range(5 * 1440, ser.n, 97):              # a few releases scattered about the last two days
        news_full[j] = 0
        if j < short.n:
            news_short[j] = 0
    ser._feat["news_min"] = news_full
    short._feat["news_min"] = news_short
    seen = 0
    for cand in sample:
        p = sim.family_params(cand)
        full_days = {d.uk: d for d in ser.dayviews()}
        for dv in short.dayviews():
            a = fam.signals(dv, p, dv.span(cand.session), news_short if code == "NEWS_MOVE" else None)
            fdv = full_days[dv.uk]
            b = fam.signals(fdv, p, fdv.span(cand.session), news_full if code == "NEWS_MOVE" else None)
            a = [(s.i, s.side, s.stop_points) for s in a if s.i < k - 1]
            b = [(s.i, s.side, s.stop_points) for s in b if s.i < k - 1]
            assert a == b, (code, cand.id, dv.uk)
            seen += len(b)
    if code != "NEWS_MOVE":                          # (it needs releases on the calendar; tested by hand above)
        assert seen > 0, "the check proved nothing: this family made no signal before the cut"


def test_the_families_do_make_signals_on_random_prices():
    ser, ent, days = random_series(10, seed=4)
    got = {}
    for code, fam in FAM.FAMILIES.items():
        cnt = 0
        for cand in fam.grid()[::5]:
            if cand.markets != "FX_MAJOR":
                continue
            for di in range(len(ser.dayviews())):
                if code == "NEWS_MOVE":
                    continue
                cnt += len(signals_for(ser, cand, di))
        got[code] = cnt
    assert got["OPENING_RANGE"] > 0 and got["RANGE_BREAK"] > 0 and got["CONTROL_TIME_OF_DAY"] > 0
    assert got["MOMENTUM_CONTINUATION"] > 0 and got["PREV_DAY_LEVELS"] > 0


# --------------------------------------------------------------- the features ---
@pytest.fixture(scope="module")
def three_days():
    days = synth.weekdays("2026-06-29", 5)
    cols = synth.gen_columns(EUR, days, seed=13)
    return cols, days


@pytest.mark.parametrize("set_name", list(F.SETS))
def test_streaming_equals_batch_for_every_feature_set(three_days, set_name):
    t, o, h, l, c, s, v = three_days[0]
    news = None
    if set_name == "news":
        news = F.NewsInputs(tuple(range(day_min("2026-06-29") + 500, t[-1], 700)),
                            day_min("2026-06-29") + 100, t[-1] - 100)
    batch = F.compute_set(set_name, t, o, h, l, c, s, v, news=news)
    sp = F.Streaming([set_name], news)
    for i in range(len(t)):
        got = sp.push(t[i], o[i], h[i], l[i], c[i], s[i], v[i])
        for name, x in got.items():
            assert batch[name][i] == x, (name, i)


@pytest.mark.parametrize("set_name", [x for x in F.SETS if x != "news"])
def test_features_never_look_ahead(three_days, set_name):
    """The value at bar i is the same if the later bars are not there at all."""
    t, o, h, l, c, s, v = three_days[0]
    full = F.compute_set(set_name, t, o, h, l, c, s, v)
    for k in (1500, 2777, 4001, 6123):
        part = F.compute_set(set_name, t[:k], o[:k], h[:k], l[:k], c[:k], s[:k], v[:k])
        for name, arr in part.items():
            assert list(arr) == list(full[name][:k]), (name, k)


def test_live_series_matches_a_series_built_in_one_go(three_days):
    cols = three_days[0]
    n = 2500
    live = F.LiveSeries(EUR)
    for i in range(n):
        live.push(*(col[i] for col in cols))
    batch = synth.series_from_columns(EUR, [list(col[:n]) for col in cols])
    for name in ("atr5", "ema50_M15", "hh60", "vwap_LONDON", "pdh", "bb_pct", "or_L30_hi", "sess"):
        assert list(live.f(name)) == list(batch.f(name)), name
    assert len(live.dayviews()) == len(batch.dayviews())
    c = cand_of("RANGE_BREAK", lookback_min=60, session="LONDON", exit="t2", markets="FX_MAJOR", news="IGNORE")
    assert [(s.i, s.side) for s in signals_for(live, c)] == [(s.i, s.side) for s in signals_for(batch, c)]


def test_the_session_bits_follow_bst_and_gmt():
    for day, london_utc, ny_cash_utc in (("2026-07-01", 7, 13.5), ("2026-01-14", 8, 14.5)):
        ser = series_of(wiggle_columns(day))
        sess = ser.f("sess")
        lon = int(london_utc * 60)
        assert not sess[lon - 1] & F.SESS_LONDON and sess[lon] & F.SESS_LONDON
        assert sess[int(ny_cash_utc * 60)] & F.SESS_US_CASH and not sess[int(ny_cash_utc * 60) - 1] & F.SESS_US_CASH
        assert sess[int(ny_cash_utc * 60)] & F.SESS_OVERLAP                  # London and New York are both open
        assert sess[60] & F.SESS_ASIA and not sess[7 * 60] & F.SESS_ASIA     # Tokyo 09:00-15:00 = 00:00-06:00 UTC


def test_the_uk_day_rolls_at_midnight_uk_and_the_fx_day_at_22_utc():
    t, o, h, l, c, s, v = [], [], [], [], [], [], []
    for d in ("2026-07-01", "2026-07-02"):
        for acc, col in zip((t, o, h, l, c, s, v), wiggle_columns(d)):
            acc.extend(col)
    ser = synth.series_from_columns(EUR, [t, o, h, l, c, s, v])
    uk = ser.f("ukday")
    k = 1440 + 22 * 60 + 59 - 1440 + 1440 - 1440
    jul1_2259 = 22 * 60 + 59                         # 22:59 UTC = 23:59 BST: still 1 July
    jul1_2300 = 23 * 60                              # 23:00 UTC = 00:00 BST: 2 July
    assert uk[jul1_2300] == uk[jul1_2259] + 1
    fx = ser.f("fxday")
    assert fx[21 * 60 + 59] + 1 == fx[22 * 60]


def test_previous_day_levels_skip_the_sunday_sliver_and_hl_excludes_the_current_bar():
    fri, sun, mon = "2026-07-03", "2026-07-05", "2026-07-06"
    cols = [[], [], [], [], [], [], []]
    fri_c = wiggle_columns(fri)
    set_bar(fri_c, 900, h=110400)
    mon_c = wiggle_columns(mon)
    sun_c = wiggle_columns(sun)
    keep = 22 * 60 + 30                              # only Sunday's last hour and a half exists
    for part in (fri_c, [col[keep:] for col in sun_c], mon_c):
        for acc, col in zip(cols, part):
            acc.extend(col)
    ser = series_of(cols)
    i = len(cols[0]) - 1440 + 100                    # 01:40 UTC on Monday
    assert ser.f("pdh")[i] == 110400                 # Friday's high, not the sliver's
    # hl: the previous N minutes only
    assert ser.f("hh60")[i] == max(ser.h[i - 60:i])
    assert ser.f("ll240")[i] == min(ser.l[i - 240:i])


@pytest.mark.parametrize("season,day_sun,day_mon,day_fri,sliver_from", [
    ("BST", "2026-07-05", "2026-07-06", "2026-07-03", 21 * 60),       # FX opens 21:00 UTC; UK Sunday ends 23:00 UTC
    ("GMT", "2026-12-06", "2026-12-07", "2026-12-04", 22 * 60),       # FX opens 22:00 UTC; UK Sunday ends 24:00 UTC
])
def test_mondays_yesterday_is_friday_even_when_sundays_two_hour_sliver_is_full(season, day_sun, day_mon, day_fri,
                                                                              sliver_from):
    """FX opens on a Sunday evening, so the UK Sunday holds exactly 120 bars when every minute exists,
    which was exactly the minimum for a 'day'. Monday's yesterday then flipped between Friday's range
    and a two-hour sliver on whether one quiet minute happened to be missing."""
    def build(sliver_minutes):
        cols = [[], [], [], [], [], [], []]
        fri_c = wiggle_columns(day_fri)
        set_bar(fri_c, 900, h=111000, l=109000)
        sun_c = wiggle_columns(day_sun)
        set_bar(sun_c, sliver_from + 5, h=110500, l=109800)             # a sliver range, far inside Friday's
        mon_c = wiggle_columns(day_mon)
        keep = 1440 - sliver_minutes if season == "GMT" else sliver_from
        take = sun_c if season == "GMT" else sun_c
        sun_part = [col[(1440 - sliver_minutes) if season == "GMT" else sliver_from:
                        (1440 if season == "GMT" else sliver_from + sliver_minutes)] for col in take]
        for part in (fri_c, sun_part, mon_c):
            for acc, col in zip(cols, part):
                acc.extend(col)
        return series_of(cols), len(cols[0]) - 1440 + 600                # 10:00 UTC on Monday
    for minutes in (120, 119, 90):
        ser, i = build(minutes)
        assert ser.f("pdh")[i] == 111000 and ser.f("pdl")[i] == 109000, (season, minutes)


def test_the_previous_day_rule_still_ignores_a_thin_weekday_and_counts_a_full_one():
    """A weekday that was a holiday (under two hours of bars) is not a 'yesterday' either."""
    cols = [[], [], [], [], [], [], []]
    tue, wed, thu = "2026-12-08", "2026-12-09", "2026-12-10"                  # winter: the UK day is the UTC day
    tue_c = wiggle_columns(tue)
    set_bar(tue_c, 600, h=112000)
    wed_c = wiggle_columns(wed)
    thin = [col[:90] for col in wed_c]                                     # a 90-minute "day"
    set_bar(thin, 10, h=130000)
    thu_c = wiggle_columns(thu)
    for part in (tue_c, thin, thu_c):
        for acc, col in zip(cols, part):
            acc.extend(col)
    ser = series_of(cols)
    i = 1440 + 90 + 600
    assert ser.f("pdh")[i] == 112000                                      # Tuesday's, not the thin Wednesday's


def test_vwap_and_sigma_by_hand():
    cols = synth.flat_columns(DAY, 110000, 4)
    t, o, h, l, c, s, v = cols
    for k in range(7 * 60, 7 * 60 + 12):             # London opens at 07:00 UTC
        c[k] = 110000 + 10 * (k - 7 * 60)
        h[k] = c[k] + 3
        l[k] = c[k] - 3
        o[k] = c[k]
        v[k] = 1 + (k % 3)
    ser = series_of(cols)
    k = 7 * 60 + 11
    tp = [((h[j] + l[j] + c[j]) / 3.0, max(v[j], 1)) for j in range(7 * 60, k + 1)]
    sw = sum(w for _, w in tp)
    vw = sum(p * w for p, w in tp) / sw
    var = sum(w * p * p for p, w in tp) / sw - vw * vw
    assert ser.f("vwap_LONDON")[k] == pytest.approx(vw, rel=1e-6)
    assert ser.f("sig_LONDON")[k] == pytest.approx(var ** 0.5, rel=1e-5)
    assert ser.f("vwap_LONDON")[7 * 60 + 8] == 0.0                  # fewer than 10 bars: not meaningful yet
    assert ser.f("vwap_LONDON")[6 * 60] == 0.0                       # not in the London session yet
    assert ser.f("vwap_ASIA")[5] == 0.0                               # Asia is open but has had under 10 bars ...
    assert ser.f("vwap_ASIA")[200] > 0.0                              # ... and by 03:20 UTC it has a figure


def test_bollinger_rank_is_unknown_until_a_hundred_widths_exist(three_days):
    t, o, h, l, c, s, v = three_days[0]
    out = F.compute_set("bb", t, o, h, l, c, s, v)["bb_pct"]
    first = next(i for i, x in enumerate(out) if x >= 0)
    assert first >= (20 + 99) * 15 - 1                                # 20 closes for the first width, then 100 widths
    assert all(x == -1 for x in out[:first]) and 0 <= max(out) <= 100


def test_atr_ema_are_zero_until_warm_and_then_sensible(three_days):
    t, o, h, l, c, s, v = three_days[0]
    atr = F.compute_set("atr", t, o, h, l, c, s, v)
    assert atr["atr5"][5 * 14 - 2] == 0 and atr["atr5"][5 * 14 + 5] > 0
    ema = F.compute_set("ema", t, o, h, l, c, s, v)
    assert ema["ema20_M15"][15 * 20 - 2] == 0 and ema["ema20_M15"][15 * 20 + 20] > 0
    assert ema["ema200_M15"][-1] > 0 and ema["ema50_H1"][-1] > 0 and ema["ema200_H1"][-1] == 0   # 5 days is under 200 hours
    assert ema["ema200_H1"][0] == 0


def test_a_higher_timeframe_bar_only_counts_once_its_last_minute_has_passed():
    b = F.TfBuilder(5)
    out = []
    for m in range(0, 11):
        out.extend(b.push(m, 100 + m, 105 + m, 95 + m, 101 + m))
    assert [x[0] for x in out] == [0, 5]                         # the bucket 10-14 is not out yet
    assert out[0][1:] == (100, 109, 95, 105)                     # open of the first, high / low across the five, close of the last
    assert out[1][1:] == (105, 114, 100, 110)
    # a missing last minute: the next bucket's first bar finishes the old one
    b = F.TfBuilder(5)
    got = []
    for m in (0, 1, 2, 3, 5):
        got.extend(b.push(m, 1, 2, 0, 1))
    assert [x[0] for x in got] == [0]


# ---------------------------------------------------------------- the cache ---
def make_store(tmp_path, ndays=45, first="2026-06-01", n=1):
    store = BarStore(LabPaths(tmp_path))
    ents = synth.universe(n)
    synth.fill_store(store, ents, synth.weekdays(first, ndays), seed=3, now=NOW)
    return store, ents


def test_a_month_of_features_is_computed_once_and_kept(tmp_path, monkeypatch):
    store, ents = make_store(tmp_path, 25, "2026-06-01")
    sym = ents[0].name
    first = F.month_features(store, sym, "2026-06", "atr")
    files = list((tmp_path / "lab" / "cache" / "features" / sym).glob("2026-06.atr.*"))
    assert {f.suffix for f in files} == {".bin", ".json"}
    calls = []
    real = F.compute_set
    monkeypatch.setattr(F, "compute_set", lambda *a, **k: calls.append(1) or real(*a, **k))
    again = F.month_features(store, sym, "2026-06", "atr")
    assert calls == [] and list(again["atr5"]) == list(first["atr5"])      # read from the cache, not recomputed
    head = json.loads((tmp_path / "lab" / "cache" / "features" / sym / "2026-06.atr.json").read_text())
    assert head["code"] == F.CODE_STAMP and head["n"] == len(first["atr5"])


def test_the_cache_refuses_itself_when_the_code_or_a_price_changes(tmp_path, monkeypatch):
    store, ents = make_store(tmp_path, 25, "2026-06-01")
    sym = ents[0].name
    F.month_features(store, sym, "2026-06", "hl")
    calls = []
    real = F.compute_set
    monkeypatch.setattr(F, "compute_set", lambda *a, **k: calls.append(1) or real(*a, **k))
    F.month_features(store, sym, "2026-06", "hl")
    assert calls == []
    monkeypatch.setattr(F, "CODE_STAMP", "changed-code")
    F.month_features(store, sym, "2026-06", "hl")
    assert len(calls) == 1                                                  # new code: recomputed
    monkeypatch.undo()
    monkeypatch.setattr(F, "compute_set", lambda *a, **k: calls.append(1) or real(*a, **k))
    # change one price in the month
    cols = store.read_month(sym, "2026-06")
    cols.c[100] += 5
    store._write_month(sym, "2026-06", cols)
    store._write_index(sym)
    calls.clear()
    F.month_features(store, sym, "2026-06", "hl")
    assert len(calls) == 1


def test_a_months_early_values_are_warm_because_the_previous_month_is_fed_in_first(tmp_path):
    store, ents = make_store(tmp_path, 45, "2026-06-01")
    sym = ents[0].name
    assert store.months(sym) == ["2026-06", "2026-07"]
    jul = F.month_features(store, sym, "2026-07", "atr")
    cold = F.compute_set("atr", *[getattr(store.read_month(sym, "2026-07"), c) for c in F.COLS])
    assert jul["atr5"][0] > 0 and cold["atr5"][0] == 0                       # warm vs cold start
    # and equal to one long run once the average has forgotten its start
    both = F.compute_set("atr", *[getattr(store.read_month(sym, "2026-06"), c) + getattr(store.read_month(sym, "2026-07"), c)
                                  for c in F.COLS])
    n6 = store.read_month(sym, "2026-06").n
    assert list(jul["atr5"][2000:]) == list(both["atr5"][n6 + 2000:])


def test_series_features_from_the_store_match_those_worked_out_in_memory(tmp_path):
    store, ents = make_store(tmp_path, 12, "2026-06-15")
    sym = ents[0].name
    w = store._build_window("past", store.trading_days([sym]), ents, cut_day="2099-01-01", stretch_id="x")
    ser = w.series[sym]
    mem = synth.series_from_columns(ents[0], [list(getattr(ser, c)) for c in F.COLS])
    for name in ("hh240", "sess", "pdh", "or_L15_hi", "vwap_LONDON"):
        assert list(ser.f(name)) == list(mem.f(name)), name


def test_the_news_feature_reads_the_calendar_and_knows_which_days_it_covers(tmp_path):
    store, ents = make_store(tmp_path, 8, "2026-06-15")
    sym = ents[0].name
    cal = CalendarStore(tmp_path / "calendar.sqlite")
    base = dt.datetime(2026, 6, 17, 12, 30, tzinfo=UTC)
    cal.upsert([CalendarEvent("e1", base, "USD", "US", "Non-farm payrolls", 3),
                CalendarEvent("e2", base + dt.timedelta(hours=2), "USD", "US", "Minor", 1),
                CalendarEvent("e3", base + dt.timedelta(days=1), "EUR", "EZ", "EZ thing", 3),
                CalendarEvent("e4", base + dt.timedelta(days=1, hours=1), "JPY", "JP", "BoJ", 3)])
    w = store._build_window("past", store.trading_days([sym]), ents, cut_day="2099-01-01", stretch_id="x",
                            calendar=cal)
    cov = cal.coverage()
    assert w.news_days == {d for d in w.days if cov.start.isoformat() <= d <= cov.end.isoformat()}
    assert w.news_days == {"2026-06-17", "2026-06-18"} and w.news_known("2026-06-17") and not w.news_known("2026-06-16")
    ser = w.series[sym]
    nm = ser.f("news_min")
    i = ser.t.index(day_min("2026-06-17") + 12 * 60 + 20)
    assert nm[i] == 10                                      # ten minutes to the 12:30 release (the medium one is not counted)
    assert nm[ser.t.index(day_min("2026-06-17") + 12 * 60 + 30)] == 0
    assert nm[ser.t.index(day_min("2026-06-16") + 600)] == -1            # a day the calendar does not cover
    j = ser.t.index(day_min("2026-06-17") + 18 * 60)
    assert nm[j] > 0                                                    # next high one is tomorrow's EUR event
    assert F.currencies_of("EURUSD") == ("EUR", "USD") and F.currencies_of("GER40") == ("EUR",)
    assert F.currencies_of("US500") == ("USD",) and F.currencies_of("XAUUSD") == ("USD",)


def test_news_move_wakes_only_with_forty_covered_trading_days(tmp_path):
    cal = CalendarStore(tmp_path / "calendar.sqlite")
    days = synth.weekdays("2026-06-01", 60)
    assert C.news_ready(cal, days) is False and C.news_ready(None, days) is False       # no calendar at all

    def releases(on_days, tag="r"):                       # one release on each of these days
        return [CalendarEvent(f"{tag}{d}", dt.datetime.fromisoformat(d).replace(hour=12, tzinfo=UTC),
                              "USD", "US", "x", 3) for d in on_days]
    cal.upsert(releases(days[:23]))
    covered = F.covered_days(cal, days)
    assert covered == days[:23] and C.news_ready(cal, days) is False
    cal.upsert(releases(days[23:41]))
    assert F.covered_days(cal, days) == days[:41] and C.news_ready(cal, days) is True
    assert F.news_inputs(CalendarStore(tmp_path / "empty.sqlite"), "EURUSD") is None


def test_a_calendar_with_holes_in_it_does_not_count_its_whole_span_as_covered(tmp_path):
    """Two days in October and two in December make a two-month span; the calendar knows four
    of its days. Judging by the span would run AVOID_15M candidates as if the news were quiet
    on days nobody recorded, and wake NEWS_MOVE on four days of data."""
    cal = CalendarStore(tmp_path / "calendar.sqlite")
    days = synth.weekdays("2026-10-01", 45)
    cal.upsert([CalendarEvent("a", dt.datetime(2026, 10, 1, 12, 30, tzinfo=UTC), "USD", "US", "x", 3),
                CalendarEvent("b", dt.datetime(2026, 10, 2, 12, 30, tzinfo=UTC), "USD", "US", "x", 3),
                CalendarEvent("c", dt.datetime(2026, 12, 1, 12, 30, tzinfo=UTC), "USD", "US", "x", 3),
                CalendarEvent("d", dt.datetime(2026, 12, 2, 12, 30, tzinfo=UTC), "USD", "US", "x", 3)])
    assert cal.coverage().start.isoformat() == "2026-10-01" and cal.coverage().end.isoformat() == "2026-12-02"
    assert F.covered_days(cal, days) == ["2026-10-01", "2026-10-02", "2026-12-01", "2026-12-02"]
    assert C.news_ready(cal, days) is False
    assert F.known_days(cal) == {"2026-10-01", "2026-10-02", "2026-12-01", "2026-12-02"}


def test_avoid_news_candidates_wait_for_enough_calendar_rather_than_being_binned_for_want_of_it():
    avoid = [c for c in C.registered() if c.news == "AVOID_15M"]
    ignore = [c for c in C.registered() if c.news == "IGNORE"]
    assert len(avoid) == 144 // 2 + 108 // 2 + 108 // 2 + 108 // 2 and len(avoid) + len(ignore) == 632
    assert all(C.waiting_for_calendar(c, 19) for c in avoid)
    assert not any(C.waiting_for_calendar(c, 20) for c in avoid)
    assert not any(C.waiting_for_calendar(c, 0) for c in ignore)


def test_old_cache_files_can_be_pruned(tmp_path, monkeypatch):
    store, ents = make_store(tmp_path, 25, "2026-06-01")
    sym = ents[0].name
    F.month_features(store, sym, "2026-06", "atr")
    F.month_features(store, sym, "2026-06", "hl")
    assert F.cache_bytes(store.paths) > 0 and F.prune_cache(store) == 0           # nothing to remove yet
    cdir = tmp_path / "lab" / "cache" / "features" / sym
    (cdir / "2020-01.atr.bin").write_bytes(b"x" * 100)                              # a month the store never held
    (cdir / "2020-01.atr.json").write_text(json.dumps({"code": F.CODE_STAMP}))
    (cdir / "2026-06.vwap.bin").write_bytes(b"y" * 50)                              # a .bin whose .json is gone
    assert F.prune_cache(store) >= 150
    assert not (cdir / "2020-01.atr.bin").exists() and not (cdir / "2026-06.vwap.bin").exists()
    assert (cdir / "2026-06.atr.bin").exists()
    monkeypatch.setattr(F, "CODE_STAMP", "other-code")                              # written by older code
    assert F.prune_cache(store) > 0 and F.cache_bytes(store.paths) == 0


def test_every_idea_has_its_own_short_readable_name():
    """13 Oct: every idea is a row of the breakdown, so its one-line name (family, markets, key settings, exit,
    news rule - from its own fields) must tell it from every other."""
    from mintel.lab.candidate import registered, short_name_of
    cands = registered(news_awake=True)
    names = [short_name_of(c) for c in cands]
    assert len(set(names)) == len(cands) == 664 and max(len(n) for n in names) <= 90
    from mintel.lab.families import get
    rb = next(c for c in get("RANGE_BREAK").grid() if c.p("lookback_min") == 60 and c.session == "LONDON"
              and c.markets == "FX_MAJOR" and c.exit == "t2" and c.news == "IGNORE")
    assert short_name_of(rb) == "Breakout · majors · 60 min, London · 2:1"
