"""The lab's price store: points, ascending times, completeness, the cut.

The rules under test (Aaron: "ensure that consistency stays, luck doesn't"):
a PAST window never holds a bar at or after the cut; fresh bars come out of
exactly one door, once; a store filled from the simulated broker is SYNTHETIC
all the way through.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import random
import re
import sqlite3
from pathlib import Path

import pytest

from mintel.broker.base import Bar
from mintel.contracts import SymbolSpec
from mintel.lab import (KIND_FRESH, LabPaths, LabSettings, day_min, epoch_min, iso_utc,
                        min_to_day, sim, synth, uk_day_min)
from mintel.lab import store as S
from mintel.lab.cut import CutError, CutState, grid_hash, save_cut, set_cut
from mintel.lab.ledger import FreshRefused, LabLedger

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 9, 28, 12, 0, tzinfo=UTC)


def make_store(tmp_path, n_symbols=2, ndays=10, first="2026-08-03", seed=1):
    paths = LabPaths(tmp_path)
    store = S.BarStore(paths)
    ents = synth.universe(n_symbols)
    days = synth.weekdays(first, ndays)
    synth.fill_store(store, ents, days, seed=seed, now=NOW)
    return store, ents, days


# --------------------------------------------------------------- round trip ---
def test_a_month_round_trips_in_integer_points_with_ascending_times(tmp_path):
    store = S.BarStore(LabPaths(tmp_path))
    point = 1e-5
    t0 = dt.datetime(2026, 8, 3, 0, 0, tzinfo=UTC)
    bars = [Bar(t0 + dt.timedelta(minutes=i), 1.10000 + 0.00001 * i, 1.10005 + 0.00001 * i,
                1.09995 + 0.00001 * i, 1.10002 + 0.00001 * i, 1_000_000.0, 0.0, 8.0)
            for i in range(50)]
    # hand the store the bars in a muddle: reversed, with a duplicate
    muddled = list(reversed(bars)) + [bars[10]]
    rep = store.merge_bars("EURUSD", muddled, point=point, digits=5, now=NOW)
    assert rep.added == 50
    cols = store.read_month("EURUSD", "2026-08")
    assert list(cols.t) == sorted(set(cols.t)) and cols.n == 50
    assert cols.t[0] == epoch_min(t0)
    assert cols.o[0] == 110000 and cols.h[0] == 110005 and cols.l[0] == 109995 and cols.c[0] == 110002
    assert cols.o[7] == 110007                       # prices are whole points: integers
    assert set(cols.s) == {8}
    assert max(cols.v) == 65535                      # tick volume is capped at a uint16
    assert store.index("EURUSD")["point"] == point and store.index("EURUSD")["digits"] == 5


def test_files_are_little_endian_columns_of_the_documented_width(tmp_path):
    store, ents, days = make_store(tmp_path, 1, 3)
    d = tmp_path / "lab" / "bars" / ents[0].name
    n = store.index(ents[0].name)["months"]["2026-08"]["n"]
    assert n == 3 * 1440
    for col, width in (("t", 4), ("o", 4), ("h", 4), ("l", 4), ("c", 4), ("s", 2), ("v", 2)):
        assert (d / f"2026-08.{col}.bin").stat().st_size == n * width
    assert (d / "index.json").exists()
    raw = (d / "2026-08.t.bin").read_bytes()
    assert int.from_bytes(raw[:4], "little") == day_min(days[0])


def test_a_later_fetch_rewrites_the_overlap_and_heals_a_gap(tmp_path):
    store = S.BarStore(LabPaths(tmp_path))
    ent = synth.entry_for("EURUSD")
    t, o, h, l, c, s, v = synth.flat_columns("2026-08-03", 110000, 5)
    # the first fetch missed 60 minutes and had a wrong close for minute 300
    keep = [i for i in range(1440) if not 500 <= i < 560]
    sub = [[col[i] for i in keep] for col in (t, o, h, l, c, s, v)]
    sub[4][keep.index(300)] = 99999
    store.merge_columns("EURUSD", *sub, point=1e-5, digits=5, now=NOW)
    assert store.index("EURUSD")["days"]["2026-08-03"]["bars"] == 1380
    rep = store.merge_columns("EURUSD", t, o, h, l, c, s, v, point=1e-5, digits=5, now=NOW)
    cols = store.read_month("EURUSD", "2026-08")
    assert cols.n == 1440 and rep.added == 60 and rep.rewritten == 1
    assert cols.c[300] == 110000                      # the new fetch won on the overlap
    assert store.index("EURUSD")["days"]["2026-08-03"]["bars"] == 1440


def test_a_second_month_is_a_second_set_of_files(tmp_path):
    store, ents, days = make_store(tmp_path, 1, 8, first="2026-07-27")
    assert store.months(ents[0].name) == ["2026-07", "2026-08"]
    d = tmp_path / "lab" / "bars" / ents[0].name
    assert (d / "2026-07.t.bin").exists() and (d / "2026-08.t.bin").exists()


# --------------------------------------------------------------- completeness ---
def test_a_day_is_complete_at_85_percent_of_the_median_weekday_not_before(tmp_path):
    store = S.BarStore(LabPaths(tmp_path))
    days = synth.weekdays("2026-08-03", 6)
    keep_counts = {days[0]: 1440, days[1]: 1440, days[2]: 1440,
                   days[3]: int(1440 * 0.85) - 1,        # just under
                   days[4]: int(1440 * 0.85) + 1,        # just over
                   days[5]: 1440}
    t, o, h, l, c, s, v = [], [], [], [], [], [], []
    for d in days:
        cols = synth.flat_columns(d, 110000, 5)
        n = keep_counts[d]
        for acc, col in zip((t, o, h, l, c, s, v), cols):
            acc.extend(col[:n])
    store.merge_columns("EURUSD", t, o, h, l, c, s, v, point=1e-5, digits=5, now=NOW)
    info = store.index("EURUSD")["days"]
    assert [info[d]["complete"] for d in days] == [True, True, True, False, True, True]
    assert store.complete_days("EURUSD") == [d for d in days if d != days[3]]


def test_weekends_and_today_are_never_complete(tmp_path):
    store = S.BarStore(LabPaths(tmp_path))
    days = ["2026-09-04", "2026-09-05", "2026-09-06", "2026-09-07"]   # Fri, Sat, Sun, Mon
    t, o, h, l, c, s, v = [], [], [], [], [], [], []
    for d in days:
        for acc, col in zip((t, o, h, l, c, s, v), synth.flat_columns(d, 110000, 5)):
            acc.extend(col)
    today = dt.datetime(2026, 9, 7, 15, 0, tzinfo=UTC)               # Monday afternoon
    store.merge_columns("EURUSD", t, o, h, l, c, s, v, point=1e-5, digits=5, now=today)
    assert store.complete_days("EURUSD") == ["2026-09-04"]           # Sat/Sun not weekdays; Monday is today


def test_trading_days_need_80_percent_of_the_symbols(tmp_path):
    store, ents, days = make_store(tmp_path, 5, 4)
    assert store.trading_days() == days
    # knock one day out of two of five symbols: 3 of 5 = 60% < 80% -> not a trading day
    for e in ents[:2]:
        t, o, h, l, c, s, v = synth.flat_columns(days[1], 110000, 5)
        keep = 600
        # rewrite the day with only 600 bars by rebuilding the month
        cols = store.read_month(e.name, "2026-08")
        fresh = S.Cols()
        for i in range(cols.n):
            if min_to_day(cols.t[i]) == days[1] and cols.t[i] - day_min(days[1]) >= keep:
                continue
            fresh.append_row(cols.t[i], cols.o[i], cols.h[i], cols.l[i], cols.c[i], cols.s[i], cols.v[i])
        store._write_month(e.name, "2026-08", fresh)
        store._rebuild_days(e.name, NOW)
        store._write_index(e.name)
    td = store.trading_days([e.name for e in ents])
    assert days[1] not in td and len(td) == 3
    assert days[1] in store.trading_days([e.name for e in ents], share=0.5)


# ------------------------------------------------------------- crash safety ---
def test_a_crash_half_way_through_a_rewrite_is_noticed_and_repaired(tmp_path, monkeypatch):
    store, ents, days = make_store(tmp_path, 1, 3)
    name = ents[0].name
    before = store.read_month(name, "2026-08").crc()
    t, o, h, l, c, s, v = synth.flat_columns(days[-1], 100, 5)       # different values, same times
    real_replace = os.replace
    calls = {"n": 0}

    def flaky(a, b):
        calls["n"] += 1
        if calls["n"] == 4:                                           # die after three columns
            raise OSError("power cut")
        return real_replace(a, b)
    monkeypatch.setattr(S.os, "replace", flaky)
    with pytest.raises(OSError):
        store.merge_columns(name, t, o, h, l, c, s, v, point=1e-5, digits=5, now=NOW)
    monkeypatch.setattr(S.os, "replace", real_replace)
    store.refresh()
    assert store.verify(name) == ["2026-08"]                          # mixed old and new: refused
    with pytest.raises(S.TornMonth):
        store.read_month(name, "2026-08")
    assert store.repair(name) == ["2026-08"]
    assert store.months(name) == [] and store.complete_days(name) == []
    store.merge_columns(name, t, o, h, l, c, s, v, point=1e-5, digits=5, now=NOW)   # and it can be refetched
    assert store.verify(name) == [] and before != store.read_month(name, "2026-08").crc()


def test_leftover_temp_files_never_break_a_read(tmp_path):
    store, ents, days = make_store(tmp_path, 1, 2)
    d = tmp_path / "lab" / "bars" / ents[0].name
    (d / ".2026-08.o.bin.999.tmp").write_bytes(b"junk")
    assert store.verify(ents[0].name) == []


def test_a_changed_point_is_refused(tmp_path):
    store, ents, days = make_store(tmp_path, 1, 2)
    t, o, h, l, c, s, v = synth.flat_columns(days[-1], 110000, 5)
    with pytest.raises(S.StoreError):
        store.merge_columns(ents[0].name, t, o, h, l, c, s, v, point=1e-4, digits=4, now=NOW)


# -------------------------------------------------------------- synthetic ---
def test_a_synthetic_store_is_stamped_and_the_stamp_survives_a_real_looking_merge(tmp_path):
    store, ents, days = make_store(tmp_path, 2, 3)
    assert store.is_synthetic()
    assert store.index(ents[0].name)["source"] == "SYNTHETIC"
    t, o, h, l, c, s, v = synth.flat_columns(days[-1], 110000, 5)
    store.merge_columns(ents[0].name, t, o, h, l, c, s, v, point=ents[0].spec.point,
                        digits=ents[0].spec.digits, source="bridge", now=NOW)
    assert store.index(ents[0].name)["source"] == "SYNTHETIC"          # never washed clean
    w = store._build_window("past", days, ents, cut_day="2099-01-01", stretch_id="s")
    assert w.synthetic is True
    res = sim.run(synth_candidate(), w, family=_NoSignals())
    assert res.synthetic is True


def test_a_bridge_store_is_not_synthetic(tmp_path):
    store = S.BarStore(LabPaths(tmp_path))
    t, o, h, l, c, s, v = synth.flat_columns("2026-08-03", 110000, 5)
    store.merge_columns("EURUSD", t, o, h, l, c, s, v, point=1e-5, digits=5, source="bridge", now=NOW)
    assert not store.is_synthetic()


class _NoSignals:
    code = "NONE"
    dormant = False

    def signals(self, *a):
        return []


def synth_candidate():
    from mintel.lab import Candidate, make_params
    return Candidate("NONE", 1, "FX_MAJOR", "ALL", "IGNORE", "t1", make_params())


# ------------------------------------------------------------ the date cut ---
def test_a_past_window_never_holds_a_bar_at_or_after_the_cut(tmp_path):
    """Property: for random cuts, in or across months, no PAST bar is at/after it."""
    store, ents, days = make_store(tmp_path, 2, 30, first="2026-07-06")
    rng = random.Random(7)
    all_days = store.trading_days([e.name for e in ents])
    assert len(all_days) == 30
    cuts = [rng.choice(all_days[3:]) for _ in range(25)] + [all_days[3], all_days[-1], "2026-08-01"]
    for cut in cuts:
        w = store._build_window("past", [d for d in all_days if d < cut], ents,
                                cut_day=cut, stretch_id="p")
        cut_min = day_min(cut)
        for ser in w.series.values():
            assert ser.n and ser.t[-1] < cut_min, (cut, ser.symbol)
            assert max(ser.t) < cut_min
        assert all(d < cut for d in w.days)


def test_a_past_window_refuses_a_cut_later_than_the_registered_one(tmp_path):
    store, ents, days = make_store(tmp_path, 2, 20)
    paths = store.paths
    save_cut(paths, CutState(cut_day=days[12], stretch_id="F-2026-09", set_utc="2026-09-01T00:00:00+00:00",
                             grid_hash="x"))
    w = store.past_window(ents)                                       # the registered cut
    assert w.cut_day == days[12] and all(d < days[12] for d in w.days)
    with pytest.raises(S.StoreError):
        store.past_window(ents, cut_day=days[13])                     # would show fresh bars to a past test
    w2 = store.past_window(ents, cut_day=days[10])                    # an earlier cut is fine
    assert w2.days[-1] < days[10]


def test_a_cut_handed_in_as_a_state_cannot_get_round_the_registered_one(tmp_path):
    """The guard used to look at cut_day= only; past_window(cut=<state>) trusted the caller's cut and
    returned a window of kind 'past' that held fresh days."""
    store, ents, days = make_store(tmp_path, 2, 20)
    save_cut(store.paths, CutState(cut_day=days[12], stretch_id="F-2026-09",
                                   set_utc="2026-09-01T00:00:00+00:00", grid_hash="x"))
    later = CutState(cut_day=days[18], stretch_id="F-2099-01", set_utc="2026-09-01T00:00:00+00:00", grid_hash="y")
    with pytest.raises(S.StoreError, match="later than the registered cut"):
        store.past_window(ents, cut=later)
    with pytest.raises(S.StoreError):
        store.past_window(ents, cut=later, cut_day=days[18])
    earlier = CutState(cut_day=days[8], stretch_id="F-2026-08", set_utc="2026-08-01T00:00:00+00:00", grid_hash="z")
    w = store.past_window(ents, cut=earlier)                          # an earlier one is fine, and names its stretch
    assert w.cut_day == days[8] and w.stretch_id == "F-2026-08" and all(d < days[8] for d in w.days)


def test_the_warm_up_bars_come_from_before_the_window_and_the_series_is_ascending(tmp_path):
    store, ents, days = make_store(tmp_path, 1, 12, first="2026-07-27")
    cut = days[8]
    w = store._build_window("fresh", [d for d in days if d >= cut], ents, cut_day=cut, stretch_id="f")
    ser = w.series[ents[0].name]
    assert ser.first_i == 400
    assert ser.t[ser.first_i] == day_min(cut)
    assert all(ser.t[i] < ser.t[i + 1] for i in range(ser.n - 1))
    assert ser.t[0] < day_min(cut)                                    # the warm-up is older prices


def test_the_uk_session_clock_in_bst_and_gmt():
    # London opens 08:00 UK: 07:00 UTC in BST, 08:00 UTC in GMT
    assert S.session_bounds("LONDON", "2026-07-01")[0] == day_min("2026-07-01") + 7 * 60
    assert S.session_bounds("LONDON", "2026-01-14")[0] == day_min("2026-01-14") + 8 * 60
    # the US cash open 09:30 New York: 13:30 UTC in summer (EDT), 14:30 UTC in winter (EST)
    assert S.session_bounds("US_CASH", "2026-07-01")[0] == day_min("2026-07-01") + 13 * 60 + 30
    assert S.session_bounds("US_CASH", "2026-01-14")[0] == day_min("2026-01-14") + 14 * 60 + 30
    # the UK day starts at 23:00 UTC the evening before in BST
    assert uk_day_min("2026-07-01") == day_min("2026-07-01") - 60
    assert uk_day_min("2026-01-14") == day_min("2026-01-14")


# ------------------------------------------------------------- fresh_once ---
def _ledger_with_past_pass(tmp_path, cid, stretch, passed=1):
    led = LabLedger(tmp_path / "lab" / "lab.sqlite", tmp_path / "research_trials.sqlite")
    from mintel.lab import Candidate, make_params
    c = Candidate("STUB", 1, "FX_MAJOR", "ALL", "IGNORE", "t1", make_params(a=1))
    led.register(c, "stub")
    tid = led.begin_test(c.id, "past", stretch, "2026-09-01", now=NOW)
    led.finish_test(tid, {"trades": 80, "net_gbp": 100.0, "passed": passed}, {}, reasons=[],
                    skipped={}, synthetic=True, seconds=0.1, now=NOW)
    led.regrade_past(tid, plateau="ok", passed=bool(passed), reasons=[])       # the family-end step
    return led, c


def test_fresh_once_gives_the_fresh_bars_once_and_only_once(tmp_path):
    store, ents, days = make_store(tmp_path, 2, 20)
    cut = days[12]
    stretch = CutState(cut_day=cut, stretch_id="F-2026-09", set_utc="2026-09-01T00:00:00+00:00",
                       grid_hash="x", fresh_cap=30)
    led, c = _ledger_with_past_pass(tmp_path, "unused", stretch.stretch_id)
    # the running row is committed BEFORE the window is handed over
    win = S.fresh_once(c.id, led, stretch, store, ents, now=NOW)
    assert win.kind == "fresh" and win.days == days[12:]
    row = led.test_row(win.test_id)
    assert row["kind"] == "fresh" and row["status"] == "running" and row["stretch_id"] == "F-2026-09"
    assert led.fresh_used("F-2026-09") == 1
    for ser in win.series.values():
        assert ser.t[ser.first_i] >= day_min(cut)                     # the tradable bars start at the cut
    # a second look at the same candidate is refused, in the same or any stretch
    with pytest.raises(FreshRefused) as e:
        S.fresh_once(c.id, led, stretch, store, ents, now=NOW)
    assert e.value.code == "ALREADY_LOOKED"
    assert led.fresh_used("F-2026-09") == 1                           # and nothing was spent
    led.close()


def test_fresh_once_needs_a_passed_past_test(tmp_path):
    store, ents, days = make_store(tmp_path, 2, 20)
    stretch = CutState(cut_day=days[12], stretch_id="F-2026-09", set_utc="2026-09-01T00:00:00+00:00",
                       grid_hash="x")
    led, c = _ledger_with_past_pass(tmp_path, "u", "F-2026-09", passed=0)
    with pytest.raises(FreshRefused) as e:
        S.fresh_once(c.id, led, stretch, store, ents, now=NOW)
    assert e.value.code == "NO_PAST_PASS"
    with pytest.raises(FreshRefused) as e:
        S.fresh_once("lab-0000000000", led, stretch, store, ents, now=NOW)
    assert e.value.code == "NO_PAST_PASS"
    assert led.fresh_used("F-2026-09") == 0
    led.close()


def test_fresh_once_stops_at_the_cap_of_looks_in_a_stretch(tmp_path):
    store, ents, days = make_store(tmp_path, 2, 20)
    stretch = CutState(cut_day=days[12], stretch_id="F-2026-09", set_utc="2026-09-01T00:00:00+00:00",
                       grid_hash="x", fresh_cap=1)
    led, c = _ledger_with_past_pass(tmp_path, "u", "F-2026-09")
    from mintel.lab import Candidate, make_params
    c2 = Candidate("STUB", 1, "FX_MAJOR", "ALL", "IGNORE", "t2", make_params(a=2))
    led.register(c2, "stub2")
    tid = led.begin_test(c2.id, "past", "F-2026-09", days[12], now=NOW)
    led.finish_test(tid, {"trades": 80, "net_gbp": 100.0, "passed": 1}, {}, reasons=[],
                    skipped={}, synthetic=True, seconds=0.1, now=NOW)
    led.regrade_past(tid, plateau="ok", passed=True, reasons=[])
    S.fresh_once(c.id, led, stretch, store, ents, now=NOW)
    with pytest.raises(FreshRefused) as e:
        S.fresh_once(c2.id, led, stretch, store, ents, now=NOW)
    assert e.value.code == "CAP_REACHED"
    led.close()


def test_the_only_door_to_fresh_bars(tmp_path):
    """No module outside store.py and stages.py may mention fresh_once, and the
    public window method refuses to build a fresh window by hand."""
    root = Path(__file__).resolve().parents[1] / "mintel"
    offenders = []
    for p in root.rglob("*.py"):
        if "fresh_once" in p.read_text(encoding="utf-8", errors="ignore"):
            offenders.append(p.relative_to(root).as_posix())
    assert set(offenders) <= {"lab/store.py", "lab/stages.py"}, offenders
    # nor may anything build a fresh window by hand: the private builder, called with the fresh kind,
    # is reached from fresh_once alone
    for p in root.rglob("*.py"):
        rel = p.relative_to(root).as_posix()
        text = p.read_text(encoding="utf-8", errors="ignore")
        if rel != "lab/store.py":
            assert "_build_window(" not in text, rel
    src = (root / "lab" / "store.py").read_text(encoding="utf-8")
    fresh_calls = [m.start() for m in re.finditer(r"_build_window\(\s*KIND_FRESH", src)]
    assert len(fresh_calls) == 1
    start = src.index("def fresh_once")
    assert start < fresh_calls[0] and "\ndef " not in src[start + 5:fresh_calls[0]]
    store, ents, days = make_store(tmp_path, 1, 5)
    assert not hasattr(store, "fresh_window") and not hasattr(store, "window")
    # past_window cannot be talked into the fresh days by passing a late cut when a cut is registered
    save_cut(store.paths, CutState(cut_day=days[3], stretch_id="s", set_utc="2026-09-01T00:00:00+00:00",
                                   grid_hash="x"))
    with pytest.raises(S.StoreError):
        store.past_window(ents, cut_day=days[4])


def test_the_fresh_result_is_final_in_the_book(tmp_path):
    led, c = _ledger_with_past_pass(tmp_path, "u", "F-2026-09")
    tid = led.claim_fresh(c.id, "F-2026-09", "2026-09-01", 30, now=NOW)
    led.finish_test(tid, {"trades": 31, "net_gbp": 10.0, "passed": 1}, {"2026-09-01": (10.0, 31)},
                    reasons=[], skipped={}, synthetic=True, seconds=0.1, now=NOW)
    with pytest.raises(sqlite3.DatabaseError, match="fresh result is final"):
        led.execute("UPDATE tests SET net_gbp=999 WHERE id=?", (tid,))
    with pytest.raises(sqlite3.DatabaseError):
        led.execute("DELETE FROM tests WHERE id=?", (tid,))
    assert led.test_row(tid)["net_gbp"] == 10.0
    led.close()


# --------------------------------------------------------------- universe ---
class FakeFeed:
    """A read-only feed with a made-up symbol list."""
    synthetic = True

    def __init__(self, table):
        self.table = table             # name -> (digits, point, tick_value, contract, spread points)

    def symbols(self):
        return list(self.table)

    def spec(self, name):
        digits, point, tv, contract, spread, *rest = self.table[name]
        full = not rest or rest[0]
        return SymbolSpec(name=name, digits=digits, point=point, tick_size=point,
                          tick_value=tv if full else 0.0, contract_size=contract,
                          volume_min=0.01, volume_max=100.0, volume_step=0.01,
                          stops_level_points=10.0, freeze_level_points=0.0, typical_spread_points=spread)

    def ping(self):
        return {"ok": True}


def _feed():
    t = {}
    for n in ("EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCHF", "USDCAD", "NZDUSD"):
        t[n] = (5, 1e-5, 0.79, 100000, 3)
    for i, n in enumerate(("EURGBP", "EURJPY", "GBPJPY", "EURCHF", "EURAUD", "EURCAD", "GBPAUD", "GBPCAD",
                           "AUDJPY", "CADJPY", "AUDNZD", "AUDCAD", "GBPCHF", "EURNZD", "NZDJPY",
                           "CHFJPY", "AUDCHF")):
        t[n] = (5, 1e-5, 0.7, 100000, 5 + i)          # spreads 5 ... 21 points
    t["US500"] = (1, 0.1, 0.079, 1, 5)                 # 0.5 index points
    t["GER40"] = (1, 0.1, 0.085, 1, 8)
    t["UK100"] = (1, 0.1, 0.1, 1, 9)
    t["US30"] = (1, 0.1, 0.079, 1, 20)                 # 2.0
    t["JP225"] = (1, 0.1, 0.0005, 1, 90)               # 9 index points: too wide
    t["XAUUSD"] = (2, 0.01, 0.79, 100, 15)
    t["XAUEUR"] = (2, 0.01, 0.79, 100, 15)             # gold, but not XAUUSD
    t["XAGUSD"] = (3, 0.001, 3.9, 5000, 30)            # silver: not a lab kind
    t["BTCUSD"] = (2, 0.01, 0.79, 1, 50)               # excluded by pattern
    t["EURUSD.b"] = (5, 1e-5, 0.79, 100000, 3)         # excluded by pattern (.b)
    t["AAPL.US"] = (2, 0.01, 0.79, 1, 4)               # a share
    t["EURSEK"] = (5, 1e-5, 0.1, 100000, 90)           # exotic
    t["USDZAR"] = (5, 1e-5, 0.04, 100000, 190)
    t["BROKEN"] = (5, 1e-5, 0.79, 100000, 3, False)    # no tick value: not a full spec
    return FakeFeed(t)


def test_the_universe_is_built_from_the_feeds_own_list(tmp_path):
    ents = S.build_universe(_feed(), now=NOW)
    names = [e.name for e in ents]
    kinds = {e.name: e.kind for e in ents}
    assert len(ents) <= 24 and len(set(names)) == len(names)
    assert all(m in names for m in ("EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCHF", "USDCAD", "NZDUSD"))
    assert "XAUUSD" in names and kinds["XAUUSD"] == "GOLD"
    for bad in ("XAUEUR", "XAGUSD", "BTCUSD", "EURUSD.b", "AAPL.US", "EURSEK", "USDZAR", "BROKEN", "JP225"):
        assert bad not in names, bad
    assert {"US500", "GER40", "UK100", "US30"} <= set(names)
    # the trader's excluded_symbols are KEPT: another rule may win where Trend & Breakout lost
    assert "USDJPY" in names and "USDCHF" in names and "GBPJPY" in names
    minors = [n for n in names if kinds[n] == "FX_MINOR"]
    assert minors and all(S.lab_kind(n) == "FX_MINOR" for n in minors)
    # minors above 12 points are out; the rest are the cheapest first
    spec = {e.name: e.spec for e in ents}
    assert all(spec[n].typical_spread_points <= 12 for n in minors)
    assert set(kinds.values()) == {"FX_MAJOR", "FX_MINOR", "INDEX", "GOLD"}
    assert all(e.spec_taken_utc for e in ents)


def test_the_universe_is_capped_and_never_crowds_out_indices_or_gold(tmp_path):
    t = {n: (5, 1e-5, 0.79, 100000, 3) for n in ("EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCHF", "USDCAD", "NZDUSD")}
    ccys = ["EUR", "GBP", "AUD", "NZD", "CAD", "CHF", "JPY"]
    k = 0
    for a in ccys:
        for b in ccys:
            if a != b and (a + b) not in t and (b + a) not in t and (a + b) != "USDJPY":
                t[a + b] = (5, 1e-5, 0.7, 100000, 4 + (k % 8))
                k += 1
    t.update({"US500": (1, 0.1, 0.079, 1, 5), "GER40": (1, 0.1, 0.085, 1, 8), "XAUUSD": (2, 0.01, 0.79, 100, 15)})
    ents = S.build_universe(FakeFeed(t), now=NOW)
    names = [e.name for e in ents]
    assert len(ents) == 24 and {"US500", "GER40", "XAUUSD"} <= set(names)


def test_the_universe_is_fixed_once_written_and_settings_symbols_override(tmp_path):
    paths = LabPaths(tmp_path)
    ents, created = S.ensure_universe(paths, _feed(), LabSettings(), now=NOW)
    assert created and paths.universe.exists()
    first = json.loads(paths.universe.read_text())
    assert [s["name"] for s in first["symbols"]] == [e.name for e in ents]
    assert first["symbols"][0]["spec"]["tick_value"] > 0 and first["written_utc"]
    # a second build (even from a different feed) changes nothing
    other = FakeFeed({"EURUSD": (5, 1e-5, 0.79, 100000, 3)})
    again, created2 = S.ensure_universe(paths, other, LabSettings(), now=NOW)
    assert not created2 and [e.name for e in again] == [e.name for e in ents]
    assert S.write_universe(paths, again, now=NOW) is False
    loaded = S.load_universe(paths, LabSettings(symbols=("EURUSD", "XAUUSD")))
    assert [e.name for e in loaded] == [n for n in (e.name for e in ents) if n in ("EURUSD", "XAUUSD")]
    assert isinstance(loaded[0].spec, SymbolSpec) and loaded[0].spec.point > 0
    assert S.load_universe(LabPaths(tmp_path / "nowhere")) == []


def test_the_universe_override_names_its_own_symbols(tmp_path):
    ents = S.build_universe(_feed(), now=NOW, only=["EURUSD", "XAUUSD", "US30", "EURSEK"])
    assert sorted(e.name for e in ents) == ["EURUSD", "US30", "XAUUSD"]   # EURSEK is an exotic: never


# ------------------------------------------------------------- hour table ---
def test_the_spread_floor_table_is_the_median_by_utc_hour_and_is_kept_for_the_stretch(tmp_path):
    store = S.BarStore(LabPaths(tmp_path))
    days = synth.weekdays("2026-08-03", 6)
    t, o, h, l, c, s, v = [], [], [], [], [], [], []
    for d in days:
        cols = synth.flat_columns(d, 110000, 5)
        for acc, col in zip((t, o, h, l, c, s, v), cols):
            acc.extend(col)
    for i in range(len(t)):
        if (t[i] // 60) % 24 == 14:
            s[i] = 9 if i % 3 else 11              # median 9 at 14:00 UTC
        if (t[i] // 60) % 24 == 3:
            s[i] = 0                               # no recorded spread at 03:00: no floor
    store.merge_columns("EURUSD", t, o, h, l, c, s, v, point=1e-5, digits=5, now=NOW)
    table = store.hour_table("EURUSD", day_min(days[5]))
    assert table[14] == 9 and table[10] == 5 and table[3] == 0
    # asked again with the same cut it is the same table even if bars were added since
    t2, o2, h2, l2, c2, s2, v2 = synth.flat_columns(days[5], 110000, 50)
    store.merge_columns("EURUSD", t2, o2, h2, l2, c2, s2, v2, point=1e-5, digits=5, now=NOW)
    assert store.hour_table("EURUSD", day_min(days[5])) == table


def test_coverage_says_how_many_days_the_lab_has(tmp_path):
    store, ents, days = make_store(tmp_path, 3, 7)
    cov = store.coverage()
    assert cov["symbols"] == 3 and cov["complete_days"] == 7 and cov["needed_days"] == 40
    assert cov["ready"] is False and cov["synthetic"] is True
    assert cov["first_day"] == days[0] and cov["last_complete"] == days[-1]


def test_the_stores_size_is_counted(tmp_path):
    store, ents, days = make_store(tmp_path, 2, 3)
    assert store.bytes_used() > 2 * 3 * 1440 * 20


# ------------------------------------------------------- corrupt rows, settings ---
def test_corrupt_bars_are_never_stored(tmp_path):
    store = S.BarStore(LabPaths(tmp_path))
    t0 = dt.datetime(2026, 8, 3, 0, 0, tzinfo=UTC)
    good = Bar(t0, 1.1, 1.1002, 1.0998, 1.1001, 10.0, 0.0, 3.0)
    bad = [Bar(t0 + dt.timedelta(minutes=1), 0.0, 1.1, 1.0, 1.1),                 # a price of zero
           Bar(t0 + dt.timedelta(minutes=2), 1.1, 1.0990, 1.1000, 1.1),          # high under low
           Bar(t0 + dt.timedelta(minutes=3), 1.1, 1.1001, 1.0999, 1.1005)]       # close above the high
    rep = store.merge_bars("EURUSD", [good] + bad, point=1e-5, digits=5, now=NOW)
    assert rep.added == 1 and rep.dropped == 3
    assert store.read_month("EURUSD", "2026-08").n == 1


def test_lab_settings_are_forgiving_and_there_is_no_live_path(tmp_path):
    d = LabSettings()
    assert (d.mode, d.risk_gbp, d.practice_slots, d.fresh_cap_per_stretch, d.fresh_per_night) == ("PAPER", 13.0, 6, 30, 3)
    assert (d.history_days, d.max_mb, d.deadline_local, d.symbols) == (130, 300, "06:00", None)
    assert LabSettings.load(tmp_path).mode == "PAPER"                         # no file: the defaults
    (tmp_path / "lab.json").write_text(json.dumps({"mode": "LIVE", "risk_gbp": "abc", "practice_slots": 9,
                                                   "symbols": ["EURUSD", "XAUUSD"], "deadline_local": "25:99"}))
    s = LabSettings.load(tmp_path)
    assert s.mode == "PAPER"                                                  # LIVE is read as PAPER
    assert s.risk_gbp == 13.0 and s.practice_slots == 9 and s.deadline_local == "06:00"
    assert s.symbols == ("EURUSD", "XAUUSD")
    for word, want in (("off", "OFF"), ("PAPER", "PAPER"), ("DEMO", "OFF"), ("", "OFF")):
        (tmp_path / "lab.json").write_text(json.dumps({"mode": word}))
        assert LabSettings.load(tmp_path).mode == want
    (tmp_path / "lab.json").write_text("{ torn")
    assert LabSettings.load(tmp_path).mode == "PAPER"
    LabSettings(mode="OFF", practice_slots=3).save(tmp_path)                  # atomic write, read back
    assert LabSettings.load(tmp_path) == LabSettings(mode="OFF", practice_slots=3)
    assert not [p for p in tmp_path.iterdir() if p.name.endswith(".tmp")]


def test_every_path_the_lab_owns_is_under_one_directory(tmp_path):
    p = LabPaths(tmp_path, tmp_path / "logs")
    assert p.root == tmp_path / "lab" and p.sqlite == tmp_path / "lab" / "lab.sqlite"
    assert p.bars == tmp_path / "lab" / "bars" and p.cut == tmp_path / "lab" / "cut.json"
    assert p.summary == tmp_path / "lab" / "summary.json" and p.universe == tmp_path / "lab" / "universe.json"
    assert p.mode_file == tmp_path / "lab.json" and p.status == tmp_path / "lab-status.json"
    assert p.pid == tmp_path / "lab.pid" and p.trials == tmp_path / "research_trials.sqlite"
    assert p.practice_log == tmp_path / "logs" / "lab.out.log" and p.nightly_log == tmp_path / "logs" / "lab-nightly.log"
    assert p.run_dir("2026-10-12") == tmp_path / "lab" / "runs" / "2026-10-12"
    assert p.lock == tmp_path / "lab" / "nightly.lock" and p.stop == tmp_path / "lab" / "STOP"
    assert LabPaths(tmp_path).practice_log is None


def test_dropping_features_frees_the_arrays_but_not_the_bars(tmp_path):
    store, ents, days = make_store(tmp_path, 2, 6)
    w = store._build_window("past", days, ents, cut_day="2099-01-01", stretch_id="x")
    ser = w.series[ents[0].name]
    ser.f("atr5"), ser.f("hh60"), ser.tf(5)
    assert ser.has_feature("atr5") and ser.has_feature("hh60")
    w.drop_features(keep=("atr",))
    assert ser.has_feature("atr5") and not ser.has_feature("hh60") and ser.n > 0
    w.drop_features()
    assert not ser.has_feature("atr5")
    assert ser.f("atr5")[-1] > 0                                          # and it comes back on demand


def test_a_day_still_being_traded_may_signal_on_its_last_bar_a_finished_one_may_not():
    cols = synth.flat_columns("2026-07-01", 110000, 5)
    cut = [col[:600] for col in cols]                              # 10:00 UTC: the data stops mid-day
    part = synth.series_from_columns(synth.entry_for("EURUSD"), cut)
    dv = part.dayviews()[0]
    assert dv.open_ended and dv.span("ALL")[1] == dv.i1 == 600
    full = synth.series_from_columns(synth.entry_for("EURUSD"), cols)
    fdv = full.dayviews()[0]
    assert not fdv.open_ended and fdv.span("ALL")[1] == fdv.flat_i == 20 * 60 + 54


def test_a_settings_override_can_name_a_symbol_the_file_does_not_hold(tmp_path):
    paths = LabPaths(tmp_path)
    ents, created = S.ensure_universe(paths, _feed(), LabSettings(), now=NOW)
    assert created and "EURSEK" not in [e.name for e in ents]
    written = paths.universe.read_text()
    only, created2 = S.ensure_universe(paths, _feed(), LabSettings(symbols=("EURUSD", "GER40")), now=NOW)
    assert not created2 and sorted(e.name for e in only) == ["EURUSD", "GER40"]
    # a name that is not a lab market at all is still refused, override or not
    only, _ = S.ensure_universe(paths, _feed(), LabSettings(symbols=("EURUSD", "EURSEK", "AAPL.US")), now=NOW)
    assert [e.name for e in only] == ["EURUSD"]
    assert paths.universe.read_text() == written              # the file itself never changes
