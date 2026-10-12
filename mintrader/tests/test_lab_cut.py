"""The cut: one date, moved forward only, once a month.

Aaron: "ensure that consistency stays, luck doesn't". The cut is what keeps
the fresh prices fresh: it never moves back, it opens a new stretch when it
moves, and the registered grid is frozen for the stretch so that the number of
things tried (N) is known before anything runs.
"""
from __future__ import annotations

import datetime as dt
import json

import pytest

from mintel.lab import (BINNED, FRESH_TEST, IDEA, PAST_TEST, Candidate, LabPaths, make_params,
                        synth)
from mintel.lab import cut as C
from mintel.lab.candidate import registered, registered_ids
from mintel.lab.cut import CutError, GridChanged
from mintel.lab.ledger import LabLedger

UTC = dt.timezone.utc


def day(n, first="2026-06-01"):
    return synth.weekdays(first, n)


def at(y, m, d=1, h=23, mi=10):
    return dt.datetime(y, m, d, h, mi, tzinfo=UTC)


@pytest.fixture
def paths(tmp_path):
    return LabPaths(tmp_path)


@pytest.fixture
def led(tmp_path):
    l = LabLedger(tmp_path / "lab" / "lab.sqlite", tmp_path / "research_trials.sqlite")
    yield l
    l.close()


# ---------------------------------------------------------------- the rule ---
@pytest.mark.parametrize("d,fresh", [(40, 20), (60, 20), (80, 20), (90, 22), (100, 25), (130, 32),
                                     (180, 45), (400, 45)])
def test_fresh_days_are_a_quarter_of_the_days_clamped_to_20_to_45(d, fresh):
    assert C.fresh_days_for(d) == fresh


def test_the_cut_day_is_the_fresh_days_th_complete_day_counted_back():
    days = day(72)
    cut = C.cut_day_for(days)
    assert cut == days[-20]                                    # 72 days -> 18 -> clamped to 20
    assert len([d for d in days if d >= cut]) == 20
    days = day(130)
    assert C.cut_day_for(days) == days[-32]
    assert len([d for d in days if d >= C.cut_day_for(days)]) == 32


def test_under_40_complete_days_there_is_no_cut():
    assert C.cut_day_for(day(39)) is None
    assert C.cut_day_for(day(40)) is not None


# ------------------------------------------------------------------ rolling ---
def test_the_first_cut_is_set_once_forty_days_are_held(paths, led):
    ids = registered_ids()
    state, moved = C.ensure_cut(paths, day(39), ids, at(2026, 8, 1), ledger=led)
    assert state is None and moved is False and not paths.cut.exists()
    days = day(40)
    state, moved = C.ensure_cut(paths, days, ids, at(2026, 8, 3), ledger=led)
    assert moved and state.cut_day == days[-20]
    assert state.stretch_id == "F-2026-08" and state.fresh_used == 0 and state.fresh_cap == 30
    assert state.grid_hash == C.grid_hash(ids) and state.grid_n == 632
    assert state.complete_days == 40 and state.fresh_days == 20 and state.short_history
    on_disk = json.loads(paths.cut.read_text())
    assert on_disk["cut_day"] == state.cut_day and on_disk["history"] == []
    assert set(on_disk) >= {"cut_day", "stretch_id", "set_utc", "grid_hash", "fresh_used", "fresh_cap",
                            "history"}


def test_it_rolls_once_a_month_on_the_first_nightly_and_never_twice(paths, led):
    ids = registered_ids()
    s1, m1 = C.ensure_cut(paths, day(50), ids, at(2026, 8, 5), ledger=led)
    assert m1
    # later in the same month, with more days: no move
    s2, m2 = C.ensure_cut(paths, day(60), ids, at(2026, 8, 20), ledger=led)
    assert not m2 and s2.cut_day == s1.cut_day and s2.stretch_id == s1.stretch_id
    # the first nightly of September: forward
    days = day(75)
    s3, m3 = C.ensure_cut(paths, days, ids, at(2026, 9, 1), ledger=led)
    assert m3 and s3.cut_day > s1.cut_day and s3.stretch_id == "F-2026-09"
    assert s3.history[0]["stretch_id"] == "F-2026-08" and s3.history[0]["cut_day"] == s1.cut_day
    assert s3.history[0]["grid_hash"] == s1.grid_hash
    # and not again on the 2nd
    s4, m4 = C.ensure_cut(paths, day(76), ids, at(2026, 9, 2), ledger=led)
    assert not m4 and s4.stretch_id == "F-2026-09"


def test_a_month_with_no_new_days_does_not_move_the_cut(paths, led):
    ids = registered_ids()
    days = day(60)
    s1, _ = C.ensure_cut(paths, days, ids, at(2026, 8, 5), ledger=led)
    s2, moved = C.ensure_cut(paths, days, ids, at(2026, 9, 1), ledger=led)         # the same days
    assert not moved and s2.cut_day == s1.cut_day


def test_set_cut_refuses_an_earlier_date_and_ignores_the_same_one(paths, led):
    ids = registered_ids()
    days = day(80)
    s1 = C.set_cut(paths, days[-20], grid_ids=ids, now=at(2026, 8, 5), ledger=led)
    with pytest.raises(CutError, match="only moves forward"):
        C.set_cut(paths, days[-21], grid_ids=ids, now=at(2026, 9, 1), ledger=led)
    assert C.load_cut(paths).cut_day == s1.cut_day                                   # untouched
    same = C.set_cut(paths, days[-20], grid_ids=ids, now=at(2026, 9, 1), ledger=led)
    assert same.stretch_id == s1.stretch_id and same.history == []
    later = C.set_cut(paths, days[-10], grid_ids=ids, now=at(2026, 9, 1), ledger=led)
    assert later.cut_day == days[-10] and later.stretch_id != s1.stretch_id


def test_a_second_move_in_the_same_month_gets_its_own_stretch_id(paths, led):
    ids = registered_ids()
    days = day(80)
    a = C.set_cut(paths, days[-30], grid_ids=ids, now=at(2026, 8, 5), ledger=led)
    b = C.set_cut(paths, days[-25], grid_ids=ids, now=at(2026, 8, 6), ledger=led)
    assert (a.stretch_id, b.stretch_id) == ("F-2026-08", "F-2026-08b")


def test_a_move_opens_a_new_stretch_resets_fresh_used_and_keeps_the_old_count(paths, led):
    ids = registered_ids()
    days = day(80)
    s1 = C.set_cut(paths, days[-30], grid_ids=ids, now=at(2026, 8, 5), ledger=led)
    c = Candidate("OPENING_RANGE", 1, "FX_MAJOR", "LONDON", "IGNORE", "t1", make_params(k=1))
    led.register(c, "one")
    tid = led.begin_test(c.id, "past", s1.stretch_id, s1.cut_day)
    led.finish_test(tid, {"trades": 80, "net_gbp": 10.0, "passed": 1}, {}, reasons=[], skipped={},
                    synthetic=True, seconds=1)
    led.regrade_past(tid, plateau="ok", passed=True, reasons=[])
    led.claim_fresh(c.id, s1.stretch_id, s1.cut_day, 30)
    mirrored = C.sync_fresh_used(paths, led)
    assert mirrored.fresh_used == 1 and C.load_cut(paths).fresh_used == 1
    s2 = C.set_cut(paths, days[-20], grid_ids=ids, now=at(2026, 9, 2), ledger=led)
    assert s2.fresh_used == 0 and s2.stretch_id != s1.stretch_id
    assert s2.history[-1]["fresh_used"] == 1                                         # the old stretch's count is kept
    assert led.fresh_used(s2.stretch_id) == 0 and led.fresh_used(s1.stretch_id) == 1


def test_a_move_marks_those_waiting_for_a_fresh_look_as_due_a_retest(paths, led):
    ids = registered_ids()
    days = day(80)
    C.set_cut(paths, days[-30], grid_ids=ids, now=at(2026, 8, 5), ledger=led)      # the first cut
    cs = [Candidate("OPENING_RANGE", 1, "FX_MAJOR", "LONDON", "IGNORE", "t1", make_params(k=i)) for i in range(4)]
    for c in cs:
        led.register(c, c.id)
    led.move(cs[0].id, PAST_TEST, "passed the past test")           # waiting for a fresh look
    led.move(cs[1].id, PAST_TEST, "passed the past test")
    led.move(cs[1].id, FRESH_TEST, "looked at fresh")                # already looked: its verdict stands
    led.move(cs[2].id, BINNED, "failed")
    assert led.retest_due_ids() == []
    C.set_cut(paths, days[-20], grid_ids=ids, now=at(2026, 9, 2), ledger=led)      # the monthly roll
    assert led.retest_due_ids() == [cs[0].id]
    assert led.candidate(cs[1].id)["retest_due"] == 0 and led.candidate(cs[2].id)["retest_due"] == 0
    assert led.candidate(cs[3].id)["retest_due"] == 0 and led.candidate(cs[3].id)["stage"] == IDEA


# ------------------------------------------------------------- the grid hash ---
def test_the_grid_hash_freezes_the_registered_set_for_the_stretch(paths, led):
    ids = registered_ids()
    s = C.set_cut(paths, day(80)[-20], grid_ids=ids, now=at(2026, 8, 5), ledger=led)
    C.check_grid(s, ids)                                                # the same grid: fine
    C.check_grid(s, list(reversed(ids)))                                # in any order
    with pytest.raises(GridChanged):
        C.check_grid(s, ids + ["lab-0000000000"])                       # a grown grid mid-stretch
    with pytest.raises(GridChanged):
        C.check_grid(s, ids[:-1])
    with pytest.raises(GridChanged):
        C.check_grid(s, registered_ids(news_awake=True))                # NEWS_MOVE waking mid-stretch
    # NEWS_MOVE joins at a roll, and the new stretch records the new hash
    s2 = C.set_cut(paths, day(80)[-10], grid_ids=registered_ids(True), now=at(2026, 9, 2), ledger=led,
                   news_awake=True)
    assert s2.grid_n == 664 and s2.news_awake and s2.grid_hash != s.grid_hash
    C.check_grid(s2, registered_ids(True))


def test_the_night_one_grid_is_632_and_the_hash_is_of_ids_only():
    assert len(registered_ids()) == 632 and len(registered_ids(True)) == 664
    h1 = C.grid_hash(registered_ids())
    assert len(h1) == 40 and h1 == C.grid_hash(sorted(registered_ids()))
    assert h1 != C.grid_hash(registered_ids(True))
    assert C.grid_hash([]) != C.grid_hash(["lab-aaaaaaaaaa"])


def test_the_state_survives_a_round_trip_through_cut_json(paths, led):
    s = C.set_cut(paths, day(80)[-20], grid_ids=registered_ids(), now=at(2026, 8, 5), ledger=led)
    again = C.load_cut(paths)
    assert again == s and again.fresh_left == 30
    paths.cut.write_text("{ not json")
    assert C.load_cut(paths) is None
    assert C.load_cut(LabPaths(paths.data_dir / "nowhere")) is None


def test_news_move_joins_the_grid_only_at_a_roll_and_only_with_enough_covered_days(paths, led, tmp_path):
    from mintel.broker.base import CalendarEvent
    from mintel.news.calendar_store import CalendarStore
    days = day(90)
    target, awake, ids = C.plan_roll(days, None)
    assert target == days[-22] and awake is False and len(ids) == 632                # no calendar: dormant
    cal = CalendarStore(tmp_path / "calendar.sqlite")

    def releases(on_days):                       # one release on each of these days
        return [CalendarEvent(f"r{d}", dt.datetime.fromisoformat(d).replace(hour=12, tzinfo=UTC),
                              "USD", "US", "x", 3) for d in on_days]
    cal.upsert(releases(days[:20]))
    assert C.plan_roll(days, cal)[1] is False                                         # 20 covered days
    cal.upsert(releases([days[0], days[59]]))                                         # a calendar with holes in it:
    assert C.plan_roll(days, cal)[1] is False                                         # a long span is not coverage
    cal.upsert(releases(days[20:60]))
    target, awake, ids = C.plan_roll(days, cal)
    assert awake is True and len(ids) == 664
    state, moved = C.ensure_cut(paths, days, ids, at(2026, 11, 2), ledger=led, news_awake=awake)
    assert moved and state.news_awake and state.grid_n == 664
    C.check_grid(state, registered_ids(state.news_awake))                              # the same set every night of the stretch
    with pytest.raises(GridChanged):
        C.check_grid(state, registered_ids(False))


# ------------------------------------------------- the cap of looks is never above thirty ---
def test_a_cap_above_thirty_is_not_honoured_by_a_move_or_by_an_edited_cut_file(paths, led):
    """FRESH_P is 0.15 spread over 30 looks. A cap of 500 (a setting, an argument, or a hand-edited
    cut.json) would be about 2.5 lucky passes a stretch under a luck line that still says 30."""
    ids = registered_ids()
    s = C.set_cut(paths, day(80)[-30], grid_ids=ids, now=at(2026, 8, 5), ledger=led, fresh_cap=500)
    assert s.fresh_cap == 30 and C.load_cut(paths).fresh_cap == 30
    lowered = C.set_cut(paths, day(80)[-20], grid_ids=ids, now=at(2026, 9, 2), ledger=led, fresh_cap=12)
    assert lowered.fresh_cap == 12                                                   # lowering is fine
    raw = json.loads(paths.cut.read_text())
    raw["fresh_cap"] = 500
    paths.cut.write_text(json.dumps(raw))
    edited = C.load_cut(paths)
    assert edited.fresh_cap == 30 and edited.fresh_left == 30
    assert C.CutState("2026-09-01", "F-2026-09", "x", "y", fresh_cap=99).fresh_cap == 30
    moved, did = C.ensure_cut(paths, day(110), ids, at(2026, 10, 1), ledger=led, fresh_cap=1000)
    assert did and moved.fresh_cap == 30
