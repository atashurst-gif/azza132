"""The fixes made after the lab's review (12 Oct 2026), one test each.

Every figure here is MADE UP for the test; nothing is a result of any idea or bot.

* a lab night still alive after 12 hours keeps its lock; a run is never longer than 11 hours;
  a weekday daytime run does nothing (the practice day is not over)
* a fresh look cut short by the process being stopped bins the idea (the look is spent)
* a past test that crashed twice and binned its idea is not run again every night, and does
  not hold its family's end-of-grid step back
* a practice look is judged on its own day's figures when that night was missed; trades closed
  before day 1 are not part of the stint
* an idea that failed its first practice look shows the failed check, and is never a standout
* the funnel and the family rows do not count binned ideas' neighbour re-tests as passes
* reopen only takes a binned idea that has not spent its fresh look
* the page: a bot's Costs and Luck never pass on a handful of trades; a practising bot's pills
  read the trades its verdict reads; an idea with no practice trade shows "Not yet"; an
  unfinished family is "not graded yet"; made-up prices show TEST DATA on /lab; a run's time
  is never made up; a live trial's count says what it is toward
"""
from __future__ import annotations

import copy
import datetime as dt
import json
import subprocess
import sys

import pytest

from mintel.lab import (BINNED, FAILED, FRESH_TEST, LIVE_TRIAL, NOT_YET, PAST_TEST, PRACTICE, RUNNING, LabPaths,
                        LabSettings, utc_stamp)
from mintel.lab import nightly as NL
from mintel.lab import report as REPORT
from mintel.lab import stages as ST
from mintel.lab.__main__ import main as lab_main
from mintel.lab.ledger import LabLedger
from mintel.ops import showcase as S
from mintel.ops import verdicts as V
from mintel.ops.dashboard import render_lab_board
from tests.test_lab_nightly import Clock, home, make, template  # noqa: F401 - fixtures
from tests.test_lab_ops import _cfg
from tests.test_lab_page import NOW as PAGE_NOW
from tests.test_lab_page import made_up_summary
from tests.test_lab_stages import (NOW, PRACTICE_DAYS, _bench, _practising, add_trades, family, fresh_world,  # noqa
                                   gate, good_cols, grid_of, led, night, past_done, result, stretch)

UTC = dt.timezone.utc


# ---------------------------------------------------------------- the lock ---
def test_a_lab_night_alive_after_twelve_hours_keeps_its_lock(tmp_path):
    paths = LabPaths(tmp_path / "data")
    paths.root.mkdir(parents=True)
    # a live process whose command line is a lab night's
    other = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", "-m", "mintel.lab.nightly"])
    try:
        started = dt.datetime(2026, 10, 13, 8, 0, tzinfo=UTC)
        paths.lock.write_text(json.dumps({"pid": other.pid, "started_utc": utc_stamp(started)}))
        later = started + dt.timedelta(hours=14)
        assert NL.nightly_running(paths, later) is True
        took, why = NL.acquire_lock(paths, later)
        assert not took and f"pid {other.pid}" in why
    finally:
        other.kill()
        other.wait()
    # an old lock whose pid is something else entirely is still a dead night's
    plain = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        paths.lock.write_text(json.dumps({"pid": plain.pid, "started_utc": utc_stamp(started)}))
        assert NL.acquire_lock(paths, started + dt.timedelta(hours=14))[0] is True
    finally:
        plain.kill()
        plain.wait()


def test_a_weekday_daytime_run_does_nothing_and_a_weekend_run_is_never_longer_than_eleven_hours(home, monkeypatch):
    monkeypatch.delenv("MINTEL_LAB_ANY_HOUR", raising=False)
    tue_3pm = dt.datetime(2026, 10, 13, 14, 0, tzinfo=UTC)                  # 15:00 London, Tuesday
    n, _c, lines = make(home, Clock(tue_3pm), no_fetch=True, max_candidates=0)
    assert n.run() == 0
    paths = LabPaths(home / "data")
    assert not (paths.run_dir(ST.night_of(tue_3pm)) / "done").exists() and not paths.lock.exists()
    assert any("the lab runs between 23:10 and 06:00 on weekdays" in x for x in lines)
    assert NL.practice_day_hours(tue_3pm)
    assert not NL.practice_day_hours(dt.datetime(2026, 10, 13, 22, 10, tzinfo=UTC))      # 23:10
    assert not NL.practice_day_hours(dt.datetime(2026, 10, 14, 4, 0, tzinfo=UTC))        # 05:00
    sat_8am = dt.datetime(2026, 10, 17, 7, 0, tzinfo=UTC)                   # 08:00 London, Saturday
    assert not NL.practice_day_hours(sat_8am)
    n, _c, _l = make(home, Clock(sat_8am), no_fetch=True, max_candidates=0)
    assert n.deadline == sat_8am + dt.timedelta(hours=11)                   # not 06:00 Sunday, 22 h away
    assert n.deadline - sat_8am < dt.timedelta(seconds=NL.LOCK_STALE_S)


# ------------------------------------------------------- a look cut short ---
def test_a_fresh_look_cut_short_by_a_stop_bins_the_idea(led, fresh_world):
    outcomes, _calls = fresh_world
    c = grid_of("PREV_DAY_LEVELS")[0]
    past_done(led, c, good_cols(), stage=PAST_TEST)
    gate(led, c, plateau="na", dsr=1.0)

    def die(_cid):
        raise SystemExit(143)                     # SIGTERM inside the simulator
    outcomes[c.id] = die
    with pytest.raises(SystemExit):
        ST.fresh_looks(led, stretch(), None, [], LabSettings(), now=NOW)
    assert led.candidate(c.id)["stage"] == FRESH_TEST
    led.crash_all_running("a lab night was cut short", now=NOW + dt.timedelta(days=1))
    assert ST.bin_spent_looks(led, now=NOW + dt.timedelta(days=1)) == [c.id]
    row = led.candidate(c.id)
    assert (row["stage"], row["bin_stage"], row["bin_reason"]) == (BINNED, FRESH_TEST, ST.LOOK_CUT_SHORT)
    assert ST.bin_spent_looks(led, now=NOW + dt.timedelta(days=2)) == []          # once
    # stopped between the claim and the move: still at PAST TEST with its look spent
    c2 = grid_of("PREV_DAY_LEVELS")[1]
    past_done(led, c2, good_cols(), stage=PAST_TEST)
    gate(led, c2, plateau="na", dsr=1.0)
    st = stretch()
    led.claim_fresh(c2.id, st.stretch_id, st.cut_day, st.fresh_cap, now=NOW)
    led.crash_all_running("a lab night was cut short", now=NOW + dt.timedelta(days=1))
    assert ST.bin_spent_looks(led, now=NOW + dt.timedelta(days=1)) == [c2.id]
    row = led.candidate(c2.id)
    assert (row["stage"], row["bin_stage"]) == (BINNED, FRESH_TEST)


def test_the_night_bins_a_look_cut_short_before_anything_else(home):
    paths = LabPaths(home / "data")
    c = grid_of("PREV_DAY_LEVELS")[0]
    ledg = LabLedger(paths.sqlite, paths.trials)
    try:
        past_done(ledg, c, good_cols(), stage=PAST_TEST)
        gate(ledg, c, plateau="na", dsr=1.0)
        st = stretch()
        ledg.claim_fresh(c.id, st.stretch_id, st.cut_day, st.fresh_cap, now=NOW)        # left 'running'
        ledg.move(c.id, FRESH_TEST, "its one look (made up)", now=NOW)
    finally:
        ledg.close()
    n, _c, lines = make(home, Clock(NOW + dt.timedelta(days=1)), no_fetch=True, max_candidates=0)
    assert n.run() == 0
    ledg = LabLedger(paths.sqlite, paths.trials)
    try:
        assert ledg.candidate(c.id)["stage"] == BINNED
        assert [r["status"] for r in ledg.fresh_rows(c.id)] == ["crashed"]
    finally:
        ledg.close()


# -------------------------------------------------- a past test that crashes ---
def test_a_binned_crashing_past_test_is_not_rerun_and_does_not_block_its_family(led, monkeypatch):
    bad = grid_of("CONTROL_TIME_OF_DAY")[0]
    led.register(bad, "made up", now=NOW)
    st = stretch()
    runs = []

    def boom(*_a, **_k):
        runs.append(1)
        raise RuntimeError("boom")
    monkeypatch.setattr(ST.sim, "run", boom)
    for k in (1, 2):
        tid = led.begin_test(bad.id, "past", st.stretch_id, st.cut_day, now=NOW)
        ST.run_past(led, led.candidate(bad.id), tid, None, st, now=NOW + dt.timedelta(minutes=k))
    assert led.candidate(bad.id)["stage"] == BINNED and len(runs) == 2
    assert ST.pending_past(led, st) == []                       # later nights leave it alone
    for c in grid_of("CONTROL_TIME_OF_DAY")[1:]:
        past_done(led, c, good_cols(net=-10.0, pf=0.5), stage=BINNED)
    assert ST.family_complete(led, "CONTROL_TIME_OF_DAY", st, covered_past_days=0)
    assert led.n_tests(kind="past", family="CONTROL_TIME_OF_DAY") == len(grid_of("CONTROL_TIME_OF_DAY"))


# --------------------------------------------------------------- practice ---
def test_a_missed_look_night_judges_look_one_on_its_own_day(led):
    c = grid_of("PREV_DAY_LEVELS")[0]
    _practising(led, c)
    for d in PRACTICE_DAYS[:10]:
        add_trades(led, c.id, d, [3.0, 3.0, 2.0, -1.0])
    for d in PRACTICE_DAYS[:9]:
        ST.grade_practice(led, [], LabSettings(), now=night(d))
    add_trades(led, c.id, PRACTICE_DAYS[10], [-40.0])            # day 11: a bad day; the night of day 10 missed
    ST.grade_practice(led, [], LabSettings(), now=night(PRACTICE_DAYS[10]))
    look = dict(led.one("SELECT * FROM looks WHERE candidate_id=? AND look=1", (c.id,)))
    assert look["due_day"] == PRACTICE_DAYS[9]
    assert (look["trades"], look["net_gbp"]) == (40, 70.0)       # day 10's figures, never day 11's


def test_trades_before_day_one_are_not_part_of_the_stint(led):
    from mintel.lab import synth
    c = grid_of("PREV_DAY_LEVELS")[0]
    _bench(led, [(c, 0.5, 0.01)])
    noon = dt.datetime(2026, 10, 12, 12, 30, tzinfo=UTC)          # Monday 13:30 London
    ST.grade_practice(led, synth.universe(24), LabSettings(), now=noon)
    since = ST.roster(led)[0]["since_utc"]
    assert ST.practice_start(since) == "2026-10-13"
    add_trades(led, c.id, "2026-10-12", [20.0] * 8)               # Monday afternoon, before day 1
    add_trades(led, c.id, "2026-10-13", [1.0])
    s, trades, days = ST.practice_stats(led, c.id, since, "2026-10-13")
    assert days == ["2026-10-13"] and (s.trades, s.net_gbp, len(trades)) == (1, 1.0, 1)


def test_a_failed_first_look_shows_failed_and_is_never_a_standout(tmp_path):
    ledg = LabLedger(tmp_path / "data" / "lab" / "lab.sqlite")
    try:
        c = grid_of("PREV_DAY_LEVELS")[0]
        _practising(ledg, c)
        for k, d in enumerate(PRACTICE_DAYS[:10]):              # ahead, but too lucky-looking to pass
            add_trades(ledg, c.id, d, [6.0, 6.0, 6.0] if k % 5 in (0, 1, 2) else [-5.0, -5.0, -5.0])
            ST.grade_practice(ledg, [], LabSettings(), now=night(d))
        look = dict(ledg.one("SELECT * FROM looks WHERE candidate_id=? AND look=1", (c.id,)))
        assert look["passed"] == 0 and look["net_gbp"] > 0 and ledg.candidate(c.id)["stage"] == PRACTICE
        add_trades(ledg, c.id, PRACTICE_DAYS[10], [6.0, 6.0, 6.0])
        now = night(PRACTICE_DAYS[10])
        ST.grade_practice(ledg, [], LabSettings(), now=now)
        sm = REPORT.build_summary(ledg, LabPaths(tmp_path / "data"), settings=LabSettings(), universe=[], now=now)
    finally:
        ledg.close()
    ent = next(e for e in sm["candidates"] if e["id"] == c.id)
    assert any(ch["state"] == FAILED for ch in ent["checks"])
    assert ent["what_to_do"].startswith("Watch - it did not pass its first look; judged again on")
    sm["store"]["complete_days"] = 72
    sm["synthetic"] = False                                       # in memory only
    for e in sm["candidates"]:
        e["synthetic"] = False
    lab = {"state": S.LAB_READY, "summary": sm, "synthetic": False}
    assert S.standouts(S.ladder_candidates(lab, now), now) == []


def test_an_idea_with_no_practice_trade_yet_shows_not_yet_never_a_figure(tmp_path):
    ledg = LabLedger(tmp_path / "data" / "lab" / "lab.sqlite")
    try:
        c = grid_of("PREV_DAY_LEVELS")[0]
        _practising(ledg, c)
        now = night(PRACTICE_DAYS[0])
        sm = REPORT.build_summary(ledg, LabPaths(tmp_path / "data"), settings=LabSettings(), universe=[], now=now)
    finally:
        ledg.close()
    ent = next(e for e in sm["candidates"] if e["id"] == c.id)
    states = {ch["key"]: ch["state"] for ch in ent["checks"]}
    assert states == {"trades": NOT_YET, "steady": NOT_YET, "fresh": "Passed", "costs": NOT_YET, "luck": NOT_YET}
    assert "p" not in next(ch for ch in ent["checks"] if ch["key"] == "luck")
    assert ent["headline"] == "Practising since 13 Oct: no practice trades yet."
    assert "£" not in ent["headline"]
    assert not S.luck_computed(next(ch for ch in ent["checks"] if ch["key"] == "luck"))


# ----------------------------------------------------------------- counts ---
def test_binned_ideas_neighbour_retests_are_not_counted_as_passes(led):
    keep, old = grid_of("PREV_DAY_LEVELS")[:2]
    past_done(led, keep, good_cols(), stage=PAST_TEST)
    past_done(led, old, good_cols(), stage=PAST_TEST)
    led.execute("UPDATE candidates SET stage=?, bin_stage=? WHERE id=?", (BINNED, PAST_TEST, old.id))
    assert REPORT._funnel(led, waiting=0)["past_passed"] == 1
    out = ST.family_end(led, "PREV_DAY_LEVELS", stretch(), now=NOW)
    assert out["passed"] == 1
    assert led.family_row("PREV_DAY_LEVELS", "F-2026-10")["past_passed"] == 1


def test_reopen_takes_only_a_binned_idea_whose_fresh_look_is_not_spent(tmp_path, capsys):
    cfg = _cfg(tmp_path)
    cfg.save(tmp_path / "data" / "config.json")
    conf = str(tmp_path / "data" / "config.json")
    paths = LabPaths(tmp_path / "data")
    ledg = LabLedger(paths.sqlite, paths.trials)
    a, b, c = grid_of("PREV_DAY_LEVELS")[:3]
    try:
        for x in (a, b, c):
            past_done(ledg, x, good_cols(), stage=PAST_TEST)
            gate(ledg, x, plateau="na")
        ledg.execute("UPDATE candidates SET stage=? WHERE id=?", (PRACTICE, a.id))
        st = stretch()
        tid = ledg.claim_fresh(b.id, st.stretch_id, st.cut_day, st.fresh_cap, now=NOW)
        ledg.crash_test(tid, "made up", now=NOW)
        ledg.move(b.id, BINNED, "its fresh test crashed (made up)", now=NOW)
        ledg.move(c.id, BINNED, "failed the past test (made up)", now=NOW)
    finally:
        ledg.close()
    assert lab_main(["reopen", a.id, "--reason", "made up", "--config", conf]) == 2
    assert lab_main(["reopen", b.id, "--reason", "made up", "--config", conf]) == 2
    assert "already had its one fresh look" in capsys.readouterr().err
    assert lab_main(["reopen", c.id, "--reason", "made up", "--config", conf]) == 0
    ledg = LabLedger(paths.sqlite, paths.trials)
    try:
        assert [ledg.candidate(x.id)["stage"] for x in (a, b, c)] == [PRACTICE, BINNED, "IDEA"]
    finally:
        ledg.close()


# ---------------------------------------------------------------- the page ---
def _trades(n_days, per_day=1, net=3.0, start=dt.date(2026, 9, 28)):
    out, d = [], start
    while len({t["closed"].date() for t in out}) < n_days:
        if d.weekday() < 5:
            for j in range(per_day):
                out.append({"closed": dt.datetime(d.year, d.month, d.day, 9, j, tzinfo=UTC), "net": net,
                            "costs": 0.4, "kind": "real"})
        d += dt.timedelta(days=1)
    return out


def test_a_bots_costs_and_luck_never_pass_on_a_handful_of_trades():
    now = dt.datetime(2026, 10, 9, 20, tzinfo=UTC)
    v = {"label": "Momentum Runner", "mode": "LIVE", "code": V.NOT_ENOUGH, "why": "made up"}
    few = S.bot_checks("momentum_runner", v, LIVE_TRIAL, "real", _trades(5), "", now=now)
    by = {c["key"]: c for c in few}
    assert by["costs"]["state"] == RUNNING and by["luck"]["state"] == RUNNING
    assert by["luck"].get("p") is not None and S.luck_computed(by["luck"])        # worked out, not passed
    assert S.check_word(S.ordered_checks(few)[0]) == "Running (5 of 20 for the trial)"
    enough = {c["key"]: c for c in S.bot_checks("momentum_runner", v, LIVE_TRIAL, "real", _trades(10, 2), "",
                                                 now=now)}
    assert enough["costs"]["state"] == "Passed" and enough["luck"]["state"] == "Passed"


def test_a_practising_bots_pills_read_the_trades_its_verdict_reads(monkeypatch):
    real = _trades(3)
    monkeypatch.setattr(S.V, "real_trades", lambda view, magic: list(real))
    monkeypatch.setattr(S.V, "practice_trades", lambda *a, **k: [])
    sd = {"bots": [{"id": "crowd_fader", "magic": 990204, "made": 9.0}], "start": "2026-09-28"}
    kind, trades, why = S.bot_record("crowd_fader", {"mode": "PAPER"}, PRACTICE, sd=sd, top={"tot": {"x": 1}},
                                     data_dir="", now=dt.datetime(2026, 10, 9, 20, tzinfo=UTC))
    assert (kind, len(trades), why) == ("mixed", 3, "")
    checks = S.bot_checks("crowd_fader", {"code": V.NOT_ENOUGH}, PRACTICE, kind, trades, "",
                          now=dt.datetime(2026, 10, 9, 20, tzinfo=UTC))
    assert checks[0]["text"].startswith("3 trades (real and practice) over 3 UK days")


def test_lab_board_families_not_graded_and_made_up_prices(tmp_path):
    from tests.test_lab_page import a_day, put_lab
    d = a_day(tmp_path)
    sm = made_up_summary()
    sm["families"][0].update(complete=False, tested=70, past_passed=0, pbo=None)
    put_lab(d.data, sm)
    board = render_lab_board(str(d.data), PAGE_NOW)
    assert "<td>Opening range</td><td>144</td><td>not graded yet (70 of 144 tried)</td><td>-</td><td>-</td>" in board
    syn = copy.deepcopy(made_up_summary(test_data=True))
    assert syn["synthetic"]
    put_lab(d.data, syn)
    board = render_lab_board(str(d.data), PAGE_NOW)
    for figure in ("632 ideas tried", "The clock-trade control passed", "Took 38 min", "<td>0.21</td>",
                   "<td>3</td><td>"):
        assert figure not in board, figure
    assert f"<td>Opening range</td><td>144</td><td>{S.TEST_DATA}</td>" in board


def test_a_runs_time_is_never_made_up():
    now = dt.datetime(2026, 10, 13, 7, tzinfo=UTC)
    base = {"date": "2026-10-12", "started": "2026-10-12T22:10:00+00:00", "tested": 10, "ok": True}
    assert S.last_night_words(dict(base, seconds=0.0), now) == "Last night: 10 tested."
    assert S.last_night_words(dict(base, seconds=30.0), now) == "Last night: 10 tested in under a minute."
    assert S.last_night_words(dict(base, seconds=2280.0), now) == "Last night: 10 tested in 38 min."
