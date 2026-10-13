"""The lab's book of record: lab.sqlite and the shared trial ledger.

What must hold (Aaron: "ensure that consistency stays, luck doesn't"):
a `tests` row exists BEFORE a simulation starts and survives a crash, so N
(the number of things tried) can never be understated; a candidate gets one
fresh look and its result is final; every finished test also lands in the
shared TrialLedger as study ``lab:<FAMILY>`` with ``is_oos`` for fresh looks.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3

import pytest

from mintel.lab import (BINNED, FRESH_TEST, IDEA, LEDGER_SCHEMA, PAST_TEST, PRACTICE, Candidate,
                        make_params, sim, synth)
from mintel.lab import ledger as L
from mintel.lab.ledger import AlreadyTested, FreshRefused, LabLedger, LedgerError
from mintel.research.trial_ledger import TrialLedger

NOW = dt.datetime(2026, 10, 12, 23, 10, tzinfo=dt.timezone.utc)
STRETCH = "F-2026-10"
CUT = "2026-09-14"


@pytest.fixture
def led(tmp_path):
    ledger = LabLedger(tmp_path / "lab" / "lab.sqlite", tmp_path / "research_trials.sqlite")
    yield ledger
    ledger.close()


def cands(n=3, family="OPENING_RANGE"):
    return [Candidate(family, 1, "FX_MAJOR", "LONDON", "IGNORE", "t1", make_params(k=i)) for i in range(n)]


def past_done(led, c, passed=1, stretch=STRETCH, trades=80, net=120.0, gate=True):
    """A finished past test. ``gate=True`` also does the family-end step (``regrade_past``) that
    the fresh door insists on; ``gate=False`` leaves it as the first verdict wrote it."""
    tid = led.begin_test(c.id, "past", stretch, CUT, now=NOW)
    led.finish_test(tid, {"trades": trades, "net_gbp": net, "passed": passed, "pf": 1.3,
                          "window_from": "2026-07-01", "window_to": "2026-09-11", "p": 0.01},
                    {"2026-09-10": (net, trades)}, reasons=[], skipped={}, synthetic=True,
                    seconds=0.5, trial={"sharpe": 1.5, "expectancy_r": 0.2}, now=NOW)
    if gate:
        led.regrade_past(tid, plateau="ok", passed=bool(passed), reasons=[])
    return tid


# ----------------------------------------------------------------- the DDL ---
SPEC_COLUMNS = {
    "candidates": "id family version def_json name stage stage_since_utc created_utc parent_id "
                  "retest_due bin_stage bin_reason binned_utc rank_score "
                  "retired_utc retired_reason",      # 13 Oct, the weekly pool refresh: ledger.CANDIDATE_EXTRA_COLUMNS
    "tests": "id candidate_id kind stretch_id cut_day status started_utc finished_utc seconds window_from "
             "window_to days trades net_gbp cost_gbp pf win_rate up_days down_days best_day_gbp p folds_up "
             "folds_worst plateau cost15_net cost20_net dsr max_dd_r passed reasons_json skipped_json "
             "synthetic ledger_row_id "
             "assumed_share gated",             # after the spec's own list: see ledger.TEST_EXTRA_COLUMNS
    "daily": "test_id day net_gbp trades",
    "families": "code stretch_id n past_passed pbo extend_grid_json wf_ok control_pass_json",
    "practice_trades": "ticket candidate_id symbol side volume entry stop target opened_utc closed_utc "
                       "exit_price exit_reason gross_gbp commission_gbp spread_points slippage_points net_gbp "
                       "risk_gbp r signal_bar_utc tick_at_signal_json fill_source mode source",
    "practice_open": "candidate_id symbol side volume entry stop target peak opened_utc",
    "practice_days": "candidate_id day net_gbp trades",
    "looks": "candidate_id look due_day done_utc trades net_gbp p pf up_days down_days cost15_net passed",
    "roster": "candidate_id slot since_utc symbols_json",
    "events": "id ts_utc candidate_id kind from_stage to_stage why by",
    "runs": "date started_utc finished_utc ok step tested fresh seconds seconds_per_candidate "
            "deadline_hit note",
}


def test_the_tables_are_exactly_the_specs(led):
    for table, cols in SPEC_COLUMNS.items():
        have = [r[1] for r in led.query(f"PRAGMA table_info({table})")]
        assert have == cols.split(), table
    assert led.one("PRAGMA user_version")[0] == LEDGER_SCHEMA
    assert led.one("PRAGMA journal_mode")[0].lower() == "wal"
    # practice defaults
    led.execute("INSERT INTO practice_trades(ticket, candidate_id) VALUES (970000001, 'x')")
    r = led.one("SELECT mode, source FROM practice_trades")
    assert (r["mode"], r["source"]) == ("PRACTICE", "bridge")
    assert led.one("SELECT sql FROM sqlite_master WHERE name='tests'")["sql"].count("UNIQUE") == 1


def test_a_second_ledger_on_the_same_file_sees_committed_rows(tmp_path):
    a = LabLedger(tmp_path / "lab.sqlite")
    b = LabLedger(tmp_path / "lab.sqlite")
    c = cands(1)[0]
    a.register(c, "one")
    tid = a.begin_test(c.id, "past", STRETCH, CUT, now=NOW)
    assert b.test_row(tid)["status"] == "running"                       # visible to the practice process
    a.close(); b.close()


# ------------------------------------------- the row exists before the run ---
def test_the_tests_row_is_committed_before_the_simulation_starts(tmp_path, monkeypatch):
    path = tmp_path / "lab" / "lab.sqlite"
    led = LabLedger(path)
    watcher = LabLedger(path)
    c = cands(1)[0]
    led.register(c, "one")
    seen = {}

    def fake_run(candidate, window, **kw):
        # at the moment the simulator runs, ANOTHER connection already sees the running row
        rows = watcher.query("SELECT status FROM tests WHERE candidate_id=?", (candidate.id,))
        seen["status"] = [r[0] for r in rows]
        return "result"
    monkeypatch.setattr(sim, "run", fake_run)
    tid = led.begin_test(c.id, "past", STRETCH, CUT, now=NOW)
    assert sim.run(c, None) == "result"
    assert seen["status"] == ["running"]
    led.close(); watcher.close()


def test_a_crash_leaves_the_row_and_it_still_counts_toward_n(led, monkeypatch):
    c = cands(1)[0]
    led.register(c, "one")
    tid = led.begin_test(c.id, "past", STRETCH, CUT, now=NOW)
    assert led.n_tests() == 1
    monkeypatch.setattr(sim, "run", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    try:
        sim.run(c, None)
    except RuntimeError as e:
        led.crash_test(tid, str(e), now=NOW)
    row = led.test_row(tid)
    assert row["status"] == "crashed" and "boom" in row["reasons_json"]
    assert led.n_tests() == 1 and led.n_tests(kind="past") == 1
    # a retry of the same candidate in the same stretch revives the SAME row: still one trial
    again = led.begin_test(c.id, "past", STRETCH, CUT, now=NOW)
    assert again == tid and led.n_tests() == 1
    assert led.test_row(tid)["status"] == "running"


def test_leftover_running_rows_from_a_dead_process_are_marked_crashed(led):
    cs = cands(3)
    for c in cs:
        led.register(c, c.id)
        led.begin_test(c.id, "past", STRETCH, CUT, now=NOW)
    past_done(led, cs[0], stretch="F-2026-09")
    assert led.crash_all_running("the run was cut short", now=NOW) == 3
    assert {r["status"] for r in led.query("SELECT status FROM tests")} == {"crashed", "done"}
    assert led.n_tests() == 4


def test_a_finished_past_test_cannot_be_begun_again(led):
    c = cands(1)[0]
    led.register(c, "one")
    past_done(led, c)
    with pytest.raises(AlreadyTested):
        led.begin_test(c.id, "past", STRETCH, CUT, now=NOW)
    # a new stretch is a new trial
    led.begin_test(c.id, "past", "F-2026-11", "2026-10-12", now=NOW)
    assert led.n_tests() == 2


def test_n_counts_running_crashed_and_done_by_family(led):
    a = cands(2, "OPENING_RANGE")
    b = cands(1, "RANGE_BREAK")
    for c in a + b:
        led.register(c, c.id)
    led.begin_test(a[0].id, "past", STRETCH, CUT, now=NOW)                    # running
    t2 = led.begin_test(a[1].id, "past", STRETCH, CUT, now=NOW)
    led.crash_test(t2, now=NOW)                                                # crashed
    past_done(led, b[0])                                                       # done
    assert led.n_tests() == 3
    assert led.n_by_family() == {"OPENING_RANGE": 2, "RANGE_BREAK": 1}
    assert led.n_tests(family="OPENING_RANGE") == 2


# ---------------------------------------------------------- the trial ledger ---
def test_every_finished_test_goes_to_the_trial_ledger_with_the_study_and_is_oos(led, tmp_path):
    c = cands(1)[0]
    led.register(c, "one")
    tid = past_done(led, c)
    f = led.claim_fresh(c.id, STRETCH, CUT, 30, now=NOW)
    led.finish_test(f, {"trades": 40, "net_gbp": 55.0, "pf": 1.4, "passed": 1, "p": 0.003,
                        "window_from": "2026-09-14", "window_to": "2026-10-09"}, {}, reasons=[],
                    skipped={}, synthetic=True, seconds=0.2, trial={"sharpe": 2.0, "expectancy_r": 0.3},
                    now=NOW)
    tl = TrialLedger(tmp_path / "research_trials.sqlite")
    rows = tl.all("lab:OPENING_RANGE")
    assert [r["is_oos"] for r in rows] == [0, 1]
    assert all(r["study"] == "lab:OPENING_RANGE" for r in rows)
    assert rows[0]["dataset"] == "SYNTHETIC" and rows[0]["trades"] == 80 and rows[0]["net_money"] == 120.0
    assert json.loads(rows[0]["params_json"])["family"] == "OPENING_RANGE"
    assert rows[0]["sharpe"] == 1.5 and rows[1]["expectancy_r"] == 0.3
    assert rows[0]["period_start"] == "2026-07-01" and rows[1]["period_end"] == "2026-10-09"
    # the test rows point at their trial rows
    assert led.test_row(tid)["ledger_row_id"] == rows[0]["id"]
    assert led.test_row(f)["ledger_row_id"] == rows[1]["id"]
    assert tl.count("lab:OPENING_RANGE") == 2
    assert led.family_sharpes("OPENING_RANGE") == [1.5]                      # past trials only
    tl.close()


def test_a_ledger_without_a_trials_path_still_keeps_the_tests(tmp_path):
    led = LabLedger(tmp_path / "x.sqlite")
    c = cands(1)[0]
    led.register(c, "one")
    assert past_done(led, c) > 0 and led.trial_ledger() is None and led.family_sharpes("X") == []
    led.close()


# ------------------------------------------------------------ fresh is final ---
def test_the_fresh_result_cannot_be_changed_or_deleted(led):
    c = cands(1)[0]
    led.register(c, "one")
    past_done(led, c)
    f = led.claim_fresh(c.id, STRETCH, CUT, 30, now=NOW)
    # while it is still running the row can be finished, once
    led.finish_test(f, {"trades": 31, "net_gbp": 10.0, "passed": 1}, {"2026-10-01": (10.0, 31)},
                    reasons=[], skipped={}, synthetic=False, seconds=1.0, now=NOW)
    for sql in ("UPDATE tests SET net_gbp=500 WHERE id=?", "UPDATE tests SET passed=0 WHERE id=?",
                "UPDATE tests SET status='crashed' WHERE id=?", "UPDATE tests SET trades=99 WHERE id=?",
                "DELETE FROM tests WHERE id=?"):
        with pytest.raises(sqlite3.DatabaseError):
            led.execute(sql, (f,))
    assert led.test_row(f)["net_gbp"] == 10.0 and led.test_row(f)["passed"] == 1
    with pytest.raises(LedgerError):                                          # and finishing again is refused
        led.finish_test(f, {"net_gbp": 1.0}, {}, reasons=[], skipped={}, synthetic=False, seconds=0)


def test_a_past_row_can_be_regraded_a_fresh_row_cannot(led):
    c = cands(1)[0]
    led.register(c, "one")
    tid = past_done(led, c, passed=1)
    led.regrade_past(tid, plateau="spike", passed=False, reasons=["NOT_STEADY"], dsr=0.4)
    r = led.test_row(tid)
    assert (r["plateau"], r["passed"], r["dsr"]) == ("spike", 0, 0.4)
    assert json.loads(r["reasons_json"]) == ["NOT_STEADY"]
    past_done(led, c, stretch="F-2026-11")
    f = led.claim_fresh(c.id, "F-2026-11", CUT, 30, now=NOW)
    with pytest.raises(LedgerError):
        led.regrade_past(f, plateau="ok", passed=True, reasons=[])


# ------------------------------------------------- the one fresh look, claimed ---
def test_the_fresh_claim_checks_and_inserts_in_one_step(led):
    c = cands(1)[0]
    led.register(c, "one")
    with pytest.raises(FreshRefused) as e:
        led.claim_fresh(c.id, STRETCH, CUT, 30, now=NOW)                      # no past test at all
    assert e.value.code == "NO_PAST_PASS"
    past_done(led, c, passed=0)
    with pytest.raises(FreshRefused) as e:
        led.claim_fresh(c.id, STRETCH, CUT, 30, now=NOW)                      # past test failed
    assert e.value.code == "NO_PAST_PASS"
    assert led.fresh_used(STRETCH) == 0
    pid = led.past_row(c.id, STRETCH)["id"]
    led.execute("UPDATE tests SET passed=1 WHERE id=?", (pid,))               # (scaffolding: force a pass)
    led.regrade_past(pid, plateau="ok", passed=True, reasons=[])
    tid = led.claim_fresh(c.id, STRETCH, CUT, 30, now=NOW)
    assert led.test_row(tid)["status"] == "running" and led.fresh_used(STRETCH) == 1
    with pytest.raises(FreshRefused) as e:
        led.claim_fresh(c.id, STRETCH, CUT, 30, now=NOW)
    assert e.value.code == "ALREADY_LOOKED"


def test_fresh_looks_are_capped_per_stretch(led):
    cs = cands(4)
    for c in cs:
        led.register(c, c.id)
        past_done(led, c)
    for c in cs[:3]:
        led.claim_fresh(c.id, STRETCH, CUT, 3, now=NOW)
    with pytest.raises(FreshRefused) as e:
        led.claim_fresh(cs[3].id, STRETCH, CUT, 3, now=NOW)
    assert e.value.code == "CAP_REACHED" and led.fresh_used(STRETCH) == 3


def test_one_retry_after_too_few_trades_on_a_later_stretch_and_no_more(led):
    c = cands(1)[0]
    led.register(c, "one")
    past_done(led, c)
    f1 = led.claim_fresh(c.id, STRETCH, CUT, 30, now=NOW)
    led.finish_test(f1, {"trades": 12, "net_gbp": 3.0, "passed": 0}, {}, reasons=["TOO_FEW_TRADES"],
                    skipped={}, synthetic=False, seconds=1.0, now=NOW)
    past_done(led, c, stretch="F-2026-11")
    f2 = led.claim_fresh(c.id, "F-2026-11", "2026-10-12", 30, now=NOW)         # the one retry
    assert f2 != f1
    led.finish_test(f2, {"trades": 14, "net_gbp": 2.0, "passed": 0}, {}, reasons=["TOO_FEW_TRADES"],
                    skipped={}, synthetic=False, seconds=1.0, now=NOW)
    past_done(led, c, stretch="F-2026-12")
    with pytest.raises(FreshRefused) as e:
        led.claim_fresh(c.id, "F-2026-12", "2026-11-09", 30, now=NOW)         # never a third
    assert e.value.code == "ALREADY_LOOKED"


def test_a_lost_fresh_look_is_never_retried_and_the_same_stretch_never_twice(led):
    c = cands(1)[0]
    led.register(c, "one")
    past_done(led, c)
    f1 = led.claim_fresh(c.id, STRETCH, CUT, 30, now=NOW)
    led.finish_test(f1, {"trades": 45, "net_gbp": -9.0, "passed": 0}, {}, reasons=["LOST"],
                    skipped={}, synthetic=False, seconds=1.0, now=NOW)
    past_done(led, c, stretch="F-2026-11")
    with pytest.raises(FreshRefused):
        led.claim_fresh(c.id, "F-2026-11", "2026-10-12", 30, now=NOW)
    # TOO_FEW_TRADES in the SAME stretch is not retried either
    d = cands(2)[1]
    led.register(d, "two")
    past_done(led, d)
    g = led.claim_fresh(d.id, STRETCH, CUT, 30, now=NOW)
    led.finish_test(g, {"trades": 5, "net_gbp": 1.0, "passed": 0}, {}, reasons=["TOO_FEW_TRADES"],
                    skipped={}, synthetic=False, seconds=1.0, now=NOW)
    with pytest.raises(FreshRefused):
        led.claim_fresh(d.id, STRETCH, CUT, 30, now=NOW)


def test_a_crashed_fresh_look_still_counts_as_spent(led):
    c = cands(1)[0]
    led.register(c, "one")
    past_done(led, c)
    f = led.claim_fresh(c.id, STRETCH, CUT, 30, now=NOW)
    led.crash_test(f, "window failed", now=NOW)
    assert led.fresh_used(STRETCH) == 1
    with pytest.raises(FreshRefused):
        led.claim_fresh(c.id, STRETCH, CUT, 30, now=NOW)


# --------------------------------- the door also keeps the family-end hold-backs ---
def _refused(led, c, why_contains=""):
    with pytest.raises(FreshRefused) as e:
        led.claim_fresh(c.id, STRETCH, CUT, 30, now=NOW)
    assert e.value.code == "NO_PAST_PASS" and why_contains in str(e.value)
    assert led.fresh_used(STRETCH) == 0


def test_a_past_pass_the_family_gate_has_not_looked_at_cannot_have_a_fresh_look(led):
    """A family cut short by the 06:00 deadline holds provisional passes: no plateau, no overfitting
    check has been applied to them, and each wrongly granted look would spend one of the thirty."""
    c = cands(1)[0]
    led.register(c, "one")
    tid = past_done(led, c, gate=False)
    assert led.test_row(tid)["passed"] == 1 and not led.test_row(tid)["gated"]
    _refused(led, c, "end-of-grid checks")
    led.regrade_past(tid, plateau="ok", passed=True, reasons=[])
    assert led.test_row(tid)["gated"] == 1
    assert led.claim_fresh(c.id, STRETCH, CUT, 30, now=NOW) > 0


@pytest.mark.parametrize("plateau,reasons,why", [
    ("extend", ["EXTEND_GRID"], "edge of the range"),
    (None, ["PLATEAU_PENDING"], "plateau is unknown"),
    ("ok", ["FAMILY_FITS_NOISE"], "FAMILY_FITS_NOISE"),
    ("extend", [], "edge of the range"),                 # the plateau column alone is enough
    ("ok", ["EXTEND_GRID"], "EXTEND_GRID"),              # so is the reason alone
])
def test_the_hold_backs_that_past_verdict_names_close_the_fresh_door(led, plateau, reasons, why):
    c = cands(1)[0]
    led.register(c, "one")
    tid = past_done(led, c, gate=False)
    led.regrade_past(tid, plateau=plateau, passed=True, reasons=reasons)       # passed, but held back
    assert led.test_row(tid)["passed"] == 1
    _refused(led, c, why)


def test_a_family_whose_row_says_it_fits_noise_closes_the_door_for_all_its_members(led):
    cs = cands(2)
    for c in cs:
        led.register(c, c.id)
        past_done(led, c)                                                      # gated, plateau ok
    led.claim_fresh(cs[0].id, STRETCH, CUT, 30, now=NOW)                      # before the family row exists
    led.upsert_family("OPENING_RANGE", STRETCH, n=144, past_passed=9, pbo=0.5, extend_grid=[])
    _refused_c = cs[1]
    with pytest.raises(FreshRefused) as e:
        led.claim_fresh(_refused_c.id, STRETCH, CUT, 30, now=NOW)
    assert e.value.code == "NO_PAST_PASS" and "fitting noise" in str(e.value)
    led.upsert_family("OPENING_RANGE", STRETCH, n=144, past_passed=9, pbo=0.35, extend_grid=[])   # on the line is fine
    led.claim_fresh(_refused_c.id, STRETCH, CUT, 30, now=NOW)
    led.upsert_family("OPENING_RANGE", STRETCH, n=144, past_passed=9, pbo=None, extend_grid=[])    # n/a is fine too


def test_a_regrade_can_only_tighten_never_turn_a_failure_into_a_pass(led):
    c = cands(1)[0]
    led.register(c, "one")
    tid = led.begin_test(c.id, "past", STRETCH, CUT, now=NOW)
    led.finish_test(tid, {"trades": 80, "net_gbp": 5.0, "passed": 0}, {}, reasons=["COSTS"], skipped={},
                    synthetic=True, seconds=0.1, now=NOW)
    led.regrade_past(tid, plateau="ok", passed=True, reasons=[])               # a stale caller says "passed"
    r = led.test_row(tid)
    assert r["passed"] == 0 and json.loads(r["reasons_json"]) == ["COSTS"] and r["gated"] == 1
    _refused(led, c)
    # and tightening still works
    d = cands(2)[1]
    led.register(d, "two")
    past_done(led, d, gate=False)
    led.regrade_past(led.past_row(d.id, STRETCH)["id"], plateau="spike", passed=False, reasons=["NOT_STEADY"])
    assert led.past_row(d.id, STRETCH)["passed"] == 0


def test_a_new_run_of_a_past_test_has_to_be_gated_again(led):
    c = cands(1)[0]
    led.register(c, "one")
    tid = led.begin_test(c.id, "past", STRETCH, CUT, now=NOW)
    led.crash_test(tid, "boom", now=NOW)
    past_done(led, c, gate=False)                                              # revives the same row and finishes it
    assert led.test_row(tid)["gated"] == 0
    led.regrade_past(tid, plateau="ok", passed=True, reasons=[])
    assert led.test_row(tid)["gated"] == 1


def test_the_cap_of_looks_can_be_lowered_but_never_raised_above_thirty(led):
    cs = cands(32)
    for c in cs:
        led.register(c, c.id)
        past_done(led, c)
    for c in cs[:30]:
        led.claim_fresh(c.id, STRETCH, CUT, 500, now=NOW)                      # a "cap of 500" is a cap of 30
    with pytest.raises(FreshRefused) as e:
        led.claim_fresh(cs[30].id, STRETCH, CUT, 500, now=NOW)
    assert e.value.code == "CAP_REACHED" and led.fresh_used(STRETCH) == 30


def test_nothing_in_a_done_fresh_row_can_be_rewritten_not_even_the_columns_that_decide_a_retry(led):
    c = cands(1)[0]
    led.register(c, "one")
    past_done(led, c)
    f = led.claim_fresh(c.id, STRETCH, CUT, 30, now=NOW)
    led.finish_test(f, {"trades": 45, "net_gbp": -9.0, "passed": 0, "pf": 0.8, "p": 0.9}, {"2026-10-01": (-9.0, 45)},
                    reasons=["LOST"], skipped={"wide_spread": 3}, synthetic=True, seconds=1.0,
                    trial={"sharpe": -1.0}, now=NOW)
    row = led.test_row(f)
    assert row["ledger_row_id"]                                                # the one write after the trial ledger answers
    for col, val in (("reasons_json", '["TOO_FEW_TRADES"]'), ("cost15_net", 5.0), ("cost20_net", 5.0),
                     ("best_day_gbp", 1.0), ("folds_up", 4), ("folds_worst", 1.0), ("dsr", 0.9),
                     ("plateau", "ok"), ("seconds", 0.0), ("synthetic", 0), ("skipped_json", "{}"),
                     ("max_dd_r", 0.0), ("stretch_id", "F-2099-01"), ("candidate_id", "lab-ffffffffff"),
                     ("cut_day", "2099-01-01"), ("kind", "past"), ("assumed_share", 0.5), ("gated", 1),
                     ("started_utc", "x"), ("finished_utc", "x"), ("ledger_row_id", 12345)):
        with pytest.raises(sqlite3.DatabaseError, match="fresh result is final"):
            led.execute(f"UPDATE tests SET {col}=? WHERE id=?", (val, f))
    assert led.test_row(f) == row
    # the old way to buy a second look: relabel the first as too few trades
    with pytest.raises(sqlite3.DatabaseError):
        led.execute("UPDATE tests SET reasons_json=? WHERE id=?", ('["TOO_FEW_TRADES"]', f))
    # past rows are not sealed (the family-end gate rewrites them)
    pid = led.past_row(c.id, STRETCH)["id"]
    led.execute("UPDATE tests SET dsr=0.1 WHERE id=?", (pid,))


def test_n_counts_every_row_even_one_whose_idea_was_never_registered(led):
    c = cands(1)[0]
    with pytest.raises(LedgerError, match="register"):
        led.begin_test(c.id, "past", STRETCH, CUT, now=NOW)                    # refused: it could not be filed under a family
    led.register(c, "one")
    past_done(led, c)
    # a stray row (say, written by hand) must still be part of N
    led.execute("INSERT INTO tests(candidate_id, kind, stretch_id, status) VALUES ('lab-ffffffffff','past',?, 'running')",
                (STRETCH,))
    assert led.n_tests() == 2 and led.n_tests(kind="past") == 2 and led.n_tests(stretch_id=STRETCH) == 2
    assert led.n_tests(family="OPENING_RANGE") == 1
    assert led.n_by_family() == {"OPENING_RANGE": 1, "": 1}


def test_a_ledger_file_made_before_the_two_extra_columns_is_migrated(tmp_path):
    path = tmp_path / "old.sqlite"
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE tests(id INTEGER PRIMARY KEY, candidate_id TEXT, kind TEXT, stretch_id TEXT, cut_day TEXT,"
                " status TEXT, started_utc TEXT, finished_utc TEXT, seconds REAL, window_from TEXT, window_to TEXT,"
                " days INT, trades INT, net_gbp REAL, cost_gbp REAL, pf REAL, win_rate REAL, up_days INT, down_days INT,"
                " best_day_gbp REAL, p REAL, folds_up INT, folds_worst REAL, plateau TEXT, cost15_net REAL,"
                " cost20_net REAL, dsr REAL, max_dd_r REAL, passed INT, reasons_json TEXT, skipped_json TEXT,"
                " synthetic INT, ledger_row_id INT, UNIQUE(candidate_id, kind, stretch_id))")
    con.execute("INSERT INTO tests(candidate_id, kind, stretch_id, status, passed) VALUES ('lab-aaaaaaaaaa','past','s','done',1)")
    con.commit()
    con.close()
    old = LabLedger(path)
    cols = [r[1] for r in old.query("PRAGMA table_info(tests)")]
    assert cols[-2:] == ["assumed_share", "gated"]
    assert old.one("SELECT passed, gated FROM tests")[:] == (1, 0)              # the old pass has NOT been gated
    old.close()


# ------------------------------------------------------------ the candidates ---
def test_a_known_id_is_never_registered_again_and_the_binned_stay_binned(led):
    c = cands(1)[0]
    assert led.register(c, "one", now=NOW) is True
    assert led.register(c, "one again", now=NOW) is False
    assert led.candidate(c.id)["stage"] == IDEA and led.candidate(c.id)["name"] == "one"
    led.move(c.id, PAST_TEST, "passed the past test", now=NOW)
    led.move(c.id, BINNED, "failed the fresh test", bin_reason="LOST", now=NOW)
    row = led.candidate(c.id)
    assert (row["stage"], row["bin_stage"], row["bin_reason"]) == (BINNED, PAST_TEST, "LOST")
    assert row["binned_utc"]
    assert led.register(c, "one more time", now=NOW) is False                   # generator skips any id present
    assert c.id in led.known_ids()
    with pytest.raises(LedgerError):
        led.move(c.id, PRACTICE, "no way back", now=NOW)
    ev = led.query("SELECT from_stage, to_stage, by FROM events WHERE candidate_id=? ORDER BY id", (c.id,))
    assert [(e[0], e[1], e[2]) for e in ev] == [(IDEA, PAST_TEST, "rule"), (PAST_TEST, BINNED, "rule")]


def test_reopen_is_claudes_call_and_is_logged(led):
    c = cands(1)[0]
    led.register(c, "one")
    led.move(c.id, BINNED, "tried and failed", now=NOW)
    led.reopen(c.id, "the cost model was wrong for indices", now=NOW)
    row = led.candidate(c.id)
    assert row["stage"] == IDEA and row["retest_due"] == 1
    ev = led.query("SELECT kind, by, why FROM events WHERE candidate_id=? ORDER BY id DESC LIMIT 1", (c.id,))[0]
    assert (ev["kind"], ev["by"]) == ("reopen", "claude") and "cost model" in ev["why"]


def test_retest_marking_touches_only_those_waiting_at_past_test(led):
    cs = cands(4)
    for c in cs:
        led.register(c, c.id)
    led.move(cs[0].id, PAST_TEST, "passed", now=NOW)
    led.move(cs[1].id, PAST_TEST, "passed", now=NOW)
    led.move(cs[1].id, FRESH_TEST, "looking", now=NOW)
    led.move(cs[2].id, BINNED, "failed", now=NOW)
    assert led.mark_retest_due() == 1
    assert led.retest_due_ids() == [cs[0].id]
    led.clear_retest_due(cs[0].id)
    assert led.retest_due_ids() == []


def test_stage_counts_and_family_rows_and_runs(led):
    cs = cands(3)
    for c in cs:
        led.register(c, c.id)
    led.move(cs[0].id, BINNED, "x")
    sc = led.stage_counts()
    assert sc[IDEA] == 2 and sc[BINNED] == 1 and sc[PRACTICE] == 0
    led.upsert_family("OPENING_RANGE", STRETCH, n=144, past_passed=9, pbo=0.4, extend_grid=["range_min"])
    led.upsert_family("OPENING_RANGE", STRETCH, n=144, past_passed=11, pbo=None, extend_grid=[])
    r = led.family_row("OPENING_RANGE", STRETCH)
    assert (r["past_passed"], r["pbo"]) == (11, None)
    led.upsert_run("2026-10-12", started_utc="a", tested=632)
    led.upsert_run("2026-10-12", finished_utc="b", ok=1)
    assert led.one("SELECT tested, finished_utc, ok FROM runs")[:] == (632, "b", 1)
    with pytest.raises(LedgerError):
        led.upsert_run("2026-10-12", nonsense=1)


# ---------------------------------------------------- a night in miniature ---
@pytest.mark.slow
def test_a_night_in_miniature_on_a_simulated_broker_store(tmp_path):
    """SimBroker -> fetch -> universe -> cut -> past tests -> one fresh look, all through the
    real modules: ids stable, N recorded before the work, the cut refuses to go back, the fresh
    look is given once, and SYNTHETIC is stamped on everything from the store to the result."""
    from mintel.broker.sim import SimBroker
    from mintel.lab import LabPaths, LabSettings, stats
    from mintel.lab import candidate as CAND
    from mintel.lab import cut as CUT
    from mintel.lab import fetch, store as STORE
    from mintel.lab.families import CONTROL, get as family

    names = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCHF", "USDCAD", "NZDUSD", "XAUUSD", "GER40"]
    end = dt.datetime(2026, 10, 10, 0, 0, tzinfo=dt.timezone.utc)
    broker = SimBroker(names, seed=5, start=end, history_bars=58 * 1440)
    broker.connect()

    class Feed:
        synthetic = True

        def ping(self):
            return {"ok": True}

        def symbols(self):
            return broker.symbols()

        def spec(self, s):
            return broker.spec(s)

        def bars(self, symbol, tf, count, end=None):
            return broker.bars(symbol, tf, count, end)

    t = [1000.0]
    clock = lambda: t[0]
    def sleep(s):
        t[0] += s

    paths = LabPaths(tmp_path)
    feed = Feed()
    universe, created = STORE.ensure_universe(paths, feed, LabSettings(), now=end)
    assert created and {e.name for e in universe} <= set(names) and len(universe) >= 8
    store = STORE.BarStore(paths)
    rep = fetch.update(feed, store, now=dt.datetime(2026, 10, 10, 9, 0, tzinfo=dt.timezone.utc),
                       budget_s=10_000, pace_s=1.0, sleep=sleep, clock=clock, symbols=[e.name for e in universe],
                       history_days=60, page=3000)
    assert rep.ok and rep.pages > 20
    cov = store.coverage([e.name for e in universe])
    assert cov["ready"] and cov["synthetic"] and cov["complete_days"] >= 40

    # the cut and the grid
    ids = CAND.registered_ids()
    now = dt.datetime(2026, 10, 12, 23, 10, tzinfo=dt.timezone.utc)
    led = LabLedger(paths.sqlite, paths.trials)
    days = store.trading_days([e.name for e in universe])
    stretch, moved = CUT.ensure_cut(paths, days, ids, now, ledger=led)
    assert moved and stretch.cut_day == days[-CUT.fresh_days_for(len(days))]
    assert len(ids) == 632 and ids == [c.id for c in CAND.registered()]                    # stable
    with pytest.raises(CUT.CutError):
        CUT.set_cut(paths, days[0], grid_ids=ids, now=now, ledger=led)                        # never backwards

    # a handful of candidates through the real past test
    fam = family(CONTROL)
    chosen = fam.grid()[:8] + family("OPENING_RANGE").grid()[:4]
    win = store.past_window(universe)
    assert win.synthetic and win.cut_day == stretch.cut_day and all(d < stretch.cut_day for d in win.days)
    verdicts = {}
    for c in chosen:
        led.register(c, CAND.name_of(c), now=now)
        tid = led.begin_test(c.id, "past", stretch.stretch_id, stretch.cut_day, now=now)
        assert led.test_row(tid)["status"] == "running"                                         # before the run
        res = sim.run(c, win)
        assert res.synthetic is True
        s = stats.summarize(res, n_trials=led.n_tests())
        v = stats.past_verdict(s, "na")
        led.finish_test(tid, s.to_cols(), res.daily, reasons=v.reasons, skipped=res.skipped,
                        synthetic=res.synthetic, seconds=res.seconds, trial=s.trial_extra(), now=now)
        verdicts[c.id] = (tid, v, res)
    assert led.n_tests() == len(chosen) and led.n_tests(kind="past") == len(chosen)
    assert TrialLedger(paths.trials).count("lab:CONTROL_TIME_OF_DAY") == 8
    assert all(r["synthetic"] == 1 for r in led.query("SELECT synthetic FROM tests"))
    # (the simulated broker's own price process drifts in "trend" blocks, so a clock trade can
    # genuinely earn on it; the driftless control test is in test_lab_stats.py)
    assert all(set(v.states()) == set(stats.CHECK_KEYS) for _t, v, _r in verdicts.values())
    assert sum(len(res.trades) for _t, _v, res in verdicts.values()) > 100

    # give one candidate a past pass (whatever the made-up prices said) to exercise the single door
    cid = chosen[0].id
    led.execute("UPDATE tests SET passed=1 WHERE id=?", (verdicts[cid][0],))      # (scaffolding)
    led.regrade_past(verdicts[cid][0], plateau="na", passed=True, reasons=[])
    fwin = STORE.fresh_once(cid, led, stretch, store, universe, now=now)
    assert fwin.kind == "fresh" and fwin.synthetic and fwin.days and fwin.days[0] >= stretch.cut_day
    assert led.test_row(fwin.test_id)["status"] == "running"
    fres = sim.run(chosen[0], fwin)
    fs = stats.summarize(fres, kind="fresh")
    fv = stats.fresh_verdict(fs)
    led.finish_test(fwin.test_id, fs.to_cols(), fres.daily, reasons=fv.reasons, skipped=fres.skipped,
                    synthetic=fres.synthetic, seconds=fres.seconds, trial=fs.trial_extra(), now=now)
    assert fres.synthetic and led.fresh_used(stretch.stretch_id) == 1
    with pytest.raises(FreshRefused):
        STORE.fresh_once(cid, led, stretch, store, universe, now=now)
    assert [r["is_oos"] for r in TrialLedger(paths.trials).all(f"lab:{chosen[0].family}")][-1] == 1
    synced = CUT.sync_fresh_used(paths, led)
    assert synced.fresh_used == 1
    led.close()
