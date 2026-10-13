"""The lab's stages and moves (spec section 6), row by row.

Aaron, 10 Oct 2026: "ensure that consistency stays, luck doesn't". Every move up
to READY FOR LIVE, and every bin, is the rule's; these tests pin the rule:
every registered idea is tried, a failed check bins it with the first failed
check as the reason, the family's end-of-grid checks gate the fresh look, the
fresh look is ranked by the PAST test and capped, a lost fresh look is final,
too few trades get one retry on a later stretch, the bench is ordered by the
past test alone (shuffling fresh results never moves it), at most two of a
family practise, the two practice looks happen on their stored dates only,
the -15 R kill, "trades too rarely", and a binned idea is never regenerated.

EVERY FIGURE HERE IS MADE-UP TEST DATA (hand-written results and made-up prices).
"""
from __future__ import annotations

import datetime as dt
import json
import random

import pytest

from mintel.lab import (BINNED, FRESH_TEST, IDEA, ON_THE_BENCH, PAST_TEST, PRACTICE, READY_FOR_LIVE,
                        Result, Trade, WindowInfo, synth)
from mintel.lab import stages as ST
from mintel.lab import stats
from mintel.lab.candidate import registered, registered_ids, waiting_for_calendar
from mintel.lab.cut import CutState, grid_hash
from mintel.lab.families import get as family
from mintel.lab.ledger import FreshRefused, LabLedger
from mintel.lab import LabSettings

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 12, 22, 10, tzinfo=UTC)          # Monday 23:10 in London
CUT = "2026-09-14"


def stretch(sid="F-2026-10", cut=CUT, cap=30, awake=False):
    return CutState(cut_day=cut, stretch_id=sid, set_utc="2026-10-01T22:10:00+00:00",
                    grid_hash=grid_hash(registered_ids(awake)), fresh_cap=cap, news_awake=awake)


@pytest.fixture
def led(tmp_path):
    ledger = LabLedger(tmp_path / "lab" / "lab.sqlite", tmp_path / "research_trials.sqlite")
    yield ledger
    ledger.close()


def grid_of(code, **want):
    out = []
    for c in family(code).grid():
        if all((getattr(c, k, None) == v) or (c.p(k) == v) for k, v in want.items()):
            out.append(c)
    return out


DAYS = synth.weekdays("2026-07-01", 40)


def good_cols(net=200.0, trades=80, days=40, pf=1.6, p=0.01):
    """A past result that passes every past check by itself (MADE UP)."""
    return {"window_from": DAYS[0], "window_to": DAYS[days - 1], "days": days, "trades": trades,
            "net_gbp": net, "cost_gbp": 40.0, "pf": pf, "win_rate": 0.55, "up_days": 26, "down_days": 12,
            "best_day_gbp": net * 0.1, "p": p, "folds_up": 4, "folds_worst": net * 0.1,
            "plateau": None, "cost15_net": net * 0.8, "cost20_net": net * 0.6, "dsr": 0.0,
            "max_dd_r": 3.0, "assumed_share": 0.0, "passed": 1}


def bad_cols(net=-50.0):
    c = good_cols(net=net)
    c.update(pf=0.8, up_days=10, down_days=25, folds_up=1, folds_worst=-60.0, cost15_net=-80.0, passed=0)
    return c


def daily_for(net, days=40):
    return {DAYS[k]: (net / days, 2) for k in range(days)}


def past_done(led, c, cols, *, sid="F-2026-10", stage=None, reasons=()):
    if led.candidate(c.id) is None:
        led.register(c, family(c.family).name(c), now=NOW)
    if stage:
        led.execute("UPDATE candidates SET stage=? WHERE id=?", (stage, c.id))
    tid = led.begin_test(c.id, "past", sid, CUT, now=NOW)
    led.finish_test(tid, cols, daily_for(cols["net_gbp"], cols["days"]), reasons=list(reasons), skipped={},
                    synthetic=True, seconds=0.1, trial={"sharpe": 1.0}, now=NOW)
    return tid


def gate(led, c, sid="F-2026-10", plateau="ok", dsr=0.0):
    row = led.past_row(c.id, sid)
    led.regrade_past(row["id"], plateau=plateau, passed=True, reasons=[], dsr=dsr)


def result(cid, *, per_day, days=25, trades_a_day=2, cost=0.5):
    """A MADE-UP fresh result: ``per_day`` pounds every day."""
    ds = synth.weekdays(CUT, days)
    trades = []
    for d in ds:
        for k in range(trades_a_day):
            net = per_day / trades_a_day
            trades.append(Trade("EURUSD", 1, 0.1, 1.1, 1.0, None, f"{d}T09:0{k}:00+00:00",
                                f"{d}T10:0{k}:00+00:00", 1.1, "TIME", net + cost / 2, cost, net, net / 13.0,
                                60, False, "bar", d))
    return Result(cid, WindowInfo("fresh", ds[0], ds[-1], days, CUT, "F-2026-10", 1), trades,
                  {d: (per_day, trades_a_day) for d in ds}, {}, True, 0.01)


class FakeWindow:
    def __init__(self, test_id):
        self.test_id = test_id
        self.synthetic = True


@pytest.fixture
def fresh_world(monkeypatch):
    """fresh_once goes through the ledger's real claim (the single door), but hands back a
    stand-in window; the simulator answers from a table of MADE-UP results."""
    outcomes: dict = {}
    calls: list = []

    def fake_fresh_once(cid, ledger, st, store, universe, *, calendar=None, now=None):
        tid = ledger.claim_fresh(cid, st.stretch_id, st.cut_day, st.fresh_cap, now=now)
        calls.append(cid)
        return FakeWindow(tid)

    def fake_run(c, window, family=None):
        return outcomes[c.id](c.id)

    monkeypatch.setattr(ST, "fresh_once", fake_fresh_once)
    monkeypatch.setattr(ST.sim, "run", fake_run)
    return outcomes, calls


# ------------------------------------------------------------ IDEA -> PAST TEST ---
def test_every_registered_idea_is_due_and_committed_before_anything_runs(led):
    st = stretch()
    binned = registered()[5]
    led.register(binned, "x", now=NOW)
    led.move(binned.id, BINNED, "made-up bin", now=NOW)
    gen = ST.generate(led, st, covered_past_days=0, now=NOW)
    ids = registered_ids()
    assert len(ids) == 632 and gen.registered == 631                    # NEWS_MOVE asleep; the binned id known
    avoid = [c for c in registered() if waiting_for_calendar(c, 0)]
    assert sorted(gen.waiting) == sorted(c.id for c in avoid if c.id != binned.id)
    # every due idea has its running row committed now: N is known before a simulation starts
    begun = led.query("SELECT candidate_id, status FROM tests WHERE kind='past'")
    assert len(begun) == len(gen.begun) == 632 - 1 - len(gen.waiting)
    assert {r["status"] for r in begun} == {"running"}
    assert binned.id not in {r["candidate_id"] for r in begun}           # BINNED never regenerated
    assert led.n_tests() == len(begun)
    # idempotent: the same step again adds nothing and changes nothing
    again = ST.generate(led, st, covered_past_days=0, now=NOW)
    assert again.registered == 0 and led.n_tests() == len(begun)


def test_a_limit_commits_only_that_many_and_the_rest_stay_ideas(led):
    gen = ST.generate(led, stretch(), covered_past_days=40, now=NOW, limit=7)
    assert len(gen.begun) == 7 and gen.due > 7
    assert led.n_tests() == 7
    pending = ST.pending_past(led, stretch())
    assert [r["family"] for _t, r in pending] == ["OPENING_RANGE"] * 7      # family order, then grid order


def test_a_retest_due_idea_is_run_again_on_the_new_stretch(led):
    c = grid_of("OPENING_RANGE", markets="FX_MAJOR", news="IGNORE")[0]
    past_done(led, c, good_cols(), sid="F-2026-09")
    led.execute("UPDATE candidates SET stage=?, retest_due=1 WHERE id=?", (PAST_TEST, c.id))
    due, _w = ST.due_past(led, stretch(), covered_past_days=40)
    assert c.id in [r["id"] for r in due]


def test_a_failed_past_check_bins_with_the_first_failed_check(led):
    """A real simulation on made-up flat prices: no trades, so Enough trades fails first."""
    c = grid_of("CONTROL_TIME_OF_DAY", markets="FX_MAJOR")[0]
    led.register(c, "made up", now=NOW)
    st = stretch()
    tid = led.begin_test(c.id, "past", st.stretch_id, st.cut_day, now=NOW)
    days = synth.weekdays("2026-07-01", 3)
    ent = synth.entry_for("EURUSD")
    cols = [[], [], [], [], [], [], []]
    for d in days:
        for col, more in zip(cols, synth.flat_columns(d)):
            col.extend(more)
    win = synth.window_of([synth.series_from_columns(ent, cols)], days)
    v = ST.run_past(led, led.candidate(c.id), tid, win, st, now=NOW)
    assert v is not None and not v.passed and v.first_failed == "trades"
    row = led.candidate(c.id)
    assert row["stage"] == BINNED and row["bin_stage"] == PAST_TEST
    assert row["bin_reason"].startswith("Failed the past test (Enough trades)")
    assert led.test_row(tid)["status"] == "done"
    moves = [(e["from_stage"], e["to_stage"]) for e in led.query("SELECT * FROM events WHERE kind='stage'")]
    assert moves == [(IDEA, PAST_TEST), (PAST_TEST, BINNED)]


def test_a_crash_leaves_the_row_and_twice_bins_it(led, monkeypatch):
    c = grid_of("CONTROL_TIME_OF_DAY")[0]
    led.register(c, "made up", now=NOW)
    st = stretch()
    monkeypatch.setattr(ST.sim, "run", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")))
    for k in (1, 2):
        tid = led.begin_test(c.id, "past", st.stretch_id, st.cut_day, now=NOW)
        assert ST.run_past(led, led.candidate(c.id), tid, None, st, now=NOW + dt.timedelta(minutes=k)) is None
        assert led.test_row(tid)["status"] == "crashed"
    assert led.n_tests() == 1                                       # revived, never duplicated
    assert led.candidate(c.id)["stage"] == BINNED


# ----------------------------------------------------- the family's end-of-grid ---
def test_a_past_pass_waits_until_its_family_is_gated(led):
    a, b = grid_of("OPENING_RANGE", markets="FX_MAJOR", news="IGNORE", exit="t2", stop="ATR",
                   session="LONDON")[:2]
    assert a.p("range_min") != b.p("range_min")                     # siblings on the plateau axis
    past_done(led, a, good_cols(net=200.0), stage=PAST_TEST, reasons=["PLATEAU_PENDING"])
    past_done(led, b, good_cols(net=120.0), stage=PAST_TEST, reasons=["PLATEAU_PENDING"])
    st = stretch()
    assert ST.fresh_ranked(led, st) == []                           # not gated yet: the door stays shut
    with pytest.raises(FreshRefused):
        led.claim_fresh(a.id, st.stretch_id, st.cut_day, 30, now=NOW)
    out = ST.family_end(led, "OPENING_RANGE", st, now=NOW)
    assert out["passed"] == 2 and out["pbo"] is None                # n/a: fewer than 8 members
    ranked = ST.fresh_ranked(led, st)
    assert {r["candidate_id"] for r in ranked} == {a.id, b.id}
    assert led.past_row(a.id, st.stretch_id)["plateau"] == "ok"
    assert led.family_row("OPENING_RANGE", st.stretch_id)["past_passed"] == 2


def test_a_spike_is_binned_at_the_family_end(led):
    a, b = grid_of("OPENING_RANGE", markets="FX_MAJOR", news="IGNORE", exit="t2", stop="ATR",
                   session="LONDON")[:2]
    past_done(led, a, good_cols(net=200.0), stage=PAST_TEST)
    past_done(led, b, bad_cols(net=-40.0), stage=BINNED)            # its neighbour lost
    ST.family_end(led, "OPENING_RANGE", stretch(), now=NOW)
    row = led.candidate(a.id)
    assert row["stage"] == BINNED and "spike" in row["bin_reason"]
    assert ST.fresh_ranked(led, stretch()) == []


def test_a_missing_neighbour_holds_the_pass_back(led):
    a = grid_of("OPENING_RANGE", markets="FX_MAJOR", news="IGNORE", exit="t2", stop="ATR",
                session="LONDON")[0]
    past_done(led, a, good_cols(), stage=PAST_TEST)
    ST.family_end(led, "OPENING_RANGE", stretch(), now=NOW)        # no window: the neighbour cannot be run
    row = led.past_row(a.id, "F-2026-10")
    assert row["plateau"] is None and "PLATEAU_PENDING" in json.loads(row["reasons_json"])
    assert ST.fresh_ranked(led, stretch()) == [] and led.candidate(a.id)["stage"] == PAST_TEST


def test_a_family_fitting_noise_forwards_nothing(led):
    c = grid_of("PREV_DAY_LEVELS", markets="FX_MAJOR")[0]          # no numeric setting: plateau 'na'
    past_done(led, c, good_cols(), stage=PAST_TEST)
    gate(led, c, plateau="na")
    assert [r["candidate_id"] for r in ST.fresh_ranked(led, stretch())] == [c.id]
    led.upsert_family("PREV_DAY_LEVELS", "F-2026-10", n=40, past_passed=1, pbo=0.6, extend_grid=[])
    assert ST.fresh_ranked(led, stretch()) == []


def test_the_control_familys_pass_rates_are_kept(led):
    for c in grid_of("CONTROL_TIME_OF_DAY")[:4]:
        past_done(led, c, bad_cols(), stage=BINNED)
    ST.family_end(led, "CONTROL_TIME_OF_DAY", stretch(), now=NOW)
    ctl = json.loads(led.family_row("CONTROL_TIME_OF_DAY", "F-2026-10")["control_pass_json"])
    assert ctl["n"] == 4 and ctl["past"] == 0 and ctl["past_pass_rate"] == 0.0


# ------------------------------------------------------------ FRESH TEST ---
def _ready_for_fresh(led, codes_dsr):
    out = []
    pool = grid_of("PREV_DAY_LEVELS") + grid_of("CONTROL_TIME_OF_DAY")
    for c, (dsr, p) in zip(pool, codes_dsr):
        past_done(led, c, good_cols(p=p), stage=PAST_TEST)
        gate(led, c, plateau="na", dsr=dsr)
        out.append(c)
    return out


def test_fresh_looks_ranked_by_past_dsr_then_p_three_a_night(led, fresh_world):
    outcomes, calls = fresh_world
    cs = _ready_for_fresh(led, [(0.2, 0.01), (0.9, 0.04), (0.5, 0.02), (0.5, 0.01), (0.1, 0.001)])
    for c in cs:
        outcomes[c.id] = lambda cid: result(cid, per_day=6.0)
    out = ST.fresh_looks(led, stretch(), None, [], LabSettings(), now=NOW)
    assert calls == [cs[1].id, cs[3].id, cs[2].id]                  # DSR first, then the past p
    assert [o["outcome"] for o in out] == ["PASS"] * 3
    assert all(led.candidate(c)["stage"] == ON_THE_BENCH for c in calls)
    assert led.candidate(cs[0].id)["stage"] == PAST_TEST           # waits for another night
    moves = [(e["from_stage"], e["to_stage"]) for e in led.query(
        "SELECT * FROM events WHERE candidate_id=? AND kind='stage' ORDER BY id", (cs[1].id,))]
    assert moves[-2:] == [(PAST_TEST, FRESH_TEST), (FRESH_TEST, ON_THE_BENCH)]


def test_the_stretch_cap_is_never_exceeded(led, fresh_world):
    outcomes, calls = fresh_world
    cs = _ready_for_fresh(led, [(0.5, 0.01)] * 4)
    for c in cs:
        outcomes[c.id] = lambda cid: result(cid, per_day=6.0)
    ST.fresh_looks(led, stretch(cap=2), None, [], LabSettings(), now=NOW)
    assert len(calls) == 2 and led.fresh_used("F-2026-10") == 2
    ST.fresh_looks(led, stretch(cap=2), None, [], LabSettings(), now=NOW)
    assert len(calls) == 2                                          # none left this stretch
    lowered = LabSettings.from_dict({"fresh_cap_per_stretch": 1})
    assert ST.fresh_looks(led, stretch(cap=30), None, [], lowered, now=NOW) == []


def test_a_lost_fresh_look_is_final(led, fresh_world):
    outcomes, calls = fresh_world
    (c,) = _ready_for_fresh(led, [(0.5, 0.01)])
    outcomes[c.id] = lambda cid: result(cid, per_day=-6.0)
    out = ST.fresh_looks(led, stretch(), None, [], LabSettings(), now=NOW)
    assert out[0]["outcome"] == "LOST"
    row = led.candidate(c.id)
    assert row["stage"] == BINNED and row["bin_stage"] == FRESH_TEST
    assert row["bin_reason"].startswith("Failed the fresh test")
    # never retried, on this stretch or a later one
    assert ST.retry_too_few(led, stretch(sid="F-2026-11"), now=NOW) == []
    assert led.candidate(c.id)["stage"] == BINNED


def test_too_few_trades_gets_one_more_go_on_a_later_stretch_and_no_more(led, fresh_world):
    outcomes, calls = fresh_world
    (c,) = _ready_for_fresh(led, [(0.5, 0.01)])
    outcomes[c.id] = lambda cid: result(cid, per_day=6.0, days=5)   # 10 trades
    out = ST.fresh_looks(led, stretch(), None, [], LabSettings(), now=NOW)
    assert out[0]["outcome"] == "TOO_FEW_TRADES"
    row = led.candidate(c.id)
    assert row["stage"] == BINNED and "one more go" in row["bin_reason"]
    assert ST.retry_too_few(led, stretch(), now=NOW) == []          # never on the same stretch
    later = stretch(sid="F-2026-11")
    assert ST.retry_too_few(led, later, now=NOW) == [c.id]
    row = led.candidate(c.id)
    assert row["stage"] == PAST_TEST and row["retest_due"] == 1
    # its retest on the new stretch passes, it is gated, and it has its second look
    past_done(led, c, good_cols(), sid="F-2026-11")
    gate(led, c, sid="F-2026-11", plateau="na")
    out = ST.fresh_looks(led, later, None, [], LabSettings(), now=NOW)
    assert out[0]["outcome"] == "TOO_FEW_TRADES" and len(led.fresh_rows(c.id)) == 2
    assert "binned for good" in led.candidate(c.id)["bin_reason"]
    assert ST.retry_too_few(led, stretch(sid="F-2026-12"), now=NOW) == []


def test_binned_ideas_are_never_regenerated(led):
    c = registered()[0]
    led.register(c, "x", now=NOW)
    led.move(c.id, BINNED, "made-up bin", now=NOW)
    assert led.register(c, "x", now=NOW) is False
    ST.generate(led, stretch(), covered_past_days=40, now=NOW)
    assert led.candidate(c.id)["stage"] == BINNED
    assert led.query("SELECT * FROM tests WHERE candidate_id=?", (c.id,)) == []


# ------------------------------------------------------------- THE BENCH ---
def _bench(led, cs_dsr_p, *, fresh_nets=None, since=NOW):
    for k, (c, dsr, p) in enumerate(cs_dsr_p):
        past_done(led, c, good_cols(p=p), stage=PAST_TEST)
        gate(led, c, plateau="na", dsr=dsr)
        tid = led.claim_fresh(c.id, "F-2026-10", CUT, 30, now=NOW)
        net = (fresh_nets or {}).get(c.id, 50.0)
        led.finish_test(tid, {"trades": 40, "net_gbp": net, "passed": 1, "p": abs(net) / 1e5, "pf": 1.5,
                              "dsr": net}, {}, reasons=[], skipped={}, synthetic=True, seconds=0.1, now=NOW)
        led.move(c.id, ON_THE_BENCH, "made-up pass", now=since)


@pytest.mark.parametrize("seed", range(6))
def test_the_bench_order_never_uses_fresh_results(tmp_path, seed):
    """Property: shuffling the fresh results among the bench never changes its order."""
    rnd = random.Random(seed)
    cs = (grid_of("PREV_DAY_LEVELS") + grid_of("CONTROL_TIME_OF_DAY"))[:7]
    past = [(c, round(rnd.random(), 3), round(rnd.random() * 0.05, 4)) for c in cs]
    nets = [round(rnd.uniform(-100, 300), 2) for _ in cs]
    orders = []
    for k in range(4):
        shuffled = nets[:]
        rnd.shuffle(shuffled)
        led = LabLedger(tmp_path / f"s{k}" / "lab.sqlite")
        _bench(led, past, fresh_nets={c.id: n for c, n in zip(cs, shuffled)})
        orders.append(ST.bench_order(led))
        led.close()
    assert all(o == orders[0] for o in orders)
    want = [c.id for c, _d, _p in sorted(past, key=lambda x: (-x[1], x[2], x[0].id))]
    assert orders[0] == want                                        # past DSR, then past p, then id


def test_bench_promotions_fill_free_places_at_most_two_a_family(led):
    ors = grid_of("PREV_DAY_LEVELS", markets="FX_MAJOR")[:5]
    ctl = grid_of("CONTROL_TIME_OF_DAY", markets="FX_MINOR")[:3]
    _bench(led, [(c, 0.9 - 0.01 * k, 0.01) for k, c in enumerate(ors)] +
           [(c, 0.5 - 0.01 * k, 0.01) for k, c in enumerate(ctl)])
    uni = synth.universe(24)
    # 13 Oct: practise_all (the default) gives every benched idea a place; the old six places and
    # two a family hold only with it off, which is what this test pins
    old = LabSettings(practise_all=False)
    out = ST.grade_practice(led, uni, old, now=NOW)
    assert out["promoted"] == [ors[0].id, ors[1].id, ctl[0].id, ctl[1].id]   # two of each family, in order
    places = ST.roster(led)
    assert [p["slot"] for p in places] == [1, 2, 3, 4]
    syms = {s for p in places for s in json.loads(p["symbols_json"])}
    assert len(syms) <= ST.MAX_PRACTICE_SYMBOLS
    looks = {r["look"]: r["due_day"] for r in led.query("SELECT * FROM looks WHERE candidate_id=?", (ors[0].id,))}
    assert looks == {1: "2026-10-26", 2: "2026-11-09"}             # the 10th and 20th UK trading days
    assert led.candidate(ors[0].id)["stage"] == PRACTICE
    # idempotent: the same night again gives no place twice
    again = ST.grade_practice(led, uni, old, now=NOW)
    assert again["promoted"] == [] and len(ST.roster(led)) == 4


def test_with_practise_all_every_benched_idea_gets_its_place_and_look_days(led):
    ors = grid_of("PREV_DAY_LEVELS", markets="FX_MAJOR")[:5]
    ctl = grid_of("CONTROL_TIME_OF_DAY", markets="FX_MINOR")[:3]
    _bench(led, [(c, 0.9 - 0.01 * k, 0.01) for k, c in enumerate(ors)] +
           [(c, 0.5 - 0.01 * k, 0.01) for k, c in enumerate(ctl)])
    settings = LabSettings.from_dict({"practice_slots": 2})          # the slot count no longer limits it
    assert settings.practise_all is True
    out = ST.grade_practice(led, synth.universe(24), settings, now=NOW)
    assert sorted(out["promoted"]) == sorted(c.id for c in ors + ctl)  # no six places, no two a family
    assert [p["slot"] for p in ST.roster(led)] == list(range(1, 9))
    assert all(led.candidate(c.id)["stage"] == PRACTICE for c in ors + ctl)
    assert ST.grade_practice(led, synth.universe(24), settings, now=NOW)["promoted"] == []   # idempotent


def test_sixty_days_on_the_bench_without_a_place_bins_it(led):
    c = grid_of("PREV_DAY_LEVELS")[0]
    _bench(led, [(c, 0.5, 0.01)], since=NOW - dt.timedelta(days=61))
    ST.grade_practice(led, [], LabSettings.from_dict({"practice_slots": 0}), now=NOW)
    row = led.candidate(c.id)
    assert row["stage"] == BINNED and row["bin_stage"] == ON_THE_BENCH and "60 days" in row["bin_reason"]


# ---------------------------------------------------------------- PRACTICE ---
def _practising(led, c):
    _bench(led, [(c, 0.5, 0.01)])
    ST.grade_practice(led, synth.universe(24), LabSettings(), now=NOW)
    assert led.candidate(c.id)["stage"] == PRACTICE


TICKET = [970000001]


def add_trades(led, cid, day, nets, *, r=None):
    for k, net in enumerate(nets):
        TICKET[0] += 1
        led.execute(
            "INSERT INTO practice_trades(ticket, candidate_id, symbol, side, volume, entry, stop, target,"
            " opened_utc, closed_utc, exit_price, exit_reason, gross_gbp, commission_gbp, spread_points,"
            " slippage_points, net_gbp, risk_gbp, r, fill_source) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (TICKET[0], cid, "EURUSD", 1, 0.1, 1.1, 1.09, None, f"{day}T09:{k:02d}:00+00:00",
             f"{day}T10:{k:02d}:00+00:00", 1.101, "TIME", net + 0.2, 0.2, 1.0, 1.0, net, 13.0,
             (net / 13.0) if r is None else r, "tick"))


def night(day):
    return dt.datetime.fromisoformat(f"{day}T22:10:00+00:00")


PRACTICE_DAYS = ST.weekdays_between("2026-10-13", "2026-11-13")


def test_look_one_on_its_day_passes_to_ready_and_never_before(led):
    c = grid_of("PREV_DAY_LEVELS")[0]
    _practising(led, c)
    for d in PRACTICE_DAYS[:9]:
        add_trades(led, c.id, d, [3.0, 3.0, 2.0, -1.0])
        out = ST.grade_practice(led, [], LabSettings(), now=night(d))
        assert out["looks"] == [], d                                 # nothing is judged before its day
    add_trades(led, c.id, PRACTICE_DAYS[9], [3.0, 3.0, 2.0, -1.0])
    out = ST.grade_practice(led, [], LabSettings(), now=night(PRACTICE_DAYS[9]))
    assert PRACTICE_DAYS[9] == "2026-10-26" and out["looks"] == [{"id": c.id, "look": 1, "passed": True}]
    assert led.candidate(c.id)["stage"] == READY_FOR_LIVE
    look = led.one("SELECT * FROM looks WHERE candidate_id=? AND look=1", (c.id,))
    assert look["trades"] == 40 and look["passed"] == 1
    # it keeps practising (its place is kept) and the practice rules can still bin it
    assert [p["candidate_id"] for p in ST.roster(led)] == [c.id]


def test_look_one_with_a_loss_bins_it(led):
    c = grid_of("PREV_DAY_LEVELS")[0]
    _practising(led, c)
    for d in PRACTICE_DAYS[:10]:
        add_trades(led, c.id, d, [-1.0, 1.0, -1.0, 0.5])
    ST.grade_practice(led, [], LabSettings(), now=night(PRACTICE_DAYS[9]))
    row = led.candidate(c.id)
    assert row["stage"] == BINNED and row["bin_stage"] == PRACTICE and ST.roster(led) == []


def test_look_one_waits_for_thirty_trades_then_look_two_says_trades_too_rarely(led):
    c = grid_of("PREV_DAY_LEVELS")[0]
    _practising(led, c)
    for k, d in enumerate(PRACTICE_DAYS[:20]):
        add_trades(led, c.id, d, [2.0] if k % 2 == 0 else [])
        out = ST.grade_practice(led, [], LabSettings(), now=night(d))
        if k < 19:
            assert out["looks"] == [] and led.candidate(c.id)["stage"] == PRACTICE, d
    assert PRACTICE_DAYS[19] == "2026-11-09"
    row = led.candidate(c.id)
    assert row["stage"] == BINNED and "trades too rarely" in row["bin_reason"]
    looks = {r["look"]: r for r in led.query("SELECT * FROM looks WHERE candidate_id=?", (c.id,))}
    assert looks[1]["passed"] is None and looks[1]["done_utc"] and looks[2]["passed"] == 0


def test_a_deferred_look_one_comes_on_the_first_day_with_thirty(led):
    c = grid_of("PREV_DAY_LEVELS")[0]
    _practising(led, c)
    judged = None
    for k, d in enumerate(PRACTICE_DAYS[:16]):
        add_trades(led, c.id, d, [2.0, 1.5])                         # 2 a day: 30 on day 15
        out = ST.grade_practice(led, [], LabSettings(), now=night(d))
        if out["looks"]:
            judged = (k + 1, out["looks"])
            break
    assert judged == (15, [{"id": c.id, "look": 1, "passed": True}])


def test_the_minus_fifteen_r_kill(led):
    c = grid_of("PREV_DAY_LEVELS")[0]
    _practising(led, c)
    add_trades(led, c.id, PRACTICE_DAYS[0], [-13.0] * 15)            # exactly 15 R down: not over
    ST.grade_practice(led, [], LabSettings(), now=night(PRACTICE_DAYS[0]))
    assert led.candidate(c.id)["stage"] == PRACTICE
    add_trades(led, c.id, PRACTICE_DAYS[1], [-2.0])                   # over 15 R: binned at once
    out = ST.grade_practice(led, [], LabSettings(), now=night(PRACTICE_DAYS[1]))
    row = led.candidate(c.id)
    assert row["stage"] == BINNED and "15 R" in row["bin_reason"] and "£195" in row["bin_reason"]
    assert out["binned"][0]["id"] == c.id and ST.roster(led) == []


def test_look_one_loss_and_day_twenty_short_both_bin_and_look_two_can_pass(led):
    c, d2 = grid_of("PREV_DAY_LEVELS")[:2]
    d3 = grid_of("CONTROL_TIME_OF_DAY")[0]                            # (at most two of a family practise)
    for x in (c, d2, d3):
        _practising(led, x)
    for k, d in enumerate(PRACTICE_DAYS[:20]):
        add_trades(led, c.id, d, [2.0, -2.5])                         # 40 trades, losing
        add_trades(led, d2.id, d, [2.0, 1.0] if k >= 10 else [])       # 20 trades after day 10: look 1 never comes
        add_trades(led, d3.id, d, [2.0] if k < 19 else [2.0] * 11)    # 19 trades by day 19, 30 on day 20
        ST.grade_practice(led, [], LabSettings(), now=night(d))
    assert led.candidate(c.id)["stage"] == BINNED                     # look 1 (net <= 0)
    assert led.candidate(d2.id)["stage"] == BINNED                    # day 20 with 20 trades
    assert led.candidate(d3.id)["stage"] == READY_FOR_LIVE            # look 2, every check passed
    looks = {r["look"]: r for r in led.query("SELECT * FROM looks WHERE candidate_id=?", (d3.id,))}
    assert looks[1]["passed"] is None and looks[2]["passed"] == 1 and looks[2]["trades"] == 30


def test_judging_day_is_the_last_finished_uk_trading_day():
    assert ST.night_of(dt.datetime(2026, 10, 12, 22, 10, tzinfo=UTC)) == "2026-10-12"
    assert ST.night_of(dt.datetime(2026, 10, 13, 0, 40, tzinfo=UTC)) == "2026-10-12"   # 01:40 BST: Monday's night
    assert ST.judge_day(dt.datetime(2026, 10, 17, 22, 10, tzinfo=UTC)) == "2026-10-16"   # Saturday: Friday
    assert ST.look_days("2026-10-12T22:10:00+00:00") == ("2026-10-26", "2026-11-09")
    assert ST.practice_start("2026-10-16T22:10:00+00:00") == "2026-10-19"                # Friday night: Monday


def test_practice_symbols_keep_the_roster_within_the_cap():
    uni = synth.universe(24)
    # 13 Oct: the roster's market cap is now every market the lab stores (24), no longer 12 - each
    # idea is simulated on its own. The cap itself still works, shown here at the old 12.
    assert ST.MAX_PRACTICE_SYMBOLS == 24
    taken = set([e.name for e in uni if e.kind == "FX_MINOR"][:8])
    assert len(ST.practice_symbols(uni, "FX_MAJOR", taken)) == 7      # every major: room for all of them
    got = ST.practice_symbols(uni, "FX_MAJOR", taken, cap=12)
    assert len(got) == 4 and len(taken | set(got)) == 12            # room for 4 of the 7 majors
    full = set([e.name for e in uni if e.kind == "FX_MINOR"][:12])
    assert ST.practice_symbols(uni, "FX_MAJOR", full, cap=12) == []   # no room: it waits on the bench
    assert ST.practice_symbols(uni, "GOLD", set()) == ["XAUUSD"]
    assert ST.practice_symbols(uni, "INDEX", {f"S{k}" for k in range(12)}, cap=12) == []
    assert ST.practice_symbols(uni, "INDEX", {f"S{k}" for k in range(24)}) == []


def test_stats_for_a_practice_stint_count_quiet_days_as_zero(led):
    c = grid_of("PREV_DAY_LEVELS")[0]
    _practising(led, c)
    add_trades(led, c.id, PRACTICE_DAYS[0], [5.0])
    s, trades, days = ST.practice_stats(led, c.id, ST.roster(led)[0]["since_utc"], PRACTICE_DAYS[4])
    assert s.days == 5 and s.trades == 1 and s.up_days == 1 and s.down_days == 0 and len(days) == 5
    assert s.cost15_net == pytest.approx(5.0 - 0.5 * 0.2)              # commission only (no universe given)
    assert stats.practice_verdict(s, at_look=False).check("trades").state == "Running"


def test_no_rule_moves_an_idea_to_real_money():
    """READY FOR LIVE is the lab's last word: LIVE TRIAL and PROVEN are Aaron's (spec section 6)."""
    import inspect
    import re as _re
    src = inspect.getsource(ST)
    moves = _re.findall(r"ledger\.move\(\s*[^,]+,\s*([A-Z_]+)", src)
    assert moves and set(moves) <= {"PAST_TEST", "BINNED", "FRESH_TEST", "ON_THE_BENCH", "PRACTICE",
                                    "READY_FOR_LIVE"}
    code = src.split('"""', 2)[2]                                      # after the module's docstring
    assert "LIVE_TRIAL" not in code and not _re.search(r"\bPROVEN\b", code)
