"""The coach's decisions (spec sections 6, 9, 12 and 21): every row of the
moves up (G1, G2, G3) and down (D1-D5, S1, S2, K1), C2 and the Runner rule,
Formula 1's judgement (fail -> practice, keep -> base next day, jump only with
Aaron's approval), the hysteresis, and section 21's demo ladder.

EVERY TRADE IN THIS FILE IS MADE UP (tests/coach_d_world.py): synthetic
records for the tests, never a result.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
from types import SimpleNamespace

import pytest

from mintel.coach import (BASE_STEP, F1_KEY, STATE_BANNED_REAL, STATE_LIVE, STATE_LIVE_TRIAL, STATE_PINNED,
                          STATE_PLACED, STATE_PRACTICE, UTC, Gates, Judgement, Policy)
from mintel.coach import decide as D
from mintel.coach import envelope as envelope_mod
from mintel.coach import policy as P
from mintel.coach.brakes import BrakeState
from tests.coach_d_world import (EQ, GOOD_KEY, MINOR_KEY, NOW, cfg, env, seg, series, trade, trading_days, winner,
                                 world)

RELEASE = dt.datetime(2026, 11, 11, 6, 30, tzinfo=UTC)          # Wednesday 06:30 UK (GMT)


def plan_keys(plan):
    return [(i["rule"], i["key"]) for i in plan.items]


def released(segs, plan, w_at, approvals=()):
    row = {"date": plan.date, "plan": plan.as_dict(), "released_utc": None}
    moves, vetoed, _ = D.release(segs, row, w_at, list(approvals))
    return D.apply(segs, [m for m in moves if m.rule not in ("VETO", "RELEASE_SKIP")], w_at.now), moves


def validate(w, ents, judged=None, demo=None):
    """The policy these entries make passes the gate's own validator."""
    e = w.env
    obj = e.to_dict()
    sha = envelope_mod.envelope_sha(obj)
    pol = Policy(seq=5, written_utc=w.now.isoformat(), envelope_sha=sha, mode="PILOT", grants_allowed=True,
                 entries=ents, account={"equity": EQ}, gates=Gates(), judged=judged or {}, demo=demo)
    ctx = P.CheckContext.from_config(w.cfg, e, sha, now=w.now)
    sealed = P.seal(pol)
    return P.validate(P.parse_bytes(P.encode(sealed)), ctx)


# ===================================================================== G1 --
def test_g1_a_proven_key_is_planned_for_the_morning_never_granted_tonight():
    segs = {GOOD_KEY: seg(GOOD_KEY)}
    w = world(winner(), segs)
    downs, plan = D.nightly(segs, w)
    assert plan_keys(plan) == [("G1", GOOD_KEY)]
    assert segs[GOOD_KEY].state == STATE_PRACTICE                      # nothing changes tonight
    item = plan.items[0]
    assert item["item"] == "1" and "unless you veto it" in item["words"]
    assert item["numbers"]["money_gbp"] <= 8.0 and item["numbers"]["risk_pct"] == 0.35
    # before 06:30: nothing; at 06:30 on the next UK trading day: the trial starts
    early = world(winner(), segs, now=RELEASE - dt.timedelta(minutes=1))
    assert released(segs, plan, early)[0] == []
    assert plan.release_utc == RELEASE.isoformat()
    done, _ = released(segs, plan, world(winner(), segs, now=RELEASE))
    assert [m.rule for m in done] == ["G1"] and segs[GOOD_KEY].state == STATE_LIVE_TRIAL
    ents = D.entries(segs, world(winner(), segs, now=RELEASE))
    assert ents[GOOD_KEY].route == "REAL" and ents[GOOD_KEY].money_gbp <= 8.0
    validate(world(winner(), segs, now=RELEASE), ents)


def test_g1_one_command_vetoes_it():
    segs = {GOOD_KEY: seg(GOOD_KEY)}
    _, plan = D.nightly(segs, world(winner(), segs))
    done, moves = released(segs, plan, world(winner(), segs, now=RELEASE),
                           approvals=[{"id": "1", "action": "veto", "plan": plan.date}])
    assert done == [] and segs[GOOD_KEY].state == STATE_PRACTICE
    assert [m.rule for m in moves] == ["VETO"]


def test_g1_a_trial_on_the_demo_starts_at_base():
    segs = {GOOD_KEY: seg(GOOD_KEY)}
    _, plan = D.nightly(segs, world(winner(), segs, demo=True))
    assert plan.items[0]["numbers"]["money_gbp"] == 13.0
    released(segs, plan, world(winner(), segs, now=RELEASE, demo=True))
    w = world(winner(), segs, now=RELEASE, demo=True)
    ents = D.entries(segs, w)
    assert ents[GOOD_KEY].money_gbp == 13.0
    validate(w, ents, demo=D.demo_limits(segs, w, ents))
    # the demo test fails on the next tick: back to the v1 trial at once
    w1 = world(winner(), segs, now=RELEASE, demo=False)
    assert D.entries(segs, w1)[GOOD_KEY].money_gbp <= 8.0


@pytest.mark.parametrize("case", ["c2_minor_pair", "runner_rides_it", "unclear", "cooloff", "luck", "day_brake",
                                  "trial_budget", "slots_full", "guard", "data", "practice_only_market"])
def test_g1_refused(case):
    key = GOOD_KEY
    c = cfg()
    segs = {}
    kw = {}
    luck = None
    if case == "c2_minor_pair":
        key = MINOR_KEY
    elif case == "runner_rides_it":
        c.runner.mode = "LIVE"
    elif case == "unclear":
        key = "tnb|SESSION_EXPANSION|INDEX|UNCLEAR"
    elif case == "cooloff":
        segs[key] = seg(key, cooloff_until=(NOW.date() + dt.timedelta(days=3)).isoformat())
    elif case == "luck":
        luck = SimpleNamespace(blocked=True, passed=9, tested=200, rate=0.045, numbers={"rate": 0.045})
    elif case == "day_brake":
        kw["brakes"] = BrakeState(day="LINE")
    elif case == "trial_budget":
        kw["brakes"] = BrakeState(trial_losses_gbp=40.0, trial_budget_gbp=40.0)
    elif case == "slots_full":
        for k in ("tnb|A1|INDEX|TREND", "tnb|A2|INDEX|TREND"):
            segs[k] = seg(k, STATE_LIVE_TRIAL, "trial")
    elif case == "guard":
        kw["mode"] = "GUARD"
    elif case == "data":
        kw["data_ok"] = False
    segs.setdefault(key, seg(key))
    trades = winner(key=key)
    w = world(trades, segs, c=c, **kw)
    blocked = ["US500", "JP225", "UK100", "DE40", "AUS200"] if case == "practice_only_market" else []
    _, plan = D.nightly(segs, w, luck=luck, blocked_symbols=blocked)
    assert ("G1", key) not in plan_keys(plan), case
    if case == "luck":
        assert any("luck check" in n for n in plan.notes)
    if case == "c2_minor_pair":                       # after Formula 1's judgement it may be granted
        judged = {F1_KEY: Judgement("KEEP", 20, NOW.isoformat())}
        _, plan2 = D.nightly(segs, world(trades, segs, judged=judged))
        assert ("G1", key) in plan_keys(plan2)


def test_g1_ties_are_broken_by_the_lower_bound_and_one_new_trial_a_night():
    k2 = "tnb|BREAKOUT_ACCEPTANCE|INDEX|TREND"
    segs = {GOOD_KEY: seg(GOOD_KEY), k2: seg(k2)}
    trades = winner() + winner(key=k2, pattern=(1.5, 1.3, -0.2))
    _, plan = D.nightly(segs, world(trades, segs))
    assert plan_keys(plan) == [("G1", k2)]                              # the higher LCB80, alone


# ===================================================================== G2 --
def _trial_world(xs, days=8, *, failures=0, epoch=""):
    since = dt.datetime.combine(trading_days(NOW.date() - dt.timedelta(days=1), days)[0], dt.time(0), tzinfo=UTC)
    segs = {GOOD_KEY: seg(GOOD_KEY, STATE_LIVE_TRIAL, "trial", since=since, g2_failures=failures, epoch=epoch)}
    trades = winner(days=20, end=since.date() - dt.timedelta(days=1)) + \
        series(GOOD_KEY, xs, trading_days(NOW.date() - dt.timedelta(days=1), days), risk=6.5)
    return segs, world(trades, segs)


def test_g2_a_trial_that_passes_moves_to_base_at_the_release():
    segs, w = _trial_world((1.1, -0.4, 0.9))
    _, plan = D.nightly(segs, w)
    assert plan_keys(plan) == [("G2", GOOD_KEY)]
    released(segs, plan, dataclasses.replace(w, now=RELEASE) if False else world(w.trades, segs, now=RELEASE))
    assert segs[GOOD_KEY].state == STATE_LIVE and segs[GOOD_KEY].step == BASE_STEP


def test_g2_a_trial_that_fails_goes_back_to_practice_at_once_with_a_cooloff_then_is_banned():
    segs, w = _trial_world((-1.0, 0.3, -0.4))
    downs, plan = D.nightly(segs, w)
    assert [m.rule for m in downs] == ["G2_FAIL"] and plan.items == []
    D.apply(segs, downs, w.now)
    s = segs[GOOD_KEY]
    assert s.state == STATE_PRACTICE and s.g2_failures == 1 and s.cooloff_until > NOW.date().isoformat()
    from mintel.coach import epochs as E
    ep = E.latest_epoch(GOOD_KEY, NOW).isoformat()
    segs2, w2 = _trial_world((-1.0, 0.3, -0.4), failures=1, epoch=ep)
    downs2, _ = D.nightly(segs2, w2)
    assert [m.rule for m in downs2] == ["G2_BAN"]
    D.apply(segs2, downs2, w2.now)
    assert segs2[GOOD_KEY].state == STATE_BANNED_REAL


# ===================================================================== G3 --
def _live(pattern=(-0.3, 1.2, 1.4), days=10, step=BASE_STEP, extra=(), **kw):
    since = dt.datetime.combine(trading_days(NOW.date() - dt.timedelta(days=1), days)[0], dt.time(0), tzinfo=UTC)
    segs = {GOOD_KEY: seg(GOOD_KEY, STATE_LIVE, step, since=since)}
    trades = winner(days=20, end=since.date() - dt.timedelta(days=1)) + \
        series(GOOD_KEY, pattern, trading_days(NOW.date() - dt.timedelta(days=1), days)) + list(extra)
    return segs, trades


def test_g3_a_step_is_announced_tonight_and_takes_effect_only_at_the_release():
    segs, trades = _live()
    w = world(trades, segs)
    _, plan = D.nightly(segs, w)
    assert plan_keys(plan) == [("G3", GOOD_KEY)]
    it = plan.items[0]
    assert it["to_step"] == "S1" and "Announced tonight" in it["words"] and it["numbers"]["kelly_fraction"] == 0.25
    assert it["numbers"]["stop_share"] <= 0.02
    assert segs[GOOD_KEY].step == BASE_STEP
    released(segs, plan, world(trades, segs, now=RELEASE))
    assert segs[GOOD_KEY].step == "S1"
    w2 = world(trades, segs, now=RELEASE)
    ents = D.entries(segs, w2)
    assert ents[GOOD_KEY].risk_pct == 0.85 and ents[GOOD_KEY].money_gbp == pytest.approx(15.85, abs=0.01)
    validate(w2, ents)


def test_g3_never_grows_right_after_a_loss_new_high_only():
    segs, trades = _live(extra=[trade(GOOD_KEY, -0.8, NOW - dt.timedelta(hours=2))])
    _, plan = D.nightly(segs, world(trades, segs))
    assert plan.items == []


@pytest.mark.parametrize("case", ["few_trades", "too_soon", "hold", "brake", "kelly", "week_cap"])
def test_g3_refused(case):
    kw = {}
    days = 10
    pattern = (-0.3, 1.2, 1.4)
    changes = ()
    if case == "few_trades":
        pattern = (1.4,)
    if case == "too_soon":
        days = 4
        pattern = (-0.3, 1.2, 1.1, 1.0, 1.0, 1.4)
    segs, trades = _live(pattern=pattern, days=days)
    if case == "hold":
        segs[GOOD_KEY].hold_until = (NOW.date() + dt.timedelta(days=2)).isoformat()
    if case == "brake":
        kw["brakes"] = BrakeState(week="LINE")
    if case == "week_cap":
        changes = [SimpleNamespace(rule="G3", ts_utc=(NOW - dt.timedelta(days=1)).isoformat()) for _ in range(2)]
    w = world(trades, segs, **kw)
    if case == "kelly":
        real = w.est(GOOD_KEY)
        w.est = lambda k: dataclasses.replace(real, lcb80_real=0.01)       # a thin lower bound: k% under 0.85
    _, plan = D.nightly(segs, w, changes=changes)
    assert plan.items == [], case


def test_g3_at_most_one_step_a_night():
    k2 = "tnb|BREAKOUT_ACCEPTANCE|INDEX|TREND"
    segs, trades = _live()
    since = segs[GOOD_KEY].since_utc
    segs[k2] = dataclasses.replace(segs[GOOD_KEY], key=k2)
    trades += [dataclasses.replace(t, approach="BREAKOUT_ACCEPTANCE", ticket=t.ticket + 50_000_000) for t in trades]
    _, plan = D.nightly(segs, world(trades, segs))
    assert len(plan.items) == 1 and plan.items[0]["rule"] == "G3"


def test_the_demo_ladder_continues_past_s4_with_half_kelly_only_on_the_demo():
    segs, trades = _live(step="S4")
    _, plan = D.nightly(segs, world(trades, segs))
    assert plan.items == []                                             # S4 is the top with real money
    _, plan = D.nightly(segs, world(trades, segs, demo=True))
    assert plan_keys(plan) == [("G3", GOOD_KEY)]
    assert plan.items[0]["to_step"] == "S5" and plan.items[0]["numbers"]["kelly_fraction"] == 0.5
    # the demo test fails before the release: the step does not happen
    done, moves = released(segs, plan, world(trades, segs, now=RELEASE, demo=False))
    assert done == [] and "real-money limits" in moves[0].words
    released(segs, plan, world(trades, segs, now=RELEASE, demo=True))
    assert segs[GOOD_KEY].step == "S5"
    wd = world(trades, segs, now=RELEASE, demo=True)
    ents = D.entries(segs, wd)
    assert ents[GOOD_KEY].risk_pct == 2.0
    validate(wd, ents, demo=D.demo_limits(segs, wd, ents))
    # on a real-money account the same key is sized at the v1 ceiling at once
    wr = world(trades, segs, now=RELEASE, demo=False)
    er = D.entries(segs, wr)
    assert er[GOOD_KEY].risk_pct == 1.5 and D.demo_limits(segs, wr, er) is None
    validate(wr, er)


def test_the_ladder_never_skips_a_step():
    s = D.Sizes.of(cfg(), env())
    assert s.ladder(False) == ("base", "S1", "S2", "S3", "S4")
    assert s.ladder(True) == ("base", "S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9")
    for demo in (False, True):
        lad = s.ladder(demo)
        for a, b in zip(lad, lad[1:]):
            assert s.next_step(a, demo) == b
    assert s.next_step("S4", False) is None and s.next_step("S9", True) is None
    for step in s.ladder(True):                                       # v1 sizes never above config's 1.5%
        p, m = s.size(step, EQ, False)
        assert p <= 1.5 and m <= EQ * 1.5 / 100
        p, m = s.size(step, EQ, True)
        assert p <= 5.0


# ===================================================================== downs --
def _governed(pattern, days=6, state=STATE_LIVE, step="S2", **kw):
    since = dt.datetime.combine(trading_days(NOW.date() - dt.timedelta(days=1), days)[0], dt.time(0), tzinfo=UTC)
    segs = {GOOD_KEY: seg(GOOD_KEY, state, step, since=since)}
    trades = series(GOOD_KEY, pattern, trading_days(NOW.date() - dt.timedelta(days=1), days), **kw)
    return segs, trades


def test_d1_cusum_sends_a_losing_live_key_back_to_practice():
    segs, trades = _governed((-0.1,) * 5, days=20, step=BASE_STEP)
    moves, _ = D.tick(segs, world(trades, segs))
    m = [m for m in moves if m.key == GOOD_KEY and m.rule != "S1"]       # Aaron's S1 also fires on it
    assert [x.rule for x in m] == ["D1"]
    assert m[0].to_state == STATE_PRACTICE and m[0].cooloff and m[0].numbers["cusum"] >= 12
    assert m[0].kind == "judgement"


def test_d1_is_a_judgement_and_needs_three_uk_days():
    segs, trades = _governed((-0.5,) * 12, days=2, step=BASE_STEP)
    moves, _ = D.tick(segs, world(trades, segs))
    assert not [m for m in moves if m.rule == "D1"]
    assert [m.kind for m in moves if m.rule == "D4"] == ["risk limit"]       # a risk limit still applies


def test_d2_a_gap_pauses_the_key_for_the_rest_of_the_uk_day():
    segs, trades = _governed((0.5, 0.4), days=4)
    trades.append(trade(GOOD_KEY, -2.7, NOW - dt.timedelta(minutes=30)))
    w = world(trades, segs)
    moves, _ = D.tick(segs, w)
    d2 = [m for m in moves if m.rule == "D2"]
    assert d2 and d2[0].pause_until == "2026-11-11T00:00:00+00:00" and d2[0].kind == "risk limit"
    D.apply(segs, d2, w.now)
    ents = D.entries(segs, w)
    assert ents[GOOD_KEY].route == "PRACTICE" and ents[GOOD_KEY].state == "PAUSED"
    tomorrow = world(trades, segs, now=dt.datetime(2026, 11, 11, 7, 0, tzinfo=UTC))
    assert D.entries(segs, tomorrow)[GOOD_KEY].route == "REAL"


@pytest.mark.parametrize("p,step,want", [(0.7, "S2", ("LIVE", "S1")), (0.55, "S3", ("LIVE", "base")),
                                         (0.4, "S1", ("PRACTICE", None)), (0.85, "S2", None)])
def test_d3_nightly_on_the_real_only_chance(p, step, want):
    segs, trades = _governed((0.2, -0.1, 0.1), days=6, step=step)
    w = world(trades, segs)
    real = w.est(GOOD_KEY)
    w.est = lambda k: dataclasses.replace(real, p_plus_real=p, n_real_eff=25.0)
    downs, _ = D.nightly(segs, w)
    got = [(m.to_state, m.to_step) for m in downs if m.rule == "D3"]
    assert got == ([want] if want else [])


def test_d4_the_keys_drawdown_steps_it_down_then_to_practice():
    segs, trades = _governed((1.0, 1.0, 1.0), days=3)
    trades += series(GOOD_KEY, (-1.0,) * 10, [NOW.date()], first_hour=0, gap_minutes=50)      # 10 R down
    moves, _ = D.tick(segs, world(trades, segs))
    d4 = [m for m in moves if m.key == GOOD_KEY and m.rule != "S1"]
    assert [m.rule for m in d4] == ["D4"]
    assert d4[0].to_state == STATE_LIVE and d4[0].to_step == "S1" and d4[0].hold and d4[0].kind == "risk limit"
    segs, trades = _governed((1.0,), days=1)
    trades += series(GOOD_KEY, (-1.0,) * 14, [NOW.date()], first_hour=0, gap_minutes=40)      # 14 R down
    moves, _ = D.tick(segs, world(trades, segs))
    assert [(m.rule, m.to_state) for m in moves if m.key == GOOD_KEY] == [("D4", STATE_PRACTICE)]


def test_d5_a_trial_that_has_lost_30_pounds_goes_back_to_practice():
    segs, trades = _governed((-1.0, 0.2), days=4, state=STATE_LIVE_TRIAL, step="trial", risk=6.5)
    trades += series(GOOD_KEY, (-1.0, -1.0, -1.0), [NOW.date()], first_hour=1, gap_minutes=60, risk=6.5)
    moves, _ = D.tick(segs, world(trades, segs))
    d5 = [m for m in moves if m.rule == "D5"]
    assert d5 and d5[0].to_state == STATE_PRACTICE and d5[0].numbers["trial_net_gbp"] <= -30


def test_s1_bans_an_approach_in_a_condition_negative_over_three_days_and_never_touches_the_pin():
    k = "tnb|SESSION_EXPANSION|INDEX|TREND"
    k_gold = "tnb|SESSION_EXPANSION|GOLD|TREND"
    segs = {k: seg(k, STATE_LIVE, "base"), k_gold: seg(k_gold), F1_KEY: seg(F1_KEY, STATE_PINNED)}
    trades = series(k, (-0.5, 0.2), trading_days(NOW.date() - dt.timedelta(days=1), 3))
    trades += series("tnb|MOMENTUM_CONTINUATION|FX_MINOR|HIGH_VOL", (-0.5, 0.2),
                     trading_days(NOW.date() - dt.timedelta(days=1), 3))
    moves, needs = D.tick(segs, world(trades, segs))
    banned = {m.key for m in moves if m.rule == "S1"}
    assert banned == {k, k_gold}
    m = [m for m in moves if m.key == k][0]
    assert m.to_state == STATE_BANNED_REAL and "three-days rule" in m.words


def test_s2_a_market_negative_on_two_thirds_of_six_days_is_practice_only():
    trades = series(GOOD_KEY, (-0.5,), trading_days(NOW.date() - dt.timedelta(days=1), 4), symbol="US500")
    trades += series(GOOD_KEY, (0.5,), trading_days(NOW.date() - dt.timedelta(days=7), 2), symbol="US500")
    w = world(trades, {})
    res = D.s2_blocks(w)
    assert [r.target for r in res] == ["US500"]
    b = D.blocks(res)
    assert b[0].symbols == ("US500",) and b[0].route == "PRACTICE" and b[0].rule == "S2"


def test_k1_a_placed_bot_judged_back_to_paper_takes_no_new_real_entries():
    c = cfg()
    c.runner.mode = "LIVE"
    key = "runner|*|*|*"
    segs = {key: seg(key, STATE_PLACED)}
    trades = []
    for d in trading_days(NOW.date() - dt.timedelta(days=1), 10):
        for i, x in enumerate((-1.0, -0.8, 0.3)):
            trades.append(trade("runner|MC|INDEX|TREND", x, dt.datetime(d.year, d.month, d.day, 14 + i, tzinfo=UTC)))
    w = world(trades, segs, c=c)
    moves, _ = D.tick(segs, w)
    k1 = [m for m in moves if m.rule == "K1"]
    assert k1 and "your call" in k1[0].words
    D.apply(segs, k1, w.now)
    ents = D.entries(segs, w)
    assert ents[key].route == "SKIP"
    # 29 trades: not judged yet - placed, Aaron's settings
    segs2 = {key: seg(key, STATE_PLACED)}
    moves2, _ = D.tick(segs2, world(trades[:29], segs2, c=c))
    assert not [m for m in moves2 if m.rule == "K1"]
    assert D.entries(segs2, world(trades[:29], segs2, c=c))[key].route == "BASELINE"


# ============================================================== Formula 1 --
def _f1(xs, n=20):
    c = cfg()
    c.tnb.live_groups = ("FX_MINOR",)
    c.tnb.live_tactics = ("MOMENTUM_CONTINUATION",)
    c.tnb.live_since_utc = "2026-10-12T07:58:00+00:00"
    days = trading_days(NOW.date() - dt.timedelta(days=1), n // 2)
    trades = series("tnb|MOMENTUM_CONTINUATION|FX_MINOR|HIGH_VOL", xs, days, risk=13.0)[:n]
    segs = {F1_KEY: seg(F1_KEY, STATE_PINNED)}
    return c, segs, trades


def test_formula1_is_left_alone_until_its_twentieth_live_trade():
    c, segs, trades = _f1((-1.0, -1.0), n=18)
    w = world(trades, segs, c=c)
    moves, needs = D.tick(segs, w)
    assert not [m for m in moves if m.key == F1_KEY]
    assert any("your call" in n for n in needs)                         # shadow only: "Needs you"
    _, plan = D.nightly(segs, w)
    assert plan.items == [] and any("20 live trades (2 to go)" in n for n in plan.notes)
    assert D.entries(segs, w)[F1_KEY].route == "PIN"


def test_formula1_judged_keep_is_live_at_base_from_the_next_release():
    c, segs, trades = _f1((1.2, -0.4))
    _, plan = D.nightly(segs, world(trades, segs, c=c))
    assert plan_keys(plan) == [("F1_KEEP", F1_KEY)]
    assert segs[F1_KEY].state == STATE_PINNED                         # not tonight
    done, _ = released(segs, plan, world(trades, segs, c=c, now=RELEASE))
    judged = {F1_KEY: Judgement(done[0].numbers["verdict"], done[0].numbers["live_trades"],
                                done[0].numbers["judged_utc"])}
    assert segs[F1_KEY].state == STATE_LIVE and segs[F1_KEY].step == BASE_STEP
    w = world(trades, segs, c=c, now=RELEASE, judged=judged)
    ents = D.entries(segs, w)
    assert ents[F1_KEY].route == "REAL" and ents[F1_KEY].money_gbp == 13.0
    validate(w, ents, judged=judged)
    # the jump to the top size only with Aaron's approval
    _, plan2 = D.nightly(segs, w)
    assert ("F1_JUMP", F1_KEY) not in plan_keys(plan2)
    _, plan3 = D.nightly(segs, w, approvals=[{"id": "f1-jump", "action": "approve", "plan": ""}])
    assert ("F1_JUMP", F1_KEY) in plan_keys(plan3)


def test_formula1_judged_back_to_practice():
    c, segs, trades = _f1((-1.0, 0.4))
    _, plan = D.nightly(segs, world(trades, segs, c=c))
    assert plan_keys(plan) == [("F1_FAIL", F1_KEY)]
    done, _ = released(segs, plan, world(trades, segs, c=c, now=RELEASE))
    judged = {F1_KEY: Judgement("PRACTICE", 20, done[0].numbers["judged_utc"])}
    assert segs[F1_KEY].state == STATE_PRACTICE
    ents = D.entries(segs, world(trades, segs, c=c, now=RELEASE, judged=judged))
    assert ents[F1_KEY].route == "PRACTICE"                           # it is real on Aaron's settings: sent to paper


# ============================================================== hysteresis --
def test_a_key_dropped_to_practice_waits_ten_uk_trading_days():
    segs, trades = _governed((-0.6, 0.1, -0.5, -0.4), days=6)
    w = world(trades, segs)
    moves, _ = D.tick(segs, w)
    D.apply(segs, moves, w.now)
    assert segs[GOOD_KEY].cooloff_until == D.add_trading_days(NOW.date(), 10).isoformat()
    later = winner()
    _, plan = D.nightly(segs, world(later, segs))
    assert ("G1", GOOD_KEY) not in plan_keys(plan)


def test_a_step_down_cannot_be_reversed_for_five_uk_trading_days():
    m = D.Move(GOOD_KEY, D.DOWN, "D4", STATE_LIVE, "S1", hold=True, words="made-up step down for the test")
    segs = {GOOD_KEY: seg(GOOD_KEY, STATE_LIVE, "S2")}
    D.apply(segs, [m], NOW)
    assert segs[GOOD_KEY].hold_until == D.add_trading_days(NOW.date(), 5).isoformat()


# ============================================================== the demo stop --
def test_the_demo_stop_is_only_ever_lifted_one_level_by_a_plan_on_the_demo(tmp_path):
    c = cfg()
    assert D.stop_level({}, c, True) == 3.0
    segs = {D.STOP_KEY: seg(D.STOP_KEY, "LIFTED", "4")}
    assert D.stop_level(segs, c, True) == 4.0
    assert D.stop_level(segs, c, False) == 3.0                        # a real-money account: config.json's
    # with nothing proven nothing is planned
    s, trades = _live()
    w = world(trades, s, demo=True)
    _, plan = D.nightly(s, w)
    assert not [i for i in plan.items if i["rule"] == "STOP_UP"]


def _blocked_day(data, day, tactic="SESSION_EXPANSION", regime="TREND"):
    """A made-up decisions row: the daily-loss stop blocked an entry of a proven key."""
    import sqlite3
    from tests.coach_d_world import write_rows
    write_rows(data, [])
    con = sqlite3.connect(data / "journal.sqlite")
    con.execute("INSERT INTO decisions (as_of_utc, symbol, direction, tactic, regime, opportunity, tier, acted, "
                "blockers, state_json) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (dt.datetime(day.year, day.month, day.day, 15, 0, tzinfo=UTC).isoformat(), "US500", 1, tactic, regime,
                 80.0, "NORMAL", 0, "DAILY_LOSS: made up for the test", "{}"))
    con.commit()
    con.close()


def test_the_demo_stop_goes_up_one_level_when_proven_keys_were_blocked_on_a_good_day(tmp_path):
    k2 = "tnb|BREAKOUT_ACCEPTANCE|INDEX|TREND"
    segs, trades = _live()
    segs[k2] = dataclasses.replace(segs[GOOD_KEY], key=k2)
    trades += [dataclasses.replace(t, approach="BREAKOUT_ACCEPTANCE", ticket=t.ticket + 70_000_000) for t in trades]
    data = tmp_path / "data"
    data.mkdir()
    _blocked_day(data, NOW.date() - dt.timedelta(days=1))
    w = world(trades, segs, demo=True, data_dir=str(data))
    _, plan = D.nightly(segs, w)
    stop = [i for i in plan.items if i["rule"] == "STOP_UP"]
    assert len(stop) == 1 and stop[0]["to_step"] == "4" and stop[0]["numbers"]["from_pct"] == 3.0
    assert stop[0]["numbers"]["trip_share"] <= 0.05 and "unless you veto it" in stop[0]["words"]
    # never on a real-money account; never without a blocked good day
    _, plan_r = D.nightly(segs, world(trades, segs, demo=False, data_dir=str(data)))
    assert not [i for i in plan_r.items if i["rule"] == "STOP_UP"]
    empty = tmp_path / "empty"
    empty.mkdir()
    _, plan_e = D.nightly(segs, world(trades, segs, demo=True, data_dir=str(empty)))
    assert not [i for i in plan_e.items if i["rule"] == "STOP_UP"]
    # released: the lease carries the lifted stop; the brakes follow it
    D.apply(segs, [D.Move.from_dict(stop[0])], RELEASE)
    assert D.stop_level(segs, cfg(), True) == 4.0
    wd = world(trades, segs, now=RELEASE, demo=True, stop=4.0)
    ents = D.entries(segs, wd)
    demo = D.demo_limits(segs, wd, ents)
    assert demo.max_daily_loss_pct == 4.0 and demo.max_drawdown_pct is None    # the breaker waits for a 6% stop
    validate(wd, ents, demo=demo)
