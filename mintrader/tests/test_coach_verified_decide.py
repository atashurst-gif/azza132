"""The verifiers' findings on the coach's decisions and run loop, fixed (12 Oct), each pinned by a test that
fails without its fix. Every record here is made up (coach_d_world's helpers); none is a result."""
from __future__ import annotations

import datetime as dt

import pytest

from mintel.coach import Entry, Paths
from mintel.coach import decide as D
from mintel.coach import records as R
from mintel.coach.ledger import Segment
from tests.coach_d_world import EQ, NOW, cfg, env, seg, trade, world

UTC = dt.timezone.utc


# ============================================== 5: the scorecard's "saved" --
def test_the_scorecard_saves_nothing_on_a_trade_that_was_practice_on_aarons_settings(tmp_path):
    """Through the real trader and gate: a PRACTICE entry on a key that is practice on Aaron's own settings
    (FX_MAJOR is not a live group) saves nothing when its paper trade loses; one on a key that was real on his
    settings (FX_MINOR, a live group) is money saved."""
    from tests.test_coach_hooks import act_on, build, full_policy
    from tests.test_coach_policy import put_policy
    from tests.test_tnb_paper_wiring import entry

    def coach(c, sim):
        put_policy(c, full_policy(c, sim), now=sim.now)
    sim, c, pb, tr = build(tmp_path, prepare=coach)
    act_on(tr, sim, entry(sim, "EURUSD", 0.0025, tactic="BREAKOUT_ACCEPTANCE"))    # practice on Aaron's settings
    act_on(tr, sim, entry(sim, "EURJPY", 0.300, tactic="TREND_PULLBACK"))          # real on Aaron's settings
    gate = R.read_gate_log(c.ops.data_dir)
    by_symbol = {line["symbol"]: (key, line) for key, line in gate.items()}
    assert by_symbol["EURUSD"][1]["route"] == "PRACTICE" and by_symbol["EURUSD"][1]["static_real"] is False
    assert by_symbol["EURJPY"][1]["route"] == "PRACTICE" and by_symbol["EURJPY"][1]["static_real"] is True
    trades = [trade("tnb|BREAKOUT_ACCEPTANCE|FX_MAJOR|TREND", -1.0, NOW, real=False,
                    ticket=by_symbol["EURUSD"][0][1]),
              trade("tnb|TREND_PULLBACK|FX_MINOR|TREND", -1.0, NOW, real=False, ticket=by_symbol["EURJPY"][0][1])]
    sizes = D.Sizes.of(cfg(), env())
    sc = D.scorecard(trades[:1], gate, sizes)
    assert sc["saved_gbp"] == 0.0 and sc["net_gbp"] == 0.0
    sc = D.scorecard(trades, gate, sizes)
    assert sc["saved_gbp"] == pytest.approx(sizes.base_money)
    tr.journal.close()
    pb.close_db()


def test_a_fill_line_that_does_not_say_static_real_saves_nothing():
    sizes = D.Sizes.of(cfg(), env())
    t = trade("tnb|BREAKOUT_ACCEPTANCE|FX_MAJOR|TREND", -1.0, NOW, real=False)
    assert D.scorecard([t], {("tnb", t.ticket): {"route": "PRACTICE"}}, sizes)["saved_gbp"] == 0.0


# ============================== 11 and 17: Aaron's veto, over a weekend and without a plan date --
def test_a_friday_veto_holds_through_the_weekends_plans(tmp_path):
    """The nightly runs every day: Saturday's and Sunday's plans re-propose Friday's vetoed trial for the same
    Monday 06:30 release. The veto holds."""
    from mintel.coach import __main__ as CLI
    from mintel.coach.run import Coach, nightly_instant
    from tests.coach_d_world import (GOOD_KEY, OTHER_KEY, FakePage, data_folder, journal_row, trading_days,
                                     write_rows)
    data, c, _ = data_folder(tmp_path)
    box = {"now": dt.datetime(2026, 10, 12, 9, 0, tzinfo=UTC)}
    page = FakePage()
    co = Coach(data, cfg=c, clock=lambda: box["now"], fetch=page, shuffles=4)
    write_rows(data, [journal_row(GOOD_KEY, 0.1, dt.datetime(2026, 10, 9, 9, 0, tzinfo=UTC))])
    co.tick()
    rows = []
    fri = dt.date(2026, 11, 13)
    for d in trading_days(fri, 17):
        for i, x in enumerate((1.4, 1.2, -0.3)):
            rows.append(journal_row(GOOD_KEY, x, dt.datetime(d.year, d.month, d.day, 9 + 2 * i, 0, tzinfo=UTC),
                                    symbol=("US500", "JP225", "UK100")[i]))
        for i, x in enumerate((-1.0, -0.8, -0.9)):
            rows.append(journal_row(OTHER_KEY, x, dt.datetime(d.year, d.month, d.day, 10 + 2 * i, 0, tzinfo=UTC),
                                    symbol=("XAUUSD", "XAUEUR", "XAUUSD")[i]))
    write_rows(data, rows)
    page.today = R.real_net_on(R.read_evidence(data, cfg=c).trades, fri)
    box["now"] = nightly_instant(fri) + dt.timedelta(minutes=1)
    out = co.tick()
    assert [(i["rule"], i["key"], i["item"]) for i in out["plan"].items] == [("G1", GOOD_KEY, "1")]
    CLI.add_approval(Paths.of(data), "1", "veto", now=box["now"] + dt.timedelta(minutes=5))
    page.today = 0.0
    for day in (dt.date(2026, 11, 14), dt.date(2026, 11, 15)):
        box["now"] = nightly_instant(day) + dt.timedelta(minutes=1)
        o = co.tick()
        assert [(i["rule"], i["key"]) for i in o["plan"].items] == [("G1", GOOD_KEY)]      # re-proposed
    box["now"] = dt.datetime(2026, 11, 16, 6, 31, tzinfo=UTC)
    pol = co.tick()["policy"]
    assert pol["entries"].get(GOOD_KEY) is None or pol["entries"][GOOD_KEY]["route"] != "REAL"
    co.close()


def _plan_row(date, release, items):
    return {"date": date, "released_utc": None, "plan": {"date": date, "release_utc": release, "items": items}}


def test_a_veto_without_a_plan_date_covers_only_the_next_release():
    item = {"item": "1", "rule": "G1", "key": "tnb|SESSION_EXPANSION|INDEX|TREND", "to_state": "LIVE_TRIAL",
            "to_step": "trial"}
    veto = [{"id": "1", "action": "veto", "plan": "", "ts": "2026-10-12T09:00:00+00:00"}]
    tue = _plan_row("2026-10-12", "2026-10-13T05:30:00+00:00", [item])           # the next release after it
    later = _plan_row("2026-11-10", "2026-11-11T06:30:00+00:00", [item])         # a month on
    assert D.vetoed_items(tue, veto)[0] == {"1"}
    assert D.vetoed_items(later, veto) == (set(), set())
    # an undated, untimed veto vetoes nothing (it used to veto item 1 of every plan for ever)
    assert D.vetoed_items(later, [{"id": "1", "action": "veto", "plan": "", "ts": ""}]) == (set(), set())


def test_a_veto_of_a_released_plan_never_carries_to_another_release():
    item = {"item": "1", "rule": "G1", "key": "tnb|SESSION_EXPANSION|INDEX|TREND", "to_state": "LIVE_TRIAL",
            "to_step": "trial"}
    old = {"date": "2026-11-09", "release_utc": "2026-11-10T06:30:00+00:00", "items": [item]}
    new = _plan_row("2026-11-10", "2026-11-11T06:30:00+00:00", [item])
    veto = [{"id": "1", "action": "veto", "plan": "2026-11-09", "ts": "2026-11-09T22:30:00+00:00"}]
    assert D.vetoed_items(new, veto, {"2026-11-09": old}) == (set(), set())


# ============================== 13: nothing promoted while watching goes real when PILOT starts --
def _coach_with_a_made_up_winner(tmp_path, *, mode="PILOT", night=dt.date(2026, 11, 10)):
    """A coach whose made-up winner earns a trial at ``night``'s nightly (returns after that nightly)."""
    from mintel.coach.run import Coach, nightly_instant
    from tests.coach_d_world import (GOOD_KEY, OTHER_KEY, FakePage, data_folder, journal_row, trading_days,
                                     write_rows)
    data, c, _ = data_folder(tmp_path, mode=mode)
    box = {"now": dt.datetime(2026, 10, 12, 9, 0, tzinfo=UTC)}
    page = FakePage()
    co = Coach(data, cfg=c, clock=lambda: box["now"], fetch=page, shuffles=4)
    write_rows(data, [journal_row(GOOD_KEY, 0.1, dt.datetime(2026, 10, 9, 9, 0, tzinfo=UTC))])
    co.tick()
    rows = []
    for d in trading_days(night, 17):
        for i, x in enumerate((1.4, 1.2, -0.3)):
            rows.append(journal_row(GOOD_KEY, x, dt.datetime(d.year, d.month, d.day, 9 + 2 * i, 0, tzinfo=UTC),
                                    symbol=("US500", "JP225", "UK100")[i]))
        for i, x in enumerate((-1.0, -0.8, -0.9)):
            rows.append(journal_row(OTHER_KEY, x, dt.datetime(d.year, d.month, d.day, 10 + 2 * i, 0, tzinfo=UTC),
                                    symbol=("XAUUSD", "XAUEUR", "XAUUSD")[i]))
    write_rows(data, rows)
    page.today = R.real_net_on(R.read_evidence(data, cfg=c).trades, night)
    box["now"] = nightly_instant(night) + dt.timedelta(minutes=1)
    out = co.tick()
    page.today = 0.0
    return data, c, box, co, out


def test_a_promotion_planned_while_watching_never_goes_real_when_pilot_starts(tmp_path):
    import json
    from mintel.coach import RiskCtx
    from mintel.coach import gate as G
    from tests.coach_d_world import GOOD_KEY
    data, c, box, co, out = _coach_with_a_made_up_winner(tmp_path, mode="WATCH")
    assert out["mode"] == "WATCH" and [i["rule"] for i in out["plan"].items] == ["G1"]   # shown, as PILOT would
    box["now"] = dt.datetime(2026, 11, 11, 6, 31, tzinfo=UTC)
    co.tick()
    assert co.ledger.segments()[GOOD_KEY].state == "PRACTICE"                      # never applied
    env_doc = json.loads((data / "coach.json").read_text())
    env_doc["mode"], env_doc["watch_until_utc"] = "PILOT", None
    (data / "coach.json").write_text(json.dumps(env_doc, indent=1))               # Aaron, 14:00 Wednesday
    box["now"] = dt.datetime(2026, 11, 11, 14, 0, tzinfo=UTC)
    o = co.tick()
    assert o["mode"] == "PILOT" and (o["policy"]["entries"].get(GOOD_KEY) or {}).get("route") != "REAL"
    at = box["now"] + dt.timedelta(seconds=10)
    d = G.CoachGate("tnb", data, c, clock=lambda: at).decide("SESSION_EXPANSION", "INDEX", "TREND", "US500", "BUY",
                                                            80, 2.5, now=at, static_real=False,
                                                            risk_ctx=RiskCtx(equity=EQ))
    assert d.route != "REAL"
    co.close()


# ============================== 12: Formula 1 holds 5 UK trading days at base after its "keep" release --
def test_formula1_does_not_step_up_the_night_after_its_keep_release():
    from mintel.coach import F1_KEY, STATE_PINNED, Judgement
    from tests.coach_d_world import series, trading_days
    release_at = dt.datetime(2026, 11, 11, 6, 30, tzinfo=UTC)
    days = trading_days(NOW.date() - dt.timedelta(days=1), 15)
    trades = series("tnb|MOMENTUM_CONTINUATION|FX_MINOR|HIGH_VOL", (-0.3, 1.4), days, risk=13.0)
    segs = {F1_KEY: seg(F1_KEY, STATE_PINNED)}
    _, plan = D.nightly(segs, world(trades, segs))
    assert [i["rule"] for i in plan.items] == ["F1_KEEP"]
    row = {"date": plan.date, "plan": plan.as_dict(), "released_utc": None}
    moves, _, _ = D.release(segs, row, world(trades, segs, now=release_at), [])
    done = D.apply(segs, moves, release_at)
    assert segs[F1_KEY].since_utc == release_at.isoformat()                      # the change is the release
    assert segs[F1_KEY].live_since_utc == "2026-10-12T07:58:00+00:00"           # its live record's start
    judged = {F1_KEY: Judgement(done[0].numbers["verdict"], done[0].numbers["live_trades"],
                                done[0].numbers["judged_utc"])}
    w = world(trades, segs, now=dt.datetime(2026, 11, 11, 22, 21, tzinfo=UTC), judged=judged)
    _, plan2 = D.nightly(segs, w)
    assert not any(i["rule"] == "G3" for i in plan2.items)


# ============================== 14: a step planned at a new high is not released after a loss --
RELEASE = dt.datetime(2026, 11, 11, 6, 30, tzinfo=UTC)


def _live_key(pattern=(-0.3, 1.2, 1.4), days=10):
    from mintel.coach import BASE_STEP, STATE_LIVE
    from tests.coach_d_world import GOOD_KEY, series, trading_days, winner
    since = dt.datetime.combine(trading_days(NOW.date() - dt.timedelta(days=1), days)[0], dt.time(0), tzinfo=UTC)
    segs = {GOOD_KEY: seg(GOOD_KEY, STATE_LIVE, BASE_STEP, since=since)}
    trades = (winner(days=20, end=since.date() - dt.timedelta(days=1))
              + series(GOOD_KEY, pattern, trading_days(NOW.date() - dt.timedelta(days=1), days)))
    return segs, trades


def test_a_step_is_not_released_after_a_loss_since_the_nightly():
    from tests.coach_d_world import GOOD_KEY
    segs, trades = _live_key()
    _, plan = D.nightly(segs, world(trades, segs))
    assert [(i["rule"], i["to_step"]) for i in plan.items] == [("G3", "S1")]
    row = {"date": plan.date, "plan": plan.as_dict(), "released_utc": None}
    # without a loss it is released
    moves, _, _ = D.release({k: Segment(**vars(v)) for k, v in segs.items()}, row, world(trades, segs, now=RELEASE), [])
    assert [m.rule for m in moves] == ["G3"]
    # a real loss of the same key at 02:00, before the 06:30 release: no new high any more
    loss = trade(GOOD_KEY, -1.0, dt.datetime(2026, 11, 11, 2, 0, tzinfo=UTC))
    moves, _, _ = D.release(segs, row, world(trades + [loss], segs, now=RELEASE), [])
    assert [m.rule for m in moves] == ["RELEASE_SKIP"] and "new high" in moves[0].words
    D.apply(segs, [m for m in moves if m.rule not in ("VETO", "RELEASE_SKIP")], RELEASE)
    assert segs[GOOD_KEY].step == "base"


# ============================== 15: the shadow figure is the v1 size at the equity the trade was sized at --
def test_the_shadow_figure_uses_the_equity_the_trade_was_sized_at():
    from mintel.coach import STATE_LIVE_TRIAL
    from mintel.coach.ledger import Change
    from tests.coach_d_world import GOOD_KEY
    sizes = D.Sizes.of(cfg(), env())
    words = "a made-up change for this test, long enough"
    before = (NOW - dt.timedelta(days=1)).isoformat()
    # (1) a real-money account's trial at the v1 size: the shadow is that same money
    _, trial_money = sizes.size("trial", EQ, False, STATE_LIVE_TRIAL)
    t1 = trade(GOOD_KEY, 1.0, NOW, risk=trial_money, coach_route="REAL")
    ch1 = [Change("state", GOOD_KEY, words, "PRACTICE", "LIVE_TRIAL:trial", "G1", {}, ts_utc=before)]
    gate1 = {("tnb", t1.ticket): {"money": trial_money, "key": GOOD_KEY, "equity": EQ}}
    assert D.shadow_rows([t1], ch1, sizes, gate1)[0]["real_money_gbp"] == trial_money
    # (2) a key at S6 held at base (£13) by a brake: v1 would also have risked £13
    _, base_money = sizes.size("base", EQ, True)
    t2 = trade(GOOD_KEY, 1.0, NOW, risk=base_money, coach_route="REAL")
    ch2 = [Change("state", GOOD_KEY, words, "LIVE:S5", "LIVE:S6", "G3", {}, ts_utc=before)]
    gate2 = {("tnb", t2.ticket): {"money": base_money, "key": GOOD_KEY, "equity": EQ}}
    assert D.shadow_rows([t2], ch2, sizes, gate2)[0]["real_money_gbp"] == base_money
    # (3) a demo trial at base: the v1 trial at that equity (0.35%, never above £8)
    t3 = trade(GOOD_KEY, 1.0, NOW, risk=base_money, coach_route="REAL")
    gate3 = {("tnb", t3.ticket): {"money": base_money, "key": GOOD_KEY, "equity": EQ}}
    assert D.shadow_rows([t3], ch1, sizes, gate3)[0]["real_money_gbp"] == trial_money
    # (4) a demo S7 trade: v1's top size (1.5%) at that equity
    _, s7 = sizes.size("S7", EQ, True)
    t4 = trade(GOOD_KEY, -1.0, NOW, risk=s7, coach_route="REAL")
    ch4 = [Change("state", GOOD_KEY, words, "LIVE:S6", "LIVE:S7", "G3", {}, ts_utc=before)]
    gate4 = {("tnb", t4.ticket): {"money": s7, "key": GOOD_KEY, "equity": EQ}}
    assert D.shadow_rows([t4], ch4, sizes, gate4)[0]["real_money_gbp"] == sizes.v1_money("S4", EQ)


def test_the_trader_logs_the_equity_with_a_coach_fill(tmp_path):
    from tests.test_coach_hooks import US500_KEY, act_on, build, full_policy
    from tests.test_coach_policy import put_policy
    from tests.test_tnb_paper_wiring import entry

    def coach(c, sim):
        put_policy(c, full_policy(c, sim, entries={US500_KEY: Entry("REAL", "LIVE_TRIAL", 0.35, 6.53, step="trial")}),
                   now=sim.now)
    sim, c, pb, tr = build(tmp_path, prepare=coach)
    equity = tr.broker.account().equity
    act_on(tr, sim, entry(sim, "US500", 10.0, tactic="SESSION_EXPANSION"))
    (line,) = [v for v in R.read_gate_log(c.ops.data_dir).values() if v.get("event") == "fill"]
    assert line["route"] == "REAL" and line["equity"] == pytest.approx(equity, abs=0.01)
    assert line["static_real"] is False
    tr.journal.close()
    pb.close_db()


# ============================== 16: a step down on a real-money account takes risk off --
def test_a_step_down_from_a_demo_step_on_a_real_money_account_takes_risk_off():
    from mintel.coach import STATE_LIVE
    from tests.coach_d_world import GOOD_KEY, series, trading_days, winner
    days = trading_days(NOW.date() - dt.timedelta(days=1), 2)
    since = dt.datetime.combine(days[0], dt.time(0), tzinfo=UTC)
    segs = {GOOD_KEY: seg(GOOD_KEY, STATE_LIVE, "S7", since=since)}
    hist = winner(days=20, end=since.date() - dt.timedelta(days=3))
    d4 = []
    for n in range(3, 15):
        trades = hist + series(GOOD_KEY, (2.0,) + (-1.5,) * n, days[:1])
        w = world(trades, segs, demo=False)
        moves, _ = D.tick(segs, w)
        d4 = [m for m in moves if m.rule == "D4" and m.to_state == STATE_LIVE]
        if d4:
            break
    assert d4 and d4[0].to_step == "S3"
    before = D.entries(segs, w)[GOOD_KEY]
    D.apply(segs, d4, w.now)
    after = D.entries(segs, world(trades, segs, demo=False))[GOOD_KEY]
    assert after.money_gbp < before.money_gbp
    # on the demo, one step down is the demo step below
    assert D.Sizes.of(cfg(), env()).prev_step("S7", True) == "S6"


# ============================== 18: B9 counts the demo's trials ==============
def _trial_starts(co, n):
    from mintel.coach.ledger import Change
    keys = ["tnb|SESSION_EXPANSION|INDEX|TREND", "tnb|BREAKOUT_ACCEPTANCE|INDEX|TREND",
            "tnb|BREAKOUT_RETEST|GOLD|TREND", "tnb|TREND_PULLBACK|INDEX|TREND", "tnb|SESSION_EXPANSION|GOLD|TREND"]
    for i, k in enumerate(keys[:n]):
        t = dt.datetime(2026, 11, 16 + i, 6, 30, tzinfo=UTC)
        co.ledger.commit([Change("state", k, "A made-up live trial start, released at the 06:30 UK release.",
                                 "PRACTICE", "LIVE_TRIAL:trial", "G1", {})], now=t)


@pytest.mark.parametrize("demo,starts,guard", [(True, 4, False), (True, 5, True), (False, 4, True), (False, 3, False)])
def test_b9_allows_the_demos_trials_in_a_week(tmp_path, demo, starts, guard):
    from mintel.coach.run import Coach
    from tests.coach_d_world import FakePage, data_folder
    data, c, _ = data_folder(tmp_path)
    co = Coach(data, cfg=c, fetch=FakePage(), shuffles=4)
    _trial_starts(co, starts)
    co._audit_meta(dt.datetime(2026, 11, 20, 6, 31, tzinfo=UTC), c, env(), EQ, True, demo=demo)
    assert bool(co.ledger.get("guard")) is guard
    co.close()


# ============================== 10: the replay's luck check sees the registrations --
def test_the_replays_luck_check_is_given_the_registrations(monkeypatch):
    from mintel.coach import Envelope
    from mintel.coach import run as RUN
    from tests.coach_d_world import series, trading_days
    seen = []
    real = RUN.RU.luck_check

    def spy(*a, **kw):
        seen.append(kw.get("registered"))
        return real(*a, **kw)
    monkeypatch.setattr(RUN.RU, "luck_check", spy)
    trades = series("tnb|SESSION_EXPANSION|INDEX|TREND", (1.0, -0.5), trading_days(dt.date(2026, 10, 2), 6))
    RUN.replay(trades, dt.date(2026, 9, 28), dt.date(2026, 10, 2), cfg(), Envelope(mode="PILOT"), shuffles=4)
    assert seen and all(r for r in seen)


# ============================== 9: Formula 1's standing rules in shadow, under "Needs you" --
def test_formula1s_three_days_rule_is_shown_in_shadow_and_never_applied():
    from mintel.coach import F1_KEY
    from tests.coach_d_world import series, trading_days
    days = trading_days(NOW.date() - dt.timedelta(days=1), 4)
    trades = series("tnb|MOMENTUM_CONTINUATION|FX_MINOR|HIGH_VOL", (-0.4, 0.2), days, symbol="EURGBP")
    segs = {F1_KEY: seg(F1_KEY, "PINNED")}
    moves, needs = D.tick(segs, world(trades, segs))
    assert moves == [] and segs[F1_KEY].state == "PINNED"
    assert any(n.startswith("Formula 1: the 3-days rule would switch it off") and "your call" in n for n in needs)


def test_formula1s_market_rule_and_nightly_test_are_shown_in_shadow():
    from mintel.coach import F1_KEY
    from tests.coach_d_world import series, trading_days
    days = trading_days(NOW.date() - dt.timedelta(days=1), 9)
    trades = series("tnb|MOMENTUM_CONTINUATION|FX_MINOR|HIGH_VOL", (-1.0, -0.6, 0.3), days, symbol="EURGBP")
    segs = {F1_KEY: seg(F1_KEY, "PINNED")}
    w = world(trades, segs)
    _, needs = D.tick(segs, w)
    assert any("would make EURGBP practice-only" in n for n in needs)
    downs, plan = D.nightly(segs, w)
    assert segs[F1_KEY].state == "PINNED" and not [m for m in downs if m.key == F1_KEY]
    assert any(n.startswith("Formula 1: the nightly test") and "your call" in n for n in plan.notes)


# ============================== 7: tonight's tau estimate cannot raise anything tonight --
def _exp7_world(c=0.3385):
    """The verifier's made-up world (scratch exp7): a live GOLD key at an edge c above five sibling keys of the
    same parent, 2-4 clustered trades a day."""
    import random
    rng = random.Random(11)
    ticket = [1_990_000_000]
    days, d = [], dt.date(2026, 6, 1)
    while len(days) < 60:
        if d.weekday() < 5:
            days.append(d)
        d += dt.timedelta(days=1)

    def outcome():
        return 2.0 if rng.random() < 1 / 3 else -1.0

    def made_key(key, edge, use_days):
        _, ap, kind, cond = key.split("|")
        out = []
        for day in use_days:
            moves = []
            for i in range(rng.randint(2, 4)):
                if moves and rng.random() < 0.5:
                    moves[-1].append(i)
                else:
                    moves.append([i])
            for mem in moves:
                mv, side, sym = outcome(), rng.choice(["BUY", "SELL"]), rng.choice(["XAUUSD", "XAUEUR"])
                hr = rng.randint(7, 19)
                for j, _ in enumerate(mem):
                    x = (mv if rng.random() < 0.7 else outcome()) + edge
                    ticket[0] += 1
                    t0 = dt.datetime(day.year, day.month, day.day, hr, min(59, 5 * j + rng.randint(0, 4)), tzinfo=UTC)
                    out.append(R.Trade("journal", "tnb", ticket[0], ap, kind, cond, sym, side, t0,
                                       t0 + dt.timedelta(minutes=20), True, round(x * 10.0, 2), 0.6, 10.0))
        return out
    target = "tnb|A0|GOLD|TREND"
    base = made_key(target, 0.0, days[:12])
    others = []
    for cnd in ("HIGH_VOL", "SQUEEZE", "RANGE", "NEWS", "QUIET"):
        others += made_key(f"tnb|A0|GOLD|{cnd}", 0.0, days)
    import dataclasses
    tt = [dataclasses.replace(t, net_gbp=round(t.net_gbp + c * t.risk_gbp, 2)) for t in base]
    night = dt.datetime(days[-1].year, days[-1].month, days[-1].day, 21, 20, tzinfo=UTC)
    return target, R.assign_clusters(tt + others), night


def test_a_worse_sibling_trade_cannot_raise_a_live_keys_numbers_through_tonights_tau():
    """Tonight's decisions use the tau held from last night; tonight's re-estimate is held from tomorrow. In
    the verifier's world one sibling trade made 3 R worse raised the live key's P+_real from just under 0.80
    to 0.812 through tau and held off its D3 step down. At the held tau it can only fall."""
    import dataclasses
    from mintel.coach import estimate as ES
    target, trades, night = _exp7_world()
    worse = [dataclasses.replace(t, net_gbp=round(t.net_gbp - 30.0, 2)) if t.ticket == 1_990_000_568 else t
             for t in trades]
    # the control: tonight's own estimate used tonight (as before) - the worse trade raises it
    a = ES.fit(trades, night, live_keys=[target], epochs=(), reestimate_tau=True).keys[target]
    b = ES.fit(worse, night, live_keys=[target], epochs=(), reestimate_tau=True).keys[target]
    assert a.p_plus_real < 0.80 < b.p_plus_real
    held = ES.fit(trades, night, live_keys=[target], epochs=(), reestimate_tau=True).taus      # last night's
    fa, ta = ES.nightly_fit(trades, night, held_taus=held, live_keys=[target], epochs=())
    fb, tb = ES.nightly_fit(worse, night, held_taus=held, live_keys=[target], epochs=())
    assert fa.taus == fb.taus == {k: float(v) for k, v in held.items()}
    assert fb.keys[target].p_plus_real <= fa.keys[target].p_plus_real
    assert fb.keys[target].lcb80_real <= fa.keys[target].lcb80_real
    assert tb[3] > ta[3]                         # tonight's estimate moved; it is held only from tomorrow


def test_formula1s_judged_trades_still_count_for_its_first_step_after_the_hold():
    from mintel.coach import F1_KEY, STATE_PINNED, Judgement
    from tests.coach_d_world import series, trading_days
    release_at = dt.datetime(2026, 11, 11, 6, 30, tzinfo=UTC)
    days = trading_days(NOW.date() - dt.timedelta(days=1), 15)
    trades = series("tnb|MOMENTUM_CONTINUATION|FX_MINOR|HIGH_VOL", (-0.3, 1.4), days, risk=13.0)
    segs = {F1_KEY: seg(F1_KEY, STATE_PINNED)}
    _, plan = D.nightly(segs, world(trades, segs))
    row = {"date": plan.date, "plan": plan.as_dict(), "released_utc": None}
    moves, _, _ = D.release(segs, row, world(trades, segs, now=release_at), [])
    done = D.apply(segs, moves, release_at)
    judged = {F1_KEY: Judgement(done[0].numbers["verdict"], done[0].numbers["live_trades"],
                                done[0].numbers["judged_utc"])}
    # 6 UK trading days later, with no new trade since the release: the 30 judged trades count at base
    later = dt.datetime(2026, 11, 19, 22, 21, tzinfo=UTC)
    _, plan2 = D.nightly(segs, world(trades, segs, now=later, judged=judged))
    assert [(i["rule"], i["key"], i["to_step"]) for i in plan2.items if i["rule"] == "G3"] == [("G3", F1_KEY, "S1")]
