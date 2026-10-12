"""The verifiers' findings on the coach's hooks, fixed (12 Oct), each pinned by a test that fails without its fix.

1. The live Momentum Runner keeps the coach's word on a ride (no ride, or a lower money cap) even when
   management closes the Trend & Breakout trade in the same pass that the Runner first sees it.
2. With the coach off, the PAPER wrapper's send-time ceiling check never refuses an order Aaron's own
   settings send: the reference run here keeps every hook attached except the gate, so the check is live.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from mintel.coach import Entry
from tests.test_coach_hooks import RUNNER, act_on, build, full_policy, record
from tests.test_coach_policy import put_policy
from tests.test_tnb_paper_wiring import TNB, entry, move

MC = "MOMENTUM_CONTINUATION"


def _close_us500_in_management(tr):
    """Management closes the US500 trade in the same pass the Runner first sees it."""
    orig = tr._manage_one

    def manage(p, now):
        if p.symbol == "US500":
            res = tr.broker.close(p.ticket)
            tr._finalise(p.ticket, now, exit_price=res.price)
            return ["closed by management"]
        return orig(p, now)
    tr._manage_one = manage


def _runner_orders(sim):
    return [(o["req"].symbol, float(o["req"].volume)) for o in sim.order_log if o["req"].magic == RUNNER]


def test_a_key_level_runner_skip_holds_when_the_trade_closes_in_the_same_pass(tmp_path):
    def skip_key(cfg, sim):
        # the coach denies the Runner this one idea; the bot itself stays allowed to enter
        put_policy(cfg, full_policy(cfg, sim, entries={
            "runner|SESSION_EXPANSION|INDEX|TREND": Entry("SKIP", "PAUSED", rule="B2")}), now=sim.now)
    sim, cfg, pb, tr = build(tmp_path, runner="LIVE", prepare=skip_key)
    act_on(tr, sim, entry(sim, "US500", 10.0, tactic="SESSION_EXPANSION"))
    (row,) = tr.journal.open_trades()
    assert row["runner_volume"] == 0.0 and tr.runner.entries_allowed()
    _close_us500_in_management(tr)
    sim.advance(1)
    tr.cycle()
    assert _runner_orders(sim) == []
    sim.advance(1)
    tr.cycle()
    assert _runner_orders(sim) == [] and tr._coach_copies == {}     # forgotten once the trade has gone
    tr.journal.close()
    pb.close_db()


def test_a_runner_money_cap_holds_when_the_trade_closes_in_the_same_pass(tmp_path):
    def cap(cfg, sim):
        put_policy(cfg, full_policy(cfg, sim, entries={"runner|*|*|*": Entry("BASELINE", "PLACED", money_gbp=3.0)}),
                   now=sim.now)
    sim, cfg, pb, tr = build(tmp_path, runner="LIVE", prepare=cap)
    act_on(tr, sim, entry(sim, "US500", 10.0, tactic="SESSION_EXPANSION"))
    (row,) = tr.journal.open_trades()
    stored = float(row["runner_volume"])
    _close_us500_in_management(tr)
    sim.advance(1)
    tr.cycle()
    ridden = _runner_orders(sim)
    assert all(v <= stored + 1e-9 for _, v in ridden)
    per_lot = sim.spec("US500").money_per_lot(10.0)
    assert all(v * per_lot <= 3.0 + 1e-6 for _, v in ridden)
    tr.journal.close()
    pb.close_db()


def test_with_the_coach_off_the_runner_path_is_as_before(tmp_path):
    """No coach word on any ride: nothing is remembered, the Runner sees exactly what it saw before."""
    sim, cfg, pb, tr = build(tmp_path, runner="LIVE")
    act_on(tr, sim, entry(sim, "US500", 10.0, tactic="SESSION_EXPANSION"))
    assert tr._coach_copies == {}
    tr.journal.close()
    pb.close_db()


def _top_size(cfg, sim):
    cfg.risk.base_risk_pct = 1.5               # sized right at the 1.5% ceiling, as the top size does
    cfg.risk.max_risk_money = 0.0


@pytest.mark.parametrize("drift", [0.00015, 0.00040])
def test_with_the_coach_off_a_price_move_before_the_send_never_refuses_aarons_order(tmp_path, drift):
    """Coach entirely absent (no envelope). An order sized at the ceiling and a price move against the stop
    between the scan and the send: the order goes exactly as it does with every hook detached."""
    out = {}
    for name in ("off", "today"):
        sim, cfg, pb, tr = build(tmp_path / name, prepare=_top_size, strip=(name == "today"))
        if name == "off":
            assert callable(pb.ceiling_pct)            # the check is attached, and must not bite
        st = entry(sim, "EURGBP", 0.0020, tactic=MC)
        move(sim, "EURGBP", drift)
        act_on(tr, sim, st, must=False)
        out[name] = record(sim, pb, tr)
        out[name]["n_real"] = len([o for o in sim.order_log if int(o["req"].magic or 0) == TNB])
        tr.journal.close()
        pb.close_db()
    assert out["off"]["n_real"] == 1
    assert out["off"] == out["today"]


def test_the_ceiling_check_still_refuses_an_oversized_coach_grant(tmp_path):
    """Kept: a coach REAL order above the ceiling at the send is refused (the third cap check)."""
    from tests.test_coach_hooks import US500_KEY

    def coach(cfg, sim):
        put_policy(cfg, full_policy(cfg, sim, entries={US500_KEY: Entry("REAL", "LIVE_TRIAL", 0.35, 6.53,
                                                                        step="trial")}), now=sim.now)
    sim, cfg, pb, tr = build(tmp_path, prepare=coach)
    pb.ceiling_pct = lambda: 0.001
    acted, blocked = act_on(tr, sim, entry(sim, "US500", 10.0, tactic="SESSION_EXPANSION"), must=False)
    assert acted is None and any("ceiling" in b for b in blocked) and not sim.order_log
    tr.journal.close()
    pb.close_db()
