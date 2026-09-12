"""Order safety: idempotency, return codes, protective stops, reconciliation."""
from __future__ import annotations

import datetime as dt

import pytest

from mintel.broker.base import (OrderRequest, Position, RetCode, Side)
from mintel.config import Config
from mintel.data.regime import Regime
from mintel.engine.evidence import Evidence, Family, MarketState
from mintel.engine.execution import Executor, IntentState, make_key
from mintel.engine.journal import Journal
from mintel.engine.risk import RiskManager

UTC = dt.timezone.utc


def state(sim, symbol="EURUSD", direction=1, stop_pips=25.0) -> MarketState:
    spec = sim.spec(symbol)
    tick = sim.tick(symbol)
    entry = tick.ask if direction > 0 else tick.bid
    stop = entry - direction * spec.pips_to_price(stop_pips)
    target = entry + direction * spec.pips_to_price(stop_pips * 2)
    return MarketState(
        symbol=symbol, as_of=sim.now, regime=Regime.TREND,
        regime_label="TREND / NORMAL VOL", direction=direction,
        tactic="MOMENTUM_CONTINUATION",
        evidence={Family.STRUCTURE: Evidence(Family.STRUCTURE, 80.0)},
        raw_score=75.0, opportunity=75.0, tier="STRONG",
        entry=entry, stop=spec.normalise_price(stop),
        target=spec.normalise_price(target), reward_risk=2.0,
        headroom_pips=50.0, cost_pips=1.0)


@pytest.fixture
def ex(sim, cfg, journal):
    return Executor(sim, cfg, journal, RiskManager(cfg), clock=lambda: sim.now)


class TestIdempotency:
    def test_identical_intent_is_not_sent_twice(self, sim, ex):
        s = state(sim)
        spec = sim.spec("EURUSD")
        first = ex.submit(s, spec, 0.10)
        assert first.ok
        second = ex.submit(s, spec, 0.10)
        assert not second.ok
        assert "already submitted" in second.message
        assert len(sim.positions()) == 1

    def test_key_is_stable_for_the_same_decision(self, sim):
        s, spec = state(sim), sim.spec("EURUSD")
        assert make_key(s, spec) == make_key(s, spec)

    def test_key_differs_for_the_other_side(self, sim):
        spec = sim.spec("EURUSD")
        assert make_key(state(sim, direction=1), spec) != \
            make_key(state(sim, direction=-1), spec)

    def test_key_differs_when_the_stop_moves_materially(self, sim):
        spec = sim.spec("EURUSD")
        assert make_key(state(sim, stop_pips=20), spec) != \
            make_key(state(sim, stop_pips=40), spec)

    def test_a_later_time_bucket_allows_a_genuinely_new_trade(self, sim, ex):
        """Re-entry must remain possible; only duplicates are blocked."""
        spec = sim.spec("EURUSD")
        first = ex.submit(state(sim), spec, 0.10)
        assert first.ok
        sim.close(first.position.ticket)
        ex.set_cooldown("EURUSD", Side.BUY, 0.0)
        sim.advance(3)
        second = ex.submit(state(sim), spec, 0.10)
        assert second.ok, "a new setup minutes later is not a duplicate"

    def test_key_is_written_into_the_order_comment(self, sim, ex):
        """Recovery depends on the broker's own records carrying the key."""
        s, spec = state(sim), sim.spec("EURUSD")
        report = ex.submit(s, spec, 0.10)
        assert report.intent.key in sim.positions()[0].comment


class TestReturnCodes:
    def test_api_success_is_not_assumed_to_be_execution_success(self, sim, ex):
        sim.fault.reject_all = RetCode.NO_MONEY
        report = ex.submit(state(sim), sim.spec("EURUSD"), 0.10)
        assert not report.ok
        assert "NO_MONEY" in report.message
        assert report.intent.state is IntentState.REJECTED
        assert not sim.positions()

    def test_requote_is_retried(self, sim, ex):
        sim.fault.reject_next = RetCode.REQUOTE
        report = ex.submit(state(sim), sim.spec("EURUSD"), 0.10)
        assert report.ok
        assert report.attempts == 2

    def test_fatal_rejection_is_not_retried(self, sim, ex):
        sim.fault.reject_all = RetCode.TRADE_DISABLED
        report = ex.submit(state(sim), sim.spec("EURUSD"), 0.10)
        assert report.attempts == 1

    def test_rejections_are_reported_to_the_risk_breakers(self, sim, cfg,
                                                          journal):
        rm = RiskManager(cfg)
        ex = Executor(sim, cfg, journal, rm, clock=lambda: sim.now)
        sim.fault.reject_all = RetCode.REJECT
        ex.submit(state(sim), sim.spec("EURUSD"), 0.10)
        assert len(rm._rejects) >= 1

    def test_rejection_starts_a_cooldown(self, sim, ex):
        sim.fault.reject_all = RetCode.REJECT
        ex.submit(state(sim), sim.spec("EURUSD"), 0.10)
        assert ex.cooldown_remaining("EURUSD", Side.BUY) > 0

    def test_invalid_stop_distance_is_refused_by_the_broker(self, sim, ex):
        s = state(sim, stop_pips=0.05)     # inside the broker's stop level
        report = ex.submit(s, sim.spec("EURUSD"), 0.10)
        assert not report.ok

    def test_partial_fill_is_detected(self, sim, ex):
        sim.fault.partial_fill_ratio = 0.4
        report = ex.submit(state(sim), sim.spec("EURUSD"), 1.0)
        assert report.ok
        assert report.intent.state is IntentState.PARTIAL
        assert "PARTIAL" in report.message
        assert sim.positions()[0].volume == pytest.approx(0.4)


class TestProtectiveStops:
    def test_stop_is_confirmed_at_the_broker_after_entry(self, sim, ex):
        report = ex.submit(state(sim), sim.spec("EURUSD"), 0.10)
        assert report.protective_stop_confirmed
        assert sim.positions()[0].sl > 0

    def test_a_dropped_stop_is_re_applied_immediately(self, sim, ex):
        sim.fault.drop_stops_on_send = True
        report = ex.submit(state(sim), sim.spec("EURUSD"), 0.10)
        assert report.ok
        assert report.protective_stop_confirmed, (
            "a position must never be left naked")
        assert sim.positions()[0].sl > 0

    def test_naked_positions_are_swept_and_protected(self, sim, ex):
        ex.submit(state(sim), sim.spec("EURUSD"), 0.10)
        sim.positions()[0].sl = 0.0
        problems = ex.enforce_stops(sim.positions())
        assert problems == []
        assert sim.positions()[0].sl > 0

    def test_enforce_reports_what_it_cannot_fix(self, sim, ex):
        ex.submit(state(sim), sim.spec("EURUSD"), 0.10)
        p = sim.positions()[0]
        p.sl = 0.0
        sim.fault.fail_modify = True
        problems = ex.enforce_stops([p])
        assert problems and "NO stop" in problems[0]

    def test_move_stop_refuses_to_widen(self, sim, ex):
        report = ex.submit(state(sim), sim.spec("EURUSD"), 0.10)
        p = report.position
        original = p.sl
        spec = sim.spec("EURUSD")
        assert ex.move_stop(p, original - 0.0010, spec) is None
        assert p.sl == pytest.approx(original)

    def test_move_stop_accepts_a_tightening(self, sim, ex):
        report = ex.submit(state(sim), sim.spec("EURUSD"), 0.10)
        p = report.position
        spec = sim.spec("EURUSD")
        res = ex.move_stop(p, p.sl + 0.0005, spec)
        assert res is not None and res.ok
        assert sim.positions()[0].sl > report.intent.sl

    def test_microscopic_improvements_are_ignored(self, sim, ex, cfg):
        cfg.flowlock.min_stop_step_points = 50.0
        report = ex.submit(state(sim), sim.spec("EURUSD"), 0.10)
        p = report.position
        assert ex.move_stop(p, p.sl + 0.00001, sim.spec("EURUSD")) is None


class TestReconciliation:
    def test_an_unknown_intent_that_filled_is_adopted(self, sim, cfg, journal):
        """Crash between send and response must not duplicate the trade."""
        ex = Executor(sim, cfg, journal, clock=lambda: sim.now)
        s, spec = state(sim), sim.spec("EURUSD")
        key = make_key(s, spec)
        # Simulate the order reaching the broker while we lost the reply.
        sim.send(OrderRequest("EURUSD", Side.BUY, 0.10, sl=s.stop,
                              comment=key, magic=cfg.magic,
                              idempotency_key=key))
        from mintel.engine.execution import Intent
        ex.intents[key] = Intent(key, "EURUSD", Side.BUY, 0.10, s.stop, 0.0,
                                 sim.now, IntentState.UNKNOWN)
        out = ex.reconcile()
        assert out["adopted"] and out["adopted"][0]["key"] == key
        assert ex.intents[key].state is IntentState.FILLED
        # And a resubmission of the same idea is refused.
        assert not ex.submit(s, spec, 0.10).ok
        assert len(sim.positions()) == 1

    def test_an_unknown_intent_that_never_filled_is_abandoned(self, sim, cfg,
                                                              journal):
        ex = Executor(sim, cfg, journal, clock=lambda: sim.now)
        from mintel.engine.execution import Intent
        ex.intents["ghost"] = Intent("ghost", "EURUSD", Side.BUY, 0.1, 1.09,
                                     0.0, sim.now, IntentState.UNKNOWN)
        out = ex.reconcile()
        assert "ghost" in out["abandoned"]
        assert ex.intents["ghost"].state is IntentState.ABANDONED

    def test_orphan_positions_are_adopted_and_protected(self, sim, cfg,
                                                        journal):
        sim.send(OrderRequest("EURUSD", Side.BUY, 0.10, sl=0.0,
                              magic=cfg.magic))
        ex = Executor(sim, cfg, journal, clock=lambda: sim.now)
        out = ex.reconcile()
        assert out["orphans"], "an unknown live position must be adopted"
        assert out["naked"]
        assert sim.positions()[0].sl > 0, "and given a stop at once"

    def test_broker_is_the_source_of_truth_after_a_restart(self, sim, cfg,
                                                            journal):
        ex = Executor(sim, cfg, journal, clock=lambda: sim.now)
        report = ex.submit(state(sim), sim.spec("EURUSD"), 0.10)
        ticket = report.position.ticket
        # A completely fresh executor, as after a process restart.
        fresh = Executor(sim, cfg, journal, clock=lambda: sim.now)
        out = fresh.reconcile()
        assert any(o["ticket"] == ticket for o in out["orphans"]) or \
            any(i.ticket == ticket for i in fresh.intents.values())
        assert len(sim.positions()) == 1, "no duplicate was created"

    def test_unresolved_intents_are_recovered_from_the_database(self, sim, cfg,
                                                                journal):
        ex = Executor(sim, cfg, journal, clock=lambda: sim.now)
        from mintel.engine.execution import Intent
        it = Intent("persisted", "EURUSD", Side.BUY, 0.1, 1.09, 0.0, sim.now,
                    IntentState.UNKNOWN)
        journal.save_intent(it)
        fresh = Executor(sim, cfg, journal, clock=lambda: sim.now)
        fresh.reconcile()
        assert "persisted" in fresh.intents

    def test_reconcile_survives_a_broker_error(self, sim, cfg, journal):
        ex = Executor(sim, cfg, journal, clock=lambda: sim.now)
        sim.fault.disconnect = True
        out = ex.reconcile()
        assert "error" in out


class TestCooldown:
    def test_cooldown_blocks_an_immediate_resubmission(self, sim, ex):
        ex.set_cooldown("EURUSD", Side.BUY, 300.0)
        report = ex.submit(state(sim), sim.spec("EURUSD"), 0.10)
        assert not report.ok and "cooling down" in report.message

    def test_cooldown_expires(self, sim, ex):
        ex.set_cooldown("EURUSD", Side.BUY, 60.0)
        sim.advance(2)          # two minutes of simulated time
        assert ex.cooldown_remaining("EURUSD", Side.BUY) == 0.0

    def test_closing_a_trade_starts_a_cooldown(self, sim, ex):
        report = ex.submit(state(sim), sim.spec("EURUSD"), 0.10)
        ex.close_position(report.position, "TEST")
        assert ex.cooldown_remaining("EURUSD", Side.BUY) > 0

    def test_cooldown_is_per_symbol_and_side(self, sim, ex):
        ex.set_cooldown("EURUSD", Side.BUY, 300.0)
        assert ex.cooldown_remaining("EURUSD", Side.SELL) == 0.0
        assert ex.cooldown_remaining("GBPUSD", Side.BUY) == 0.0
