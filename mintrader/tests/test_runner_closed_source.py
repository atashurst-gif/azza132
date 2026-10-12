"""The LIVE Momentum Runner never rides a Trend & Breakout trade that the same
management pass has just closed, and a trade whose approach is unknown is
never ridden (it could otherwise slip past ``skip_tactics``).

Before the fix, ``Trader.manage`` handed ``runner.observe`` the position list
read before management: a trade closed above had no open journal row any
more, so the Runner saw no approach (skip_tactics bypassed) and no copy
volume (Trend & Breakout's full volume copied), and opened a real order
beside a trade that no longer existed.
"""
from __future__ import annotations

import dataclasses

import pytest

from mintel.botexec import MAGIC_RUNNER
from mintel.engine import tactics as T
from mintel.engine.flowlock import FlowState
from mintel.runner.shadow import MomentumRunner, RunnerConfig
from tests.test_runner_feed import TNB, close_all, make, real_trade, signal, index_sim


@pytest.fixture
def market(monkeypatch):
    tactics: list = []
    monkeypatch.setattr(T, "candidates_for", lambda regime: tuple(tactics))
    return tactics


def _open_index_trade(tmp_path, market, name):
    market.append(T.Tactic(name, tuple(T.Regime), signal(name, 80.0)))
    sim, pb, cfg, tr = make(tmp_path, symbols=("US500",))
    tr.bootstrap()
    result = tr.cycle()
    assert result.acted is not None, result.blocked_reasons
    mine = pb.positions(TNB)
    assert len(mine) == 1 and mine[0].symbol == "US500"
    rows = tr.journal.open_trades()
    assert [r["tactic"] for r in rows] == [name]
    return sim, pb, cfg, tr, mine[0]


def _close_on_this_pass(tr):
    """FlowLock says close on the next update (a thesis exit, say)."""
    real = tr.flowlock.update

    def update(position, **kw):
        d = real(position, **kw)
        return dataclasses.replace(d, close=True, close_reason="THESIS_EXIT")
    tr.flowlock.update = update


class TestAClosedSourceIsNeverRidden:
    def test_a_skipped_approach_closed_in_the_same_pass_is_not_ridden(self, tmp_path, market):
        sim, pb, cfg, tr, real = _open_index_trade(tmp_path, market, "MOMENTUM_CONTINUATION")
        assert "MOMENTUM_CONTINUATION" in tr.runner.cfg.skip_tactics
        _close_on_this_pass(tr)
        tr.manage_cycle()
        assert pb.positions(TNB) == []                       # the real trade was closed by this pass
        assert tr.journal.open_trades() == []
        assert sim.positions(MAGIC_RUNNER) == [], "the Runner rode a trade closed earlier in the same pass"
        assert not [o for o in sim.order_log if o["req"].magic == MAGIC_RUNNER]
        assert not tr.runner.open
        close_all(tr.journal, pb)

    def test_a_ridden_approach_closed_in_the_same_pass_is_not_ridden_either(self, tmp_path, market):
        sim, pb, cfg, tr, real = _open_index_trade(tmp_path, market, "SESSION_EXPANSION")
        _close_on_this_pass(tr)
        tr.manage_cycle()
        assert pb.positions(TNB) == []
        assert sim.positions(MAGIC_RUNNER) == [] and not tr.runner.open
        close_all(tr.journal, pb)

    def test_still_open_it_is_ridden_as_before(self, tmp_path, market):
        sim, pb, cfg, tr, real = _open_index_trade(tmp_path, market, "SESSION_EXPANSION")
        tr.manage_cycle()
        runner = sim.positions(MAGIC_RUNNER)
        assert len(runner) == 1 and runner[0].volume == pytest.approx(real.volume)
        close_all(tr.journal, pb)

    def test_still_open_a_skipped_approach_is_not_ridden_as_before(self, tmp_path, market):
        sim, pb, cfg, tr, real = _open_index_trade(tmp_path, market, "MOMENTUM_CONTINUATION")
        tr.manage_cycle()
        assert sim.positions(MAGIC_RUNNER) == [] and not tr.runner.open
        close_all(tr.journal, pb)


class TestAnUnknownApproachFailsSafe:
    def _runner(self, tmp_path, b):
        return MomentumRunner(tmp_path, RunnerConfig(mode="LIVE", max_risk_money=0.0), spec_fn=b.spec,
                              clock=lambda: b.now, broker=b)

    def test_missing_from_the_traders_approaches_is_not_ridden_and_says_so(self, tmp_path, caplog):
        b = index_sim()
        r = self._runner(tmp_path, b)
        p, sl = real_trade(b)
        with caplog.at_level("WARNING", logger="mintel.runner.shadow"):
            notes = r.observe([p], {1: sl}, b.tick, b.now, tactics={})
        assert not r.open and b.positions(MAGIC_RUNNER) == []
        assert any("approach is unknown" in n for n in notes)
        assert any("approach is unknown" in m.getMessage() for m in caplog.records)
        # said once, not every pass; never recorded as a refusal for good
        assert r.observe([p], {1: sl}, b.tick, b.now, tactics={}) == []
        assert r.db.execute("SELECT COUNT(*) FROM skipped").fetchone()[0] == 0
        r.close()

    def test_an_empty_approach_is_not_ridden(self, tmp_path):
        b = index_sim()
        r = self._runner(tmp_path, b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, b.now, tactics={1: None})
        assert not r.open and b.positions(MAGIC_RUNNER) == []
        r.close()

    def test_a_known_approach_is_ridden(self, tmp_path):
        b = index_sim()
        r = self._runner(tmp_path, b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, b.now, tactics={1: "SESSION_EXPANSION"})
        assert len(b.positions(MAGIC_RUNNER)) == 1
        r.close()
