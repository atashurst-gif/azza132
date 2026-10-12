"""Aaron's HALT switch, ``data/HALT`` (spec sections 1.8, 10.3 step 1 and
10.4): every bot refuses new real-money entries, whatever the coach, its
envelope, COACH-OFF or its policy say - even with no coach installed at all.
Open positions are still managed. Practice carries on."""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from mintel.coach import Entry, RiskCtx
from mintel.coach import gate as G
from tests.test_coach_hooks import MC, act_on, build, full_policy, ian
from tests.test_coach_policy import put_envelope, put_policy
from tests.test_tnb_paper_wiring import TNB, entry


def halt(cfg):
    (Path(cfg.ops.data_dir) / "HALT").write_text("YES")


@pytest.mark.parametrize("coach", ["none", "off", "pilot"])
def test_trend_and_breakout_sends_no_new_real_entry_and_keeps_practising(tmp_path, coach):
    def prepare(cfg, sim):
        if coach == "off":
            (Path(cfg.ops.data_dir) / "COACH-OFF").write_text("YES")
        if coach == "pilot":
            put_policy(cfg, full_policy(cfg, sim), now=sim.now)
    sim, cfg, pb, tr = build(tmp_path, live_tactics=(MC,), prepare=prepare)
    act_on(tr, sim, entry(sim, "EURGBP", 0.0030, tactic=MC))          # Formula 1, real before HALT
    (real,) = sim.positions(TNB)
    halt(cfg)
    tr.last_entry_utc = None
    acted, _ = act_on(tr, sim, entry(sim, "EURUSD", 0.0025, tactic="BREAKOUT_ACCEPTANCE"))   # practice: as ever
    assert pb.is_paper(tr.journal.open_trades()[-1]["ticket"])
    sim2_orders = len(sim.order_log)
    # Formula 1 again on another minor pair would be real: under HALT it practises instead
    cfg.risk.max_positions_per_symbol = 2
    act_on(tr, sim, entry(sim, "EURGBP", 0.0035, tactic=MC))         # a new idea (another stop)
    assert len(sim.order_log) == sim2_orders                          # nothing new reached the broker
    newest = max(r["ticket"] for r in tr.journal.open_trades())
    assert pb.is_paper(newest)
    # the open real trade is still managed at the broker: its stop moved and it can be closed
    spec = sim.spec("EURGBP")
    pos = next(p for p in pb.positions(TNB) if p.ticket == real.ticket)
    res = tr.executor.move_stop(pos, spec.normalise_price(pos.sl + 0.0005), spec)
    assert res is not None and res.ok and sim.modify_log
    assert tr.executor.close_position(pos, "test").ok
    tr.journal.close()
    pb.close_db()


def test_the_runner_band_breaker_and_crowd_fader_refuse_new_real_entries(tmp_path):
    sim, cfg, pb, tr = build(tmp_path, runner="LIVE")
    acct = tr.broker.account()
    tr._last_snapshot = tr.risk.snapshot(acct, tr.broker.positions(TNB), sim.now)
    assert tr.runner.entries_allowed() is True
    halt(cfg)
    assert tr.runner.entries_allowed() is False
    assert tr.bandbreaker.entries_allowed() is True and tr.crowd.entries_allowed() is True      # PAPER
    cfg.bandbreaker.mode = cfg.crowd.mode = "LIVE"
    assert tr.bandbreaker.entries_allowed() is False and tr.crowd.entries_allowed() is False
    # and the Runner's copy of a trade is not ridden
    state = entry(sim, "US500", 10.0, tactic="SESSION_EXPANSION")
    act_on(tr, sim, state)
    (row,) = tr.journal.open_trades()
    assert row["runner_volume"] == 0.0
    cfg.runner.mode = "PAPER"
    assert tr.runner.entries_allowed() is True                       # a PAPER Runner keeps practising
    tr.journal.close()
    pb.close_db()


def test_financial_ian_refuses_new_real_entries(tmp_path):
    from mintel.config import Config
    now = dt.datetime(2026, 10, 13, 10, 0, tzinfo=dt.timezone.utc)
    cfg = Config()
    cfg.ops.data_dir = str(tmp_path / "data")
    Path(cfg.ops.data_dir).mkdir(parents=True)
    halt(cfg)
    g = G.CoachGate("ian", cfg.ops.data_dir, cfg, clock=lambda: now)
    assert ian(tmp_path / "a", g)._entry_block(now).startswith("HALT")           # no envelope, no policy
    assert ian(tmp_path / "b", g, mode="PAPER")._entry_block(now) == ""         # practice carries on
    # a gate that could not be built still honours HALT
    base = G.BaselineGate(cfg, "ian", Path(cfg.ops.data_dir) / "HALT")
    assert ian(tmp_path / "c", base)._entry_block(now).startswith("HALT")


def test_halt_holds_whatever_the_coach_says(tmp_path):
    from mintel.config import Config
    now = dt.datetime(2026, 10, 13, 10, 0, tzinfo=dt.timezone.utc)
    cfg = Config()
    cfg.ops.data_dir = str(tmp_path / "data")
    Path(cfg.ops.data_dir).mkdir(parents=True)
    sha = put_envelope(cfg)
    from tests.test_coach_policy import make_policy, grant
    put_policy(cfg, make_policy(sha, now=now, entries={"tnb|SESSION_EXPANSION|INDEX|TREND": grant()}), now=now)
    g = G.CoachGate("tnb", cfg.ops.data_dir, cfg, clock=lambda: now)
    ctx = RiskCtx(equity=1865.61)
    assert g.decide("SESSION_EXPANSION", "INDEX", "TREND", "US500", "BUY", 80, 2, now, False, ctx).route == "REAL"
    halt(cfg)
    d = g.decide("SESSION_EXPANSION", "INDEX", "TREND", "US500", "BUY", 80, 2, now, False, ctx)
    assert d.route == "BASELINE" and d.source == "halt"                  # a practice entry stays practice
    d = g.decide("MOMENTUM_CONTINUATION", "FX_MINOR", "TREND", "EURGBP", "BUY", 80, 2, now, True, ctx)
    assert d.route == "PRACTICE" and d.source == "halt"                  # pinned Formula 1 too
    d = g.decide("*", "CRYPTO", "TREND", "BTCUSD", 1, 80, None, now, True, RiskCtx(has_paper=False), bot="ian")
    assert d.route == "SKIP"
    assert g.confirm_order(type("R", (), {"symbol": "US500", "tactic": "SESSION_EXPANSION"}), kind="INDEX") is False
    assert g.effective_limits(now).max_daily_loss_pct == cfg.risk.max_daily_loss_pct
