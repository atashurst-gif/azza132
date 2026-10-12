"""The caps, enforced three times (spec sections 1.7, 6.1 and 10.3 step 8):
the gate's clamp, ``RiskManager.size`` with the grant's money as its cap, and
the ceiling check in ``PaperBroker.send``. The 1.5% ceiling and the 3% stop
are only ever read (section 21 lifts them on a demo account only; see
test_coach_demo_limits.py)."""
from __future__ import annotations

import dataclasses
import json
import logging
import random
from pathlib import Path

import pytest

from mintel.broker.base import OrderRequest, Side
from mintel.coach import (DEMO_DAILY_STOP_LADDER, DEMO_MAX_STEP_PCT, DEMO_STEPS_PCT, STEP_ORDER, STEPS_PCT, Entry,
                          RiskCtx, step_money, step_pct)
from mintel.config import Config, RiskConfig
from mintel.engine.risk import RiskManager, base_risk
from tests.test_coach_hooks import US500_KEY, act_on, build, full_policy
from tests.test_coach_policy import put_policy, put_raw, sealed_obj
from tests.test_tnb_paper_wiring import TNB, entry


def test_risk_size_with_a_money_cap_never_goes_above_the_ceiling_after_lot_rounding():
    from mintel.broker.sim import SimBroker
    sim = SimBroker(seed=5, history_bars=50)
    sim.connect()
    cfg = Config()
    rm = RiskManager(cfg)
    rng = random.Random(11)
    from mintel.engine.evidence import MarketState
    from mintel.data.regime import Regime
    checked = 0
    for i in range(3000):
        sym = rng.choice(sim.symbols())
        spec = sim.spec(sym)
        t = sim.tick(sym)
        dist = spec.point * rng.uniform(5, 3000)
        st = MarketState(symbol=sym, as_of=sim.now, regime=Regime.TREND, regime_label="TREND", direction=1,
                         tactic="SESSION_EXPANSION", evidence={}, raw_score=70, opportunity=70, tier="STRONG",
                         entry=t.ask, stop=t.ask - dist, target=None, reward_risk=2.0, headroom_pips=10,
                         cost_pips=1)
        equity = rng.choice([1865.61, 500.0, 10_000.0, rng.uniform(100, 50_000)])
        acct = dataclasses.replace(sim.account(), equity=equity, balance=equity, margin_free=equity * 50,
                                   margin=0.0)
        money = rng.choice([6.53, 13.0, 27.98, 1000.0, 1e9, rng.uniform(0.1, 500)])
        r = dataclasses.replace(cfg.risk, max_risk_money=money)
        res = rm.size(st, spec, acct, [], money / equity * 100.0, None, 0.0, top=False, risk=r)
        if res.ok:
            checked += 1
            assert res.risk_money <= min(money, equity * cfg.risk.max_risk_pct / 100.0) + 0.01, (sym, money, res)
    assert checked > 500


def test_the_step_table_never_exceeds_the_ceiling():
    cfg = Config()
    base_pct, cap = base_risk(cfg.risk)
    for name, pct in STEPS_PCT:
        assert step_pct(name, base_pct) <= cfg.risk.max_risk_pct + 1e-12
        for equity in (100.0, 1865.61, 2000.0, 1e6):
            assert step_money(name, equity, base_pct, cap) <= equity * cfg.risk.max_risk_pct / 100.0 + 0.01
    assert step_money("trial", 1e6, base_pct, cap) == 8.0                  # never above £8
    assert step_money("base", 1865.61, base_pct, cap) == 13.0              # 0.70% capped at £13
    assert step_money("S4", 1865.61, base_pct, cap) == pytest.approx(27.98)
    # the demo ladder (section 21): after S4, never skipping, never above 5%
    pcts = [step_pct(n, base_pct) for n in STEP_ORDER]
    assert pcts == sorted(pcts) and len(set(pcts)) == len(pcts)
    assert max(p for _, p in DEMO_STEPS_PCT) == DEMO_MAX_STEP_PCT == 5.0
    assert list(DEMO_DAILY_STOP_LADDER) == sorted(DEMO_DAILY_STOP_LADDER) and DEMO_DAILY_STOP_LADDER[0] == 3.0


def oversized(pb, sim, coach_route="REAL", volume=50.0, symbol="US500"):
    t = sim.tick(symbol)
    stop = sim.spec(symbol).normalise_price(t.bid - 30.0)
    return pb.send(OrderRequest(symbol=symbol, side=Side.BUY, volume=volume, sl=stop, magic=TNB, comment="x",
                                idempotency_key="x", tactic="SESSION_EXPANSION", coach_route=coach_route))


def test_paper_broker_send_refuses_an_oversized_real_request(tmp_path, caplog):
    sim, cfg, pb, tr = build(tmp_path)
    pb.coach_confirm = lambda req, kind=None: True                   # whatever the envelope says
    with caplog.at_level(logging.CRITICAL, logger="mintel.paper"):
        res = oversized(pb, sim)
    assert not res.ok and "ceiling" in res.comment
    assert not sim.order_log                                         # nothing was sent
    assert any("REFUSED a real order" in r.getMessage() for r in caplog.records)
    # a small one goes through
    ok = oversized(pb, sim, volume=0.1)
    assert ok.ok and sim.order_log and not pb.is_paper(ok.ticket) and pb.is_coach_real(ok.ticket)
    # Aaron's own live-group order is not checked here: it goes exactly as before the coach (RiskManager.size
    # sized it under config.json's ceiling; a price move between scan and send must not refuse it)
    t = sim.tick("EURGBP")
    big = pb.send(OrderRequest(symbol="EURGBP", side=Side.BUY, volume=50.0,
                               sl=sim.spec("EURGBP").normalise_price(t.bid - 0.0050), magic=TNB, comment="y",
                               idempotency_key="y", tactic="MOMENTUM_CONTINUATION"))
    assert big.ok and len(sim.order_log) == 2 and not pb.is_paper(big.ticket) and not pb.is_coach_real(big.ticket)
    tr.journal.close()
    pb.close_db()


def test_without_the_trader_attaching_a_ceiling_the_wrapper_is_as_before(tmp_path):
    from tests.test_tnb_paper_wiring import paper_on, setup
    sim, cfg = setup(tmp_path, symbols=("EURGBP", "US500"))
    cfg.tnb.live_groups = ("FX_MINOR",)
    pb = paper_on(sim, cfg)
    assert pb.ceiling_pct is None and pb.coach_confirm is None
    t = sim.tick("EURGBP")
    res = pb.send(OrderRequest(symbol="EURGBP", side=Side.BUY, volume=5.0,
                               sl=sim.spec("EURGBP").normalise_price(t.bid - 0.0050), magic=TNB, comment="z",
                               idempotency_key="z"))
    assert res.ok and not pb.is_paper(res.ticket)
    # a coach REAL order with no envelope check attached never goes real
    res = oversized(pb, sim, volume=0.1)
    assert res.ok and pb.is_paper(res.ticket)
    pb.close_db()


def test_a_policy_granting_1000_pounds_gives_at_most_one_and_a_half_percent(tmp_path):
    """The coach's own view of the equity was 100,000 (so £1,000 passed its checks at 1%); the account
    holds 10,000. The gate clamps to 1.5% of the LIVE equity, the sizing holds it after lot rounding."""
    def coach(cfg, sim):
        put_policy(cfg, full_policy(cfg, sim, equity=100_000.0, headroom_gbp=None,
                                    entries={US500_KEY: Entry("REAL", "LIVE", 1.5, 1000.0, step="S4")}),
                   now=sim.now)
    sim, cfg, pb, tr = build(tmp_path, prepare=coach)
    state = entry(sim, "US500", 10.0, tactic="SESSION_EXPANSION")
    equity = tr.broker.account().equity                              # the equity it was sized on
    acted, _ = act_on(tr, sim, state)
    (order,) = [o for o in sim.order_log if o["req"].magic == TNB]
    risk = sim.spec("US500").money_per_lot(abs(state.entry - state.stop)) * order["req"].volume
    assert order["req"].coach_route == "REAL" and 0 < risk <= equity * 1.5 / 100.0 + 0.01
    d = tr.coach_gate.decide("SESSION_EXPANSION", "INDEX", "TREND", "US500", "BUY", 80, 2, sim.now, False,
                             RiskCtx(equity=equity))
    assert d.money_gbp <= equity * 0.015 + 1e-9 and d.risk_pct <= 1.5
    tr.journal.close()
    pb.close_db()


def test_the_trader_never_sends_a_grant_sized_above_its_money(tmp_path, monkeypatch):
    """The trader's own check: a sizing above the grant (a bug anywhere below) goes to practice."""
    def coach(cfg, sim):
        put_policy(cfg, full_policy(cfg, sim, entries={US500_KEY: Entry("REAL", "LIVE_TRIAL", 0.35, 6.53,
                                                                        step="trial")}), now=sim.now)
    sim, cfg, pb, tr = build(tmp_path, prepare=coach)
    real_size = tr.risk.size

    def too_big(*a, **kw):
        res = real_size(*a, **kw)
        return dataclasses.replace(res, risk_money=res.risk_money + 50.0) if kw.get("risk") is not None else res
    monkeypatch.setattr(tr.risk, "size", too_big)
    act_on(tr, sim, entry(sim, "US500", 10.0, tactic="SESSION_EXPANSION"))
    assert not [o for o in sim.order_log if o["req"].magic == TNB]
    assert all(pb.is_paper(p.ticket) for p in pb.positions(TNB))
    tr.journal.close()
    pb.close_db()


def test_the_coach_never_assigns_the_ceiling_or_the_stop():
    """A source check: nothing in the coach's package, the gate's hooks or the bots assigns the daily stop,
    the ceiling, the practice switch or the top size in config (they are only ever read)."""
    import re
    root = Path(__file__).resolve().parents[1] / "mintel"
    files = list((root / "coach").glob("*.py")) + [root / "engine" / "trader.py", root / "broker" / "paper.py",
                                                    root / "ian" / "engine.py", root / "ian" / "run.py"]
    pat = re.compile(r"\.(max_daily_loss_pct|max_risk_pct|daily_loss_counts_practice|top_size_enabled)\s*=[^=]")
    for f in files:
        for i, line in enumerate(f.read_text().splitlines(), 1):
            assert not pat.search(line), f"{f.name}:{i}: {line.strip()}"
        text = f.read_text()
        assert "secrets.json\")" not in text and "open(\"config.json\"" not in text


def test_a_grant_refused_at_the_ceiling_is_in_the_gate_log(tmp_path):
    """Spec 10.4: a REAL order above the ceiling reaches PaperBroker.send - refused, logged CRITICAL, and the
    coach is told through the gate log."""
    def coach(cfg, sim):
        put_policy(cfg, full_policy(cfg, sim, entries={US500_KEY: Entry("REAL", "LIVE_TRIAL", 0.35, 6.53,
                                                                        step="trial")}), now=sim.now)
    sim, cfg, pb, tr = build(tmp_path, prepare=coach)
    pb.ceiling_pct = lambda: 0.001                                    # as if the order were far too big
    acted, blocked = act_on(tr, sim, entry(sim, "US500", 10.0, tactic="SESSION_EXPANSION"), must=False)
    assert acted is None and any("ceiling" in b for b in blocked) and not sim.order_log
    rows = [json.loads(x) for x in (Path(cfg.ops.data_dir) / "coach" / "gate" / f"tnb-{sim.now.date()}.jsonl")
            .read_text().splitlines()]
    assert rows[-1]["event"] == "refused_at_ceiling" and rows[-1]["key"] == US500_KEY
    tr.journal.close()
    pb.close_db()


def test_a_gate_that_cannot_be_built_still_honours_halt(tmp_path, monkeypatch):
    import mintel.coach.gate as gate_mod

    def broken(*a, **kw):
        raise RuntimeError("boom")
    monkeypatch.setattr(gate_mod.CoachGate, "__init__", broken)
    sim, cfg, pb, tr = build(tmp_path, live_tactics=("MOMENTUM_CONTINUATION",))
    assert isinstance(tr.coach_gate, gate_mod.BaselineGate)
    (Path(cfg.ops.data_dir) / "HALT").write_text("YES")
    act_on(tr, sim, entry(sim, "EURGBP", 0.0030, tactic="MOMENTUM_CONTINUATION"))
    assert not sim.order_log and all(pb.is_paper(p.ticket) for p in pb.positions(TNB))
    tr.journal.close()
    pb.close_db()
