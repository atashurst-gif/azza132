"""Rapid Scalper: a second strategy that cannot touch the first.

Every isolation claim in the specification has a test here, plus the
invariants: the GBP 10 planned-loss ceiling, the never-backwards hard stop,
duplicate protection, fail-closed modes, and the attribution arithmetic.
"""
from __future__ import annotations

import datetime as dt
import json
import random
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

import pytest

from mintel.broker.base import AccountInfo, OrderRequest, OrderResult, Position, Side, Tick, TF
from mintel.broker.sim import SimBroker
from mintel.config import Config
from mintel.scalper import STRATEGY_ID, EXISTING_STRATEGY_ID
from mintel.scalper.config import ScalperConfig
from mintel.scalper.engine import ScalperEngine
from mintel.scalper.execution import LiveExecutor, PaperExecutor, make_executor
from mintel.scalper.features import TickBuffer, compute
from mintel.scalper.journal import ScalperJournal
from mintel.scalper.manage import Manager, ScalpTrade, StopInvariantError
from mintel.scalper.risk import Breakers, account_safety, plan
from mintel.scalper.score import score
from mintel.scalper.stats import combine, strategy_stats

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 10, 1, 9, 0, tzinfo=UTC)
MI_MAGIC = Config().magic


def sim_broker():
    s = SimBroker(["EURUSD", "GBPUSD"], start=T0 - dt.timedelta(days=2), history_bars=3000)
    s.connect()
    return s


def synthetic_ticks(n_minutes=35, impulse=True, base=1.1000, spread=0.00012, seed=3):
    """One tick a second: a calm drift, then a clean impulse in the last minute."""
    rng = random.Random(seed)
    out = []
    px = base
    for i in range(n_minutes * 60):
        t = T0 - dt.timedelta(minutes=n_minutes) + dt.timedelta(seconds=i)
        last_min = i >= (n_minutes - 1) * 60
        drift = (0.00004 if (impulse and last_min) else 0.000002) + rng.gauss(0, 0.000004)
        px += drift
        out.append(Tick("EURUSD", t, round(px - spread / 2, 5), round(px + spread / 2, 5), px, 3.0 if last_min else 1.0))
    return out


@dataclass
class FakeBroker:
    """A scripted broker: scalper and existing positions live side by side,
    tagged by magic, exactly as at MetaTrader."""
    ticks_by_symbol: dict = field(default_factory=dict)
    positions_: dict = field(default_factory=dict)
    sent: list = field(default_factory=list)
    modified: list = field(default_factory=list)
    closed: list = field(default_factory=list)
    seq: int = 500
    sim: SimBroker = field(default_factory=sim_broker)
    now: dt.datetime = T0
    connected: bool = True

    def is_connected(self): return self.connected
    def connect(self): return True
    def account(self):
        return AccountInfo(1, "demo", "GBP", 1000.0, 1000.0, 10.0, 990.0, 5000.0, 30, True, True)
    def spec(self, symbol): return self.sim.spec("EURUSD")
    def tick(self, symbol):
        series = self.ticks_by_symbol.get(symbol) or []
        live = [t for t in series if t.time <= self.now]
        return live[-1] if live else None
    def bars(self, symbol, tf, count, end=None):
        from mintel.scalper.backtest import bars_from_ticks
        return bars_from_ticks([t for t in self.ticks_by_symbol.get(symbol, []) if t.time <= self.now])[-count:]
    def positions(self, magic=None):
        return [p for p in self.positions_.values() if magic is None or p.magic == magic]
    def send(self, req: OrderRequest):
        self.sent.append(req)
        self.seq += 1
        t = self.tick(req.symbol)
        px = t.ask if req.side is Side.BUY else t.bid
        self.positions_[self.seq] = Position(self.seq, req.symbol, req.side, req.volume, px, req.sl, req.tp,
                                             self.now, magic=req.magic, comment=req.comment)
        return OrderResult(True, 10009, ticket=self.seq, price=px, filled_volume=req.volume)
    def modify_stops(self, ticket, sl, tp):
        self.modified.append((ticket, sl, tp))
        p = self.positions_[ticket]; p.sl = sl
        return OrderResult(True, 10009, ticket=ticket)
    def close(self, ticket, volume=0.0, comment=""):
        self.closed.append((ticket, volume, comment))
        p = self.positions_.pop(ticket)
        t = self.tick(p.symbol)
        return OrderResult(True, 10009, ticket=ticket, price=t.bid if p.side is Side.BUY else t.ask, filled_volume=p.volume)
    def calendar(self, start, end, currencies=()): return []
    def closed_deal(self, ticket): return None
    def deals_since(self, since, magic=0, closing_only=True): return []


def scfg(**kw) -> ScalperConfig:
    base = dict(symbols=("EURUSD",), min_ticks_per_minute=5.0, min_confidence=40.0)
    base.update(kw)
    return ScalperConfig(**base)


# ============================================================ 6 / 7: the GBP 10 ceiling --
class TestTenPoundCeiling:
    def test_size_keeps_the_planned_loss_at_or_under_ten(self):
        spec = sim_broker().spec("EURUSD")
        rp = plan(spec, Side.BUY, 1.1000, 1.0990, 0.00012, scfg())          # 100-point stop
        assert rp.ok, rp.reason
        assert rp.planned_loss <= 10.0 + 1e-9
        assert rp.volume >= spec.volume_min
        # it is sized from the REAL distance: stop + spread + slippage + commission
        assert rp.effective_distance_points > rp.stop_distance_points
        assert rp.per_lot_loss > spec.money_per_lot(0.0010)

    def test_minimum_lot_over_the_ceiling_means_no_trade(self):
        spec = sim_broker().spec("EURUSD")
        c = scfg(planned_max_trade_risk_gbp=0.20)
        rp = plan(spec, Side.BUY, 1.1000, 1.0990, 0.00012, c)
        assert not rp.ok and "minimum size" in rp.reason

    def test_stop_is_never_moved_to_fit_a_size(self):
        spec = sim_broker().spec("EURUSD")
        rp = plan(spec, Side.SELL, 1.1000, 1.1012, 0.00012, scfg())
        assert rp.ok and rp.stop == 1.1012

    def test_wrong_side_and_noise_floor_rejected(self):
        spec = sim_broker().spec("EURUSD")
        assert not plan(spec, Side.BUY, 1.1000, 1.1010, 0.0001, scfg()).ok
        assert not plan(spec, Side.BUY, 1.1000, 1.09995, 0.0001, scfg()).ok


# ============================================================ 8 / 34: the stop invariant --
class TestHardStopNeverMovesBackwards:
    def _trade(self, side=Side.BUY):
        spec = sim_broker().spec("EURUSD")
        entry, stop = (1.1000, 1.0990) if side is Side.BUY else (1.1000, 1.1010)
        return ScalpTrade(1, "EURUSD", side, 0.1, entry, entry, T0, stop, stop, 0.0010, 10.0, spec.point, spec=spec)

    def test_enforce_raises_on_a_backward_move(self):
        t = self._trade()
        with pytest.raises(StopInvariantError):
            Manager.enforce_monotonic(t, 1.0985)
        s = self._trade(Side.SELL)
        with pytest.raises(StopInvariantError):
            Manager.enforce_monotonic(s, 1.1015)

    def test_fuzz_random_candidates_only_ever_tighten(self):
        rng = random.Random(1)
        m = Manager(scfg())
        for side in (Side.BUY, Side.SELL):
            t = self._trade(side)
            price = 1.1000
            for _ in range(500):
                price += rng.uniform(-0.0003, 0.0004) * side.sign
                cand = price - side.sign * rng.uniform(0.0001, 0.0030)
                before = t.stop
                m._advance(t, cand, "fuzz", T0, price)
                # Invariant: the stop only ever tightens. A stop that ends up on the
                # wrong side of price after a move against us is a stop HIT, not a
                # backwards move, so the price check only applies to newly accepted
                # candidates.
                if side is Side.BUY:
                    assert t.stop >= before - 1e-12
                else:
                    assert t.stop <= before + 1e-12
                if t.stop != before:
                    assert (price - t.stop) * side.sign > 0

    def test_protected_floor_never_retreats(self):
        m = Manager(scfg())
        t = self._trade()
        m._advance(t, 1.1005, "lock", T0, 1.1020)
        floor = t.protected_floor_money
        assert floor > 0
        m._advance(t, 1.0999, "worse", T0, 1.1020)          # refused silently
        assert t.protected_floor_money == floor and t.stop == 1.1005


# ==================================================================== behaviour --
class TestProveItAndRunner:
    def _trade(self):
        spec = sim_broker().spec("EURUSD")
        return ScalpTrade(1, "EURUSD", Side.BUY, 0.1, 1.1000, 1.1000, T0, 1.0990, 1.0990, 0.0010, 10.0, spec.point, spec=spec)

    def test_failed_thesis_exits_well_before_the_full_stop(self):
        m = Manager(scfg(prove_it_seconds=3.0))
        t = self._trade()
        against = SimpleNamespace(velocity_5s=-0.5, persistence=0.2, trend_bias=0, volume_accel=1.0, acceleration=0.0,
                                  realised_vol_points=2.0, bar_range_ratio=1.0, consecutive_ticks=-6, micro_low=1.0995, micro_high=1.1003)
        # 0.85R against on the mid and the tape still running at the stop: certainly going there
        d = m.update(t, Tick("EURUSD", T0 + dt.timedelta(seconds=2), 1.09909, 1.09921), against, T0 + dt.timedelta(seconds=2))
        assert d.close and d.exit_reason == "THESIS_FAILED" and "certainly" in d.reason
        assert t.r_of(1.09909) > -1.0                      # cut before the full 1R, not at it

    def test_no_progress_after_the_proving_window_is_a_failed_thesis(self):
        m = Manager(scfg(prove_it_seconds=3.0))
        t = self._trade()
        f = SimpleNamespace(velocity_5s=-0.2, persistence=0.2, trend_bias=0, volume_accel=1.0, acceleration=0.0,
                            realised_vol_points=2.0, bar_range_ratio=1.0, consecutive_ticks=-2, micro_low=1.0995, micro_high=1.1003)
        # flat after the window but not under water: LEFT ALONE, the stop is the exit
        d = m.update(t, Tick("EURUSD", T0 + dt.timedelta(seconds=4), 1.10001, 1.10013), f, T0 + dt.timedelta(seconds=4))
        assert not d.close
        # 0.55R under on the mid, no progress ever, still moving against: that is a failed thesis
        f.consecutive_ticks = -5
        d = m.update(t, Tick("EURUSD", T0 + dt.timedelta(seconds=5), 1.09939, 1.09951), f, T0 + dt.timedelta(seconds=5))
        assert d.close and d.exit_reason == "THESIS_FAILED"

    def test_states_advance_and_the_stop_follows(self):
        m = Manager(scfg())
        t = self._trade()
        f = SimpleNamespace(velocity_5s=3.0, persistence=0.9, trend_bias=1, volume_accel=1.8, acceleration=0.5,
                            realised_vol_points=1.0, bar_range_ratio=2.0, consecutive_ticks=6, micro_low=1.1000, micro_high=1.1030)
        seen = []
        for i, px in enumerate([1.1004, 1.1007, 1.1011, 1.1017, 1.1026, 1.1030]):
            f.micro_low = px - 0.0006
            d = m.update(t, Tick("EURUSD", T0 + dt.timedelta(seconds=10 + i), px, px + 0.00012), f, T0 + dt.timedelta(seconds=10 + i))
            seen.append(t.state)
            assert not d.close
        assert seen[0] == "INITIAL_RISK" and "CAPITAL_SAFE" in seen and seen[-1] in ("PROFIT_PROTECTED", "RUNNER")
        assert t.stop > 1.1000, "money has been protected"
        assert t.protected_floor_money > 0
        assert t.runner_probability > 0
        if t.state == "RUNNER":
            assert t.exit_tolerance == "WIDENED"

    def test_momentum_decay_exit_when_the_reason_is_gone(self):
        m = Manager(scfg())
        t = self._trade()
        strong = SimpleNamespace(velocity_5s=3.0, persistence=0.9, trend_bias=1, volume_accel=1.8, acceleration=0.5,
                                 realised_vol_points=1.0, bar_range_ratio=2.0, consecutive_ticks=6, micro_low=1.0998, micro_high=1.1030)
        for i, px in enumerate([1.1004, 1.1008, 1.1012, 1.1016]):
            m.update(t, Tick("EURUSD", T0 + dt.timedelta(seconds=10 + i), px, px + 0.0001), strong, T0 + dt.timedelta(seconds=10 + i))
        dead = SimpleNamespace(**{**strong.__dict__, "velocity_5s": -2.5, "consecutive_ticks": -6, "micro_low": 1.0998})
        d = m.update(t, Tick("EURUSD", T0 + dt.timedelta(seconds=20), 1.1012, 1.1013), dead, T0 + dt.timedelta(seconds=20))
        assert d.close and d.exit_reason == "MOMENTUM_DECAY" and "reversed" in d.reason


# ============================================================ score and features --
class TestScoreIsMathematical:
    def test_every_component_is_logged_and_chop_is_detected(self):
        buf = TickBuffer("EURUSD")
        ticks = synthetic_ticks()
        for t in ticks:
            buf.add(t)
        from mintel.scalper.backtest import bars_from_ticks
        f = compute("EURUSD", buf, bars_from_ticks(ticks)[:-1], 0.00001, ticks[-1].time)
        assert f.ok and f.velocity_5s > 0 and f.ticks_per_minute >= 59
        assert f.order_flow_available is False
        opp = score(f, scfg())
        assert set(opp.contributions) >= {"Momentum", "Trend alignment", "Retest quality", "Volume", "Spread",
                                          "Liquidity", "Order flow (disabled)"}
        assert opp.contributions["Order flow (disabled)"] == 0.0
        assert 0 <= opp.confidence <= 100
        assert "Rapid Scalper opportunity" in opp.explain()

    def test_chop_blocks_and_wide_spread_blocks(self):
        buf = TickBuffer("EURUSD")
        ticks = synthetic_ticks(impulse=False, spread=0.0030)
        for t in ticks:
            buf.add(t)
        from mintel.scalper.backtest import bars_from_ticks
        f = compute("EURUSD", buf, bars_from_ticks(ticks)[:-1], 0.00001, ticks[-1].time)
        opp = score(f, scfg())
        assert not opp.tradable
        assert any("spread" in b for b in opp.blockers)


# ================================================================ breakers / modes --
class TestFailClosed:
    def test_stale_feed_disconnect_and_losses_block(self):
        b = Breakers(scfg(max_consecutive_losses=2))
        assert any("stale" in w for w in b.check(T0, tick_age_seconds=30.0, connected=True, latency_ms=10))
        assert any("not connected" in w for w in b.check(T0, tick_age_seconds=1.0, connected=False, latency_ms=10))
        assert any("no market data" in w for w in b.check(T0, tick_age_seconds=None, connected=True, latency_ms=10))
        b.record_result(-5, T0); b.record_result(-5, T0)
        assert any("losses in a row" in w for w in b.check(T0, tick_age_seconds=1.0, connected=True, latency_ms=10))
        b.reconciliation_ok = False
        assert any("reconciled" in w for w in b.check(T0, tick_age_seconds=1.0, connected=True, latency_ms=10))

    def test_slippage_deterioration_rests_then_measures_again(self):
        b = Breakers(scfg(max_avg_slippage_points=2.0))
        for _ in range(4):
            b.record_slippage(5.0, spread_points=1.0)
        assert any("slippage" in w for w in b.check(T0, tick_age_seconds=1.0, connected=True, latency_ms=10))
        # a timed rest, not a lock
        assert any("resting" in w for w in b.check(T0 + dt.timedelta(minutes=5), tick_age_seconds=1.0, connected=True, latency_ms=10))
        assert not any("slippage" in w or "resting" in w
                       for w in b.check(T0 + dt.timedelta(minutes=16), tick_age_seconds=1.0, connected=True, latency_ms=10))

    def test_gold_slippage_is_judged_against_golds_spread_not_four_cents(self):
        # 2 October: 4.8 points of slippage on gold (a point is 0.01) with a 15-point
        # spread paused the scalper for eight hours. One spread is the real yardstick.
        b = Breakers(scfg(max_avg_slippage_points=4.0))
        for _ in range(4):
            b.record_slippage(4.8, spread_points=15.0)
        assert not any("slippage" in w for w in b.check(T0, tick_age_seconds=1.0, connected=True, latency_ms=10))
        for _ in range(4):
            b.record_slippage(60.0, spread_points=15.0)        # well over 1.5 spreads: that IS poor
        assert any("slippage" in w for w in b.check(T0, tick_age_seconds=1.0, connected=True, latency_ms=10))

    def test_unknown_mode_is_off_and_default_is_paper(self, tmp_path):
        assert ScalperConfig().mode == "PAPER"
        p = tmp_path / "scalper.json"
        p.write_text(json.dumps({"mode": "banana"}))
        assert ScalperConfig.load(p).mode == "OFF"
        c = ScalperConfig(mode="LIVE"); c.save(p)
        assert ScalperConfig.load(p).mode == "LIVE" and ScalperConfig.load(tmp_path / "missing.json").mode == "PAPER"

    def test_off_has_no_executor_and_paper_never_touches_the_broker(self):
        class Spy:
            def send(self, *a, **k): raise AssertionError("PAPER sent an order")
            def modify_stops(self, *a, **k): raise AssertionError("PAPER modified a stop at the broker")
            def close(self, *a, **k): raise AssertionError("PAPER closed at the broker")
            def positions(self, magic=None): return []
        assert make_executor(ScalperConfig(mode="OFF"), Spy()) is None
        ex = make_executor(ScalperConfig(mode="PAPER"), Spy())
        assert isinstance(ex, PaperExecutor)
        spec = sim_broker().spec("EURUSD")
        tick = Tick("EURUSD", T0, 1.1000, 1.1001)
        fill = ex.open("EURUSD", Side.BUY, 0.1, 1.0990, tick, spec, T0)
        assert fill.ok and ex.modify_stop(fill.ticket, 1.0995) and ex.close(fill.ticket, tick, spec).ok

    def test_account_safety_blocks_but_never_touches_the_other_bot(self):
        acct = AccountInfo(1, "d", "GBP", 1000, 1000, 10, 990, 5000, 30, True, True)
        mi = [Position(1, "EURUSD", Side.BUY, 0.1, 1.1, 1.09, 0, T0, magic=MI_MAGIC)]
        ok, why = account_safety(scfg(), acct, mi, [], 5.0, 0.0, 0.0, 0.0, "EURUSD", Side.BUY)
        assert not ok and "other strategy" in why
        ok, why = account_safety(scfg(), acct, mi, [], 55.0, 0.0, 0.0, 0.0, "GBPUSD", Side.BUY)
        assert not ok and "open risk" in why
        ok, why = account_safety(scfg(max_daily_loss_gbp=10), acct, [], [], 0, 0, -11.0, -11.0, "GBPUSD", Side.BUY)
        assert not ok and "daily loss" in why
        assert mi[0].sl == 1.09 and mi[0].volume == 0.1


# ================================================================ isolation (1, 2, 9, 10) --
def _engine(broker, tmp_path, mode="LIVE", **kw):
    cfg = Config(); cfg.ops.data_dir = str(tmp_path); cfg.ops.log_dir = str(tmp_path)
    c = scfg(mode=mode, **kw)
    return ScalperEngine(cfg, c, broker, clock=lambda: broker.now, data_dir=tmp_path)


class TestIsolation:
    def test_scalper_never_adopts_or_touches_existing_bot_positions(self, tmp_path):
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        b.positions_[1] = Position(1, "EURUSD", Side.BUY, 0.3, 1.0950, 1.0900, 0.0, T0, magic=MI_MAGIC)
        e = _engine(b, tmp_path)
        for _ in range(3):
            e.cycle()
        assert 1 not in e.trades
        assert b.positions_[1].sl == 1.0900 and b.positions_[1].volume == 0.3
        assert not b.modified and not b.closed

    def test_existing_bot_never_sees_scalper_positions(self):
        from tests.conftest import make_trader
        sim = sim_broker()
        cfg = Config()
        t = make_trader(sim, cfg)
        tick = sim.tick("EURUSD")
        res = sim.send(OrderRequest("EURUSD", Side.BUY, 0.05, sl=tick.ask - 0.0020, tp=0.0, magic=ScalperConfig().magic, comment="RS"))
        assert res.ok
        before = [p for p in sim.positions() if p.ticket == res.ticket][0]
        sl_before = before.sl
        t.manage_cycle()
        after = [p for p in sim.positions() if p.ticket == res.ticket][0]
        assert after.sl == sl_before and after.volume == 0.05
        assert res.ticket not in t.flowlock.trackers
        assert all(p.magic == cfg.magic for p in sim.positions(cfg.magic))

    def test_duplicate_protection_one_trade_per_symbol(self, tmp_path):
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        e = _engine(b, tmp_path, min_confidence=0.0, max_open_positions=3, max_stop_points=400.0)
        from mintel.scalper.score import Opportunity
        opp = Opportunity("EURUSD", 1, 90.0)
        buf = e.buffers["EURUSD"]
        for t in synthetic_ticks():
            buf.add(t)
        e._bars_for("EURUSD", 100.0)
        from mintel.scalper.features import compute as fc
        opp.features = fc("EURUSD", buf, e.bars["EURUSD"], 0.00001, b.now)
        opp.expected_move_points = 300.0
        ticks = {"EURUSD": buf.last}
        first = e._try_open(opp, b.now, ticks)
        assert len(e.trades) == 1, first
        second = e._try_open(opp, b.now + dt.timedelta(seconds=60), ticks)
        assert len(e.trades) == 1 and "already" in second
        assert len(b.sent) == 1

    def test_reconnection_adopts_rather_than_duplicating(self, tmp_path):
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        b.positions_[7] = Position(7, "EURUSD", Side.BUY, 0.05, 1.1000, 1.0990, 0.0, T0, magic=ScalperConfig().magic)
        e = _engine(b, tmp_path)
        e._reconcile(b.now); e._reconcile(b.now)
        assert list(e.trades) == [7] and len(b.sent) == 0

    def test_a_scalper_position_without_a_broker_stop_is_closed_at_once(self, tmp_path):
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        b.positions_[8] = Position(8, "EURUSD", Side.BUY, 0.05, 1.1000, 0.0, 0.0, T0, magic=ScalperConfig().magic)
        e = _engine(b, tmp_path)
        e._reconcile(b.now)
        assert 8 not in e.trades and b.closed and b.closed[0][0] == 8

    def test_live_orders_carry_the_scalper_magic_and_a_stop(self, tmp_path):
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        ex = LiveExecutor(ScalperConfig(), b)
        spec = b.spec("EURUSD")
        fill = ex.open("EURUSD", Side.BUY, 0.05, 1.0990, b.tick("EURUSD"), spec, T0)
        assert fill.ok and b.sent[0].magic == ScalperConfig().magic != MI_MAGIC and b.sent[0].sl == 1.0990

    def test_off_engine_opens_nothing(self, tmp_path):
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        e = _engine(b, tmp_path, mode="OFF")
        for _ in range(3):
            e.cycle()
        assert e.executor is None and not b.sent and not e.trades
        assert e.blocked_because == ["Rapid Scalper is OFF"]

    def test_disabling_the_scalper_leaves_the_existing_bot_alone(self, tmp_path):
        from mintel.ops import watchdog as wd
        cfg = Config(); cfg.ops.data_dir = str(tmp_path); cfg.ops.log_dir = str(tmp_path)
        ScalperConfig(mode="OFF").save(ScalperConfig.default_path(tmp_path))
        w = wd.build_default(cfg, str(tmp_path / "config.json"))
        assert [p.name for p in w.processes] == ["trader"]
        ScalperConfig(mode="PAPER").save(ScalperConfig.default_path(tmp_path))
        w2 = wd.build_default(cfg, str(tmp_path / "config.json"))
        assert [p.name for p in w2.processes] == ["trader", "scalper"]
        assert w.processes[0].command == w2.processes[0].command, "the trader's command is identical either way"
        # and the existing strategy's code never imports the scalper
        root = Path(__file__).parent.parent / "mintel"
        for f in ("engine/trader.py", "engine/scanner.py", "engine/flowlock.py", "engine/execution.py",
                  "engine/risk.py", "engine/tactics.py", "config.py"):
            assert "scalper" not in (root / f).read_text(), f
        assert "scalper" not in json.dumps(Config().__dict__, default=str)


# ================================================================ attribution (3, 4, 5) --
class TestAttribution:
    def test_each_bot_in_its_own_column_and_overall_is_the_sum(self, tmp_path):
        from mintel.engine.journal import Journal
        from mintel.ops.attribution import build_strategies
        j = Journal(tmp_path / "journal.sqlite")
        j._exec("INSERT INTO trades (ticket, symbol, side, opened_utc, closed_utc, pnl_money, tactic, regime, "
                "opportunity, tier, exit_reason, realised_r, mfe_r, mae_r, final_flow_state) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (1, "EURUSD", "BUY", "2026-10-01T08:00:00+00:00", "2026-10-01T08:30:00+00:00", 6.5, "SESSION_EXPANSION",
                 "TREND", 70.0, "NORMAL", "[tp 1.1]", 2.0, 2.0, 0.1, "STRONG_FLOW"))
        status = {"updated": T0.isoformat(), "status": "SCANNING", "mode": "PAPER",
                  "stats": strategy_stats([{"net_pnl": -4.0, "duration_seconds": 8.0, "commission": 0.3, "peak_r": 0.3, "mae_r": 1.0,
                                            "exit_reason": "THESIS_FAILED"},
                                           {"net_pnl": 12.0, "duration_seconds": 95.0, "commission": 0.3, "peak_r": 3.1, "mae_r": 0.2,
                                            "exit_reason": "PROFIT_TRAIL", "reached_runner": 1, "reached_protected": 1}],
                                          [], label="Rapid Scalper", strategy_id=STRATEGY_ID, scalper=True),
                  "trades_today": [{"ticket": 900000001, "symbol": "GBPUSD", "net": -4.0, "closed": "2026-10-01T09:00:00", "strategy": STRATEGY_ID},
                                   {"ticket": 900000002, "symbol": "EURUSD", "net": 12.0, "closed": "2026-10-01T09:10:00", "strategy": STRATEGY_ID}]}
        (tmp_path / "scalper-status.json").write_text(json.dumps(status))
        s = build_strategies(j, [], T0.replace(hour=0), "GBP", tmp_path, now=T0)
        mi, rs, ov = s[EXISTING_STRATEGY_ID], s[STRATEGY_ID], s["overall"]
        assert mi["trades"] == 1 and mi["net_today"] == 6.5
        assert rs["trades"] == 2 and rs["net_today"] == 8.0 and rs["reached_runner"] == 1 and rs["exits_under_10s"] == 1
        assert ov["net_today"] == pytest.approx(14.5) and ov["trades"] == 3 and ov["wins"] == 2
        assert {t["strategy"] for t in ov["trades_today"]} == {EXISTING_STRATEGY_ID, STRATEGY_ID}
        assert all(t["strategy"] == EXISTING_STRATEGY_ID for t in mi["trades_today"])
        assert all(t["strategy"] == STRATEGY_ID for t in rs["trades_today"])
        j.close()

    def test_a_missing_or_stale_scalper_reads_as_not_running(self, tmp_path):
        from mintel.ops.attribution import read_scalper_status
        assert read_scalper_status(tmp_path)["status"] == "NOT RUNNING"
        (tmp_path / "scalper-status.json").write_text(json.dumps({"updated": (T0 - dt.timedelta(minutes=10)).isoformat(), "stats": {}}))
        assert "NOT RUNNING" in read_scalper_status(tmp_path, now=T0)["status"]

    def test_page_shows_the_tabs_and_which_bot_made_each_trade(self):
        from mintel.ops.dashboard import render_status
        mi = strategy_stats([{"net_pnl": 3.0, "duration_seconds": 100}], [], label="Trend & Breakout", strategy_id=EXISTING_STRATEGY_ID)
        mi["trades_today"] = [{"ticket": 1, "symbol": "EURUSD", "net": 3.0, "closed": "2026-10-01T09:00:00", "tactic": "SESSION_EXPANSION", "strategy": EXISTING_STRATEGY_ID}]
        rs = strategy_stats([{"net_pnl": -4.0, "duration_seconds": 8}], [], label="Rapid Scalper", strategy_id=STRATEGY_ID, scalper=True)
        rs["trades_today"] = [{"ticket": 2, "symbol": "GBPUSD", "net": -4.0, "closed": "2026-10-01T09:05:00", "exit_reason": "THESIS_FAILED", "strategy": STRATEGY_ID}]
        snap = {"status": {"bot": "RUNNING"}, "health": {}, "thinking": [], "results": {}, "positions": [], "events": [],
                "strategies": {"overall": combine([mi, rs]), EXISTING_STRATEGY_ID: mi, STRATEGY_ID: rs,
                               "scalper": {"status": "SCANNING", "mode": "PAPER", "tagline": "x", "position": None, "blocked_because": []},
                               "labels": {"overall": "Overall", EXISTING_STRATEGY_ID: "Trend & Breakout", STRATEGY_ID: "Rapid Scalper"}}}
        page = render_status(snap, "overall")
        for label in ("Overall", "Trend &amp; Breakout", "Rapid Scalper", "RAPID SCALPER", "THESIS_FAILED", "SESSION_EXPANSION"):
            assert label in page, label
        assert "Exits under 10 s" in render_status(snap, STRATEGY_ID)


# ============================================================ paper end to end + ledger --
class TestPaperEndToEnd:
    def test_a_paper_session_trades_records_and_reports(self, tmp_path):
        b = FakeBroker(); ticks = synthetic_ticks(n_minutes=40); b.ticks_by_symbol["EURUSD"] = ticks
        e = _engine(b, tmp_path, mode="PAPER", min_confidence=0.0, chop_block=False,
                    max_spread_fraction_of_expected_move=5.0, spread_expansion_limit=99.0)
        opened = closed = 0
        start = ticks[-120].time
        for i in range(600):
            b.now = start + dt.timedelta(milliseconds=250 * i)
            notes = e.cycle()
            opened += sum(1 for n in notes if "lots at" in n)
            closed += sum(1 for n in notes if "closed" in n)
            if b.now > ticks[-1].time:
                break
        assert not b.sent, "PAPER never reaches the broker"
        st = e.status(b.now, 0.0)
        assert st["mode"] == "PAPER" and st["strategy_id"] == STRATEGY_ID
        assert (tmp_path / "scalper-status.json").exists()
        rows = e.journal.closed_since(T0 - dt.timedelta(days=1))
        if opened:
            assert e.journal.rejected_since(T0 - dt.timedelta(days=1)) >= 0
        for r in rows:
            assert r["exit_reason"] in ("INITIAL_STOP", "THESIS_FAILED", "MOMENTUM_DECAY", "PROFIT_TRAIL",
                                        "STRUCTURE_BREAK", "SPREAD_EMERGENCY", "SYSTEM_SHUTDOWN")
            assert r["components_json"] and r["planned_loss"] <= 10.0 + 1e-9
            assert r["entry_slippage_points"] is not None
        e.shutdown()
        assert not e.trades


# ======================================================================= backtest --
class TestRealTickBacktest:
    def test_replay_runs_tick_by_tick_with_costs_and_stress(self):
        from mintel.scalper.backtest import replay, stress_test, walk_forward, Stress
        spec = sim_broker().spec("EURUSD")
        ticks = []
        for d in range(3):
            day = [Tick(t.symbol, t.time + dt.timedelta(days=d), t.bid, t.ask, t.last, t.volume) for t in synthetic_ticks(n_minutes=40, seed=d)]
            ticks.extend(day)
        c = scfg(min_confidence=0.0, chop_block=False, max_spread_fraction_of_expected_move=5.0, spread_expansion_limit=99.0)
        r = replay("EURUSD", ticks, spec, c)
        assert r.attempted >= 0
        for t in r.trades:
            assert t.volume >= spec.volume_min and t.exit_reason
            assert t.net <= t.gross                   # commission always costs
        rows = stress_test("EURUSD", ticks, spec, c)
        assert [x["label"] for x in rows][:3] == ["baseline", "+25% spread", "+50% spread"]
        assert all("fragile" in x for x in rows)
        wf = walk_forward("EURUSD", ticks, spec, c)
        assert wf["train_days"] and wf["unseen_days"] and len(wf["results"]) == 9
        worse = replay("EURUSD", ticks, spec, c, Stress(spread_multiplier=1.5, extra_slippage_points=3.0, label="w"))
        assert worse.net <= r.net + 1e-6 or not r.trades


# ================================================================ period filter --
class TestPeriodFilter:
    def _seed(self, tmp_path, now):
        import sqlite3
        from mintel.engine.journal import Journal
        from mintel.scalper.journal import ScalperJournal
        Journal(tmp_path / "journal.sqlite")
        ScalperJournal(tmp_path / "scalper.sqlite").close()
        con = sqlite3.connect(tmp_path / "journal.sqlite")
        for i, (days, pnl) in enumerate([(0, 3.0), (1, -2.0), (3, 5.0), (40, 7.0), (400, 100.0)]):
            o = now - dt.timedelta(days=days, hours=2); c = now - dt.timedelta(days=days, hours=1)
            con.execute("INSERT INTO trades (ticket,symbol,opened_utc,closed_utc,pnl_money,tactic) VALUES (?,?,?,?,?,?)",
                        (i + 1, "EURUSD", o.isoformat(), c.isoformat(), pnl, "SESSION_EXPANSION"))
        con.commit(); con.close()
        con = sqlite3.connect(tmp_path / "scalper.sqlite")
        con.execute("INSERT INTO trades (ticket,symbol,opened_utc,closed_utc,net_pnl,exit_reason,duration_seconds,mode) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (99, "GBPUSD", (now - dt.timedelta(days=1, hours=3)).isoformat(),
                     (now - dt.timedelta(days=1, hours=2)).isoformat(), -4.0, "THESIS_FAILED", 8, "PAPER"))
        con.commit(); con.close()

    def test_bounds_cover_every_period_and_a_custom_range(self):
        from mintel.ops.attribution import period_bounds
        now = dt.datetime(2026, 10, 1, 12, 0, tzinfo=dt.timezone.utc)
        for key in ("today", "yesterday", "week", "month", "6m", "1y"):
            a, b, label = period_bounds(key, now)
            assert a < b and label
            if key == "yesterday":
                assert b <= now and (b - a) == dt.timedelta(days=1)
            else:
                assert a <= now < b
        a, b, label = period_bounds("custom", now, "2026-09-10", "2026-09-01")   # reversed dates are fine
        assert (b - a) == dt.timedelta(days=10) and label == "2026-09-01 to 2026-09-10"
        a, b, _ = period_bounds("custom", now, "nonsense", "")                   # bad input -> today
        assert a <= now < b

    def test_each_period_adds_up_the_right_trades_from_both_journals(self, tmp_path):
        from mintel.ops.attribution import make_period_resolver
        now = dt.datetime.now(dt.timezone.utc).replace(hour=12)
        self._seed(tmp_path, now)
        r = make_period_resolver(tmp_path)
        y = r("yesterday")
        assert y["overall"]["trades"] == 2 and y["overall"]["net_today"] == -6.0
        assert y[EXISTING_STRATEGY_ID]["net_today"] == -2.0 and y[STRATEGY_ID]["net_today"] == -4.0
        assert y["period"]["key"] == "yesterday" and y["period"]["label"] == "Yesterday"
        assert r("6m")["overall"]["trades"] == 5 and r("1y")["overall"]["trades"] == 5
        assert r("custom", (now - dt.timedelta(days=500)).date().isoformat(), now.date().isoformat())["overall"]["trades"] == 6
        assert r("today")["overall"]["trades"] == 1

    def test_the_page_offers_the_periods_and_a_calendar(self, tmp_path):
        from mintel.ops.attribution import make_period_resolver
        from mintel.ops.dashboard import render_status
        now = dt.datetime.now(dt.timezone.utc).replace(hour=12)
        self._seed(tmp_path, now)
        strategies = make_period_resolver(tmp_path)("yesterday")
        snap = {"status": {"bot": "RUNNING"}, "health": {}, "thinking": [], "results": {}, "positions": [], "events": [],
                "strategies": strategies}
        page = render_status(snap, "overall")
        for needle in ("Today", "Yesterday", "This week", "This month", "Last 6 months", "Last year",
                       'type="date"', "THESIS_FAILED", "Net P&amp;L (Yesterday)", "period=yesterday"):
            assert needle in page, needle

    def test_the_server_answers_period_queries_without_touching_today(self, tmp_path):
        import urllib.request
        from mintel.ops.attribution import make_period_resolver
        from mintel.ops.dashboard import DashboardState, start_dashboard
        now = dt.datetime.now(dt.timezone.utc).replace(hour=12)
        self._seed(tmp_path, now)
        state = DashboardState()
        state.period_resolver = make_period_resolver(tmp_path)
        state.update(status={"bot": "RUNNING"}, strategies={"overall": {"net_today": 3.0, "trades": 1, "currency": "GBP"},
                                                             "labels": {"overall": "Overall"}})
        httpd = start_dashboard(state, "127.0.0.1", 0)
        port = httpd.server_address[1]
        try:
            today = urllib.request.urlopen(f"http://127.0.0.1:{port}/?strategy=overall").read().decode()
            week = urllib.request.urlopen(f"http://127.0.0.1:{port}/?strategy=overall&period=week").read().decode()
            custom = urllib.request.urlopen(f"http://127.0.0.1:{port}/?strategy=rapid_scalper&period=custom"
                                            f"&from={(now - dt.timedelta(days=2)).date()}&to={now.date()}").read().decode()
        finally:
            httpd.shutdown()
        assert "Net P&amp;L (Today)" in today and "+3.00" in today
        assert "Net P&amp;L (This week)" in week
        assert "Exits under 10 s" in custom and "THESIS_FAILED" in custom


# ================================================== the spread is a cost, not a signal --
class TestSpreadIsNotASignal:
    def test_a_stop_inside_four_spreads_is_refused(self):
        spec = sim_broker().spec("EURUSD")
        # 20-point stop with a 6-point spread: inside 4 spreads -> no trade
        rp = plan(spec, Side.BUY, 1.1000, 1.0998, 0.00006, scfg(min_stop_points=10.0))
        assert not rp.ok and "spreads" in rp.reason
        # same stop with a 1-point spread is fine
        assert plan(spec, Side.BUY, 1.1000, 1.0998, 0.00001, scfg(min_stop_points=10.0)).ok

    def test_a_fresh_buy_sitting_at_minus_spread_is_not_thesis_failed(self):
        spec = sim_broker().spec("EURUSD")
        m = Manager(scfg(prove_it_seconds=15.0, thesis_fail_adverse_r=0.55))
        entry, stop = 1.10020, 1.09990          # 30-point stop, 2-point spread
        t = ScalpTrade(1, "EURUSD", Side.BUY, 0.1, entry, entry, T0, stop, stop, entry - stop, 10.0, spec.point, spec=spec)
        # ask stays at entry; bid is 2 points under: the spread, nothing else
        for s in range(1, 12):
            tick = Tick("EURUSD", T0 + dt.timedelta(seconds=s), 1.10000, 1.10020, 1.10010, 1.0)
            d = m.update(t, tick, None, T0 + dt.timedelta(seconds=s))
            assert not d.close, (s, d.exit_reason, d.reason)
        # a real move against (mid 27 points under entry = -0.9R) with the tape still running down does fail it
        against = SimpleNamespace(velocity_5s=-0.5, persistence=0.2, trend_bias=0, volume_accel=1.0, acceleration=0.0,
                                  realised_vol_points=2.0, bar_range_ratio=1.0, consecutive_ticks=-6, micro_low=1.0995, micro_high=1.1003)
        tick = Tick("EURUSD", T0 + dt.timedelta(seconds=12), 1.09983, 1.10003, 1.09993, 1.0)
        d = m.update(t, tick, against, T0 + dt.timedelta(seconds=12))
        assert d.close and d.exit_reason == "THESIS_FAILED" and "mid" in d.reason

    def test_the_page_names_the_mode_of_each_scalper_trade(self):
        from mintel.ops.dashboard import render_status
        rs = strategy_stats([{"net_pnl": -4.0, "duration_seconds": 8}], [], label="Rapid Scalper", strategy_id=STRATEGY_ID, scalper=True)
        rs["trades_today"] = [{"ticket": 2, "symbol": "GBPUSD", "net": -4.0, "closed": "2026-10-01T09:05:00",
                               "exit_reason": "THESIS_FAILED", "mode": "PAPER", "strategy": STRATEGY_ID}]
        snap = {"status": {"bot": "RUNNING"}, "health": {}, "thinking": [], "results": {}, "positions": [], "events": [],
                "strategies": {"overall": combine([rs]), STRATEGY_ID: rs, "scalper": {},
                               "labels": {"overall": "Overall", STRATEGY_ID: "Rapid Scalper"}}}
        assert "PAPER</span>" in render_status(snap, STRATEGY_ID)


# ================================================ the ten pounds is the exit, not a hint --
class TestTheStopIsTheExit:
    def _trade(self):
        spec = sim_broker().spec("EURUSD")
        return ScalpTrade(1, "EURUSD", Side.BUY, 0.1, 1.1000, 1.1000, T0, 1.0990, 1.0990, 0.0010, 10.0, spec.point, spec=spec)

    def test_a_trade_that_is_merely_negative_is_left_to_its_stop(self):
        m = Manager(scfg(prove_it_seconds=3.0))
        t = self._trade()
        calm = SimpleNamespace(velocity_5s=0.1, persistence=0.3, trend_bias=1, volume_accel=1.0, acceleration=0.0,
                               realised_vol_points=2.0, bar_range_ratio=1.0, consecutive_ticks=1, micro_low=1.0995, micro_high=1.1003)
        # 0.85R against on the mid but the tape has turned back up: NOT certainly going there
        for s in range(1, 10):
            d = m.update(t, Tick("EURUSD", T0 + dt.timedelta(seconds=s), 1.09909, 1.09921), calm, T0 + dt.timedelta(seconds=s))
            assert not d.close, (s, d.reason)
        # and with no features at all we never guess: the stop does the job
        d = m.update(t, Tick("EURUSD", T0 + dt.timedelta(seconds=10), 1.09909, 1.09921), None, T0 + dt.timedelta(seconds=10))
        assert not d.close

    def test_moving_towards_the_stop_counts_by_ticks_as_well_as_velocity(self):
        m = Manager(scfg(prove_it_seconds=3.0))
        t = self._trade()
        f = SimpleNamespace(velocity_5s=0.0, persistence=0.3, trend_bias=0, volume_accel=1.0, acceleration=0.0,
                            realised_vol_points=2.0, bar_range_ratio=1.0, consecutive_ticks=-4, micro_low=1.0995, micro_high=1.1003)
        d = m.update(t, Tick("EURUSD", T0 + dt.timedelta(seconds=2), 1.09909, 1.09921), f, T0 + dt.timedelta(seconds=2))
        assert d.close and d.exit_reason == "THESIS_FAILED"


# ========================================= true numbers: the broker's money, the real costs --
class TestTrueNumbers:
    def test_live_figures_are_the_brokers_deal_history_not_our_arithmetic(self, tmp_path):
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        e = _engine(b, tmp_path, mode="LIVE")
        # our own record says one trade lost 4.00; the broker says it lost 5.10 after commission and swap
        spec = b.spec("EURUSD")
        t = ScalpTrade(31, "EURUSD", Side.BUY, 0.1, 1.1, 1.1, T0, 1.099, 1.099, 0.001, 10.0, spec.point, spec=spec)
        e.journal.open_trade(t, "LIVE", 1.0, 2.0, {}, 0.0, 100.0)
        e.journal.close_trade(t, closed_at=T0 + dt.timedelta(seconds=9), exit_requested=1.0996, exit_filled=1.0996,
                              exit_reason="THESIS_FAILED", exit_detail="", spread_at_exit=1.0, gross=-3.4, commission=0.6,
                              spread_cost=0.1, slippage_cost=0.0, net=-4.0, exit_slip=0.0, exit_latency=100.0)
        b.deals_since = lambda since, magic=0, closing_only=True: [
            {"position": 31, "symbol": "EURUSD", "volume": 0.1, "profit": -0.3, "commission": -0.3, "is_entry": True, "time": T0},
            {"position": 31, "symbol": "EURUSD", "volume": 0.1, "profit": -4.8, "commission": -0.3, "is_entry": False, "time": T0},
        ]
        s = e.stats_today()
        assert s["trades"] == 1 and s["losses"] == 1 and s["realised"] == -5.1 and s["net_today"] == -5.1
        assert s["total_costs"] == 0.6 and s["largest_loss"] == -5.1
        assert "broker" in s["source"]
        st = e.status(b.now, 0.5)
        assert st["trades_today"][0]["net"] == -5.1          # the trade list shows the broker's figure too

    def test_when_the_broker_cannot_be_asked_the_page_says_so_rather_than_inventing(self, tmp_path):
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        e = _engine(b, tmp_path, mode="LIVE")
        def boom(*a, **k): raise RuntimeError("terminal gone")
        b.deals_since = boom
        s = e.stats_today()
        assert "unavailable" in s["source"]
        p = _engine(b, tmp_path / "paper", mode="PAPER").stats_today()
        assert p["source"].startswith("simulated")

    def test_a_live_close_takes_its_money_from_the_brokers_deal(self, tmp_path):
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        e = _engine(b, tmp_path, mode="LIVE")
        spec = b.spec("EURUSD")
        t = ScalpTrade(41, "EURUSD", Side.BUY, 0.1, 1.1, 1.1, T0, 1.099, 1.099, 0.001, 10.0, spec.point, spec=spec)
        e.trades[41] = t
        e.journal.open_trade(t, "LIVE", 1.0, 2.0, {}, 0.0, 100.0)
        b.closed_deal = lambda ticket: {"exit_price": 1.0997, "pnl": -2.75, "volume": 0.1}
        from mintel.scalper.execution import Fill
        e._finalise(t, T0 + dt.timedelta(seconds=20), Fill(True, 41, 1.0998, 1.0998, 0.0, 50.0), "THESIS_FAILED", "x")
        row = e.journal.closed_since(T0)[0]
        assert row["net_pnl"] == -2.75 and row["exit_filled"] == 1.0997

    def test_trades_whose_costs_eat_the_planned_loss_are_refused(self, tmp_path):
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        # a 60-a-lot commission makes costs dwarf the move
        e = _engine(b, tmp_path, min_confidence=0.0, max_stop_points=400.0, commission_per_lot_round_turn=60.0)
        from mintel.scalper.score import Opportunity
        from mintel.scalper.features import compute as fc
        opp = Opportunity("EURUSD", 1, 90.0)
        buf = e.buffers["EURUSD"]
        for t in synthetic_ticks():
            buf.add(t)
        e._bars_for("EURUSD", 100.0)
        opp.features = fc("EURUSD", buf, e.bars["EURUSD"], 0.00001, b.now)
        opp.expected_move_points = 5.0
        why = e._try_open(opp, b.now, {"EURUSD": buf.last})
        assert not e.trades and not b.sent and ("costs" in why or "round-trip cost" in why)
        assert e.journal.rejected_since(T0 - dt.timedelta(days=1)) == 1


class TestPaperAndLiveNeverMix:
    def test_the_live_tab_ignores_this_mornings_paper_trades(self, tmp_path):
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        e = _engine(b, tmp_path, mode="LIVE")
        spec = b.spec("EURUSD")
        for ticket, mode, net in ((1, "PAPER", 9.0), (2, "PAPER", 9.0), (3, "LIVE", -4.0)):
            t = ScalpTrade(ticket, "EURUSD", Side.BUY, 0.1, 1.1, 1.1, T0, 1.099, 1.099, 0.001, 10.0, spec.point, spec=spec)
            e.journal.open_trade(t, mode, 1.0, 2.0, {}, 0.0, 100.0)
            e.journal.close_trade(t, closed_at=T0 + dt.timedelta(seconds=9), exit_requested=1.1, exit_filled=1.1,
                                  exit_reason="THESIS_FAILED", exit_detail="", spread_at_exit=1.0, gross=net, commission=0.0,
                                  spread_cost=0.0, slippage_cost=0.0, net=net, exit_slip=0.0, exit_latency=100.0)
        b.deals_since = lambda since, magic=0, closing_only=True: [
            {"position": 3, "symbol": "EURUSD", "volume": 0.1, "profit": -4.0, "commission": 0.0, "is_entry": False, "time": T0}]
        st = e.status(b.now, 0.5)
        assert st["stats"]["trades"] == 1 and st["stats"]["net_today"] == -4.0
        assert [t["ticket"] for t in st["trades_today"]] == [3] and st["trades_today"][0]["mode"] == "LIVE"


# ================================= the settings file never freezes yesterday's defaults --
class TestSettingsFileHoldsOnlyTheMode:
    def test_a_fresh_file_is_just_the_mode_and_new_defaults_reach_it(self, tmp_path):
        p = tmp_path / "scalper.json"
        ScalperConfig.save_minimal(p, "PAPER")
        assert json.loads(p.read_text()) == {"mode": "PAPER"}
        c = ScalperConfig.load(p)
        assert c.mode == "PAPER" and c.thesis_fail_adverse_r == ScalperConfig().thesis_fail_adverse_r

    def test_an_old_full_dump_is_collapsed_but_the_mode_is_kept(self, tmp_path):
        p = tmp_path / "scalper.json"
        old = ScalperConfig(mode="LIVE"); old.thesis_fail_adverse_r = 0.55; old.prove_it_seconds = 5.0
        old.save(p)
        assert ScalperConfig.load(p).prove_it_seconds == 5.0          # the frozen value, before
        assert ScalperConfig.normalise_file(p)
        assert json.loads(p.read_text()) == {"mode": "LIVE"}
        c = ScalperConfig.load(p)
        assert c.mode == "LIVE" and c.prove_it_seconds == ScalperConfig().prove_it_seconds
        assert not ScalperConfig.normalise_file(p)                     # idempotent

    def test_deliberate_overrides_are_respected(self, tmp_path):
        p = tmp_path / "scalper.json"
        p.write_text(json.dumps({"mode": "PAPER", "keep_overrides": True, "max_open_positions": 2}))
        assert not ScalperConfig.normalise_file(p)
        assert ScalperConfig.load(p).max_open_positions == 2

    def test_the_process_entry_collapses_the_file_on_start(self, tmp_path):
        from mintel.scalper import run as rs_run
        cfg = Config(); cfg.ops.data_dir = str(tmp_path); cfg.ops.log_dir = str(tmp_path)
        cfg.save(tmp_path / "config.json")
        p = tmp_path / "scalper.json"
        ScalperConfig(mode="OFF").save(p)
        assert len(json.loads(p.read_text())) > 1
        rs_run.main(["--config", str(tmp_path / "config.json")])    # OFF: exits at once
        assert json.loads(p.read_text()) == {"mode": "OFF"}


# ======================================= lessons of 1 October: costs, resting, no pressing --
class TestLessonsOfDayOne:
    def _ready(self, tmp_path, **kw):
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        e = _engine(b, tmp_path, min_confidence=0.0, max_stop_points=400.0, **kw)
        from mintel.scalper.score import Opportunity
        from mintel.scalper.features import compute as fc
        opp = Opportunity("EURUSD", 1, 90.0)
        buf = e.buffers["EURUSD"]
        for t in synthetic_ticks():
            buf.add(t)
        e._bars_for("EURUSD", 100.0)
        opp.features = fc("EURUSD", buf, e.bars["EURUSD"], 0.00001, b.now)
        opp.expected_move_points = 300.0
        return b, e, opp, {"EURUSD": buf.last}

    def test_commission_is_judged_against_the_money_at_the_stop_not_the_padded_plan(self, tmp_path):
        # A big slippage allowance pads the planned loss; commission must still be
        # small next to what the STOP risks, or there is no trade.
        b, e, opp, ticks = self._ready(tmp_path, commission_per_lot_round_turn=30.0, slippage_allowance_points=200.0)
        why = e._try_open(opp, b.now, ticks)
        assert not e.trades and "at risk to the stop" in why

    def test_a_market_that_just_lost_rests_for_ten_minutes(self, tmp_path):
        b, e, opp, ticks = self._ready(tmp_path)
        e.last_loss_by_symbol["EURUSD"] = b.now - dt.timedelta(minutes=2)
        why = e._try_open(opp, b.now, ticks)
        assert not e.trades and "rests for another 8 min" in why
        e.last_loss_by_symbol["EURUSD"] = b.now - dt.timedelta(minutes=11)
        e._try_open(opp, b.now, ticks)
        assert len(e.trades) == 1

    def test_three_losses_in_a_row_pause_everything_for_half_an_hour_six_end_the_day(self):
        br = Breakers(scfg())
        now = T0
        for i in range(3):
            br.record_result(-1.0, now + dt.timedelta(minutes=i))
        why = br.check(now + dt.timedelta(minutes=3), tick_age_seconds=0.5, connected=True, latency_ms=50)
        assert any("paused for another 29 min" in w for w in why)
        why = br.check(now + dt.timedelta(minutes=40), tick_age_seconds=0.5, connected=True, latency_ms=50)
        assert not any("losses in a row" in w for w in why)
        for i in range(3, 6):
            br.record_result(-1.0, now + dt.timedelta(minutes=40 + i))
        why = br.check(now + dt.timedelta(minutes=50), tick_age_seconds=0.5, connected=True, latency_ms=50)
        assert any("paused for the day" in w for w in why)
        br.record_result(5.0, now + dt.timedelta(minutes=51))        # a win ends the streak
        assert br.consecutive_losses == 0


# ================================================ reconcile a day against the broker --
class TestReconcile:
    def test_per_bot_totals_add_up_to_the_account(self):
        from types import SimpleNamespace
        from mintel.ops.reconcile import reconcile, render
        day = dt.date(2026, 10, 2)
        t = dt.datetime(2026, 10, 2, 10, 0, tzinfo=dt.timezone.utc)
        deals = [  # position, magic, profit(net incl commission), commission, is_entry
            (1, 990311, -0.30, -0.30, True), (1, 990311, -4.00, -0.30, False),
            (2, 990311, 0.00, 0.00, True), (2, 990311, 7.54, 0.00, False),
            (3, 990411, -0.30, -0.30, True), (3, 990411, 13.39, -0.30, False),
            (4, 0, 0.00, 0.00, True), (4, 0, -1.48, 0.00, False),            # a hand trade
            (5, 990311, -0.30, -0.30, True),                                  # still open: not counted
        ]
        class B:
            clock = None
            def deals_since(self, since, magic=0, closing_only=True):
                return [{"position": p, "profit": pr, "commission": c, "is_entry": e, "time": t, "symbol": "X"}
                        for p, m, pr, c, e in deals if not magic or m == magic]
        cfg = Config()
        res = reconcile(B(), cfg, day, 990411)
        assert res["mi"]["net"] == 3.24 and res["mi"]["positions"] == 2 and res["mi"]["commission"] == -0.6
        assert res["rs"]["net"] == 13.09 and res["rs"]["positions"] == 1
        assert res["other"]["net"] == -1.48 and res["all"]["net"] == 14.85
        text = render(res, cfg, day, SimpleNamespace(balance=100.0, equity=100.0))
        assert "Profit +14.85" in text and "difference to the account: -1.48" in text



# ======================================= 4 Oct: commission-free indices can actually trade --
class TestIndicesCanTrade:
    def _index_spec(self, name="US30"):
        from mintel.contracts import SymbolSpec
        return SymbolSpec(name=name, digits=2, point=0.01, tick_size=0.01, tick_value=0.0079,
                          contract_size=1.0, volume_min=0.01, volume_max=100.0, volume_step=0.01,
                          stops_level_points=0, freeze_level_points=0)

    def test_index_limits_are_in_index_points_not_broker_points(self):
        c = ScalperConfig()
        ms, mn, mx = c.limits_points("US30", 0.01)
        assert (ms, mn, mx) == pytest.approx((400.0, 600.0, 6000.0))
        # gold and FX keep the point-based limits
        assert c.limits_points("XAUUSD", 0.01) == (c.max_spread_points, c.min_stop_points, c.max_stop_points)

    def test_a_normal_us30_trade_is_planned_not_refused(self):
        spec = self._index_spec()
        # 2.0 index-point spread, 10 index-point stop: ordinary for US30
        rp = plan(spec, Side.BUY, 50700.00, 50690.00, 2.0, ScalperConfig())
        assert rp.ok, rp.reason
        assert rp.planned_loss <= 10.0 + 1e-9 and rp.commission == 0.0          # indices: no commission
        # before the fix the same trade was "too far for a scalp"
        old = ScalperConfig(); old.symbol_limits = {}
        assert not plan(spec, Side.BUY, 50700.00, 50690.00, 2.0, old).ok

    def test_the_spread_blocker_uses_the_per_market_limit(self):
        from mintel.scalper.features import Features
        from mintel.scalper.score import score
        f = Features(symbol="US30", ok=True, point=0.01, spread_points=200.0, spread_avg_points=200.0,
                     velocity_5s=50.0, rate_of_change_30s_points=800.0, realised_vol_points=300.0, atr_points=3000.0)
        assert not any("spread too wide" in b for b in score(f, ScalperConfig()).blockers)
        f.spread_points = 500.0                                   # 5 index points: genuinely wide
        assert any("spread too wide" in b for b in score(f, ScalperConfig()).blockers)

    def test_the_file_can_override_one_market(self, tmp_path):
        p = tmp_path / "scalper.json"
        p.write_text(json.dumps({"mode": "PAPER", "keep_overrides": True,
                                 "symbol_limits": {"US500": {"max_spread": 0.6, "min_stop": 1.0, "max_stop": 10.0}}}))
        c = ScalperConfig.load(p)
        assert c.limits_points("US500", 0.01) == pytest.approx((60.0, 100.0, 1000.0))



# ======================== 5 Oct: liquidity was the bot's own polling speed, not the market --
class TestRealLiquidity:
    def _bars(self, vols):
        from mintel.broker.base import Bar
        t = T0 - dt.timedelta(minutes=len(vols))
        return [Bar(t + dt.timedelta(minutes=i), 1.1, 1.1001, 1.0999, 1.1, tick_volume=v) for i, v in enumerate(vols)]

    def test_ticks_per_minute_uses_the_brokers_bar_tick_count(self):
        from mintel.scalper.features import TickBuffer, compute
        buf = TickBuffer("EURUSD")
        for i in range(9):                                   # nine polls in the last minute
            buf.add(Tick("EURUSD", T0 - dt.timedelta(seconds=60 - 6 * i), 1.1, 1.10002, 1.10001, 1.0))
        bars = self._bars([30] * 30 + [140, 160, 150, 20])   # last bar still forming
        f = compute("EURUSD", buf, bars, 0.00001, T0)
        assert f.ticks_per_minute == 150.0
        # without bar volumes it falls back to what it saw
        f2 = compute("EURUSD", buf, self._bars([0] * 34), 0.00001, T0)
        assert f2.ticks_per_minute == 9.0

    def test_rejected_setups_are_logged_once_a_minute_per_reason(self, tmp_path):
        from mintel.scalper.journal import ScalperJournal
        from mintel.scalper.score import Opportunity
        j = ScalperJournal(tmp_path / "s.sqlite")
        o = Opportunity("EURUSD", 1, 50.0); o.blockers = ["confidence 50 below 70"]
        for s in range(0, 120, 2):
            j.record_rejected(o, 1.0, T0 + dt.timedelta(seconds=s), 60.0)
        assert j.rejected_since(T0 - dt.timedelta(minutes=1)) == 2
        o2 = Opportunity("EURUSD", 1, 50.0); o2.blockers = ["spread too wide"]
        j.record_rejected(o2, 1.0, T0 + dt.timedelta(seconds=121), 60.0)
        assert j.rejected_since(T0 - dt.timedelta(minutes=1)) == 3
        j.close()

    def test_the_status_file_is_written_every_two_seconds_not_every_pass(self, tmp_path):
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        e = _engine(b, tmp_path, mode="PAPER")
        calls = []
        real = e.status
        e.status = lambda *a, **k: calls.append(1) or real(*a, **k)
        for _ in range(20):
            e.cycle()
        assert 1 <= len(calls) <= 3
        assert "cycle_ms" in json.loads(e.status_path.read_text())

    def test_the_broker_day_is_cached_and_refreshed_after_a_close(self, tmp_path):
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        e = _engine(b, tmp_path, mode="LIVE")
        n = []
        b.deals_since = lambda *a, **k: n.append(1) or []
        day = e.day_start()
        for _ in range(5):
            e._broker_day(day)
        assert len(n) == 1
        e._broker_day_dirty = True
        e._broker_day(day)
        assert len(n) == 2

    def test_the_other_bots_day_counts_commission_once_and_includes_entries(self, tmp_path):
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        e = _engine(b, tmp_path, mode="LIVE")
        b.deals_since = lambda since, magic=0, closing_only=True: [
            {"position": 1, "profit": -0.3, "commission": -0.3, "is_entry": True},
            {"position": 1, "profit": -4.3, "commission": -0.3, "is_entry": False}]
        assert e._other_day_pnl(e.day_start()) == pytest.approx(-4.6)



class TestScalperCommissionPerMarket:
    def test_indices_carry_no_commission_in_the_plan(self):
        from mintel.contracts import SymbolSpec
        spec = SymbolSpec(name="US500", digits=2, point=0.01, tick_size=0.01, tick_value=0.0079,
                          contract_size=1.0, volume_min=0.01, volume_max=100.0, volume_step=0.01,
                          stops_level_points=0, freeze_level_points=0)
        rp = plan(spec, Side.BUY, 7600.00, 7597.00, 0.5, ScalperConfig())
        assert rp.ok and rp.commission == 0.0
        assert ScalperConfig().commission_for("XAUUSD") == ScalperConfig().commission_per_lot_round_turn



class TestEntryHours:
    def test_entries_only_inside_the_chosen_hours(self, tmp_path):
        p = tmp_path / "scalper.json"
        p.write_text(json.dumps({"mode": "LIVE", "entry_hours_utc": [0]}))
        c = ScalperConfig.load(p)
        assert c.entry_hours_utc == (0,)
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        e = _engine(b, tmp_path / "e", entry_hours_utc=(0,))
        e.cycle()
        assert b.now.hour != 0 and any("outside its trading hours" in w for w in e.blocked_because)
        assert ScalperConfig().entry_hours_utc == ()                    # default: every hour



class TestResetToZero:
    def test_the_scalpers_day_starts_at_the_measuring_start_when_later(self, tmp_path):
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        e = _engine(b, tmp_path)
        midnight = b.now.replace(hour=0, minute=0, second=0, microsecond=0)
        assert e.day_start() == midnight
        e.cfg.tracking_start_utc = (b.now - dt.timedelta(minutes=5)).isoformat()
        assert e.day_start() == b.now - dt.timedelta(minutes=5)
        e.cfg.tracking_start_utc = (b.now + dt.timedelta(hours=1)).isoformat()   # a future start is ignored
        assert e.day_start() == midnight
