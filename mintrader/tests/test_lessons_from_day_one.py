"""Day one live: ~100 trades with 2-7 pip stops inside the noise, most stopped
within minutes, and repeated re-entries on the same markets; commission made
a break-even day a losing one. These are the guards that came out of it."""
from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pytest

from mintel.broker.base import Side
from mintel.broker.sim import SimBroker
from mintel.config import Config
from mintel.engine.scanner import Scanner
from mintel.engine.trader import Trader

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 9, 14, 13, 0, tzinfo=UTC)
SYMS = ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD"]


def _scan(cfg=None, **scanner_attrs):
    cfg = cfg or Config()
    broker = SimBroker(SYMS, start=NOW - dt.timedelta(days=5))
    broker.connect()
    sc = Scanner(broker, cfg)
    for k, v in scanner_attrs.items():
        setattr(sc, k, v)
    return sc.scan(broker.now, []), broker


class TestCostsMayNotEatTheStop:
    def test_absurd_commission_blocks_everything(self):
        states, _ = _scan(commission_per_lot={"*": 500.0})
        assert states
        for st in states:
            assert any("costs would be" in b for b in st.blockers), (st.symbol, st.blockers)

    def test_zero_limit_disables_the_guard(self):
        cfg = Config()
        cfg.scan.max_cost_fraction_of_stop = 0.0
        states, _ = _scan(cfg, commission_per_lot={"*": 500.0})
        assert not any("costs would be" in b for st in states for b in st.blockers)


class TestStopsSitOutsideTheNoise:
    def test_every_stop_is_at_least_the_floor_away(self):
        cfg = Config()
        cfg.scan.stop_noise_floor_atr = 2.0       # deliberately large, to bite
        broker = SimBroker(SYMS, start=NOW - dt.timedelta(days=5))
        broker.connect()
        sc = Scanner(broker, cfg)
        states = sc.scan(broker.now, [])
        assert states
        checked = 0
        for st in states:
            ctx = sc.build_context(st.symbol, broker.now)
            if ctx is None or ctx.atr_ref <= 0:
                continue
            tick = broker.spec(st.symbol).tick_size
            assert abs(st.entry - st.stop) >= 2.0 * ctx.atr_ref - tick, (
                st.symbol, st.direction, abs(st.entry - st.stop), ctx.atr_ref)
            checked += 1
        assert checked >= 2, "at least two markets must have been checked"

    def test_a_zero_floor_leaves_tight_structural_stops_alone(self):
        cfg = Config()
        cfg.scan.stop_noise_floor_atr = 0.0
        broker = SimBroker(SYMS, start=NOW - dt.timedelta(days=5))
        broker.connect()
        sc = Scanner(broker, cfg)
        states = sc.scan(broker.now, [])
        tight = 0
        for st in states:
            ctx = sc.build_context(st.symbol, broker.now)
            if ctx is not None and ctx.atr_ref > 0 and abs(st.entry - st.stop) < 2.0 * ctx.atr_ref:
                tight += 1
        assert tight > 0, "without the floor some stops must sit closer than 2 ATR, or the first test proves nothing"


class TestLossesCloseAMarketForTheDay:
    def test_blocker_after_the_daily_limit(self):
        states, _ = _scan(losses_today=lambda sym: 3)
        assert states
        for st in states:
            assert any("losing trades here today" in b for b in st.blockers)

    def test_below_the_limit_no_blocker(self):
        states, _ = _scan(losses_today=lambda sym: 0)
        assert not any("losing trades here today" in b for st in states for b in st.blockers)


class TestAfterAClose:
    def _ns(self):
        cfg = Config()
        cooldowns = []
        rows = [{"symbol": "EURUSD", "pnl_money": -3.0}, {"symbol": "EURUSD", "pnl_money": 2.0},
                {"symbol": "GBPUSD", "pnl_money": -1.0}]
        return SimpleNamespace(
            cfg=cfg, clock=lambda: NOW,
            executor=SimpleNamespace(set_cooldown=lambda s, side, secs: cooldowns.append((s, side, secs))),
            journal=SimpleNamespace(closed_trades=lambda limit=0, since=None: rows),
            _losses={"date": None, "counts": {}}, _cooldowns=cooldowns)

    def _bind(self, ns):
        ns.losses_today = lambda sym: Trader.losses_today(ns, sym)
        return ns

    def test_losses_today_is_rebuilt_from_the_journal(self):
        ns = self._ns()
        assert Trader.losses_today(ns, "EURUSD") == 1
        assert Trader.losses_today(ns, "GBPUSD") == 1
        assert Trader.losses_today(ns, "USDJPY") == 0

    def test_a_loss_earns_a_long_same_direction_cooldown_and_counts(self):
        ns = self._bind(self._ns())
        tracker = SimpleNamespace(symbol="EURUSD", side=Side.SELL)
        Trader._after_close(ns, tracker, -4.2, NOW)
        assert ns._cooldowns == [("EURUSD", Side.SELL, 1800.0)]
        assert Trader.losses_today(ns, "EURUSD") == 2

    def test_a_win_changes_nothing(self):
        ns = self._bind(self._ns())
        Trader._after_close(ns, SimpleNamespace(symbol="EURUSD", side=Side.BUY), 3.0, NOW)
        assert ns._cooldowns == []
        assert Trader.losses_today(ns, "EURUSD") == 1
