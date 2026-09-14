"""On the first live day the spread said 0.1 pips while the broker charged
the equivalent of 0.7 in commission, and dozens of 3-pip scalps paid more in
fees than they could earn. Commission is now learned from the broker's own
deals and counted in every cost and reward:risk test."""
from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pytest

from mintel.broker.bridge_server import BridgeService
from mintel.broker.sim import SimBroker
from mintel.config import Config
from mintel.engine.scanner import Scanner
from mintel.engine.trader import Trader

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 9, 14, 13, 0, tzinfo=UTC)


def _scanner(commission_cfg=0.0):
    cfg = Config()
    cfg.risk.commission_per_lot = commission_cfg
    broker = SimBroker(["EURUSD", "XAUUSD"], start=NOW - dt.timedelta(days=3))
    broker.connect()
    return Scanner(broker, cfg), broker


class TestCommissionInPips:
    def test_learned_commission_becomes_pips_and_part_of_cost(self):
        sc, broker = _scanner()
        spec = broker.spec("EURUSD")
        assert sc.commission_pips("EURUSD", spec) == 0.0, "nothing learned yet: no invented cost"
        sc.commission_per_lot = {"EURUSD": 5.5, "*": 6.0}
        pips = sc.commission_pips("EURUSD", spec)
        expected = 5.5 / spec.money_per_lot(spec.pip_size)
        assert pips == pytest.approx(expected)
        assert 0.1 < pips < 5.0, pips
        # a symbol not yet traded uses the default
        assert sc.commission_pips("XAUUSD", broker.spec("XAUUSD")) == pytest.approx(
            6.0 / broker.spec("XAUUSD").money_per_lot(broker.spec("XAUUSD").pip_size))
        tick = broker.tick("EURUSD")
        bars = broker.bars("EURUSD", broker.spec("EURUSD") and __import__("mintel.broker.base", fromlist=["TF"]).TF.M5, 50)
        q = sc.execution_quality("EURUSD", spec, tick, broker.now, bars)
        assert q.commission_pips == pytest.approx(pips, abs=1e-3)
        assert q.cost_pips == pytest.approx(q.spread_pips + q.expected_slippage_pips + q.commission_pips)

    def test_config_override_wins(self):
        sc, broker = _scanner(commission_cfg=7.0)
        sc.commission_per_lot = {"EURUSD": 5.5}
        assert sc.commission_money_per_lot("EURUSD") == 7.0


class TestLearningFromDeals:
    def _rows(self):
        # EURUSD: 0.20 lots closed, 2 entry + 2 exit deals at -0.55 each side => 2.2 / 0.2 = 11/lot
        # XAUUSD: 0.01 closed, fees 0.03 + 0.03 => 6/lot
        r = []
        for pos, sym, vol, fee in ((1, "EURUSD", 0.1, -0.55), (2, "EURUSD", 0.1, -0.55),
                                   (3, "XAUUSD", 0.01, -0.03)):
            r.append({"position": pos, "symbol": sym, "volume": vol, "profit": 0.0,
                      "commission": fee, "is_entry": True, "time": NOW})
            r.append({"position": pos, "symbol": sym, "volume": vol, "profit": 1.0,
                      "commission": fee, "is_entry": False, "time": NOW})
        return r

    def _ns(self, rows, commission_cfg=0.0):
        cfg = Config()
        cfg.risk.commission_per_lot = commission_cfg
        calls = []
        def deals_since(since, magic=0, closing_only=True):
            calls.append((since, magic, closing_only))
            return rows
        return SimpleNamespace(cfg=cfg, broker=SimpleNamespace(deals_since=deals_since),
                               scanner=SimpleNamespace(commission_per_lot={}),
                               last_commission_refresh=None, _calls=calls)

    def test_learns_per_symbol_and_a_default(self):
        ns = self._ns(self._rows())
        Trader.refresh_commissions(ns, NOW)
        learned = ns.scanner.commission_per_lot
        assert learned["EURUSD"] == pytest.approx(11.0)
        assert learned["XAUUSD"] == pytest.approx(6.0)
        assert learned["*"] == pytest.approx((2.2 + 0.06) / 0.21)
        since, magic, closing_only = ns._calls[0]
        assert closing_only is False, "entry deals carry commission too"
        assert magic == ns.cfg.magic
        assert since == NOW - dt.timedelta(days=30)

    def test_throttled_to_every_ten_minutes(self):
        ns = self._ns(self._rows())
        Trader.refresh_commissions(ns, NOW)
        Trader.refresh_commissions(ns, NOW + dt.timedelta(minutes=5))
        assert len(ns._calls) == 1
        Trader.refresh_commissions(ns, NOW + dt.timedelta(minutes=11))
        assert len(ns._calls) == 2

    def test_configured_commission_skips_learning(self):
        ns = self._ns(self._rows(), commission_cfg=5.0)
        Trader.refresh_commissions(ns, NOW)
        assert ns._calls == []

    def test_broker_failure_is_harmless(self):
        ns = self._ns([])
        def boom(*a, **k):
            raise RuntimeError("no")
        ns.broker.deals_since = boom
        Trader.refresh_commissions(ns, NOW)
        assert ns.scanner.commission_per_lot == {}


class TestBridgePassesTheFlag:
    def test_closing_only_travels(self):
        seen = []
        class B:
            def deals_since(self, since, magic=0, closing_only=True):
                seen.append(closing_only)
                return []
        svc = BridgeService(B(), magic=1)
        svc.handle("deals_since", {"since": NOW.isoformat(), "magic": 1, "closing_only": False})
        svc.handle("deals_since", {"since": NOW.isoformat(), "magic": 1})
        assert seen == [False, True]
