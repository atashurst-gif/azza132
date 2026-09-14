"""The first live day booked every closed trade with the same exit price and
P&L: history_deals_get(from, to, position=...) ignores the position when dates
are given, so each trade was credited with the whole week's deals.  These
tests pin the fix at both ends: the MT5 adapter asks by position alone and
filters by position id, and the trader refuses a deal that cannot be its own.
"""
from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pytest

from mintel.broker.mt5_adapter import Mt5Broker


def _deal(position_id, entry, price, volume, profit, symbol="EURUSD",
          commission=-0.5, swap=0.0, comment="[sl 1.1000]"):
    return SimpleNamespace(position_id=position_id, entry=entry, price=price,
                           volume=volume, profit=profit, symbol=symbol,
                           commission=commission, swap=swap, comment=comment,
                           time=1_700_000_000)


class FakeMt5:
    def __init__(self, deals):
        self.deals = deals
        self.calls = []

    def history_deals_get(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if args:
            # dates given: the real package ignores position= and returns
            # everything in the window - exactly the bug
            return tuple(self.deals)
        pos = kwargs.get("position")
        return tuple(d for d in self.deals if d.position_id == pos)


class TestAdapterAsksByPositionAlone:
    def _broker(self, deals):
        b = Mt5Broker()
        b._mt5 = FakeMt5(deals)
        return b

    def test_only_the_positions_own_closing_deals_are_summed(self):
        deals = [
            _deal(111, 0, 1.1000, 0.10, 0.0),          # our entry
            _deal(111, 1, 1.1020, 0.10, 20.0),         # our exit
            _deal(222, 0, 3180.0, 0.01, 0.0, "XAUGBP"),
            _deal(222, 1, 3170.0, 0.01, 10.0, "XAUGBP"),
            _deal(333, 1, 0.5760, 0.50, -9.0, "NZDUSD"),
        ]
        b = self._broker(deals)
        out = b.closed_deal(111)
        assert out is not None
        assert out["exit_price"] == pytest.approx(1.1020)
        assert out["pnl"] == pytest.approx(20.0 - 0.5)
        assert out["volume"] == pytest.approx(0.10)
        assert out["symbol"] == "EURUSD"
        assert out["position"] == 111
        # and it asked by position, never with a date window
        (args, kwargs), = b._mt5.calls
        assert args == ()
        assert kwargs == {"position": 111}

    def test_a_package_that_ignores_the_filter_is_filtered_anyway(self):
        deals = [_deal(111, 1, 1.1020, 0.10, 20.0),
                 _deal(222, 1, 3170.0, 0.01, 10.0, "XAUGBP")]
        b = self._broker(deals)
        b._mt5.history_deals_get = lambda *a, **k: tuple(deals)   # returns all
        out = b.closed_deal(111)
        assert out["pnl"] == pytest.approx(19.5)
        assert out["exit_price"] == pytest.approx(1.1020)

    def test_no_closing_deal_means_none(self):
        b = self._broker([_deal(111, 0, 1.1000, 0.10, 0.0)])
        assert b.closed_deal(111) is None
        assert b.closed_deal(999) is None


class TestTraderRefusesForeignDeals:
    def test_matching(self):
        from mintel.engine.trader import Trader
        tracker = SimpleNamespace(symbol="NZDUSD", entry=0.57663)
        good = {"symbol": "NZDUSD", "exit_price": 0.5769, "pnl": 3.0}
        wrong_symbol = {"symbol": "XAUGBP", "exit_price": 0.5769, "pnl": 3.0}
        wrong_price = {"symbol": "", "exit_price": 5314.31, "pnl": 29.93}
        assert Trader._deal_matches(good, tracker)
        assert not Trader._deal_matches(wrong_symbol, tracker)
        assert not Trader._deal_matches(wrong_price, tracker)
        assert not Trader._deal_matches({"exit_price": 0}, tracker)
