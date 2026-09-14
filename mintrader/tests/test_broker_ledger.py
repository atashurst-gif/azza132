"""The status page measures the bot from the broker's own deal history,
from a fixed start, counting only the bot's trades. This is the number
you compare with MetaTrader, and it must not depend on the journal."""
from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pytest

from mintel.broker.bridge_server import BridgeService
from mintel.broker.mt5_adapter import Mt5Broker
from mintel.broker import wire
from mintel.config import Config
from mintel.run import broker_ledger, _LEDGER_CACHE
from mintel.ops.dashboard import render_status

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 9, 14, 12, 0, tzinfo=UTC)


def _deal(pos, entry, typ, profit, when, magic=990311, commission=-0.5, swap=0.0):
    return SimpleNamespace(position_id=pos, entry=entry, type=typ, profit=profit,
                           commission=commission, swap=swap, magic=magic,
                           symbol="EURUSD", volume=0.1,
                           time=int(when.replace(tzinfo=UTC).timestamp()))


class TestAdapterDealHistory:
    def test_only_the_bots_closing_trade_deals_since_the_start(self):
        b = Mt5Broker()
        deals = [
            _deal(1, 0, 0, 0.0, T0 + dt.timedelta(minutes=1)),        # entry: skip
            _deal(1, 1, 1, 10.0, T0 + dt.timedelta(minutes=5)),       # ours
            _deal(2, 1, 0, -4.0, T0 + dt.timedelta(minutes=6), magic=0),  # by hand
            _deal(3, 1, 2, 1200.0, T0 + dt.timedelta(minutes=7)),     # balance op
            _deal(4, 1, 1, 3.0, T0 - dt.timedelta(days=1)),            # too old
        ]
        b._mt5 = SimpleNamespace(history_deals_get=lambda *a, **k: tuple(deals))
        rows = b.deals_since(T0, magic=990311)
        assert [r["position"] for r in rows] == [1]
        assert rows[0]["profit"] == pytest.approx(9.5)
        assert rows[0]["time"] == T0 + dt.timedelta(minutes=5)
        # without a magic filter, the hand trade counts too
        assert [r["position"] for r in b.deals_since(T0, magic=0)] == [1, 2]

    def test_failure_is_an_empty_list(self):
        b = Mt5Broker()
        def boom(*a, **k):
            raise RuntimeError("no connection")
        b._mt5 = SimpleNamespace(history_deals_get=boom)
        assert b.deals_since(T0, 1) == []


class FakeBroker:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def deals_since(self, since, magic=0, closing_only=True):
        self.calls.append((since, magic))
        return [r for r in self.rows if r["time"] >= since]


def _rows():
    return [
        {"position": 10, "symbol": "EURUSD", "volume": 0.1, "profit": 5.0,
         "time": dt.datetime(2026, 9, 13, 23, 30, tzinfo=UTC)},   # yesterday
        {"position": 11, "symbol": "GBPUSD", "volume": 0.1, "profit": -3.0,
         "time": dt.datetime(2026, 9, 14, 9, 0, tzinfo=UTC)},
        {"position": 12, "symbol": "XAUUSD", "volume": 0.01, "profit": 4.0,
         "time": dt.datetime(2026, 9, 14, 10, 0, tzinfo=UTC)},
        {"position": 12, "symbol": "XAUUSD", "volume": 0.01, "profit": -1.0,
         "time": dt.datetime(2026, 9, 14, 10, 5, tzinfo=UTC)},    # same position, 2nd deal
    ]


class TestBridgeCarriesDealHistory:
    def test_roundtrip(self):
        svc = BridgeService(FakeBroker(_rows()), magic=990311)
        out = svc.handle("deals_since", {"since": wire._iso(T0 - dt.timedelta(days=1)), "magic": 990311})
        assert len(out) == 4
        assert isinstance(out[0]["time"], str)
        assert wire._dt(out[0]["time"]) == _rows()[0]["time"]
        assert svc.broker.calls[0][1] == 990311


class TestLedger:
    def _trader(self, rows, start=""):
        cfg = Config()
        cfg.magic = 990311
        cfg.tracking_start_utc = start
        return SimpleNamespace(broker=FakeBroker(rows), cfg=cfg)

    def setup_method(self):
        _LEDGER_CACHE["at"] = None
        _LEDGER_CACHE["value"] = {}

    def test_today_and_since_start_group_by_position(self):
        tr = self._trader(_rows(), "2026-09-13T20:00:00+00:00")
        led = broker_ledger(tr, T0)
        assert led["source"] == "broker"
        assert led["today"] == {"net": 0.0, "trades": 2, "wins": 1, "win_rate": 50.0}
        assert led["since_start"] == {"net": 5.0, "trades": 3, "wins": 2, "win_rate": 66.7}
        assert led["tracking_start"].startswith("2026-09-13T20:00")
        # asked the broker once, for the bot's magic, from the earlier of the two starts
        assert tr.broker.calls == [(dt.datetime(2026, 9, 13, 20, 0, tzinfo=UTC), 990311)]

    def test_no_start_means_today(self):
        tr = self._trader(_rows())
        led = broker_ledger(tr, T0)
        assert led["since_start"] == led["today"]
        assert led["tracking_start"].startswith("2026-09-14T00:00")

    def test_cached_for_thirty_seconds(self):
        tr = self._trader(_rows(), "2026-09-14T00:00:00+00:00")
        broker_ledger(tr, T0)
        broker_ledger(tr, T0 + dt.timedelta(seconds=10))
        assert len(tr.broker.calls) == 1
        broker_ledger(tr, T0 + dt.timedelta(seconds=40))
        assert len(tr.broker.calls) == 2

    def test_broker_without_history_gives_nothing(self):
        tr = SimpleNamespace(broker=SimpleNamespace(), cfg=Config())
        assert broker_ledger(tr, T0) == {}


class TestStatusPageShowsSinceStart:
    def test_tile(self):
        snap = {"status": {"since_start_pnl": -84.6, "since_start_trades": 97,
                           "tracking_start": "2026-09-14T00:00:00+00:00",
                           "currency": "GBP", "today_pnl": -12.0},
                "health": {}, "thinking": [], "positions": [], "events": [], "results": {}}
        html = render_status(snap)
        assert "Since start (2026-09-14)" in html
        assert "over 97 trades" in html
