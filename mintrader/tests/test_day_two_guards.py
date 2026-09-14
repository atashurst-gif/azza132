"""Day one, evening: the 3% daily-loss stop never fired on an 11% day
(its day-start equity reset on every restart), and the fresh build opened
eight trades in forty seconds at the Asian open."""
from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

from mintel.broker.base import AccountInfo, Position, Side
from mintel.config import Config
from mintel.engine.risk import RiskManager
from mintel.engine.trader import Trader

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 9, 15, 9, 0, tzinfo=UTC)


def _account(equity):
    return AccountInfo(login=1, server="x", currency="GBP", balance=equity, equity=equity,
                       margin=0.0, margin_free=equity, margin_level=0.0, leverage=30,
                       trade_allowed=True, is_demo=True)


class TestDailyLossFromTheBroker:
    def test_restart_no_longer_resets_the_day(self):
        cfg = Config()
        cfg.risk.max_daily_loss_pct = 3.0
        # a fresh RiskManager mid-day, equity already down 11% - the old bug
        rm = RiskManager(cfg)
        snap = rm.snapshot(_account(890.0), [], NOW)
        assert not any(b.name == "DAILY_LOSS" and b.tripped for b in snap.breakers)
        # with the broker's record of today's realised losses it trips at once
        rm2 = RiskManager(cfg)
        rm2.realised_today = lambda: -110.0
        snap = rm2.snapshot(_account(890.0), [], NOW)
        trip = next(b for b in snap.breakers if b.name == "DAILY_LOSS")
        assert trip.tripped
        assert "11.00%" in trip.reason

    def test_floating_losses_count_too(self):
        cfg = Config()
        cfg.risk.max_daily_loss_pct = 3.0
        rm = RiskManager(cfg)
        rm.realised_today = lambda: -20.0
        pos = Position(ticket=1, symbol="EURUSD", side=Side.BUY, volume=0.1,
                       entry_price=1.1, sl=1.09, tp=0.0, open_time=NOW, magic=1,
                       profit=-15.0)
        snap = rm.snapshot(_account(965.0), [pos], NOW)
        trip = next(b for b in snap.breakers if b.name == "DAILY_LOSS")
        assert trip.tripped        # -35 on a 1000 start = 3.5%

    def test_a_failing_broker_call_falls_back_quietly(self):
        cfg = Config()
        rm = RiskManager(cfg)
        def boom():
            raise RuntimeError("no")
        rm.realised_today = boom
        snap = rm.snapshot(_account(1000.0), [], NOW)
        assert not any(b.name == "DAILY_LOSS" and b.tripped for b in snap.breakers)


class TestEntryPacing:
    def _ns(self, **over):
        cfg = Config()
        for k, v in over.items():
            setattr(cfg.scan, k, v)
        return SimpleNamespace(cfg=cfg, last_entry_utc=None, _entry_times=[])

    def test_rollover_window_blocks(self):
        ns = self._ns()
        assert "rollover" in Trader._entry_gate(ns, NOW.replace(hour=22, minute=5))
        assert "rollover" in Trader._entry_gate(ns, NOW.replace(hour=23, minute=29))
        assert Trader._entry_gate(ns, NOW.replace(hour=23, minute=30)) == ""
        assert Trader._entry_gate(ns, NOW.replace(hour=9, minute=0)) == ""

    def test_window_crossing_midnight(self):
        ns = self._ns(no_entry_utc_windows=("23:30-00:15",))
        assert Trader._entry_gate(ns, NOW.replace(hour=23, minute=45)) != ""
        assert Trader._entry_gate(ns, NOW.replace(hour=0, minute=10)) != ""
        assert Trader._entry_gate(ns, NOW.replace(hour=0, minute=20)) == ""

    def test_entries_are_spaced_and_capped(self):
        ns = self._ns(min_seconds_between_entries=180.0, max_new_positions_per_hour=2)
        assert Trader._entry_gate(ns, NOW) == ""
        Trader._note_entry(ns, NOW)
        assert "spacing" in Trader._entry_gate(ns, NOW + dt.timedelta(seconds=40))
        assert Trader._entry_gate(ns, NOW + dt.timedelta(seconds=200)) == ""
        Trader._note_entry(ns, NOW + dt.timedelta(seconds=200))
        assert "in the last hour" in Trader._entry_gate(ns, NOW + dt.timedelta(seconds=600))
        assert Trader._entry_gate(ns, NOW + dt.timedelta(hours=1, seconds=1)) == ""

    def test_defaults(self):
        sc = Config().scan
        assert sc.min_seconds_between_entries == 180.0
        assert sc.max_new_positions_per_hour == 4
        assert sc.no_entry_utc_windows == ("21:45-23:30",)
