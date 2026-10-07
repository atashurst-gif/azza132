"""Momentum Runner: a paper shadow of the main bot's index trades."""
import datetime as dt
import json
from types import SimpleNamespace

from mintel.broker.base import Position, Side, Tick
from mintel.runner import STRATEGY_ID, STRATEGY_LABEL
from mintel.runner.shadow import MomentumRunner, RunnerConfig

T0 = dt.datetime(2026, 10, 7, 13, 0, tzinfo=dt.timezone.utc)


class Spec:
    """US30-like: 1 index point = 1.00 of account money per lot."""
    def money(self, dist, volume):
        return dist * volume * 1.0

    def normalise_price(self, p):
        return round(p, 2)


def runner(tmp_path, **kw):
    return MomentumRunner(tmp_path, RunnerConfig(**kw), spec_fn=lambda s: Spec(), clock=lambda: T0)


def pos(ticket=1, symbol="US30", side=Side.BUY, entry=50000.0, sl=49990.0, volume=1.0):
    return Position(ticket, symbol, side, volume, entry, sl, 0.0, T0, magic=990311)


def tick(symbol, bid, ask=None):
    return Tick(symbol, T0, bid, ask if ask is not None else bid + 2.0)


class TestWhatItShadows:
    def test_only_index_trades_and_only_once(self, tmp_path):
        r = runner(tmp_path)
        assert r.adopt(pos(), 49990.0, T0) is not None
        assert r.adopt(pos(), 49990.0, T0) is None                       # already shadowing
        assert r.adopt(pos(ticket=2, symbol="EURUSD", entry=1.1, sl=1.099), 1.099, T0) is None
        assert r.adopt(pos(ticket=3, symbol="XAUUSD", entry=4000, sl=3990), 3990, T0) is None
        assert set(r.open) == {1}

    def test_it_uses_the_original_stop_not_the_trailed_one(self, tmp_path):
        r = runner(tmp_path)
        p = pos(sl=49998.0)                                             # the broker's stop has been pulled up
        s = r.adopt(p, 49990.0, T0)
        assert s.initial_stop == 49990.0 and s.risk_distance == 10.0

    def test_a_restart_keeps_the_open_shadows(self, tmp_path):
        r = runner(tmp_path)
        r.adopt(pos(), 49990.0, T0)
        r.update(r.open[1], 50020.0, 50022.0, T0 + dt.timedelta(minutes=5))
        r.close()
        r2 = runner(tmp_path)
        assert 1 in r2.open and r2.open[1].peak_r == 2.0

    def test_off_means_nothing_happens(self, tmp_path):
        r = runner(tmp_path, enabled=False)
        assert r.observe([pos()], {1: 49990.0}, lambda s: tick(s, 50000.0), T0) == []
        assert not r.open and r.status()["mode"] == "OFF"


class TestHowItRides:
    def test_the_stop_is_left_alone_until_three_r_then_trails_three_r(self, tmp_path):
        r = runner(tmp_path)
        s = r.adopt(pos(), 49990.0, T0)
        assert r.update(s, 50025.0, 50027.0, T0 + dt.timedelta(minutes=1)) is None     # +2.5 R: no change
        assert s.stop == 49990.0 and not s.peak_r >= 3
        assert r.update(s, 50040.0, 50042.0, T0 + dt.timedelta(minutes=2)) is None     # +4 R: trail 3 R behind
        assert s.stop == 50010.0
        assert r.update(s, 50030.0, 50032.0, T0 + dt.timedelta(minutes=3)) is None     # a dip: the stop never retreats
        assert s.stop == 50010.0
        reason = r.update(s, 50009.0, 50011.0, T0 + dt.timedelta(minutes=4))
        assert reason == "TRAIL_STOP" and 1 not in r.open
        row = r.closed_since(T0)[0]
        assert row["exit_price"] == 50010.0 and row["net_pnl"] == 10.0 and row["realised_r"] == 1.0
        assert row["peak_r"] == 4.0 and row["mode"] == "PAPER"

    def test_a_loser_goes_to_the_full_original_stop(self, tmp_path):
        r = runner(tmp_path)
        s = r.adopt(pos(), 49990.0, T0)
        r.update(s, 50005.0, 50007.0, T0 + dt.timedelta(minutes=1))                    # +0.5 R, no break-even move
        assert r.update(s, 49989.0, 49991.0, T0 + dt.timedelta(minutes=2)) == "INITIAL_STOP"
        row = r.closed_since(T0)[0]
        assert row["net_pnl"] == -10.0 and row["exit_reason"] == "INITIAL_STOP"       # at the stop, never better

    def test_a_sell_is_marked_on_the_ask(self, tmp_path):
        r = runner(tmp_path)
        s = r.adopt(pos(side=Side.SELL, entry=50000.0, sl=50010.0), 50010.0, T0)
        r.update(s, 49958.0, 49960.0, T0 + dt.timedelta(minutes=1))                    # ask 49960: +4 R
        assert s.stop == 49990.0
        assert r.update(s, 49989.0, 49991.0, T0 + dt.timedelta(minutes=2)) == "TRAIL_STOP"
        assert r.closed_since(T0)[0]["net_pnl"] == 10.0

    def test_out_at_the_window_end(self, tmp_path):
        r = runner(tmp_path, window_hours=1.0)
        s = r.adopt(pos(), 49990.0, T0)
        r.update(s, 50015.0, 50017.0, T0 + dt.timedelta(minutes=30))
        assert r.update(s, 50012.0, 50014.0, T0 + dt.timedelta(minutes=61)) == "WINDOW_END"
        assert r.closed_since(T0)[0]["net_pnl"] == 12.0

    def test_the_shadow_outlives_the_real_trade(self, tmp_path):
        r = runner(tmp_path)
        prices = {"US30": 50000.0}
        r.observe([pos()], {1: 49990.0}, lambda s: tick(s, prices[s]), T0)
        # the real trade is gone (closed by the ladder); the shadow carries on
        prices["US30"] = 50045.0
        notes = r.observe([], {}, lambda s: tick(s, prices[s]), T0 + dt.timedelta(minutes=10))
        assert 1 in r.open and r.open[1].stop == 50015.0 and notes == []
        prices["US30"] = 50014.0
        notes = r.observe([], {}, lambda s: tick(s, prices[s]), T0 + dt.timedelta(minutes=11))
        assert any("shadow closed TRAIL_STOP" in n for n in notes) and not r.open


class TestWhatThePageSees:
    def test_status_file_and_tab(self, tmp_path):
        from mintel.ops.attribution import runner_tab
        r = runner(tmp_path)
        prices = {"US30": 50000.0}
        r.observe([pos()], {1: 49990.0}, lambda s: tick(s, prices[s]), T0)
        st = json.loads((tmp_path / "runner-status.json").read_text())
        assert st["label"] == STRATEGY_LABEL and st["mode"] == "PAPER" and st["status"] == "IN TRADE"
        assert st["open"][0]["symbol"] == "US30" and st["open"][0]["trailing"] is False
        prices["US30"] = 49989.0
        r._last_status = 0.0
        r.observe([], {}, lambda s: tick(s, prices[s]), T0 + dt.timedelta(minutes=3))
        stats, status = runner_tab(tmp_path, T0 - dt.timedelta(hours=1), None, "GBP", now=T0 + dt.timedelta(minutes=3))
        assert stats["trades"] == 1 and stats["net_today"] == -10.0 and stats["id"] == STRATEGY_ID
        assert stats["trades_today"][0]["mode"] == "PAPER" and "not in Overall" in stats["source"]
        assert status["status"] == "WATCHING"

    def test_never_in_the_account_total(self, tmp_path):
        from mintel.ops.attribution import build_strategies
        from mintel.scalper import EXISTING_STRATEGY_ID
        r = runner(tmp_path)
        s = r.adopt(pos(), 49990.0, T0)
        r.update(s, 50040.0, 50042.0, T0 + dt.timedelta(minutes=1))
        r.update(s, 50009.0, 50011.0, T0 + dt.timedelta(minutes=2))                     # +10 on paper
        journal = SimpleNamespace(closed_trades=lambda **kw: [], open_trades=lambda: [])
        out = build_strategies(journal, [], T0 - dt.timedelta(hours=1), "GBP", tmp_path, None, now=T0 + dt.timedelta(minutes=3))
        assert out[STRATEGY_ID]["net_today"] == 10.0
        assert out["overall"]["net_today"] == 0.0 and out[EXISTING_STRATEGY_ID]["net_today"] == 0.0
        assert STRATEGY_ID in out["labels"]

    def test_the_page_has_the_third_tab_and_its_box(self, tmp_path):
        from mintel.ops.attribution import build_strategies
        from mintel.ops.dashboard import render_status
        r = runner(tmp_path)
        r.observe([pos()], {1: 49990.0}, lambda s: tick(s, 50000.0), T0)
        journal = SimpleNamespace(closed_trades=lambda **kw: [], open_trades=lambda: [])
        strategies = build_strategies(journal, [], T0 - dt.timedelta(hours=1), "GBP", tmp_path, None, now=T0)
        snap = {"status": {"bot": "RUNNING"}, "health": {}, "thinking": [], "results": {}, "positions": [],
                "strategies": strategies}
        page = render_status(snap, STRATEGY_ID)
        assert "Momentum Runner" in page and "MOMENTUM RUNNER" in page and "Shadows open" in page
        assert "Reached the 3 R trail" in page


class TestInsideTheTrader:
    def test_the_trader_shadows_its_own_index_trade(self, tmp_path):
        """Full path: the main bot opens US30, the Runner shadows it from the
        tracker's ORIGINAL stop, and the real bot is unaffected."""
        import pytest
        from mintel.config import Config
        from mintel.engine.trader import Trader
        sim = pytest.importorskip("mintel.broker.sim")
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path)
        cfg.mode = "DEMO"
        broker = sim.SimBroker(["US30", "EURUSD"], start=T0 - dt.timedelta(days=2), history_bars=500)
        broker.connect()
        tr = Trader(broker, cfg, enable_model=False)
        assert tr.runner is not None and tr.runner.cfg.trail_r == 3.0
        # adopt a position straight into FlowLock, as an entry would
        p = pos()
        tr.flowlock.adopt(p, 49990.0, T0)
        notes = []
        for n in tr.runner.observe([p], {p.ticket: tr.flowlock.trackers[p.ticket].initial_stop},
                                    lambda s: tick(s, 50000.0), T0):
            notes.append(n)
        assert any("shadowing US30" in n for n in notes)
        assert tr.runner.open[1].initial_stop == 49990.0
