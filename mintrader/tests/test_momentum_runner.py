"""Momentum Runner: PAPER shadows of the main bot's index trades, and LIVE
positions of its own beside them."""
import datetime as dt
import json
import sqlite3
from types import SimpleNamespace

import pytest

from mintel.botexec import MAGIC_RUNNER
from mintel.broker.base import OrderRequest, Position, Side, Tick
from mintel.broker.sim import SimBroker
from mintel.config import Config
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
        # the account gate reads the cycle's risk snapshot (closed until one is taken)
        tr._last_snapshot = tr.risk.snapshot(broker.account(), [], T0)
        # adopt a position straight into FlowLock, as an entry would
        p = pos()
        tr.flowlock.adopt(p, 49990.0, T0)
        notes = []
        for n in tr.runner.observe([p], {p.ticket: tr.flowlock.trackers[p.ticket].initial_stop},
                                    lambda s: tick(s, 50000.0), T0):
            notes.append(n)
        assert any("shadowing US30" in n for n in notes)
        assert tr.runner.open[1].initial_stop == 49990.0


# ------------------------------------------------------------------ LIVE --
def sim(symbols=("US500",)):
    b = SimBroker(list(symbols), start=T0, history_bars=300)
    b.connect()
    return b


def live_runner(tmp_path, broker, entries_allowed=None, **kw):
    return MomentumRunner(tmp_path, RunnerConfig(mode="LIVE", **kw), spec_fn=broker.spec, clock=lambda: T0,
                          broker=broker, entries_allowed=entries_allowed)


def real_trade(broker, ticket=1, side=Side.BUY, risk=30.0):
    """Trend & Breakout's trade as the Runner sees it: entry at the touch, the
    original stop ``risk`` points away. It is not at the sim: the Runner must
    never need it to be."""
    t = broker.tick("US500")
    entry = t.ask if side is Side.BUY else t.bid
    sl = entry - side.sign * risk
    return Position(ticket, "US500", side, 1.0, entry, sl, 0.0, T0, magic=Config().magic), sl


def minutes(n):
    return T0 + dt.timedelta(minutes=n)


def count_rows(r):
    return r.db.execute("SELECT COUNT(*) FROM trades").fetchone()[0]


class TestLive:
    def test_an_entry_is_the_runners_own_order_with_the_stop_at_the_broker(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        notes = r.observe([p], {1: sl}, b.tick, T0)
        mine = b.positions(MAGIC_RUNNER)
        assert len(mine) == 1 and mine[0].magic == MAGIC_RUNNER and mine[0].comment == "RUNNER"
        assert mine[0].sl == pytest.approx(sl) and mine[0].tp == 0.0 and mine[0].side is Side.BUY and mine[0].volume == 1.0
        assert not b.positions(Config().magic)                          # Trend & Breakout's magic never sees it
        assert len(b.order_log) == 1 and b.order_log[0]["req"].magic == MAGIC_RUNNER
        s = r.open[mine[0].ticket]
        assert s.ticket == mine[0].ticket and s.ticket != 1 and s.source_ticket == 1 and s.mode == "LIVE"
        assert s.entry == pytest.approx(mine[0].entry_price) and s.initial_stop == sl
        row = dict(r.db.execute("SELECT * FROM trades").fetchone())
        assert row["ticket"] == s.ticket and row["source_ticket"] == 1 and row["mode"] == "LIVE" and row["entry"] == s.entry
        assert any("opened US500 BUY" in n and "beside real trade 1" in n for n in notes)
        # once per real trade: a second pass sends nothing more
        assert r.observe([p], {1: sl}, b.tick, minutes(1)) == [] and len(b.order_log) == 1
        st = r.status()
        assert st["mode"] == "LIVE" and st["live"] is True and st["magic"] == MAGIC_RUNNER and st["executor_error"] == ""
        assert st["open"][0]["source_ticket"] == 1 and st["open"][0]["ticket"] == s.ticket and st["open"][0]["mode"] == "LIVE"
        assert st["status"] == "IN TRADE" and st["refused"] == 0

    def test_a_sell_rides_with_its_stop_above(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b, side=Side.SELL)
        r.observe([p], {1: sl}, b.tick, T0)
        mine = b.positions(MAGIC_RUNNER)
        assert len(mine) == 1 and mine[0].side is Side.SELL and mine[0].sl == pytest.approx(sl) and sl > mine[0].entry_price

    def test_a_trailed_stop_is_moved_at_the_broker(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        s = next(iter(r.open.values()))
        b.push_bar("US500", s.entry + 60.0)                             # +2 R on the bid: the stop stays
        r.observe([], {}, b.tick, minutes(1))
        assert s.stop == sl and b.positions(MAGIC_RUNNER)[0].sl == pytest.approx(sl) and not b.modify_log
        b.push_bar("US500", s.entry + 135.0)                            # past +4 R: trail 3 R behind the best bid
        r.observe([], {}, b.tick, minutes(2))
        want = b.spec("US500").normalise_price(b.tick("US500").bid - 3 * s.risk_distance)
        assert s.stop == pytest.approx(want) and s.stop > sl
        assert b.positions(MAGIC_RUNNER)[0].sl == pytest.approx(want)
        assert b.modify_log[-1]["ticket"] == s.ticket and b.modify_log[-1]["sl"] == pytest.approx(want)
        b.push_bar("US500", s.entry + 120.0)                            # a dip: the stop never retreats
        r.observe([], {}, b.tick, minutes(3))
        assert s.stop == pytest.approx(want) and b.positions(MAGIC_RUNNER)[0].sl == pytest.approx(want)
        assert r.db.execute("SELECT stop FROM trades").fetchone()[0] == pytest.approx(want)

    def test_a_refused_stop_move_is_rolled_back_and_tried_again(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        s = next(iter(r.open.values()))
        b.fault.fail_modify = True
        b.push_bar("US500", s.entry + 135.0, low=s.entry + 130.0)       # (a low above the trailed stop: the sim's bar check)
        notes = r.observe([], {}, b.tick, minutes(1))
        assert s.stop == sl and b.positions(MAGIC_RUNNER)[0].sl == pytest.approx(sl)
        assert any("stop move" in n and "refused" in n for n in notes)
        assert r.observe([], {}, b.tick, minutes(2)) == []            # said once, not every pass
        b.fault.fail_modify = False
        r.observe([], {}, b.tick, minutes(3))
        assert s.stop > sl and b.positions(MAGIC_RUNNER)[0].sl == pytest.approx(s.stop)

    def test_the_window_end_closes_at_the_broker_and_keeps_the_brokers_figure(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b, window_hours=1.0)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        s = next(iter(r.open.values()))
        b.push_bar("US500", s.entry + 12.0)
        assert r.observe([], {}, b.tick, minutes(30)) == [] and b.positions(MAGIC_RUNNER)
        notes = r.observe([], {}, b.tick, minutes(61))
        assert not b.positions(MAGIC_RUNNER) and not r.open
        deal = b.closed_deal(s.ticket)
        row = r.closed_since(T0)[0]
        assert row["exit_reason"] == "WINDOW_END" and row["mode"] == "LIVE" and row["ticket"] == s.ticket
        assert row["net_pnl"] == pytest.approx(round(deal["pnl"], 2)) and row["exit_price"] == pytest.approx(deal["exit_price"])
        assert row["net_pnl"] > 0 and any("WINDOW_END" in n for n in notes)
        assert r.status()["status"] == "WATCHING"

    def test_a_refused_close_keeps_the_position_and_retries(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b, window_hours=1.0)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        s = next(iter(r.open.values()))

        real_close = b.close
        calls = {"n": 0}

        def flaky_close(ticket, volume=0.0, comment=""):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("server busy")
            return real_close(ticket, volume, comment)
        b.close = flaky_close
        notes = r.observe([], {}, b.tick, minutes(61))
        assert s.ticket in r.open and b.positions(MAGIC_RUNNER) and any("close refused" in n for n in notes)
        r.observe([], {}, b.tick, minutes(62))
        assert not r.open and not b.positions(MAGIC_RUNNER)
        assert r.closed_since(T0)[0]["net_pnl"] == pytest.approx(round(b.closed_deal(s.ticket)["pnl"], 2))

    def test_a_position_the_broker_closed_is_finalised_once_from_its_record(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        s = next(iter(r.open.values()))
        b.push_bar("US500", s.entry - 8.0)
        b.close(s.ticket)                                               # the broker's stop (or a hand) took it
        notes = r.observe([], {}, b.tick, minutes(1))
        deal = b.closed_deal(s.ticket)
        rows = r.closed_since(T0)
        assert len(rows) == 1 and not r.open and count_rows(r) == 1
        assert rows[0]["net_pnl"] == pytest.approx(round(deal["pnl"], 2)) and rows[0]["net_pnl"] < 0
        assert rows[0]["exit_price"] == pytest.approx(deal["exit_price"]) and rows[0]["exit_reason"] == "INITIAL_STOP"
        assert any("closed at the broker INITIAL_STOP" in n for n in notes)
        assert r.observe([], {}, b.tick, minutes(2)) == []            # finalised once; nothing more to say
        assert r.closed_since(T0) == rows and count_rows(r) == 1

    def test_without_a_broker_record_the_row_is_estimated_and_says_so(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        s = next(iter(r.open.values()))
        b.close(s.ticket)
        b.closed.clear()                                                # the history has not caught up
        r.executor.closed_deal = lambda ticket, tries=1, pause=0.0: None
        notes = r.observe([], {}, b.tick, minutes(1))                  # one read without it is not a close
        assert s.ticket in r.open and r.closed_since(T0) == [] and s.misses == 1
        assert any("no closing deal yet" in n for n in notes)
        assert r.observe([], {}, b.tick, minutes(2)) == [] and s.ticket in r.open      # said once, counted again
        r.observe([], {}, b.tick, minutes(3))                           # the third read in a row: estimated, and marked
        assert not r.open
        row = r.closed_since(T0)[0]
        assert row["exit_reason"] == "INITIAL_STOP?" and row["exit_price"] == pytest.approx(b.tick("US500").bid)
        assert row["net_pnl"] == pytest.approx(round(b.spec("US500").money(row["exit_price"] - s.entry, 1.0), 2))

    def test_a_window_end_close_without_a_broker_record_is_marked_estimated(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b, window_hours=1.0)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        s = next(iter(r.open.values()))
        b.push_bar("US500", s.entry + 12.0)
        r.executor.closed_deal = lambda ticket, tries=1, pause=0.0: None     # the close went through; the history lags
        notes = r.observe([], {}, b.tick, minutes(61))
        assert not b.positions(MAGIC_RUNNER) and not r.open
        row = r.closed_since(T0)[0]
        assert row["exit_reason"] == "WINDOW_END?" and row["mode"] == "LIVE"
        assert row["net_pnl"] == pytest.approx(round(b.spec("US500").money(row["exit_price"] - s.entry, 1.0), 2))
        assert any("WINDOW_END?" in n for n in notes)

    def test_one_empty_read_is_not_a_close(self, tmp_path):
        """MT5 answers None to positions_get() when the terminal hiccups and the
        adapter relays that as an empty list: the position is still there."""
        b = sim()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        s = next(iter(r.open.values()))
        real_positions = b.positions
        b.positions = lambda magic=None: []
        notes = r.observe([], {}, b.tick, minutes(1))
        assert s.ticket in r.open and r.closed_since(T0) == [] and any("no closing deal yet" in n for n in notes)
        b.positions = real_positions
        notes = r.observe([], {}, b.tick, minutes(2))                  # found again: not an orphan, nothing closed
        assert notes == [] and s.ticket in r.open and s.misses == 0
        assert [q.ticket for q in b.positions(MAGIC_RUNNER)] == [s.ticket] and not b.closed and count_rows(r) == 1

    def test_a_position_found_again_after_an_estimate_reopens_its_row(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        s = next(iter(r.open.values()))
        real_positions = b.positions
        b.positions = lambda magic=None: []
        for i in range(3):
            r.observe([], {}, b.tick, minutes(1 + i))
        assert not r.open and r.closed_since(T0)[0]["exit_reason"] == "INITIAL_STOP?"
        b.positions = real_positions
        notes = r.observe([], {}, b.tick, minutes(4))
        assert s.ticket in r.open and any("reopened and managed on" in n for n in notes)
        assert b.positions(MAGIC_RUNNER) and not b.closed              # never closed as an orphan
        assert r.closed_since(T0) == [] and count_rows(r) == 1
        b.close(s.ticket)                                               # and when the broker really closes it: its record, once
        r.observe([], {}, b.tick, minutes(5))
        row = r.closed_since(T0)[0]
        assert row["net_pnl"] == pytest.approx(round(b.closed_deal(s.ticket)["pnl"], 2))
        assert row["exit_reason"] == "INITIAL_STOP" and count_rows(r) == 1 and not r.open

    def test_a_real_trade_still_open_after_the_runners_row_closed_is_not_entered_again(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        s = next(iter(r.open.values()))
        b.close(s.ticket)                                               # the Runner's own position is gone; the real trade is not
        r.observe([p], {1: sl}, b.tick, minutes(1))
        assert not r.open and r.closed_since(T0)[0]["source_ticket"] == 1
        assert r.observe([p], {1: sl}, b.tick, minutes(2)) == [] and len(b.order_log) == 1 and not r.open

    def test_a_broker_that_cannot_be_read_at_start_is_reconciled_on_the_first_pass(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        s = next(iter(r.open.values()))
        r.close()
        b.close(s.ticket)                                               # closed while the bot was down
        real_positions = b.positions
        b.positions = lambda magic=None: (_ for _ in ()).throw(RuntimeError("bridge down"))
        r2 = live_runner(tmp_path, b)
        assert s.ticket in r2.open                                      # nothing is judged gone until the broker answers
        b.positions = real_positions
        notes = r2.observe([], {}, b.tick, minutes(1))
        assert any("could not read the broker's positions at start" in n and "bridge down" in n for n in notes)
        assert any("closed at the broker INITIAL_STOP" in n for n in notes) and not r2.open
        row = r2.closed_since(T0)[0]
        assert row["net_pnl"] == pytest.approx(round(b.closed_deal(s.ticket)["pnl"], 2)) and row["exit_reason"] == "INITIAL_STOP"

    def test_a_refused_order_records_nothing_and_the_bot_carries_on(self, tmp_path):
        class Refuses(SimBroker):
            def send(self, req):
                raise RuntimeError("market closed")
        b = Refuses(["US500"], start=T0, history_bars=300)
        b.connect()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        notes = r.observe([p], {1: sl}, b.tick, T0)
        assert any("order refused" in n and "market closed" in n for n in notes)
        assert not r.open and count_rows(r) == 0 and not b.positions()
        assert r.observe([p], {1: sl}, b.tick, minutes(1)) == []      # not hammered: that trade is remembered as attempted
        st = r.status()
        assert st["refused"] == 1 and "market closed" in st["executor_error"] and st["status"] == "WATCHING"
        assert st["note"].startswith("order refused")
        p2, sl2 = real_trade(b, ticket=2)
        assert any("order refused" in n for n in r.observe([p2], {2: sl2}, b.tick, minutes(2)))   # a new trade is its own attempt
        assert r.status()["refused"] == 2 and count_rows(r) == 0

    def test_a_rejected_order_is_a_clean_no_too(self, tmp_path):
        from mintel.broker.base import RetCode
        b = sim()
        b.fault.reject_next = RetCode.MARKET_CLOSED
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        notes = r.observe([p], {1: sl}, b.tick, T0)
        assert any("order refused" in n for n in notes) and not r.open and not b.positions() and count_rows(r) == 0

    def test_a_restart_keeps_the_live_position_and_finalises_one_the_broker_closed(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        s = next(iter(r.open.values()))
        r.close()
        r2 = live_runner(tmp_path, b)                                   # still at the broker: carried on
        assert s.ticket in r2.open and r2.open[s.ticket].mode == "LIVE" and r2.open[s.ticket].source_ticket == 1
        assert r2.observe([p], {1: sl}, b.tick, minutes(1)) == [] and len(b.order_log) == 1
        r2.close()
        b.close(s.ticket)                                               # gone while the bot was down
        r3 = live_runner(tmp_path, b)
        assert not r3.open
        row = r3.closed_since(T0)[0]
        assert row["net_pnl"] == pytest.approx(round(b.closed_deal(s.ticket)["pnl"], 2)) and row["exit_reason"] == "INITIAL_STOP"
        notes = r3.observe([], {}, b.tick, minutes(2))
        assert any("while the bot was down" in n for n in notes) and count_rows(r3) == 1

    def test_a_restart_closes_an_orphan_at_the_broker_and_never_adopts_it(self, tmp_path):
        b = sim()
        t = b.tick("US500")
        res = b.send(OrderRequest("US500", Side.BUY, 1.0, sl=t.ask - 30.0, tp=0.0, comment="RUNNER", magic=MAGIC_RUNNER,
                                  idempotency_key="stray", filling="IOC"))
        assert res.ok and b.positions(MAGIC_RUNNER)
        r = live_runner(tmp_path, b)
        assert not b.positions(MAGIC_RUNNER) and not r.open and count_rows(r) == 1      # closed, and on record
        deal = b.closed_deal(res.ticket)
        row = r.closed_since(T0 - dt.timedelta(days=1))[0]
        assert row["ticket"] == res.ticket and row["exit_reason"] == "ORPHAN_CLOSED" and row["mode"] == "LIVE"
        assert row["source_ticket"] == 0 and row["symbol"] == "US500" and row["side"] == "BUY" and row["volume"] == 1.0
        assert row["net_pnl"] == pytest.approx(round(deal["pnl"], 2)) and row["exit_price"] == pytest.approx(deal["exit_price"])
        notes = r.observe([], {}, b.tick, T0)
        assert any("orphan closed" in n and str(res.ticket) in n and "ORPHAN_CLOSED" in n for n in notes)
        assert r.status()["orphans"][0]["ticket"] == res.ticket and r.status()["orphans"][0]["result"] == "orphan closed"
        assert r.observe([], {}, b.tick, minutes(1)) == [] and count_rows(r) == 1 and not r.open    # recorded once, never adopted

    def test_a_stray_position_found_while_running_is_closed_too(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        t = b.tick("US500")
        res = b.send(OrderRequest("US500", Side.SELL, 0.5, sl=t.bid + 30.0, tp=0.0, comment="RUNNER", magic=MAGIC_RUNNER,
                                  idempotency_key="stray2", filling="IOC"))
        notes = r.observe([], {}, b.tick, minutes(1))
        assert any("orphan closed" in n for n in notes)
        assert [q.ticket for q in b.positions(MAGIC_RUNNER)] == [next(iter(r.open))] and res.ticket not in r.open
        rows = {row["ticket"]: row for row in r.closed_since(T0)}
        assert count_rows(r) == 2 and rows[res.ticket]["exit_reason"] == "ORPHAN_CLOSED" and rows[res.ticket]["side"] == "SELL"
        assert rows[res.ticket]["net_pnl"] == pytest.approx(round(b.closed_deal(res.ticket)["pnl"], 2))

    def test_another_bots_magic_is_refused_before_anything_is_sent(self, tmp_path):
        from mintel.botexec import MAGIC_BANDBREAKER, MAGIC_CROWD
        b = sim()
        t = b.tick("US500")
        res = b.send(OrderRequest("US500", Side.BUY, 1.0, sl=t.ask - 30.0, tp=0.0, comment="T&B", magic=Config().magic,
                                  idempotency_key="tb", filling="IOC"))
        with pytest.raises(ValueError, match="another bot's number"):
            live_runner(tmp_path, b, magic=Config().magic)              # Trend & Breakout's number
        assert [q.ticket for q in b.positions(Config().magic)] == [res.ticket] and not b.closed and len(b.order_log) == 1
        for magic in (990_411, MAGIC_BANDBREAKER, MAGIC_CROWD, 0, -5):   # the scalper's, the other paper bots', nonsense
            with pytest.raises(ValueError):
                live_runner(tmp_path, b, magic=magic)
        assert b.positions(Config().magic) and not b.closed and not (tmp_path / "runner.sqlite").exists()
        r = live_runner(tmp_path, b)                                    # its own number is fine
        assert r.live and r.status()["magic"] == MAGIC_RUNNER and b.positions(Config().magic)

    def test_a_late_entry_is_refused_for_good_and_a_restart_does_not_retry_it(self, tmp_path):
        """The gate reopening, a restart, a start with a fresh data dir: a
        real trade that is no longer fresh is a different trade at the same
        size, so the Runner says no, on record."""
        b = sim()
        r = live_runner(tmp_path, b)
        t = b.tick("US500")
        p = Position(1, "US500", Side.BUY, 1.0, t.ask, t.ask - 30.0, 0.0, T0 - dt.timedelta(minutes=10), magic=Config().magic)
        notes = r.observe([p], {1: t.ask - 30.0}, b.tick, T0)
        assert any("too late to enter" in n and "10.0 min old" in n for n in notes)
        assert not b.order_log and not r.open and count_rows(r) == 0
        st = r.status()
        assert st["skipped"] == 1 and st["refused"] == 0 and st["note"].startswith("too late to enter")
        assert r.observe([p], {1: t.ask - 30.0}, b.tick, minutes(1)) == [] and not b.order_log
        r.close()
        r2 = live_runner(tmp_path, b)                                   # the record outlives the process
        assert r2.observe([p], {1: t.ask - 30.0}, b.tick, minutes(2)) == [] and not b.order_log
        skipped = r2.skipped_since(T0 - dt.timedelta(days=1))
        assert len(skipped) == 1 and skipped[0]["source_ticket"] == 1 and "too late" in skipped[0]["why"]
        p2 = Position(2, "US500", Side.BUY, 1.0, t.ask, t.ask - 30.0, 0.0, minutes(2), magic=Config().magic)
        r2.observe([p2], {2: t.ask - 30.0}, b.tick, minutes(2))         # a fresh trade is entered as usual
        assert len(b.order_log) == 1 and len(r2.open) == 1 and next(iter(r2.open.values())).source_ticket == 2

    def test_an_entry_whose_price_has_left_the_real_entry_is_refused(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b)
        t = b.tick("US500")
        # the real trade: 30 points of risk, and the price now 25 points (0.83 R) above its entry
        p = Position(1, "US500", Side.BUY, 1.0, t.ask - 25.0, t.ask - 55.0, 0.0, T0, magic=Config().magic)
        notes = r.observe([p], {1: t.ask - 55.0}, b.tick, T0)
        assert any("too late to enter" in n and "from the real entry" in n for n in notes)
        assert not b.order_log and not r.open and r.status()["skipped"] == 1 and r.status()["refused"] == 0
        assert r.skipped_since(T0)[0]["source_ticket"] == 1 and "0.83 R" in r.skipped_since(T0)[0]["why"]
        # a trailed stop handed over as the "initial" one (after a restart), already above the entry: refused too
        p2 = Position(2, "US500", Side.BUY, 1.0, t.ask - 100.0, t.ask - 130.0, 0.0, T0, magic=Config().magic)
        notes = r.observe([p2], {2: t.ask - 10.0}, b.tick, T0)
        assert any("too late to enter" in n for n in notes) and not b.order_log and r.status()["skipped"] == 2
        # within half an R of the real entry, the Runner enters
        p3 = Position(3, "US500", Side.BUY, 1.0, t.ask - 10.0, t.ask - 40.0, 0.0, T0, magic=Config().magic)
        r.observe([p3], {3: t.ask - 40.0}, b.tick, T0)
        assert len(b.order_log) == 1 and len(r.open) == 1 and b.positions(MAGIC_RUNNER)[0].sl == pytest.approx(t.ask - 40.0)

    def test_a_refused_order_is_not_retried_after_a_restart(self, tmp_path):
        class Refuses(SimBroker):
            def send(self, req):
                raise RuntimeError("market closed")
        b = Refuses(["US500"], start=T0, history_bars=300)
        b.connect()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        assert r.status()["refused"] == 1 and r.skipped_since(T0)[0]["why"].startswith("order refused")
        r.close()
        b2 = sim()                                                      # the broker is back
        r2 = live_runner(tmp_path, b2)
        assert r2.observe([p], {1: sl}, b2.tick, minutes(1)) == [] and not b2.order_log and not r2.open

    def test_the_account_gate_blocks_a_new_entry_but_never_management(self, tmp_path):
        b = sim()
        gate = {"open": True}
        r = live_runner(tmp_path, b, entries_allowed=lambda: gate["open"])
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        assert len(b.positions(MAGIC_RUNNER)) == 1
        gate["open"] = False
        p2 = Position(2, "US500", Side.BUY, 1.0, p.entry_price, sl, 0.0, T0, magic=Config().magic)
        notes = r.observe([p2], {2: sl}, b.tick, minutes(1))
        assert any("entries paused by the account gate" in n for n in notes) and len(b.positions(MAGIC_RUNNER)) == 1
        assert r.status()["note"] == "entries paused by the account gate" and len(b.order_log) == 1
        assert r.observe([p2], {2: sl}, b.tick, minutes(2)) == []    # said once while the gate holds
        b.push_bar("US500", p.entry_price + 135.0, low=p.entry_price + 130.0)   # managing is never gated: the trail still moves
        r.observe([p2], {2: sl}, b.tick, minutes(3))
        assert b.positions(MAGIC_RUNNER)[0].sl > sl
        gate["open"] = True
        # the gate reopens four minutes on: that trade is no longer fresh (and the price has run), so it is not chased
        notes = r.observe([p2], {2: sl}, b.tick, minutes(4))
        assert any("too late to enter" in n for n in notes) and len(b.order_log) == 1 and r.status()["skipped"] == 1
        t = b.tick("US500")
        p3 = Position(3, "US500", Side.BUY, 1.0, t.ask, t.ask - 30.0, 0.0, minutes(4), magic=Config().magic)
        r.observe([p2, p3], {2: sl, 3: t.ask - 30.0}, b.tick, minutes(4))     # a fresh one is entered
        assert len(b.positions(MAGIC_RUNNER)) == 2 and len(b.order_log) == 2

    def test_a_broker_that_cannot_be_read_judges_nothing_gone(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        s = next(iter(r.open.values()))
        real_positions = b.positions
        b.positions = lambda magic=None: (_ for _ in ()).throw(RuntimeError("bridge down"))
        notes = r.observe([], {}, b.tick, minutes(1))
        assert s.ticket in r.open and any("could not read the broker's positions" in n for n in notes)
        b.positions = real_positions
        assert r.observe([], {}, b.tick, minutes(2)) == [] and s.ticket in r.open

    def test_mode_off_does_nothing(self, tmp_path):
        b = sim()
        r = MomentumRunner(tmp_path, RunnerConfig(mode="OFF"), spec_fn=b.spec, clock=lambda: T0, broker=b)
        p, sl = real_trade(b)
        assert r.executor is None and r.observe([p], {1: sl}, b.tick, T0) == []
        assert not r.open and not b.positions() and count_rows(r) == 0
        assert r.status()["mode"] == "OFF" and r.status()["status"] == "OFF" and r.status()["live"] is False

    def test_live_needs_a_broker(self, tmp_path):
        with pytest.raises(ValueError):
            MomentumRunner(tmp_path, RunnerConfig(mode="LIVE"), clock=lambda: T0)


class TestPaperStaysPaper:
    def test_no_order_is_ever_sent_and_the_ticket_is_the_real_trades(self, tmp_path):
        b = sim()
        r = MomentumRunner(tmp_path, RunnerConfig(), spec_fn=b.spec, clock=lambda: T0, broker=b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        assert 1 in r.open and r.open[1].source_ticket == 1 and r.open[1].mode == "PAPER" and r.open[1].entry == p.entry_price
        assert not b.positions() and not b.order_log and r.status()["live"] is False and r.status()["mode"] == "PAPER"
        row = dict(r.db.execute("SELECT ticket, source_ticket, mode FROM trades").fetchone())
        assert row == {"ticket": 1, "source_ticket": 1, "mode": "PAPER"}
        assert r.open_rows()[0]["source_ticket"] == 1 and r.open_rows()[0]["mode"] == "PAPER"

    def test_the_gate_holds_in_paper_too(self, tmp_path):
        r = MomentumRunner(tmp_path, RunnerConfig(), spec_fn=lambda s: Spec(), clock=lambda: T0, entries_allowed=lambda: False)
        notes = r.observe([pos()], {1: 49990.0}, lambda s: tick(s, 50000.0), T0)
        assert not r.open and any("entries paused by the account gate" in n for n in notes)
        assert r.status()["note"] == "entries paused by the account gate"

    def test_an_older_record_file_gains_the_new_columns(self, tmp_path):
        con = sqlite3.connect(tmp_path / "runner.sqlite")
        con.executescript("""CREATE TABLE trades (ticket INTEGER PRIMARY KEY, symbol TEXT, side TEXT, volume REAL,
            entry REAL, initial_stop REAL, risk_distance REAL, opened_utc TEXT, closed_utc TEXT, stop REAL,
            peak_price REAL, peak_r REAL, exit_price REAL, realised_r REAL, net_pnl REAL, commission REAL,
            exit_reason TEXT, duration_seconds REAL, tactic TEXT)""")
        con.execute("INSERT INTO trades (ticket, symbol, side, volume, entry, initial_stop, risk_distance, opened_utc, "
                    "stop, peak_price, peak_r) VALUES (7, 'US30', 'BUY', 1.0, 50000.0, 49990.0, 10.0, ?, 49990.0, 0, 0)",
                    (T0.isoformat(),))
        con.commit()
        con.close()
        r = runner(tmp_path)
        cols = {row[1] for row in r.db.execute("PRAGMA table_info(trades)")}
        assert {"mode", "source_ticket"} <= cols
        assert r.open[7].source_ticket == 7 and r.open[7].mode == "PAPER"
        assert r.db.execute("SELECT source_ticket FROM trades WHERE ticket=7").fetchone()[0] == 7
        assert r.adopt(pos(ticket=7), 49990.0, T0) is None             # still one shadow per real trade

    def test_paper_shadows_a_trade_whatever_its_age(self, tmp_path):
        """Freshness is a LIVE rule: the shadow takes the real fill and open time, as before."""
        r = runner(tmp_path)
        old = Position(1, "US30", Side.BUY, 1.0, 50000.0, 49990.0, 0.0, T0 - dt.timedelta(hours=2), magic=990311)
        s = r.adopt(old, 49990.0, T0)
        assert s is not None and s.opened == T0 - dt.timedelta(hours=2) and s.entry == 50000.0 and s.mode == "PAPER"

    def test_a_live_row_left_open_after_a_switch_back_to_paper_is_watched_not_managed(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        s = next(iter(r.open.values()))
        r.close()
        r2 = MomentumRunner(tmp_path, RunnerConfig(), spec_fn=b.spec, clock=lambda: T0, broker=b)
        b.push_bar("US500", s.entry + 135.0, low=s.entry + 130.0)       # LIVE would trail here; it is not ours to touch
        notes = r2.observe([], {}, b.tick, minutes(1))
        assert s.ticket in r2.open and any("not managed in PAPER" in n for n in notes)
        assert b.positions(MAGIC_RUNNER)[0].sl == pytest.approx(sl) and not b.modify_log and not b.closed     # nothing sent
        st = r2.status()
        assert st["note"].startswith(f"US500 ticket {s.ticket} was opened LIVE") and st["unmanaged"][0]["ticket"] == s.ticket
        assert r2.open_rows()[0]["pnl"] > 0                             # marked at the price, so the page is right
        assert r2.closed_since(T0) == [] and r2.observe([], {}, b.tick, minutes(2)) == []       # said once
        b.close(s.ticket)                                               # the broker's stop (or a hand) then closes it
        notes = r2.observe([], {}, b.tick, minutes(3))
        assert not r2.open and any("closed at the broker INITIAL_STOP" in n and "nothing was sent" in n for n in notes)
        row = r2.closed_since(T0)[0]
        assert row["net_pnl"] == pytest.approx(round(b.closed_deal(s.ticket)["pnl"], 2)) and row["net_pnl"] > 0
        assert row["mode"] == "LIVE" and row["exit_reason"] == "INITIAL_STOP" and row["ticket"] == s.ticket
        assert len(b.order_log) == 1 and len(b.closed) == 1 and not b.modify_log       # the one close was the hand above
        assert "closed at the broker" in r2.status()["note"] and r2.status()["unmanaged"] == []

    def test_a_live_row_left_open_under_off_is_finalised_from_the_brokers_record_too(self, tmp_path):
        b = sim()
        r = live_runner(tmp_path, b)
        p, sl = real_trade(b)
        r.observe([p], {1: sl}, b.tick, T0)
        s = next(iter(r.open.values()))
        r.close()
        r3 = MomentumRunner(tmp_path, RunnerConfig(mode="OFF"), spec_fn=b.spec, clock=lambda: T0, broker=b)
        assert r3.executor is None and s.ticket in r3.open
        notes = r3.observe([p], {1: sl}, b.tick, minutes(1))
        assert any("not managed in OFF" in n for n in notes) and s.ticket in r3.open and not b.closed
        assert len(b.order_log) == 1 and r3.status()["mode"] == "OFF"
        b.close(s.ticket)
        r3.observe([], {}, b.tick, minutes(2))
        assert not r3.open and r3.closed_since(T0)[0]["net_pnl"] == pytest.approx(round(b.closed_deal(s.ticket)["pnl"], 2))
        assert len(b.order_log) == 1 and len(b.closed) == 1            # the OFF Runner sent nothing, ever
