"""Replay broker and backtest honesty."""
from __future__ import annotations

import datetime as dt

import pytest

from mintel.broker.base import Bar, CalendarEvent, OrderRequest, Side, TF
from mintel.config import Config
from mintel.research.backtest import full_report, run_backtest
from mintel.research.replay import (Costs, ReplayBroker, default_spec,
                                    load_bars_csv)

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 3, 9, 0, 0, tzinfo=UTC)


def bars(closes, start=T0, step=60):
    out, prev = [], closes[0]
    for i, c in enumerate(closes):
        o = prev
        out.append(Bar(start + dt.timedelta(seconds=step * i), o,
                       max(o, c) + 0.00005, min(o, c) - 0.00005, c, 300.0))
        prev = c
    return out


@pytest.fixture
def replay() -> ReplayBroker:
    series = {"EURUSD": bars([1.1000 + 0.00005 * i for i in range(600)])}
    specs = {"EURUSD": default_spec("EURUSD", 1.1)}
    b = ReplayBroker(series, specs,
                     costs={"EURUSD": Costs(commission_per_lot_per_side=3.0)})
    b.connect()
    for _ in range(100):
        b.step()
    return b


class TestNoLookAhead:
    def test_bars_never_extend_past_the_cursor(self, replay):
        seen = replay.bars("EURUSD", TF.M1, 1000)
        assert all(b.time <= replay.now for b in seen)
        replay.step()
        assert len(replay.bars("EURUSD", TF.M1, 1000)) == len(seen) + 1

    def test_coarse_timeframes_are_also_bounded(self, replay):
        for _ in range(200):
            replay.step()
        for b in replay.bars("EURUSD", TF.M15, 100):
            assert b.time <= replay.now

    def test_tick_is_the_current_bar_only(self, replay):
        t = replay.tick("EURUSD")
        assert t.time == replay.now

    def test_future_calendar_values_are_hidden(self):
        future = T0 + dt.timedelta(minutes=300)
        series = {"EURUSD": bars([1.1 + 0.00005 * i for i in range(600)])}
        b = ReplayBroker(series, {"EURUSD": default_spec("EURUSD", 1.1)},
                         events=[CalendarEvent("cpi", future, "USD", "US",
                                               "CPI", 3, actual=3.6,
                                               forecast=3.1, previous=3.4)])
        b.connect()
        for _ in range(50):
            b.step()
        got = b.calendar(T0, future + dt.timedelta(days=1))
        assert len(got) == 1
        assert got[0].actual is None, "an unreleased number must not be visible"
        assert got[0].forecast == pytest.approx(3.1), (
            "but the forecast is known in advance, as it is live")
        while b.now < future:
            if not b.step():
                break
        released = b.calendar(T0, future + dt.timedelta(days=1))
        assert released[0].actual == pytest.approx(3.6)


class TestCosts:
    def test_spread_is_charged_on_entry(self, replay):
        spec = replay.spec("EURUSD")
        mid = replay.bars("EURUSD", TF.M1, 1)[-1].close
        res = replay.send(OrderRequest("EURUSD", Side.BUY, 0.1,
                                       sl=mid - 0.0030))
        assert res.ok
        assert res.price > mid, "a buy fills above mid"

    def test_slippage_worsens_the_fill(self):
        series = {"EURUSD": bars([1.1000 + 0.00005 * i for i in range(300)])}
        specs = {"EURUSD": default_spec("EURUSD", 1.1)}
        cheap = ReplayBroker(series, specs,
                             costs={"EURUSD": Costs(slippage_spread_multiple=0.0)})
        dear = ReplayBroker(series, specs,
                            costs={"EURUSD": Costs(slippage_spread_multiple=2.0)})
        for b in (cheap, dear):
            b.connect()
            for _ in range(100):
                b.step()
        mid = cheap.bars("EURUSD", TF.M1, 1)[-1].close
        a = cheap.send(OrderRequest("EURUSD", Side.BUY, 0.1, sl=mid - 0.0030))
        c = dear.send(OrderRequest("EURUSD", Side.BUY, 0.1, sl=mid - 0.0030))
        assert c.price > a.price

    def test_commission_is_charged_both_sides(self, replay):
        start = replay.account().balance
        mid = replay.bars("EURUSD", TF.M1, 1)[-1].close
        res = replay.send(OrderRequest("EURUSD", Side.BUY, 1.0,
                                       sl=mid - 0.0050))
        assert replay.account().balance == pytest.approx(start - 3.0)
        replay.close(res.ticket)
        assert replay.commission_paid == pytest.approx(6.0)

    def test_stop_is_assumed_to_fill_before_the_target(self):
        """The conservative assumption on a bar that could hit both."""
        series = {"EURUSD": bars([1.1000, 1.1000])}
        series["EURUSD"].append(Bar(T0 + dt.timedelta(minutes=2), 1.1000,
                                    1.1050, 1.0950, 1.1000, 300.0))
        b = ReplayBroker(series, {"EURUSD": default_spec("EURUSD", 1.1)})
        b.connect()
        b.step()
        res = b.send(OrderRequest("EURUSD", Side.BUY, 0.1, sl=1.0960,
                                  tp=1.1040))
        assert res.ok
        b.step()
        assert not b.positions(), "the position closed on that bar"
        assert b.closed[-1]["reason"] == "SL", (
            "when both were reachable, the stop is assumed to have filled")


class TestReplayMechanics:
    def test_invalid_stops_are_refused(self, replay):
        mid = replay.bars("EURUSD", TF.M1, 1)[-1].close
        res = replay.send(OrderRequest("EURUSD", Side.BUY, 0.1, sl=mid + 0.001))
        assert not res.ok
        assert replay.rejections == 1

    def test_partial_close_respects_the_lot_grid(self, replay):
        mid = replay.bars("EURUSD", TF.M1, 1)[-1].close
        res = replay.send(OrderRequest("EURUSD", Side.BUY, 1.0,
                                       sl=mid - 0.0050))
        out = replay.close(res.ticket, 0.004)
        assert not out.ok, "below the lot step"
        ok = replay.close(res.ticket, 0.4)
        assert ok.ok
        assert replay.positions()[0].volume == pytest.approx(0.6)

    def test_margin_is_reported(self, replay):
        mid = replay.bars("EURUSD", TF.M1, 1)[-1].close
        replay.send(OrderRequest("EURUSD", Side.BUY, 1.0, sl=mid - 0.0050))
        acct = replay.account()
        assert acct.margin > 0 and acct.margin_level > 0

    def test_csv_loading(self, tmp_path):
        path = tmp_path / "bars.csv"
        path.write_text(
            "time,open,high,low,close,tick_volume,spread\n"
            "2026-03-09 00:00:00,1.1000,1.1010,1.0995,1.1005,120,12\n"
            "2026-03-09 00:01:00,1.1005,1.1015,1.1000,1.1010,130,12\n")
        loaded = load_bars_csv(path)
        assert len(loaded) == 2
        assert loaded[0].close == pytest.approx(1.1005)
        assert loaded[1].spread_points == pytest.approx(12.0)

    def test_mt5_tab_separated_export_is_accepted(self, tmp_path):
        path = tmp_path / "mt5.csv"
        path.write_text(
            "<DATE>\t<TIME>\t<OPEN>\t<HIGH>\t<LOW>\t<CLOSE>\t<TICKVOL>\t"
            "<VOL>\t<SPREAD>\n"
            "2026.03.09\t00:00:00\t1.1000\t1.1010\t1.0995\t1.1005\t120\t0\t12\n")
        loaded = load_bars_csv(path)
        assert len(loaded) == 1
        assert loaded[0].high == pytest.approx(1.1010)


class TestBacktestHonesty:
    def _series(self, n=1400):
        px = 1.1000
        closes = []
        for i in range(n):
            px += 0.00004 if (i // 200) % 2 == 0 else -0.00004
            closes.append(px)
        return {"EURUSD": bars(closes)}

    def test_a_run_without_calendar_data_is_not_called_news_aware(self):
        result = run_backtest(self._series(),
                              {"EURUSD": default_spec("EURUSD", 1.1)},
                              label="no_news", data_kind="SYNTHETIC",
                              cycle_every=20, warmup_bars=500)
        assert result.news_aware is False
        assert "NOT news-aware" in result.news_note

    def test_a_run_whose_window_is_not_covered_says_so(self):
        series = self._series()
        far_future = series["EURUSD"][-1].time + dt.timedelta(days=40)
        result = run_backtest(
            series, {"EURUSD": default_spec("EURUSD", 1.1)},
            label="partial_news", data_kind="SYNTHETIC",
            events=[CalendarEvent("x", far_future, "USD", "US", "CPI", 3,
                                  3.6, 3.1, 3.4)],
            cycle_every=20, warmup_bars=500)
        assert result.news_aware is False
        assert "does not span" in result.news_note

    def test_synthetic_data_is_flagged_as_synthetic(self):
        result = run_backtest(self._series(),
                              {"EURUSD": default_spec("EURUSD", 1.1)},
                              label="synth", data_kind="SYNTHETIC",
                              cycle_every=20, warmup_bars=500)
        assert result.data_kind == "SYNTHETIC"
        assert any("SYNTHETIC" in n for n in result.notes)
        report = full_report(result)
        assert report["honesty"]["data_kind"] == "SYNTHETIC"
        assert "costs_included" in report["honesty"]
        assert "not_modelled" in report["honesty"]

    def test_no_open_positions_are_left_unrealised(self):
        result = run_backtest(self._series(),
                              {"EURUSD": default_spec("EURUSD", 1.1)},
                              label="closeout", data_kind="SYNTHETIC",
                              cycle_every=20, warmup_bars=500)
        assert result.ending_equity == pytest.approx(
            result.starting_equity + result.net_money, abs=0.01)

    def test_trials_are_recorded_in_the_ledger(self, tmp_path):
        from mintel.research.trial_ledger import TrialLedger
        led = TrialLedger(tmp_path / "trials.sqlite")
        for aggression in ("NORMAL", "AGGRESSIVE"):
            cfg = Config()
            cfg.aggression = aggression
            run_backtest(self._series(),
                         {"EURUSD": default_spec("EURUSD", 1.1)},
                         cfg=cfg, label="aggression", data_kind="SYNTHETIC",
                         cycle_every=20, warmup_bars=500, ledger=led)
        assert led.count("aggression") == 2
        assert led.honest_report("aggression")["trials_run"] == 2
        led.close()

    def test_the_backtest_runs_the_live_engine(self):
        """No parallel strategy implementation may exist."""
        import inspect
        from mintel.research import backtest
        src = inspect.getsource(backtest)
        assert "Trader(" in src
        assert "best_signal" not in src, (
            "the backtester must not re-implement decision logic")


class TestLossesAreLosses:
    def test_a_losing_trade_reduces_the_balance(self):
        series = {"EURUSD": bars([1.1000, 1.1000, 1.0900, 1.0900])}
        b = ReplayBroker(series, {"EURUSD": default_spec("EURUSD", 1.1)})
        b.connect()
        b.step()
        start = b.account().balance
        res = b.send(OrderRequest("EURUSD", Side.BUY, 1.0, sl=1.0950))
        assert res.ok
        b.step()
        assert not b.positions(), "the stop should have been hit"
        assert b.closed[-1]["pnl"] < 0, "a loss must be recorded as negative"
        assert b.account().balance < start

    def test_a_winning_trade_increases_the_balance(self):
        series = {"EURUSD": bars([1.1000, 1.1000, 1.1100, 1.1100])}
        b = ReplayBroker(series, {"EURUSD": default_spec("EURUSD", 1.1)})
        b.connect()
        b.step()
        start = b.account().balance
        res = b.send(OrderRequest("EURUSD", Side.BUY, 1.0, sl=1.0900,
                                  tp=1.1050))
        assert res.ok
        b.step()
        assert b.closed[-1]["pnl"] > 0
        assert b.account().balance > start

    def test_short_side_symmetry(self):
        down = {"EURUSD": bars([1.1000, 1.1000, 1.0900, 1.0900])}
        up = {"EURUSD": bars([1.1000, 1.1000, 1.1100, 1.1100])}
        spec = {"EURUSD": default_spec("EURUSD", 1.1)}
        winner = ReplayBroker(down, spec)
        loser = ReplayBroker(up, spec)
        for b, tp, sl in ((winner, 1.0950, 1.1100), (loser, 1.0900, 1.1050)):
            b.connect()
            b.step()
            b.send(OrderRequest("EURUSD", Side.SELL, 1.0, sl=sl, tp=tp))
            b.step()
        assert winner.closed[-1]["pnl"] > 0
        assert loser.closed[-1]["pnl"] < 0

    def test_equity_curve_can_fall(self):
        """The whole point: a strategy that loses must show losing equity."""
        px = 1.1000
        closes = [px]
        for i in range(1400):
            px += 0.00005 if (i // 60) % 2 == 0 else -0.00006
            closes.append(px)
        from mintel.research.backtest import run_backtest
        r = run_backtest({"EURUSD": bars(closes)},
                         {"EURUSD": default_spec("EURUSD", 1.1)},
                         label="losing_check", data_kind="SYNTHETIC",
                         cycle_every=20, warmup_bars=500)
        if r.trades == 0:
            pytest.skip("no trades in this window")
        assert r.losses > 0, "some trades must be recorded as losses"
        assert any(v < r.starting_equity for _, v in r.equity_curve) or \
            r.net_money > 0
