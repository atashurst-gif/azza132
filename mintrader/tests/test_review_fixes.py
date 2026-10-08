"""The 8 October code review, each confirmed finding pinned by a test:
tick stamps to the millisecond, velocity over the horizon, session windows
in their own zones, a malformed settings file, a refused stop or close at
the broker, the floor in money at the stop, the Band Breaker's last check
and its forming bar, and the nightly paper rows bounded by the day."""
import datetime as dt
import json
import sqlite3
from types import SimpleNamespace

import pytest

from mintel.broker.base import Side, Tick
from mintel.config import Config

UTC = dt.timezone.utc


class TestTickStamps:
    def test_the_millisecond_field_is_used_when_the_terminal_gives_it(self):
        from mintel.broker.mt5_adapter import _tick_seconds
        t = SimpleNamespace(time=1791443347, time_msc=1791443347907)
        assert _tick_seconds(t) == pytest.approx(1791443347.907)
        row = {"time": 1791443347, "time_msc": 1791443347907}          # a copy_ticks_range row
        assert _tick_seconds(row) == pytest.approx(1791443347.907)

    def test_without_it_the_whole_second_is_kept(self):
        from mintel.broker.mt5_adapter import _tick_seconds
        assert _tick_seconds(SimpleNamespace(time=1791443347)) == 1791443347.0
        assert _tick_seconds(SimpleNamespace(time=1791443347, time_msc=0)) == 1791443347.0
        assert _tick_seconds({"time": 1791443347}) == 1791443347.0
        assert _tick_seconds(SimpleNamespace()) == 0.0


class TestVelocityOnASecondStampedFeed:
    def test_a_burst_reads_as_movement_at_every_phase_of_the_clock(self, tmp_path):
        from mintel.scalper.velocity import micro
        from tests.test_velocity_scalper import POINT, TestInsideTheEngine, burst, quiet, tape
        exact = tape(quiet() + burst(), start=dt.datetime(2026, 10, 7, 8, 10, tzinfo=UTC) - dt.timedelta(seconds=63))
        e, b = TestInsideTheEngine()._engine(tmp_path)
        buf = e.buffers["EURUSD"]
        for t in exact.ticks:
            buf.add(Tick(t.symbol, t.time.replace(microsecond=0), t.bid, t.ask))   # as the old adapter stamped them
        true_v1 = micro(exact, POINT, exact.last.time).v(1.0)
        assert true_v1 > 0
        for phase in (0.0, 0.25, 0.5, 0.9):
            m = micro(buf, POINT, buf.last.time + dt.timedelta(seconds=phase))
            assert m.v(1.0) > 0, phase
            assert m.v(1.0) <= 2.2 * true_v1 + 1e-9, (phase, m.v(1.0), true_v1)
            assert m.v(0.5) >= 0

    def test_velocity_is_points_per_second_over_the_horizon(self):
        from mintel.scalper.features import TickBuffer
        from mintel.scalper.velocity import micro
        from tests.test_velocity_scalper import POINT
        buf = TickBuffer("EURUSD")
        t0 = dt.datetime(2026, 10, 7, 8, 10, tzinfo=UTC)
        # 5 ticks a second, each 2 points higher, for 20 seconds: 10 points a second
        for i in range(100):
            mid = 1.1000 + i * 2 * POINT
            buf.add(Tick("EURUSD", t0 + dt.timedelta(seconds=0.2 * i), mid - 0.5 * POINT, mid + 0.5 * POINT))
        m = micro(buf, POINT, buf.last.time)
        for h in (1.0, 2.0, 5.0, 10.0):
            assert m.v(h) == pytest.approx(10.0, rel=0.05), h


class TestSessionWindowsInTheirOwnZones:
    def _at(self, y, mo, d, hh, mm):
        return dt.datetime(y, mo, d, hh, mm, tzinfo=UTC)

    def test_the_week_the_uk_and_us_clocks_differ(self):
        from mintel.scalper.sessions import current_window, default_windows
        w = default_windows()
        # Wed 28 Oct 2026: London is back on GMT (25 Oct), New York still on daylight time (until 1 Nov)
        cw = current_window(self._at(2026, 10, 28, 13, 35), w)              # 09:35 New York
        assert (cw.name, cw.phase) == ("US cash", "OPENING")
        cw = current_window(self._at(2026, 10, 28, 12, 5), w)               # 08:05 New York
        assert (cw.name, cw.phase) == ("New York", "OPENING")
        cw = current_window(self._at(2026, 10, 28, 8, 10), w)               # 08:10 London
        assert (cw.name, cw.phase) == ("London", "OPENING")
        cw = current_window(self._at(2026, 10, 28, 14, 35), w)              # 10:35 New York: post-open
        assert (cw.name, cw.phase) == ("US cash", "POST_OPEN")

    def test_the_ordinary_week_is_unchanged_and_weekends_are_quiet(self):
        from mintel.scalper.sessions import current_window, default_windows
        w = default_windows()
        cw = current_window(self._at(2026, 10, 7, 13, 35), w)               # 09:35 New York in summer = 14:35 London
        assert (cw.name, cw.phase) == ("US cash", "OPENING")
        assert current_window(self._at(2026, 10, 31, 13, 35), w) is None   # Saturday


class TestAMalformedSettingsFile:
    def test_bad_entries_keep_their_defaults_and_good_ones_still_load(self, tmp_path):
        from mintel.scalper.config import ScalperConfig
        p = tmp_path / "scalper.json"
        p.write_text(json.dumps({"floor_ladder_r": [[1.0]], "loss_ladder_size": ["a", "b"],
                                 "velocity_weights": {"x": "not a number"}, "giveback_by_state": "nope",
                                 "min_confidence": 55, "entry_style": "VELOCITY"}))
        d = ScalperConfig()
        c = ScalperConfig.load(p)
        assert c.floor_ladder_r == d.floor_ladder_r and c.loss_ladder_size == d.loss_ladder_size
        assert c.velocity_weights == d.velocity_weights and c.giveback_by_state == d.giveback_by_state
        assert c.min_confidence == 55.0 and c.entry_style == "VELOCITY"


class TestWhenTheBrokerRefuses:
    def _engine_with_a_trade(self, tmp_path):
        from mintel.scalper.manage import Decision
        from tests.test_velocity_scalper import TestInsideTheEngine, trade
        e, b = TestInsideTheEngine()._engine(tmp_path)
        t = trade(b.spec("EURUSD"))
        e.trades[t.ticket] = t
        e.executor.stop_hit = lambda ticket, tick: False
        now = b.now
        tick = Tick("EURUSD", now, 1.10004, 1.10005)
        return e, t, now, tick, Decision

    def test_a_refused_stop_change_is_rolled_back_on_the_page_and_retried(self, tmp_path):
        e, t, now, tick, Decision = self._engine_with_a_trade(tmp_path)

        def wants_a_higher_stop(trade, tick, f, now, m=None):
            trade.stop_changes.append({"from": trade.stop, "to": 1.09995, "why": "test"})
            trade.stop, trade.protected_floor_money = 1.09995, 0.5
            return Decision(new_stop=1.09995, state=trade.state)
        e.manager.update = wants_a_higher_stop
        e.executor.modify_stop = lambda ticket, stop: False
        notes = e._manage(now, {"EURUSD": tick})
        assert t.stop == 1.09990 and t.protected_floor_money == 0.0 and t.stop_changes == []
        assert any("refused" in n for n in notes)
        e.executor.modify_stop = lambda ticket, stop: True
        e._manage(now, {"EURUSD": tick})
        assert t.stop == 1.09995 and t.protected_floor_money == 0.5 and len(t.stop_changes) == 1

    def test_a_refused_close_keeps_the_trade(self, tmp_path):
        from mintel.scalper.execution import Fill
        e, t, now, tick, Decision = self._engine_with_a_trade(tmp_path)
        e.manager.update = lambda trade, tick, f, now, m=None: Decision(close=True, exit_reason="FAILED_MOMENTUM", reason="test")
        e.executor.close = lambda ticket, tick, spec, volume=0.0, at_price=None: Fill(False, message="requote")
        notes = e._manage(now, {"EURUSD": tick})
        assert t.ticket in e.trades and t.state == "TRADE_VALIDATION"
        assert any("close refused" in n for n in notes)
        rows = e.journal._exec("SELECT * FROM events").fetchall()
        assert any("EXIT_FAILED" in " ".join(str(v) for v in tuple(r)) for r in rows)


class TestTheFloorIsMoneyAtTheStop:
    def test_half_a_lot_banks_half_the_money(self):
        from tests.test_velocity_scalper import TestTheRider, mk, trade
        r = TestTheRider(); r.setup_method()
        t = trade(r.spec, volume=0.5)                 # 10 points of risk on half a lot: about 5 per R at this spec
        r_money = r.spec.money_per_lot(t.risk_distance) * t.volume
        assert r_money != t.planned_loss               # the planned-loss figure would be the wrong unit here
        tk, now = r.tick(1.0, 1.0); r.m.update(t, tk, None, now, mk())
        tk, now = r.tick(7.0, 4.0); r.m.update(t, tk, None, now, mk())
        tk, now = r.tick(21.0, 6.0); r.m.update(t, tk, None, now, mk(accel_ratio=2.5, pressure=0.9))
        assert t.protected_floor_money >= 0.99 * 1.0 * r_money
        assert t.protected_floor_money < 1.0 * t.planned_loss or r_money > t.planned_loss


class TestBandBreakerLastCheck:
    def test_the_15_30_check_fires_and_ignores_the_forming_bar(self, tmp_path):
        from tests.test_band_breaker import TODAY, TestTheTrade, at, engine
        e = engine(tmp_path)
        t = TestTheTrade()
        inside = [(f"{h:02d}:{m:02d}", 5010, 5014, 5006, 5010) for h in range(10, 15) for m in (0, 30)]
        today = [("09:30", 5000, 5012, 4995, 5010)] + inside + [("15:00", 5010, 5014, 5006, 5010),
                                                                ("15:30", 5010, 5080, 5008, 5075)]   # 15:30 bar still forming
        t._run(e, at(TODAY, 15, 30), 5075.0, today, (15, 30))
        assert not e.open
        rows = e.checks_since(at(TODAY, 0, 0))
        assert rows and rows[-1]["check_ny"] == "15:30" and "inside the band" in rows[-1]["decision"]
        assert rows[-1]["close"] == pytest.approx(5010.0)


class TestPaperRowsStopAtTheDaysEnd:
    def test_tomorrows_trade_is_not_in_todays_file(self, tmp_path):
        from mintel.ops.report_upload import paper_bot_json
        cfg = Config(); cfg.ops.data_dir = str(tmp_path)
        db = sqlite3.connect(tmp_path / "runner.sqlite")
        db.execute("CREATE TABLE trades (ticket INTEGER, symbol TEXT, closed_utc TEXT, net REAL)")
        db.executemany("INSERT INTO trades VALUES (?,?,?,?)",
                       [(1, "US30", "2026-10-07T15:00:00+00:00", 5.0), (2, "US30", "2026-10-08T06:45:00+00:00", -3.0)])
        db.commit(); db.close()
        (tmp_path / "runner-status.json").write_text("{}")
        out = json.loads(paper_bot_json(cfg, dt.datetime(2026, 10, 7, tzinfo=UTC), "runner-status.json", "runner.sqlite"))
        assert [r["ticket"] for r in out["trades"]] == [1]
