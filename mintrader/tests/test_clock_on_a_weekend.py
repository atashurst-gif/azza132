"""Sat 26 Sep: "this machine's clock is 180s out of step with the broker",
new trades paused. The offset had been measured from Friday's last tick.
A tick that is hours old says the market is closed, not that the clock is
wrong."""
from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pytest

from mintel.clock import UTC, ServerClock
from mintel.broker.mt5_adapter import Mt5Broker
from mintel.config import Config
from mintel.ops.health import HealthSupervisor, Severity

NOW = dt.datetime(2026, 9, 26, 8, 31, tzinfo=UTC)          # Saturday morning UTC


class FakeMt5:
    def __init__(self, tick_utc: dt.datetime, offset_h: float = 3.0):
        self.tick_server = tick_utc + dt.timedelta(hours=offset_h)
    def symbol_select(self, sym, flag): return True
    def symbol_info_tick(self, sym):
        return SimpleNamespace(time=int(self.tick_server.replace(tzinfo=UTC).timestamp()))


def _broker(tick_utc, monkeypatch, offset_h=3.0):
    b = Mt5Broker()
    b._mt5 = FakeMt5(tick_utc, offset_h)
    import mintel.broker.mt5_adapter as m
    monkeypatch.setattr(m, "utcnow", lambda: NOW)
    return b


class TestQuantisedMeasurement:
    def test_live_tick_gives_the_half_hour_offset_and_a_tiny_residual(self):
        srv = (NOW + dt.timedelta(hours=3, seconds=4)).replace(tzinfo=None)
        clock, residual = ServerClock.measure_quantised(srv, NOW)
        assert clock.offset_seconds == 3 * 3600 and abs(residual) == pytest.approx(4)

    def test_half_hour_brokers_are_respected(self):
        srv = (NOW + dt.timedelta(hours=5, minutes=30, seconds=-2)).replace(tzinfo=None)
        clock, residual = ServerClock.measure_quantised(srv, NOW)
        assert clock.offset_seconds == 5 * 3600 + 1800 and abs(residual) == pytest.approx(2)

    def test_a_stale_tick_shows_as_a_large_residual(self):
        friday_close = NOW - dt.timedelta(hours=11, minutes=33)
        srv = (friday_close + dt.timedelta(hours=3)).replace(tzinfo=None)
        clock, residual = ServerClock.measure_quantised(srv, NOW)
        assert abs(residual) > 120


class TestAdapterDoesNotTrustAStaleTick:
    def test_saturday_restart_keeps_a_known_offset(self, monkeypatch):
        b = _broker(NOW - dt.timedelta(seconds=3), monkeypatch)
        b._sync_clock()                                   # Friday: live tick
        assert b.clock.offset_seconds == 3 * 3600 and b._clock_measured
        b._mt5 = FakeMt5(NOW - dt.timedelta(hours=11, minutes=33))  # Saturday: Friday's tick
        b._sync_clock()
        assert b.clock.offset_seconds == 3 * 3600, "a stale tick must not move the offset"

    def test_first_sync_on_a_weekend_reads_the_offset_from_the_friday_close(self, monkeypatch):
        # Friday 25 Sep 2026 close = 17:00 New York = 21:00 UTC; last tick 20:58 UTC
        b = _broker(dt.datetime(2026, 9, 25, 20, 58, tzinfo=UTC), monkeypatch)
        b._sync_clock()
        assert b.clock.offset_seconds == 3 * 3600
        assert b._clock_measured is False                 # to be confirmed on Monday
        out = b.clock_skew()
        assert out["skew_seconds"] is None and out["tick_age_seconds"] > 3600

    def test_weekday_open_market_with_an_old_tick_means_a_fast_machine(self, monkeypatch):
        thursday = dt.datetime(2026, 9, 24, 10, 0, tzinfo=UTC)
        b = _broker(thursday - dt.timedelta(seconds=3), monkeypatch)
        import mintel.broker.mt5_adapter as m
        monkeypatch.setattr(m, "utcnow", lambda: thursday)
        b._sync_clock()
        b._mt5 = FakeMt5(thursday - dt.timedelta(seconds=180))
        assert b.clock_skew()["skew_seconds"] == pytest.approx(180)

    def test_market_hours(self):
        from mintel.clock import fx_market_open, last_fx_close_utc
        assert fx_market_open(dt.datetime(2026, 9, 24, 10, 0, tzinfo=UTC))       # Thursday
        assert not fx_market_open(NOW)                                            # Saturday
        assert not fx_market_open(dt.datetime(2026, 9, 27, 12, 0, tzinfo=UTC))   # Sunday noon
        assert fx_market_open(dt.datetime(2026, 9, 27, 22, 0, tzinfo=UTC))       # Sunday 18:00 NY
        assert not fx_market_open(dt.datetime(2026, 9, 24, 21, 2, tzinfo=UTC))   # daily rollover
        assert last_fx_close_utc(NOW) == dt.datetime(2026, 9, 25, 21, 0, tzinfo=UTC)

    def test_live_tick_reports_skew_and_heals_a_wrong_offset(self, monkeypatch):
        b = _broker(NOW - dt.timedelta(seconds=5), monkeypatch)
        b._clock = ServerClock(2 * 3600)                  # wrong by an hour
        out = b.clock_skew()
        assert out["skew_seconds"] == pytest.approx(5)
        assert b.clock.offset_seconds == 3 * 3600

    def test_a_genuinely_wrong_machine_clock_is_still_caught(self, monkeypatch):
        # the Mac is 3 minutes slow: a live tick lands 180s "ahead" of now
        b = _broker(NOW + dt.timedelta(seconds=180), monkeypatch)
        out = b.clock_skew()
        assert out["skew_seconds"] == pytest.approx(180)


class TestHealthCheckOnAWeekend:
    def _sup(self, cfg, journal, broker, sim):
        return HealthSupervisor(cfg, broker, journal, expected_login=sim.login,
                                expected_server=sim.server, clock=lambda: NOW)

    def test_closed_market_is_not_a_clock_problem(self, cfg, sim, journal):
        class Weekend:
            login = sim.login; server = sim.server
            def clock_skew(self): return {"skew_seconds": None, "tick_age_seconds": 41580.0}
            def __getattr__(self, name): return getattr(sim, name)
        r = self._sup(cfg, journal, Weekend(), sim).run_checks(positions=[], last_scan=NOW)
        c = next(c for c in r.checks if c.name == "CLOCK")
        assert c.severity is Severity.OK and "closed or quiet" in c.message
        assert not c.blocks_entries

    def test_a_live_skew_still_fails(self, cfg, sim, journal):
        class Skewed:
            login = sim.login; server = sim.server
            def clock_skew(self): return {"skew_seconds": 180.0, "tick_age_seconds": 180.0}
            def __getattr__(self, name): return getattr(sim, name)
        r = self._sup(cfg, journal, Skewed(), sim).run_checks(positions=[], last_scan=NOW)
        c = next(c for c in r.checks if c.name == "CLOCK")
        assert c.severity is Severity.FAIL and c.blocks_entries

    def test_bridge_exposes_it_and_an_old_bridge_is_harmless(self):
        from mintel.broker.bridge_server import BridgeService
        svc = BridgeService(SimpleNamespace(clock_skew=lambda: {"skew_seconds": 2.0, "tick_age_seconds": 2.0}))
        assert svc.handle("clock_skew", {})["skew_seconds"] == 2.0
        svc2 = BridgeService(SimpleNamespace())
        assert svc2.handle("clock_skew", {})["skew_seconds"] is None
