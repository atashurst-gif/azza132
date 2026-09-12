"""Health checks, safe mode, self-healing, heartbeats and the watchdog."""
from __future__ import annotations

import datetime as dt
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from mintel.broker.base import OrderRequest, Position, Side
from mintel.config import Config
from mintel.ops.health import (Healer, HealthSupervisor, Heartbeat, Severity,
                               read_all_heartbeats)
from mintel.ops.watchdog import (ManagedProcess, Watchdog, pid_alive,
                                 process_running)

UTC = dt.timezone.utc


@pytest.fixture
def sup(sim, cfg, journal):
    return HealthSupervisor(cfg, sim, journal, None, None,
                            expected_login=sim.login,
                            expected_server=sim.server,
                            clock=lambda: sim.now,
                            own_heartbeats=("strategy",))


def check(report, name):
    return next(c for c in report.checks if c.name == name)


class TestHealthyBaseline:
    def test_clean_system_reports_healthy(self, sup, sim):
        r = sup.run_checks(positions=[], last_scan=sim.now)
        assert not r.failures, [c.message for c in r.failures]
        assert r.entries_allowed
        assert not r.safe_mode
        assert r.summary


class TestBrokerChecks:
    def test_disconnect_enters_safe_mode(self, sup, sim):
        sim.fault.disconnect = True
        r = sup.run_checks(positions=[], last_scan=sim.now)
        assert r.safe_mode
        assert not r.entries_allowed
        assert "not connected" in check(r, "BROKER_CONNECTION").message

    def test_wrong_account_is_critical(self, cfg, sim, journal):
        sup = HealthSupervisor(cfg, sim, journal, expected_login=999,
                               expected_server=sim.server,
                               clock=lambda: sim.now)
        r = sup.run_checks(positions=[], last_scan=sim.now)
        assert r.safe_mode
        assert "expects 999" in check(r, "ACCOUNT").message

    def test_wrong_server_is_critical(self, cfg, sim, journal):
        sup = HealthSupervisor(cfg, sim, journal, expected_login=sim.login,
                               expected_server="SomeOtherBroker-Live",
                               clock=lambda: sim.now)
        r = sup.run_checks(positions=[], last_scan=sim.now)
        assert r.safe_mode

    def test_live_account_with_demo_config_refuses_to_trade(self, cfg, journal):
        from mintel.broker.sim import SimBroker
        live = SimBroker(seed=1, start=dt.datetime(2026, 3, 10, 13, 30,
                                                   tzinfo=UTC),
                         is_demo=False, history_bars=800)
        live.connect()
        cfg.mode = "DEMO"
        sup = HealthSupervisor(cfg, live, journal, expected_login=live.login,
                               expected_server=live.server,
                               clock=lambda: live.now)
        r = sup.run_checks(positions=[], last_scan=live.now)
        assert r.safe_mode
        assert "LIVE account" in check(r, "ACCOUNT").message

    def test_trading_disabled_blocks_entries(self, sup, sim):
        sim.fault.trade_disabled = True
        r = sup.run_checks(positions=[], last_scan=sim.now)
        assert not r.entries_allowed
        assert "Algo Trading" in check(r, "TRADE_PERMISSION").message


class TestDataChecks:
    def test_stale_ticks_are_detected(self, sup, sim):
        sim.fault.stale_ticks = True
        sim.advance(10)                  # ten minutes with no new prices
        r = sup.run_checks(positions=[], last_scan=sim.now)
        assert not r.entries_allowed
        assert "stale" in check(r, "MARKET_DATA").message

    def test_weekend_staleness_is_a_warning_not_a_failure(self, cfg, journal):
        from mintel.broker.sim import SimBroker
        weekend = SimBroker(seed=3,
                            start=dt.datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
                            history_bars=800)
        weekend.connect()
        weekend.fault.stale_ticks = True
        weekend.advance(400)          # beyond even the relaxed weekend limit
        sup = HealthSupervisor(cfg, weekend, journal,
                               expected_login=weekend.login,
                               expected_server=weekend.server,
                               clock=lambda: weekend.now)
        r = sup.run_checks(positions=[], last_scan=weekend.now)
        c = check(r, "MARKET_DATA")
        assert c.severity is Severity.WARN
        assert not r.safe_mode, "a closed market is not a malfunction"

    def test_clock_skew_is_detected(self, cfg, sim, journal):
        sup = HealthSupervisor(cfg, sim, journal, expected_login=sim.login,
                               expected_server=sim.server,
                               clock=lambda: sim.now + dt.timedelta(minutes=5))
        r = sup.run_checks(positions=[], last_scan=sim.now)
        assert "out of step" in check(r, "CLOCK").message
        assert not r.entries_allowed

    def test_scan_freshness_uses_the_supplied_limit(self, sup, sim):
        old = sim.now - dt.timedelta(seconds=600)
        tight = sup.run_checks(positions=[], last_scan=old)
        assert not tight.entries_allowed
        loose = sup.run_checks(positions=[], last_scan=old,
                               scan_stale_limit=3600.0)
        assert loose.entries_allowed


class TestPositionProtection:
    def test_a_naked_position_blocks_new_entries(self, sup, sim):
        p = Position(1, "EURUSD", Side.BUY, 0.1, 1.10, 0.0, 0.0, sim.now)
        r = sup.run_checks(positions=[p], last_scan=sim.now)
        assert not r.entries_allowed
        assert "no stop loss" in check(r, "POSITION_PROTECTION").message

    def test_protected_positions_pass(self, sup, sim):
        p = Position(1, "EURUSD", Side.BUY, 0.1, 1.10, 1.09, 0.0, sim.now)
        r = sup.run_checks(positions=[p], last_scan=sim.now)
        assert check(r, "POSITION_PROTECTION").ok


class TestSafeMode:
    def test_safe_mode_is_entered_and_left_automatically(self, sup, sim):
        sim.fault.disconnect = True
        assert sup.run_checks(positions=[], last_scan=sim.now).safe_mode
        sim.fault.disconnect = False
        sim.connect()
        assert not sup.run_checks(positions=[], last_scan=sim.now).safe_mode

    def test_safe_mode_is_journalled(self, sup, sim, journal):
        sim.fault.disconnect = True
        sup.run_checks(positions=[], last_scan=sim.now)
        kinds = [e["kind"] for e in journal.recent_events()]
        assert "SAFE_MODE" in kinds


class TestSelfHealing:
    def test_broker_reconnect_is_attempted_and_succeeds(self, sup, sim):
        sim.fault.disconnect = True
        report = sup.run_checks(positions=[], last_scan=sim.now)
        sim.fault.disconnect = False        # the fault clears
        actions = sup.heal(report)
        assert any("recovered" in a for a in actions)
        assert sim.is_connected()

    def test_database_reconnect(self, sup, journal, sim):
        journal.close()
        report = sup.run_checks(positions=[], last_scan=sim.now)
        assert any(c.name == "DATABASE" and not c.ok for c in report.checks)
        actions = sup.heal(report)
        assert any("database" in a for a in actions)
        assert journal.healthy()

    def test_healer_is_rate_limited(self, sup, sim):
        calls = {"n": 0}

        def always_fail():
            calls["n"] += 1
            return False

        sup.register_healer("BROKER_CONNECTION",
                            Healer("test", always_fail, max_per_hour=2))
        sim.fault.disconnect = True
        for _ in range(6):
            report = sup.run_checks(positions=[], last_scan=sim.now)
            sup.heal(report)
        assert calls["n"] == 2, "a broken environment must not loop forever"

    def test_reconnect_triggers_reconciliation(self, cfg, sim, journal):
        from mintel.engine.execution import Executor
        ex = Executor(sim, cfg, journal, clock=lambda: sim.now)
        sup = HealthSupervisor(cfg, sim, journal, None, ex,
                               expected_login=sim.login,
                               expected_server=sim.server,
                               clock=lambda: sim.now)
        sim.send(OrderRequest("EURUSD", Side.BUY, 0.1, sl=0.0,
                              magic=cfg.magic))
        sim.fault.disconnect = True
        report = sup.run_checks(positions=[], last_scan=sim.now)
        sim.fault.disconnect = False
        actions = sup.heal(report)
        assert any("reconciled" in a for a in actions)


class TestHeartbeats:
    def test_beat_and_read(self, workdir, sim):
        hb = Heartbeat(workdir / "hb", "worker", clock=lambda: sim.now)
        hb.beat({"x": 1})
        data = hb.read()
        assert data["name"] == "worker" and data["detail"]["x"] == 1
        assert hb.age_seconds() == pytest.approx(0.0, abs=1.0)

    def test_missing_heartbeat_reads_as_none(self, workdir):
        hb = Heartbeat(workdir / "hb", "absent")
        assert hb.read() is None
        assert hb.age_seconds() is None

    def test_a_process_does_not_certify_its_own_health(self, sup, sim, workdir):
        """The trader's own beats are excluded from its own check."""
        own = Heartbeat(sup.heartbeat_dir, "strategy", clock=lambda: sim.now)
        own.beat()
        sim.advance(60)                     # its own beat is now an hour old
        r = sup.run_checks(positions=[], last_scan=sim.now)
        c = check(r, "HEARTBEATS")
        assert c.severity is Severity.WARN, (
            "the missing WATCHDOG should be the complaint, not its own beat")
        assert "watchdog" in c.message

    def test_a_dead_watchdog_warns_but_does_not_stop_trading(self, sup, sim):
        wd = Heartbeat(sup.heartbeat_dir, "watchdog", clock=lambda: sim.now)
        wd.beat()
        sim.advance(60)
        r = sup.run_checks(positions=[], last_scan=sim.now)
        c = check(r, "HEARTBEATS")
        assert c.severity is Severity.WARN
        assert r.entries_allowed, (
            "the trader is demonstrably working; losing its supervisor is a "
            "warning, not a reason to stop")

    def test_another_dead_component_does_stop_entries(self, sup, sim):
        other = Heartbeat(sup.heartbeat_dir, "news_worker",
                          clock=lambda: sim.now)
        other.beat()
        sim.advance(60)
        r = sup.run_checks(positions=[], last_scan=sim.now)
        assert not r.entries_allowed

    def test_atomic_write_never_leaves_a_partial_file(self, workdir, sim):
        hb = Heartbeat(workdir / "hb", "w", clock=lambda: sim.now)
        for _ in range(30):
            hb.beat({"n": 1})
            assert hb.read() is not None


class TestWatchdog:
    def _worker_script(self, tmp_path: Path, beats: int, sleep: float) -> Path:
        root = Path(__file__).resolve().parents[1]
        script = tmp_path / "worker.py"
        script.write_text(
            "import sys, time\n"
            f"sys.path.insert(0, {str(root)!r})\n"
            "from mintel.ops.health import Heartbeat\n"
            f"hb = Heartbeat({str(tmp_path / 'data' / 'heartbeats')!r}, 'w')\n"
            f"for i in range({beats}):\n"
            f"    hb.beat({{'i': i}})\n"
            f"    time.sleep({sleep})\n")
        return script

    def test_watchdog_starts_a_missing_process(self, tmp_path):
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path / "data")
        cfg.ops.log_dir = str(tmp_path / "logs")
        script = self._worker_script(tmp_path, 20, 0.1)
        mp = ManagedProcess("w", [sys.executable, str(script)],
                            heartbeat="w", grace_seconds=0.05)
        wd = Watchdog(cfg, [mp])
        actions = wd.check_once()
        assert any("restarting" in a for a in actions)
        time.sleep(0.5)
        assert wd.check_once() == [], "a healthy worker is left alone"
        mp.stop()

    def test_watchdog_restarts_a_dead_process(self, tmp_path):
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path / "data")
        cfg.ops.log_dir = str(tmp_path / "logs")
        script = self._worker_script(tmp_path, 1, 0.0)
        mp = ManagedProcess("w", [sys.executable, str(script)],
                            heartbeat="w", grace_seconds=0.05)
        wd = Watchdog(cfg, [mp])
        wd.check_once()
        time.sleep(0.8)                     # the worker exits
        actions = wd.check_once()
        assert any("not running" in a for a in actions)
        mp.stop()

    def test_restart_limit_escalates_to_a_human(self, tmp_path):
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path / "data")
        cfg.ops.log_dir = str(tmp_path / "logs")
        mp = ManagedProcess("w", [sys.executable, "-c", "pass"],
                            heartbeat="never", grace_seconds=0.0,
                            max_restarts_per_hour=1)
        wd = Watchdog(cfg, [mp])
        wd.check_once()
        mp.backoff = 0.0
        actions = wd.check_once()
        assert any("NEEDS A HUMAN" in a for a in actions)

    def test_watchdog_writes_its_own_heartbeat(self, tmp_path):
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path / "data")
        cfg.ops.log_dir = str(tmp_path / "logs")
        wd = Watchdog(cfg, [])
        wd.check_once()
        assert wd.own_heartbeat.read() is not None

    def test_watchdog_logs_its_actions(self, tmp_path):
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path / "data")
        cfg.ops.log_dir = str(tmp_path / "logs")
        mp = ManagedProcess("w", [sys.executable, "-c", "pass"],
                            heartbeat="never", grace_seconds=0.0)
        wd = Watchdog(cfg, [mp])
        wd.check_once()
        log_file = Path(cfg.ops.log_dir) / "watchdog.log"
        assert log_file.exists() and log_file.read_text().strip()

    def test_pid_liveness(self):
        assert pid_alive(os.getpid()) is True
        assert pid_alive(2 ** 22) is False

    def test_unknown_pid_state_is_not_treated_as_dead(self, tmp_path):
        mp = ManagedProcess("x", ["true"], pid_file=str(tmp_path / "none.pid"))
        assert mp.alive_by_pid() is None
