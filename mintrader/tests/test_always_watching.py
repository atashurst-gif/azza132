"""Sep 24: the bot went down at 10:32 and nobody knew until the evening.

Never again: the trader waits for MetaTrader instead of dying, the bridge
serves its port before MetaTrader answers and says how MetaTrader is, the
watchdog restarts a terminal that has gone deaf, and an independent pulse
records UP/DOWN every ten minutes so an outage is a number in the review.
"""
from __future__ import annotations

import datetime as dt
import json
import threading
from pathlib import Path

import pytest

from mintel.config import Config
from mintel.ops import pulse as pl
from mintel.ops.dashboard import DashboardState

NOW = dt.datetime(2026, 9, 24, 10, 32, tzinfo=dt.timezone.utc)


def _cfg(workdir) -> Config:
    cfg = Config()
    cfg.ops.data_dir = str(workdir)
    cfg.ops.log_dir = str(workdir)
    cfg.broker_mode = "bridge"
    return cfg


class FlakyBroker:
    """Fails ``fail`` times, then connects."""
    def __init__(self, fail: int):
        self.fail = fail
        self.attempts = 0
        self.connected = False
        self.last_error = "IPC initialize failed"

    def connect(self) -> bool:
        self.attempts += 1
        self.connected = self.attempts > self.fail
        return self.connected

    def is_connected(self) -> bool:
        return self.connected


# ------------------------------------------------------------ the trader --
class TestTraderWaitsForMetaTrader:
    def test_keeps_trying_heartbeats_and_says_so_on_the_page(self, workdir):
        from mintel.run import wait_for_broker
        cfg = _cfg(workdir)
        broker = FlakyBroker(fail=3)
        state = DashboardState()
        stop = threading.Event()
        waits: list[float] = []
        ok = wait_for_broker(broker, cfg, state, stop, retry_seconds=15.0,
                             sleep=waits.append)
        assert ok and broker.attempts == 4
        assert waits == [15.0, 15.0, 15.0]
        # It told the page what it was waiting for, with the broker's reason.
        assert state.status["bot"] == "WAITING FOR METATRADER"
        assert "MetaTrader" in state.status["waiting"]
        assert "IPC initialize failed" in state.status["detail"]
        # And it heartbeat while waiting, so the watchdog leaves it alone.
        hb = json.loads((workdir / "heartbeats" / "strategy.heartbeat.json").read_text())
        assert hb["detail"]["waiting"] and hb["detail"]["attempt"] == 3

    def test_stop_while_waiting_returns_false_without_connecting(self, workdir):
        from mintel.run import wait_for_broker
        cfg = _cfg(workdir)
        broker = FlakyBroker(fail=10 ** 6)
        stop = threading.Event()

        def sleep(_s):
            stop.set()
        assert wait_for_broker(broker, cfg, DashboardState(), stop, sleep=sleep) is False
        assert broker.attempts == 1

    def test_a_bounded_wait_gives_up(self, workdir, monkeypatch):
        from mintel import run
        cfg = _cfg(workdir)
        clock = [0.0]
        monkeypatch.setattr(run.time, "time", lambda: clock[0])

        def sleep(s):
            clock[0] += s
        ok = run.wait_for_broker(FlakyBroker(fail=10 ** 6), cfg, DashboardState(),
                                 threading.Event(), max_wait_seconds=60, sleep=sleep)
        assert ok is False

    def test_page_and_status_command_show_the_wait(self):
        from mintel.ops.dashboard import render_status
        from mintel.status import render
        snap = {"status": {"bot": "WAITING FOR METATRADER",
                           "waiting": "waiting for the MetaTrader bridge on 127.0.0.1:8790",
                           "detail": "IPC initialize failed", "attempts": 7},
                "health": {}}
        page = render_status(snap)
        assert "WAITING FOR METATRADER" in page and "attempt 7" in page
        assert "WAITING FOR METATRADER" in render(snap)


# ------------------------------------------------------------ the bridge --
class TestBridgeServesBeforeMetaTraderAnswers:
    def test_ping_reports_metatrader_state_and_failures(self):
        from mintel.broker.bridge_server import BridgeService
        svc = BridgeService(FlakyBroker(fail=2))
        assert svc.handle("ping", {})["mt5_connected"] is False
        assert svc.connect_broker() is False
        assert svc.connect_broker() is False
        info = svc.handle("ping", {})
        assert info["connect_failures"] == 2
        assert "IPC initialize failed" in info["mt5_last_error"]
        assert svc.connect_broker() is True
        info = svc.handle("ping", {})
        assert info["mt5_connected"] is True and info["connect_failures"] == 0

    def test_ping_is_not_queued_behind_a_slow_broker_call(self):
        from mintel.broker.bridge_server import BridgeService
        svc = BridgeService(FlakyBroker(fail=0))
        svc.lock.acquire()                       # a connect in flight
        try:
            done = threading.Event()
            out: dict = {}

            def ping():
                out.update(svc.handle("ping", {}))
                done.set()
            threading.Thread(target=ping, daemon=True).start()
            assert done.wait(2.0), "ping waited for the service lock"
        finally:
            svc.lock.release()
        assert out["protocol"] >= 2

    def test_background_dialling_keeps_trying_until_connected(self):
        from mintel.broker.bridge_server import BridgeService, keep_connected
        broker = FlakyBroker(fail=2)
        svc = BridgeService(broker)
        stop = threading.Event()
        t = keep_connected(svc, interval=0.01, stop=stop)
        deadline = dt.datetime.now() + dt.timedelta(seconds=3)
        while not svc.mt5_connected and dt.datetime.now() < deadline:
            stop.wait(0.01)
        stop.set()
        t.join(1.0)
        assert svc.mt5_connected and broker.attempts >= 3

    def test_mt5_backend_is_not_dialled_before_the_port_is_bound(self):
        src = (Path(__file__).parent.parent / "mintel" / "broker" / "bridge_server.py").read_text()
        body = src.split("def main(")[1]
        assert body.index("server = serve(") < body.index("keep_connected(service")
        mt5_part = src.split("def build_broker")[1].split("def keep_connected")[0]
        assert "broker.connect()" not in mt5_part.split("from mintel.broker.mt5_adapter")[1]


# ---------------------------------------------------------- the watchdog --
class TestWatchdogTalksToTheBridgeProperly:
    def test_probe_carries_the_token(self, monkeypatch, workdir):
        from mintel.ops import watchdog as wd
        import mintel.broker.bridge_client as bc
        monkeypatch.setattr(wd, "port_open", lambda h, p, timeout=2.0: True)
        seen: dict = {}

        class Client:
            def __init__(self, **kw):
                seen.update(kw)
            def outdated(self):
                return ""
            def ping(self):
                return {"mt5_connected": True}
            def disconnect(self):
                pass
        monkeypatch.setattr(bc, "BridgeBroker", Client)
        tok = workdir / "bridge-token.txt"
        assert wd.bridge_current("127.0.0.1", 8790, str(tok)) is True
        assert seen["token_file"] == str(tok)
        assert wd.bridge_status("127.0.0.1", 8790, str(tok))["mt5_connected"] is True

    def test_build_default_wires_the_token_into_the_probe(self, workdir, monkeypatch):
        from mintel.ops import watchdog as wd
        cfg = _cfg(workdir)
        cfg.wine_python = "C:/py/python.exe"
        calls: list = []
        monkeypatch.setattr(wd, "bridge_current",
                            lambda h, p, tf="": calls.append(tf) or True)
        w = wd.build_default(cfg, str(workdir / "config.json"))
        bridge = [mp for mp in w.processes if mp.name == "bridge"][0]
        bridge.probe()
        assert calls == [cfg.bridge_token_file]
        assert w.bridge_status_fn is not None

    def test_a_deaf_terminal_is_closed_after_the_grace_period(self, workdir, monkeypatch):
        from mintel.ops import watchdog as wd
        cfg = _cfg(workdir)
        w = wd.Watchdog(cfg, [], mt5_command=["wine", "terminal64.exe"])
        w.mt5_unreachable_seconds = 600
        monkeypatch.setattr(wd, "process_running", lambda name: True)
        killed: list[str] = []
        monkeypatch.setattr(wd, "kill_process", killed.append)
        deaf = {"mt5_connected": False, "connect_failures": 3,
                "mt5_last_error": "mt5.initialize failed: (-10003, 'IPC initialize failed')"}
        w.bridge_status_fn = lambda: deaf
        t0 = NOW
        assert w._check_mt5(t0) == [] and killed == []                  # first sighting
        assert w._check_mt5(t0 + dt.timedelta(seconds=300)) == []       # still patient
        acts = w._check_mt5(t0 + dt.timedelta(seconds=601))
        assert killed == ["terminal64.exe"]
        assert acts and "not answered the bridge" in acts[0] and "IPC" in acts[0]
        # Once it answers, the clock resets.
        w.bridge_status_fn = lambda: {"mt5_connected": True, "connect_failures": 0}
        assert w._check_mt5(t0 + dt.timedelta(seconds=700)) == []
        assert w._mt5_unreachable_since is None

    def test_an_old_bridge_that_cannot_say_is_left_alone(self, workdir, monkeypatch):
        from mintel.ops import watchdog as wd
        cfg = _cfg(workdir)
        w = wd.Watchdog(cfg, [], mt5_command=["wine", "terminal64.exe"])
        monkeypatch.setattr(wd, "process_running", lambda name: True)
        killed: list[str] = []
        monkeypatch.setattr(wd, "kill_process", killed.append)
        w.bridge_status_fn = lambda: {"protocol": 2}
        for k in range(5):
            assert w._check_mt5(NOW + dt.timedelta(seconds=600 * k)) == []
        assert killed == []


# ------------------------------------------------------------- the pulse --
def _heartbeats(strategy_age=None, watchdog_age=None, waiting=None):
    out = {}
    if strategy_age is not None:
        out["strategy"] = {"age_seconds": strategy_age,
                           "detail": {"waiting": waiting} if waiting else {}}
    if watchdog_age is not None:
        out["watchdog"] = {"age_seconds": watchdog_age, "detail": {}}
    return out


class TestPulse:
    def test_up_when_the_page_says_running(self, workdir):
        cfg = _cfg(workdir)
        body = json.dumps({"status": {"bot": "RUNNING", "mt5_connected": True,
                                      "broker_connected": True, "equity": 1012.4,
                                      "open_positions": 1, "today_pnl": 3.2,
                                      "build": "abc"}, "health": {}})
        p = pl.take_pulse(cfg, NOW, fetch=lambda url: body,
                          heartbeats=_heartbeats(5, 3), bridge=lambda: {"mt5_connected": True},
                          port_is_open=lambda: True, terminal_running=lambda: True)
        assert p["up"] is True and p["reason"] == ""
        line = pl.pulse_line(p)
        assert line.startswith("2026-09-24T10:32:00Z UP") and "equity=1012.40" in line
        assert "today=+3.20" in line and "open=1" in line

    def test_down_explains_itself_from_the_outside(self, workdir):
        cfg = _cfg(workdir)
        (workdir / "watchdog.log").write_text(
            "2026-09-24T10:33:00 trader: it looks hung - restarting\n"
            "2026-09-24T11:00:00 trader: the process is not running, but the restart "
            "limit has been reached - NEEDS A HUMAN\n")

        def dead(url):
            raise OSError("connection refused")
        p = pl.take_pulse(cfg, NOW, fetch=dead, heartbeats=_heartbeats(37932, 4),
                          bridge=lambda: {"mt5_connected": False, "connect_failures": 4,
                                          "mt5_last_error": "IPC initialize failed"},
                          port_is_open=lambda: True, terminal_running=lambda: True)
        assert p["up"] is False
        r = p["reason"]
        assert "trader not answering (last heartbeat 37932s ago)" in r
        assert "watchdog alive" in r and "NEEDS A HUMAN" in r
        assert "MetaTrader is not connected (IPC initialize failed)" in r
        assert "terminal running" in r
        assert pl.pulse_line(p).startswith("2026-09-24T10:32:00Z DOWN trader not answering")

    def test_waiting_trader_is_down_with_the_bridge_view(self, workdir):
        cfg = _cfg(workdir)
        body = json.dumps({"status": {"bot": "WAITING FOR METATRADER",
                                      "waiting": "waiting for the MetaTrader bridge on 127.0.0.1:8790",
                                      "detail": "bridge connect failed"}, "health": {}})
        p = pl.take_pulse(cfg, NOW, fetch=lambda url: body, heartbeats=_heartbeats(2, 2),
                          bridge=lambda: None, port_is_open=lambda: False,
                          terminal_running=lambda: False)
        assert not p["up"]
        assert p["reason"].startswith("trader up, waiting for the MetaTrader bridge")
        assert "bridge not answering" in p["reason"]
        assert "terminal NOT running" in p["reason"]

    def test_safe_mode_and_disconnected_count_as_down(self, workdir):
        cfg = _cfg(workdir)
        probes = dict(heartbeats=_heartbeats(2, 2), bridge=lambda: {"mt5_connected": True},
                      port_is_open=lambda: True, terminal_running=lambda: True)
        safe = json.dumps({"status": {"bot": "RUNNING"}, "health": {"safe_mode": True, "summary": "data stale"}})
        assert "SAFE MODE: data stale" in pl.take_pulse(cfg, NOW, fetch=lambda u: safe, **probes)["reason"]
        off = json.dumps({"status": {"bot": "RUNNING", "mt5_connected": False}, "health": {}})
        assert "not connected" in pl.take_pulse(cfg, NOW, fetch=lambda u: off, **probes)["reason"]

    def test_log_is_appended_and_filtered_by_day(self, workdir):
        cfg = _cfg(workdir)
        pl.append_uptime(cfg, "2026-09-23T23:50:00Z UP   equity=1.00 open=0 today=+0.00")
        pl.append_uptime(cfg, "2026-09-24T10:32:00Z DOWN trader not answering")
        pl.append_uptime(cfg, "2026-09-24T10:42:00Z DOWN trader not answering")
        assert len(pl.uptime_lines(cfg)) == 3
        assert len(pl.uptime_lines(cfg, NOW.replace(hour=0, minute=0))) == 2
        files = pl.pulse_files({"ts_utc": "x", "up": False, "reason": "r", "status": {"a": 1}},
                               pl.uptime_lines(cfg))
        assert set(files) == {"reports/live/pulse.json", "reports/live/status.json",
                              "reports/live/uptime.log"}
        assert json.loads(files["reports/live/status.json"]) == {"a": 1}
        assert "status" not in json.loads(files["reports/live/pulse.json"])
        assert files["reports/live/uptime.log"].count("\n") == 3

    def test_summary_sums_the_gaps(self):
        lines = ["2026-09-24T09:00:00Z UP   equity=1.00 open=0 today=+0.00",
                 "2026-09-24T09:10:00Z UP   equity=1.00 open=0 today=+0.00",
                 "2026-09-24T10:40:00Z DOWN trader not answering (last heartbeat 480s ago); watchdog alive",
                 "2026-09-24T10:50:00Z DOWN trader not answering (last heartbeat 1080s ago); watchdog alive",
                 "2026-09-24T21:10:00Z UP   equity=1.00 open=0 today=+0.00",
                 "2026-09-24T21:20:00Z DOWN bridge not answering"]
        s = pl.uptime_summary(lines)
        assert "up for 50% of the day's pulses (3 of 6" in s
        assert "DOWN 10:40 to 10:50 (~20 min): trader not answering" in s
        assert "DOWN 21:20 to still down (~10 min): bridge not answering" in s
        assert "No pulse record" in pl.uptime_summary([])
        assert "No outages" in pl.uptime_summary(lines[:2])

    def test_nightly_bundle_carries_the_days_uptime(self, workdir):
        from mintel.ops import report_upload as ru
        from tests.test_report_upload import FakeBroker, _journal_with_trades
        cfg = _cfg(workdir)
        cfg.tracking_start_utc = "2026-09-15T12:59:00+00:00"
        _journal_with_trades(workdir)
        pl.append_uptime(cfg, "2026-09-17T10:40:00Z DOWN trader not answering")
        pl.append_uptime(cfg, "2026-09-17T10:50:00Z UP   equity=1.00 open=0 today=+0.00")
        day = dt.datetime(2026, 9, 17, tzinfo=dt.timezone.utc)
        files = ru.build_bundle(cfg, FakeBroker(seed=1), day, fetch=lambda u: "{}")
        assert files["reports/2026-09-17/uptime.log"].count("DOWN") == 1
        assert "BOT UPTIME" in files["reports/2026-09-17/day_review.txt"]
        assert "DOWN 10:40 to 10:40 (~10 min)" in files["reports/2026-09-17/day_review.txt"]
        assert "reports/latest/uptime.log" in files

    def test_main_writes_the_log_without_a_token(self, workdir, monkeypatch, capsys):
        cfg = _cfg(workdir)
        (workdir / "config.json").write_text("{}")
        monkeypatch.setattr(pl.Config, "load", staticmethod(lambda p: cfg))
        monkeypatch.setattr(pl, "take_pulse", lambda c: {"ts_utc": NOW.isoformat(), "up": False,
                                                          "reason": "bridge not answering", "status": None})
        assert pl.main(["--config", str(workdir / "config.json")]) == 0
        assert "DOWN bridge not answering" in (workdir / "uptime.log").read_text()
        assert "no GitHub token" in capsys.readouterr().out


class TestInstallerWiring:
    def test_pulse_agent_runs_every_ten_minutes(self):
        body = (Path(__file__).parent.parent / "deploy" / "mac" / "mintel_mac.sh").read_text()
        assert "com.mintel.pulse" in body and "mintel.ops.pulse" in body
        assert "<key>StartInterval</key><integer>600</integer>" in body
        assert "install_pulse_agent\n" in body.split("install_report_agent() {")[1]
