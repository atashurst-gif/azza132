"""The status page must never show a false £0.00.

Day two: MetaTrader showed the bot's trades, the page showed nothing. The
bridge that answers the trader had survived the upgrade (the installer's
kill pattern did not match its Wine command line), so it lacked the deal
history call and the client turned its error into "no deals".
"""
from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pytest

from mintel.broker.base import BrokerError
from mintel.broker.stamp import code_stamp
from mintel.config import Config

NOW = dt.datetime(2026, 9, 15, 17, 30, tzinfo=dt.timezone.utc)


class TestBridgeStamp:
    def test_stamp_is_stable_and_changes_with_the_code(self, tmp_path):
        a = code_stamp()
        assert a == code_stamp() and len(a) == 12
        root = tmp_path
        for name in ("bridge_server.py", "mt5_adapter.py", "wire.py", "base.py"):
            (root / name).write_text("x")
        s1 = code_stamp(root)
        (root / "mt5_adapter.py").write_text("y")
        assert code_stamp(root) != s1

    def test_ping_reports_the_stamp(self):
        from mintel.broker.bridge_server import BridgeService, PROTOCOL_VERSION
        from mintel.broker.sim import SimBroker
        svc = BridgeService(SimBroker(("EURUSD",), start=NOW))
        info = svc.do_ping()
        assert info["code_stamp"] == code_stamp()
        assert PROTOCOL_VERSION >= 2

    def test_client_flags_an_old_bridge(self, monkeypatch):
        from mintel.broker.bridge_client import BridgeBroker
        c = BridgeBroker(host="127.0.0.1", port=1)
        monkeypatch.setattr(c, "ping", lambda: {"protocol": 1})      # no stamp
        assert "old code" in c.outdated()
        monkeypatch.setattr(c, "ping", lambda: {"code_stamp": code_stamp()})
        assert c.outdated() == ""
        monkeypatch.setattr(c, "ping", lambda: None)
        assert c.outdated() == ""       # not answering is a different fault


class TestWatchdogReplacesAStaleBridge:
    def test_probe_fails_for_an_outdated_bridge(self, monkeypatch):
        from mintel.ops import watchdog as wd
        monkeypatch.setattr(wd, "port_open", lambda h, p, timeout=2.0: True)

        class FakeClient:
            def __init__(self, **kw): pass
            def outdated(self): return "the MetaTrader bridge is running old code"
            def disconnect(self): pass
        import mintel.broker.bridge_client as bc
        monkeypatch.setattr(bc, "BridgeBroker", FakeClient)
        assert wd.bridge_current("127.0.0.1", 8790) is False

        class Fresh(FakeClient):
            def outdated(self): return ""
        monkeypatch.setattr(bc, "BridgeBroker", Fresh)
        assert wd.bridge_current("127.0.0.1", 8790) is True

    def test_stop_kills_an_adopted_pid_and_the_pattern(self, tmp_path, monkeypatch):
        from mintel.ops.watchdog import ManagedProcess
        import mintel.ops.watchdog as wd
        killed, ran = [], []
        (tmp_path / "bridge.pid").write_text("4242")
        monkeypatch.setattr(wd.os, "kill", lambda pid, sig: killed.append((pid, sig)))
        monkeypatch.setattr(wd, "pid_alive", lambda pid: False)
        monkeypatch.setattr(wd.subprocess, "run",
                            lambda cmd, **kw: ran.append(cmd) or SimpleNamespace(returncode=0))
        mp = ManagedProcess(name="bridge", command=["x"], pid_file=str(tmp_path / "bridge.pid"),
                            kill_pattern="bridge_server.py")
        mp.stop()
        assert killed and killed[0][0] == 4242
        assert ran and ran[0][:3] == ["pkill", "-f", "bridge_server.py"]

    def test_installer_kill_pattern_matches_a_wine_command_line(self):
        import re
        from pathlib import Path
        body = (Path(__file__).parent.parent / "deploy" / "mac" / "mintel_mac.sh").read_text()
        assert 'pkill -f "bridge_server.py"' in body
        wine_cmdline = r"wine64 C:\Python311\python.exe Z:\Users\me\MarketBot\app\mintel\broker\bridge_server.py --port 8790"
        assert re.search("bridge_server.py", wine_cmdline)
        # the watchdog's pattern matches the bridge but not the installer's check
        from mintel.config import Config
        from mintel.ops import watchdog as wd
        cfg = Config(); cfg.broker_mode = "bridge"; cfg.wine_python = "C:/py/python.exe"
        w = wd.build_default(cfg, "/tmp/x/config.json")
        pat = [mp for mp in w.processes if mp.name == "bridge"][0].kill_pattern
        assert re.search(pat, wine_cmdline)
        assert not re.search(pat, r"python.exe Z:\...\bridge_server.py --help")
        assert "(again)" in body
        assert not re.search("mintel/broker/bridge_server.py", wine_cmdline)


class TestHistoryQueryIsRobust:
    def _adapter(self, deals, calls):
        from mintel.broker.mt5_adapter import Mt5Broker
        from mintel.clock import ServerClock

        class FakeMt5:
            def history_deals_get(self, *a, **k):
                calls.append((a, k))
                return deals
            def last_error(self): return (1, "no history")
        a = Mt5Broker()
        a._mt5 = FakeMt5()
        a._clock = ServerClock(offset_seconds=3 * 3600)
        return a

    def test_bounds_are_integer_seconds_padded_a_day_each_side(self):
        calls = []
        a = self._adapter((), calls)
        since = NOW.replace(hour=0, minute=0)
        assert a.deals_since(since, 0, False) == []
        (lo, hi), _ = calls[0]
        assert isinstance(lo, int) and isinstance(hi, int)
        import calendar
        expect_lo = calendar.timegm((since - dt.timedelta(days=1)
                                     + dt.timedelta(hours=3)).timetuple())
        assert lo == expect_lo
        assert hi > calendar.timegm((dt.datetime.now(dt.timezone.utc)
                                     + dt.timedelta(hours=26)).timetuple())

    def test_none_from_metatrader_is_an_error_not_no_deals(self):
        a = self._adapter(None, [])
        with pytest.raises(BrokerError):
            a.deals_since(NOW, 0, False)

    def test_deals_are_filtered_by_converted_time(self):
        import calendar
        since = NOW.replace(hour=12, minute=59)
        def deal(t_utc, entry, pos, profit):
            server = t_utc + dt.timedelta(hours=3)
            return SimpleNamespace(time=calendar.timegm(server.timetuple()), entry=entry,
                                   type=1, magic=990311, position_id=pos, symbol="GBPCAD",
                                   volume=0.18, profit=profit, commission=-0.5, swap=0.0)
        deals = (deal(NOW.replace(hour=9), 0, 1, 0.0), deal(NOW.replace(hour=9, minute=30), 1, 1, -5.0),
                 deal(NOW.replace(hour=16, minute=44), 0, 2, 0.0), deal(NOW.replace(hour=17), 1, 2, -4.61))
        a = self._adapter(deals, [])
        rows = a.deals_since(since, 990311, False)
        assert [r["position"] for r in rows] == [2, 2]
        assert rows[1]["profit"] == pytest.approx(-5.11)
        assert rows[0]["time"] == NOW.replace(hour=16, minute=44)


class TestTheLedgerIsHonest:
    def _trader(self, workdir, fn):
        from mintel.engine.journal import Journal
        j = Journal(workdir / "j.sqlite")
        broker = SimpleNamespace(deals_since=fn)
        cfg = Config()
        cfg.tracking_start_utc = "2026-09-15T12:59:00+00:00"
        return SimpleNamespace(broker=broker, cfg=cfg, journal=j), j

    def test_broker_error_is_reported_not_zeroed(self, workdir):
        from mintel.run import broker_ledger, _LEDGER_CACHE
        _LEDGER_CACHE["at"] = None
        def boom(*a, **k): raise BrokerError("unknown method 'deals_since'")
        t, j = self._trader(workdir, boom)
        try:
            j._exec("INSERT INTO trades (ticket, symbol, opened_utc, closed_utc, pnl_money) VALUES (?,?,?,?,?)",
                    (1, "GBPCAD", "2026-09-15T16:44:00+00:00", "2026-09-15T17:00:00+00:00", -4.61))
            j._exec("INSERT INTO trades (ticket, symbol, opened_utc, closed_utc, pnl_money) VALUES (?,?,?,?,?)",
                    (2, "USDCHF", "2026-09-15T09:00:00+00:00", "2026-09-15T09:30:00+00:00", -9.0))
            led = broker_ledger(t, NOW)
        finally:
            j.close()
        assert led["source"] == "journal" and "deals_since" in led["error"]
        assert led["since_start"]["trades"] == 1
        assert led["since_start"]["net"] == pytest.approx(-4.61)
        assert led["today"]["net"] == pytest.approx(-4.61)

    def test_page_explains_the_fallback(self):
        from mintel.ops.dashboard import render_status
        snap = {"status": {"pnl_source": "journal", "pnl_error": "unknown method 'deals_since'",
                           "since_start_pnl": -4.61, "since_start_trades": 1},
                "health": {}, "thinking": [], "results": {}, "positions": [], "events": []}
        page = render_status(snap)
        assert "trade history could not be read" in page
        assert "deals_since" in page
        good = dict(snap); good["status"] = {"pnl_source": "broker"}
        assert "broker&#x27;s own deal records" in render_status(good) or "broker's own deal records" in render_status(good)


class TestHealthFlagsAnOldBridge:
    def test_outdated_bridge_is_critical(self, workdir):
        from mintel.ops.health import HealthSupervisor
        from mintel.broker.sim import SimBroker
        sim = SimBroker(("EURUSD",), start=NOW)
        sim.connect()
        sim.outdated = lambda: "the MetaTrader bridge is running old code (abc vs installed def)"
        cfg = Config(); cfg.ops.data_dir = str(workdir)
        hs = HealthSupervisor(cfg, sim, clock=lambda: NOW)
        report = hs.run_checks()
        names = {c.name: c for c in report.checks}
        assert "BRIDGE_VERSION" in names and names["BRIDGE_VERSION"].critical
        assert not report.entries_allowed


class TestThePageNamesItsBuild:
    def test_running_stamp_is_stable_and_shown(self):
        from mintel.version import running_stamp, RUNNING_STAMP
        assert running_stamp() == RUNNING_STAMP and len(RUNNING_STAMP) == 10
        from mintel.ops.dashboard import render_status
        snap = {"status": {"build": RUNNING_STAMP, "started": "2026-09-16 00:30 UTC"},
                "health": {}, "thinking": [], "results": {}, "positions": [], "events": []}
        page = render_status(snap)
        assert RUNNING_STAMP in page and "running since 2026-09-16 00:30 UTC" in page

    def test_installer_prints_the_same_stamp(self):
        from pathlib import Path
        body = (Path(__file__).parent.parent / "deploy" / "mac" / "mintel_mac.sh").read_text()
        assert "from mintel.version import running_stamp" in body
        assert "Build stamp:" in body


class TestTodayIsTheBrokersDay:
    def test_day_start_follows_the_server_clock(self):
        from mintel.clock import ServerClock
        c = ServerClock(offset_seconds=3 * 3600)           # broker is UTC+3
        at = dt.datetime(2026, 9, 16, 7, 46, tzinfo=dt.timezone.utc)
        # 07:46 UTC is 10:46 server; the server day began 00:00 server = 21:00 UTC yesterday
        assert c.day_start_utc(at) == dt.datetime(2026, 9, 15, 21, 0, tzinfo=dt.timezone.utc)
        assert ServerClock(0).day_start_utc(at) == at.replace(hour=0, minute=0)

    def test_ledger_today_includes_the_late_evening_win(self, workdir):
        from mintel.run import broker_ledger, _LEDGER_CACHE
        from mintel.clock import ServerClock
        _LEDGER_CACHE["at"] = None
        us30_open = dt.datetime(2026, 9, 15, 20, 46, tzinfo=dt.timezone.utc)
        rows = [{"position": 1, "symbol": "US30", "volume": 0.2, "profit": 0.0,
                 "commission": -0.4, "is_entry": True, "time": us30_open},
                {"position": 1, "symbol": "US30", "volume": 0.2, "profit": 24.59,
                 "commission": -0.4, "is_entry": False, "time": us30_open + dt.timedelta(minutes=74)}]
        broker = SimpleNamespace(deals_since=lambda *a, **k: rows, clock=ServerClock(3 * 3600))
        cfg = Config(); cfg.tracking_start_utc = "2026-09-15T12:59:00+00:00"
        t = SimpleNamespace(broker=broker, cfg=cfg, journal=None)
        led = broker_ledger(t, dt.datetime(2026, 9, 16, 7, 46, tzinfo=dt.timezone.utc))
        # opened 23:46 server, closed 01:00 server: MetaTrader's Today has it
        assert led["today"]["trades"] == 1 and led["today"]["net"] == pytest.approx(24.59)
        labels = [p["label"] for p in led["periods"]]
        assert labels == ["Today", "Yesterday", "This week", "Last 7 days", "Last 14 days", "Since start"]
        assert led["periods"][1]["trades"] == 0            # yesterday (broker's day)
        assert led["periods"][-1]["trades"] == 1
        _LEDGER_CACHE["at"] = None
        broker.clock = ServerClock(0)                         # midnight UTC: closed 22:00 = yesterday
        led = broker_ledger(t, dt.datetime(2026, 9, 16, 7, 46, tzinfo=dt.timezone.utc))
        assert led["today"]["trades"] == 0 and led["periods"][1]["trades"] == 1

    def test_periods_are_rendered(self):
        from mintel.ops.dashboard import render_status
        snap = {"status": {"currency": "GBP", "periods": [
                    {"label": "Today", "net": -7.6, "trades": 8, "win_rate": 37.5},
                    {"label": "Since start", "net": 2.91, "trades": 15, "win_rate": 40.0}]},
                "health": {}, "thinking": [], "results": {}, "positions": [], "events": []}
        page = render_status(snap)
        assert "Results by period" in page and "Yesterday" not in page
        assert "-£7.60" in page or "-GBP7.60" in page or "7.60" in page
        assert "38%" in page
