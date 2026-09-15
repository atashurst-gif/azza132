"""The broker bridge that lets the engine run natively on macOS.

MetaTrader 5's Python package is Windows-only. On a Mac, MT5 and a small
Windows Python run inside a Wine prefix; everything else runs natively. These
tests exercise that split for real - two processes, a genuine socket - using
the simulator as the far-side backend so the whole thing is deterministic.
"""
from __future__ import annotations

import datetime as dt
import json
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from mintel.broker import wire
from mintel.broker.base import (Bar, BrokerError, NotConnected, OrderRequest,
                                Position, Side, TF, Tick)
from mintel.broker.bridge_client import BridgeBroker, from_url
from mintel.broker.bridge_server import BridgeService, serve
from mintel.broker.sim import SimBroker
from mintel.config import Config
from mintel.engine.trader import Trader

UTC = dt.timezone.utc
MID_LONDON = dt.datetime(2026, 3, 10, 13, 30, tzinfo=UTC)


@pytest.fixture
def backend() -> SimBroker:
    s = SimBroker(seed=101, start=MID_LONDON, history_bars=6000)
    s.connect()
    return s


@pytest.fixture
def bridge(backend):
    """A real server on a real socket, in a thread, with a token."""
    service = BridgeService(backend, magic=990_311)
    server = serve("127.0.0.1", 0, service, token="test-token")
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    client = BridgeBroker("127.0.0.1", port, token="test-token", timeout=15.0)
    yield client, server, service, backend
    try:
        client.shutdown()
    except Exception:
        pass
    server.shutdown()
    server.server_close()


class TestWireFormat:
    def test_every_domain_type_survives_a_round_trip(self, backend):
        tick = backend.tick("EURUSD")
        assert wire.tick_in(json.loads(json.dumps(wire.tick_out(tick)))) == tick

        bar = backend.bars("EURUSD", TF.M5, 3)[0]
        assert wire.bar_in(json.loads(json.dumps(wire.bar_out(bar)))) == bar

        spec = backend.spec("EURUSD")
        assert wire.spec_in(json.loads(json.dumps(wire.spec_out(spec)))) == spec

        account = backend.account()
        assert wire.account_in(
            json.loads(json.dumps(wire.account_out(account)))) == account

    def test_timestamps_stay_utc_across_the_wire(self, backend):
        tick = backend.tick("EURUSD")
        back = wire.tick_in(wire.tick_out(tick))
        assert back.time == tick.time
        assert back.time.tzinfo is not None

    def test_position_and_order_round_trip(self):
        p = Position(7, "EURUSD", Side.SELL, 0.25, 1.1, 1.102, 1.095,
                     MID_LONDON, profit=-3.5, comment="k", magic=990_311)
        assert wire.position_in(wire.position_out(p)) == p
        req = OrderRequest("EURUSD", Side.BUY, 0.1, sl=1.09, tp=1.11,
                           comment="abc", idempotency_key="abc")
        back = wire.request_in(wire.request_out(req))
        assert back.symbol == req.symbol and back.side is req.side
        assert back.idempotency_key == req.idempotency_key

    def test_none_values_survive(self):
        assert wire.tick_in(wire.tick_out(None)) is None
        assert wire.spec_in(wire.spec_out(None)) is None


class TestBridgeTransport:
    def test_ping_reports_the_protocol(self, bridge):
        client, *_ = bridge
        info = client.ping()
        assert info["protocol"] >= 2
        assert len(info["code_stamp"]) == 12
        assert info["backend"] == "SimBroker"

    def test_connect_and_basic_reads(self, bridge):
        client, *_ = bridge
        assert client.connect()
        assert client.is_connected()
        account = client.account()
        assert account.currency == "GBP"
        assert len(client.symbols()) > 5
        assert client.spec("EURUSD").digits == 5
        assert client.tick("EURUSD") is not None

    def test_bars_match_the_backend_exactly(self, bridge):
        client, _server, _service, backend = bridge
        client.connect()
        for tf in (TF.M1, TF.M5, TF.M15, TF.H1):
            over_wire = client.bars("EURUSD", tf, 50)
            direct = backend.bars("EURUSD", tf, 50)
            assert over_wire == direct, tf

    def test_batched_ticks(self, bridge):
        client, *_ = bridge
        client.connect()
        ticks = client.ticks(["EURUSD", "GBPUSD", "XAUUSD"])
        assert set(ticks) == {"EURUSD", "GBPUSD", "XAUUSD"}
        assert all(isinstance(t, Tick) for t in ticks.values())

    def test_specs_are_cached(self, bridge):
        client, _server, service, _backend = bridge
        client.connect()
        client.spec("EURUSD")
        calls = service.calls
        for _ in range(20):
            client.spec("EURUSD")
        assert service.calls == calls, "repeated spec reads must not hit the wire"

    def test_clock_offset_crosses_the_bridge(self, bridge):
        client, _server, _service, backend = bridge
        client.connect()
        assert client.clock.offset_seconds == backend.server_offset_seconds


class TestBridgeSecurity:
    def test_a_bad_token_is_refused(self, bridge):
        client, server, _service, _backend = bridge
        port = server.server_address[1]
        impostor = BridgeBroker("127.0.0.1", port, token="wrong", timeout=10)
        with pytest.raises(BrokerError) as exc:
            impostor.account()
        assert "authentication" in str(exc.value)

    def test_a_missing_token_is_refused(self, bridge):
        client, server, _service, _backend = bridge
        port = server.server_address[1]
        anonymous = BridgeBroker("127.0.0.1", port, token="", timeout=10)
        with pytest.raises(BrokerError):
            anonymous.account()

    def test_unknown_methods_are_refused(self, bridge):
        client, *_ = bridge
        client.connect()
        with pytest.raises(BrokerError) as exc:
            client._rpc("os_system", cmd="rm -rf /")
        assert "unknown method" in str(exc.value)

    def test_private_attributes_are_not_reachable(self, bridge):
        client, *_ = bridge
        client.connect()
        for attempt in ("_rpc", "__init__", "handle", "broker"):
            with pytest.raises(BrokerError):
                client._rpc(attempt)

    def test_the_server_only_accepts_localhost(self, bridge):
        _client, server, *_ = bridge
        assert server.server_address[0] == "127.0.0.1"


class TestBridgeFailure:
    def test_a_dead_bridge_looks_like_a_disconnected_broker(self):
        """So the health supervisor's existing handling just works."""
        dead = BridgeBroker("127.0.0.1", _free_port(), token="x", timeout=2)
        with pytest.raises(NotConnected):
            dead.account()
        assert dead.is_connected() is False
        assert dead.connect() is False

    def test_a_restarted_bridge_is_picked_up_transparently(self, bridge):
        client, server, service, backend = bridge
        client.connect()
        assert client.account() is not None
        # Drop the socket underneath the client, as a bridge restart would.
        client._close_socket()
        assert client.account() is not None, "one transparent retry is expected"

    def test_broker_errors_cross_the_wire_intact(self, bridge):
        client, _server, _service, backend = bridge
        client.connect()
        backend.fault.calendar_timeout = True
        with pytest.raises(BrokerError) as exc:
            client.calendar(MID_LONDON - dt.timedelta(days=1),
                            MID_LONDON + dt.timedelta(days=1))
        assert "timeout" in str(exc.value)

    def test_a_disconnected_backend_reads_as_not_connected(self, bridge):
        client, _server, _service, backend = bridge
        client.connect()
        backend.fault.disconnect = True
        assert client.is_connected() is False
        with pytest.raises(NotConnected):
            client.account()

    def test_a_send_is_never_retried_after_a_lost_reply(self, bridge):
        """Re-sending an order whose reply was lost is how duplicates happen."""
        client, _server, _service, backend = bridge
        client.connect()
        import inspect
        src = inspect.getsource(BridgeBroker.send)
        assert "retry=False" in src
        assert "duplicate" in src.lower()

    def test_reconnect_restores_service(self, bridge):
        client, _server, _service, backend = bridge
        client.connect()
        backend.fault.disconnect = True
        assert client.is_connected() is False
        backend.fault.disconnect = False
        backend.connect()
        assert client.reconnect(attempts=2, base_delay=0.01)
        assert client.is_connected()


class TestTradingOverTheBridge:
    def test_orders_stops_and_closes_all_work(self, bridge):
        client, _server, _service, backend = bridge
        client.connect()
        spec = client.spec("EURUSD")
        tick = client.tick("EURUSD")
        stop = spec.normalise_price(tick.ask - spec.pips_to_price(30))
        result = client.send(OrderRequest("EURUSD", Side.BUY, 0.10, sl=stop,
                                          magic=990_311,
                                          idempotency_key="bridge-1",
                                          comment="bridge-1"))
        assert result.ok and result.ticket
        positions = client.positions(990_311)
        assert len(positions) == 1
        assert positions[0].sl > 0

        tighter = spec.normalise_price(positions[0].sl + spec.pips_to_price(5))
        assert client.modify_stops(positions[0].ticket, tighter, 0.0).ok
        assert client.positions(990_311)[0].sl == pytest.approx(tighter)

        assert client.close(positions[0].ticket, comment="done").ok
        assert client.positions(990_311) == []
        deal = client.closed_deal(result.ticket)
        assert deal and "pnl" in deal
        assert isinstance(deal["time"], dt.datetime)

    def test_margin_calculation_crosses_the_bridge(self, bridge):
        client, _server, _service, backend = bridge
        client.connect()
        over_wire = client.calc_margin("EURUSD", Side.BUY, 1.0, 1.10)
        direct = backend.calc_margin("EURUSD", Side.BUY, 1.0, 1.10)
        assert over_wire == pytest.approx(direct)

    def test_rejections_cross_the_bridge(self, bridge):
        from mintel.broker.base import RetCode
        client, _server, _service, backend = bridge
        client.connect()
        backend.fault.reject_all = RetCode.NO_MONEY
        result = client.send(OrderRequest("EURUSD", Side.BUY, 0.1, sl=1.0))
        assert not result.ok
        assert result.code is RetCode.NO_MONEY


class TestFullEngineOverTheBridge:
    """The real proof: the whole trader, unmodified, driven through a socket."""

    def test_the_trader_runs_end_to_end_over_the_bridge(self, bridge, tmp_path):
        client, _server, _service, backend = bridge
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path / "data")
        cfg.ops.log_dir = str(tmp_path / "logs")
        cfg.news.store_path = "calendar.sqlite"
        cfg.universe.max_symbols = 6
        cfg.account_login = backend.login
        cfg.account_server = backend.server

        client.connect()
        trader = Trader(client, cfg, clock=lambda: backend.now,
                        enable_model=False)
        info = trader.bootstrap()
        assert len(info["universe"]) >= 4, (
            "universe discovery must work through the bridge")

        acted = 0
        for _ in range(40):
            result = trader.cycle()
            if result.acted:
                acted += 1
            backend.advance(3)

        assert trader.top_opportunities, "it must be ranking markets"
        assert not trader.health.safe_mode, (
            "a healthy bridge must not look like a broken broker")
        # Whatever it did, every position it holds is protected at the broker.
        for p in backend.positions():
            assert p.sl > 0
        trader.journal.close()

    def test_safe_mode_when_the_bridge_dies_mid_run(self, bridge, tmp_path):
        client, server, _service, backend = bridge
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path / "data2")
        cfg.ops.log_dir = str(tmp_path / "logs2")
        cfg.news.store_path = "calendar.sqlite"
        cfg.universe.max_symbols = 4
        cfg.account_login = backend.login
        cfg.account_server = backend.server
        client.connect()
        trader = Trader(client, cfg, clock=lambda: backend.now,
                        enable_model=False)
        trader.bootstrap()
        trader.cycle()
        assert not trader.health.safe_mode

        # Simulate the Wine-side process dying: the listener goes away AND the
        # established connection is severed.  Stopping serve_forever alone
        # would leave the existing socket happily answering, which is not what
        # a crashed process looks like.
        server.shutdown()
        server.server_close()
        client._close_socket()

        result = trader.cycle()
        assert result.safe_mode, "a dead bridge must trigger SAFE MODE"
        assert result.acted is None, "and nothing may be traded"
        trader.journal.close()


class TestBridgeSubprocess:
    """Prove the server runs as its own process, exactly as it will in Wine."""

    def test_server_process_serves_a_real_client(self, tmp_path):
        token_file = tmp_path / "token.txt"
        token_file.write_text("sub-token")
        port_file = tmp_path / "port.txt"
        root = Path(__file__).resolve().parents[1]
        proc = subprocess.Popen(
            [sys.executable, "-m", "mintel.broker.bridge_server",
             "--backend", "sim", "--port", "0",
             "--port-file", str(port_file),
             "--token-file", str(token_file),
             "--sim-bars", "800"],
            cwd=str(root), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.time() + 30
            while time.time() < deadline and not port_file.exists():
                time.sleep(0.1)
            assert port_file.exists(), "the server never bound a port"
            port = int(port_file.read_text().strip())
            client = BridgeBroker("127.0.0.1", port, token="sub-token",
                                  timeout=20)
            deadline = time.time() + 20
            while time.time() < deadline:
                if client.ping():
                    break
                time.sleep(0.2)
            assert client.connect()
            assert client.account().login > 0
            assert len(client.bars("EURUSD", TF.M5, 20)) == 20
            client.shutdown()
        finally:
            proc.terminate()
            proc.wait(timeout=20)

    def test_from_url_parses_a_bridge_address(self):
        client = from_url("bridge://127.0.0.1:9123")
        assert client.host == "127.0.0.1" and client.port == 9123


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestLiveSimulation:
    """A wall-clock-tracking simulator: the basis of the demo and self-test."""

    def test_ticks_are_genuinely_fresh(self):
        from mintel.clock import utcnow
        sim = SimBroker(live=True, speed=30.0, history_bars=300)
        sim.connect()
        age = (utcnow() - sim.tick("EURUSD").time).total_seconds()
        assert abs(age) < 2.0, "a live feed must not look stale"

    def test_bars_accumulate_with_the_wall_clock(self):
        sim = SimBroker(live=True, speed=120.0, history_bars=300)
        sim.connect()
        before = len(sim.bars("EURUSD", TF.M1, 10_000))
        time.sleep(2.0)
        after = len(sim.bars("EURUSD", TF.M1, 10_000))
        assert after > before, "time must actually pass in live mode"

    def test_a_frozen_simulation_is_unchanged(self):
        sim = SimBroker(history_bars=300)
        sim.connect()
        before = len(sim.bars("EURUSD", TF.M1, 10_000))
        time.sleep(1.0)
        assert len(sim.bars("EURUSD", TF.M1, 10_000)) == before, (
            "the deterministic fixture must stay deterministic")

    def test_stale_tick_injection_still_works_in_live_mode(self):
        from mintel.clock import utcnow
        sim = SimBroker(live=True, speed=60.0, history_bars=300)
        sim.connect()
        sim.fault.stale_ticks = True
        first = sim.tick("EURUSD").time
        time.sleep(1.5)
        assert sim.tick("EURUSD").time == first, (
            "fault injection must still be able to freeze the feed")


class TestCredentialHandling:
    def test_the_password_never_reaches_a_command_line(self, tmp_path):
        """Anything in argv is readable by every process via `ps`."""
        from mintel.config import Config
        from mintel.ops.watchdog import build_default
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path)
        cfg.broker_mode = "bridge"
        cfg.wine_python = "C:\\Python311\\python.exe"
        cfg.account_login = 12345
        cfg.account_server = "Broker-Demo"
        cfg.account_password = "hunter2"
        cfg.mt5_terminal_path = "C:\\MT5\\terminal64.exe"
        bridge = next(p for p in build_default(cfg, str(tmp_path / "c.json")).processes
                      if p.name == "bridge")
        argv = " ".join(str(a) for a in bridge.command)
        assert "hunter2" not in argv
        assert "--password" not in argv
        assert "--secrets-file" in argv

    def test_the_bridge_reads_the_password_from_the_secrets_file(self, tmp_path):
        import json
        from mintel.broker import bridge_server
        secrets = tmp_path / "secrets.json"
        secrets.write_text(json.dumps({"account_password": "from-file"}))
        captured = {}

        class FakeMt5:
            def __init__(self, **kw):
                captured.update(kw)

            def connect(self):
                return True

        import mintel.broker.mt5_adapter as adapter
        real_broker, real_available = adapter.Mt5Broker, adapter.mt5_available
        adapter.Mt5Broker, adapter.mt5_available = FakeMt5, lambda: True
        try:
            args = type("A", (), {
                "login": 5, "password": "", "server": "S", "terminal": "T",
                "magic": 1, "secrets_file": str(secrets)})()
            bridge_server.build_broker("mt5", args)
        finally:
            adapter.Mt5Broker, adapter.mt5_available = real_broker, real_available
        assert captured["password"] == "from-file"

    def test_disconnect_leaves_the_far_side_running(self, bridge):
        """Inspecting a live system must not log MetaTrader out."""
        client, _server, _service, backend = bridge
        client.connect()
        assert backend.is_connected()
        client.disconnect()
        assert backend.is_connected(), (
            "disconnect must close only our socket")
        assert client.account() is not None, "and reconnect on the next call"

    def test_shutdown_does_stop_the_far_side(self, bridge):
        client, _server, _service, backend = bridge
        client.connect()
        client.shutdown()
        assert not backend.is_connected()

    def test_verify_only_disconnects(self):
        import inspect
        from mintel import verify
        src = inspect.getsource(verify)
        assert "disconnect()" in src
        assert "log MetaTrader out" in src
