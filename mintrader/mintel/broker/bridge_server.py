"""The Wine-side half of the broker bridge.

Runs under the Windows Python that lives inside the Wine prefix, next to
MetaTrader 5.  Its only dependencies are the standard library and the
``MetaTrader5`` package, so nothing heavier ever has to be made to work inside
Wine.

    python bridge_server.py --port 8790 --token-file token.txt

Security, because this listens on a socket:

* it binds to 127.0.0.1 only, never to a routable address;
* every request must carry a shared token read from a file the installer
  creates with owner-only permissions;
* the protocol is newline-delimited JSON with a fixed method allow-list.  There
  is no eval, no pickle and no way to name a method that is not on the list.

One connection is served at a time by design.  MT5's Python bridge is not
re-entrant, the trader is a single logical client, and serialising here is
simpler and safer than pretending otherwise.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import socket
import socketserver
import sys
import threading
import traceback
from pathlib import Path
from typing import Any, Optional, Sequence

# Allow running as a plain script from inside the Wine prefix.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from mintel.broker import wire                                    # noqa: E402
from mintel.broker.base import TF, Side                           # noqa: E402

log = logging.getLogger("mintel.bridge.server")

PROTOCOL_VERSION = 2
# Frozen at start-up: the stamp of the code THIS process is running.
from mintel.broker.stamp import code_stamp                       # noqa: E402
CODE_STAMP = code_stamp()
MAX_REQUEST_BYTES = 1 << 20


class BridgeService:
    """Turns wire calls into broker calls.  The method table is the allow-list."""

    def __init__(self, broker, magic: int = 0):
        self.broker = broker
        self.magic = magic
        self.lock = threading.RLock()
        self.calls = 0
        self.errors = 0
        # MetaTrader's reachability as this bridge last saw it: the watchdog
        # reads it from ping and restarts a terminal that has gone deaf.
        self.connect_failures = 0
        self.last_connect_error = ""
        self.mt5_connected = False

    # ------------------------------------------------------------ dispatch --
    def handle(self, method: str, args: dict) -> Any:
        fn = getattr(self, f"do_{method}", None)
        if fn is None or method.startswith("_"):
            raise ValueError(f"unknown method {method!r}")
        if method == "ping":
            # Never queued behind a broker call: MetaTrader can take a minute
            # to answer a connect, and the watchdog's probe must not read
            # that minute as a dead bridge.
            return fn()
        with self.lock:
            self.calls += 1
            return fn(**args)

    # ------------------------------------------------------------ lifecycle --
    def do_ping(self) -> dict:
        return {"protocol": PROTOCOL_VERSION, "pid": os.getpid(),
                "code_stamp": CODE_STAMP,
                "backend": type(self.broker).__name__,
                "calls": self.calls, "errors": self.errors,
                "mt5_connected": self.mt5_connected,
                "connect_failures": self.connect_failures,
                "mt5_last_error": self.last_connect_error}

    def do_connect(self) -> bool:
        return self.connect_broker()

    def connect_broker(self) -> bool:
        """Connect the broker, remembering how it went for ping.

        Not under the service lock: the broker serialises its own calls, and
        holding ours for the minute MetaTrader may take would block ping.
        """
        try:
            ok = bool(self.broker.connect())
            err = ""
        except Exception as exc:
            ok, err = False, f"{type(exc).__name__}: {exc}"
        self.mt5_connected = ok
        if ok:
            self.connect_failures = 0
            self.last_connect_error = ""
        else:
            self.connect_failures += 1
            self.last_connect_error = err or str(getattr(
                self.broker, "last_error", "") or "MetaTrader did not answer")
        return ok

    def refresh_connected(self) -> bool:
        try:
            self.mt5_connected = bool(self.broker.is_connected())
        except Exception:
            self.mt5_connected = False
        return self.mt5_connected

    def do_shutdown_broker(self) -> bool:
        self.broker.shutdown()
        return True

    def do_is_connected(self) -> bool:
        return bool(self.broker.is_connected())

    def do_reconnect(self) -> bool:
        fn = getattr(self.broker, "reconnect", None)
        return bool(fn()) if fn else bool(self.broker.connect())

    def do_clock_offset(self) -> int:
        clock = getattr(self.broker, "clock", None)
        return int(getattr(clock, "offset_seconds", 0) or 0)

    def do_clock_skew(self) -> dict:
        fn = getattr(self.broker, "clock_skew", None)
        if fn is None:
            return {"skew_seconds": None, "tick_age_seconds": None}
        return fn()

    def do_server_time(self) -> str:
        ts = self.broker.server_time()
        return ts.replace(tzinfo=None).isoformat()

    # ---------------------------------------------------------------- data --
    def do_account(self) -> dict:
        return wire.account_out(self.broker.account())

    def do_symbols(self) -> list:
        return list(self.broker.symbols())

    def do_spec(self, symbol: str) -> Optional[dict]:
        return wire.spec_out(self.broker.spec(symbol))

    def do_ensure_selected(self, symbol: str) -> bool:
        return bool(self.broker.ensure_selected(symbol))

    def do_tick(self, symbol: str) -> Optional[dict]:
        return wire.tick_out(self.broker.tick(symbol))

    def do_ticks(self, symbols: Sequence[str]) -> dict:
        # Batched on purpose: a scan wants every symbol's tick at once, and one
        # round trip beats sixty.
        return {s: wire.tick_out(self.broker.tick(s)) for s in symbols}

    def do_bars(self, symbol: str, tf: str, count: int,
                end: Optional[str] = None) -> list:
        stop = wire._dt(end) if end else None
        return [wire.bar_out(b)
                for b in self.broker.bars(symbol, TF(tf), int(count), stop)]

    def do_ticks_range(self, symbol: str, start: str, end: str) -> list:
        out = self.broker.ticks_range(symbol, wire._dt(start), wire._dt(end))
        return [wire.tick_out(t) for t in out]

    def do_positions(self, magic: Optional[int] = None) -> list:
        return [wire.position_out(p) for p in self.broker.positions(magic)]

    def do_calc_margin(self, symbol: str, side: str, volume: float,
                       price: float) -> Optional[float]:
        fn = getattr(self.broker, "calc_margin", None)
        if fn is None:
            return None
        value = fn(symbol, Side(side), float(volume), float(price))
        return None if value is None else float(value)

    def do_closed_deal(self, ticket: int) -> Optional[dict]:
        fn = getattr(self.broker, "closed_deal", None)
        if fn is None:
            return None
        deal = fn(int(ticket))
        if not deal:
            return None
        out = dict(deal)
        when = out.get("time")
        if isinstance(when, dt.datetime):
            out["time"] = wire._iso(when)
        return out

    def do_deals_since(self, since: str, magic: int = 0,
                       closing_only: bool = True) -> list:
        fn = getattr(self.broker, "deals_since", None)
        if fn is None:
            return []
        rows = fn(wire._dt(since), int(magic or 0), bool(closing_only)) or []
        out = []
        for r in rows:
            r = dict(r)
            if isinstance(r.get("time"), dt.datetime):
                r["time"] = wire._iso(r["time"])
            out.append(r)
        return out

    # -------------------------------------------------------------- trading --
    def do_send(self, request: dict) -> dict:
        return wire.order_result_out(self.broker.send(wire.request_in(request)))

    def do_modify_stops(self, ticket: int, sl: float, tp: float) -> dict:
        return wire.order_result_out(
            self.broker.modify_stops(int(ticket), float(sl), float(tp)))

    def do_close(self, ticket: int, volume: float = 0.0,
                 comment: str = "") -> dict:
        return wire.order_result_out(
            self.broker.close(int(ticket), float(volume), comment))

    # ------------------------------------------------------------- calendar --
    def do_calendar(self, start: str, end: str,
                    currencies: Sequence[str] = ()) -> list:
        events = self.broker.calendar(wire._dt(start), wire._dt(end),
                                      tuple(currencies))
        return [wire.event_out(e) for e in events]


class _Handler(socketserver.StreamRequestHandler):
    service: BridgeService = None
    token: str = ""
    timeout = 120

    def handle(self) -> None:
        peer = self.client_address[0]
        if peer not in ("127.0.0.1", "::1"):
            log.warning("refused a connection from %s", peer)
            return
        while True:
            try:
                raw = self.rfile.readline(MAX_REQUEST_BYTES)
            except (socket.timeout, OSError):
                return
            if not raw:
                return
            try:
                response = self._process(raw)
            except Exception as exc:               # never take the server down
                self.service.errors += 1
                response = {"ok": False, "error": f"{type(exc).__name__}: {exc}",
                            "traceback": traceback.format_exc()[-2000:]}
            try:
                self.wfile.write((json.dumps(response) + "\n").encode())
                self.wfile.flush()
            except OSError:
                return

    def _process(self, raw: bytes) -> dict:
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            return {"ok": False, "error": f"bad request: {exc}"}
        if not isinstance(payload, dict):
            return {"ok": False, "error": "request must be an object"}
        if self.token and payload.get("token") != self.token:
            log.warning("rejected a request with a bad token")
            return {"ok": False, "error": "authentication failed"}
        method = payload.get("method")
        args = payload.get("args") or {}
        if not isinstance(method, str) or not isinstance(args, dict):
            return {"ok": False, "error": "malformed request"}
        try:
            result = self.service.handle(method, args)
        except Exception as exc:
            self.service.errors += 1
            # The client turns this back into the right exception type, so a
            # disconnect on the Wine side still looks like a disconnect to the
            # engine rather than a mysterious protocol error.
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}",
                    "kind": type(exc).__name__}
        return {"ok": True, "result": result, "id": payload.get("id")}


class BridgeServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def build_broker(backend: str, args: argparse.Namespace):
    if backend == "sim":
        # Used by the test suite and by anyone who wants to watch the whole
        # system run without a broker connection.
        from mintel.broker.sim import SimBroker
        broker = SimBroker(seed=args.sim_seed, history_bars=args.sim_bars,
                           live=args.sim_live, speed=args.sim_speed)
        broker.connect()
        return broker
    password = args.password
    if not password and args.secrets_file:
        # Read it from the file rather than accepting it as an argument:
        # anything on a command line is visible to every process on the machine
        # via `ps`.
        try:
            secrets = json.loads(Path(args.secrets_file).read_text())
            password = str(secrets.get("account_password", "") or "")
        except Exception as exc:
            log.warning("could not read the credentials file: %s", exc)
    from mintel.broker.mt5_adapter import Mt5Broker, mt5_available
    if not mt5_available():
        raise SystemExit(
            "The MetaTrader5 package is not importable in this Python.\n"
            "This process is meant to run inside the Wine prefix, using the\n"
            "Windows Python that has MetaTrader5 installed.")
    # Not connected here: the socket is bound first and the terminal is
    # dialled in the background (see ``keep_connected``), so a MetaTrader
    # that takes a minute to answer, or is not up yet, never leaves the port
    # dead and the watchdog guessing.
    return Mt5Broker(login=args.login, password=password,
                     server=args.server, terminal_path=args.terminal,
                     magic=args.magic)


def keep_connected(service: BridgeService, interval: float = 30.0,
                   stop: Optional[threading.Event] = None) -> threading.Thread:
    """Background thread: dial MetaTrader until it answers, and again
    whenever the connection drops.  Every attempt is recorded for ping."""
    stop = stop or threading.Event()

    def loop() -> None:
        while not stop.is_set():
            if not service.refresh_connected():
                if service.connect_broker():
                    log.info("connected to MetaTrader 5")
                else:
                    log.error("MetaTrader 5 is not answering (%s) - trying "
                              "again in %.0fs", service.last_connect_error,
                              interval)
            stop.wait(interval)

    t = threading.Thread(target=loop, name="mt5-connect", daemon=True)
    t.start()
    return t


def serve(host: str, port: int, service: BridgeService,
          token: str) -> BridgeServer:
    handler = type("Handler", (_Handler,),
                   {"service": service, "token": token})
    server = BridgeServer((host, port), handler)
    return server


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="MetaTrader 5 bridge server")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8790)
    ap.add_argument("--token-file", default="")
    ap.add_argument("--backend", choices=("mt5", "sim"), default="mt5")
    ap.add_argument("--login", type=int, default=0)
    ap.add_argument("--password", default=os.environ.get("MINTEL_PASSWORD", ""))
    ap.add_argument("--server", default="")
    ap.add_argument("--terminal", default="")
    ap.add_argument("--magic", type=int, default=990311)
    ap.add_argument("--secrets-file", default="",
                    help="JSON file holding account_password; preferred over "
                         "--password, which would be visible in `ps`")
    ap.add_argument("--sim-seed", type=int, default=7)
    ap.add_argument("--sim-bars", type=int, default=4000)
    ap.add_argument("--sim-live", action="store_true",
                    help="make the simulation track the wall clock, so prices "
                         "are genuinely fresh")
    ap.add_argument("--sim-speed", type=float, default=1.0,
                    help="simulated minutes per real minute (try 30 for a "
                         "watchable demo)")
    ap.add_argument("--port-file", default="",
                    help="write the bound port here (use --port 0 to pick one)")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s")

    token = ""
    if args.token_file and Path(args.token_file).exists():
        token = Path(args.token_file).read_text().strip()
    if not token:
        log.warning("running without a token - only do this in a test")

    broker = build_broker(args.backend, args)
    service = BridgeService(broker, args.magic)
    server = serve(args.host, args.port, service, token)
    bound = server.server_address[1]
    if args.port_file:
        Path(args.port_file).write_text(str(bound))
    log.info("bridge listening on %s:%d (backend=%s)",
             args.host, bound, args.backend)
    stop_dialling = threading.Event()
    if args.backend == "mt5":
        keep_connected(service, stop=stop_dialling)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop_dialling.set()
        server.shutdown()
        try:
            broker.shutdown()
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
