"""The native-side half of the broker bridge.

``BridgeBroker`` implements the same :class:`~mintel.broker.base.Broker`
protocol as the direct MT5 adapter, so the scanner, the risk manager, FlowLock
and the executor cannot tell the difference.  That is the whole point of having
had a broker protocol in the first place.

Design notes that matter in production:

* **A dropped bridge looks like a dropped broker.**  Socket failures are raised
  as :class:`~mintel.broker.base.NotConnected`, which is exactly what the health
  supervisor already knows how to handle: enter SAFE MODE, reconnect, reconcile.
* **Reconnection is automatic and bounded.**  One retry per call, so a brief
  blip is invisible while a real outage surfaces immediately instead of hanging.
* **Specs are cached.**  They change rarely and are read constantly, and every
  avoided round trip is latency the engine does not spend.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import socket
import threading
import time
from pathlib import Path
from typing import Any, Optional, Sequence

from ..clock import ServerClock, UTC
from ..contracts import SymbolSpec
from . import wire
from .base import (AccountInfo, Bar, BrokerError, CalendarEvent, NotConnected,
                   OrderRequest, OrderResult, Position, RetCode, Side, TF, Tick)

log = logging.getLogger("mintel.bridge.client")


class BridgeBroker:
    """Talks to :mod:`mintel.broker.bridge_server` over localhost."""

    def __init__(self, host: str = "127.0.0.1", port: int = 8790,
                 token: str = "", *, token_file: str = "",
                 timeout: float = 30.0, magic: int = 990_311,
                 spec_ttl: float = 300.0):
        self.host = host
        self.port = int(port)
        self.magic = magic
        self.timeout = timeout
        self.token = token
        if not self.token and token_file and Path(token_file).exists():
            self.token = Path(token_file).read_text().strip()
        self._lock = threading.RLock()
        self._sock: Optional[socket.socket] = None
        self._file = None
        self._seq = 0
        self._clock = ServerClock(0)
        self._spec_cache: dict[str, tuple[float, Optional[SymbolSpec]]] = {}
        self._spec_ttl = spec_ttl
        self._connected = False
        self.last_error = ""

    # ------------------------------------------------------------- plumbing --
    def _open(self) -> None:
        self._close_socket()
        sock = socket.create_connection((self.host, self.port),
                                        timeout=self.timeout)
        sock.settimeout(self.timeout)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock = sock
        self._file = sock.makefile("rwb")

    def _close_socket(self) -> None:
        for obj in (self._file, self._sock):
            try:
                if obj is not None:
                    obj.close()
            except Exception:
                pass
        self._file = None
        self._sock = None

    def _rpc(self, method: str, retry: bool = True, **args) -> Any:
        with self._lock:
            if self._sock is None:
                try:
                    self._open()
                except OSError as exc:
                    self._connected = False
                    self.last_error = str(exc)
                    raise NotConnected(
                        f"the MetaTrader bridge is not answering on "
                        f"{self.host}:{self.port} ({exc})") from exc
            self._seq += 1
            payload = {"method": method, "args": args, "id": self._seq}
            if self.token:
                payload["token"] = self.token
            try:
                self._file.write((json.dumps(payload) + "\n").encode())
                self._file.flush()
                line = self._file.readline()
            except (OSError, socket.timeout) as exc:
                self._close_socket()
                if retry:
                    # One transparent retry: a stale socket after the bridge
                    # restarted should not look like a broker outage.
                    return self._rpc(method, retry=False, **args)
                self._connected = False
                self.last_error = str(exc)
                raise NotConnected(
                    f"lost the connection to the MetaTrader bridge: "
                    f"{exc}") from exc
            if not line:
                self._close_socket()
                if retry:
                    return self._rpc(method, retry=False, **args)
                self._connected = False
                raise NotConnected("the MetaTrader bridge closed the "
                                   "connection")
            try:
                response = json.loads(line.decode("utf-8"))
            except Exception as exc:
                raise BrokerError(f"unreadable reply from the bridge: "
                                  f"{exc}") from exc
            if not response.get("ok"):
                error = str(response.get("error", "unknown bridge error"))
                self.last_error = error
                if response.get("kind") in ("NotConnected", "BrokerError") \
                        or "not connected" in error.lower():
                    raise NotConnected(error)
                raise BrokerError(error)
            return response.get("result")

    # ------------------------------------------------------------ lifecycle --
    def connect(self) -> bool:
        try:
            self._rpc("ping")
            ok = bool(self._rpc("connect"))
        except (NotConnected, BrokerError) as exc:
            log.error("bridge connect failed: %s", exc)
            self._connected = False
            return False
        self._connected = ok
        if ok:
            self._sync_clock()
        return ok

    def shutdown(self) -> None:
        """Shut down the BROKER on the far side as well as this connection.

        Only the owner of the bridge should call this.  A tool that merely
        inspects a running system wants :meth:`disconnect` instead, or it will
        log MetaTrader out from underneath a live trader.
        """
        try:
            self._rpc("shutdown_broker", retry=False)
        except Exception:
            pass
        self._close_socket()
        self._connected = False

    def disconnect(self) -> None:
        """Close this connection and leave the far side running."""
        self._close_socket()
        self._connected = False

    def is_connected(self) -> bool:
        try:
            return bool(self._rpc("is_connected"))
        except Exception:
            return False

    def reconnect(self, attempts: int = 1, base_delay: float = 0.0) -> bool:
        """Reconnect both hops: our socket, and MT5 on the far side.

        Single-shot by default, and it never sleeps after the final attempt.
        The health supervisor calls this from inside the trading cycle, so a
        long internal retry loop would stall position management and leave the
        dashboard frozen exactly when the operator most needs to see what is
        happening.  Repetition comes from the cycle; backoff comes from the
        healer's own rate limit.
        """
        for i in range(max(1, attempts)):
            self._close_socket()
            try:
                if bool(self._rpc("reconnect")):
                    self._connected = True
                    self._sync_clock()
                    log.info("bridge and broker reconnected on attempt %d",
                             i + 1)
                    return True
            except Exception as exc:
                log.warning("reconnect attempt %d failed: %s", i + 1, exc)
            if base_delay > 0 and i < attempts - 1:
                time.sleep(min(base_delay * (2 ** i), 30.0))
        return False

    def ping(self) -> Optional[dict]:
        try:
            return self._rpc("ping")
        except Exception:
            return None

    # ---------------------------------------------------------------- clock --
    def _sync_clock(self) -> None:
        try:
            self._clock = ServerClock(int(self._rpc("clock_offset")))
        except Exception:
            self._clock = ServerClock(0)

    @property
    def clock(self) -> ServerClock:
        return self._clock

    def server_time(self) -> dt.datetime:
        try:
            return dt.datetime.fromisoformat(self._rpc("server_time"))
        except Exception:
            return dt.datetime.now(UTC).replace(tzinfo=None)

    # ----------------------------------------------------------------- data --
    def account(self) -> AccountInfo:
        return wire.account_in(self._rpc("account"))

    def symbols(self) -> Sequence[str]:
        return tuple(self._rpc("symbols"))

    def spec(self, symbol: str) -> Optional[SymbolSpec]:
        now = time.time()
        hit = self._spec_cache.get(symbol)
        if hit and now - hit[0] < self._spec_ttl:
            return hit[1]
        spec = wire.spec_in(self._rpc("spec", symbol=symbol))
        self._spec_cache[symbol] = (now, spec)
        return spec

    def ensure_selected(self, symbol: str) -> bool:
        try:
            return bool(self._rpc("ensure_selected", symbol=symbol))
        except Exception:
            return False

    def tick(self, symbol: str) -> Optional[Tick]:
        return wire.tick_in(self._rpc("tick", symbol=symbol))

    def ticks(self, symbols: Sequence[str]) -> dict[str, Optional[Tick]]:
        raw = self._rpc("ticks", symbols=list(symbols))
        return {k: wire.tick_in(v) for k, v in raw.items()}

    def bars(self, symbol: str, tf: TF, count: int,
             end: Optional[dt.datetime] = None) -> list[Bar]:
        rows = self._rpc("bars", symbol=symbol, tf=tf.value, count=int(count),
                         end=wire._iso(end) if end else None)
        return [wire.bar_in(r) for r in rows]

    def ticks_range(self, symbol: str, start: dt.datetime,
                    end: dt.datetime) -> list[Tick]:
        rows = self._rpc("ticks_range", symbol=symbol,
                         start=wire._iso(start), end=wire._iso(end))
        return [wire.tick_in(r) for r in rows]

    def positions(self, magic: Optional[int] = None) -> list[Position]:
        rows = self._rpc("positions", magic=magic)
        return [wire.position_in(r) for r in rows]

    def calc_margin(self, symbol: str, side: Side, volume: float,
                    price: float) -> Optional[float]:
        try:
            return self._rpc("calc_margin", symbol=symbol, side=side.value,
                             volume=float(volume), price=float(price))
        except Exception:
            return None

    def closed_deal(self, ticket: int) -> Optional[dict]:
        try:
            deal = self._rpc("closed_deal", ticket=int(ticket))
        except Exception:
            return None
        if not deal:
            return None
        out = dict(deal)
        if isinstance(out.get("time"), str):
            out["time"] = wire._dt(out["time"])
        return out

    # -------------------------------------------------------------- trading --
    def send(self, req: OrderRequest) -> OrderResult:
        try:
            return wire.order_result_in(
                self._rpc("send", retry=False, request=wire.request_out(req)))
        except NotConnected:
            # Deliberately NOT retried.  A send whose reply was lost may well
            # have reached the broker, and re-sending it is how duplicate
            # positions happen.  Reporting a connection failure lets the
            # executor leave the intent UNKNOWN and reconcile against the
            # broker, which is the only safe way to resolve it.
            raise

    def modify_stops(self, ticket: int, sl: float, tp: float) -> OrderResult:
        return wire.order_result_in(
            self._rpc("modify_stops", ticket=int(ticket), sl=float(sl),
                      tp=float(tp)))

    def close(self, ticket: int, volume: float = 0.0,
              comment: str = "") -> OrderResult:
        return wire.order_result_in(
            self._rpc("close", retry=False, ticket=int(ticket),
                      volume=float(volume), comment=comment))

    # ------------------------------------------------------------- calendar --
    def calendar(self, start: dt.datetime, end: dt.datetime,
                 currencies: Sequence[str] = ()) -> list[CalendarEvent]:
        rows = self._rpc("calendar", start=wire._iso(start),
                         end=wire._iso(end), currencies=list(currencies))
        return [wire.event_in(r) for r in rows]


def from_url(url: str, token_file: str = "", magic: int = 990_311) -> BridgeBroker:
    """Build a client from ``bridge://host:port``."""
    raw = url.split("://", 1)[-1]
    host, _, port = raw.partition(":")
    return BridgeBroker(host=host or "127.0.0.1", port=int(port or 8790),
                        token_file=token_file, magic=magic)
