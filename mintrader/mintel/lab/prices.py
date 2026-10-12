"""The lab's window on the market: prices only, and nothing that could trade.

``PriceFeed`` is the ONE place under ``mintel/lab/`` that reaches the
MetaTrader bridge. It builds a ``BridgeBroker`` inside ``_open()``, keeps only
that broker's bound wire call (no attribute is named after it), forgets the
broker itself, and offers exactly seven questions, each with its method name
written out by hand:

    ping  server_time  symbols  spec  tick  ticks  bars

``ALLOWED`` holds those seven names. Anything else, by any route (a method of
this class, an attribute nobody defined, a name handed to the wire call), is
refused with ``LabMayNotTrade`` BEFORE a byte goes to the bridge. There is no
method here, under any name, that sends, changes or closes an order, and the
safety tests read this file to keep it that way.

Aaron, 10 Oct 2026: "Never invent performance." The lab's practice runs on
real prices with no money: its trades are rows in ``data/lab/lab.sqlite``
marked PRACTICE and nowhere else.
"""
from __future__ import annotations

import datetime as dt
from typing import Callable, Optional, Sequence

from ..broker import wire
from ..broker.base import TF
from ..broker.bridge_client import BridgeBroker

# The seven questions the lab may ask. Nothing else is ever sent.
ALLOWED = frozenset({"ping", "server_time", "symbols", "spec", "tick", "ticks", "bars"})


class LabMayNotTrade(AttributeError):
    """The lab asked for something that is not a price. Raised before anything
    reaches the bridge. (An AttributeError, so a name the feed does not have
    reads as absent to ``getattr(feed, name, default)`` and ``hasattr``.)"""


def _guarded(call: Callable) -> Callable:
    """The wire call wrapped so that it checks ``ALLOWED`` itself: even called
    directly (``feed._wire("send", ...)``), a name that is not a price question is
    refused before anything is sent, and the wrapper has no ``__self__`` that
    leads back to the broker."""
    def ask(method: str, **args):
        if method not in ALLOWED:
            raise LabMayNotTrade(f"the lab may only read prices; {method!r} is not one of "
                                 f"{sorted(ALLOWED)}")
        return call(method, **args)
    return ask


class PriceFeed:
    """Read-only prices through the bridge (or through a stand-in with the same
    wire call, for the tests). ``factory(host=, port=, token_file=, timeout=)``
    builds the broker whose wire call is kept; it defaults to ``BridgeBroker``."""

    __slots__ = ("_host", "_port", "_token_file", "_timeout", "_factory", "_wire", "synthetic",
                 "last_error")

    def __init__(self, host: str = "127.0.0.1", port: int = 8790, token_file: str = "", *,
                 timeout: float = 30.0, factory: Optional[Callable] = None):
        self._host = host
        self._port = int(port)
        self._token_file = token_file
        self._timeout = float(timeout)
        self._factory = factory
        self._wire = None
        self.synthetic = False          # set once ping() names a simulated backend
        self.last_error = ""

    # ------------------------------------------------------------ plumbing --
    def _open(self) -> None:
        """Build the broker, keep ONLY its wire call, drop the broker."""
        make = self._factory or BridgeBroker
        broker = make(host=self._host, port=self._port, token_file=self._token_file,
                      timeout=self._timeout)
        self._wire = _guarded(broker._rpc)   # noqa: SLF001 - the guarded call is all the lab keeps
        del broker

    def _ask(self, method: str, **args):
        """The single door to the bridge. A name outside ``ALLOWED`` is refused
        here, before anything is opened or sent."""
        if method not in ALLOWED:
            raise LabMayNotTrade(f"the lab may only read prices; {method!r} is not one of "
                                 f"{sorted(ALLOWED)}")
        if self._wire is None:
            self._open()
        return self._wire(method, **args)

    def __getattr__(self, name: str):
        # Only reached for names this class does not define. Dunder look-ups (copy,
        # pickle, repr helpers) get the ordinary answer; every other name is refused.
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        raise LabMayNotTrade(f"the lab's price feed has no {name!r}: it reads prices and "
                             f"nothing else")

    # ------------------------------------------------- the seven questions --
    def ping(self) -> Optional[dict]:
        """The bridge's own ping, unchanged (its 'backend' says when it is
        simulated, which is how a made-up feed is recognised). None when the
        bridge does not answer."""
        try:
            info = self._ask("ping")
        except LabMayNotTrade:
            raise
        except Exception as exc:
            self.last_error = str(exc)
            return None
        if isinstance(info, dict) and "sim" in str(info.get("backend", "")).lower():
            self.synthetic = True
        return info

    def server_time(self) -> dt.datetime:
        return dt.datetime.fromisoformat(self._ask("server_time"))

    def symbols(self) -> Sequence[str]:
        return tuple(self._ask("symbols"))

    def spec(self, symbol: str):
        return wire.spec_in(self._ask("spec", symbol=symbol))

    def tick(self, symbol: str):
        return wire.tick_in(self._ask("tick", symbol=symbol))

    def ticks(self, symbols: Sequence[str]) -> dict:
        raw = self._ask("ticks", symbols=list(symbols)) or {}
        return {k: wire.tick_in(v) for k, v in raw.items()}

    def bars(self, symbol: str, tf: TF, count: int, end: Optional[dt.datetime] = None) -> list:
        rows = self._ask("bars", symbol=symbol, tf=tf.value, count=int(count),
                         end=wire._iso(end) if end else None)
        return [wire.bar_in(r) for r in rows or []]


def from_config(cfg, *, factory: Optional[Callable] = None, timeout: float = 30.0) -> PriceFeed:
    """The feed for this machine's bridge (host, port and its token file from the config)."""
    return PriceFeed(cfg.bridge_host, cfg.bridge_port, cfg.bridge_token_file, timeout=timeout,
                     factory=factory)
