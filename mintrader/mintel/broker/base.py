"""Broker abstraction.

The intelligence layer never imports MetaTrader5 directly.  It talks to this
interface, which has two implementations:

* :class:`mintel.broker.mt5_adapter.Mt5Broker` - production, Windows VPS.
* :class:`mintel.broker.sim.SimBroker`         - deterministic simulation used
  by the test suite, the chaos harness and the backtester.

That split is what makes the whole system testable on any OS, and it is why the
failure-injection tests (stale ticks, rejections, spread explosions, restarts)
can be run at all.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Protocol, Sequence

from ..contracts import SymbolSpec


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

    @property
    def sign(self) -> int:
        return 1 if self is Side.BUY else -1

    def opposite(self) -> "Side":
        return Side.SELL if self is Side.BUY else Side.BUY


class TF(str, Enum):
    """Timeframes.  ``S5``/``S10``/``S30`` are synthesised from ticks."""
    S5 = "S5"
    S10 = "S10"
    S30 = "S30"
    M1 = "M1"
    M5 = "M5"
    M15 = "M15"
    M30 = "M30"
    H1 = "H1"
    H4 = "H4"
    D1 = "D1"

    @property
    def seconds(self) -> int:
        return {
            "S5": 5, "S10": 10, "S30": 30, "M1": 60, "M5": 300, "M15": 900,
            "M30": 1800, "H1": 3600, "H4": 14400, "D1": 86400,
        }[self.value]

    @property
    def is_synthetic(self) -> bool:
        return self.value in ("S5", "S10", "S30")


@dataclass(frozen=True)
class Tick:
    symbol: str
    time: dt.datetime      # UTC
    bid: float
    ask: float
    last: float = 0.0
    volume: float = 0.0

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread(self) -> float:
        return self.ask - self.bid


@dataclass(frozen=True)
class Bar:
    time: dt.datetime      # UTC, bar OPEN time
    open: float
    high: float
    low: float
    close: float
    tick_volume: float = 0.0
    real_volume: float = 0.0
    spread_points: float = 0.0

    @property
    def range(self) -> float:
        return self.high - self.low

    @property
    def body(self) -> float:
        return abs(self.close - self.open)

    @property
    def body_pct(self) -> float:
        return self.body / self.range if self.range > 0 else 0.0

    @property
    def upper_wick(self) -> float:
        return self.high - max(self.open, self.close)

    @property
    def lower_wick(self) -> float:
        return min(self.open, self.close) - self.low

    @property
    def is_up(self) -> bool:
        return self.close >= self.open


@dataclass
class Position:
    ticket: int
    symbol: str
    side: Side
    volume: float
    entry_price: float
    sl: float
    tp: float
    open_time: dt.datetime
    profit: float = 0.0
    swap: float = 0.0
    comment: str = ""
    magic: int = 0


@dataclass
class OrderRequest:
    symbol: str
    side: Side
    volume: float
    sl: float
    tp: float = 0.0
    price: float = 0.0          # 0 => market
    deviation_points: int = 20
    comment: str = ""
    magic: int = 0
    idempotency_key: str = ""
    filling: str = "IOC"


class RetCode(int, Enum):
    """Subset of MT5 return codes the engine reacts to by name."""
    DONE = 10009
    DONE_PARTIAL = 10010
    PLACED = 10008
    REQUOTE = 10004
    REJECT = 10006
    INVALID_PRICE = 10015
    INVALID_STOPS = 10016
    NO_MONEY = 10019
    PRICE_CHANGED = 10020
    PRICE_OFF = 10021
    TRADE_DISABLED = 10017
    MARKET_CLOSED = 10018
    TOO_MANY_REQUESTS = 10024
    INVALID_VOLUME = 10014
    CONNECTION = 10031
    LIMIT_VOLUME = 10030
    UNKNOWN = 0

    @classmethod
    def of(cls, v: int) -> "RetCode":
        try:
            return cls(v)
        except ValueError:
            return cls.UNKNOWN


RETRYABLE = {RetCode.REQUOTE, RetCode.PRICE_CHANGED, RetCode.PRICE_OFF,
             RetCode.TOO_MANY_REQUESTS, RetCode.CONNECTION}
FATAL_FOR_SETUP = {RetCode.NO_MONEY, RetCode.TRADE_DISABLED,
                   RetCode.MARKET_CLOSED, RetCode.INVALID_VOLUME,
                   RetCode.LIMIT_VOLUME}


@dataclass
class OrderResult:
    ok: bool
    retcode: int
    ticket: int = 0
    deal: int = 0
    filled_volume: float = 0.0
    price: float = 0.0
    requested_volume: float = 0.0
    comment: str = ""
    idempotency_key: str = ""
    raw: dict = field(default_factory=dict)

    @property
    def code(self) -> RetCode:
        return RetCode.of(self.retcode)

    @property
    def retryable(self) -> bool:
        return self.code in RETRYABLE

    @property
    def partial(self) -> bool:
        return (self.filled_volume > 0
                and self.requested_volume > 0
                and self.filled_volume < self.requested_volume - 1e-9)


@dataclass
class AccountInfo:
    login: int
    server: str
    currency: str
    balance: float
    equity: float
    margin: float
    margin_free: float
    margin_level: float
    leverage: int
    trade_allowed: bool
    is_demo: bool
    company: str = ""
    name: str = ""


@dataclass
class CalendarEvent:
    event_id: str
    time_utc: dt.datetime
    currency: str
    country: str
    name: str
    importance: int              # 0 none, 1 low, 2 medium, 3 high
    actual: Optional[float] = None
    forecast: Optional[float] = None
    previous: Optional[float] = None
    revised_previous: Optional[float] = None
    unit: str = ""
    source: str = "mt5"


class BrokerError(RuntimeError):
    pass


class NotConnected(BrokerError):
    pass


class Broker(Protocol):
    """Everything the engine is allowed to ask of a broker."""

    def connect(self) -> bool: ...
    def shutdown(self) -> None: ...
    def is_connected(self) -> bool: ...
    def account(self) -> AccountInfo: ...
    def server_time(self) -> dt.datetime: ...
    def symbols(self) -> Sequence[str]: ...
    def spec(self, symbol: str) -> Optional[SymbolSpec]: ...
    def ensure_selected(self, symbol: str) -> bool: ...
    def tick(self, symbol: str) -> Optional[Tick]: ...
    def bars(self, symbol: str, tf: TF, count: int,
             end: Optional[dt.datetime] = None) -> list[Bar]: ...
    def ticks_range(self, symbol: str, start: dt.datetime,
                    end: dt.datetime) -> list[Tick]: ...
    def positions(self, magic: Optional[int] = None) -> list[Position]: ...
    def send(self, req: OrderRequest) -> OrderResult: ...
    def modify_stops(self, ticket: int, sl: float, tp: float) -> OrderResult: ...
    def close(self, ticket: int, volume: float = 0.0,
              comment: str = "") -> OrderResult: ...
    def calendar(self, start: dt.datetime, end: dt.datetime,
                 currencies: Sequence[str] = ()) -> list[CalendarEvent]: ...
    def closed_deal(self, ticket: int) -> Optional[dict]:
        """What the broker says actually happened to a closed position.

        Returns ``{"exit_price", "pnl", "volume", "time"}`` or ``None``.
        The engine prefers this over any local calculation: the broker
        knows the fills, the commission and the swap, and we do not.
        """
        ...

    def calc_margin(self, symbol: str, side: Side, volume: float,
                    price: float) -> Optional[float]:
        """Margin required, in ACCOUNT currency, or None if the broker
        cannot say.  Never guessed here: a wrong margin number sizes
        positions wrongly, so an unknown value is reported as unknown and
        the caller falls back to a conservative estimate."""
        ...
