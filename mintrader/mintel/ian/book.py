"""The one normalised internal order-book format.

Every vendor adapter (Databento, a futures broker's API, a recording, the
synthetic scenario generator) turns what it receives into these three event
types, and nothing downstream ever sees a vendor's own record:

* :class:`BookEvent`  - a change to the resting book.
* :class:`TradeEvent` - an execution, with the aggressor side when the
  exchange says which side initiated it.
* :class:`FeedNotice` - something about the feed itself (a gap the vendor
  reported, a possibly bad book, a reconnect).

Semantics of :class:`BookEvent`, precisely:

* Price-level events (``order_id == 0``, market-by-price, MBP): ``size`` is
  the level's NEW TOTAL size. ``add`` = a level appeared, ``modify`` = its size
  changed, ``cancel`` = the level is gone (size 0).
* Order events (``order_id != 0``, market-by-order, MBO): ``add`` = a new
  order of ``size`` at ``price``; ``modify`` = the order now rests at
  ``price`` with ``size`` (both absolute); ``cancel`` = ``size`` was cancelled
  from it (0 or at least its remaining size removes it).
* ``clear`` empties the book (one side when ``side`` is set, else both).

``cause`` separates exchange actions from artefacts of how we see the book:
``""`` is a real exchange action; ``"view"`` is a level entering or leaving a
depth-limited view (an MBP-10 level pushed past the tenth price is NOT a
cancellation); ``"snapshot"`` is a book rebuild. Features count only real
actions as adds and cancels.

Prices are kept to 9 decimals (the vendors' fixed-point precision) so the same
price always lands on the same dictionary key.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Optional, Union

BID, ASK = "BID", "ASK"
ADD, MODIFY, CANCEL, CLEAR = "add", "modify", "cancel", "clear"
ACTIONS = (ADD, MODIFY, CANCEL, CLEAR)
BUY, SELL, UNKNOWN = "BUY", "SELL", "UNKNOWN"
AGGRESSORS = (BUY, SELL, UNKNOWN)

UTC = dt.timezone.utc


def px(p) -> float:
    """A price as a stable dictionary key."""
    return round(float(p), 9)


@dataclass(frozen=True)
class BookEvent:
    instrument: str
    ts_exchange: dt.datetime
    ts_received: dt.datetime
    sequence: int
    side: str                   # BID / ASK ("" only for a clear of both sides)
    price: float
    size: float
    action: str                 # add / modify / cancel / clear
    level: int = -1             # depth index when the vendor gives one (0 = top of book)
    order_id: int = 0           # MBO order id; 0 = a price-level (MBP) event
    cause: str = ""             # "" exchange action, "view" depth-window artefact, "snapshot" rebuild
    source: str = ""

    kind = "book"


@dataclass(frozen=True)
class TradeEvent:
    instrument: str
    ts: dt.datetime             # exchange time of the execution
    price: float
    size: float
    aggressor: str              # BUY (lifted the offer) / SELL (hit the bid) / UNKNOWN
    sequence: int
    ts_received: Optional[dt.datetime] = None
    source: str = ""

    kind = "trade"

    @property
    def received(self) -> dt.datetime:
        return self.ts_received or self.ts


@dataclass(frozen=True)
class FeedNotice:
    instrument: str             # "" = the whole feed
    ts: dt.datetime
    notice: str                 # GAP, BAD_BOOK, DISCONNECTED, RECONNECTED, SNAPSHOT_BEGIN, SNAPSHOT_END, ERROR, INFO
    detail: str = ""
    source: str = ""

    kind = "notice"

    @property
    def received(self) -> dt.datetime:
        return self.ts


Event = Union[BookEvent, TradeEvent, FeedNotice]


def received_at(ev: Event) -> dt.datetime:
    if isinstance(ev, BookEvent):
        return ev.ts_received
    return ev.received


def exchange_time(ev: Event) -> dt.datetime:
    if isinstance(ev, BookEvent):
        return ev.ts_exchange
    return ev.ts


# ------------------------------------------------------------ serialisation --

def _iso(t: Optional[dt.datetime]) -> Optional[str]:
    return t.astimezone(UTC).isoformat() if t is not None else None


def _dt(s) -> Optional[dt.datetime]:
    if s is None or s == "":
        return None
    t = dt.datetime.fromisoformat(str(s))
    return t if t.tzinfo is not None else t.replace(tzinfo=UTC)


def to_dict(ev: Event) -> dict:
    """A JSON-safe dict; :func:`from_dict` gives back an equal event."""
    if isinstance(ev, BookEvent):
        d = {"t": "book", "i": ev.instrument, "te": _iso(ev.ts_exchange), "tr": _iso(ev.ts_received),
             "q": int(ev.sequence), "s": ev.side, "p": ev.price, "z": ev.size, "a": ev.action}
        if ev.level >= 0:
            d["l"] = ev.level
        if ev.order_id:
            d["o"] = ev.order_id
        if ev.cause:
            d["c"] = ev.cause
        if ev.source:
            d["src"] = ev.source
        return d
    if isinstance(ev, TradeEvent):
        d = {"t": "trade", "i": ev.instrument, "te": _iso(ev.ts), "tr": _iso(ev.ts_received), "q": int(ev.sequence),
             "p": ev.price, "z": ev.size, "g": ev.aggressor}
        if ev.source:
            d["src"] = ev.source
        return d
    if isinstance(ev, FeedNotice):
        return {"t": "notice", "i": ev.instrument, "te": _iso(ev.ts), "n": ev.notice, "d": ev.detail,
                "src": ev.source}
    raise TypeError(f"not an event: {type(ev).__name__}")


def from_dict(d: dict) -> Event:
    t = d.get("t")
    if t == "book":
        return BookEvent(str(d["i"]), _dt(d["te"]), _dt(d.get("tr") or d["te"]), int(d.get("q") or 0),
                         str(d.get("s") or ""), px(d.get("p") or 0.0), float(d.get("z") or 0.0), str(d["a"]),
                         int(d.get("l", -1)), int(d.get("o") or 0), str(d.get("c") or ""), str(d.get("src") or ""))
    if t == "trade":
        g = str(d.get("g") or UNKNOWN).upper()
        return TradeEvent(str(d["i"]), _dt(d["te"]), px(d["p"]), float(d["z"]), g if g in AGGRESSORS else UNKNOWN,
                          int(d.get("q") or 0), _dt(d.get("tr")), str(d.get("src") or ""))
    if t == "notice":
        return FeedNotice(str(d.get("i") or ""), _dt(d["te"]), str(d.get("n") or "INFO"), str(d.get("d") or ""),
                          str(d.get("src") or ""))
    raise ValueError(f"unknown event type {t!r}")


# ---------------------------------------------------------------- the book --

@dataclass(frozen=True)
class LevelChange:
    """What one book event did to one price level (aggregated over orders)."""
    side: str
    price: float
    before: float
    after: float
    action: str
    cause: str
    best_before: Optional[float]     # the same side's best price before the change

    @property
    def delta(self) -> float:
        return self.after - self.before


@dataclass(frozen=True)
class BookSnapshot:
    instrument: str
    ts: Optional[dt.datetime]
    sequence: int
    bids: tuple                      # ((price, size), ...) best first
    asks: tuple
    mode: str                        # MBP or MBO

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0][0] if self.asks else None

    @property
    def mid(self) -> Optional[float]:
        if not self.bids or not self.asks:
            return None
        return (self.bids[0][0] + self.asks[0][0]) / 2.0

    def to_dict(self) -> dict:
        return {"instrument": self.instrument, "ts": _iso(self.ts), "sequence": self.sequence, "mode": self.mode,
                "bids": [list(x) for x in self.bids], "asks": [list(x) for x in self.asks]}


class OrderBook:
    """N levels each side by price (MBP), and per order (MBO) when the feed
    gives order ids. One instance per instrument."""

    def __init__(self, instrument: str, max_levels: int = 10):
        self.instrument = instrument
        self.max_levels = int(max_levels)
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.orders: dict[int, tuple[str, float, float]] = {}    # MBO: id -> (side, price, size)
        self.mode = "MBP"
        self.sequence = 0
        self.last_ts: Optional[dt.datetime] = None
        self.updates = 0

    # --------------------------------------------------------------- reads --
    def _side(self, side: str) -> dict[float, float]:
        return self.bids if side == BID else self.asks

    def best(self, side: str) -> Optional[float]:
        book = self._side(side)
        if not book:
            return None
        return max(book) if side == BID else min(book)

    def best_bid(self) -> Optional[float]:
        return self.best(BID)

    def best_ask(self) -> Optional[float]:
        return self.best(ASK)

    def size_at(self, side: str, price: float) -> float:
        return self._side(side).get(px(price), 0.0)

    def levels(self, side: str, n: Optional[int] = None) -> list[tuple[float, float]]:
        book = self._side(side)
        keys = sorted(book, reverse=(side == BID))
        if n is not None:
            keys = keys[:n]
        return [(k, book[k]) for k in keys]

    def depth(self, side: str, n: int) -> float:
        return float(sum(s for _, s in self.levels(side, n)))

    def mid(self) -> Optional[float]:
        b, a = self.best_bid(), self.best_ask()
        if b is None or a is None:
            return None
        return (a + b) / 2.0

    def spread(self) -> Optional[float]:
        b, a = self.best_bid(), self.best_ask()
        if b is None or a is None:
            return None
        return a - b

    def microprice(self) -> Optional[float]:
        """(P_ask * Q_bid + P_bid * Q_ask) / (Q_bid + Q_ask) at the touch."""
        b, a = self.best_bid(), self.best_ask()
        if b is None or a is None:
            return None
        qb, qa = self.bids[b], self.asks[a]
        if qb + qa <= 0:
            return (a + b) / 2.0
        return (a * qb + b * qa) / (qb + qa)

    def crossed(self) -> bool:
        b, a = self.best_bid(), self.best_ask()
        return b is not None and a is not None and b >= a

    def snapshot(self, n: Optional[int] = None) -> BookSnapshot:
        n = self.max_levels if n is None else n
        return BookSnapshot(self.instrument, self.last_ts, self.sequence, tuple(self.levels(BID, n)),
                            tuple(self.levels(ASK, n)), self.mode)

    # -------------------------------------------------------------- writes --
    def _set_level(self, side: str, price: float, size: float) -> float:
        book = self._side(side)
        before = book.get(price, 0.0)
        if size <= 0:
            book.pop(price, None)
        else:
            book[price] = float(size)
        return before

    def apply(self, ev: BookEvent) -> list[LevelChange]:
        """Apply one event. Returns the level changes it caused (none for a
        no-op, two for an MBO order that moved price)."""
        self.sequence = max(self.sequence, int(ev.sequence))
        self.last_ts = ev.ts_exchange
        self.updates += 1
        if ev.action == CLEAR:
            sides = (ev.side,) if ev.side in (BID, ASK) else (BID, ASK)
            for s in sides:
                self._side(s).clear()
            self.orders = {k: v for k, v in self.orders.items() if v[0] not in sides}
            return []
        side = ev.side
        if side not in (BID, ASK):
            return []
        price = px(ev.price)
        best_before = self.best(side)
        if ev.order_id:
            self.mode = "MBO"
            return self._apply_order(ev, side, price, best_before)
        size = float(ev.size) if ev.action != CANCEL else 0.0
        before = self._set_level(side, price, size)
        after = self._side(side).get(price, 0.0)
        if before == after:
            return []
        return [LevelChange(side, price, before, after, ev.action, ev.cause, best_before)]

    def _apply_order(self, ev: BookEvent, side: str, price: float, best_before) -> list[LevelChange]:
        out: list[LevelChange] = []
        oid = int(ev.order_id)
        prev = self.orders.get(oid)

        def move(s: str, p: float, dq: float, action: str) -> None:
            book = self._side(s)
            before = book.get(p, 0.0)
            after = max(0.0, before + dq)
            if after <= 1e-12:
                book.pop(p, None)
                after = 0.0
            else:
                book[p] = after
            if before != after:
                out.append(LevelChange(s, p, before, after, action, ev.cause, best_before))

        if ev.action == ADD:
            if prev is not None:                       # a repeated add is a modify
                move(prev[0], prev[1], -prev[2], MODIFY)
            self.orders[oid] = (side, price, float(ev.size))
            move(side, price, float(ev.size), ADD)
        elif ev.action == MODIFY:
            if prev is not None:
                move(prev[0], prev[1], -prev[2], MODIFY)
            if ev.size > 0:
                self.orders[oid] = (side, price, float(ev.size))
                move(side, price, float(ev.size), MODIFY)
            else:
                self.orders.pop(oid, None)
        elif ev.action == CANCEL:
            if prev is None:
                return out
            q = float(ev.size)
            if q <= 0 or q >= prev[2] - 1e-12:
                self.orders.pop(oid, None)
                move(prev[0], prev[1], -prev[2], CANCEL)
            else:
                self.orders[oid] = (prev[0], prev[1], prev[2] - q)
                move(prev[0], prev[1], -q, CANCEL)
        return out
