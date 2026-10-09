"""Financial Ian on past CME order-book data: the REAL engine, a simulated broker, honest costs.

What decides is Ian's own code - :class:`mintel.ian.engine.IanEngine` with its book, features, regime,
score, signal, MT5 bridge checks, order-flow trail and data-quality gate - fed the recorded events one
at a time, with a decision pass every ``decision_interval_s`` of EVENT time, exactly as
:meth:`IanEngine.run_virtual` does. Only the world around it is simulated (no MetaTrader, no orders):

* SPOT QUOTES: each futures quote is mapped to its pair with Ian's own mapping
  (:func:`mintel.ian.mapping.futures_to_spot_price`: 1/x for 6J, 6C and 6S), plus the pair's typical
  spot spread, widened when the futures book's spread widens. The quote's time is the time of the
  future's last event, so a silent book gives an old quote, as a frozen feed would.
* THE EXECUTOR: a market order fills ``latency_ms`` after the decision, at the pair's quote then, plus
  slippage. A STOP is held "at the broker": the first quote that touches it fills it, at the stop or
  WORSE (a gap fills at the first price beyond it), never better. Commission is charged per lot per side.
* MONEY: Ian's own sizing (``risk_money`` at the stop, from ian.json) in GBP, the account currency,
  converted at the replay's own prices (GBPUSD from 6B, USDJPY from 6J ...).
* THE CHART (live: MT5 bars, RETAIL data) is built from the same mapped prices; the CALENDAR is the bot's
  own record (data/calendar.sqlite) where it covers the days, read-only.

NO LOOK-AHEAD: a decision at time t sees only events and quotes stamped before t. A fill uses the quote at
t + latency - that is when the order would fill, not information the decision had.
ONLY INSIDE THE TEST WINDOWS: Ian decides only while a window is open. When one ends, a trade still open is
closed at the last quote of ITS OWN contract released by then (never a later one, never the next contract's
after a roll), and nothing is opened until the next window starts.
THE LIVE STREAM: past records become events through the live adapter's own mapper and its one-count rule for
trades (the copy inside mbp-10 is skipped when the trades schema is there, as live with both subscriptions).
DETERMINISTIC: the same files give the same trades (event time only, no wall clock, no randomness).

Anything run on fixture or synthetic events is labelled SYNTHETIC - NOT PERFORMANCE everywhere.
"""
from __future__ import annotations

import csv
import dataclasses
import datetime as dt
import heapq
import io
import json
import shutil
import sqlite3
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Iterator, Optional, Sequence

from ...broker.base import Bar, CalendarEvent, Side, TF, Tick
from ...contracts import SymbolSpec, classify_fx
from .. import INSTITUTIONAL, SYNTHETIC_LABEL
from ..book import BookEvent, OrderBook, received_at
from ..engine import PAPER_FIRST_TICKET, IanConfig, IanEngine
from ..feeds.base import FINISHED, LIVE, FeedAdapter, FeedHealth
from ..feeds.databento import RecordMapper, _get, book_trades_wanted
from ..mapping import CONTRACTS, future_for_spot, futures_to_spot_price, root_of

UTC = dt.timezone.utc
REAL_LABEL = "REAL PAST CME DATA"
SOURCE_DATABENTO = "Databento GLBX.MDP3 (CME Globex MDP 3.0), downloaded by this tool"

# Typical IC Markets Raw spot spreads in pips: approximate published averages (the same figures the
# Rapid Momentum Rider's research uses), not measured on Aaron's account. Change them with --spread.
DEFAULT_SPREAD_PIPS = {"EURUSD": 0.12, "GBPUSD": 0.25, "AUDUSD": 0.2, "NZDUSD": 0.3, "USDCAD": 0.25,
                       "USDJPY": 0.15, "USDCHF": 0.25}
# Used to turn dollars, yen, Canadian dollars and francs into pounds ONLY until the replay has its own
# price for that rate (6B, 6J, 6C, 6S); every use is counted and reported.
REFERENCE_RATES = {"GBPUSD": 1.27, "USDJPY": 150.0, "USDCAD": 1.37, "USDCHF": 0.89}
RATE_FUTURE = {"JPY": "6J", "CAD": "6C", "CHF": "6S"}
LOT = 100_000.0
BAR_GAP_S = 1800.0             # a quote gap longer than this (the night, a halt) starts the 1-minute chart again
STALE_FILL_S = 10.0            # a market fill on a quote older than this is flagged as stale
MIN_TRADES = 30
STRESS = (1.0, 1.5, 2.0)

INSUFFICIENT = "INSUFFICIENT DATA"
NO_EDGE = "NO EDGE"
FRAGILE = "FRAGILE"
PROMISING = "PROMISING - WORTH A LIVE FEED TRIAL"


def pip_size(symbol: str) -> float:
    return 0.01 if "JPY" in symbol.upper() else 0.0001


def _iso(t: Optional[dt.datetime]) -> Optional[str]:
    return t.astimezone(UTC).isoformat() if t is not None else None


# ------------------------------------------------------------------- costs --

@dataclass
class CostModel:
    latency_ms: float = 250.0                  # decision -> fill, for market orders (entries and market exits)
    slippage_pips: float = 0.1                 # per market fill and per stop fill, against us
    commission_usd_per_lot_side: float = 3.50
    spread_pips: dict = field(default_factory=lambda: dict(DEFAULT_SPREAD_PIPS))
    spread_follows_futures: bool = True        # widen the spot spread when the futures spread widens
    max_spread_widening: float = 10.0

    @property
    def latency(self) -> dt.timedelta:
        return dt.timedelta(milliseconds=float(self.latency_ms))

    def to_dict(self) -> dict:
        return {"latency_ms": self.latency_ms, "slippage_pips": self.slippage_pips,
                "commission_usd_per_lot_side": self.commission_usd_per_lot_side,
                "spread_pips": dict(self.spread_pips), "spread_follows_futures": self.spread_follows_futures,
                "max_spread_widening": self.max_spread_widening}


# -------------------------------------------------------------------- bars --

class BarSeries:
    """OHLC bars of one length from a stream of prices; the forming bar is included (as MT5's are)."""

    def __init__(self, seconds: int, keep: int = 400):
        self.seconds = int(seconds)
        self._bars: deque = deque(maxlen=keep)      # [key, open, high, low, close, n]

    def add(self, t: dt.datetime, price: float) -> None:
        key = int(t.timestamp()) // self.seconds
        b = self._bars[-1] if self._bars else None
        if b is not None and b[0] == key:
            if price > b[2]:
                b[2] = price
            if price < b[3]:
                b[3] = price
            b[4] = price
            b[5] += 1
        elif b is None or key > b[0]:
            self._bars.append([key, price, price, price, price, 1])

    def reset(self) -> None:
        self._bars.clear()

    def bars(self, count: int) -> list[Bar]:
        out = list(self._bars)[-max(0, int(count)):]
        return [Bar(dt.datetime.fromtimestamp(k * self.seconds, UTC), o, h, lo, c, float(n))
                for k, o, h, lo, c, n in out]

    def __len__(self) -> int:
        return len(self._bars)


# ------------------------------------------------------------------ market --

@dataclass(frozen=True)
class Quote:
    """The futures book's best bid and offer, as of ``t`` (the time of the future's last event)."""
    t: dt.datetime
    instrument: str
    bid: Optional[float]
    ask: Optional[float]

    @property
    def ok(self) -> bool:
        return self.bid is not None and self.ask is not None and self.bid > 0 and self.ask >= self.bid

    @property
    def mid(self) -> float:
        return (float(self.bid) + float(self.ask)) / 2.0


class Market:
    """The futures quotes the backtest trades against, built from the very events the engine replays.

    A SHADOW book per contract reads the event stream slightly AHEAD of the engine - only so far that a
    fill ``latency`` after a decision can be priced. Everything the engine and the broker view see is
    released in time order by :meth:`advance` (quotes stamped strictly before the decision time), so no
    decision can see a quote from its own future."""

    def __init__(self, batches: Iterable[list], roots: Sequence[str], costs: CostModel):
        self._src: Iterator[list] = iter(batches)
        self.roots = [r for r in roots if r in CONTRACTS]
        self._roots = set(self.roots)
        self.costs = costs
        self._events: deque = deque()                     # read, not yet handed to the engine
        self._tape: deque = deque()                       # (t, root, instrument, bid, ask, changed), read, not released
        self._books: dict[str, OrderBook] = {}
        self._read_bbo: dict[str, tuple] = {}
        self._read_inst: dict[str, str] = {}
        self.read_t: Optional[dt.datetime] = None
        self.done = False
        self.now: Optional[dt.datetime] = None
        self.cur: dict[str, Quote] = {}                   # released quotes: as of ``now``
        self.cur_by_inst: dict[str, Quote] = {}           # the same, per contract (a roll keeps the old one's)
        # while set (a test window has ended), :meth:`quote_at` uses nothing stamped at or after it
        self.horizon: Optional[dt.datetime] = None
        self.min_spread: dict[str, float] = {}
        self._last_change: dict[str, dt.datetime] = {}
        self.bars: dict[str, dict[int, BarSeries]] = {r: self._new_bars() for r in self.roots}
        self.on_quote: Optional[Callable[[str, Quote], None]] = None
        self.events_read = 0
        self.reference_rate_uses = 0                      # calls answered with a fixed reference rate
        self.reference_rates_used: set[str] = set()
        self.rolls: list[tuple] = []

    @staticmethod
    def _new_bars() -> dict[int, BarSeries]:
        return {60: BarSeries(60, 400), 300: BarSeries(300, 200), 3600: BarSeries(3600, 60)}

    # ------------------------------------------------------------- reading --
    def _read(self) -> bool:
        try:
            batch = next(self._src)
        except StopIteration:
            self.done = True
            return False
        if not batch:
            return True
        touched: dict[str, str] = {}
        for ev in batch:
            self._events.append(ev)
            self.events_read += 1
            inst = getattr(ev, "instrument", "") or ""
            if not inst or "-" in inst:
                continue
            root = root_of(inst)
            if root not in self._roots:
                continue
            if isinstance(ev, BookEvent):
                b = self._books.get(inst)
                if b is None:
                    b = self._books[inst] = OrderBook(inst)
                b.apply(ev)
            touched[root] = inst
        t = received_at(batch[-1])
        for root, inst in touched.items():
            b = self._books.get(inst)
            bbo = (b.best_bid(), b.best_ask()) if b is not None else (None, None)
            changed = bbo != self._read_bbo.get(root) or inst != self._read_inst.get(root)
            self._read_bbo[root] = bbo
            self._read_inst[root] = inst
            self._tape.append((t, root, inst, bbo[0], bbo[1], changed))
        if self.read_t is None or t > self.read_t:
            self.read_t = t
        return True

    def next_event(self):
        while not self._events:
            if self.done or not self._read():
                if not self._events:
                    return None
        return self._events.popleft()

    def ensure(self, t: dt.datetime) -> None:
        """Read ahead until everything stamped at or before ``t`` is in the shadow books."""
        while not self.done and (self.read_t is None or self.read_t <= t):
            self._read()

    # ----------------------------------------------------------- releasing --
    def advance(self, t: dt.datetime) -> None:
        """Release every quote stamped strictly before ``t``: the current quotes, the bars, the spread
        reference and the executor's stop checks all move to time ``t``."""
        tape = self._tape
        while tape and tape[0][0] < t:
            qt, root, inst, bid, ask, changed = tape.popleft()
            prev = self.cur.get(root)
            if prev is not None and prev.instrument != inst:
                self.bars[root] = self._new_bars()           # a new contract: a new price level
                self.min_spread.pop(root, None)
                self._last_change.pop(root, None)
                self.rolls.append((qt, root, prev.instrument, inst))
            q = Quote(qt, inst, bid, ask)
            self.cur[root] = q
            self.cur_by_inst[inst] = q
            if not (changed and q.ok):
                continue
            sp = float(ask) - float(bid)
            if sp > 0 and (root not in self.min_spread or sp < self.min_spread[root]):
                self.min_spread[root] = sp
            last = self._last_change.get(root)
            if last is not None and (qt - last).total_seconds() > BAR_GAP_S:
                self.bars[root][60].reset()                  # no stale 1-minute chart across the night
            self._last_change[root] = qt
            mid = self.spot_mid(root, q)
            for s in self.bars[root].values():
                s.add(qt, mid)
            if self.on_quote is not None:
                self.on_quote(root, q)
        if self.now is None or t > self.now:
            self.now = t

    def quote_at(self, root: str, t: dt.datetime, instrument: Optional[str] = None) -> Optional[Quote]:
        """The futures quote as of ``t`` (for a FILL at ``t``; decisions use :attr:`cur`). With ``instrument``
        only that contract's quotes count: a position is priced on the contract it was filled on, never on the
        next one after a roll. While :attr:`horizon` is set, nothing stamped at or after it is used (a trade
        closed because the test data ends is priced on the last quote released by then, not on later data)."""
        h = self.horizon
        self.ensure(t if h is None else min(t, h))
        q = self.cur_by_inst.get(instrument) if instrument else self.cur.get(root)
        for qt, r, inst, bid, ask, _ in self._tape:
            if qt > t or (h is not None and qt >= h):
                break
            if r == root and (not instrument or inst == instrument):
                q = Quote(qt, inst, bid, ask)
        return q

    # -------------------------------------------------------------- prices --
    @staticmethod
    def spot_mid(root: str, q: Quote) -> float:
        return futures_to_spot_price(root, q.mid)

    def widening(self, root: str, q: Quote) -> float:
        if not self.costs.spread_follows_futures:
            return 1.0
        m = self.min_spread.get(root)
        if not m or m <= 0 or not q.ok:
            return 1.0
        return max(1.0, min(self.costs.max_spread_widening, (float(q.ask) - float(q.bid)) / m))

    def spot_tick(self, root: str, q: Quote) -> Optional[Tick]:
        if q is None or not q.ok:
            return None
        sym = CONTRACTS[root].spot
        mid = self.spot_mid(root, q)
        half = 0.5 * float(self.costs.spread_pips.get(sym, 0.3)) * pip_size(sym) * self.widening(root, q)
        return Tick(sym, q.t, mid - half, mid + half)

    def _spot_now(self, root: str) -> Optional[float]:
        q = self.cur.get(root)
        return self.spot_mid(root, q) if q is not None and q.ok else None

    def rate(self, name: str) -> float:
        root = {"GBPUSD": "6B", "USDJPY": "6J", "USDCAD": "6C", "USDCHF": "6S"}[name]
        v = self._spot_now(root)
        if v is None or v <= 0:
            self.reference_rate_uses += 1
            self.reference_rates_used.add(name)
            return REFERENCE_RATES[name]
        return v

    def to_gbp(self, ccy: str) -> float:
        """Pounds per one unit of ``ccy``, at the replay's prices as of now."""
        ccy = ccy.upper()
        if ccy == "GBP":
            return 1.0
        gbpusd = self.rate("GBPUSD")
        if ccy == "USD":
            return 1.0 / gbpusd
        if ccy in RATE_FUTURE:
            return 1.0 / (gbpusd * self.rate(f"USD{ccy}"))
        raise KeyError(f"no conversion for {ccy}")

    def gbp_per_pip(self, symbol: str) -> float:
        """Pounds per pip per 1.0 lot."""
        _, quote, _ = classify_fx(symbol)
        return pip_size(symbol) * LOT * self.to_gbp(quote or "USD")

    def spec(self, symbol: str) -> Optional[SymbolSpec]:
        root = future_for_spot(symbol)
        if root not in self._roots:
            return None
        base, quote, group = classify_fx(symbol)
        pip = pip_size(symbol)
        point = pip / 10.0
        digits = 3 if pip == 0.01 else 5
        return SymbolSpec(name=symbol, digits=digits, point=point, tick_size=point,
                          tick_value=point * LOT * self.to_gbp(quote), contract_size=LOT, volume_min=0.01,
                          volume_max=100.0, volume_step=0.01, stops_level_points=0.0, freeze_level_points=0.0,
                          base_currency=base, profit_currency=quote, margin_currency=base, asset_group=group or "FX_MAJOR",
                          typical_spread_points=float(self.costs.spread_pips.get(symbol, 0.3)) * 10.0)


class SpotBroker:
    """The engine's "MetaTrader" in the backtest: spot quotes, bars, contracts and the calendar, all from
    the replayed futures and the bot's own calendar record. It cannot place anything."""

    def __init__(self, market: Market, calendar: Sequence[CalendarEvent] = ()):
        self.market = market
        self._calendar = sorted(calendar, key=lambda e: e.time_utc)

    def is_connected(self) -> bool:
        return True

    def symbols(self) -> list[str]:
        return [CONTRACTS[r].spot for r in self.market.roots]

    def spec(self, symbol: str):
        return self.market.spec(symbol)

    def tick(self, symbol: str) -> Optional[Tick]:
        root = future_for_spot(symbol)
        q = self.market.cur.get(root)
        return self.market.spot_tick(root, q) if q is not None else None

    def bars(self, symbol: str, tf: TF, count: int, end=None) -> list[Bar]:
        root = future_for_spot(symbol)
        series = self.market.bars.get(root, {}).get(TF(tf).seconds)
        return series.bars(count) if series is not None else []

    def calendar(self, start: dt.datetime, end: dt.datetime, currencies: Sequence[str] = ()) -> list[CalendarEvent]:
        """Events in [start, end]. A release later than the replay's 'now' is shown as MT5 would show it:
        scheduled, with no actual figure yet."""
        now = self.market.now
        ccys = {c.upper() for c in currencies}
        out = []
        for e in self._calendar:
            if e.time_utc < start or e.time_utc > end or (ccys and e.currency.upper() not in ccys):
                continue
            if now is not None and e.time_utc > now and e.actual is not None:
                e = dataclasses.replace(e, actual=None)
            out.append(e)
        return out

    def positions(self, magic=None) -> list:
        return []

    def closed_deal(self, ticket: int):
        return None

    def deals_since(self, *a, **k) -> list:
        return []


class BacktestFeed(FeedAdapter):
    """What the engine sees as its feed: virtual time, the past data's own label. The driver hands the
    events to the engine itself (``ingest``), in arrival order."""

    virtual_time = True

    def __init__(self, market: Market, synthetic: bool, sequence_scope: str = "channel",
                 vendor: str = "databento-history"):
        self.market = market
        self.synthetic = bool(synthetic)
        self.sequence_scope = sequence_scope
        self.vendor = vendor
        self.finished = False

    def connect(self) -> bool:
        return True

    def events(self, max_events: int = 10_000) -> list:
        return []

    def now(self) -> Optional[dt.datetime]:
        return self.market.now

    def health(self) -> FeedHealth:
        state = FINISHED if self.finished else LIVE
        reason = ("synthetic test events (not a market)" if self.synthetic
                  else "replaying past CME order-book data")
        return FeedHealth(state, reason, self.vendor, INSTITUTIONAL, self.synthetic, True, self.market.events_read,
                          _iso(self.market.now))

    def recover(self, reason: str = "") -> bool:
        return True                      # past data is what it is


# ---------------------------------------------------------------- executor --

@dataclass
class SimTrade:
    ticket: int
    symbol: str
    root: str
    side: Side
    volume: float
    sl: float
    decided: dt.datetime
    filled: dt.datetime
    entry: float
    entry_mid: float
    entry_quote_t: dt.datetime
    commission_gbp: float = 0.0
    trigger: Optional[tuple] = None           # (time, fill price, mid) when a quote touched the stop
    exit_t: Optional[dt.datetime] = None
    exit: Optional[float] = None
    exit_mid: Optional[float] = None
    exit_kind: str = ""
    stale_exit: bool = False
    reference_rate: bool = False              # its money was converted with a fixed reference rate
    gbp_per_pip: float = 0.0
    result: dict = field(default_factory=dict)
    instrument: str = ""                      # the futures contract it was filled on (priced on it only)


@dataclass
class _Fill:
    ok: bool
    ticket: int = 0
    price: float = 0.0
    requested: float = 0.0
    volume: float = 0.0
    message: str = ""


class BacktestExecutor:
    """The PAPER executor's interface, with fills priced from the replayed market: a market order fills
    ``latency`` after it is sent at the quote then plus slippage; a stop fills at the stop or WORSE the
    moment any quote touches it (held at the broker, between the engine's passes too)."""

    mode = "PAPER"

    def __init__(self, market: Market, costs: CostModel, first_ticket: int = PAPER_FIRST_TICKET):
        self.market = market
        self.costs = costs
        self._next = int(first_ticket)
        self.held: dict[int, SimTrade] = {}          # open positions by ticket
        self.closed: list[SimTrade] = []
        self.unbooked: list[SimTrade] = []          # closed, their net not yet written into Ian's journal
        self.last_error = ""
        self.refused_fills = 0
        market.on_quote = self._on_quote

    # ------------------------------------------------------------- helpers --
    def _slip(self, symbol: str) -> float:
        return float(self.costs.slippage_pips) * pip_size(symbol)

    def _commission_gbp(self, volume: float) -> float:
        return float(self.costs.commission_usd_per_lot_side) * float(volume) * self.market.to_gbp("USD")

    def _stop_fill(self, p: SimTrade, tick: Tick) -> Optional[float]:
        """The fill of a stop this quote touches: at the stop or worse, never better."""
        if p.side is Side.BUY:
            return (min(p.sl, tick.bid) - self._slip(p.symbol)) if tick.bid <= p.sl else None
        return (max(p.sl, tick.ask) + self._slip(p.symbol)) if tick.ask >= p.sl else None

    def _on_quote(self, root: str, q: Quote) -> None:
        if not self.held:
            return
        tick = None
        for p in self.held.values():
            if p.root != root or p.trigger is not None or q.t < p.filled or not p.sl:
                continue
            if p.instrument and q.instrument != p.instrument:
                continue                          # another contract's quote (after a roll) is not this position's
            tick = tick or self.market.spot_tick(root, q)
            if tick is None:
                return
            px = self._stop_fill(p, tick)
            if px is not None:
                p.trigger = (q.t, px, tick.mid)

    # ------------------------------------------------------- the interface --
    def reserve(self, ticket: int) -> None:
        self._next = max(self._next, int(ticket) + 1)

    def adopt(self, position) -> None:
        pass                                       # a backtest starts with nothing open

    def open(self, symbol: str, side: Side, volume: float, stop: float, tp: float, tick: Tick, spec,
             now: dt.datetime, comment: str = "") -> _Fill:
        root = future_for_spot(symbol)
        at = now + self.costs.latency
        seen = self.market.cur.get(root)                 # the contract the decision was made on
        q = self.market.quote_at(root, at, seen.instrument if seen is not None else None)
        st = self.market.spot_tick(root, q) if q is not None else None
        if st is None:
            self.refused_fills += 1
            self.last_error = "no futures price to fill at"
            return _Fill(False, message=self.last_error)
        px = (st.ask + self._slip(symbol)) if side is Side.BUY else (st.bid - self._slip(symbol))
        ticket = self._next
        self._next += 1
        before = self.market.reference_rate_uses
        comm = self._commission_gbp(volume)
        self.held[ticket] = SimTrade(ticket, symbol, root, side, float(volume), float(stop or 0.0), now, at,
                                     float(px), st.mid, q.t, commission_gbp=comm,
                                     reference_rate=self.market.reference_rate_uses > before, instrument=q.instrument)
        touch = float(tick.ask if side is Side.BUY else tick.bid) if tick is not None else px
        return _Fill(True, ticket, float(px), touch, float(volume), "paper fill")

    def modify_stop(self, ticket: int, stop: float, tp: Optional[float] = None) -> bool:
        p = self.held.get(ticket)
        if p is None or p.trigger is not None:
            self.last_error = "the position is gone" if p is None else "the stop has already been hit"
            return False
        p.sl = float(stop)
        return True

    def stop_hit(self, ticket: int, tick: Tick) -> bool:
        p = self.held.get(ticket)
        if p is None or not p.sl:
            return False
        if p.trigger is None and tick is not None and self.market.now is not None and self.market.now >= p.filled:
            px = self._stop_fill(p, tick)
            if px is not None:
                p.trigger = (self.market.now, px, tick.mid)
        return p.trigger is not None

    def target_hit(self, ticket: int, tick: Tick) -> bool:
        return False

    def close(self, ticket: int, tick: Tick, spec, at_price: Optional[float] = None, comment: str = "") -> _Fill:
        p = self.held.get(ticket)
        if p is None:
            return _Fill(False, message="no such position")
        now = self.market.now or p.filled
        if p.trigger is not None:
            p.exit_t, p.exit, p.exit_mid = p.trigger
            p.exit_kind = "STOP"
        elif at_price is not None and tick is not None:
            px = self._stop_fill(dataclasses.replace(p, sl=float(at_price)), tick)
            if px is None:                          # asked to close AT a level the quote has not reached
                px = (tick.bid if p.side is Side.BUY else tick.ask) - p.side.sign * self._slip(p.symbol)
            p.exit_t, p.exit, p.exit_mid, p.exit_kind = now, px, tick.mid, "STOP"
        else:
            at = max(now, p.filled) + self.costs.latency
            q = self.market.quote_at(p.root, at, p.instrument or None)
            st = self.market.spot_tick(p.root, q) if q is not None else None
            if st is None:
                st = tick
            if st is None:
                return _Fill(False, message="no price to close at")
            px = (st.bid - self._slip(p.symbol)) if p.side is Side.BUY else (st.ask + self._slip(p.symbol))
            p.exit_t, p.exit, p.exit_mid, p.exit_kind = at, px, st.mid, "MARKET"
            p.stale_exit = (at - st.time).total_seconds() > STALE_FILL_S
        del self.held[ticket]
        self._settle(p)
        self.closed.append(p)
        self.unbooked.append(p)
        mark = float(tick.bid if p.side is Side.BUY else tick.ask) if tick is not None else p.exit
        return _Fill(True, ticket, float(p.exit), mark, p.volume, "paper close")

    def _settle(self, p: SimTrade) -> None:
        pip = pip_size(p.symbol)
        s = p.side.sign
        before = self.market.reference_rate_uses
        p.gbp_per_pip = self.market.gbp_per_pip(p.symbol)
        comm = p.commission_gbp + self._commission_gbp(p.volume)
        p.reference_rate = p.reference_rate or self.market.reference_rate_uses > before
        gross_pips = (p.exit - p.entry) * s / pip
        per_trade_pip = p.gbp_per_pip * p.volume
        comm_pips = comm / per_trade_pip if per_trade_pip > 0 else 0.0
        mid_pips = (p.exit_mid - p.entry_mid) * s / pip
        price_cost = mid_pips - gross_pips                  # spread and slippage paid, entry and exit
        p.result = {"gross_pips": gross_pips, "commission_gbp": comm, "commission_pips": comm_pips,
                    "net_pips": gross_pips - comm_pips, "net_gbp": gross_pips * per_trade_pip - comm,
                    "mid_pips": mid_pips, "price_cost_pips": price_cost, "cost_pips": price_cost + comm_pips,
                    "gbp_per_pip_trade": per_trade_pip}

    def positions(self) -> list:
        return []

    def closed_deal(self, ticket: int):
        return None


# ----------------------------------------------------------------- sources --

@dataclass
class DayStats:
    day: str
    mbp_records: int = 0
    trade_records: int = 0
    mbp_trade_records: int = 0                 # trade records inside mbp-10 (not used again when trades exist)
    outside_window: int = 0
    other_contracts: int = 0                   # records of contracts this run does not test (bought for another)
    events: int = 0
    unknown_instruments: dict = field(default_factory=dict)
    not_found: list = field(default_factory=list)
    first: Optional[str] = None
    last: Optional[str] = None
    error: str = ""

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)


def event_batches(events: Iterable) -> Iterator[list]:
    """Internal events (a fixture or a recording), one batch each, in arrival order."""
    evs = sorted(enumerate(events), key=lambda x: (received_at(x[1]), x[0]))
    for _, ev in evs:
        yield [ev]


def instrument_map(store) -> dict[int, str]:
    """instrument_id -> raw symbol, from a DBN file's own symbology."""
    out: dict[int, str] = {}
    for raw, intervals in dict(getattr(store, "mappings", None) or {}).items():
        for iv in intervals or ():
            sym = _get(iv, "symbol")
            try:
                out[int(sym)] = str(raw)
            except (TypeError, ValueError):
                continue
    return out


def _ns(t: dt.datetime) -> int:
    """Nanoseconds since the epoch of an aware datetime (exact integer arithmetic)."""
    d = t - dt.datetime(1970, 1, 1, tzinfo=UTC)
    return (d.days * 86_400 + d.seconds) * 1_000_000_000 + d.microseconds * 1000


def day_files(day_dir: Path, schema: str) -> list[Path]:
    """Every complete downloaded piece of ``schema`` in a day's folder: ``<schema>.dbn.zst`` and, when more
    hours or contracts were bought for that day later, ``<schema>.<n>.dbn.zst`` (pieces never overlap)."""
    d = Path(day_dir)
    return sorted(f for f in d.glob(f"{schema}*.dbn.zst")
                  if f.name == f"{schema}.dbn.zst" or f.name[len(schema) + 1:-len(".dbn.zst")].isdigit())


def dbn_day_batches(day_dir: Path, start: dt.datetime, end: dt.datetime, stats: DayStats,
                    use_trades: bool = True, open_store: Optional[Callable] = None,
                    files: Optional[dict] = None, symbols: Optional[Iterable[str]] = None) -> Iterator[list]:
    """One day of downloaded Databento files as internal events, through the LIVE adapter's own
    :class:`~mintel.ian.feeds.databento.RecordMapper` - so a past record and a live one become exactly
    the same events, and the STREAM is the live one too: the mbp-10 and trades files are merged by capture
    time (``ts_recv``; at the same instant the trade first, as the exchange reports it) and, when the trades
    file is there, the trade inside an mbp-10 record is skipped exactly as the live feed skips it with both
    subscriptions (:func:`~mintel.ian.feeds.databento.book_trades_wanted`) - every trade counted once.

    ``files`` ({schema: [paths]}) names the pieces to read (default: :func:`day_files`); ``symbols`` keeps
    only those contracts' records (a day can also hold contracts bought for an earlier run)."""
    day_dir = Path(day_dir)
    if open_store is None:
        import databento as db
        open_store = db.DBNStore.from_file
    if files is None:
        files = {sch: day_files(day_dir, sch) for sch in ("mbp-10", "trades")}

    def usable(sch: str) -> list[Path]:
        return [Path(f) for f in (files.get(sch) or []) if Path(f).exists() and Path(f).stat().st_size > 0]

    mbp_paths = usable("mbp-10")
    if not mbp_paths:
        stats.error = "no mbp-10 file"
        return
    trd_paths = usable("trades") if use_trades else []
    mbps = [open_store(str(f)) for f in mbp_paths]
    trds = [open_store(str(f)) for f in trd_paths]
    ids: dict[int, str] = {}
    for store in mbps + trds:
        ids.update(instrument_map(store))
        md = getattr(store, "metadata", None)
        for sym in list(getattr(md, "not_found", None) or []):
            if str(sym) not in stats.not_found:
                stats.not_found.append(str(sym))
    want = {str(x) for x in symbols} if symbols is not None else None
    # one mapper for both schemas, as the live feed has: with a trades file, the book's copy is skipped
    mapper = RecordMapper(ids, book_trades=book_trades_wanted("mbp-10", bool(trds)))
    lo, hi = _ns(start), _ns(end)

    def records(store, rank: int, k: int):
        for i, rec in enumerate(store):
            ts = _get(rec, "ts_recv")
            if ts is None:
                ts = _get(rec, "ts_event", 0)
            yield int(ts or 0), rank, k, i, rec

    streams = [records(st, 1, k) for k, st in enumerate(mbps)]
    streams += [records(st, 0, len(mbps) + k) for k, st in enumerate(trds)]
    for ts, rank, _, _, rec in heapq.merge(*streams, key=lambda x: x[:4]):
        if ts < lo or ts >= hi:
            stats.outside_window += 1
            continue
        if want is not None:
            iid = _get(rec, "instrument_id")
            sym = ids.get(int(iid)) if iid is not None else None
            if sym is not None and sym not in want:
                stats.other_contracts += 1
                continue
        if rank == 1:
            stats.mbp_records += 1
            before = mapper.book_trades_skipped
            evs = mapper.map(rec)
            stats.mbp_trade_records += mapper.book_trades_skipped - before
        else:
            stats.trade_records += 1
            evs = mapper.map(rec)
        if not evs:
            continue
        for e in evs:
            inst = getattr(e, "instrument", "")
            if inst and not root_of(inst):
                stats.unknown_instruments[inst] = stats.unknown_instruments.get(inst, 0) + 1
        stats.events += len(evs)
        t = _iso(received_at(evs[-1]))
        stats.first = stats.first or t
        stats.last = t
        yield evs


# ------------------------------------------------------------------ driver --

@dataclass
class Window:
    start: dt.datetime
    end: dt.datetime
    day: str = ""
    contracts: dict = field(default_factory=dict)


@dataclass
class BacktestResult:
    label: str
    synthetic: bool
    windows: list
    trades: list
    signals: dict
    quality: dict
    events_in: int
    costs: dict
    ian: dict
    end_of_data_closes: int
    stale_exits: int
    reference_rate_uses: int
    rolls: list
    wall_seconds: float
    engine_notes: int


def _quality_state(text: str) -> str:
    s = str(text or "").split(":")[0].strip().upper()
    return s or "DOWN"


def _force_close(engine: IanEngine, broker: SpotBroker, executor: BacktestExecutor, when: dt.datetime,
                 why: str) -> int:
    """Close what is still open because the test data stops at ``when``: at the last price released by then
    (never a later quote - not the next window's, not the next contract's after a roll)."""
    n = 0
    market = executor.market
    market.horizon = when
    try:
        for sym, tr in list(engine.open.items()):
            tick = broker.tick(sym)
            p = executor.held.get(tr.ticket)
            if tick is None and p is not None:
                tick = Tick(sym, when, p.entry, p.entry)
            if tick is None:
                continue
            if p is not None and p.trigger is not None:
                engine._close(tr, tick, "STOP", when, level=tr.stop, why="the stop was reached")
            else:
                engine._close(tr, tick, "END_OF_DATA", when, why=why)
                n += 1
    finally:
        market.horizon = None
    return n


def _book_net(engine: IanEngine, executor: BacktestExecutor) -> None:
    """The money each closed trade made AFTER costs (spread and slippage are in its prices; commission
    here) goes into Ian's own records, as the broker's figure does in LIVE - so its daily loss limit
    counts what a real trade would have cost."""
    if not executor.unbooked:
        return
    j = engine.journal
    with j._lock:
        for p in executor.unbooked:
            j.db.execute("UPDATE trades SET net_pnl=? WHERE ticket=?", (round(p.result["net_gbp"], 2), p.ticket))
    executor.unbooked.clear()


def run_backtest(batches: Iterable[list], windows: Sequence[Window], ian_cfg: IanConfig, costs: CostModel,
                 work_dir: str | Path, *, synthetic: bool, sequence_scope: str = "channel",
                 calendar: Sequence[CalendarEvent] = (), progress: Optional[Callable[[dict], None]] = None,
                 progress_every_s: float = 20.0, wall: Callable[[], float] = time.monotonic) -> BacktestResult:
    """Replay ``batches`` (lists of internal events, in arrival order) through a fresh IanEngine."""
    if not windows:
        raise ValueError("no test windows")
    windows = sorted(windows, key=lambda w: w.start)
    work = Path(work_dir)
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True, exist_ok=True)
    label = SYNTHETIC_LABEL if synthetic else REAL_LABEL
    roots = [r for r in ian_cfg.instruments if r in CONTRACTS]
    market = Market(batches, roots, costs)
    broker = SpotBroker(market, calendar)
    feed = BacktestFeed(market, synthetic, sequence_scope, "fixture" if synthetic else "databento-history")
    executor = BacktestExecutor(market, costs)
    cfg = dataclasses.replace(ian_cfg, mode="PAPER", record=False, status_interval_s=1e12,
                              feed={"vendor": "backtest"}, allow_paper_on_synthetic=bool(synthetic))
    t0 = windows[0].start
    engine = IanEngine(work, cfg, broker=broker, feed=feed, clock=lambda: market.now or t0, executor=executor,
                       spec_fn=market.spec, wall=lambda: market.now or t0)
    step = dt.timedelta(seconds=cfg.decision_interval_s)
    slow = dt.timedelta(seconds=30)
    total = sum((w.end - w.start).total_seconds() for w in windows) or 1.0
    q_feed: dict[str, float] = {}
    q_root: dict[str, dict[str, float]] = {}
    state = {"wi": 0, "prev": None, "prev_feed": "", "prev_roots": {}, "eod": 0, "notes": 0}
    started = wall()
    last_report = started

    def account(t: dt.datetime) -> None:
        """Time from the previous pass to this one, inside the test windows, by the data's state then."""
        prev = state["prev"]
        if prev is None:
            return
        for w in windows:
            a, b = max(prev, w.start), min(t, w.end)
            if b > a:
                secs = (b - a).total_seconds()
                q_feed[state["prev_feed"]] = q_feed.get(state["prev_feed"], 0.0) + secs
                for r, s in state["prev_roots"].items():
                    d = q_root.setdefault(r, {})
                    d[s] = d.get(s, 0.0) + secs

    def do_step(t: dt.datetime) -> None:
        while state["wi"] < len(windows) and t >= windows[state["wi"]].end:
            w = windows[state["wi"]]
            nxt_w = windows[state["wi"] + 1] if state["wi"] + 1 < len(windows) else None
            if nxt_w is None or nxt_w.start > w.end or nxt_w.contracts != w.contracts:
                market.advance(w.end)
                state["eod"] += _force_close(engine, broker, executor, w.end,
                                             f"the test data ends at {w.end:%H:%M} UTC on {w.end:%a %d %b}, so "
                                             "the trade was closed at the last price")
                _book_net(engine, executor)
            state["wi"] += 1
        market.advance(t)
        account(t)
        # Ian decides only INSIDE a test window: after one ends (its trades closed above) there is no data,
        # only the last quotes frozen - a decision then would open a trade on a stale price and hold it
        # through the gap. The engine waits for the next window instead.
        if state["wi"] < len(windows) and windows[state["wi"]].start <= t:
            state["notes"] += len(engine.step(t))
            _book_net(engine, executor)
        state["prev"], state["prev_feed"] = t, engine.feed_state
        state["prev_roots"] = {r: _quality_state(x) for r, x in dict(getattr(engine, "quality_text", {})).items()
                               if r in roots}

    nxt: Optional[dt.datetime] = None
    n = 0
    while True:
        ev = market.next_event()
        if ev is None:
            break
        t = received_at(ev)
        if nxt is None:
            nxt = t
        gaps = 0
        while t >= nxt:
            do_step(nxt)
            gaps += 1
            nxt = nxt + (step if gaps < 240 else slow)
        engine.ingest([ev])
        n += 1
        if progress is not None and n % 20000 == 0:
            w_now = wall()
            if w_now - last_report >= progress_every_s:
                last_report = w_now
                done = sum(max(0.0, (min(t, w.end) - w.start).total_seconds()) for w in windows if t > w.start)
                frac = max(0.0, min(1.0, done / total))
                el = w_now - started
                progress({"time": t, "fraction": frac, "events": engine.events_in, "trades": len(executor.closed),
                          "open": len(executor.held), "elapsed_s": el,
                          "eta_s": (el / frac - el) if frac > 0.01 else None})
    feed.finished = True
    if nxt is not None:
        do_step(nxt)
    end = max(market.now or windows[-1].end, t0)
    if engine.open:
        market.advance(end)
        state["eod"] += _force_close(engine, broker, executor, end,
                                     "the downloaded data ends here, so the trade was closed at the last price")
        _book_net(engine, executor)
    trades = _trades(engine, executor, label)
    signals = _signals(engine)
    events_in = engine.events_in
    try:
        engine.write_status(end, force=True)
    except Exception:
        pass
    engine.close()
    quality = {"feed_seconds": {k: round(v, 1) for k, v in sorted(q_feed.items())},
               "by_future_seconds": {r: {k: round(v, 1) for k, v in sorted(d.items())} for r, d in sorted(q_root.items())},
               "window_seconds": round(total, 1)}
    ian = {k: getattr(cfg, k) for k in ("risk_money", "max_risk_money", "max_lots", "max_open", "max_entries_per_day",
                                         "cooldown_s", "max_daily_loss_money", "entry_score", "min_groups",
                                         "hold_max_minutes", "decision_interval_s", "max_spread_pips",
                                         "min_stop_pips", "max_stop_pips", "roll_days")}
    ian["instruments"] = list(roots)
    return BacktestResult(label, bool(synthetic), [{"day": w.day, "start": _iso(w.start), "end": _iso(w.end),
                                                     "contracts": dict(w.contracts)} for w in windows],
                          trades, signals, quality, events_in, costs.to_dict(), ian, state["eod"],
                          sum(1 for p in executor.closed if p.stale_exit),
                          sum(1 for p in executor.closed if p.reference_rate),
                          [(_iso(a), b, c, d) for a, b, c, d in market.rolls], round(wall() - started, 1),
                          state["notes"])


def _trades(engine: IanEngine, executor: BacktestExecutor, label: str) -> list[dict]:
    sims = {p.ticket: p for p in executor.closed}
    rows = [dict(r) for r in engine.journal.db.execute("SELECT * FROM trades WHERE closed_utc IS NOT NULL "
                                                         "ORDER BY opened_utc, ticket")]
    out = []
    for r in rows:
        p = sims.get(int(r["ticket"]))
        if p is None:
            continue
        res = p.result
        reason = str(r.get("exit_reason") or p.exit_kind)
        if p.exit_kind == "STOP" and reason not in ("STOP",):
            reason = "STOP"
        # the engine's own sentence quotes its figure before commission: say it again with the real net
        why = str(r.get("exit_explanation") or "")
        why = why.split("): ", 1)[1].rstrip(".") if "): " in why else ""
        mfe = r.get("mfe_r")
        # R AFTER every cost: Ian's journal R (fill to fill over the distance from the entry to the initial
        # stop) has the spread and slippage in it but not the commission; this one has all of it
        try:
            risk_pips = abs(float(r["entry"]) - float(r["initial_stop"])) / pip_size(p.symbol)
        except (KeyError, TypeError, ValueError):
            risk_pips = 0.0
        net_r = res["net_pips"] / risk_pips if risk_pips > 0 else None
        expl = (f"Closed {reason} at {round(float(p.exit), 6)} for {res['net_gbp']:+.2f} GBP after costs "
                f"({res['net_pips']:+.1f} pips" + (f"; best {float(mfe):+.2f} R" if mfe is not None else "") + ")"
                + (f": {why}." if why else "."))
        out.append({
            "data_label": label, "ticket": p.ticket, "signal_id": r.get("signal_id"), "future": p.root,
            "contract": r.get("instrument"), "pair": p.symbol, "side": p.side.value,
            "decided_utc": _iso(p.decided), "filled_utc": _iso(p.filled), "closed_utc": _iso(p.exit_t),
            "minutes": round((p.exit_t - p.filled).total_seconds() / 60.0, 2) if p.exit_t else None,
            "day": p.filled.date().isoformat(), "hour_utc": p.filled.hour, "regime": r.get("regime") or "",
            "score": r.get("score"), "volume": p.volume,
            "risk_gbp": round(float(r["risk_money"]), 2) if r.get("risk_money") is not None else None,
            "entry": round(p.entry, 6), "initial_stop": r.get("initial_stop"), "final_stop": round(p.sl, 6),
            "exit": round(float(p.exit), 6), "exit_reason": reason,
            "mid_pips": round(res["mid_pips"], 6), "cost_pips": round(res["cost_pips"], 6),
            "spread_slippage_pips": round(res["price_cost_pips"], 6),
            "commission_pips": round(res["commission_pips"], 6), "gross_pips": round(res["gross_pips"], 6),
            "net_pips": round(res["net_pips"], 6), "commission_gbp": round(res["commission_gbp"], 6),
            "net_gbp": round(res["net_gbp"], 6), "gbp_per_pip": round(res["gbp_per_pip_trade"], 8),
            "realised_r": r.get("realised_r"), "net_r": round(net_r, 4) if net_r is not None else None,
            "mfe_r": r.get("mfe_r"), "mae_r": r.get("mae_r"),
            "capture": r.get("capture"), "stale_exit": p.stale_exit, "exit_explanation": expl,
        })
    return out


REFUSAL_KINDS = (("quote is", "old spot quote"), ("spread", "spread too wide"), ("does not confirm", "spot did not confirm"),
                 ("already run", "spot had already run"), ("minimum distance", "stop inside the minimum distance"),
                 ("not on the losing side", "stop on the wrong side"), ("smallest size", "smallest size risks too much"),
                 ("no valid quote", "no spot quote"), ("cannot value", "cannot value the stop"))


def _signals(engine: IanEngine) -> dict:
    rows = [dict(r) for r in engine.journal.db.execute("SELECT status, message, root FROM signals")]
    by_status: dict[str, int] = {}
    refused: dict[str, int] = {}
    by_future: dict[str, int] = {}
    for r in rows:
        st = str(r.get("status") or "NEW")
        by_status[st] = by_status.get(st, 0) + 1
        by_future[str(r.get("root") or "")] = by_future.get(str(r.get("root") or ""), 0) + 1
        if st == "REFUSED":
            msg = str(r.get("message") or "").lower()
            kind = next((k for key, k in REFUSAL_KINDS if key in msg), "other")
            refused[kind] = refused.get(kind, 0) + 1
    return {"total": len(rows), "by_status": dict(sorted(by_status.items())),
            "refused_by_reason": dict(sorted(refused.items(), key=lambda x: -x[1])),
            "by_future": dict(sorted(by_future.items()))}


# ---------------------------------------------------------------- summary --

def _r(x, n=2):
    return round(float(x), n) if x is not None else None


def trade_stats(trades: Sequence[dict], days: float) -> dict:
    n = len(trades)
    money = [float(t["net_gbp"]) for t in trades]
    pips = [float(t["net_pips"]) for t in trades]
    wins = [i for i, m in enumerate(money) if m > 0]
    losses = [i for i, m in enumerate(money) if m < 0]
    eq = peak = dd = 0.0
    for t in sorted(trades, key=lambda t: str(t.get("closed_utc") or "")):
        eq += float(t["net_gbp"])
        peak = max(peak, eq)
        dd = min(dd, eq - peak)
    win_m = sum(money[i] for i in wins)
    loss_m = -sum(money[i] for i in losses)
    # R after EVERY cost (spread, slippage and commission), on the same basis as the pounds beside it - never
    # Ian's journal R, which leaves the commission out and can show a losing edge as a positive R
    rr = [float(t["net_r"]) for t in trades if t.get("net_r") is not None]
    pos_mfe = sum(float(t["mfe_r"]) for t in trades
                  if t.get("net_r") is not None and t.get("mfe_r") is not None and float(t["mfe_r"]) > 0)
    avg = lambda xs: (sum(xs) / len(xs)) if xs else None
    return {
        "trades": n, "trades_per_day": _r(n / days, 2) if days > 0 else None,
        "wins": len(wins), "losses": len(losses), "win_rate": _r(len(wins) / n, 3) if n else None,
        "net_pips": _r(sum(pips)), "net_gbp": _r(sum(money)),
        "gross_pips": _r(sum(float(t["gross_pips"]) for t in trades)),
        "costs_pips": _r(sum(float(t["cost_pips"]) for t in trades)),
        "profit_factor": (_r(win_m / loss_m) if loss_m > 0 else None),
        "avg_win_gbp": _r(avg([money[i] for i in wins])), "avg_loss_gbp": _r(avg([money[i] for i in losses])),
        "avg_win_pips": _r(avg([pips[i] for i in wins])), "avg_loss_pips": _r(avg([pips[i] for i in losses])),
        "expectancy_gbp": _r(avg(money)), "expectancy_r": _r(avg(rr), 3),
        "max_drawdown_gbp": _r(dd),
        "mfe_capture": _r(sum(rr) / pos_mfe, 3) if pos_mfe > 0 and rr else None,
        "r_basis": "expectancy_r and mfe_capture are after every cost (spread, slippage and commission); "
                   "1 R = the distance from the entry fill to the initial stop",
    }


def stressed(trades: Sequence[dict], mult: float) -> dict:
    """The same trades with every cost (spread, slippage, commission) multiplied by ``mult``."""
    pips = money = 0.0
    for t in trades:
        p = float(t["mid_pips"]) - mult * float(t["cost_pips"])
        pips += p
        money += p * float(t["gbp_per_pip"])
    return {"cost_multiple": mult, "net_pips": _r(pips), "net_gbp": _r(money)}


def group(trades: Sequence[dict], key: str) -> dict:
    out: dict = {}
    for t in trades:
        k = str(t.get(key))
        d = out.setdefault(k, {"trades": 0, "wins": 0, "net_pips": 0.0, "net_gbp": 0.0})
        d["trades"] += 1
        d["wins"] += 1 if float(t["net_gbp"]) > 0 else 0
        d["net_pips"] += float(t["net_pips"])
        d["net_gbp"] += float(t["net_gbp"])
    for d in out.values():
        d["win_rate"] = _r(d["wins"] / d["trades"], 3) if d["trades"] else None
        d["net_pips"], d["net_gbp"] = _r(d["net_pips"]), _r(d["net_gbp"])
    if key == "hour_utc":
        return dict(sorted(out.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 99))
    return dict(sorted(out.items()))


def verdict(n_trades: int, net_gbp: float, net_gbp_15x: float, contributing: int,
            min_trades: int = MIN_TRADES) -> tuple[str, list[str]]:
    """INSUFFICIENT DATA (< min_trades), NO EDGE (not positive after costs), FRAGILE (1.5x costs turn it
    into a loss, or one instrument makes all the profit), else PROMISING - WORTH A LIVE FEED TRIAL."""
    if n_trades < min_trades:
        return INSUFFICIENT, [f"only {n_trades} trade{'' if n_trades == 1 else 's'}: at least {min_trades} are "
                              "needed before the result means anything"]
    if net_gbp <= 0:
        return NO_EDGE, [f"after costs the {n_trades} trades lost GBP {-net_gbp:.2f}" if net_gbp < 0
                         else f"after costs the {n_trades} trades made nothing"]
    reasons = []
    if net_gbp_15x <= 0:
        reasons.append(f"it made GBP {net_gbp:.2f} after costs, but with costs 1.5 times higher it would have "
                       f"{'lost' if net_gbp_15x < 0 else 'made'} GBP {abs(net_gbp_15x):.2f}: the edge is thinner than "
                       "the costs")
    if contributing < 2:
        reasons.append("the profit comes from one instrument only" if contributing == 1
                       else "no single instrument made money on its own")
    if reasons:
        return FRAGILE, reasons
    return PROMISING, [f"GBP {net_gbp:.2f} after costs over {n_trades} trades, still positive with costs 1.5 times "
                       f"higher (GBP {net_gbp_15x:.2f}), and {contributing} instruments made money"]


def caveats(result: BacktestResult, meta: Optional[dict] = None) -> list[str]:
    """What this test cannot show, in plain English - every simplification it makes."""
    meta = dict(meta or {})
    out = []
    if result.synthetic:
        out.append(f"{SYNTHETIC_LABEL}: generated test events, not a market. Nothing here is a result.")
    out += [
        "Spot prices are the futures prices mapped to the pair with Ian's own mapping (1/x for JPY, CAD and CHF), "
        "without the forward points: distances in pips match the real pair to within about 1%, but the price level "
        "differs from MetaTrader's by the forward points.",
        "Spot spreads are typical IC Markets Raw figures (see Costs), not measured on this account; they are widened "
        "when the futures spread widens, but real spreads around news can be wider still.",
        "The chart context (live: MetaTrader's spot bars) is built from the same mapped futures prices. After each "
        "overnight gap the 1-minute chart starts again, so for about 21 minutes each morning Ian had no 1-minute "
        "chart, which live it would have.",
        "The cost stress multiplies the costs of the SAME trades; with really wider spreads some stops would also be "
        "hit sooner, which it does not model.",
        f"Orders fill a fixed {result.costs.get('latency_ms', 0):g} ms after the decision; live delays vary.",
        "Ian's own learning (its score adjusts after 20 closed trades of the same kind) works as it does live, from "
        "trades already closed in this replay only.",
        "ES, GC and ZN (the context markets live Ian may watch) were not replayed; Ian's score does not use them "
        "(its cross-market evidence comes from the other FX futures), so no decision is lost.",
    ]
    cal = meta.get("calendar") or {}
    days = [w.get("day") for w in result.windows if w.get("day")]
    covered = [d for d in days if d in set(cal.get("covered_days") or [])]
    if not cal.get("available") or not cal.get("events"):
        out.append("No economic calendar was available for these days, so Ian's news rules (no entry just before or "
                   "just after a high-impact release, and the surprise as evidence) could not act: the test may "
                   "include trades live Ian would have skipped.")
    elif days and len(covered) < len(days):
        out.append(f"The bot's own calendar record covers {len(covered)} of the {len(days)} test days; on the others "
                   "Ian's news rules could not act.")
    if result.end_of_data_closes:
        out.append(f"{result.end_of_data_closes} trade(s) were still open when the test data stopped (end of a test "
                   "window) and were closed at the last price; live, Ian would have kept managing them.")
    if result.stale_exits:
        out.append(f"{result.stale_exits} exit(s) were priced on a quote more than {STALE_FILL_S:g} s old (no newer "
                   "futures price existed).")
    if result.reference_rate_uses:
        out.append(f"{result.reference_rate_uses} trade(s) had their pips turned into pounds with a fixed reference "
                   "rate (GBPUSD 1.27, USDJPY 150, USDCAD 1.37, USDCHF 0.89) because the replay had no price of its own "
                   "for that rate yet (6B, 6J, 6C or 6S).")
    n_days = len(set(days)) or len(result.windows)
    out.append(f"{n_days} day(s) is a small sample: a good result here is a reason for a longer test or a PAPER trial "
               "on the live feed, not proof of an edge.")
    return out + list(meta.get("extra_caveats") or [])


def summarise(result: BacktestResult, meta: Optional[dict] = None) -> dict:
    meta = dict(meta or {})
    meta["caveats"] = caveats(result, meta)
    meta.pop("extra_caveats", None)
    trades = result.trades
    days = len({w["day"] for w in result.windows if w.get("day")}) or len(result.windows)
    st = trade_stats(trades, days)
    stress = [stressed(trades, m) for m in STRESS]
    by_inst = group(trades, "future")
    contributing = sum(1 for d in by_inst.values() if d["net_gbp"] > 0)
    v, why = verdict(len(trades), st["net_gbp"] or 0.0, stress[1]["net_gbp"] or 0.0, contributing)
    if result.synthetic:
        why = [f"pipeline check only: on this synthetic data the rules would say {v}, which means nothing about "
               "the real market"]
        v = SYNTHETIC_LABEL
    q = result.quality
    feed_s = q.get("feed_seconds", {})
    tot = sum(feed_s.values()) or 0.0
    return {
        "label": result.label, "synthetic": result.synthetic,
        "source": meta.get("source") or ("synthetic fixture events" if result.synthetic else SOURCE_DATABENTO),
        "generated_utc": meta.get("generated_utc"), "period": meta.get("period") or {},
        "hours": meta.get("hours"), "windows": result.windows, "days": days,
        "out_of_sample": ("Ian's parameters were never tuned on this data, so the whole period is out-of-sample; "
                          "this tool never tunes anything"),
        "verdict": v, "verdict_reasons": why, "stats": st, "cost_stress": stress,
        "fragile_rule": "FRAGILE when costs 1.5 times higher turn a profit into a loss",
        "by_instrument": by_inst, "contributing_instruments": contributing,
        "by_regime": group(trades, "regime"), "by_hour_utc": group(trades, "hour_utc"),
        "by_exit": group(trades, "exit_reason"), "signals": result.signals,
        "events_replayed": result.events_in,
        "data_quality": {"feed_seconds": feed_s,
                         "feed_share": {k: _r(v2 / tot, 3) for k, v2 in feed_s.items()} if tot else {},
                         "by_future_seconds": q.get("by_future_seconds", {}), "window_seconds": q.get("window_seconds")},
        "costs": result.costs, "ian_settings": result.ian,
        "end_of_data_closes": result.end_of_data_closes, "stale_exits": result.stale_exits,
        "reference_rate_uses": result.reference_rate_uses, "contract_rolls": result.rolls,
        "replay_wall_seconds": result.wall_seconds,
        **{k: v2 for k, v2 in meta.items() if k not in ("source", "generated_utc", "period", "hours")},
    }


# ------------------------------------------------------------------ report --

TRADE_COLUMNS = ("data_label", "ticket", "signal_id", "future", "contract", "pair", "side", "decided_utc", "filled_utc",
                 "closed_utc", "minutes", "day", "hour_utc", "regime", "score", "volume", "risk_gbp", "entry",
                 "initial_stop", "final_stop", "exit", "exit_reason", "mid_pips", "spread_slippage_pips",
                 "commission_pips", "cost_pips", "gross_pips", "net_pips", "commission_gbp", "net_gbp", "gbp_per_pip",
                 "realised_r", "net_r", "mfe_r", "mae_r", "capture", "stale_exit", "exit_explanation")


def trades_csv(trades: Sequence[dict]) -> str:
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(TRADE_COLUMNS)
    for t in trades:
        w.writerow([t.get(c) for c in TRADE_COLUMNS])
    return out.getvalue()


def _money(x) -> str:
    if x is None:
        return "-"
    return f"{'-' if x < 0 else ''}GBP {abs(x):,.2f}"


def _num(x, fmt="{:,.1f}") -> str:
    return "-" if x is None else fmt.format(x)


def _hours(secs: float) -> str:
    return f"{secs / 3600.0:,.1f} h" if secs >= 3600 else f"{secs / 60.0:,.1f} min"


def report_md(s: dict) -> str:
    syn = s["synthetic"]
    st = s["stats"]
    L: list[str] = []
    title = "Financial Ian - test on past CME data"
    if syn:
        title += f" ({SYNTHETIC_LABEL})"
    L += [f"# {title}", ""]
    if syn:
        L += [f"**{SYNTHETIC_LABEL}.** These numbers come from generated test events, not from any market. "
              "They show that the pipeline runs end to end; they say nothing about whether Financial Ian works.", ""]
    else:
        L += [f"**{REAL_LABEL}.** Source: {s['source']}. Every number below comes from replaying those files "
              "through Financial Ian's own decision code; nothing is estimated or invented.", ""]
    p = s.get("period") or {}
    L += [f"## Verdict: {s['verdict']}", ""]
    L += [f"- {r[0].upper() + r[1:]}." for r in s["verdict_reasons"]]
    L += ["", f"- {s['out_of_sample']}.", ""]
    L += ["## What was tested", ""]
    if p:
        hours = s.get("hours_text") or s.get("hours") or ""
        L.append(f"- **Period:** {p.get('first')} to {p.get('last')} ({s['days']} weekday{'s' if s['days'] != 1 else ''})"
                 + (f", {hours}" if hours else ""))
    contracts = sorted({c for w in s["windows"] for c in (w.get("contracts") or {}).values()})
    if contracts:
        L.append(f"- **Contracts:** {', '.join(contracts)} - the front month each day by Ian's own roll rule "
                 "(8 days before the last trading day)")
    L.append(f"- **Events replayed:** {s['events_replayed']:,} order-book changes and trades"
             + (f" from {s['records']:,} Databento records" if s.get("records") else ""))
    ian = s.get("ian_settings") or {}
    L.append(f"- **Ian's settings (from ian.json):** GBP {ian.get('risk_money')} at the stop a trade (most "
             f"{ian.get('max_lots')} lots), entry score {ian.get('entry_score')}, at most {ian.get('max_open')} open, "
             f"{ian.get('max_entries_per_day')} entries a day per future, own daily loss limit GBP "
             f"{ian.get('max_daily_loss_money')}, a decision every {ian.get('decision_interval_s')} s")
    c = s.get("costs") or {}
    sp = ", ".join(f"{k} {v:g}" for k, v in sorted((c.get("spread_pips") or {}).items()))
    L.append(f"- **Costs:** spot spread in pips ({sp}){', wider when the futures spread widens' if c.get('spread_follows_futures') else ''}; "
             f"slippage {c.get('slippage_pips')} pip a fill; commission USD {c.get('commission_usd_per_lot_side')} "
             f"a lot a side; orders filled {c.get('latency_ms'):g} ms after the decision; stops filled at the stop "
             "or worse")
    L.append("- **No MetaTrader, no orders:** a simulated executor. Ian's real engine decides everything: the "
             "book, the features, the regime, the score, the bridge's checks, the trail and the data-quality gate.")
    L += ["", "## Results after costs", "", "| | |", "|---|---|"]
    rows = [("Trades", f"{st['trades']} ({_num(st['trades_per_day'], '{:.1f}')} a day)"),
            ("Win rate", "-" if st["win_rate"] is None else f"{st['win_rate']:.0%}"),
            ("Net", f"{_num(st['net_pips'])} pips, {_money(st['net_gbp'])}"),
            ("Before costs (mid to mid)", f"{_num((st['net_pips'] or 0) + (st['costs_pips'] or 0))} pips"),
            ("Costs paid", f"{_num(st['costs_pips'])} pips"),
            ("Profit factor", _num(st["profit_factor"], "{:.2f}") if st["profit_factor"] is not None
             else ("no losing trade" if st["wins"] else "-")),
            ("Average win", f"{_money(st['avg_win_gbp'])} ({_num(st['avg_win_pips'])} pips)"),
            ("Average loss", f"{_money(st['avg_loss_gbp'])} ({_num(st['avg_loss_pips'])} pips)"),
            ("Maximum drawdown", _money(st["max_drawdown_gbp"])),
            ("MFE capture (after costs)", "-" if st["mfe_capture"] is None
             else f"{st['mfe_capture']:.0%} of the best open profit kept, after every cost"),
            ("Expectancy (after costs)", f"{_money(st['expectancy_gbp'])} a trade "
                                         f"({_num(st['expectancy_r'], '{:+.3f}')} R after every cost)")]
    L += [f"| {a} | {b} |" for a, b in rows]
    L += ["", "## Cost stress (the same trades, every cost multiplied)", "", "| Costs | Net pips | Net |", "|---|---|---|"]
    L += [f"| {x['cost_multiple']:g}x | {_num(x['net_pips'])} | {_money(x['net_gbp'])} |" for x in s["cost_stress"]]
    L += ["", f"{s['fragile_rule']}.", ""]
    dq = s["data_quality"]
    L += ["## Data quality (inside the test hours)", ""]
    fs = dq.get("feed_seconds") or {}
    if fs:
        tot = sum(fs.values())
        L.append("- Feed as a whole: " + ", ".join(f"{k} {_hours(v)} ({v / tot:.0%})" for k, v in fs.items()) +
                 ". Ian enters only while the whole feed is LIVE.")
    for r, d in (dq.get("by_future_seconds") or {}).items():
        tot = sum(d.values()) or 1.0
        L.append(f"- {r}: " + ", ".join(f"{k} {v / tot:.0%}" for k, v in d.items()))
    sg = s["signals"]
    L += ["", "## Signals", "", f"- {sg['total']} signals; by what happened: "
          + (", ".join(f"{k} {v}" for k, v in sg["by_status"].items()) or "none")]
    if sg.get("refused_by_reason"):
        L.append("- Refused by the MT5 bridge's checks: " + ", ".join(f"{k} {v}" for k, v in sg["refused_by_reason"].items()))
    for name, key in (("By instrument", "by_instrument"), ("By regime", "by_regime"), ("By hour (UTC, entry)", "by_hour_utc"),
                      ("By exit", "by_exit")):
        g = s.get(key) or {}
        L += ["", f"## {name}", ""]
        if not g:
            L.append("No trades.")
            continue
        L += ["| | Trades | Win rate | Net pips | Net |", "|---|---|---|---|---|"]
        L += [f"| {k} | {d['trades']} | {'-' if d['win_rate'] is None else format(d['win_rate'], '.0%')} | "
              f"{_num(d['net_pips'])} | {_money(d['net_gbp'])} |" for k, d in g.items()]
    L += ["", "## What this test cannot show", ""]
    L += [f"- {x}" for x in s.get("caveats") or []]
    L += ["", "## Files", "",
          "- `trades.csv` - every trade, its prices, its costs and how it ended (`net_r` is after every cost; "
          "`realised_r`, `mfe_r`, `mae_r` and `capture` are Ian's own figures, before commission)",
          "- `summary.json` - every number on this page"]
    if s.get("data_files"):
        L.append(f"- The downloaded data stays on this computer: `{s['data_files']}`")
    L.append("")
    return "\n".join(L)


def write_report(out_dir: str | Path, summary: dict, trades: Sequence[dict]) -> dict[str, Path]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    files = {"report.md": report_md(summary), "summary.json": json.dumps(summary, indent=2, default=str),
             "trades.csv": trades_csv(trades)}
    paths = {}
    for name, text in files.items():
        p = out / name
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(p)
        paths[name] = p
    return paths


# ---------------------------------------------------------------- calendar --

def load_calendar(path: str | Path, start: dt.datetime, end: dt.datetime) -> tuple[list[CalendarEvent], dict]:
    """The bot's own calendar record, READ-ONLY (the running bot writes to it). Returns the events in the
    period (padded a day either side) and which test days it covers."""
    p = Path(path)
    if not p.exists():
        return [], {"path": str(p), "available": False, "covered_days": []}
    out: list[CalendarEvent] = []
    covered: list[str] = []
    try:
        con = sqlite3.connect(f"file:{p}?mode=ro", uri=True, timeout=5.0)
        try:
            con.row_factory = sqlite3.Row
            lo, hi = (start - dt.timedelta(days=1)).isoformat(), (end + dt.timedelta(days=1)).isoformat()
            for r in con.execute("SELECT * FROM events WHERE time_utc >= ? AND time_utc <= ? ORDER BY time_utc",
                                 (lo, hi)):
                try:
                    t = dt.datetime.fromisoformat(str(r["time_utc"]))
                except ValueError:
                    continue
                t = t if t.tzinfo is not None else t.replace(tzinfo=UTC)
                f = lambda k: (float(r[k]) if r[k] is not None else None)
                out.append(CalendarEvent(str(r["event_id"]), t.astimezone(UTC), str(r["currency"]), str(r["country"] or ""),
                                         str(r["name"] or ""), int(r["importance"] or 0), f("actual"), f("forecast"),
                                         f("previous"), f("revised_previous"), str(r["unit"] or ""), str(r["source"] or "")))
            try:
                covered = sorted({str(r[0]) for r in con.execute("SELECT day FROM coverage")})
            except sqlite3.Error:
                covered = []
        finally:
            con.close()
    except sqlite3.Error as exc:
        return [], {"path": str(p), "available": False, "error": str(exc), "covered_days": []}
    return out, {"path": str(p), "available": True, "events": len(out), "covered_days": covered}
