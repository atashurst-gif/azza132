"""Deterministic synthetic order-book scenarios - TEST DATA, NEVER PERFORMANCE.

A small book simulator that writes the normalised events of
:mod:`mintel.ian.book` for named market situations, so every feature,
regime and safety check can be tested against a situation that is known to
contain (or not to contain) the thing it looks for. Everything is seeded:
the same scenario and seed give byte-for-byte the same events.

It is not a model of the real CME book and nothing measured on it is a
result. Every output built from it is labelled SYNTHETIC - NOT PERFORMANCE.

Each scenario starts with a balanced warm-up (so the features have a
baseline to compare against) and then plays its situation:

    balanced            two-sided, nothing special
    sweep_up/down       one aggressive order takes several levels at once
    offer_pulling       offers ahead of price cancelled (no trades), price lifts
    bid_pulling         the mirror image
    wall_absorbed_consumed  a large offer wall is hit, refreshes, then is taken
    absorption_buyers   heavy buying into an offer that keeps refilling; price stuck
    absorption_sellers  the mirror image
    refresh_iceberg     a small visible bid that keeps refilling as it is hit
    liquidity_vacuum    depth vanishes on both sides, the spread opens
    news_shock          a release: depth pulled, spread out, a burst, then recovery
    trend_up/down       persistent one-sided aggression that moves price
    breakout            balanced, then a sweep and follow-through
    reversal            a trend up, absorption at the top, then selling
    high_volatility     large two-sided prints swinging the price
    divergence_bearish  price lifted by passive bids while sellers dominate the tape (and the mirror)
    sequence_gap        balanced with a jump in the sequence numbers
    stale_feed          balanced, 30 seconds of silence, balanced again
    out_of_order        balanced with late, out-of-order events
"""
from __future__ import annotations

import datetime as dt
import math
import random
from dataclasses import dataclass
from typing import Optional, Sequence

from ...broker.base import CalendarEvent
from .. import INSTITUTIONAL
from ..book import ADD, ASK, BID, BUY, CANCEL, MODIFY, SELL, BookEvent, Event, TradeEvent, px
from ..mapping import contract, root_of
from .base import FINISHED, LIVE, FeedAdapter, FeedHealth

UTC = dt.timezone.utc
DEFAULT_START = dt.datetime(2026, 10, 7, 12, 0, tzinfo=UTC)      # a Wednesday, London/New York overlap
STEP = 0.05                                                       # seconds of book time per generator step

SCENARIOS = ("balanced", "sweep_up", "sweep_down", "offer_pulling", "bid_pulling", "wall_absorbed_consumed",
             "absorption_buyers", "absorption_sellers", "refresh_iceberg", "liquidity_vacuum", "news_shock",
             "trend_up", "trend_down", "breakout", "reversal", "high_volatility", "divergence_bearish",
             "divergence_bullish", "sequence_gap", "stale_feed", "out_of_order")

BASE_PRICES = {"6E": 1.0850, "6B": 1.2700, "6A": 0.6550, "6N": 0.6050, "6C": 0.7350, "6J": 0.0066650,
               "6S": 1.1235, "ES": 5200.0, "GC": 2320.0, "ZN": 110.0}


@dataclass
class Params:
    buy_rate: float = 1.5           # aggressive buy orders per second
    sell_rate: float = 1.5
    trade_size: float = 3.0         # mean aggressive order size (contracts)
    add_rate: float = 6.0           # passive adds per second per side, near the touch
    cancel_rate: float = 6.0
    add_bid: Optional[float] = None
    add_ask: Optional[float] = None
    cancel_bid: Optional[float] = None
    cancel_ask: Optional[float] = None
    pull_ask: float = 0.0           # large cancellations of offers ahead (per second)
    pull_bid: float = 0.0
    depth_bid: float = 1.0          # size of newly posted levels relative to normal
    depth_ask: float = 1.0
    spread_fill: float = 0.8        # chance per step a wide spread is narrowed by a new level
    bias: float = 0.0               # -1..1: which side narrows a wide spread (+ = bids step up)
    absorb_ask: Optional[float] = None   # the best offer refills to this size as it is hit
    absorb_bid: Optional[float] = None
    max_spread: int = 1             # makers keep the spread at most this many ticks (when > 1, they back off)


class _Gen:
    def __init__(self, instrument: str, tick: float, base_price: float, seed: int, start: dt.datetime,
                 latency_ms: float, levels: int = 10, base_size: float = 40.0):
        self.instrument = instrument
        self.tick = tick
        self.rng = random.Random(seed)
        self.t = start
        self.latency = dt.timedelta(milliseconds=latency_ms)
        self.last_recv = start
        self.seq = 0
        self.levels = levels
        self.base = base_size
        self.out: list[Event] = []
        k0 = int(round(base_price / tick))
        self.bids: dict[int, float] = {}
        self.asks: dict[int, float] = {}
        for i in range(levels):
            self._set(BID, k0 - i, self._size(1.0), cause="snapshot")
            self._set(ASK, k0 + 1 + i, self._size(1.0), cause="snapshot")
        self.p = Params()
        self.marks: dict[str, dt.datetime] = {}

    # ---------------------------------------------------------- primitives --
    def price(self, k: int) -> float:
        return px(k * self.tick)

    def _size(self, mult: float) -> float:
        return float(max(1, int(round(self.base * mult * self.rng.uniform(0.6, 1.4)))))

    def _recv(self) -> dt.datetime:
        r = self.t + self.latency + dt.timedelta(microseconds=self.rng.randint(0, 900))
        if r <= self.last_recv:
            r = self.last_recv + dt.timedelta(microseconds=1)
        self.last_recv = r
        return r

    def _next_seq(self) -> int:
        self.seq += 1
        return self.seq

    def _set(self, side: str, k: int, size: float, cause: str = "") -> None:
        book = self.bids if side == BID else self.asks
        old = book.get(k, 0.0)
        size = max(0.0, float(int(size)))
        if size == old:
            return
        if size <= 0:
            book.pop(k, None)
            action = CANCEL
        elif old <= 0:
            book[k] = size
            action = ADD
        else:
            book[k] = size
            action = MODIFY
        self.out.append(BookEvent(self.instrument, self.t, self._recv(), self._next_seq(), side, self.price(k), size,
                                  action, cause=cause, source="synthetic"))

    def _trade_ev(self, aggressor: str, k: int, q: float) -> None:
        self.out.append(TradeEvent(self.instrument, self.t, self.price(k), float(q), aggressor, self._next_seq(),
                                   self._recv(), "synthetic"))

    def best_bid(self) -> Optional[int]:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> Optional[int]:
        return min(self.asks) if self.asks else None

    def advance(self, seconds: float) -> None:
        self.t = self.t + dt.timedelta(seconds=seconds)

    # -------------------------------------------------------------- trading --
    def trade(self, aggressor: str, qty: float, refill_to: Optional[float] = None, max_levels: int = 50) -> int:
        """A marketable order: executes against the opposite side from the
        best price outward. With ``refill_to`` the touch keeps refilling
        (an absorbing, refreshing passive order) so price cannot move."""
        side = ASK if aggressor == BUY else BID
        book = self.asks if side == ASK else self.bids
        qty = float(int(max(1, qty)))
        hit = 0
        while qty > 0 and book and hit < max_levels:
            k = min(book) if side == ASK else max(book)
            if refill_to is not None:
                if book[k] <= 1:
                    self._set(side, k, refill_to)
                take = min(qty, book[k] - 1)
                self._trade_ev(aggressor, k, take)
                self._set(side, k, book[k] - take)
                if book[k] < 0.3 * refill_to:
                    self._set(side, k, refill_to)        # the refresh
                qty -= take
                continue
            take = min(qty, book[k])
            self._trade_ev(aggressor, k, take)
            self._set(side, k, book[k] - take)
            qty -= take
            if k not in book:
                hit += 1
        return hit

    def sweep(self, aggressor: str, n_levels: int, pieces: int = 3) -> None:
        """Take ``n_levels`` whole levels in a few orders inside 0.1 s."""
        book = self.asks if aggressor == BUY else self.bids
        keys = sorted(book)[:n_levels] if aggressor == BUY else sorted(book, reverse=True)[:n_levels]
        total = sum(book[k] for k in keys) + 1
        per = math.ceil(total / pieces)
        for _ in range(pieces):
            self.trade(aggressor, per)
            self.advance(0.03)

    def pull(self, side: str, max_offset: int = 5, frac: float = 1.0) -> None:
        """Cancel liquidity near the touch on one side (no trades)."""
        book = self.bids if side == BID else self.asks
        if not book:
            return
        keys = sorted(book, reverse=(side == BID))[:max_offset]
        for k in keys:
            self._set(side, k, book[k] * (1.0 - frac))

    def thin(self, mult: float) -> None:
        for side, book in ((BID, self.bids), (ASK, self.asks)):
            for k in sorted(book):
                self._set(side, k, max(1.0, book[k] * mult))

    def widen(self, ticks: int) -> None:
        """Makers step back until the spread is ``ticks`` wide."""
        guard = 0
        while self.bids and self.asks and self.best_ask() - self.best_bid() < ticks and guard < 50:
            guard += 1
            if guard % 2:
                self._set(BID, self.best_bid(), 0)
            else:
                self._set(ASK, self.best_ask(), 0)

    # --------------------------------------------------------- one step --
    def _poisson(self, rate: float) -> int:
        lam = max(0.0, rate) * STEP
        if lam <= 0:
            return 0
        n, p, L = 0, 1.0, math.exp(-lam)
        while True:
            p *= self.rng.random()
            if p <= L:
                return n
            n += 1

    def _offset(self) -> int:
        o = 0
        while o < 4 and self.rng.random() < 0.45:
            o += 1
        return o

    def step(self) -> None:
        p = self.p
        for side in (BID, ASK):
            book = self.bids if side == BID else self.asks
            best = self.best_bid() if side == BID else self.best_ask()
            if best is None:
                continue
            add_rate = (p.add_bid if side == BID else p.add_ask)
            cancel_rate = (p.cancel_bid if side == BID else p.cancel_ask)
            add_rate = p.add_rate if add_rate is None else add_rate
            cancel_rate = p.cancel_rate if cancel_rate is None else cancel_rate
            mult = p.depth_bid if side == BID else p.depth_ask
            for _ in range(self._poisson(add_rate)):
                k = best - self._offset() if side == BID else best + self._offset()
                self._set(side, k, book.get(k, 0.0) + max(1, int(self.rng.uniform(1, 8) * mult)))
            for _ in range(self._poisson(cancel_rate)):
                k = best - self._offset() if side == BID else best + self._offset()
                if k in book and book[k] > 1:
                    # bigger levels lose more: sizes revert towards normal instead of drifting
                    cut = int(self.rng.uniform(1, 8) * max(0.5, book[k] / self.base))
                    self._set(side, k, max(1.0, book[k] - cut))
            pull = p.pull_bid if side == BID else p.pull_ask
            for _ in range(self._poisson(pull)):
                keys = sorted(book, reverse=(side == BID))[:5]
                if keys:
                    k = keys[min(len(keys) - 1, self._offset())]
                    self._set(side, k, int(book[k] * self.rng.uniform(0.0, 0.3)))
        for _ in range(self._poisson(p.buy_rate)):
            self.trade(BUY, max(1, int(self.rng.expovariate(1.0 / p.trade_size)) + 1), refill_to=p.absorb_ask)
        for _ in range(self._poisson(p.sell_rate)):
            self.trade(SELL, max(1, int(self.rng.expovariate(1.0 / p.trade_size)) + 1), refill_to=p.absorb_bid)
        self.maintain()
        self.advance(STEP)

    def maintain(self) -> None:
        """Market makers: keep ``levels`` contiguous levels each side, narrow a
        wide spread (towards ``max_spread``), and drop what falls out of view."""
        p = self.p
        bb, ba = self.best_bid(), self.best_ask()
        if bb is None:
            self._set(BID, (ba or 0) - p.max_spread, self._size(p.depth_bid))
        if ba is None:
            self._set(ASK, (self.best_bid() or 0) + p.max_spread, self._size(p.depth_ask))
        bb, ba = self.best_bid(), self.best_ask()
        if ba - bb > p.max_spread and self.rng.random() < p.spread_fill:
            if self.rng.random() < 0.5 + p.bias / 2.0:
                self._set(BID, bb + 1, self._size(p.depth_bid))
            else:
                self._set(ASK, ba - 1, self._size(p.depth_ask))
        bb, ba = self.best_bid(), self.best_ask()
        # deeper liquidity that was already resting beyond the ten visible prices comes INTO VIEW
        # (cause "view", as an MBP-10 feed shows it) - it is not a new order
        for i in range(self.levels):
            if bb - i not in self.bids:
                self._set(BID, bb - i, self._size(p.depth_bid), cause="view")
            if ba + i not in self.asks:
                self._set(ASK, ba + i, self._size(p.depth_ask), cause="view")
        for k in sorted(self.bids):
            if k <= bb - self.levels:
                self._set(BID, k, 0, cause="view")
        for k in sorted(self.asks):
            if k >= ba + self.levels:
                self._set(ASK, k, 0, cause="view")

    def run(self, seconds: float, **params) -> None:
        self.p = Params(**params)
        for _ in range(int(round(seconds / STEP))):
            self.step()


def _play(g: _Gen, scenario: str, warmup: float) -> None:
    g.marks["start"] = g.t
    g.run(warmup)
    g.marks["warmup_end"] = g.t
    s = scenario
    if s == "balanced":
        g.run(120)
    elif s in ("sweep_up", "sweep_down"):
        up = s == "sweep_up"
        g.run(20)
        g.marks["event"] = g.t
        g.sweep(BUY if up else SELL, 5)
        g.p = Params(bias=0.6 if up else -0.6)
        g.maintain()
        g.run(20, buy_rate=3.0 if up else 1.0, sell_rate=1.0 if up else 3.0, bias=0.6 if up else -0.6)
    elif s in ("offer_pulling", "bid_pulling"):
        offers = s == "offer_pulling"
        g.run(10)
        g.marks["event"] = g.t
        g.pull(ASK if offers else BID, 5, 0.85)
        kw = dict(pull_ask=8.0, add_ask=1.0, depth_ask=0.3, bias=0.4) if offers else \
            dict(pull_bid=8.0, add_bid=1.0, depth_bid=0.3, bias=-0.4)
        g.run(15, **kw)
    elif s == "wall_absorbed_consumed":
        g.run(10)
        ba = g.best_ask()
        wall = ba + 2
        g._set(ASK, wall, 400)
        g.marks["wall_placed"] = g.t
        g.run(5)
        # buyers walk up to the wall
        guard = 0
        while g.best_ask() < wall and guard < 200:
            guard += 1
            g.trade(BUY, 8)
            g.maintain()
            g.advance(0.1)
        g.marks["wall_reached"] = g.t
        # hit repeatedly; the wall refreshes twice
        refreshes = 0
        for i in range(60):
            g.trade(BUY, 6)
            if g.asks.get(wall, 0) < 120 and refreshes < 2:
                g._set(ASK, wall, 300)
                refreshes += 1
            if wall not in g.asks:
                break
            g.p = Params(bias=0.3)
            g.maintain()
            g.advance(0.4)
        # finally consumed
        g.marks["wall_consumed"] = g.t
        if wall in g.asks:
            g.trade(BUY, g.asks[wall] + 5)
        g.maintain()
        g.run(10, buy_rate=3.0, sell_rate=1.0, bias=0.6)
    elif s in ("absorption_buyers", "absorption_sellers"):
        buyers = s == "absorption_buyers"
        g.run(10)
        g.marks["event"] = g.t
        if buyers:
            g.run(30, buy_rate=8.0, sell_rate=1.0, trade_size=5.0, absorb_ask=40.0)
        else:
            g.run(30, buy_rate=1.0, sell_rate=8.0, trade_size=5.0, absorb_bid=40.0)
        g.marks["after"] = g.t
        if buyers:
            g.run(20, buy_rate=1.0, sell_rate=3.5, bias=-0.6)
        else:
            g.run(20, buy_rate=3.5, sell_rate=1.0, bias=0.6)
    elif s == "refresh_iceberg":
        g.run(10)
        g.marks["event"] = g.t
        g.run(30, buy_rate=1.0, sell_rate=3.0, trade_size=4.0, absorb_bid=15.0)
    elif s == "liquidity_vacuum":
        g.run(10)
        g.marks["event"] = g.t
        g.thin(0.12)
        g.p = Params(depth_bid=0.12, depth_ask=0.12, max_spread=4, spread_fill=0.0)
        g.widen(4)
        g.run(30, buy_rate=0.5, sell_rate=0.5, trade_size=2.0, add_rate=1.0, cancel_rate=1.0,
              depth_bid=0.12, depth_ask=0.12, max_spread=4, spread_fill=0.0)
    elif s == "news_shock":
        g.run(20)
        g.marks["event"] = g.t
        g.thin(0.2)
        g.p = Params(depth_bid=0.2, depth_ask=0.2, max_spread=6, spread_fill=0.0)
        g.widen(6)
        g.run(1.0, add_rate=1.0, cancel_rate=4.0, depth_bid=0.2, depth_ask=0.2, max_spread=6, spread_fill=0.0,
              buy_rate=0.5, sell_rate=0.5)
        g.marks["burst"] = g.t
        g.run(3.0, buy_rate=25.0, sell_rate=2.0, trade_size=6.0, depth_bid=0.25, depth_ask=0.25, max_spread=4,
              spread_fill=0.5, bias=0.8)
        g.marks["recovery"] = g.t
        g.run(30, buy_rate=3.0, sell_rate=2.0, bias=0.2)
    elif s in ("trend_up", "trend_down"):
        up = s == "trend_up"
        g.marks["event"] = g.t
        g.run(90, buy_rate=4.0 if up else 1.2, sell_rate=1.2 if up else 4.0, trade_size=4.0,
              add_bid=8.0 if up else 4.0, add_ask=4.0 if up else 8.0, pull_ask=0.6 if up else 0.0,
              pull_bid=0.0 if up else 0.6, bias=0.7 if up else -0.7)
    elif s == "breakout":
        g.run(20)
        g.marks["event"] = g.t
        g.sweep(BUY, 5)
        g.run(30, buy_rate=4.0, sell_rate=1.0, trade_size=4.0, add_bid=8.0, add_ask=4.0, pull_ask=0.6, bias=0.7)
    elif s == "reversal":
        g.run(60, buy_rate=4.0, sell_rate=1.2, trade_size=4.0, add_bid=8.0, add_ask=4.0, pull_ask=0.6, bias=0.7)
        g.marks["top"] = g.t
        g.run(15, buy_rate=7.0, sell_rate=1.0, trade_size=5.0, absorb_ask=40.0)
        g.marks["event"] = g.t
        g.run(45, buy_rate=1.2, sell_rate=4.5, trade_size=4.0, add_bid=4.0, add_ask=8.0, pull_bid=0.8, bias=-0.7)
    elif s in ("divergence_bearish", "divergence_bullish"):
        bear = s == "divergence_bearish"
        g.run(10)
        g.marks["event"] = g.t
        # price lifted (dropped) by the passive side stepping up (down) and the other side pulling,
        # while the aggressive flow is the other way: the delta falls (rises) as price makes new highs (lows)
        if bear:
            g.run(60, buy_rate=0.8, sell_rate=2.5, trade_size=3.0, pull_ask=4.0, add_ask=2.0, add_bid=10.0, bias=0.95)
        else:
            g.run(60, buy_rate=2.5, sell_rate=0.8, trade_size=3.0, pull_bid=4.0, add_bid=2.0, add_ask=10.0, bias=-0.95)
    elif s == "high_volatility":
        g.marks["event"] = g.t
        g.run(60, buy_rate=3.0, sell_rate=3.0, trade_size=45.0, spread_fill=0.9)
    elif s == "sequence_gap":
        g.run(30)
        g.marks["event"] = g.t
        g.seq += 100                                   # 100 messages never arrived
        g.run(60)
    elif s == "stale_feed":
        g.run(30)
        g.marks["event"] = g.t
        g.advance(30.0)                               # silence: no events for 30 seconds
        g.last_recv = g.t
        g.marks["resume"] = g.t
        g.run(60)
    elif s == "out_of_order":
        g.run(30)
        g.marks["event"] = g.t
        # an event that arrives late, carrying an older timestamp and sequence
        late_t = g.t - dt.timedelta(seconds=2)
        bb = g.best_bid()
        g.out.append(BookEvent(g.instrument, late_t, g._recv(), g.seq - 5, BID, g.price(bb - 3),
                               float(g.bids.get(bb - 3, 10.0)), MODIFY, source="synthetic"))
        g.run(60)
    else:
        raise ValueError(f"unknown scenario {scenario!r}; known: {', '.join(SCENARIOS)}")
    g.marks["end"] = g.t


def generate(scenario: str = "balanced", instrument: str = "6EZ6", seed: int = 7,
             start: Optional[dt.datetime] = None, warmup: float = 120.0, latency_ms: float = 2.0,
             base_price: Optional[float] = None, tick: Optional[float] = None) -> tuple[list[Event], dict]:
    """The events for one scenario and the times of its key moments. ``tick``: the price step (for an
    instrument whose definition has none - a generic one)."""
    root = root_of(instrument) or "6E"
    c = contract(root)
    tick = tick or (c.tick_size if c is not None and c.tick_size else 0.00005)
    bp = base_price if base_price is not None else BASE_PRICES.get(root, 1.0)
    g = _Gen(instrument, tick, bp, seed, start or DEFAULT_START, latency_ms)
    _play(g, scenario, warmup)
    return g.out, dict(g.marks)


class SyntheticFeed(FeedAdapter):
    """A feed that plays one or more generated scenarios. SYNTHETIC: the
    engine never sends a live order from it and never reports its numbers
    as performance."""

    vendor = "synthetic"
    synthetic = True
    virtual_time = True
    sequence_scope = "instrument"

    def __init__(self, scenario: str = "balanced", instruments: Sequence[str] = ("6EZ6",), seed: int = 7,
                 start: Optional[dt.datetime] = None, warmup: float = 120.0,
                 scenarios: Optional[dict[str, str]] = None):
        self.scenario = scenario
        self.instruments = list(instruments)
        self.marks: dict[str, dict] = {}
        streams: list[list[Event]] = []
        for i, inst in enumerate(self.instruments):
            sc = (scenarios or {}).get(inst, scenario)
            evs, marks = generate(sc, inst, seed + i * 101, start, warmup)
            self.marks[inst] = marks
            streams.append(evs)
        # one stream, in arrival order; ties broken by instrument order then sequence
        merged = []
        for i, evs in enumerate(streams):
            for j, ev in enumerate(evs):
                t = ev.ts_received if isinstance(ev, BookEvent) else ev.received
                merged.append((t, i, j, ev))
        merged.sort(key=lambda x: (x[0], x[1], x[2]))
        self._events = [m[3] for m in merged]
        self._i = 0
        self._connected = False
        self._last: Optional[dt.datetime] = None

    @property
    def all_events(self) -> list[Event]:
        return list(self._events)

    def connect(self) -> bool:
        self._connected = True
        return True

    def subscribe(self, instruments: Sequence[str]) -> None:
        pass

    def events(self, max_events: int = 10_000) -> list[Event]:
        if not self._connected:
            return []
        out = self._events[self._i:self._i + max(1, int(max_events))]
        self._i += len(out)
        if out:
            last = out[-1]
            self._last = last.ts_received if isinstance(last, BookEvent) else last.received
        return out

    def now(self) -> Optional[dt.datetime]:
        return self._last

    def exhausted(self) -> bool:
        return self._i >= len(self._events)

    def health(self) -> FeedHealth:
        state = FINISHED if self.exhausted() else LIVE
        reason = (f"synthetic scenario '{self.scenario}' finished" if state == FINISHED
                  else f"synthetic scenario '{self.scenario}' (test data, not a market)")
        return FeedHealth(state if self._connected else "CONNECTING", reason, self.vendor, INSTITUTIONAL, True,
                          self._connected, self._i, self._last.isoformat() if self._last else None,
                          subscribed=list(self.instruments))

    def recover(self, reason: str = "") -> bool:
        return True

    def calendar_events(self) -> list[CalendarEvent]:
        """The release a news_shock scenario plays (a US payrolls beat)."""
        out = []
        for inst, marks in self.marks.items():
            if "burst" in marks and "event" in marks:
                out.append(CalendarEvent(f"synthetic-{inst}", marks["event"], "USD", "US",
                                         "Non-Farm Payrolls (synthetic)", 3, actual=120.0, forecast=180.0,
                                         previous=150.0, source="synthetic"))
        return out
