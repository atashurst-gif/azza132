"""Binance's PUBLIC market data into Ian's internal format - no account, no key.

What it reads (the documented public streams of the spot exchange):

* ``<symbol>@depth@100ms`` - the DIFF-DEPTH stream: every 100 ms, each price
  level whose quantity changed, with its new absolute quantity (0 = gone),
  and the range of book update ids it covers (``U`` first, ``u`` last);
* ``<symbol>@aggTrade`` - every trade, with ``m`` = "the buyer is the maker":
  m true -> the BUYER was resting, so the AGGRESSOR SOLD (hit the bid);
  m false -> the aggressor BOUGHT (lifted the offer);
* REST ``/api/v3/depth?limit=5000`` for the snapshot the diffs are applied to,
  ``/api/v3/exchangeInfo`` for the price tick and ``/api/v3/klines`` for the
  recent volatility that sizes the analysis price step.

The local book follows Binance's documented procedure exactly: buffer the
diffs, take a snapshot, drop every buffered event with ``u`` <= the
snapshot's ``lastUpdateId``, then apply an event only when
``U <= last + 1 <= u`` and set ``last = u``; an event with ``u <= last`` is
stale and dropped; ``U > last + 1`` is a GAP - events were missed. On a gap
the feed reports it (a GAP notice: Ian's data quality goes DEGRADED, so no
new entries), CLEARs the book, and rebuilds it from a fresh snapshot; the
instrument stays DEGRADED until the rebuilt book has run clean. A trade whose
aggregate id skips ahead is reported as a gap too.

What the engine sees: the book GROUPED into price steps (the analysis grid,
announced first in a GRID notice and written into recordings), the ten best
steps each side, as price-level events - ``add`` / ``modify`` / ``cancel``
with the step's new total; a step entering or leaving the ten-step window
because the price moved is marked ``cause="view"`` (not a cancellation), a
rebuild ``cause="snapshot"``. Binance quotes BTC to 0.01 USDT, far finer
than anything the order-flow rules mean by a tick, so the step is sized from
the instrument's own recent volatility: a quarter of its typical two-minute
move (what one tick is to a CME FX future), rounded to 1, 2, 2.5 or 5 x a
power of ten. Each 100 ms diff nets the orders within it - an add and a
cancel at the same step inside one batch are seen as their sum - which is
finer than a snapshot and coarser than a market-by-order feed.

Times: every book event carries the exchange's event time ``E`` and every
trade the exchange's trade time ``T``; the local receipt time is kept beside
it for the latency. Events are released after ``hold_ms`` (150 ms) in
exchange-time order, so a trade is seen before the 100 ms book batch that
already shows its effect; a GAP / CLEAR / GRID is a barrier nothing is moved
across.

Connections: WebSocket first (the ``websocket-client`` package if it is
installed, else the built-in client in :mod:`.wsclient`), reconnected with a
back-off (1, 2, 4 ... 60 s); every reconnect rebuilds the books. If the
WebSocket cannot be reached at all, the feed falls back to REST polling of
depth snapshots and aggregate trades (and keeps trying the WebSocket every
five minutes). A polled book is too coarse for the order-flow features, so
REST polling is DEGRADED: the page keeps showing the market, no new trades
are entered (``allow_rest_entries`` changes that).

It is labelled for what it is everywhere: source "binance", data class
EXCHANGE, "Binance public order book (crypto exchange)". Never CME, never
institutional FX. The engine trades it through the broker's matching crypto
CFD (BTCUSDT -> BTCUSD, ETHUSDT -> ETHUSD).

The adapter never raises into the engine: a blocked network, a refused
location (HTTP 451) or a dropped stream is a state with a plain reason.
"""
from __future__ import annotations

import datetime as dt
import itertools
import json
import logging
import math
import statistics as stats
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from .. import EXCHANGE
from ..book import ADD, ASK, BID, BUY, CANCEL, CLEAR, MODIFY, SELL, BookEvent, Event, FeedNotice, TradeEvent, px
from .base import CONNECTING, DEGRADED, DOWN, LIVE, NOT_CONFIGURED, FeedAdapter, FeedHealth

log = logging.getLogger("mintel.ian.binance")

UTC = dt.timezone.utc
SOURCE = "binance"
LABEL = "Binance public order book (crypto exchange)"
WS_BASES = ("wss://stream.binance.com:9443", "wss://data-stream.binance.vision")
REST_BASES = ("https://data-api.binance.vision", "https://api.binance.com")
GRID = "GRID"                       # the notice that announces an instrument's analysis price step
USER_AGENT = "mintel-financial-ian/1.0 (public market data reader)"
MAX_BUFFER = 5000                   # diff messages held per symbol while a snapshot is fetched
DEPTH_LIMIT = 5000                  # levels a side in a stream snapshot: Binance's most, so the ten-step window is
                                    # covered (fewer levels can reach less far than ten of BTC's price steps)


# ------------------------------------------------------------------ helpers --

def ms_time(ms) -> Optional[dt.datetime]:
    try:
        v = int(ms)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    return dt.datetime.fromtimestamp(v // 1000, tz=UTC) + dt.timedelta(milliseconds=v % 1000)


def nice_step(x: float) -> float:
    """The nearest of 1, 2, 2.5 and 5 x a power of ten (on a log scale)."""
    if not x or x <= 0 or not math.isfinite(x):
        return 0.0
    e = math.floor(math.log10(x))
    best, err = 0.0, float("inf")
    for k in (e - 1, e, e + 1):
        for m in (1.0, 2.0, 2.5, 5.0):
            c = m * 10.0 ** k
            d = abs(math.log(c / x))
            if d < err:
                best, err = c, d
    return float(f"{best:.12g}")


def two_minute_sigma(closes: Sequence[float]) -> Optional[float]:
    """The standard deviation of the two-minute move, as a fraction of the price, from one-minute closes."""
    cs = [float(c) for c in closes if c and float(c) > 0]
    if len(cs) < 20:
        return None
    rets = [math.log(b / a) for a, b in zip(cs, cs[1:])]
    s1 = stats.pstdev(rets)
    return s1 * math.sqrt(2.0) if s1 > 0 else None


def choose_grid(mid: float, exchange_tick: float, sigma2: Optional[float] = None, grid_bp: float = 2.0,
                price_grid: float = 0.0) -> tuple[float, str]:
    """The analysis price step and how it was chosen. Always a whole number of exchange ticks."""
    tick = exchange_tick if exchange_tick and exchange_tick > 0 else 0.01
    if price_grid and price_grid > 0:
        raw, how = float(price_grid), "set in ian.json"
    elif sigma2 and mid > 0:
        raw = mid * sigma2 / 4.0
        raw = min(max(raw, mid * 0.5e-4), mid * 10e-4)
        raw, how = nice_step(raw), (f"a quarter of its typical two-minute move ({sigma2 * 1e4:.1f} bp, from the last "
                                    f"two hours of one-minute bars)")
    else:
        raw, how = nice_step(mid * max(grid_bp, 0.1) / 1e4), f"{grid_bp:g} bp of the price (volatility not measured)"
    g = max(tick, round(raw / tick) * tick)
    return float(f"{g:.12g}"), how


def bucket(price: float, side: str, grid: float) -> float:
    """The step a level belongs to: bids are grouped DOWN, offers UP - so a grouped book is never crossed."""
    k = round(price / grid, 6)
    return px((math.floor(k) if side == BID else math.ceil(k)) * grid)


def grouped_view(levels: dict, side: str, grid: float, n: int, truncated: bool,
                 edge: Optional[float] = None) -> dict:
    """The ``n`` best steps on one side: {step price: total quantity}. A step is only shown when it is known
    to be complete: when the book was cut off (the snapshot's depth limit), the deepest step it reaches may
    hold more than we know of and is left out. ``edge`` is the deepest price the cut-off book was known to
    (the snapshot's last level): levels the diff stream has added beyond it sit among levels that have not
    changed since, which Binance never sends, so the step holding the edge and every step past it are left
    out however many levels they show."""
    out: dict[float, float] = {}
    keys = sorted(levels, reverse=(side == BID))
    bid = side == BID
    stop_at = None
    if truncated and edge is not None:
        stop_at = bucket(edge, side, grid) if grid > 0 else edge
    complete = False
    for p in keys:
        b = bucket(p, side, grid) if grid > 0 else p
        if stop_at is not None and ((b <= stop_at) if bid else (b >= stop_at)):
            break                                     # the edge step and beyond: only partly known
        if b not in out:
            if len(out) >= n:
                complete = True
                break
            out[b] = 0.0
        out[b] += levels[p]
    if out and truncated and stop_at is None and not complete:
        out.pop(next(reversed(out)))
    return {k: round(v, 8) for k, v in out.items() if v > 0}


def diff_views(inst: str, te: dt.datetime, tr: dt.datetime, seq: int, side: str, old: dict, new: dict, n: int,
               snap: bool, source: str = SOURCE) -> list[Event]:
    """Level changes between two n-step views (as for an MBP-10 feed): a step beyond the far end of the other
    view, when that view is full, only moved into or out of the window ("view"); it was not added or cancelled."""
    out: list[Event] = []
    bid = side == BID
    full_old, full_new = len(old) >= n, len(new) >= n
    far_new = (min(new) if bid else max(new)) if new else None
    far_old = (min(old) if bid else max(old)) if old else None
    for p in sorted(set(old) | set(new), reverse=bid):
        a, b = old.get(p, 0.0), new.get(p, 0.0)
        if a == b:
            continue
        cause = "snapshot" if snap else ""
        if b <= 0:
            beyond = far_new is not None and ((p < far_new) if bid else (p > far_new))
            if not cause and full_new and beyond:
                cause = "view"
            out.append(BookEvent(inst, te, tr, seq, side, p, 0.0, CANCEL, cause=cause, source=source))
        elif a <= 0:
            beyond = far_old is not None and ((p < far_old) if bid else (p > far_old))
            if not cause and full_old and beyond:
                cause = "view"
            out.append(BookEvent(inst, te, tr, seq, side, p, b, ADD, cause=cause, source=source))
        else:
            out.append(BookEvent(inst, te, tr, seq, side, p, b, MODIFY, cause=cause, source=source))
    return out


class LocalBook:
    """One symbol's full local book, kept by Binance's documented snapshot + diff procedure."""

    def __init__(self, max_levels: int = 10_000):
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_id: Optional[int] = None
        self.max_levels = int(max_levels)
        self.truncated = {BID: True, ASK: True}
        # the deepest price a cut-off side is known to (None: the whole side is known, or nothing is yet)
        self.edge: dict[str, Optional[float]] = {BID: None, ASK: None}

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.last_id = None
        self.edge = {BID: None, ASK: None}

    @staticmethod
    def _apply(book: dict, levels) -> None:
        for lv in levels or ():
            try:
                p, q = px(float(lv[0])), float(lv[1])
            except (TypeError, ValueError, IndexError):
                continue
            if q <= 0:
                book.pop(p, None)
            else:
                book[p] = q

    def load(self, snap: dict, limit: int) -> None:
        self.reset()
        self._apply(self.bids, snap.get("bids"))
        self._apply(self.asks, snap.get("asks"))
        self.last_id = int(snap.get("lastUpdateId"))
        self.truncated = {BID: len(snap.get("bids") or ()) >= limit, ASK: len(snap.get("asks") or ()) >= limit}
        for side, book in ((BID, self.bids), (ASK, self.asks)):
            if self.truncated[side] and book:
                self.edge[side] = min(book) if side == BID else max(book)

    def check(self, U: int, u: int) -> str:
        """'apply' (U <= last + 1 <= u), 'stale' (u <= last: already in the book) or 'gap' (U > last + 1)."""
        last = self.last_id if self.last_id is not None else -1
        if u <= last:
            return "stale"
        if U <= last + 1 <= u:
            return "apply"
        return "gap"

    def apply(self, d: dict) -> None:
        self._apply(self.bids, d.get("b"))
        self._apply(self.asks, d.get("a"))
        self.last_id = int(d["u"])
        for side, book in ((BID, self.bids), (ASK, self.asks)):
            if len(book) > self.max_levels:
                keep = sorted(book, reverse=(side == BID))[: self.max_levels]
                for p in set(book) - set(keep):
                    del book[p]
                self.truncated[side] = True
                deepest, old = keep[-1], self.edge[side]       # known no further than what is kept
                self.edge[side] = deepest if old is None else (max(old, deepest) if side == BID else min(old, deepest))

    def mid(self) -> Optional[float]:
        if not self.bids or not self.asks:
            return None
        return (max(self.bids) + min(self.asks)) / 2.0


class _Sym:
    def __init__(self, symbol: str, max_levels: int = 10_000):
        self.symbol = symbol
        self.book = LocalBook(max_levels)
        self.synced = False
        self.buffer: list[tuple[dict, dt.datetime]] = []
        self.view = ({}, {})
        self.grid = 0.0
        self.grid_how = ""
        self.exchange_tick = 0.0
        self.last_agg: Optional[int] = None
        self.last_e: Optional[dt.datetime] = None
        self.stale = 0
        self.gaps = 0
        self.trade_gaps = 0
        self.resyncs = 0
        self.why = "waiting for its first snapshot"
        self.rest_id: Optional[int] = None


class BinanceMapper:
    """Pure translation of Binance's public messages and REST answers into Ian's events, with the local books.
    No network and no threads, so recorded or hand-made messages test it exactly."""

    def __init__(self, symbols: Sequence[str], view_levels: int = 10, depth_limit: int = DEPTH_LIMIT,
                 source: str = SOURCE):
        self.n = int(view_levels)
        self.depth_limit = int(depth_limit)
        self.source = source
        # the local book may grow past the snapshot as the stream adds levels: room for twice the snapshot
        levels = max(2 * self.depth_limit, 2000)
        self.syms: dict[str, _Sym] = {s.upper(): _Sym(s.upper(), levels) for s in symbols}
        self.unknown = 0
        self.dropped_buffer = 0

    # ----------------------------------------------------------- the grid --
    def set_grid(self, symbol: str, grid: float, how: str, exchange_tick: float, rx: dt.datetime) -> list[Event]:
        s = self.syms.get(symbol)
        if s is None or grid <= 0:
            return []
        if s.grid > 0:
            return []                                  # fixed for the life of the feed (stable feature keys)
        s.grid, s.grid_how, s.exchange_tick = float(grid), how, float(exchange_tick or 0.0)
        detail = (f"{grid:g} (the book is grouped into {grid:g} USDT price steps - {how}; Binance quotes "
                  f"{symbol} to {exchange_tick:g})")
        return [FeedNotice(symbol, rx, GRID, detail, self.source)]

    # ------------------------------------------------------------- stream --
    def on_text(self, text: str, rx: dt.datetime) -> list[Event]:
        try:
            obj = json.loads(text)
        except (TypeError, ValueError):
            self.unknown += 1
            return []
        return self.on_message(obj, rx)

    def on_message(self, obj: Any, rx: dt.datetime) -> list[Event]:
        d = obj.get("data") if isinstance(obj, dict) and "data" in obj else obj
        if not isinstance(d, dict):
            self.unknown += 1
            return []
        e = d.get("e")
        if e == "depthUpdate":
            return self.on_depth(d, rx)
        if e == "aggTrade":
            return self.on_agg_trade(d, rx)
        self.unknown += 1
        return []

    def on_depth(self, d: dict, rx: dt.datetime) -> list[Event]:
        s = self.syms.get(str(d.get("s") or "").upper())
        if s is None:
            self.unknown += 1
            return []
        try:
            U, u = int(d["U"]), int(d["u"])
        except (KeyError, TypeError, ValueError):
            self.unknown += 1
            return []
        te = ms_time(d.get("E")) or rx
        s.last_e = te if s.last_e is None or te > s.last_e else s.last_e
        if not s.synced:
            s.buffer.append((d, rx))
            if len(s.buffer) > MAX_BUFFER:
                s.buffer.pop(0)
                self.dropped_buffer += 1
            return []
        verdict = s.book.check(U, u)
        if verdict == "stale":
            s.stale += 1
            return []
        if verdict == "gap":
            missing = U - (s.book.last_id or 0) - 1
            out = self.out_of_sync(s.symbol, te, rx,
                                   f"Binance depth stream gap: {missing} update(s) missing after #{s.book.last_id}; "
                                   "rebuilding the book from a fresh snapshot")
            s.gaps += 1
            s.buffer.append((d, rx))                   # the first event of the new buffer
            return out
        s.book.apply(d)
        return self._emit_view(s, te, rx, u, snap=False)

    def on_agg_trade(self, d: dict, rx: dt.datetime, symbol: str = "") -> list[Event]:
        s = self.syms.get(str(symbol or d.get("s") or "").upper())
        if s is None:
            self.unknown += 1
            return []
        try:
            a = int(d["a"])
            p, q = px(float(d["p"])), float(d["q"])
        except (KeyError, TypeError, ValueError):
            self.unknown += 1
            return []
        out: list[Event] = []
        if s.last_agg is not None and a <= s.last_agg:
            return out                                 # already seen (a reconnect or a poll overlapping)
        if s.last_agg is not None and a > s.last_agg + 1 and s.grid > 0:
            s.trade_gaps += 1
            out.append(FeedNotice(s.symbol, rx, "GAP", f"{a - s.last_agg - 1} trade(s) missed (aggregate trade ids "
                                                       f"{s.last_agg + 1}-{a - 1})", self.source))
        s.last_agg = a
        if s.grid <= 0 or p <= 0 or q <= 0:
            return out                                 # before the price step is known: the id is kept, the print is not used
        # m = "the buyer is the maker": the buyer was resting, so the aggressor SOLD
        aggressor = SELL if bool(d.get("m")) else BUY
        te = ms_time(d.get("T")) or ms_time(d.get("E")) or rx
        out.append(TradeEvent(s.symbol, te, p, q, aggressor, a, rx, self.source))
        return out

    # ----------------------------------------------------------- snapshot --
    def needs_snapshot(self) -> list[str]:
        """Symbols waiting for a snapshot that have buffered at least one diff (Binance's procedure: note the
        first event's U, THEN take the snapshot - so the stream is known to join it)."""
        return [k for k, s in self.syms.items() if not s.synced and s.buffer]

    def on_snapshot(self, symbol: str, snap: dict, rx: dt.datetime) -> tuple[list[Event], str]:
        """A REST depth snapshot for a symbol that is waiting for one. Returns (events, '') when the book is
        rebuilt, or ([], why) when this snapshot cannot be used and another is needed."""
        s = self.syms.get(symbol.upper())
        if s is None or s.synced:
            return [], ""
        try:
            last_id = int(snap["lastUpdateId"])
        except (KeyError, TypeError, ValueError):
            return [], "the snapshot had no update id"
        if s.grid <= 0:
            return [], "no price step yet"
        if s.buffer and last_id < int(s.buffer[0][0]["U"]) - 1:
            return [], "the snapshot is older than the buffered stream; another is needed"
        s.book.load(snap, self.depth_limit)
        te = s.last_e or rx
        keep = [(d, r) for d, r in s.buffer if int(d["u"]) > last_id]
        for d, _ in keep:
            verdict = s.book.check(int(d["U"]), int(d["u"]))
            if verdict == "stale":
                continue
            if verdict == "gap":
                s.book.reset()
                return [], "the buffered stream does not join the snapshot; another is needed"
            s.book.apply(d)
            te = ms_time(d.get("E")) or te
        s.buffer = []
        s.synced = True
        s.resyncs += 1
        s.why = ""
        s.view = ({}, {})
        out: list[Event] = [BookEvent(s.symbol, te, rx, int(s.book.last_id or 0), "", 0.0, 0.0, CLEAR,
                                      cause="snapshot", source=self.source)]
        out += self._emit_view(s, te, rx, int(s.book.last_id or 0), snap=True)
        return out, ""

    def out_of_sync(self, symbol: str, te: dt.datetime, rx: dt.datetime, why: str) -> list[Event]:
        """The book can no longer be trusted: say so (a GAP: no new entries), clear it, wait for a snapshot."""
        s = self.syms[symbol]
        s.synced = False
        s.book.reset()
        s.buffer = []
        s.view = ({}, {})
        s.why = why
        return [FeedNotice(s.symbol, rx, "GAP", why, self.source),
                BookEvent(s.symbol, te, rx, 0, "", 0.0, 0.0, CLEAR, cause="snapshot", source=self.source)]

    def disconnected(self, rx: dt.datetime, why: str) -> list[Event]:
        out: list[Event] = [FeedNotice("", rx, "DISCONNECTED", why, self.source)]
        for s in self.syms.values():
            was = s.synced
            s.synced = False
            s.book.reset()
            s.buffer = []
            s.view = ({}, {})
            s.why = "waiting for the stream to reconnect"
            if was:
                out.append(BookEvent(s.symbol, s.last_e or rx, rx, 0, "", 0.0, 0.0, CLEAR, cause="snapshot",
                                     source=self.source))
        return out

    # --------------------------------------------------------------- REST --
    def on_rest_depth(self, symbol: str, snap: dict, rx: dt.datetime, limit: Optional[int] = None) -> list[Event]:
        """REST polling (no WebSocket): each snapshot replaces the book; the change in the grouped view is
        emitted. The REST depth carries no exchange time: the receipt time is used (said in the feed's health).
        ``limit``: the depth that was asked for (a snapshot that holds that many levels was cut off there)."""
        s = self.syms.get(symbol.upper())
        if s is None or s.grid <= 0:
            return []
        try:
            last_id = int(snap["lastUpdateId"])
        except (KeyError, TypeError, ValueError):
            return []
        if s.rest_id is not None and last_id <= s.rest_id and s.synced:
            return []
        s.book.load(snap, int(limit) if limit else self.depth_limit)
        s.rest_id = last_id
        first = not s.synced
        s.synced = True
        s.buffer = []
        s.why = ""
        out: list[Event] = []
        if first:
            s.view = ({}, {})
            out.append(BookEvent(s.symbol, rx, rx, last_id, "", 0.0, 0.0, CLEAR, cause="snapshot", source=self.source))
        out += self._emit_view(s, rx, rx, last_id, snap=first)
        return out

    def rest_mode_reset(self) -> None:
        """Leaving REST polling for the stream: every book is rebuilt from a snapshot again."""
        for s in self.syms.values():
            s.synced = False
            s.rest_id = None
            s.buffer = []
            s.book.reset()

    # --------------------------------------------------------------- view --
    def _emit_view(self, s: _Sym, te: dt.datetime, rx: dt.datetime, seq: int, snap: bool) -> list[Event]:
        nb = grouped_view(s.book.bids, BID, s.grid, self.n, s.book.truncated[BID], s.book.edge[BID])
        na = grouped_view(s.book.asks, ASK, s.grid, self.n, s.book.truncated[ASK], s.book.edge[ASK])
        ob, oa = s.view
        out = diff_views(s.symbol, te, rx, seq, BID, ob, nb, self.n, snap, self.source)
        out += diff_views(s.symbol, te, rx, seq, ASK, oa, na, self.n, snap, self.source)
        s.view = (nb, na)
        return out


# --------------------------------------------------------------- the feed --

class RateLimited(RuntimeError):
    def __init__(self, msg: str, retry_after: float):
        super().__init__(msg)
        self.retry_after = retry_after


def _http_get_json(url: str, timeout: float = 10.0):
    from .wsclient import ssl_context
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ssl_context()) as r:   # noqa: S310 - fixed https host
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code in (418, 429):
            try:
                ra = float(exc.headers.get("Retry-After") or 60)
            except (TypeError, ValueError):
                ra = 60.0
            raise RateLimited(f"Binance asked us to slow down (HTTP {exc.code})", ra) from exc
        if exc.code == 451:
            raise ConnectionError("Binance refuses requests from this location (HTTP 451)") from exc
        raise


class BinanceFeed(FeedAdapter):
    vendor = "binance"
    data_class = EXCHANGE
    synthetic = False
    virtual_time = False
    sequence_scope = "none"           # book update ids and trade ids are separate sequences; the feed checks both itself
    # the book comes in 100 ms batches and the trades on their own stream: a little leeway for their order; a
    # home computer's clock and its distance from Binance's servers: a little for the latency and the clock
    quality_overrides = {"order_tolerance_ms": 500.0, "max_latency_ms": 2500.0, "clock_skew_ms": 1000.0}

    def __init__(self, data_dir: str | Path | None = None, symbols: Sequence[str] = (), *,
                 instruments: Optional[dict] = None, transport: str = "auto",
                 ws_bases: Sequence[str] = WS_BASES, rest_bases: Sequence[str] = REST_BASES,
                 depth_limit: int = DEPTH_LIMIT, view_levels: int = 10, hold_ms: float = 150.0,
                 rest_poll_s: float = 1.0, rest_depth_limit: int = 500, rest_after_failures: int = 3,
                 rest_period_s: float = 300.0, recv_timeout_s: float = 30.0, allow_rest_entries: bool = False,
                 http_get: Optional[Callable[[str], Any]] = None, ws_connect: Optional[Callable[[str], Any]] = None,
                 clock: Optional[Callable[[], dt.datetime]] = None, sleep: Optional[Callable[[float], None]] = None,
                 start_threads: bool = True, prefer_library: bool = True, max_queue: int = 400_000):
        self.data_dir = Path(data_dir) if data_dir is not None else None
        self.instruments = dict(instruments or {})            # symbol -> CryptoInstrument (grid settings)
        self.transport = str(transport or "auto").lower()
        self.ws_bases = list(ws_bases)
        self.rest_bases = list(rest_bases)
        self.depth_limit = int(depth_limit)
        self.view_levels = int(view_levels)
        self.hold = dt.timedelta(milliseconds=float(hold_ms))
        self.rest_poll_s = float(rest_poll_s)
        self.rest_depth_limit = int(rest_depth_limit)
        self.rest_after_failures = int(rest_after_failures)
        self.rest_period_s = float(rest_period_s)
        self.recv_timeout_s = float(recv_timeout_s)
        self.allow_rest_entries = bool(allow_rest_entries)
        self._http = http_get or _http_get_json
        self._ws_connect = ws_connect
        self._clock = clock or (lambda: dt.datetime.now(UTC))
        self._sleep_fn = sleep
        self.start_threads = bool(start_threads)
        self.prefer_library = bool(prefer_library)
        self.max_queue = int(max_queue)
        self.symbols: list[str] = []
        self.mapper = BinanceMapper([], self.view_levels, self.depth_limit)
        self._lock = threading.RLock()
        self._pending: list[tuple] = []                       # heap of (segment, exchange time, rank, n, event, rx)
        self._n = itertools.count()
        self._segment = 0
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._snap_wake = threading.Event()
        self._threads: list[threading.Thread] = []
        self._conn = None
        self.mode = "ws"                                      # ws / rest
        self.connected = False
        self.ever_connected = False
        self.connect_failures = 0
        self.reconnects = 0
        self.last_error = ""
        self.retry_in = 0.0
        self.impl = ""
        self.count = 0
        self.dropped = 0
        self.last_event: Optional[dt.datetime] = None
        self.last_message: Optional[dt.datetime] = None
        self.clock_offset_ms: Optional[float] = None
        self._rest_base_i = 0
        self._ws_base_i = 0
        self._ticks: dict[str, float] = {}
        self._sigma: dict[str, Optional[float]] = {}
        self._rest_until: Optional[float] = None                 # monotonic: no REST request before this (Retry-After)
        self._rest_limited = ""
        self._rest_reason = ""

    # ----------------------------------------------------------- the API --
    def connect(self) -> bool:
        return True                     # the connection is made by subscribe(), which needs the symbols

    def subscribe(self, instruments: Sequence[str]) -> None:
        syms = [str(i).upper() for i in instruments if i]
        if syms == self.symbols and self._threads:
            return
        self.close()
        with self._lock:
            self.symbols = syms
            self.mapper = BinanceMapper(syms, self.view_levels, self.depth_limit)
            self._pending = []
        self._stop = stop = threading.Event()             # each pair of threads keeps its own: a resubscribe never revives them
        if self.start_threads and syms:
            for name, target in (("ws", self._ws_loop), ("snapshots", self._snap_loop)):
                t = threading.Thread(target=target, args=(stop,), name=f"ian-binance-{name}", daemon=True)
                t.start()
                self._threads.append(t)

    def events(self, max_events: int = 10_000) -> list[Event]:
        """What has arrived more than ``hold_ms`` ago, in exchange-time order within each segment between
        barriers (a GAP, CLEAR or GRID never moves)."""
        cut = self._clock() - self.hold
        out: list[Event] = []
        with self._lock:
            ready = [x for x in self._pending if x[5] <= cut]
            if not ready:
                return out
            ready.sort(key=lambda x: x[:4])
            ready = ready[: max(1, int(max_events))]
            gone = {id(x) for x in ready}
            self._pending = [x for x in self._pending if id(x) not in gone]
        return [x[4] for x in ready]

    def health(self) -> FeedHealth:
        with self._lock:
            state, reason = self._state()
            return FeedHealth(state, reason[:400], self.vendor, EXCHANGE, False, self.connected, self.count,
                              self.last_event.isoformat() if self.last_event else None, self.last_error,
                              self.reconnects, list(self.symbols))

    def _state(self) -> tuple[str, str]:
        if not self.symbols:
            if self.instruments:
                # configured, not subscribed yet (the engine and --check-feed subscribe straight after)
                return CONNECTING, (f"{LABEL}, {' and '.join(self.instruments)}: not subscribed yet "
                                    "(public data, no key needed)")
            return NOT_CONFIGURED, f"{LABEL}: no instruments to read"
        head = f"{LABEL}, {' and '.join(self.symbols)}"
        grids = [f"{k} in {s.grid:g} USDT steps" for k, s in self.mapper.syms.items() if s.grid > 0]
        tail = (f"; book grouped: {', '.join(grids)}" if grids else "")
        if self.clock_offset_ms is not None and abs(self.clock_offset_ms) > 1000:
            tail += (f"; this computer's clock is {abs(self.clock_offset_ms) / 1000:.1f} s "
                     f"{'behind' if self.clock_offset_ms > 0 else 'ahead of'} Binance's - check its time settings")
        if self.mode == "rest" and self.connected:
            why = (f"{head} by REST polling every {self.rest_poll_s:g} s (the WebSocket stream cannot be reached: "
                   f"{self._rest_reason or 'no reason given'}; retried every {self.rest_period_s / 60:g} min)")
            if self.allow_rest_entries:
                return LIVE, why + tail
            return DEGRADED, why + " - a polled book is too coarse for the order-flow reading: no new trades" + tail
        if not self.connected:
            if not self.ever_connected and not self.last_error:
                return CONNECTING, f"{head}: connecting (public data, no key needed)"
            nxt = f"; trying again in {self.retry_in:.0f} s" if self.retry_in else ""
            return DOWN, f"{head}: cannot reach Binance ({self.last_error or 'disconnected'}){nxt}"
        out = [f"{k}: {s.why}" for k, s in self.mapper.syms.items() if not s.synced]
        how = f"{head} over WebSocket ({self.impl or 'stream'})"
        if self.last_message is not None:
            quiet = (self._clock() - self.last_message).total_seconds()
            if quiet > 30:
                out.append(f"nothing received for {quiet:.0f} s")
        if out:
            return DEGRADED, f"{how}; " + "; ".join(out) + tail
        return LIVE, how + tail

    def recover(self, reason: str = "") -> bool:
        """The engine asks for a clean book: reconnect now if the stream is down, and fetch a snapshot for every
        symbol still waiting for one. The feed resynchronises a gap by itself; this only hurries it."""
        self._wake.set()
        self._snap_wake.set()
        return True

    def close(self) -> None:
        self._stop.set()
        self._wake.set()
        self._snap_wake.set()
        c, self._conn = self._conn, None
        if c is not None:
            try:
                c.close()
            except Exception:
                pass
        for t in self._threads:
            if t is not threading.current_thread():
                t.join(timeout=2.0)
        self._threads = []
        self.connected = False

    # ------------------------------------------------------- the handlers --
    def _put(self, evs: list[Event], rx: dt.datetime) -> None:
        """Queue events (the caller holds the lock)."""
        for ev in evs:
            if len(self._pending) >= self.max_queue:
                self.dropped += 1
                if self.dropped == 1:
                    self.last_error = "the engine is not keeping up with the feed (queue full)"
                continue
            if isinstance(ev, FeedNotice):
                self._segment += 1
                rank, te = -2, ev.ts
            elif isinstance(ev, BookEvent) and ev.action == CLEAR:
                self._segment += 1
                rank, te = -1, ev.ts_exchange
            elif isinstance(ev, TradeEvent):
                rank, te = 0, ev.ts
            else:
                rank, te = 1, ev.ts_exchange
            self._pending.append((self._segment, te, rank, next(self._n), ev, rx))
            self.count += 1
        if evs:
            self.last_event = rx

    def handle_text(self, text: str, rx: Optional[dt.datetime] = None) -> int:
        """One stream message (the WebSocket thread, or a test). Returns the number of events it gave."""
        rx = rx or self._clock()
        with self._lock:
            self.last_message = rx
            try:
                evs = self.mapper.on_text(text, rx)
            except Exception as exc:                  # never into the engine
                self.last_error = f"a message could not be read: {exc}"
                return 0
            self._put(evs, rx)
            if any(isinstance(e, FeedNotice) and e.notice == "GAP" for e in evs):
                self._snap_wake.set()
            return len(evs)

    def handle_snapshot(self, symbol: str, snap: dict, rx: Optional[dt.datetime] = None) -> str:
        """A REST snapshot for a symbol waiting for one. '' when the book was rebuilt, else why not."""
        rx = rx or self._clock()
        with self._lock:
            evs, why = self.mapper.on_snapshot(symbol, snap, rx)
            self._put(evs, rx)
            return why

    def set_grid(self, symbol: str, mid: float, rx: Optional[dt.datetime] = None) -> float:
        """Choose and announce a symbol's analysis price step (once)."""
        rx = rx or self._clock()
        inst = self.instruments.get(symbol)
        g, how = choose_grid(mid, self._ticks.get(symbol, 0.0), self._sigma.get(symbol),
                             float(getattr(inst, "grid_bp", 2.0) or 2.0), float(getattr(inst, "price_grid", 0.0) or 0.0))
        with self._lock:
            self._put(self.mapper.set_grid(symbol, g, how, self._ticks.get(symbol, 0.0), rx), rx)
            return self.mapper.syms[symbol].grid

    def handle_disconnect(self, why: str, rx: Optional[dt.datetime] = None) -> None:
        rx = rx or self._clock()
        with self._lock:
            was = self.connected
            self.connected = False
            self.last_error = why
            if was:
                self._put(self.mapper.disconnected(rx, why), rx)

    def handle_connected(self, impl: str = "", rx: Optional[dt.datetime] = None) -> None:
        rx = rx or self._clock()
        with self._lock:
            if self.ever_connected:
                self.reconnects += 1
                self._put([FeedNotice("", rx, "RECONNECTED", f"{LABEL}: stream reconnected; rebuilding the books",
                                      SOURCE)], rx)
            self.connected = True
            self.ever_connected = True
            self.connect_failures = 0
            self.retry_in = 0.0
            self.last_error = ""
            self.impl = impl or self.impl
            self.mode = "ws"
        self._snap_wake.set()

    # ------------------------------------------------------------- REST --
    def _get(self, path: str, params: dict):
        """GET a public endpoint from the first base that answers (the one that worked last time first).

        After a 429 / 418 nothing is asked again until its Retry-After has passed, whatever wakes the threads
        (a recovery, a GAP, a reconnect): Binance answers requests made inside that pause with an IP ban."""
        wait = (self._rest_until - time.monotonic()) if self._rest_until is not None else 0.0
        if wait > 0:
            raise RateLimited(self._rest_limited or "Binance asked us to slow down", wait)
        q = urllib.parse.urlencode(params)
        last: Optional[Exception] = None
        n = len(self.rest_bases)
        for k in range(n):
            i = (self._rest_base_i + k) % n
            try:
                out = self._http(f"{self.rest_bases[i]}{path}?{q}")
                self._rest_base_i = i
                return out
            except RateLimited as exc:
                until = time.monotonic() + max(0.0, float(exc.retry_after or 0.0))
                self._rest_until = max(until, self._rest_until or 0.0)
                self._rest_limited = str(exc)
                raise
            except Exception as exc:
                last = exc
        raise last or ConnectionError("no REST endpoint configured")

    def _prepare(self, symbol: str) -> None:
        """Once per symbol: the price tick and the recent volatility (for the price step), and the clock check."""
        if symbol not in self._ticks:
            tick = 0.0
            try:
                info = self._get("/api/v3/exchangeInfo", {"symbol": symbol})
                for f in ((info.get("symbols") or [{}])[0].get("filters") or []):
                    if f.get("filterType") == "PRICE_FILTER":
                        tick = float(f.get("tickSize") or 0.0)
            except RateLimited:
                raise
            except Exception as exc:
                log.info("binance: no tick size for %s (%s)", symbol, exc)
            self._ticks[symbol] = tick
        if symbol not in self._sigma:
            sigma = None
            try:
                rows = self._get("/api/v3/klines", {"symbol": symbol, "interval": "1m", "limit": 120})
                sigma = two_minute_sigma([float(r[4]) for r in rows])
            except RateLimited:
                raise
            except Exception as exc:
                log.info("binance: no recent bars for %s (%s)", symbol, exc)
            self._sigma[symbol] = sigma
        if self.clock_offset_ms is None:
            try:
                t0 = time.time()
                srv = self._get("/api/v3/time", {})
                t1 = time.time()
                self.clock_offset_ms = float(srv["serverTime"]) - (t0 + t1) / 2.0 * 1000.0
            except RateLimited:
                raise
            except Exception:
                self.clock_offset_ms = None

    def snapshot_now(self, symbol: str) -> str:
        """Fetch a snapshot for one symbol and rebuild its book. '' when done, else why not."""
        self._prepare(symbol)
        snap = self._get("/api/v3/depth", {"symbol": symbol, "limit": self.depth_limit})
        rx = self._clock()
        if self.mapper.syms[symbol].grid <= 0:
            book = LocalBook()
            book.load(snap, self.depth_limit)
            mid = book.mid()
            if mid is None:
                return "the snapshot had no two-sided price"
            self.set_grid(symbol, mid, rx)
        return self.handle_snapshot(symbol, snap, rx)

    def rest_poll_once(self, stop: Optional[threading.Event] = None) -> int:
        """REST polling, one round: the new aggregate trades and a depth snapshot for every symbol."""
        n = 0
        for sym in list(self.symbols):
            if (stop or self._stop).is_set():
                break
            self._prepare(sym)
            s = self.mapper.syms[sym]
            params = {"symbol": sym, "limit": 1000}
            if s.last_agg is not None:
                params["fromId"] = s.last_agg + 1
            else:
                params["limit"] = 1                       # start from now: the past is not replayed
            rows = self._get("/api/v3/aggTrades", params) or []
            snap = self._get("/api/v3/depth", {"symbol": sym, "limit": self.rest_depth_limit})
            rx = self._clock()
            if s.grid <= 0:
                book = LocalBook()
                book.load(snap, self.rest_depth_limit)
                mid = book.mid()
                if mid is None:
                    continue
                self.set_grid(sym, mid, rx)
            with self._lock:
                evs = self.mapper.on_rest_depth(sym, snap, rx, self.rest_depth_limit)
                for r in rows:
                    evs += self.mapper.on_agg_trade(r, rx, sym)
                self._put(evs, rx)
                self.last_message = rx
                n += len(evs)
        return n

    # ---------------------------------------------------------- threads --
    def _sleep(self, seconds: float, ev: Optional[threading.Event] = None) -> None:
        if self._sleep_fn is not None:
            self._sleep_fn(seconds)
            return
        (ev or self._wake).wait(max(0.0, seconds))
        if ev is None:
            self._wake.clear()

    def _stream_url(self, base: str) -> str:
        names = "/".join(f"{s.lower()}@depth@100ms/{s.lower()}@aggTrade" for s in self.symbols)
        return f"{base}/stream?streams={names}"

    def _open(self, url: str):
        if self._ws_connect is not None:
            return self._ws_connect(url)
        from .wsclient import open_websocket
        return open_websocket(url, self.recv_timeout_s, self.prefer_library)

    def _ws_loop(self, stop: Optional[threading.Event] = None, max_attempts: Optional[int] = None) -> None:
        stop = stop or self._stop
        backoff = 1.0
        attempts = 0
        while not stop.is_set():
            if max_attempts is not None and attempts >= max_attempts:
                return
            attempts += 1
            if self.transport == "rest" or (self.transport == "auto" and self.connect_failures >= self.rest_after_failures
                                            and self.rest_after_failures > 0):
                self._rest_period(self.rest_period_s if self.transport != "rest" else float("inf"), stop)
                if stop.is_set():
                    return
                self.connect_failures = 0
            url = self._stream_url(self.ws_bases[self._ws_base_i % len(self.ws_bases)])
            try:
                conn = self._open(url)
            except Exception as exc:
                self.connect_failures += 1
                self._ws_base_i += 1                       # the next attempt tries the other address
                with self._lock:
                    self.last_error = f"{type(exc).__name__}: {exc}"[:200]
                    self.retry_in = backoff
                self._sleep(backoff)
                backoff = min(backoff * 2.0, 60.0)
                continue
            if stop.is_set():
                try:
                    conn.close()
                except Exception:
                    pass
                return
            self._conn = conn
            started = time.monotonic()
            self.handle_connected(getattr(conn, "impl", ""))
            why = self._session(conn, stop)
            try:
                conn.close()
            except Exception:
                pass
            self._conn = None
            if stop.is_set():
                return
            self.handle_disconnect(why)
            if time.monotonic() - started > 60:
                backoff = 1.0
            with self._lock:
                self.retry_in = backoff
            self._sleep(backoff)
            backoff = min(backoff * 2.0, 60.0)

    def _session(self, conn, stop: Optional[threading.Event] = None) -> str:
        stop = stop or self._stop
        while not stop.is_set():
            try:
                text = conn.recv()
            except Exception as exc:
                return f"{type(exc).__name__}: {exc}"[:200] if not stop.is_set() else "stopped"
            if text:
                self.handle_text(text)
        return "stopped"

    def _rest_period(self, seconds: float, stop: Optional[threading.Event] = None) -> None:
        """REST polling while the WebSocket cannot be reached (for ``seconds``, then the stream is tried again)."""
        stop = stop or self._stop
        with self._lock:
            self._rest_reason = self.last_error or "no connection"
            self.mode = "rest"
            self.mapper.rest_mode_reset()
        until = time.monotonic() + seconds
        while not stop.is_set() and time.monotonic() < until:
            t0 = time.monotonic()
            try:
                self.rest_poll_once(stop)
                with self._lock:
                    self.connected = True
                    self.ever_connected = True
                    self.retry_in = 0.0
            except RateLimited as exc:
                with self._lock:
                    self.last_error = str(exc)
                self._sleep(exc.retry_after)
                continue
            except Exception as exc:
                self.handle_disconnect(f"{type(exc).__name__}: {exc}"[:200])
                self._sleep(5.0)
                continue
            self._sleep(max(0.0, self.rest_poll_s - (time.monotonic() - t0)))
        with self._lock:
            self.mode = "ws"
            self.connected = False
            self.mapper.rest_mode_reset()

    def _snap_loop(self, stop: Optional[threading.Event] = None) -> None:
        stop = stop or self._stop
        backoff: dict[str, float] = {}
        while not stop.is_set():
            with self._lock:
                todo = self.mapper.needs_snapshot() if (self.connected and self.mode == "ws") else []
            for sym in todo:
                if stop.is_set():
                    return
                try:
                    why = self.snapshot_now(sym)
                except RateLimited as exc:
                    with self._lock:
                        self.last_error = str(exc)
                    self._sleep(exc.retry_after, self._snap_wake)
                    break
                except Exception as exc:
                    why = f"{type(exc).__name__}: {exc}"[:200]
                if why:
                    b = backoff.get(sym, 1.0)
                    backoff[sym] = min(b * 2.0, 30.0)
                    with self._lock:
                        s = self.mapper.syms.get(sym)
                        if s is not None and not s.synced:
                            s.why = f"rebuilding the book: {why} (trying again in {b:.0f} s)"
                    self._sleep(b, self._snap_wake)
                else:
                    backoff.pop(sym, None)
            self._snap_wake.wait(0.5)
            self._snap_wake.clear()
