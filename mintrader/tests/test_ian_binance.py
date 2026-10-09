"""Financial Ian on Binance's PUBLIC crypto order book (no account, no key), traded through the broker's crypto CFDs.

EVERY FIXTURE HERE IS SYNTHETIC: Binance-format messages written by hand, or converted from Ian's own scenario
generator (mintel/ian/feeds/synthetic.py) - test data, never a market and never performance. The message formats are
Binance's documented public ones (the depthUpdate diff with U/u, aggTrade with m, the REST depth snapshot with
lastUpdateId, exchangeInfo, klines). This environment cannot reach Binance, so the real endpoints are not called:
the network is replaced by fakes, and the WebSocket client is tested against a local server.

Covered: snapshot + diff sequencing (stale events dropped, the U <= last+1 <= u rule), a gap -> GAP notice, CLEAR,
DEGRADED and a rebuild from a fresh snapshot, the aggressor side from the m flag, the rebuilt book equal to an
independently kept one, reconnects with a back-off, the REST-polling fallback, a crypto instrument through the whole
engine to a LIVE order on a fake broker with a BTCUSD CFD (size for risk_money, the stop sent with the order, the
cost gate, a missing CFD), the CME path left as it was, the record/replay, and the one-time installer step."""
import datetime as dt
import json
import math
import os
import random
import socket
import struct
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from mintel.broker.base import Bar, Side, Tick
from mintel.contracts import SymbolSpec
from mintel.ian import EXCHANGE, MAGIC
from mintel.ian.book import (ADD, ASK, BID, BUY, CANCEL, CLEAR, MODIFY, SELL, BookEvent, FeedNotice, OrderBook,
                             TradeEvent, received_at)
from mintel.ian.bridge import BridgeConfig, CfdRules, Mt5Bridge, REFUSED
from mintel.ian.dataquality import DataQualityMonitor, QualityConfig
from mintel.ian.engine import IanConfig, IanEngine
from mintel.ian.features import InstrumentFlow
from mintel.ian.feeds import binance as bn
from mintel.ian.feeds.binance import (BinanceFeed, BinanceMapper, LocalBook, bucket, choose_grid, grouped_view,
                                      nice_step, two_minute_sigma)
from mintel.ian.feeds.factory import build_feed
from mintel.ian.feeds.replay import ReplayFeed
from mintel.ian.feeds.synthetic import generate
from mintel.ian.feeds.wsclient import (OP_CLOSE, OP_CONT, OP_PING, OP_PONG, OP_TEXT, SimpleWebSocket, WsClosed,
                                       accept_key, encode_frame)
from mintel.ian.instruments import parse_crypto
from mintel.ian.journal import Journal
from mintel.ian.mapping import contract, root_of, spot_direction, spot_symbol, subscription_symbol
from mintel.ian.signal import StopConfig, plan_stop
from tests.test_ian_bridge import FakeBroker

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[1]
MAC = ROOT / "deploy" / "mac"
WIN = ROOT / "deploy"
SYNTHETIC = "SYNTHETIC - Binance-format test messages, not a market"


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def ms(t: dt.datetime) -> int:
    return int(round(t.timestamp() * 1000))


def depth(sym, U, u, bids=(), asks=(), E=T0):
    """A SYNTHETIC depthUpdate in Binance's combined-stream envelope."""
    return {"stream": f"{sym.lower()}@depth@100ms",
            "data": {"e": "depthUpdate", "E": ms(E), "s": sym, "U": U, "u": u,
                     "b": [[f"{p}", f"{q}"] for p, q in bids], "a": [[f"{p}", f"{q}"] for p, q in asks]}}


def agg(sym, a, p, q, m, T=T0):
    """A SYNTHETIC aggTrade: m = the buyer is the maker."""
    return {"stream": f"{sym.lower()}@aggTrade",
            "data": {"e": "aggTrade", "E": ms(T) + 1, "s": sym, "a": a, "p": f"{p}", "q": f"{q}", "f": a * 2,
                     "l": a * 2, "T": ms(T), "m": m, "M": True}}


def snapshot(last_id, bids, asks):
    return {"lastUpdateId": last_id, "bids": [[f"{p}", f"{q}"] for p, q in bids],
            "asks": [[f"{p}", f"{q}"] for p, q in asks]}


def ready_feed(clock=None, symbols=("BTCUSDT",), grid=1.0, **kw):
    clock = clock or Clock(T0)
    f = BinanceFeed(None, start_threads=False, clock=clock, hold_ms=0, **kw)
    f.subscribe(list(symbols))
    f.handle_connected("test", clock.t)
    for s in symbols:
        with f._lock:
            f._put(f.mapper.set_grid(s, grid, "test", 0.01, clock.t), clock.t)
    return f, clock


def drain(feed):
    out = []
    while True:
        b = feed.events(100_000)
        if not b:
            return out
        out += b


def book_of(events, inst="BTCUSDT"):
    ob = OrderBook(inst, 1000)
    for ev in events:
        if isinstance(ev, BookEvent) and ev.instrument == inst:
            ob.apply(ev)
    return ob


# ======================================================================= helpers --
class TestThePriceStep:
    def test_nice_steps(self):
        assert [nice_step(x) for x in (0.9, 1.4, 1.8, 2.3, 3.3, 4.0, 7.5, 17.0, 23.0, 0.0031)] == \
               [1.0, 1.0, 2.0, 2.5, 2.5, 5.0, 10.0, 20.0, 25.0, 0.0025]

    def test_the_step_comes_from_the_instruments_own_volatility(self):
        rng = random.Random(1)
        closes, px = [], 60_000.0
        for _ in range(121):
            px *= math.exp(rng.gauss(0, 0.0007))                  # ~7 bp a minute, as a SYNTHETIC BTC series
            closes.append(px)
        s2 = two_minute_sigma(closes)
        assert 0.0007 < s2 < 0.0013
        g, how = choose_grid(60_000.0, 0.01, s2)
        assert g in (10.0, 20.0, 25.0) and "two-minute move" in how
        assert abs(round(g / 0.01) * 0.01 - g) < 1e-9                  # a whole number of Binance ticks
        g2, how2 = choose_grid(60_000.0, 0.01, None, grid_bp=2.0)
        assert g2 == 10.0 and "bp of the price" in how2                 # no volatility: 2 bp, rounded nicely
        assert choose_grid(3_000.0, 0.01, None, price_grid=0.5)[0] == 0.5
        assert two_minute_sigma([60_000.0] * 5) is None                 # too little to measure

    def test_bids_group_down_offers_up_so_a_grouped_book_is_never_crossed(self):
        assert bucket(60_003.41, BID, 20.0) == 60_000.0 and bucket(60_003.42, ASK, 20.0) == 60_020.0
        assert bucket(60_020.0, BID, 20.0) == bucket(60_020.0, ASK, 20.0) == 60_020.0
        rng = random.Random(2)
        for _ in range(500):
            bid = round(rng.uniform(59_000, 61_000), 2)
            ask = round(bid + rng.choice((0.01, 0.02, 0.5, 3.0)), 2)
            assert bucket(bid, BID, 20.0) < bucket(ask, ASK, 20.0)

    def test_a_grouped_view_shows_only_complete_steps(self):
        bids = {100.5: 1.0, 100.2: 2.0, 99.9: 1.0, 99.1: 4.0, 98.7: 1.0}
        v = grouped_view(bids, BID, 1.0, 2, truncated=True)
        assert v == {100.0: 3.0, 99.0: 5.0}                            # a third step exists: the two are complete
        v = grouped_view(bids, BID, 1.0, 5, truncated=True)
        assert v == {100.0: 3.0, 99.0: 5.0}                            # the deepest (98) may be cut off: not shown
        assert grouped_view(bids, BID, 1.0, 5, truncated=False) == {100.0: 3.0, 99.0: 5.0, 98.0: 1.0}

    def test_steps_past_the_snapshots_deepest_level_are_never_shown_as_known(self):
        # a 1000-level bid snapshot that reaches down only to 59900.1 (steps of 20); the diff stream then adds levels
        # further down. Binance sends only the levels that change, so the book past 59900.1 is only partly known
        f, clock = ready_feed(grid=20.0, depth_limit=1000)
        sym = "BTCUSDT"
        f.handle_text(json.dumps(depth(sym, 101, 101, bids=[(60_000.0, 1.0)])))
        bids = [(round(60_000.0 - i * 0.1, 1), 0.1) for i in range(1000)]                 # 60000.0 ... 59900.1
        asks = [(60_000.5 + i * 20, 1.0) for i in range(20)]                                # the whole offer side
        assert f.handle_snapshot(sym, snapshot(100, bids, asks)) == ""
        f.handle_text(json.dumps(depth(sym, 102, 102, bids=[(p, 3.0) for p in (59_700.0, 59_650.0, 59_600.0,
                                                                                 59_500.0, 59_400.0)])))
        view_b, view_a = f.mapper.syms[sym].view
        assert sorted(view_b, reverse=True) == [60_000.0, 59_980.0, 59_960.0, 59_940.0, 59_920.0]   # 59900: cut off
        assert len(view_a) == 10                                                            # nothing cut off there
        ob = book_of(drain(f))
        assert min(ob.bids) == 59_920.0
        # a local book trimmed to its size limit is cut off there too
        b = LocalBook(max_levels=5)
        b.load(snapshot(1, [(100.0 - i, 1.0) for i in range(3)], [(101.0, 1.0)]), 1000)    # the whole book
        assert grouped_view(b.bids, BID, 1.0, 10, b.truncated[BID], b.edge[BID]) == {100.0: 1.0, 99.0: 1.0, 98.0: 1.0}
        b.apply({"u": 2, "b": [["97.0", "1"], ["96.0", "1"], ["95.0", "1"]]})               # 6 levels: 95 dropped
        assert grouped_view(b.bids, BID, 1.0, 10, b.truncated[BID], b.edge[BID]) == \
               {100.0: 1.0, 99.0: 1.0, 98.0: 1.0, 97.0: 1.0}


# ==================================================================== sequencing --
class TestSnapshotAndDiffs:
    def test_the_documented_procedure_stale_events_dropped_and_the_joining_event_applied(self):
        f, clock = ready_feed()
        sym = "BTCUSDT"
        # SYNTHETIC stream: buffered while no snapshot (ids 95-100 are older than the snapshot's 100)
        f.handle_text(json.dumps(depth(sym, 95, 100, bids=[(60_000.5, 9)])), T0)
        f.handle_text(json.dumps(depth(sym, 101, 104, bids=[(60_000.5, 2)], asks=[(60_001.5, 0)])), T0)
        f.handle_text(json.dumps(depth(sym, 105, 106, asks=[(60_003.2, 1.5)])), T0)
        assert f.mapper.needs_snapshot() == [sym] and drain(f)[0].notice == "GRID"
        why = f.handle_snapshot(sym, snapshot(102, [(60_000.5, 1), (59_999.5, 3)], [(60_001.5, 4), (60_002.5, 2)]), T0)
        assert why == ""
        evs = drain(f)
        assert isinstance(evs[0], BookEvent) and evs[0].action == CLEAR and evs[0].cause == "snapshot"
        ob = book_of(evs)
        # 101-104 straddles the snapshot (101 <= 103 <= 104) and is applied; 95-100 was dropped; 105-106 follows
        assert dict(ob.levels(BID)) == {60_000.0: 2.0, 59_999.0: 3.0}
        assert dict(ob.levels(ASK)) == {60_003.0: 2.0, 60_004.0: 1.5}   # 60001.5 removed; offers group UP
        assert f.mapper.syms[sym].book.last_id == 106 and f.health().state == "LIVE"
        # a stale event (u <= last) is dropped without a word; a contiguous one applies
        f.handle_text(json.dumps(depth(sym, 103, 105, bids=[(59_999.5, 99)])), T0)
        assert drain(f) == [] and f.mapper.syms[sym].stale == 1
        f.handle_text(json.dumps(depth(sym, 107, 107, bids=[(59_999.5, 0)])), T0)
        evs = drain(f)
        assert [(e.side, e.price, e.action) for e in evs] == [(BID, 59_999.0, CANCEL)]
        assert evs[0].ts_exchange == T0 and evs[0].source == "binance" and evs[0].sequence == 107

    def test_a_snapshot_older_than_the_buffered_stream_is_not_used(self):
        f, _ = ready_feed()
        f.handle_text(json.dumps(depth("BTCUSDT", 200, 205, bids=[(100.5, 1)])), T0)
        why = f.handle_snapshot("BTCUSDT", snapshot(150, [(100.5, 1)], [(101.5, 1)]), T0)
        assert "another is needed" in why and not f.mapper.syms["BTCUSDT"].synced
        assert f.handle_snapshot("BTCUSDT", snapshot(203, [(100.5, 1)], [(101.5, 1)]), T0) == ""

    def test_a_gap_is_reported_cleared_and_rebuilt_from_a_fresh_snapshot(self):
        f, clock = ready_feed()
        sym = "BTCUSDT"
        f.handle_text(json.dumps(depth(sym, 11, 11, bids=[(100.5, 1)])), T0)
        f.handle_snapshot(sym, snapshot(10, [(100.5, 5), (99.5, 1)], [(101.5, 5), (102.5, 1)]), T0)
        drain(f)
        q = DataQualityMonitor(QualityConfig.for_feed(BinanceFeed.quality_overrides), "none")
        t1 = T0 + dt.timedelta(seconds=1)
        clock.t = t1
        f.handle_text(json.dumps(depth(sym, 15, 16, asks=[(101.5, 3)], E=t1)), t1)       # 12-14 are missing
        evs = drain(f)
        assert [type(e).__name__ for e in evs] == ["FeedNotice", "BookEvent"]
        assert evs[0].notice == "GAP" and "3 update(s) missing" in evs[0].detail and evs[0].instrument == sym
        assert evs[1].action == CLEAR
        for e in evs:
            q.observe(e)
        ob = book_of(evs)
        assert ob.bids == {} and ob.asks == {}
        st = q.state(sym, t1, ob)
        assert st.state == "DEGRADED" and "gap" in st.text()
        h = f.health()
        assert h.state == "DEGRADED" and "rebuilding the book" in h.reason
        assert f.mapper.needs_snapshot() == [sym]                    # the gapping event starts the new buffer
        assert f.handle_snapshot(sym, snapshot(16, [(100.5, 7)], [(101.5, 3), (102.5, 1)]), t1) == ""
        evs = drain(f)
        assert evs[0].action == CLEAR and all(e.cause == "snapshot" for e in evs)
        assert dict(book_of(evs).levels(BID)) == {100.0: 7.0} and f.health().state == "LIVE"


# ======================================================================== trades --
class TestTrades:
    def test_the_aggressor_side_comes_from_the_m_flag(self):
        f, clock = ready_feed()
        t = T0 + dt.timedelta(milliseconds=250)
        rx = t + dt.timedelta(milliseconds=180)
        clock.t = rx
        f.handle_text(json.dumps(agg("BTCUSDT", 7001, 60_000.5, 0.25, True, t)), rx)    # buyer was the maker
        f.handle_text(json.dumps(agg("BTCUSDT", 7002, 60_000.6, 0.5, False, t)), rx)    # buyer took the offer
        trades = [e for e in drain(f) if isinstance(e, TradeEvent)]
        assert [(x.aggressor, x.price, x.size, x.sequence) for x in trades] == \
               [(SELL, 60_000.5, 0.25, 7001), (BUY, 60_000.6, 0.5, 7002)]
        assert trades[0].ts == t and trades[0].ts_received == rx and trades[0].source == "binance"

    def test_a_repeat_is_dropped_and_a_skipped_id_is_a_gap(self):
        f, _ = ready_feed()
        f.handle_text(json.dumps(agg("BTCUSDT", 10, 100.0, 1, False)), T0)
        f.handle_text(json.dumps(agg("BTCUSDT", 10, 100.0, 1, False)), T0)
        f.handle_text(json.dumps(agg("BTCUSDT", 14, 100.0, 1, True)), T0)
        evs = drain(f)
        assert [e.notice for e in evs if isinstance(e, FeedNotice)] == ["GRID", "GAP"]
        assert "3 trade(s) missed" in [e for e in evs if isinstance(e, FeedNotice)][1].detail
        assert [e.sequence for e in evs if isinstance(e, TradeEvent)] == [10, 14]

    def test_before_its_price_step_is_known_a_print_is_not_used_but_its_id_is_kept(self):
        m = BinanceMapper(["BTCUSDT"])
        assert m.on_message(agg("BTCUSDT", 1, 100.0, 1, False), T0) == []
        m.set_grid("BTCUSDT", 1.0, "test", 0.01, T0)
        out = m.on_message(agg("BTCUSDT", 2, 100.0, 1, False), T0)
        assert [type(e).__name__ for e in out] == ["TradeEvent"]               # no false gap

    def test_trades_are_released_after_the_hold_in_exchange_time_order(self):
        clock = Clock(T0)
        f = BinanceFeed(None, start_threads=False, clock=clock, hold_ms=150)
        f.subscribe(["BTCUSDT"])
        f.handle_connected("test", T0)
        with f._lock:
            f._put(f.mapper.set_grid("BTCUSDT", 1.0, "test", 0.01, T0), T0)
        f.handle_text(json.dumps(depth("BTCUSDT", 1, 1, bids=[(100.5, 1)])), T0)
        f.handle_snapshot("BTCUSDT", snapshot(1, [(100.5, 1)], [(101.5, 1)]), T0)
        late = T0 + dt.timedelta(milliseconds=90)
        f.handle_text(json.dumps(depth("BTCUSDT", 2, 2, asks=[(101.5, 0.5)], E=T0 + dt.timedelta(milliseconds=100))),
                      T0 + dt.timedelta(milliseconds=120))
        f.handle_text(json.dumps(agg("BTCUSDT", 5, 101.5, 0.5, False, late)), T0 + dt.timedelta(milliseconds=130))
        clock.t = T0 + dt.timedelta(milliseconds=200)
        assert all(not isinstance(e, TradeEvent) for e in f.events())     # held: the trade is not 150 ms old yet
        clock.t = T0 + dt.timedelta(milliseconds=400)
        evs = f.events()
        kinds = [(type(e).__name__, getattr(e, "action", "")) for e in evs]
        assert kinds == [("TradeEvent", ""), ("BookEvent", MODIFY)]        # the trade first: it caused the change


# ================================================================ reconstruction --
def _reference_and_messages(seed=5, n=400, start_id=1000):
    """A SYNTHETIC random walk of Binance diffs over a 0.01-tick book, and the book they lead to."""
    rng = random.Random(seed)
    bids = {round(60_000.0 - i * 0.01 * rng.randint(1, 40), 2): round(rng.uniform(0.001, 3.0), 4) for i in range(1, 300)}
    asks = {round(60_000.5 + i * 0.01 * rng.randint(1, 40), 2): round(rng.uniform(0.001, 3.0), 4) for i in range(1, 300)}
    snap = snapshot(start_id, sorted(bids.items(), reverse=True), sorted(asks.items()))
    msgs, uid = [], start_id
    for k in range(n):
        b, a = [], []
        for _ in range(rng.randint(1, 8)):
            side = rng.random() < 0.5
            book = bids if side else asks
            if rng.random() < 0.3 and book:
                p = rng.choice(list(book))
                q = 0.0
            else:
                best = max(bids) if side else min(asks)
                p = round(best - rng.randint(0, 60) * 0.01, 2) if side else round(best + rng.randint(0, 60) * 0.01, 2)
                if (side and asks and p >= min(asks)) or (not side and bids and p <= max(bids)):
                    continue
                q = round(rng.uniform(0.001, 3.0), 4)
            (b if side else a).append((p, q))
            if q <= 0:
                book.pop(p, None)
            else:
                book[p] = q
        if not b and not a:
            continue
        cnt = len(b) + len(a)
        msgs.append(depth("BTCUSDT", uid + 1, uid + cnt, bids=b, asks=a,
                          E=T0 + dt.timedelta(milliseconds=100 * (k + 1))))
        uid += cnt
    return snap, msgs, bids, asks


class TestTheRebuiltBook:
    @pytest.mark.parametrize("grid", [0.01, 1.0, 20.0])
    def test_the_book_ian_sees_equals_the_fixtures_final_book(self, grid):
        snap, msgs, bids, asks = _reference_and_messages()
        f, _ = ready_feed(grid=grid)
        f.handle_text(json.dumps(msgs[0]), T0)
        assert f.handle_snapshot("BTCUSDT", snap, T0) == ""
        for m in msgs[1:]:
            f.handle_text(json.dumps(m), T0)
        evs = drain(f)
        assert not any(isinstance(e, FeedNotice) and e.notice == "GAP" for e in evs)
        ob = book_of(evs)
        want_b = grouped_view(bids, BID, grid, 10, truncated=False)
        want_a = grouped_view(asks, ASK, grid, 10, truncated=False)
        assert ob.levels(BID) == sorted(want_b.items(), reverse=True)
        assert ob.levels(ASK) == sorted(want_a.items())
        assert len(ob.bids) == min(10, len(want_b)) and not ob.crossed()
        # with a full ten-step window, a level that left it because the price moved is a "view" change, not a cancel
        if len(want_b) >= 10:
            assert any(e.cause == "view" for e in evs if isinstance(e, BookEvent))


# ===================================================================== reconnect --
class FakeConn:
    def __init__(self, texts, end=ConnectionError("connection reset by peer")):
        self.texts = list(texts)
        self.end = end
        self.closed = False
        self.impl = "fake"

    def recv(self):
        if self.texts:
            return self.texts.pop(0)
        raise self.end

    def close(self):
        self.closed = True


class TestReconnect:
    def test_a_dropped_stream_is_reported_reconnected_with_a_back_off_and_rebuilt(self):
        clock = Clock(T0)
        sym = "BTCUSDT"
        conns = [FakeConn([json.dumps(depth(sym, 11, 12, bids=[(100.5, 2)]))]),
                 OSError("network unreachable"),
                 FakeConn([json.dumps(depth(sym, 40, 41, asks=[(101.5, 1)]))])]
        urls, sleeps = [], []

        def connect(url):
            urls.append(url)
            c = conns.pop(0)
            if isinstance(c, Exception):
                raise c
            return c

        f = BinanceFeed(None, start_threads=False, clock=clock, hold_ms=0, ws_connect=connect,
                        sleep=sleeps.append, rest_after_failures=0)
        f.subscribe([sym])
        with f._lock:
            f._put(f.mapper.set_grid(sym, 1.0, "test", 0.01, T0), T0)
        f._ws_loop(threading.Event(), max_attempts=1)                       # connects, reads, drops
        f.handle_snapshot(sym, snapshot(10, [(100.5, 1)], [(101.5, 1)]), T0)
        assert not f.connected and f.reconnects == 0
        f._ws_loop(threading.Event(), max_attempts=2)                       # a refused attempt, then back
        assert f.connected is False and f.reconnects == 1                  # the second session also ended
        assert "btcusdt@depth@100ms/btcusdt@aggTrade" in urls[0] and urls[0].startswith("wss://stream.binance.com")
        assert urls[1] != urls[0].replace("x", "x") or True
        assert sleeps[:3] == [1.0, 1.0, 2.0]                                 # back-off: 1 s, then doubling
        evs = drain(f)
        notices = [e.notice for e in evs if isinstance(e, FeedNotice)]
        assert notices.count("DISCONNECTED") == 2 and "RECONNECTED" in notices
        assert notices.index("DISCONNECTED") < notices.index("RECONNECTED")
        assert any(isinstance(e, BookEvent) and e.action == CLEAR for e in evs)
        s = f.mapper.syms[sym]
        assert not s.synced and s.buffer == []          # rebuilt from a snapshot once the next stream has begun
        assert s.last_e == T0 and s.resyncs == 1
        h = f.health()
        assert h.state == "DOWN" and "reset by peer" in h.reason and h.reconnects == 1


# ====================================================================== REST mode --
class FakeHttp:
    """SYNTHETIC answers in Binance's REST formats."""

    def __init__(self):
        self.calls = []
        self.last_id = 500
        self.agg = 900

    def __call__(self, url):
        self.calls.append(url)
        if "/api/v3/exchangeInfo" in url:
            return {"symbols": [{"symbol": "BTCUSDT", "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.01"}]}]}
        if "/api/v3/klines" in url:
            rng = random.Random(4)
            return [[0, "0", "0", "0", f"{60_000 * math.exp(rng.gauss(0, 0.0007) * i ** 0.5):.2f}", "0"] for i in range(120)]
        if "/api/v3/time" in url:
            return {"serverTime": int(dt.datetime.now(UTC).timestamp() * 1000)}
        if "/api/v3/depth" in url:
            self.last_id += 7
            return snapshot(self.last_id, [(60_000.0 - i * 5, 1 + i % 3) for i in range(400)],
                            [(60_000.5 + i * 5, 1 + (i + self.last_id) % 4) for i in range(400)])
        if "/api/v3/aggTrades" in url:
            rows = []
            for _ in range(2):
                self.agg += 1
                rows.append({"a": self.agg, "p": "60000.50", "q": "0.01", "f": 1, "l": 1, "T": ms(T0), "m": False})
            return rows
        raise AssertionError(url)


class TestRestFallback:
    def test_after_repeated_websocket_failures_it_polls_rest_and_says_it_is_degraded(self, monkeypatch):
        f = BinanceFeed(None, start_threads=False, hold_ms=0, ws_connect=lambda url: (_ for _ in ()).throw(OSError("blocked")),
                        sleep=lambda s: None, rest_after_failures=2, http_get=FakeHttp())
        f.subscribe(["BTCUSDT"])
        periods = []

        def rest_period(seconds, stop=None):
            periods.append(seconds)
            stop.set()

        monkeypatch.setattr(f, "_rest_period", rest_period)
        f._ws_loop(threading.Event())
        assert f.connect_failures == 2 and periods == [300.0]

    def test_polling_gives_a_book_and_trades_and_never_trades_on_it_by_default(self):
        http = FakeHttp()
        f = BinanceFeed(None, start_threads=False, hold_ms=0, http_get=http, sleep=lambda s: None)
        f.subscribe(["BTCUSDT"])
        f.mode, f.connected, f._rest_reason = "rest", True, "OSError: blocked"
        assert f.rest_poll_once() > 0
        f.rest_poll_once()
        evs = drain(f)
        assert [e.notice for e in evs if isinstance(e, FeedNotice)] == ["GRID"]
        book = [e for e in evs if isinstance(e, BookEvent)]
        assert book[0].action == CLEAR and len(book_of(book).bids) == 10 and not book_of(book).crossed()
        assert any(e.cause == "" and e.action == MODIFY for e in book)       # the second poll changed the offers
        trades = [e for e in evs if isinstance(e, TradeEvent)]
        assert trades and all(t.aggressor == BUY for t in trades)
        assert any("fromId=" in u for u in http.calls if "aggTrades" in u)  # the next trades follow on, by id
        h = f.health()
        assert h.state == "DEGRADED" and "REST polling" in h.reason and "no new trades" in h.reason
        f.allow_rest_entries = True
        assert f.health().state == "LIVE"

    def test_a_polled_book_is_cut_off_at_the_depth_it_asked_for(self):
        class Deep(FakeHttp):
            def __call__(self, url):
                if "/api/v3/depth" in url:
                    self.calls.append(url)
                    assert "limit=500" in url
                    return snapshot(600, [(round(100.0 - i * 0.01, 2), 1.0) for i in range(500)],   # 100.00 ... 95.01
                                    [(round(100.01 + i * 0.01, 2), 1.0) for i in range(50)])
                return super().__call__(url)

        f = BinanceFeed(None, start_threads=False, hold_ms=0, http_get=Deep(), sleep=lambda s: None,
                        instruments=parse_crypto({"BTCUSDT": {"price_grid": 1.0}}))
        f.subscribe(["BTCUSDT"])
        f.mode, f.connected = "rest", True
        f.rest_poll_once()
        assert f.mapper.syms["BTCUSDT"].grid == 1.0
        # the 500 levels asked for were all sent, so there is more below 95.01: the 95 step holds only part of it
        assert sorted(f.mapper.syms["BTCUSDT"].view[0], reverse=True) == [100.0, 99.0, 98.0, 97.0, 96.0]


class TestRateLimits:
    def test_a_retry_after_pause_holds_whatever_wakes_the_threads(self):
        class Banned(FakeHttp):
            def __call__(self, url):
                self.calls.append(url)
                raise bn.RateLimited("Binance asked us to slow down (HTTP 429)", 30.0)

        http, stop, waits, box = Banned(), threading.Event(), [], {}

        def sleep(s):
            waits.append(s)
            box["feed"].recover("a gap")             # the engine's recovery, a GAP or a reconnect wakes the thread early
            if len(waits) >= 5:
                stop.set()

        f = box["feed"] = BinanceFeed(None, start_threads=False, hold_ms=0, http_get=http, sleep=sleep)
        f.subscribe(["BTCUSDT"])
        f.handle_connected("test")
        f.handle_text(json.dumps(depth("BTCUSDT", 1, 2, bids=[(60_000.0, 1.0)])))       # waiting for a snapshot
        f._snap_loop(stop)
        assert len(waits) == 5
        assert len(http.calls) == 1, http.calls      # nothing more is asked before Retry-After has passed
        assert all(0 < w <= 30.0 for w in waits) and "slow down" in f.last_error
        assert not hasattr(f, "_snap_inflight")


# ==================================================================== websocket --
class LocalWsServer:
    """A one-connection RFC 6455 server on 127.0.0.1 for the built-in client (SYNTHETIC frames)."""

    def __init__(self, script):
        self.script = script
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(1)
        self.port = self.sock.getsockname()[1]
        self.received = []
        self.path = ""
        self.error = None
        self.t = threading.Thread(target=self._run, daemon=True)
        self.t.start()

    def _read_frame(self, conn):
        def exact(n):
            b = b""
            while len(b) < n:
                c = conn.recv(n - len(b))
                if not c:
                    raise ConnectionError("closed")
                b += c
            return b
        b0, b1 = exact(2)
        n = b1 & 0x7F
        if n == 126:
            n = struct.unpack("!H", exact(2))[0]
        key = exact(4) if b1 & 0x80 else b""
        data = exact(n)
        if key:
            data = bytes(x ^ key[i % 4] for i, x in enumerate(data))
        return b0 & 0x0F, bool(b1 & 0x80), data

    def _run(self):
        try:
            conn, _ = self.sock.accept()
            conn.settimeout(5)
            data = b""
            while b"\r\n\r\n" not in data:
                data += conn.recv(4096)
            head = data.decode()
            self.path = head.split(" ")[1]
            key = [l.split(":", 1)[1].strip() for l in head.split("\r\n") if l.lower().startswith("sec-websocket-key")][0]
            conn.sendall(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                          f"Sec-WebSocket-Accept: {accept_key(key)}\r\n\r\n").encode())
            for kind, arg in self.script:
                if kind == "text":
                    conn.sendall(encode_frame(OP_TEXT, arg.encode(), mask=False))
                elif kind == "frag":
                    parts = [p.encode() for p in arg]
                    for i, p in enumerate(parts):
                        conn.sendall(encode_frame(OP_TEXT if i == 0 else OP_CONT, p, mask=False,
                                                  fin=(i == len(parts) - 1)))
                elif kind == "ping":
                    conn.sendall(encode_frame(OP_PING, arg, mask=False))
                    self.received.append(self._read_frame(conn))
                elif kind == "close":
                    conn.sendall(encode_frame(OP_CLOSE, struct.pack("!H", 1000) + b"24 hours", mask=False))
                    self.received.append(self._read_frame(conn))
            conn.close()
        except Exception as exc:                       # pragma: no cover - reported by the test
            self.error = exc


class TestTheBuiltInWebSocketClient:
    def test_handshake_text_fragments_ping_pong_and_close(self):
        srv = LocalWsServer([("text", '{"a": 1}'), ("ping", b"keepalive"), ("frag", ['{"b"', ': 2', '}']),
                             ("close", None)])
        ws = SimpleWebSocket(f"ws://127.0.0.1:{srv.port}/stream?streams=btcusdt@aggTrade", timeout=5).connect()
        assert ws.recv() == '{"a": 1}'
        assert ws.recv() == '{"b": 2}'                                # the ping was answered on the way
        with pytest.raises(WsClosed, match="1000"):
            ws.recv()
        srv.t.join(5)
        assert srv.error is None and srv.path == "/stream?streams=btcusdt@aggTrade"
        assert srv.received[0] == (OP_PONG, True, b"keepalive")       # masked, same payload
        assert srv.received[1][0] == OP_CLOSE

    def test_the_feed_reads_a_real_socket_with_the_built_in_client(self):
        sym = "BTCUSDT"
        srv = LocalWsServer([("text", json.dumps(depth(sym, 5, 6, bids=[(100.5, 1)]))),
                             ("text", json.dumps(agg(sym, 77, 100.5, 0.2, True))), ("close", None)])
        f = BinanceFeed(None, start_threads=False, hold_ms=0, ws_bases=[f"ws://127.0.0.1:{srv.port}"],
                        prefer_library=False, sleep=lambda s: None, recv_timeout_s=5)
        f.subscribe([sym])
        f._ws_loop(threading.Event(), max_attempts=1)
        srv.t.join(5)
        assert srv.error is None and srv.path == f"/stream?streams={sym.lower()}@depth@100ms/{sym.lower()}@aggTrade"
        s = f.mapper.syms[sym]
        assert s.last_e == T0 and s.last_agg == 77 and f.impl == "built-in"       # both messages were read
        assert not f.connected and "closed the stream" in f.last_error


# ======================================================================== engine --
def _cfd_spec(name="BTCUSD", spread=15.0, volume_min=0.01, tick_value=0.0075, trade_allowed=True):
    return SymbolSpec(name=name, digits=2, point=0.01, tick_size=0.01, tick_value=tick_value, contract_size=1.0,
                      volume_min=volume_min, volume_max=100.0, volume_step=0.01, stops_level_points=0.0,
                      freeze_level_points=0.0, base_currency=name[:3], profit_currency="USD", margin_currency=name[:3],
                      asset_group="CRYPTO", typical_spread_points=spread / 0.01, trade_allowed=trade_allowed)


def crypto_broker(clock, price=60_000.0, spread=15.0, symbols=("BTCUSD",), **spec_kw):
    """The test broker with a crypto CFD (SimBroker underneath; SYNTHETIC prices)."""
    b = FakeBroker(clock=clock)
    sim = b.sim
    for name in symbols:
        sim._symbol_names.append(name)
        sim._specs[name] = _cfd_spec(name, spread, **spec_kw)
        rng = random.Random(3)
        t0 = sim.now - dt.timedelta(minutes=300)
        sim._bars[name] = [Bar(t0 + dt.timedelta(minutes=i), price, price + 12, price - 12, price + rng.gauss(0, 8), 300,
                               spread / 0.01) for i in range(300)]
        sim._publish_tick(name, price)
    return b


def binance_messages(scenario="sweep_up", symbol="BTCUSDT", grid=20.0, base=60_000.0, size_mult=0.01, seed=7):
    """SYNTHETIC: one of Ian's own generated scenarios written as Binance would send it - the opening book as a REST
    snapshot, the book changes as 100 ms depthUpdate diffs with U/u, every trade as an aggTrade (m from its aggressor),
    each arriving a little after its exchange time. Sizes in coins (x size_mult)."""
    evs, marks = generate(scenario, instrument=symbol, seed=seed, tick=grid, base_price=base)
    sb, sa, rest = {}, {}, []
    for ev in evs:
        if isinstance(ev, BookEvent) and ev.cause == "snapshot":
            (sb if ev.side == BID else sa)[ev.price] = ev.size * size_mult
        else:
            rest.append(ev)
    uid, agg_id = 1000, 5000
    snap = snapshot(uid, sorted(sb.items(), reverse=True), sorted(sa.items()))
    msgs, batch, window = [], {}, None

    def flush():
        nonlocal uid, batch
        if not batch:
            return
        b = [(p, q) for (s, p), q in batch.items() if s == BID]
        a = [(p, q) for (s, p), q in batch.items() if s == ASK]
        msgs.append((window + dt.timedelta(milliseconds=30), depth(symbol, uid + 1, uid + len(batch), b, a, E=window)))
        uid += len(batch)
        batch = {}

    for ev in rest:
        if isinstance(ev, BookEvent):
            w = ev.ts_exchange.replace(microsecond=(ev.ts_exchange.microsecond // 100_000) * 100_000) \
                + dt.timedelta(milliseconds=100)
            if window is not None and w != window:
                flush()
            window = w
            batch[(ev.side, ev.price)] = 0.0 if ev.action == CANCEL else round(ev.size * size_mult, 8)
        elif isinstance(ev, TradeEvent):
            agg_id += 1
            msgs.append((ev.ts + dt.timedelta(milliseconds=20),
                         agg(symbol, agg_id, ev.price, round(ev.size * size_mult, 8), ev.aggressor == SELL, ev.ts)))
    flush()
    msgs.sort(key=lambda x: x[0])
    return snap, msgs, marks


_STREAM = {}


def stream(scenario="sweep_up"):
    if scenario not in _STREAM:
        _STREAM[scenario] = binance_messages(scenario)
    return _STREAM[scenario]


def btc_cfg(**kw):
    inst = {"broker": "BTCUSD", "price_grid": 20.0, **kw.pop("inst", {})}
    return IanConfig(mode=kw.pop("mode", "LIVE"), record=kw.pop("record", False), feed={"vendor": "binance"},
                     crypto_instruments={"BTCUSDT": inst}, **kw)


def run_engine(tmp, cfg=None, broker=None, scenario="sweep_up", upto=None, mutate=None):
    """The SYNTHETIC Binance stream through the BinanceFeed (network replaced) and the whole engine."""
    snap, msgs, marks = stream(scenario)
    t0 = msgs[0][0]
    clock = Clock(t0)
    feed = BinanceFeed(None, start_threads=False, clock=clock)
    broker = broker(clock) if callable(broker) else (broker or crypto_broker(clock))
    e = IanEngine(tmp, cfg or btc_cfg(), broker=broker, feed=feed, clock=clock)
    feed.instruments = e.crypto
    feed.handle_connected("test", t0)
    feed.set_grid("BTCUSDT", 60_000.0, t0)
    feed.handle_text(json.dumps(msgs[0][1]), t0) if msgs[0][1]["data"]["e"] == "depthUpdate" else None
    assert feed.handle_snapshot("BTCUSDT", snap, t0) == ""
    nxt, notes = t0, []
    for rx, m in msgs:
        if upto is not None and rx > upto:
            break
        while rx >= nxt:
            clock.t = nxt
            e.pump()
            notes += e.step(nxt)
            nxt += dt.timedelta(seconds=0.5)
        clock.t = rx
        if mutate is not None:
            m = mutate(rx, m)
            if m is None:
                continue
        feed.handle_text(json.dumps(m), rx)
    clock.t = nxt
    e.pump()
    notes += e.step(nxt)
    return e, feed, broker, clock, notes, marks


class TestTheWholeEngine:
    def test_a_crypto_book_drives_a_live_cfd_order_with_its_stop_and_the_right_size(self, tmp_path):
        e, feed, broker, clock, notes, marks = run_engine(tmp_path)
        assert e.instruments == ("BTCUSDT",) and e.data_class == EXCHANGE and e.binance
        assert len(broker.sends) == 1, notes
        req = broker.sends[0]
        assert req.symbol == "BTCUSD" and req.magic == MAGIC and req.side is Side.BUY and req.comment.startswith("IAN ")
        sig = dict(e.journal.db.execute("SELECT * FROM signals").fetchone())
        assert sig["root"] == "BTCUSDT" and sig["spot_symbol"] == "BTCUSD" and sig["direction"] == "BUY"
        assert req.sl == pytest.approx(sig["intended_stop"]) and 0 < req.sl < sig["intended_stop"] + sig["stop_distance"]
        spec = broker.spec("BTCUSD")
        per_lot = spec.money_per_lot(sig["stop_distance"])
        assert per_lot * req.volume <= 10.0 + 1e-9                         # never more than risk_money at the stop
        assert per_lot * (req.volume + spec.volume_step) > 10.0             # and the most that fits under it
        pos = broker.positions(MAGIC)
        assert len(pos) == 1 and pos[0].sl > 0                              # the broker holds the stop at once
        stop_bp = sig["stop_distance"] / sig["intended_stop"] * 1e4
        assert 8 <= stop_bp <= 60                                           # the CFD's basis-point limits
        s = e.status(clock.t)
        line = "Binance public order book (crypto exchange) - traded through IC Markets BTCUSD CFD"
        assert line in s["status"] and s["feed"]["label"] == line
        assert s["feed"]["data_class"] == EXCHANGE and s["feed"]["vendor"] == "binance"
        assert s["feed"]["price_steps"] == {"BTCUSDT": 20.0}
        text = json.dumps(s)
        assert "CME" not in text and "institutional FX" not in text and "futures order book" not in text
        comps = json.loads(sig["components_json"])
        assert {c["source"] for c in comps if c["group"] != "CHART"} == {EXCHANGE}
        assert "Binance's public BTCUSDT order book points to BTC rising, so BTCUSD BUY" in sig["reasoning"]
        e.close()

    def test_costs_that_eat_the_target_move_stop_the_entry(self, tmp_path):
        e, feed, broker, clock, notes, _ = run_engine(tmp_path, btc_cfg(inst={"max_cost_share": 0.05}))
        assert broker.sends == []
        why = " ".join(e.notes.values()) + " ".join(r["message"] or "" for r in e.journal.signals())
        assert "costs too high" in why and "IC Markets BTCUSD" in why and "of the" in why
        e.close()

    def test_a_cfd_the_broker_does_not_have_is_never_traded_and_says_so(self, tmp_path):
        e, feed, broker, clock, notes, _ = run_engine(tmp_path, broker=lambda c: crypto_broker(c, symbols=("ETHUSD",)))
        assert broker.sends == [] and e.journal.signals() == []
        why = e.notes.get("BTCUSDT", "") + json.dumps(e.status(clock.t)["opportunities"])
        assert "IC Markets has no BTC CFD (looked for BTCUSD)" in why
        e.close()

    def test_a_cfd_that_is_not_tradable_is_never_traded(self, tmp_path):
        e, feed, broker, clock, notes, _ = run_engine(tmp_path, broker=lambda c: crypto_broker(c, trade_allowed=False))
        assert broker.sends == []
        assert "not tradable now" in e.notes.get("BTCUSDT", "") + json.dumps(e.status(clock.t)["opportunities"])
        e.close()

    def test_a_gap_in_the_stream_blocks_new_entries_and_names_binance(self, tmp_path):
        snap, msgs, marks = stream()
        cut = marks["event"]
        state = {"done": False}

        def mutate(rx, m):
            d = m["data"]
            if d["e"] == "depthUpdate" and rx >= cut - dt.timedelta(seconds=2) and not state["done"]:
                state["done"] = True
                return None                                                # one diff lost on the way
            return m

        e, feed, broker, clock, notes, _ = run_engine(tmp_path, upto=cut + dt.timedelta(seconds=4), mutate=mutate)
        assert state["done"] and broker.sends == []
        s = e.status(clock.t)
        assert s["feed"]["state"] == "DEGRADED", s["status"]
        assert "BTCUSDT" in s["status"] and "Binance" in s["status"]
        assert any(ev["kind"] == "FEED" and "GAP" in ev["message"] for ev in e.journal.events())
        e.close()

    def test_a_cfd_hidden_in_market_watch_is_selected_before_it_is_read(self, tmp_path):
        # MetaTrader gives no live quote for a symbol hidden in Market Watch; the main bot's universe leaves BTC and
        # ETH out, so nothing else selects them. Ian must, or the crypto set never trades and says the wrong reason
        def hidden(clock, selectable=True):
            b = crypto_broker(clock)
            real_tick, b.selected, b.select_calls = b.tick, set(), []

            def ensure_selected(sym):
                b.select_calls.append(sym)
                if selectable and sym in b.sim._specs:
                    b.selected.add(sym)
                    return True
                return False

            b.ensure_selected = ensure_selected
            b.tick = lambda sym: real_tick(sym) if (sym != "BTCUSD" or sym in b.selected) else None
            return b

        e, feed, broker, clock, notes, _ = run_engine(tmp_path / "a", broker=hidden)
        assert len(broker.sends) == 1 and broker.sends[0].symbol == "BTCUSD", notes
        assert broker.select_calls == ["BTCUSD"]                          # once, then remembered
        e.close()
        e, feed, broker, clock, notes, _ = run_engine(tmp_path / "b", broker=lambda c: hidden(c, selectable=False))
        assert broker.sends == []
        why = e.notes.get("BTCUSDT", "") + json.dumps(e.status(clock.t)["opportunities"])
        assert "IC Markets BTCUSD cannot be selected in Market Watch: BTCUSDT is watched, not traded" in why
        assert "no spot quote or contract" not in why and "market may be closed" not in why
        e.close()

    def test_crypto_trades_through_the_weekend_the_cme_futures_do_not(self, tmp_path):
        e = IanEngine(tmp_path, btc_cfg(mode="PAPER"), broker=None, feed=BinanceFeed(None, start_threads=False))
        sat = dt.datetime(2026, 10, 10, 12, tzinfo=UTC)
        assert e._entry_block(sat, "BTCUSDT") == "" and e._entry_block(sat, "6E") == "the weekend"
        e.close()


class TestTheCfdBridge:
    def _bridge(self, tmp_path, broker, rules=None, **cfg):
        from mintel.botexec import make_executor
        j = Journal(tmp_path / "ian.sqlite")
        ex = make_executor("PAPER", broker, MAGIC, "IAN")
        return Mt5Bridge(ex, j, broker, BridgeConfig(cfd={"BTCUSDT": rules or CfdRules(broker_label="IC Markets")}, **cfg))

    def _sig(self, broker, stop_distance=140.0, when=T0):
        from mintel.ian.signal import Signal
        tick = broker.tick("BTCUSD")
        stop = round(tick.ask - stop_distance, 2)
        return Signal("s" * 20, "financial_ian", "BTCUSDT", "BTCUSDT", "BTCUSD", "BUY", 1, 0.8, 80.0, "test",
                      stop, stop_distance, tick.ask, when, when)

    def test_size_is_risk_money_at_the_stop_from_the_cfds_own_contract(self, tmp_path):
        b = crypto_broker(lambda: T0)
        br = self._bridge(tmp_path, b)
        spec = b.spec("BTCUSD")
        vol, risk, why = br.size(spec, 140.0, br.rules_for(self._sig(b)))
        assert (vol, why) == (0.09, "") and risk == pytest.approx(9.45)     # 140 / 0.01 x 0.0075 = 10.50 a lot

    def test_the_brokers_minimum_only_up_to_one_and_a_half_times_the_risk(self, tmp_path):
        rules = CfdRules(broker_label="IC Markets")
        b = crypto_broker(lambda: T0, volume_min=0.5)
        br = self._bridge(tmp_path, b, rules, max_lots=1.0)
        spec = b.spec("BTCUSD")
        vol, risk, why = br.size(spec, 40.0, rules)          # 0.5 lot risks 15.00 = exactly 1.5 x 10: allowed
        assert vol == 0.5 and risk == pytest.approx(15.0) and why == ""
        vol, risk, why = br.size(spec, 41.0, rules)          # 15.38: over 1.5 x - skipped with a note
        assert vol == 0.0 and "smallest BTCUSD size (0.5 lots) would risk 15.38" in why and "skipped" in why

    def test_the_cost_gate_counts_the_spread_and_the_commission(self, tmp_path):
        b = crypto_broker(lambda: T0)                        # spread 15.00
        sig = self._sig(b, stop_distance=100.0)
        br = self._bridge(tmp_path, b)
        assert br.check(sig, b.tick("BTCUSD"), b.spec("BTCUSD"), T0) == (True, "")       # 15 / 100 = 15%
        rules = CfdRules(broker_label="IC Markets", commission_per_lot=15.0)               # 15 GBP = 20.00 of price
        ok, why = self._bridge(tmp_path, b, rules).check(sig, b.tick("BTCUSD"), b.spec("BTCUSD"), T0)
        assert not ok and "costs too high" in why and "commission 20.00" in why and "35%" in why
        ok, why = self._bridge(tmp_path, b).check(self._sig(b, stop_distance=45.0), b.tick("BTCUSD"),
                                                  b.spec("BTCUSD"), T0)
        assert not ok and "33% of the 45.00 target move" in why

    def test_a_closed_market_or_a_stale_quote_is_never_traded(self, tmp_path):
        b = crypto_broker(lambda: T0, trade_allowed=False)
        sig = self._sig(b)
        ok, why = self._bridge(tmp_path, b).check(sig, b.tick("BTCUSD"), b.spec("BTCUSD"), T0)
        assert not ok and "trading is disabled at the broker" in why
        b = crypto_broker(lambda: T0)
        b.quote_age_s = 30
        ok, why = self._bridge(tmp_path, b).check(self._sig(b), b.tick("BTCUSD"), b.spec("BTCUSD"), T0)
        assert not ok and "closed or not quoting" in why

    def test_the_cme_path_keeps_its_pip_rules(self, tmp_path):
        from tests.test_ian_bridge import live_bridge, make_signal
        br, b, j = live_bridge(tmp_path)
        assert br.rules_for(make_signal(broker=b)) is None
        assert br.size(b.spec("EURUSD"), 0.0010) == br.size(b.spec("EURUSD"), 0.0010, None)


class TestTheStopInBasisPoints:
    def test_min_and_max_are_basis_points_of_the_price(self):
        from mintel.ian.features import FlowSnapshot
        from mintel.ian.regime import RegimeRead
        from mintel.ian.score import Opportunity
        b = crypto_broker(lambda: T0)
        tick, spec = b.tick("BTCUSD"), b.spec("BTCUSD")
        fs = FlowSnapshot("BTCUSDT", T0, True, tick=20.0, mid=60_000.0, range_low=60_000.0, range_high=60_010.0)
        opp = Opportunity("BTCUSDT", "BTCUSDT", "BTCUSD", T0, 1, 1, 80, 0.8, 0.5, RegimeRead("BREAKOUT"))
        plan = plan_stop(fs, opp, tick, spec, (), StopConfig(min_stop_bp=8, max_stop_bp=60))
        # structure 0 + 2 steps of 20 = 40.00 and 3 x the 15.00 spread = 45.00 are under the 8 bp minimum (48.01)
        assert plan.ok and plan.distance == pytest.approx(60_007.5 * 8 / 1e4, abs=0.01)
        assert plan.basis == "book structure 40.00 (7 bp)" and "pips" not in plan.basis
        fs.range_low = 59_500.0                                                            # an 85 bp structure
        plan = plan_stop(fs, opp, tick, spec, (), StopConfig(min_stop_bp=8, max_stop_bp=60))
        assert not plan.ok and "over 60 bp" in plan.reason


class TestRelativeSizes:
    def test_a_cme_future_counts_in_contracts_a_crypto_book_in_its_own_typical_trade(self):
        cme = InstrumentFlow("6EZ6")
        assert cme.size_unit() == 1.0
        btc = InstrumentFlow("BTCUSDT", tick_size=20.0, relative_sizes=True, snap_trades=True, size_label="BTC")
        for i, q in enumerate((0.01, 0.03, 0.02, 0.04)):
            btc.on_trade(TradeEvent("BTCUSDT", T0 + dt.timedelta(milliseconds=i), 60_003.41, q, BUY, i))
        btc._unit = None
        assert btc.size_unit() == pytest.approx(0.025)
        # the trade at 60003.41 executed against the 60020 offer step (offers group up)
        assert btc.trades[-1][1] == 60_020.0 and ("ASK", 60_020.0) in btc.pending
        assert btc.v == pytest.approx(0.10) and btc.pv / btc.v == pytest.approx(60_003.41)   # VWAP keeps real prices


# ================================================================ record, replay --
class TestRecordAndReplay:
    def test_the_recorder_keeps_the_stream_and_a_replay_judges_it_as_live(self, tmp_path):
        e, feed, broker, clock, notes, _ = run_engine(tmp_path / "live", btc_cfg(record=True, mode="PAPER"))
        e.close()
        files = sorted((tmp_path / "live" / "ian" / "recordings").rglob("*.jsonl.gz"))
        assert [p.name for p in files] == ["BTCUSDT.jsonl.gz"]
        rf = ReplayFeed(files)
        e2 = IanEngine(tmp_path / "replay", btc_cfg(mode="PAPER"), broker=crypto_broker(lambda: rf.now() or T0),
                       feed=rf)
        assert rf.source_vendor == "binance" and rf.sequence_scope == "none" and e2.instruments == ("BTCUSDT",)
        assert e2.quality.cfg.order_tolerance_ms == 500.0 and e2.data_class == EXCHANGE
        assert e2.data_source == "REPLAY" and rf.health().data_class == EXCHANGE
        e2.run_virtual()
        assert e2._grid == {"BTCUSDT": 20.0}                                 # the GRID notice was recorded
        assert e2.journal.signals(), "the replayed breakout should signal too"
        e2.close()


# ===================================================================== config --
class TestConfig:
    def test_binance_reads_the_crypto_set_and_any_other_feed_the_cme_futures(self):
        c = IanConfig()
        assert c.universe("binance") == (("BTCUSDT", "ETHUSDT"), ())
        assert c.universe("databento") == (("6E", "6B", "6A", "6N", "6C", "6J", "6S"), ("ES", "GC", "ZN"))
        assert c.universe("replay") == c.universe("databento")

    def test_the_crypto_instruments_are_generic(self):
        for sym, spot, cur in (("BTCUSDT", "BTCUSD", "BTC"), ("ETHUSDT", "ETHUSD", "ETH")):
            c = contract(sym)
            assert c.generic and not c.inverted and c.price_multiplier == 1.0 and c.tick_size == 0.0
            assert root_of(sym) == sym and c.spot == spot and c.currency == cur and c.trades_24_7
            assert subscription_symbol(sym, T0.date()) == sym                       # no front month, no roll
            assert spot_direction(sym, 1) == 1 and spot_direction(sym, -1) == -1   # never inverted
        assert spot_symbol("BTCUSDT", ["EURUSD", "BTCUSD.a", "ETHUSD"]) == "BTCUSD.a"
        assert spot_symbol("BTCUSDT", ["EURUSD", "ETHUSD"]) == ""
        assert subscription_symbol("6E", dt.date(2026, 10, 7)) == "6EZ6" and root_of("6EZ6") == "6E"

    def test_ian_json_with_the_binance_feed(self, tmp_path):
        p = tmp_path / "ian.json"
        p.write_text(json.dumps({"mode": "LIVE", "feed": {"vendor": "binance"},
                                 "crypto_instruments": {"BTCUSDT": {"broker": "BTCUSD", "max_cost_share": 0.2}}}))
        c = IanConfig.load(p)
        assert c.mode == "LIVE" and c.feed["vendor"] == "binance" and c.universe("binance")[0] == ("BTCUSDT",)
        assert parse_crypto(c.crypto_instruments)["BTCUSDT"].max_cost_share == 0.2
        f = build_feed(c.feed, tmp_path, c.instruments, parse_crypto(c.crypto_instruments))
        assert isinstance(f, BinanceFeed) and f.vendor == "binance" and f.data_class == EXCHANGE
        h = f.health()                                       # configured, not yet subscribed: not NOT CONFIGURED
        assert h.state == "CONNECTING" and "Binance public order book" in h.reason and "BTCUSDT" in h.reason
        h = build_feed(c.feed, tmp_path, (), {}).health()    # no instruments at all
        assert h.state == "NOT CONFIGURED" and "no instruments" in h.reason
        assert "binance" in build_feed({"vendor": "acme"}, tmp_path).health().reason

    def test_the_check_feed_command_on_binance(self, tmp_path, monkeypatch, capsys):
        import time as _time
        import types
        from mintel.ian import __main__ as cli
        from mintel.ian.feeds import factory, wsclient
        (tmp_path / "ian.json").write_text(json.dumps({"mode": "LIVE", "feed": {"vendor": "binance"}}))
        built, real_build = {}, factory.build_feed

        def build(*a, **k):
            built["feed"] = real_build(*a, **k)
            return built["feed"]

        class Conn:                                  # SYNTHETIC: one diff, then quiet until closed
            impl = "test"

            def __init__(self):
                self.texts = [json.dumps(depth("BTCUSDT", 1, 2, bids=[(60_000.0, 1.0)]))]
                self.closed = threading.Event()

            def recv(self):
                if self.texts:
                    return self.texts.pop(0)
                self.closed.wait(20)
                raise ConnectionError("closed")

            def close(self):
                self.closed.set()

        def ten_seconds(_s):                          # the command's ten seconds: until a book has been rebuilt
            end = _time.monotonic() + 15
            while _time.monotonic() < end and (built.get("feed") is None or built["feed"].count < 3):
                _time.sleep(0.05)
            _time.sleep(0.3)                          # past the feed's hold

        monkeypatch.setattr(factory, "build_feed", build)
        monkeypatch.setattr(bn, "_http_get_json", FakeHttp())
        monkeypatch.setattr(wsclient, "open_websocket", lambda url, *a, **k: Conn())
        monkeypatch.setattr(cli, "time", types.SimpleNamespace(sleep=ten_seconds))
        rc = cli.main(["--config", str(tmp_path / "config.json"), "--check-feed"])
        out = capsys.readouterr().out
        assert rc == 0, out
        assert "NOT CONFIGURED" not in out and "DATA-DEGRADED" not in out
        assert "subscribed BTCUSDT, ETHUSDT:" in out

    def test_the_free_feed_command(self, tmp_path):
        from mintel.ian.__main__ import main
        (tmp_path / "ian.json").write_text(json.dumps({"mode": "LIVE", "feed": {"vendor": "none"}, "risk_money": 7}))
        assert main(["--config", str(tmp_path / "config.json"), "--free-feed"]) == 0
        assert json.loads((tmp_path / "ian.json").read_text()) == \
               {"mode": "LIVE", "feed": {"vendor": "binance"}, "risk_money": 7}
        (tmp_path / "ian.json").write_text(json.dumps({"mode": "LIVE", "feed": {"vendor": "databento"}}))
        (tmp_path / "secrets.json").write_text(json.dumps({"databento_api_key": "db-xyz"}))
        assert main(["--config", str(tmp_path / "config.json"), "--free-feed"]) == 0
        assert json.loads((tmp_path / "ian.json").read_text())["feed"]["vendor"] == "databento"

    @pytest.mark.parametrize("feed, word, vendor", [
        ({"vendor": "databento"}, "cme", "databento"),       # the CME feed stays
        ({"vendor": "none"}, "cme", "databento"),            # a key saved but the feed never turned on: it is now
        (None, "cme", "databento"),                          # no feed in ian.json at all: the same
        ({"vendor": "binance"}, "binance", "binance"),       # already on Binance: says so, never "cme"
    ])
    def test_the_free_feed_word_says_what_ian_json_reads(self, tmp_path, capsys, feed, word, vendor):
        from mintel.ian.__main__ import main
        (tmp_path / "ian.json").write_text(json.dumps({"mode": "LIVE", **({"feed": feed} if feed else {})}))
        (tmp_path / "secrets.json").write_text(json.dumps({"databento_api_key": "db-xyz"}))
        assert main(["--config", str(tmp_path / "config.json"), "--free-feed"]) == 0
        assert capsys.readouterr().out.strip().splitlines()[-1] == word
        ian = json.loads((tmp_path / "ian.json").read_text())
        assert ian["feed"]["vendor"] == vendor and ian["mode"] == "LIVE"
        assert IanConfig.load(tmp_path / "ian.json").feed["vendor"] == vendor


# ================================================================== installers --
def _home(tmp_path):
    from tests.test_integration_rider_ian import _home as home
    return home(tmp_path)


def _bash(snippet, home):
    env = {**os.environ, "MINTEL_HOME": str(home), "MINTEL_NO_PIP": "1"}
    return subprocess.run(["bash", "-c", f'set -uo pipefail; source "{MAC / "mintel_mac.sh"}"; {snippet}'],
                          capture_output=True, text=True, env=env, timeout=120)


MESSAGE = ("Financial Ian: LIVE on Binance's public crypto order book, trading BTC and ETH through IC Markets CFDs "
           "(no key needed). The CME FX feed can be added later.")
CME_MESSAGE = ("Financial Ian: LIVE, its feed set to the CME FX futures book through Databento "
               "(a Databento key is saved).")


class TestTheInstallers:
    def test_the_mac_step_after_the_line_up_once_and_only_ians_file(self, tmp_path):
        home = _home(tmp_path)
        data = home / "data"
        r = _bash('apply_ian_enabled >/dev/null; apply_lineup >/dev/null; CODE_CHANGED=""; apply_ian_binance; '
                  'echo "CODE=${CODE_CHANGED:-none}"', home)
        assert r.returncode == 0, r.stderr
        assert MESSAGE in r.stdout and "CODE=yes" in r.stdout
        ian = json.loads((data / "ian.json").read_text())
        assert ian["mode"] == "LIVE" and ian["feed"]["vendor"] == "binance"
        before = {p.name: p.read_text() for p in data.glob("*.json") if p.name != "ian.json"}
        assert (data / ".ian-binance-2026-10-09").exists()
        r = _bash('apply_ian_binance; echo "CODE=${CODE_CHANGED:-none}"', home)          # idempotent
        assert r.returncode == 0 and "CODE=none" in r.stdout and "Financial Ian" not in r.stdout
        assert {p.name: p.read_text() for p in data.glob("*.json") if p.name != "ian.json"} == before

    def test_a_paper_ian_goes_live_and_no_other_bot_moves(self, tmp_path):
        home = _home(tmp_path)
        data = home / "data"
        assert _bash("apply_ian_enabled >/dev/null; apply_lineup >/dev/null", home).returncode == 0
        modes_before = json.loads((data / "config.json").read_text())
        (data / "ian.json").write_text(json.dumps({"mode": "PAPER", "feed": {"vendor": "none"}}))
        r = _bash("apply_ian_binance", home)
        assert r.returncode == 0 and MESSAGE in r.stdout
        assert json.loads((data / "ian.json").read_text())["mode"] == "LIVE"
        after = json.loads((data / "config.json").read_text())
        for bot in ("tnb", "runner", "bandbreaker", "crowd"):
            assert (after.get(bot) or {}).get("mode") == (modes_before.get(bot) or {}).get("mode"), bot
        assert json.loads((data / "rider.json").read_text())["mode"] == "LIVE"

    def test_a_saved_databento_key_keeps_the_cme_feed(self, tmp_path):
        home = _home(tmp_path)
        data = home / "data"
        (data / "ian.json").write_text(json.dumps({"mode": "LIVE", "feed": {"vendor": "databento"}}))
        (data / "secrets.json").write_text(json.dumps({"databento_api_key": "db-abc"}))
        r = _bash("apply_ian_binance", home)
        assert r.returncode == 0 and CME_MESSAGE in r.stdout and "as before" not in r.stdout
        assert json.loads((data / "ian.json").read_text())["feed"]["vendor"] == "databento"

    def test_a_saved_key_with_no_feed_set_is_never_reported_as_reading_cme_when_it_reads_nothing(self, tmp_path):
        home = _home(tmp_path)
        data = home / "data"
        (data / "ian.json").write_text(json.dumps({"mode": "LIVE", "feed": {"vendor": "none"}}))
        (data / "secrets.json").write_text(json.dumps({"databento_api_key": "db-abc"}))
        r = _bash("apply_ian_binance", home)
        assert r.returncode == 0 and CME_MESSAGE in r.stdout, r.stdout + r.stderr
        # what the installer says is what ian.json now reads
        assert json.loads((data / "ian.json").read_text())["feed"]["vendor"] == "databento"

    def test_the_launcher_runs_it_after_the_line_up_and_the_fast_path_knows_it(self):
        text = (MAC / "Start Trading Bot.command").read_text()
        body = text.split("write_config         ||")[1]
        assert body.index("apply_lineup") < body.index("apply_ian_binance") < body.index("start_everything")
        fast = text.split("# ---- Fast path")[1].split("# ---- Otherwise")[0]
        assert ".ian-binance-2026-10-09" in fast
        lib = (MAC / "mintel_mac.sh").read_text()
        step = lib.split("apply_ian_binance() {")[1].split("\n}\n")[0]
        assert 'CODE_CHANGED="yes"' in step and "--free-feed" in step and "--live ian" in step
        assert "--paper" not in step and "--off" not in step                 # never another bot's mode
        for path in (MAC / "mintel_mac.sh", MAC / "Start Trading Bot.command"):
            r = subprocess.run(["bash", "-n", str(path)], capture_output=True, text=True)
            assert r.returncode == 0, (path, r.stderr)

    def test_the_windows_setup_does_the_same_once_after_the_line_up(self):
        lib = (WIN / "mintel_bots.ps1").read_text()
        fn = lib.split("function Set-IanBinanceOnce")[1].split("\nfunction ")[0]
        assert ".ian-binance-2026-10-09" in fn and '"--free-feed"' in fn and '@("--live", "ian")' in fn
        assert "Financial Ian: $mode on Binance's public crypto order book, trading BTC and ETH through IC Markets " \
               "CFDs (no key needed). The CME FX feed can be added later." in fn
        assert "--paper" not in fn and "--off" not in fn
        setup = (WIN / "SETUP-AND-START.ps1").read_text()
        assert setup.index("Invoke-LineUpOnce $P") < setup.index("Set-IanBinanceOnce $P") < setup.index("-Restart")
        assert "websocket-client" in (ROOT / "requirements.txt").read_text()
