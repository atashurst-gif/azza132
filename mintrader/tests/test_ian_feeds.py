"""Financial Ian's DATA layer: the normalised book format (MBP and MBO), serialisation, the synthetic
scenarios (deterministic), the recorder and the replay (deterministic, real-time pacing), and the
Databento record mapping - tested with hand-made records, no package, no key, no internet."""
import datetime as dt
import gzip
import json

import pytest

from mintel.ian.book import (ADD, ASK, BID, BUY, CANCEL, CLEAR, MODIFY, SELL, UNKNOWN, BookEvent, FeedNotice,
                             OrderBook, TradeEvent, from_dict, to_dict)
from mintel.ian.feeds.databento import (F_MAYBE_BAD_BOOK, UNDEF_PRICE, DatabentoFeed, RecordMapper, read_api_key,
                                        save_api_key)
from mintel.ian.feeds.replay import ReplayFeed
from mintel.ian.feeds.synthetic import SCENARIOS, SyntheticFeed, generate
from mintel.ian.recorder import Recorder, record_events

UTC = dt.timezone.utc
T = dt.datetime(2026, 10, 7, 12, 0, tzinfo=UTC)


def ev(seq, side, price, size, action=MODIFY, oid=0, cause=""):
    return BookEvent("6EZ6", T, T, seq, side, price, size, action, order_id=oid, cause=cause)


class TestTheBook:
    def test_price_levels(self):
        b = OrderBook("6EZ6")
        b.apply(ev(1, BID, 1.08500, 40, ADD))
        b.apply(ev(2, BID, 1.08495, 30, ADD))
        b.apply(ev(3, ASK, 1.08505, 25, ADD))
        assert b.best_bid() == 1.085 and b.best_ask() == 1.08505 and b.spread() == pytest.approx(0.00005)
        ch = b.apply(ev(4, BID, 1.08500, 55, MODIFY))
        assert ch[0].before == 40 and ch[0].after == 55 and ch[0].delta == 15
        b.apply(ev(5, BID, 1.08500, 0, CANCEL))
        assert b.best_bid() == 1.08495 and b.levels(BID) == [(1.08495, 30)]
        snap = b.snapshot()
        assert snap.bids == ((1.08495, 30),) and snap.asks == ((1.08505, 25),) and snap.sequence == 5
        assert snap.mode == "MBP" and snap.to_dict()["bids"] == [[1.08495, 30]]
        b.apply(BookEvent("6EZ6", T, T, 6, "", 0, 0, CLEAR))
        assert not b.bids and not b.asks

    def test_orders(self):
        b = OrderBook("6EZ6")
        b.apply(ev(1, BID, 1.08500, 10, ADD, oid=7))
        b.apply(ev(2, BID, 1.08500, 5, ADD, oid=8))
        assert b.size_at(BID, 1.085) == 15 and b.mode == "MBO"
        b.apply(ev(3, BID, 1.08495, 10, MODIFY, oid=7))                   # order 7 moves down a tick
        assert b.size_at(BID, 1.085) == 5 and b.size_at(BID, 1.08495) == 10
        b.apply(ev(4, BID, 1.08495, 4, CANCEL, oid=7))                    # a partial cancel
        assert b.size_at(BID, 1.08495) == 6
        b.apply(ev(5, BID, 1.08495, 0, CANCEL, oid=7))                    # the rest
        assert b.size_at(BID, 1.08495) == 0 and 7 not in b.orders
        b.apply(ev(6, BID, 1.08495, 3, CANCEL, oid=99))                   # unknown order: nothing
        assert b.size_at(BID, 1.085) == 5

    def test_crossed(self):
        b = OrderBook("6EZ6")
        b.apply(ev(1, BID, 1.0851, 1, ADD))
        b.apply(ev(2, ASK, 1.0850, 1, ADD))
        assert b.crossed()

    def test_every_event_type_round_trips_through_json(self):
        evs = [BookEvent("6EZ6", T, T + dt.timedelta(microseconds=1500), 9, ASK, 1.08505, 12, ADD, 3, 77, "view", "x"),
               TradeEvent("6EZ6", T, 1.085, 4, SELL, 10, T + dt.timedelta(milliseconds=2), "x"),
               TradeEvent("6EZ6", T, 1.085, 4, UNKNOWN, 11),
               FeedNotice("", T, "GAP", "lost 3 packets", "x")]
        for e in evs:
            assert from_dict(json.loads(json.dumps(to_dict(e)))) == e


class TestSynthetic:
    def test_every_scenario_is_deterministic(self):
        for sc in ("balanced", "sweep_up", "news_shock"):
            a, ma = generate(sc, seed=3)
            b, mb = generate(sc, seed=3)
            assert a == b and ma == mb
            c, _ = generate(sc, seed=4)
            assert a != c

    def test_all_scenarios_generate_a_sane_book(self):
        for sc in SCENARIOS:
            evs, marks = generate(sc, warmup=60)
            b = OrderBook("6EZ6")
            for e in evs:
                if isinstance(e, BookEvent):
                    b.apply(e)
            assert not b.crossed() and len(b.bids) >= 5 and len(b.asks) >= 5, sc
            assert all(e.source == "synthetic" for e in evs if hasattr(e, "source"))

    def test_the_feed_is_labelled_synthetic_and_finishes(self):
        f = SyntheticFeed("balanced", warmup=30)
        assert f.synthetic and f.virtual_time
        assert f.events() == []                                  # not connected yet
        f.connect()
        n = 0
        while True:
            b = f.events(1000)
            if not b:
                break
            n += len(b)
        assert n == len(f.all_events) and f.health().state == "FINISHED" and f.health().synthetic

    def test_two_instruments_merge_in_arrival_order(self):
        f = SyntheticFeed("balanced", instruments=("6EZ6", "6JZ6"), warmup=30)
        f.connect()
        evs = f.events(10 ** 6)
        assert {e.instrument for e in evs} == {"6EZ6", "6JZ6"}
        rx = [e.ts_received if isinstance(e, BookEvent) else e.received for e in evs]
        assert rx == sorted(rx)


class TestRecordAndReplay:
    def test_files_per_instrument_per_day(self, tmp_path):
        f = SyntheticFeed("balanced", instruments=("6EZ6", "6BZ6"), warmup=20)
        files = record_events(f.all_events, tmp_path)
        assert [p.relative_to(tmp_path).as_posix() for p in files] == ["2026-10-07/6BZ6.jsonl.gz", "2026-10-07/6EZ6.jsonl.gz"]
        with gzip.open(files[0], "rt") as fh:
            first = json.loads(fh.readline())
        assert first["i"] == "6BZ6" and first["t"] in ("book", "trade")

    def test_appending_after_a_restart_keeps_the_file_readable(self, tmp_path):
        evs, _ = generate("balanced", warmup=20)
        half = len(evs) // 2
        r = Recorder(tmp_path)
        r.write_many(evs[:half])
        r.close()
        r2 = Recorder(tmp_path)
        r2.write_many(evs[half:])
        r2.close()
        feed = ReplayFeed(tmp_path)
        assert feed.connect()
        out = feed.events(10 ** 6)
        assert out == evs

    def test_replay_merges_instruments_deterministically(self, tmp_path):
        f = SyntheticFeed("sweep_up", instruments=("6EZ6", "6JZ6"), warmup=20)
        record_events(f.all_events, tmp_path)
        runs = []
        for _ in range(2):
            rf = ReplayFeed(tmp_path)
            rf.connect()
            runs.append(rf.events(10 ** 6))
        assert runs[0] == runs[1] and len(runs[0]) == len(f.all_events)
        assert sorted(map(repr, runs[0])) == sorted(map(repr, f.all_events))

    def test_real_time_pacing(self, tmp_path):
        evs, _ = generate("balanced", warmup=20)
        record_events(evs, tmp_path)
        wall = [0.0]
        rf = ReplayFeed(tmp_path, speed="realtime", wall_clock=lambda: wall[0])
        rf.connect()
        first = rf.events(10 ** 6)
        t0 = first[0].ts_received
        assert all((e.ts_received if isinstance(e, BookEvent) else e.received) == t0 for e in first)
        wall[0] = 5.0                                           # five seconds later
        more = rf.events(10 ** 6)
        last = more[-1].ts_received if isinstance(more[-1], BookEvent) else more[-1].received
        assert 4.0 <= (last - t0).total_seconds() <= 5.0
        fast = ReplayFeed(tmp_path, speed=10.0, wall_clock=lambda: wall[0])
        wall[0] = 0.0
        fast.connect()
        fast.events(10 ** 6)
        wall[0] = 1.0
        got = fast.events(10 ** 6)
        lt = got[-1].ts_received if isinstance(got[-1], BookEvent) else got[-1].received
        assert 9.0 <= (lt - t0).total_seconds() <= 10.0

    def test_a_missing_recording_is_down_not_an_exception(self, tmp_path):
        rf = ReplayFeed(tmp_path / "nothing")
        assert rf.connect() is False and rf.health().state == "DOWN" and rf.events() == []


# ------------------------------------------------------------------ databento --

NS = int(T.timestamp()) * 1_000_000_000


def lvl(bp, bs, ap, az):
    return {"bid_px": int(round(bp * 1e9)) if bp else UNDEF_PRICE, "bid_sz": bs,
            "ask_px": int(round(ap * 1e9)) if ap else UNDEF_PRICE, "ask_sz": az, "bid_ct": 1, "ask_ct": 1}


def mbp(levels, seq=1, flags=0, action="A", side="B", price=0, size=0):
    return {"_type": "mbp10", "instrument_id": 42, "ts_event": NS, "ts_recv": NS + 2_000_000, "sequence": seq,
            "flags": flags, "action": action, "side": side, "price": price, "size": size, "levels": levels}


class TestDatabentoMapping:
    def test_symbols_come_from_the_mapping_messages(self):
        m = RecordMapper()
        assert m.map({"_type": "mapping", "instrument_id": 42, "stype_out_symbol": "6EZ6"}) == []
        out = m.map({"_type": "trade", "instrument_id": 42, "ts_event": NS, "ts_recv": NS + 1000, "sequence": 5,
                     "price": 1_085_050_000, "size": 3, "side": "B", "action": "T"})
        t = out[0]
        assert isinstance(t, TradeEvent) and t.instrument == "6EZ6" and t.price == pytest.approx(1.08505)
        assert t.aggressor == BUY and t.size == 3 and t.ts == T and t.sequence == 5

    @pytest.mark.parametrize("side,aggr", [("B", BUY), ("A", SELL), ("N", UNKNOWN), (b"A", SELL), (65, SELL)])
    def test_aggressor_sides(self, side, aggr):
        m = RecordMapper({42: "6EZ6"})
        out = m.map({"_type": "trade", "instrument_id": 42, "ts_event": NS, "price": 1_085_000_000, "size": 1,
                     "side": side})
        assert out[0].aggressor == aggr

    def test_mbp10_changes_and_the_view(self):
        m = RecordMapper({42: "6EZ6"}, levels=3)
        first = m.map(mbp([lvl(1.0850, 10, 1.08505, 12), lvl(1.08495, 20, 1.0851, 22), lvl(1.0849, 30, 1.08515, 32)]))
        assert len(first) == 7 and all(e.cause == "snapshot" for e in first)
        assert first[0].action == CLEAR and all(e.action == ADD for e in first[1:])     # a rebuild starts clean
        # the best offer is taken out and a new deeper offer comes into the three-level view
        nxt = m.map(mbp([lvl(1.0850, 10, 1.0851, 22), lvl(1.08495, 20, 1.08515, 32), lvl(1.0849, 30, 1.0852, 5)],
                        seq=2, action="C", side="A"))
        asks = {e.price: e for e in nxt if e.side == ASK}
        assert asks[1.08505].action == CANCEL and asks[1.08505].cause == ""
        assert asks[1.0852].action == ADD and asks[1.0852].cause == "view"          # came into view, not a new order
        assert not [e for e in nxt if e.side == BID]
        # the bids step up: the old third bid falls out of the view (not a cancellation)
        nxt2 = m.map(mbp([lvl(1.08505, 7, 1.0851, 22), lvl(1.0850, 10, 1.08515, 32), lvl(1.08495, 20, 1.0852, 5)], seq=3))
        bids = {e.price: e for e in nxt2 if e.side == BID}
        assert bids[1.08505].action == ADD and bids[1.08505].cause == ""
        assert bids[1.0849].action == CANCEL and bids[1.0849].cause == "view"

    def test_undefined_prices_and_a_bad_book_flag(self):
        m = RecordMapper({42: "6EZ6"})
        out = m.map(mbp([lvl(1.0850, 10, None, 0)], flags=F_MAYBE_BAD_BOOK))
        assert isinstance(out[0], FeedNotice) and out[0].notice == "BAD_BOOK"
        assert out[1].action == CLEAR and [e.side for e in out[2:]] == [BID]

    def test_mbo_actions(self):
        m = RecordMapper({42: "6EZ6"})
        base = {"_type": "mbo", "instrument_id": 42, "ts_event": NS, "ts_recv": NS, "sequence": 1, "flags": 0}
        a = m.map({**base, "action": "A", "side": "B", "price": 1_085_000_000, "size": 5, "order_id": 9})[0]
        assert a.action == ADD and a.order_id == 9 and a.side == BID and a.size == 5
        mo = m.map({**base, "action": "M", "side": "B", "price": 1_084_950_000, "size": 4, "order_id": 9})[0]
        assert mo.action == MODIFY and mo.price == pytest.approx(1.08495)
        c = m.map({**base, "action": "C", "side": "B", "price": 1_084_950_000, "size": 4, "order_id": 9})[0]
        assert c.action == CANCEL
        r = m.map({**base, "action": "R", "side": "N", "price": UNDEF_PRICE, "size": 0, "order_id": 0})[0]
        assert r.action == CLEAR
        t = m.map({**base, "action": "T", "side": "A", "price": 1_085_000_000, "size": 2, "order_id": 0})
        assert isinstance(t[0], TradeEvent) and t[0].aggressor == SELL
        assert m.map({**base, "action": "F", "side": "B", "price": 1_085_000_000, "size": 2, "order_id": 9}) == []

    def test_after_a_recovery_the_book_is_rebuilt_not_left_crossed(self):
        m = RecordMapper({42: "6EZ6"})
        book = OrderBook("6EZ6")
        for r in views(2):
            for e in m.map(r):
                book.apply(e)
        assert (book.best_bid(), book.best_ask()) == (1.085, 1.08505)
        m.prev.clear()                                   # what DatabentoFeed.recover() does
        moved = views(1, start=1.0851, seq0=5000)[0]     # the market moved up two ticks during the reconnect
        for e in m.map(moved):
            book.apply(e)
        assert not book.crossed()
        assert (book.best_bid(), book.best_ask()) == (1.0851, 1.08515)
        assert len(book.bids) == 10 and len(book.asks) == 10
        for r in views(6, start=1.0851, shift_every=1, seq0=5000)[1:]:     # and it keeps moving up
            for e in m.map(r):
                book.apply(e)
        assert not book.crossed() and len(book.bids) == 10 and len(book.asks) == 10
        assert (book.best_bid(), book.best_ask()) == (1.08535, 1.0854)

    def test_errors_and_gaps_become_notices(self):
        m = RecordMapper()
        e = m.map({"_type": "error", "err": "auth failed", "ts_event": NS})[0]
        assert isinstance(e, FeedNotice) and e.notice == "ERROR"
        g = m.map({"_type": "system", "msg": "Gap detected in channel 312", "ts_event": NS})[0]
        assert g.notice == "GAP"
        assert m.map({"_type": "system", "msg": "Heartbeat", "ts_event": NS}) == []


def views(n, start=1.0850, tick=0.00005, shift_every=20, seq0=1000, seq_step=3):
    """``n`` hand-made ten-level mbp-10 records for one future, 50 ms apart, drifting up a tick every
    ``shift_every`` records; sequence numbers step by ``seq_step`` (CME numbers per channel)."""
    out = []
    for k in range(n):
        mid = start + tick * (k // shift_every)
        lv = [lvl(mid - i * tick, 10 + (k + i) % 7, mid + tick + i * tick, 12 + (3 * k + i) % 5) for i in range(10)]
        r = mbp(lv, seq=seq0 + seq_step * k)
        r["ts_event"] = NS + k * 50_000_000
        r["ts_recv"] = r["ts_event"] + 2_000_000
        out.append(r)
    return out


def drain(feed):
    out = []
    while True:
        b = feed.events(5000)
        if not b:
            return out
        out += b


class TestReplayOfDatabento:
    def test_a_replayed_databento_recording_is_checked_per_channel_as_it_was_live(self, tmp_path):
        from mintel.ian.book import received_at
        from mintel.ian.dataquality import DataQualityMonitor, QualityConfig
        m = RecordMapper({42: "6EZ6"})
        evs = [e for r in views(400) for e in m.map(r)]
        assert len({e.sequence for e in evs}) < len(evs)       # one record -> several level events, one number
        record_events(evs, tmp_path / "rec")
        feed = ReplayFeed(tmp_path / "rec")
        assert feed.connect() and feed.sequence_scope == "channel" and not feed.synthetic
        replayed = drain(feed)
        assert len(replayed) == len(evs)

        def end_state(scope):
            q, book = DataQualityMonitor(QualityConfig(), scope), OrderBook("6EZ6")
            for e in replayed:
                q.observe(e)
                book.apply(e)
            return q.state("6EZ6", received_at(replayed[-1]), book, "LIVE")
        live = DatabentoFeed(tmp_path, client_factory=FakeLive)
        assert end_state(live.sequence_scope).state == "LIVE"
        assert end_state(feed.sequence_scope).state == "LIVE"      # the replay: the same state as live
        assert end_state("instrument").state == "DEGRADED"          # what a per-instrument check makes of it

    def test_a_recording_of_another_source_keeps_the_per_instrument_check(self, tmp_path):
        evs, _ = generate("balanced", warmup=20)
        record_events(evs, tmp_path / "rec")
        feed = ReplayFeed(tmp_path / "rec")
        assert feed.connect() and feed.sequence_scope == "instrument"


class FakeLive:
    """Stands in for databento.Live: records the subscriptions, hands records to the callback."""
    instances = []

    def __init__(self, key=None, **kw):
        self.key = key
        self.subs = []
        self.cb = None
        self.started = False
        self.stopped = False
        FakeLive.instances.append(self)

    def subscribe(self, **kw):
        self.subs.append(kw)

    def add_callback(self, cb, err=None):
        self.cb = cb
        self.err = err

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True


class TestDatabentoFeed:
    def test_the_key_lives_in_secrets_json(self, tmp_path):
        assert read_api_key(tmp_path) == ""
        (tmp_path / "secrets.json").write_text(json.dumps({"coinversa_api_key": "cvsa_x"}))
        save_api_key(tmp_path, "db-TESTKEY ")
        s = json.loads((tmp_path / "secrets.json").read_text())
        assert s == {"coinversa_api_key": "cvsa_x", "databento_api_key": "db-TESTKEY"}
        assert read_api_key(tmp_path) == "db-TESTKEY"

    def test_subscribes_to_glbx_and_maps_what_arrives(self, tmp_path):
        save_api_key(tmp_path, "db-TESTKEY")
        FakeLive.instances.clear()
        f = DatabentoFeed(tmp_path, client_factory=FakeLive)
        assert f.connect()
        f.subscribe(["6EZ6", "6JZ6"])
        c = FakeLive.instances[-1]
        assert c.key == "db-TESTKEY" and c.started
        assert c.subs[0]["dataset"] == "GLBX.MDP3" and c.subs[0]["schema"] == "mbp-10"
        assert c.subs[0]["symbols"] == ["6EZ6", "6JZ6"] and c.subs[1]["schema"] == "trades"
        assert f.health().state == "LIVE" and f.sequence_scope == "channel"
        c.cb({"_type": "mapping", "instrument_id": 42, "stype_out_symbol": "6EZ6"})
        c.cb(mbp([lvl(1.0850, 10, 1.08505, 12)]))
        c.cb({"_type": "trade", "instrument_id": 42, "ts_event": NS, "price": 1_085_050_000, "size": 2, "side": "B"})
        evs = f.events()
        assert [type(e).__name__ for e in evs] == ["BookEvent", "BookEvent", "BookEvent", "TradeEvent"]
        assert evs[0].action == CLEAR
        assert all(e.instrument == "6EZ6" for e in evs)
        assert f.recover("test") and len(FakeLive.instances) == 2 and c.stopped

    def test_a_refused_connection_is_down_not_an_exception(self, tmp_path):
        save_api_key(tmp_path, "db-TESTKEY")

        def boom(**kw):
            raise ValueError("authentication failed")
        f = DatabentoFeed(tmp_path, client_factory=boom)
        f.subscribe(["6EZ6"])
        h = f.health()
        assert h.state == "DOWN" and "authentication failed" in h.reason
        assert any(e.notice == "DISCONNECTED" for e in f.events())

    def test_mbo_asks_for_a_snapshot(self, tmp_path):
        save_api_key(tmp_path, "db-TESTKEY")
        FakeLive.instances.clear()
        f = DatabentoFeed(tmp_path, schema="mbo", client_factory=FakeLive)
        f.subscribe(["6EZ6"])
        assert FakeLive.instances[-1].subs[0]["snapshot"] is True
