"""Financial Ian end to end: NOT CONFIGURED is a safe, plain DATA-DEGRADED state; synthetic data never
trades unless a test allows it and is always labelled; PAPER and LIVE paths with the broker stop at once;
a gap in the data stops new signals until it recovers; restarts; deterministic replay; the process."""
import datetime as dt
import json
import os

import pytest

from mintel.broker.base import Side
from mintel.ian import MAGIC, SYNTHETIC_LABEL
from mintel.ian.book import BookEvent, TradeEvent, received_at
from mintel.ian.engine import IanConfig, IanEngine
from mintel.ian.feeds.base import FeedAdapter, FeedHealth, NotConfiguredFeed
from mintel.ian.feeds.factory import build_feed
from mintel.ian.feeds.replay import ReplayFeed
from mintel.ian.feeds.synthetic import SyntheticFeed, generate
from mintel.ian.recorder import record_events
from tests.test_ian_bridge import FakeBroker

UTC = dt.timezone.utc


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


class ScriptedFeed(FeedAdapter):
    """A feed the engine treats as a REAL one (not synthetic, not virtual time): the test hands it events
    and sets the clock. Used to exercise the LIVE path and the data-quality gate end to end."""
    vendor = "scripted"
    synthetic = False
    virtual_time = False

    def __init__(self):
        self.q = []

    def connect(self):
        return True

    def events(self, max_events=10_000):
        out, self.q = self.q[:max_events], self.q[max_events:]
        return out

    def health(self):
        return FeedHealth("LIVE", "scripted test feed", self.vendor, connected=True)


def drive(engine, feed, clock, events, every=0.5, upto=None):
    """Hand the events over in arrival order with a decision pass every ``every`` seconds."""
    nxt = None
    notes = []
    for ev in events:
        t = received_at(ev)
        if upto is not None and t > upto:
            break
        if nxt is None:
            nxt = t
        while t >= nxt:
            clock.t = nxt
            notes += engine.step(nxt)
            nxt += dt.timedelta(seconds=every)
        feed.q.append(ev)
        engine.pump()
    return notes


def status(tmp):
    return json.loads((tmp / "ian-status.json").read_text())


# --------------------------------------------------------------- not configured --

class TestNotConfigured:
    def test_no_feed_is_data_degraded_with_a_plain_status_and_no_signals(self, tmp_path):
        e = IanEngine(tmp_path, IanConfig(), broker=FakeBroker(), feed=NotConfiguredFeed())
        notes = e.step(dt.datetime(2026, 10, 7, 12, tzinfo=UTC))
        s = status(tmp_path)
        assert s["feed"]["state"] == "NOT CONFIGURED"
        assert s["status"].startswith("DATA-DEGRADED") and "no institutional feed" in s["status"]
        assert "no signals, no trades" in s["status"]
        assert s["feed"]["data_class"] == "INSTITUTIONAL" and s["mt5"]["data_class"] == "RETAIL"
        assert s["top_opportunity"] is None and s["opportunities"] == {} and s["open_positions"] == []
        assert e.journal.signals() == [] and notes == []
        assert s["mode"] == "PAPER" and s["magic"] == MAGIC == 990911
        e.close()

    def test_the_factory_never_fakes_a_feed(self, tmp_path):
        assert build_feed({"vendor": "none"}, tmp_path).health().state == "NOT CONFIGURED"
        mt5 = build_feed({"vendor": "mt5"}, tmp_path).health()
        assert mt5.state == "NOT CONFIGURED" and "RETAIL" in mt5.reason
        assert "unknown feed vendor" in build_feed({"vendor": "acme"}, tmp_path).health().reason
        db = build_feed({"vendor": "databento"}, tmp_path).health()
        assert db.state == "NOT CONFIGURED"
        assert ("databento package" in db.reason) or ("API key" in db.reason)

    def test_databento_without_a_key_is_not_configured_and_never_raises(self, tmp_path):
        from mintel.ian.feeds.databento import DatabentoFeed
        f = DatabentoFeed(tmp_path, client_factory=lambda **kw: None)
        assert f.health().state == "NOT CONFIGURED" and "databento_api_key" in f.health().reason
        assert f.connect() is False
        f.subscribe(["6EZ6"])
        e = IanEngine(tmp_path, IanConfig(), broker=None, feed=f)
        e.step(dt.datetime(2026, 10, 7, 12, tzinfo=UTC))
        assert status(tmp_path)["feed"]["state"] == "NOT CONFIGURED"
        e.close()

    def test_config_defaults_and_a_broken_file(self, tmp_path):
        p = tmp_path / "ian.json"
        IanConfig.save_minimal(p)
        c = IanConfig.load(p)
        assert c.mode == "PAPER" and c.feed == {"vendor": "none"} and c.magic == 990911
        p.write_text("{not json")
        assert IanConfig.load(p).mode == "PAPER"
        p.write_text(json.dumps({"mode": "live", "max_lots": "0.2", "instruments": ["6E", "6J"]}))
        c = IanConfig.load(p)
        assert c.mode == "LIVE" and c.max_lots == 0.2 and c.instruments == ("6E", "6J")
        p.write_text(json.dumps({"mode": "YOLO"}))
        assert IanConfig.load(p).mode == "PAPER"


# ------------------------------------------------------------------ synthetic --

def synthetic_engine(tmp, scenario="sweep_up", allow=True, cfg=None):
    feed = SyntheticFeed(scenario)
    clock = Clock(dt.datetime(2026, 10, 7, 12, tzinfo=UTC))
    broker = FakeBroker(clock=lambda: feed.now() or clock.t)
    c = cfg or IanConfig(allow_paper_on_synthetic=allow, record=False)
    return IanEngine(tmp, c, broker=broker, feed=feed, clock=clock), feed, broker


class TestSynthetic:
    def test_synthetic_data_places_nothing_by_default_and_is_labelled(self, tmp_path):
        e, feed, broker = synthetic_engine(tmp_path, allow=False)
        e.run_virtual()
        s = e.status()
        assert e.mode == "OFF" and e.open == {} and broker.sends == []
        assert s["data_label"] == SYNTHETIC_LABEL and s["data_source"] == "SYNTHETIC"
        assert s["top_opportunity"] is not None                 # it still reads the book
        e.close()

    def test_paper_end_to_end_on_a_sweep(self, tmp_path):
        e, feed, broker = synthetic_engine(tmp_path)
        notes = e.run_virtual()
        assert e.mode == "PAPER"
        rows = e.journal.db.execute("SELECT * FROM trades").fetchall()
        assert len(rows) == 1, notes
        r = dict(rows[0])
        assert r["side"] == "BUY" and r["spot_symbol"] == "EURUSD" and r["data_source"] == "SYNTHETIC"
        assert r["mode"] == "PAPER" and r["initial_stop"] < r["entry"]
        book = json.loads(r["book_json"])
        assert book["data_class"] == "INSTITUTIONAL" and book["chart_data_class"] == "RETAIL"
        assert book["features"]["sweep_dir"] == 1 and book["ladder"]["rows"]
        assert book["regime"]["regime"] and book["score"]["components"]
        lat = json.loads(r["latency_json"])
        for hop in ("data_received", "signal", "order_request", "fill"):
            assert lat.get(hop), hop
        sig = e.journal.signals()[0]
        assert sig["status"] == "FILLED" and "EURUSD BUY" in sig["reasoning"]
        assert broker.sends == []                                # PAPER never sends
        s = status(tmp_path)
        assert s["data_label"] == SYNTHETIC_LABEL
        assert s["open_positions"] and s["open_positions"][0]["symbol"] == "EURUSD"
        lad = s["ladder"]
        assert lad["data_class"] == "INSTITUTIONAL" and len(lad["rows"]) >= 10
        assert {"price", "ask", "bid", "bought", "sold", "delta", "flags"} <= set(lad["rows"][0])
        assert lad["pressure"]
        e.close()


    def test_a_paper_trade_lives_and_dies_by_the_flow(self, tmp_path):
        e, feed, broker = synthetic_engine(tmp_path, "reversal")
        notes = e.run_virtual()
        rows = [dict(r) for r in e.journal.db.execute("SELECT * FROM trades ORDER BY ticket")]
        assert rows and rows[0]["side"] == "BUY"                       # bought the trend ...
        t = rows[0]
        assert t["closed_utc"] and t["exit_reason"] == "THESIS"        # ... and left when the sellers took over
        assert t["stop"] >= t["initial_stop"]                          # tightened, never widened
        assert t["exit_explanation"].startswith("Closed THESIS at") and "thesis is dead" in t["exit_explanation"]
        assert t["mfe_r"] is not None and t["mae_r"] is not None and t["mae_r"] <= 0
        assert any("stop tightened" in n for n in notes)
        today = status(tmp_path)["today"]
        assert today["trades"] == 1 and today["data_source"] == "SYNTHETIC" and today["net"] == pytest.approx(t["net_pnl"])
        assert status(tmp_path)["last_trade"]["exit_reason"] == "THESIS"
        e.close()


# -------------------------------------------------------------- determinism --

class TestReplayDeterminism:
    def test_the_same_recording_gives_the_same_signals_and_decisions(self, tmp_path):
        evs, _ = generate("breakout")
        files = record_events(evs, tmp_path / "rec")
        assert files and all(p.name == "6EZ6.jsonl.gz" for p in files)
        results = []
        for k in range(2):
            d = tmp_path / f"run{k}"
            feed = ReplayFeed(tmp_path / "rec")
            clock = Clock(dt.datetime(2026, 10, 7, 12, tzinfo=UTC))
            broker = FakeBroker(clock=lambda f=feed: f.now() or clock.t)
            e = IanEngine(d, IanConfig(allow_paper_on_synthetic=True, record=False), broker=broker, feed=feed,
                          clock=clock)
            assert feed.synthetic and e.data_source == "SYNTHETIC"          # recorded synthetic stays synthetic
            e.run_virtual()
            sigs = [(r["signal_id"], r["trigger_ts"], r["direction"], r["score"], r["status"], r["reasoning"])
                    for r in e.journal.db.execute("SELECT * FROM signals ORDER BY signal_id")]
            trades = [(r["ticket"], r["side"], r["entry"], r["initial_stop"], r["regime"])
                      for r in e.journal.db.execute("SELECT * FROM trades ORDER BY ticket")]
            opps = {k2: (o.score, o.direction, o.regime.regime) for k2, o in e.opps.items()}
            results.append((sigs, trades, opps))
            e.close()
        assert results[0] == results[1]
        assert results[0][0], "the breakout produced no signal"

    def test_a_recording_replays_the_same_whatever_the_date_it_is_replayed_on(self, tmp_path):
        evs, _ = generate("breakout")
        record_events(evs, tmp_path / "rec")                          # a 6EZ6 (December contract) session
        results = []
        for k, today in enumerate((dt.datetime(2026, 10, 7, 12, tzinfo=UTC),
                                   dt.datetime(2026, 12, 10, 12, tzinfo=UTC))):     # after the December roll
            feed = ReplayFeed(tmp_path / "rec")
            clock = Clock(today)
            broker = FakeBroker(clock=lambda f=feed, c=clock: f.now() or c.t)
            e = IanEngine(tmp_path / f"run{k}", IanConfig(allow_paper_on_synthetic=True, record=False), broker=broker,
                          feed=feed, clock=clock)
            e.run_virtual()
            sigs = [(r["signal_id"], r["direction"], r["status"])
                    for r in e.journal.db.execute("SELECT * FROM signals ORDER BY signal_id")]
            results.append((e.events_in, sigs, e.feed_state))
            e.close()
        assert results[0] == results[1]
        assert results[0][0] == len(evs) and results[0][1], "the replay should play in full and signal"

    def test_replay_equals_the_live_stream(self, tmp_path):
        from mintel.ian.research.harness import sample
        evs, _ = generate("absorption_buyers")
        record_events(evs, tmp_path / "rec")
        feed = ReplayFeed(tmp_path / "rec")
        feed.connect()
        replayed = []
        while True:
            b = feed.events(5000)
            if not b:
                break
            replayed += b
        assert len(replayed) == len(evs)
        a = [s.fs.as_dict() for s in sample(evs, every_s=1.0)]
        b = [s.fs.as_dict() for s in sample(replayed, every_s=1.0)]
        assert a == b


# ---------------------------------------------------------------------- live --

class StoplessBroker(FakeBroker):
    """A broker that ignores the SL sent with an order (sim fault ``drop_stops_on_send``), and optionally
    loses the reply to the first send, or fails the positions read-back right after the first send."""

    def __init__(self, *a, lose_reply_once=False, fail_readback_once=False, **k):
        super().__init__(*a, **k)
        self.sim.fault.drop_stops_on_send = True
        self.lose_reply_once = lose_reply_once
        self.fail_readback_once = fail_readback_once
        self._readback_due = False

    def send(self, req):
        res = super().send(req)
        if self.lose_reply_once:
            self.lose_reply_once = False
            raise ConnectionError("the reply to the order was lost")
        self._readback_due, self.fail_readback_once = self.fail_readback_once, False
        return res

    def positions(self, magic=None):
        if self._readback_due:
            self._readback_due = False
            raise ConnectionError("positions read failed")
        return self.sim.positions(magic)


class TestLive:
    def _live(self, tmp, cfg=None, broker=None):
        feed = ScriptedFeed()
        clock = Clock(dt.datetime(2026, 10, 7, 12, tzinfo=UTC))
        broker = broker(clock) if broker is not None else FakeBroker(clock=clock)
        e = IanEngine(tmp, cfg or IanConfig(mode="LIVE", record=False), broker=broker, feed=feed, clock=clock)
        return e, feed, clock, broker

    def _fall(self, e, clock, broker, pips):
        """The spot falls ``pips`` against a BUY, one pip a second, with a pass each second."""
        start = broker.tick("EURUSD").bid
        for k in range(1, int(pips) + 1):
            broker.sim.push_bar("EURUSD", start - 0.0001 * k)
            clock.t = clock.t + dt.timedelta(seconds=1)
            e.step(clock.t)

    def _held_stop_is_real(self, e, broker):
        """Every Ian position at the broker holds a stop, and it is the stop the engine shows."""
        pos = broker.positions(MAGIC)
        for p in pos:
            assert p.sl > 0, f"ticket {p.ticket} has no stop at the broker"
            tr = e.open.get(p.symbol)
            assert tr is not None and tr.ticket == p.ticket and tr.stop == pytest.approx(p.sl)
        return pos

    def test_a_position_adopted_after_a_lost_reply_gets_its_stop(self, tmp_path):
        e, feed, clock, broker = self._live(tmp_path, broker=lambda c: StoplessBroker(clock=c, lose_reply_once=True))
        evs, marks = generate("sweep_up")
        notes = drive(e, feed, clock, evs)
        pos = self._held_stop_is_real(e, broker)
        assert len(pos) == 1 and len(broker.sends) == 1, notes          # the one order, never a second
        assert any("no stop at the broker" in n for n in notes), notes
        self._fall(e, clock, broker, 40)
        assert broker.positions(MAGIC) == [] and e.open == {}
        t = dict(e.journal.db.execute("SELECT * FROM trades").fetchone())
        assert t["closed_utc"] and t["exit_reason"] == "STOP"
        e.close()

    def test_a_position_adopted_after_a_lost_reply_is_closed_when_the_stop_is_refused(self, tmp_path):
        def mk(c):
            b = StoplessBroker(clock=c, lose_reply_once=True)
            b.sim.fault.fail_modify = True
            return b
        e, feed, clock, broker = self._live(tmp_path, broker=mk)
        evs, marks = generate("sweep_up")
        notes = drive(e, feed, clock, evs)
        assert broker.positions(MAGIC) == [] and e.open == {}, notes
        assert broker.sim.closed and any("closed" in n and "stop" in n for n in notes), notes
        e.close()

    def test_a_fill_whose_stop_was_not_confirmed_gets_its_stop_on_the_next_pass(self, tmp_path):
        e, feed, clock, broker = self._live(tmp_path, broker=lambda c: StoplessBroker(clock=c, fail_readback_once=True))
        evs, marks = generate("sweep_up")
        notes = drive(e, feed, clock, evs)
        pos = self._held_stop_is_real(e, broker)
        assert len(pos) == 1, notes
        assert any(ev["kind"] == "STOP_REPAIR" for ev in e.journal.events())
        self._fall(e, clock, broker, 30)
        assert broker.positions(MAGIC) == [] and e.open == {}
        e.close()

    def test_a_stop_lost_at_the_broker_with_the_price_through_it_is_closed_at_market(self, tmp_path):
        e, feed, clock, broker = self._live(tmp_path)
        evs, marks = generate("sweep_up")
        drive(e, feed, clock, evs)
        (p,) = self._held_stop_is_real(e, broker)
        tr = e.open[p.symbol]
        broker.sim._positions[p.ticket].sl = 0.0                     # the broker no longer holds a stop
        broker.sim.push_bar("EURUSD", tr.stop - 0.0010)              # and the price is 10 pips through ours
        clock.t = clock.t + dt.timedelta(seconds=1)
        notes = e.step(clock.t)
        assert broker.positions(MAGIC) == [] and e.open == {}, notes
        t = dict(e.journal.db.execute("SELECT * FROM trades").fetchone())
        assert t["exit_reason"] == "NO_STOP" and "already through its stop" in t["exit_explanation"]
        e.close()

    def test_a_fill_whose_stop_cannot_be_placed_is_closed_at_market(self, tmp_path):
        def mk(c):
            b = StoplessBroker(clock=c, fail_readback_once=True)
            b.sim.fault.fail_modify = True
            return b
        e, feed, clock, broker = self._live(tmp_path, broker=mk)
        evs, marks = generate("sweep_up")
        notes = drive(e, feed, clock, evs)
        assert broker.positions(MAGIC) == [] and e.open == {}, notes
        t = dict(e.journal.db.execute("SELECT * FROM trades").fetchone())
        assert t["closed_utc"] and t["exit_reason"] == "NO_STOP" and "no stop at the broker" in t["exit_explanation"]
        assert any("closed at market" in n for n in notes), notes
        e.close()

    def test_a_live_order_has_our_magic_and_its_stop_at_once_and_the_stop_only_tightens(self, tmp_path):
        e, feed, clock, broker = self._live(tmp_path)
        assert e.mode == "LIVE" and e.data_source == "LIVE FEED"
        evs, marks = generate("sweep_up")
        drive(e, feed, clock, evs)
        pos = broker.positions(MAGIC)
        assert len(pos) == 1 and broker.sends and all(r.magic == MAGIC for r in broker.sends)
        p = pos[0]
        assert p.sl > 0 and p.sl < p.entry_price and p.comment.startswith("IAN ")
        first = dict(e.journal.db.execute("SELECT * FROM trades").fetchone())
        assert broker.sends[0].sl == pytest.approx(first["initial_stop"])   # the stop went WITH the order
        assert p.sl >= broker.sends[0].sl                                # and since then it has only tightened
        # the spot runs in the trade's favour: the trail may only ever raise the broker stop
        stops = [p.sl]
        for k in range(1, 25):
            broker.sim.push_bar("EURUSD", p.entry_price + 0.0002 * k)
            clock.t = clock.t + dt.timedelta(seconds=1)
            e.step(clock.t)
            cur = broker.positions(MAGIC)
            if not cur:
                break
            stops.append(cur[0].sl)
        assert all(b >= a for a, b in zip(stops, stops[1:]))
        assert stops[-1] > stops[0]                                     # and it did tighten
        e.close()

    def test_an_orphan_with_our_magic_is_closed(self, tmp_path):
        from mintel.broker.base import OrderRequest
        e, feed, clock, broker = self._live(tmp_path)
        tick = broker.tick("GBPUSD")
        broker.sim.send(OrderRequest("GBPUSD", Side.BUY, 0.01, sl=tick.bid - 0.0020, magic=MAGIC, comment="IAN stray"))
        notes = e.step(clock.t)
        assert any("orphan" in n for n in notes) and broker.positions(MAGIC) == []
        e.close()

    def test_a_gap_in_the_data_blocks_new_signals_until_it_recovers(self, tmp_path):
        evs, marks = generate("sweep_up")
        gap = []
        for ev in evs:
            if received_at(ev) >= marks["event"] and isinstance(ev, (BookEvent, TradeEvent)):
                if isinstance(ev, BookEvent):
                    ev = BookEvent(ev.instrument, ev.ts_exchange, ev.ts_received, ev.sequence + 100, ev.side,
                                   ev.price, ev.size, ev.action, ev.level, ev.order_id, ev.cause, ev.source)
                else:
                    ev = TradeEvent(ev.instrument, ev.ts, ev.price, ev.size, ev.aggressor, ev.sequence + 100,
                                    ev.ts_received, ev.source)
            gap.append(ev)
        window_end = marks["event"] + dt.timedelta(seconds=5)
        # control: the clean stream signals inside the window
        e0, f0, c0, b0 = self._live(tmp_path / "clean")
        drive(e0, f0, c0, evs, upto=window_end)
        assert e0.journal.signals(), "the clean stream should have signalled"
        e0.close()
        e, feed, clock, broker = self._live(tmp_path / "gap")
        drive(e, feed, clock, gap, upto=window_end)
        assert e.journal.signals() == [] and broker.sends == []
        s = e.status(clock.t)
        assert s["feed"]["state"] == "DEGRADED" and "sequence gap" in s["status"]
        assert any(ev["kind"] == "RECOVERY" for ev in e.journal.events())
        e.close()

    def test_a_synthetic_feed_is_never_live(self, tmp_path):
        feed = SyntheticFeed("sweep_up")
        e = IanEngine(tmp_path, IanConfig(mode="LIVE", allow_paper_on_synthetic=True, record=False),
                      broker=FakeBroker(clock=lambda: feed.now() or dt.datetime(2026, 10, 7, 12, tzinfo=UTC)),
                      feed=feed)
        assert e.mode == "PAPER"
        e.close()


class TestFeedStart:
    def test_a_feed_that_could_not_connect_at_start_is_retried_with_a_back_off(self, tmp_path):
        from mintel.ian.feeds.databento import DatabentoFeed, save_api_key
        from tests.test_ian_feeds import FakeLive
        save_api_key(tmp_path, "db-TESTKEY")
        calls = []

        def factory(**kw):
            calls.append(kw)
            if len(calls) == 1:
                raise ConnectionError("network not up yet")       # a VPS reboot: the network comes up later
            return FakeLive(**kw)
        feed = DatabentoFeed(tmp_path, client_factory=factory)
        clock = Clock(dt.datetime(2026, 10, 7, 12, tzinfo=UTC))
        e = IanEngine(tmp_path, IanConfig(record=False), broker=None, feed=feed, clock=clock)
        assert len(calls) == 1 and feed.health().state == "DOWN"
        e.step(clock.t)
        assert e.status(clock.t)["status"].startswith("FEED DOWN") and "network not up yet" in e.feed_reason
        for _ in range(60):
            clock.t += dt.timedelta(seconds=1)
            e.step(clock.t)
        assert len(calls) == 2 and feed.health().state == "LIVE"
        assert any(ev["kind"] == "RECOVERY" and "feed is down" in ev["message"] for ev in e.journal.events())
        for _ in range(300):                                        # once up, it is not reconnected again
            clock.t += dt.timedelta(seconds=1)
            e.step(clock.t)
        assert len(calls) == 2
        e.close()

    def test_a_feed_that_is_not_configured_is_never_retried(self, tmp_path):
        from mintel.ian.feeds.databento import DatabentoFeed
        feed = DatabentoFeed(tmp_path, client_factory=lambda **kw: pytest.fail("no key: never connect"))
        clock = Clock(dt.datetime(2026, 10, 7, 12, tzinfo=UTC))
        e = IanEngine(tmp_path, IanConfig(record=False), broker=None, feed=feed, clock=clock)
        for _ in range(30):
            clock.t += dt.timedelta(seconds=1)
            e.step(clock.t)
        assert e.feed_state == "NOT CONFIGURED" and not any(ev["kind"] == "RECOVERY" for ev in e.journal.events())
        e.close()


class TestTheRoll:
    def test_the_front_month_is_followed(self, tmp_path):
        class Sub(ScriptedFeed):
            def __init__(self):
                super().__init__()
                self.subs = []

            def subscribe(self, instruments):
                self.subs.append(list(instruments))
        feed = Sub()
        clock = Clock(dt.datetime(2026, 12, 5, 12, tzinfo=UTC))
        e = IanEngine(tmp_path, IanConfig(record=False, context=()), broker=None, feed=feed, clock=clock)
        assert "6EZ6" in feed.subs[0] and "6JZ6" in feed.subs[0]
        assert e.step(clock.t) == []
        clock.t = dt.datetime(2026, 12, 6, 12, tzinfo=UTC)
        notes = e.step(clock.t)
        assert feed.subs[-1][0] == "6EH7" and "6JH7" in feed.subs[-1] and any("rolled to" in n for n in notes)
        assert any(ev["kind"] == "ROLL" for ev in e.journal.events())
        e.close()


# ------------------------------------------------------------------- restart --

class TestRestart:
    def test_an_open_paper_trade_comes_back(self, tmp_path):
        e, feed, broker = synthetic_engine(tmp_path)
        e.run_virtual()
        assert len(e.open) == 1
        (sym, tr), = e.open.items()
        e.close()
        e2, feed2, broker2 = synthetic_engine(tmp_path)
        assert sym in e2.open and e2.open[sym].ticket == tr.ticket and e2.open[sym].stop == tr.stop
        assert any(p.ticket == tr.ticket for p in e2.executor.positions())
        e2.close()

    def test_a_trade_from_another_mode_is_left_alone(self, tmp_path):
        e, feed, broker = synthetic_engine(tmp_path)
        e.run_virtual()
        e.close()
        e2 = IanEngine(tmp_path, IanConfig(record=False), broker=FakeBroker(), feed=NotConfiguredFeed())
        assert e2.open == {} and any("left as it is" in n for n in e2._pending_notes)
        e2.close()


# ------------------------------------------------------------------- process --

@pytest.fixture
def process_sandbox():
    """main() installs signal handlers and root log handlers, as a process should; put them back."""
    import logging
    import signal as sig
    saved = {s: sig.getsignal(s) for s in (sig.SIGINT, sig.SIGTERM)}
    root = logging.getLogger()
    before = list(root.handlers)
    yield
    for s, h in saved.items():
        sig.signal(s, h)
    for h in list(root.handlers):
        if h not in before:
            root.removeHandler(h)
            h.close()


@pytest.mark.usefixtures("process_sandbox")
class TestProcess:
    def test_the_process_runs_without_a_feed_or_mt5_and_says_why(self, tmp_path):
        from mintel.ian.run import main
        data = tmp_path / "data"
        cfgp = tmp_path / "config.json"
        cfgp.write_text(json.dumps({"ops": {"data_dir": str(data), "log_dir": str(tmp_path / "logs")}}))
        assert main(["--config", str(cfgp), "--cycles", "2", "--no-broker"]) == 0
        assert json.loads((data / "ian.json").read_text())["mode"] == "PAPER"
        s = json.loads((data / "ian-status.json").read_text())
        assert s["feed"]["state"] == "NOT CONFIGURED" and s["status"].startswith("DATA-DEGRADED")
        hb = json.loads((data / "heartbeats" / "ian.heartbeat.json").read_text())
        assert hb["name"] == "ian" and hb["detail"]["feed"] == "NOT CONFIGURED"
        assert not (data / "ian.pid").exists()
        assert (tmp_path / "logs" / "ian.log").exists()

    def test_a_second_copy_will_not_start(self, tmp_path):
        from mintel.ian.run import claim_pid
        p = tmp_path / "ian.pid"
        p.write_text(str(os.getppid()))                 # a live process that is not us
        assert claim_pid(p) is False
        p.write_text("999999999")                       # a dead one
        assert claim_pid(p) is True and p.read_text() == str(os.getpid())

    def test_off_exits_at_once(self, tmp_path):
        from mintel.ian.run import main
        data = tmp_path / "data"
        data.mkdir()
        (data / "ian.json").write_text(json.dumps({"mode": "OFF"}))
        cfgp = tmp_path / "config.json"
        cfgp.write_text(json.dumps({"ops": {"data_dir": str(data), "log_dir": str(tmp_path / "logs")}}))
        assert main(["--config", str(cfgp), "--no-broker"]) == 0
        assert not (data / "ian-status.json").exists()
