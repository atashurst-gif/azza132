"""Financial Ian: fixes from the review of 9 October.

* While the feed as a whole is DEGRADED, nothing new is entered on ANY future (not only the one at
  fault), the page names the futures at fault, and open positions are still managed.
* While an earlier order's outcome is unknown, no new order is sent on that spot pair, whatever its
  signal id - until the order is found at the broker and adopted, or repeated positions reads over at
  least a minute and the deal history show it is not there (one empty read is never enough: the MT5
  adapter returns [] when the read fails). A lost reply the adapter reports as retcode 10031 is unknown,
  not rejected.
* LIVE with no MetaTrader runs safely (nothing can be placed, and the page says so, still showing today's
  LIVE figures) instead of crashing; while MetaTrader cannot be reached the page says it is waiting; and a
  start-up failure never leaves the pid file behind.
* The magic number is fixed at 990911: ian.json cannot change it, so the LIVE sweep can never close
  another bot's positions.

And from the second review:

* A fill the executor reports 'closed at once' is REJECTED only when the broker's closing deal shows the
  close; otherwise it is UNKNOWN (the position may still be open with its stop), so it is found and
  adopted, never closed as an orphan, and nothing is sent twice.
* An open LIVE trade the positions read stops showing is never booked without the broker's closing deal;
  the sweep adopts a position of any of our orders that holds its stop, and books every orphan it closes.
* A positions read made while MetaTrader is not connected to the trade server never counts towards
  'not at the broker', and the deal history is searched from well before the send.

All data here is synthetic test data; nothing in this file is a result.
"""
import datetime as dt
import json
import logging

import pytest

from mintel.botexec import make_executor
from mintel.broker.base import OrderRequest, OrderResult, RetCode, Side
from mintel.ian import MAGIC, TAG
from mintel.ian.book import BookEvent, received_at
from mintel.ian.bridge import DUPLICATE, FILLED_S, Mt5Bridge
from mintel.ian.engine import MISSING_PASSES, MISSING_SECONDS, IanConfig, IanEngine
from mintel.ian.feeds.base import NotConfiguredFeed
from mintel.ian.feeds.synthetic import generate
from mintel.ian.journal import Journal
from tests.test_ian_bridge import T as BT, FakeBroker, live_bridge, make_signal
from tests.test_ian_engine import Clock, ScriptedFeed, drive, process_sandbox  # noqa: F401  (fixture)

UTC = dt.timezone.utc
NOON = dt.datetime(2026, 10, 7, 12, tzinfo=UTC)


def live_engine(tmp, start=NOON, cfg=None, broker_cls=FakeBroker, clock=None):
    feed = ScriptedFeed()
    clock = clock or Clock(start)
    broker = broker_cls(clock=clock)
    e = IanEngine(tmp, cfg or IanConfig(mode="LIVE", record=False), broker=broker, feed=feed, clock=clock)
    return e, feed, clock, broker


def a_future_that_goes_quiet(at, instrument="6BZ6", mid=1.33):
    """Three levels a side for a second future, then nothing more: its book goes stale, then DOWN."""
    out = []
    for k in range(3):
        for side, sign in (("BID", -1), ("ASK", 1)):
            out.append(BookEvent(instrument, at, at + dt.timedelta(milliseconds=2), len(out) + 1, side,
                                 round(mid + sign * 0.0001 * (k + 1), 5), 20.0, "add", k))
    return out


# ------------------------------------------------------------------ DEGRADED --

class TestDegradedBlocksEveryEntry:
    def test_one_future_at_fault_stops_new_entries_on_every_future_and_is_named(self, tmp_path):
        evs, marks = generate("sweep_up")
        window_end = marks["event"] + dt.timedelta(seconds=5)
        # control: 6E alone, clean, enters inside the window
        e0, f0, c0, b0 = live_engine(tmp_path / "clean")
        drive(e0, f0, c0, evs, upto=window_end)
        assert b0.sends, "the clean 6E stream should have entered"
        e0.close()
        # the same 6E stream, while 6B's data has gone quiet: the feed as a whole is DEGRADED
        first = received_at(evs[0]) - dt.timedelta(milliseconds=5)
        stream = sorted(a_future_that_goes_quiet(first) + list(evs), key=received_at)
        e, feed, clock, broker = live_engine(tmp_path / "degraded")
        seen = _watch(e)
        drive(e, feed, clock, stream, upto=window_end)
        assert e.feed_state == "DEGRADED"
        assert broker.sends == [] and e.open == {}                 # 6E's own data was clean: still nothing sent
        s = e.status(clock.t)
        assert s["feed"]["state"] == "DEGRADED"
        assert "6B" in s["status"] and "no new trades" in s["status"]
        # 6E is still read and scored, and its note says plainly why it was not entered
        assert any("Not entered: the data is degraded on 6B" in n for n in seen), seen[-5:]
        e.close()

    def test_open_positions_are_still_managed_while_the_feed_is_degraded(self, tmp_path):
        evs, marks = generate("sweep_up")
        e, feed, clock, broker = live_engine(tmp_path)
        drive(e, feed, clock, evs)
        (p,) = broker.positions(MAGIC)
        feed.q += a_future_that_goes_quiet(clock.t)                # 6B arrives, then goes quiet
        e.pump()
        for _ in range(15):
            clock.t += dt.timedelta(seconds=1)
            e.step(clock.t)
        assert e.feed_state == "DEGRADED"
        stops = [broker.positions(MAGIC)[0].sl]
        for k in range(1, 25):                                     # the spot runs the trade's way
            broker.sim.push_bar("EURUSD", p.entry_price + 0.0002 * k)
            clock.t += dt.timedelta(seconds=1)
            e.step(clock.t)
            assert e.feed_state == "DEGRADED"
            cur = broker.positions(MAGIC)
            if not cur:
                break
            stops.append(cur[0].sl)
        assert all(b >= a for a, b in zip(stops, stops[1:])) and stops[-1] > stops[0]
        assert len(broker.sends) == 1                              # managed, and nothing new sent
        e.close()


# ------------------------------------------------------------ unknown outcome --

def _watch(e, root="6E"):
    """Record the market note for ``root`` after every pass."""
    seen = []
    orig = e.step

    def step(now=None):
        out = orig(now)
        seen.append(e.notes.get(root, ""))
        return out
    e.step = step
    return seen


class TestAnUnknownSendBlocksThePair:
    # the sweep_up opportunity starting at 12:01:50 lasts into 12:02 - a new signal id (a new minute)
    START = NOON - dt.timedelta(seconds=30)

    def test_no_second_order_on_the_pair_while_the_first_ones_outcome_is_unknown(self, tmp_path):
        e, feed, clock, broker = live_engine(tmp_path, start=self.START)
        broker.fail_before_send = True                             # the reply never comes back
        seen = _watch(e)
        evs, marks = generate("sweep_up", start=self.START)
        notes = drive(e, feed, clock, evs)
        assert len(broker.sends) == 1, notes                       # never a second order while the first is unknown
        (row,) = e.journal.orders_in("UNKNOWN")
        assert any("outcome is still unknown" in n for n in seen), seen
        assert e._unknown_send("EURUSD", "another-signal")
        # the once-a-minute sweep: MISSING_PASSES positions reads in a row, over at least MISSING_SECONDS, do
        # not show it and the deal history has no entry for it - then it is ABSENT and no longer blocks
        sent = dt.datetime.fromisoformat(row["updated_utc"])
        for k in range(1, MISSING_PASSES + 2):
            clock.t = sent + dt.timedelta(seconds=MISSING_SECONDS + 60 * k)
            e.step(clock.t)
        assert e._unknown_send("EURUSD", "another-signal") == ""
        assert e.journal.order(row["signal_id"])["state"] == "ABSENT"
        assert len(broker.sends) == 1
        e.close()

    def test_an_unknown_send_that_reached_the_broker_is_adopted_and_unblocks(self, tmp_path):
        e, feed, clock, broker = live_engine(tmp_path, start=self.START)
        broker.fail_after_send = True                              # it reached the broker; the reply was lost
        evs, marks = generate("sweep_up", start=self.START)
        notes = drive(e, feed, clock, evs)
        assert len(broker.sends) == 1, notes
        pos = broker.positions(MAGIC)
        assert len(pos) == 1 and pos[0].sl > 0
        assert "EURUSD" in e.open and e.open["EURUSD"].ticket == pos[0].ticket
        assert e.journal.orders_in("UNKNOWN", "SENDING") == []
        assert e._unknown_send("EURUSD", "another-signal") == ""
        e.close()

    def test_a_lost_reply_the_mt5_adapter_reports_as_no_connection_is_adopted_never_sent_twice(self, tmp_path):
        # mt5.order_send() gave None after the terminal took the order: the adapter answers OrderResult(False,
        # 10031) and raises nothing. Booked REJECTED, the next minute's sweep closed the position as an orphan
        # (never booked here) and a second EURUSD order went out. It is UNKNOWN: found, adopted, sent once
        e, feed, clock, broker = live_engine(tmp_path, start=self.START)
        broker.lost_reply_retcode = RetCode.CONNECTION.value
        evs, marks = generate("sweep_up", start=self.START)
        notes = drive(e, feed, clock, evs)
        assert len(broker.sends) == 1, notes
        (pos,) = broker.positions(MAGIC)
        assert pos.sl > 0 and "EURUSD" in e.open and e.open["EURUSD"].ticket == pos.ticket
        assert e.journal.order(e.open["EURUSD"].signal_id)["state"] == "ADOPTED"
        assert broker.sim.closed == [] and not any(ev["kind"] == "ORPHAN" for ev in e.journal.events())
        e.close()


class BlindBroker(FakeBroker):
    """MetaTrader as the adapter sees it when positions_get() fails: while ``blind`` the positions read comes
    back EMPTY - not an error - exactly as the MT5 adapter returns it. The deal history still reads, with the
    entry deals MetaTrader keeps (the sim keeps closing deals only), unless ``history_fails``. While
    ``trade_server`` is False the terminal is up but not connected to the trade server (is_connected() is
    False) and what it shows is its own stale copy. ``deal_clock_offset`` stamps the entry deals on the
    broker's clock when that differs from this machine's."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.blind = False
        self.history_fails = False
        self.trade_server = True
        self.deal_clock_offset = dt.timedelta(0)
        self.entry_deals = []

    def is_connected(self):
        return self.trade_server and self.sim.is_connected()

    def send(self, req):
        before = {p.ticket for p in self.sim.positions()}
        try:
            return super().send(req)
        finally:
            for p in self.sim.positions():
                if p.ticket not in before:
                    self.entry_deals.append({"position": p.ticket, "symbol": p.symbol, "volume": p.volume,
                                             "magic": p.magic, "profit": 0.0, "commission": 0.0, "is_entry": True,
                                             "time": self.clock() + self.deal_clock_offset})

    def positions(self, magic=None):
        return [] if self.blind else self.sim.positions(magic)

    def deals_since(self, since_utc, magic=0, closing_only=True):
        if self.history_fails:
            raise RuntimeError("deal history unavailable from MetaTrader")
        entries = [] if closing_only else [d for d in self.entry_deals
                                           if d["time"] >= since_utc and (not magic or d["magic"] == magic)]
        return entries + self.sim.deals_since(since_utc, magic, closing_only)


class TestAnEmptyPositionsReadIsNotAbsence:
    """The MT5 adapter returns [] when positions_get() fails, so an empty read is not proof an unknown send
    never arrived. All synthetic test data."""

    def _unknown_send(self, e, broker, clock, reached):
        sig = make_signal(broker=broker, when=clock.t)
        e.journal.record_signal(sig, "NEW", mode="LIVE")
        if reached:
            broker.fail_after_send = True                          # the terminal took it; the reply was lost
        else:
            broker.fail_before_send = True                         # it never left
        assert e.bridge.submit(sig, broker.tick("EURUSD"), clock.t).status == "UNKNOWN"
        broker.fail_after_send = broker.fail_before_send = False
        return sig

    def _minutes(self, e, clock, n=1):
        for _ in range(n):
            clock.t += dt.timedelta(minutes=1)
            e.step(clock.t)

    def test_empty_reads_never_clear_an_order_that_is_at_the_broker(self, tmp_path):
        e, feed, clock, broker = live_engine(tmp_path, broker_cls=BlindBroker)
        sig = self._unknown_send(e, broker, clock, reached=True)
        (pos,) = broker.sim.positions(MAGIC)
        broker.blind = True                                        # every positions read now comes back empty
        for k in range(10):
            self._minutes(e, clock)
            assert e._unknown_send("EURUSD", "another-signal"), f"an empty read cleared it after {k + 1} minute(s)"
        assert e.journal.order(sig.signal_id)["state"] == "UNKNOWN"
        broker.blind = False                                       # the reads work again: found and adopted
        self._minutes(e, clock)
        assert "EURUSD" in e.open and e.open["EURUSD"].ticket == pos.ticket
        assert e.journal.order(sig.signal_id)["state"] == "ADOPTED"
        assert broker.sim.closed == [] and len(broker.sends) == 1
        assert not any(ev["kind"] == "ORPHAN" for ev in e.journal.events())
        e.close()

    def test_while_the_deal_history_cannot_be_read_it_stays_blocking(self, tmp_path):
        e, feed, clock, broker = live_engine(tmp_path, broker_cls=BlindBroker)
        sig = self._unknown_send(e, broker, clock, reached=False)
        broker.history_fails = True
        self._minutes(e, clock, 10)
        assert e._unknown_send("EURUSD", "another-signal") and e.journal.order(sig.signal_id)["state"] == "UNKNOWN"
        broker.history_fails = False
        self._minutes(e, clock)
        assert e._unknown_send("EURUSD", "another-signal") == ""
        assert e.journal.order(sig.signal_id)["state"] == "ABSENT"
        e.close()

    def test_one_that_never_arrived_clears_only_after_repeated_reads_and_a_restart_keeps_that(self, tmp_path):
        e, feed, clock, broker = live_engine(tmp_path, broker_cls=BlindBroker)
        sig = self._unknown_send(e, broker, clock, reached=False)
        for k in range(1, MISSING_PASSES):
            self._minutes(e, clock)
            assert e._unknown_send("EURUSD", "another-signal"), f"cleared after {k} read(s)"
        self._minutes(e, clock)
        assert e._unknown_send("EURUSD", "another-signal") == ""
        row = e.journal.order(sig.signal_id)
        assert row["state"] == "ABSENT" and "not open at the broker" in row["message"]
        assert any(ev["kind"] == "ABSENT" for ev in e.journal.events())
        e.close()
        e2, _, _, _ = live_engine(tmp_path, broker_cls=BlindBroker, clock=clock)      # a restart: same records
        assert e2._unknown_send("EURUSD", "another-signal") == ""
        e2.close()

    def test_one_that_arrived_and_was_closed_at_the_broker_clears(self, tmp_path):
        e, feed, clock, broker = live_engine(tmp_path, broker_cls=BlindBroker)
        sig = self._unknown_send(e, broker, clock, reached=True)
        (pos,) = broker.sim.positions(MAGIC)
        broker.blind = True
        broker.sim.push_bar("EURUSD", pos.sl - 0.0010)            # its broker stop is hit while we cannot see it
        assert broker.sim.positions(MAGIC) == [] and broker.sim.closed_deal(pos.ticket)
        self._minutes(e, clock, MISSING_PASSES)
        assert e._unknown_send("EURUSD", "another-signal") == ""
        row = e.journal.order(sig.signal_id)
        assert row["state"] == "ABSENT" and f"position {pos.ticket} and was closed there" in row["message"]
        assert len(broker.sends) == 1
        e.close()


# ------------------------------------------ second review: empty reads, the sweep, the trade server --

def no_retry_pause(monkeypatch):
    """The executor's closed_deal() pauses between retries (the broker's history can lag a close): not here."""
    import mintel.botexec as botexec
    monkeypatch.setattr(botexec.time, "sleep", lambda s: None)


def send_unknown(e, broker, clock, reached):
    """One LIVE send whose outcome is unknown: it reached the broker (the reply was lost) or it never left."""
    sig = make_signal(broker=broker, when=clock.t)
    e.journal.record_signal(sig, "NEW", mode="LIVE")
    if reached:
        broker.fail_after_send = True
    else:
        broker.fail_before_send = True
    assert e.bridge.submit(sig, broker.tick("EURUSD"), clock.t).status == "UNKNOWN"
    broker.fail_after_send = broker.fail_before_send = False
    return sig


def minutes(e, clock, n=1):
    notes = []
    for _ in range(n):
        clock.t += dt.timedelta(minutes=1)
        notes += e.step(clock.t)
    return notes


class ReadBackBlindBroker(BlindBroker):
    """Right after a fill the next ``reads`` positions reads come back EMPTY (the MT5 adapter's answer when
    positions_get() fails), and while they do MetaTrader cannot modify or close the position it cannot see.
    So the executor's read-back finds nothing, its stop check fails, its fail-closed close is refused - and
    it still reports 'closed at once' while the position stays open with the stop it was sent with."""

    def __init__(self, *a, reads=3, **k):
        super().__init__(*a, **k)
        self.reads = reads
        self.blind_reads = 0

    def _cannot_see(self):
        return self.blind or self.blind_reads > 0

    def send(self, req):
        res = super().send(req)
        if res.ok:
            self.blind_reads = self.reads
        return res

    def positions(self, magic=None):
        if self.blind_reads > 0:
            self.blind_reads -= 1
            return []
        return super().positions(magic)

    def modify_stops(self, ticket, sl, tp):
        if self._cannot_see():
            return OrderResult(False, RetCode.REJECT.value, ticket=ticket, comment="position not found")
        return self.sim.modify_stops(ticket, sl, tp)

    def close(self, ticket, volume=0.0, comment=""):
        if self._cannot_see():
            return OrderResult(False, RetCode.REJECT.value, ticket=ticket, comment="position not found")
        return self.sim.close(ticket, volume, comment)


class TestAFillClosedAtOnceIsCheckedAtTheBroker:
    """The broker filled the order; the executor's read-back came back empty, the stop check failed and it
    reported 'closed at once' without checking that the close happened. All synthetic test data."""

    @pytest.mark.parametrize("timed", [True, False])
    def test_an_unconfirmed_close_is_unknown_and_the_position_is_adopted_never_sent_twice(self, tmp_path, timed,
                                                                                          monkeypatch):
        no_retry_pause(monkeypatch)
        b = ReadBackBlindBroker()
        if timed:
            br, b, j = live_bridge(tmp_path, broker=b)
        else:
            j = Journal(tmp_path / "ian.sqlite")
            br = Mt5Bridge(make_executor("LIVE", b, MAGIC, TAG), j, b)
        sig = make_signal(broker=b)
        rep = br.submit(sig, b.tick("EURUSD"), BT)
        (pos,) = b.sim.positions(MAGIC)                            # the close never happened: open, with its stop
        assert pos.sl == pytest.approx(sig.intended_stop)
        assert rep.status == "UNKNOWN" and not rep.ok and "closed at once" in rep.message, rep.message
        row = j.order(sig.signal_id)
        assert row["state"] == "UNKNOWN"
        if timed:
            assert row["ticket"] == rep.ticket == pos.ticket
        for k in range(1, 6):                                      # nothing sent again; adopted once it can be seen
            again = br.submit(sig, b.tick("EURUSD"), BT + dt.timedelta(seconds=k))
            if again.status == FILLED_S:
                break
            assert again.status == DUPLICATE
        assert again.ok and again.ticket == pos.ticket and "nothing sent again" in again.message
        assert j.order(sig.signal_id)["state"] == "ADOPTED"
        assert len(b.sends) == 1 and b.sim.closed == []

    def test_a_close_the_broker_confirms_is_still_rejected(self, tmp_path):
        br, b, j = live_bridge(tmp_path)
        b.sim.fault.drop_stops_on_send = True                     # the broker ignored the stop ...
        b.sim.fault.fail_modify = True                            # ... and refused it again: closed at once
        sig = make_signal(broker=b)
        rep = br.submit(sig, b.tick("EURUSD"), BT)
        assert rep.status == "REJECTED" and rep.message.startswith("closed at once")
        assert b.positions(MAGIC) == [] and b.sim.closed_deal(b.sim.closed[0]["ticket"])
        assert j.order(sig.signal_id)["state"] == "REJECTED"

    def test_the_engine_sends_once_adopts_it_and_never_closes_it_as_an_orphan(self, tmp_path, monkeypatch):
        # booked REJECTED, the next minute's sweep closed the still-open position as an orphan (never booked
        # here, so this bot's own daily loss limit never saw it) and a later signal sent a second order
        no_retry_pause(monkeypatch)
        e, feed, clock, broker = live_engine(tmp_path, broker_cls=ReadBackBlindBroker)
        evs, marks = generate("sweep_up")
        notes = drive(e, feed, clock, evs)
        notes += minutes(e, clock, 3)
        assert len(broker.sends) == 1, notes
        (pos,) = broker.sim.positions(MAGIC)
        assert pos.sl > 0 and "EURUSD" in e.open and e.open["EURUSD"].ticket == pos.ticket, notes
        assert e.journal.order(e.open["EURUSD"].signal_id)["state"] == "ADOPTED"
        assert broker.sim.closed == [] and not any(ev["kind"] == "ORPHAN" for ev in e.journal.events())
        e.close()


class TestAMissingTradeIsBookedOnlyFromTheBrokersDeal:
    """An open LIVE trade the positions read stops showing - the MT5 adapter returns [] when positions_get()
    fails - is kept, with its broker stop, until the broker's closing deal shows how it closed. All
    synthetic test data."""

    def _open(self, tmp_path):
        e, feed, clock, broker = live_engine(tmp_path, broker_cls=BlindBroker)
        evs, marks = generate("sweep_up")
        drive(e, feed, clock, evs)
        (pos,) = broker.sim.positions(MAGIC)
        assert e.open["EURUSD"].ticket == pos.ticket
        return e, clock, broker, pos

    def test_ten_minutes_of_empty_reads_never_close_the_trade_or_make_an_orphan(self, tmp_path, monkeypatch):
        no_retry_pause(monkeypatch)
        e, clock, broker, pos = self._open(tmp_path)
        tr = e.open["EURUSD"]
        stop, mods = broker.sim.positions(MAGIC)[0].sl, len(broker.sim.modify_log)
        broker.blind = True
        notes = []
        for _ in range(40):                                        # ten minutes, a pass every 15 s
            clock.t += dt.timedelta(seconds=15)
            notes += e.step(clock.t)
            assert e.open.get("EURUSD") is tr, notes
        row = e.journal.trade(pos.ticket)
        assert row["closed_utc"] is None and row["exit_reason"] is None and row["net_pnl"] is None
        assert sum("kept as open with its broker stop" in n for n in notes) == 1, notes
        assert len(broker.sim.modify_log) == mods and broker.sim.positions(MAGIC)[0].sl == stop   # no stop moved
        broker.blind = False                                       # the reads work again: the same trade, managed
        notes += minutes(e, clock, 2)
        assert e.open.get("EURUSD") is tr and broker.sim.positions(MAGIC)[0].ticket == pos.ticket
        assert not any(ev["kind"] == "ORPHAN" for ev in e.journal.events()) and broker.sim.closed == []
        assert len(broker.sends) == 1
        e.close()

    def test_a_stop_hit_while_the_reads_are_empty_is_booked_with_the_brokers_figure(self, tmp_path, monkeypatch):
        no_retry_pause(monkeypatch)
        e, clock, broker, pos = self._open(tmp_path)
        broker.blind = True
        minutes(e, clock, 3)
        assert "EURUSD" in e.open
        broker.sim.push_bar("EURUSD", broker.sim.positions(MAGIC)[0].sl - 0.0010)   # its broker stop is hit
        deal = broker.sim.closed_deal(pos.ticket)
        assert deal
        minutes(e, clock, 1)
        assert e.open == {}
        row = e.journal.trade(pos.ticket)
        assert row["exit_reason"] == "STOP" and row["net_pnl"] == pytest.approx(round(deal["pnl"], 2))
        e.close()

    def test_the_sweep_adopts_a_position_of_a_filled_order_it_is_not_managing(self, tmp_path):
        e, feed, clock, broker = live_engine(tmp_path)
        sig = make_signal(broker=broker, when=clock.t)
        e.journal.record_signal(sig, "NEW", mode="LIVE")
        assert e.bridge.submit(sig, broker.tick("EURUSD"), clock.t).status == FILLED_S
        assert e.open == {} and e.journal.order(sig.signal_id)["state"] == "FILLED"   # never handed to the engine
        minutes(e, clock, 1)
        (pos,) = broker.sim.positions(MAGIC)
        tr = e.open.get("EURUSD")
        assert tr is not None and tr.ticket == pos.ticket and tr.stop == pytest.approx(pos.sl)
        assert e.journal.trade(pos.ticket)["closed_utc"] is None
        assert broker.sim.closed == [] and not any(ev["kind"] == "ORPHAN" for ev in e.journal.events())
        e.close()

    def test_a_trade_booked_closed_that_the_broker_still_holds_is_reopened_and_managed(self, tmp_path):
        # records from before this fix: an estimated close ('BROKER_CLOSED?') for a position still open
        e, clock, broker, pos = self._open(tmp_path)
        tr = e.open["EURUSD"]
        e._finalise(tr, tr.entry - 0.0003, -3.0, "BROKER_CLOSED?", clock.t, "estimated")
        assert e.open == {} and e.journal.trade(pos.ticket)["closed_utc"]
        minutes(e, clock, 1)
        assert e.open["EURUSD"].ticket == pos.ticket and e.journal.trade(pos.ticket)["closed_utc"] is None
        assert broker.sim.closed == [] and not any(ev["kind"] == "ORPHAN" for ev in e.journal.events())
        broker.sim.push_bar("EURUSD", broker.sim.positions(MAGIC)[0].sl - 0.0010)   # then its stop is hit
        deal = broker.sim.closed_deal(pos.ticket)
        minutes(e, clock, 1)
        row = e.journal.trade(pos.ticket)
        assert e.open == {} and row["exit_reason"] == "STOP" and row["net_pnl"] == pytest.approx(round(deal["pnl"], 2))
        e.close()

    def test_an_orphan_close_is_booked_with_the_brokers_figure(self, tmp_path):
        e, feed, clock, broker = live_engine(tmp_path)
        tick = broker.tick("GBPUSD")
        broker.sim.send(OrderRequest("GBPUSD", Side.BUY, 0.01, sl=tick.bid - 0.0020, magic=MAGIC, comment="IAN stray"))
        notes = e.step(clock.t)
        assert broker.sim.positions(MAGIC) == [] and any("orphan" in n for n in notes)
        (row,) = e.journal.closed_since(clock.t - dt.timedelta(hours=1))
        deal = broker.sim.closed_deal(row["ticket"])
        assert row["exit_reason"] == "ORPHAN" and row["net_pnl"] == pytest.approx(round(deal["pnl"], 2))
        assert row["mode"] == "LIVE" and row["data_source"] == "LIVE FEED" and row["spot_symbol"] == "GBPUSD"
        assert e.status(clock.t)["today"]["trades"] == 1             # this bot's own daily figures count it
        assert any(ev["kind"] == "ORPHAN" for ev in e.journal.events())
        e.close()

    def test_an_unknown_send_found_with_no_stop_is_closed_and_booked(self, tmp_path):
        e, feed, clock, broker = live_engine(tmp_path)
        broker.sim.fault.drop_stops_on_send = True                 # the broker ignored the stop sent with it
        broker.failed_reads = 1                                    # and the bridge's look straight after failed
        sig = send_unknown(e, broker, clock, reached=True)
        (pos,) = broker.sim.positions(MAGIC)
        assert pos.sl == 0.0
        minutes(e, clock, 1)
        assert broker.sim.positions(MAGIC) == [] and e.open == {}
        row = e.journal.trade(pos.ticket)
        deal = broker.sim.closed_deal(pos.ticket)
        assert row["signal_id"] == sig.signal_id and row["exit_reason"] == "ORPHAN"
        assert row["net_pnl"] == pytest.approx(round(deal["pnl"], 2))
        assert len(broker.sends) == 1
        e.close()


class TestAbsenceNeedsTheTradeServer:
    """An unknown send is taken as not at the broker only from reads made while MetaTrader is connected to
    the trade server, and the deal history is searched from well before the send. All synthetic test data."""

    def test_reads_while_off_the_trade_server_never_clear_an_unknown_send(self, tmp_path):
        e, feed, clock, broker = live_engine(tmp_path, broker_cls=BlindBroker)
        sig = send_unknown(e, broker, clock, reached=False)
        broker.trade_server = False                                # the terminal shows its own stale copy
        for k in range(10):
            minutes(e, clock)
            assert e._unknown_send("EURUSD", "another-signal"), f"cleared after {k + 1} stale read(s)"
        assert e.journal.order(sig.signal_id)["state"] == "UNKNOWN"
        broker.trade_server = True                                 # connected again: the count starts again
        for k in range(1, MISSING_PASSES):
            minutes(e, clock)
            assert e._unknown_send("EURUSD", "another-signal"), f"cleared after {k} fresh read(s)"
        minutes(e, clock)
        assert e._unknown_send("EURUSD", "another-signal") == ""
        assert e.journal.order(sig.signal_id)["state"] == "ABSENT"
        e.close()

    def test_the_deal_history_is_searched_from_well_before_the_send(self, tmp_path):
        # the broker's clock runs 30 s behind this machine's: the order's entry deal is stamped before the send
        e, feed, clock, broker = live_engine(tmp_path, broker_cls=BlindBroker)
        broker.deal_clock_offset = -dt.timedelta(seconds=30)
        sig = send_unknown(e, broker, clock, reached=True)
        (pos,) = broker.sim.positions(MAGIC)
        broker.blind = True
        for k in range(10):
            minutes(e, clock)
            assert e._unknown_send("EURUSD", "another-signal"), f"cleared after {k + 1} minute(s)"
        assert e.journal.order(sig.signal_id)["state"] == "UNKNOWN"
        broker.blind = False
        minutes(e, clock)
        assert e.open["EURUSD"].ticket == pos.ticket and e.journal.order(sig.signal_id)["state"] == "ADOPTED"
        assert len(broker.sends) == 1 and broker.sim.closed == []
        e.close()


# ---------------------------------------------------------- LIVE, no MT5 --

class TestLiveWithoutMetaTrader:
    def test_the_engine_runs_places_nothing_and_says_why(self, tmp_path):
        e = IanEngine(tmp_path, IanConfig(mode="LIVE", record=False), broker=None, feed=NotConfiguredFeed())
        assert e.executor is None and e.mode == "OFF"
        notes = e.step(NOON)
        assert any("LIVE needs MetaTrader: nothing can be placed" in n for n in notes), notes
        s = json.loads((tmp_path / "ian-status.json").read_text())
        assert "LIVE needs MetaTrader" in s["status"]
        e.close()

    def test_todays_live_results_still_show_with_no_metatrader(self, tmp_path):
        # a made-up LIVE record (synthetic test data, not a result): with no executor the engine's own mode
        # reads OFF, and today's LIVE figures must not drop to zero because of that
        from mintel.ian.journal import Journal
        j = Journal(tmp_path / "ian.sqlite")
        j.open_trade({"ticket": 5, "signal_id": "s", "instrument": "6EZ6", "root": "6E", "spot_symbol": "EURUSD",
                      "side": "BUY", "volume": 0.1, "entry": 1.1, "initial_stop": 1.099, "stop": 1.099,
                      "opened_utc": (NOON - dt.timedelta(hours=1)).isoformat(), "mode": "LIVE",
                      "data_source": "LIVE FEED", "session_date": NOON.date().isoformat()})
        j.close_trade(5, closed_utc=(NOON - dt.timedelta(minutes=30)).isoformat(), exit_price=1.101, net_pnl=12.5,
                      realised_r=1.0, exit_reason="STOP")
        j.close()
        e = IanEngine(tmp_path, IanConfig(mode="LIVE", record=False), broker=None, feed=NotConfiguredFeed(),
                      clock=Clock(NOON))
        today = e.status(NOON)["today"]
        assert today["trades"] == 1 and today["net"] == pytest.approx(12.5)
        e.close()


@pytest.mark.usefixtures("process_sandbox")
class TestProcessStart:
    def _setup(self, tmp, ian):
        data = tmp / "data"
        data.mkdir()
        (data / "ian.json").write_text(json.dumps(ian))
        cfgp = tmp / "config.json"
        cfgp.write_text(json.dumps({"ops": {"data_dir": str(data), "log_dir": str(tmp / "logs")}}))
        return cfgp, data

    def test_live_with_no_metatrader_runs_safely_and_leaves_no_pid_file(self, tmp_path):
        from mintel.ian.run import main
        cfgp, data = self._setup(tmp_path, {"mode": "LIVE"})
        assert main(["--config", str(cfgp), "--cycles", "2", "--no-broker"]) == 0
        s = json.loads((data / "ian-status.json").read_text())
        assert "LIVE needs MetaTrader" in s["status"]
        assert not (data / "ian.pid").exists()

    def test_while_metatrader_cannot_be_reached_the_page_says_so(self, tmp_path, monkeypatch):
        # MetaTrader is installed but the terminal will not log in: the process waits for it. The page must
        # say so, not keep showing whatever an earlier run last wrote
        import os
        import signal
        import mintel.run as trader_run
        from mintel.ian.run import main
        cfgp, data = self._setup(tmp_path, {"mode": "LIVE"})
        (data / "ian-status.json").write_text(json.dumps({"mode": "LIVE", "status": "an old status from yesterday",
                                                          "updated": "2026-10-06T12:00:00+00:00"}))

        class NoTerminal:
            attempts = 0

            def connect(self):
                NoTerminal.attempts += 1
                os.kill(os.getpid(), signal.SIGTERM)              # then stop it, as STOP-BOT would
                return False
        monkeypatch.setattr(trader_run, "build_broker", lambda cfg: NoTerminal())
        assert main(["--config", str(cfgp), "--cycles", "1"]) == 0
        assert NoTerminal.attempts == 1
        s = json.loads((data / "ian-status.json").read_text())
        assert "WAITING FOR METATRADER" in s["status"] and "old status" not in s["status"]
        assert "nothing is placed" in s["status"]
        assert s["mode"] == "LIVE" and s["mt5"]["connected"] is False and s["feed"]["state"] == "NOT STARTED"
        assert s["open_positions"] == [] and s["top_opportunity"] is None
        assert not (data / "ian.pid").exists()

    def test_a_start_up_failure_removes_the_pid_file(self, tmp_path, monkeypatch):
        import mintel.ian.run as run
        cfgp, data = self._setup(tmp_path, {"mode": "PAPER"})

        def boom(*a, **k):
            raise RuntimeError("the feed could not be built")
        monkeypatch.setattr(run, "build_feed", boom)
        with pytest.raises(RuntimeError):
            run.main(["--config", str(cfgp), "--cycles", "1", "--no-broker"])
        assert not (data / "ian.pid").exists()


# --------------------------------------------------------------------- magic --

class TestTheMagicIsFixed:
    OTHER = 123456                                                  # e.g. the main bot's magic

    def test_ian_json_cannot_change_the_magic(self, tmp_path, caplog):
        p = tmp_path / "ian.json"
        p.write_text(json.dumps({"mode": "LIVE", "magic": self.OTHER}))
        with caplog.at_level(logging.WARNING, logger="mintel.ian"):
            c = IanConfig.load(p)
        assert c.magic == MAGIC == 990911 and c.mode == "LIVE"
        assert any(str(self.OTHER) in r.getMessage() and "magic" in r.getMessage() for r in caplog.records)
        p.write_text(json.dumps({"mode": "LIVE", "magic": "990911"}))
        assert IanConfig.load(p).magic == MAGIC

    def test_the_live_sweep_never_closes_another_bots_position_even_if_ian_json_names_its_magic(self, tmp_path):
        p = tmp_path / "ian.json"
        p.write_text(json.dumps({"mode": "LIVE", "magic": self.OTHER, "record": False}))
        clock = Clock(NOON)
        broker = FakeBroker(clock=clock)
        tick = broker.tick("GBPUSD")
        broker.sim.send(OrderRequest("GBPUSD", Side.BUY, 0.01, sl=tick.bid - 0.0020, magic=self.OTHER, comment="MAIN"))
        e = IanEngine(tmp_path, IanConfig.load(p), broker=broker, feed=ScriptedFeed(), clock=clock)
        e.step(clock.t)
        assert len(broker.positions(self.OTHER)) == 1               # the other bot's position is untouched
        e.close()

    def test_the_engine_refuses_live_with_any_other_magic(self, tmp_path):
        broker = FakeBroker()
        with pytest.raises(ValueError, match="990911"):
            IanEngine(tmp_path, IanConfig(mode="LIVE", magic=self.OTHER, record=False), broker=broker,
                      feed=ScriptedFeed())
        assert broker.sends == []
