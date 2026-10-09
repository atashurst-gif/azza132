"""Financial Ian: fixes from the third review (9 October).

* A send whose outcome is unknown, found at the broker with no stop, is protected at once: the bridge
  looks once more straight after the send and places the stop (or closes the position), and the engine's
  sweep runs on the very next pass instead of up to a minute later.
* A position that filled and was closed at once because the broker held no stop is a real trade: the
  REJECTED report carries its ticket and the engine books it (NO_STOP, with the broker's own figure), so
  this bot's own daily loss limit and today's figures count it.
* A trade kept while the broker's positions do not show it says so on the page, until it is seen again.
* A position the sweep takes over keeps the broker's open time: the four-hour limit runs from the fill.
* The sweep's note for one of our own orders closed because its pair already has an open trade says so.

All data here is synthetic test data; nothing in this file is a result.
"""
import datetime as dt

import pytest

from mintel.broker.base import OrderResult, RetCode
from mintel.ian import MAGIC
from mintel.ian.bridge import FILLED_S
from mintel.ian.book import received_at
from mintel.ian.engine import MISSING_PASSES, MISSING_SECONDS, IanConfig
from mintel.ian.feeds.synthetic import generate
from tests.test_ian_bridge import T as BT, FakeBroker, live_bridge, make_signal
from tests.test_ian_engine import drive
from tests.test_ian_review_fixes import BlindBroker, live_engine, minutes, no_retry_pause, send_unknown

UTC = dt.timezone.utc


class BareFillBroker(FakeBroker):
    """The broker fills the order but drops the stop sent with it. Straight after the fill the next
    ``blind_reads`` positions reads come back EMPTY (the MT5 adapter's answer when positions_get() fails),
    and MetaTrader refuses the next ``refuse_stops`` stop changes and the next ``refuse_closes`` closes. So
    the executor cannot confirm the stop, cannot place it and cannot close the position, and reports
    'closed at once' while the position is open with no stop. After that it behaves normally."""

    def __init__(self, *a, refuse_stops=1, refuse_closes=1, blind_reads=1, **k):
        super().__init__(*a, **k)
        self.sim.fault.drop_stops_on_send = True
        self._plan = (refuse_stops, refuse_closes, blind_reads)
        self.refuse_stops = self.refuse_closes = self.blind_reads = 0

    def send(self, req):
        res = super().send(req)
        if res.ok:
            self.refuse_stops, self.refuse_closes, self.blind_reads = self._plan
        return res

    def positions(self, magic=None):
        if self.blind_reads > 0:
            self.blind_reads -= 1
            return []
        return super().positions(magic)

    def modify_stops(self, ticket, sl, tp):
        if self.refuse_stops > 0:
            self.refuse_stops -= 1
            return OrderResult(False, RetCode.REJECT.value, ticket=ticket, comment="refused")
        return self.sim.modify_stops(ticket, sl, tp)

    def close(self, ticket, volume=0.0, comment=""):
        if self.refuse_closes > 0:
            self.refuse_closes -= 1
            return OrderResult(False, RetCode.REJECT.value, ticket=ticket, comment="refused")
        return self.sim.close(ticket, volume, comment)


def bare(broker):
    return [p for p in broker.sim.positions(MAGIC) if not p.sl]


# ------------------------------------------------- a bare fill is protected at once --

class TestABareFillIsProtectedAtOnce:
    def test_the_bridge_places_the_stop_before_it_returns(self, tmp_path, monkeypatch):
        no_retry_pause(monkeypatch)
        br, b, j = live_bridge(tmp_path, broker=BareFillBroker())
        sig = make_signal(broker=b)
        rep = br.submit(sig, b.tick("EURUSD"), BT)
        (pos,) = b.sim.positions(MAGIC)
        assert pos.sl == pytest.approx(sig.intended_stop), "left at the broker with no stop"
        assert rep.status == "UNKNOWN" and rep.ticket == pos.ticket and "was placed at once" in rep.message
        assert j.order(sig.signal_id)["state"] == "UNKNOWN" and len(b.sends) == 1
        again = br.submit(sig, b.tick("EURUSD"), BT + dt.timedelta(seconds=1))      # then adopted, never re-sent
        assert again.status == FILLED_S and again.ticket == pos.ticket and len(b.sends) == 1

    def test_when_the_stop_is_refused_again_the_bridge_closes_it_and_reports_the_ticket(self, tmp_path,
                                                                                         monkeypatch):
        no_retry_pause(monkeypatch)
        br, b, j = live_bridge(tmp_path, broker=BareFillBroker(refuse_stops=2))
        sig = make_signal(broker=b)
        rep = br.submit(sig, b.tick("EURUSD"), BT)
        assert b.sim.positions(MAGIC) == []
        (closed,) = b.sim.closed
        assert rep.status == "REJECTED" and rep.message.startswith("closed at once")
        assert rep.ticket == closed["ticket"] and rep.price > 0 and rep.volume > 0
        row = j.order(sig.signal_id)
        assert row["state"] == "REJECTED" and row["ticket"] == closed["ticket"]

    def test_the_engine_sweeps_on_the_pass_right_after_an_unknown_send(self, tmp_path, monkeypatch):
        # the bridge's own look straight after the send is blind too, so only the sweep can protect it; a new
        # signal id every second means the next pass cannot re-send the same id and protect it that way
        no_retry_pause(monkeypatch)
        cfg = IanConfig(mode="LIVE", record=False, signal_bucket_s=1)
        e, feed, clock, broker = live_engine(tmp_path, cfg=cfg,
                                             broker_cls=lambda clock: BareFillBroker(clock=clock, blind_reads=2))
        evs, marks = generate("sweep_up")
        bare_passes, notes = 0, []
        nxt = None
        for ev in evs:
            t = received_at(ev)
            nxt = nxt or t
            while t >= nxt:
                clock.t = nxt
                notes += e.step(nxt)
                bare_passes += bool(bare(broker))
                nxt += dt.timedelta(seconds=1)
            feed.q.append(ev)
            e.pump()
        assert len(broker.sends) == 1, notes
        assert bare_passes == 1, f"a position with no stop was left for {bare_passes} passes: {notes}"
        (closed,) = broker.sim.closed
        row = e.journal.trade(closed["ticket"])
        assert row is not None and row["closed_utc"] and row["net_pnl"] == pytest.approx(round(closed["pnl"], 2))
        e.close()


# ---------------------------------------- a fill closed at once is booked as a trade --

class TestAFillClosedAtOnceIsBooked:
    def test_the_bridges_close_of_a_bare_fill_is_booked_and_counts_today(self, tmp_path, monkeypatch):
        no_retry_pause(monkeypatch)
        cfg = IanConfig(mode="LIVE", record=False, max_daily_loss_money=0.01)
        e, feed, clock, broker = live_engine(tmp_path, cfg=cfg,
                                             broker_cls=lambda clock: BareFillBroker(clock=clock, refuse_stops=2))
        evs, marks = generate("sweep_up")
        notes = drive(e, feed, clock, evs)
        assert broker.sim.positions(MAGIC) == [] and e.open == {}, notes
        first = broker.sim.closed[0]
        row = e.journal.trade(first["ticket"])
        assert row is not None, f"ticket {first['ticket']} closed at the broker but never booked: {notes}"
        deal = broker.sim.closed_deal(first["ticket"])
        assert row["exit_reason"] == "NO_STOP" and row["net_pnl"] == pytest.approx(round(deal["pnl"], 2))
        assert row["mode"] == "LIVE" and row["data_source"] == "LIVE FEED" and row["root"] == "6E"
        today = e.status(clock.t)["today"]
        assert today["trades"] == len(broker.sim.closed) and today["net"] < 0
        assert "lost" in e._entry_block(clock.t)                    # this bot's own daily loss limit sees it
        assert len(broker.sends) == 1, notes                        # and nothing more was entered today
        e.close()

    def test_an_executor_close_the_broker_confirms_is_booked(self, tmp_path, monkeypatch):
        no_retry_pause(monkeypatch)
        e, feed, clock, broker = live_engine(tmp_path, cfg=IanConfig(mode="LIVE", record=False, max_open=1))
        broker.sim.fault.drop_stops_on_send = True                  # the broker ignored the stop ...
        broker.sim.fault.fail_modify = True                         # ... and refused it again: closed at once
        evs, marks = generate("sweep_up")
        notes = drive(e, feed, clock, evs)
        assert broker.sim.closed, notes
        for c in broker.sim.closed:
            row = e.journal.trade(c["ticket"])
            assert row is not None, f"ticket {c['ticket']} closed at the broker but never booked: {notes}"
            assert row["exit_reason"] == "NO_STOP"
            assert row["net_pnl"] == pytest.approx(round(broker.sim.closed_deal(c["ticket"])["pnl"], 2))
        assert e.status(clock.t)["today"]["trades"] == len(broker.sim.closed)
        e.close()


# ---------------------------------------------- a trade not shown: the page says so --

class TestATradeNotShownSaysSoOnThePage:
    def test_the_open_position_says_since_when_and_clears_when_seen_again(self, tmp_path, monkeypatch):
        no_retry_pause(monkeypatch)
        e, feed, clock, broker = live_engine(tmp_path, broker_cls=BlindBroker)
        drive(e, feed, clock, generate("sweep_up")[0])
        (pos,) = broker.sim.positions(MAGIC)
        broker.blind = True
        start = clock.t
        for _ in range(int(MISSING_SECONDS // 15) + MISSING_PASSES + 2):
            clock.t += dt.timedelta(seconds=15)
            e.step(clock.t)
        (row,) = e.status(clock.t)["open_positions"]
        assert row["ticket"] == pos.ticket
        assert row["trail"].startswith("not shown in the broker's positions since"), row
        assert "kept with its broker stop, not moved" in row["trail"]
        since = dt.datetime.fromisoformat(row["not_seen_since"])
        assert start < since <= clock.t
        broker.blind = False                                        # seen again: the note goes
        clock.t += dt.timedelta(seconds=15)
        e.step(clock.t)
        (row,) = e.status(clock.t)["open_positions"]
        assert row["not_seen_since"] is None and "not shown" not in (row["trail"] or "")
        e.close()


# ---------------------------------------- a take-over keeps the broker's open time --

class TestATakeOverKeepsTheOpenTime:
    def test_the_hold_limit_runs_from_the_fill_not_the_take_over(self, tmp_path):
        cfg = IanConfig(mode="LIVE", record=False, hold_max_minutes=30)
        e, feed, clock, broker = live_engine(tmp_path, cfg=cfg)
        sig = send_unknown(e, broker, clock, reached=True)           # filled at 12:00; this run never knew
        (pos,) = broker.sim.positions(MAGIC)
        assert pos.sl > 0 and e.open == {}
        clock.t = pos.open_time + dt.timedelta(minutes=31)          # found and taken over at 12:31
        notes = e.step(clock.t)
        row = e.journal.trade(pos.ticket)
        assert row is not None and row["signal_id"] == sig.signal_id
        assert dt.datetime.fromisoformat(row["opened_utc"]) == pos.open_time
        assert row["session_date"] == pos.open_time.date().isoformat()
        assert row["exit_reason"] == "TIME", notes                  # held 31 minutes, over its 30
        assert broker.sim.positions(MAGIC) == []
        e.close()


# ------------------------------------------ the sweep's note for a pair already held --

class TestTheSweepNoteForAPairAlreadyHeld:
    def test_one_of_our_orders_on_a_pair_already_held_is_closed_with_an_accurate_note(self, tmp_path):
        e, feed, clock, broker = live_engine(tmp_path)
        first = make_signal(broker=broker, when=clock.t)
        e.journal.record_signal(first, "NEW", mode="LIVE")
        assert e.bridge.submit(first, broker.tick("EURUSD"), clock.t).status == FILLED_S
        minutes(e, clock, 1)                                        # the sweep takes the first one over
        held = e.open["EURUSD"]
        # an order whose outcome was unknown turns up later, with its stop, while EURUSD is already held
        other = make_signal(broker=broker, when=clock.t, sid="later-order-0001")
        e.journal.record_signal(other, "NEW", mode="LIVE")
        broker.fail_after_send = True
        assert e.bridge.submit(other, broker.tick("EURUSD"), clock.t).status == "UNKNOWN"
        broker.fail_after_send = False
        p2 = next(p for p in broker.sim.positions(MAGIC) if p.ticket != held.ticket)
        assert p2.sl > 0
        notes = minutes(e, clock, 1)
        note = next(n for n in notes if str(p2.ticket) in n)
        assert "one of our orders (signal later-order-0001)" in note
        assert "EURUSD already has an open trade" in note and "no record here" not in note
        assert e.open["EURUSD"] is held and [p.ticket for p in broker.sim.positions(MAGIC)] == [held.ticket]
        assert e.journal.trade(p2.ticket)["exit_reason"] == "ORPHAN"
        e.close()
