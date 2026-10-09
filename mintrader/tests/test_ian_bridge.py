"""Financial Ian's MT5 bridge: the checks (tradability, quote freshness, spread, spot confirmation, the
stop), the size, the broker stop at once, and IDEMPOTENCY - a duplicate or a retry after a communication
failure never produces a second order, even across a restart."""
import datetime as dt

import pytest

from mintel.botexec import make_executor
from mintel.broker.base import OrderResult, RetCode, Side, Tick
from mintel.broker.sim import SimBroker
from mintel.ian import MAGIC, STRATEGY_ID, TAG
from mintel.ian.bridge import DUPLICATE, FILLED_S, REFUSED, BridgeConfig, Mt5Bridge, TimedBroker, comment_for
from mintel.ian.journal import ADOPTED, FILLED, UNKNOWN, Journal
from mintel.ian.signal import Signal, make_signal_id

UTC = dt.timezone.utc
T = dt.datetime(2026, 10, 7, 12, 0, tzinfo=UTC)


class FakeBroker:
    """SimBroker for contracts, orders and positions; quotes stamped with a clock the test controls; and
    communication failures on demand (before the order reaches the broker, or after)."""

    def __init__(self, now=T, clock=None):
        self.sim = SimBroker(seed=11, start=now, history_bars=300)
        self.sim.connect()
        self.clock = clock or (lambda: now)
        self.quote_age_s = 0.0
        self.fail_before_send = False
        self.fail_after_send = False
        self.lost_reply_retcode = None      # the next send fills, but the answer is this retcode (one time)
        self.failed_reads = 0               # the next this-many positions reads fail (MetaTrader cannot read them)
        self.sends = []

    def positions(self, magic=None):
        if self.failed_reads > 0:
            self.failed_reads -= 1
            raise ConnectionError("positions read failed")
        return self.sim.positions(magic)

    def tick(self, symbol):
        t = self.sim.tick(symbol)
        if t is None:
            return None
        return Tick(symbol, self.clock() - dt.timedelta(seconds=self.quote_age_s), t.bid, t.ask, t.last, t.volume)

    def send(self, req):
        self.sends.append(req)
        if self.fail_before_send:
            raise ConnectionError("connection reset before the order left")
        res = self.sim.send(req)
        if self.fail_after_send:
            raise ConnectionError("connection reset after the order left")
        if self.lost_reply_retcode is not None:
            # what the MT5 adapter returns when mt5.order_send() gives None (an IPC timeout after the
            # terminal took the order): a plain "not ok" with retcode 10031, no exception
            code, self.lost_reply_retcode = self.lost_reply_retcode, None
            return OrderResult(False, int(code), comment="(-10005, 'IPC timeout')", requested_volume=req.volume,
                               idempotency_key=req.idempotency_key)
        return res

    def __getattr__(self, name):
        return getattr(self.sim, name)


def make_signal(direction="BUY", when=T, stop_pips=10.0, symbol="EURUSD", root="6E", broker=None, sid=None):
    b = broker or FakeBroker()
    tick = b.tick(symbol)
    spec = b.spec(symbol)
    side = Side.BUY if direction == "BUY" else Side.SELL
    touch = tick.ask if side is Side.BUY else tick.bid
    dist = stop_pips * spec.pip_size
    stop = spec.normalise_price(touch - side.sign * dist)
    sid = sid or make_signal_id(STRATEGY_ID, root, side.sign, when)
    return Signal(sid, STRATEGY_ID, f"{root}Z6", root, symbol, direction, side.sign, 0.8, 80.0,
                  "buyers are taking the offers", stop, abs(touch - stop), touch, when, when,
                  latency={"data_received": when.isoformat(), "signal": when.isoformat()})


def live_bridge(tmp_path, broker=None, cfg=None):
    b = broker or FakeBroker()
    timed = TimedBroker(b)
    ex = make_executor("LIVE", timed, MAGIC, TAG)
    j = Journal(tmp_path / "ian.sqlite")
    return Mt5Bridge(ex, j, b, cfg or BridgeConfig(), timed=timed), b, j


class TestTheSignalId:
    def test_same_strategy_future_direction_and_minute_same_id(self):
        a = make_signal_id(STRATEGY_ID, "6EZ6", 1, T)
        assert a == make_signal_id(STRATEGY_ID, "6E", 1, T + dt.timedelta(seconds=59))
        assert a != make_signal_id(STRATEGY_ID, "6E", 1, T + dt.timedelta(seconds=60))
        assert a != make_signal_id(STRATEGY_ID, "6E", -1, T)
        assert a != make_signal_id(STRATEGY_ID, "6B", 1, T)
        assert len(a) == 20


class TestLiveOrders:
    def test_the_order_carries_the_magic_the_comment_and_the_stop(self, tmp_path):
        br, b, j = live_bridge(tmp_path)
        sig = make_signal(broker=b)
        rep = br.submit(sig, b.tick("EURUSD"), T)
        assert rep.status == FILLED_S and rep.ok and rep.ticket
        pos = b.positions(MAGIC)
        assert len(pos) == 1 and pos[0].magic == MAGIC and pos[0].sl == pytest.approx(sig.intended_stop)
        assert pos[0].comment == comment_for(sig.signal_id) and pos[0].comment.startswith("IAN ")
        assert j.order(sig.signal_id)["state"] == FILLED
        assert rep.risk_money == pytest.approx(10.0, rel=0.15)
        for hop in ("data_received", "signal", "order_request", "broker_accept", "fill"):
            assert rep.latency.get(hop), hop
        assert rep.latency["mt5_receipt"] is None and "not measured" in rep.latency["note"]

    def test_a_duplicate_never_sends_a_second_order(self, tmp_path):
        br, b, j = live_bridge(tmp_path)
        sig = make_signal(broker=b)
        assert br.submit(sig, b.tick("EURUSD"), T).status == FILLED_S
        again = br.submit(sig, b.tick("EURUSD"), T + dt.timedelta(seconds=5))
        assert again.status == DUPLICATE and not again.ok and "nothing sent again" in again.message
        same_minute = make_signal(broker=b, when=T + dt.timedelta(seconds=30))
        assert same_minute.signal_id == sig.signal_id
        assert br.submit(same_minute, b.tick("EURUSD"), T + dt.timedelta(seconds=30)).status == DUPLICATE
        assert len(b.sends) == 1 and len(b.positions(MAGIC)) == 1

    def test_a_retry_after_the_connection_dropped_mid_send_adopts_the_one_order(self, tmp_path):
        br, b, j = live_bridge(tmp_path)
        sig = make_signal(broker=b)
        b.fail_after_send = True                    # the order reached the broker; the answer never came back
        first = br.submit(sig, b.tick("EURUSD"), T)
        assert first.status == UNKNOWN and not first.ok
        assert j.order(sig.signal_id)["state"] == UNKNOWN
        b.fail_after_send = False
        retry = br.submit(sig, b.tick("EURUSD"), T + dt.timedelta(seconds=3))
        assert retry.status == FILLED_S and retry.ok and "nothing sent again" in retry.message
        assert len(b.sends) == 1 and len(b.positions(MAGIC)) == 1
        assert j.order(sig.signal_id)["state"] == ADOPTED and retry.ticket == b.positions(MAGIC)[0].ticket

    def test_an_adopted_order_without_a_stop_gets_the_signal_stop_at_once(self, tmp_path):
        br, b, j = live_bridge(tmp_path)
        b.sim.fault.drop_stops_on_send = True        # the broker ignored the SL sent with the order ...
        sig = make_signal(broker=b)
        b.fail_after_send = True                    # ... and the answer never came back ...
        b.failed_reads = 1                          # ... nor could the positions be read straight after
        assert br.submit(sig, b.tick("EURUSD"), T).status == UNKNOWN
        b.fail_after_send = False
        assert b.positions(MAGIC)[0].sl == 0.0
        retry = br.submit(sig, b.tick("EURUSD"), T + dt.timedelta(seconds=3))
        assert retry.status == FILLED_S and retry.ok and "no stop at the broker" in retry.message
        pos = b.positions(MAGIC)
        assert len(pos) == 1 and pos[0].sl == pytest.approx(sig.intended_stop)
        assert retry.stop == pytest.approx(sig.intended_stop) and retry.risk_money > 0
        assert len(b.sends) == 1 and j.order(sig.signal_id)["state"] == ADOPTED

    def test_an_adopted_order_whose_stop_is_refused_is_closed_and_rejected(self, tmp_path):
        br, b, j = live_bridge(tmp_path)
        b.sim.fault.drop_stops_on_send = True
        sig = make_signal(broker=b)
        b.fail_after_send = True
        b.failed_reads = 1                          # not seen straight after the send: found at the retry
        assert br.submit(sig, b.tick("EURUSD"), T).status == UNKNOWN
        b.fail_after_send = False
        b.sim.fault.fail_modify = True
        retry = br.submit(sig, b.tick("EURUSD"), T + dt.timedelta(seconds=3))
        assert retry.status == "REJECTED" and not retry.ok and retry.message.startswith("closed at once")
        assert b.positions(MAGIC) == [] and j.order(sig.signal_id)["state"] == "REJECTED"
        assert br.submit(sig, b.tick("EURUSD"), T + dt.timedelta(seconds=6)).status == DUPLICATE
        assert len(b.sends) == 1

    def test_an_adopted_order_past_its_stop_is_closed(self, tmp_path):
        br, b, j = live_bridge(tmp_path)
        b.sim.fault.drop_stops_on_send = True
        sig = make_signal(broker=b, stop_pips=10.0)
        b.fail_after_send = True
        b.failed_reads = 1                          # not seen straight after the send: found at the retry
        br.submit(sig, b.tick("EURUSD"), T)
        b.fail_after_send = False
        b.sim.push_bar("EURUSD", b.tick("EURUSD").bid - 0.0015)   # 15 pips down: through the 10-pip stop
        retry = br.submit(sig, b.tick("EURUSD"), T + dt.timedelta(seconds=3))
        assert retry.status == "REJECTED" and "already through its stop" in retry.message
        assert b.positions(MAGIC) == [] and b.sim.modify_log == []

    @pytest.mark.parametrize("code", [RetCode.CONNECTION.value, 10012])        # no connection; timed out
    @pytest.mark.parametrize("timed", [True, False])
    def test_a_lost_reply_reported_as_a_retcode_is_unknown_and_the_order_is_adopted(self, tmp_path, code, timed):
        # the real MT5 adapter does not raise when the reply is lost: order_send() gives None and the adapter
        # answers OrderResult(False, 10031). The order may well be at the broker, so it must be UNKNOWN (never
        # REJECTED): the retry searches the broker's positions and adopts it, and nothing is sent twice
        if timed:
            br, b, j = live_bridge(tmp_path)
        else:
            b = FakeBroker()
            j = Journal(tmp_path / "ian.sqlite")
            br = Mt5Bridge(make_executor("LIVE", b, MAGIC, TAG), j, b)
        sig = make_signal(broker=b)
        b.lost_reply_retcode = code
        first = br.submit(sig, b.tick("EURUSD"), T)
        assert first.status == UNKNOWN and not first.ok and "may have reached the broker" in first.message
        assert j.order(sig.signal_id)["state"] == UNKNOWN
        assert len(b.positions(MAGIC)) == 1                    # the terminal did take it
        retry = br.submit(sig, b.tick("EURUSD"), T + dt.timedelta(seconds=3))
        assert retry.status == FILLED_S and retry.ok and "nothing sent again" in retry.message
        assert len(b.sends) == 1 and len(b.positions(MAGIC)) == 1
        assert j.order(sig.signal_id)["state"] == ADOPTED and retry.ticket == b.positions(MAGIC)[0].ticket

    def test_a_retry_when_the_order_never_left_still_sends_nothing(self, tmp_path):
        br, b, j = live_bridge(tmp_path)
        sig = make_signal(broker=b)
        b.fail_before_send = True
        assert br.submit(sig, b.tick("EURUSD"), T).status == UNKNOWN
        b.fail_before_send = False
        retry = br.submit(sig, b.tick("EURUSD"), T + dt.timedelta(seconds=3))
        assert retry.status == DUPLICATE and len(b.positions(MAGIC)) == 0
        assert len(b.sends) == 1                    # the one attempt; never a second

    def test_the_dedupe_store_survives_a_restart(self, tmp_path):
        br, b, j = live_bridge(tmp_path)
        sig = make_signal(broker=b)
        assert br.submit(sig, b.tick("EURUSD"), T).ok
        j.close()
        br2, _, j2 = live_bridge(tmp_path, broker=b)          # a new process, the same data/ian.sqlite
        assert br2.submit(sig, b.tick("EURUSD"), T + dt.timedelta(seconds=10)).status == DUPLICATE
        assert len(b.sends) == 1

    def test_a_stop_the_broker_does_not_hold_is_closed_at_once(self, tmp_path):
        br, b, j = live_bridge(tmp_path)
        b.sim.fault.drop_stops_on_send = True
        b.sim.fault.fail_modify = True
        rep = br.submit(make_signal(broker=b), b.tick("EURUSD"), T)
        assert not rep.ok and "closed at once" in rep.message
        assert b.positions(MAGIC) == []

    def test_a_rejection_is_recorded_and_not_retried(self, tmp_path):
        from mintel.broker.base import RetCode
        br, b, j = live_bridge(tmp_path)
        b.sim.fault.reject_next = RetCode.REJECT
        sig = make_signal(broker=b)
        assert br.submit(sig, b.tick("EURUSD"), T).status == "REJECTED"
        assert br.submit(sig, b.tick("EURUSD"), T).status == DUPLICATE
        assert len(b.sends) == 1


class TestTheChecks:
    def test_a_stale_quote_is_refused(self, tmp_path):
        br, b, j = live_bridge(tmp_path)
        b.quote_age_s = 8.0
        rep = br.submit(make_signal(broker=b), b.tick("EURUSD"), T)
        assert rep.status == REFUSED and "old" in rep.message and b.sends == []

    def test_a_wide_spread_is_refused(self, tmp_path):
        br, b, j = live_bridge(tmp_path)
        sig = make_signal(broker=b)
        b.sim.fault.spread_multiplier = 20.0
        rep = br.submit(sig, b.tick("EURUSD"), T)
        assert rep.status == REFUSED and "spread" in rep.message and b.sends == []
        # a refusal claims nothing: the same signal can go once the spread is back
        b.sim.fault.spread_multiplier = 1.0
        assert br.submit(sig, b.tick("EURUSD"), T).status == FILLED_S

    def test_the_spot_must_confirm(self, tmp_path):
        br, b, j = live_bridge(tmp_path)
        sig = make_signal(broker=b)
        now = b.tick("EURUSD")
        pip = b.spec("EURUSD").pip_size
        higher = Tick("EURUSD", T - dt.timedelta(seconds=5), now.bid + 3 * pip, now.ask + 3 * pip)
        rep = br.submit(sig, now, T, spot_history=[higher, now])           # spot fell 3 pips under a BUY
        assert rep.status == REFUSED and "does not confirm" in rep.message
        lower = Tick("EURUSD", T - dt.timedelta(seconds=5), now.bid - 8 * pip, now.ask - 8 * pip)
        rep = br.submit(sig, now, T, spot_history=[lower, now])            # already ran 8 of a 10-pip stop
        assert rep.status == REFUSED and "not chasing" in rep.message
        assert br.submit(sig, now, T, spot_history=[now]).ok

    def test_a_stop_on_the_wrong_side_or_too_close_is_refused(self, tmp_path):
        br, b, j = live_bridge(tmp_path)
        sig = make_signal(broker=b)
        sig.intended_stop = sig.entry_ref + 0.0005
        assert "losing side" in br.submit(sig, b.tick("EURUSD"), T).message
        sig2 = make_signal(broker=b, stop_pips=0.5)
        assert "minimum distance" in br.submit(sig2, b.tick("EURUSD"), T).message

    def test_off_sends_nothing_and_trading_disabled_is_refused(self, tmp_path):
        j = Journal(tmp_path / "ian.sqlite")
        b = FakeBroker()
        off = Mt5Bridge(make_executor("OFF", b, MAGIC, TAG), j, b)
        assert off.submit(make_signal(broker=b), b.tick("EURUSD"), T).status == REFUSED
        br, b2, _ = live_bridge(tmp_path / "x")
        b2.sim.fault.trade_disabled = True
        assert "does not allow trading" in br.submit(make_signal(broker=b2), b2.tick("EURUSD"), T).message

    def test_size_is_money_at_the_stop_capped(self, tmp_path):
        br, b, j = live_bridge(tmp_path, cfg=BridgeConfig(risk_money=10.0, max_lots=0.05))
        spec = b.spec("EURUSD")
        vol, risk, why = br.size(spec, 10 * spec.pip_size)
        assert vol == pytest.approx(0.05) and risk <= 10.0 + 1e-9         # capped by max_lots
        br2, _, _ = live_bridge(tmp_path / "y", broker=b, cfg=BridgeConfig(risk_money=10.0, max_lots=5.0))
        vol2, risk2, _ = br2.size(spec, 10 * spec.pip_size)
        assert risk2 == pytest.approx(10.0, rel=0.1)
        br3, _, _ = live_bridge(tmp_path / "z", broker=b, cfg=BridgeConfig(risk_money=0.5, max_risk_money=1.0))
        assert br3.size(spec, 100 * spec.pip_size)[0] == 0.0                # the smallest lot would risk too much


class TestPaper:
    def test_paper_fills_and_is_just_as_idempotent(self, tmp_path):
        b = FakeBroker()
        j = Journal(tmp_path / "ian.sqlite")
        br = Mt5Bridge(make_executor("PAPER", None, MAGIC, TAG, first_ticket=900_000_001), j, b)
        sig = make_signal(broker=b)
        rep = br.submit(sig, b.tick("EURUSD"), T)
        assert rep.ok and rep.ticket == 900_000_001 and rep.mode == "PAPER"
        assert rep.latency["broker_accept"] is None and "PAPER" in rep.latency["note"]
        assert br.submit(sig, b.tick("EURUSD"), T).status == DUPLICATE
        assert b.sends == []                          # PAPER never touches the broker
