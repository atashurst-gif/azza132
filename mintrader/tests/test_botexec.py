"""The shared executor behind the Runner, the Band Breaker and the Crowd Fader."""
import datetime as dt

import pytest

from mintel.botexec import MAGIC_BANDBREAKER, LiveExecutor, PaperExecutor, make_executor
from mintel.broker.base import OrderRequest, Side, Tick
from mintel.broker.sim import SimBroker
from mintel.config import Config

T0 = dt.datetime(2026, 10, 7, 9, 0, tzinfo=dt.timezone.utc)


def sim():
    b = SimBroker(["EURUSD"], start=T0 - dt.timedelta(days=1), history_bars=300)
    b.connect()
    return b


class TestPaper:
    def test_fills_at_the_touch_plus_slippage_and_closes_at_the_level(self):
        b = sim(); spec = b.spec("EURUSD"); t = b.tick("EURUSD")
        x = PaperExecutor(slippage_points=1.0, first_ticket=5000, tag="BB")
        f = x.open("EURUSD", Side.BUY, 0.1, t.bid - 0.0010, t.bid + 0.0020, t, spec, T0)
        assert f.ok and f.ticket == 5000 and f.price == pytest.approx(t.ask + spec.point)
        assert x.positions()[0].sl == pytest.approx(t.bid - 0.0010) and x.positions()[0].tp == pytest.approx(t.bid + 0.0020)
        low = Tick("EURUSD", T0, t.bid - 0.0011, t.ask - 0.0011)
        assert x.stop_hit(5000, low) and not x.target_hit(5000, low)
        high = Tick("EURUSD", T0, t.bid + 0.0021, t.ask + 0.0021)
        assert x.target_hit(5000, high) and not x.stop_hit(5000, high)
        c = x.close(5000, low, spec, at_price=t.bid - 0.0010)
        assert c.ok and c.price == pytest.approx(t.bid - 0.0010) and not x.positions()
        assert not x.close(5000, low, spec).ok

    def test_tickets_never_repeat_after_a_restart(self):
        x = PaperExecutor(first_ticket=100)
        x.reserve(250)
        b = sim(); t = b.tick("EURUSD")
        assert x.open("EURUSD", Side.SELL, 0.1, t.ask + 0.001, 0.0, t, b.spec("EURUSD"), T0).ticket == 251


class TestLive:
    def test_orders_carry_the_bots_magic_and_the_stop_sits_at_the_broker(self):
        b = sim(); spec = b.spec("EURUSD"); t = b.tick("EURUSD")
        x = LiveExecutor(b, MAGIC_BANDBREAKER, "BB")
        f = x.open("EURUSD", Side.BUY, 0.1, t.bid - 0.0010, 0.0, t, spec, T0)
        assert f.ok and f.ticket
        mine = x.positions()
        assert len(mine) == 1 and mine[0].magic == MAGIC_BANDBREAKER and mine[0].sl == pytest.approx(t.bid - 0.0010)
        assert not [p for p in b.positions(Config().magic) if p.ticket == f.ticket]      # Trend & Breakout never sees it
        assert x.modify_stop(f.ticket, t.bid - 0.0005)
        assert x.positions()[0].sl == pytest.approx(t.bid - 0.0005)
        c = x.close(f.ticket, b.tick("EURUSD"), spec)
        assert c.ok and not x.positions()
        d = x.closed_deal(f.ticket, tries=1)
        assert d is not None and "pnl" in d and d.get("exit_price")

    def test_a_refused_order_is_a_clean_no(self):
        class Refuses:
            def send(self, req): raise RuntimeError("market closed")
            def positions(self, magic=None): return []
        b = sim(); spec = b.spec("EURUSD"); t = b.tick("EURUSD")
        x = LiveExecutor(Refuses(), 1, "X")
        f = x.open("EURUSD", Side.BUY, 0.1, t.bid - 0.001, 0.0, t, spec, T0)
        assert not f.ok and "send failed" in f.message and not x.stop_hit(1, t)


class TestFactory:
    def test_off_sends_nothing_and_live_needs_a_broker(self):
        assert make_executor("OFF") is None
        assert make_executor("paper").mode == "PAPER"
        with pytest.raises(ValueError):
            make_executor("LIVE")
        assert make_executor("LIVE", broker=sim(), magic=5, tag="T").mode == "LIVE"
