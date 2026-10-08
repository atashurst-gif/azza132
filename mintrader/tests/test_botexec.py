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
        f = x.open("EURUSD", Side.BUY, 0.1, t.bid - 0.0100, 0.0, t, spec, T0)
        assert f.ok and f.ticket
        mine = x.positions()
        assert len(mine) == 1 and mine[0].magic == MAGIC_BANDBREAKER and mine[0].sl == pytest.approx(t.bid - 0.0100)
        assert not [p for p in b.positions(Config().magic) if p.ticket == f.ticket]      # Trend & Breakout never sees it
        assert x.modify_stop(f.ticket, t.bid - 0.0080)
        assert x.positions()[0].sl == pytest.approx(t.bid - 0.0080)
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


class TestTheWholeRoundTrip:
    def test_the_entry_commission_is_added_to_the_brokers_figure(self):
        class B:
            def closed_deal(self, ticket):
                return {"pnl": 10.0, "exit_price": 1.2, "volume": 0.1, "time": T0}
            def deals_since(self, since, magic=0, closing_only=True):
                assert magic == 7 and closing_only is False
                return [{"position": 5, "is_entry": True, "commission": -0.35},
                        {"position": 5, "is_entry": False, "commission": -0.35},
                        {"position": 6, "is_entry": True, "commission": -9.0}]
            def positions(self, magic=None): return []
        d = LiveExecutor(B(), 7, "T").closed_deal(5, tries=1)
        assert d["pnl"] == pytest.approx(9.65) and d["entry_commission"] == pytest.approx(-0.35)

    def test_a_history_that_cannot_be_read_leaves_the_figure_and_says_so(self):
        class B:
            def closed_deal(self, ticket): return {"pnl": 10.0, "time": T0}
            def deals_since(self, *a, **k): raise RuntimeError("history unavailable")
        d = LiveExecutor(B(), 7, "T").closed_deal(5, tries=1)
        assert d["pnl"] == 10.0 and "history unavailable" in d["entry_commission_unknown"]

    def test_a_failed_read_back_after_a_fill_still_reports_the_fill(self):
        b = sim(); spec = b.spec("EURUSD"); t = b.tick("EURUSD")
        x = LiveExecutor(b, 9, "T")
        real_positions = b.positions
        calls = {"n": 0}
        def flaky(magic=None):
            calls["n"] += 1
            raise RuntimeError("bridge dropped")
        b.positions = flaky
        f = x.open("EURUSD", Side.BUY, 0.1, t.bid - 0.0100, 0.0, t, spec, T0)
        b.positions = real_positions
        assert f.ok and f.ticket and "not yet confirmed" in f.message
        assert [p for p in b.positions(9) if p.ticket == f.ticket]

