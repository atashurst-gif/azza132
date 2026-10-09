"""PAPER broker for Trend & Breakout (mintel/broker/paper.py).

The wrapper must let the same trader code decide on REAL prices while every
order it sends is simulated: fills at the real bid/ask, stops and targets
held as a broker would hold them, commission and profit in pounds, deal rows
in exactly the MT5 adapter's shape, and nothing - ever - reaching the real
broker's order methods (unless a market is deliberately left LIVE).
"""
from __future__ import annotations

import datetime as dt
import random
import threading

import pytest

from mintel.broker.base import (AccountInfo, Bar, OrderRequest, OrderResult, Position, RetCode, Side, TF, Tick)
from mintel.broker.paper import PAPER_FIRST_TICKET, PaperBroker, market_kind, real_broker
from mintel.broker.sim import SimBroker
from mintel.clock import ServerClock
from mintel.contracts import SymbolSpec

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 10, 7, 13, 0, tzinfo=UTC)        # a Wednesday, markets open
TNB = 990_311
RUNNER = 990_511
ADAPTER_DEAL_KEYS = {"position", "magic", "symbol", "volume", "profit", "commission", "is_entry", "time"}
ADAPTER_CLOSED_KEYS = {"exit_price", "pnl", "volume", "symbol", "position", "time", "reason"}


def _spec(name, digits, point, tick_value, contract, *, group, profit_ccy, stops=0.0,
          vmin=0.01, vmax=50.0, vstep=0.01, trade_allowed=True, freeze=0.0) -> SymbolSpec:
    return SymbolSpec(name=name, digits=digits, point=point, tick_size=point, tick_value=tick_value,
                      contract_size=contract, volume_min=vmin, volume_max=vmax, volume_step=vstep,
                      stops_level_points=stops, freeze_level_points=freeze, base_currency=name[:3],
                      profit_currency=profit_ccy, asset_group=group, trade_allowed=trade_allowed)


class FakeInner:
    """A REAL broker with scripted ticks. Its order methods raise unless a
    test deliberately allows them (live groups)."""

    def __init__(self, now: dt.datetime = T0):
        self.now = now
        self.clock = ServerClock(0)
        self.timeout = 30.0
        self.specs = {
            # GBP account; USD->GBP 0.80 is built into every tick value below
            "EURUSD": _spec("EURUSD", 5, 0.00001, 0.80, 100_000, group="FX_MAJOR", profit_ccy="USD", stops=20),
            "USDJPY": _spec("USDJPY", 3, 0.001, 0.50, 100_000, group="FX_MAJOR", profit_ccy="JPY"),
            "XAUUSD": _spec("XAUUSD", 2, 0.01, 0.80, 100, group="GOLD", profit_ccy="USD"),
            "US500": _spec("US500", 2, 0.01, 0.008, 1, group="INDEX", profit_ccy="USD", vmin=0.1, vstep=0.1),
            "GBPUSD": _spec("GBPUSD", 5, 0.00001, 0.80, 100_000, group="FX_MAJOR", profit_ccy="USD"),
        }
        self.ticks: dict[str, Tick] = {}
        self.history: dict[str, list[Tick]] = {}
        self.minute_bars: dict[str, list[Bar]] = {}
        self.range_calls: list[tuple[str, dt.datetime, dt.datetime]] = []
        self.bar_calls: list[tuple] = []
        self.real_positions: list[Position] = []
        self.real_deals: list[dict] = []
        self.allow_orders = False
        self.order_calls: list[tuple] = []
        self.next_real = 5_000_001
        self.set_tick("GBPUSD", 1.24995, 1.25005)

    # ---- scripting
    def set_tick(self, symbol, bid, ask, seconds: float = 1.0) -> Tick:
        self.now = self.now + dt.timedelta(seconds=seconds)
        t = Tick(symbol, self.now, bid, ask)
        self.ticks[symbol] = t
        self.history.setdefault(symbol, []).append(t)
        return t

    # ---- read-only surface
    def connect(self): return True
    def shutdown(self): pass
    def is_connected(self): return True
    def server_time(self): return self.now.replace(tzinfo=None)
    def symbols(self): return tuple(self.specs)
    def spec(self, s): return self.specs.get(s)
    def ensure_selected(self, s): return s in self.specs
    def tick(self, s): return self.ticks.get(s)
    def bars(self, s, tf, count, end=None):
        self.bar_calls.append((s, tf, count, end))
        rows = [b for b in self.minute_bars.get(s, []) if tf is TF.M1 and (end is None or b.time <= end)]
        return rows[-count:]
    def calendar(self, start, end, currencies=()): return []
    def calc_margin(self, *a): return 100.0
    def clock_skew(self): return {"skew_seconds": 0.0, "tick_age_seconds": 0.0}

    def ticks_range(self, s, start, end):
        self.range_calls.append((s, start, end))
        return [t for t in self.history.get(s, []) if start <= t.time <= end]

    def account(self):
        return AccountInfo(login=1, server="ICMarketsSC-Demo", currency="GBP", balance=2000.0, equity=2000.0,
                           margin=0.0, margin_free=2000.0, margin_level=0.0, leverage=30, trade_allowed=True,
                           is_demo=True)

    def positions(self, magic=None):
        return [p for p in self.real_positions if magic is None or p.magic == magic]

    def deals_since(self, since, magic=0, closing_only=True):
        return [dict(r) for r in self.real_deals if r["time"] >= since and (not magic or r["magic"] == magic)
                and not (closing_only and r["is_entry"])]

    def closed_deal(self, ticket):
        rows = [r for r in self.real_deals if r["position"] == ticket and not r["is_entry"]]
        if not rows:
            return None
        return {"exit_price": 1.0, "pnl": sum(r["profit"] for r in rows), "volume": rows[-1]["volume"],
                "symbol": rows[-1]["symbol"], "position": ticket, "time": rows[-1]["time"], "reason": "real"}

    # ---- order methods: a PAPER leak if ever reached without permission
    def _order(self, name, *args):
        self.order_calls.append((name,) + args)
        if not self.allow_orders:
            raise AssertionError(f"PAPER leak: inner.{name} was called")

    def send(self, req: OrderRequest) -> OrderResult:
        self._order("send", req)
        t = self.ticks[req.symbol]
        price = t.ask if req.side is Side.BUY else t.bid
        ticket = self.next_real
        self.next_real += 1
        self.real_positions.append(Position(ticket, req.symbol, req.side, req.volume, price, req.sl, req.tp,
                                            self.now, comment=req.comment, magic=req.magic or TNB))
        return OrderResult(True, RetCode.DONE.value, ticket=ticket, deal=ticket, filled_volume=req.volume,
                           price=price, requested_volume=req.volume, idempotency_key=req.idempotency_key)

    def modify_stops(self, ticket, sl, tp):
        self._order("modify_stops", ticket, sl, tp)
        for p in self.real_positions:
            if p.ticket == ticket:
                p.sl, p.tp = sl, tp
                return OrderResult(True, RetCode.DONE.value, ticket=ticket)
        return OrderResult(False, RetCode.REJECT.value, comment="position not found")

    def close(self, ticket, volume=0.0, comment=""):
        self._order("close", ticket, volume, comment)
        self.real_positions = [p for p in self.real_positions if p.ticket != ticket]
        return OrderResult(True, RetCode.DONE.value, ticket=ticket)


def req(symbol, side, volume, sl=0.0, tp=0.0, magic=TNB, key="k1") -> OrderRequest:
    return OrderRequest(symbol=symbol, side=side, volume=volume, sl=sl, tp=tp, comment=key, magic=magic,
                        idempotency_key=key)


@pytest.fixture
def inner():
    return FakeInner()


def make(inner, path, **kw) -> PaperBroker:
    """A wrapper on the fake broker's clock. Slippage 0 unless asked, so the
    fill tests can name exact prices; the default (1 point) and slippage on
    stops have tests of their own."""
    kw.setdefault("slippage_points", 0.0)
    kw.setdefault("now_fn", lambda: inner.now)
    return PaperBroker(inner, path, **kw)


@pytest.fixture
def paper(inner, tmp_path):
    pb = make(inner, tmp_path)
    yield pb
    pb.close_db()


def _open_buy(inner, paper, symbol="EURUSD", bid=1.10000, ask=1.10010, volume=0.5, sl=1.09500, tp=0.0):
    inner.set_tick(symbol, bid, ask)
    res = paper.send(req(symbol, Side.BUY, volume, sl=sl, tp=tp, key=f"k-{symbol}-{random.random():.6f}"))
    assert res.ok, res.comment
    return res


# ---------------------------------------------------------------- fills --
class TestFills:
    def test_buy_fills_at_the_real_ask_and_sell_at_the_real_bid(self, inner, paper):
        inner.set_tick("EURUSD", 1.10000, 1.10010)
        buy = paper.send(req("EURUSD", Side.BUY, 0.5, sl=1.09500, key="b"))
        sell = paper.send(req("EURUSD", Side.SELL, 0.5, sl=1.10500, key="s"))
        assert buy.ok and sell.ok
        assert buy.price == pytest.approx(1.10010) and sell.price == pytest.approx(1.10000)
        assert buy.retcode == RetCode.DONE.value and buy.filled_volume == 0.5
        assert buy.ticket >= PAPER_FIRST_TICKET and sell.ticket > buy.ticket
        assert {p.ticket for p in paper.positions(TNB)} == {buy.ticket, sell.ticket}
        assert inner.order_calls == []

    def test_slippage_is_always_against_the_trade(self, inner, tmp_path):
        pb = make(inner, tmp_path, slippage_points=2)
        inner.set_tick("EURUSD", 1.10000, 1.10010)
        buy = pb.send(req("EURUSD", Side.BUY, 0.1, sl=1.09500, key="b"))
        sell = pb.send(req("EURUSD", Side.SELL, 0.1, sl=1.10500, key="s"))
        assert buy.price == pytest.approx(1.10012) and sell.price == pytest.approx(1.09998)
        out = pb.close(buy.ticket)
        assert out.ok and out.price == pytest.approx(1.09998)        # the bid, less 2 points
        pb.close_db()

    def test_refused_with_no_tick_in_the_adapters_shape(self, inner, paper):
        res = paper.send(req("XAUUSD", Side.BUY, 0.1, sl=2300.0))
        assert not res.ok and res.retcode == RetCode.PRICE_OFF.value and res.comment == "no tick"
        assert res.idempotency_key == "k1" and res.ticket == 0
        assert paper.positions(TNB) == []

    def test_refused_with_a_stale_tick(self, inner, paper):
        inner.set_tick("EURUSD", 1.1, 1.1001)
        inner.now += dt.timedelta(minutes=10)                     # Wednesday afternoon: just stale
        res = paper.send(req("EURUSD", Side.BUY, 0.1, sl=1.09))
        assert not res.ok and res.retcode == RetCode.PRICE_OFF.value and "PAPER" in res.comment

    def test_refused_when_the_market_is_closed(self, inner, paper):
        inner.set_tick("EURUSD", 1.1, 1.1001)
        inner.now = dt.datetime(2026, 10, 10, 12, 0, tzinfo=UTC)  # Saturday: Friday's last price
        res = paper.send(req("EURUSD", Side.BUY, 0.1, sl=1.09))
        assert not res.ok and res.retcode == RetCode.MARKET_CLOSED.value

    def test_refused_when_the_broker_says_no_new_orders(self, inner, paper):
        inner.specs["XAUUSD"] = _spec("XAUUSD", 2, 0.01, 0.8, 100, group="GOLD", profit_ccy="USD",
                                      trade_allowed=False)
        inner.set_tick("XAUUSD", 2400.0, 2400.3)
        res = paper.send(req("XAUUSD", Side.BUY, 0.1, sl=2390.0))
        assert not res.ok and res.retcode == RetCode.MARKET_CLOSED.value

    @pytest.mark.parametrize("volume", [0.0, 0.005, 51.0, 0.015])
    def test_volume_off_the_spec_is_refused(self, inner, paper, volume):
        inner.set_tick("EURUSD", 1.1, 1.1001)
        res = paper.send(req("EURUSD", Side.BUY, volume, sl=1.09))
        assert not res.ok and res.retcode == RetCode.INVALID_VOLUME.value
        if volume < 0.01:
            assert res.comment == "volume below minimum"           # exactly the adapter's words

    def test_stops_on_the_wrong_side_or_inside_the_stops_level_are_refused(self, inner, paper):
        inner.set_tick("EURUSD", 1.10000, 1.10010)
        for sl, tp in ((1.10005, 0.0),        # buy stop above the bid
                       (1.09990, 0.0),        # 10 points away, stops level is 20
                       (1.09000, 1.09990)):   # target below the bid
            res = paper.send(req("EURUSD", Side.BUY, 0.1, sl=sl, tp=tp))
            assert not res.ok and res.retcode == RetCode.INVALID_STOPS.value, (sl, tp)
        assert paper.positions(TNB) == []

    def test_another_bots_order_is_refused_and_never_sent(self, inner, paper):
        inner.set_tick("US500", 5000.0, 5000.5)
        res = paper.send(req("US500", Side.BUY, 1.0, sl=4990.0, magic=RUNNER))
        assert not res.ok and res.retcode == RetCode.REJECT.value
        assert "real broker" in res.comment and inner.order_calls == []

    def test_paper_comment_carries_the_idempotency_key(self, inner, paper):
        inner.set_tick("EURUSD", 1.1, 1.1001)
        key = "a" * 24
        res = paper.send(req("EURUSD", Side.BUY, 0.1, sl=1.09, key=key))
        (p,) = paper.positions(TNB)
        assert p.ticket == res.ticket and key in p.comment and p.comment.startswith("PAPER")
        assert p.magic == TNB and p.open_time == inner.now


# ------------------------------------------------------ stops and targets --
class TestStopsAndTargets:
    def test_buy_stop_fills_at_the_stop(self, inner, paper):
        res = _open_buy(inner, paper, sl=1.09500)
        inner.set_tick("EURUSD", 1.09500, 1.09510)
        assert paper.positions(TNB) == []
        deal = paper.closed_deal(res.ticket)
        assert deal["exit_price"] == pytest.approx(1.09500) and deal["reason"].startswith("[sl")
        assert "PAPER" in deal["reason"]

    def test_gap_through_the_stop_fills_at_the_worse_price(self, inner, paper):
        res = _open_buy(inner, paper, sl=1.09500)
        inner.set_tick("EURUSD", 1.09400, 1.09410)                # gapped 10 pips through
        paper.tick("EURUSD")
        assert paper.closed_deal(res.ticket)["exit_price"] == pytest.approx(1.09400)

    def test_sell_stop_uses_the_ask(self, inner, paper):
        inner.set_tick("EURUSD", 1.10000, 1.10010)
        res = paper.send(req("EURUSD", Side.SELL, 0.2, sl=1.10300))
        inner.set_tick("EURUSD", 1.10295, 1.10300)                # bid below, ask AT the stop
        assert paper.positions(TNB) == []
        assert paper.closed_deal(res.ticket)["exit_price"] == pytest.approx(1.10300)
        res2 = paper.send(req("EURUSD", Side.SELL, 0.2, sl=1.10600, key="2"))
        inner.set_tick("EURUSD", 1.10700, 1.10710)                # gapped through
        paper.tick("EURUSD")
        assert paper.closed_deal(res2.ticket)["exit_price"] == pytest.approx(1.10710)

    def test_target_fills_only_when_traded_through_and_never_better(self, inner, paper):
        res = _open_buy(inner, paper, sl=1.09500, tp=1.10500)
        inner.set_tick("EURUSD", 1.10500, 1.10510)                # touched, not through
        assert [p.ticket for p in paper.positions(TNB)] == [res.ticket]
        inner.set_tick("EURUSD", 1.10800, 1.10810)                # through, by a lot
        assert paper.positions(TNB) == []
        deal = paper.closed_deal(res.ticket)
        assert deal["exit_price"] == pytest.approx(1.10500) and deal["reason"].startswith("[tp")

    def test_a_spike_between_two_reads_is_not_missed(self, inner, paper):
        res = _open_buy(inner, paper, sl=1.09500)
        inner.set_tick("EURUSD", 1.09700, 1.09710)
        inner.set_tick("EURUSD", 1.09480, 1.09490)                # through the stop ...
        inner.set_tick("EURUSD", 1.09900, 1.09910)                # ... and back, before anyone looked
        assert paper.positions(TNB) == []
        deal = paper.closed_deal(res.ticket)
        assert deal["exit_price"] == pytest.approx(1.09480)
        assert deal["time"] == inner.history["EURUSD"][-2].time

    def test_without_replay_only_the_current_tick_counts(self, inner, tmp_path):
        pb = make(inner, tmp_path, replay_ticks=False)
        res = _open_buy(inner, pb, sl=1.09500)
        inner.set_tick("EURUSD", 1.09480, 1.09490)
        inner.set_tick("EURUSD", 1.09900, 1.09910)
        assert [p.ticket for p in pb.positions(TNB)] == [res.ticket]
        pb.close_db()

    def test_no_look_ahead(self, inner, paper):
        res = _open_buy(inner, paper, sl=1.09500)
        inner.set_tick("EURUSD", 1.09900, 1.09910)
        future = Tick("EURUSD", inner.now + dt.timedelta(seconds=30), 1.09000, 1.09010)
        inner.history["EURUSD"].append(future)                    # printed AFTER the current tick
        inner.ticks_range = lambda s, a, b: list(inner.history.get(s, []))   # a broker that hands it over anyway
        assert [p.ticket for p in paper.positions(TNB)] == [res.ticket]

    def test_reading_a_tick_through_the_wrapper_fills_the_stop(self, inner, paper):
        res = _open_buy(inner, paper, sl=1.09500)
        inner.set_tick("EURUSD", 1.09400, 1.09410)
        assert paper.closed == []                                 # nothing has looked at the price yet
        t = paper.tick("EURUSD")
        assert t.bid == 1.09400                                   # the real tick, unchanged
        assert [c["ticket"] for c in paper.closed] == [res.ticket]
        assert paper.closed[0]["exit"] == pytest.approx(1.09400)


def _boom(*_a, **_k):
    raise TimeoutError("bridge timed out")


# ------------------------------------------- every printed price is judged --
class TestReplay:
    """Every price printed between two reads is judged once, oldest first,
    against the stop and size in force when it printed, whatever the speed
    the trader reads at (its manage loop reads about once a second)."""

    def test_a_spike_between_reads_under_a_second_apart_is_not_missed(self, inner, paper):
        res = _open_buy(inner, paper, sl=1.09500)
        spike = None
        # ticks 0.3 s apart; the trader reads every third one (0.9 s apart)
        for i, bid in enumerate((1.09800, 1.09750, 1.09700, 1.09480, 1.09700, 1.09900,
                                 1.09900, 1.09950, 1.09900, 1.09920, 1.09910, 1.09900)):
            t = inner.set_tick("EURUSD", bid, bid + 0.0001, seconds=0.3)
            spike = t if bid == 1.09480 else spike
            if i % 3 == 2:
                paper.positions(TNB)
                paper.tick("EURUSD")
        assert paper.positions(TNB) == []
        deal = paper.closed_deal(res.ticket)
        assert deal["exit_price"] == pytest.approx(1.09480) and deal["time"] == spike.time

    def test_a_failed_history_read_is_read_again_not_skipped(self, inner, paper):
        res = _open_buy(inner, paper, sl=1.09500)
        spike = inner.set_tick("EURUSD", 1.09480, 1.09490, seconds=2)   # through the stop, unseen
        inner.set_tick("EURUSD", 1.09900, 1.09910, seconds=2)
        inner.ticks_range = _boom                                        # the bridge times out once
        assert [p.ticket for p in paper.positions(TNB)] == [res.ticket]  # cannot know yet
        del inner.ticks_range
        inner.set_tick("EURUSD", 1.09900, 1.09910, seconds=6)            # after the retry wait
        assert paper.positions(TNB) == []
        deal = paper.closed_deal(res.ticket)
        assert deal["exit_price"] == pytest.approx(1.09480) and deal["time"] == spike.time

    def test_a_stop_change_judges_earlier_prices_on_the_old_stop(self, inner, paper):
        res = _open_buy(inner, paper, sl=1.09500)
        inner.set_tick("EURUSD", 1.09550, 1.09560, seconds=0.3)          # above the OLD stop, never read
        inner.set_tick("EURUSD", 1.10100, 1.10110, seconds=0.3)
        assert paper.modify_stops(res.ticket, 1.09600, 0.0).ok            # valid against 1.10100
        for _ in range(4):
            inner.set_tick("EURUSD", 1.10100, 1.10110, seconds=0.9)
            paper.positions(TNB)
        # 1.09550 printed before the change: it is never judged against 1.09600
        assert [p.ticket for p in paper.positions(TNB)] == [res.ticket] and paper.closed == []

    def test_a_change_is_refused_while_the_history_cannot_be_read(self, inner, paper):
        res = _open_buy(inner, paper, sl=1.09500)
        inner.set_tick("EURUSD", 1.09550, 1.09560, seconds=0.5)
        inner.set_tick("EURUSD", 1.10100, 1.10110, seconds=0.5)
        inner.ticks_range = _boom
        for out in (paper.modify_stops(res.ticket, 1.09600, 0.0), paper.close(res.ticket)):
            assert not out.ok and out.retcode == RetCode.CONNECTION.value
            assert "nothing was changed" in out.comment and out.ticket == res.ticket
        (p,) = paper.positions(TNB)
        assert p.sl == pytest.approx(1.09500) and p.volume == pytest.approx(0.5)
        del inner.ticks_range
        assert paper.modify_stops(res.ticket, 1.09600, 0.0).ok            # read at once: no waiting
        assert inner.order_calls == []

    def test_a_history_that_stays_unreadable_is_given_up_with_a_warning(self, inner, paper, caplog):
        res = _open_buy(inner, paper, sl=1.09500)
        inner.ticks_range = _boom
        inner.set_tick("EURUSD", 1.10100, 1.10110, seconds=2)
        assert not paper.modify_stops(res.ticket, 1.09600, 0.0).ok
        inner.set_tick("EURUSD", 1.10100, 1.10110, seconds=901)          # 15 minutes of failures
        with caplog.at_level("WARNING", logger="mintel.paper"):
            assert paper.modify_stops(res.ticket, 1.09600, 0.0).ok        # the book can still be managed
        assert "NOT checked for stops" in caplog.text

    def test_a_partial_close_after_an_unseen_stop_finds_the_stop_first(self, inner, paper):
        res = _open_buy(inner, paper, volume=0.5, sl=1.09500)
        inner.set_tick("EURUSD", 1.09480, 1.09490, seconds=0.3)          # stopped, unseen
        inner.set_tick("EURUSD", 1.10200, 1.10210, seconds=0.3)
        out = paper.close(res.ticket, 0.2, "PARTIAL")
        assert not out.ok and "position not found" in out.comment       # as the real broker would say
        deal = paper.closed_deal(res.ticket)
        assert deal["volume"] == pytest.approx(0.5) and deal["exit_price"] == pytest.approx(1.09480)

    def test_a_fill_on_the_current_tick_looks_back_first(self, inner, paper):
        res = _open_buy(inner, paper, sl=1.09500)
        gap = inner.set_tick("EURUSD", 1.09400, 1.09410, seconds=0.3)    # gapped through, unseen
        inner.set_tick("EURUSD", 1.09450, 1.09460, seconds=0.3)          # still through when read
        paper.tick("EURUSD")
        deal = paper.closed_deal(res.ticket)
        assert deal["exit_price"] == pytest.approx(1.09400) and deal["time"] == gap.time

    def test_a_late_tick_never_fills_a_newer_stop(self, inner, paper):
        """Two threads: one read a tick, another then tightened the stop; the
        first one's tick arrives last. It printed before the stop existed."""
        res = _open_buy(inner, paper, sl=1.09500)
        late = inner.set_tick("EURUSD", 1.09950, 1.09960)
        inner.set_tick("EURUSD", 1.10100, 1.10110)
        assert paper.modify_stops(res.ticket, 1.09980, 0.0).ok            # valid against 1.10100
        inner.tick = lambda s: late                                      # the earlier read, judged now
        assert paper.tick("EURUSD") is late
        del inner.tick
        assert [p.ticket for p in paper.positions(TNB)] == [res.ticket] and paper.closed == []

    def test_a_stop_hit_hours_before_a_restart_is_found(self, inner, tmp_path):
        a = make(inner, tmp_path)
        res = _open_buy(inner, a, sl=1.09500)
        a.close_db()                                                     # the Mac sleeps
        hit = inner.set_tick("EURUSD", 1.09400, 1.09410, seconds=1800)   # +30 min: stopped
        inner.set_tick("EURUSD", 1.09900, 1.09910, seconds=3 * 3600 - 1800)   # +3 h: back above
        b = make(inner, tmp_path)
        assert b.positions(TNB) == []
        deal = b.closed_deal(res.ticket)
        assert deal["exit_price"] == pytest.approx(1.09400) and deal["time"] == hit.time
        b.close_db()

    def test_a_long_gap_is_read_in_pieces_end_to_end(self, inner, tmp_path):
        a = make(inner, tmp_path)
        res = _open_buy(inner, a, sl=1.09500)
        opened = inner.now
        a.close_db()
        for _ in range(10):                                              # 5 quiet hours
            inner.set_tick("EURUSD", 1.09900, 1.09910, seconds=1800)
        inner.range_calls.clear()
        b = make(inner, tmp_path)
        assert [p.ticket for p in b.positions(TNB)] == [res.ticket]
        spans = [(lo, hi) for s, lo, hi in inner.range_calls if s == "EURUSD"]
        assert len(spans) == 5 and spans[0][0] == opened and spans[-1][1] == inner.now
        assert all(hi - lo <= dt.timedelta(minutes=60) for lo, hi in spans)
        assert all(spans[i][1] == spans[i + 1][0] for i in range(len(spans) - 1))   # no hole
        b.close_db()

    def test_older_than_the_tick_limit_is_judged_on_minute_bars(self, inner, tmp_path):
        a = make(inner, tmp_path)
        buy = _open_buy(inner, a, sl=1.09500)
        sell = a.send(req("EURUSD", Side.SELL, 0.2, sl=1.10300, key="s"))
        gbp = _open_buy(inner, a, symbol="GBPUSD", bid=1.25000, ask=1.25010, volume=0.1, sl=1.24700)
        a.close_db()                                                     # off for four days
        m = inner.now.replace(second=0, microsecond=0) + dt.timedelta(minutes=1)
        h = dt.timedelta(hours=1)
        inner.minute_bars = {
            # bid bars; the 24 h bar reaches the buy's stop inside the minute
            "EURUSD": [Bar(m + 2 * h, 1.10000, 1.10100, 1.09800, 1.10000, spread_points=10),
                       Bar(m + 24 * h, 1.09800, 1.09850, 1.09450, 1.09700, spread_points=10),
                       # bid high 1.10290 + 20 points of spread = an ask of 1.10310: the sell's stop
                       Bar(m + 20 * h, 1.10200, 1.10290, 1.10150, 1.10250, spread_points=20)],
            # opens straight through the stop: filled at the open, not the stop
            "GBPUSD": [Bar(m + 26 * h, 1.24600, 1.24650, 1.24550, 1.24600, spread_points=10)],
        }
        # 100 h later: the last 72 h are on ticks (quiet), everything before on the bars
        inner.set_tick("EURUSD", 1.10000, 1.10010, seconds=100 * 3600)
        inner.set_tick("GBPUSD", 1.25000, 1.25010, seconds=1)
        b = make(inner, tmp_path)
        assert b.positions(TNB) == []
        for res, price, when in ((buy, 1.09500, m + 24 * h), (sell, 1.10300, m + 20 * h),
                                 (gbp, 1.24600, m + 26 * h)):
            deal = b.closed_deal(res.ticket)
            assert deal["exit_price"] == pytest.approx(price) and deal["time"] == when
            assert deal["reason"].startswith("[sl")
        assert all(tf is TF.M1 for _s, tf, _n, _e in inner.bar_calls)
        b.close_db()


# ----------------------------------------- slippage on a triggered stop --
class TestSlippageOnStops:
    def test_a_stop_fills_the_slippage_worse_and_a_target_exactly(self, inner, tmp_path):
        pb = make(inner, tmp_path, slippage_points=5)
        inner.set_tick("EURUSD", 1.10000, 1.10010)
        buy = pb.send(req("EURUSD", Side.BUY, 0.5, sl=1.09500, key="b"))
        sell = pb.send(req("EURUSD", Side.SELL, 0.5, sl=1.10500, key="s"))
        tgt = pb.send(req("EURUSD", Side.BUY, 0.5, sl=1.09000, tp=1.10300, key="t"))
        assert buy.price == pytest.approx(1.10015) and sell.price == pytest.approx(1.09995)
        inner.set_tick("EURUSD", 1.10310, 1.10320)                       # through the target
        inner.set_tick("EURUSD", 1.10520, 1.10530)                       # the sell's stop
        inner.set_tick("EURUSD", 1.09490, 1.09500)                       # the buy's stop
        assert pb.positions(TNB) == []
        assert pb.closed_deal(tgt.ticket)["exit_price"] == pytest.approx(1.10300)   # exactly the target
        assert pb.closed_deal(sell.ticket)["exit_price"] == pytest.approx(1.10535)  # the ask, 5 points worse
        deal = pb.closed_deal(buy.ticket)
        assert deal["exit_price"] == pytest.approx(1.09485)              # the bid, 5 points worse
        assert deal["reason"] == "[sl 1.09500] PAPER"                    # the level, as MT5 writes it
        pb.close_db()

    def test_the_default_is_one_point_on_entries_and_stops(self, inner, tmp_path):
        pb = PaperBroker(inner, tmp_path, now_fn=lambda: inner.now)
        assert pb.cfg.slippage_points == 1.0
        inner.set_tick("EURUSD", 1.10000, 1.10010)
        res = pb.send(req("EURUSD", Side.BUY, 0.5, sl=1.09500))
        inner.set_tick("EURUSD", 1.09500, 1.09510)
        pb.tick("EURUSD")
        assert res.price == pytest.approx(1.10011)
        assert pb.closed_deal(res.ticket)["exit_price"] == pytest.approx(1.09499)
        pb.close_db()


# --------------------------------------------------------- modify and close --
class TestModifyAndClose:
    def test_modify_accepts_valid_levels_and_refuses_invalid_ones_like_mt5(self, inner, paper):
        res = _open_buy(inner, paper, sl=1.09500, tp=1.11000)
        ok = paper.modify_stops(res.ticket, 1.09800, 1.11000)
        assert ok.ok and ok.retcode == RetCode.DONE.value
        assert paper.positions(TNB)[0].sl == pytest.approx(1.09800)
        wider = paper.modify_stops(res.ticket, 1.09000, 1.11000)  # the broker does not police widening
        assert wider.ok
        wrong = paper.modify_stops(res.ticket, 1.10050, 1.11000)  # above the bid
        assert not wrong.ok and wrong.retcode == RetCode.INVALID_STOPS.value and wrong.ticket == res.ticket
        same = paper.modify_stops(res.ticket, 1.09000, 1.11000)
        assert not same.ok and same.retcode == 10025               # MT5 "No changes"
        assert paper.positions(TNB)[0].sl == pytest.approx(1.09000)

    def test_a_zero_stop_keeps_the_stop_and_a_zero_target_removes_the_target(self, inner, paper):
        res = _open_buy(inner, paper, sl=1.09500, tp=1.11000)
        assert paper.modify_stops(res.ticket, 0.0, 0.0).ok
        p = paper.positions(TNB)[0]
        assert p.sl == pytest.approx(1.09500) and p.tp == 0.0

    def test_unknown_ticket_is_not_found_in_the_adapters_shape(self, inner, paper):
        out = paper.modify_stops(123, 1.0, 0.0)
        assert not out.ok and out.retcode == RetCode.REJECT.value and "position not found" in out.comment
        out = paper.close(123)
        assert not out.ok and out.retcode == RetCode.REJECT.value and "position not found" in out.comment

    def test_close_at_the_current_bid_and_ask(self, inner, paper):
        buy = _open_buy(inner, paper, sl=1.09500)
        sell = paper.send(req("EURUSD", Side.SELL, 0.5, sl=1.10500, key="s"))
        inner.set_tick("EURUSD", 1.10100, 1.10112)
        b = paper.close(buy.ticket, comment="thesis broken")
        s = paper.close(sell.ticket)
        assert b.ok and b.price == pytest.approx(1.10100) and b.filled_volume == 0.5
        assert s.ok and s.price == pytest.approx(1.10112)
        assert paper.closed_deal(buy.ticket)["reason"] == "PAPER thesis broken"
        again = paper.close(buy.ticket)
        assert not again.ok and again.retcode == RetCode.REJECT.value

    def test_partial_close_then_the_rest(self, inner, paper):
        res = _open_buy(inner, paper, volume=0.5, sl=1.09500)
        inner.set_tick("EURUSD", 1.10210, 1.10220)
        part = paper.close(res.ticket, 0.2, "PARTIAL")
        assert part.ok and part.filled_volume == pytest.approx(0.2)
        (p,) = paper.positions(TNB)
        assert p.volume == pytest.approx(0.3) and p.ticket == res.ticket
        inner.set_tick("EURUSD", 1.09400, 1.09410)                # the rest is stopped (gapped)
        assert paper.positions(TNB) == []
        deal = paper.closed_deal(res.ticket)
        assert deal["volume"] == pytest.approx(0.5)
        assert deal["exit_price"] == pytest.approx((1.10210 * 0.2 + 1.09400 * 0.3) / 0.5)


# ------------------------------------------------------------------ money --
class TestMoney:
    """GBP account, USD->GBP 0.80 (the spec's tick values and the GBPUSD tick
    both say so), IC Markets Raw commission USD 3.50 a lot a side."""

    def test_fx_pair(self, inner, paper):
        res = _open_buy(inner, paper, bid=1.10000, ask=1.10010, volume=0.5, sl=1.09500)
        inner.set_tick("EURUSD", 1.10210, 1.10220)
        paper.close(res.ticket)
        # 200 points x 0.80 GBP x 0.5 lots = 80.00; commission 3.50 x 0.8 x 0.5 = 1.40 a side
        rows = [r for r in paper.deals_since(T0, TNB, False) if r["position"] == res.ticket]
        entry, exit_ = rows
        assert entry["is_entry"] and entry["profit"] == pytest.approx(-1.40) and entry["commission"] == pytest.approx(-1.40)
        assert not exit_["is_entry"] and exit_["profit"] == pytest.approx(80.00 - 1.40)
        assert paper.closed_deal(res.ticket)["pnl"] == pytest.approx(78.60)   # closing deals only, like MT5
        assert paper.commission_per_side("EURUSD", 1.0) == pytest.approx(-2.80)

    def test_jpy_pair(self, inner, paper):
        inner.set_tick("USDJPY", 150.000, 150.010)
        res = paper.send(req("USDJPY", Side.SELL, 0.3, sl=151.000))
        inner.set_tick("USDJPY", 149.490, 149.500)
        paper.close(res.ticket)
        # 500 points x 0.50 GBP x 0.3 = 75.00; USD commission via the GBPUSD tick (1/1.25 = 0.80): 0.84 a side
        rows = [r for r in paper.deals_since(T0, TNB, False) if r["position"] == res.ticket]
        assert rows[0]["commission"] == pytest.approx(-0.84)
        assert rows[1]["profit"] == pytest.approx(75.00 - 0.84)

    def test_gold(self, inner, paper):
        inner.set_tick("XAUUSD", 2400.20, 2400.50)
        res = paper.send(req("XAUUSD", Side.BUY, 0.1, sl=2390.00))
        inner.set_tick("XAUUSD", 2390.00, 2390.30)                # the stop
        assert paper.positions(TNB) == []
        # -1050 points x 0.80 x 0.1 = -84.00; commission 0.28 a side
        deal = paper.closed_deal(res.ticket)
        assert deal["pnl"] == pytest.approx(-84.00 - 0.28)
        net = sum(r["profit"] for r in paper.deals_since(T0, TNB, False) if r["position"] == res.ticket)
        assert net == pytest.approx(-84.56)

    def test_index_has_no_commission(self, inner, paper):
        inner.set_tick("US500", 5000.00, 5000.50)
        res = paper.send(req("US500", Side.SELL, 1.0, sl=5020.00, tp=4990.00))
        inner.set_tick("US500", 4989.00, 4989.50)                 # traded through the target
        assert paper.positions(TNB) == []
        # +10.00 points = 1000 ticks x 0.008 GBP x 1.0 = 8.00, no commission on indices
        rows = [r for r in paper.deals_since(T0, TNB, False) if r["position"] == res.ticket]
        assert [r["commission"] for r in rows] == [0.0, 0.0]
        assert paper.closed_deal(res.ticket)["pnl"] == pytest.approx(8.00)

    def test_configured_rate_and_commission_override_the_broker(self, inner, tmp_path):
        pb = PaperBroker(inner, tmp_path, fx_rates={"USD": 0.70}, commission={"INDEX": (1.0, "GBP")},
                         now_fn=lambda: inner.now)
        assert pb.commission_per_side("EURUSD", 1.0) == pytest.approx(-2.45)
        assert pb.commission_per_side("US500", 2.0) == pytest.approx(-2.00)
        pb.close_db()

    def test_floating_profit_is_marked_on_the_closing_side(self, inner, paper):
        _open_buy(inner, paper, bid=1.10000, ask=1.10010, volume=1.0)
        inner.set_tick("EURUSD", 1.10060, 1.10070)
        (p,) = paper.positions(TNB)
        assert p.profit == pytest.approx(50 * 0.80) and p.swap == 0.0

    def test_market_kind_matches_the_trader(self):
        assert market_kind("US500") == "INDEX" and market_kind("XAUUSD") == "GOLD"
        assert market_kind("USDJPY").startswith("FX")


# ------------------------------------------------------- deals and journal --
class TestDealsAndJournal:
    def test_rows_have_exactly_the_adapters_shape(self, inner, paper):
        res = _open_buy(inner, paper)
        inner.set_tick("EURUSD", 1.1012, 1.1013)
        paper.close(res.ticket)
        rows = paper.deals_since(T0, TNB, False)
        assert rows and all(set(r) == ADAPTER_DEAL_KEYS for r in rows)
        assert all(isinstance(r["time"], dt.datetime) and r["time"].tzinfo for r in rows)
        assert [r["is_entry"] for r in paper.deals_since(T0, TNB)] == [False]   # closing only by default
        assert set(paper.closed_deal(res.ticket)) == ADAPTER_CLOSED_KEYS
        assert paper.closed_deal(res.ticket)["position"] == res.ticket

    def test_open_position_has_no_closed_deal(self, inner, paper):
        res = _open_buy(inner, paper)
        assert paper.closed_deal(res.ticket) is None

    def test_the_reconcile_helper_reads_paper_rows(self, inner, paper):
        from mintel.ops.reconcile import summarise
        res = _open_buy(inner, paper, volume=0.5)
        inner.set_tick("EURUSD", 1.10210, 1.10220)
        paper.close(res.ticket)
        s = summarise(paper.deals_since(T0, TNB, False), T0, T0 + dt.timedelta(days=1))
        assert s["positions"] == 1 and s["wins"] == 1
        assert s["net"] == pytest.approx(77.20) and s["commission"] == pytest.approx(-2.80)

    def test_the_trader_finalises_a_paper_trade_from_the_paper_deal(self, sim, cfg):
        """The real code path: Trend & Breakout's own Trader on SimBroker prices,
        orders on paper. The sim's order methods would explode if reached."""
        def boom(*a, **k):
            raise AssertionError("PAPER leak: the real broker received an order call")
        sim.send = sim.modify_stops = sim.close = boom
        from mintel.engine.trader import Trader
        paper = PaperBroker(sim, cfg.ops.data_dir, magic=cfg.magic, now_fn=lambda: sim.now)
        cfg.account_login, cfg.account_server = sim.login, sim.server
        t = Trader(paper, cfg, clock=lambda: sim.now, enable_model=False)
        t.bootstrap()
        for _ in range(80):
            t.cycle()
            sim.advance(3)
        closed = t.journal.closed_trades(limit=50)
        assert closed, "trades must complete on paper and be journalled"
        assert sim.order_log == [] and sim.positions() == []
        for row in closed:
            assert row["ticket"] >= PAPER_FIRST_TICKET and paper.is_paper(row["ticket"])
            deal = paper.closed_deal(row["ticket"])
            assert deal is not None
            assert row["pnl_money"] == pytest.approx(deal["pnl"], abs=0.01)
        # the daily-loss stop's figure is the paper ledger (entries' commission included)
        start = t._strategy_day_start(sim.now)
        expected = sum(r["profit"] for r in paper.deals_since(start, cfg.magic, False))
        t._realised_cache = {"at": None, "value": 0.0}
        assert t.realised_today() == pytest.approx(expected)
        paper.close_db()


# -------------------------------------------- the real broker is never touched --
class TestIsolation:
    def _with_others(self, inner):
        inner.real_positions = [
            Position(7001, "US500", Side.BUY, 1.0, 5000.0, 4990.0, 0.0, T0, magic=RUNNER, comment="RUNNER"),
            Position(7002, "EURUSD", Side.SELL, 0.1, 1.1, 1.11, 0.0, T0, magic=0, comment="by hand"),
            Position(7003, "GBPUSD", Side.BUY, 0.2, 1.25, 1.24, 0.0, T0, magic=TNB, comment="from LIVE"),
        ]
        inner.real_deals = [
            {"position": 7004, "magic": RUNNER, "symbol": "US500", "volume": 1.0, "profit": -12.0,
             "commission": 0.0, "is_entry": False, "time": T0},
            {"position": 7005, "magic": TNB, "symbol": "EURUSD", "volume": 0.1, "profit": 5.0,
             "commission": -0.28, "is_entry": False, "time": T0},
        ]

    def test_inner_order_methods_are_never_called(self, inner, paper):
        self._with_others(inner)
        res = _open_buy(inner, paper, sl=1.09500, tp=1.11000)
        paper.modify_stops(res.ticket, 1.09700, 1.11000)
        paper.close(res.ticket, 0.2)
        paper.close(res.ticket)
        for ticket in (7001, 7002, 7003, 999):                    # other bots, hand trades, T&B's own real one
            assert not paper.modify_stops(ticket, 1.0, 0.0).ok
            assert not paper.close(ticket).ok
        paper.summary()
        assert inner.order_calls == []
        for name in ("_rpc", "order_send", "cancel", "mt5"):
            with pytest.raises(AttributeError):
                getattr(paper, name)

    def test_other_magics_are_never_shown_as_its_own(self, inner, paper):
        self._with_others(inner)
        res = _open_buy(inner, paper)
        own = paper.positions(TNB)
        assert {p.ticket for p in own} == {7003, res.ticket}        # its paper trade + its own real leftover
        assert all(p.magic == TNB for p in own)
        assert [p.ticket for p in paper.positions(RUNNER)] == [7001]  # the real answer, unchanged
        assert {p.ticket for p in paper.positions(None)} == {7001, 7002, 7003}   # the whole REAL account
        everything = paper.deals_since(T0, 0, False)
        assert {r["position"] for r in everything} == {7004, 7005}   # paper is never in the account total
        mine = paper.deals_since(T0, TNB, False)
        assert {r["position"] for r in mine} == {7005, res.ticket}
        assert not paper.is_paper(7003) and paper.is_paper(res.ticket)

    def test_a_leftover_real_trade_can_be_wound_down_only_when_asked(self, inner, tmp_path):
        self._with_others(inner)
        inner.allow_orders = True
        pb = make(inner, tmp_path, manage_leftovers=True)
        assert pb.modify_stops(7003, 1.245, 0.0).ok                 # T&B's own real trade: tightened at the broker
        for ticket in (7001, 7002):                                 # never another bot's, never a hand trade
            assert not pb.modify_stops(ticket, 1.0, 0.0).ok and not pb.close(ticket).ok
        inner.set_tick("EURUSD", 1.1, 1.1001)
        assert pb.send(req("EURUSD", Side.BUY, 0.1, sl=1.09)).ok  # new orders stay on paper
        assert pb.close(7003).ok
        assert [(c[0], c[1]) for c in inner.order_calls] == [("modify_stops", 7003), ("close", 7003)]
        pb.close_db()

    def test_read_only_calls_pass_through(self, inner, paper):
        assert paper.account().currency == "GBP" and paper.symbols() == inner.symbols()
        assert paper.spec("EURUSD") is inner.specs["EURUSD"]
        assert paper.clock is inner.clock and paper.clock_skew()["skew_seconds"] == 0.0
        assert paper.calc_margin("EURUSD", Side.BUY, 1.0, 1.1) == 100.0
        paper.timeout = 10.0
        assert inner.timeout == 10.0


# ------------------------------------------------------------- live groups --
class TestLiveGroups:
    def test_indices_go_to_the_real_broker_and_the_rest_stays_on_paper(self, inner, tmp_path):
        inner.allow_orders = True
        pb = make(inner, tmp_path, live_groups=("index",))
        inner.set_tick("US500", 5000.00, 5000.50)
        inner.set_tick("EURUSD", 1.10000, 1.10010)
        real = pb.send(req("US500", Side.BUY, 1.0, sl=4990.0, key="idx"))
        fx = pb.send(req("EURUSD", Side.BUY, 0.1, sl=1.095, key="fx"))
        assert real.ok and real.ticket == 5_000_001 and not pb.is_paper(real.ticket)
        assert fx.ok and pb.is_paper(fx.ticket)
        assert [c[0] for c in inner.order_calls] == ["send"]
        assert {p.ticket for p in pb.positions(TNB)} == {real.ticket, fx.ticket}
        # routed by origin
        assert pb.modify_stops(real.ticket, 4995.0, 0.0).ok
        assert pb.modify_stops(fx.ticket, 1.0970, 0.0).ok
        assert [c[0] for c in inner.order_calls] == ["send", "modify_stops"]
        assert all(c[1] != fx.ticket for c in inner.order_calls[1:])
        assert pb.close(fx.ticket).ok and pb.close(real.ticket).ok
        assert [c[0] for c in inner.order_calls] == ["send", "modify_stops", "close"]
        assert inner.order_calls[-1][1] == real.ticket
        s = pb.summary()
        assert s["live_groups"] == ["INDEX"] and "real broker" in s["note"]
        pb.close_db()

    def test_summary_says_which_is_which(self, inner, tmp_path):
        inner.allow_orders = True
        pb = make(inner, tmp_path, live_groups=("INDEX",))
        inner.set_tick("US500", 5000.00, 5000.50)
        inner.set_tick("EURUSD", 1.10000, 1.10010)
        pb.send(req("US500", Side.BUY, 1.0, sl=4990.0, key="idx"))
        pb.send(req("EURUSD", Side.BUY, 0.1, sl=1.095, key="fx"))
        origins = {r["symbol"]: r["origin"] for r in pb.summary()["open"]}
        assert origins == {"US500": "REAL", "EURUSD": "PAPER"}
        pb.close_db()

    def test_default_is_everything_on_paper_even_indices(self, inner, paper):
        inner.set_tick("US500", 5000.00, 5000.50)
        res = paper.send(req("US500", Side.BUY, 1.0, sl=4990.0))
        assert res.ok and paper.is_paper(res.ticket) and inner.order_calls == []


# --------------------------------------------------------------- restart --
class TestRestart:
    def test_a_new_wrapper_on_the_same_file_restores_everything(self, inner, tmp_path):
        a = make(inner, tmp_path)
        keep = _open_buy(inner, a, sl=1.09500, tp=1.11000)
        gone = a.send(req("EURUSD", Side.SELL, 0.3, sl=1.10500, key="s"))
        a.modify_stops(keep.ticket, 1.09700, 1.11000)
        inner.set_tick("EURUSD", 1.10100, 1.10110)
        a.close(gone.ticket)
        before_pos = [(p.ticket, p.volume, p.entry_price, p.sl, p.tp) for p in a.positions(TNB)]
        before_deals = a.deals_since(T0, TNB, False)
        a.close_db()

        b = make(inner, tmp_path)
        assert [(p.ticket, p.volume, p.entry_price, p.sl, p.tp) for p in b.positions(TNB)] == before_pos
        assert b.deals_since(T0, TNB, False) == before_deals
        assert b.closed_deal(gone.ticket) == a.closed_deal(gone.ticket)
        new = b.send(req("EURUSD", Side.BUY, 0.1, sl=1.095, key="n"))
        used = {r["position"] for r in before_deals} | {keep.ticket, gone.ticket}
        assert new.ticket not in used and new.ticket > max(used)
        # the restored stop is still held: a stop hit after the restart closes it
        inner.set_tick("EURUSD", 1.09700, 1.09710)
        assert keep.ticket not in {p.ticket for p in b.positions(TNB)}
        assert b.closed_deal(keep.ticket)["exit_price"] == pytest.approx(1.09700)
        b.close_db()

    def test_a_stop_hit_while_down_is_found_in_the_tick_history(self, inner, tmp_path):
        a = make(inner, tmp_path)
        res = _open_buy(inner, a, sl=1.09500)
        a.close_db()
        inner.set_tick("EURUSD", 1.09450, 1.09460, seconds=60)    # while the bot was restarting
        inner.set_tick("EURUSD", 1.09800, 1.09810, seconds=60)
        b = make(inner, tmp_path)
        assert b.positions(TNB) == []
        assert b.closed_deal(res.ticket)["exit_price"] == pytest.approx(1.09450)
        b.close_db()

    def test_a_read_only_view_never_books_or_sends(self, inner, tmp_path):
        a = make(inner, tmp_path)
        res = _open_buy(inner, a, sl=1.09500)
        view = make(inner, tmp_path, read_only=True)
        assert [p.ticket for p in view.positions(TNB)] == [res.ticket]
        inner.set_tick("EURUSD", 1.09400, 1.09410)                # the stop is hit
        assert [p.ticket for p in view.positions(TNB)] == [res.ticket]   # the view books nothing
        for out in (view.send(req("EURUSD", Side.BUY, 0.1, sl=1.09)), view.modify_stops(res.ticket, 1.0, 0.0),
                    view.close(res.ticket)):
            assert not out.ok and "read-only" in out.comment
        assert a.positions(TNB) == []                             # the trader's own instance books it, once
        view.reload()
        assert view.positions(TNB) == [] and view.closed_deal(res.ticket)["exit_price"] == pytest.approx(1.094)
        assert len([r for r in view.deals_since(T0, TNB) if r["position"] == res.ticket]) == 1
        assert inner.order_calls == []
        empty = make(inner, tmp_path / "nothing-yet", read_only=True)
        assert empty.positions(TNB) == [] and not (tmp_path / "nothing-yet").exists()
        for pb in (a, view, empty):
            pb.close_db()

    def test_tickets_never_collide_with_a_real_position(self, inner, tmp_path):
        inner.real_positions = [Position(PAPER_FIRST_TICKET, "US500", Side.BUY, 1.0, 5000.0, 4990.0, 0.0, T0,
                                         magic=RUNNER)]
        pb = make(inner, tmp_path)
        res = _open_buy(inner, pb)
        assert res.ticket != PAPER_FIRST_TICKET
        pb.close_db()


# ------------------------------------------------------------- threads --
class TestConcurrency:
    def test_many_threads_share_one_book(self, inner, tmp_path):
        pb = make(inner, tmp_path, replay_ticks=False)
        inner.set_tick("EURUSD", 1.10000, 1.10010)
        inner.set_tick("XAUUSD", 2400.0, 2400.3)
        errors: list[BaseException] = []
        opened: list[int] = []
        lock = threading.Lock()

        def worker(seed: int) -> None:
            rnd = random.Random(seed)
            try:
                for i in range(40):
                    sym = rnd.choice(("EURUSD", "XAUUSD"))
                    if rnd.random() < 0.4:
                        side = rnd.choice((Side.BUY, Side.SELL))
                        t = inner.ticks[sym]
                        dist = 0.005 if sym == "EURUSD" else 10.0
                        sl = t.bid - dist if side is Side.BUY else t.ask + dist
                        r = pb.send(req(sym, side, 0.1, sl=sl, key=f"{seed}-{i}"))
                        if r.ok:
                            with lock:
                                opened.append(r.ticket)
                    elif rnd.random() < 0.5:
                        mine = pb.positions(TNB)
                        if mine:
                            pb.close(rnd.choice(mine).ticket, rnd.choice((0.0, 0.05)))
                    else:
                        pb.tick(sym)
                        pb.deals_since(T0, TNB, False)
                        pb.summary()
            except BaseException as exc:                     # pragma: no cover - the failure itself
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(s,)) for s in range(6)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=30)
        assert not errors, errors
        assert len(opened) == len(set(opened)) and opened
        rows = pb.deals_since(T0, TNB, False)
        assert sorted(r["position"] for r in rows if r["is_entry"]) == sorted(opened)
        open_now = {p.ticket: p.volume for p in pb.positions(TNB)}
        for ticket in opened:                                   # every lot is either still open or closed
            entry = sum(r["volume"] for r in rows if r["position"] == ticket and r["is_entry"])
            out = sum(r["volume"] for r in rows if r["position"] == ticket and not r["is_entry"])
            assert entry == pytest.approx(out + open_now.get(ticket, 0.0))
        pb.close_db()
        again = make(inner, tmp_path, replay_ticks=False)
        assert {p.ticket: p.volume for p in again.positions(TNB)} == open_now
        again.close_db()


def test_from_config_takes_the_traders_magic_and_folder(inner, tmp_path):
    from mintel.config import Config
    c = Config()
    c.ops.data_dir = str(tmp_path / "d")
    pb = PaperBroker.from_config(inner, c, live_groups=("INDEX",))
    assert pb.magic == c.magic and pb.path.parent == tmp_path / "d" and pb.live_groups == {"INDEX"}
    assert pb.slippage_points == 1.0                           # as pessimistic as the other paper bots
    pb.close_db()


def test_sim_paper_round_trip_without_a_trader(tmp_path):
    """A plain SimBroker underneath (no scripted ticks): fills at its bid/ask."""
    sim = SimBroker(seed=3, history_bars=200)
    sim.connect()
    pb = PaperBroker(sim, tmp_path, now_fn=lambda: sim.now)
    t = sim.tick("EURUSD")
    res = pb.send(req("EURUSD", Side.BUY, 0.1, sl=round(t.bid - 0.003, 5)))
    assert res.ok and res.price == pytest.approx(t.ask + sim.spec("EURUSD").point)   # the default 1 point worse
    sim.advance(5)
    pb.positions(TNB)
    assert sim.order_log == []
    pb.close_db()


# ------------------------------------------------------------------ wiring --
def _paper_on_sim(sim, cfg) -> PaperBroker:
    cfg.account_login, cfg.account_server = sim.login, sim.server
    return PaperBroker(sim, cfg.ops.data_dir, magic=cfg.magic, now_fn=lambda: sim.now)


class TestWiring:
    """The wrapper is for Trend & Breakout alone. The other bots must be
    handed the REAL broker (wiring step 2 in mintel/broker/paper.py)."""

    def test_real_broker_unwraps_only_the_paper_wrapper(self, inner, paper):
        assert real_broker(paper) is inner and real_broker(inner) is inner and real_broker(None) is None

    def test_with_the_wiring_the_live_runner_trades_at_the_real_broker(self, sim, cfg, monkeypatch):
        from mintel.engine.trader import Trader
        unwired = Trader._bot_kwargs

        def wired(self, kls):                         # the change wiring step 2 asks of trader.py
            out = unwired(self, kls)
            if "broker" in out:
                out["broker"] = real_broker(out["broker"])
            return out

        monkeypatch.setattr(Trader, "_bot_kwargs", wired)
        cfg.runner.mode = "LIVE"
        paper = _paper_on_sim(sim, cfg)
        t = Trader(paper, cfg, clock=lambda: sim.now, enable_model=False)
        assert t.broker is paper and t.runner is not None and t.runner.live
        assert t.runner.executor.broker is sim and t.runner.broker is sim
        tick, spec = sim.tick("EURUSD"), sim.spec("EURUSD")
        fill = t.runner.executor.open("EURUSD", Side.BUY, 0.1, round(tick.bid - 0.003, 5), 0.0, tick, spec, sim.now)
        assert fill.ok, fill.message
        assert [p.ticket for p in sim.positions(RUNNER)] == [fill.ticket]   # the Runner's own REAL order
        assert t.runner.executor.modify_stop(fill.ticket, round(tick.bid - 0.002, 5))   # trailed there
        assert paper.positions(TNB) == [] and paper.closed == []         # nothing on the paper book
        paper.close_db()

    def test_the_trader_itself_gives_the_bots_the_real_broker(self, sim, cfg):
        from mintel.engine.trader import Trader
        cfg.runner.mode = "LIVE"
        paper = _paper_on_sim(sim, cfg)
        try:
            t = Trader(paper, cfg, clock=lambda: sim.now, enable_model=False)
            assert t.runner.executor.broker is sim
        finally:
            paper.close_db()

    def test_a_report_reads_the_paper_record_through_a_read_only_view(self, inner, tmp_path):
        """What wiring step 3 asks of day_review, ledger_check and report_upload."""
        from mintel.config import Config
        from mintel.ops.day_review import build_day_review
        writer = make(inner, tmp_path)
        res = _open_buy(inner, writer, volume=0.5)
        inner.set_tick("EURUSD", 1.10210, 1.10220)
        writer.close(res.ticket)
        c = Config()
        c.ops.data_dir = str(tmp_path)
        view = PaperBroker(inner, tmp_path, magic=c.magic, read_only=True)
        _text, positions, _rows = build_day_review(c, view, T0.replace(hour=0))
        assert [p["position"] for p in positions] == [res.ticket]
        assert positions[0]["net"] == pytest.approx(77.20)
        assert inner.order_calls == []
        for pb in (writer, view):
            pb.close_db()
