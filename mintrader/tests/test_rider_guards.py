"""The Rapid Momentum Rider's guards added on 9 Oct: the CURRENCY CLUSTER
and the LOSS COOLDOWN (RiderCore.guard_hold).

The evidence they answer (the Rider's first LIVE morning, Fri 9 Oct, broker
figures): 23 trades 08:54-11:05 UTC, 9 won 14 lost, net +4.7 pips but
-18.27 GBP after 22.92 of costs; eleven trades in five minutes, seven of
them JPY crosses on the same yen move. One idea was traded many times over.

Every price here is SYNTHETIC (seeded, made up) or scripted by hand. The
tests show the rules behave as designed - one idea is entered once, the
waits run for exactly their minutes, they survive a restart, they never
change a size or any other rule - and say NOTHING about whether the guards
make money (that is the research registry's RMR-GUARDS-ON / RMR-GUARDS-OFF
pair, on real ticks)."""
import bisect
import datetime as dt
import json
import random

import pytest

from mintel.broker.base import Side
from mintel.rider import synth
from mintel.rider.config import RiderConfig
from mintel.rider.core import EntryIntent, RiderCore
from mintel.rider.engine import GATE_WORDS
from mintel.rider.journal import RiderJournal
from mintel.rider.research.costs import CostModel
from mintel.rider.research.replay import DayData, DayReplay, _bars_from_ticks
from mintel.rider.research.tickstore import DayTicks, day_start_ms, dt_to_ms, ms_to_dt
from tests.test_rider_engine import FakeBroker, make, run

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 10, 9, 9, 0, tzinfo=UTC).timestamp()
GUARDS_OFF = {"currency_cluster_guard": False, "loss_cooldown_guard": False}


# ------------------------------------------------------------ the pure core --

def core_with(*symbols, **cfg):
    core = RiderCore(RiderConfig(max_cost_ratio=0.25, **cfg))
    for s in symbols:
        core.set_spec(s, synth.make_spec(s))
    return core


def lose(core, sym, side, t, key, net=-4.97):
    core.record_exit(sym, side, t, net, key=key)


def opened(core, sym, side, t, ticket):
    """A position on the core's books (as after a fill)."""
    pip = synth.pip_of(sym)
    ref = 100.0 if sym.endswith("JPY") else 1.1
    stop = ref - side * 20 * pip
    it = EntryIntent(sym, Side.BUY if side > 0 else Side.SELL, stop, "scripted", {}, "breakout", 70.0, 1, 0.0,
                     dt.datetime.fromtimestamp(t, UTC), ref, 5 * pip, 0.2 * pip, 0.8 * pip, 20 * pip, 0.1, 0.6, 2.0,
                     20.0)
    return core.open_position(it, ref, 0.14, ticket, dt.datetime.fromtimestamp(t, UTC))


class TestCurrencyCluster:
    def test_the_same_side_of_a_currency_through_another_market_is_refused_in_plain_words(self):
        core = core_with("EURJPY", "GBPJPY", "EURUSD", "USDCHF")
        opened(core, "EURJPY", -1, T0, 1)                           # short EURJPY: short EUR, long JPY
        assert core.guard_hold("GBPJPY", -1, T0 + 5) == ("currency_cluster", "already long JPY through EURJPY")
        assert core.guard_hold("GBPJPY", +1, T0 + 5) is None        # long GBPJPY is short JPY: another idea
        opened(core, "EURUSD", +1, T0, 2)                           # long EURUSD: long EUR, short USD
        assert core.guard_hold("USDCHF", -1, T0 + 5) == ("currency_cluster", "already short USD through EURUSD")
        assert core.guard_hold("USDCHF", +1, T0 + 5) is None

    def test_the_cap_and_the_switch(self):
        core = core_with("EURJPY", "GBPJPY", "AUDJPY", max_same_currency_positions=2)
        opened(core, "EURJPY", -1, T0, 1)
        assert core.guard_hold("GBPJPY", -1, T0) is None            # two positions long JPY are allowed ...
        opened(core, "GBPJPY", -1, T0, 2)
        assert core.guard_hold("AUDJPY", -1, T0) == ("currency_cluster",
                                                     "already long JPY through EURJPY and GBPJPY")   # ... not three
        off = core_with("EURJPY", "GBPJPY", currency_cluster_guard=False)
        opened(off, "EURJPY", -1, T0, 1)
        assert off.guard_hold("GBPJPY", -1, T0) is None

    def test_an_entry_sent_but_not_filled_yet_counts_for_a_few_seconds(self):
        core = core_with("EURJPY", "GBPJPY")
        core.last_signal["EURJPY"], core.signal_side["EURJPY"] = T0, -1
        assert core.guard_hold("GBPJPY", -1, T0 + 1)[0] == "currency_cluster"
        assert core.guard_hold("GBPJPY", -1, T0 + 6) is None        # never filled: it stops counting


class TestLossCooldown:
    def test_a_losing_exit_rests_the_market_for_15_minutes(self):
        core = core_with("EURJPY", "GBPJPY", "EURUSD", "AUDCAD")
        lose(core, "EURJPY", -1, T0, 1)
        rule, words = core.guard_hold("EURJPY", -1, T0 + 60)
        assert rule == "loss_cooldown" and words == "EURJPY lost 1 min ago: no new entry on it for 15 min - 14 min left"
        assert core.guard_hold("EURJPY", +1, T0 + 899)[0] == "loss_cooldown"     # either direction
        assert core.guard_hold("EURJPY", 0, T0 + 899)[0] == "loss_cooldown"      # (the scanner, no direction yet)
        assert core.guard_hold("EURJPY", -1, T0 + 901) is None
        assert core.guard_hold("AUDCAD", +1, T0 + 60) is None                     # another market, other currencies

    def test_a_losing_exit_rests_its_currency_sides_for_15_minutes(self):
        core = core_with("EURJPY", "GBPJPY", "EURUSD")
        lose(core, "EURJPY", -1, T0, 1)                             # a losing short EURJPY: long JPY, short EUR
        assert core.guard_hold("GBPJPY", -1, T0 + 300) == (
            "currency_loss", "long JPY lost through EURJPY 5 min ago: no new long JPY for 15 min - 10 min left")
        assert core.guard_hold("EURUSD", -1, T0 + 300)[1].startswith("short EUR lost through EURJPY")
        assert core.guard_hold("GBPJPY", +1, T0 + 300) is None      # short JPY was not the idea that lost
        assert core.guard_hold("GBPJPY", -1, T0 + 901) is None
        only_market = core_with("EURJPY", "GBPJPY", loss_cooldown_same_currency=False)
        lose(only_market, "EURJPY", -1, T0, 1)
        assert only_market.guard_hold("GBPJPY", -1, T0 + 300) is None

    def test_two_losing_exits_in_a_row_pause_the_market_for_2_hours(self):
        core = core_with("EURJPY")
        lose(core, "EURJPY", -1, T0, 1)
        lose(core, "EURJPY", +1, T0 + 1200, 2)
        rule, words = core.guard_hold("EURJPY", -1, T0 + 1260)
        assert rule == "loss_pause" and words == ("two losing exits in a row on EURJPY: no new entry on it for 2 h - "
                                                  "1 h 59 min left")
        assert core.guard_hold("EURJPY", +1, T0 + 1200 + 7199)[0] == "loss_pause"
        assert core.guard_hold("EURJPY", +1, T0 + 1200 + 7201) is None

    def test_two_losses_further_apart_than_the_pause_get_only_the_cooldown(self):
        # Thursday 16:00 UTC lost, Friday 09:10 lost (17 h 10 min later): the replay starts Friday with a fresh
        # core and gives the second loss only the 15-min cooldown; live (exits kept across midnight) must agree.
        thu = dt.datetime(2026, 10, 8, 16, 0, tzinfo=UTC).timestamp()
        fri = dt.datetime(2026, 10, 9, 9, 10, tzinfo=UTC).timestamp()
        core = core_with("EURJPY")
        lose(core, "EURJPY", -1, thu, 1)
        lose(core, "EURJPY", -1, fri, 2)
        assert core.guard_hold("EURJPY", -1, fri + 60)[0] == "loss_cooldown"
        assert core.guard_hold("EURJPY", -1, fri + 901) is None
        assert not any("in a row" in w for w in core.guard_status(fri + 60)["now"])
        mon = dt.datetime(2026, 10, 12, 7, 5, tzinfo=UTC).timestamp()     # Friday's loss, then Monday's
        lose(core, "EURJPY", +1, mon, 3)
        assert core.guard_hold("EURJPY", +1, mon + 901) is None
        edge = core_with("EURJPY")                                         # the second exactly 2 h after the first:
        lose(edge, "EURJPY", -1, T0, 1)                                    # still in a row
        lose(edge, "EURJPY", -1, T0 + 7200, 2)
        assert edge.guard_hold("EURJPY", -1, T0 + 7200 + 901)[0] == "loss_pause"
        late = core_with("EURJPY")                                         # one second more: only the cooldown
        lose(late, "EURJPY", -1, T0, 1)
        lose(late, "EURJPY", -1, T0 + 7201, 2)
        assert late.guard_hold("EURJPY", -1, T0 + 7201 + 901) is None

    def test_a_win_in_between_breaks_the_run(self):
        core = core_with("EURJPY")
        lose(core, "EURJPY", -1, T0, 1)
        core.record_exit("EURJPY", -1, T0 + 1000, +3.10, key=2)
        lose(core, "EURJPY", -1, T0 + 2000, 3)
        assert core.guard_hold("EURJPY", -1, T0 + 2060)[0] == "loss_cooldown"
        assert core.guard_hold("EURJPY", -1, T0 + 2000 + 901) is None
        core.record_exit("EURJPY", -1, T0 + 1000, -0.40, key=2)      # the broker's figure replaces the estimate
        assert core.guard_hold("EURJPY", -1, T0 + 2060)[0] == "loss_pause"

    def test_four_losing_exits_within_10_minutes_brake_every_market_for_15_minutes(self):
        markets = ("EURUSD", "GBPJPY", "AUDCAD", "NZDCHF")          # every currency of the eight used once
        core = core_with(*markets, "EURGBP")
        for k, (sym, dt_s) in enumerate(zip(markets, (0, 120, 240, 540))):
            lose(core, sym, +1, T0 + dt_s, k + 1)
        rule, words = core.guard_hold("EURGBP", +1, T0 + 600)
        assert rule == "loss_brake" and words == ("4 losing exits within 10 min (the last at 09:09 UTC): no new entry "
                                                  "on any market for 15 min - 14 min left")
        assert core.guard_hold("EURGBP", -1, T0 + 540 + 899)[0] == "loss_brake"
        assert core.guard_hold("EURGBP", -1, T0 + 540 + 901) is None
        spread = core_with(*markets, "EURGBP")                     # the same four over 10 min 1 s: no brake
        for k, (sym, dt_s) in enumerate(zip(markets, (0, 200, 400, 601))):
            lose(spread, sym, +1, T0 + dt_s, k + 1)
        hold = spread.guard_hold("EURGBP", -1, T0 + 700)
        assert hold is None or hold[0] != "loss_brake"

    def test_wins_and_breakevens_never_count_and_the_switch_turns_it_all_off(self):
        core = core_with("EURJPY")
        core.record_exit("EURJPY", -1, T0, 0.0, key=1)
        core.record_exit("EURJPY", -1, T0 + 10, 2.0, key=2)
        core.record_exit("EURJPY", -1, T0 + 20, None, key=3)          # no figure: nothing recorded
        assert core.guard_hold("EURJPY", -1, T0 + 30) is None and len(core.exit_log) == 2
        off = core_with("EURJPY", "GBPJPY", loss_cooldown_guard=False)
        for k in range(5):
            lose(off, "EURJPY", -1, T0 + k, k)
        assert off.guard_hold("EURJPY", -1, T0 + 60) is None and off.guard_hold("GBPJPY", -1, T0 + 60) is None

    def test_no_look_ahead_an_exit_after_now_does_not_count(self):
        core = core_with("EURJPY")
        lose(core, "EURJPY", -1, T0 + 100, 1)
        assert core.guard_hold("EURJPY", -1, T0 + 50) is None

    def test_every_guard_rule_has_plain_words_for_why_no_trade(self):
        for rule in ("currency_cluster", "loss_cooldown", "loss_pause", "currency_loss", "loss_brake"):
            assert GATE_WORDS[rule]


# --------------------------------------------- the 08:54-08:59 JPY shape --

CROSSES = {"EURJPY": 164.05, "GBPJPY": 192.00, "CADJPY": 110.40, "AUDJPY": 99.10, "CHFJPY": 184.30}
FEED_FROM = dt.datetime(2026, 10, 9, 8, 50, tzinfo=UTC)


def jpy_shape(seed=21, waves=4, down_s=50.0, down_ppm=14.0, snap_s=15.0, snap_ppm=30.0, pause_s=20.0,
              warm_min=44.0):
    """SYNTHETIC: a quiet warm-up, then from 08:54 UTC four waves of yen
    strength - every JPY cross falls 14 pips a minute for 50 s, snaps back
    30 pips a minute for 15 s, pauses 20 s - then quiet again. Each cross is
    the one yen factor plus its own noise: the same idea in five markets."""
    rng = random.Random(seed)
    t, x, fac = 0.0, 0.0, []
    while t < warm_min * 60:
        t += 0.25
        x += rng.gauss(0, 0.05) - 0.002 * x
        fac.append((t, x))
    for _ in range(waves):
        for secs, ppm in ((down_s, -down_ppm), (snap_s, snap_ppm), (pause_s, 0.0)):
            end = t + secs
            while t < end:
                t += 0.25
                x += ppm / 60.0 * 0.25 + rng.gauss(0, 0.05)
                fac.append((t, x))
    end = t + 300
    while t < end:
        t += 0.25
        x += rng.gauss(0, 0.05) - 0.01 * (x - fac[-1][1])
        fac.append((t, x))
    start = dt.datetime(2026, 10, 9, 8, 54, tzinfo=UTC) - dt.timedelta(minutes=warm_min)
    out = {}
    for k, (sym, px) in enumerate(CROSSES.items()):
        r = random.Random(seed * 100 + k)
        path = [(tt, xx + r.gauss(0, 0.08)) for tt, xx in fac if r.random() < 0.85]
        out[sym] = synth.ticks_from_path(sym, start, path, px, spread_pips=0.8, digits=3)
    return out


def jpy_specs():
    return {s: synth.make_spec(s, digits=3, tick_value=0.4926) for s in CROSSES}


def jpy_day(ticks_by):
    """The shape as a replay day: one- and five-minute bars of the warm-up
    as the seed (what live gets from the broker's bars), ticks from 08:50."""
    d0 = day_start_ms("2026-10-09")
    cut = dt_to_ms(FEED_FROM)
    ticks, seeds = {}, {}
    for s, ts in ticks_by.items():
        warm, feed = DayTicks(s), DayTicks(s)
        for tk in ts:
            d = feed if dt_to_ms(tk.time) >= cut else warm
            d.times.append(dt_to_ms(tk.time))
            d.bids.append(tk.bid)
            d.asks.append(tk.ask)
        ticks[s] = feed
        seeds[s] = ([b for b in _bars_from_ticks(warm, 60) if dt_to_ms(b.time) + 60_000 <= cut],
                    [b for b in _bars_from_ticks(warm, 300) if dt_to_ms(b.time) + 300_000 <= cut])
    return DayData("2026-10-09", sorted(ticks), jpy_specs(), [], cut, d0, d0 + 86_400_000, d0 + 86_400_000, ticks,
                   seeds)


def replay(data, **cfg):
    c = RiderConfig(max_cost_ratio=0.25, **cfg)
    return DayReplay(c, data, CostModel(slippage_pips=c.slippage_pips, latency_ms=c.latency_ms), "T").run_full()


def long_jpy(trades):
    return [t for t in trades if t["symbol"].endswith("JPY") and t["side"] == "SHORT"]


@pytest.fixture(scope="module")
def shape():
    data = jpy_day(jpy_shape())
    return {"data": data, "off": replay(data, **GUARDS_OFF), "on": replay(data)}


class TestTheJpyMorning:
    def test_one_yen_idea_is_entered_once_not_seven_times(self, shape):
        off, on = shape["off"].trades, shape["on"].trades
        assert len(off) >= 7 and long_jpy(off) == off                  # without the guards: the 9 Oct pattern
        assert len({t["symbol"] for t in off}) >= 4
        first = min(t["fill_ms"] for t in off)
        assert max(t["fill_ms"] for t in off) - first <= 5 * 60_000      # ... all inside five minutes
        assert ms_to_dt(first).strftime("%H:%M") >= "08:54"
        assert len(on) == 1 and long_jpy(on) == on                       # with them: once
        assert len(shape["on"].intents) == 1

    def test_the_market_rules_alone_let_the_idea_back_in_through_the_next_cross(self, shape):
        """Why a losing exit also rests its currency sides: with only the
        per-market cooldown, the next JPY cross takes the same idea again as
        soon as the first trade has closed."""
        only_market = replay(shape["data"], loss_cooldown_same_currency=False)
        assert 1 < len(only_market.trades) < len(shape["off"].trades)

    def test_the_guards_never_change_a_size(self, shape):
        sizes = {t["symbol"]: t["volume"] for t in shape["off"].trades}
        for t in shape["on"].trades:
            assert t["volume"] == sizes.get(t["symbol"], t["volume"])
            assert t["gbp_per_pip"] == pytest.approx(1.0, rel=0.05)      # Aaron's GBP 1 a pip, as always

    def test_one_position_per_market_and_at_most_four_are_unchanged(self, shape):
        for out in (shape["off"], shape["on"]):
            spans = [(t["fill_ms"], t["exit_ms"], t["symbol"]) for t in out.trades]
            for f, _e, s in spans:
                live = [x for x in spans if x[0] <= f < x[1]]
                assert len(live) <= 4 and len([x for x in live if x[2] == s]) <= 1
        assert max(sum(1 for x in [(t["fill_ms"], t["exit_ms"]) for t in shape["off"].trades] if x[0] <= f < x[1])
                   for f in [t["fill_ms"] for t in shape["off"].trades]) == 4   # the cap did bind without the guards

    def test_a_guard_that_cannot_bind_changes_nothing(self, shape):
        """Cluster cap 10 and no loss cooldown: every trade, price and stop
        exactly as with the guards off - no score, threshold, gate, size or
        FlowLock X behaviour moved."""
        loose = replay(shape["data"], max_same_currency_positions=10, loss_cooldown_guard=False)
        assert loose.trades == shape["off"].trades

    def test_same_ticks_same_decisions_and_the_future_never_changes_the_past(self, shape):
        data = shape["data"]
        assert replay(data).trades == shape["on"].trades
        cut = dt_to_ms(dt.datetime(2026, 10, 9, 8, 57, tzinfo=UTC))
        ticks = {}
        for s, d in data.ticks.items():
            nd = DayTicks(s)
            for t, b, a in zip(d.times, d.bids, d.asks):
                jump = 0.30 if t >= cut else 0.0                         # 30 pips up after 08:57
                nd.times.append(t)
                nd.bids.append(b + jump)
                nd.asks.append(a + jump)
            ticks[s] = nd
        from dataclasses import replace as dc_replace
        other = replay(dc_replace(data, ticks=ticks))
        assert [t for t in other.trades if t["fill_ms"] < cut] == [t for t in shape["on"].trades if t["fill_ms"] < cut]


# --------------------------------------------------------- the live engine --

class FastBroker(FakeBroker):
    """The engine tests' fake MetaTrader, with indexed tick lookups (five
    markets of ticks)."""

    def __init__(self, ticks_by_symbol, clock):
        super().__init__(ticks_by_symbol, clock)
        self.specs = jpy_specs()
        self._t = {s: [t.time for t in ts] for s, ts in self.ticks_by.items()}

    def _live(self, s):
        return self.ticks_by.get(s, [])[:bisect.bisect_right(self._t.get(s, []), self.clock.now)]

    def ticks_range(self, s, start, end):
        ts, tt = self.ticks_by.get(s, []), self._t.get(s, [])
        return ts[bisect.bisect_left(tt, start):bisect.bisect_right(tt, min(end, self.clock.now))]


def test_live_the_jpy_morning_is_entered_once_and_the_scanner_says_why(tmp_path):
    """SYNTHETIC. The live engine (LIVE, against the fake broker) on the same
    shape: the same core rules, so one entry; while it is held back the
    scanner says so in plain words, and the "why no trade" record names the
    guard."""
    ticks = jpy_shape()
    eng, broker, clock = make(tmp_path, ticks, FEED_FROM, broker_cls=FastBroker)
    held: dict = {}

    def hook(now, notes):
        for r in eng.last_rows:
            if r.held:
                held.setdefault(r.held_rule, set()).add(r.reason)
    run(eng, broker, clock, dt.datetime(2026, 10, 9, 9, 3, tzinfo=UTC), hook=hook)
    assert len(broker.sent) == 1 and broker.sent[0].side is Side.SELL           # once: the one yen idea
    vol = broker.sent[0].volume
    assert vol == pytest.approx(eng.core.size(broker.sent[0].symbol)[0])         # the usual size
    assert held, "a JPY cross should have been held back while the idea was taken or resting"
    reasons = " ".join(r for rs in held.values() for r in rs)
    assert ("already long JPY through" in reasons) or ("long JPY lost through" in reasons)
    assert all(r.startswith("Held back: ") for rs in held.values() for r in rs)
    why = eng.why_no_trade(clock.now)
    assert set(why["markets"]) == set(CROSSES)
    gates = {b["gate"] for b in why["blocked_by"]}
    assert gates & {"currency_cluster", "currency_loss", "loss_cooldown", "loss_pause", "loss_brake"}
    # the exit fed the loss guards with the broker's figure, after every cost
    closed = eng.journal.closed_since(dt.datetime(2026, 1, 1, tzinfo=UTC))
    for r in closed:
        rec = eng.core.exit_log[int(r["ticket"])]
        assert rec.lost == (r["net_money"] < 0) and rec.symbol == r["symbol"]
    st = eng.status(clock.now)
    assert st["guards"]["currency_cluster_guard"] and st["guards"]["loss_cooldown_guard"]
    assert "held_back" in st["scanner"][0]


def _closed_row(ticket, sym, side, closed, net, **kw):
    row = {"ticket": ticket, "mode": "LIVE", "symbol": sym, "side": side, "volume": 0.14, "gbp_per_pip": 1.0,
           "pip": 0.01, "signal_utc": (closed - dt.timedelta(minutes=2)).isoformat(),
           "opened_utc": (closed - dt.timedelta(minutes=2)).isoformat(), "closed_utc": closed.isoformat(),
           "entry": 164.0, "exit_price": 164.05, "exit_reason": "THESIS_DEAD", "realised_pips": net,
           "net_money": net, "money_source": "broker", "entry_family": "breakout"}
    row.update(kw)
    return row


def test_the_loss_guards_carry_on_after_a_restart(tmp_path):
    """The waits are read back from rider.sqlite at start: a restart neither
    lifts a pause nor starts one."""
    now = dt.datetime(2026, 10, 9, 9, 0, tzinfo=UTC)
    m = dt.timedelta(minutes=1)
    rows = [_closed_row(1, "EURJPY", "SELL", now - 30 * m, -4.97),
            _closed_row(2, "EURJPY", "SELL", now - 10 * m, -5.07),             # two in a row: paused to 10:50
            _closed_row(3, "GBPJPY", "BUY", now - 5 * m, 3.10),                # a win counts as nothing
            _closed_row(4, "NZDCAD", "SELL", now - 1 * m, -9.0, signal_utc=None, entry_family=None,
                        exit_reason="ORPHAN_CLOSED"),                          # not the Rider's own trade
            _closed_row(5, "AUDCHF", "BUY", now - 2 * m, -1.0, exit_reason="SWITCHED TO LIVE"),   # bookkeeping
            _closed_row(6, "CADJPY", "BUY", now - 4 * m, None, realised_pips=-2.0)]  # no money yet: the pips
    j = RiderJournal(tmp_path / "rider.sqlite")
    for r in rows:
        j.insert_trade(r)
    j.close()
    eng, _broker, clock = make(tmp_path, {"EURJPY": jpy_shape()["EURJPY"]}, now, broker_cls=FastBroker)
    core = eng.core
    assert core.guard_hold("EURJPY", +1, now.timestamp())[0] == "loss_pause"
    assert core.guard_hold("EURJPY", +1, (now + 109 * m).timestamp())[0] == "loss_pause"
    assert core.guard_hold("EURJPY", +1, (now + 111 * m).timestamp()) is None
    assert core.guard_hold("NZDCAD", +1, now.timestamp()) is None
    assert core.guard_hold("AUDCHF", -1, now.timestamp()) is None
    assert core.guard_hold("CADJPY", +1, now.timestamp())[0] == "loss_cooldown"
    # exactly what a bot that never stopped would hold: the same exits fed as they happened
    ref = RiderCore(RiderConfig(max_cost_ratio=0.25))
    for r in rows[:3] + rows[5:]:
        ref.record_exit(r["symbol"], 1 if r["side"] == "BUY" else -1, dt.datetime.fromisoformat(r["closed_utc"]),
                        r["net_money"] if r["net_money"] is not None else r["realised_pips"], key=r["ticket"])
    for sym in ("EURJPY", "GBPJPY", "CADJPY", "EURUSD", "USDJPY", "NZDCAD", "AUDCHF"):
        for side in (-1, 1):
            for k in (0, 3, 6, 20, 60, 111):
                t = (now + k * m).timestamp()
                assert core.guard_hold(sym, side, t) == ref.guard_hold(sym, side, t), (sym, side, k)
    eng.write_status(clock.now)
    st = json.loads(eng.status_path.read_text())
    assert any("two losing exits in a row on EURJPY" in line for line in st["guards"]["now"])


def test_the_settings_are_read_from_rider_json_and_bad_values_keep_their_defaults():
    c = RiderConfig.from_dict({"currency_cluster_guard": False, "max_same_currency_positions": 0,
                               "loss_cooldown_minutes": -5, "loss_pause_minutes": 60, "loss_brake_losses": "x"})
    assert c.currency_cluster_guard is False and c.loss_cooldown_guard is True
    assert c.max_same_currency_positions == 1 and c.loss_cooldown_minutes == 15.0 and c.loss_pause_minutes == 60.0
    assert c.loss_brake_losses == 4 and c.loss_brake_window_minutes == 10.0 and c.loss_brake_minutes == 15.0
    d = RiderConfig(max_cost_ratio=0.25)
    assert d.currency_cluster_guard and d.loss_cooldown_guard and d.loss_cooldown_same_currency
    assert d.user_pip_value_gbp == 1.0 and d.max_concurrent_positions == 4 and d.trigger_score == 60.0
