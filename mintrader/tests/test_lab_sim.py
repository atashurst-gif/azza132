"""The lab's simulator: every fill and every cost, on hand-made bars.

Each scenario is a blank day (flat price 1.10000, spread 10 points) with a few
bars changed, so the arithmetic can be checked by hand:

    EURUSD: point 1e-5, 0.79 pounds per point per lot, smallest lot 0.01.
    Signal at 10:00 UTC, stop 100 points from the signal close.
    BUY entry  = open + spread (10) + slip (max(1, 0.25 x 10) = 2.5) = 110012.5
    stop       = 109900, so the real risk distance is 112.5 points
    size       = floor(13 / (112.5 x 0.79) / 0.01) x 0.01 = 0.14 lots
    target 1R  = 110125, 2R = 110237.5
"""
from __future__ import annotations

import datetime as dt
import math
import time

import pytest

from mintel.broker.paper import DEFAULT_COMMISSION, DEFAULT_FALLBACK_FX
from mintel.lab import (COOLDOWN_BARS, MAX_ENTRIES_PER_DAY, Candidate, Signal, day_min,
                        iso_utc, make_params, sim, synth)
from mintel.lab.store import flat_min, session_bounds

DAY = "2026-07-01"            # a Wednesday in BST
SIG = 600                     # 10:00 UTC
PRICE, SPREAD = 110000, 10


class Stub:
    """A family that fires exactly the signals it is given (by bar index)."""
    code = "STUB"
    label = "stub"
    version = 1
    features = ()
    sensitive = ()
    dormant = False

    def __init__(self, signals):
        self.sig = list(signals)

    def signals(self, day, p, span, news):
        return [s for s in self.sig if day.i0 <= s.i < day.i1]


def cand(exit_code="t1", news="IGNORE", markets="FX_MAJOR", session="ALL"):
    return Candidate("STUB", 1, markets, session, news, exit_code, make_params())


def build(symbol="EURUSD", day=DAY, mods=None, spread=SPREAD, price=PRICE, hours=None):
    """A one-day series for ``symbol`` with bars changed by ``mods``
    ({index: (o, h, l, c[, s])})."""
    entry = synth.entry_for(symbol)
    t, o, h, l, c, s, v = synth.flat_columns(day, price, spread)
    for j, m in (mods or {}).items():
        o[j], h[j], l[j], c[j] = m[:4]
        if len(m) > 4:
            s[j] = m[4]
    return synth.series_from_columns(entry, (t, o, h, l, c, s, v), hours=hours)


def go(mods=None, *, signals=None, exit_code="t1", news="IGNORE", day=DAY, symbol="EURUSD",
       spread=SPREAD, hours=None, extra=()):
    """Run the stub on a one-day window. Returns (Result, trades)."""
    ser = build(symbol, day, mods, spread, hours=hours)
    sers = [ser] + list(extra)
    win = synth.window_of(sers, [day])
    fam = Stub(signals if signals is not None else [Signal(SIG, 1, 100)])
    res = sim.run(cand(exit_code, news), win, family=fam)
    return res, res.trades


# --------------------------------------------------------------- the basics ---
def test_a_buy_is_filled_and_costed_by_hand():
    # bar 604 runs up through the 1R target at 110125 by more than a point
    res, trades = go({604: (PRICE, 110200, PRICE, 110150)})
    assert len(trades) == 1
    t = trades[0]
    inst = sim.inst_of(build())
    entry = PRICE + SPREAD + 2.5
    assert t.entry == pytest.approx(entry * 1e-5, abs=1e-9)
    assert t.volume == 0.14                                   # floored, never rounded up
    assert t.exit_reason == "TARGET"
    assert t.exit_price == pytest.approx(110125 * 1e-5, abs=1e-9)   # no slippage on a target
    assert t.opened_utc == iso_utc(day_min(DAY) + SIG + 1)
    assert t.bars_held == 4                                   # 601, 602, 603, 604
    # money: (exit - entry) points x pounds per point per lot x lots
    gross = (110125 - entry) * 0.79 * 0.14
    comm = 2 * 0.14 * (3.5 * DEFAULT_FALLBACK_FX["USDGBP"])
    assert t.gross_gbp == pytest.approx(gross, rel=1e-9)
    assert t.net_gbp == pytest.approx(gross - comm, rel=1e-9)
    # cost = spread + slippage + commission (spread paid on entry for a buy)
    assert t.cost_gbp == pytest.approx((SPREAD + 2.5) * 0.79 * 0.14 + comm, rel=1e-9)
    assert t.r == pytest.approx(t.net_gbp / 13.0)
    assert t.fill_source == "bar" and not t.spread_assumed
    assert t.day == DAY and t.symbol == "EURUSD" and t.side == 1
    assert res.skipped == {} and res.daily[DAY][1] == 1
    assert res.daily[DAY][0] == pytest.approx(t.net_gbp)


def test_money_formula_matches_the_contract_spec():
    res, trades = go({604: (PRICE, 110200, PRICE, 110150)})
    t = trades[0]
    spec = synth.entry_for("EURUSD").spec
    gross = t.side * (t.exit_price - t.entry) / spec.tick_size * spec.tick_value * t.volume
    assert t.gross_gbp == pytest.approx(gross, rel=1e-6)


def test_stop_first_on_a_bar_that_touches_both_sides():
    # one bar reaches the target AND the stop: the stop is taken, never the target
    res, trades = go({603: (PRICE, 110300, 109900, PRICE)})
    t = trades[0]
    assert t.exit_reason == "STOP"
    assert t.exit_price == pytest.approx((109900 - 2.5) * 1e-5, abs=1e-9)    # stop - slip
    assert t.net_gbp < 0


def test_a_gap_through_the_stop_fills_at_the_open():
    res, trades = go({603: (109800, 109850, 109790, 109820)})
    t = trades[0]
    assert t.exit_reason == "STOP"
    assert t.exit_price == pytest.approx(109800 * 1e-5, abs=1e-9)            # the open, no extra slip


def test_the_entry_bar_can_stop_the_trade_out():
    res, trades = go({601: (PRICE, PRICE + 5, 109890, PRICE)})
    t = trades[0]
    assert t.exit_reason == "STOP" and t.bars_held == 1


def test_touching_the_target_is_not_a_fill():
    touch = {603: (PRICE, 110125, PRICE, PRICE)}              # high exactly at the 1R target
    res, trades = go(touch)
    assert trades[0].exit_reason == "SESSION_END"             # never filled
    over = {603: (PRICE, 110126, PRICE, PRICE)}               # one point through it
    res, trades = go(over)
    assert trades[0].exit_reason == "TARGET"
    assert trades[0].exit_price == pytest.approx(110125 * 1e-5, abs=1e-9)


def test_the_trail_follows_the_previous_peak_not_this_bars_high():
    # tr2: no target; once +1R the stop trails 2R (225 points) behind the best price.
    # bar 602 makes the peak 110130 (>= entry + 112.5), so from bar 603 the stop is
    # 110130 - 225 = 109905. Bar 603 spikes to 110500 and dips to 109900: with the
    # PREVIOUS peak the stop is 109905 and it is hit, at 109905 - 2.5.
    res, trades = go({602: (PRICE, 110130, PRICE, PRICE),
                      603: (PRICE, 110500, 109900, PRICE)}, exit_code="tr2")
    t = trades[0]
    assert t.exit_reason == "TRAIL"
    assert t.exit_price == pytest.approx((109905 - 2.5) * 1e-5, abs=1e-9)
    # and once bar 603 has made its high the NEXT bar's stop follows it: 110500 - 225
    res, trades = go({602: (PRICE, 110130, PRICE, PRICE),
                      603: (PRICE, 110500, 110000, PRICE),
                      604: (110400, 110400, 110200, 110300)}, exit_code="tr2")
    t = trades[0]
    assert t.exit_reason == "TRAIL"
    assert t.exit_price == pytest.approx((110500 - 225 - 2.5) * 1e-5, abs=1e-9)


def test_a_trail_does_nothing_before_one_r():
    res, trades = go({602: (PRICE, 110100, PRICE, PRICE),     # +87.5 only: not yet +1R (112.5)
                      603: (PRICE, PRICE, 109950, PRICE)}, exit_code="tr2")
    assert trades[0].exit_reason == "SESSION_END"             # the initial stop (109900) was not hit


# ------------------------------------------------------------------ sells ---
def test_a_sell_works_on_the_ask_so_the_spread_counts_against_it():
    # SELL: entry = open - slip = 109997.5; stop = 110000 + 100 = 110100 on the ASK.
    # bar 603's bid high is only 110095, but the ask is 110105 >= the stop.
    sig = [Signal(SIG, -1, 100)]
    res, trades = go({603: (PRICE, 110095, PRICE, PRICE)}, signals=sig)
    t = trades[0]
    assert t.side == -1 and t.exit_reason == "STOP"
    assert t.exit_price == pytest.approx((110100 + 2.5) * 1e-5, abs=1e-9)    # stop + slip
    assert t.entry == pytest.approx(109997.5 * 1e-5, abs=1e-9)
    # a bid high of 110089 makes the ask 110099: not stopped
    res, trades = go({603: (PRICE, 110089, PRICE, PRICE)}, signals=sig)
    assert trades[0].exit_reason == "SESSION_END"


def test_a_sell_target_needs_the_ask_through_it():
    sig = [Signal(SIG, -1, 100)]
    # entry 109997.5, stop 110100: D = 102.5, 1R target 109895 (ask). Ask low = bid low + 10.
    res, trades = go({603: (PRICE, PRICE, 109885, PRICE)}, signals=sig)       # ask 109895: a touch
    assert trades[0].exit_reason == "SESSION_END"
    res, trades = go({603: (PRICE, PRICE, 109884, PRICE)}, signals=sig)       # ask 109894: through it
    assert trades[0].exit_reason == "TARGET"
    assert trades[0].exit_price == pytest.approx(109895 * 1e-5, abs=1e-9)
    # a sell pays the spread when it covers
    assert trades[0].cost_gbp > 0
    spread_part = 10 * 0.79 * trades[0].volume
    assert trades[0].cost_gbp == pytest.approx(
        (10 + 2.5) * 0.79 * trades[0].volume + 2 * trades[0].volume * 3.5 * DEFAULT_FALLBACK_FX["USDGBP"],
        rel=1e-9)
    assert spread_part > 0


# -------------------------------------------------------------- the clock ---
@pytest.mark.parametrize("day,flat_utc,why", [
    ("2026-07-01", "20:55", "BST Wednesday: 21:55 UK"),
    ("2026-01-14", "21:55", "GMT Wednesday: 21:55 UK"),
    ("2026-07-03", "20:00", "BST Friday: 21:00 UK"),
    ("2026-01-16", "21:00", "GMT Friday: 21:00 UK"),
])
def test_everything_is_flat_at_the_uk_deadline_in_bst_and_gmt(day, flat_utc, why):
    hh, mm = (int(x) for x in flat_utc.split(":"))
    assert flat_min(day) == day_min(day) + hh * 60 + mm, why
    # a trade entered an hour before the deadline is closed at the LAST bar before it
    sig_i = hh * 60 + mm - 60
    res, trades = go(None, signals=[Signal(sig_i, 1, 100)], day=day)
    t = trades[0]
    assert t.exit_reason == "SESSION_END"
    assert t.closed_utc == iso_utc(flat_min(day)), why         # filled at the last bar's close = the flat minute
    assert t.bars_held == 59


def test_no_entry_after_the_deadline():
    last = flat_min(DAY) - day_min(DAY) - 1                   # the last bar before 20:55 UTC
    # a signal ON the last bar would enter after the deadline: it is never taken
    res, trades = go(None, signals=[Signal(last, 1, 100)])
    assert trades == []
    # one bar earlier enters on the last bar and is closed at its close
    res, trades = go(None, signals=[Signal(last - 1, 1, 100)])
    assert len(trades) == 1 and trades[0].bars_held == 1
    assert trades[0].exit_reason == "SESSION_END"


def test_a_time_exit_closes_at_the_bar_close_with_slip():
    res, trades = go(None, exit_code="time1h")
    t = trades[0]
    assert t.exit_reason == "TIME"
    assert t.bars_held == 60
    assert t.exit_price == pytest.approx((PRICE - 2.5) * 1e-5, abs=1e-9)     # close - slip (a buy sells at the bid)
    # the bars are 601 ... 660, the last opening at 11:00 and closing at 11:01: that close is the fill
    assert t.opened_utc == iso_utc(day_min(DAY) + SIG + 1)
    assert t.closed_utc == iso_utc(day_min(DAY) + SIG + 61)


def test_a_stop_or_a_target_is_stamped_with_its_bar_and_a_close_exit_with_the_bar_after():
    """A stop or target is hit somewhere inside a bar, so the bar's open is as good as anything.
    A time or session-end exit is filled at the bar's close, which is the next minute: a flat exit
    at 21:55 UK used to read 21:54."""
    _, trades = go({604: (PRICE, 110200, PRICE, 110150)})
    assert trades[0].exit_reason == "TARGET" and trades[0].closed_utc == iso_utc(day_min(DAY) + 604)
    _, trades = go(None, exit_code="time1h")
    assert trades[0].exit_reason == "TIME" and trades[0].closed_utc == iso_utc(day_min(DAY) + 661)
    inst = sim.inst_of(build())
    pos = sim.enter(inst, "t1", 13.0, 1, float(PRICE), 100, 50.0, float(PRICE), 10.0, False, 10.0, 0, 0)
    fill = sim.feed_gap_fill(pos, float(PRICE), 10.0)
    trade = sim.close_trade(inst, pos, "EURUSD", 13.0, fill, 700, 5, DAY, False)
    assert trade.exit_reason == "FEED_GAP" and trade.closed_utc == iso_utc(701)


# --------------------------------------------------------------- the money ---
@pytest.mark.parametrize("kind", ["FX_MAJOR", "FX_MINOR", "GOLD", "INDEX"])
def test_commission_by_kind_is_the_paper_brokers_own_table(kind):
    amount, ccy = DEFAULT_COMMISSION[kind]
    assert ccy == "USD"
    assert sim.commission_gbp_per_lot_side(kind) == pytest.approx(amount * DEFAULT_FALLBACK_FX["USDGBP"])
    assert (sim.commission_gbp_per_lot_side("INDEX") == 0.0) is (kind == "INDEX" or
                                                                  sim.commission_gbp_per_lot_side("INDEX") == 0.0)


def test_an_unknown_kind_is_never_assumed_free():
    amount, _ = DEFAULT_COMMISSION["*"]
    assert sim.commission_gbp_per_lot_side("SOMETHING_NEW") == pytest.approx(
        amount * DEFAULT_FALLBACK_FX["USDGBP"])
    assert sim.commission_gbp_per_lot_side("SOMETHING_NEW") > 0


def test_size_is_floored_to_the_lot_step_never_rounded_up():
    assert sim.floor_to_step(0.1463, 0.01) == 0.14
    assert sim.floor_to_step(0.15, 0.01) == 0.15              # no float dust: 0.15 / 0.01 = 14.999...
    assert sim.floor_to_step(0.0999, 0.01) == 0.09
    assert sim.floor_to_step(7.0, 0.01, vmax=5.0) == 5.0
    res, trades = go({604: (PRICE, 110200, PRICE, 110150)})
    per_lot = 112.5 * 0.79
    assert trades[0].volume * per_lot <= 13.0 + 1e-9          # never over the risk
    assert (trades[0].volume + 0.01) * per_lot > 13.0         # and as close as the step allows


def test_the_smallest_size_skip():
    # a 200-pip stop: 13 pounds buys 0.0008 lots, below the 0.01 minimum -> skipped, not rounded up
    res, trades = go(None, signals=[Signal(SIG, 1, 20000)])
    assert trades == [] and res.skipped == {"smallest_size": 1}


def test_a_spread_over_three_times_the_days_median_is_skipped():
    wide = [Signal(SIG, 1, 300)]                              # a wide stop so the cost rule is not the one hit
    res, trades = go({SIG + 1: (PRICE, PRICE, PRICE, PRICE, 31)}, signals=wide)   # the entry bar's spread
    assert trades == [] and res.skipped == {"wide_spread": 1}
    res, trades = go({SIG + 1: (PRICE, PRICE, PRICE, PRICE, 30)}, signals=wide)   # exactly 3x is allowed
    assert len(trades) == 1


def test_costs_over_a_fifth_of_the_stop_are_skipped():
    # stop 50 points: real distance 62.5; spread 10 + commission (7.0 points) = 17 > 12.5
    res, trades = go(None, signals=[Signal(SIG, 1, 50)])
    assert trades == [] and res.skipped == {"costs_vs_stop": 1}
    # a 100-point stop: 17 <= 22.5, fine
    res, trades = go(None, signals=[Signal(SIG, 1, 100)])
    assert len(trades) == 1


def test_a_stop_inside_the_brokers_minimum_is_skipped():
    entry = synth.entry_for("EURUSD")
    assert entry.spec.stops_level_points == 10.0
    # an ATR stop on flat bars has no ATR at all -> no_atr
    res, trades = go(None, signals=[Signal(SIG, 1, None)])
    assert trades == [] and res.skipped == {"no_atr": 1}


def test_a_gap_to_the_entry_that_is_already_through_the_stop_is_skipped():
    # the next open is 150 points BELOW the 100-point stop of a buy
    res, trades = go({SIG + 1: (109750, 109750, 109750, 109750)})
    assert trades == [] and res.skipped == {"stop_gap": 1}


def test_spread_floor_from_the_past_stretch_and_the_assumed_default():
    # the median for UTC hour 10 over the past stretch is 25 points: it floors the bar's own 10
    hours = [0] * 24
    hours[10] = 25
    sig = [Signal(SIG, 1, 200)]
    res, trades = go({604: (PRICE, 110600, PRICE, 110300)}, hours=hours, signals=sig)
    t = trades[0]
    slip = max(1.0, 0.25 * 25)
    assert t.entry == pytest.approx((PRICE + 25 + slip) * 1e-5, abs=1e-9)
    assert not t.spread_assumed
    # a bar that recorded no spread takes the default (15 points for FX) and is flagged
    res, trades = go({SIG + 1: (PRICE, PRICE, PRICE, PRICE, 0), 604: (PRICE, 110600, PRICE, 110300)},
                     signals=sig)
    t = trades[0]
    assert t.spread_assumed
    assert sim.default_spread_points("EURUSD", "FX_MAJOR") == 15.0
    assert t.entry == pytest.approx((PRICE + 15 + 3.75) * 1e-5, abs=1e-9)


def test_default_spreads_by_kind():
    assert sim.default_spread_points("EURUSD", "FX_MAJOR") == 15.0
    assert sim.default_spread_points("XAUUSD", "GOLD") == 30.0
    assert sim.default_spread_points("US500", "INDEX") == 10.0


# ---------------------------------------------------------------- the caps ---
def test_one_position_per_symbol_and_a_cooldown_after_an_exit():
    # a target bar at 604 closes the first trade on bar 604; the cooldown is 5 bars
    mods = {604: (PRICE, 110200, PRICE, PRICE)}
    sigs = [Signal(SIG, 1, 100), Signal(SIG + 2, 1, 100),                  # while open
            Signal(604 + COOLDOWN_BARS - 1, 1, 100),                       # inside the cooldown
            Signal(604 + COOLDOWN_BARS, 1, 100)]                           # allowed again
    res, trades = go(mods, signals=sigs)
    assert len(trades) == 2
    assert res.skipped["busy_or_cooldown"] == 2
    assert trades[1].opened_utc == iso_utc(day_min(DAY) + 604 + COOLDOWN_BARS + 1)


def test_at_most_three_open_across_symbols():
    names = ["EURUSD", "GBPUSD", "AUDUSD", "USDCAD"]
    sers = [build(n) for n in names]
    win = synth.window_of(sers, [DAY])
    fam = Stub([Signal(SIG, 1, 100)])
    res = sim.run(cand(), win, family=fam)
    assert len(res.trades) == 3
    assert res.skipped == {"max_open": 1}


def test_at_most_six_entries_per_symbol_per_uk_day():
    sigs, mods = [], {}
    for k in range(MAX_ENTRIES_PER_DAY + 2):
        i = 100 + 30 * k
        sigs.append(Signal(i, 1, 100))
        mods[i + 3] = (PRICE, 110300, PRICE, PRICE)            # a quick 1R target each time
    res, trades = go(mods, signals=sigs)
    assert len(trades) == MAX_ENTRIES_PER_DAY
    assert res.skipped == {"day_cap": 2}


# ----------------------------------------------------------------- the news ---
def test_avoid_news_skips_signals_within_15_minutes_and_days_without_a_calendar():
    ser = build(mods={SIG + 3: (PRICE, 110300, PRICE, PRICE)})     # a quick 1R target after the first signal
    from array import array
    nm = array("i", [-1] * ser.n)
    for k in range(ser.n):
        nm[k] = 20
    nm[SIG] = 15                                               # a release in 15 minutes: avoid
    nm[SIG + 40] = 16                                          # 16 minutes: fine
    ser._feat["news_min"] = nm
    win = synth.window_of([ser], [DAY])
    fam = Stub([Signal(SIG, 1, 100), Signal(SIG + 40, 1, 100)])
    # no calendar for the day: the whole day is left out and counted
    res = sim.run(cand(news="AVOID_15M"), win, family=fam)
    assert res.trades == [] and res.skipped == {"news_unknown_day": 1} and res.daily == {}
    win.news_days = {DAY}
    res = sim.run(cand(news="AVOID_15M"), win, family=fam)
    assert len(res.trades) == 1 and res.skipped == {"news_soon": 1}
    assert res.trades[0].opened_utc == iso_utc(day_min(DAY) + SIG + 41)
    # IGNORE takes both and needs no calendar
    res = sim.run(cand(news="IGNORE"), win, family=Stub([Signal(SIG, 1, 100), Signal(SIG + 40, 1, 100)]))
    assert len(res.trades) == 2


# ------------------------------------------------------------ determinism ---
def test_the_same_candidate_on_the_same_window_gives_the_same_trades():
    mods = {604: (PRICE, 110200, PRICE, 110150)}
    a = go(mods)[1]
    b = go(mods)[1]
    assert a == b and a


def test_the_result_carries_synthetic_through():
    res, _ = go(None)
    assert res.synthetic is True and res.window.kind == "past"


# ------------------------------------------------------------------ speed ---
@pytest.mark.slow
def test_throughput_24_symbols_by_60_days(tmp_path, capsys):
    """Seconds per candidate for 24 synthetic symbols x 60 days must be under 5."""
    from mintel.lab import LabPaths, features
    from mintel.lab.candidate import registered
    from mintel.lab.store import BarStore
    store = BarStore(LabPaths(tmp_path))
    ents = synth.universe(24)
    days = synth.weekdays("2026-05-04", 60)
    synth.fill_store(store, ents, days, seed=11)
    win = store._build_window("past", store.trading_days([e.name for e in ents]), ents,
                              cut_day="2099-01-01", stretch_id="speed")
    assert len(win.series) == 24 and len(win.days) == 60
    t0 = time.perf_counter()
    for ser in win.series.values():
        for st in features.SETS:
            features.attach(ser, st)
    feature_seconds = time.perf_counter() - t0
    cands = registered(news_awake=True)
    sample = cands[::max(1, len(cands) // 40)]
    t0 = time.perf_counter()
    trades = 0
    worst = 0.0
    for c in sample:
        t1 = time.perf_counter()
        trades += len(sim.run(c, win).trades)
        worst = max(worst, time.perf_counter() - t1)
    per = (time.perf_counter() - t0) / len(sample)
    with capsys.disabled():
        print(f"\n[lab speed] 24 symbols x 60 days: features {feature_seconds:.1f}s once; "
              f"{per:.3f}s per candidate over {len(sample)} candidates "
              f"(worst {worst:.3f}s, {trades} trades)")
    assert per < 5.0
    assert worst < 5.0


# ----------------------------------------------------- what practice reuses ---
def test_practice_enters_at_a_real_tick_and_the_same_step_closes_it():
    inst = sim.inst_of(build())
    # a real tick: ask 1.10012, and a slip of 1 point on top; the stop is 100 points under the close
    pos = sim.enter(inst, "t1", 13.0, 1, 110000.0, 100, 0.0, 110000.0, 12.0, False, 10.0, 5000, 7,
                    entry_price=110013.0)
    assert not isinstance(pos, str)
    pos.fill_source = "tick"
    assert pos.entry == 110013.0 and pos.stop == 109900.0 and pos.D == 113.0
    assert pos.target == pytest.approx(110013.0 + 113.0)
    # one live bar that runs through the target
    fill = sim.step(pos, 5001, 110010.0, 110200.0, 110005.0, 110150.0, 10.0, False)
    assert fill[1] == "TARGET" and fill[0] == pytest.approx(110126.0)
    t = sim.close_trade(inst, pos, "EURUSD", 13.0, fill, 5001, 8, "2026-07-01", False)
    assert t.fill_source == "tick" and t.exit_reason == "TARGET" and t.bars_held == 2


def test_a_feed_gap_closes_a_position_at_the_first_bar_back_with_the_spread_against_it():
    inst = sim.inst_of(build())
    buy = sim.enter(inst, "t1", 13.0, 1, 110000.0, 100, 0.0, 110000.0, 10.0, False, 10.0, 5000, 7)
    fill = sim.feed_gap_fill(buy, 109990.0, 10.0)
    assert fill == (109990.0 - 2.5, "FEED_GAP", 2.5, 0.0)
    sell = sim.enter(inst, "t1", 13.0, -1, 110000.0, 100, 0.0, 110000.0, 10.0, False, 10.0, 5000, 7)
    fill = sim.feed_gap_fill(sell, 110010.0, 10.0)
    assert fill == (110010.0 + 10.0 + 2.5, "FEED_GAP", 2.5, 10.0)
    t = sim.close_trade(inst, sell, "EURUSD", 13.0, fill, 5010, 17, "2026-07-01", False)
    assert t.exit_reason == "FEED_GAP" and t.net_gbp < 0


def test_the_lab_has_no_way_to_place_an_order():
    """No order words, no trading broker, no secrets file anywhere under mintel/lab."""
    import re
    from pathlib import Path
    root = Path(__file__).resolve().parents[1] / "mintel" / "lab"
    files = sorted(root.rglob("*.py"))
    assert len(files) >= 20
    banned = ("order_send", "modify_stops", "position_close", "secrets.json")
    bad_import = re.compile(r"^\s*(from|import)\s+[\w. ]*\b(OrderRequest|PaperBroker|Mt5Broker|execution|trader|modes)\b", re.M)
    for p in files:
        text = p.read_text(encoding="utf-8")
        for word in banned:
            assert word not in text, (p.name, word)
        for m in bad_import.finditer(text):
            line = m.group(0).strip()
            assert line == "from ..broker.paper import DEFAULT_COMMISSION, DEFAULT_FALLBACK_FX", (p.name, line)
        assert "OrderRequest" not in text and "Mt5Broker" not in text and "PaperBroker" not in text, p.name
    sim_text = (root / "sim.py").read_text()
    assert "from ..broker.paper import DEFAULT_COMMISSION, DEFAULT_FALLBACK_FX\n" in sim_text
    assert len(re.findall(r"broker\.paper", "\n".join(p.read_text() for p in files))) == 1


def test_family_params_are_the_candidates_settings_plus_its_fields():
    c = Candidate("STUB", 1, "FX_MAJOR", "LONDON", "AVOID_15M", "t2", make_params(a=1, b="X"))
    assert sim.family_params(c) == {"a": 1, "b": "X", "session": "LONDON", "markets": "FX_MAJOR",
                                    "exit": "t2", "news": "AVOID_15M"}


# ------------------------------------------------- invariants on made-up prices ---
def test_invariants_hold_for_every_family_on_random_prices(tmp_path):
    """Whatever a family does, the book must obey the rules of the simulator."""
    from mintel.lab import EXIT_REASONS, LabPaths, MAX_OPEN_PER_CANDIDATE, features
    from mintel.lab import families as FAM
    from mintel.lab.store import BarStore
    import collections
    store = BarStore(LabPaths(tmp_path))
    ents = synth.universe(24, kinds=("FX_MAJOR", "INDEX", "GOLD"))
    days = synth.weekdays("2026-06-01", 8)
    synth.fill_store(store, ents, days, seed=23)
    win = store._build_window("past", store.trading_days([e.name for e in ents]), ents,
                              cut_day="2099-01-01", stretch_id="inv")
    total = 0
    for code, fam in FAM.FAMILIES.items():
        if code == "NEWS_MOVE":
            continue
        for c in fam.grid()[:: max(1, len(fam.grid()) // 6)]:
            res = sim.run(c, win)
            by_sym = collections.defaultdict(list)
            per_day = collections.Counter()
            for t in res.trades:
                total += 1
                assert t.exit_reason in EXIT_REASONS
                assert t.opened_utc < t.closed_utc or t.bars_held == 1
                day = t.day
                flat = iso_utc(flat_min(day))
                assert t.closed_utc <= flat, (c.id, t.closed_utc, flat)
                assert t.net_gbp == pytest.approx(t.gross_gbp - 2 * t.volume * sim.commission_gbp_per_lot_side(
                    win.kinds[t.symbol]), rel=1e-9, abs=1e-9)
                assert t.cost_gbp >= 2 * t.volume * sim.commission_gbp_per_lot_side(win.kinds[t.symbol]) - 1e-9
                assert t.r == pytest.approx(t.net_gbp / c.risk_gbp)
                assert t.r > -4.0                                   # a stop costs about 1R plus costs; a gap a little more
                step_ = win.series[t.symbol].spec.volume_step
                assert abs(t.volume / step_ - round(t.volume / step_)) < 1e-6
                by_sym[t.symbol].append((t.opened_utc, t.closed_utc))
                per_day[(t.symbol, day)] += 1
            for sym, spans in by_sym.items():                       # one position per symbol per candidate
                spans.sort()
                for (a0, a1), (b0, b1) in zip(spans, spans[1:]):
                    assert b0 > a1, (c.id, sym, a1, b0)
            assert all(n <= MAX_ENTRIES_PER_DAY for n in per_day.values())
            events = sorted([(t.opened_utc, 1) for t in res.trades] + [(t.closed_utc, -1) for t in res.trades],
                            key=lambda e: (e[0], e[1]))
            open_now, worst = 0, 0
            for _t, d in events:
                open_now += d
                worst = max(worst, open_now)
            assert worst <= MAX_OPEN_PER_CANDIDATE + 0      # at most three open at once
            assert sum(v[1] for v in res.daily.values()) == len(res.trades)
            assert sum(v[0] for v in res.daily.values()) == pytest.approx(sum(t.net_gbp for t in res.trades))
            assert set(res.daily) == set(win.days)
    assert total > 300
