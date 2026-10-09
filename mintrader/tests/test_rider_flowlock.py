"""FlowLock X: RULE 1 (NEVER WIDEN) over thousands of random price paths,
and one test per state. All paths are SYNTHETIC and seeded."""
import random

import pytest

from mintel.rider.flowlock_x import (ATR_CHANDELIER, COSTS_COVERED, CURVES, EXHAUSTION, INITIAL, MOMENTUM_DECAY,
                                     PROFIT_RATCHET, PROVING, RUNNER, SWING_TRAIL, THESIS_DEAD, FlowInputs,
                                     FlowLockParams, FlowLockX, GivebackCurve, never_widen)

PIP = 0.0001


def random_inputs(rng, t, bid, ask, atr, lean=0.0):
    """Random market context so every branch (runner, decay, exhaustion,
    swing, ema, psar) is exercised on the property paths. ``lean`` tilts the
    velocity the trade's way on some paths so they live long enough to trail."""
    mid = (bid + ask) / 2
    x = FlowInputs(t, bid, ask, atr=atr)
    broken = rng.random() < 0.03                              # now and then a swing the price has already broken
    if rng.random() < 0.6:
        x.swing_low = mid - (rng.uniform(-1.0, 0.0) if broken else rng.uniform(0.3, 3.0)) * atr
    if rng.random() < 0.6:
        x.swing_high = mid + (rng.uniform(-1.0, 0.0) if broken else rng.uniform(0.3, 3.0)) * atr
    x.velocity = rng.gauss(lean, 1.5)
    x.accel = rng.gauss(0, 2.0)
    x.efficiency = rng.random()
    x.efficiency_dir = rng.choice((-1, 0, 1))
    x.adx_change = rng.gauss(0, 2)
    if rng.random() < 0.7:
        x.ema_fast = mid + rng.gauss(0, 1.5) * atr
        x.ema_slope = rng.gauss(0, 0.2)
    x.extension_atr = rng.gauss(0, 3.0)
    x.opposing_candles = rng.random()
    x.cross_confirm = rng.choice((None, True, False))
    x.tick_rate_ratio = rng.uniform(0.0, 2.0)
    x.failed_breakout = rng.random() < 0.02
    x.min_stop_distance = rng.choice((0.0, 0.5 * PIP, 2 * PIP))
    return x


class TestNeverWiden:
    def test_the_one_rule(self):
        assert never_widen(1, 1.1000, 1.0990) == 1.1000          # a long's stop never falls
        assert never_widen(1, 1.1000, 1.1010) == 1.1010
        assert never_widen(-1, 1.1000, 1.1010) == 1.1000         # a short's never rises
        assert never_widen(-1, 1.1000, 1.0990) == 1.0990
        assert never_widen(1, 1.1000, None) == 1.1000
        assert never_widen(1, 1.1000, float("nan")) == 1.1000

    def test_commit_refuses_a_wider_stop(self):
        fl = FlowLockX()
        st = fl.start(1, 1.1000, 1.0990, 0.00008, 0.0)
        assert not fl.commit(st, 1.0980) and st.stop == 1.0990
        assert fl.commit(st, 1.0995) and st.stop == 1.0995
        st2 = fl.start(-1, 1.1000, 1.1010, 0.00008, 0.0)
        assert not fl.commit(st2, 1.1020) and st2.stop == 1.1010


@pytest.mark.parametrize("params", [FlowLockParams(), FlowLockParams(curve="tight", use_psar=True),
                                    FlowLockParams(curve="loose", runner_chandelier_atr=4.0)])
def test_property_stop_never_widens_and_stays_on_the_right_side(params):
    """5,000 seeded random paths (long and short, random spreads, gaps,
    random broker refusals): the committed stop never moves against the
    position, and every stop proposed or held is on the correct side of the
    market. Data source: SYNTHETIC random walks."""
    rng = random.Random(20261008 + int(params.use_psar) + int(params.runner_chandelier_atr))
    n_paths = 5000 if params == FlowLockParams() else 1500
    exits = moves = 0
    states_seen = set()
    for _ in range(n_paths):
        fl = FlowLockX(params)
        side = rng.choice((1, -1))
        spread = rng.uniform(0.1, 2.0) * PIP
        mid = 1.1000 + rng.uniform(-0.01, 0.01)
        atr = rng.uniform(0.5, 4.0) * PIP
        risk = rng.uniform(3.0, 25.0) * PIP
        entry = mid + side * spread / 2
        stop = entry - side * risk
        t = 0.0
        trig = entry - side * rng.uniform(0, 3) * PIP if rng.random() < 0.5 else 0.0
        st = fl.start(side, entry, stop, rng.uniform(0.3, 2.0) * PIP, t, trigger_level=trig,
                      entry_efficiency=rng.random(), entry_velocity=rng.uniform(0, 3))
        friendly = rng.random() < 0.6
        drift = (side * abs(rng.gauss(0.3, 0.3)) if friendly else rng.gauss(0, 0.4)) * PIP
        lean = side * rng.uniform(0.5, 3.0) if friendly else 0.0
        vol = rng.uniform(0.3, 3.0) * PIP
        for _step in range(rng.randint(40, 160)):
            t += rng.uniform(0.2, 3.0)
            mid += drift + rng.gauss(0, vol)
            if rng.random() < 0.02:
                mid += rng.choice((-1, 1)) * rng.uniform(5, 40) * PIP        # a gap
            sp = spread * (rng.uniform(1, 8) if rng.random() < 0.05 else 1.0)
            bid, ask = mid - sp / 2, mid + sp / 2
            before = st.stop
            x = random_inputs(rng, t, bid, ask, atr, lean)
            d = fl.update(st, x)
            states_seen.add(d.state)
            assert st.stop == before                          # update() never moves the stop itself
            if d.exit:
                assert d.new_stop is None
                exits += 1
                break
            # not exited: the held stop is on the correct side of the market
            assert (st.stop < bid) if side > 0 else (st.stop > ask)
            if d.new_stop is not None:
                assert (d.new_stop - before) * side > 0       # only ever tighter
                assert (d.new_stop < bid) if side > 0 else (d.new_stop > ask)
                if rng.random() < 0.85:                       # the broker accepts most moves
                    fl.commit(st, d.new_stop)
                    moves += 1
            if rng.random() < 0.05:                           # a buggy caller tries to widen it
                fl.commit(st, st.stop - side * rng.uniform(0.1, 10) * PIP)
            assert (st.stop - before) * side >= 0             # NEVER WIDEN
    assert exits > n_paths * 0.3 and moves > n_paths          # the paths really exercised the machine
    assert {INITIAL, PROVING, COSTS_COVERED, RUNNER, PROFIT_RATCHET, SWING_TRAIL, ATR_CHANDELIER, MOMENTUM_DECAY,
            EXHAUSTION, THESIS_DEAD}.issubset(states_seen)


# ------------------------------------------------------------- per state --

def long_trade(params=None, cost=0.5 * PIP, trigger=0.0):
    fl = FlowLockX(params or FlowLockParams())
    st = fl.start(1, 1.10000, 1.09900, cost, 0.0, trigger_level=trigger, entry_efficiency=0.6, entry_velocity=2.0)
    return fl, st                                             # R = 10 pips


def at(t, bid, spread=0.1 * PIP, **kw):
    return FlowInputs(t, bid, bid + spread, **kw)


class TestStates:
    def test_initial_keeps_the_structural_stop(self):
        fl, st = long_trade()
        d = fl.update(st, at(1, 1.10020, atr=2 * PIP, velocity=1.0, efficiency=0.6))
        assert d.state == INITIAL and d.new_stop is None and not d.exit

    def test_proving_cuts_the_loss_without_strangling(self):
        fl, st = long_trade()
        d = fl.update(st, at(5, 1.10050, atr=2 * PIP, velocity=1.0, efficiency=0.6))      # +5 pips = 0.5 R
        assert d.state == PROVING
        # keep 0.6 R of risk (1.0994) unless that is closer than 1 ATR to the price
        assert d.new_stop == pytest.approx(min(1.10000 - 0.6 * 0.0010, 1.10050 - 2 * PIP))
        fl.commit(st, d.new_stop)
        d2 = fl.update(st, at(6, 1.10052, atr=20 * PIP, velocity=1.0, efficiency=0.6))  # huge ATR: room wins
        assert d2.new_stop is None and st.stop == pytest.approx(1.0994)

    def test_costs_covered_is_true_net_breakeven(self):
        fl, st = long_trade(cost=0.8 * PIP)
        d = fl.update(st, at(10, 1.10090, atr=20 * PIP, velocity=1.0, efficiency=0.6))  # 0.9 R, chandelier far away
        assert st.costs_covered and d.state == COSTS_COVERED
        assert d.new_stop == pytest.approx(1.10000 + 0.8 * PIP)                          # entry + commission + slippage

    def test_runner_gets_more_room(self):
        fl, st = long_trade()
        fl.update(st, at(10, 1.10100, atr=2 * PIP, velocity=1.0, efficiency=0.6))
        x = at(20, 1.10300, atr=2 * PIP, velocity=3.0, efficiency=0.8, efficiency_dir=1)  # +30 pips, fast, fresh high
        d = fl.update(st, x)
        assert d.state == RUNNER
        normal = 1.10300 - FlowLockParams().chandelier_atr * 2 * PIP
        assert d.new_stop < normal                            # looser than the normal trail ...
        assert d.new_stop >= 1.10000 + 0.5 * PIP              # ... but never below costs covered

    def test_profit_ratchet_locks_a_share_of_the_mfe(self):
        fl, st = long_trade()
        x = at(30, 1.10200, atr=30 * PIP, velocity=0.8, efficiency=0.5)                   # +20 pips = 2 R
        d = fl.update(st, x)
        assert d.state == PROFIT_RATCHET
        g = CURVES["balanced"].giveback(2.0, st.decay)
        assert d.new_stop == pytest.approx(1.10200 - g * 0.0020)

    def test_swing_trail_follows_the_last_micro_swing(self):
        fl, st = long_trade()
        x = at(30, 1.10110, atr=5 * PIP, velocity=0.8, efficiency=0.5, swing_low=1.10100)  # 1.1 R, swing well up
        d = fl.update(st, x)
        assert d.state == SWING_TRAIL
        assert d.new_stop == pytest.approx(1.10100 - FlowLockParams().swing_buffer_atr * 5 * PIP)

    def test_atr_chandelier(self):
        fl, st = long_trade()
        x = at(30, 1.10100, atr=1 * PIP, velocity=0.8, efficiency=0.5)                   # 1 R, tight ATR
        d = fl.update(st, x)
        assert d.state == ATR_CHANDELIER
        assert d.new_stop == pytest.approx(1.10100 - 2 * 1 * PIP)

    def test_momentum_decay_tightens_the_trail(self):
        fl, st = long_trade()
        fl.update(st, at(10, 1.10150, atr=3 * PIP, velocity=2.0, efficiency=0.6))         # the high at t=10
        x = at(90, 1.10140, atr=3 * PIP, velocity=0.0, efficiency=0.3, tick_rate_ratio=0.5)
        d = fl.update(st, x)
        assert d.state == MOMENTUM_DECAY and 0.5 <= d.decay < 0.85
        assert d.new_stop > 1.10150 - 2.0 * 3 * PIP          # tighter than the normal chandelier

    def test_exhaustion_protects_hard(self):
        fl, st = long_trade()
        fl.update(st, at(10, 1.10300, atr=3 * PIP, velocity=3.0, efficiency=0.8))
        x = at(80, 1.10290, atr=3 * PIP, velocity=0.5, efficiency=0.5, extension_atr=6.0, tick_rate_ratio=0.8)
        d = fl.update(st, x)
        assert d.state == EXHAUSTION
        assert d.new_stop >= 1.10000 + 0.75 * 0.0030 - 1e-9   # keeps at least 75% of the MFE

    def test_thesis_dead_when_the_breakout_fails(self):
        fl, st = long_trade(trigger=1.10010)
        d = fl.update(st, at(8, 1.09995, atr=2 * PIP, velocity=-0.5, efficiency=0.4))     # back through the level
        assert d.exit and d.exit_reason == "THESIS_DEAD" and d.state == THESIS_DEAD

    def test_thesis_dead_on_a_hard_turn_against(self):
        fl, st = long_trade()
        d = fl.update(st, at(5, 1.09980, atr=2 * PIP, velocity=-2.5, efficiency=0.6))
        assert d.exit and d.exit_reason == "THESIS_DEAD"

    def test_thesis_dead_when_it_never_gets_going(self):
        fl, st = long_trade()
        d = fl.update(st, at(200 - 40, 1.09995, atr=2 * PIP, velocity=-0.1, efficiency=0.4))
        assert d.exit and "never got going" in d.why

    def test_a_failed_breakout_after_costs_are_covered_counts_as_decay(self):
        fl, st = long_trade()
        fl.update(st, at(10, 1.10200, atr=3 * PIP, velocity=3.0, efficiency=0.8))
        assert fl.decay_score(st, at(11, 1.10190, atr=3 * PIP, velocity=2.0, failed_breakout=True)) >= 0.9

    def test_momentum_died_banks_the_move(self):
        fl, st = long_trade()
        fl.update(st, at(10, 1.10200, atr=3 * PIP, velocity=3.0, efficiency=0.8))
        x = at(120, 1.10180, atr=3 * PIP, velocity=-0.5, accel=-3.0, efficiency=0.1, efficiency_dir=-1,
               adx_change=-3.0, ema_fast=1.10170, ema_slope=0.0, opposing_candles=0.8, cross_confirm=False,
               tick_rate_ratio=0.2)
        d = fl.update(st, x)
        assert d.exit and d.exit_reason == "MOMENTUM_DIED" and d.decay >= 0.85

    def test_stop_reached(self):
        fl, st = long_trade()
        d = fl.update(st, at(3, 1.09899, atr=2 * PIP))
        assert d.exit and d.exit_reason == "STOP"

    def test_a_short_mirrors_a_long(self):
        fl = FlowLockX()
        st = fl.start(-1, 1.10000, 1.10100, 0.5 * PIP, 0.0, entry_efficiency=0.6, entry_velocity=2.0)
        d = fl.update(st, FlowInputs(5, 1.09940, 1.09950, atr=2 * PIP, velocity=-1.0, efficiency=0.6))
        assert d.state == PROVING and d.new_stop is not None and d.new_stop < 1.10100 and d.new_stop > 1.09950


class TestCurves:
    def test_early_gives_more_room_than_mature_and_decay_tightens(self):
        c = GivebackCurve()
        assert c.giveback(0.5, 0.0) > c.giveback(2.0, 0.0) > c.giveback(4.0, 0.0)
        assert c.giveback(4.0, 1.0) == pytest.approx(c.decelerating)
        assert c.giveback(0.5, 0.5) < c.giveback(0.5, 0.0)

    def test_curves_are_a_parameter_research_can_compare(self):
        assert {"balanced", "loose", "tight"} <= set(CURVES)
        a, b = FlowLockX(FlowLockParams(curve="loose")), FlowLockX(FlowLockParams(curve="tight"))
        sa = a.start(1, 1.1, 1.099, 0.00005, 0.0)
        sb = b.start(1, 1.1, 1.099, 0.00005, 0.0)
        x = at(30, 1.10200, atr=30 * PIP, velocity=0.8, efficiency=0.5)
        da, db = a.update(sa, x), b.update(sb, x)
        assert da.new_stop < db.new_stop                      # the tight curve locks more

    def test_params_from_dict_keep_defaults_for_bad_entries(self):
        p = FlowLockParams.from_dict({"curve": "tight", "chandelier_atr": 2.5, "proving_r": "x", "use_psar": 1,
                                      "nonsense": 4})
        assert p.curve == "tight" and p.chandelier_atr == 2.5 and p.proving_r == 0.4 and p.use_psar is False
