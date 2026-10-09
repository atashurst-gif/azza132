"""Financial Ian: the order-flow trail rides strong flow, tightens as it fades, exits when the thesis
dies - and the stop NEVER widens, over thousands of random price and flow paths."""
import random

import pytest

from mintel.broker.base import Side
from mintel.ian.trail import FlowRead, FlowTrail, TrailConfig, tighten_only


def random_flow(rng: random.Random) -> FlowRead:
    return FlowRead(support=rng.uniform(-1, 1), imbalance_with=rng.uniform(-1, 1),
                    opposite_absorption=rng.random() ** 2, regeneration_against=rng.random() ** 2,
                    thesis_dead=rng.random() < 0.002, available=rng.random() > 0.1)


class TestNeverWidens:
    def test_thousands_of_random_paths(self):
        rng = random.Random(20261008)
        paths = updates = moves = 0
        for _ in range(4000):
            side = Side.BUY if rng.random() < 0.5 else Side.SELL
            entry = 1.08 + rng.uniform(-0.02, 0.02)
            risk = rng.uniform(0.0003, 0.003)
            tr = FlowTrail(side, entry, entry - side.sign * risk, TrailConfig(min_step_points=rng.choice([0.0, 1.0])),
                           point=0.00001)
            price = entry
            stops = [tr.stop]
            vol = risk * rng.uniform(0.02, 0.3)
            for _ in range(rng.randint(20, 120)):
                price += rng.gauss(rng.uniform(-0.2, 0.2) * vol, vol)
                d = tr.update(price, random_flow(rng), min_distance=rng.choice([0.0, 0.00005, 0.0005]))
                updates += 1
                moves += d.moved
                if (price - tr.stop) * side.sign <= 0:                 # the stop is hit
                    break
                stops.append(tr.stop)                                 # an exit decision never moves the stop: go on
            for a, b in zip(stops, stops[1:]):
                if side is Side.BUY:
                    assert b >= a - 1e-15, f"a long's stop fell from {a} to {b}"
                else:
                    assert b <= a + 1e-15, f"a short's stop rose from {a} to {b}"
            assert (tr.stop - tr.initial_stop) * side.sign >= -1e-15
            paths += 1
        assert paths == 4000 and updates >= 50_000 and moves >= 5_000

    def test_tighten_only(self):
        assert tighten_only(Side.BUY, 1.0800, 1.0790) == 1.0800
        assert tighten_only(Side.BUY, 1.0800, 1.0810) == 1.0810
        assert tighten_only(Side.SELL, 1.0800, 1.0810) == 1.0800
        assert tighten_only(Side.SELL, 1.0800, 1.0790) == 1.0790
        assert tighten_only(Side.BUY, 1.0800, None) == 1.0800

    def test_the_initial_stop_must_be_on_the_losing_side(self):
        with pytest.raises(ValueError):
            FlowTrail(Side.BUY, 1.0850, 1.0860)
        with pytest.raises(ValueError):
            FlowTrail(Side.SELL, 1.0850, 1.0840)


class TestBehaviour:
    def _ride(self, flow: FlowRead, side=Side.BUY):
        tr = FlowTrail(side, 1.0850, 1.0850 - side.sign * 0.0010, TrailConfig(min_step_points=0.0), point=0.00001)
        for k in range(1, 31):                                # the price runs 3 R in the trade's favour
            tr.update(1.0850 + side.sign * 0.0001 * k, flow)
        return tr

    def test_strong_flow_gets_room_and_fading_flow_gets_protected(self):
        strong = self._ride(FlowRead(support=0.8, imbalance_with=0.5))
        weak = self._ride(FlowRead(support=0.1, imbalance_with=0.1))
        reversing = self._ride(FlowRead(support=0.1, imbalance_with=-0.5))
        hostile = self._ride(FlowRead(support=0.1, imbalance_with=0.0, opposite_absorption=0.6))
        assert strong.stop == pytest.approx(1.0880 - 1.0 * 0.0010)          # 1 R behind the best price
        assert weak.stop == pytest.approx(1.0880 - 0.7 * 0.0010)
        assert reversing.stop == pytest.approx(1.0880 - 0.45 * 0.0010)
        assert hostile.stop == pytest.approx(1.0880 - 0.30 * 0.0010)
        assert strong.stop < weak.stop < reversing.stop < hostile.stop

    def test_no_fixed_target(self):
        tr = self._ride(FlowRead(support=0.9, imbalance_with=0.6))
        d = tr.update(1.0950, FlowRead(support=0.9, imbalance_with=0.6))     # 10 R: still riding
        assert not d.exit and tr.stop == pytest.approx(1.0940)

    def test_costs_are_locked_in_after_one_r(self):
        tr = FlowTrail(Side.SELL, 1.0850, 1.0860, TrailConfig(min_step_points=0.0), point=0.00001)
        tr.update(1.0839, FlowRead(available=False))                         # 1.1 R
        assert tr.stop <= 1.0850 - 0.1 * 0.0010 + 1e-12

    def test_the_thesis_dying_is_an_exit(self):
        tr = FlowTrail(Side.BUY, 1.0850, 1.0840)
        assert tr.update(1.0852, FlowRead(support=-0.7, imbalance_with=-0.5)).exit
        tr2 = FlowTrail(Side.BUY, 1.0850, 1.0840)
        assert tr2.update(1.0852, FlowRead(support=0.2, opposite_absorption=0.85)).exit
        tr3 = FlowTrail(Side.BUY, 1.0850, 1.0840)
        d = tr3.update(1.0852, FlowRead(thesis_dead=True, note="sellers now have the edge"))
        assert d.exit and "thesis" in d.reason
        tr4 = FlowTrail(Side.BUY, 1.0850, 1.0840)
        assert not tr4.update(1.0852, FlowRead(support=-0.7, imbalance_with=-0.5, available=False)).exit   # blind: no flow exit

    def test_a_stop_the_broker_would_refuse_is_never_sent(self):
        tr = FlowTrail(Side.BUY, 1.0850, 1.0840, TrailConfig(min_step_points=0.0), point=0.00001)
        tr.update(1.0870, FlowRead(support=0.0, imbalance_with=0.0, opposite_absorption=0.6))
        # 0.3 R behind 1.0870 would be 1.0867; the broker wants 5 pips: the stop stays 5 pips away
        tr2 = FlowTrail(Side.BUY, 1.0850, 1.0840, TrailConfig(min_step_points=0.0), point=0.00001)
        d = tr2.update(1.0870, FlowRead(support=0.0, opposite_absorption=0.6), min_distance=0.0005)
        assert tr.stop == pytest.approx(1.0867) and tr2.stop == pytest.approx(1.0865) and d.moved

    def test_mfe_and_mae(self):
        tr = FlowTrail(Side.BUY, 1.0850, 1.0840)
        for p in (1.0845, 1.0862, 1.0855):
            tr.update(p, FlowRead(available=False))
        assert tr.mfe_r == pytest.approx(1.2) and tr.mae_r == pytest.approx(-0.5)
