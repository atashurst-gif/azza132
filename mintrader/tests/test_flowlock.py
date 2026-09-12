"""FlowLock: cut failures, let winners run, lock profit as moves die."""
from __future__ import annotations

import datetime as dt

import pytest

from mintel.broker.base import Bar, Position, Side
from mintel.config import FlowLockConfig
from mintel.engine.flowlock import FlowLock, FlowState

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 3, 10, 13, 0, tzinfo=UTC)
ATR = 0.0010


class Mom:
    """Minimal momentum stand-in with the fields FlowLock consults."""

    def __init__(self, direction=1, efficiency=0.5, adx=30.0, accel=0.1):
        self.direction = direction
        self.efficiency = efficiency
        self.adx = adx
        self.acceleration_atr = accel


def pos(side=Side.BUY, entry=1.1000, sl=1.0980, ticket=1) -> Position:
    return Position(ticket, "EURUSD", side, 1.0, entry, sl, 0.0, T0)


def bar(price, t=T0) -> Bar:
    return Bar(t, price, price + 0.00005, price - 0.00005, price, 300.0)


def drive(fl: FlowLock, p: Position, prices, momentum=None, thesis=70.0):
    """Feed a price path and record what FlowLock did."""
    out = []
    for i, px in enumerate(prices):
        m = momentum(i) if callable(momentum) else momentum
        d = fl.update(p, price=px, atr=ATR, bar=bar(px, T0 + dt.timedelta(minutes=i)),
                      momentum=m, thesis_strength=thesis,
                      now=T0 + dt.timedelta(minutes=i))
        if d.new_stop:
            p.sl = d.new_stop
        out.append((px, d))
    return out


class TestStopNeverWidens:
    def test_long_stop_only_rises(self):
        fl, p = FlowLock(), pos()
        prices = [1.1000 + 0.0003 * i for i in range(20)] + \
                 [1.1057 - 0.0004 * i for i in range(15)]
        stops = [p.sl]
        for px, d in drive(fl, p, prices, Mom()):
            stops.append(p.sl)
        assert all(stops[i] >= stops[i - 1] for i in range(1, len(stops)))

    def test_short_stop_only_falls(self):
        fl, p = FlowLock(), pos(side=Side.SELL, sl=1.1020)
        prices = [1.1000 - 0.0003 * i for i in range(20)] + \
                 [1.0943 + 0.0004 * i for i in range(15)]
        stops = [p.sl]
        for px, d in drive(fl, p, prices, Mom(direction=-1, accel=-0.1)):
            stops.append(p.sl)
        assert all(stops[i] <= stops[i - 1] for i in range(1, len(stops)))

    def test_monotonic_guard_rejects_a_looser_stop_directly(self):
        fl = FlowLock()
        p = pos()
        t = fl.adopt(p, 1.0980)
        t.stop = 1.0995
        assert fl._monotonic(t, 1.0990) is None
        assert fl._monotonic(t, 1.0999) == pytest.approx(1.0999)

    def test_volatile_chop_cannot_loosen_the_stop(self):
        fl, p = FlowLock(), pos()
        prices = []
        px = 1.1000
        for i in range(60):
            px += 0.0012 if i % 2 == 0 else -0.0010
            prices.append(px)
        stops = [p.sl]
        for _px, d in drive(fl, p, prices, Mom()):
            stops.append(p.sl)
        assert all(stops[i] >= stops[i - 1] for i in range(1, len(stops)))


class TestStates:
    def test_initial_leaves_the_structural_stop_alone(self):
        fl, p = FlowLock(), pos()
        out = drive(fl, p, [1.1002, 1.1003], Mom())
        assert out[0][1].state is FlowState.INITIAL
        assert out[0][1].new_stop is None
        assert p.sl == pytest.approx(1.0980)

    def test_progression_to_strong_flow_then_decay(self):
        fl, p = FlowLock(), pos()
        rising = [1.1000 + 0.0004 * i for i in range(16)]
        stalling = [rising[-1] - 0.00002 * i for i in range(20)]
        states = [d.state for _px, d in drive(fl, p, rising + stalling, Mom())]
        assert FlowState.PROVING in states
        assert FlowState.STRONG_FLOW in states
        assert states[-1] in (FlowState.DECAY, FlowState.NORMAL_FLOW)

    def test_strong_flow_keeps_more_room_than_decay(self):
        """The central FlowLock behaviour: do not strangle an exceptional move."""
        prices = [1.1000 + 0.0004 * i for i in range(16)]
        strong = FlowLock()
        p1 = pos(ticket=1)
        drive(strong, p1, prices, Mom(efficiency=0.7, adx=40, accel=0.3))
        weak = FlowLock()
        p2 = pos(ticket=2)
        drive(weak, p2, prices,
              Mom(efficiency=0.05, adx=8, accel=-0.5))
        t1 = strong.trackers[1]
        t2 = weak.trackers[2]
        assert t1.state is FlowState.STRONG_FLOW
        room_strong = prices[-1] - p1.sl
        room_weak = prices[-1] - p2.sl
        assert room_strong > room_weak

    def test_broken_thesis_closes_the_trade(self):
        fl, p = FlowLock(), pos()
        d = fl.update(p, price=1.1005, atr=ATR, thesis_strength=10.0, now=T0)
        assert d.state is FlowState.REVERSAL
        assert d.close and "thesis" in d.close_reason

    def test_structure_breaking_against_an_unprofitable_trade_closes_it(self):
        class St:
            bos_down, bos_up, bos_bars_ago = True, False, 1
            last_swing_low = last_swing_high = None
        fl, p = FlowLock(), pos()
        d = fl.update(p, price=1.1001, atr=ATR, structure=St(),
                      thesis_strength=60.0, now=T0)
        assert d.close

    def test_structure_break_does_not_close_a_big_winner(self):
        class St:
            bos_down, bos_up, bos_bars_ago = True, False, 1
            last_swing_low = last_swing_high = None
        fl, p = FlowLock(), pos()
        drive(fl, p, [1.1000 + 0.0004 * i for i in range(16)], Mom())
        d = fl.update(p, price=1.1060, atr=ATR, structure=St(),
                      momentum=Mom(), thesis_strength=60.0,
                      now=T0 + dt.timedelta(minutes=30))
        assert not d.close


class TestExcursions:
    def test_mfe_and_mae_are_tracked_from_bar_extremes(self):
        fl, p = FlowLock(), pos()
        fl.update(p, price=1.1000, atr=ATR,
                  bar=Bar(T0, 1.1000, 1.1030, 1.0990, 1.1000), now=T0)
        t = fl.trackers[1]
        assert t.mfe == pytest.approx(0.0030)
        assert t.mae == pytest.approx(0.0010)

    def test_r_multiples_use_the_initial_risk(self):
        fl, p = FlowLock(), pos(entry=1.1000, sl=1.0980)   # 20 pip risk
        fl.update(p, price=1.1040, atr=ATR, bar=bar(1.1040), now=T0)
        t = fl.trackers[1]
        assert t.mfe_r == pytest.approx(2.0, abs=0.05)

    def test_capture_statistics(self):
        fl, p = FlowLock(), pos(entry=1.1000, sl=1.0980)
        drive(fl, p, [1.1000 + 0.0004 * i for i in range(16)], Mom())
        t = fl.trackers[1]
        stats = fl.capture_stats(t, 1.1030)
        assert stats["realised_r"] == pytest.approx(1.5, abs=0.05)
        assert 0 < stats["mfe_capture_pct"] <= 100.5
        assert stats["final_state"]

    def test_short_excursions_are_symmetric(self):
        fl, p = FlowLock(), pos(side=Side.SELL, entry=1.1000, sl=1.1020)
        fl.update(p, price=1.0960, atr=ATR,
                  bar=Bar(T0, 1.0960, 1.1010, 1.0960, 1.0960), now=T0)
        t = fl.trackers[1]
        assert t.mfe == pytest.approx(0.0040)
        assert t.mae == pytest.approx(0.0010)
        assert t.mfe_r == pytest.approx(2.0, abs=0.05)


class TestPartialsAndBreakeven:
    def test_partial_fires_once_at_the_configured_r(self):
        fl, p = FlowLock(), pos()
        out = drive(fl, p, [1.1000 + 0.0004 * i for i in range(20)], Mom())
        partials = [d.partial_volume for _px, d in out if d.partial_volume > 0]
        assert len(partials) == 1
        assert partials[0] == pytest.approx(1.0 * 0.4)

    def test_partials_can_be_switched_off(self):
        cfg = FlowLockConfig()
        cfg.partial_enabled = False
        fl, p = FlowLock(cfg), pos()
        out = drive(fl, p, [1.1000 + 0.0004 * i for i in range(20)], Mom())
        assert not any(d.partial_volume for _px, d in out)

    def test_breakeven_is_not_applied_too_early(self):
        fl, p = FlowLock(), pos(entry=1.1000, sl=1.0980)
        drive(fl, p, [1.1004, 1.1006], Mom())
        assert p.sl < 1.1000, "must not scratch the trade at 0.3R"

    def test_breakeven_applied_once_earned(self):
        fl, p = FlowLock(), pos(entry=1.1000, sl=1.0980)
        drive(fl, p, [1.1000 + 0.0005 * i for i in range(10)], Mom())
        assert p.sl >= 1.1000


class TestRestart:
    def test_tracker_state_survives_a_restart(self, journal):
        fl, p = FlowLock(), pos()
        drive(fl, p, [1.1000 + 0.0004 * i for i in range(16)], Mom())
        original = fl.trackers[1]
        journal.save_tracker(original)

        revived = FlowLock()
        revived.restore(journal.load_trackers())
        t = revived.trackers[1]
        assert t.state is original.state
        assert t.mfe_r == pytest.approx(original.mfe_r)
        assert t.partial_done == original.partial_done
        assert t.stop == pytest.approx(original.stop)

    def test_adopting_an_existing_position_does_not_reset_it(self):
        fl, p = FlowLock(), pos()
        drive(fl, p, [1.1000 + 0.0004 * i for i in range(10)], Mom())
        before = fl.trackers[1].mfe
        fl.adopt(p, p.sl)
        assert fl.trackers[1].mfe == pytest.approx(before)
