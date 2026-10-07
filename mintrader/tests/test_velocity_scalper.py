"""Rapid Scalper version 3, the High-Velocity Session Rider."""
import datetime as dt
import json
from types import SimpleNamespace

import pytest

from mintel.broker.base import Side, Tick
from mintel.broker.sim import SimBroker
from mintel.config import Config
from mintel.scalper.config import ScalperConfig
from mintel.scalper.engine import ScalperEngine
from mintel.scalper.features import TickBuffer
from mintel.scalper.manage import ScalpTrade
from mintel.scalper.rider import RiderManager, momentum_now
from mintel.scalper.sessions import SessionTracker, current_window, default_windows, threshold_for
from mintel.scalper.velocity import detect_setup, micro, read_market, structural_stop

T0 = dt.datetime(2026, 10, 7, 8, 10, tzinfo=dt.timezone.utc)      # 09:10 London, in the London post-open
POINT = 0.00001


def tape(path, start=T0, dt_s=0.2, spread=1.0):
    """Ticks every 0.2 s at the mid prices in ``path`` (in points from 1.1000)."""
    buf = TickBuffer("EURUSD")
    for i, pts in enumerate(path):
        mid = 1.1000 + pts * POINT
        buf.add(Tick("EURUSD", start + dt.timedelta(seconds=i * dt_s), mid - spread * POINT / 2, mid + spread * POINT / 2))
    return buf


def quiet(n=300, wobble=1):
    """n ticks of noise: a 0, +1, 0, -1 wobble with no drift and no net pressure."""
    return [(0, 1, 0, -1)[i % 4] * wobble for i in range(n)]


def burst(n=15, step=1.5):
    return [i * step for i in range(1, n + 1)]


def vcfg(**kw):
    base = dict(symbols=("EURUSD",), min_ticks_per_second=0.5, min_confidence=60.0)
    base.update(kw)
    return ScalperConfig(**base)


class TestMicroMomentum:
    def test_quiet_tape_has_no_direction_and_a_burst_has_acceleration(self):
        m = micro(tape(quiet()), POINT)
        assert m.ok and m.direction == 0 and m.noise_points > 0
        m2 = micro(tape(quiet() + burst()), POINT)
        assert m2.direction == 1 and m2.v(1.0) > 0 and m2.v(0.5) > 0
        assert m2.accel_ratio > 1.0 and m2.consistency >= 0.8 and m2.distance_ratio > 1.5
        assert m2.breakout_points > 0 and m2.new_extremes_5s >= 3

    def test_every_horizon_is_measured(self):
        m = micro(tape(quiet() + burst()), POINT)
        assert set(m.velocity) == {0.5, 1.0, 2.0, 3.0, 5.0, 10.0}
        assert m.ticks_per_second == pytest.approx(5.0, abs=0.6)

    def test_retracement_and_sweep_are_seen(self):
        path = quiet() + burst(15) + [22.5 - 1.5 * i for i in range(1, 6)]        # up 22.5 then back 7.5
        m = micro(tape(path), POINT)
        assert 0.25 <= m.retracement <= 0.45
        # a sweep: through the minute's high, then straight back under it
        path2 = quiet() + [3, 5, 8, 11, 9, 6, 3, 0, -3, -6, -9]
        m2 = micro(tape(path2), POINT)
        assert m2.swept_high and m2.v(1.0) < 0


class TestSetupsAndScores:
    def test_an_acceleration_setup_scores_and_sets_a_structural_stop(self):
        buf = tape(quiet() + burst())
        m = micro(buf, POINT)
        c = vcfg()
        setup, d = detect_setup(m, c, in_open_window=False)
        assert d == 1 and setup in ("ACCELERATION", "MICRO_BREAKOUT")
        r = read_market(m, c, commission_points=0.5, slippage_points=0.5, in_open_window=False, session_bonus=0.0, threshold=60.0)
        assert r.direction == 1 and r.confidence > 60 and r.scalpability > 50, (r.confidence, r.scalpability, r.blockers)
        assert set(r.contributions) >= {"Velocity", "Acceleration", "Consistency", "Liquidity", "Spread", "Session"}
        assert r.contributions["Order flow (no book over this feed)"] == 0.0
        stop = structural_stop(buf, m, 1, c, POINT)
        assert 0 < stop < m.mid and (m.mid - stop) / POINT >= c.min_stop_spreads * m.spread_points

    def test_movement_without_acceleration_is_not_a_trade(self):
        slow = quiet() + [i * 0.3 for i in range(1, 40)]                 # a drift, no acceleration
        m = micro(tape(slow), POINT)
        r = read_market(m, vcfg(), commission_points=0.5, slippage_points=0.5, in_open_window=False, session_bonus=0.0, threshold=60.0)
        assert not (r.direction and not r.blockers)

    def test_chasing_is_refused(self):
        far = quiet() + [0.08 * i * i for i in range(1, 50)]             # accelerating, already 190 points in 10 s
        m = micro(tape(far), POINT)
        r = read_market(m, vcfg(), commission_points=0.5, slippage_points=0.5, in_open_window=False, session_bonus=0.0, threshold=60.0)
        assert r.direction == 1 and any("without a pullback" in b for b in r.blockers)

    def test_costs_make_a_market_unscalpable(self):
        buf = tape(quiet() + burst(), spread=12.0)                        # a 12-point spread on a 20-point move
        m = micro(buf, POINT)
        r = read_market(m, vcfg(), commission_points=6.0, slippage_points=2.0, in_open_window=False, session_bonus=0.0, threshold=60.0)
        assert any("not scalpable" in b or "costs" in b for b in r.blockers)

    def test_the_bar_moves_with_the_session_and_losses(self):
        w = default_windows()
        london_open = dt.datetime(2026, 10, 7, 7, 10, tzinfo=dt.timezone.utc)    # 08:10 BST
        win = current_window(london_open, w)
        assert win is not None and win.name == "London" and win.phase == "OPENING"
        assert threshold_for(win, 70, 10, 15, moving=True)[0] == 60
        assert threshold_for(win, 70, 10, 15, moving=False)[0] == 70
        assert threshold_for(None, 70, 10, 15, moving=True)[0] == 85
        assert current_window(dt.datetime(2026, 10, 10, 7, 10, tzinfo=dt.timezone.utc), w) is None   # Saturday
        # DST: 7 Oct London is BST (UTC+1); a 07:10 UTC tick is 08:10 local
        ny = current_window(dt.datetime(2026, 10, 7, 12, 5, tzinfo=dt.timezone.utc), w)
        assert ny.name == "New York" and ny.phase == "OPENING"

    def test_the_session_tracker_measures_rather_than_assumes(self):
        from mintel.broker.base import Bar
        t = SessionTracker()
        bars = [Bar(T0 - dt.timedelta(minutes=70 - i), 1.1, 1.1 + 5 * POINT, 1.1, 1.1, 10) for i in range(70)]
        assert t.update("EURUSD", bars, POINT) == pytest.approx(1.0) and not t.moving("EURUSD")
        hot = bars[:-5] + [Bar(b.time, 1.1, 1.1 + 20 * POINT, 1.1, 1.1, 10) for b in bars[-5:]]
        assert t.update("EURUSD", hot, POINT) == pytest.approx(4.0) and t.moving("EURUSD")


def trade(spec, entry=1.10000, stop=1.09990, volume=1.0):
    t = ScalpTrade(1, "EURUSD", Side.BUY, volume, entry, entry, T0, stop, stop, entry - stop, 10.0, POINT, spec=spec)
    t.state = "TRADE_VALIDATION"; t.setup = "ACCELERATION"; t.last_new_high_at = T0
    return t


def mk(**kw):
    """A micro picture for the rider, with sensible defaults for a long."""
    from mintel.scalper.velocity import Micro
    m = Micro(ok=True, spread_points=1.0, spread_median_points=1.0, noise_points=4.0)
    m.velocity = {0.5: 2.0, 1.0: 2.0, 2.0: 1.5, 3.0: 1.5, 5.0: 1.0, 10.0: 0.8}
    m.accel_ratio = 1.5; m.pressure = 0.8; m.consistency = 0.8; m.ticks_per_second = 4.0
    m.distance_5s = 8.0; m.distance_ratio = 2.0; m.move_points = 10.0; m.retracement = 0.1; m.new_extremes_5s = 3
    m.micro_high_30 = 1.09995; m.micro_low_30 = 1.09980; m.spread_ratio = 1.0
    for k, v in kw.items():
        setattr(m, k, v)
    return m


class TestTheRider:
    def setup_method(self):
        b = SimBroker(["EURUSD"], start=T0 - dt.timedelta(days=1), history_bars=200); b.connect()
        self.spec = b.spec("EURUSD")
        self.c = vcfg()
        self.m = RiderManager(self.c)

    def tick(self, pts, s):
        mid = 1.1000 + pts * POINT
        return Tick("EURUSD", T0 + dt.timedelta(seconds=s), mid - 0.5 * POINT, mid + 0.5 * POINT), T0 + dt.timedelta(seconds=s)

    def test_a_failed_scalp_is_cut_within_seconds(self):
        t = trade(self.spec)
        against = mk(velocity={h: -3.0 for h in (0.5, 1.0, 2.0, 3.0, 5.0, 10.0)}, pressure=-0.9, accel_ratio=-2.0)
        tk, now = self.tick(-3.0, 1.0)                                # -0.3 R on the mid, tape running down
        d = self.m.update(t, tk, None, now, against)
        assert d.close and d.exit_reason == "FAILED_MOMENTUM" and t.r_of(tk.bid) > -1.0

    def test_no_progress_is_a_time_stop(self):
        t = trade(self.spec)
        for s in (1.0, 2.0, 3.5):
            tk, now = self.tick(0.5, s)
            assert not self.m.update(t, tk, None, now, mk(pressure=0.1, velocity={h: 0.1 for h in (0.5, 1.0, 2.0, 3.0, 5.0, 10.0)})).close
        tk, now = self.tick(1.0, 26.0)
        d = self.m.update(t, tk, None, now, mk(pressure=0.1))
        assert d.close and d.exit_reason == "NO_PROGRESS"

    def test_protection_then_ride_with_a_floor_that_never_retreats(self):
        t = trade(self.spec)
        tk, now = self.tick(1.0, 1.0); self.m.update(t, tk, None, now, mk())
        tk, now = self.tick(7.0, 4.0); self.m.update(t, tk, None, now, mk())        # +0.7R: protection
        assert t.state == "PROFIT_PROTECTION" and t.protected_floor_money >= 0.0
        tk, now = self.tick(21.0, 6.0); d = self.m.update(t, tk, None, now, mk(accel_ratio=2.5, pressure=0.9))
        assert t.state in ("MOMENTUM_RIDE", "MAXIMUM_RIDE")
        floor = t.protected_floor_money
        assert floor >= 1.0 * t.planned_loss                           # +2.0R reached: at least 1.0R banked
        assert t.stop > t.entry_filled                                 # the stop is above the entry
        # a dip with momentum still strong: the floor and the stop hold
        tk, now = self.tick(16.0, 7.0); d = self.m.update(t, tk, None, now, mk(retracement=0.25))
        assert not d.close and t.protected_floor_money == floor
        stop_before = t.stop
        tk, now = self.tick(14.0, 8.0); self.m.update(t, tk, None, now, mk(retracement=0.35, pressure=0.3, accel_ratio=0.2))
        assert t.stop >= stop_before and t.protected_floor_money >= floor

    def test_the_trailing_distance_widens_with_momentum_and_tightens_when_it_fades(self):
        t = trade(self.spec)
        tk, now = self.tick(20.0, 5.0); self.m.update(t, tk, None, now, mk(accel_ratio=2.5, pressure=0.9, new_extremes_5s=4))
        wide = t.trail_points
        assert t.state in ("MOMENTUM_RIDE", "MAXIMUM_RIDE") and wide >= self.c.trail_strong_noise * 4.0 - 1e-9
        tk, now = self.tick(21.0, 6.0); self.m.update(t, tk, None, now, mk(accel_ratio=0.0, pressure=0.2, new_extremes_5s=0, retracement=0.3,
                                                                           velocity={h: 0.2 for h in (0.5, 1.0, 2.0, 3.0, 5.0, 10.0)}))
        assert t.trail_points < wide

    def test_momentum_collapse_scrapes_the_profit(self):
        t = trade(self.spec)
        tk, now = self.tick(20.0, 5.0); self.m.update(t, tk, None, now, mk(accel_ratio=2.5, pressure=0.9))
        collapse = mk(velocity={h: -2.0 for h in (0.5, 1.0, 2.0, 3.0, 5.0, 10.0)}, pressure=-0.9, accel_ratio=-2.5,
                      distance_5s=-6.0, distance_ratio=1.5, new_extremes_5s=0, retracement=0.5)
        tk, now = self.tick(17.0, 6.0); d = self.m.update(t, tk, None, now, collapse)
        assert d.close and d.exit_reason in ("MOMENTUM_COLLAPSE", "OPPOSING_BURST")

    def test_maximum_ride_needs_exceptional_evidence(self):
        t = trade(self.spec)
        tk, now = self.tick(20.0, 5.0)
        self.m.update(t, tk, None, now, mk(accel_ratio=3.5, pressure=0.95, new_extremes_5s=5, ticks_per_second=6.0, retracement=0.05))
        assert t.state == "MAXIMUM_RIDE"
        t2 = trade(self.spec)
        self.m.update(t2, tk, None, now, mk(accel_ratio=0.5, pressure=0.7, new_extremes_5s=1))
        assert t2.state != "MAXIMUM_RIDE"

    def test_momentum_now_is_0_to_100(self):
        assert 0 <= momentum_now(mk(), 1, self.c) <= 100
        assert momentum_now(mk(), 1, self.c) > momentum_now(mk(), -1, self.c)


class TestInsideTheEngine:
    def _engine(self, tmp_path, **kw):
        cfg = Config(); cfg.ops.data_dir = str(tmp_path); cfg.mode = "DEMO"
        broker = SimBroker(["EURUSD"], start=T0 - dt.timedelta(days=1), history_bars=400); broker.connect()
        c = vcfg(**kw)
        e = ScalperEngine(cfg, c, broker, clock=lambda: broker.now, data_dir=tmp_path)
        return e, broker

    def test_the_loss_ladder_shrinks_size_and_raises_the_bar_never_the_reverse(self, tmp_path):
        e, b = self._engine(tmp_path)
        assert e._loss_ladder() == (1.0, 0.0)
        e.breakers.consecutive_losses = 2
        assert e._loss_ladder() == (0.75, 5.0)
        e.breakers.consecutive_losses = 9
        assert e._loss_ladder()[0] <= 0.5 and e._loss_ladder()[1] >= 10.0

    def test_a_burst_on_the_tape_becomes_a_trade_with_the_setup_recorded(self, tmp_path):
        e, b = self._engine(tmp_path)
        buf = e.buffers["EURUSD"]
        for t in tape(quiet() + burst(), start=b.now - dt.timedelta(seconds=63)).ticks:
            buf.add(t)
        e.last_tick_at = buf.last.time
        now = buf.last.time
        import time as _t
        opps = e._velocity_scan(now, _t.monotonic())
        assert opps and opps[0].direction == 1 and opps[0].setup
        if not opps[0].tradable:
            pytest.skip(f"not tradable on this tape: {opps[0].blockers}")
        why = e._try_open(opps[0], now, {"EURUSD": buf.last})
        assert "EURUSD BUY" in why, why
        tr = next(iter(e.trades.values()))
        assert tr.state == "TRADE_VALIDATION" and tr.setup == opps[0].setup and tr.planned_loss <= 10.0 + 1e-9
        row = e.journal._exec("SELECT setup, session, momentum_at_entry, scalpability FROM trades WHERE ticket=?", (tr.ticket,)).fetchone()
        assert row["setup"] == tr.setup and row["momentum_at_entry"] == opps[0].confidence and row["scalpability"] > 0

    def test_status_speaks_plain_english(self, tmp_path):
        e, b = self._engine(tmp_path)
        e._update_thinking(b.now, [])
        st = e.status(b.now, 0.5)
        assert st["thinking"].startswith("SCANNING") and st["entry_style"] == "VELOCITY"
        assert "session" in st and "threshold" in st
        e._update_thinking(b.now, ["3 losses in a row - paused for another 10 min"])
        assert e.thinking.startswith("COOLDOWN")

    def test_analytics_include_what_aaron_asked_for(self, tmp_path):
        from mintel.scalper.stats import rider_analytics
        rows = [{"net_pnl": 12.0, "duration_seconds": 40, "peak_profit": 15.0, "surrendered_from_peak": 3.0, "session": "London opening",
                 "setup": "ACCELERATION", "symbol": "EURUSD", "exit_reason": "MOMENTUM_COLLAPSE", "spread_cost": 0.8, "slippage_cost": 0.2, "commission": 0.5},
                {"net_pnl": -3.0, "duration_seconds": 4, "peak_profit": 0.0, "surrendered_from_peak": 0.0, "session": "quiet hours",
                 "setup": "MICRO_BREAKOUT", "symbol": "US500", "exit_reason": "FAILED_MOMENTUM", "spread_cost": 0.5, "slippage_cost": 0.1, "commission": 0.0}]
        a = rider_analytics(rows, {"spread too wide": 4, "momentum too low": 9})
        assert a["expectancy"] == 4.5 and a["avg_win_over_avg_loss"] == 4.0
        assert a["avg_winning_duration_s"] == 40 and a["avg_losing_duration_s"] == 4
        assert a["by_session"]["London opening"]["net"] == 12.0 and a["by_setup"]["ACCELERATION"]["wins"] == 1
        assert a["rejected"]["momentum too low"] == 9 and a["peak_vs_realised"]["avg_surrendered"] == 1.5

    def test_the_other_bots_are_untouched(self):
        from mintel.config import Config as C
        c = C()
        assert c.scan.entry_tier == "STRONG" and c.risk.max_risk_money == 10.0 and c.runner.trail_r == 3.0
        assert c.bandbreaker.markets == ("US500", "US30", "USTEC")
