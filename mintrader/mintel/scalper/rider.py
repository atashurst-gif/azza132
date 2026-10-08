"""Rapid Scalper version 3: the rider. A proper state machine for a trade:

    TRADE_VALIDATION -> INITIAL_RISK -> PROFIT_PROTECTION -> MOMENTUM_RIDE -> MAXIMUM_RIDE

Lose small and quickly; win disproportionately when the market gives a
genuine explosive move. Two things are kept apart and both only ever move
in the trade's favour: the TRAILING STOP (where the protective stop is) and
the PROFIT FLOOR (the least profit the trade is now allowed to surrender).
The trailing distance may widen as profit and momentum grow; the floor
never retreats; the risk of the original trade is never increased.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Optional

from ..broker.base import Side, Tick
from .config import ScalperConfig
from .manage import Decision, Manager, ScalpTrade
from .velocity import Micro

STATES_V3 = ("TRADE_VALIDATION", "INITIAL_RISK", "PROFIT_PROTECTION", "MOMENTUM_RIDE", "MAXIMUM_RIDE")


def momentum_now(m: Optional[Micro], sign: int, cfg: ScalperConfig) -> float:
    """0..100: how confident we are the move still has further to go."""
    if m is None or not m.ok:
        return 50.0
    noise = max(m.noise_points, m.spread_points, 1e-9)
    v = max(0.0, min(1.0, (m.v(1.0) * sign) / (cfg.full_marks_velocity_noise_per_s * noise)))
    a = max(0.0, min(1.0, 0.5 + (m.accel_ratio * sign) / (2.0 * cfg.full_marks_accel_ratio)))
    c = max(0.0, min(1.0, (m.pressure * sign + 1.0) / 2.0))
    fresh = max(0.0, min(1.0, m.new_extremes_5s / 3.0)) if m.distance_5s * sign >= 0 else 0.0
    retr = 1.0 - max(0.0, min(1.0, m.retracement)) if m.move_points * sign > 0 else 0.3
    spread_ok = 1.0 if m.spread_ratio <= cfg.spread_expansion_limit else 0.3
    score = 100.0 * (0.30 * v + 0.25 * a + 0.20 * c + 0.10 * fresh + 0.15 * retr) * spread_ok
    return round(max(0.0, min(100.0, score)), 1)


class RiderManager(Manager):
    """Same interface as the version-2 manager; the behaviour is the rider's."""

    def update(self, trade: ScalpTrade, tick: Tick, f, now: dt.datetime,
               m: Optional[Micro] = None) -> Decision:
        c = self.cfg
        sign = trade.side.sign
        price = tick.bid if trade.side is Side.BUY else tick.ask
        money = trade.money_of(price)
        r_now = trade.r_of(price)
        mid = (tick.bid + tick.ask) / 2.0
        r_mid = trade.r_of(mid)
        if money > trade.high_water_money:
            trade.high_water_money = money
            trade.high_water_price = price
            trade.peak_r = max(trade.peak_r, r_now)
            trade.last_new_high_at = now
        trade.mae_r = max(trade.mae_r, -r_now)
        trade.peak_mid_r = max(getattr(trade, "peak_mid_r", 0.0), r_mid)
        secs = trade.duration(now)
        conf = momentum_now(m, sign, c)
        trade.momentum_now = conf
        trade.runner_probability = conf
        spread_pts = ((tick.ask - tick.bid) / trade.point) if trade.point else 0.0
        noise_pts = max((m.noise_points if m is not None else 0.0), spread_pts, 1e-9)
        if trade.state not in STATES_V3:
            trade.state = "TRADE_VALIDATION"

        def out(reason: str, why: str) -> Decision:
            trade.state_reason = why
            return Decision(close=True, exit_reason=reason, reason=why, state=trade.state)

        # ---- spread emergency: only ever costs a winner its spread, never adds risk
        emergency = c.limits_points(trade.symbol, trade.point)[0] * 2
        if spread_pts > emergency and money > 0:
            return out("SPREAD_EMERGENCY", f"spread blew out to {spread_pts:.0f} points (limit {emergency:.0f})")

        # ---- STAGE 1: survival. The trade must prove itself at once.
        if trade.state == "TRADE_VALIDATION":
            if secs >= c.validate_seconds:
                trade.state = "INITIAL_RISK"
                trade.state_reason = f"survived the first {c.validate_seconds:.0f} s"
            else:
                against = m is not None and m.ok and (m.v(1.0) * sign < 0) and (m.pressure * sign < -0.3)
                if against and r_mid <= -c.validate_fail_r:
                    return out("FAILED_MOMENTUM", f"momentum reversed at once ({r_mid:.2f}R on the mid)")
                if m is not None and m.ok and trade.setup == "MICRO_BREAKOUT":
                    back_inside = (mid < m.micro_high_30) if sign > 0 else (mid > m.micro_low_30)
                    if back_inside and r_mid < 0:
                        return out("FAILED_MOMENTUM", "breakout failed: price is back inside the range")
        if trade.state == "INITIAL_RISK":
            if m is not None and m.ok:
                collapsed = (m.v(1.0) * sign < 0 and m.pressure * sign <= -c.collapse_pressure and r_mid <= -c.collapse_fail_r)
                if collapsed:
                    return out("FAILED_MOMENTUM", f"acceleration collapsed and reversed ({r_mid:.2f}R)")
                if m.spread_ratio > c.spread_expansion_limit and r_mid <= 0:
                    return out("FAILED_MOMENTUM", f"spread widened {m.spread_ratio:.1f}x while under water")
            if secs >= c.max_no_progress_seconds and trade.peak_r < c.no_progress_min_r:
                return out("NO_PROGRESS", f"{secs:.0f} s without reaching +{c.no_progress_min_r:.1f}R: not the explosive move we wanted")

        # ---- the profit floor ladder (in R of the planned loss), never retreats
        floor_r = 0.0
        for reached, protect in c.floor_ladder_r:
            if trade.peak_r >= reached:
                floor_r = max(floor_r, protect)
        costs = (trade.spec.money_per_lot((tick.ask - tick.bid)) * trade.volume if trade.spec else 0.0) \
            + c.commission_for(trade.symbol) * trade.volume
        r_money = (trade.spec.money_per_lot(trade.risk_distance) * trade.volume
                   if (trade.spec is not None and trade.risk_distance > 0) else trade.planned_loss)
        floor_money = floor_r * r_money
        if floor_r > 0 and floor_money < costs and trade.peak_r >= c.floor_ladder_r[0][0]:
            floor_money = costs                              # the first rung is "nothing lost after costs"
        if floor_money > trade.protected_floor_money:
            trade.protected_floor_money = floor_money

        # ---- state progression (forward only)
        if trade.state in ("INITIAL_RISK", "TRADE_VALIDATION") and trade.peak_r >= c.protect_at_r:
            trade.state = "PROFIT_PROTECTION"; trade.state_reason = f"+{trade.peak_r:.2f}R reached: protecting"
        if trade.state == "PROFIT_PROTECTION" and trade.peak_r >= c.ride_at_r and conf >= c.ride_min_momentum:
            trade.state = "MOMENTUM_RIDE"; trade.state_reason = f"momentum {conf:.0f}/100 at +{trade.peak_r:.2f}R: riding"
        if trade.state == "MOMENTUM_RIDE" and self._maximum_ride(m, sign, conf, c):
            trade.state = "MAXIMUM_RIDE"; trade.state_reason = f"exceptional acceleration ({conf:.0f}/100): maximum ride"
        if trade.state == "MAXIMUM_RIDE" and conf < c.max_ride_exit_momentum:
            trade.state = "MOMENTUM_RIDE"; trade.state_reason = f"momentum eased to {conf:.0f}/100: normal ride"   # the stop stays

        # ---- exits once in profit: ride until the market shows the move is ending
        if money > 0 and m is not None and m.ok and trade.state in ("PROFIT_PROTECTION", "MOMENTUM_RIDE", "MAXIMUM_RIDE"):
            if m.v(1.0) * sign < 0 and conf <= c.collapse_momentum and m.distance_ratio >= 1.0:
                return out("MOMENTUM_COLLAPSE", f"momentum collapsed ({conf:.0f}/100) and price is running the other way")
            if m.distance_5s * sign < 0 and m.distance_ratio >= c.opposing_burst_ratio:
                return out("OPPOSING_BURST", f"an opposing burst of {abs(m.distance_5s):.1f} points")
            since_high = (now - trade.last_new_high_at).total_seconds() if trade.last_new_high_at else 0.0
            if trade.state != "MAXIMUM_RIDE" and since_high >= c.stall_seconds and abs(m.v(3.0)) < 0.15 * noise_pts:
                return out("STALLED", f"no new high for {since_high:.0f} s and the tape has gone quiet: taking the money")
            give = c.giveback_by_state.get(trade.state, 0.5)
            if trade.high_water_money > 0 and money < trade.high_water_money * (1.0 - give) and money > trade.protected_floor_money:
                if conf < c.ride_min_momentum or trade.state == "PROFIT_PROTECTION":
                    return out("DEEP_RETRACEMENT", f"gave back {1 - money / trade.high_water_money:.0%} of the peak with momentum {conf:.0f}/100")

        # ---- the trailing stop: distance from the current price set by momentum, never below the floor
        new_stop = None
        trail_pts = 0.0
        if trade.state in ("PROFIT_PROTECTION", "MOMENTUM_RIDE", "MAXIMUM_RIDE"):
            if conf < c.ride_min_momentum:
                mult = c.trail_weak_noise
            elif trade.state == "MAXIMUM_RIDE":
                mult = c.trail_max_ride_noise
            elif conf >= c.trail_strong_momentum:
                mult = c.trail_strong_noise
            else:
                mult = c.trail_normal_noise
            trail_pts = max(mult * noise_pts, c.min_trail_spreads * spread_pts)
            cand = trade.high_water_price - sign * trail_pts * trade.point
            if trade.state == "MAXIMUM_RIDE" and m is not None and m.ok:
                # deeper structural pullbacks, not tick wobble: behind the last 10 s swing
                swing = m.move_start_mid if m.move_points * sign > 0 else None
                if swing is not None:
                    cand = min(cand, swing - sign * spread_pts * trade.point) if sign > 0 else max(cand, swing - sign * spread_pts * trade.point)
            # the floor: a price that banks at least the protected money
            if trade.protected_floor_money > 0 and trade.spec is not None:
                per_point = trade.spec.money_per_lot(trade.point) * trade.volume
                if per_point > 0:
                    floor_px = trade.entry_filled + sign * (trade.protected_floor_money / per_point) * trade.point
                    cand = max(cand, floor_px) if sign > 0 else min(cand, floor_px)
            new_stop = self._advance(trade, cand, f"{trade.state.lower().replace('_', ' ')}: trailing {trail_pts:.1f} points "
                                     f"(momentum {conf:.0f}/100), floor {trade.protected_floor_money:+.2f}", now, price)
        trade.trail_points = trail_pts
        trade.exit_tolerance = {"MAXIMUM_RIDE": "WIDENED", "MOMENTUM_RIDE": "NORMAL"}.get(trade.state, "TIGHT")
        trade.last_note = (f"{trade.state} {r_now:+.2f}R now, peak {trade.peak_r:.2f}R, momentum {conf:.0f}/100, "
                           f"floor {trade.protected_floor_money:+.2f}, trail {trail_pts:.1f} pts")
        return Decision(new_stop=new_stop, reason=trade.last_note, state=trade.state)

    @staticmethod
    def _maximum_ride(m: Optional[Micro], sign: int, conf: float, c: ScalperConfig) -> bool:
        if m is None or not m.ok:
            return False
        return (conf >= c.max_ride_momentum and m.accel_ratio * sign > 0 and m.retracement <= c.max_ride_max_retrace
                and m.ticks_per_second >= 2.0 * c.min_ticks_per_second and m.pressure * sign >= c.burst_min_consistency
                and m.new_extremes_5s >= c.max_ride_min_fresh and m.spread_ratio <= c.spread_expansion_limit)
