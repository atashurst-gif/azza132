"""The Rapid Scalper trade manager: prove it fast, cut failures faster,
protect capital, and let exceptional momentum run.

States: INITIAL_RISK -> RISK_REDUCTION -> CAPITAL_SAFE -> PROFIT_PROTECTED
-> RUNNER. The hard protective stop NEVER moves backwards: that is an
invariant enforced in code and in tests.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Optional

from ..broker.base import Side, Tick
from ..contracts import SymbolSpec
from .config import ScalperConfig
from .features import Features

EXIT_REASONS = ("INITIAL_STOP", "THESIS_FAILED", "MOMENTUM_DECAY", "PROFIT_TRAIL",
                "STRUCTURE_BREAK", "SPREAD_EMERGENCY", "SLIPPAGE_PROTECTION",
                "RISK_KILL_SWITCH", "ACCOUNT_PROTECTION", "MANUAL_CLOSE", "SYSTEM_SHUTDOWN")
STATES = ("INITIAL_RISK", "RISK_REDUCTION", "CAPITAL_SAFE", "PROFIT_PROTECTED", "RUNNER")


class StopInvariantError(AssertionError):
    """Raised if anything ever tries to move the hard stop backwards."""


@dataclass
class ScalpTrade:
    ticket: int
    symbol: str
    side: Side
    volume: float
    entry_requested: float
    entry_filled: float
    opened_at: dt.datetime
    stop: float                          # the hard protective stop at the broker
    initial_stop: float
    risk_distance: float                 # price units
    planned_loss: float                  # account currency
    point: float
    spec: Optional[SymbolSpec] = None
    state: str = "INITIAL_RISK"
    high_water_money: float = 0.0
    high_water_price: float = 0.0
    peak_r: float = 0.0
    mae_r: float = 0.0
    peak_velocity: float = 0.0
    protected_floor_money: float = 0.0   # formally protected; never retreats
    runner_probability: float = 0.0
    runner_history: list = field(default_factory=list)
    stop_changes: list = field(default_factory=list)
    confidence: float = 0.0
    components: dict = field(default_factory=dict)
    entry_reason: str = ""
    partial_done: bool = False
    exit_tolerance: str = "TIGHT"
    last_note: str = ""
    peak_mid_r: float = 0.0

    def r_of(self, price: float) -> float:
        if self.risk_distance <= 0:
            return 0.0
        return ((price - self.entry_filled) * self.side.sign) / self.risk_distance

    def money_of(self, price: float) -> float:
        if self.spec is None:
            return 0.0
        return self.spec.money((price - self.entry_filled) * self.side.sign, self.volume)

    def duration(self, now: dt.datetime) -> float:
        return max(0.0, (now - self.opened_at).total_seconds())


@dataclass
class Decision:
    new_stop: Optional[float] = None
    close: bool = False
    exit_reason: str = ""
    partial_volume: float = 0.0
    reason: str = ""
    state: str = ""


class Manager:
    def __init__(self, cfg: ScalperConfig):
        self.cfg = cfg

    # ------------------------------------------------------------ invariant --
    @staticmethod
    def enforce_monotonic(trade: ScalpTrade, new_stop: float) -> float:
        """The hard stop may only move in the trade's favour. Ever."""
        if trade.side is Side.BUY and new_stop < trade.stop - 1e-12:
            raise StopInvariantError(f"long stop would move back {trade.stop} -> {new_stop}")
        if trade.side is Side.SELL and new_stop > trade.stop + 1e-12:
            raise StopInvariantError(f"short stop would move back {trade.stop} -> {new_stop}")
        return new_stop

    def _advance(self, trade: ScalpTrade, candidate: float, why: str,
                 now: dt.datetime, price: float) -> Optional[float]:
        """Only ever tighten; never through the current price; record why."""
        sign = trade.side.sign
        if (price - candidate) * sign <= 0:
            return None
        if trade.spec is not None:
            candidate = trade.spec.normalise_price(candidate)
            min_dist = trade.spec.min_stop_distance_price()
            if min_dist and (price - candidate) * sign < min_dist:
                candidate = trade.spec.normalise_price(price - sign * min_dist)
        improves = (candidate > trade.stop) if trade.side is Side.BUY else (candidate < trade.stop)
        if not improves:
            return None
        self.enforce_monotonic(trade, candidate)
        trade.stop_changes.append({"at": now.isoformat(), "from": trade.stop, "to": candidate,
                                   "why": why, "state": trade.state})
        trade.stop = candidate
        floor = trade.money_of(candidate)
        if floor > trade.protected_floor_money:
            trade.protected_floor_money = floor
        return candidate

    # ---------------------------------------------------------------- update --
    def update(self, trade: ScalpTrade, tick: Tick, f: Optional[Features],
               now: dt.datetime) -> Decision:
        c = self.cfg
        sign = trade.side.sign
        price = tick.bid if trade.side is Side.BUY else tick.ask    # what we could close at
        r_now = trade.r_of(price)
        money = trade.money_of(price)
        if money > trade.high_water_money:
            trade.high_water_money = money
            trade.high_water_price = price
            trade.peak_r = max(trade.peak_r, r_now)
            if f is not None:
                trade.peak_velocity = max(trade.peak_velocity, f.velocity_5s * sign)
        trade.mae_r = max(trade.mae_r, -r_now)
        # Progress and adverse movement are judged on the MID price: a fresh
        # buy sits at -spread on the bid the instant it fills, and that is a
        # cost we already paid, not the thesis failing.
        mid = (tick.bid + tick.ask) / 2.0
        r_mid = trade.r_of(mid)
        trade.peak_mid_r = max(getattr(trade, "peak_mid_r", 0.0), r_mid)
        secs = trade.duration(now)
        vel = (f.velocity_5s * sign) if f is not None else 0.0
        spread_pts = ((tick.ask - tick.bid) / trade.point) if trade.point else 0.0

        # ---- hard stop reached (paper) is handled by the executor; here: thesis checks
        # PHASE 1 - PROVE IT
        # The planned loss IS the exit. We step out early only when the trade
        # is certainly going there: deep against us on the mid AND the tape is
        # still moving towards the stop. Flat, wobbling or merely negative
        # trades are left to prove themselves or to the stop.
        # the reason for the trade has gone: the 1- and 5-minute moves have
        # turned against it while it is under water - cut it (5 Oct)
        if (trade.state == "INITIAL_RISK" and f is not None and getattr(f, "bars_ok", False)
                and r_mid <= -c.context_cut_r and f.move_1m_points * sign < 0 and f.move_5m_points * sign < 0):
            return Decision(close=True, exit_reason="THESIS_FAILED",
                            reason=f"{r_mid:.2f}R under and the 1- and 5-minute moves have turned against it",
                            state=trade.state)
        # no follow-through: a scalp that has not got going in five minutes
        # is paying its costs for nothing
        if (trade.state == "INITIAL_RISK" and secs >= c.no_follow_through_seconds
                and trade.peak_mid_r < c.no_follow_through_peak_r and r_mid < 0):
            return Decision(close=True, exit_reason="THESIS_FAILED",
                            reason=f"no follow-through: {secs / 60:.0f} min in, never reached "
                                   f"+{c.no_follow_through_peak_r:.1f}R, now {r_mid:.2f}R", state=trade.state)
        if c.early_tick_cut and trade.state == "INITIAL_RISK" and secs <= c.prove_it_seconds * 3:
            against = f is not None and (vel < 0 or f.consecutive_ticks * sign <= -c.thesis_fail_min_ticks_against)
            if r_mid <= -c.thesis_fail_adverse_r and against:
                return Decision(close=True, exit_reason="THESIS_FAILED",
                                reason=f"{r_mid:.2f}R against on the mid and still moving towards the stop "
                                       f"({secs:.0f}s): certainly going there", state=trade.state)
            if (secs >= c.prove_it_seconds and trade.peak_mid_r < c.prove_it_min_progress_r
                    and r_mid <= -c.thesis_fail_stale_r and against):
                return Decision(close=True, exit_reason="THESIS_FAILED",
                                reason=f"no progress after {secs:.0f}s, {r_mid:.2f}R under and moving against",
                                state=trade.state)
        # twice this market's own spread limit (index limits are in index
        # points; 5 Oct: a 25-point cap read every normal US30 spread as an
        # emergency and closed 11 winners at +0.15 to +0.86)
        emergency = c.limits_points(trade.symbol, trade.point)[0] * 2
        if spread_pts > emergency and money > 0:
            return Decision(close=True, exit_reason="SPREAD_EMERGENCY",
                            reason=f"spread blew out to {spread_pts:.0f} points (limit {emergency:.0f})",
                            state=trade.state)

        # ---- state progression (only forward)
        order = STATES
        want = trade.state
        if trade.peak_r >= c.runner_at_r and trade.runner_probability >= c.runner_min_probability:
            want = "RUNNER"
        elif trade.peak_r >= c.profit_protect_at_r:
            want = "PROFIT_PROTECTED"
        elif trade.peak_r >= c.capital_safe_at_r:
            want = "CAPITAL_SAFE"
        elif trade.peak_r >= c.risk_reduction_at_r:
            want = "RISK_REDUCTION"
        if order.index(want) > order.index(trade.state):
            trade.state = want

        # ---- runner probability (0..100) from live evidence
        if f is not None and trade.peak_r > 0.5:
            p = 0.0
            if trade.peak_velocity > 0:
                p += 30.0 * max(0.0, min(1.0, vel / trade.peak_velocity))
            p += 20.0 * max(0.0, min(1.0, f.persistence))
            p += 15.0 * (1.0 if f.trend_bias == sign else 0.0)
            p += 15.0 * max(0.0, min(1.0, (f.volume_accel - 0.9) / 1.1))
            p += 10.0 * max(0.0, min(1.0, (f.acceleration * sign) / max(f.realised_vol_points * 0.3, 0.1) + 0.5))
            p += 10.0 * max(0.0, min(1.0, (f.bar_range_ratio - 1.0) / 1.5))
            trade.runner_probability = round(max(0.0, min(100.0, p)), 1)
            trade.runner_history.append((now.isoformat(), trade.runner_probability))
            if len(trade.runner_history) > 400:
                trade.runner_history = trade.runner_history[-400:]

        # ---- momentum decay exit: the reason we entered is gone
        # Ride the high: a winner is only let go when the BIGGER picture turns -
        # never on a few seconds of ticks (5 Oct). Momentum decay needs both the
        # 1- and 5-minute moves against the trade; a structure break needs the
        # price through the last two completed one-minute bars.
        if trade.state in ("PROFIT_PROTECTED", "RUNNER") and f is not None and getattr(f, "bars_ok", False):
            turned = f.move_1m_points * sign < 0 and f.move_5m_points * sign < 0
            if turned and money > 0:
                return Decision(close=True, exit_reason="MOMENTUM_DECAY",
                                reason=f"the 1- and 5-minute moves have both turned "
                                       f"({f.move_1m_points:+.0f}, {f.move_5m_points:+.0f} points)", state=trade.state)
            lost_level = (price < f.bar_low_2) if sign > 0 else (price > f.bar_high_2)
            if lost_level and money > 0 and trade.state != "RUNNER":
                return Decision(close=True, exit_reason="STRUCTURE_BREAK",
                                reason="price broke the last two one-minute bars", state=trade.state)

        # ---- stop advancement by state
        new_stop = None
        noise = max(f.realised_vol_points * 2.0 if f is not None else 0.0, spread_pts) * trade.point
        if trade.state == "RISK_REDUCTION":
            cand = trade.entry_filled - sign * max(0.5 * trade.risk_distance, noise)
            new_stop = self._advance(trade, cand, "risk reduced: half the original risk off", now, price)
            trade.exit_tolerance = "TIGHT"
        elif trade.state == "CAPITAL_SAFE":
            costs = (trade.spec.money_per_lot(tick.ask - tick.bid) * trade.volume if trade.spec else 0.0) \
                + c.commission_for(trade.symbol) * trade.volume
            be_dist = (costs / max(trade.spec.money_per_lot(trade.point) * trade.volume, 1e-9)) * trade.point if trade.spec else 0.0
            cand = trade.entry_filled + sign * max(be_dist, 0.0)
            new_stop = self._advance(trade, cand, "capital safe: break-even after costs", now, price)
            trade.exit_tolerance = "NORMAL"
        elif trade.state in ("PROFIT_PROTECTED", "RUNNER"):
            giveback = c.giveback_runner if trade.state == "RUNNER" else c.giveback_established
            if trade.state == "RUNNER":
                giveback = min(0.75, giveback + 0.15 * (trade.runner_probability - c.runner_min_probability) / 40.0)
            hw = trade.high_water_price
            cand = hw - sign * max(giveback * abs(hw - trade.entry_filled), noise)
            if f is not None and getattr(f, "bars_ok", False):
                # trail behind the last two completed one-minute bars, not the
                # last few seconds of ticks: pullbacks inside a trend are allowed
                struct = f.bar_low_2 - noise if sign > 0 else f.bar_high_2 + noise
                cand = min(cand, struct) if sign > 0 else max(cand, struct)
            # the floor never retreats: at least what was already protected
            new_stop = self._advance(trade, cand, f"{trade.state.lower().replace('_', ' ')}: trailing "
                                     f"{giveback:.0%} behind the high-water mark", now, price)
            trade.exit_tolerance = "WIDENED" if trade.state == "RUNNER" else "NORMAL"
        else:
            trade.exit_tolerance = "TIGHT"

        partial = 0.0
        if c.enable_partial_profit and not trade.partial_done and r_now >= c.partial_at_r:
            partial = round(trade.volume * c.partial_fraction, 8)
            trade.partial_done = True
        trade.last_note = (f"{trade.state} {r_now:+.2f}R now, peak {trade.peak_r:.2f}R, "
                           f"runner {trade.runner_probability:.0f}%")
        return Decision(new_stop=new_stop, partial_volume=partial, reason=trade.last_note, state=trade.state)
