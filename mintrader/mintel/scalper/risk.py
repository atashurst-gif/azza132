"""Rapid Scalper risk: the GBP 10 planned-loss ceiling, account-level
safety and the circuit breakers. Fail closed."""
from __future__ import annotations

import datetime as dt
import math
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, Sequence

from ..broker.base import AccountInfo, Position, Side
from ..contracts import SymbolSpec
from .config import ScalperConfig


@dataclass
class RiskPlan:
    ok: bool
    reason: str = ""
    side: Optional[Side] = None
    entry: float = 0.0
    stop: float = 0.0                    # the thesis invalidation, as the hard stop
    stop_distance_points: float = 0.0
    effective_distance_points: float = 0.0   # + spread + slippage allowance
    volume: float = 0.0
    per_lot_loss: float = 0.0            # account currency, at the stop, 1.0 lot, costs in
    planned_loss: float = 0.0            # expected loss at the stop for `volume`
    commission: float = 0.0
    spread_cost: float = 0.0
    slippage_allowance: float = 0.0


def plan(spec: SymbolSpec, side: Side, entry: float, invalidation: float,
         spread_price: float, cfg: ScalperConfig) -> RiskPlan:
    """Size so that the EXPECTED loss if the stop fills is <= the ceiling.

    Position size = allowable risk / real market stop distance, where the
    real distance includes the spread, the slippage allowance and the
    commission. The stop is never moved to make a size fit: if the broker's
    minimum lot risks more than the ceiling, there is no trade.
    """
    dist = (entry - invalidation) * side.sign
    if dist <= 0:
        return RiskPlan(False, "invalidation is on the wrong side of the entry")
    pts = dist / spec.point
    if pts < cfg.min_stop_points:
        return RiskPlan(False, f"stop {pts:.1f} points is inside the noise floor ({cfg.min_stop_points:.0f})")
    if pts > cfg.max_stop_points:
        return RiskPlan(False, f"stop {pts:.1f} points is too far for a scalp ({cfg.max_stop_points:.0f} max)")
    min_broker = spec.min_stop_distance_price(spread_price) if hasattr(spec, "min_stop_distance_price") else 0.0
    if min_broker and dist < min_broker:
        return RiskPlan(False, "stop closer than the broker's minimum distance")
    slip = cfg.slippage_allowance_points * spec.point * 2      # entry and exit
    effective = dist + spread_price + slip
    per_lot = spec.money_per_lot(effective) + cfg.commission_per_lot_round_turn
    if per_lot <= 0:
        return RiskPlan(False, "cannot value the stop distance")
    budget = cfg.planned_max_trade_risk_gbp
    raw = budget / per_lot
    step = spec.volume_step or 0.01
    volume = math.floor(raw / step + 1e-9) * step
    volume = round(volume, 8)
    if volume < spec.volume_min:
        return RiskPlan(False, f"the broker's minimum size ({spec.volume_min:g} lots) would risk "
                               f"{per_lot * spec.volume_min:.2f}, over the {budget:.0f} ceiling")
    volume = min(volume, spec.volume_max)
    planned = per_lot * volume
    if planned > budget + 1e-9:
        return RiskPlan(False, "planned loss exceeds the ceiling after rounding")
    return RiskPlan(True, "", side, entry, invalidation, pts, effective / spec.point, volume,
                    per_lot, planned, cfg.commission_per_lot_round_turn * volume,
                    spec.money_per_lot(spread_price) * volume,
                    spec.money_per_lot(slip) * volume)


# ---------------------------------------------------------------- account --
def account_safety(cfg: ScalperConfig, account: Optional[AccountInfo],
                   existing_positions: Sequence[Position],
                   scalper_positions: Sequence[Position],
                   existing_open_risk: float, scalper_open_risk: float,
                   scalper_day_pnl: float, account_day_pnl: float,
                   symbol: str, side: Side) -> tuple[bool, str]:
    """May BLOCK a new scalper trade. Never touches the other strategy."""
    if account is None:
        return False, "account state unknown"
    if not account.trade_allowed:
        return False, "trading is disabled on the account or terminal"
    if account.margin_level and account.margin_level < cfg.min_margin_level_pct:
        return False, f"margin level {account.margin_level:.0f}% below {cfg.min_margin_level_pct:.0f}%"
    total = existing_open_risk + scalper_open_risk + cfg.planned_max_trade_risk_gbp
    if total > cfg.max_total_open_risk_gbp:
        return False, f"total open risk would be {total:.0f}, over {cfg.max_total_open_risk_gbp:.0f}"
    if scalper_day_pnl <= -cfg.max_daily_loss_gbp:
        return False, f"Rapid Scalper daily loss limit reached ({scalper_day_pnl:+.2f})"
    if account_day_pnl <= -cfg.max_account_daily_loss_gbp:
        return False, f"whole-account daily loss limit reached ({account_day_pnl:+.2f})"
    for p in existing_positions:
        if p.symbol == symbol:
            return False, f"the other strategy already has {symbol} open - no doubling up"
    if len(scalper_positions) >= cfg.max_open_positions:
        return False, "already at the Rapid Scalper position limit"
    return True, ""


# --------------------------------------------------------- circuit breakers --
@dataclass
class Breakers:
    cfg: ScalperConfig
    consecutive_losses: int = 0
    day: Optional[dt.date] = None
    day_pnl: float = 0.0
    api_errors: deque = field(default_factory=deque)
    rejects: deque = field(default_factory=deque)
    slippage: deque = field(default_factory=deque)
    reconciliation_ok: bool = True
    tripped: dict = field(default_factory=dict)      # name -> reason (manual reset needed)

    def _roll(self, now: dt.datetime) -> None:
        if self.day != now.date():
            self.day = now.date()
            self.day_pnl = 0.0
            self.consecutive_losses = 0
        cutoff = now - dt.timedelta(hours=1)
        for q in (self.api_errors, self.rejects):
            while q and q[0] < cutoff:
                q.popleft()

    def record_result(self, net: float, now: dt.datetime) -> None:
        self._roll(now)
        self.day_pnl += net
        self.consecutive_losses = self.consecutive_losses + 1 if net < 0 else 0

    def record_api_error(self, now: dt.datetime) -> None:
        self._roll(now); self.api_errors.append(now)

    def record_reject(self, now: dt.datetime) -> None:
        self._roll(now); self.rejects.append(now)

    def record_slippage(self, points: float) -> None:
        self.slippage.append(abs(points))
        while len(self.slippage) > self.cfg.slippage_window_trades:
            self.slippage.popleft()

    def avg_slippage(self) -> float:
        return sum(self.slippage) / len(self.slippage) if self.slippage else 0.0

    def check(self, now: dt.datetime, *, tick_age_seconds: Optional[float],
              connected: bool, latency_ms: float, spread_points: Optional[float] = None,
              duplicate_suspected: bool = False) -> list[str]:
        """Every reason NOT to open a trade right now. Empty means go."""
        self._roll(now)
        c = self.cfg
        why: list[str] = []
        if self.tripped:
            why.extend(f"tripped: {v}" for v in self.tripped.values())
        if not connected:
            why.append("broker not connected")
        if tick_age_seconds is None:
            why.append("no market data")
        elif tick_age_seconds > c.stale_tick_seconds:
            why.append(f"market data stale ({tick_age_seconds:.0f}s)")
        if latency_ms > c.max_latency_ms:
            why.append(f"latency {latency_ms:.0f}ms too high")
        if self.consecutive_losses >= c.max_consecutive_losses:
            why.append(f"{self.consecutive_losses} losses in a row - paused for the day")
        if self.day_pnl <= -c.max_daily_loss_gbp:
            why.append(f"daily loss limit reached ({self.day_pnl:+.2f})")
        if len(self.slippage) >= min(3, c.slippage_window_trades) and self.avg_slippage() > c.max_avg_slippage_points:
            why.append(f"execution quality poor: average slippage {self.avg_slippage():.1f} points")
        if spread_points is not None and spread_points > c.max_spread_points:
            why.append(f"spread {spread_points:.1f} points abnormal")
        if len(self.api_errors) >= c.max_api_errors_per_hour:
            why.append("too many API errors this hour")
        if len(self.rejects) >= c.max_rejects_per_hour:
            why.append("too many rejected orders this hour")
        if not self.reconciliation_ok:
            why.append("position state could not be reconciled with the broker")
        if duplicate_suspected:
            why.append("a duplicate order is suspected")
        return why
