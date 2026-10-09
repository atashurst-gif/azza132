"""The order-flow trailing engine: ride strong flow, protect more as it fades.

No fixed target. The stop trails the best price reached by a distance that
shrinks as the order flow behind the trade deteriorates:

    trail distance = f x R, R = the initial risk distance (entry to first stop)

    f = 1.00   flow still with the trade (support >= 0.3)
        0.70   aggression weakening (support < 0.3)
        0.45   imbalance reversing against the trade (imbalance_with < -0.2)
        0.30   the passive side absorbing the trade's flow, or liquidity
               regenerating in the way (opposite_absorption or regeneration >= 0.5)

    once the trade has been 1 R in profit the stop is at least at entry plus
    lock_r x R (costs covered); exit at once when the THESIS is dead (flow
    strongly against: support <= -0.6 with imbalance_with <= -0.4, or
    opposite absorption >= 0.8, or the caller says so).

THE STOP NEVER WIDENS. Every candidate passes through :func:`tighten_only`;
a candidate that would move the stop away from the price is discarded, and so
is one that the broker could not accept (closer to the price than its minimum
distance) - in that case the old stop simply stays. tests/test_ian_trail.py
proves it over thousands of random price and flow paths.

Everything here is in the SPOT trade's own direction: the engine translates
the futures flow first (an inverted pair flips every sign).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ..broker.base import Side


@dataclass
class FlowRead:
    """The futures flow, already turned into the trade's direction. All in [-1, 1] (or [0, 1])."""
    support: float = 0.0               # aggression and OFI with the trade (+) or against it (-)
    imbalance_with: float = 0.0        # book imbalance with the trade
    opposite_absorption: float = 0.0   # 0..1: the trade's flow being absorbed by the other side
    regeneration_against: float = 0.0  # 0..1: liquidity coming back in the trade's way
    thesis_dead: bool = False
    available: bool = True             # False when the book is not trustworthy (DEGRADED): mechanical rules only
    note: str = ""


@dataclass
class TrailConfig:
    lock_after_r: float = 1.0
    lock_r: float = 0.1
    f_strong: float = 1.0
    f_weakening: float = 0.7
    f_reversing: float = 0.45
    f_hostile: float = 0.30
    f_blind: float = 0.8               # no reliable flow: a plain trail, a little tighter than the strong one
    min_step_points: float = 1.0       # do not send a modification smaller than this


@dataclass
class TrailDecision:
    stop: float                        # the stop after this update (== the old one when unchanged)
    moved: bool
    exit: bool
    reason: str
    factor: float = 1.0


def tighten_only(side: Side, current: float, candidate: Optional[float]) -> float:
    """The one gate every stop change goes through: a long's stop may only rise, a short's only fall."""
    if candidate is None:
        return current
    if not current:
        return candidate
    return max(current, candidate) if side is Side.BUY else min(current, candidate)


class FlowTrail:
    def __init__(self, side: Side, entry: float, stop: float, cfg: Optional[TrailConfig] = None, point: float = 0.0):
        if (entry - stop) * side.sign <= 0:
            raise ValueError("the initial stop must be on the losing side of the entry")
        self.side = side
        self.entry = float(entry)
        self.stop = float(stop)
        self.initial_stop = float(stop)
        self.r = abs(self.entry - self.stop)
        self.cfg = cfg or TrailConfig()
        self.point = float(point)
        self.best = self.entry                 # best price reached in the trade's favour
        self.worst = self.entry
        self.factor = 1.0

    def r_of(self, price: float) -> float:
        return (price - self.entry) * self.side.sign / self.r if self.r else 0.0

    @property
    def mfe_r(self) -> float:
        return self.r_of(self.best)

    @property
    def mae_r(self) -> float:
        return self.r_of(self.worst)

    def _factor(self, flow: FlowRead) -> tuple[float, str]:
        c = self.cfg
        if not flow.available:
            return c.f_blind, "no reliable order book: a plain trail"
        if flow.opposite_absorption >= 0.5 or flow.regeneration_against >= 0.5:
            return c.f_hostile, ("the other side is absorbing the move" if flow.opposite_absorption >= 0.5
                                 else "liquidity is regenerating in the way")
        if flow.imbalance_with < -0.2:
            return c.f_reversing, "the book has turned against the trade"
        if flow.support < 0.3:
            return c.f_weakening, "the aggression behind the trade is weakening"
        return c.f_strong, "the flow is still with the trade"

    def update(self, mark: float, flow: Optional[FlowRead] = None, min_distance: float = 0.0) -> TrailDecision:
        """``mark`` is the price the position would close at (bid for a long, ask for a short);
        ``min_distance`` is the broker's minimum stop distance from it."""
        flow = flow or FlowRead(available=False)
        s = self.side.sign
        if (mark - self.best) * s > 0:
            self.best = mark
        if (mark - self.worst) * s < 0:
            self.worst = mark
        if flow.available and (flow.thesis_dead or (flow.support <= -0.6 and flow.imbalance_with <= -0.4)
                               or flow.opposite_absorption >= 0.8):
            why = ("the thesis is dead" if flow.thesis_dead else
                   "the order flow has turned hard against the trade" if flow.support <= -0.6 else
                   "the trade's flow is being absorbed")
            return TrailDecision(self.stop, False, True, why + (f" ({flow.note})" if flow.note else ""), self.factor)
        f, why = self._factor(flow)
        self.factor = f
        cand = self.best - s * f * self.r
        if self.mfe_r >= self.cfg.lock_after_r:
            lock = self.entry + s * self.cfg.lock_r * self.r
            cand = lock if (cand - lock) * s < 0 else cand
        # never inside the broker's minimum distance: such a stop would be refused (or hit at once)
        if min_distance > 0 and (mark - cand) * s < min_distance:
            cand = mark - s * min_distance
        new = tighten_only(self.side, self.stop, cand)
        step = abs(new - self.stop)
        if new != self.stop and step + 1e-12 >= self.cfg.min_step_points * self.point:
            old = self.stop
            self.stop = new
            return TrailDecision(new, True, False, f"stop {old} -> {new}: {why}", f)
        return TrailDecision(self.stop, False, False, why, f)
