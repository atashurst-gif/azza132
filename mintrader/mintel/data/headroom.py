"""Structural headroom.

Before entering, ask the question a human asks last and an algorithm usually
never asks at all: *what is price about to run into?*

A textbook-perfect long with a heavily respected daily resistance three pips
overhead is a bad trade.  The same setup with clear air for 40 pips is a good
one.  This module measures that distance and converts it into a multiplier and
a realistic first target.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Iterable, Optional, Sequence

from ..contracts import SymbolSpec
from .structure import Level


@dataclass
class Obstacle:
    price: float
    kind: str
    strength: float
    distance: float
    distance_atr: float
    distance_pips: float


@dataclass
class HeadroomReading:
    direction: int
    price: float
    atr: float
    nearest: Optional[Obstacle]
    weighted_clearance_atr: float
    obstacles: list[Obstacle] = field(default_factory=list)

    @property
    def headroom_pips(self) -> float:
        return self.nearest.distance_pips if self.nearest else float("inf")

    @property
    def headroom_atr(self) -> float:
        return self.nearest.distance_atr if self.nearest else float("inf")

    def multiplier(self) -> float:
        """Opportunity multiplier in [0.55, 1.12].

        Below ~0.5 ATR of clearance the trade is penalised hard; beyond ~2 ATR
        it gets a small bonus.  A *weak* level (one touch, minor timeframe) is
        discounted, so the engine is not scared out of trades by every minor
        swing high.
        """
        if self.nearest is None:
            return 1.10
        clear = self.weighted_clearance_atr
        if clear <= 0.25:
            return 0.55
        if clear >= 2.0:
            return 1.12
        return 0.55 + (clear - 0.25) / (2.0 - 0.25) * (1.12 - 0.55)

    def first_meaningful(self, min_strength: float = 0.45) -> Optional[Obstacle]:
        """Nearest obstacle strong enough to actually stop a move.

        Single-touch minor swings litter every chart; treating the first of them
        as the profit objective would make almost every trade look like a 0.3:1
        proposition and the bot would never trade anything.
        """
        for o in self.obstacles:
            if o.strength >= min_strength:
                return o
        return None

    # Only a genuinely respected barrier caps a profit objective.  Round
    # numbers and VWAP are everywhere and do stop moves sometimes, so they
    # belong in the headroom PENALTY (via `multiplier`), not in the target -
    # capping every target at the next figure would make every trade look
    # unprofitable before it started.
    TARGET_STRENGTH = 0.70

    def realistic_target(self, objective_atr: float = 2.0,
                         min_strength: Optional[float] = None) -> Optional[float]:
        """First profit objective.

        Whichever comes first: a genuinely respected level, or a
        volatility-scaled objective sized to the regime.  Stopping short of the
        obstacle by a fraction of ATR keeps the order out of the queue that
        forms exactly at the level.  If the barrier sits closer than 0.8 ATR
        there is no trade worth taking here, and the resulting reward:risk is
        left to fail honestly rather than being massaged upward.
        """
        strength = self.TARGET_STRENGTH if min_strength is None else min_strength
        obstacle = self.first_meaningful(strength)
        vol_distance = objective_atr * self.atr
        if obstacle is not None and obstacle.distance < vol_distance:
            pad = 0.15 * self.atr
            dist = max(obstacle.distance - pad, 0.0)
        else:
            dist = vol_distance
        return self.price + self.direction * dist


def round_numbers(price: float, spec: SymbolSpec, direction: int,
                  count: int = 2) -> list[float]:
    """Psychological round levels above/below price.

    Grid size is chosen per asset class: 50 pips for FX (the classic 00/50
    figure), whole dollars for gold, hundreds for indices.
    """
    if spec.asset_group.startswith("FX"):
        grid = spec.pip_size * 50.0
    elif spec.asset_group in ("GOLD",):
        grid = 5.0
    elif spec.asset_group in ("SILVER",):
        grid = 0.5
    elif spec.asset_group in ("INDEX",):
        grid = 100.0
    else:
        grid = 10 ** math.floor(math.log10(max(price, 1e-9))) / 10.0
    out = []
    base = math.floor(price / grid) * grid
    for i in range(count + 1):
        lv = base + grid * (i + 1) if direction > 0 else base - grid * i
        if (lv - price) * direction > 0:
            out.append(round(lv, spec.digits))
    return out[:count]


def measure(price: float, direction: int, atr_value: float, spec: SymbolSpec,
            levels: Iterable[Level] = (),
            named_levels: Iterable[tuple[str, float]] = (),
            vwap: Optional[float] = None,
            max_atr: float = 6.0) -> HeadroomReading:
    """Collect every obstacle ahead within ``max_atr`` and rank by proximity.

    ``weighted_clearance_atr`` divides raw distance by the obstacle's strength,
    so a weak level 1 ATR away reads like clear air while a heavily respected
    level at the same distance reads as a wall.
    """
    atr_value = max(atr_value, 1e-12)
    obstacles: list[Obstacle] = []

    STRENGTH_BY_KIND = {
        "PDH": 0.85, "PDL": 0.85,
        "ASIA_HIGH": 0.55, "ASIA_LOW": 0.55,
        "LONDON_HIGH": 0.8, "LONDON_LOW": 0.8,
        "NEWYORK_HIGH": 0.75, "NEWYORK_LOW": 0.75,
        "US_CASH_HIGH": 0.7, "US_CASH_LOW": 0.7,
        "ROUND": 0.45, "VWAP": 0.5,
    }

    def add(p: float, kind: str, strength: float) -> None:
        d = (p - price) * (1 if direction > 0 else -1)
        if d <= 0:
            return
        d_atr = d / atr_value
        if d_atr > max_atr:
            return
        obstacles.append(Obstacle(price=p, kind=kind, strength=strength,
                                  distance=d, distance_atr=d_atr,
                                  distance_pips=spec.price_to_pips(d)))

    for lv in levels:
        add(lv.price, lv.kind, max(0.25, lv.strength))
    for name, p in named_levels:
        base = name.split("_")[0]
        add(p, name, STRENGTH_BY_KIND.get(name, STRENGTH_BY_KIND.get(base, 0.5)))
    for p in round_numbers(price, spec, direction):
        add(p, "ROUND", STRENGTH_BY_KIND["ROUND"])
    if vwap is not None:
        add(vwap, "VWAP", STRENGTH_BY_KIND["VWAP"])

    obstacles.sort(key=lambda o: o.distance)
    # Ignore anything so close it is effectively current price (inside spread /
    # sub-tick noise) - it is not an obstacle, it is where we already are.
    meaningful = [o for o in obstacles if o.distance_atr > 0.05]
    nearest = None
    clearance = max_atr
    if meaningful:
        # The binding obstacle is the one with the smallest strength-weighted
        # clearance, which is not always the physically nearest one.
        scored = [(o.distance_atr / max(o.strength, 0.2), o) for o in meaningful]
        scored.sort(key=lambda t: t[0])
        clearance, nearest = scored[0]
        clearance = min(clearance, max_atr)
    return HeadroomReading(direction=direction, price=price, atr=atr_value,
                           nearest=nearest, weighted_clearance_atr=clearance,
                           obstacles=meaningful[:10])
