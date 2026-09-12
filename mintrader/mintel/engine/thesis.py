"""Continuous position thesis.

A position is not a fire-and-forget bet.  Every management cycle the reasons
that justified the trade are re-measured against the *current* world:

structure, momentum, multi-timeframe agreement, currency strength, cross-market
confirmation, news, execution conditions.

The result is ``THESIS_STRENGTH`` in 0..100, which drives FlowLock:

* improving  -> the winner is given room;
* deteriorating -> the stop tightens;
* broken     -> the position is closed, without waiting to be stopped out.

The thesis is compared with what was true at entry, so *deterioration* is
measured rather than merely low absolute values - a trade entered in a
mediocre-but-acceptable environment is not closed just because the environment
is still mediocre.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Optional

from ..broker.base import Position, Side
from .evidence import Family, MarketState, SymbolContext


# Which families matter for keeping a trade alive, and how much.  Note this is
# NOT the entry weighting: headroom and session liquidity matter much less once
# you are already in, while structure and momentum matter more.
HOLD_WEIGHTS: dict[Family, float] = {
    Family.STRUCTURE: 0.26,
    Family.MOMENTUM: 0.24,
    Family.MULTI_TF: 0.14,
    Family.CROSS_MARKET: 0.12,
    Family.NEWS: 0.10,
    Family.PARTICIPATION: 0.06,
    Family.VOLATILITY: 0.04,
    Family.EXECUTION: 0.04,
}


@dataclass
class ThesisReading:
    strength: float                 # 0..100
    entry_strength: float
    delta: float                    # strength - entry_strength
    verdict: str                    # IMPROVED | INTACT | WEAKENING | BROKEN
    components: dict[str, float] = field(default_factory=dict)
    reasons: tuple[str, ...] = ()

    @property
    def broken(self) -> bool:
        return self.verdict == "BROKEN"

    def describe(self) -> str:
        word = {"IMPROVED": "has got stronger",
                "INTACT": "still holds",
                "WEAKENING": "is weakening",
                "BROKEN": "has broken"}.get(self.verdict, "is unclear")
        base = f"The reason for this trade {word} ({self.strength:.0f}/100"
        if self.entry_strength:
            base += f", was {self.entry_strength:.0f} at entry"
        base += ")."
        if self.reasons:
            base += " " + " ".join(self.reasons)
        return base


class ThesisTracker:
    """Holds the entry snapshot for each position and re-scores it live."""

    def __init__(self):
        self.entry_state: dict[int, MarketState] = {}
        self.entry_strength: dict[int, float] = {}
        self.history: dict[int, list[tuple[dt.datetime, float]]] = {}

    def register(self, ticket: int, state: MarketState) -> float:
        strength = self._score(state.evidence)
        self.entry_state[ticket] = state
        self.entry_strength[ticket] = strength
        self.history[ticket] = []
        return strength

    def forget(self, ticket: int) -> None:
        self.entry_state.pop(ticket, None)
        self.entry_strength.pop(ticket, None)
        self.history.pop(ticket, None)

    @staticmethod
    def _score(evidence: dict[Family, "object"]) -> float:
        num = den = 0.0
        for fam, w in HOLD_WEIGHTS.items():
            ev = evidence.get(fam)
            if ev is None:
                continue
            num += w * ev.effective
            den += w
        return round(num / den, 2) if den else 50.0

    def evaluate(self, position: Position, state: Optional[MarketState],
                 now: Optional[dt.datetime] = None,
                 r_now: float = 0.0) -> ThesisReading:
        """Re-score the held direction.

        ``state`` must be the MarketState for the direction the position is
        *in* - scoring the opposite direction would invert every conclusion.
        """
        ticket = position.ticket
        entry = self.entry_strength.get(ticket, 55.0)
        if state is None:
            # No fresh read (data gap): report INTACT rather than inventing
            # deterioration, and let the health layer handle the data problem.
            return ThesisReading(entry, entry, 0.0, "INTACT", {},
                                 ("no fresh analysis available this cycle; "
                                  "holding the existing view.",))
        strength = self._score(state.evidence)
        delta = strength - entry
        comps = {f.value: round(state.family(f), 1) for f in HOLD_WEIGHTS
                 if f in state.evidence}

        reasons: list[str] = []
        structure = state.family(Family.STRUCTURE)
        momentum = state.family(Family.MOMENTUM)

        # BROKEN needs a real, specific failure - not merely a soft score.
        broken = False
        if structure <= 30.0:
            broken = True
            reasons.append("Market structure has turned against the position.")
        if momentum <= 25.0 and r_now < 0.5:
            broken = True
            reasons.append("Momentum has flipped against it while the trade is "
                           "not yet meaningfully in profit.")
        if state.blockers and any("spread" in b for b in state.blockers):
            reasons.append("Trading conditions have deteriorated "
                           "(spread widened sharply).")
        news = state.evidence.get(Family.NEWS)
        if news is not None and news.effective <= 25.0:
            broken = broken or r_now < 0.3
            reasons.append("Fresh news now argues against this direction.")

        if broken:
            verdict = "BROKEN"
        elif delta >= 6.0:
            verdict = "IMPROVED"
            reasons.append("The evidence has strengthened since entry, so the "
                           "trade is being given more room.")
        elif delta <= -12.0 or strength < 42.0:
            verdict = "WEAKENING"
            reasons.append("The evidence is fading, so profit is being "
                           "protected more tightly.")
        else:
            verdict = "INTACT"

        hist = self.history.setdefault(ticket, [])
        hist.append((now or dt.datetime.now(dt.timezone.utc), strength))
        if len(hist) > 240:
            del hist[:-240]

        return ThesisReading(strength=strength, entry_strength=entry,
                             delta=round(delta, 2), verdict=verdict,
                             components=comps, reasons=tuple(reasons))
