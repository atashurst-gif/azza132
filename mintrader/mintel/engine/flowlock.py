"""FlowLock - stateful profit management.

Purpose, in order of priority:

1. Cut failed trades quickly.
2. Let exceptional trades run.
3. Lock profit as the move dies.

The mechanism is a state machine over the *behaviour of the move*, not over
elapsed time or fixed pip targets:

``INITIAL``      the trade has not proven anything yet; the stop stays where
                 structure put it.
``PROVING``      evidence is improving; the stop starts to follow.
``STRONG_FLOW``  momentum is exceptional; the stop is deliberately kept WIDE,
                 because strangling a trend is the most expensive mistake a
                 trailing stop can make.
``NORMAL_FLOW``  the move is working but ordinary; protect progressively.
``DECAY``        the move has stopped extending; tighten hard.
``REVERSAL``     the thesis has broken; close.

The one invariant, enforced here and re-checked in the executor: **the stop
never widens.**  For a long it may only stay or rise; for a short only stay or
fall.
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Sequence

from ..broker.base import Bar, Position, Side
from ..clock import to_utc, utcnow
from ..config import FlowLockConfig
from ..contracts import SymbolSpec
from ..data.momentum import MomentumReading
from ..data.structure import StructureReading

log = logging.getLogger("mintel.flowlock")


class FlowState(str, Enum):
    INITIAL = "INITIAL"
    PROVING = "PROVING"
    STRONG_FLOW = "STRONG_FLOW"
    NORMAL_FLOW = "NORMAL_FLOW"
    DECAY = "DECAY"
    REVERSAL = "REVERSAL"


@dataclass
class TradeTracker:
    """Per-position running state.  Persisted so a restart does not forget."""
    ticket: int
    symbol: str
    side: Side
    volume: float
    entry: float
    initial_stop: float
    initial_risk: float
    opened_utc: dt.datetime
    state: FlowState = FlowState.INITIAL
    stop: float = 0.0
    mfe: float = 0.0                 # best favourable excursion, price units
    mae: float = 0.0                 # worst adverse excursion, price units
    mfe_r: float = 0.0
    mae_r: float = 0.0
    best_price: float = 0.0
    worst_price: float = 0.0
    bars_since_extension: int = 0
    last_extension_utc: Optional[dt.datetime] = None
    last_bar_time: Optional[dt.datetime] = None
    partial_done: bool = False
    partial_volume: float = 0.0
    thesis_strength: float = 60.0
    updates: int = 0
    breakeven_done: bool = False
    state_history: tuple[str, ...] = ()

    def r_of(self, price: float) -> float:
        if self.initial_risk <= 0:
            return 0.0
        return ((price - self.entry) * self.side.sign) / self.initial_risk

    def to_row(self) -> dict:
        return {
            "ticket": self.ticket, "symbol": self.symbol,
            "side": self.side.value, "volume": self.volume,
            "entry": self.entry,
            "initial_stop": self.initial_stop,
            "initial_risk": self.initial_risk, "state": self.state.value,
            "stop": self.stop, "mfe": self.mfe, "mae": self.mae,
            "mfe_r": round(self.mfe_r, 4), "mae_r": round(self.mae_r, 4),
            "partial_done": self.partial_done,
            "thesis_strength": round(self.thesis_strength, 2),
            "updates": self.updates,
            "state_history": list(self.state_history),
        }


@dataclass
class FlowDecision:
    state: FlowState
    new_stop: Optional[float]
    close: bool
    close_reason: str = ""
    partial_volume: float = 0.0
    reason: str = ""
    r_now: float = 0.0
    mfe_r: float = 0.0
    stop_moved: bool = False


class FlowLock:
    def __init__(self, cfg: Optional[FlowLockConfig] = None):
        self.cfg = cfg or FlowLockConfig()
        self.trackers: dict[int, TradeTracker] = {}

    # ------------------------------------------------------------- lifecycle --
    def adopt(self, position: Position, initial_stop: float,
              now: Optional[dt.datetime] = None) -> TradeTracker:
        """Start (or resume) tracking a position.

        Called for new fills and for positions adopted during recovery, so a
        restart mid-trade continues managing rather than abandoning.
        """
        existing = self.trackers.get(position.ticket)
        if existing is not None:
            return existing
        stop = initial_stop or position.sl or 0.0
        risk = abs(position.entry_price - stop) if stop else 0.0
        t = TradeTracker(
            ticket=position.ticket, symbol=position.symbol, side=position.side,
            volume=position.volume,
            entry=position.entry_price, initial_stop=stop, initial_risk=risk,
            opened_utc=to_utc(now or position.open_time or utcnow()),
            stop=position.sl or stop, best_price=position.entry_price,
            worst_price=position.entry_price)
        self.trackers[position.ticket] = t
        return t

    def forget(self, ticket: int) -> Optional[TradeTracker]:
        return self.trackers.pop(ticket, None)

    def restore(self, trackers: Sequence[TradeTracker]) -> None:
        for t in trackers:
            self.trackers[t.ticket] = t

    # -------------------------------------------------------------- excursion --
    def _update_excursions(self, t: TradeTracker, bar: Optional[Bar],
                           price: float, now: dt.datetime) -> bool:
        """Update MFE/MAE.  Returns True when a new favourable extreme printed.

        Bar extremes are used when available: a spike that reached 3R and came
        back was still a 3R excursion, and pretending otherwise would make the
        MFE-capture statistics flattering and useless.

        Two rules learned on day four. A bar that OPENED before the trade did
        contributes nothing: its high and low include prices from before the
        entry, which once credited a 3-minute trade with a +4R "best" and
        pushed the trail up under the live price. And the stall counter
        advances once per NEW bar, not once per management cycle: counted
        per cycle it declared a trade stale after twenty seconds and clipped
        every winner at half its target.
        """
        new_bar = bar is not None and bar.time != t.last_bar_time
        if bar is not None:
            t.last_bar_time = bar.time
            opened = t.opened_utc
            bar_t = bar.time if bar.time.tzinfo else bar.time.replace(tzinfo=dt.timezone.utc)
            if opened is not None and opened.tzinfo is None:
                opened = opened.replace(tzinfo=dt.timezone.utc)
            if opened is not None and bar_t < opened:
                bar = None                       # opened before the trade
        if t.side is Side.BUY:
            high = max(price, bar.high) if bar else price
            low = min(price, bar.low) if bar else price
            extended = high > t.best_price
            t.best_price = max(t.best_price, high)
            t.worst_price = min(t.worst_price or low, low)
            t.mfe = max(0.0, t.best_price - t.entry)
            t.mae = max(0.0, t.entry - t.worst_price)
        else:
            high = max(price, bar.high) if bar else price
            low = min(price, bar.low) if bar else price
            extended = low < (t.worst_price if t.best_price == 0 else t.best_price)
            t.best_price = min(t.best_price or low, low)
            t.worst_price = max(t.worst_price, high)
            t.mfe = max(0.0, t.entry - t.best_price)
            t.mae = max(0.0, t.worst_price - t.entry)
        if t.initial_risk > 0:
            t.mfe_r = t.mfe / t.initial_risk
            t.mae_r = t.mae / t.initial_risk
        if extended:
            t.bars_since_extension = 0
            t.last_extension_utc = now
        elif new_bar:
            t.bars_since_extension += 1
        return extended

    # -------------------------------------------------------------- decision --
    def update(self, position: Position, *, price: float,
               atr: float, bar: Optional[Bar] = None,
               momentum: Optional[MomentumReading] = None,
               structure: Optional[StructureReading] = None,
               thesis_strength: Optional[float] = None,
               now: Optional[dt.datetime] = None) -> FlowDecision:
        """One management cycle for one position."""
        now = to_utc(now or utcnow())
        t = self.trackers.get(position.ticket) or self.adopt(position,
                                                             position.sl, now)
        t.updates += 1
        if thesis_strength is not None:
            t.thesis_strength = thesis_strength
        if position.sl:
            # Trust the broker's value: it may have been tightened elsewhere.
            if (t.side is Side.BUY and position.sl > t.stop) or \
               (t.side is Side.SELL and (t.stop == 0 or position.sl < t.stop)):
                t.stop = position.sl
        self._update_excursions(t, bar, price, now)

        r_now = t.r_of(price)
        atr = max(atr, 1e-12)
        new_state = self._classify(t, r_now, momentum, structure, atr)
        if new_state is not t.state:
            t.state_history = (t.state_history
                               + (f"{now.isoformat()} {t.state.value}->"
                                  f"{new_state.value}",))[-12:]
            t.state = new_state

        if new_state is FlowState.REVERSAL:
            return FlowDecision(new_state, None, True,
                                close_reason="thesis broke: "
                                             + self._reversal_reason(t, momentum),
                                reason="closing on a broken thesis",
                                r_now=r_now, mfe_r=t.mfe_r)

        candidate = self._stop_for(t, price, atr, structure, r_now)
        moved = False
        if candidate is not None:
            candidate = self._monotonic(t, candidate)
            if candidate is not None:
                t.stop = candidate
                moved = True

        partial = 0.0
        if (self.cfg.partial_enabled and not t.partial_done
                and r_now >= self.cfg.partial_at_r
                and t.state in (FlowState.NORMAL_FLOW, FlowState.STRONG_FLOW,
                                FlowState.PROVING)):
            partial = round(position.volume * self.cfg.partial_fraction, 8)
            if partial > 0:
                t.partial_done = True
                t.partial_volume = partial

        return FlowDecision(
            state=new_state, new_stop=t.stop if moved else None, close=False,
            partial_volume=partial,
            reason=self._explain(t, r_now, momentum), r_now=r_now,
            mfe_r=t.mfe_r, stop_moved=moved)

    # -------------------------------------------------------- state machine --
    def _classify(self, t: TradeTracker, r_now: float,
                  momentum: Optional[MomentumReading],
                  structure: Optional[StructureReading],
                  atr: float) -> FlowState:
        c = self.cfg
        if t.thesis_strength <= c.reversal_thesis_floor:
            return FlowState.REVERSAL
        # Structure breaking against an open trade is a thesis break, not a
        # reason to wait for the stop.
        if structure is not None:
            against = (structure.bos_down if t.side is Side.BUY
                       else structure.bos_up)
            if against and structure.bos_bars_ago is not None \
                    and structure.bos_bars_ago <= 2 \
                    and r_now < c.structure_break_max_r:
                return FlowState.REVERSAL

        aligned = momentum is not None and momentum.direction == t.side.sign
        strong = bool(aligned and momentum
                      and momentum.efficiency >= 0.34
                      and momentum.adx >= 24
                      and (momentum.acceleration_atr * t.side.sign) > -0.05)

        if r_now < c.proving_r and t.mfe_r < c.proving_r:
            return FlowState.INITIAL
        if t.bars_since_extension >= c.stall_bars_to_decay:
            return FlowState.DECAY
        if momentum is not None and not aligned and t.mfe_r >= 1.0:
            return FlowState.DECAY
        if strong and t.mfe_r >= 1.0:
            return FlowState.STRONG_FLOW
        if t.mfe_r >= c.proving_r and t.mfe_r < 1.0:
            return FlowState.PROVING
        return FlowState.NORMAL_FLOW

    def _reversal_reason(self, t: TradeTracker,
                         momentum: Optional[MomentumReading]) -> str:
        if t.thesis_strength <= self.cfg.reversal_thesis_floor:
            return (f"the reasons for the trade have gone "
                    f"(thesis {t.thesis_strength:.0f}/100)")
        return "market structure broke against the position"

    def _stop_for(self, t: TradeTracker, price: float, atr: float,
                  structure: Optional[StructureReading],
                  r_now: float) -> Optional[float]:
        """Candidate stop for the current state.

        Three references are considered and the *tightest sensible* one used per
        state: an ATR trail, a swing-based trail, and an MFE-giveback level.
        STRONG_FLOW deliberately chooses the loosest of them.
        """
        c = self.cfg
        sign = t.side.sign
        if t.state is FlowState.INITIAL:
            return None                      # let the structural stop work
        if t.state is FlowState.PROVING and not getattr(c, "proving_trail", True):
            return None                      # nothing is trailed before +1R

        atr_mult = {
            FlowState.PROVING: c.normal_flow_atr,
            FlowState.STRONG_FLOW: c.strong_flow_atr,
            FlowState.NORMAL_FLOW: c.normal_flow_atr,
            FlowState.DECAY: c.decay_atr,
        }.get(t.state, c.normal_flow_atr)
        atr_stop = price - sign * atr_mult * atr

        giveback = {
            FlowState.PROVING: c.mfe_giveback_normal,
            FlowState.STRONG_FLOW: c.mfe_giveback_strong,
            FlowState.NORMAL_FLOW: c.mfe_giveback_normal,
            FlowState.DECAY: c.mfe_giveback_decay,
        }.get(t.state, c.mfe_giveback_normal)
        mfe_stop = None
        if t.mfe > 0:
            best = t.entry + sign * t.mfe
            mfe_stop = best - sign * giveback * t.mfe

        swing_stop = None
        if structure is not None:
            sw = (structure.last_swing_low if t.side is Side.BUY
                  else structure.last_swing_high)
            if sw is not None:
                cand = sw.price - sign * 0.25 * atr
                # only usable if it is actually behind current price
                if (price - cand) * sign > 0:
                    swing_stop = cand

        options = [x for x in (atr_stop, mfe_stop, swing_stop) if x is not None]
        if not options:
            return None
        if t.state is FlowState.STRONG_FLOW:
            # Deliberately the most generous option: an exceptional move is
            # given room to breathe rather than being clipped at the first
            # pullback.
            chosen = min(options) if t.side is Side.BUY else max(options)
        elif t.state is FlowState.DECAY:
            chosen = max(options) if t.side is Side.BUY else min(options)
        else:
            options.sort(reverse=(t.side is Side.SELL))
            chosen = options[len(options) // 2]      # the middle reference

        # Break-even protection, but only once the trade has earned it: moving
        # to break-even too early converts winners into scratches.
        if (not t.breakeven_done and r_now >= c.breakeven_at_r
                and t.state is not FlowState.INITIAL):
            be = t.entry + sign * 0.05 * t.initial_risk
            chosen = max(chosen, be) if t.side is Side.BUY else min(chosen, be)
            t.breakeven_done = True

        # Never propose a stop through current price.
        if (price - chosen) * sign <= 0:
            return None
        return chosen

    def _monotonic(self, t: TradeTracker, candidate: float) -> Optional[float]:
        """The invariant: a stop may tighten, never loosen."""
        if not t.stop:
            return candidate
        if t.side is Side.BUY:
            return candidate if candidate > t.stop else None
        return candidate if candidate < t.stop else None

    def _explain(self, t: TradeTracker, r_now: float,
                 momentum: Optional[MomentumReading]) -> str:
        bits = [f"{t.state.value.replace('_', ' ').lower()}",
                f"{r_now:+.2f}R now, best {t.mfe_r:.2f}R"]
        if t.state is FlowState.STRONG_FLOW:
            bits.append("momentum is exceptional, so the stop is being kept "
                        "wide on purpose to let it run")
        elif t.state is FlowState.DECAY:
            bits.append(f"no new extreme for {t.bars_since_extension} bars, "
                        f"so profit is being locked in aggressively")
        elif t.state is FlowState.INITIAL:
            bits.append("the trade has not proven itself yet; the structural "
                        "stop stands")
        if t.partial_done:
            bits.append("part of the position has already been banked")
        return "; ".join(bits)

    # ------------------------------------------------------------ statistics --
    def capture_stats(self, t: TradeTracker, exit_price: float) -> dict:
        """MFE capture: did the exit respect what the trade actually offered?

        The realised move is folded into MFE first.  By definition the maximum
        favourable excursion cannot be smaller than what the trade actually
        banked, and management runs on a timer, so a position that ran to its
        target between two cycles would otherwise report an MFE below its own
        result and a capture figure above 100% - or, for a loser that closed
        beyond its recorded best, a nonsensical negative one.
        """
        realised = (exit_price - t.entry) * t.side.sign
        mfe = max(t.mfe, max(0.0, realised))
        mae = max(t.mae, max(0.0, -realised))
        realised_r = realised / t.initial_risk if t.initial_risk > 0 else 0.0
        mfe_r = mfe / t.initial_risk if t.initial_risk > 0 else 0.0
        mae_r = mae / t.initial_risk if t.initial_risk > 0 else 0.0
        # Capture is only meaningful for a trade that went in our favour at all.
        capture = (max(0.0, realised) / mfe * 100.0) if mfe > 0 else 0.0
        return {"realised": realised, "realised_r": round(realised_r, 4),
                "mfe": mfe, "mfe_r": round(mfe_r, 4), "mae": mae,
                "mae_r": round(mae_r, 4),
                "mfe_capture_pct": round(min(capture, 100.0), 2),
                "final_state": t.state.value}
