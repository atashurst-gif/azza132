"""Order execution and state reconciliation.

Two hard rules:

1. **The broker is the source of truth.**  After any restart, crash or
   reconnection, local state is rebuilt from live positions and deal history.
   Local memory is a cache, never an authority.
2. **An API call that returned is not a trade that happened.**  Every send is
   checked by return code, and every intent is tracked through the chain
   INTENT -> ORDER -> DEAL -> POSITION so a lost response can never become a
   duplicate position.

Idempotency
-----------
Each intent gets a deterministic key derived from symbol, side, the decision
minute and the stop price.  The key is:

* recorded in the intent ledger *before* the order is sent, so a crash between
  send and response is detectable;
* written into the order comment, so the *broker's own records* can be searched
  for it during recovery.  That is what makes recovery work even when our
  database is lost.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional, Sequence

from ..broker.base import (Broker, FATAL_FOR_SETUP, OrderRequest, OrderResult,
                           Position, RetCode, Side, Tick)
from ..clock import to_utc, utcnow
from ..config import Config
from ..contracts import SymbolSpec
from .evidence import MarketState

log = logging.getLogger("mintel.exec")


class IntentState(str, Enum):
    CREATED = "CREATED"
    SENT = "SENT"
    FILLED = "FILLED"
    PARTIAL = "PARTIAL"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"      # sent, no confirmed outcome - must be reconciled
    ABANDONED = "ABANDONED"


@dataclass
class Intent:
    key: str
    symbol: str
    side: Side
    volume: float
    sl: float
    tp: float
    created_utc: dt.datetime
    state: IntentState = IntentState.CREATED
    attempts: int = 0
    ticket: int = 0
    deal: int = 0
    filled_volume: float = 0.0
    fill_price: float = 0.0
    last_retcode: int = 0
    last_comment: str = ""
    opportunity: float = 0.0
    tactic: str = ""

    @property
    def resolved(self) -> bool:
        return self.state in (IntentState.FILLED, IntentState.PARTIAL,
                              IntentState.REJECTED, IntentState.ABANDONED)


def make_key(state: MarketState, spec: SymbolSpec, *,
             bucket_seconds: int = 60, salt: str = "") -> str:
    """Deterministic idempotency key.

    Two scans a few seconds apart that reach the same conclusion produce the
    same key and therefore cannot open two positions.  A genuinely different
    idea - different side, materially different stop, or a later time bucket -
    produces a different key, which is what keeps legitimate re-entry possible.
    """
    bucket = int(to_utc(state.as_of).timestamp()) // bucket_seconds
    stop_points = round(state.stop / max(spec.point, 1e-12))
    raw = (f"{state.symbol}|{state.side.value}|{bucket}|{stop_points}|"
           f"{state.tactic}|{salt}")
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


@dataclass
class ExecutionReport:
    ok: bool
    intent: Intent
    position: Optional[Position]
    message: str
    protective_stop_confirmed: bool = False
    attempts: int = 0


class Executor:
    """Sends orders safely and keeps local state honest."""

    def __init__(self, broker: Broker, cfg: Config, journal=None,
                 risk=None, clock: Callable[[], dt.datetime] = utcnow):
        self.broker = broker
        self.cfg = cfg
        self.journal = journal
        self.risk = risk
        self.clock = clock
        self.intents: dict[str, Intent] = {}
        self.last_error = ""
        self._cooldowns: dict[tuple[str, str], dt.datetime] = {}

    # ------------------------------------------------------------- cooldowns --
    def cooldown_remaining(self, symbol: str, side: Side) -> float:
        until = self._cooldowns.get((symbol, side.value))
        if until is None:
            return 0.0
        return max(0.0, (until - to_invariant(self.clock())).total_seconds())

    def set_cooldown(self, symbol: str, side: Side, seconds: float) -> None:
        self._cooldowns[(symbol, side.value)] = (
            to_invariant(self.clock()) + dt.timedelta(seconds=seconds))

    # ------------------------------------------------------------ submission --
    def already_submitted(self, key: str) -> Optional[Intent]:
        it = self.intents.get(key)
        if it is not None:
            return it
        if self.journal is not None:
            try:
                row = self.journal.find_intent(key)
                if row is not None:
                    return row
            except Exception:
                pass
        return None

    def submit(self, state: MarketState, spec: SymbolSpec, volume: float,
               *, max_attempts: int = 3,
               deviation_points: int = 0) -> ExecutionReport:
        """Send one entry, then verify the broker actually holds a stop.

        Requote-class rejections are retried with a refreshed price; everything
        else fails fast.  After a fill the protective stop is confirmed and, if
        the broker dropped it, re-applied immediately - a position without a
        broker-side stop is the single most dangerous state this system can be
        in.
        """
        key = make_key(state, spec)
        existing = self.already_submitted(key)
        if existing is not None and existing.state is not IntentState.REJECTED:
            return ExecutionReport(
                False, existing, None,
                f"this exact trade was already submitted "
                f"(state {existing.state.value}) - not sending it twice")

        cd = self.cooldown_remaining(state.symbol, state.side)
        if cd > 0:
            intent = Intent(key, state.symbol, state.side, volume, state.stop,
                            state.target or 0.0, to_invariant(self.clock()),
                            IntentState.ABANDONED)
            return ExecutionReport(False, intent, None,
                                   f"cooling down for another {cd:.0f}s on "
                                   f"{state.symbol} {state.side.value}")

        intent = Intent(key=key, symbol=state.symbol, side=state.side,
                        volume=volume, sl=state.stop, tp=state.target or 0.0,
                        created_utc=to_invariant(self.clock()),
                        opportunity=state.opportunity, tactic=state.tactic)
        self.intents[key] = intent
        self._persist(intent)

        dev = deviation_points or max(
            10, int(round((state.cost_pips * spec.points_per_pip) * 2)))
        result: Optional[OrderResult] = None
        for attempt in range(1, max_attempts + 1):
            intent.attempts = attempt
            # Mark UNKNOWN before the call: if the process dies here, recovery
            # knows an order may exist at the broker.
            intent.state = IntentState.UNKNOWN
            self._persist(intent)
            req = OrderRequest(
                symbol=state.symbol, side=state.side, volume=volume,
                sl=state.stop, tp=state.target or 0.0,
                deviation_points=dev, comment=key,
                magic=self.cfg.magic, idempotency_key=key,
                filling=spec.filling_modes[0] if spec.filling_modes else "IOC")
            try:
                result = self.broker.send(req)
            except Exception as exc:
                self.last_error = str(exc)
                intent.last_comment = str(exc)
                log.error("send failed for %s: %s", state.symbol, exc)
                # State stays UNKNOWN: the order may have reached the broker.
                self._persist(intent)
                return ExecutionReport(False, intent, None,
                                       f"order could not be sent: {exc}")
            intent.last_retcode = result.retcode
            intent.last_comment = result.comment
            if result.ok:
                break
            if self.risk is not None:
                self.risk.note_reject(to_invariant(self.clock()))
            if result.code in FATAL_FOR_SETUP or not result.retryable:
                intent.state = IntentState.REJECTED
                self._persist(intent)
                self.set_cooldown(state.symbol, state.side,
                                  self.cfg.scan.cooldown_seconds_after_reject)
                return ExecutionReport(
                    False, intent, None,
                    f"broker rejected the order ({result.code.name})"
                    + (f": {result.comment}" if result.comment else ""),
                    attempts=attempt)
            # Retryable: refresh the price and try again shortly.
            time.sleep(min(0.25 * attempt, 1.0))

        if result is None or not result.ok:
            intent.state = IntentState.REJECTED
            self._persist(intent)
            return ExecutionReport(False, intent, None,
                                   f"order not accepted after {max_attempts} "
                                   f"attempts (last code "
                                   f"{RetCode.of(intent.last_retcode).name})")

        intent.ticket = result.ticket
        intent.deal = result.deal
        intent.filled_volume = result.filled_volume
        intent.fill_price = result.price
        intent.state = (IntentState.PARTIAL if result.partial
                        else IntentState.FILLED)
        self._persist(intent)
        if self.risk is not None:
            self.risk.note_order(to_invariant(self.clock()))

        position = self._find_position(result, state, key)
        stop_ok = False
        if position is not None:
            intent.ticket = position.ticket
            stop_ok = self._ensure_protective_stop(position, state.stop)
            self._persist(intent)
        msg = (f"filled {result.filled_volume:g} lots at {result.price}"
               + (" (PARTIAL fill)" if result.partial else ""))
        if position is None:
            msg += " but no position could be matched - reconciliation needed"
        elif not stop_ok:
            msg += " - WARNING: protective stop not confirmed at the broker"
        return ExecutionReport(position is not None, intent, position, msg,
                               stop_ok, intent.attempts)

    def _find_position(self, result: OrderResult, state: MarketState,
                       key: str) -> Optional[Position]:
        """Match the fill to a live position.

        MT5 returns an *order* ticket, which is not always the position ticket,
        so the search falls back to the comment (carrying the idempotency key)
        and then to symbol/side.
        """
        try:
            positions = self.broker.positions(self.cfg.magic)
        except Exception:
            return None
        for p in positions:
            if p.ticket == result.ticket:
                return p
        for p in positions:
            if key and key in (p.comment or ""):
                return p
        cands = [p for p in positions
                 if p.symbol == state.symbol and p.side is state.side]
        return cands[-1] if cands else None

    def _ensure_protective_stop(self, position: Position, stop: float,
                                attempts: int = 3) -> bool:
        """Guarantee the broker holds a catastrophic stop for this position.

        This does not depend on the bot staying alive: once the broker has the
        stop, a crashed process, a dead VPS or a severed network cannot leave
        the position unprotected.
        """
        for i in range(attempts):
            try:
                live = next((p for p in self.broker.positions(self.cfg.magic)
                             if p.ticket == position.ticket), None)
            except Exception:
                live = None
            if live is not None and live.sl:
                position.sl = live.sl
                return True
            try:
                res = self.broker.modify_stops(position.ticket, stop,
                                               position.tp or 0.0)
                if res.ok:
                    position.sl = stop
                    return True
                log.warning("could not set the protective stop on %s: %s",
                            position.ticket, RetCode.of(res.retcode).name)
            except Exception as exc:
                log.error("stop placement raised for %s: %s",
                          position.ticket, exc)
            time.sleep(0.2 * (i + 1))
        return False

    def enforce_stops(self, positions: Sequence[Position],
                      fallback_atr: dict[str, float] | None = None) -> list[str]:
        """Sweep every position and make sure each has a broker-side stop.

        Called on every management cycle and immediately after recovery.  A
        position found naked gets a volatility-based catastrophic stop at once -
        an imperfect stop is enormously better than none.
        """
        problems: list[str] = []
        for p in positions:
            if p.sl:
                continue
            spec = self.broker.spec(p.symbol)
            if spec is None:
                problems.append(f"{p.symbol}: no specification, cannot place "
                                f"a stop")
                continue
            atr = (fallback_atr or {}).get(p.symbol)
            if not atr:
                try:
                    from ..data.series import atr as atr_fn
                    from ..broker.base import TF
                    atr = atr_fn(self.broker.bars(p.symbol, TF.M15, 100), 14)
                except Exception:
                    atr = 0.0
            tick = self.broker.tick(p.symbol)
            spread = tick.spread if tick else 0.0
            distance = max(2.0 * (atr or 0.0),
                           spec.min_stop_distance_price(spread, 2.0))
            px = p.entry_price if tick is None else (
                tick.bid if p.side is Side.BUY else tick.ask)
            stop = spec.normalise_price(px - p.side.sign * distance)
            if self._ensure_protective_stop(p, stop):
                log.warning("placed an emergency protective stop on %s at %s",
                            p.symbol, stop)
            else:
                problems.append(f"{p.symbol} ticket {p.ticket} has NO stop at "
                                f"the broker and one could not be placed")
        return problems

    # --------------------------------------------------------- reconciliation --
    def reconcile(self) -> dict:
        """Rebuild local truth from the broker.

        Run at start-up, after every reconnection and after any UNKNOWN intent.
        Resolves three dangerous situations:

        * an intent marked UNKNOWN that actually filled  -> adopt the position;
        * an intent marked UNKNOWN that never filled     -> mark abandoned;
        * a live position the bot has no record of        -> adopt and protect.
        """
        out = {"adopted": [], "abandoned": [], "orphans": [], "naked": []}
        try:
            positions = self.broker.positions(self.cfg.magic)
        except Exception as exc:
            self.last_error = f"reconcile failed: {exc}"
            out["error"] = str(exc)
            return out

        by_comment = {}
        for p in positions:
            if p.comment:
                by_comment[p.comment.strip()] = p

        if self.journal is not None:
            try:
                for it in self.journal.unresolved_intents():
                    self.intents.setdefault(it.key, it)
            except Exception:
                pass

        for key, intent in list(self.intents.items()):
            if intent.state is not IntentState.UNKNOWN:
                continue
            match = by_comment.get(key) or next(
                (p for p in positions if key in (p.comment or "")), None)
            if match is not None:
                intent.state = IntentState.FILLED
                intent.ticket = match.ticket
                intent.filled_volume = match.volume
                intent.fill_price = match.entry_price
                out["adopted"].append({"key": key, "ticket": match.ticket})
            else:
                # Nothing at the broker carries this key, so the order never
                # became a position.  Marking it abandoned is what prevents the
                # next scan from treating it as "already open".
                intent.state = IntentState.ABANDONED
                out["abandoned"].append(key)
            self._persist(intent)

        known = {i.ticket for i in self.intents.values() if i.ticket}
        for p in positions:
            if p.ticket not in known:
                out["orphans"].append({"ticket": p.ticket, "symbol": p.symbol,
                                       "volume": p.volume,
                                       "side": p.side.value})
                self.intents[f"adopted:{p.ticket}"] = Intent(
                    key=f"adopted:{p.ticket}", symbol=p.symbol, side=p.side,
                    volume=p.volume, sl=p.sl, tp=p.tp,
                    created_utc=to_invariant(self.clock()),
                    state=IntentState.FILLED, ticket=p.ticket,
                    filled_volume=p.volume, fill_price=p.entry_price)
            if not p.sl:
                out["naked"].append(p.ticket)
        if out["naked"]:
            out["naked_fixed"] = self.enforce_stops(
                [p for p in positions if not p.sl])
        return out

    # ----------------------------------------------------------------- exits --
    def close_position(self, position: Position, reason: str,
                       volume: float = 0.0) -> OrderResult:
        try:
            res = self.broker.close(position.ticket, volume, reason[:31])
        except Exception as exc:
            self.last_error = str(exc)
            return OrderResult(False, RetCode.CONNECTION.value,
                               comment=str(exc), ticket=position.ticket)
        if res.ok:
            self.set_cooldown(position.symbol, position.side,
                              self.cfg.scan.cooldown_seconds_after_exit)
            if self.risk is not None:
                self.risk.forget(position.ticket)
        return res

    def move_stop(self, position: Position, new_stop: float,
                  spec: SymbolSpec) -> Optional[OrderResult]:
        """Tighten a stop.  Refuses to widen it, ever.

        This is the last line of defence behind FlowLock's own monotonic logic:
        even a bug upstream cannot loosen a stop through this method.
        """
        if new_stop <= 0:
            return None
        new_stop = spec.normalise_price(new_stop)
        if position.sl:
            if position.side is Side.BUY and new_stop <= position.sl:
                return None
            if position.side is Side.SELL and new_stop >= position.sl:
                return None
        step = abs(new_stop - (position.sl or position.entry_price))
        if position.sl and step < self.cfg.flowlock.min_stop_step_points * spec.point:
            return None
        try:
            res = self.broker.modify_stops(position.ticket, new_stop,
                                           position.tp or 0.0)
        except Exception as exc:
            self.last_error = str(exc)
            return OrderResult(False, RetCode.CONNECTION.value,
                               comment=str(exc), ticket=position.ticket)
        if res.ok:
            position.sl = new_stop
        return res

    # ------------------------------------------------------------ persistence --
    def _persist(self, intent: Intent) -> None:
        if self.journal is None:
            return
        try:
            self.journal.save_intent(intent)
        except Exception as exc:
            log.warning("could not persist intent %s: %s", intent.key, exc)


def to_invariant(ts: dt.datetime) -> dt.datetime:
    return to_utc(ts) if ts.tzinfo else ts.replace(tzinfo=dt.timezone.utc)
