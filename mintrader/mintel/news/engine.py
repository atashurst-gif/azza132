"""NewsIntelligenceEngine.

The rule this module exists to enforce: **news provides information, price
provides the verdict.**

A release generates a *hypothesis* (see :mod:`mintel.news.surprise`).  The
engine then watches what the market actually did - direction, spread behaviour,
tick participation, persistence, cross-market agreement - and produces a
:class:`NewsVerdict`.  If theoretically dollar-bullish data lands and the dollar
is sold across the board, the verdict follows the selling.  We never fight the
tape because a textbook says we should be right.

Wave model
----------
``PENDING``     release imminent - no new entries (execution is unpriceable)
``FIRST_SPIKE`` the initial jump; usually chaos, wide spreads, frequently
                reversed.  Entries here require exceptional execution quality.
``FIRST_WAVE``  the initial move has held and spreads normalised - tradable
``SECOND_WAVE`` slower real participation joins; often the better entry
``REJECTED``    the initial move has been given back entirely
``WHIPSAW``     both directions traded through; no verdict
``STALE``       the reaction window has passed; news is context only
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Sequence

from ..broker.base import Bar, CalendarEvent, Tick
from ..clock import UTC, to_utc, utcnow
from ..config import NewsConfig
from ..contracts import SymbolSpec, classify_fx
from ..data.series import atr
from .adapters import AdapterRegistry, Headline
from .calendar_store import CalendarStore
from .surprise import SurpriseReading, measure as measure_surprise

log = logging.getLogger("mintel.news")


class Wave(str, Enum):
    NONE = "NONE"
    PENDING = "PENDING"
    FIRST_SPIKE = "FIRST_SPIKE"
    FIRST_WAVE = "FIRST_WAVE"
    SECOND_WAVE = "SECOND_WAVE"
    REJECTED = "REJECTED"
    WHIPSAW = "WHIPSAW"
    STALE = "STALE"


@dataclass
class Reaction:
    """What price actually did after a release, measured on the instrument."""
    symbol: str
    jump_atr: float              # size of the initial move, in ATR
    direction: int               # sign of the initial move
    current_atr: float           # where price is now relative to release price
    persistence: float           # 0..1, fraction of the jump still held
    spread_ratio: float          # current spread / typical spread
    spread_normalised: bool
    participation: float         # tick volume vs median
    whipsawed: bool
    bars_since: int

    @property
    def net_direction(self) -> int:
        if abs(self.current_atr) < 0.1:
            return 0
        return 1 if self.current_atr > 0 else -1


@dataclass
class NewsVerdict:
    symbol: str
    wave: Wave
    event: Optional[CalendarEvent]
    surprise: Optional[SurpriseReading]
    reaction: Optional[Reaction]
    bias: int                    # -1/0/+1 - the direction news SUPPORTS
    support: float               # 0..100 score for trading WITH `bias`
    blackout: bool               # refuse new entries right now
    reason: str
    headlines: tuple[Headline, ...] = ()
    hypothesis_contradicted: bool = False

    def score_for(self, side_sign: int) -> float:
        """News evidence score for a given trade direction.

        Neutral 50 when news has nothing to say - the absence of a catalyst is
        not a reason to avoid an otherwise good chart.
        """
        if self.bias == 0 or self.wave in (Wave.NONE, Wave.STALE, Wave.WHIPSAW):
            return 50.0
        return self.support if self.bias == side_sign else 100.0 - self.support


@dataclass
class EngineHealth:
    last_refresh_utc: Optional[dt.datetime] = None
    last_error: str = ""
    events_known: int = 0
    adapters_healthy: int = 0
    adapters_total: int = 0
    store_healthy: bool = True
    degraded_reason: str = ""

    def stale(self, max_age: float, now: Optional[dt.datetime] = None) -> bool:
        if self.last_refresh_utc is None:
            return True
        return (to_utc(now or utcnow())
                - self.last_refresh_utc).total_seconds() > max_age

    @property
    def healthy(self) -> bool:
        return (self.store_healthy
                and (self.adapters_total == 0 or self.adapters_healthy > 0))


class NewsIntelligenceEngine:
    """Scheduled-calendar intelligence plus live price-reaction reading."""

    def __init__(self, registry: AdapterRegistry, store: CalendarStore,
                 cfg: Optional[NewsConfig] = None):
        self.registry = registry
        self.store = store
        self.cfg = cfg or NewsConfig()
        self.health = EngineHealth()
        self._events: list[CalendarEvent] = []
        self._headlines: list[Headline] = []
        self._release_price: dict[tuple[str, str], float] = {}

    # ------------------------------------------------------------- refresh --
    def refresh(self, now: Optional[dt.datetime] = None,
                window_hours: int = 36, *, force: bool = False) -> bool:
        """Pull the calendar, archive it, and refresh headlines.

        Always non-fatal.  On total failure the engine falls back to whatever is
        already in the local store, and records *why* it is degraded so the
        dashboard can say so in plain English.
        """
        now = to_utc(now or utcnow())
        start = now - dt.timedelta(hours=window_hours)
        end = now + dt.timedelta(hours=window_hours)
        errors: list[str] = []
        events: list[CalendarEvent] = []
        try:
            events, errors = self.registry.fetch_calendar(start, end,
                                                          force=force)
        except Exception as exc:
            errors.append(str(exc))
        if events:
            try:
                self.store.upsert(events)
            except Exception as exc:
                errors.append(f"store: {exc}")
                self.health.store_healthy = self.store.reconnect()
        # The store is authoritative for reads, so a live-feed failure still
        # leaves us with the last good calendar rather than a blank one.
        try:
            self._events = self.store.query(start, end)
            self.health.store_healthy = True
        except Exception as exc:
            errors.append(f"store read: {exc}")
            self.health.store_healthy = self.store.reconnect()
            self._events = events

        try:
            self._headlines = self.registry.fetch_headlines(
                now - dt.timedelta(hours=6),
                allow_social=self.cfg.allow_social_sources)
        except Exception as exc:
            errors.append(f"headlines: {exc}")

        healths = self.registry.health()
        self.health.adapters_total = len(healths)
        self.health.adapters_healthy = sum(1 for h in healths if h.healthy)
        self.health.events_known = len(self._events)
        self.health.last_error = "; ".join(errors)
        if not errors:
            # A window with no releases in it is a perfectly good answer; only
            # an actual failure should make the feed look stale.
            self.health.last_refresh_utc = now
        elif events or self._events:
            self.health.last_refresh_utc = now
        self.health.degraded_reason = ""
        stale_sources = getattr(self.registry, "serving_stale", ())
        if stale_sources:
            self.health.degraded_reason = (
                "news source(s) " + ", ".join(stale_sources)
                + " are failing; serving their last good data")
        if errors and self._events:
            self.health.degraded_reason = (
                "live news feed failing; using stored calendar")
        elif errors:
            self.health.degraded_reason = "no calendar data available"
        return not errors

    # -------------------------------------------------------------- lookup --
    def upcoming(self, currencies: Sequence[str], now: dt.datetime,
                 within_seconds: float = 3600.0,
                 min_importance: int = 2) -> list[CalendarEvent]:
        now = to_utc(now)
        cs = {c.upper() for c in currencies}
        return [e for e in self._events
                if e.currency in cs and e.importance >= min_importance
                and 0 <= (e.time_utc - now).total_seconds() <= within_seconds]

    def recent(self, currencies: Sequence[str], now: dt.datetime,
               within_seconds: float = 1800.0,
               min_importance: int = 2) -> list[CalendarEvent]:
        now = to_utc(now)
        cs = {c.upper() for c in currencies}
        out = [e for e in self._events
               if e.currency in cs and e.importance >= min_importance
               and 0 <= (now - e.time_utc).total_seconds() <= within_seconds]
        return sorted(out, key=lambda e: e.time_utc, reverse=True)

    def currencies_for(self, symbol: str, spec: Optional[SymbolSpec] = None) -> tuple[str, ...]:
        base, quote, _ = classify_fx(symbol)
        if base and quote:
            return (base, quote)
        if spec is not None:
            out = tuple(c for c in (spec.base_currency, spec.profit_currency) if c)
            if out:
                return out
        # Indices and metals are dominated by USD-denominated macro.
        return ("USD",)

    # ------------------------------------------------------------- reaction --
    def read_reaction(self, symbol: str, event: CalendarEvent,
                      bars: Sequence[Bar], tick: Optional[Tick],
                      spec: SymbolSpec, now: dt.datetime) -> Optional[Reaction]:
        """Measure the instrument's behaviour since ``event``.

        The release-time price is captured once and cached so the measurement
        does not drift as bars roll.
        """
        now = to_utc(now)
        if not bars:
            return None
        a = atr(bars[-min(len(bars), 200):], 14) or 1e-12
        after = [b for b in bars if b.time >= event.time_utc]
        if not after:
            return None
        key = (symbol, event.event_id + event.time_utc.isoformat())
        before = [b for b in bars if b.time < event.time_utc]
        release_px = self._release_price.get(key)
        if release_px is None:
            release_px = before[-1].close if before else after[0].open
            self._release_price[key] = release_px

        first = after[0]
        jump = (first.close - release_px)
        if abs(jump) < 1e-12 and len(after) > 1:
            jump = after[1].close - release_px
        direction = 1 if jump > 0 else (-1 if jump < 0 else 0)
        current = (after[-1].close - release_px)
        persistence = 0.0
        if abs(jump) > 1e-12:
            persistence = max(0.0, min(1.5, (current * direction) / abs(jump)))

        highest = max(b.high for b in after)
        lowest = min(b.low for b in after)
        whipsawed = ((highest - release_px) > 0.5 * a
                     and (release_px - lowest) > 0.5 * a)

        typical = max(spec.typical_spread_points * spec.point, spec.point)
        spread_ratio = (tick.spread / typical) if tick else 1.0
        vols = [b.tick_volume for b in bars[-100:]]
        med = sorted(vols)[len(vols) // 2] if vols else 0.0
        participation = (after[-1].tick_volume / med) if med > 0 else 1.0

        return Reaction(
            symbol=symbol, jump_atr=abs(jump) / a, direction=direction,
            current_atr=current / a, persistence=persistence,
            spread_ratio=spread_ratio,
            spread_normalised=spread_ratio <= 1.8,
            participation=participation, whipsawed=whipsawed,
            bars_since=len(after))

    def _wave(self, event: CalendarEvent, reaction: Optional[Reaction],
              now: dt.datetime, bar_seconds: float) -> Wave:
        secs = (to_utc(now) - event.time_utc).total_seconds()
        if secs < -self.cfg.blackout_before_seconds:
            return Wave.NONE
        if secs < 0:
            return Wave.PENDING
        if reaction is None:
            return Wave.PENDING if secs < 60 else Wave.STALE
        if reaction.whipsawed and reaction.persistence < 0.35:
            return Wave.WHIPSAW
        if secs <= self.cfg.blackout_after_seconds or not reaction.spread_normalised:
            return Wave.FIRST_SPIKE
        if secs > self.cfg.second_wave_window_seconds:
            return Wave.STALE
        if reaction.jump_atr >= 0.4 and reaction.persistence < 0.25:
            return Wave.REJECTED
        # A held move with renewed participation later in the window is the
        # second wave: slower, broader, and usually the better entry.
        if (secs > max(180.0, 3 * bar_seconds) and reaction.persistence >= 0.6
                and reaction.participation >= 1.2):
            return Wave.SECOND_WAVE
        return Wave.FIRST_WAVE

    # -------------------------------------------------------------- verdict --
    def verdict(self, symbol: str, bars: Sequence[Bar], tick: Optional[Tick],
                spec: SymbolSpec, now: dt.datetime,
                bar_seconds: float = 60.0,
                breadth_direction: Optional[int] = None) -> NewsVerdict:
        """The engine's opinion on ``symbol`` right now.

        ``breadth_direction`` is the cross-market/currency-strength direction
        for the currency in question, used to check whether the reaction is
        broad-based or instrument-specific.
        """
        now = to_utc(now)
        ccys = self.currencies_for(symbol, spec)

        if not self.cfg.enabled:
            return NewsVerdict(symbol, Wave.NONE, None, None, None, 0, 50.0,
                               False, "news engine disabled")

        # --- imminent high-importance release: refuse new entries ------------
        soon = self.upcoming(ccys, now, self.cfg.blackout_before_seconds,
                             self.cfg.min_importance_for_blackout)
        if soon:
            ev = soon[0]
            return NewsVerdict(symbol, Wave.PENDING, ev, None, None, 0, 50.0,
                               True,
                               f"{ev.currency} {ev.name} due in "
                               f"{int((ev.time_utc - now).total_seconds())}s - "
                               f"execution unpriceable, no new entries")

        recent = self.recent(ccys, now,
                             self.cfg.second_wave_window_seconds + 60, 2)
        recent = [e for e in recent if e.actual is not None] or recent
        if not recent:
            return NewsVerdict(symbol, Wave.NONE, None, None, None, 0, 50.0,
                               False, "no recent scheduled release",
                               headlines=tuple(self._relevant_headlines(ccys, now)))

        event = recent[0]
        history = []
        try:
            history = self.store.historical_values(event.name, event.currency,
                                                   event.time_utc)
        except Exception:
            pass
        surprise = measure_surprise(event, history)
        reaction = self.read_reaction(symbol, event, bars, tick, spec, now)
        wave = self._wave(event, reaction, now, bar_seconds)

        blackout = wave is Wave.PENDING or (
            wave is Wave.FIRST_SPIKE and not (reaction and reaction.spread_normalised))

        # --- whose currency moved, and which way does that point? -----------
        base, quote, _ = classify_fx(symbol)
        ccy_sign = 0
        if surprise.hypothesis_sign and base and quote:
            if event.currency == base:
                ccy_sign = surprise.hypothesis_sign
            elif event.currency == quote:
                ccy_sign = -surprise.hypothesis_sign
        elif surprise.hypothesis_sign and not base:
            # Index/metal: strong dollar is a headwind, so invert.
            ccy_sign = -surprise.hypothesis_sign if event.currency == "USD" else 0

        price_sign = reaction.net_direction if reaction else 0
        contradicted = bool(ccy_sign and price_sign and ccy_sign != price_sign)

        # PRICE OUTRANKS THE HEADLINE.  When they disagree, the tape wins and
        # the fundamental hypothesis is recorded as contradicted.
        bias = price_sign or ccy_sign

        support = self._support_score(surprise, reaction, wave, contradicted,
                                      ccy_sign, price_sign, breadth_direction,
                                      bias)
        reason = self._explain(event, surprise, reaction, wave, contradicted)

        if wave in (Wave.WHIPSAW, Wave.REJECTED):
            if wave is Wave.REJECTED and reaction and reaction.direction:
                # A fully rejected impulse is information in its own right: the
                # failed direction becomes the tradable one, but only with
                # modest support until structure confirms.
                bias = -reaction.direction
                support = min(support, 66.0)
            else:
                bias, support = 0, 50.0

        return NewsVerdict(symbol=symbol, wave=wave, event=event,
                           surprise=surprise, reaction=reaction, bias=bias,
                           support=round(support, 1), blackout=blackout,
                           reason=reason,
                           headlines=tuple(self._relevant_headlines(ccys, now)),
                           hypothesis_contradicted=contradicted)

    def _support_score(self, surprise: SurpriseReading,
                       reaction: Optional[Reaction], wave: Wave,
                       contradicted: bool, ccy_sign: int, price_sign: int,
                       breadth_direction: Optional[int], bias: int) -> float:
        s = 50.0
        if reaction is None:
            return s
        s += 22.0 * min(1.0, reaction.jump_atr / 1.2)
        s += 16.0 * min(1.0, reaction.persistence)
        s += 8.0 * min(1.0, max(0.0, reaction.participation - 1.0) / 1.0)
        if not reaction.spread_normalised:
            s -= 14.0
        if surprise.is_surprise:
            s += 10.0 * surprise.magnitude
        if ccy_sign and price_sign and ccy_sign == price_sign:
            s += 8.0                      # data and tape agree
        if contradicted:
            s -= 6.0                      # tradable, but less trustworthy
        if breadth_direction is not None and bias:
            s += 8.0 if breadth_direction == bias else -10.0
        if wave is Wave.SECOND_WAVE:
            s += 6.0
        elif wave is Wave.FIRST_SPIKE:
            s -= 12.0
        elif wave is Wave.STALE:
            s -= 18.0
        return max(0.0, min(100.0, s))

    def _explain(self, event: CalendarEvent, surprise: SurpriseReading,
                 reaction: Optional[Reaction], wave: Wave,
                 contradicted: bool) -> str:
        bits = [surprise.describe()]
        if reaction:
            moved = "rose" if reaction.current_atr > 0 else "fell"
            bits.append(f"price {moved} {abs(reaction.current_atr):.1f} ATR "
                        f"since the release, holding "
                        f"{reaction.persistence * 100:.0f}% of the initial move")
            if not reaction.spread_normalised:
                bits.append(f"spread still {reaction.spread_ratio:.1f}x normal")
        bits.append(f"wave: {wave.value}")
        if contradicted:
            bits.append("the market moved AGAINST the textbook reading, so we "
                        "follow price, not the headline")
        return "; ".join(bits)

    def _relevant_headlines(self, ccys: Sequence[str],
                            now: dt.datetime) -> list[Headline]:
        cs = {c.upper() for c in ccys}
        cutoff = to_utc(now) - dt.timedelta(hours=3)
        return [h for h in self._headlines
                if h.time_utc >= cutoff and (not h.currencies
                                             or cs & set(h.currencies))][:5]
