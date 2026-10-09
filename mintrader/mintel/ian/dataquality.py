"""Is the order-book data good enough to trade on?

Per instrument, and for the feed as a whole, the monitor watches:

* sequence gaps (per instrument, where the feed numbers each instrument's
  messages contiguously; vendor-reported gaps on any feed),
* out-of-order events (a sequence or exchange time going backwards),
* timestamp problems (received before the exchange stamped it, beyond a
  clock-skew allowance; or impossibly far in the future),
* latency (received minus exchange time),
* stale book and stale trades (no update for too long),
* missing depth (fewer than ``min_levels`` on a side) and a crossed book,
* the feed's own state (disconnected, not configured).

and reports LIVE / DEGRADED / DOWN with the reasons in plain English.
DEGRADED means no new signals. After a fault the instrument stays DEGRADED
until ``recovery_events`` clean events have arrived over at least
``recovery_seconds`` - a book that has just had a gap may be wrong until it
has been rebuilt - and the engine asks the feed to recover (reconnect,
resubscribe, re-snapshot), with a back-off, while it waits.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Optional

from .book import BookEvent, FeedNotice, OrderBook, TradeEvent, exchange_time, received_at

LIVE = "LIVE"
DEGRADED = "DEGRADED"
DOWN = "DOWN"
NOT_CONFIGURED = "NOT CONFIGURED"


@dataclass
class QualityConfig:
    stale_book_s: float = 10.0
    stale_trades_s: float = 120.0
    down_after_s: float = 60.0
    max_latency_ms: float = 1500.0
    clock_skew_ms: float = 250.0
    future_tolerance_s: float = 5.0
    min_levels: int = 3
    recovery_events: int = 200
    recovery_seconds: float = 5.0
    recover_backoff_s: float = 5.0
    recover_backoff_max_s: float = 120.0
    # how far an event's exchange time may go back before it counts as out of order. 1 ms for a feed whose
    # events come in exchange order (CME); a feed that sends the book in 100 ms batches and the trades on a
    # separate stream (Binance) says so with its own figure (FeedAdapter.quality_overrides)
    order_tolerance_ms: float = 1.0

    @classmethod
    def for_feed(cls, overrides: Optional[dict] = None) -> "QualityConfig":
        """The defaults, with the feed's own figures for the fields it names (unknown names ignored)."""
        c = cls()
        for k, v in dict(overrides or {}).items():
            if hasattr(c, k):
                try:
                    setattr(c, k, type(getattr(c, k))(v))
                except (TypeError, ValueError):
                    pass
        return c


@dataclass
class QualityState:
    state: str
    reasons: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.state == LIVE

    def text(self) -> str:
        return self.state + (": " + "; ".join(self.reasons) if self.reasons else "")


@dataclass
class _Inst:
    last_seq: int = 0
    last_exchange: Optional[dt.datetime] = None
    last_book_rx: Optional[dt.datetime] = None
    last_trade_rx: Optional[dt.datetime] = None
    last_rx: Optional[dt.datetime] = None
    fault: str = ""
    fault_at: Optional[dt.datetime] = None
    clean_events: int = 0
    clean_since: Optional[dt.datetime] = None
    latency_ms: float = 0.0
    latency_max_ms: float = 0.0
    gaps: int = 0
    out_of_order: int = 0
    timestamp_errors: int = 0
    events: int = 0


class DataQualityMonitor:
    def __init__(self, cfg: Optional[QualityConfig] = None, sequence_scope: str = "instrument"):
        self.cfg = cfg or QualityConfig()
        self.sequence_scope = sequence_scope
        self.inst: dict[str, _Inst] = {}
        self.feed_fault: str = ""
        self._next_recover: dict[str, dt.datetime] = {}
        self._backoff: dict[str, float] = {}
        self.recover_attempts = 0

    def _get(self, instrument: str) -> _Inst:
        st = self.inst.get(instrument)
        if st is None:
            st = self.inst[instrument] = _Inst()
        return st

    def _fault(self, st: _Inst, why: str, at: dt.datetime) -> None:
        st.fault = why
        st.fault_at = at
        st.clean_events = 0
        st.clean_since = None

    def observe(self, ev) -> None:
        """Look at one event BEFORE it is applied to the book."""
        if isinstance(ev, FeedNotice):
            self._notice(ev)
            return
        rx = received_at(ev)
        ex = exchange_time(ev)
        st = self._get(ev.instrument)
        st.events += 1
        problem = ""
        seq = int(getattr(ev, "sequence", 0) or 0)
        if self.sequence_scope == "instrument" and seq:
            if st.last_seq and seq > st.last_seq + 1:
                st.gaps += 1
                problem = f"sequence gap: {seq - st.last_seq - 1} message(s) missing after #{st.last_seq}"
            elif st.last_seq and seq <= st.last_seq:
                st.out_of_order += 1
                problem = f"out-of-order event: #{seq} after #{st.last_seq}"
        if not problem and st.last_exchange is not None and \
                ex < st.last_exchange - dt.timedelta(milliseconds=self.cfg.order_tolerance_ms):
            st.out_of_order += 1
            problem = f"out-of-order event: exchange time went back {(st.last_exchange - ex).total_seconds():.3f} s"
        if not problem and st.last_rx is not None and (rx - st.last_rx).total_seconds() >= self.cfg.stale_book_s:
            problem = f"the feed was silent for {(rx - st.last_rx).total_seconds():.0f} s"
        lat = (rx - ex).total_seconds() * 1000.0
        if lat < -self.cfg.clock_skew_ms:
            st.timestamp_errors += 1
            problem = problem or f"timestamp problem: received {-lat:.0f} ms before the exchange stamped it"
        st.latency_ms = lat
        st.latency_max_ms = max(st.latency_max_ms, lat)
        if lat > self.cfg.max_latency_ms:
            problem = problem or f"latency {lat:.0f} ms (over {self.cfg.max_latency_ms:.0f} ms)"
        if problem:
            self._fault(st, problem, rx)
        else:
            st.clean_events += 1
            if st.clean_since is None:
                st.clean_since = rx
        if self.sequence_scope == "instrument" and seq and seq > st.last_seq:
            st.last_seq = seq
        if st.last_exchange is None or ex > st.last_exchange:
            st.last_exchange = ex
        st.last_rx = rx
        if isinstance(ev, BookEvent):
            st.last_book_rx = rx
        elif isinstance(ev, TradeEvent):
            st.last_trade_rx = rx

    def _notice(self, ev: FeedNotice) -> None:
        targets = [ev.instrument] if ev.instrument else list(self.inst)
        n = ev.notice.upper()
        if n in ("GAP", "BAD_BOOK", "ERROR"):
            for i in targets:
                st = self._get(i)
                if n == "GAP":
                    st.gaps += 1
                self._fault(st, f"the feed reported {n.replace('_', ' ').lower()}" + (f": {ev.detail}" if ev.detail else ""),
                            ev.ts)
            if not ev.instrument:
                self.feed_fault = f"{n.lower()}: {ev.detail}"
        elif n == "DISCONNECTED":
            self.feed_fault = "disconnected" + (f": {ev.detail}" if ev.detail else "")
            for i in targets:
                self._fault(self._get(i), "the feed disconnected", ev.ts)
        elif n in ("RECONNECTED", "SNAPSHOT_END"):
            if not ev.instrument:
                self.feed_fault = ""

    def state(self, instrument: str, now: dt.datetime, book: Optional[OrderBook] = None,
              feed_state: str = LIVE) -> QualityState:
        if feed_state == NOT_CONFIGURED:
            return QualityState(NOT_CONFIGURED, ["no institutional order-book feed is configured"])
        st = self.inst.get(instrument)
        reasons: list[str] = []
        if feed_state == DOWN:
            return QualityState(DOWN, ["the feed is down"] + ([self.feed_fault] if self.feed_fault else []))
        if st is None or st.last_rx is None:
            return QualityState(DOWN, ["no data received yet for this instrument"])
        finished = feed_state == "FINISHED"            # a replay that has run out: silence is not staleness
        quiet = (now - st.last_book_rx).total_seconds() if st.last_book_rx else 1e9
        if quiet >= self.cfg.down_after_s and not finished:
            return QualityState(DOWN, [f"no book update for {quiet:.0f} s"])
        if self.feed_fault:
            reasons.append(f"feed: {self.feed_fault}")
        if quiet >= self.cfg.stale_book_s and not finished:
            reasons.append(f"stale book: no update for {quiet:.0f} s")
        tq = (now - st.last_trade_rx).total_seconds() if st.last_trade_rx else None
        if tq is not None and tq >= self.cfg.stale_trades_s and not finished:
            reasons.append(f"stale trades: none for {tq:.0f} s")
        if st.fault:
            recovered = (st.clean_events >= self.cfg.recovery_events and st.clean_since is not None
                         and (st.last_rx - st.clean_since).total_seconds() >= self.cfg.recovery_seconds)
            if recovered:
                st.fault = ""
            else:
                reasons.append(f"{st.fault} - recovering ({st.clean_events}/{self.cfg.recovery_events} clean events)")
        if book is not None:
            nb, na = len(book.bids), len(book.asks)
            if nb < self.cfg.min_levels or na < self.cfg.min_levels:
                reasons.append(f"missing depth: {nb} bid and {na} offer levels")
            if book.crossed():
                reasons.append("the book is crossed")
        return QualityState(DEGRADED if reasons else LIVE, reasons)

    def needs_recovery(self, instrument: str) -> bool:
        st = self.inst.get(instrument)
        return bool(st and st.fault) or bool(self.feed_fault)

    def recovery_due(self, key: str, now: dt.datetime) -> bool:
        """With a back-off: 5 s, 10 s, 20 s ... up to the maximum between attempts."""
        due = self._next_recover.get(key)
        if due is not None and now < due:
            return False
        b = self._backoff.get(key, self.cfg.recover_backoff_s)
        self._next_recover[key] = now + dt.timedelta(seconds=b)
        self._backoff[key] = min(b * 2.0, self.cfg.recover_backoff_max_s)
        self.recover_attempts += 1
        return True

    def recovered(self, key: str) -> None:
        self._next_recover.pop(key, None)
        self._backoff.pop(key, None)

    def stats(self, instrument: str) -> dict:
        st = self.inst.get(instrument)
        if st is None:
            return {}
        return {"events": st.events, "gaps": st.gaps, "out_of_order": st.out_of_order,
                "timestamp_errors": st.timestamp_errors, "latency_ms": round(st.latency_ms, 1),
                "latency_max_ms": round(st.latency_max_ms, 1),
                "last_received": st.last_rx.isoformat() if st.last_rx else None}
