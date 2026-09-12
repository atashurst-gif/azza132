"""Deterministic simulated broker.

Used by the test suite, the chaos harness and the backtester.  It implements the
same :class:`~mintel.broker.base.Broker` surface as the live MT5 adapter, plus a
fault-injection API so the engine can be tortured:

    sim.fault.stale_ticks = True        # feed stops advancing
    sim.fault.disconnect = True         # every call raises NotConnected
    sim.fault.spread_multiplier = 20.0  # spread explosion
    sim.fault.reject_next = RetCode.REQUOTE
    sim.fault.partial_fill_ratio = 0.4

Price generation is seeded, so a failing test is always reproducible.
"""
from __future__ import annotations

import datetime as dt
import math
import random
from dataclasses import dataclass, field
from typing import Optional, Sequence

from ..clock import UTC, utcnow
from ..contracts import SymbolSpec, infer_group, classify_fx
from .base import (AccountInfo, Bar, Broker, BrokerError, CalendarEvent,
                   NotConnected, OrderRequest, OrderResult, Position, RetCode,
                   Side, TF, Tick)


@dataclass
class Faults:
    disconnect: bool = False
    trade_disabled: bool = False
    stale_ticks: bool = False
    spread_multiplier: float = 1.0
    reject_next: Optional[RetCode] = None
    reject_all: Optional[RetCode] = None
    partial_fill_ratio: float = 0.0
    slippage_points: float = 0.0
    calendar_timeout: bool = False
    drop_stops_on_send: bool = False   # simulates a broker ignoring the SL
    hang_seconds: float = 0.0
    fail_modify: bool = False


DEFAULT_UNIVERSE = {
    # symbol      digits  point     tick_value(per lot per tick)  contract
    "EURUSD":  (5, 0.00001, 1.0,   100_000, 1.2),
    "GBPUSD":  (5, 0.00001, 1.0,   100_000, 1.5),
    "USDJPY":  (3, 0.001,   0.68,  100_000, 1.3),
    "AUDUSD":  (5, 0.00001, 1.0,   100_000, 1.4),
    "USDCHF":  (5, 0.00001, 1.12,  100_000, 1.6),
    "USDCAD":  (5, 0.00001, 0.74,  100_000, 1.8),
    "NZDUSD":  (5, 0.00001, 1.0,   100_000, 2.0),
    "EURJPY":  (3, 0.001,   0.68,  100_000, 1.8),
    "GBPJPY":  (3, 0.001,   0.68,  100_000, 2.6),
    "EURGBP":  (5, 0.00001, 1.29,  100_000, 1.7),
    "XAUUSD":  (2, 0.01,    1.0,   100,     18.0),
    "XAGUSD":  (3, 0.001,   5.0,   5_000,   25.0),
    "GER40":   (1, 0.1,     0.1,   1,       10.0),
    "US500":   (1, 0.1,     0.1,   1,       6.0),
}

START_PRICES = {
    "EURUSD": 1.0850, "GBPUSD": 1.2700, "USDJPY": 151.20, "AUDUSD": 0.6550,
    "USDCHF": 0.8900, "USDCAD": 1.3600, "NZDUSD": 0.6050, "EURJPY": 164.05,
    "GBPJPY": 192.00, "EURGBP": 0.8540, "XAUUSD": 2320.0, "XAGUSD": 27.40,
    "GER40": 18200.0, "US500": 5200.0,
}


class SimBroker:
    """Simulated broker with a synthetic-but-plausible price process.

    The generator deliberately produces regime blocks (trend / range / squeeze /
    high-vol) so the regime classifier and tactic router are exercised by data
    that actually contains those regimes rather than white noise.
    """

    def __init__(self, symbols: Sequence[str] = (), *, seed: int = 7,
                 start: Optional[dt.datetime] = None, balance: float = 10_000.0,
                 currency: str = "GBP", is_demo: bool = True,
                 server_offset_seconds: int = 3 * 3600,
                 bar_seconds: int = 60, history_bars: int = 3000,
                 live: bool = False, speed: float = 1.0):
        # ``live`` makes the simulation track the wall clock, so prices are
        # genuinely fresh and every staleness check behaves as it would against
        # a real feed.  That is what makes it usable as a demo and as a
        # self-test, rather than only as a fixture with a frozen clock.
        self.live = live
        self.speed = max(speed, 0.01)
        self._live_started = utcnow() if live else None
        self.rng = random.Random(seed)
        self.fault = Faults()
        self._connected = False
        self._symbol_names = list(symbols) if symbols else list(DEFAULT_UNIVERSE)
        self._selected: set[str] = set()
        self.server_offset_seconds = server_offset_seconds
        self.bar_seconds = bar_seconds
        self.now = start or (utcnow().replace(microsecond=0) if live
                             else dt.datetime(2026, 3, 10, 8, 0, tzinfo=UTC))
        self._balance = balance
        self._equity = balance
        self.currency = currency
        self.is_demo = is_demo
        self.login = 5_000_123 if is_demo else 9_000_777
        self.server = "SimBroker-Demo" if is_demo else "SimBroker-Live"
        self._next_ticket = 1000
        self._next_deal = 5000
        self._positions: dict[int, Position] = {}
        self.closed: list[dict] = []
        self.sent_keys: dict[str, int] = {}
        self.order_log: list[dict] = []
        self.modify_log: list[dict] = []
        self._events: list[CalendarEvent] = []
        self.margin_factor = 1.0
        self._resample_cache: dict = {}
        self._specs: dict[str, SymbolSpec] = {}
        self._bars: dict[str, list[Bar]] = {}
        self._last_tick: dict[str, Tick] = {}
        self._build_specs()
        self._seed_history(history_bars)

    # -------------------------------------------------------------- set-up --
    def _build_specs(self) -> None:
        for name in self._symbol_names:
            digits, point, tick_value, contract, spread_pts = DEFAULT_UNIVERSE.get(
                name, (5, 0.00001, 1.0, 100_000, 1.5))
            base, quote, _ = classify_fx(name)
            group = infer_group(name)
            self._specs[name] = SymbolSpec(
                name=name, digits=digits, point=point, tick_size=point,
                tick_value=tick_value, contract_size=contract,
                volume_min=0.01, volume_max=200.0, volume_step=0.01,
                stops_level_points=(10.0 if group.startswith("FX") else 30.0),
                freeze_level_points=0.0, base_currency=base or name[:3],
                profit_currency=quote or "USD", margin_currency=base or "USD",
                margin_initial=0.0, asset_group=group,
                typical_spread_points=spread_pts,
            )

    def _regime_for(self, i: int, sym: str) -> tuple[str, float, float]:
        """Return (regime, drift_per_bar_in_sigma, vol_multiplier)."""
        block = (i // 400 + hash(sym) % 4) % 4
        return [
            ("TREND", 0.16, 1.0),
            ("RANGE", 0.0, 0.8),
            ("SQUEEZE", 0.0, 0.35),
            ("HIGHVOL", 0.05, 2.2),
        ][block]

    def _seed_history(self, n: int) -> None:
        for sym in self._symbol_names:
            spec = self._specs[sym]
            px = START_PRICES.get(sym, 1.0)
            sigma = px * 0.00011 if spec.asset_group.startswith("FX") else px * 0.00025
            bars: list[Bar] = []
            t0 = self.now - dt.timedelta(seconds=self.bar_seconds * n)
            anchor = px
            for i in range(n):
                regime, drift, volmul = self._regime_for(i, sym)
                s = sigma * volmul
                step = self.rng.gauss(drift * s, s)
                if regime == "RANGE":
                    step += -0.05 * (px - anchor)  # mean reversion to anchor
                else:
                    anchor += (px - anchor) * 0.01
                o = px
                c = px + step
                wick = abs(self.rng.gauss(0, s * 0.6))
                h = max(o, c) + wick
                l = min(o, c) - abs(self.rng.gauss(0, s * 0.6))
                px = c
                bars.append(Bar(
                    time=t0 + dt.timedelta(seconds=self.bar_seconds * i),
                    open=spec.round_price(o), high=spec.round_price(h),
                    low=spec.round_price(l), close=spec.round_price(c),
                    tick_volume=max(1.0, self.rng.gauss(300 * volmul, 60)),
                    spread_points=spec.typical_spread_points,
                ))
            self._bars[sym] = bars
            self._publish_tick(sym, bars[-1].close)

    def _publish_tick(self, sym: str, mid: float) -> None:
        spec = self._specs[sym]
        half = spec.typical_spread_points * spec.point * self.fault.spread_multiplier / 2.0
        self._last_tick[sym] = Tick(
            symbol=sym, time=self.now,
            bid=spec.round_price(mid - half), ask=spec.round_price(mid + half),
            last=spec.round_price(mid), volume=1.0,
        )

    # ------------------------------------------------------- time stepping --
    def _catch_up(self) -> None:
        """In live mode, generate whatever bars wall-clock time has earned."""
        if not self.live or self._live_started is None:
            return
        elapsed = (utcnow() - self._live_started).total_seconds() * self.speed
        target = self._live_started + dt.timedelta(seconds=elapsed)
        missed = int((target - self.now).total_seconds() // self.bar_seconds)
        if missed > 0:
            self.advance(min(missed, 500))

    def advance(self, bars: int = 1) -> None:
        """Advance simulated time by ``bars`` base bars, extending all series."""
        for _ in range(bars):
            self.now += dt.timedelta(seconds=self.bar_seconds)
            for sym in self._symbol_names:
                spec = self._specs[sym]
                series = self._bars[sym]
                last = series[-1]
                i = len(series)
                regime, drift, volmul = self._regime_for(i, sym)
                base = START_PRICES.get(sym, 1.0)
                sigma = (base * 0.00011 if spec.asset_group.startswith("FX")
                         else base * 0.00025) * volmul
                c = last.close + self.rng.gauss(drift * sigma, sigma)
                o = last.close
                h = max(o, c) + abs(self.rng.gauss(0, sigma * 0.6))
                l = min(o, c) - abs(self.rng.gauss(0, sigma * 0.6))
                series.append(Bar(
                    time=self.now, open=spec.round_price(o),
                    high=spec.round_price(h), low=spec.round_price(l),
                    close=spec.round_price(c),
                    tick_volume=max(1.0, self.rng.gauss(300 * volmul, 60)),
                    spread_points=spec.typical_spread_points))
                if not self.fault.stale_ticks:
                    self._publish_tick(sym, c)
            self._mark_positions()

    def push_bar(self, sym: str, close: float, high: Optional[float] = None,
                 low: Optional[float] = None) -> None:
        """Append a hand-crafted bar - used to build exact test scenarios."""
        spec = self._specs[sym]
        series = self._bars[sym]
        o = series[-1].close if series else close
        self.now += dt.timedelta(seconds=self.bar_seconds)
        series.append(Bar(time=self.now, open=spec.round_price(o),
                          high=spec.round_price(high if high is not None else max(o, close)),
                          low=spec.round_price(low if low is not None else min(o, close)),
                          close=spec.round_price(close), tick_volume=300.0,
                          spread_points=spec.typical_spread_points))
        if not self.fault.stale_ticks:
            self._publish_tick(sym, close)
        self._mark_positions()

    # ------------------------------------------------------ position marking --
    def _mark_positions(self) -> None:
        """Mark open positions and honour broker-side SL/TP against bar extremes.

        The stop is checked against the *bar* high/low, not just the close, so a
        wick through the stop closes the trade exactly as a real broker would.
        """
        for ticket in list(self._positions):
            pos = self._positions[ticket]
            spec = self._specs[pos.symbol]
            bar = self._bars[pos.symbol][-1]
            tick = self._last_tick[pos.symbol]
            hit_price = None
            reason = ""
            if pos.side is Side.BUY:
                if pos.sl and bar.low <= pos.sl:
                    hit_price, reason = pos.sl, "SL"
                elif pos.tp and bar.high >= pos.tp:
                    hit_price, reason = pos.tp, "TP"
            else:
                if pos.sl and bar.high >= pos.sl:
                    hit_price, reason = pos.sl, "SL"
                elif pos.tp and bar.low <= pos.tp:
                    hit_price, reason = pos.tp, "TP"
            if hit_price is not None:
                self._settle(ticket, hit_price, reason)
                continue
            px = tick.bid if pos.side is Side.BUY else tick.ask
            pos.profit = spec.money((px - pos.entry_price) * pos.side.sign, pos.volume)
        self._equity = self._balance + sum(p.profit for p in self._positions.values())

    def _settle(self, ticket: int, price: float, reason: str,
                volume: Optional[float] = None) -> float:
        pos = self._positions[ticket]
        spec = self._specs[pos.symbol]
        vol = pos.volume if volume is None else min(volume, pos.volume)
        pnl = spec.money((price - pos.entry_price) * pos.side.sign, vol)
        self._balance += pnl
        self.closed.append({
            "ticket": ticket, "symbol": pos.symbol, "side": pos.side.value,
            "volume": vol, "entry": pos.entry_price, "exit": price,
            "pnl": pnl, "reason": reason, "time": self.now,
            "open_time": pos.open_time,
        })
        if vol >= pos.volume - 1e-9:
            del self._positions[ticket]
        else:
            pos.volume = round(pos.volume - vol, 8)
        return pnl

    # -------------------------------------------------------- Broker surface --
    def _guard(self) -> None:
        if not self._connected:
            raise NotConnected("sim broker not connected")
        if self.fault.disconnect:
            self._connected = False
            raise NotConnected("simulated broker disconnect")

    def connect(self) -> bool:
        if self.fault.disconnect:
            return False
        self._connected = True
        return True

    def shutdown(self) -> None:
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected and not self.fault.disconnect

    def account(self) -> AccountInfo:
        self._guard()
        # Use the same model as calc_margin so that a position sized against
        # the margin estimate cannot then be reported as using a different
        # amount - an inconsistency there would hide margin bugs in the risk
        # layer rather than expose them.
        margin = sum((self.calc_margin(p.symbol, p.side, p.volume,
                                       p.entry_price) or 0.0)
                     for p in self._positions.values())
        free = max(0.0, self._equity - margin)
        return AccountInfo(
            login=self.login, server=self.server, currency=self.currency,
            balance=round(self._balance, 2), equity=round(self._equity, 2),
            margin=margin, margin_free=free,
            margin_level=(self._equity / margin * 100.0) if margin else 0.0,
            leverage=30, trade_allowed=not self.fault.trade_disabled,
            is_demo=self.is_demo, company="SimBroker Ltd", name="Sim Account")

    def server_time(self) -> dt.datetime:
        return (self.now + dt.timedelta(seconds=self.server_offset_seconds)).replace(tzinfo=None)

    @property
    def clock(self):
        from ..clock import ServerClock
        return ServerClock(self.server_offset_seconds)

    def symbols(self) -> Sequence[str]:
        self._guard()
        return tuple(self._symbol_names)

    def spec(self, symbol: str) -> Optional[SymbolSpec]:
        self._guard()
        return self._specs.get(symbol)

    def ensure_selected(self, symbol: str) -> bool:
        self._guard()
        if symbol not in self._specs:
            return False
        self._selected.add(symbol)
        return True

    def tick(self, symbol: str) -> Optional[Tick]:
        self._guard()
        self._catch_up()
        t = self._last_tick.get(symbol)
        if t is None:
            return None
        if self.live and not self.fault.stale_ticks:
            # A real feed delivers ticks continuously between bars, so the
            # timestamp is the moment of the quote, not the bar's open.
            t = Tick(t.symbol, utcnow(), t.bid, t.ask, t.last, t.volume)
        if self.fault.spread_multiplier != 1.0:
            spec = self._specs[symbol]
            half = spec.typical_spread_points * spec.point * self.fault.spread_multiplier / 2.0
            t = Tick(symbol, t.time, spec.round_price(t.mid - half),
                     spec.round_price(t.mid + half), t.last, t.volume)
        return t

    def bars(self, symbol: str, tf: TF, count: int,
             end: Optional[dt.datetime] = None) -> list[Bar]:
        self._guard()
        self._catch_up()
        base = self._bars.get(symbol, [])
        if end is not None:
            base = [b for b in base if b.time <= end]
        if tf.seconds == self.bar_seconds:
            return base[-count:]
        if tf.seconds < self.bar_seconds:
            # finer than the stored series: return what we have (the live
            # adapter synthesises these from ticks)
            return base[-count:]
        if end is not None:
            return resample(base, tf.seconds)[-count:]
        key = (symbol, tf.seconds)
        r = self._resample_cache.get(key)
        if r is None:
            r = IncrementalResampler(tf.seconds)
            self._resample_cache[key] = r
        return r.update(base)[-count:]

    def ticks_range(self, symbol: str, start: dt.datetime,
                    end: dt.datetime) -> list[Tick]:
        self._guard()
        out = []
        for b in self._bars.get(symbol, []):
            if start <= b.time <= end:
                spec = self._specs[symbol]
                half = spec.typical_spread_points * spec.point / 2
                out.append(Tick(symbol, b.time, spec.round_price(b.close - half),
                                spec.round_price(b.close + half), b.close,
                                b.tick_volume))
        return out

    def positions(self, magic: Optional[int] = None) -> list[Position]:
        self._guard()
        return [p for p in self._positions.values()
                if magic is None or p.magic == magic]

    def send(self, req: OrderRequest) -> OrderResult:
        self._guard()
        spec = self._specs.get(req.symbol)
        if spec is None:
            return OrderResult(False, RetCode.REJECT.value,
                               comment="unknown symbol",
                               idempotency_key=req.idempotency_key)
        if self.fault.trade_disabled:
            return OrderResult(False, RetCode.TRADE_DISABLED.value,
                               idempotency_key=req.idempotency_key)
        forced = self.fault.reject_all or self.fault.reject_next
        if self.fault.reject_next:
            self.fault.reject_next = None
        if forced:
            self.order_log.append({"req": req, "retcode": forced.value})
            return OrderResult(False, forced.value,
                               requested_volume=req.volume,
                               idempotency_key=req.idempotency_key)
        vol = spec.normalise_volume(req.volume)
        if vol <= 0:
            return OrderResult(False, RetCode.INVALID_VOLUME.value,
                               idempotency_key=req.idempotency_key)
        tick = self.tick(req.symbol)
        raw = tick.ask if req.side is Side.BUY else tick.bid
        fill = spec.round_price(raw + req.side.sign * self.fault.slippage_points * spec.point)
        min_dist = spec.min_stop_distance_price(tick.spread)
        if req.sl:
            if abs(fill - req.sl) < min_dist - 1e-12:
                return OrderResult(False, RetCode.INVALID_STOPS.value,
                                   requested_volume=req.volume,
                                   idempotency_key=req.idempotency_key)
            wrong_side = ((req.side is Side.BUY and req.sl >= fill)
                          or (req.side is Side.SELL and req.sl <= fill))
            if wrong_side:
                return OrderResult(False, RetCode.INVALID_STOPS.value,
                                   requested_volume=req.volume,
                                   idempotency_key=req.idempotency_key)
        if self.fault.partial_fill_ratio:
            vol = spec.normalise_volume(vol * self.fault.partial_fill_ratio)
            if vol <= 0:
                return OrderResult(False, RetCode.INVALID_VOLUME.value,
                                   idempotency_key=req.idempotency_key)
        self._next_ticket += 1
        self._next_deal += 1
        ticket = self._next_ticket
        sl = 0.0 if self.fault.drop_stops_on_send else req.sl
        self._positions[ticket] = Position(
            ticket=ticket, symbol=req.symbol, side=req.side, volume=vol,
            entry_price=fill, sl=sl, tp=req.tp, open_time=self.now,
            comment=req.comment, magic=req.magic)
        if req.idempotency_key:
            self.sent_keys[req.idempotency_key] = ticket
        res = OrderResult(True, RetCode.DONE.value, ticket=ticket,
                          deal=self._next_deal, filled_volume=vol, price=fill,
                          requested_volume=req.volume,
                          idempotency_key=req.idempotency_key)
        self.order_log.append({"req": req, "retcode": res.retcode, "ticket": ticket})
        self._mark_positions()
        return res

    def modify_stops(self, ticket: int, sl: float, tp: float) -> OrderResult:
        self._guard()
        pos = self._positions.get(ticket)
        if pos is None:
            return OrderResult(False, RetCode.REJECT.value, comment="no position")
        if self.fault.fail_modify:
            return OrderResult(False, RetCode.INVALID_STOPS.value, ticket=ticket)
        spec = self._specs[pos.symbol]
        tick = self.tick(pos.symbol)
        px = tick.bid if pos.side is Side.BUY else tick.ask
        if sl and abs(px - sl) < spec.min_stop_distance_price() - 1e-12:
            return OrderResult(False, RetCode.INVALID_STOPS.value, ticket=ticket)
        pos.sl = spec.round_price(sl) if sl else pos.sl
        if tp:
            pos.tp = spec.round_price(tp)
        self.modify_log.append({"ticket": ticket, "sl": pos.sl, "tp": pos.tp,
                                "time": self.now})
        self._mark_positions()
        return OrderResult(True, RetCode.DONE.value, ticket=ticket)

    def close(self, ticket: int, volume: float = 0.0,
              comment: str = "") -> OrderResult:
        self._guard()
        pos = self._positions.get(ticket)
        if pos is None:
            return OrderResult(False, RetCode.REJECT.value, comment="no position")
        tick = self.tick(pos.symbol)
        px = tick.bid if pos.side is Side.BUY else tick.ask
        spec = self._specs[pos.symbol]
        if volume <= 0:
            vol = pos.volume
        else:
            vol = min(spec.normalise_volume(volume), pos.volume)
            if vol <= 0:
                return OrderResult(False, RetCode.INVALID_VOLUME.value,
                                   ticket=ticket,
                                   comment="partial volume below the lot step")
            remainder = round(pos.volume - vol, 8)
            if 0 < remainder < spec.volume_min:
                # A real broker will not leave a sub-minimum remainder open.
                vol = pos.volume
        self._settle(ticket, px, comment or "MANUAL", vol)
        self._mark_positions()
        return OrderResult(True, RetCode.DONE.value, ticket=ticket,
                           filled_volume=vol, price=px)

    def closed_deal(self, ticket: int) -> Optional[dict]:
        rows = [c for c in self.closed if c["ticket"] == ticket]
        if not rows:
            return None
        # Partial closes make several settlements for one position; the
        # volume-weighted exit is what the position as a whole achieved.
        volume = sum(r["volume"] for r in rows)
        pnl = sum(r["pnl"] for r in rows)
        exit_price = (sum(r["exit"] * r["volume"] for r in rows) / volume
                      if volume else rows[-1]["exit"])
        return {"exit_price": exit_price, "pnl": pnl, "volume": volume,
                "time": rows[-1]["time"], "reason": rows[-1]["reason"]}

    def calc_margin(self, symbol: str, side: Side, volume: float,
                    price: float) -> Optional[float]:
        spec = self._specs.get(symbol)
        if spec is None:
            return None
        # Simple 1:30 notional model in the account currency.
        return volume * spec.contract_size * price / 30.0 * self.margin_factor

    # ------------------------------------------------------------ calendar --
    def add_event(self, ev: CalendarEvent) -> None:
        self._events.append(ev)

    def calendar(self, start: dt.datetime, end: dt.datetime,
                 currencies: Sequence[str] = ()) -> list[CalendarEvent]:
        if self.fault.calendar_timeout:
            raise BrokerError("calendar timeout")
        cs = set(currencies)
        return [e for e in self._events
                if start <= e.time_utc <= end and (not cs or e.currency in cs)]


class IncrementalResampler:
    """Keeps coarse-timeframe series up to date without re-aggregating history.

    Re-running a full resample of every base bar, for every timeframe, for
    every symbol, on every scan dominated the entire run.  Only the newest
    (still forming) bucket can change, so only that bucket and anything after
    it is recomputed.
    """

    def __init__(self, target_seconds: int):
        self.secs = target_seconds
        self._n = 0
        self._out: list[Bar] = []

    def update(self, base: Sequence[Bar]) -> list[Bar]:
        n = len(base)
        if n == self._n and self._out:
            return self._out
        if n < self._n or not self._out:
            self._out = resample(base, self.secs)
            self._n = n
            return self._out
        # Re-merge from the start of the last emitted bucket: it may have been
        # incomplete, and everything after it is new.
        boundary = self._out[-1].time
        i = self._n - 1
        while i > 0 and base[i].time >= boundary:
            i -= 1
        while i < n and base[i].time < boundary:
            i += 1
        self._out = self._out[:-1] + resample(base[i:], self.secs)
        self._n = n
        return self._out


def resample(bars: Sequence[Bar], target_seconds: int) -> list[Bar]:
    """Aggregate bars into a coarser timeframe on aligned epoch boundaries."""
    out: list[Bar] = []
    bucket: list[Bar] = []
    cur_key = None
    for b in bars:
        key = int(b.time.timestamp()) // target_seconds
        if cur_key is None:
            cur_key = key
        if key != cur_key:
            if bucket:
                out.append(_merge(bucket, cur_key, target_seconds))
            bucket, cur_key = [], key
        bucket.append(b)
    if bucket:
        out.append(_merge(bucket, cur_key, target_seconds))
    return out


def _merge(bucket: Sequence[Bar], key: int, secs: int) -> Bar:
    return Bar(
        time=dt.datetime.fromtimestamp(key * secs, tz=UTC),
        open=bucket[0].open,
        high=max(b.high for b in bucket),
        low=min(b.low for b in bucket),
        close=bucket[-1].close,
        tick_volume=sum(b.tick_volume for b in bucket),
        real_volume=sum(b.real_volume for b in bucket),
        spread_points=sum(b.spread_points for b in bucket) / len(bucket),
    )
