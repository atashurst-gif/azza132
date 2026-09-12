"""Historical replay broker.

Implements the same :class:`~mintel.broker.base.Broker` interface as the live
MT5 adapter, so a backtest runs *the identical engine code* rather than a
parallel re-implementation.  That is the only way a backtest result says
anything about the live system.

What is modelled honestly:

* **Spread** per bar, from recorded spread where the data has it, otherwise
  from a configured per-symbol assumption.
* **Slippage** on entry, scaled with the spread.
* **Commission** per lot per side.
* **Intrabar stop and target fills**, checked against the bar's high and low,
  with the conservative assumption that if a bar could have touched both the
  stop and the target, the **stop** was hit first.
* **No look-ahead**: the engine can only ever see bars up to and including the
  current one.

What is *not* modelled, and is stated rather than hidden: queue position,
partial fills from genuine illiquidity, and weekend gaps beyond what the data
contains.
"""
from __future__ import annotations

import csv
import datetime as dt
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional, Sequence

from ..broker.base import (AccountInfo, Bar, CalendarEvent, NotConnected,
                           OrderRequest, OrderResult, Position, RetCode, Side,
                           TF, Tick)
from ..broker.sim import IncrementalResampler, resample
from ..clock import UTC, to_utc
from ..contracts import SymbolSpec, classify_fx, infer_group


@dataclass
class Costs:
    spread_points: float = 0.0          # fallback when data has no spread
    commission_per_lot_per_side: float = 0.0
    slippage_points: float = 0.0
    slippage_spread_multiple: float = 0.25


class ReplayBroker:
    """Bar-by-bar replay over stored history."""

    def __init__(self, series: dict[str, list[Bar]],
                 specs: dict[str, SymbolSpec], *,
                 base_tf: TF = TF.M1, balance: float = 10_000.0,
                 currency: str = "GBP",
                 costs: Optional[dict[str, Costs]] = None,
                 leverage: int = 30,
                 events: Sequence[CalendarEvent] = (),
                 server_offset_seconds: int = 0):
        self.series = {s: sorted(b, key=lambda x: x.time)
                       for s, b in series.items() if b}
        self.specs = specs
        self.base_tf = base_tf
        self.costs = costs or {}
        self.currency = currency
        self.leverage = leverage
        self._events = list(events)
        self.server_offset_seconds = server_offset_seconds

        self._timeline: list[dt.datetime] = sorted(
            {b.time for bars in self.series.values() for b in bars})
        self._index: dict[str, int] = {s: 0 for s in self.series}
        self.cursor = 0
        self._connected = False
        self._balance = balance
        self._equity = balance
        self._next_ticket = 700_000
        self._positions: dict[int, Position] = {}
        self.closed: list[dict] = []
        self.commission_paid = 0.0
        self.rejections = 0
        self._resampled: dict[tuple[str, int], IncrementalResampler] = {}

    # ---------------------------------------------------------------- loading
    @classmethod
    def from_csv(cls, paths: dict[str, str | Path], **kw) -> "ReplayBroker":
        """Load ``symbol -> csv path``.

        Accepts the MT5 "Export Bars" format and any CSV with columns
        time/open/high/low/close plus optional tick_volume and spread.
        """
        series: dict[str, list[Bar]] = {}
        specs: dict[str, SymbolSpec] = {}
        for symbol, path in paths.items():
            bars = load_bars_csv(path)
            if not bars:
                continue
            series[symbol] = bars
            specs[symbol] = kw.pop(f"spec_{symbol}", None) or default_spec(
                symbol, bars[-1].close)
        return cls(series, specs, **kw)

    # ------------------------------------------------------------------ clock
    @property
    def now(self) -> dt.datetime:
        if not self._timeline:
            return dt.datetime(1970, 1, 1, tzinfo=UTC)
        return self._timeline[min(self.cursor, len(self._timeline) - 1)]

    @property
    def finished(self) -> bool:
        return self.cursor >= len(self._timeline) - 1

    def step(self) -> bool:
        """Advance one base bar and settle stops/targets against it."""
        if self.finished:
            return False
        self.cursor += 1
        now = self.now
        for sym, bars in self.series.items():
            i = self._index[sym]
            while i + 1 < len(bars) and bars[i + 1].time <= now:
                i += 1
            self._index[sym] = i
        self._settle_open_positions()
        return True

    @property
    def clock(self):
        from ..clock import ServerClock
        return ServerClock(self.server_offset_seconds)

    def server_time(self) -> dt.datetime:
        return (self.now
                + dt.timedelta(seconds=self.server_offset_seconds)).replace(
                    tzinfo=None)

    # -------------------------------------------------------- Broker surface
    def connect(self) -> bool:
        self._connected = True
        return True

    def shutdown(self) -> None:
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def _guard(self) -> None:
        if not self._connected:
            raise NotConnected("replay broker not connected")

    def account(self) -> AccountInfo:
        self._guard()
        margin = sum((self.calc_margin(p.symbol, p.side, p.volume,
                                       p.entry_price) or 0.0)
                     for p in self._positions.values())
        return AccountInfo(
            login=1, server="Replay", currency=self.currency,
            balance=round(self._balance, 2), equity=round(self._equity, 2),
            margin=margin, margin_free=max(0.0, self._equity - margin),
            margin_level=(self._equity / margin * 100.0) if margin else 0.0,
            leverage=self.leverage, trade_allowed=True, is_demo=True,
            company="Replay", name="Backtest")

    def symbols(self) -> Sequence[str]:
        return tuple(self.series)

    def spec(self, symbol: str) -> Optional[SymbolSpec]:
        return self.specs.get(symbol)

    def ensure_selected(self, symbol: str) -> bool:
        return symbol in self.series

    def _current_bar(self, symbol: str) -> Optional[Bar]:
        bars = self.series.get(symbol)
        if not bars:
            return None
        return bars[self._index[symbol]]

    def _spread(self, symbol: str) -> float:
        spec = self.specs[symbol]
        bar = self._current_bar(symbol)
        cost = self.costs.get(symbol, Costs())
        points = (bar.spread_points if bar and bar.spread_points > 0
                  else (cost.spread_points or spec.typical_spread_points))
        return max(points, 0.0) * spec.point

    def tick(self, symbol: str) -> Optional[Tick]:
        self._guard()
        bar = self._current_bar(symbol)
        if bar is None:
            return None
        spec = self.specs[symbol]
        half = self._spread(symbol) / 2.0
        return Tick(symbol=symbol, time=bar.time,
                    bid=spec.round_price(bar.close - half),
                    ask=spec.round_price(bar.close + half),
                    last=bar.close, volume=bar.tick_volume)

    def bars(self, symbol: str, tf: TF, count: int,
             end: Optional[dt.datetime] = None) -> list[Bar]:
        """History up to and including the current bar.  Never beyond it."""
        self._guard()
        bars = self.series.get(symbol)
        if not bars:
            return []
        upto = self._index[symbol] + 1
        visible = bars[:upto]
        if end is not None:
            visible = [b for b in visible if b.time <= end]
        if tf.seconds <= self.base_tf.seconds:
            return visible[-count:]
        key = (symbol, tf.seconds)
        r = self._resampled.get(key)
        if r is None:
            r = IncrementalResampler(tf.seconds)
            self._resampled[key] = r
        return r.update(visible)[-count:]

    def ticks_range(self, symbol: str, start: dt.datetime,
                    end: dt.datetime) -> list[Tick]:
        out = []
        spec = self.specs[symbol]
        half = self._spread(symbol) / 2
        for b in self.series.get(symbol, [])[:self._index[symbol] + 1]:
            if start <= b.time <= end:
                out.append(Tick(symbol, b.time, spec.round_price(b.close - half),
                                spec.round_price(b.close + half), b.close,
                                b.tick_volume))
        return out

    def positions(self, magic: Optional[int] = None) -> list[Position]:
        self._guard()
        return [p for p in self._positions.values()
                if magic is None or p.magic == magic]

    def closed_deal(self, ticket: int) -> Optional[dict]:
        rows = [c for c in self.closed if c["ticket"] == ticket]
        if not rows:
            return None
        volume = sum(r["volume"] for r in rows)
        pnl = sum(r["pnl"] for r in rows)
        exit_price = (sum(r["exit"] * r["volume"] for r in rows) / volume
                      if volume else rows[-1]["exit"])
        return {"exit_price": exit_price, "pnl": pnl, "volume": volume,
                "time": rows[-1]["time"], "reason": rows[-1]["reason"]}

    def calc_margin(self, symbol: str, side: Side, volume: float,
                    price: float) -> Optional[float]:
        spec = self.specs.get(symbol)
        if spec is None:
            return None
        return volume * spec.contract_size * price / max(self.leverage, 1)

    def send(self, req: OrderRequest) -> OrderResult:
        self._guard()
        spec = self.specs.get(req.symbol)
        tick = self.tick(req.symbol)
        if spec is None or tick is None:
            self.rejections += 1
            return OrderResult(False, RetCode.PRICE_OFF.value,
                               idempotency_key=req.idempotency_key)
        vol = spec.normalise_volume(req.volume)
        if vol <= 0:
            self.rejections += 1
            return OrderResult(False, RetCode.INVALID_VOLUME.value,
                               idempotency_key=req.idempotency_key)
        cost = self.costs.get(req.symbol, Costs())
        slip_points = (cost.slippage_points
                       + cost.slippage_spread_multiple
                       * self._spread(req.symbol) / spec.point)
        raw = tick.ask if req.side is Side.BUY else tick.bid
        fill = spec.round_price(raw + req.side.sign * slip_points * spec.point)
        if req.sl:
            min_dist = spec.min_stop_distance_price(tick.spread)
            wrong_side = ((req.side is Side.BUY and req.sl >= fill)
                          or (req.side is Side.SELL and req.sl <= fill))
            if wrong_side or abs(fill - req.sl) < min_dist:
                self.rejections += 1
                return OrderResult(False, RetCode.INVALID_STOPS.value,
                                   idempotency_key=req.idempotency_key)
        commission = cost.commission_per_lot_per_side * vol
        self._balance -= commission
        self.commission_paid += commission
        self._next_ticket += 1
        self._positions[self._next_ticket] = Position(
            ticket=self._next_ticket, symbol=req.symbol, side=req.side,
            volume=vol, entry_price=fill, sl=req.sl, tp=req.tp,
            open_time=self.now, comment=req.comment, magic=req.magic)
        self._settle_open_positions()
        return OrderResult(True, RetCode.DONE.value, ticket=self._next_ticket,
                           deal=self._next_ticket, filled_volume=vol,
                           price=fill, requested_volume=req.volume,
                           idempotency_key=req.idempotency_key)

    def modify_stops(self, ticket: int, sl: float, tp: float) -> OrderResult:
        self._guard()
        pos = self._positions.get(ticket)
        if pos is None:
            return OrderResult(False, RetCode.REJECT.value)
        spec = self.specs[pos.symbol]
        tick = self.tick(pos.symbol)
        px = tick.bid if pos.side is Side.BUY else tick.ask
        if sl and abs(px - sl) < spec.min_stop_distance_price():
            return OrderResult(False, RetCode.INVALID_STOPS.value, ticket=ticket)
        if sl:
            pos.sl = spec.round_price(sl)
        if tp:
            pos.tp = spec.round_price(tp)
        return OrderResult(True, RetCode.DONE.value, ticket=ticket)

    def close(self, ticket: int, volume: float = 0.0,
              comment: str = "") -> OrderResult:
        self._guard()
        pos = self._positions.get(ticket)
        if pos is None:
            return OrderResult(False, RetCode.REJECT.value)
        spec = self.specs[pos.symbol]
        tick = self.tick(pos.symbol)
        px = tick.bid if pos.side is Side.BUY else tick.ask
        if volume <= 0:
            vol = pos.volume
        else:
            vol = min(spec.normalise_volume(volume), pos.volume)
            if vol <= 0:
                return OrderResult(False, RetCode.INVALID_VOLUME.value,
                                   ticket=ticket)
            if 0 < round(pos.volume - vol, 8) < spec.volume_min:
                vol = pos.volume
        self._settle(ticket, px, comment or "MANUAL", vol)
        return OrderResult(True, RetCode.DONE.value, ticket=ticket,
                           filled_volume=vol, price=px)

    def calendar(self, start: dt.datetime, end: dt.datetime,
                 currencies: Sequence[str] = ()) -> list[CalendarEvent]:
        """Only events dated at or before 'now' may be *known* with a value.

        A backtest that could see an ``actual`` before its release time would
        be trading tomorrow's newspaper.  Future events are visible as
        *scheduled* items with their forecast, which is exactly what a live bot
        knows in advance.
        """
        now = self.now
        cs = {c.upper() for c in currencies}
        out = []
        for e in self._events:
            if not (start <= e.time_utc <= end):
                continue
            if cs and e.currency not in cs:
                continue
            if e.time_utc > now:
                out.append(CalendarEvent(
                    event_id=e.event_id, time_utc=e.time_utc,
                    currency=e.currency, country=e.country, name=e.name,
                    importance=e.importance, actual=None,
                    forecast=e.forecast, previous=e.previous,
                    revised_previous=None, unit=e.unit, source=e.source))
            else:
                out.append(e)
        return out

    # ------------------------------------------------------------- settlement
    def _settle_open_positions(self) -> None:
        for ticket in list(self._positions):
            pos = self._positions[ticket]
            bar = self._current_bar(pos.symbol)
            if bar is None:
                continue
            spec = self.specs[pos.symbol]
            half = self._spread(pos.symbol) / 2.0
            hit = None
            reason = ""
            # Conservative: when a bar's range covers both the stop and the
            # target, assume the STOP filled first.  Anything else flatters the
            # result on exactly the bars that matter most.
            if pos.side is Side.BUY:
                if pos.sl and (bar.low - half) <= pos.sl:
                    hit, reason = pos.sl, "SL"
                elif pos.tp and (bar.high - half) >= pos.tp:
                    hit, reason = pos.tp, "TP"
            else:
                if pos.sl and (bar.high + half) >= pos.sl:
                    hit, reason = pos.sl, "SL"
                elif pos.tp and (bar.low + half) <= pos.tp:
                    hit, reason = pos.tp, "TP"
            if hit is not None:
                self._settle(ticket, hit, reason)
                continue
            px = bar.close - (half if pos.side is Side.BUY else -half)
            pos.profit = spec.money((px - pos.entry_price) * pos.side.sign,
                                    pos.volume)
        self._equity = self._balance + sum(p.profit
                                           for p in self._positions.values())

    def _settle(self, ticket: int, price: float, reason: str,
                volume: Optional[float] = None) -> None:
        pos = self._positions[ticket]
        spec = self.specs[pos.symbol]
        vol = pos.volume if volume is None else min(volume, pos.volume)
        cost = self.costs.get(pos.symbol, Costs())
        commission = cost.commission_per_lot_per_side * vol
        pnl = spec.money((price - pos.entry_price) * pos.side.sign, vol) - commission
        self._balance += pnl
        self.commission_paid += commission
        self.closed.append({
            "ticket": ticket, "symbol": pos.symbol, "side": pos.side.value,
            "volume": vol, "entry": pos.entry_price, "exit": price,
            "pnl": pnl, "commission": commission, "reason": reason,
            "time": self.now, "open_time": pos.open_time,
        })
        if vol >= pos.volume - 1e-9:
            del self._positions[ticket]
        else:
            pos.volume = round(pos.volume - vol, 8)


# ---------------------------------------------------------------- data i/o ----

def load_bars_csv(path: str | Path) -> list[Bar]:
    """Read bars from CSV, tolerating MT5's tab-separated export."""
    p = Path(path)
    if not p.exists():
        return []
    text = p.read_text().strip()
    if not text:
        return []
    delim = "\t" if "\t" in text.splitlines()[0] else ","
    rows = list(csv.DictReader(text.splitlines(), delimiter=delim))
    out: list[Bar] = []
    for r in rows:
        keys = {k.strip().lower().lstrip("<").rstrip(">"): v
                for k, v in r.items() if k}
        # MT5's bar export splits the stamp into <DATE> and <TIME>; other
        # exporters put the whole thing in one column.  Combine when both are
        # present, otherwise take whichever exists.
        date_part = (keys.get("date") or "").strip()
        time_part = (keys.get("time") or "").strip()
        if date_part and time_part:
            raw_ts = f"{date_part} {time_part}"
        else:
            raw_ts = date_part or time_part
        ts = _parse_dt(raw_ts)
        if ts is None:
            continue
        try:
            out.append(Bar(
                time=ts, open=float(keys["open"]), high=float(keys["high"]),
                low=float(keys["low"]), close=float(keys["close"]),
                tick_volume=float(keys.get("tickvol")
                                  or keys.get("tick_volume") or 0) or 0.0,
                real_volume=float(keys.get("vol") or keys.get("volume") or 0)
                or 0.0,
                spread_points=float(keys.get("spread") or 0) or 0.0))
        except (KeyError, ValueError):
            continue
    return out


def _parse_dt(raw: str) -> Optional[dt.datetime]:
    raw = (raw or "").strip().replace(".", "-")
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
                "%Y-%m-%dT%H:%M:%S"):
        try:
            return dt.datetime.strptime(raw, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    try:
        d = dt.datetime.fromisoformat(raw)
        return d if d.tzinfo else d.replace(tzinfo=UTC)
    except ValueError:
        return None


DEFAULT_CONTRACTS = {
    "FX_MAJOR": (100_000, 5), "FX_MINOR": (100_000, 5), "FX_EXOTIC": (100_000, 5),
    "GOLD": (100, 2), "SILVER": (5_000, 3), "INDEX": (1, 1), "ENERGY": (1_000, 2),
}


def default_spec(symbol: str, price: float) -> SymbolSpec:
    """A conservative specification for replay when none is supplied.

    Deliberately conservative: wider typical spread and non-zero stop level, so
    a backtest built on assumed specs errs toward pessimism.
    """
    group = infer_group(symbol)
    contract, digits = DEFAULT_CONTRACTS.get(group, (100_000, 5))
    base, quote, _ = classify_fx(symbol)
    if group in ("FX_MAJOR", "FX_MINOR") and quote == "JPY":
        digits = 3
    point = 10 ** (-digits)
    if group.startswith("FX"):
        tick_value = contract * point
        spread_points = 15.0 if digits in (3, 5) else 2.0
    elif group == "GOLD":
        tick_value = contract * point
        spread_points = 30.0
    elif group == "SILVER":
        tick_value = contract * point
        spread_points = 30.0
    else:
        tick_value = contract * point * 10
        spread_points = 10.0
    return SymbolSpec(
        name=symbol, digits=digits, point=point, tick_size=point,
        tick_value=tick_value, contract_size=contract, volume_min=0.01,
        volume_max=100.0, volume_step=0.01, stops_level_points=10.0,
        freeze_level_points=0.0, base_currency=base, profit_currency=quote,
        margin_currency=base, asset_group=group,
        typical_spread_points=spread_points)
