"""Rapid Scalper execution: PAPER (no orders ever reach the broker) and
LIVE (orders carry the scalper's own magic and a server-side stop, or they
are closed at once). OFF has no executor at all."""
from __future__ import annotations

import datetime as dt
import itertools
import time
from dataclasses import dataclass, field
from typing import Optional

from ..broker.base import OrderRequest, OrderResult, Position, Side, Tick
from ..contracts import SymbolSpec
from .config import ScalperConfig


@dataclass
class Fill:
    ok: bool
    ticket: int = 0
    price: float = 0.0
    requested: float = 0.0
    slippage_points: float = 0.0
    latency_ms: float = 0.0
    message: str = ""
    volume: float = 0.0


class PaperExecutor:
    """Fills at the touch plus a configured slippage, with the stop honoured
    on every tick exactly as a server-side stop would be."""
    mode = "PAPER"

    def __init__(self, cfg: ScalperConfig):
        self.cfg = cfg
        self._seq = itertools.count(900_000_001)
        self._positions: dict[int, Position] = {}

    def open(self, symbol: str, side: Side, volume: float, stop: float, tick: Tick,
             spec: SymbolSpec, now: dt.datetime) -> Fill:
        requested = tick.ask if side is Side.BUY else tick.bid
        slip = self.cfg.paper_slippage_points * spec.point
        price = spec.normalise_price(requested + slip * side.sign)
        ticket = next(self._seq)
        self._positions[ticket] = Position(ticket, symbol, side, volume, price, stop, 0.0, now,
                                           magic=self.cfg.magic, comment="RS-PAPER")
        return Fill(True, ticket, price, requested, slip / spec.point, self.cfg.paper_latency_ms, "paper fill", volume)

    def modify_stop(self, ticket: int, stop: float) -> bool:
        p = self._positions.get(ticket)
        if p is None:
            return False
        p.sl = stop
        return True

    def close(self, ticket: int, tick: Tick, spec: SymbolSpec, volume: float = 0.0,
              at_price: Optional[float] = None) -> Fill:
        p = self._positions.get(ticket)
        if p is None:
            return Fill(False, message="no such paper position")
        requested = at_price if at_price is not None else (tick.bid if p.side is Side.BUY else tick.ask)
        slip = self.cfg.paper_slippage_points * spec.point
        price = spec.normalise_price(requested - slip * p.side.sign)
        vol = volume if 0 < volume < p.volume else p.volume
        if vol >= p.volume - 1e-12:
            del self._positions[ticket]
        else:
            p.volume = round(p.volume - vol, 8)
        return Fill(True, ticket, price, requested, slip / spec.point, self.cfg.paper_latency_ms, "paper close", vol)

    def positions(self) -> list[Position]:
        return list(self._positions.values())

    def stop_hit(self, ticket: int, tick: Tick) -> bool:
        p = self._positions.get(ticket)
        if p is None or not p.sl:
            return False
        return tick.bid <= p.sl if p.side is Side.BUY else tick.ask >= p.sl


class LiveExecutor:
    """Real orders. Every order carries the scalper's magic; every position
    must have its stop at the broker or it is closed immediately."""
    mode = "LIVE"

    def __init__(self, cfg: ScalperConfig, broker):
        self.cfg = cfg
        self.broker = broker
        self.last_error = ""

    def open(self, symbol: str, side: Side, volume: float, stop: float, tick: Tick,
             spec: SymbolSpec, now: dt.datetime) -> Fill:
        requested = tick.ask if side is Side.BUY else tick.bid
        price = 0.0
        if self.cfg.order_type.upper() == "LIMIT":
            price = spec.normalise_price(requested - side.sign * self.cfg.limit_offset_points * spec.point)
        filling = self.cfg.filling if self.cfg.filling in spec.filling_modes else (spec.filling_modes[0] if spec.filling_modes else "IOC")
        req = OrderRequest(symbol, side, volume, sl=spec.normalise_price(stop), tp=0.0, price=price,
                           deviation_points=int(self.cfg.slippage_allowance_points * 2),
                           comment="RS", magic=self.cfg.magic,
                           idempotency_key=f"rs-{symbol}-{int(now.timestamp() * 1000)}", filling=filling)
        t0 = time.monotonic()
        try:
            res: OrderResult = self.broker.send(req)
        except Exception as exc:
            self.last_error = str(exc)
            return Fill(False, message=f"send failed: {exc}")
        latency = (time.monotonic() - t0) * 1000.0
        if not res.ok:
            return Fill(False, message=f"rejected ({res.retcode}) {res.comment}", latency_ms=latency)
        fill_price = res.price or requested
        slip = ((fill_price - requested) * side.sign) / spec.point
        # Fail closed: the protective stop must be at the broker.
        pos = next((p for p in self.positions() if p.ticket == res.ticket), None)
        if pos is None or not pos.sl:
            try:
                r = self.broker.modify_stops(res.ticket, spec.normalise_price(stop), 0.0)
                if not r.ok:
                    raise RuntimeError(f"stop not accepted ({r.retcode})")
            except Exception as exc:
                self.last_error = f"no server-side stop: {exc}"
                try:
                    self.broker.close(res.ticket, 0.0, "RS no-stop")
                except Exception:
                    pass
                return Fill(False, message=f"closed at once: {self.last_error}", latency_ms=latency)
        return Fill(True, res.ticket, fill_price, requested, slip, latency, "filled",
                    res.filled_volume or volume)

    def modify_stop(self, ticket: int, stop: float) -> bool:
        try:
            r = self.broker.modify_stops(ticket, stop, 0.0)
            return bool(r.ok)
        except Exception as exc:
            self.last_error = str(exc)
            return False

    def close(self, ticket: int, tick: Tick, spec: SymbolSpec, volume: float = 0.0,
              at_price: Optional[float] = None) -> Fill:
        pos = next((p for p in self.positions() if p.ticket == ticket), None)
        requested = (tick.bid if (pos and pos.side is Side.BUY) else tick.ask)
        t0 = time.monotonic()
        try:
            r = self.broker.close(ticket, volume, "RS")
        except Exception as exc:
            self.last_error = str(exc)
            return Fill(False, message=f"close failed: {exc}")
        latency = (time.monotonic() - t0) * 1000.0
        if not r.ok:
            return Fill(False, message=f"close rejected ({r.retcode})", latency_ms=latency)
        price = r.price or requested
        sign = pos.side.sign if pos else 1
        slip = ((requested - price) * sign) / spec.point
        return Fill(True, ticket, price, requested, slip, latency, "closed", r.filled_volume or volume)

    def positions(self) -> list[Position]:
        return [p for p in self.broker.positions(self.cfg.magic) if p.magic == self.cfg.magic]

    def stop_hit(self, ticket: int, tick: Tick) -> bool:
        return False            # the broker closes it; reconciliation notices


def make_executor(cfg: ScalperConfig, broker):
    """OFF -> None (nothing can ever be sent). PAPER -> PaperExecutor. LIVE -> LiveExecutor."""
    if cfg.mode == "LIVE":
        return LiveExecutor(cfg, broker)
    if cfg.mode == "PAPER":
        return PaperExecutor(cfg)
    return None
