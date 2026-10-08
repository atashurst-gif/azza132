"""One executor for the paper bots' orders: PAPER simulates, LIVE sends.

Used by the Momentum Runner, the Band Breaker and the Crowd Fader. The
engines decide; this is the only place their decisions touch the broker.

PAPER is pessimistic by construction: a fill at the touch plus slippage, a
stop or target closes AT its level and never better, a buy is marked on
the bid and a sell on the ask. LIVE sends real orders, every one carrying
the bot's own magic number so no bot can ever see or touch another's
positions, and fails closed: a position whose stop the broker did not take
is closed again at once. In LIVE the broker's own record of a closed
position (profit + commission + swap) is the truth, read with
``closed_deal``.
"""
from __future__ import annotations

import datetime as dt
import itertools
import time
from dataclasses import dataclass
from typing import Optional

from .broker.base import OrderRequest, OrderResult, Position, Side, Tick
from .clock import to_utc

MAGIC_RUNNER = 990_511
MAGIC_BANDBREAKER = 990_611
MAGIC_CROWD = 990_711


@dataclass
class Fill:
    ok: bool
    ticket: int = 0
    price: float = 0.0
    requested: float = 0.0
    volume: float = 0.0
    message: str = ""


class PaperExecutor:
    mode = "PAPER"

    def __init__(self, slippage_points: float = 1.0, first_ticket: int = 700_000_001, tag: str = "PAPER"):
        self.slippage_points = float(slippage_points)
        self.tag = tag
        self._next = int(first_ticket)
        self._positions: dict[int, Position] = {}
        self.last_error = ""

    def reserve(self, ticket: int) -> None:
        """Never reuse a ticket that is already on record (after a restart)."""
        self._next = max(self._next, int(ticket) + 1)

    def adopt(self, position: Position) -> None:
        self._positions[position.ticket] = position
        self.reserve(position.ticket)

    def open(self, symbol: str, side: Side, volume: float, stop: float, tp: float, tick: Tick, spec,
             now: dt.datetime, comment: str = "") -> Fill:
        touch = float(tick.ask if side is Side.BUY else tick.bid)
        slip = self.slippage_points * float(getattr(spec, "point", 0.0) or 0.0)
        price = spec.normalise_price(touch + side.sign * slip)
        ticket = self._next
        self._next += 1
        self._positions[ticket] = Position(ticket, symbol, side, float(volume), float(price), float(stop), float(tp or 0.0),
                                           to_utc(now), magic=0, comment=comment or self.tag)
        return Fill(True, ticket, float(price), touch, float(volume), "paper fill")

    def modify_stop(self, ticket: int, stop: float, tp: Optional[float] = None) -> bool:
        p = self._positions.get(ticket)
        if p is None:
            return False
        p.sl = float(stop)
        if tp is not None:
            p.tp = float(tp)
        return True

    def close(self, ticket: int, tick: Tick, spec, at_price: Optional[float] = None, comment: str = "") -> Fill:
        p = self._positions.get(ticket)
        if p is None:
            return Fill(False, message="no such paper position")
        mark = float(tick.bid if p.side is Side.BUY else tick.ask)
        if at_price is not None:
            price = float(at_price)                        # AT the level, never better
        else:
            slip = self.slippage_points * float(getattr(spec, "point", 0.0) or 0.0)
            price = spec.normalise_price(mark - p.side.sign * slip)
        del self._positions[ticket]
        return Fill(True, ticket, float(price), mark, float(p.volume), "paper close")

    def stop_hit(self, ticket: int, tick: Tick) -> bool:
        p = self._positions.get(ticket)
        if p is None or not p.sl:
            return False
        return float(tick.bid) <= p.sl if p.side is Side.BUY else float(tick.ask) >= p.sl

    def target_hit(self, ticket: int, tick: Tick) -> bool:
        p = self._positions.get(ticket)
        if p is None or not p.tp:
            return False
        return float(tick.bid) >= p.tp if p.side is Side.BUY else float(tick.ask) <= p.tp

    def positions(self) -> list[Position]:
        return list(self._positions.values())

    def closed_deal(self, ticket: int) -> Optional[dict]:
        return None


class LiveExecutor:
    mode = "LIVE"

    def __init__(self, broker, magic: int, tag: str, deviation_points: int = 20):
        self.broker = broker
        self.magic = int(magic)
        self.tag = tag
        self.deviation_points = int(deviation_points)
        self.last_error = ""

    def reserve(self, ticket: int) -> None:
        pass

    def adopt(self, position: Position) -> None:
        pass

    def open(self, symbol: str, side: Side, volume: float, stop: float, tp: float, tick: Tick, spec,
             now: dt.datetime, comment: str = "") -> Fill:
        touch = float(tick.ask if side is Side.BUY else tick.bid)
        modes = list(getattr(spec, "filling_modes", ()) or ())
        filling = "IOC" if ("IOC" in modes or not modes) else modes[0]
        sl = spec.normalise_price(stop)
        tpx = spec.normalise_price(tp) if tp else 0.0
        req = OrderRequest(symbol, side, float(volume), sl=sl, tp=tpx, price=0.0,
                           deviation_points=self.deviation_points, comment=(comment or self.tag)[:31], magic=self.magic,
                           idempotency_key=f"{self.tag.lower()}-{symbol}-{int(to_utc(now).timestamp() * 1000)}",
                           filling=filling)
        try:
            res: OrderResult = self.broker.send(req)
        except Exception as exc:
            self.last_error = str(exc)
            return Fill(False, message=f"send failed: {exc}")
        if not res.ok:
            self.last_error = f"rejected ({res.retcode}) {res.comment}"
            return Fill(False, message=self.last_error)
        price = float(res.price or touch)
        # fail closed: the protective stop must be at the broker
        try:
            pos = next((p for p in self.positions() if p.ticket == res.ticket), None)
        except Exception as exc:
            # the order filled and carried its stop; the read-back failed. Report
            # the fill (the engine must track it) and let the next pass confirm
            # the stop from the broker's own list.
            self.last_error = f"filled, stop not yet confirmed: {exc}"
            return Fill(True, int(res.ticket), price, touch, float(res.filled_volume or volume),
                        "filled (stop not yet confirmed)")
        if pos is None or not pos.sl:
            try:
                r = self.broker.modify_stops(res.ticket, sl, tpx)
                if not r.ok:
                    raise RuntimeError(f"stop not accepted ({r.retcode})")
            except Exception as exc:
                self.last_error = f"no server-side stop: {exc}"
                try:
                    self.broker.close(res.ticket, 0.0, f"{self.tag} no-stop")
                except Exception:
                    pass
                return Fill(False, message=f"closed at once: {self.last_error}")
        return Fill(True, int(res.ticket), price, touch, float(res.filled_volume or volume), "filled")

    def modify_stop(self, ticket: int, stop: float, tp: Optional[float] = None) -> bool:
        try:
            if tp is None:
                pos = next((p for p in self.positions() if p.ticket == ticket), None)
                tp = float(pos.tp) if pos is not None else 0.0
            r = self.broker.modify_stops(ticket, float(stop), float(tp or 0.0))
            if not r.ok:
                self.last_error = f"stop refused ({r.retcode})"
            return bool(r.ok)
        except Exception as exc:
            self.last_error = str(exc)
            return False

    def close(self, ticket: int, tick: Tick, spec, at_price: Optional[float] = None, comment: str = "") -> Fill:
        pos = next((p for p in self.positions() if p.ticket == ticket), None)
        mark = float(tick.bid if (pos is None or pos.side is Side.BUY) else tick.ask)
        try:
            r = self.broker.close(ticket, 0.0, (comment or self.tag)[:31])
        except Exception as exc:
            self.last_error = str(exc)
            return Fill(False, message=f"close failed: {exc}")
        if not r.ok:
            self.last_error = f"close rejected ({r.retcode})"
            return Fill(False, message=self.last_error)
        return Fill(True, ticket, float(r.price or mark), mark, float(r.filled_volume or (pos.volume if pos else 0.0)), "closed")

    def stop_hit(self, ticket: int, tick: Tick) -> bool:
        return False                                       # the broker closes it; the engine notices it is gone

    def target_hit(self, ticket: int, tick: Tick) -> bool:
        return False

    def positions(self) -> list[Position]:
        try:
            return [p for p in self.broker.positions(self.magic) if int(getattr(p, "magic", 0) or 0) == self.magic]
        except Exception as exc:
            self.last_error = str(exc)
            raise

    def closed_deal(self, ticket: int, tries: int = 3, pause: float = 0.2) -> Optional[dict]:
        """The broker's record of the closed position: pnl is profit + commission
        + swap. Retried briefly because the history can lag the close."""
        for i in range(max(1, tries)):
            try:
                d = self.broker.closed_deal(ticket)
            except Exception:
                d = None
            if d and d.get("pnl") is not None:
                return self._with_entry_commission(ticket, dict(d))
            if i + 1 < tries:
                time.sleep(pause)
        return None

    def _with_entry_commission(self, ticket: int, d: dict) -> dict:
        """MetaTrader charges commission on the entry deal as well as the exit;
        ``closed_deal`` sums the closing deals only. Add the entry half from the
        deal history so the figure is the whole round trip. If the history
        cannot be read the figure is left as it is and marked."""
        fn = getattr(self.broker, "deals_since", None)
        if fn is None:
            return d
        try:
            when = d.get("time")
            since = (to_utc(when) if isinstance(when, dt.datetime) else to_utc(dt.datetime.now(dt.timezone.utc))) \
                - dt.timedelta(days=7)
            rows = fn(since, self.magic, False) or []
            entry = sum(float(r.get("commission") or 0.0) for r in rows
                        if r.get("is_entry") and int(r.get("position") or 0) == int(ticket))
            d["entry_commission"] = round(entry, 2)
            d["pnl"] = float(d["pnl"]) + entry
        except Exception as exc:
            d["entry_commission_unknown"] = str(exc)[:120]
        return d


def make_executor(mode: str, broker=None, magic: int = 0, tag: str = "BOT", slippage_points: float = 1.0,
                  first_ticket: int = 700_000_001):
    """OFF -> None (nothing can ever be sent). PAPER -> PaperExecutor. LIVE -> LiveExecutor."""
    m = str(mode or "PAPER").upper()
    if m == "LIVE":
        if broker is None:
            raise ValueError("LIVE needs a broker")
        return LiveExecutor(broker, magic, tag)
    if m == "PAPER":
        return PaperExecutor(slippage_points, first_ticket, tag)
    return None
