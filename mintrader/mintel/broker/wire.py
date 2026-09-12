"""Wire format for the broker bridge.

On macOS the ``MetaTrader5`` Python package cannot run natively - it is a
Windows library.  MetaTrader 5 itself runs under Wine, and so does a small
Windows Python process that talks to it.  Everything else - the intelligence,
the journal, the dashboard - runs as ordinary native macOS Python.

This module is the contract between those two halves: plain JSON, so the Wine
side needs nothing but the standard library and ``MetaTrader5``.  That matters
a great deal in practice: getting numpy and scikit-learn wheels working inside
a Wine prefix is painful, and this design means we never have to.

Timestamps cross the wire as UTC ISO-8601 strings.  Enums cross as their
values.  Nothing crosses as a pickle, because a pickle from another process is
code execution wearing a hat.
"""
from __future__ import annotations

import datetime as dt
from typing import Any, Optional

from ..clock import UTC
from ..contracts import SymbolSpec
from .base import (AccountInfo, Bar, CalendarEvent, OrderRequest, OrderResult,
                   Position, Side, Tick)


def _iso(ts: Optional[dt.datetime]) -> Optional[str]:
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC).isoformat()


def _dt(raw: Optional[str]) -> Optional[dt.datetime]:
    if not raw:
        return None
    parsed = dt.datetime.fromisoformat(raw)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


# ------------------------------------------------------------------ encode ---

def tick_out(t: Optional[Tick]) -> Optional[dict]:
    if t is None:
        return None
    return {"symbol": t.symbol, "time": _iso(t.time), "bid": t.bid,
            "ask": t.ask, "last": t.last, "volume": t.volume}


def bar_out(b: Bar) -> list:
    # A list, not a dict: bars move in thousands and the key names would be
    # most of the payload.
    return [_iso(b.time), b.open, b.high, b.low, b.close, b.tick_volume,
            b.real_volume, b.spread_points]


def position_out(p: Position) -> dict:
    return {"ticket": p.ticket, "symbol": p.symbol, "side": p.side.value,
            "volume": p.volume, "entry_price": p.entry_price, "sl": p.sl,
            "tp": p.tp, "open_time": _iso(p.open_time), "profit": p.profit,
            "swap": p.swap, "comment": p.comment, "magic": p.magic}


def spec_out(s: Optional[SymbolSpec]) -> Optional[dict]:
    if s is None:
        return None
    return s.to_dict()


def account_out(a: AccountInfo) -> dict:
    return {"login": a.login, "server": a.server, "currency": a.currency,
            "balance": a.balance, "equity": a.equity, "margin": a.margin,
            "margin_free": a.margin_free, "margin_level": a.margin_level,
            "leverage": a.leverage, "trade_allowed": a.trade_allowed,
            "is_demo": a.is_demo, "company": a.company, "name": a.name}


def order_result_out(r: OrderResult) -> dict:
    return {"ok": r.ok, "retcode": r.retcode, "ticket": r.ticket,
            "deal": r.deal, "filled_volume": r.filled_volume,
            "price": r.price, "requested_volume": r.requested_volume,
            "comment": r.comment, "idempotency_key": r.idempotency_key}


def event_out(e: CalendarEvent) -> dict:
    return {"event_id": e.event_id, "time_utc": _iso(e.time_utc),
            "currency": e.currency, "country": e.country, "name": e.name,
            "importance": e.importance, "actual": e.actual,
            "forecast": e.forecast, "previous": e.previous,
            "revised_previous": e.revised_previous, "unit": e.unit,
            "source": e.source}


def request_out(r: OrderRequest) -> dict:
    return {"symbol": r.symbol, "side": r.side.value, "volume": r.volume,
            "sl": r.sl, "tp": r.tp, "price": r.price,
            "deviation_points": r.deviation_points, "comment": r.comment,
            "magic": r.magic, "idempotency_key": r.idempotency_key,
            "filling": r.filling}


# ------------------------------------------------------------------ decode ---

def tick_in(d: Optional[dict]) -> Optional[Tick]:
    if not d:
        return None
    return Tick(symbol=d["symbol"], time=_dt(d["time"]), bid=d["bid"],
                ask=d["ask"], last=d.get("last", 0.0),
                volume=d.get("volume", 0.0))


def bar_in(row: list) -> Bar:
    return Bar(time=_dt(row[0]), open=row[1], high=row[2], low=row[3],
               close=row[4], tick_volume=row[5], real_volume=row[6],
               spread_points=row[7])


def position_in(d: dict) -> Position:
    return Position(ticket=d["ticket"], symbol=d["symbol"],
                    side=Side(d["side"]), volume=d["volume"],
                    entry_price=d["entry_price"], sl=d["sl"], tp=d["tp"],
                    open_time=_dt(d["open_time"]), profit=d.get("profit", 0.0),
                    swap=d.get("swap", 0.0), comment=d.get("comment", ""),
                    magic=d.get("magic", 0))


def spec_in(d: Optional[dict]) -> Optional[SymbolSpec]:
    if not d:
        return None
    payload = dict(d)
    payload["filling_modes"] = tuple(payload.get("filling_modes") or ("IOC",))
    return SymbolSpec(**payload)


def account_in(d: dict) -> AccountInfo:
    return AccountInfo(**d)


def order_result_in(d: dict) -> OrderResult:
    return OrderResult(ok=d["ok"], retcode=d["retcode"],
                       ticket=d.get("ticket", 0), deal=d.get("deal", 0),
                       filled_volume=d.get("filled_volume", 0.0),
                       price=d.get("price", 0.0),
                       requested_volume=d.get("requested_volume", 0.0),
                       comment=d.get("comment", ""),
                       idempotency_key=d.get("idempotency_key", ""))


def event_in(d: dict) -> CalendarEvent:
    return CalendarEvent(event_id=d["event_id"], time_utc=_dt(d["time_utc"]),
                         currency=d["currency"], country=d.get("country", ""),
                         name=d.get("name", ""),
                         importance=d.get("importance", 0),
                         actual=d.get("actual"), forecast=d.get("forecast"),
                         previous=d.get("previous"),
                         revised_previous=d.get("revised_previous"),
                         unit=d.get("unit", ""), source=d.get("source", "mt5"))


def request_in(d: dict) -> OrderRequest:
    return OrderRequest(symbol=d["symbol"], side=Side(d["side"]),
                        volume=d["volume"], sl=d["sl"], tp=d.get("tp", 0.0),
                        price=d.get("price", 0.0),
                        deviation_points=d.get("deviation_points", 20),
                        comment=d.get("comment", ""), magic=d.get("magic", 0),
                        idempotency_key=d.get("idempotency_key", ""),
                        filling=d.get("filling", "IOC"))
