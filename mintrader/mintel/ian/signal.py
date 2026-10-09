"""The signal handed from the intelligence layer to the execution layer.

A :class:`Signal` is everything the MT5 side needs and nothing it has to
interpret: the instrument it came from, the spot symbol and the SPOT
direction (already translated - JPY, CAD and CHF inverted), the confidence,
the plain-English reasoning, the intended protective stop as a spot price,
the strategy id and the time.

Its id is IDEMPOTENT: a pure function of the strategy, the futures root, the
direction and the trigger time rounded down to a bucket (60 s by default). The
same opportunity seen again in the same minute - or the same signal sent
again after a timeout - has the same id, and the bridge's persisted dedupe
store guarantees one id can never produce a second order.
"""
from __future__ import annotations

import datetime as dt
import hashlib
from dataclasses import dataclass, field
from typing import Optional, Sequence

from ..broker.base import Bar, Side, Tick
from . import STRATEGY_ID
from .features import FlowSnapshot
from .mapping import futures_distance_to_spot, root_of
from .score import Opportunity


def make_signal_id(strategy: str, instrument: str, spot_direction: int, trigger_ts: dt.datetime,
                   bucket_seconds: int = 60) -> str:
    """sha1(strategy | root | BUY/SELL | floor(trigger time / bucket)) - 20 hex characters."""
    root = root_of(instrument) or str(instrument)
    side = "BUY" if spot_direction > 0 else "SELL"
    bucket = int(trigger_ts.timestamp() // max(1, int(bucket_seconds)))
    raw = f"{strategy}|{root}|{side}|{bucket}"
    return hashlib.sha1(raw.encode()).hexdigest()[:20]


@dataclass
class Signal:
    signal_id: str
    strategy_id: str
    instrument: str               # the futures contract (6EZ6)
    root: str                     # 6E
    spot_symbol: str              # EURUSD
    direction: str                # BUY / SELL on the SPOT pair
    futures_direction: int        # +1 / -1 in the futures book
    confidence: float
    score: float
    reasoning: str
    intended_stop: float          # spot price
    stop_distance: float          # spot price distance from the reference entry
    entry_ref: float              # the spot touch the stop was planned from
    trigger_ts: dt.datetime       # market time of the book reading that triggered it
    created_ts: dt.datetime
    regime: str = ""
    components: list = field(default_factory=list)
    stop_basis: str = ""
    data_class: str = "INSTITUTIONAL"
    data_source: str = "LIVE FEED"     # LIVE FEED / REPLAY / SYNTHETIC
    latency: dict = field(default_factory=dict)

    @property
    def side(self) -> Side:
        return Side.BUY if self.direction == "BUY" else Side.SELL

    def to_dict(self) -> dict:
        return {"signal_id": self.signal_id, "strategy_id": self.strategy_id, "instrument": self.instrument,
                "root": self.root, "spot_symbol": self.spot_symbol, "direction": self.direction,
                "futures_direction": self.futures_direction, "confidence": self.confidence, "score": self.score,
                "reasoning": self.reasoning, "intended_stop": self.intended_stop, "stop_distance": self.stop_distance,
                "entry_ref": self.entry_ref, "trigger_ts": self.trigger_ts.isoformat(),
                "created_ts": self.created_ts.isoformat(), "regime": self.regime, "components": self.components,
                "stop_basis": self.stop_basis, "data_class": self.data_class, "data_source": self.data_source,
                "latency": dict(self.latency)}


@dataclass
class StopConfig:
    structure_buffer_ticks: float = 2.0    # beyond the futures structure, in futures ticks
    atr_mult: float = 1.5                  # x the spot 1-minute ATR
    min_stop_pips: float = 6.0
    max_stop_pips: float = 30.0
    spread_mult: float = 3.0
    # a CFD on a generic instrument (BTCUSD ...): its "pip" is one point, so the limits are basis points of the
    # price instead (both > 0 replaces the pip limits)
    min_stop_bp: float = 0.0
    max_stop_bp: float = 0.0

    @property
    def in_bp(self) -> bool:
        return self.min_stop_bp > 0 and self.max_stop_bp > 0


@dataclass
class StopPlan:
    ok: bool
    stop: float = 0.0
    distance: float = 0.0
    basis: str = ""
    reason: str = ""


def _atr(bars: Sequence[Bar], n: int = 14) -> float:
    if len(bars) < 2:
        return 0.0
    trs = [max(b.high - b.low, abs(b.high - a.close), abs(b.low - a.close)) for a, b in zip(bars, bars[1:])][-n:]
    return sum(trs) / len(trs) if trs else 0.0


def _dist_txt(d: float, price: float) -> str:
    """A CFD distance in price and basis points: '150.00 (15 bp)'."""
    return f"{d:.2f} ({d / price * 1e4:.0f} bp)" if price > 0 else f"{d:.2f}"


def plan_stop(fs: FlowSnapshot, opp: Opportunity, tick: Tick, spec, m1: Sequence[Bar] = (),
              cfg: Optional[StopConfig] = None) -> StopPlan:
    """The protective stop, as a SPOT price. Its distance is the largest of:
    the futures structure (beyond the medium-window low for a bullish future, high for a bearish one, plus a
    buffer, converted to spot units - 1/x for the inverted pairs), atr_mult x the spot 1-minute ATR, the
    minimum in pips, and spread_mult x the spot spread. More than max_stop_pips away = no trade: the stop is
    never squeezed to make a trade fit."""
    cfg = cfg or StopConfig()
    side = Side.BUY if opp.spot_direction > 0 else Side.SELL
    touch = float(tick.ask if side is Side.BUY else tick.bid)
    pip = float(spec.pip_size)
    if pip <= 0 or touch <= 0:
        return StopPlan(False, reason="no usable spot price or pip size")
    root = root_of(fs.instrument)
    basis = []
    d_struct = 0.0
    if fs.ok and fs.mid is not None and fs.range_low is not None and fs.range_high is not None:
        buf = cfg.structure_buffer_ticks * fs.tick
        f_dist = (fs.mid - fs.range_low + buf) if opp.direction > 0 else (fs.range_high - fs.mid + buf)
        try:
            d_struct = futures_distance_to_spot(root, fs.mid, max(f_dist, 0.0))
        except (ValueError, KeyError):
            d_struct = 0.0
        basis.append(f"book structure {_dist_txt(d_struct, touch)}" if cfg.in_bp
                     else f"futures structure {d_struct / pip:.1f} pips")
    d_atr = cfg.atr_mult * _atr(list(m1)) if m1 else 0.0
    if d_atr:
        basis.append(f"{cfg.atr_mult:g}x 1-min ATR {_dist_txt(d_atr, touch)}" if cfg.in_bp
                     else f"{cfg.atr_mult:g}x 1-min ATR {d_atr / pip:.1f} pips")
    d_min = (touch * cfg.min_stop_bp / 1e4) if cfg.in_bp else cfg.min_stop_pips * pip
    d_spread = cfg.spread_mult * max(float(tick.ask) - float(tick.bid), 0.0)
    try:
        d_broker = spec.min_stop_distance_price(float(tick.ask) - float(tick.bid)) * 1.2
    except Exception:
        d_broker = 0.0
    dist = max(d_struct, d_atr, d_min, d_spread, d_broker)
    if cfg.in_bp:
        d_max = touch * cfg.max_stop_bp / 1e4
        if dist > d_max + 1e-12:
            return StopPlan(False, distance=dist, basis=", ".join(basis),
                            reason=(f"the structural stop would be {dist:.2f} away ({dist / touch * 1e4:.0f} bp of the "
                                    f"price), over {cfg.max_stop_bp:g} bp"))
        stop = spec.normalise_price(touch - side.sign * dist)
        return StopPlan(True, stop, abs(touch - stop), ", ".join(basis) or f"minimum {cfg.min_stop_bp:g} bp")
    if dist > cfg.max_stop_pips * pip + 1e-12:
        return StopPlan(False, distance=dist, basis=", ".join(basis),
                        reason=f"the structural stop would be {dist / pip:.1f} pips away, over {cfg.max_stop_pips:g}")
    stop = spec.normalise_price(touch - side.sign * dist)
    return StopPlan(True, stop, abs(touch - stop), ", ".join(basis) or f"minimum {cfg.min_stop_pips:g} pips")


def build_signal(opp: Opportunity, fs: FlowSnapshot, tick: Tick, spec, now: dt.datetime,
                 m1: Sequence[Bar] = (), stop_cfg: Optional[StopConfig] = None, data_source: str = "LIVE FEED",
                 data_received: Optional[dt.datetime] = None, bucket_seconds: int = 60,
                 strategy_id: str = STRATEGY_ID) -> tuple[Optional[Signal], str]:
    """A Signal from a tradeable opportunity, or (None, why not)."""
    if not opp.tradeable or opp.spot_direction == 0:
        return None, opp.why_not or "not tradeable"
    plan = plan_stop(fs, opp, tick, spec, m1, stop_cfg)
    if not plan.ok:
        return None, plan.reason
    side = "BUY" if opp.spot_direction > 0 else "SELL"
    trigger = fs.ts
    sid = make_signal_id(strategy_id, opp.root, opp.spot_direction, trigger, bucket_seconds)
    touch = float(tick.ask if side == "BUY" else tick.bid)
    lat = {"data_received": (data_received or trigger).isoformat(), "signal": now.isoformat()}
    return Signal(sid, strategy_id, fs.instrument, opp.root, opp.spot_symbol, side, opp.direction,
                  opp.confidence, opp.score, opp.reasoning, plan.stop, plan.distance, touch, trigger, now,
                  opp.regime.regime, [c.to_dict() for c in opp.components], plan.basis, opp.data_class, data_source,
                  lat), ""
