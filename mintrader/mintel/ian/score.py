"""The FINANCIAL IAN OPPORTUNITY SCORE.

One score per instrument, built from independent pieces of evidence, each a
signed number in [-1, 1] in the FUTURES direction (+ = buyers have the edge):

    component        group         source           what it measures
    order_flow       FLOW          INSTITUTIONAL    order-flow imbalance at the touch (OFI)
    aggressive       FLOW          INSTITUTIONAL    who is crossing the spread, how hard, large prints
    delta_footprint  FLOW          INSTITUTIONAL    stacked footprint imbalance, cumulative delta slope
    book_pressure    BOOK          INSTITUTIONAL    distance-weighted imbalance, its persistence, microprice
    liquidity_ahead  BOOK          INSTITUTIONAL    how thin the book is in each direction, walls in the way
    consumption      CONSUMPTION   INSTITUTIONAL    eaten vs replenished, sweeps, pulling, walls taken, resilience
    absorption       ABSORPTION    INSTITUTIONAL    ABSORPTION_SCORE and POSSIBLE REFRESH levels
    profile_vwap     CONTEXT       INSTITUTIONAL    acceptance above/below value and VWAP
    chart            CHART         RETAIL (MT5)     spot momentum and structure on 1m, 5m and 1h bars
    news             NEWS          RETAIL (MT5)     the latest release's surprise for the two currencies
    cross_market     CROSS         INSTITUTIONAL    the other dollar futures moving the same way

    S = sum(w_i * v_i) / sum(w_i)          (weights adjusted by the regime's tactic)
    direction = sign(S); strength = |S|
    score = 100 * min(1, strength / full_strength) * regime x execution quality x history

Early entry, not a chain of confirmations: a trade needs the score over the
threshold and at least ``min_groups`` independent groups agreeing (two of them
from the institutional book), with no group strongly against. Execution
quality (the spot spread and quote age) and the bot's own history after costs
scale the score; they never invent a direction.
"""
from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass, field
from typing import Optional, Sequence

from . import INSTITUTIONAL, RETAIL
from ..broker.base import Bar, CalendarEvent
from ..news.surprise import family_sign
from .features import FlowSnapshot
from .mapping import CONTRACTS, contract, currency_effect, futures_direction, root_of, spot_direction
from .regime import (ABSORPTION, BALANCED, BREAKOUT, NewsContext, REVERSAL, RegimeRead, TRENDING)

GROUPS = ("FLOW", "BOOK", "CONSUMPTION", "ABSORPTION", "CONTEXT", "CHART", "NEWS", "CROSS")
INSTITUTIONAL_GROUPS = ("FLOW", "BOOK", "CONSUMPTION", "ABSORPTION")

BASE_WEIGHTS = {"order_flow": 0.8, "aggressive": 1.2, "delta_footprint": 0.6, "book_pressure": 0.8,
                "liquidity_ahead": 0.5, "consumption": 1.0, "absorption": 1.0, "profile_vwap": 0.4,
                "chart": 0.7, "news": 0.6, "cross_market": 0.5}
COMPONENT_GROUP = {"order_flow": "FLOW", "aggressive": "FLOW", "delta_footprint": "FLOW", "book_pressure": "BOOK",
                   "liquidity_ahead": "BOOK", "consumption": "CONSUMPTION", "absorption": "ABSORPTION",
                   "profile_vwap": "CONTEXT", "chart": "CHART", "news": "NEWS", "cross_market": "CROSS"}
REGIME_WEIGHTS = {
    ABSORPTION: {"aggressive": 0.25, "order_flow": 0.25, "absorption": 1.5},
    BREAKOUT: {"consumption": 1.3, "aggressive": 1.2, "book_pressure": 0.7},
    TRENDING: {"aggressive": 1.2, "profile_vwap": 1.5},
    BALANCED: {"book_pressure": 1.2, "absorption": 1.2},
    REVERSAL: {"absorption": 1.3},
}


@dataclass
class ScoreConfig:
    full_strength: float = 0.45       # |S| at which the raw score reaches 100
    entry_score: float = 60.0
    min_groups: int = 3
    min_institutional_groups: int = 2
    agree_level: float = 0.15
    against_level: float = 0.35
    no_entry_before_release_s: float = 120.0
    no_entry_after_release_s: float = 60.0


@dataclass
class Component:
    name: str
    value: float
    weight: float
    group: str
    source: str
    note: str = ""

    def to_dict(self) -> dict:
        return {"name": self.name, "value": round(self.value, 3), "weight": round(self.weight, 3), "group": self.group,
                "source": self.source, "note": self.note}


@dataclass
class ChartContext:
    """RETAIL: built from the MT5 spot bars. ``value`` is in the SPOT pair's direction."""
    symbol: str
    ok: bool
    value: float = 0.0
    note: str = ""
    atr_m1: float = 0.0
    where: str = ""


@dataclass
class Opportunity:
    instrument: str
    root: str
    spot_symbol: str
    ts: dt.datetime
    direction: int                    # FUTURES direction
    spot_direction: int               # after the mapping (JPY, CAD, CHF invert)
    score: float
    confidence: float
    strength: float
    regime: RegimeRead
    components: list = field(default_factory=list)
    groups: dict = field(default_factory=dict)
    agree: list = field(default_factory=list)
    against: list = field(default_factory=list)
    tradeable: bool = False
    why_not: str = ""
    reasoning: str = ""
    data_class: str = INSTITUTIONAL

    def to_dict(self) -> dict:
        return {"instrument": self.instrument, "root": self.root, "spot_symbol": self.spot_symbol,
                "ts": self.ts.isoformat(), "direction": self.direction, "spot_direction": self.spot_direction,
                "spot_side": "BUY" if self.spot_direction > 0 else "SELL" if self.spot_direction < 0 else "",
                "score": round(self.score, 1), "confidence": round(self.confidence, 3), "strength": round(self.strength, 3),
                "regime": self.regime.to_dict(), "components": [c.to_dict() for c in self.components],
                "groups": {k: round(v, 3) for k, v in self.groups.items()}, "agree": list(self.agree),
                "against": list(self.against), "tradeable": self.tradeable, "why_not": self.why_not,
                "reasoning": self.reasoning, "data_class": self.data_class}


def _clip(x: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return lo if x < lo else hi if x > hi else x


def _s(x: float) -> int:
    return (x > 0) - (x < 0)


# ------------------------------------------------------------------ chart --

def _ema(vals: Sequence[float], n: int) -> list[float]:
    if not vals:
        return []
    k = 2.0 / (n + 1.0)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out


def _atr(bars: Sequence[Bar], n: int = 14) -> float:
    if len(bars) < 2:
        return 0.0
    trs = [max(b.high - b.low, abs(b.high - a.close), abs(b.low - a.close)) for a, b in zip(bars, bars[1:])]
    trs = trs[-n:]
    return sum(trs) / len(trs) if trs else 0.0


def chart_context(symbol: str, m1: Sequence[Bar] = (), m5: Sequence[Bar] = (), h1: Sequence[Bar] = ()) -> ChartContext:
    """Spot chart context (RETAIL, MT5 bars), in the SPOT direction:
    m1 = tanh((close - EMA20) / ATR14) on 1-minute bars; m5 = tanh((EMA20 - EMA50) / ATR14) on 5-minute bars;
    h1 = position in the last 24 hourly bars' range mapped to [-1, 1].
    value = 0.5 m1 + 0.3 m5 + 0.2 h1. The book says WHAT is happening; this says WHERE."""
    m1, m5, h1 = list(m1 or []), list(m5 or []), list(h1 or [])
    if len(m1) < 21:
        return ChartContext(symbol, False, note="not enough 1-minute bars")
    atr1 = _atr(m1)
    if atr1 <= 0:
        return ChartContext(symbol, False, note="no range on the 1-minute bars")
    closes = [b.close for b in m1]
    v1 = math.tanh((closes[-1] - _ema(closes, 20)[-1]) / atr1)
    v5 = 0.0
    if len(m5) >= 51:
        c5 = [b.close for b in m5]
        atr5 = _atr(m5) or atr1
        v5 = math.tanh((_ema(c5, 20)[-1] - _ema(c5, 50)[-1]) / atr5)
    vh, where = 0.0, ""
    if len(h1) >= 6:
        hi = max(b.high for b in h1[-24:])
        lo = min(b.low for b in h1[-24:])
        if hi > lo:
            pos = (closes[-1] - lo) / (hi - lo)
            vh = _clip(2.0 * pos - 1.0)
            where = ("at the top of the day's range" if pos >= 0.85 else "at the bottom of the day's range"
                     if pos <= 0.15 else "inside the day's range")
    value = _clip(0.5 * v1 + 0.3 * v5 + 0.2 * vh)
    words = "rising" if value > 0.15 else "falling" if value < -0.15 else "flat"
    return ChartContext(symbol, True, value, f"the {symbol} chart is {words}", atr1, where)


# ------------------------------------------------------------------- news --

def news_context(root: str, events: Sequence[CalendarEvent], now: dt.datetime, min_importance: int = 3,
                 surprise_window_s: float = 1800.0) -> NewsContext:
    """From the MT5 calendar (RETAIL): the latest and next high-impact release for the future's two
    currencies, and what the latest surprise means for the FUTURE: family sign x sign(actual - forecast)
    gives good/bad for that currency; good for the foreign currency = future up, good for USD = future down."""
    c = contract(root)
    if c is None or c.asset != "FX":
        return NewsContext()
    ccys = {c.currency, "USD"}
    rel = [e for e in events if e.currency in ccys and int(e.importance) >= min_importance]
    past = sorted((e for e in rel if e.time_utc <= now), key=lambda e: e.time_utc)
    fut = sorted((e for e in rel if e.time_utc > now), key=lambda e: e.time_utc)
    ctx = NewsContext()
    if past:
        last = past[-1]
        ctx.seconds_since_release = (now - last.time_utc).total_seconds()
        ctx.event = f"{last.currency} {last.name}"
        if last.actual is not None and last.forecast is not None and ctx.seconds_since_release <= surprise_window_s:
            fam = family_sign(last.name)
            gap = float(last.actual) - float(last.forecast)
            if fam and gap:
                ctx.futures_effect = currency_effect(root, last.currency, fam * _s(gap))
                ctx.surprise = gap
    if fut:
        ctx.seconds_to_next = (fut[0].time_utc - now).total_seconds()
        if not ctx.event:
            ctx.event = f"{fut[0].currency} {fut[0].name}"
    return ctx


# ------------------------------------------------------------ cross-market --

def flow_direction(fs: FlowSnapshot) -> float:
    """One number for an instrument's flow: 0.5 x medium aggression + 0.5 x tanh(OFI / 2)."""
    return _clip(0.5 * fs.aggr_medium + 0.5 * math.tanh(fs.ofi_medium / 2.0))


def cross_market_value(root: str, snaps: dict) -> Optional[float]:
    """The dollar factor: every FX future is priced in dollars per foreign unit, so a broad dollar move
    lifts or drops them all together. Mean flow direction of the OTHER FX futures with a reliable book;
    None with fewer than two of them."""
    vals = [flow_direction(fs) for r, fs in snaps.items()
            if r != root and r in CONTRACTS and fs is not None and fs.ok]
    if len(vals) < 2:
        return None
    return _clip(sum(vals) / len(vals))


# -------------------------------------------------------------- the score --

def components(fs: FlowSnapshot, regime: str = "") -> list[Component]:
    """The institutional components, from the futures book. In a REVERSAL the aggression is read from
    the last 5 seconds only: the 30-second window still remembers the side that was absorbed."""
    out = []
    of = _clip(0.5 * math.tanh(fs.ofi_short) + 0.5 * math.tanh(fs.ofi_medium / 2.0))
    out.append(Component("order_flow", of, BASE_WEIGHTS["order_flow"], "FLOW", INSTITUTIONAL,
                         f"OFI {fs.ofi_short:+.2f} (5 s), {fs.ofi_medium:+.2f} (30 s) touch-depths"))
    if regime == REVERSAL:
        ag = _clip(fs.aggr_short * min(1.0, fs.volume_intensity) + 0.5 * fs.large_net)
    else:
        ag = _clip(0.6 * fs.aggr_short * min(1.0, fs.volume_intensity) + 0.4 * fs.aggr_medium + 0.5 * fs.large_net)
    out.append(Component("aggressive", ag, BASE_WEIGHTS["aggressive"], "FLOW", INSTITUTIONAL,
                         f"aggression {fs.aggr_short:+.2f} (5 s) {fs.aggr_medium:+.2f} (30 s), intensity "
                         f"{fs.intensity:.1f}x, large prints {fs.large_net:+.2f}"))
    df = 0.5 * fs.footprint_signal
    if fs.delta_reliable:
        df += 0.4 * math.tanh(fs.delta_slope) + 0.3 * fs.delta_divergence
    out.append(Component("delta_footprint", _clip(df), BASE_WEIGHTS["delta_footprint"], "FLOW", INSTITUTIONAL,
                         ("stacked buying" if fs.stacked_buy else "stacked selling" if fs.stacked_sell else
                          "no stacked imbalance") + (f", delta slope {fs.delta_slope:+.2f}" if fs.delta_reliable
                                                     else ", delta not reliable on this feed")))
    bp = _clip(0.4 * fs.imbalance + 0.4 * fs.imbalance_avg * fs.imbalance_persist + 0.2 * fs.micro_offset)
    out.append(Component("book_pressure", bp, BASE_WEIGHTS["book_pressure"], "BOOK", INSTITUTIONAL,
                         f"imbalance {fs.imbalance:+.2f} now, {fs.imbalance_avg:+.2f} over 30 s, microprice "
                         f"{fs.micro_offset:+.2f}"))
    la = math.tanh(1.5 * (fs.depth5_bid_ratio - fs.depth5_ask_ratio))
    if fs.wall_ask and fs.wall_ask.get("distance_ticks", 99) <= 3:
        la -= 0.3
    if fs.wall_bid and fs.wall_bid.get("distance_ticks", 99) <= 3:
        la += 0.3
    out.append(Component("liquidity_ahead", _clip(la), BASE_WEIGHTS["liquidity_ahead"], "BOOK", INSTITUTIONAL,
                         f"near depth bid {fs.depth5_bid_ratio:.0%} / offer {fs.depth5_ask_ratio:.0%} of normal"))
    cs = 0.4 * fs.consumption_signal + 0.3 * fs.pulling_signal + 0.3 * fs.wall_signal + 0.2 * fs.resilience_signal
    if fs.sweep_dir and fs.sweep_age_s is not None and fs.sweep_age_s <= 20:
        cs += 0.4 * fs.sweep_dir
    out.append(Component("consumption", _clip(cs), BASE_WEIGHTS["consumption"], "CONSUMPTION", INSTITUTIONAL,
                         f"consumed-vs-replenished {fs.consumption_signal:+.2f}, pulling {fs.pulling_signal:+.2f}, "
                         f"walls {fs.wall_signal:+.2f}, sweep {fs.sweep_dir:+d}"))
    ab = fs.absorption_dir * fs.absorption_score + 0.5 * fs.refresh_signal
    out.append(Component("absorption", _clip(ab), BASE_WEIGHTS["absorption"], "ABSORPTION", INSTITUTIONAL,
                         f"absorption {fs.absorption_score:.2f} toward {fs.absorption_dir:+d}, refresh {fs.refresh_signal:+.2f}"))
    pv = 0.5 * fs.profile_signal + 0.5 * fs.vwap_signal
    out.append(Component("profile_vwap", _clip(pv), BASE_WEIGHTS["profile_vwap"], "CONTEXT", INSTITUTIONAL,
                         f"{fs.profile_location or 'profile building'}, {fs.vwap_dist_ticks:+.1f} ticks from VWAP"))
    return out


def score(fs: FlowSnapshot, regime: RegimeRead, *, spot_symbol: str = "", chart: Optional[ChartContext] = None,
          news: Optional[NewsContext] = None, cross: Optional[float] = None, exec_quality: float = 1.0,
          exec_note: str = "", history_mult: float = 1.0, history_note: str = "",
          cfg: Optional[ScoreConfig] = None) -> Opportunity:
    cfg = cfg or ScoreConfig()
    root = root_of(fs.instrument)
    c = contract(root)
    comps = components(fs, regime.regime) if fs.ok else []
    # retail and cross-market evidence, in the FUTURES direction
    if chart is not None and chart.ok and c is not None and c.spot:
        comps.append(Component("chart", _clip(futures_direction(root, _s(chart.value)) * abs(chart.value)),
                               BASE_WEIGHTS["chart"], "CHART", RETAIL, chart.note + (f", {chart.where}" if chart.where else "")))
    if news is not None and news.futures_effect:
        comps.append(Component("news", float(news.futures_effect), BASE_WEIGHTS["news"], "NEWS", RETAIL,
                               f"{news.event}: surprise {news.surprise:+g}"))
    if cross is not None:
        comps.append(Component("cross_market", cross, BASE_WEIGHTS["cross_market"], "CROSS", INSTITUTIONAL,
                               f"the other dollar futures {cross:+.2f}"))
    # the regime's tactic re-weights the evidence (it never adds any)
    mult = REGIME_WEIGHTS.get(regime.regime, {})
    usef = fs.usefulness.get("book_imbalance") if fs.ok else None
    for comp in comps:
        comp.weight *= mult.get(comp.name, 1.0)
        if comp.name == "book_pressure" and usef and usef.get("n", 0) >= 100 and usef.get("hit_rate") is not None:
            comp.weight *= _clip(1.0 + 4.0 * (usef["hit_rate"] - 0.5), 0.5, 1.5)    # historical usefulness
    wsum = sum(x.weight for x in comps)
    S = sum(x.weight * x.value for x in comps) / wsum if wsum > 0 else 0.0
    direction = _s(S) if abs(S) >= 0.05 else 0
    groups: dict[str, float] = {}
    for g in GROUPS:
        mem = [x for x in comps if x.group == g]
        w = sum(x.weight for x in mem)
        if mem and w > 0:
            groups[g] = sum(x.weight * x.value for x in mem) / w
    agree = [g for g, v in groups.items() if direction and v * direction >= cfg.agree_level]
    against = [g for g, v in groups.items() if direction and v * direction <= -cfg.against_level]
    raw = 100.0 * min(1.0, abs(S) / cfg.full_strength)
    sc = raw * regime.confidence_mult * _clip(exec_quality, 0.0, 1.0) * _clip(history_mult, 0.5, 1.3)
    spot = spot_symbol or (c.spot if c is not None else "")
    sdir = spot_direction(root, direction) if (direction and c is not None) else 0
    opp = Opportunity(fs.instrument, root, spot, fs.ts, direction, sdir, round(sc, 1), round(min(1.0, sc / 100.0), 3),
                      abs(S), regime, comps, groups, agree, against)
    # can it be traded?
    why = []
    inst = [g for g in agree if g in INSTITUTIONAL_GROUPS]
    if not fs.ok:
        why.append(fs.reason or "no reliable book")
    if c is None or c.context_only or not spot:
        why.append("context market only (not traded)" if c is not None and c.context_only else "no spot symbol to trade")
    if direction == 0:
        why.append("no side has the advantage")
    if not regime.allow_entry:
        why.append(f"{regime.regime}: {regime.tactic}")
    threshold = cfg.entry_score + regime.extra_score
    if sc < threshold:
        why.append(f"score {sc:.0f} below {threshold:.0f}")
    if len(agree) < cfg.min_groups or len(inst) < cfg.min_institutional_groups:
        why.append(f"{len(agree)} independent groups agree ({len(inst)} from the book); "
                   f"{cfg.min_groups} ({cfg.min_institutional_groups}) needed")
    if against:
        why.append("strong evidence against: " + ", ".join(against))
    if regime.direction and direction and direction == -regime.direction and regime.regime in (
            ABSORPTION, REVERSAL, BREAKOUT, TRENDING):
        why.append(f"against the {regime.regime.lower()} ({'up' if regime.direction > 0 else 'down'})")
    if news is not None:
        if news.seconds_to_next is not None and news.seconds_to_next <= cfg.no_entry_before_release_s:
            why.append(f"{news.event} due in {news.seconds_to_next:.0f} s: no new trade into a release")
        if news.seconds_since_release is not None and news.seconds_since_release < cfg.no_entry_after_release_s:
            why.append(f"{news.event} {news.seconds_since_release:.0f} s ago: never chase the first print")
    if exec_quality < 0.5:
        why.append("spot execution too poor: " + (exec_note or "wide spread or old quote"))
    opp.tradeable = not why
    opp.why_not = "; ".join(why)
    opp.reasoning = reasoning(opp, fs, chart, news, history_note)
    return opp


# ---------------------------------------------------------------- English --

def _phrase(comp: Component, d: int, fs: FlowSnapshot, cur: str, spot: str, news: Optional[NewsContext]) -> str:
    up = d > 0
    n = comp.name
    if n == "aggressive":
        who = "buyers" if up else "sellers"
        what = f"{cur} offers" if up else f"{cur} bids"
        verb = "taking" if up else "hitting"
        if fs.large_net * d >= 0.2:
            return f"large {who} are repeatedly {verb} the available {what}"
        share = (fs.buy_vol_medium if up else fs.sell_vol_medium) / max(fs.buy_vol_medium + fs.sell_vol_medium, 1e-9)
        return f"{who} are {verb} the {what} ({share:.0%} of the last 30 seconds' aggressive volume)"
    if n == "order_flow":
        return "the order flow at the top of the book favours " + ("buyers" if up else "sellers")
    if n == "delta_footprint":
        if (fs.stacked_buy and up) or (fs.stacked_sell and not up):
            return f"the footprint shows stacked {'buying' if up else 'selling'} through several prices"
        return f"the cumulative delta is {'rising' if up else 'falling'}"
    if n == "book_pressure":
        s = f"the book leans to the {'bid' if up else 'offer'} (pressure {comp.value:+.2f})"
        if fs.imbalance_persist >= 0.6 and fs.imbalance_avg * d > 0:
            s += f" and has for {fs.imbalance_persist:.0%} of the last 30 seconds"
        return s
    if n == "liquidity_ahead":
        return f"there is little resting liquidity {'above' if up else 'below'} the price"
    if n == "consumption":
        bits = []
        if fs.sweep_dir == d and fs.sweep_age_s is not None and fs.sweep_age_s <= 20:
            bits.append(f"a {'buyer' if up else 'seller'} swept {fs.sweep_levels} price levels {fs.sweep_age_s:.0f} seconds ago")
        if fs.pulling_signal * d >= 0.3:
            bits.append(f"{'offers above' if up else 'bids below'} the price are being pulled")
        for e in fs.wall_events:
            if e["status"] == "CONSUMED" and (e["side"] == "ASK") == up:
                bits.append(f"the large {'offer' if up else 'bid'} wall at {e['price']} was taken out")
                break
        if fs.consumption_signal * d >= 0.2:
            bits.append(f"{'offers' if up else 'bids'} are being eaten faster than they are replaced")
        return " and ".join(bits[:2]) or f"liquidity on the {'offer' if up else 'bid'} is being consumed"
    if n == "absorption":
        if fs.absorption_score > 0 and fs.absorption_dir == d:
            s = (f"{'sellers keep hitting the bid but the price will not fall - buyers are absorbing them' if up else 'buyers keep lifting the offer but the price will not rise - sellers are absorbing them'}")
        else:
            s = f"{'bids' if up else 'offers'} keep refilling as they are hit"
        lv = [r for r in fs.refresh_levels if (r["side"] == "BID") == up]
        if lv:
            s += f" (POSSIBLE REFRESH / ABSORPTION at {lv[0]['price']})"
        return s
    if n == "profile_vwap":
        return f"price is accepted {'above' if up else 'below'} the session VWAP and value"
    if n == "chart":
        return comp.note.split(",")[0]
    if n == "news" and news is not None:
        return f"{news.event} surprised in the {cur}'s {'favour' if up else 'disfavour'}" if cur != "USD" else news.event
    if n == "cross_market":
        return "the other dollar futures are moving the same way (a broad dollar move)"
    return n


def reasoning(opp: Opportunity, fs: FlowSnapshot, chart: Optional[ChartContext] = None,
              news: Optional[NewsContext] = None, history_note: str = "") -> str:
    c = contract(opp.root)
    if not fs.ok:
        return f"No reading: {fs.reason}."
    if opp.direction == 0 or c is None:
        return "Neither side has a meaningful advantage in the book or the tape."
    d = opp.direction
    lead = sorted((x for x in opp.components if x.value * d > 0.1), key=lambda x: -(x.weight * x.value * d))
    parts = []
    for comp in lead[:3]:
        p = _phrase(comp, d, fs, c.currency, opp.spot_symbol, news)
        p = (p or "").strip()
        if p and p not in parts:
            parts.append(p)
    if not parts:
        parts = ["the evidence leans only slightly one way"]
    if len(parts) == 1:
        body = parts[0]
    elif len(parts) == 2:
        body = f"{parts[0]} and {parts[1]}"
    else:
        body = ", ".join(parts[:-1]) + ", and " + parts[-1]
    body = body[0].upper() + body[1:]
    side = "BUY" if opp.spot_direction > 0 else "SELL"
    tail = (f" {c.name} futures ({c.root}) point to the {c.currency} {'rising' if d > 0 else 'falling'}, so "
            f"{opp.spot_symbol} {side}")
    if c.inverted:
        tail += " (the pair is quoted the other way round)"
    tail += "."
    if opp.against:
        tail += " Against it: " + ", ".join(g.lower() for g in opp.against) + "."
    if history_note:
        tail += " " + history_note
    return body + "." + tail
