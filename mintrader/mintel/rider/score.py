"""MOMENTUM_OPPORTUNITY_SCORE (0-100 per market), its direction and reason,
and the entry states WATCHING -> BUILDING -> READY -> TRIGGER.

The score is built from CATEGORIES, never from 30 voting indicators. Each
evidence family is counted in exactly one category; correlated measures in
one category are GROUPED (the strongest of several breakout kinds counts
once, the trend-context measures are averaged) so nothing is double counted:

    category               families (spec numbering)
    volatility_expansion   1 volatility regime, 10 squeeze release (grouped: the stronger)
    directional_efficiency 2 TREND_EFFICIENCY
    velocity               3 velocity (1s..3m, ATR-normalised)
    acceleration           3 acceleration and jerk
    price_structure        16 microstructure, 15 candle quality, 23 sweep/reclaim
    breakout_quality       5 range breakout, 6 first retest, 8 opening range, 9 session breakout (grouped)
    pullback_quality       7 momentum pullback
    participation          11 tick activity
    multi_timeframe        4 momentum measures, 13 EMA geometry, 14 ADX/DMI, 12 VWAP, five-minute slope
    session                time of day for this pair's currencies
    news                   20 catalyst, 21 post-news momentum
    currency_strength      18
    cross_market           19
    headroom               24
    execution_cost         26 COST_RATIO
    historical_match       learning.py (neutral until enough history)

Family 27 (anti-chop) is a GATE and a reason, not points: chop means WAIT.
Family 22 (failed breakout) feeds anti-chop here and the exits in FlowLock X.
Family 17 (order flow) does not exist on retail MT5 FX and has no category.

A TRIGGER needs the score, price actually confirming the direction (the
live price taking out the last 20 seconds' extreme with the last few
seconds moving the same way), an acceptable COST_RATIO, no chop, headroom,
no high-impact news about to land and a spread that is not blown out.
Early entry is the goal: the trigger is tick-level, never "wait for the
next one-minute close".
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .features import (Evidence, MarketView, clip, cost_ratio, fam_acceleration, fam_cost, fam_headroom,
                       fam_news, price_confirmation)

WATCHING, BUILDING, READY, TRIGGER, ENTERED = "WATCHING", "BUILDING", "READY", "TRIGGER", "ENTERED"
STATE_ORDER = {WATCHING: 0, BUILDING: 1, READY: 2, TRIGGER: 3, ENTERED: 4}

DEFAULT_WEIGHTS = {
    "volatility_expansion": 8.0,
    "directional_efficiency": 12.0,
    "velocity": 12.0,
    "acceleration": 8.0,
    "price_structure": 7.0,
    "breakout_quality": 10.0,
    "pullback_quality": 5.0,
    "participation": 6.0,
    "multi_timeframe": 8.0,
    "session": 4.0,
    "news": 3.0,
    "currency_strength": 5.0,
    "cross_market": 4.0,
    "headroom": 3.0,
    "execution_cost": 3.0,
    "historical_match": 2.0,
}

ENTRY_FAMILIES = ("squeeze_release", "first_retest", "opening_range", "session_breakout", "breakout",
                  "pullback", "sweep", "news")


def weights_from(overrides: Optional[dict]) -> dict:
    w = dict(DEFAULT_WEIGHTS)
    for k, v in (overrides or {}).items():
        if k in w:
            try:
                w[k] = max(0.0, float(v))
            except (TypeError, ValueError):
                pass
    total = sum(w.values())
    if total <= 0:
        return dict(DEFAULT_WEIGHTS)
    return {k: v * 100.0 / total for k, v in w.items()}


def session_quality(levels, base: str, quote: str) -> Evidence:
    """Time of day for this pair: the London and New York opens and their
    overlap are when FX runs; Asia for yen and Antipodean pairs."""
    active = tuple(getattr(levels, "active", ()) or ())
    mins = getattr(levels, "minutes_since_open", {}) or {}
    ccys = {base, quote}
    if "LONDON" in active and "NEWYORK" in active:
        return Evidence("session", 1.0, 0, "London and New York both open", {"active": active})
    if "LONDON" in active and mins.get("LONDON", 999) <= 120:
        return Evidence("session", 1.0, 0, "the London open", {"active": active})
    if "NEWYORK" in active and mins.get("NEWYORK", 999) <= 120:
        return Evidence("session", 1.0, 0, "the New York open", {"active": active})
    if "LONDON" in active:
        return Evidence("session", 0.8, 0, "London session", {"active": active})
    if "NEWYORK" in active:
        late = mins.get("NEWYORK", 0) > 6 * 60
        return Evidence("session", 0.35 if late else 0.7, 0, "late New York" if late else "New York session",
                        {"active": active})
    if "ASIA" in active:
        asian = bool(ccys & {"JPY", "AUD", "NZD"})
        return Evidence("session", 0.6 if asian else 0.35, 0, "Asian session", {"active": active})
    return Evidence("session", 0.15, 0, "between sessions", {"active": active})


@dataclass
class ScoreResult:
    side: int = 0
    score: float = 0.0
    categories: dict = field(default_factory=dict)
    families: dict = field(default_factory=dict)
    gates: dict = field(default_factory=dict)
    reason: str = ""
    family: str = "velocity_burst"
    confirmed: bool = False
    trigger_level: float = 0.0
    confirm_why: str = ""
    cost_ratio: float = 0.0
    cost_price: float = 0.0
    expected_move: float = 0.0
    headroom_atr: float = 0.0
    chop: float = 0.0
    warm: bool = False

    @property
    def gates_ok(self) -> bool:
        return all(self.gates.values()) if self.gates else False


def _direction(f: dict) -> int:
    vel, eff = f["velocity"], f["efficiency"]
    d = vel.direction if vel.strength >= 0.05 else 0
    if d == 0:
        for k in ("squeeze_release", "breakout", "session_breakout", "opening_range", "first_retest", "pullback", "sweep"):
            e = f.get(k)
            if e is not None and e.strength >= 0.5 and e.direction:
                d = e.direction
                break
    if d and eff.direction == -d and eff.strength >= 0.3:
        return 0
    return d


def score_market(v: MarketView, fams: dict, strength_map, learning_match, weights: dict, cfg) -> ScoreResult:
    """``learning_match(buckets) -> (strength, phrase)``; ``strength_map`` from strength.py."""
    r = ScoreResult()
    r.warm = v.ok and v.bars.n >= cfg.min_warm_m1_bars
    if not v.ok:
        r.reason = "Warming up: not enough price history yet"
        r.gates = {"warm": False}
        return r
    f = dict(fams)
    d = _direction(f)
    raw_d = d or f["velocity"].direction or f["efficiency"].direction
    use_d = d or raw_d
    f["acceleration"] = fam_acceleration(v, use_d)
    f["headroom"] = fam_headroom(v, use_d, cfg.min_headroom_atr)
    f["cost"] = fam_cost(v, use_d, cfg.max_cost_ratio)
    f["news"] = fam_news(v, use_d)
    if strength_map is not None and use_d:
        f["currency_strength"] = strength_map.pair_alignment(v.base, v.quote, use_d)
        f["cross_market"] = strength_map.cross_market(v.symbol, v.base, v.quote, use_d)
    else:
        f["currency_strength"] = Evidence("currency_strength", 0.5, 0, "", {})
        f["cross_market"] = Evidence("cross_market", 0.5, 0, "", {})
    f["session"] = session_quality(v.sessions, v.base, v.quote)
    sd = use_d
    cat = {}
    cat["volatility_expansion"] = max(f["volatility"].strength, f["squeeze_release"].toward(sd))
    cat["directional_efficiency"] = f["efficiency"].toward(sd) if f["efficiency"].direction else 0.0
    cat["velocity"] = f["velocity"].toward(sd) if f["velocity"].direction else 0.0
    cat["acceleration"] = f["acceleration"].strength if f["acceleration"].direction in (0, sd) else 0.0
    struct = 0.6 * f["microstructure"].toward(sd) + 0.4 * f["candles"].toward(sd)
    cat["price_structure"] = max(struct, f["sweep"].toward(sd))
    cat["breakout_quality"] = max(f[k].toward(sd) for k in ("breakout", "first_retest", "opening_range", "session_breakout"))
    cat["pullback_quality"] = f["pullback"].toward(sd)
    cat["participation"] = f["participation"].strength
    m5 = 0.0
    if v.bars.m5_ok and sd:
        m5 = clip((0.5 * v.bars.m5_slope / 0.3 + 0.5 * v.bars.m5_roc3 / 1.5) * sd)
    mtf = [f["momentum"].toward(sd), f["ema_geometry"].toward(sd), f["adx"].toward(sd), f["vwap"].toward(sd), m5]
    cat["multi_timeframe"] = sum(mtf) / len(mtf)
    cat["session"] = f["session"].strength
    cat["news"] = f["news"].toward(sd) if f["news"].direction else f["news"].strength
    cat["currency_strength"] = f["currency_strength"].toward(sd)
    cat["cross_market"] = f["cross_market"].strength
    cat["headroom"] = f["headroom"].strength
    cat["execution_cost"] = f["cost"].strength
    ratio, cost, expected = cost_ratio(v, sd) if sd else (float("inf"), 0.0, 0.0)
    r.cost_ratio, r.cost_price, r.expected_move = ratio, cost, expected
    r.headroom_atr = float(f["headroom"].values.get("headroom_atr", 0.0) or 0.0)
    r.chop = f["chop"].strength
    buckets = bucket_key(v, f, sd, ratio, guess_family(f, sd))
    hm, hm_phrase = learning_match(buckets) if learning_match else (0.5, "")
    f["historical_match"] = Evidence("historical_match", hm, 0, hm_phrase, {"buckets": buckets})
    cat["historical_match"] = hm
    r.categories = {k: round(clip(x), 4) for k, x in cat.items()}
    r.score = round(sum(weights.get(k, 0.0) * clip(x) for k, x in cat.items()), 1)
    r.families = f
    r.family = guess_family(f, sd)
    spread_ok = v.ticks.spread <= cfg.max_spread_atr * v.atr
    news_vals = f["news"].values
    r.gates = {
        "warm": r.warm,
        "direction": d != 0,
        "not_chop": r.chop < cfg.max_chop,
        "cost": ratio <= cfg.max_cost_ratio,
        "headroom": r.headroom_atr >= cfg.min_headroom_atr,
        "news": not news_vals.get("blocked", False),
        "spread": spread_ok,
        "activity": v.ticks.tick_rate60 >= cfg.min_ticks_per_minute,
    }
    if r.chop >= cfg.max_chop:
        d = 0                                   # too choppy: no direction at all ("EURUSD NONE 43")
    r.side = d
    if d:
        ok, level, why = price_confirmation(v, d, cfg.trigger_break_seconds, cfg.trigger_velocity_seconds,
                                            cfg.trigger_min_break_atr)
        r.confirmed, r.trigger_level, r.confirm_why = ok, level, why
    r.reason = reason_for(r, f, cfg)
    return r


def guess_family(f: dict, d: int) -> str:
    """The entry family: the strongest set-up pointing the trade's way."""
    best, name = 0.4, "velocity_burst"
    for k in ENTRY_FAMILIES:
        e = f.get(k)
        if e is None or not e.direction or e.direction != d:
            continue
        if k == "news" and not e.values.get("continuation"):
            continue
        if e.strength > best:
            best, name = e.strength, k
    return name


def bucket_key(v: MarketView, f: dict, d: int, ratio: float, family: str) -> dict:
    from .learning import cost_band, efficiency_band, symbol_group
    active = tuple(getattr(v.sessions, "active", ()) or ())
    sess = "OVERLAP" if ("LONDON" in active and "NEWYORK" in active) else (active[-1] if active else "OFF_HOURS")
    return {"entry_family": family, "session": sess, "symbol_group": symbol_group(v.base, v.quote),
            "vol_regime": f["volatility"].values.get("regime", "normal"),
            "efficiency_band": efficiency_band(float(f["efficiency"].values.get("efficiency", 0.0))),
            "cost_band": cost_band(ratio if ratio != float("inf") else 9.0)}


def reason_for(r: ScoreResult, f: dict, cfg) -> str:
    vel = f["velocity"].strength
    eff = f["efficiency"].strength
    if not r.gates.get("warm", False):
        return "Warming up: not enough price history yet"
    if not r.gates.get("spread", True):
        return "Spread too wide right now"
    if not r.gates.get("activity", True):
        return "Hardly any price updates - market too thin"
    if r.side == 0:
        if r.chop >= cfg.max_chop and vel >= 0.25:
            return "Moving quickly but too choppy"
        if r.chop >= cfg.max_chop:
            return "Choppy and going nowhere"
        if vel < 0.15:
            return "Quiet - nothing moving"
        return "No clear direction"
    speed = "Very fast" if vel >= 0.75 else "Fast" if vel >= 0.45 else "Steady" if vel >= 0.15 else "Slow"
    clean = " clean" if eff >= 0.6 else "" if eff >= 0.3 else " ragged"
    head = f"{speed}{clean} {'upward' if r.side > 0 else 'downward'} momentum"
    if r.chop >= cfg.max_chop:
        return "Moving quickly but too choppy" if vel >= 0.25 else "Choppy and going nowhere"
    if not r.gates.get("news", True):
        return f"{head}, but {f['news'].phrase}"
    if not r.gates.get("cost", True):
        return f"{head}, but costs would eat the move"
    if not r.gates.get("headroom", True):
        return f"{head}, but {f['headroom'].phrase}"
    extra = ""
    fam = r.family
    if fam != "velocity_burst" and fam in f:
        extra = f" - {f[fam].phrase}"
    elif f["cross_market"].strength >= 0.6:
        extra = f" - {f['cross_market'].phrase}"
    return head + extra


class EntryStates:
    """WATCHING -> BUILDING -> READY -> TRIGGER per market, with a little
    hysteresis so a score sitting on a threshold does not flicker. A fast
    move may go straight from WATCHING to TRIGGER: early entry is the goal.
    Every NEW arrival in TRIGGER gets a fresh trigger id; an entry spends
    one id, and a re-entry needs a fresh one (momentum comes in waves)."""

    WHIPSAW_SECONDS = 90.0

    def __init__(self, cfg):
        self.cfg = cfg
        self.state: dict[str, str] = {}
        self.direction: dict[str, int] = {}
        self.trigger_id: dict[str, int] = {}
        self.since: dict[str, float] = {}
        self.last_trigger: dict[str, tuple[float, int]] = {}     # symbol -> (time, direction)
        self.note: dict[str, str] = {}

    def update(self, symbol: str, r: ScoreResult, now: float) -> tuple[str, int]:
        c = self.cfg
        prev = self.state.get(symbol, WATCHING)
        if r.side != self.direction.get(symbol, 0):
            prev = WATCHING
        h = c.state_hysteresis
        build = c.building_score - (h if STATE_ORDER[prev] >= 1 else 0.0)
        ready = c.ready_score - (h if STATE_ORDER[prev] >= 2 else 0.0)
        gates = r.gates
        ready_gates = all(gates.get(k, False) for k in ("warm", "direction", "not_chop", "cost", "headroom",
                                                        "news", "spread", "activity"))
        if r.side == 0 or not gates.get("warm", False) or r.score < build:
            new = WATCHING
        elif r.score < ready or not ready_gates:
            new = BUILDING
        elif r.score >= c.trigger_score and r.confirmed:
            new = TRIGGER
        else:
            new = READY
        self.note.pop(symbol, None)
        if new == TRIGGER:
            lt = self.last_trigger.get(symbol)
            if lt is not None and lt[1] == -r.side and now - lt[0] < self.WHIPSAW_SECONDS:
                # it triggered the OTHER way moments ago: that is a whipsaw, not a run
                new = READY
                self.note[symbol] = "Whipsawing - it was running the other way a moment ago; waiting"
            else:
                self.last_trigger[symbol] = (now, r.side)
        if new == TRIGGER and (prev != TRIGGER or self.state.get(symbol) != TRIGGER):
            self.trigger_id[symbol] = self.trigger_id.get(symbol, 0) + 1
        if new != self.state.get(symbol):
            self.since[symbol] = now
        self.state[symbol] = new
        self.direction[symbol] = r.side
        return new, self.trigger_id.get(symbol, 0)
