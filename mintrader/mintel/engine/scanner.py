"""Universe discovery and the market scan.

Two responsibilities:

1. **Discovery.**  Ask the broker what it offers, classify it, and admit
   everything in the enabled groups whose contract specification we fully
   understand.  There is no whitelist of blessed symbols and no requirement
   that an instrument "earn" the right to be traded - a market with no history
   competes immediately, just with less confidence.

2. **Scanning.**  For every admitted instrument, build one
   :class:`~mintel.engine.evidence.SymbolContext`, evaluate both directions
   through the regime-appropriate tactics, and emit ranked
   :class:`~mintel.engine.evidence.MarketState` objects so every market
   competes for the same capital on the same scale.
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass, field
from typing import Callable, Optional, Sequence

from ..broker.base import Bar, Broker, Position, Side, TF, Tick
from ..clock import is_weekend_gap, to_utc, utcnow
from ..config import Config
from ..contracts import SymbolSpec, classify_fx, infer_group
from ..data import crossmarket, headroom as hr, momentum as mom, regime as rg
from ..data import sessions as sess, strength as stg, structure as struct
from ..data.series import MultiTFData, atr, percentile_rank, session_vwap
from ..news.engine import NewsIntelligenceEngine, NewsVerdict, Wave
from .evidence import (Evidence, ExecutionQuality, Family, MarketState,
                       SymbolContext, aggregate)
from .opportunity import Calibrator, ScoreBreakdown, score as score_opportunity, tier_for
from .tactics import best_signal

log = logging.getLogger("mintel.scanner")

# Timeframe roles.  Fast frames time entries; slow frames supply context.
CONTEXT_TFS: tuple[TF, ...] = (TF.H4, TF.H1, TF.D1)
STRUCTURE_TFS: tuple[TF, ...] = (TF.M15, TF.M30)
FAST_TFS: tuple[TF, ...] = (TF.M1, TF.M5)
# Sub-minute frames are for entry TIMING only, and only where the broker can
# actually supply the ticks to build them; they are fetched last and never
# required.
TIMING_TFS: tuple[TF, ...] = (TF.S10, TF.S30)
ALL_TFS: tuple[TF, ...] = FAST_TFS + STRUCTURE_TFS + CONTEXT_TFS

# How far a winner is expected to be able to travel, per regime, in ATR.
# This sets the reward side of the pre-trade reward:risk test; FlowLock is what
# actually decides where a winner is finally closed.
OBJECTIVE_ATR: dict = {
    rg.Regime.TREND: 2.6, rg.Regime.HIGH_VOL: 2.4, rg.Regime.SQUEEZE: 2.2,
    rg.Regime.NEWS: 2.2, rg.Regime.UNCLEAR: 1.8, rg.Regime.RANGE: 1.3,
    rg.Regime.LOW_VOL: 1.2,
}

# How the multi-timeframe view is weighted: higher frames carry the context,
# lower frames the timing.  They are deliberately NOT required to agree.
MTF_WEIGHTS: dict = {
    TF.D1: 0.18, TF.H4: 0.22, TF.H1: 0.24, TF.M30: 0.14, TF.M15: 0.12,
    TF.M5: 0.10,
}

# Widest stop, in decision-timeframe ATR, that can still be called a stop.
MAX_STOP_ATR = 3.0

# Regimes where a level ahead genuinely caps the move rather than being the
# thing the trade intends to break.
MEAN_REVERTING = (rg.Regime.RANGE, rg.Regime.LOW_VOL)

BARS_NEEDED = {
    TF.S10: 120, TF.S30: 120, TF.M1: 400, TF.M5: 400, TF.M15: 400,
    TF.M30: 300, TF.H1: 300, TF.H4: 200, TF.D1: 120,
}


@dataclass
class Universe:
    symbols: tuple[str, ...]
    specs: dict[str, SymbolSpec]
    rejected: dict[str, str]
    groups: dict[str, str]

    def fx_pairs(self) -> tuple[str, ...]:
        return tuple(s for s in self.symbols
                     if self.groups.get(s, "").startswith("FX"))


def discover(broker: Broker, cfg: Config) -> Universe:
    """Build the tradable universe from the broker's own symbol list.

    An instrument is rejected - loudly, with a reason - when any field needed
    for the money math is missing.  We would rather trade 30 instruments we can
    size correctly than 300 we cannot.
    """
    accepted: list[str] = []
    specs: dict[str, SymbolSpec] = {}
    rejected: dict[str, str] = {}
    groups: dict[str, str] = {}
    wanted = set(g.upper() for g in cfg.universe.groups)

    for name in broker.symbols():
        upper = name.upper()
        if any(p.upper() in upper for p in cfg.universe.exclude_patterns):
            rejected[name] = "excluded by pattern"
            continue
        group = infer_group(name)
        if group not in wanted:
            rejected[name] = f"group {group} not enabled"
            continue
        if not broker.ensure_selected(name):
            rejected[name] = "cannot be selected in Market Watch"
            continue
        spec = broker.spec(name)
        if spec is None:
            rejected[name] = "no contract specification"
            continue
        spec = SymbolSpec(**{**spec.to_dict(), "asset_group": group,
                             "filling_modes": tuple(spec.filling_modes)})
        missing = spec.missing_fields()
        if cfg.universe.require_full_spec and missing:
            rejected[name] = f"incomplete specification: {', '.join(missing)}"
            continue
        if not spec.trade_allowed:
            rejected[name] = "trading disabled for this symbol"
            continue
        accepted.append(name)
        specs[name] = spec
        groups[name] = group

    # Prefer the most liquid instruments when the broker offers more than the
    # configured budget: tighter typical spread first, then FX majors.
    def rank(sym: str) -> tuple:
        sp = specs[sym]
        grp_rank = {"FX_MAJOR": 0, "GOLD": 1, "INDEX": 2, "FX_MINOR": 3,
                    "SILVER": 4, "ENERGY": 5}.get(groups[sym], 9)
        typical = sp.typical_spread_points * sp.point / max(sp.pip_size, 1e-12)
        return (grp_rank, typical, sym)

    accepted.sort(key=rank)
    if len(accepted) > cfg.universe.max_symbols:
        for extra in accepted[cfg.universe.max_symbols:]:
            rejected[extra] = "beyond max_symbols budget"
        accepted = accepted[:cfg.universe.max_symbols]
    return Universe(tuple(accepted), specs, rejected, groups)


class Scanner:
    def __init__(self, broker: Broker, cfg: Config,
                 news: Optional[NewsIntelligenceEngine] = None,
                 calibrator: Optional[Calibrator] = None,
                 history_match=None):
        self.broker = broker
        self.cfg = cfg
        self.news = news
        self.calibrator = calibrator or Calibrator()
        self.history_match = history_match     # callable(MarketState-ish) -> (score, conf, note)
        self.universe: Optional[Universe] = None
        # Round-trip commission per lot in account currency, learned from the
        # broker's deals (see Trader.refresh_commissions) or set in config.
        self.commission_per_lot: dict[str, float] = {}
        self.commission_default: float = float(getattr(cfg.risk, "commission_per_lot", 0.0) or 0.0)
        # callable(symbol) -> number of losing trades on it today (set by Trader)
        self.losses_today: Optional[Callable[[str], int]] = None
        self.last_scan_utc: Optional[dt.datetime] = None
        self.last_error = ""
        self._bar_cache: dict[tuple[str, TF], tuple[dt.datetime, list[Bar]]] = {}

    # ------------------------------------------------------------- universe --
    def refresh_universe(self) -> Universe:
        self.universe = discover(self.broker, self.cfg)
        log.info("universe: %d instruments (%d rejected)",
                 len(self.universe.symbols), len(self.universe.rejected))
        return self.universe

    def ensure_universe(self) -> Universe:
        return self.universe or self.refresh_universe()

    # ----------------------------------------------------------------- data --
    def fetch_frames(self, symbol: str) -> MultiTFData:
        frames: dict[TF, list[Bar]] = {}
        for tf in ALL_TFS:
            try:
                bars = self.broker.bars(symbol, tf, BARS_NEEDED[tf])
            except Exception as exc:
                self.last_error = f"{symbol} {tf.value}: {exc}"
                bars = []
            if bars:
                frames[tf] = bars
        return MultiTFData(symbol=symbol, frames=frames, as_of=utcnow())

    def decision_tf(self, data: MultiTFData) -> TF:
        """Pick the finest frame with enough history to analyse reliably."""
        for tf in (TF.M5, TF.M15, TF.M1, TF.M30, TF.H1):
            if data.has(tf, 120):
                return tf
        return next((tf for tf in ALL_TFS if data.has(tf, 60)), TF.M5)

    # ------------------------------------------------------------ execution --
    def execution_quality(self, symbol: str, spec: SymbolSpec,
                          tick: Optional[Tick], now: dt.datetime,
                          bars: Sequence[Bar]) -> ExecutionQuality:
        """Measure what it costs to trade right now, and whether we even can."""
        pip = max(spec.pip_size, 1e-12)
        typical_price = max(spec.typical_spread_points * spec.point, spec.point)
        if tick is None:
            return ExecutionQuality(0, 0, typical_price / pip, 99.0, 1.0, 1e9,
                                    spec.min_stop_distance_price() / pip, 0,
                                    False, "no tick available")
        age = max(0.0, (to_utc(now) - to_utc(tick.time)).total_seconds())
        limit = (self.cfg.scan.max_tick_age_seconds_weekend
                 if is_weekend_gap(now) else self.cfg.scan.max_tick_age_seconds)
        spread = max(tick.spread, 0.0)
        ratio = spread / typical_price if typical_price > 0 else 99.0
        hist = [b.spread_points * spec.point for b in bars[-200:]
                if b.spread_points > 0]
        pct = percentile_rank(spread, hist) if hist else 0.5
        # Slippage expectation grows with the spread ratio: when the book is
        # thin the fill is worse, and that has to be priced before entry.
        slip = (0.25 + 0.5 * max(0.0, ratio - 1.0)) * spread / pip
        commission_pips = self.commission_pips(symbol, spec)

        tradable, reason = True, ""
        if age > limit:
            tradable, reason = False, (
                f"price data is {age:.0f}s old (limit {limit:.0f}s)")
        elif spread <= 0:
            tradable, reason = False, "no spread quoted"
        elif ratio > self.cfg.risk.max_spread_multiple:
            tradable, reason = False, (
                f"spread is {ratio:.1f}x its normal size")
        return ExecutionQuality(
            spread_price=spread, spread_pips=spread / pip,
            typical_spread_pips=typical_price / pip, spread_ratio=round(ratio, 3),
            spread_percentile=round(pct, 3), tick_age_seconds=round(age, 2),
            min_stop_distance_pips=spec.min_stop_distance_price(spread) / pip,
            expected_slippage_pips=round(slip, 3), tradable=tradable,
            reason=reason, commission_pips=round(commission_pips, 3))

    def commission_money_per_lot(self, symbol: str) -> float:
        configured = float(getattr(self.cfg.risk, "commission_per_lot", 0.0) or 0.0)
        if configured > 0:
            return configured
        return float(self.commission_per_lot.get(symbol)
                     or self.commission_per_lot.get("*") or 0.0)

    def commission_pips(self, symbol: str, spec: SymbolSpec) -> float:
        """Round-trip commission for this symbol, in pips of its price."""
        money = self.commission_money_per_lot(symbol)
        if money <= 0:
            return 0.0
        per_pip = spec.money_per_lot(max(spec.pip_size, 1e-12))
        return money / per_pip if per_pip > 0 else 0.0

    # -------------------------------------------------------------- context --
    def build_context(self, symbol: str, now: dt.datetime,
                      strength: Optional[stg.StrengthReading] = None,
                      peer_bars: Optional[dict[str, list[Bar]]] = None,
                      positions: Sequence[Position] = ()) -> Optional[SymbolContext]:
        uni = self.ensure_universe()
        spec = uni.specs.get(symbol)
        if spec is None:
            return None
        data = self.fetch_frames(symbol)
        if not data.frames:
            return None
        dtf = self.decision_tf(data)
        if not data.has(dtf, self.cfg.universe.min_history_bars // 2):
            return None
        try:
            tick = self.broker.tick(symbol)
        except Exception:
            tick = None

        ctx = SymbolContext(symbol=symbol, spec=spec, as_of=to_utc(now),
                            tick=tick, data=data, decision_tf=dtf)

        # Structure is built on every frame the multi-timeframe view weighs
        # plus the decision frame: that is what makes the analysis both
        # top-down (daily context first) and bottom-up (entry timing last).
        # Momentum is measured only where it is actually consulted - computing
        # it on nine frames per symbol per scan was pure waste.
        structure_tfs = [tf for tf in MTF_WEIGHTS if data.has(tf, 60)]
        if dtf not in structure_tfs:
            structure_tfs.append(dtf)
        for tf in structure_tfs:
            bars = data.get(tf, 60)
            if not bars:
                continue
            st = struct.analyse(bars, tf_name=tf.value)
            if st:
                ctx.structure[tf] = st
        for tf in (dtf, TF.M1, TF.M15):
            if tf in ctx.momentum or not data.has(tf, 60):
                continue
            mo = mom.measure(data.get(tf, 60))
            if mo:
                ctx.momentum[tf] = mo

        # Volatility reference for stops, targets and every ATR-normalised
        # measure.  Deliberately the MAX of a fast and a slow ATR: a brief
        # volatility collapse would otherwise shrink the reference so far that
        # every ordinary structural stop looks five ATR wide, and the engine
        # would refuse trades for a purely arithmetic reason.
        dbars = data.get(dtf, 30)
        ctx.atr_ref = max(atr(dbars, 14), atr(dbars, 50)) if dbars else 0.0
        d1 = data.get(TF.D1, 20)
        ctx.daily_atr = atr(d1, 14) if d1 else 0.0

        m15 = data.get(TF.M15, 60)
        m1 = data.get(TF.M1, 60)
        if m15:
            ctx.session = sess.read(m15, m1, now)
            if ctx.session and "ASIA" in ctx.session.ranges:
                anchor = min(r.start for r in ctx.session.ranges.values())
                ctx.vwap = session_vwap(m1 or m15, anchor)
        # Exhaustion reference: how much of a normal day is already spent.
        if ctx.daily_atr > 0 and m15:
            today = [b for b in m15 if b.time.date() == to_utc(now).date()]
            ctx.range_consumed = mom.range_consumed(today or m15[-96:],
                                                    ctx.daily_atr)

        ctx.strength = strength
        ctx.execution = self.execution_quality(symbol, spec, tick, now,
                                               data.get(dtf, 200))
        ctx.open_position = next((p for p in positions if p.symbol == symbol),
                                 None)

        if peer_bars:
            for side in (1, -1):
                ctx.cross[side] = crossmarket.analyse(
                    symbol, side, data.get(dtf, 200), peer_bars)

        # Regime first - it decides which tactics are even eligible.
        in_news = False
        importance = 0
        if self.news:
            base_dir = self._breadth_direction(ctx, strength)
            bar_secs = dtf.seconds
            ctx.news = self.news.verdict(symbol, data.get(dtf, 60), tick, spec,
                                         now, bar_secs, base_dir)
            in_news = ctx.news.wave in (Wave.FIRST_SPIKE, Wave.FIRST_WAVE,
                                        Wave.SECOND_WAVE)
            importance = ctx.news.event.importance if ctx.news.event else 0
        ctx.regime = rg.classify(ctx.st(dtf), ctx.mo(dtf),
                                 in_news_window=in_news,
                                 news_importance=importance)
        return ctx

    def _breadth_direction(self, ctx: SymbolContext,
                           strength: Optional[stg.StrengthReading]) -> Optional[int]:
        if strength is None:
            return None
        base, quote, _ = classify_fx(ctx.symbol)
        if not base or not quote:
            return None
        div = strength.divergence(base, quote)
        if abs(div) < 10:
            return None
        return 1 if div > 0 else -1

    # ------------------------------------------------------------- evidence --
    def evidence_for(self, ctx: SymbolContext, side: int) -> dict[Family, Evidence]:
        """One score per family.  Related measurements share a family by design."""
        ev: dict[Family, Evidence] = {}
        dtf = ctx.decision_tf
        st, mo = ctx.st(dtf), ctx.mo(dtf)

        if st:
            ev[Family.STRUCTURE] = Evidence(
                Family.STRUCTURE, st.score_0_100(side), 1.0,
                f"{st.trend.value.lower()} structure on {dtf.value}")
        if mo:
            aligned = mo.score_0_100() if mo.direction == side else \
                max(0.0, 100.0 - mo.score_0_100())
            ev[Family.MOMENTUM] = Evidence(
                Family.MOMENTUM, aligned, 1.0,
                f"efficiency {mo.efficiency:.2f}, ADX {mo.adx:.0f}")
            # VOLATILITY and PARTICIPATION are separate *information*, not
            # restatements of momentum: one says whether the market is moving
            # enough to pay for costs, the other whether anyone is there.
            vol_score = 50.0 + 40.0 * (mo.vol_percentile - 0.5) * 2 * 0.5
            if mo.vol_expansion > 1.2:
                vol_score += 10
            ev[Family.VOLATILITY] = Evidence(
                Family.VOLATILITY, max(0.0, min(100.0, vol_score)), 1.0,
                f"volatility at the {mo.vol_percentile * 100:.0f}th percentile")
            part = 50.0 + 30.0 * min(1.0, max(-1.0, mo.tick_participation - 1.0))
            part += 15.0 * min(1.0, max(-1.0, mo.tick_acceleration))
            ev[Family.PARTICIPATION] = Evidence(
                Family.PARTICIPATION, max(0.0, min(100.0, part)), 0.7,
                f"{mo.tick_participation:.1f}x median tick volume")

        # --- multi-timeframe: top-down context vs bottom-up timing -----------
        mtf_scores, mtf_weights = [], []
        for tf, w in MTF_WEIGHTS.items():
            s = ctx.st(tf)
            if s is None:
                continue
            mtf_scores.append(s.score_0_100(side))
            mtf_weights.append(w)
        if mtf_scores:
            total = sum(mtf_weights)
            mtf = sum(s * w for s, w in zip(mtf_scores, mtf_weights)) / total
            ev[Family.MULTI_TF] = Evidence(
                Family.MULTI_TF, mtf, min(1.0, len(mtf_scores) / 4.0),
                f"{len(mtf_scores)} timeframes weighed, higher ones first")

        if ctx.session:
            liq = ctx.session.liquidity_score()
            s = 40.0 + 0.45 * liq
            act = ",".join(ctx.session.active) or "quiet hours"
            ev[Family.SESSION] = Evidence(Family.SESSION,
                                          max(0.0, min(100.0, s)), 0.8,
                                          f"session: {act}")

        cm = ctx.cross.get(side)
        if cm is not None:
            ev[Family.CROSS_MARKET] = Evidence(
                Family.CROSS_MARKET, cm.score_0_100(),
                min(1.0, cm.breadth() / 3.0),
                f"{cm.breadth()} related markets with a usable relationship")

        # FX breadth folds into CROSS_MARKET rather than forming its own family,
        # because currency strength and cross-pair agreement measure the same
        # underlying thing and would otherwise be counted twice.
        if ctx.strength is not None:
            base, quote, _ = classify_fx(ctx.symbol)
            if base and quote:
                div = ctx.strength.divergence(base, quote) * side
                s = max(0.0, min(100.0, 50.0 + div * 0.8))
                conf = min(ctx.strength.confidence(base),
                           ctx.strength.confidence(quote))
                cur = ev.get(Family.CROSS_MARKET)
                note = (f"{base} {ctx.strength.score(base):.0f} vs "
                        f"{quote} {ctx.strength.score(quote):.0f}")
                if cur is None:
                    ev[Family.CROSS_MARKET] = Evidence(
                        Family.CROSS_MARKET, s, conf, note)
                else:
                    merged = (cur.effective * 0.5 + s * 0.5)
                    ev[Family.CROSS_MARKET] = Evidence(
                        Family.CROSS_MARKET, merged,
                        max(cur.confidence, conf),
                        f"{cur.note}; {note}")

        if ctx.news is not None:
            ev[Family.NEWS] = Evidence(
                Family.NEWS, ctx.news.score_for(side),
                0.9 if ctx.news.event else 0.4,
                ctx.news.reason[:160])

        if ctx.execution is not None:
            ev[Family.EXECUTION] = Evidence(
                Family.EXECUTION, ctx.execution.score_0_100(), 1.0,
                f"spread {ctx.execution.spread_pips:.1f} pips "
                f"({ctx.execution.spread_ratio:.1f}x normal)"
                + (f", commission {ctx.execution.commission_pips:.1f} pips"
                   if ctx.execution.commission_pips > 0 else ""))

        if ctx.regime is not None:
            # Regime evidence rewards a *clearly characterised* market, whatever
            # its character - conviction about the environment is itself useful.
            s = 40.0 + 45.0 * ctx.regime.confidence
            ev[Family.REGIME] = Evidence(Family.REGIME,
                                         max(0.0, min(100.0, s)), 1.0,
                                         ctx.regime.label)
        return ev

    # ------------------------------------------------------- entry timing --
    def timing_confirmation(self, symbol: str, side: int) -> tuple[bool, str]:
        """Sub-minute check, applied only to the candidate about to be traded.

        Fetched lazily for one instrument rather than for the whole universe:
        synthesising 5/10/30-second bars from ticks is expensive, and it is
        only worth doing at the moment of entry.  Its job is narrow - refuse to
        buy into an active sub-minute flush, and refuse to sell into an active
        sub-minute rip - and it is never allowed to veto on absent data,
        because many brokers do not serve enough tick history to build it.
        """
        for tf in TIMING_TFS:
            try:
                bars = self.broker.bars(symbol, tf, 40)
            except Exception:
                continue
            if len(bars) < 12:
                continue
            recent = bars[-6:]
            net = recent[-1].close - recent[0].open
            path = sum(abs(recent[i].close - recent[i - 1].close)
                       for i in range(1, len(recent)))
            if path <= 0:
                continue
            efficiency = abs(net) / path
            against = (net * side) < 0
            # Only an efficient move in the wrong direction counts: noise
            # against us is normal and waiting for it would mean never
            # entering.
            if against and efficiency >= 0.55:
                return False, (f"price is moving against the trade on the "
                               f"{tf.value} view right now - waiting for it "
                               f"to stop")
            return True, f"{tf.value} timing agrees"
        return True, "no sub-minute data on this feed; timing not checked"

    # ---------------------------------------------------------------- scan --
    def scan(self, now: Optional[dt.datetime] = None,
             positions: Sequence[Position] = ()) -> list[MarketState]:
        """Score every instrument in both directions and rank the results."""
        now = to_utc(now or utcnow())
        uni = self.ensure_universe()
        if not uni.symbols:
            return []

        # Peer bars are fetched once and shared: currency strength and
        # cross-market correlation both need the whole cross-section.
        peer_bars: dict[str, list[Bar]] = {}
        for sym in uni.symbols:
            try:
                bars = self.broker.bars(sym, TF.M15, 300)
            except Exception:
                bars = []
            if bars:
                peer_bars[sym] = bars
        strength = stg.compute({s: b for s, b in peer_bars.items()
                                if uni.groups.get(s, "").startswith("FX")})

        states: list[MarketState] = []
        for sym in uni.symbols:
            try:
                ctx = self.build_context(sym, now, strength, peer_bars, positions)
            except Exception as exc:
                log.warning("context failed for %s: %s", sym, exc)
                continue
            if ctx is None:
                continue
            for side in (1, -1):
                ms = self.state_for(ctx, side)
                if ms is not None:
                    states.append(ms)

        states.sort(key=lambda m: -m.opportunity)
        self.last_scan_utc = now
        return states

    def state_for(self, ctx: SymbolContext, side: int) -> Optional[MarketState]:
        """Build the MarketState for one instrument and one direction."""
        regime = ctx.regime.regime if ctx.regime else rg.Regime.UNCLEAR
        sig = best_signal(ctx, side, regime,
                          disabled=getattr(self.cfg.scan, "disabled_tactics", ()) or (),
                          disabled_in=getattr(self.cfg.scan, "disabled_tactic_regimes", ()) or ())
        if sig is None:
            return None
        ev = self.evidence_for(ctx, side)

        # Historical experience adds or removes confidence but never permission.
        if self.history_match is not None:
            try:
                h_score, h_conf, h_note = self.history_match(
                    ctx.symbol, regime.value, sig.name, side, ctx)
                ev[Family.HISTORICAL] = Evidence(Family.HISTORICAL, h_score,
                                                 h_conf, h_note)
            except Exception:
                pass

        raw = aggregate(ev, regime)
        entry = ctx.entry_price(Side.BUY if side > 0 else Side.SELL)
        spec = ctx.spec
        pip = max(spec.pip_size, 1e-12)

        # --- stop from structural invalidation plus a volatility allowance ---
        # The stop is derived from where the IDEA is wrong, never from a fixed
        # per-instrument distance, then padded for volatility and spread.
        spread = ctx.execution.spread_price if ctx.execution else 0.0
        allowance = 0.35 * ctx.atr_ref + spread
        stop = sig.invalidation - side * allowance
        min_dist = spec.min_stop_distance_price(spread, 1.5)
        if abs(entry - stop) < min_dist:
            stop = entry - side * min_dist
        # Noise floor: a stop inside half an ATR is a stop inside the normal
        # wobble of the market, and day one proved what happens to those.
        noise_floor = self.cfg.scan.stop_noise_floor_atr * max(ctx.atr_ref, 0.0)
        if noise_floor > 0 and abs(entry - stop) < noise_floor:
            stop = entry - side * noise_floor
        stop = spec.normalise_price(stop)

        # If price has run so far past its own invalidation level that the stop
        # would have to be absurdly wide, the entry has been MISSED.  Widening
        # the stop to fit would quietly turn a good idea into a bad bet, so the
        # setup is reported as chased instead.  Re-entry stays available the
        # moment a fresh, tighter setup forms.
        stop_atr = abs(entry - stop) / max(ctx.atr_ref, 1e-12)
        chased = stop_atr > MAX_STOP_ATR and abs(entry - stop) > min_dist * 1.5

        # --- headroom & target ------------------------------------------------
        levels = list(ctx.st(ctx.decision_tf).levels) if ctx.st(ctx.decision_tf) else []
        named = ctx.session.levels() if ctx.session else []
        head = hr.measure(entry, side, ctx.atr_ref or 1e-9, spec, levels, named,
                          ctx.vwap)
        # Profit objective scaled to the regime: a trend is given room to pay,
        # a range is not.  A tactic with its own natural target (the far side of
        # a range, session VWAP) overrides the generic objective.
        objective_atr = OBJECTIVE_ATR.get(regime, 2.0)
        target = sig.target_hint or head.realistic_target(objective_atr)
        risk = abs(entry - stop)
        cost_pips = ctx.execution.cost_pips if ctx.execution else 0.0

        # Reward:risk is tested against the excursion the REGIME supports, not
        # against the first level overhead.  In a trend that level is the thing
        # the trade intends to break, and treating it as a ceiling would veto
        # every continuation trade ever taken.  A level that really is a wall
        # already costs the setup through `headroom_multiplier`, so charging it
        # twice - once in the score, once as a hard veto - would be double
        # counting.  In mean-reverting regimes the level DOES tend to hold, so
        # there it is allowed to cap the expectation.
        excursion = objective_atr * max(ctx.atr_ref, 1e-12)
        if regime in MEAN_REVERTING and target is not None:
            excursion = min(excursion, abs(target - entry))
        rr = None
        if risk > 0:
            rr = round((excursion - cost_pips * pip) / risk, 3)

        breakdown = score_opportunity(
            raw, sig.quality, head, ctx.mo(ctx.decision_tf), ctx.range_consumed,
            ctx.execution.score_0_100() if ctx.execution else 50.0,
            ctx.session.liquidity_score() if ctx.session else 60.0)
        tier = tier_for(breakdown.final, self.cfg.scan)

        blockers: list[str] = []
        floor = (getattr(self.cfg.scan, "tactic_min_score", None) or {}).get(sig.name)
        if floor is not None and breakdown.final < float(floor):
            blockers.append(f"{sig.name.replace('_', ' ').lower()} needs a score of "
                            f"{float(floor):.0f}+ (this is {breakdown.final:.0f})")
        from .opportunity import TIERS
        want = str(self.cfg.scan.entry_tier or "NORMAL").upper()
        if tier != "NO_TRADE" and want in TIERS and \
                TIERS.index(tier) < TIERS.index(want):
            blockers.append(f"{tier} setup - only {want} or better is traded")
        if ctx.execution and not ctx.execution.tradable:
            blockers.append(ctx.execution.reason)
        if ctx.news and ctx.news.blackout:
            blockers.append(ctx.news.reason)
        if risk <= 0:
            blockers.append("stop distance computed as zero")
        if rr is not None and rr < self.cfg.scan.min_reward_risk:
            blockers.append(f"reward:risk only {rr:.2f} after costs")
        cost_limit = self.cfg.scan.max_cost_fraction_of_stop
        if risk > 0 and cost_limit > 0 and cost_pips * pip > cost_limit * risk:
            blockers.append(
                f"costs would be {cost_pips * pip / risk:.0%} of the stop "
                f"distance (limit {cost_limit:.0%}) - too small a trade to pay for itself")
        if self.losses_today is not None:
            try:
                n_lost = int(self.losses_today(ctx.symbol))
            except Exception:
                n_lost = 0
            limit_l = self.cfg.scan.max_losses_per_symbol_per_day
            if limit_l > 0 and n_lost >= limit_l:
                blockers.append(f"{n_lost} losing trades here today - "
                                f"leaving this market alone until tomorrow")
        if chased:
            blockers.append(
                f"entry is {stop_atr:.1f} ATR from the level that invalidates "
                f"this idea - the move has been missed")
        if ctx.open_position is not None:
            same = (ctx.open_position.side is Side.BUY) == (side > 0)
            blockers.append("already long here" if same else
                            "an opposite position is open here")

        notes = list(breakdown.notes)
        if sig.note:
            notes.append(sig.note)
        if sig.requires_confirmation:
            notes.append("waiting for confirmation of the turn")

        return MarketState(
            symbol=ctx.symbol, as_of=ctx.as_of, regime=regime,
            regime_label=ctx.regime.label if ctx.regime else regime.value,
            direction=side, tactic=sig.name, evidence=ev,
            raw_score=raw, opportunity=breakdown.final, tier=tier,
            entry=entry, stop=stop, target=target, reward_risk=rr,
            headroom_pips=(head.headroom_pips if head.nearest else 999.0),
            cost_pips=round(cost_pips, 2), notes=tuple(notes),
            blockers=tuple(blockers))
