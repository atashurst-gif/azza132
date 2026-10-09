"""RiderCore - the Rapid Momentum Rider's PURE decision object.

No broker IO, no wall clock, no randomness: every decision is a function of
the ticks it has been fed, the contract specifications and calendar it has
been given, and the ``now`` passed in. The live engine (engine.py) and the
research backtester drive the SAME object, so a replay of historical ticks
reproduces the live decisions exactly.

API (the contract the research backtester relies on)
----------------------------------------------------

Set-up::

    core = RiderCore(cfg)                         # cfg: RiderConfig
    core.set_spec(symbol, spec)                   # SymbolSpec (pip, tick value, volume grid)
    core.set_account_currency("GBP")
    core.set_calendar(events)                     # [CalendarEvent] from the MT5 calendar
    core.seed_bars(symbol, TF.M1 | TF.M5, bars, now)   # optional: history before the first tick

Feeding (in time order, per symbol; older or duplicate ticks are ignored)::

    core.on_tick(symbol, tick)                    # Tick(symbol, time, bid, ask, ...)

Deciding (call once per ``cfg.scan_interval_seconds`` of MARKET time; the
entry states have memory, so a replay must use the live cadence)::

    rows = core.scan(now)                         # ranked [ScanRow]: MARKET DIRECTION SCORE STATE REASON
    intents = core.entries(now)                   # [EntryIntent]: symbol, side, stop, reason, snapshot
    pos = core.open_position(intent, fill_price, volume, ticket, fill_time)
    d = core.manage(pos, tick, now)               # FlowDecision: new stop / exit + reason
    core.stop_move_due(pos, stop, tick, now)      # is this move worth sending? (min step, min interval)
    core.stop_move_sent(pos, now)                 # it was sent
    core.confirm_stop(pos, stop, sent_at)         # once the broker (or the paper fill) holds it
    core.position_closed(pos, now)                # starts the re-entry cooldown

The live engine's order each pass, which a replay must copy: feed every new
tick (``on_tick``) -> ``manage`` each open position (a proposed stop is
snapped to the tick grid, sent only when ``stop_move_due``, then
``confirm_stop`` / close as the broker answers) -> ``scan(now)`` -> ``entries(now)`` (only when
the health checks allow new entries). A fill at or through the planned
stop makes ``open_position`` raise ValueError: close that fill at once.

Rules the core keeps on its own: one position per market, the max
concurrent cap, re-entry only on a FRESH trigger (a new arrival in TRIGGER)
and only after ``cooldown_seconds``. An intent spends its trigger at once,
so asking again before the fill (replay latency) never duplicates it.

No look-ahead: feed every tick with time <= now BEFORE calling scan(now) /
manage(..., now). Bars count as closed only once their end time has passed;
ticks after ``now`` are ignored by the views. Latency for replay is
``cfg.latency_ms``: the research fills an intent at the first tick at or
after ``intent.signal_time + latency``.
"""
from __future__ import annotations

import bisect
import datetime as dt
from dataclasses import dataclass, field
from typing import Optional

from ..broker.base import Bar, Side, TF, Tick
from ..scalper.features import TickBuffer
from .bars import BarSeries, SessionTracker
from .config import RiderConfig
from .features import (BarStats, Evidence, MarketView, compute_bar_stats, compute_families, compute_tick_stats,
                       ORDER_FLOW_AVAILABLE)
from .flowlock_x import FlowDecision, FlowInputs, FlowLockParams, FlowLockX, FlowState
from .learning import BucketStats
from .news import NewsTracker
from .pipvalue import account_per_profit_unit, fx_currencies, fx_pip_size, lots_for_gbp_per_pip
from .score import (ENTERED, TRIGGER, WATCHING, EntryStates, ScoreResult, score_market, weights_from)
from .strength import PairMove, StrengthMap, compute_strength, pair_move_z

UTC = dt.timezone.utc
TICK_SECONDS = 200.0


def _ts(x: dt.datetime) -> float:
    return x.timestamp()


@dataclass
class ScanRow:
    symbol: str
    direction: str                     # LONG / SHORT / NONE
    score: float
    state: str                         # WATCHING / BUILDING / READY / TRIGGER / ENTERED
    reason: str
    evidence: dict = field(default_factory=dict)
    side: int = 0
    family: str = ""
    trigger_id: int = 0
    confirmed: bool = False
    atr: float = 0.0
    cost_ratio: float = 0.0
    headroom_atr: float = 0.0
    chop: float = 0.0

    def to_dict(self) -> dict:
        return {"market": self.symbol, "direction": self.direction, "score": round(self.score, 1),
                "state": self.state, "reason": self.reason, "family": self.family}


@dataclass
class EntryIntent:
    symbol: str
    side: Side
    stop: float
    reason: str
    snapshot: dict
    family: str
    score: float
    trigger_id: int
    trigger_level: float
    signal_time: dt.datetime
    reference_price: float             # the touch at the signal (ask for a buy, bid for a sell)
    atr: float
    spread: float
    cost_price: float                  # commission (round trip) + exit slippage, price: FlowLock's breakeven buffer
    expected_move: float
    cost_ratio: float
    efficiency: float
    velocity: float
    stop_pips: float


@dataclass
class RiderPosition:
    ticket: int
    symbol: str
    side: Side
    volume: float
    entry: float
    opened_at: dt.datetime
    flow: FlowState
    family: str = ""
    score: float = 0.0
    reason: str = ""
    snapshot: dict = field(default_factory=dict)
    trigger_id: int = 0
    pip: float = 0.0
    gbp_per_pip: float = 0.0
    mode: str = "PAPER"
    signal_time: Optional[dt.datetime] = None
    meta: dict = field(default_factory=dict)

    @property
    def stop(self) -> float:
        return self.flow.stop

    def pips_of(self, price: float) -> float:
        return ((price - self.entry) * self.side.sign) / self.pip if self.pip else 0.0


class _Sym:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.buffer = TickBuffer(symbol, seconds=TICK_SECONDS)
        self.m1 = BarSeries(60, keep=400)
        self.m5 = BarSeries(300, keep=400)
        self.sessions = SessionTracker()
        self.last_time = -1e18
        self.last_bid = 0.0
        self.last_ask = 0.0
        self.bar_key: Optional[tuple] = None
        self.bar_stats = BarStats()
        self.spec = None
        self.pip = 0.0
        self.base = ""
        self.quote = ""
        self.ticks_seen = 0
        self._tail: list = []


class RiderCore:
    def __init__(self, cfg: Optional[RiderConfig] = None, learning: Optional[BucketStats] = None,
                 flow_params: Optional[FlowLockParams] = None):
        self.cfg = cfg or RiderConfig()
        self.learning = learning or BucketStats(self.cfg.learning_min_samples, self.cfg.learning_window_trades,
                                                self.cfg.learning_min_days)
        self.flowlock = FlowLockX(flow_params or FlowLockParams.from_dict(self.cfg.flowlock))
        self.weights = weights_from(self.cfg.weights)
        self.syms: dict[str, _Sym] = {}
        self.account_currency = "GBP"
        self.learned_commission: Optional[float] = None        # account currency per lot per side
        self.news = NewsTracker(self.cfg.news_min_importance, self.cfg.news_window_minutes,
                                self.cfg.news_settle_seconds, self.cfg.news_pre_block_seconds)
        self.states = EntryStates(self.cfg)
        self.open: dict[str, RiderPosition] = {}
        self.spent_trigger: dict[str, int] = {}
        self.last_signal: dict[str, float] = {}
        self.last_exit: dict[str, float] = {}
        self.last_scan_at: Optional[float] = None
        self.last_rows: list[ScanRow] = []
        self.last_results: dict[str, ScoreResult] = {}
        self.last_views: dict[str, MarketView] = {}
        self.strength = StrengthMap()
        self.order_flow_available = ORDER_FLOW_AVAILABLE        # family 17: none on retail MT5 FX
        self.notes: list[str] = []

    # ------------------------------------------------------------ set-up --
    def _sym(self, symbol: str) -> _Sym:
        s = self.syms.get(symbol)
        if s is None:
            s = self.syms[symbol] = _Sym(symbol)
        return s

    def set_spec(self, symbol: str, spec) -> None:
        s = self._sym(symbol)
        s.spec = spec
        s.base, s.quote = fx_currencies(spec)
        s.pip, _ = fx_pip_size(spec)

    def set_account_currency(self, ccy: str) -> None:
        self.account_currency = str(ccy or "GBP").upper()

    def set_learned_commission(self, per_lot_side_account: Optional[float]) -> None:
        if per_lot_side_account is not None and per_lot_side_account >= 0:
            self.learned_commission = float(per_lot_side_account)

    def set_calendar(self, events) -> None:
        self.news.set_events(events)

    def seed_bars(self, symbol: str, tf: TF, bars: list[Bar], now: dt.datetime) -> int:
        s = self._sym(symbol)
        series = s.m1 if tf is TF.M1 else s.m5 if tf is TF.M5 else None
        if series is None:
            return 0
        t = _ts(now)
        n = series.seed(bars, before_epoch=t)
        for b in bars:
            bt = _ts(b.time)
            if bt + tf.seconds <= t:
                s.sessions.update(bt, b.high, b.low, span=float(tf.seconds))
        return n

    @property
    def symbols(self) -> list[str]:
        return sorted(k for k, s in self.syms.items() if s.spec is not None and s.pip > 0)

    # ----------------------------------------------------------- feeding --
    def on_tick(self, symbol: str, tick: Tick) -> bool:
        s = self._sym(symbol)
        bid, ask = float(tick.bid), float(tick.ask)
        if bid <= 0 or ask <= 0 or ask < bid:
            return False
        t = _ts(tick.time)
        if t < s.last_time or (t == s.last_time and bid == s.last_bid and ask == s.last_ask):
            return False
        s.last_time, s.last_bid, s.last_ask = t, bid, ask
        s.buffer.add(tick)
        mid = (bid + ask) * 0.5
        spread = ask - bid
        s.m1.add(t, mid, spread)
        s.m5.add(t, mid, spread)
        s.sessions.update(t, mid, mid)
        if s.base:
            self.news.on_tick(symbol, (s.base, s.quote), t, mid, spread)
        s.ticks_seen += 1
        return True

    def last_tick(self, symbol: str) -> Optional[Tick]:
        s = self.syms.get(symbol)
        return s.buffer.last if s is not None else None

    # ------------------------------------------------------------- costs --
    def rate(self, frm: str, to: str) -> Optional[float]:
        """Units of ``to`` per 1 ``frm`` from the latest mids of the pairs fed."""
        frm, to = frm.upper(), to.upper()
        if frm == to:
            return 1.0
        for sym, s in self.syms.items():
            if s.last_bid <= 0:
                continue
            mid = (s.last_bid + s.last_ask) * 0.5
            if s.base == frm and s.quote == to:
                return mid
            if s.base == to and s.quote == frm and mid > 0:
                return 1.0 / mid
        return None

    def commission_account_per_lot_side(self) -> Optional[float]:
        if self.cfg.learn_commission_from_deals and self.learned_commission is not None:
            return self.learned_commission
        ccy = self.cfg.commission_currency.upper()
        if ccy == self.account_currency:
            return self.cfg.commission_per_lot_side
        for s in self.syms.values():
            if s.spec is not None and s.quote == ccy:
                per = account_per_profit_unit(s.spec)
                if per > 0:
                    return self.cfg.commission_per_lot_side * per
        r = self.rate(ccy, self.account_currency)
        return self.cfg.commission_per_lot_side * r if r else None

    def cost_prices(self, symbol: str) -> tuple[float, float]:
        """(commission round trip + slippage both sides, commission round trip
        + exit slippage) in price units for one lot - the scan's cost and
        FlowLock X's breakeven buffer. The spread is added live."""
        s = self.syms[symbol]
        spec = s.spec
        slip = self.cfg.slippage_pips * s.pip
        comm = 0.0
        per_side = self.commission_account_per_lot_side()
        if spec is not None and per_side:
            per_price = float(spec.tick_value) / float(spec.tick_size) if spec.tick_size else 0.0
            if per_price > 0:
                comm = 2.0 * per_side / per_price
        return comm + 2.0 * slip, comm + slip

    def size(self, symbol: str) -> tuple[float, float, str]:
        """Lots for Aaron's GBP per pip on this market (never changed here)."""
        s = self.syms.get(symbol)
        if s is None or s.spec is None:
            return 0.0, 0.0, f"{symbol}: no contract specification"
        return lots_for_gbp_per_pip(s.spec, self.cfg.user_pip_value_gbp, self.account_currency,
                                    lambda a, b: self.rate(a, b))

    # -------------------------------------------------------------- view --
    def view(self, symbol: str, now: float) -> Optional[MarketView]:
        s = self.syms.get(symbol)
        if s is None or s.spec is None or s.pip <= 0:
            return None
        ticks = s.buffer.ticks
        if ticks and _ts(ticks[-1].time) > now:
            ticks = [x for x in ticks if _ts(x.time) <= now]
        else:
            ticks = list(ticks)
        m1 = s.m1.closed(now)
        m5 = s.m5.closed(now)
        key = (s.m1.key(now), s.m5.key(now))
        if key != s.bar_key:
            s.bar_stats = compute_bar_stats(m1, m5, self.cfg.min_warm_m1_bars)
            s.bar_key = key
            s._tail = m1[-6:]
        bs = s.bar_stats
        ts = compute_tick_stats(ticks, now, bs.atr)
        lv = s.sessions.levels(now)
        news = self.news.context(symbol, (s.base, s.quote), now, ts.mid, ts.spread, bs.atr) if ts.ok else None
        cost, _be = self.cost_prices(symbol)
        return MarketView(symbol, now, s.pip, s.base, s.quote, bs, ts, lv, news, s.m1.forming(now), cost,
                          self.cfg.expected_run_atr, self.cfg.expected_run_atr5, self.cfg.expected_run_impulse)

    # -------------------------------------------------------------- scan --
    def scan(self, now: dt.datetime) -> list[ScanRow]:
        t = _ts(now)
        views: dict[str, MarketView] = {}
        fams: dict[str, dict] = {}
        for sym in self.symbols:
            v = self.view(sym, t)
            if v is None:
                continue
            views[sym] = v
            if v.ok:
                fams[sym] = compute_families(v, getattr(self.syms[sym], "_tail", []))
        moves = [PairMove(sym, v.base, v.quote, pair_move_z(v.ticks.velocity, v.bars.roc5))
                 for sym, v in views.items() if v.ok and sym in fams]
        self.strength = compute_strength(moves)
        match = lambda b: self.learning.match(b)[:2]    # noqa: E731
        rows: list[ScanRow] = []
        results: dict[str, ScoreResult] = {}
        for sym, v in views.items():
            if sym in fams:
                res = score_market(v, fams[sym], self.strength, match, self.weights, self.cfg)
            else:
                res = ScoreResult(reason="Warming up: not enough price history yet", gates={"warm": False})
            state, tid = self.states.update(sym, res, t)
            if self.states.note.get(sym):
                res.reason = self.states.note[sym]
            results[sym] = res
            shown = ENTERED if sym in self.open else state
            rows.append(ScanRow(sym, "LONG" if res.side > 0 else "SHORT" if res.side < 0 else "NONE", res.score, shown,
                                res.reason, {"categories": res.categories,
                                             "families": {k: {"s": round(e.strength, 2), "d": e.direction, "p": e.phrase}
                                                          for k, e in res.families.items()}},
                                res.side, res.family, tid, res.confirmed, v.atr if v.ok else 0.0,
                                res.cost_ratio if res.cost_ratio != float("inf") else 99.0, res.headroom_atr, res.chop))
        rows.sort(key=lambda r: (-r.score, r.symbol))
        self.last_scan_at = t
        self.last_rows = rows
        self.last_results = results
        self.last_views = views
        return rows

    # ----------------------------------------------------------- entries --
    def entries(self, now: dt.datetime) -> list[EntryIntent]:
        t = _ts(now)
        if self.last_scan_at != t:
            self.scan(now)
        out: list[EntryIntent] = []
        self.notes = []
        for row in self.last_rows:
            if row.state != TRIGGER:
                continue
            sym = row.symbol
            if sym in self.open:
                continue
            if self.spent_trigger.get(sym, 0) >= row.trigger_id:
                continue                                          # this wave was already taken: wait for a fresh trigger
            last = max(self.last_signal.get(sym, -1e18), self.last_exit.get(sym, -1e18))
            if t - last < self.cfg.cooldown_seconds:
                continue
            if len(self.open) + len(out) >= self.cfg.max_concurrent_positions:
                self.notes.append(f"{sym}: trigger skipped - {self.cfg.max_concurrent_positions} positions already open")
                break
            intent, why = self._intent(sym, row, now)
            if intent is None:
                self.notes.append(f"{sym}: {why}")
                continue
            self.spent_trigger[sym] = row.trigger_id
            self.last_signal[sym] = t
            out.append(intent)
        return out

    def initial_stop(self, v: MarketView, d: int, entry: float) -> tuple[Optional[float], str]:
        """The broker-side stop at entry: just beyond the last micro swing
        against the trade (the move's invalidation), else beyond the low
        (high) of the last few seconds; never closer than ``stop_min_atr``
        ATRs, ``stop_min_spreads`` spreads or the broker's stops level; when
        the structure is further than ``stop_max_atr`` ATRs (or
        ``stop_max_pips``) the stop is a volatility stop at that distance.
        Returns (stop, how it was placed) or (None, why not)."""
        c = self.cfg
        s = self.syms[v.symbol]
        tk = v.ticks
        atr = v.atr
        spread = tk.spread
        since = v.now - c.stop_swing_seconds
        swings = [p for t, p, k in tk.pivots if k == -d and t >= since and (entry - p) * d > 0]
        if swings:
            ref, how = swings[-1], "beyond the last swing"
        else:
            i0 = bisect.bisect_left(tk.times, v.now - c.trigger_break_seconds)
            seg = tk.mids[i0:] or [tk.mid]
            ref, how = (min(seg) if d > 0 else max(seg)), "beyond the recent low" if d > 0 else "beyond the recent high"
        struct = ref - d * (spread / 2.0 + c.stop_buffer_atr * atr)
        dist = (entry - struct) * d
        point = float(getattr(s.spec, "point", 0.0) or 0.0)
        broker_min = float(getattr(s.spec, "stops_level_points", 0.0) or 0.0) * point + spread
        min_dist = max(c.stop_min_atr * atr, c.stop_min_spreads * spread, broker_min)
        max_dist = min(c.stop_max_atr * atr, c.stop_max_pips * s.pip)
        if max_dist < min_dist:
            return None, (f"no sensible stop: the spread and the broker's minimum need {min_dist / s.pip:.1f} pips, "
                          f"more than the {max_dist / s.pip:.1f}-pip limit")
        if dist < min_dist:
            struct, how = entry - d * min_dist, "at the minimum distance"
        elif dist > max_dist:
            struct, how = entry - d * max_dist, "a volatility stop (the swing is further away)"
        stop = s.spec.normalise_price(struct)
        if (entry - stop) * d <= 0:
            return None, "could not place a stop on the right side"
        return stop, how

    def _intent(self, sym: str, row: ScanRow, now: dt.datetime) -> tuple[Optional[EntryIntent], str]:
        v = self.last_views.get(sym)
        res = self.last_results.get(sym)
        if v is None or res is None or not v.ok:
            return None, "no view"
        d = res.side
        side = Side.BUY if d > 0 else Side.SELL
        ref = v.ticks.ask if d > 0 else v.ticks.bid
        stop, how = self.initial_stop(v, d, ref)
        if stop is None:
            return None, how
        _cost, be = self.cost_prices(sym)
        snap = self.snapshot(sym, row, res, v, now)
        snap["stop_placed"] = how
        eff = float(res.families["efficiency"].values.get("efficiency", 0.5))
        return EntryIntent(sym, side, stop, res.reason, snap, res.family, res.score, row.trigger_id, res.trigger_level,
                           now, ref, v.atr, v.ticks.spread, be, res.expected_move, res.cost_ratio, eff,
                           v.ticks.velocity, abs(ref - stop) / v.pip), ""

    def snapshot(self, sym: str, row: ScanRow, res: ScoreResult, v: MarketView, now: dt.datetime) -> dict:
        """The full pre-entry market state, stored with every trade."""
        lv = v.sessions
        return {
            "time": now.isoformat(), "symbol": sym, "score": res.score, "state": row.state,
            "direction": row.direction, "reason": res.reason, "family": res.family, "trigger_id": row.trigger_id,
            "categories": res.categories, "gates": res.gates,
            "families": {k: e.to_dict() for k, e in res.families.items()},
            "atr": v.atr, "atr_pips": v.atr / v.pip if v.pip else 0.0, "spread_pips": v.ticks.spread / v.pip,
            "cost_ratio": res.cost_ratio if res.cost_ratio != float("inf") else 99.0,
            "expected_move_pips": res.expected_move / v.pip if v.pip else 0.0,
            "headroom_atr": res.headroom_atr, "chop": res.chop, "trigger_level": res.trigger_level,
            "confirm": res.confirm_why, "currency_strength": self.strength.to_dict(),
            "sessions": {"active": list(getattr(lv, "active", ())),
                         "ranges": {k: list(x) for k, x in (getattr(lv, "ranges", {}) or {}).items()},
                         "prev_day": list(lv.prev_day) if getattr(lv, "prev_day", None) else None},
            "order_flow_available": ORDER_FLOW_AVAILABLE,
            "buckets": res.families.get("historical_match", Evidence("x")).values.get("buckets", {}),
        }

    # ---------------------------------------------------------- positions --
    def open_position(self, intent: EntryIntent, fill_price: float, volume: float, ticket: int,
                      fill_time: dt.datetime, mode: str = "PAPER", gbp_per_pip: float = 0.0) -> RiderPosition:
        """Register a filled entry. The stop is the intent's (the broker's)."""
        s = self.syms[intent.symbol]
        d = intent.side.sign
        stop = intent.stop
        if (fill_price - stop) * d <= 0:
            # filled at or through the planned stop: the stop is NEVER moved further
            # away to fit the fill - the caller closes such a position at once
            raise ValueError(f"{intent.symbol}: filled at {fill_price} through the planned stop {stop}")
        flow = self.flowlock.start(d, fill_price, stop, intent.cost_price, _ts(fill_time), intent.trigger_level,
                                   intent.efficiency, abs(intent.velocity))
        pos = RiderPosition(int(ticket), intent.symbol, intent.side, float(volume), float(fill_price), fill_time, flow,
                            intent.family, intent.score, intent.reason, intent.snapshot, intent.trigger_id, s.pip,
                            gbp_per_pip, mode, intent.signal_time)
        self.open[intent.symbol] = pos
        self.spent_trigger[intent.symbol] = max(self.spent_trigger.get(intent.symbol, 0), intent.trigger_id)
        return pos

    def restore_position(self, pos: RiderPosition) -> None:
        """After a restart: a position the records hold, managed on. A restored
        position also spends whatever trigger is current on its market."""
        self.open[pos.symbol] = pos
        self.spent_trigger[pos.symbol] = max(self.spent_trigger.get(pos.symbol, 0),
                                             self.states.trigger_id.get(pos.symbol, 0))

    def position_closed(self, pos: RiderPosition, now: dt.datetime) -> None:
        if self.open.get(pos.symbol) is pos or (pos.symbol in self.open and self.open[pos.symbol].ticket == pos.ticket):
            self.open.pop(pos.symbol, None)
        self.last_exit[pos.symbol] = _ts(now)

    def flow_inputs(self, pos: RiderPosition, tick: Tick, now: float) -> FlowInputs:
        s = self.syms.get(pos.symbol)
        x = FlowInputs(now, float(tick.bid), float(tick.ask))
        if s is None:
            return x
        v = self.view(pos.symbol, now)
        if v is None or not v.bars.ok:
            return x
        b, t = v.bars, v.ticks
        x.atr = b.atr
        opened = _ts(pos.opened_at)
        if t.ok:
            lows = [p for tt, p, k in t.pivots if k < 0 and tt >= opened]
            highs = [p for tt, p, k in t.pivots if k > 0 and tt >= opened]
            half = (x.ask - x.bid) / 2.0
            if lows:
                x.swing_low = lows[-1] - half
            if highs:
                x.swing_high = highs[-1] + half
            x.velocity = t.velocity
            x.accel = t.accel
            x.efficiency = 0.6 * t.eff60 + 0.4 * t.eff120
            x.efficiency_dir = t.eff60_dir
            x.tick_rate_ratio = (t.tick_rate30 / b.vol_baseline) if b.vol_baseline > 0 else 1.0
            x.extension_atr = (t.mid - b.ema21) / b.atr if b.atr > 0 else 0.0
        x.adx_change = b.adx_change
        x.ema_fast = b.ema9
        x.ema_slope = b.ema9_slope
        d = pos.side.sign
        tail = list(getattr(s, "_tail", []))[-3:]
        f = v.forming
        if f is not None:
            tail.append(f)
        if tail:
            opp = 0.0
            for c in tail:
                r = c.high - c.low
                if r <= 0:
                    continue
                body_against = max(0.0, (c.open - c.close) * d) / r
                wick_against = ((c.high - max(c.open, c.close)) if d > 0 else (min(c.open, c.close) - c.low)) / r
                opp += 0.6 * body_against + 0.4 * wick_against
            x.opposing_candles = opp / len(tail)
        if s.base and self.strength.moves:
            x.cross_confirm = self.strength.confirms(pos.symbol, s.base, s.quote, d)
        mark = x.bid if d > 0 else x.ask
        if pos.flow.trigger_level and (pos.flow.trigger_level - mark) * d > 0:
            x.failed_breakout = True
        if s.spec is not None:
            x.min_stop_distance = float(getattr(s.spec, "stops_level_points", 0.0) or 0.0) * float(s.spec.point)
        return x

    def manage(self, pos: RiderPosition, tick: Tick, now: dt.datetime) -> FlowDecision:
        """FlowLock X's decision for an open position. The stop is NOT changed
        here: call confirm_stop once the broker (or the paper book) holds it."""
        return self.flowlock.update(pos.flow, self.flow_inputs(pos, tick, _ts(now)))

    def stop_move_due(self, pos: RiderPosition, new_stop: float, tick: Optional[Tick], now: dt.datetime) -> bool:
        """THE one rule for when a proposed stop move is worth sending to the
        broker - the live engine and the replay both ask here. It only ever
        holds a move back, so NEVER WIDEN is untouched. A move is due when:

        * it gains at least ``stop_min_step_pips`` pips AND at least
          ``stop_min_step_spreads`` spreads (no 0.1-pip nudges); and
        * at least ``stop_min_modify_seconds`` have passed since the last
          move was sent, accepted or refused (a refusal waits too).

        One exception to the wait (never to the step): the first move to net
        breakeven - costs covered while the stop still sits behind the
        entry - goes at once, unless the last move sent was refused.
        Anything held back is still protected: FlowLock X closes the trade
        when the price comes back to a level it wanted to protect."""
        c = self.cfg
        fl = pos.flow
        s = pos.side.sign
        gain = (float(new_stop) - fl.stop) * s
        if not gain > 0:
            return False
        pip = pos.pip
        if not pip:
            sy = self.syms.get(pos.symbol)
            pip = sy.pip if sy is not None else 0.0
        spread = max(float(tick.ask) - float(tick.bid), 0.0) if tick is not None else 0.0
        step = max(c.stop_min_step_pips * pip, c.stop_min_step_spreads * spread)
        if gain < step - 1e-6 * (pip or 1e-9):
            return False
        if _ts(now) - fl.last_attempt_at >= c.stop_min_modify_seconds - 1e-9:
            return True
        refused_last = fl.last_attempt_at > fl.last_modify_at
        return (not refused_last) and fl.costs_covered and (fl.entry - fl.stop) * s > 0

    def stop_move_sent(self, pos: RiderPosition, now: dt.datetime) -> None:
        """A stop move has gone to the broker (whatever its answer)."""
        pos.flow.last_attempt_at = _ts(now)

    def confirm_stop(self, pos: RiderPosition, stop: Optional[float], sent_at: Optional[dt.datetime] = None) -> bool:
        """Commit a stop the broker holds. ``sent_at``: when that accepted move
        was sent (stop_move_sent), for the pacing in stop_move_due."""
        ok = self.flowlock.commit(pos.flow, stop)
        if ok and sent_at is not None:
            pos.flow.last_modify_at = _ts(sent_at)
        return ok
