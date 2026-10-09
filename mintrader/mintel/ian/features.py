"""Order-flow features from the normalised book and trade events.

Everything here is deterministic and defined by a formula (given in each
docstring): the same events at the same evaluation times always give the
same numbers. Nothing is inferred that the feed does not carry - no queue
positions are invented, and an iceberg is only ever a POSSIBLE REFRESH /
ABSORPTION, never a certainty.

Direction convention: every signed value is in the FUTURES direction
(+ = buyers have the advantage / the future is expected to rise). The
translation to the spot pair (JPY, CAD, CHF invert) happens only in
:mod:`mintel.ian.mapping`, at the edge.

Units: prices are converted to TICKS of the instrument (the smaller of the
reference tick and the grid observed in the book), sizes are contracts.

How an executed order is told apart from a cancelled one: CME (and the
vendors that carry it) publish the trade BEFORE the book update it causes.
A trade at price p leaves "pending executed volume" at (side, p); a size
decrease at that level within ``exec_match_s`` is attributed to execution up
to that amount, and only the rest is counted as a cancellation. A level that
leaves a depth-limited view (cause "view") is neither.

Windows: SHORT (5 s), MEDIUM (30 s), LONG (120 s), and a BASELINE of the
300 s before the medium window - "normal" for this market right now, so
every intensity, depth and spread figure is relative to its own recent past.
"""
from __future__ import annotations

import datetime as dt
import math
import statistics as st
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Optional
from zoneinfo import ZoneInfo

from .book import (ASK, BID, BUY, SELL, UNKNOWN, BookEvent, LevelChange, OrderBook, TradeEvent, px)
from .mapping import contract, root_of

TZ_CHICAGO = ZoneInfo("America/Chicago")


@dataclass
class FlowConfig:
    levels: int = 10
    near_ticks: int = 5
    imbalance_decay: float = 0.5           # lambda in w = exp(-lambda * distance_ticks)
    short_s: int = 5
    medium_s: int = 30
    long_s: int = 120
    baseline_s: int = 300
    min_baseline_s: int = 30
    exec_match_s: float = 0.25
    absorption_s: int = 10
    absorption_min_pressure: float = 1.0
    absorption_full_pressure: float = 3.0
    absorption_ticks_per_pressure: float = 1.0
    sweep_gap_s: float = 0.5
    sweep_min_levels: int = 3
    sweep_exec_share: float = 0.6
    large_print_mult: float = 5.0
    large_print_min: float = 10.0
    wall_mult: float = 3.0
    refresh_ratio: float = 1.5
    refresh_min_volume: float = 20.0
    refresh_volume_mult: float = 1.5       # x the normal one-sided volume of the whole market in the window
    footprint_ratio: float = 3.0
    footprint_min_volume: float = 5.0
    footprint_volume_share: float = 0.08    # a price needs this share of the window's normal volume to count
    stacked_levels: int = 3
    value_area: float = 0.70
    profile_min_volume: float = 200.0
    resilience_wait_s: float = 3.0
    consumption_event_share: float = 0.5   # executed near-touch volume in one second vs normal near-touch depth
    delta_unknown_max: float = 0.05
    divergence_ticks: float = 2.0
    divergence_volume: float = 0.5
    usefulness_horizon_s: int = 10


class _Sec:
    """One second of activity, and the book as it stood at the end of it."""
    __slots__ = ("t", "trades", "vol", "buy", "sell", "unk", "adds_n", "adds_v", "cancels_n", "cancels_v",
                 "execs_n", "execs_v", "add_near_b", "add_near_a", "can_near_b", "can_near_a", "exe_near_b",
                 "exe_near_a", "ofi", "turnover", "mid", "spread", "d5b", "d5a", "d10", "imb", "micro", "bb", "ba",
                 "lvl_med", "touch", "cum_delta", "vwap", "closed")

    def __init__(self, t: int):
        self.t = t
        for k in self.__slots__[1:]:
            setattr(self, k, 0.0)
        self.mid = None
        self.closed = False


@dataclass
class _Wall:
    side: str
    price: float
    first_seen: dt.datetime
    max_size: float
    size: float
    executed: float = 0.0
    refreshes: int = 0
    last_exec: Optional[dt.datetime] = None
    touched: bool = False
    status: str = "STANDING"
    ended: Optional[dt.datetime] = None


@dataclass
class FlowSnapshot:
    instrument: str
    ts: dt.datetime
    ok: bool
    reason: str = ""
    tick: float = 0.0
    # book
    bid: Optional[float] = None
    ask: Optional[float] = None
    mid: Optional[float] = None
    spread_ticks: float = 0.0
    microprice: Optional[float] = None
    micro_offset: float = 0.0
    imbalance: float = 0.0
    imbalance_avg: float = 0.0
    imbalance_persist: float = 0.0
    imbalance_change: float = 0.0
    depth5_bid: float = 0.0
    depth5_ask: float = 0.0
    depth_ratio: float = 1.0
    depth5_bid_ratio: float = 1.0           # near-touch depth on each side vs its normal
    depth5_ask_ratio: float = 1.0
    spread_ratio: float = 1.0
    # executed flow
    ofi_short: float = 0.0
    ofi_medium: float = 0.0
    buy_vol_short: float = 0.0
    sell_vol_short: float = 0.0
    buy_vol_medium: float = 0.0
    sell_vol_medium: float = 0.0
    aggr_short: float = 0.0
    aggr_medium: float = 0.0
    unknown_share: float = 0.0
    intensity: float = 1.0
    volume_intensity: float = 1.0
    acceleration: float = 0.0
    large_threshold: float = 0.0
    large_buy: float = 0.0
    large_sell: float = 0.0
    large_net: float = 0.0
    large_cluster: int = 0
    move_short_ticks: float = 0.0
    move_medium_ticks: float = 0.0
    move_long_ticks: float = 0.0
    range_low: Optional[float] = None       # lowest / highest mid over the medium window
    range_high: Optional[float] = None
    # absorption
    absorption_score: float = 0.0
    absorption_dir: int = 0
    absorption_pressure: float = 0.0
    absorption_response: float = 0.0
    # sweeps
    sweep_dir: int = 0
    sweep_levels: int = 0
    sweep_volume: float = 0.0
    sweep_age_s: Optional[float] = None
    sweep_exec_share: float = 0.0
    sweeps_medium: int = 0
    sweeps_up: int = 0
    sweeps_down: int = 0
    # pulling
    pull_ask: float = 0.0
    pull_bid: float = 0.0
    pulling_signal: float = 0.0
    # walls
    wall_bid: Optional[dict] = None
    wall_ask: Optional[dict] = None
    wall_events: list = field(default_factory=list)
    wall_signal: float = 0.0
    # refresh
    refresh_levels: list = field(default_factory=list)
    refresh_signal: float = 0.0
    # velocity
    adds_per_s: float = 0.0
    cancels_per_s: float = 0.0
    execs_per_s: float = 0.0
    velocity_ratio: float = 1.0
    depth_change: float = 0.0
    turnover: float = 0.0
    spread_change_ticks: float = 0.0
    # consumption
    consume_ask: float = 0.0
    replenish_ask: float = 0.0
    consume_bid: float = 0.0
    replenish_bid: float = 0.0
    consumption_signal: float = 0.0
    # resilience
    resilience: Optional[float] = None
    snapback: Optional[float] = None
    resilience_side: str = ""
    resilience_signal: float = 0.0
    # cumulative delta
    delta_reliable: bool = False
    cum_delta: float = 0.0
    delta_slope: float = 0.0
    delta_accel: float = 0.0
    delta_divergence: int = 0
    # footprint
    buy_imbalances: int = 0
    sell_imbalances: int = 0
    stacked_buy: bool = False
    stacked_sell: bool = False
    footprint_signal: float = 0.0
    delta_by_level: dict = field(default_factory=dict)
    # volume profile
    poc: Optional[float] = None
    vah: Optional[float] = None
    val: Optional[float] = None
    hvn: list = field(default_factory=list)
    lvn: list = field(default_factory=list)
    profile_location: str = ""
    profile_acceptance: float = 0.0
    profile_signal: float = 0.0
    # vwap
    vwap: Optional[float] = None
    vwap_dist_ticks: float = 0.0
    vwap_slope: float = 0.0
    vwap_acceptance: float = 0.0
    vwap_signal: float = 0.0
    # volatility
    vol_ticks: float = 0.0
    vol_ratio: float = 1.0
    usefulness: dict = field(default_factory=dict)
    labels: list = field(default_factory=list)

    def as_dict(self) -> dict:
        d = asdict(self)
        d["ts"] = self.ts.isoformat()
        return d


def _clip(x: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return lo if x < lo else hi if x > hi else x


def _sign(x: float) -> int:
    return (x > 0) - (x < 0)


def session_key(ts: dt.datetime) -> str:
    """The CME Globex trade date: a session runs 17:00 to 16:00 Chicago time,
    and after 17:00 it is already the next day's session."""
    return (ts.astimezone(TZ_CHICAGO) + dt.timedelta(hours=7)).date().isoformat()


class InstrumentFlow:
    """All the order-flow state of one futures instrument."""

    def __init__(self, instrument: str, cfg: Optional[FlowConfig] = None, tick_size: Optional[float] = None,
                 aggressor_reliable: bool = True):
        self.instrument = instrument
        self.cfg = cfg or FlowConfig()
        c = contract(root_of(instrument))
        self.ref_tick = float(tick_size or (c.tick_size if c is not None else 0.0) or 0.0)
        self.grid_tick = 0.0
        self.aggressor_reliable = bool(aggressor_reliable)
        self.book = OrderBook(instrument, self.cfg.levels)
        keep = self.cfg.baseline_s + self.cfg.medium_s + self.cfg.long_s + 5
        self.secs: deque[_Sec] = deque(maxlen=keep)
        self.cur: Optional[_Sec] = None
        self.trades: deque = deque()          # (ts, price, size, aggressor, seq)
        self.changes: deque = deque()         # (ts, side, price, delta, kind, near)
        self.sizes: deque = deque(maxlen=3000)
        self.pending: dict[tuple, list] = {}  # (side, price) -> [amount, ts]
        self.max_vis: dict[tuple, list] = {}  # (side, price) -> [max size, ts]
        self.walls: dict[tuple, _Wall] = {}
        self.wall_log: deque = deque(maxlen=50)
        self.consumption_event: Optional[dict] = None
        self.last_ts: Optional[dt.datetime] = None
        self.events = 0
        self._top = (None, 0.0, None, 0.0)
        self._base: Optional[dict] = None
        # session accumulators
        self.session = ""
        self.cum_delta = 0.0
        self.cum_classified = 0.0
        self.cum_unknown = 0.0
        self.pv = 0.0
        self.v = 0.0
        self.profile: dict[int, float] = {}
        # absorption episodes and feature usefulness
        self.absorption_episodes: deque = deque(maxlen=200)
        self._abs_active = False
        self.useful: dict[str, list] = {}     # name -> [n, hits]
        self._useful_pending: deque = deque()

    # ------------------------------------------------------------ helpers --
    @property
    def tick(self) -> float:
        t = [x for x in (self.ref_tick, self.grid_tick) if x > 0]
        return min(t) if t else 1e-9

    def _ticks(self, price_distance: float) -> float:
        return price_distance / self.tick

    def _tick_index(self, price: float) -> int:
        return int(round(price / self.tick))

    def _observe_grid(self) -> None:
        for side in (BID, ASK):
            lv = self.book.levels(side, 5)
            for (a, _), (b, _) in zip(lv, lv[1:]):
                g = round(abs(a - b), 9)
                if g > 0 and (self.grid_tick == 0.0 or g < self.grid_tick - 1e-12):
                    self.grid_tick = g

    # ----------------------------------------------------------- the clock --
    def _sec_of(self, ts: dt.datetime) -> int:
        return int(ts.timestamp() // 1)

    def _close_current(self) -> None:
        s = self.cur
        if s is None or s.closed:
            return
        b = self.book
        bb, ba = b.best_bid(), b.best_ask()
        s.bb, s.ba = bb, ba
        s.mid = b.mid()
        s.spread = self._ticks(b.spread()) if b.spread() is not None else 0.0
        s.d5b, s.d5a = b.depth(BID, 5), b.depth(ASK, 5)
        s.d10 = b.depth(BID, 10) + b.depth(ASK, 10)
        s.imb = self.weighted_imbalance()
        mp = b.microprice()
        sp = b.spread()
        s.micro = ((mp - s.mid) / (sp / 2.0)) if (mp is not None and sp) else 0.0
        sizes = [q for _, q in b.levels(BID, 10)] + [q for _, q in b.levels(ASK, 10)]
        s.lvl_med = st.median(sizes) if sizes else 0.0
        s.touch = ((b.bids.get(bb, 0.0) if bb is not None else 0.0) + (b.asks.get(ba, 0.0) if ba is not None else 0.0)) / 2.0
        s.cum_delta = self.cum_delta
        s.vwap = (self.pv / self.v) if self.v > 0 else (s.mid or 0.0)
        s.closed = True
        self._observe_grid()
        self._after_close(s)

    def _roll(self, ts: dt.datetime) -> None:
        sec = self._sec_of(ts)
        if self.cur is None:
            self.cur = _Sec(sec)
            self.secs.append(self.cur)
            return
        if sec <= self.cur.t:
            return                              # same second (or a late event): it counts in the current one
        self._close_current()
        gap = sec - self.cur.t
        prev = self.cur
        for k in range(1, min(gap, self.secs.maxlen or gap)):
            q = _Sec(prev.t + k)                # a quiet second: no activity, the book unchanged
            for f in ("mid", "spread", "d5b", "d5a", "d10", "imb", "micro", "bb", "ba", "lvl_med", "touch",
                      "cum_delta", "vwap"):
                setattr(q, f, getattr(prev, f))
            q.closed = True
            self.secs.append(q)
            self._after_close(q)
        self.cur = _Sec(sec)
        self.secs.append(self.cur)
        self._base = None
        self._trim(ts)

    def _trim(self, ts: dt.datetime) -> None:
        keep = dt.timedelta(seconds=self.cfg.medium_s + 2)
        while self.trades and ts - self.trades[0][0] > keep:
            self.trades.popleft()
        while self.changes and ts - self.changes[0][0] > keep:
            self.changes.popleft()
        stale = dt.timedelta(seconds=max(2.0, self.cfg.exec_match_s * 4))
        for k in [k for k, v in self.pending.items() if ts - v[1] > stale]:
            self.pending.pop(k, None)
        for k in [k for k, v in self.max_vis.items() if ts - v[1] > keep]:
            self.max_vis.pop(k, None)

    def _after_close(self, s: _Sec) -> None:
        """Per closed second: consumption events, usefulness bookkeeping."""
        base = self.baseline()
        # a consumption event: a lot of near-touch liquidity EXECUTED in one second
        if base is not None and s.mid is not None:
            for side, exe, d5 in ((ASK, s.exe_near_a, base["d5a"]), (BID, s.exe_near_b, base["d5b"])):
                if d5 > 0 and exe >= self.cfg.consumption_event_share * d5:
                    prev = self.secs[-2] if len(self.secs) >= 2 and self.secs[-2] is not s else None
                    before_mid = prev.mid if prev is not None and prev.mid is not None else s.mid
                    before_depth = (prev.d5a if side == ASK else prev.d5b) if prev is not None else d5
                    self.consumption_event = {"t": s.t, "side": side, "depth_before": max(before_depth, 1e-9),
                                              "mid_before": before_mid, "mid_after": s.mid}
        # usefulness: did the sign of each signal agree with the next move?
        if s.mid is not None:
            h = self.cfg.usefulness_horizon_s
            self._useful_pending.append((s.t, s.mid, {"book_imbalance": s.imb, "microprice": s.micro,
                                                      "aggression": s.buy - s.sell, "ofi": s.ofi}))
            while self._useful_pending and self._useful_pending[0][0] <= s.t - h:
                t0, m0, vals = self._useful_pending.popleft()
                move = s.mid - m0
                if abs(move) < self.tick * 0.5:
                    continue
                for name, v in vals.items():
                    if abs(v) < 1e-9:
                        continue
                    rec = self.useful.setdefault(name, [0, 0])
                    rec[0] += 1
                    rec[1] += int(_sign(v) == _sign(move))

    # ------------------------------------------------------------ ingest --
    def _new_session(self, ts: dt.datetime) -> None:
        key = session_key(ts)
        if key != self.session:
            self.session = key
            self.cum_delta = self.cum_classified = self.cum_unknown = 0.0
            self.pv = self.v = 0.0
            self.profile = {}

    def on_book(self, ev: BookEvent) -> list[LevelChange]:
        ts = ev.ts_exchange
        self._roll(ts)
        self.last_ts = ts if self.last_ts is None or ts > self.last_ts else self.last_ts
        self.events += 1
        changes = self.book.apply(ev)
        s = self.cur
        for ch in changes:
            self._on_change(ch, ts, s)
        self._ofi(s)
        return changes

    def _ofi(self, s: _Sec) -> None:
        """Order-flow imbalance (Cont, Kukanov & Stoikov 2014), per top-of-book update n:
        e_n = 1{Pb_n >= Pb_n-1} Qb_n - 1{Pb_n <= Pb_n-1} Qb_n-1 - 1{Pa_n <= Pa_n-1} Qa_n + 1{Pa_n >= Pa_n-1} Qa_n-1."""
        b = self.book
        bb, ba = b.best_bid(), b.best_ask()
        qb = b.bids.get(bb, 0.0) if bb is not None else 0.0
        qa = b.asks.get(ba, 0.0) if ba is not None else 0.0
        pbb, pqb, pba, pqa = self._top
        if pbb is not None and bb is not None and pba is not None and ba is not None:
            e = (qb if bb >= pbb else 0.0) - (pqb if bb <= pbb else 0.0) \
                - (qa if ba <= pba else 0.0) + (pqa if ba >= pba else 0.0)
            s.ofi += e
        self._top = (bb, qb, ba, qa)

    def _near(self, ch: LevelChange) -> bool:
        if ch.best_before is None:
            return True
        d = (ch.best_before - ch.price) if ch.side == BID else (ch.price - ch.best_before)
        return self._ticks(d) < self.cfg.near_ticks - 1e-9

    def _wall_threshold(self) -> float:
        base = self.baseline()
        ref = base["lvl_med"] if base is not None and base["lvl_med"] > 0 else 0.0
        if ref <= 0:
            sizes = [q for _, q in self.book.levels(BID, 10)] + [q for _, q in self.book.levels(ASK, 10)]
            ref = st.median(sizes) if sizes else 0.0
        return self.cfg.wall_mult * ref if ref > 0 else float("inf")

    def _on_change(self, ch: LevelChange, ts: dt.datetime, s: _Sec) -> None:
        key = (ch.side, ch.price)
        mv = self.max_vis.get(key)
        if mv is None:
            self.max_vis[key] = [max(ch.after, ch.before), ts]
        else:
            mv[0] = max(mv[0], ch.after)
            mv[1] = ts
        d = ch.delta
        if ch.cause:                           # a depth-window artefact or a rebuild: not an exchange action
            self.changes.append((ts, ch.side, ch.price, d, "view", False))
            w = self.walls.get(key)
            if w is not None and ch.after <= 0:
                self.walls.pop(key, None)
            return
        near = self._near(ch)
        s.turnover += abs(d)
        if d > 0:
            s.adds_n += 1
            s.adds_v += d
            if near:
                if ch.side == BID:
                    s.add_near_b += d
                else:
                    s.add_near_a += d
            self.changes.append((ts, ch.side, ch.price, d, "add", near))
            exe = 0.0
        else:
            dec = -d
            exe = 0.0
            p = self.pending.get(key)
            if p is not None and (ts - p[1]).total_seconds() <= self.cfg.exec_match_s and p[0] > 0:
                exe = min(dec, p[0])
                p[0] -= exe
            can = dec - exe
            if exe > 0:
                s.execs_n += 1
                s.execs_v += exe
                if near:
                    if ch.side == BID:
                        s.exe_near_b += exe
                    else:
                        s.exe_near_a += exe
                self.changes.append((ts, ch.side, ch.price, -exe, "exec", near))
            if can > 0:
                s.cancels_n += 1
                s.cancels_v += can
                if near:
                    if ch.side == BID:
                        s.can_near_b += can
                    else:
                        s.can_near_a += can
                self.changes.append((ts, ch.side, ch.price, -can, "cancel", near))
        self._track_wall(ch, ts, exe)

    def _track_wall(self, ch: LevelChange, ts: dt.datetime, exe: float) -> None:
        key = (ch.side, ch.price)
        w = self.walls.get(key)
        if w is None:
            if ch.after >= self._wall_threshold() and len(self.walls) < 40:
                self.walls[key] = _Wall(ch.side, ch.price, ts, ch.after, ch.after)
            return
        if exe > 0:
            w.executed += exe
            w.last_exec = ts
            if ch.best_before is not None and abs(ch.best_before - ch.price) < 1e-12:
                w.touched = True
        if ch.delta > 0 and w.last_exec is not None and (ts - w.last_exec).total_seconds() <= 1.0:
            w.refreshes += 1
        w.size = ch.after
        w.max_size = max(w.max_size, ch.after)
        if ch.after <= 0:
            w.status = "CONSUMED" if w.executed >= 0.5 * w.max_size else "PULLED"
            w.ended = ts
            self.walls.pop(key, None)
            self.wall_log.append(w)
        elif ch.after < 0.5 * self._wall_threshold() and w.executed < 0.5 * w.max_size:
            self.walls.pop(key, None)            # no longer a wall (it faded); not an event

    def on_trade(self, ev: TradeEvent) -> None:
        ts = ev.ts
        self._roll(ts)
        self.last_ts = ts if self.last_ts is None or ts > self.last_ts else self.last_ts
        self.events += 1
        self._new_session(ts)
        s = self.cur
        size = float(ev.size)
        price = px(ev.price)
        aggr = ev.aggressor
        if aggr == UNKNOWN:
            # the side can still be known from where it printed (at the offer = a buyer), but the
            # trade stays UNKNOWN for the delta: only the exchange's own flag is trusted there
            bb, ba = self.book.best_bid(), self.book.best_ask()
            hit = ASK if (ba is not None and price >= ba) else BID if (bb is not None and price <= bb) else ""
        else:
            hit = ASK if aggr == BUY else BID
        if hit:
            p = self.pending.get((hit, price))
            if p is None or (ts - p[1]).total_seconds() > self.cfg.exec_match_s:
                self.pending[(hit, price)] = [size, ts]
            else:
                p[0] += size
                p[1] = ts
        s.trades += 1
        s.vol += size
        if aggr == BUY:
            s.buy += size
            self.cum_delta += size
            self.cum_classified += size
        elif aggr == SELL:
            s.sell += size
            self.cum_delta -= size
            self.cum_classified += size
        else:
            s.unk += size
            self.cum_unknown += size
        self.sizes.append(size)
        self.trades.append((ts, price, size, aggr, ev.sequence, hit))
        self.pv += price * size
        self.v += size
        k = self._tick_index(price)
        self.profile[k] = self.profile.get(k, 0.0) + size

    def on_event(self, ev) -> None:
        if isinstance(ev, BookEvent):
            self.on_book(ev)
        elif isinstance(ev, TradeEvent):
            self.on_trade(ev)

    # ------------------------------------------------------------ windows --
    def _window(self, n: int) -> list[_Sec]:
        out = []
        for s in reversed(self.secs):
            out.append(s)
            if len(out) >= n:
                break
        return out

    def baseline(self) -> Optional[dict]:
        """Means over the BASELINE seconds (the ``baseline_s`` before the
        medium window). None until ``min_baseline_s`` of history exists."""
        if self._base is not None:
            return self._base
        closed = [s for s in self.secs if s.closed]
        if len(closed) <= self.cfg.medium_s:
            return None
        pool = closed[:-self.cfg.medium_s][-self.cfg.baseline_s:]
        if len(pool) < self.cfg.min_baseline_s:
            return None
        n = float(len(pool))
        mids = [s.mid for s in pool if s.mid is not None]
        dm = [self._ticks(b - a) for a, b in zip(mids, mids[1:])]
        base = {
            "n": len(pool),
            "trades": sum(s.trades for s in pool) / n,
            "vol": sum(s.vol for s in pool) / n,
            "activity": sum(s.adds_n + s.cancels_n + s.execs_n for s in pool) / n,
            "d5b": sum(s.d5b for s in pool) / n,
            "d5a": sum(s.d5a for s in pool) / n,
            "d10": sum(s.d10 for s in pool) / n,
            "spread": sum(s.spread for s in pool) / n,
            "lvl_med": sum(s.lvl_med for s in pool) / n,
            "touch": sum(s.touch for s in pool) / n,
            "vol_ticks": (st.pstdev(dm) if len(dm) > 2 else 0.0),
        }
        self._base = base
        return base

    # ---------------------------------------------------------- features --
    def weighted_imbalance(self) -> float:
        """Distance-weighted book imbalance over the visible levels:
        I = sum_i w(d_i) (Qbid_i) - sum_j w(d_j) (Qask_j)  /  sum w(d) Q over both sides,
        w(d) = exp(-lambda * d), d = distance from the mid in ticks. In [-1, 1]; + = bid-heavy."""
        b = self.book
        mid = b.mid()
        if mid is None:
            return 0.0
        lam = self.cfg.imbalance_decay
        num = den = 0.0
        for p, q in b.levels(BID, self.cfg.levels):
            w = math.exp(-lam * self._ticks(mid - p))
            num += w * q
            den += w * q
        for p, q in b.levels(ASK, self.cfg.levels):
            w = math.exp(-lam * self._ticks(p - mid))
            num -= w * q
            den += w * q
        return num / den if den > 0 else 0.0

    def _prints(self, trades) -> list[tuple]:
        """Aggressive ORDERS: consecutive trades with the same exchange time and
        aggressor are one order that took several prices."""
        out: list[list] = []
        for t in trades:
            ts, price, size, aggr = t[0], t[1], t[2], t[3]
            if out and out[-1][0] == ts and out[-1][3] == aggr:
                out[-1][2] += size
                out[-1][4].append(price)
            else:
                out.append([ts, price, size, aggr, [price]])
        return [tuple(x) for x in out]

    def compute(self, now: Optional[dt.datetime] = None) -> FlowSnapshot:
        cfg = self.cfg
        now = now or self.last_ts
        if now is None:
            return FlowSnapshot(self.instrument, dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc), False,
                                "no events yet")
        self._roll(now)
        b = self.book
        fs = FlowSnapshot(self.instrument, now, False, tick=self.tick)
        fs.bid, fs.ask, fs.mid = b.best_bid(), b.best_ask(), b.mid()
        if fs.mid is None:
            fs.reason = "the book has no two-sided price"
            return fs
        if b.crossed():
            fs.reason = "the book is crossed (bid at or above the offer)"
            return fs
        base = self.baseline()
        if base is None:
            have = sum(1 for s in self.secs if s.closed)
            fs.reason = f"warming up: {have} s of market history, {cfg.medium_s + cfg.min_baseline_s} s needed"
            return fs
        fs.ok = True
        tick = self.tick
        sp = b.spread() or 0.0
        fs.spread_ticks = sp / tick
        fs.microprice = b.microprice()
        fs.micro_offset = _clip((fs.microprice - fs.mid) / (sp / 2.0)) if sp > 0 and fs.microprice is not None else 0.0
        short, medium, longw = self._window(cfg.short_s), self._window(cfg.medium_s), self._window(cfg.long_s)
        # the spread relative to normal, averaged over the short window with the spread now, so one
        # momentary gap does not read as a thin market
        sps = [s.spread for s in short if s.closed] + [fs.spread_ticks]
        fs.spread_ratio = (sum(sps) / len(sps)) / max(base["spread"], 1.0)
        closed_med = [s for s in medium if s.closed]

        # --- book imbalance ---
        fs.imbalance = self.weighted_imbalance()
        imbs = [s.imb for s in closed_med] or [fs.imbalance]
        fs.imbalance_avg = sum(imbs) / len(imbs)
        sg = _sign(fs.imbalance)
        fs.imbalance_persist = (sum(1 for x in imbs if _sign(x) == sg and sg != 0) / len(imbs)) if imbs else 0.0
        fs.imbalance_change = fs.imbalance - (closed_med[-1].imb if closed_med else fs.imbalance)
        fs.depth5_bid, fs.depth5_ask = b.depth(BID, 5), b.depth(ASK, 5)
        d10 = b.depth(BID, 10) + b.depth(ASK, 10)
        fs.depth_ratio = d10 / base["d10"] if base["d10"] > 0 else 1.0
        fs.depth5_bid_ratio = fs.depth5_bid / base["d5b"] if base["d5b"] > 0 else 1.0
        fs.depth5_ask_ratio = fs.depth5_ask / base["d5a"] if base["d5a"] > 0 else 1.0

        # --- executed flow ---
        touch = max(base["touch"], 1.0)
        fs.ofi_short = sum(s.ofi for s in short) / touch
        fs.ofi_medium = sum(s.ofi for s in medium) / touch
        fs.buy_vol_short, fs.sell_vol_short = sum(s.buy for s in short), sum(s.sell for s in short)
        fs.buy_vol_medium, fs.sell_vol_medium = sum(s.buy for s in medium), sum(s.sell for s in medium)
        tot_s = fs.buy_vol_short + fs.sell_vol_short
        tot_m = fs.buy_vol_medium + fs.sell_vol_medium
        fs.aggr_short = (fs.buy_vol_short - fs.sell_vol_short) / tot_s if tot_s > 0 else 0.0
        fs.aggr_medium = (fs.buy_vol_medium - fs.sell_vol_medium) / tot_m if tot_m > 0 else 0.0
        unk = sum(s.unk for s in medium)
        fs.unknown_share = unk / (tot_m + unk) if (tot_m + unk) > 0 else 0.0
        rate_s = sum(s.trades for s in short) / cfg.short_s
        fs.intensity = rate_s / max(base["trades"], 0.05)
        fs.volume_intensity = (sum(s.vol for s in short) / cfg.short_s) / max(base["vol"], 0.05)
        prev_short = self._window(2 * cfg.short_s)[cfg.short_s:]
        rate_prev = sum(s.trades for s in prev_short) / cfg.short_s if prev_short else rate_s
        fs.acceleration = (rate_s - rate_prev) / max(base["trades"], 0.05)

        trades_med = list(self.trades)
        cut = now - dt.timedelta(seconds=cfg.medium_s)
        trades_med = [t for t in trades_med if t[0] > cut]
        prints = self._prints(trades_med)
        med_size = st.median(self.sizes) if self.sizes else 1.0
        fs.large_threshold = max(cfg.large_print_min, cfg.large_print_mult * med_size)
        large = [p for p in prints if p[2] >= fs.large_threshold and p[3] in (BUY, SELL)]
        fs.large_buy = sum(p[2] for p in large if p[3] == BUY)
        fs.large_sell = sum(p[2] for p in large if p[3] == SELL)
        # the large prints' net, as a share of ALL the aggressive volume in the window (one big print in
        # a busy minute is not a signal; a few that make up most of the volume are)
        fs.large_net = (fs.large_buy - fs.large_sell) / tot_m if tot_m > 0 else 0.0
        for side, sgn in ((BUY, 1), (SELL, -1)):
            ts_l = [p[0] for p in large if p[3] == side]
            for i in range(len(ts_l) - 2):
                if (ts_l[i + 2] - ts_l[i]).total_seconds() <= 2.0:
                    fs.large_cluster = sgn if fs.large_cluster == 0 else fs.large_cluster
                    break

        def mid_back(n: int) -> Optional[float]:
            w = self._window(n + 1)
            for s in reversed(w):               # the oldest second in the window with a mid
                if s.closed and s.mid is not None:
                    return s.mid
            return None

        for attr, n in (("move_short_ticks", cfg.short_s), ("move_medium_ticks", cfg.medium_s),
                        ("move_long_ticks", cfg.long_s)):
            m0 = mid_back(n)
            setattr(fs, attr, (fs.mid - m0) / tick if m0 is not None else 0.0)

        mids = [s.mid for s in closed_med if s.mid is not None] + [fs.mid]
        fs.range_low, fs.range_high = min(mids), max(mids)
        self._absorption(fs, base, now)
        self._sweeps(fs, trades_med, now)
        self._pulling(fs, base, medium)
        self._walls(fs, now)
        self._refresh(fs, trades_med, now)
        self._velocity(fs, base, short, medium)
        self._consumption(fs, base, medium)
        self._resilience(fs, now)
        self._delta(fs, base, medium, longw)
        self._footprint(fs, trades_med)
        self._profile(fs, trades_med)
        self._vwap(fs, medium, longw, trades_med)
        dm = [(b2.mid - a.mid) / tick for a, b2 in zip(reversed(closed_med), list(reversed(closed_med))[1:])
              if a.mid is not None and b2.mid is not None]
        fs.vol_ticks = st.pstdev(dm) if len(dm) > 2 else 0.0
        fs.vol_ratio = fs.vol_ticks / max(base["vol_ticks"], 0.1)
        fs.usefulness = {k: {"n": v[0], "hit_rate": round(v[1] / v[0], 3) if v[0] else None}
                         for k, v in self.useful.items()}
        self._labels(fs)
        return fs

    def _absorption(self, fs: FlowSnapshot, base: dict, now: dt.datetime) -> None:
        """ABSORPTION_SCORE in [0, 1]: heavy one-sided aggression that fails to move price.
        Over W = absorption_s seconds: N = buy - sell aggressive volume; P = |N| / (baseline volume per
        second * W); M = sign(N) * mid move in ticks; E = kappa * P.
        score = clip((P - P_min) / (P_full - P_min), 0, 1) * clip(1 - max(M, 0) / max(E, 1), 0, 1) * H,
        H = 1 if the absorbing touch held (it did not retreat more than one tick), else 0.5.
        The implied direction is AGAINST the aggressor (-sign(N)): the passive side is absorbing it."""
        cfg = self.cfg
        w = self._window(cfg.absorption_s)
        n = sum(s.buy - s.sell for s in w)
        bvol = max(base["vol"] * cfg.absorption_s, 1.0)
        p = abs(n) / bvol
        fs.absorption_pressure = p
        old = None
        for s in reversed(self._window(cfg.absorption_s + 1)):
            if s.closed and s.mid is not None:
                old = s
                break
        if old is None or p < cfg.absorption_min_pressure or n == 0:
            fs.absorption_score, fs.absorption_dir, fs.absorption_response = 0.0, 0, 0.0
            self._abs_active = False
            return
        tick = self.tick
        m = _sign(n) * (fs.mid - old.mid) / tick
        e = cfg.absorption_ticks_per_pressure * p
        response = _clip(1.0 - max(m, 0.0) / max(e, 1.0), 0.0, 1.0)
        pressure = _clip((p - cfg.absorption_min_pressure) / (cfg.absorption_full_pressure - cfg.absorption_min_pressure),
                         0.0, 1.0)
        if n > 0:
            held = fs.ask is not None and old.ba is not None and fs.ask <= old.ba + tick + 1e-12
        else:
            held = fs.bid is not None and old.bb is not None and fs.bid >= old.bb - tick - 1e-12
        fs.absorption_response = response
        fs.absorption_score = round(pressure * response * (1.0 if held else 0.5), 4)
        fs.absorption_dir = -_sign(n) if fs.absorption_score > 0 else 0
        if fs.absorption_score >= 0.6 and not self._abs_active:
            self.absorption_episodes.append({"ts": now, "dir": fs.absorption_dir, "mid": fs.mid})
        self._abs_active = fs.absorption_score >= 0.6

    def _sweeps(self, fs: FlowSnapshot, trades_med: list, now: dt.datetime) -> None:
        """A sweep: a burst of same-side aggressive trades (gaps <= sweep_gap_s, each at the same or a
        worse price than the one before - a step back ends the burst) that prints at >= sweep_min_levels
        distinct prices, i.e. moves through the book, where the depth
        removed on the swept side in that time was mostly EXECUTED (share >= sweep_exec_share) -
        not cancelled. The most recent one in the medium window is reported."""
        cfg = self.cfg
        bursts: list[list] = []
        for t in trades_med:
            if t[3] not in (BUY, SELL):
                continue
            if bursts:
                last = bursts[-1][-1]
                onward = (t[1] >= last[1] - 1e-12) if t[3] == BUY else (t[1] <= last[1] + 1e-12)
                if last[3] == t[3] and onward and (t[0] - last[0]).total_seconds() <= cfg.sweep_gap_s:
                    bursts[-1].append(t)
                    continue
            bursts.append([t])                  # a new burst: other side, a pause, or a step back
        found = []
        for bu in bursts:
            distinct = set(t[1] for t in bu)
            if len(distinct) < cfg.sweep_min_levels:
                continue
            up = bu[0][3] == BUY
            t0, t1 = bu[0][0], bu[-1][0] + dt.timedelta(seconds=cfg.exec_match_s)
            side = ASK if up else BID
            exe = can = 0.0
            for c in self.changes:
                if c[0] < t0 or c[0] > t1 or c[1] != side:
                    continue
                if c[4] == "exec":
                    exe += -c[3]
                elif c[4] == "cancel":
                    can += -c[3]
            share = exe / (exe + can) if (exe + can) > 0 else 0.0
            if share < cfg.sweep_exec_share:
                continue
            found.append((bu[-1][0], 1 if up else -1, len(distinct), sum(t[2] for t in bu), share))
        fs.sweeps_medium = len(found)
        fs.sweeps_up = sum(1 for f in found if f[1] > 0)
        fs.sweeps_down = len(found) - fs.sweeps_up
        if found:
            last = found[-1]
            fs.sweep_dir, fs.sweep_levels, fs.sweep_volume = last[1], last[2], last[3]
            fs.sweep_age_s = (now - last[0]).total_seconds()
            fs.sweep_exec_share = round(last[4], 3)

    def _pulling(self, fs: FlowSnapshot, base: dict, medium: list) -> None:
        """Liquidity pulling: near-touch depth removed by CANCELLATION net of new adds, per side,
        over the medium window, relative to that side's normal near-touch depth:
        pull_ask = max(0, cancelled_ask_near - added_ask_near) / baseline depth5_ask.
        pulling_signal = clip(pull_ask - pull_bid): + = offers being pulled ahead of price (bullish)."""
        ca = sum(s.can_near_a for s in medium)
        aa = sum(s.add_near_a for s in medium)
        cb = sum(s.can_near_b for s in medium)
        ab = sum(s.add_near_b for s in medium)
        fs.pull_ask = max(0.0, ca - aa) / max(base["d5a"], 1.0)
        fs.pull_bid = max(0.0, cb - ab) / max(base["d5b"], 1.0)
        fs.pulling_signal = _clip(fs.pull_ask - fs.pull_bid)

    def _walls(self, fs: FlowSnapshot, now: dt.datetime) -> None:
        """Walls: a level of at least wall_mult x the normal level size. For each: size, size relative
        to normal, distance from the touch (ticks), persistence (s), executed volume against it,
        refreshes, and how it ended - CONSUMED (removed after >= 50% of its peak size executed),
        PULLED (removed by cancellation) or REJECTED (traded into, still standing, price 2+ ticks away).
        wall_signal sums recent endings: ask wall consumed +1, bid wall consumed -1, ask pulled +0.5,
        bid pulled -0.5, bid rejected +0.5, ask rejected -0.5; clipped to [-1, 1]."""
        tick = self.tick
        thr = self._wall_threshold()
        ref = thr / self.cfg.wall_mult if thr != float("inf") else 0.0
        for w in list(self.walls.values()):
            if w.touched and w.status == "STANDING":
                away = (fs.mid - w.price) / tick if w.side == BID else (w.price - fs.mid) / tick
                if away >= 2.0:
                    w.status = "REJECTED"
                    w.ended = now
                    self.wall_log.append(_Wall(**{**w.__dict__}))
                    w.touched = False
                    w.status = "STANDING"
        best = {BID: None, ASK: None}
        for w in self.walls.values():
            if w.side == BID and fs.bid is not None and w.price <= fs.bid + 1e-12:
                if best[BID] is None or w.price > best[BID].price:
                    best[BID] = w
            if w.side == ASK and fs.ask is not None and w.price >= fs.ask - 1e-12:
                if best[ASK] is None or w.price < best[ASK].price:
                    best[ASK] = w
        for side, attr in ((BID, "wall_bid"), (ASK, "wall_ask")):
            w = best[side]
            if w is None:
                continue
            touch = fs.bid if side == BID else fs.ask
            setattr(fs, attr, {"price": w.price, "size": w.size, "ratio": round(w.size / ref, 2) if ref else None,
                               "distance_ticks": round(abs(w.price - touch) / tick, 1),
                               "persistence_s": round((now - w.first_seen).total_seconds(), 1),
                               "executed": w.executed, "refreshes": w.refreshes})
        sig = 0.0
        recent = now - dt.timedelta(seconds=self.cfg.medium_s)
        events = []
        for w in self.wall_log:
            if w.ended is None or w.ended < recent:
                continue
            v = {("ASK", "CONSUMED"): 1.0, ("BID", "CONSUMED"): -1.0, ("ASK", "PULLED"): 0.5,
                 ("BID", "PULLED"): -0.5, ("BID", "REJECTED"): 0.5, ("ASK", "REJECTED"): -0.5}.get((w.side, w.status), 0.0)
            sig += v
            events.append({"side": w.side, "price": w.price, "status": w.status, "peak": w.max_size,
                           "executed": w.executed, "refreshes": w.refreshes,
                           "age_s": round((now - w.ended).total_seconds(), 1)})
        fs.wall_events = events
        fs.wall_signal = _clip(sig)

    def _refresh(self, fs: FlowSnapshot, trades_med: list, now: dt.datetime) -> None:
        """POSSIBLE REFRESH / ABSORPTION at a price: executed volume there over the medium window
        >= refresh_ratio x the largest size ever visible there in that window, AND >= refresh_volume_mult x
        the whole market's normal one-sided volume for such a window (baseline volume per second / 2 x
        medium_s), AND >= refresh_min_volume, while the level still stands. More was traded at one price
        than was ever shown there, and far more than usual: something kept refilling it. Never called an
        iceberg as a fact. refresh_signal: + for a refreshing bid (support), - for an offer."""
        cfg = self.cfg
        base = self.baseline() or {"vol": 0.0}
        floor = max(cfg.refresh_min_volume, cfg.refresh_volume_mult * base["vol"] / 2.0 * cfg.medium_s)
        exe: dict[tuple, float] = {}
        for t in trades_med:
            hit = t[5]
            if hit:
                exe[(hit, t[1])] = exe.get((hit, t[1]), 0.0) + t[2]
        out = []
        sig = 0.0
        for (side, price), v in exe.items():
            vis = self.max_vis.get((side, price), [0.0])[0]
            stands = self.book.size_at(side, price) > 0
            if v >= floor and vis > 0 and v >= cfg.refresh_ratio * vis and stands:
                out.append({"side": side, "price": price, "executed": v, "max_visible": vis,
                            "ratio": round(v / vis, 2), "label": "POSSIBLE REFRESH / ABSORPTION"})
                sig += (1.0 if side == BID else -1.0) * min(1.0, v / (vis * cfg.refresh_ratio * 2))
        out.sort(key=lambda d: -d["executed"])
        fs.refresh_levels = out[:5]
        fs.refresh_signal = _clip(sig)

    def _velocity(self, fs: FlowSnapshot, base: dict, short: list, medium: list) -> None:
        """Book velocity over the short window: adds, cancels and executions per second; velocity_ratio =
        their total rate / the baseline rate; depth_change = (depth10 now - depth10 a medium window ago) /
        baseline depth10; turnover = sum |size change| per second / baseline depth10; spread change in ticks
        against the baseline spread."""
        n = float(self.cfg.short_s)
        fs.adds_per_s = sum(s.adds_n for s in short) / n
        fs.cancels_per_s = sum(s.cancels_n for s in short) / n
        fs.execs_per_s = sum(s.execs_n for s in short) / n
        fs.velocity_ratio = (fs.adds_per_s + fs.cancels_per_s + fs.execs_per_s) / max(base["activity"], 0.1)
        old = next((s for s in reversed(medium) if s.closed), None)
        d10 = self.book.depth(BID, 10) + self.book.depth(ASK, 10)
        fs.depth_change = (d10 - old.d10) / base["d10"] if old is not None and base["d10"] > 0 else 0.0
        fs.turnover = (sum(s.turnover for s in short) / n) / max(base["d10"], 1.0)
        fs.spread_change_ticks = fs.spread_ticks - base["spread"]

    def _consumption(self, fs: FlowSnapshot, base: dict, medium: list) -> None:
        """Rate of consumption vs replenishment near the touch over the medium window:
        consumed = executed volume, replenished = added volume, per side.
        consumption_signal = tanh((consumed_ask - replenished_ask) / d5a) - tanh((consumed_bid - replenished_bid) / d5b):
        + = offers being eaten faster than they come back (bullish), - = bids."""
        fs.consume_ask = sum(s.exe_near_a for s in medium)
        fs.replenish_ask = sum(s.add_near_a for s in medium)
        fs.consume_bid = sum(s.exe_near_b for s in medium)
        fs.replenish_bid = sum(s.add_near_b for s in medium)
        a = math.tanh((fs.consume_ask - fs.replenish_ask) / max(base["d5a"], 1.0))
        b = math.tanh((fs.consume_bid - fs.replenish_bid) / max(base["d5b"], 1.0))
        fs.consumption_signal = _clip(a - b)

    def _resilience(self, fs: FlowSnapshot, now: dt.datetime) -> None:
        """Book resilience after the latest consumption event (a second in which executed near-touch
        volume on one side >= consumption_event_share x its normal depth). After resilience_wait_s:
        resilience = depth5 of that side now / depth5 just before; snapback = fraction of the move
        retraced. Less than 30% retraced = genuine repricing in the consumption direction (signal
        +0.7 x dir); depth back to >= 80% and >= 60% retraced = the move was rejected (-0.7 x dir).
        (With a depth-limited feed, depth refills from beyond the view, so the price retrace is the
        deciding measure and depth only confirms a rejection.)"""
        ev = self.consumption_event
        if ev is None:
            return
        age = now.timestamp() - ev["t"]
        if age < self.cfg.resilience_wait_s or age > self.cfg.long_s:
            return
        side = ev["side"]
        depth_now = fs.depth5_ask if side == ASK else fs.depth5_bid
        fs.resilience = round(depth_now / ev["depth_before"], 3)
        move = ev["mid_after"] - ev["mid_before"]
        d = 1 if side == ASK else -1
        if abs(move) > 1e-12:
            fs.snapback = round(_clip((ev["mid_after"] - fs.mid) / move, -1.0, 2.0), 3)
        else:
            fs.snapback = 0.0
        fs.resilience_side = side
        if fs.snapback < 0.3:
            fs.resilience_signal = 0.7 * d          # the new price held: genuine repricing
        elif fs.snapback >= 0.6 and fs.resilience >= 0.8:
            fs.resilience_signal = -0.7 * d         # liquidity came back and price snapped back: rejected

    def _delta(self, fs: FlowSnapshot, base: dict, medium: list, longw: list) -> None:
        """Cumulative delta = sum(buy aggressor volume) - sum(sell aggressor volume) this session. Only
        reported as reliable when the feed classifies the aggressor and < delta_unknown_max of the session's
        volume is unclassified. slope = delta change over the medium window / baseline volume of that window;
        accel = slope of the last half minus the first half. Divergence over the long window: price makes a
        new high (low) by >= divergence_ticks in the last medium window while the delta's high (low) stays at
        least divergence_volume x the window's baseline volume below (above) its earlier one -> -1 (+1)."""
        tot = self.cum_classified + self.cum_unknown
        fs.delta_reliable = self.aggressor_reliable and tot > 0 and (self.cum_unknown / tot) < self.cfg.delta_unknown_max
        fs.cum_delta = self.cum_delta
        if not fs.delta_reliable:
            return
        bvol = max(base["vol"] * self.cfg.medium_s, 1.0)
        d_med = sum(s.buy - s.sell for s in medium)
        fs.delta_slope = d_med / bvol
        half = len(medium) // 2
        recent, older = medium[:half], medium[half:]
        fs.delta_accel = (sum(s.buy - s.sell for s in recent) - sum(s.buy - s.sell for s in older)) / bvol
        closed = [s for s in reversed(longw) if s.closed and s.mid is not None]
        if len(closed) < self.cfg.medium_s + 10:
            return
        early, late = closed[:-self.cfg.medium_s], closed[-self.cfg.medium_s:]
        tick = self.tick
        need = self.cfg.divergence_ticks * tick
        gap = self.cfg.divergence_volume * bvol
        if max(s.mid for s in late) >= max(s.mid for s in early) + need - 1e-12 and \
                max(s.cum_delta for s in late) <= max(s.cum_delta for s in early) - gap:
            fs.delta_divergence = -1
        elif min(s.mid for s in late) <= min(s.mid for s in early) - need + 1e-12 and \
                min(s.cum_delta for s in late) >= min(s.cum_delta for s in early) + gap:
            fs.delta_divergence = 1

    def _footprint(self, fs: FlowSnapshot, trades_med: list) -> None:
        """Footprint over the medium window: at each price, volume bought at the offer (ask_vol) and sold at
        the bid (bid_vol). Diagonal buy imbalance at p: ask_vol(p) >= ratio x bid_vol(p - 1 tick) and
        ask_vol(p) >= min volume (the larger of footprint_min_volume and footprint_volume_share x the window's
        baseline volume); sell imbalance at p: bid_vol(p) >= ratio x ask_vol(p + 1 tick). Stacked:
        stacked_levels consecutive prices with the same imbalance. footprint_signal: +1 stacked buying,
        -1 stacked selling, else (buys - sells) / (buys + sells) of the imbalance counts x 0.25."""
        cfg = self.cfg
        base = self.baseline() or {"vol": 0.0}
        min_vol = max(cfg.footprint_min_volume, cfg.footprint_volume_share * base["vol"] * cfg.medium_s)
        ask_v: dict[int, float] = {}
        bid_v: dict[int, float] = {}
        for t in trades_med:
            k = self._tick_index(t[1])
            if t[3] == BUY:
                ask_v[k] = ask_v.get(k, 0.0) + t[2]
            elif t[3] == SELL:
                bid_v[k] = bid_v.get(k, 0.0) + t[2]
        buys = sorted(k for k, v in ask_v.items() if v >= min_vol
                      and v >= cfg.footprint_ratio * bid_v.get(k - 1, 0.0))
        sells = sorted(k for k, v in bid_v.items() if v >= min_vol
                       and v >= cfg.footprint_ratio * ask_v.get(k + 1, 0.0))

        def stacked(keys: list[int]) -> bool:
            run = 1
            for a, b2 in zip(keys, keys[1:]):
                run = run + 1 if b2 == a + 1 else 1
                if run >= cfg.stacked_levels:
                    return True
            return cfg.stacked_levels <= 1 and bool(keys)

        fs.buy_imbalances, fs.sell_imbalances = len(buys), len(sells)
        fs.stacked_buy, fs.stacked_sell = stacked(buys), stacked(sells)
        if fs.stacked_buy and not fs.stacked_sell:
            fs.footprint_signal = 1.0
        elif fs.stacked_sell and not fs.stacked_buy:
            fs.footprint_signal = -1.0
        elif buys or sells:
            fs.footprint_signal = 0.25 * (len(buys) - len(sells)) / (len(buys) + len(sells))
        keys = sorted(set(ask_v) | set(bid_v))
        fs.delta_by_level = {f"{px(k * self.tick)}": ask_v.get(k, 0.0) - bid_v.get(k, 0.0) for k in keys[-25:]}

    def _profile(self, fs: FlowSnapshot, trades_med: list) -> None:
        """Session volume profile: volume by price; POC = the price with the most volume; value area = the
        smallest range around the POC holding value_area of the volume (grown one price at a time towards the
        side with more volume); HVN / LVN = local maxima >= 1.5x / minima <= 0.5x the mean of a 3-price smoothed
        profile. Acceptance = share of the medium window's volume traded beyond the value area on the side the
        price is on. Signal: above value and accepted (>= 60%) +0.5, below and accepted -0.5; outside but not
        accepted (< 30%) = a failed auction back towards value (-0.3 above, +0.3 below)."""
        prof = self.profile
        total = sum(prof.values())
        if total < self.cfg.profile_min_volume or not prof:
            return
        tick = self.tick
        keys = sorted(prof)
        poc = max(keys, key=lambda k: (prof[k], -k))
        lo = hi = poc
        acc = prof[poc]
        idx = {k: i for i, k in enumerate(keys)}
        while acc < self.cfg.value_area * total:
            i_lo, i_hi = idx[lo], idx[hi]
            down = prof[keys[i_lo - 1]] if i_lo > 0 else -1.0
            up = prof[keys[i_hi + 1]] if i_hi + 1 < len(keys) else -1.0
            if up < 0 and down < 0:
                break
            if up >= down:
                hi = keys[i_hi + 1]
                acc += up
            else:
                lo = keys[i_lo - 1]
                acc += down
        fs.poc, fs.val, fs.vah = px(poc * tick), px(lo * tick), px(hi * tick)
        full = list(range(keys[0], keys[-1] + 1))
        vals = [prof.get(k, 0.0) for k in full]
        sm = [(vals[max(0, i - 1)] + vals[i] + vals[min(len(vals) - 1, i + 1)]) / 3.0 for i in range(len(vals))]
        mean = sum(sm) / len(sm) if sm else 0.0
        hv, lv = [], []
        for i in range(1, len(sm) - 1):
            if sm[i] >= sm[i - 1] and sm[i] >= sm[i + 1] and sm[i] >= 1.5 * mean:
                hv.append(px(full[i] * tick))
            if sm[i] <= sm[i - 1] and sm[i] <= sm[i + 1] and sm[i] <= 0.5 * mean:
                lv.append(px(full[i] * tick))
        near = lambda lst: sorted(lst, key=lambda p: abs(p - fs.mid))[:3]
        fs.hvn, fs.lvn = near(hv), near(lv)
        vol_m = sum(t[2] for t in trades_med)
        if fs.mid > fs.vah + 1e-12:
            fs.profile_location = "ABOVE VALUE"
            fs.profile_acceptance = sum(t[2] for t in trades_med if t[1] > fs.vah + 1e-12) / vol_m if vol_m else 0.0
            fs.profile_signal = 0.5 if fs.profile_acceptance >= 0.6 else (-0.3 if fs.profile_acceptance < 0.3 else 0.0)
        elif fs.mid < fs.val - 1e-12:
            fs.profile_location = "BELOW VALUE"
            fs.profile_acceptance = sum(t[2] for t in trades_med if t[1] < fs.val - 1e-12) / vol_m if vol_m else 0.0
            fs.profile_signal = -0.5 if fs.profile_acceptance >= 0.6 else (0.3 if fs.profile_acceptance < 0.3 else 0.0)
        else:
            fs.profile_location = "IN VALUE"

    def _vwap(self, fs: FlowSnapshot, medium: list, longw: list, trades_med: list) -> None:
        """Session VWAP = sum(price x size) / sum(size). distance = (mid - VWAP) in ticks; slope = VWAP change
        over the long window in ticks per minute; acceptance = share of the medium window's volume traded on
        the same side of the VWAP as the mid is now. vwap_signal = sign(distance) x min(1, |distance| / 5) x
        acceptance, halved when the slope disagrees; 0 within one tick."""
        if self.v <= 0:
            return
        tick = self.tick
        fs.vwap = self.pv / self.v
        fs.vwap_dist_ticks = (fs.mid - fs.vwap) / tick
        old = next((s for s in reversed(longw) if s.closed and s.vwap), None)
        mins = max(len(longw), 1) / 60.0
        fs.vwap_slope = ((fs.vwap - old.vwap) / tick) / mins if old is not None else 0.0
        vol_m = sum(t[2] for t in trades_med)
        if vol_m > 0:
            same = sum(t[2] for t in trades_med if _sign(t[1] - fs.vwap) == _sign(fs.vwap_dist_ticks))
            fs.vwap_acceptance = same / vol_m
        if abs(fs.vwap_dist_ticks) >= 1.0:
            sig = _sign(fs.vwap_dist_ticks) * min(1.0, abs(fs.vwap_dist_ticks) / 5.0) * fs.vwap_acceptance
            if _sign(fs.vwap_slope) != _sign(fs.vwap_dist_ticks):
                sig *= 0.5
            fs.vwap_signal = _clip(sig)

    def _labels(self, fs: FlowSnapshot) -> None:
        L = fs.labels
        if fs.absorption_score >= 0.6:
            who = "sellers absorbing aggressive buyers" if fs.absorption_dir < 0 else "buyers absorbing aggressive sellers"
            L.append(f"ABSORPTION: {who} (score {fs.absorption_score:.2f})")
        if fs.sweep_dir and fs.sweep_age_s is not None and fs.sweep_age_s <= self.cfg.medium_s:
            L.append(f"SWEEP {'UP' if fs.sweep_dir > 0 else 'DOWN'}: {fs.sweep_levels} levels, "
                     f"{fs.sweep_volume:g} lots, {fs.sweep_age_s:.0f} s ago")
        if fs.pull_ask >= 0.8:
            L.append("OFFERS PULLED ahead of price")
        if fs.pull_bid >= 0.8:
            L.append("BIDS PULLED below price")
        for r in fs.refresh_levels[:2]:
            L.append(f"POSSIBLE REFRESH / ABSORPTION at {r['price']} ({'bid' if r['side'] == BID else 'offer'}): "
                     f"{r['executed']:g} traded vs {r['max_visible']:g} ever shown")
        for e in fs.wall_events[-2:]:
            L.append(f"{'OFFER' if e['side'] == ASK else 'BID'} WALL {e['status']} at {e['price']}")
        if fs.stacked_buy:
            L.append("STACKED BUY IMBALANCE")
        if fs.stacked_sell:
            L.append("STACKED SELL IMBALANCE")
        if fs.delta_divergence:
            L.append("DELTA DIVERGENCE " + ("(bullish)" if fs.delta_divergence > 0 else "(bearish)"))

    # -------------------------------------------------------------- ladder --
    def ladder(self, fs: Optional[FlowSnapshot] = None, n: int = 10) -> dict:
        """A small price ladder for the page: per price the resting ask and bid size, the volume bought and
        sold there in the medium window, and the important liquidity flags."""
        b = self.book
        tick = self.tick
        bids = dict(b.levels(BID, n))
        asks = dict(b.levels(ASK, n))
        buy_v: dict[float, float] = {}
        sell_v: dict[float, float] = {}
        for t in self.trades:
            if t[3] == BUY:
                buy_v[t[1]] = buy_v.get(t[1], 0.0) + t[2]
            elif t[3] == SELL:
                sell_v[t[1]] = sell_v.get(t[1], 0.0) + t[2]
        walls = {(w.side, w.price) for w in self.walls.values()}
        refresh = {(r["side"], r["price"]) for r in (fs.refresh_levels if fs else [])}
        prices = sorted(set(bids) | set(asks), reverse=True)
        rows = []
        for p in prices:
            flags = []
            if (BID, p) in walls or (ASK, p) in walls:
                flags.append("WALL")
            if (BID, p) in refresh or (ASK, p) in refresh:
                flags.append("POSSIBLE REFRESH / ABSORPTION")
            if fs is not None:
                if fs.poc is not None and abs(p - fs.poc) < tick / 2:
                    flags.append("POC")
                if fs.vwap is not None and abs(p - fs.vwap) < tick / 2:
                    flags.append("VWAP")
                if any(abs(p - h) < tick / 2 for h in fs.hvn):
                    flags.append("HVN")
                if any(abs(p - h) < tick / 2 for h in fs.lvn):
                    flags.append("LVN")
            bv, sv = buy_v.get(p, 0.0), sell_v.get(p, 0.0)
            rows.append({"price": p, "ask": asks.get(p, 0.0), "bid": bids.get(p, 0.0), "bought": bv, "sold": sv,
                         "delta": bv - sv, "flags": flags})
        pressure = "balanced"
        if fs is not None and fs.ok:
            score = 0.5 * fs.imbalance_avg + 0.5 * fs.aggr_medium
            if score > 0.2:
                pressure = f"buyers (book {fs.imbalance_avg:+.2f}, aggression {fs.aggr_medium:+.2f})"
            elif score < -0.2:
                pressure = f"sellers (book {fs.imbalance_avg:+.2f}, aggression {fs.aggr_medium:+.2f})"
        return {"instrument": self.instrument, "data_class": "INSTITUTIONAL", "tick": tick,
                "bid": b.best_bid(), "ask": b.best_ask(), "mid": b.mid(),
                "imbalance": round(fs.imbalance, 3) if fs is not None else None, "pressure": pressure, "rows": rows}
