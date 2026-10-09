"""Replay: RiderCore driven tick by tick over stored ticks, exactly as live.

The live engine's order each pass (once a second of market time), copied:

    feed every new tick (on_tick)  ->  manage every open position with
    FlowLock X (stop moves and exits go to the "broker")  ->  scan(now)  ->
    entries(now) when the health checks allow (fresh prices; inside the day)

The "broker" here is honest about execution:

* an entry is sent at the pass that produced it and FILLS at the first tick
  at or after ``signal + latency``, at the ASK for a buy / the BID for a
  sell, plus slippage (the spread is paid);
* the stop sits at the broker from the fill: EVERY tick is checked against
  it, and it fills at the stop or worse (a gap fills at the gapped price,
  plus slippage);
* a stop move is sent only when the live engine's gate says so
  (``RiderCore.stop_move_due``: a minimum step and a minimum time between
  moves), is effective only after the round trip, and only if it is still
  on the right side of the market then (a refused move is not committed and
  FlowLock X tries again after the same wait, as live);
* an exit FlowLock X decides on fills like an entry: first tick after the
  delay, at the bid/ask, plus slippage;
* commission per lot per side, round trip, from the core's own conversion;
* a fill at or through the planned stop is closed at once (as live).

No look-ahead: ticks are fed strictly in time order and only ticks at or
before ``now`` exist when scan/manage run at ``now``. The calendar is
refreshed every few minutes like live, with the ACTUAL figure removed from
every event that has not been released yet.

Each UTC day is replayed on its own: a fresh RiderCore is seeded with the
one- and five-minute bars of the previous stored days (live seeds from the
broker's bars), fed the last ``warm_s`` seconds before midnight, and opens
trades only inside the day. A position still open at midnight is followed
into the next day's ticks until it closes. The bot's own learning
(historical match, 2 points of 100) stays neutral in replay.

Cost stress (``run_fixed``): the SAME entry signals as the normal run,
filled and managed with the spread and slippage multiplied (or a longer
delay). FlowLock X manages each position exactly as in the normal run; the
cross-market confirmation it reads comes from the normal run's scans (the
mid prices are identical under stress, so it is the same reading).
"""
from __future__ import annotations

import datetime as dt
import json
import math
from array import array
from collections import Counter
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Optional, Sequence

from ...broker.base import Bar, Side, TF, Tick
from ..bars import BarSeries
from ..config import RiderConfig
from ..core import EntryIntent, RiderCore, RiderPosition
from ..engine import RiderEngine
from ..flowlock_x import FlowState
from ..learning import BucketStats
from ..strength import PairMove, StrengthMap, compute_strength
from .costs import CostModel, trade_money
from .tickstore import DAY_MS, DayTicks, TickStore, add_days, day_start_ms, merged, ms_to_dt

UTC = dt.timezone.utc
SEED_LOOKBACK_DAYS = 4
SEED_DAYS = 2


# ------------------------------------------------------------- day input --

def _bars_from_ticks(ticks: DayTicks, seconds: int) -> list[Bar]:
    bs = BarSeries(seconds, keep=10_000)
    for t, b, a in zip(ticks.times, ticks.bids, ticks.asks):
        bs.add(t / 1000.0, 0.5 * (b + a), a - b)
    end = (ticks.times[-1] / 1000.0 + seconds) if len(ticks) else 0.0
    return bs.closed(end + seconds)


def _bar_row(b: Bar) -> list:
    return [b.time.timestamp(), b.open, b.high, b.low, b.close, b.tick_volume, b.spread_points]


def _row_bar(r: list) -> Bar:
    return Bar(dt.datetime.fromtimestamp(r[0], UTC), r[1], r[2], r[3], r[4], r[5], 0.0, r[6])


def day_bars(store: TickStore, symbol: str, day: str, cache_dir: Optional[Path] = None) -> tuple[list[Bar], list[Bar]]:
    """The one- and five-minute MID bars of one stored day (cached)."""
    path = store.day_path(symbol, day)
    if not path.exists():
        return [], []
    st = path.stat()
    cpath = None
    if cache_dir is not None:
        cpath = Path(cache_dir) / "bars" / symbol / f"{day}.json"
        try:
            c = json.loads(cpath.read_text())
            if c.get("size") == st.st_size and c.get("mtime") == int(st.st_mtime):
                return [_row_bar(r) for r in c["m1"]], [_row_bar(r) for r in c["m5"]]
        except Exception:
            pass
    ticks = store.load(symbol, day)
    m1, m5 = _bars_from_ticks(ticks, 60), _bars_from_ticks(ticks, 300)
    if cpath is not None:
        cpath.parent.mkdir(parents=True, exist_ok=True)
        tmp = cpath.with_name(cpath.name + ".tmp")
        tmp.write_text(json.dumps({"size": st.st_size, "mtime": int(st.st_mtime),
                                   "m1": [_bar_row(b) for b in m1], "m5": [_bar_row(b) for b in m5]}))
        tmp.replace(cpath)
    return m1, m5


def seed_days(store: TickStore, symbols: Sequence[str], day: str) -> list[str]:
    """The stored days (at most SEED_DAYS) within SEED_LOOKBACK_DAYS before ``day``."""
    out = []
    for k in range(1, SEED_LOOKBACK_DAYS + 1):
        d = add_days(day, -k)
        if any(store.day_path(s, d).exists() for s in symbols):
            out.append(d)
        if len(out) >= SEED_DAYS:
            break
    return sorted(out)


@dataclass
class DayData:
    day: str
    symbols: list
    specs: dict
    calendar: list
    seed_ms: int
    start_ms: int
    end_ms: int
    stop_ms: int
    ticks: dict
    seed_bars: dict
    account_currency: str = "GBP"
    notes: list = field(default_factory=list)

    def tick_count(self) -> int:
        return sum(len(t) for t in self.ticks.values())


def load_day(store: TickStore, day: str, symbols: Sequence[str], specs: dict, calendar: Sequence = (), *,
             warm_s: int = 600, tail_s: int = 4 * 3600, cache_dir: Optional[Path] = None,
             account_currency: str = "GBP") -> DayData:
    d0 = day_start_ms(day)
    seed_ms = d0 - warm_s * 1000
    end_ms = d0 + DAY_MS
    stop_ms = end_ms + tail_s * 1000
    prev = seed_days(store, symbols, day)
    notes = []
    if not prev:
        notes.append(f"{day}: no stored day before it - the bars warm up from this day's own ticks")
    ticks: dict[str, DayTicks] = {}
    seeds: dict[str, tuple[list, list]] = {}
    for s in symbols:
        if s not in specs:
            continue
        t = store.load_span(s, seed_ms, stop_ms)
        if not len(t):
            notes.append(f"{s} {day}: no ticks")
            continue
        ticks[s] = t
        m1: list[Bar] = []
        m5: list[Bar] = []
        for p in prev:
            a, b = day_bars(store, s, p, cache_dir)
            m1 += a
            m5 += b
        cut = seed_ms / 1000.0
        seeds[s] = ([b for b in m1 if b.time.timestamp() + 60 <= cut][-400:],
                    [b for b in m5 if b.time.timestamp() + 300 <= cut][-400:])
    return DayData(day, sorted(ticks), dict(specs), list(calendar), seed_ms, d0, end_ms, stop_ms, ticks, seeds,
                   account_currency, notes)


# ----------------------------------------------------------- strengths --

class ZRecord:
    """The normal run's cross-market readings, one per pass (compact), so a
    stress run can hand FlowLock X exactly the same confirmation."""

    def __init__(self, symbols: Sequence[str]):
        self.symbols = list(symbols)
        self.cols = {s: array("d") for s in self.symbols}
        self.meta: dict[str, tuple[str, str]] = {}

    def put(self, k: int, sm: StrengthMap) -> None:
        nan = float("nan")
        for s in self.symbols:
            col = self.cols[s]
            if len(col) < k:
                col.extend([nan] * (k - len(col)))
            m = sm.moves.get(s)
            val = m.z if m is not None else nan
            if len(col) == k:
                col.append(val)
            else:
                col[k] = val
            if m is not None and s not in self.meta:
                self.meta[s] = (m.base, m.quote)

    def get(self, k: int) -> StrengthMap:
        moves = []
        for s in self.symbols:
            col = self.cols[s]
            if k < len(col) and not math.isnan(col[k]) and s in self.meta:
                b, q = self.meta[s]
                moves.append(PairMove(s, b, q, col[k]))
        return compute_strength(moves) if moves else StrengthMap()


def calendar_at(events: Sequence, now_ms: int, cfg: RiderConfig) -> list:
    """What the MetaTrader calendar would say at ``now``: the window the live
    engine asks for, with no ACTUAL yet on events not released by then."""
    lo = now_ms - int((cfg.news_window_minutes + 5) * 60_000)
    hi = now_ms + 12 * 3_600_000
    out = []
    for e in events:
        t = int(e.time_utc.timestamp() * 1000)
        if lo <= t <= hi:
            out.append(e if t <= now_ms else replace(e, actual=None))
    return out


def make_core(cfg: RiderConfig, data: DayData) -> RiderCore:
    """A fresh RiderCore for one day: specs, account currency and the seed
    bars (what live gets from the broker's bars at start-up). Its learning
    is empty, so the historical-match category stays neutral."""
    core = RiderCore(cfg, BucketStats(cfg.learning_min_samples, cfg.learning_window_trades, cfg.learning_min_days))
    core.set_account_currency(data.account_currency)
    seed_dt = ms_to_dt(data.seed_ms)
    for s in data.symbols:
        core.set_spec(s, data.specs[s])
        m1, m5 = data.seed_bars.get(s, ([], []))
        if m1:
            core.seed_bars(s, TF.M1, m1, seed_dt)
        if m5:
            core.seed_bars(s, TF.M5, m5, seed_dt)
    return core


# ---------------------------------------------------------------- replay --

@dataclass
class _Open:
    pos: RiderPosition
    intent: EntryIntent
    signal_ms: int
    fill_ms: int
    volume: float
    gbp_pp: float
    comm_side: float
    spread_at_fill: float
    best: float = 0.0
    worst: float = 0.0
    pending_exit: Optional[tuple] = None          # (due_ms, reason, why)
    pending_stop: Optional[tuple] = None          # (due_ms, stop, state, sent_ms)
    stop_refusals: int = 0


@dataclass
class ReplayOutput:
    trades: list
    intents: list
    zrec: Optional[ZRecord]
    counters: dict
    notes: list


class DayReplay:
    def __init__(self, cfg: RiderConfig, data: DayData, cost: CostModel, variant: str = "", label: str = ""):
        self.cfg = cfg
        self.data = data
        self.cost = cost
        self.variant = variant
        self.label = label or cost.label
        self.pass_ms = max(1, int(round(cfg.scan_interval_seconds * 1000)))
        self.lat_ms = max(0, int(round(cost.latency_ms)))
        self.stale_ms = int(cfg.stale_tick_seconds * 1000)

    # ---------------------------------------------------------- set-up --
    def _core(self) -> RiderCore:
        return make_core(self.cfg, self.data)

    def _calendar(self, now_ms: int) -> list:
        return calendar_at(self.data.calendar, now_ms, self.cfg)

    # ------------------------------------------------------------- runs --
    def run_full(self) -> ReplayOutput:
        return self._run(None, None)

    def run_fixed(self, intents: Sequence[EntryIntent], zrec: ZRecord) -> ReplayOutput:
        by_ms: dict[int, list] = {}
        for it in intents:
            by_ms.setdefault(int(round(it.signal_time.timestamp() * 1000)), []).append(it)
        return self._run(by_ms, zrec)

    def _run(self, fixed: Optional[dict], zrec_in: Optional[ZRecord]) -> ReplayOutput:
        D = self.data
        cfg = self.cfg
        core = self._core()
        full = fixed is None
        zrec = ZRecord(D.symbols) if full else zrec_in
        self.core = core
        self.open: dict[str, _Open] = {}
        self.pending: dict[str, tuple] = {}               # symbol -> (intent, due_ms, volume, gbp_pp, signal_ms)
        self.latest: dict[str, Tick] = {}
        self.trades: list[dict] = []
        self.counters = Counter()
        self.notes: list[str] = list(D.notes)
        self._ticket = 0
        intents_all: list[EntryIntent] = []
        order = D.symbols
        stream = merged(D.ticks, order)
        nxt = next(stream, None)
        newest = None
        now = D.seed_ms
        cal_at = None
        cal_every = int(cfg.calendar_refresh_seconds * 1000)
        while True:
            # 1. every tick up to now (broker stops, fills and stop moves happen tick by tick)
            while nxt is not None and nxt[0] <= now:
                t_ms, k, b, a = nxt
                self._on_tick(order[k], t_ms, b, a)
                newest = t_ms
                nxt = next(stream, None)
            self._resolve(now)
            stale = newest is None or now - newest > self.stale_ms
            if not stale:
                self.counters["passes"] += 1
                if cal_at is None or now - cal_at >= cal_every:
                    core.set_calendar(self._calendar(now))
                    cal_at = now
                kpass = (now - D.seed_ms) // self.pass_ms
                now_dt = ms_to_dt(now)
                # 2. manage every open position with FlowLock X
                if full:
                    zrec.put(kpass, core.strength)
                elif self.open:
                    core.strength = zrec.get(kpass) if zrec is not None else StrengthMap()
                self._manage(now, now_dt)
                self._resolve(now)
                # 3. scan, 4. entries
                inside = D.start_ms <= now < D.end_ms
                if full:
                    core.scan(now_dt)
                    self.counters["scans"] += 1
                    if inside:
                        intents = core.entries(now_dt)
                        for it in intents:
                            intents_all.append(it)
                            self._submit(it, now)
                elif inside and now in fixed:
                    for it in fixed[now]:
                        self._submit(it, now)
                self._resolve(now)
            else:
                self.counters["stale_passes"] += 1
            # termination
            if now >= D.end_ms and not self.open and not self.pending:
                break
            if nxt is None and (stale or now > D.stop_ms):
                self._end_of_data(newest)
                break
            now += self.pass_ms
            if stale and nxt is not None and nxt[0] > now:
                steps = -((now - nxt[0]) // self.pass_ms)       # jump the dead time on the pass grid
                now += steps * self.pass_ms
        self.counters["intents"] = len(intents_all) if full else sum(len(v) for v in fixed.values())
        self.counters["trades"] = len(self.trades)
        return ReplayOutput(self.trades, intents_all if full else [], zrec if full else None, dict(self.counters),
                            self.notes)

    # ------------------------------------------------------------ ticks --
    def _pip(self, sym: str) -> float:
        return self.core.syms[sym].pip

    def _on_tick(self, sym: str, t_ms: int, bid: float, ask: float) -> None:
        tk = self.cost.tick(sym, ms_to_dt(t_ms), bid, ask)
        if not self.core.on_tick(sym, tk):
            return
        self.latest[sym] = tk
        o = self.open.get(sym)
        if o is not None and o.pending_stop is not None and o.pending_stop[0] <= t_ms:
            self._apply_stop(o, tk)
        pe = self.pending.get(sym)
        if pe is not None and pe[1] <= t_ms:
            self._fill(sym, tk, t_ms)
        o = self.open.get(sym)
        if o is not None:
            self._mark(o, tk)
            side = o.pos.side
            if self.cost.stop_touched(side, o.pos.flow.stop, tk):
                self._close(o, self.cost.stop_fill(side, o.pos.flow.stop, tk, o.pos.pip), t_ms, "STOP",
                            "the stop at the broker was reached")
            elif o.pending_exit is not None and o.pending_exit[0] <= t_ms:
                self._close(o, self.cost.exit_fill(side, tk, o.pos.pip), t_ms, o.pending_exit[1], o.pending_exit[2])

    def _mark(self, o: _Open, tk: Tick) -> None:
        d = o.pos.side.sign
        mark = tk.bid if d > 0 else tk.ask
        fav = (mark - o.pos.entry) * d
        if fav > o.best:
            o.best = fav
        if -fav > o.worst:
            o.worst = -fav

    def _resolve(self, now: int) -> None:
        """Stop moves whose round trip has completed by ``now`` meet the
        latest price (the broker refuses a stop already through it). Entries
        and exits fill only on a tick: the first one at or after the delay."""
        for sym in sorted(self.open):
            o = self.open[sym]
            tk = self.latest.get(sym)
            if tk is not None and o.pending_stop is not None and o.pending_stop[0] <= now:
                self._apply_stop(o, tk)

    # ---------------------------------------------------------- entries --
    def _submit(self, it: EntryIntent, now: int) -> None:
        sym = it.symbol
        c = self.counters
        if sym in self.open or sym in self.pending:
            c["skipped_busy"] += 1
            return
        if len(self.open) + len(self.pending) >= self.cfg.max_concurrent_positions:
            c["skipped_cap"] += 1
            return
        volume, gpp, why = self.core.size(sym)
        if volume <= 0:
            c["skipped_no_size"] += 1
            if len(self.notes) < 50:
                self.notes.append(f"{sym}: no size - {why}")
            return
        tk = self.latest.get(sym)
        if tk is None or ((tk.ask if it.side is Side.BUY else tk.bid) - it.stop) * it.side.sign <= 0:
            c["skipped_through_stop"] += 1
            return
        self.pending[sym] = (it, now + self.lat_ms, volume, gpp, now)

    def _fill(self, sym: str, tk: Tick, t_ms: int) -> None:
        it, _due, volume, gpp, signal_ms = self.pending.pop(sym)
        pip = self._pip(sym)
        price = self.cost.entry_fill(it.side, tk, pip)
        comm = self.core.commission_account_per_lot_side()
        if comm is None:
            comm = 0.0
            self.counters["commission_unknown"] += 1
        self._ticket += 1
        if (price - it.stop) * it.side.sign <= 0:
            # filled at or through the planned stop: closed at once, as live (the core never holds it)
            flow = FlowState(it.side.sign, price, it.stop, it.stop, abs(price - it.stop), it.cost_price, t_ms / 1000.0)
            pos = RiderPosition(self._ticket, sym, it.side, volume, price, ms_to_dt(t_ms), flow, it.family,
                                it.score, it.reason, {}, it.trigger_id, pip, gpp, "REPLAY", it.signal_time)
            o = _Open(pos, it, signal_ms, t_ms, volume, gpp, comm, tk.ask - tk.bid)
            self._record(o, self.cost.exit_fill(it.side, tk, pip), t_ms, "FILL_THROUGH_STOP",
                         "filled at or through the planned stop - closed at once", core_held=False)
            return
        pos = self.core.open_position(it, price, volume, self._ticket, ms_to_dt(t_ms), "REPLAY", gpp)
        self.open[sym] = _Open(pos, it, signal_ms, t_ms, volume, gpp, comm, tk.ask - tk.bid)

    # -------------------------------------------------------- management --
    def _manage(self, now: int, now_dt: dt.datetime) -> None:
        for sym in sorted(self.open):
            o = self.open[sym]
            if o.pending_exit is not None:
                continue
            tk = self.latest.get(sym)
            if tk is None:
                continue
            d = self.core.manage(o.pos, tk, now_dt)
            if d.exit:
                if d.exit_reason == "STOP":
                    self._close(o, self.cost.stop_fill(o.pos.side, o.pos.flow.stop, tk, o.pos.pip), now, "STOP",
                                d.why)
                else:
                    o.pending_exit = (now + self.lat_ms, d.exit_reason, d.why)
                continue
            if d.new_stop is not None and o.pending_stop is None:
                new = RiderEngine._snap(self.data.specs.get(sym), d.new_stop, o.pos.side)
                if self.core.stop_move_due(o.pos, new, tk, now_dt):       # the live engine's own gate
                    self.core.stop_move_sent(o.pos, now_dt)
                    o.pending_stop = (now + self.lat_ms, new, d.state, now)

    def _apply_stop(self, o: _Open, tk: Tick) -> None:
        _due, new, _state, sent_ms = o.pending_stop
        o.pending_stop = None
        ok = new < tk.bid if o.pos.side is Side.BUY else new > tk.ask
        if ok:
            self.core.confirm_stop(o.pos, new, ms_to_dt(sent_ms))
            self.counters["stop_moves"] += 1
        else:
            o.stop_refusals += 1                         # the broker would refuse it: nothing is committed
            self.counters["stop_refused"] += 1

    # ----------------------------------------------------------- exits --
    def _close(self, o: _Open, price: float, t_ms: int, reason: str, detail: str) -> None:
        self._record(o, price, t_ms, reason, detail, core_held=True)

    def _record(self, o: _Open, price: float, t_ms: int, reason: str, detail: str, core_held: bool) -> None:
        pos, it = o.pos, o.intent
        pip = pos.pip
        side = pos.side
        m = trade_money(side, pos.entry, price, pip, o.gbp_pp, o.volume, o.comm_side)
        mfe = o.best / pip if pip else 0.0
        mae = o.worst / pip if pip else 0.0
        stop_pips = abs(pos.entry - pos.flow.initial_stop) / pip if pip else 0.0
        snap = it.snapshot or {}
        self.trades.append({
            "variant": self.variant, "cost": self.label, "day": self.data.day, "symbol": pos.symbol,
            "side": "LONG" if side is Side.BUY else "SHORT", "family": it.family, "score": round(it.score, 1),
            "entry_reason": it.reason, "signal_ms": o.signal_ms, "fill_ms": o.fill_ms, "exit_ms": t_ms,
            "latency_ms": o.fill_ms - o.signal_ms, "duration_s": round((t_ms - o.fill_ms) / 1000.0, 1),
            "reference_price": round(it.reference_price, 7), "entry": round(pos.entry, 7),
            "initial_stop": round(pos.flow.initial_stop, 7), "final_stop": round(pos.flow.stop, 7),
            "exit_price": round(price, 7), "stop_pips": round(stop_pips, 2),
            "volume": o.volume, "gbp_per_pip": round(o.gbp_pp, 4),
            "gross_pips": round(m["gross_pips"], 3), "commission_gbp": round(m["commission_gbp"], 4),
            "net_money": round(m["net_money"], 4), "net_pips": round(m["net_pips"], 3),
            "r_multiple": round(m["gross_pips"] / stop_pips, 3) if stop_pips > 0 else None,
            "mfe_pips": round(mfe, 2), "mae_pips": round(mae, 2),
            "capture": round(m["gross_pips"] / mfe, 3) if mfe > 0 else None,
            "exit_reason": reason, "exit_detail": detail, "exit_state": pos.flow.state,
            "flow_states": list(pos.flow.visited), "stop_moves": pos.flow.stop_moves,
            "stop_refusals": o.stop_refusals,
            "entry_slip_pips": round((pos.entry - it.reference_price) * side.sign / pip, 3) if pip else 0.0,
            "spread_pips_at_fill": round(o.spread_at_fill / pip, 3) if pip else 0.0,
            "cost_ratio": round(it.cost_ratio, 4) if it.cost_ratio != float("inf") else None,
            "efficiency": round(it.efficiency, 3), "expected_move_pips": round(it.expected_move / pip, 2) if pip else 0.0,
            "categories": snap.get("categories", {}),
        })
        if core_held:
            # the trade's money after every cost feeds the loss guards, exactly as the live engine's _finalise
            self.core.position_closed(pos, ms_to_dt(t_ms), m["net_money"])
            self.open.pop(pos.symbol, None)

    def _end_of_data(self, newest: Optional[int]) -> None:
        for sym in sorted(self.open):
            o = self.open[sym]
            tk = self.latest.get(sym)
            if tk is None:
                continue
            self._close(o, self.cost.exit_fill(o.pos.side, tk, o.pos.pip), int(tk.time.timestamp() * 1000),
                        "END_OF_DATA", "the stored ticks ended with the trade still open - closed at the last price")
        if self.pending:
            self.counters["unfilled_at_end"] += len(self.pending)
            self.pending.clear()


def replay_day(cfg: RiderConfig, data: DayData, cost: CostModel, variant: str = "",
               stresses: Sequence[CostModel] = ()) -> dict:
    """The normal run plus each stress (same signals). Returns
    {cost label: {"trades": [...], "counters": {...}}, "_notes": [...]}"""
    base = DayReplay(cfg, data, cost, variant)
    out_full = base.run_full()
    res = {cost.label: {"trades": out_full.trades, "counters": out_full.counters}}
    for sc in stresses:
        r = DayReplay(cfg, data, sc, variant).run_fixed(out_full.intents, out_full.zrec)
        res[sc.label] = {"trades": r.trades, "counters": r.counters}
    res["_notes"] = out_full.notes[:50]
    return res
