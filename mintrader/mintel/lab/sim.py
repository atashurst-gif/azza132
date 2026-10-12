"""The simulator: the ONE place a lab trade is filled and costed.

Past tests, fresh tests and practice all use these functions, so a number from
one can be compared with a number from another. It is pessimistic by
construction. Nothing here can place an order; it works on arrays of bars.

How a trade is made (spec section 3):

1. The signal is known at the CLOSE of bar i. The entry is at bar i+1's open:
   a BUY pays ``open + spread + slip``, a SELL gets ``open - slip``, where
   ``slip = max(1 point, 0.25 x spread)``. Bars are bid prices, so a SELL's
   stop, target and exits are tested on the ASK (bid + spread).
2. ``spread = max(the bar's recorded spread, that symbol's median for that UTC
   hour over the past stretch)``. A bar that recorded none (0) takes the
   default for its kind (15 points FX, 30 gold, 10 others) and the trade is
   flagged ``spread_assumed``.
3. Skipped, and counted: a spread over 3x the symbol-day median; spread plus
   commission over 20% of the stop distance; a stop closer than the broker
   allows; a size below the smallest the broker offers.
4. Size = risk in pounds / (stop distance x pounds per point per lot), floored
   to the lot step. Never rounded up: the same as the live engine.
5. On every bar from the entry bar on, THE STOP IS TESTED FIRST. A BUY is stopped
   when low <= stop (filled at stop - slip, or at the open if it opened through
   it). A target is a fill only when the extreme goes through it by at least a
   point (touching is not enough) and has no slippage. A trail follows the peak
   of the PREVIOUS bars. Time and session-end exits are at the bar's close, with
   the spread (sells) and slip against us.
6. Everything is flat by 21:55 UK (21:00 UK on Fridays), in BST and in GMT.
   One position per symbol per candidate, at most 3 open across symbols, a
   5-bar cooldown after an exit, at most 6 entries per symbol per UK day.
7. Money: gross from the fills; commission from the paper broker's own table
   (``DEFAULT_COMMISSION`` at ``DEFAULT_FALLBACK_FX``), charged on entry and on
   exit; ``cost_gbp`` = spread + slippage + commission, kept on every trade so
   ``cost_stress`` can re-price it; ``r`` = net / the risk in pounds.
8. In practice the same functions get live bars; an entry uses one real tick
   (``fill_source="tick"``) and a feed gap over 5 minutes with a position open
   closes it at the first bar back (``FEED_GAP``).

Aaron, 10 Oct 2026: "Never invent performance."
"""
from __future__ import annotations

import time
from math import ceil, floor
from typing import Optional

from ..broker.paper import DEFAULT_COMMISSION, DEFAULT_FALLBACK_FX
from ..research.replay import default_spec
from . import (COOLDOWN_BARS, COST_SKIP_SHARE, MAX_ENTRIES_PER_DAY, MAX_OPEN_PER_CANDIDATE,
               MIN_LOT_RISK_MULT, NEWS_AVOID_MINUTES, SLIP_MIN_POINTS, SLIP_SPREAD_SHARE,
               SPREAD_SKIP_MULT, TARGET_REACH_POINTS, TRAIL_ACTIVATE_R, Candidate, Result,
               Trade, iso_utc)
from .candidate import family_of
from .store import SymbolSeries, Window

AT_THE_CLOSE = ("TIME", "SESSION_END", "FEED_GAP")       # exits filled at a bar's close

# exit code -> (ATR multiple for the stop, target in R, trail mode, trail size, hold minutes)
EXIT_RULES = {
    "t1": (1.0, 1.0, None, 0.0, 0),
    "t2": (1.0, 2.0, None, 0.0, 0),
    "t15w": (1.5, 1.5, None, 0.0, 0),
    "tr1": (1.0, None, "atr", 1.0, 0),
    "tr2": (1.0, None, "r", 2.0, 0),
    "time4h": (1.0, None, None, 0.0, 240),
    "time1h": (1.0, None, None, 0.0, 60),
    "vwap": (1.0, "vwap", None, 0.0, 0),
}


# ------------------------------------------------------------ instruments ---
_DEFAULT_SPREAD: dict = {}


def default_spread_points(symbol: str, kind: str = "") -> float:
    """The spread assumed for a bar that recorded none: replay's conservative
    default for the symbol's kind (15 points FX, 30 gold, 10 indices)."""
    v = _DEFAULT_SPREAD.get(symbol)
    if v is None:
        v = float(default_spec(symbol, 1.0).typical_spread_points)
        _DEFAULT_SPREAD[symbol] = v
    return v


def commission_gbp_per_lot_side(kind: str) -> float:
    """Pounds per lot per side for a kind of market, from the paper broker's
    own table at its own fallback exchange rate. Nothing is retyped here."""
    amount, ccy = DEFAULT_COMMISSION.get(kind, DEFAULT_COMMISSION["*"])
    if ccy == "USD":
        rate = DEFAULT_FALLBACK_FX["USDGBP"]
    elif ccy == "GBP":
        rate = 1.0
    else:
        amount, ccy = DEFAULT_COMMISSION["*"]
        rate = DEFAULT_FALLBACK_FX["USDGBP"]
    return float(amount) * rate


class Inst:
    """The money facts of one symbol, taken from its contract specification."""
    __slots__ = ("symbol", "kind", "point", "digits", "pp", "vmin", "vstep", "vmax", "stops_level",
                 "comm_side", "dflt_spread")

    def __init__(self, symbol, kind, spec):
        self.symbol, self.kind = symbol, kind
        self.point = float(spec.point)
        self.digits = int(spec.digits)
        # pounds per point per lot: tick_value * point / tick_size
        self.pp = float(spec.tick_value) * float(spec.point) / float(spec.tick_size)
        self.vmin, self.vstep = float(spec.volume_min), float(spec.volume_step)
        self.vmax = float(spec.volume_max or 0.0)
        self.stops_level = float(spec.stops_level_points or 0.0)
        self.comm_side = commission_gbp_per_lot_side(kind)
        self.dflt_spread = default_spread_points(symbol, kind)


def inst_of(series: SymbolSeries) -> Inst:
    got = series._memo.get("inst")
    if got is None:
        got = Inst(series.symbol, series.kind, series.spec)
        series._memo["inst"] = got
    return got


def floor_to_step(x: float, step: float, vmax: float = 0.0) -> float:
    v = floor(x / step + 1e-9) * step
    if vmax and v > vmax:
        v = floor(vmax / step + 1e-9) * step
    return round(v, 8)


# ------------------------------------------------------------- one position ---
class Pos:
    """An open simulated position (also what the practice process keeps)."""
    __slots__ = ("side", "vol", "entry", "stop", "stop0", "target", "peak", "D", "trail",
                 "trail_dist", "hold", "t_open", "j_open", "spread_in", "slip_in",
                 "assumed", "fill_source", "ref")

    def __init__(self):
        self.assumed = False
        self.fill_source = "bar"
        self.ref = ""


def enter(inst: Inst, exit_code: str, risk_gbp: float, side: int, ref_close: float,
          stop_points: Optional[int], atr: float, o_next: float, spread: float,
          spread_assumed: bool, day_median_spread: float, t_open: int, j_open: int,
          vwap: float = 0.0, entry_price: Optional[float] = None, flat_ok: bool = True):
    """Try to open a position. Returns a ``Pos`` or a string naming why not
    (the simulator counts those strings; the page shows them).

    ``entry_price`` lets the practice process pass the real fill (a tick's ask
    for a buy, bid for a sell, plus slip); otherwise it is worked out from the
    next bar's open as in the past test."""
    atr_k, tgt_r, trail_mode, trail_k, hold = EXIT_RULES[exit_code]
    if not flat_ok:
        return "late_entry"
    if stop_points is None:
        if atr <= 0:
            return "no_atr"
        d = ceil(atr_k * atr)
    else:
        d = stop_points
    if d <= 0:
        return "no_atr"
    if day_median_spread > 0 and spread > SPREAD_SKIP_MULT * day_median_spread:
        return "wide_spread"
    slip = max(SLIP_MIN_POINTS, SLIP_SPREAD_SHARE * spread)
    if entry_price is None:
        entry = o_next + spread + slip if side > 0 else o_next - slip
    else:
        entry = entry_price
    stop = ref_close - d if side > 0 else ref_close + d
    dist = (entry - stop) if side > 0 else (stop - entry)           # the real risk distance
    if dist <= 0:
        return "stop_gap"
    if dist < max(inst.stops_level, spread):
        return "stop_too_close"
    comm_pts = 2.0 * inst.comm_side / inst.pp
    if spread + comm_pts > COST_SKIP_SHARE * dist:
        return "costs_vs_stop"
    per_lot = dist * inst.pp
    vol = floor_to_step(risk_gbp / per_lot, inst.vstep, inst.vmax)
    # Never rounded UP to the broker's smallest lot, as in the live engine (engine/risk.py
    # floors onto the lot grid and refuses a trade smaller than the minimum). The spec's second
    # clause, "or the minimum lot risks more than 1.5 x the risk", is therefore already covered
    # by the first (a minimum lot that risks more than the allowance cannot fit it at all); it
    # is kept so that a future round-up-to-the-minimum rule (Ian's bridge allows one, within
    # 1.5 x) would not silently lose the guard. Reading to be confirmed by Aaron or Claude.
    if vol < inst.vmin - 1e-12 or inst.vmin * per_lot > MIN_LOT_RISK_MULT * risk_gbp:
        return "smallest_size"
    p = Pos()
    p.side, p.vol, p.entry, p.stop, p.stop0, p.D = side, vol, entry, stop, stop, dist
    if tgt_r == "vwap":
        reach = (vwap - entry) if side > 0 else (entry - vwap)
        if vwap <= 0 or reach <= 0:
            return "target_behind"
        p.target = vwap
    elif tgt_r is None:
        p.target = None
    else:
        p.target = entry + side * tgt_r * dist
    p.trail = trail_mode
    p.trail_dist = (trail_k * atr if trail_mode == "atr" else trail_k * dist) if trail_mode else 0.0
    if trail_mode == "atr" and p.trail_dist <= 0:
        return "no_atr"
    p.hold, p.t_open, p.j_open = hold, t_open, j_open
    p.peak = entry
    p.spread_in, p.slip_in = spread, slip
    p.assumed = spread_assumed
    return p


def step(pos: Pos, t: int, o: float, h: float, l: float, c: float, sp: float, last: bool):
    """Advance an open position through one bar. Returns None while it stays
    open, else ``(exit_price, reason, slip_points, spread_points_paid)``.

    The stop is tested first. A BUY works on the bid, a SELL on the ask (bid +
    spread). The trail follows the peak of the previous bars only."""
    slip = SLIP_SPREAD_SHARE * sp
    if slip < SLIP_MIN_POINTS:
        slip = SLIP_MIN_POINTS
    if pos.side > 0:
        if pos.trail and pos.peak - pos.entry >= TRAIL_ACTIVATE_R * pos.D:
            ns = pos.peak - pos.trail_dist
            if ns > pos.stop:
                pos.stop = ns
        stop = pos.stop
        if l <= stop:
            reason = "STOP" if stop == pos.stop0 else "TRAIL"
            if o < stop:
                return o, reason, 0.0, 0.0
            return stop - slip, reason, slip, 0.0
        tg = pos.target
        if tg is not None and h >= tg + TARGET_REACH_POINTS:
            return tg, "TARGET", 0.0, 0.0
        if h > pos.peak:
            pos.peak = h
        if pos.hold and t + 1 - pos.t_open >= pos.hold:
            return c - slip, "TIME", slip, 0.0
        if last:
            return c - slip, "SESSION_END", slip, 0.0
        return None
    if pos.trail and pos.entry - pos.peak >= TRAIL_ACTIVATE_R * pos.D:
        ns = pos.peak + pos.trail_dist
        if ns < pos.stop:
            pos.stop = ns
    stop = pos.stop
    if h + sp >= stop:
        reason = "STOP" if stop == pos.stop0 else "TRAIL"
        ask_open = o + sp
        if ask_open > stop:
            return ask_open, reason, 0.0, sp
        return stop + slip, reason, slip, sp
    tg = pos.target
    if tg is not None and l + sp <= tg - TARGET_REACH_POINTS:
        return tg, "TARGET", 0.0, sp
    a = l + sp
    if a < pos.peak:
        pos.peak = a
    if pos.hold and t + 1 - pos.t_open >= pos.hold:
        return c + sp + slip, "TIME", slip, sp
    if last:
        return c + sp + slip, "SESSION_END", slip, sp
    return None


def feed_gap_fill(pos: Pos, close: float, spread: float) -> tuple:
    """The exit tuple for a position closed because the price feed went quiet
    for more than 5 minutes (practice only): at the first bar back, at its
    close, with the spread (a sell covers at the ask) and slip against us."""
    slip = max(SLIP_MIN_POINTS, SLIP_SPREAD_SHARE * spread)
    if pos.side > 0:
        return close - slip, "FEED_GAP", slip, 0.0
    return close + spread + slip, "FEED_GAP", slip, spread


def close_trade(inst: Inst, pos: Pos, symbol: str, risk_gbp: float, fill: tuple, t_exit: int,
                j_exit: int, day: str, exit_assumed: bool) -> Trade:
    """Turn a position and its exit into a Trade (money, cost, R). ``t_exit`` is the open
    minute of the bar the exit happened in. A stop or a target is hit somewhere inside that
    bar, so it is stamped with the bar's open; a time, session-end or feed-gap exit is filled
    at the bar's CLOSE, so it is stamped one minute later (a 21:55 UK flat exit reads 21:55)."""
    price, reason, slip_out, spread_out = fill
    if reason in AT_THE_CLOSE:
        t_exit += 1
    pp, vol = inst.pp, pos.vol
    gross = pos.side * (price - pos.entry) * pp * vol
    commission = 2.0 * inst.comm_side * vol
    spread_paid = pos.spread_in if pos.side > 0 else spread_out
    slip_paid = pos.slip_in + slip_out
    cost = (spread_paid + slip_paid) * pp * vol + commission
    net = gross - commission
    pt, nd = inst.point, inst.digits + 3          # fills can sit between points (slippage)
    return Trade(
        symbol=symbol, side=pos.side, volume=vol, entry=round(pos.entry * pt, nd),
        stop=round(pos.stop0 * pt, nd),
        target=None if pos.target is None else round(pos.target * pt, nd),
        opened_utc=iso_utc(pos.t_open),
        closed_utc=iso_utc(t_exit), exit_price=round(price * pt, nd), exit_reason=reason, gross_gbp=gross,
        cost_gbp=cost, net_gbp=net, r=net / risk_gbp, bars_held=j_exit - pos.j_open + 1,
        spread_assumed=bool(pos.assumed or exit_assumed), fill_source=pos.fill_source, day=day)


# ---------------------------------------------------------------- the run ---
def _news_array(series: SymbolSeries):
    return series.f("news_min")


def family_params(c: Candidate) -> dict:
    """What a family's ``signals`` receives as ``p``: the candidate's settings
    plus the fields every candidate carries (session, markets, exit, news)."""
    p = dict(c.params)
    p.update(session=c.session, markets=c.markets, exit=c.exit, news=c.news)
    return p


def run(candidate: Candidate, window: Window, *, family=None) -> Result:
    """Run one candidate over one window and return every trade it would have made.

    Deterministic: the same candidate on the same window gives the same result,
    byte for byte. Days the candidate cannot judge (AVOID_15M on a day with no
    calendar) are left out and counted."""
    t_start = time.perf_counter()
    fam = family or family_of(candidate)
    c = candidate
    p = family_params(c)
    skipped: dict = {}

    def skip(reason: str, n: int = 1) -> None:
        if n > 0:
            skipped[reason] = skipped.get(reason, 0) + n

    avoid = c.news == "AVOID_15M"
    needs_news = avoid or "NEWS" in fam.code
    days = list(window.days)
    if avoid:
        known = [d for d in days if window.news_known(d)]
        skip("news_unknown_day", len(days) - len(known))
        days = known
    dayset = set(days)
    daily = {d: [0.0, 0] for d in days}

    syms = window.symbols_of(c.markets)
    if not syms:
        skip("no_markets")
    series = [window.series[s] for s in syms]
    insts = [inst_of(s) for s in series]

    # 1. every signal of every symbol-day
    events = []
    for k, ser in enumerate(series):
        news = _news_array(ser) if needs_news else None
        for dv in ser.dayviews():
            if dv.uk not in dayset:
                continue
            lo, hi = dv.span(c.session)
            if hi - lo < 2:
                continue
            for sg in fam.signals(dv, p, (lo, hi), news):
                if lo <= sg.i < hi:
                    events.append((ser.t[sg.i], k, sg.i, sg, dv))
    events.sort(key=lambda e: (e[0], e[1], e[2]))

    # 2. take them in time order under the caps
    trades: list = []
    next_ok = [0] * len(series)                  # per symbol: first bar index a signal may fire at
    entries: dict = {}                           # (symbol index, UK day) -> entries so far
    open_exits: list = []                        # exit minute of every position still open
    news_cache = {}
    for ts, k, i, sg, dv in events:
        ser, inst = series[k], insts[k]
        if i < next_ok[k]:
            skip("busy_or_cooldown")
            continue
        if open_exits:
            open_exits = [x for x in open_exits if x > ts]
        if len(open_exits) >= MAX_OPEN_PER_CANDIDATE:
            skip("max_open")
            continue
        key = (k, dv.uk)
        if entries.get(key, 0) >= MAX_ENTRIES_PER_DAY:
            skip("day_cap")
            continue
        if avoid:
            na = news_cache.get(k)
            if na is None:
                na = news_cache[k] = _news_array(ser)
            nm = na[i]
            if 0 <= nm <= NEWS_AVOID_MINUTES:
                skip("news_soon")
                continue
        j0 = i + 1
        if j0 >= ser.n or j0 > dv.flat_i:
            skip("late_entry")
            continue
        eff, assumed = ser.spread_arrays()
        sp0 = eff[j0]
        vw = 0.0
        if c.exit == "vwap":
            vw = ser.f(f"vwap_{c.session}")[i]
        pos = enter(inst, c.exit, c.risk_gbp, sg.side, float(ser.c[i]), sg.stop_points,
                    float(ser.f("atr5")[i]), float(ser.o[j0]), float(sp0), bool(assumed[j0]),
                    ser.day_median_spread(dv), ser.t[j0], j0, vwap=vw)
        if isinstance(pos, str):
            skip(pos)
            continue
        pos.ref = sg.ref
        entries[key] = entries.get(key, 0) + 1
        # walk it forward to its exit
        t_, o_, h_, l_, c_ = ser.t, ser.o, ser.h, ser.l, ser.c
        flat_i, n = dv.flat_i, ser.n
        j = j0
        fill = None
        last_bar = n - 1                         # the data may end before the flat time
        while True:
            fill = step(pos, t_[j], o_[j], h_[j], l_[j], c_[j], eff[j],
                        j >= flat_i or j >= last_bar)
            if fill is not None:
                break
            j += 1
        tr = close_trade(inst, pos, ser.symbol, c.risk_gbp, fill, t_[j], j, dv.uk,
                         bool(assumed[j]))
        trades.append(tr)
        open_exits.append(t_[j])
        next_ok[k] = j + COOLDOWN_BARS
        row = daily.get(dv.uk)
        if row is not None:
            row[0] += tr.net_gbp
            row[1] += 1
    return Result(candidate_id=c.id, window=window.info, trades=trades,
                  daily={d: (v[0], v[1]) for d, v in daily.items()}, skipped=skipped,
                  synthetic=window.synthetic, seconds=time.perf_counter() - t_start)
