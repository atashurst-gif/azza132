"""SUCCESSFUL MOMENTUM RUN labels.

A moment is the start of a successful LONG run for setting (X, Y, H) when,
buying at the ASK at that moment, the BID reaches entry + X pips before it
falls to entry - Y pips, within H seconds. A SHORT run sells at the BID and
watches the ASK. Costs are therefore inside the label: the spread is paid.
(``price="mid"`` measures both on the mid instead.)

Resolution: one second. Second ``s`` of a day holds the ticks with time in
(T0 + s - 1, T0 + s]; the entry at second ``s`` is the last touch at or
before T0 + s and the future is seconds s+1, s+2, ... If the favourable and
the adverse level are both crossed inside the same second the order is
unknown and the label is a FAILURE (the conservative reading).

The search is exact and fast: for every start, "the first later second whose
high reaches the start's own level" is answered for all starts at once by
sorting levels and values and sweeping with a union-find "next index"
structure - O(n log n) in pure Python, no numpy needed.

``label_ticks`` answers the same question for ONE entry on raw ticks (used
for the latency test: does the run survive a 250 ms / 1 s / 2 s delay?).
"""
from __future__ import annotations

import bisect
import math
from array import array
from dataclasses import dataclass
from typing import Optional, Sequence

from .tickstore import DayTicks

INF = float("inf")


@dataclass(frozen=True)
class RunSetting:
    favourable_pips: float
    adverse_pips: float
    horizon_s: int

    @property
    def name(self) -> str:
        h = self.horizon_s
        hz = f"{h // 60} min" if h % 60 == 0 else f"{h} s"
        return f"+{self.favourable_pips:g} before -{self.adverse_pips:g} pips within {hz}"

    @property
    def key(self) -> str:
        return f"X{self.favourable_pips:g}_Y{self.adverse_pips:g}_H{self.horizon_s}"

    def to_dict(self) -> dict:
        return {"favourable_pips": self.favourable_pips, "adverse_pips": self.adverse_pips,
                "horizon_s": self.horizon_s, "name": self.name, "key": self.key}


DEFAULT_SETTINGS = (
    RunSetting(5, 2.5, 120),
    RunSetting(10, 5, 300),
    RunSetting(20, 8, 600),
    RunSetting(30, 12, 900),
    RunSetting(50, 20, 1800),
)


@dataclass
class SecondBars:
    """Per-second highs and lows of the bid and the ask, and the closing touch."""
    t0_ms: int
    n: int
    bid_hi: array
    bid_lo: array
    ask_hi: array
    ask_lo: array
    bid_close: array                  # carried forward; NaN before the first tick
    ask_close: array
    last_tick_s: array                # the second of the latest tick at or before s (-1 = none)

    def time_ms(self, s: int) -> int:
        return self.t0_ms + 1000 * s

    def active(self, s: int, max_age_s: int = 10) -> bool:
        lt = self.last_tick_s[s]
        return lt >= 0 and s - lt <= max_age_s

    def mid(self, s: int) -> float:
        return 0.5 * (self.bid_close[s] + self.ask_close[s])


def second_bars(ticks: DayTicks, t0_ms: int, n: int) -> SecondBars:
    nan = float("nan")
    bh = array("d", [-INF]) * n
    bl = array("d", [INF]) * n
    ah = array("d", [-INF]) * n
    al = array("d", [INF]) * n
    has = bytearray(n)
    bc = array("d", [nan]) * n
    ac = array("d", [nan]) * n
    for t, b, a in zip(ticks.times, ticks.bids, ticks.asks):
        j = -((t0_ms - t) // 1000)                  # ceil((t - t0) / 1000)
        if j < 0:
            j = 0                                    # earlier ticks only set the first touch
        if j >= n:
            break
        if b > bh[j]:
            bh[j] = b
        if b < bl[j]:
            bl[j] = b
        if a > ah[j]:
            ah[j] = a
        if a < al[j]:
            al[j] = a
        bc[j], ac[j] = b, a
        has[j] = 1
    last = array("q", [-1]) * n
    lb, la, ls = nan, nan, -1
    for s in range(n):
        if has[s]:
            lb, la, ls = bc[s], ac[s], s
        bc[s], ac[s], last[s] = lb, la, ls
    return SecondBars(t0_ms, n, bh, bl, ah, al, bc, ac, last)


def first_reach(values: Sequence[float], thresholds: Sequence[float], starts: Sequence[int]) -> list[int]:
    """For each query q: the first index j > starts[q] with values[j] >= thresholds[q]
    (len(values) when there is none)."""
    n = len(values)
    parent = list(range(n + 1))

    def find(x: int) -> int:
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    order_v = sorted(range(n), key=values.__getitem__)
    order_q = sorted(range(len(starts)), key=thresholds.__getitem__)
    out = [n] * len(starts)
    p = 0
    for q in order_q:
        lvl = thresholds[q]
        while p < n and values[order_v[p]] < lvl:
            i = order_v[p]
            parent[i] = i + 1
            p += 1
        s = starts[q] + 1
        out[q] = find(s) if s <= n else n
    return out


@dataclass
class Labels:
    setting: RunSetting
    side: int                              # +1 long, -1 short
    ok: bytearray                          # per second: 1 = a successful run starts here
    t_fav: array                           # seconds to the favourable level (-1 = not reached in time)
    starts: int = 0                        # seconds that were checked (active market)

    def count(self) -> int:
        return sum(self.ok)


def label_runs(bars: SecondBars, setting: RunSetting, pip: float, side: int, price: str = "touch",
               start_range: Optional[tuple[int, int]] = None, max_age_s: int = 10) -> Labels:
    """Label every active second in ``start_range`` (default: all)."""
    n = bars.n
    lo, hi = start_range or (0, n)
    starts = [s for s in range(max(0, lo), min(n, hi)) if bars.active(s, max_age_s)]
    X, Y = setting.favourable_pips * pip, setting.adverse_pips * pip
    mid = price == "mid"
    if side > 0:
        entry = [(bars.mid(s) if mid else bars.ask_close[s]) for s in starts]
        if mid:
            fav_v = [0.5 * (bh + ah) if bh > -INF else -INF for bh, ah in zip(bars.bid_hi, bars.ask_hi)]
            adv_v = [-(0.5 * (bl + al)) if bl < INF else -INF for bl, al in zip(bars.bid_lo, bars.ask_lo)]
        else:
            fav_v = list(bars.bid_hi)
            adv_v = [-v for v in bars.bid_lo]
        fav_t = [e + X for e in entry]
        adv_t = [-(e - Y) for e in entry]
    else:
        entry = [(bars.mid(s) if mid else bars.bid_close[s]) for s in starts]
        if mid:
            fav_v = [-(0.5 * (bl + al)) if bl < INF else -INF for bl, al in zip(bars.bid_lo, bars.ask_lo)]
            adv_v = [0.5 * (bh + ah) if bh > -INF else -INF for bh, ah in zip(bars.bid_hi, bars.ask_hi)]
        else:
            fav_v = [-v for v in bars.ask_lo]
            adv_v = list(bars.ask_hi)
        fav_t = [-(e - X) for e in entry]
        adv_t = [e + Y for e in entry]
    jf = first_reach(fav_v, fav_t, starts)
    ja = first_reach(adv_v, adv_t, starts)
    ok = bytearray(n)
    tf = array("q", [-1]) * n
    H = setting.horizon_s
    for s, f, a in zip(starts, jf, ja):
        if f < n and f - s <= H and f < a:
            ok[s] = 1
            tf[s] = f - s
    return Labels(setting, side, ok, tf, len(starts))


@dataclass
class RunEvent:
    second: int
    side: int
    t_fav: int


def run_events(labels: Labels) -> list[RunEvent]:
    """The START of each distinct run: the first second of a block of
    labelled seconds; a later block that begins before the previous run
    reached its target is the same move and is not counted again."""
    out: list[RunEvent] = []
    busy_until = -1
    prev = 0
    ok, tf = labels.ok, labels.t_fav
    for s in range(len(ok)):
        cur = ok[s]
        if cur and not prev and s > busy_until:
            out.append(RunEvent(s, labels.side, int(tf[s])))
            busy_until = s + int(tf[s])
        prev = cur
    return out


def label_ticks(ticks: DayTicks, entry_ms: int, side: int, setting: RunSetting, pip: float,
                slippage_pips: float = 0.0, use_last_touch: bool = False) -> Optional[dict]:
    """One entry on raw ticks. The fill is the first tick at or after
    ``entry_ms`` (or the last touch at or before it when ``use_last_touch``),
    at the ask for a long / the bid for a short, plus slippage. Returns
    {"ok", "fill", "fill_ms", "seconds"} or None when there is no tick."""
    times = ticks.times
    if use_last_touch:
        i = bisect.bisect_right(times, entry_ms) - 1
    else:
        i = bisect.bisect_left(times, entry_ms)
    if i < 0 or i >= len(times):
        return None
    slip = slippage_pips * pip
    fill = (ticks.asks[i] + slip) if side > 0 else (ticks.bids[i] - slip)
    up, dn = fill + side * setting.favourable_pips * pip, fill - side * setting.adverse_pips * pip
    end = times[i] + setting.horizon_s * 1000
    for k in range(i + 1, len(times)):
        if times[k] > end:
            break
        mark = ticks.bids[k] if side > 0 else ticks.asks[k]
        if (mark - dn) * side <= 0:
            return {"ok": False, "fill": fill, "fill_ms": times[i], "seconds": (times[k] - times[i]) / 1000.0}
        if (mark - up) * side >= 0:
            return {"ok": True, "fill": fill, "fill_ms": times[i], "seconds": (times[k] - times[i]) / 1000.0}
    return {"ok": False, "fill": fill, "fill_ms": times[i], "seconds": None}


def base_rates(bars: SecondBars, settings: Sequence[RunSetting], pip: float, start_range: tuple[int, int],
               price: str = "touch") -> list[dict]:
    """How often a successful run starts, per setting and side."""
    out = []
    for st in settings:
        row = {"setting": st.name, "key": st.key}
        for side, nm in ((1, "long"), (-1, "short")):
            lab = label_runs(bars, st, pip, side, price, start_range)
            ev = run_events(lab)
            row[f"{nm}_runs"] = len(ev)
            row[f"{nm}_seconds_ok"] = lab.count()
            row["seconds_checked"] = lab.starts
        out.append(row)
    return out


def finite(x: float) -> bool:
    return not (math.isnan(x) or math.isinf(x))
