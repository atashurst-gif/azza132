"""EVENT STUDY: what changes immediately before a large, clean, short-term run?

1. Every second of every day is labelled with the PRIMARY run setting
   (labeler.py, e.g. +10 pips before -5 within 5 minutes, spread paid). The
   START of each distinct run is an EVENT, in the run's direction.
2. ORDINARY MOMENTS (the baseline) are drawn at random (seeded) from the
   same markets and days: active seconds that are not the start of a run
   either way and not within two minutes of one; each gets a random
   direction, so "pointing the run's way" has a fair comparison.
3. At 30 s before, 10 s before and AT each moment, the evidence families are
   measured with the SAME code the bot runs (mintel.rider.features,
   score.py, strength.py), on a RiderCore fed only the ticks up to that
   moment (no look-ahead). Directional families are ALIGNED to the
   direction: +strength pointing that way, -strength pointing against.
4. Outcomes 10 s, 30 s, 1 min and 5 min after (mid move in the direction,
   pips), and the best/worst excursion within 5 minutes at bid/ask.
5. Runs versus ordinary moments, per feature: Cohen's d and AUC (the chance
   a random run moment shows a higher value than a random ordinary one;
   0.5 = no difference). "What changed before the run": the change from
   -30 s to the moment, runs versus ordinary moments.
6. A caution the numbers themselves show: the START of a run is, in
   hindsight, the earliest entry that would have worked - usually the
   bottom of a small dip - so very short-term speed points AGAINST the run
   there. A momentum bot cannot buy that blind. So a second comparison
   asks the bot's own question:
7. IGNITION: every fast BURST (the mid moving ``ignition`` x X pips within
   15 seconds - 3 pips for the 10-pip setting) is a moment the bot could
   notice. Bursts that carried on into a full successful run (entered at the
   touch at once) are compared with bursts that died: what did the good
   ones have, at the burst and 30/10 seconds before?
8. LATENCY: for every burst that became a run, entering 0 / 250 / 500 /
   1000 / 2000 ms late at the touch, plus slippage - is it still a
   successful run, and what did the delay cost?
"""
from __future__ import annotations

import bisect
import math
import random
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

from ...broker.base import Tick
from ..config import RiderConfig
from ..features import Evidence, compute_families
from ..pipvalue import fx_pip_size
from ..score import score_market
from ..strength import PairMove, compute_strength, pair_move_z
from .labeler import DEFAULT_SETTINGS, RunSetting, base_rates, label_runs, label_ticks, run_events, second_bars
from .replay import calendar_at, load_day, make_core
from .stats import median
from .tickstore import DAY_MS, TickStore, merged, ms_to_dt

OFFSETS = (-30, -10, 0)
HORIZONS = (10, 30, 60, 300)
LATENCIES_MS = (0, 250, 500, 1000, 2000)

DIRECTIONAL = ("efficiency", "momentum", "breakout", "first_retest", "pullback", "opening_range", "session_breakout",
               "squeeze_release", "vwap", "ema_geometry", "adx", "candles", "microstructure", "sweep")
FEATURE_NAMES = {
    "score": "the MOMENTUM_OPPORTUNITY_SCORE pointing the run's way",
    "confirmed": "price taking out the 20-second high/low the run's way (the trigger's confirmation)",
    "velocity": "speed the run's way (ATR-normalised, 1 s to 3 min blended)",
    "z5": "the last 5 seconds' move the run's way (in ATRs)",
    "z15": "the last 15 seconds' move the run's way (in ATRs)",
    "z60": "the last minute's move the run's way (in ATRs)",
    "accel": "acceleration the run's way",
    "jerk": "the change in acceleration the run's way",
    "efficiency": "TREND_EFFICIENCY the run's way",
    "eff60": "one-minute efficiency (straightness, any direction)",
    "volatility": "volatility expansion",
    "range60_atr": "the last minute's range (in ATRs)",
    "participation": "price-update activity against normal",
    "rate10": "price updates in the last 10 s against normal",
    "chop": "the anti-chop score (back-and-forth)",
    "spread_atr": "the spread against the ATR",
    "atr_pips": "the one-minute ATR in pips",
    "currency_strength": "currency strength agreeing with the run",
    "cross_market": "other pairs confirming (a broad currency move)",
    "momentum": "one-minute momentum measures the run's way",
    "breakout": "a 20-minute range breakout the run's way",
    "first_retest": "a first retest holding the run's way",
    "pullback": "a shallow pullback resuming the run's way",
    "opening_range": "an opening-range break the run's way",
    "session_breakout": "a session high/low break the run's way",
    "squeeze_release": "a squeeze releasing the run's way",
    "vwap": "VWAP slope and side agreeing with the run",
    "ema_geometry": "moving averages fanning out the run's way",
    "adx": "ADX/DMI trend strength the run's way",
    "candles": "strong candles the run's way",
    "microstructure": "tick-level higher highs/lows the run's way",
    "sweep": "a liquidity sweep and reclaim the run's way",
}


def _aligned(e: Optional[Evidence], d: int) -> float:
    if e is None or not e.direction:
        return 0.0
    return e.strength if e.direction == d else -e.strength


def feature_row(v, res, strength_map, d: int) -> dict:
    """Every measured feature at one moment, aligned to direction ``d``."""
    f = res.families
    t = v.ticks
    b = v.bars
    row = {
        "score": res.score if res.side == d else (-res.score if res.side == -d else 0.0),
        "confirmed": 1.0 if (res.confirmed and res.side == d) else 0.0,
        "velocity": t.velocity * d,
        "z5": t.z.get(5, 0.0) * d,
        "z15": t.z.get(15, 0.0) * d,
        "z60": t.z.get(60, 0.0) * d,
        "accel": t.accel * d,
        "jerk": t.jerk * d,
        "eff60": t.eff60,
        "volatility": f["volatility"].strength,
        "range60_atr": t.range60 / v.atr if v.atr > 0 else 0.0,
        "participation": f["participation"].strength,
        "rate10": (t.tick_rate10 / b.vol_baseline) if b.vol_baseline > 0 else 1.0,
        "chop": f["chop"].strength,
        "spread_atr": t.spread / v.atr if v.atr > 0 else 0.0,
        "atr_pips": v.atr / v.pip if v.pip else 0.0,
        "currency_strength": strength_map.pair_alignment(v.base, v.quote, d).strength,
        "cross_market": strength_map.cross_market(v.symbol, v.base, v.quote, d).strength,
    }
    for k in DIRECTIONAL:
        row[k] = _aligned(f.get(k), d)
    return {k: (float(x) if x == x and abs(x) != float("inf") else 0.0) for k, x in row.items()}


@dataclass
class _Sample:
    sid: int
    symbol: str
    second: int
    side: int
    kind: str                      # "run", "base", "burst_ok" or "burst_fail"
    t_fav: int = -1


def study_day(store: TickStore, day: str, symbols: Sequence[str], specs: dict, calendar: Sequence, cfg: RiderConfig,
              settings: Sequence[RunSetting] = DEFAULT_SETTINGS, primary: int = 1, *, n_baseline: int = 120,
              max_events: int = 150, seed: int = 11, latencies: Sequence[int] = LATENCIES_MS,
              slippage_pips: float = 0.2, cache_dir: Optional[Path] = None, warm_s: int = 600,
              account_currency: str = "GBP", ignition: float = 0.3, ignition_window_s: int = 15,
              max_bursts: int = 150) -> dict:
    prim = settings[primary]
    longest = max(s.horizon_s for s in settings)
    data = load_day(store, day, symbols, specs, calendar, warm_s=warm_s, tail_s=longest + 60, cache_dir=cache_dir,
                    account_currency=account_currency)
    t0 = data.start_ms
    nsec = DAY_MS // 1000
    n = nsec + longest + 1
    out = {"day": day, "setting": prim.to_dict(), "base_rates": {}, "samples": [], "notes": list(data.notes)}
    samples: list[_Sample] = []
    bars_by: dict = {}
    for sym in data.symbols:
        pip = _pip(specs[sym])
        if not pip:
            continue
        bars = second_bars(data.ticks[sym], t0, n)
        bars_by[sym] = bars
        out["base_rates"][sym] = base_rates(bars, settings, pip, (0, nsec))
        lab_l = label_runs(bars, prim, pip, 1, start_range=(0, nsec))
        lab_s = label_runs(bars, prim, pip, -1, start_range=(0, nsec))
        evs = [e for e in run_events(lab_l) + run_events(lab_s) if e.second >= 30]
        rng = random.Random(seed * 100_003 + zlib.crc32(f"{sym}|{day}".encode()))
        if len(evs) > max_events:
            out["notes"].append(f"{sym}: {max_events} of its {len(evs)} runs sampled at random (the cap)")
            evs = rng.sample(evs, max_events)
        evs.sort(key=lambda e: e.second)
        starts = [e.second for e in evs]
        for e in evs:
            samples.append(_Sample(len(samples), sym, e.second, e.side, "run", e.t_fav))
        eligible = []
        for s in range(60, nsec - 300):
            if lab_l.ok[s] or lab_s.ok[s] or not bars.active(s):
                continue
            i = bisect.bisect_left(starts, s - 120)
            if i < len(starts) and starts[i] <= s + 120:
                continue
            eligible.append(s)
        pick = rng.sample(eligible, min(n_baseline, len(eligible))) if eligible else []
        for s in sorted(pick):
            samples.append(_Sample(len(samples), sym, s, rng.choice((1, -1)), "base"))
        bursts = find_bursts(bars, ignition * prim.favourable_pips * pip, ignition_window_s, (60, nsec - 300))
        out.setdefault("burst_counts", {})[sym] = len(bursts)
        if len(bursts) > max_bursts:
            out["notes"].append(f"{sym}: {max_bursts} of its {len(bursts)} bursts sampled at random (the cap)")
            bursts = sorted(rng.sample(bursts, max_bursts))
        for s, d in bursts:
            lab = lab_l if d > 0 else lab_s
            samples.append(_Sample(len(samples), sym, s, d, "burst_ok" if lab.ok[s] else "burst_fail",
                                   int(lab.t_fav[s])))
    # ---- measure the families at -30 s, -10 s and the moment, ticks up to then only --
    requests: dict[int, list[tuple[_Sample, int]]] = {}
    for sm in samples:
        for off in OFFSETS:
            requests.setdefault(t0 + 1000 * (sm.second + off), []).append((sm, off))
    feats: dict[int, dict] = {sm.sid: {} for sm in samples}
    core = make_core(cfg, data)
    stream = merged(data.ticks, data.symbols)
    nxt = next(stream, None)
    cal_at = None
    cal_every = int(cfg.calendar_refresh_seconds * 1000)
    for T in sorted(requests):
        if data.calendar and (cal_at is None or T - cal_at >= cal_every):
            core.set_calendar(calendar_at(data.calendar, T, cfg))     # as live: no actual before the release
            cal_at = T
        while nxt is not None and nxt[0] <= T:
            t_ms, k, b, a = nxt
            core.on_tick(data.symbols[k], _tick(data.symbols[k], t_ms, b, a))
            nxt = next(stream, None)
        now = T / 1000.0
        views = {}
        for sym in data.symbols:
            v = core.view(sym, now)
            if v is not None and v.ok:
                views[sym] = v
        sm_map = compute_strength([PairMove(s, v.base, v.quote, pair_move_z(v.ticks.velocity, v.bars.roc5))
                                   for s, v in views.items()])
        fams_cache: dict = {}
        for sm, off in requests[T]:
            v = views.get(sm.symbol)
            if v is None:
                continue
            res = fams_cache.get(sm.symbol)
            if res is None:
                fams = compute_families(v, getattr(core.syms[sm.symbol], "_tail", []))
                res = score_market(v, fams, sm_map, None, core.weights, cfg)
                fams_cache[sm.symbol] = res
            feats[sm.sid][str(off)] = feature_row(v, res, sm_map, sm.side)
    # ---- outcomes and latency survival (the future is used only as the LABEL) --
    for sm in samples:
        bars = bars_by[sm.symbol]
        pip = _pip(specs[sm.symbol])
        s, d = sm.second, sm.side
        m0 = bars.mid(s)
        oc = {}
        for h in HORIZONS:
            if s + h < bars.n and m0 == m0:
                oc[str(h)] = round((bars.mid(s + h) - m0) * d / pip, 3)
        entry = bars.ask_close[s] if d > 0 else bars.bid_close[s]
        hi = lo = None
        for j in range(s + 1, min(bars.n, s + 301)):
            if d > 0:
                if bars.bid_hi[j] > -math.inf:
                    hi = bars.bid_hi[j] if hi is None else max(hi, bars.bid_hi[j])
                    lo = bars.bid_lo[j] if lo is None else min(lo, bars.bid_lo[j])
            else:
                if bars.ask_lo[j] < math.inf:
                    hi = bars.ask_lo[j] if hi is None else min(hi, bars.ask_lo[j])
                    lo = bars.ask_hi[j] if lo is None else max(lo, bars.ask_hi[j])
        if hi is not None and entry == entry:
            oc["mfe5m"] = round(max(0.0, (hi - entry) * d / pip), 2)
            oc["mae5m"] = round(max(0.0, (entry - lo) * d / pip), 2)
        lat = {}
        if sm.kind == "burst_ok":
            touch = entry
            tick_data = data.ticks[sm.symbol]
            for L in latencies:
                r = label_ticks(tick_data, t0 + 1000 * s + int(L), d, prim, pip, slippage_pips, use_last_touch=(L == 0))
                if r is None:
                    continue
                lat[str(L)] = {"ok": bool(r["ok"]), "delay_cost_pips": round((r["fill"] - touch) * d / pip, 3)}
        out["samples"].append({"symbol": sm.symbol, "second": s, "side": d, "kind": sm.kind, "t_fav": sm.t_fav,
                               "features": feats[sm.sid], "outcomes": oc, "latency": lat})
    return out


def _pip(spec) -> float:
    pip, _ = fx_pip_size(spec)
    return pip


def _tick(sym: str, t_ms: int, b: float, a: float) -> Tick:
    return Tick(sym, ms_to_dt(t_ms), b, a)


# ------------------------------------------------------------- combining --

def cohen_d(a: Sequence[float], b: Sequence[float]) -> Optional[float]:
    na, nb = len(a), len(b)
    if na < 2 or nb < 2:
        return None
    ma, mb = sum(a) / na, sum(b) / nb
    va = sum((x - ma) ** 2 for x in a) / (na - 1)
    vb = sum((x - mb) ** 2 for x in b) / (nb - 1)
    sp = math.sqrt(((na - 1) * va + (nb - 1) * vb) / (na + nb - 2))
    if sp <= 1e-12:
        return 0.0
    return (ma - mb) / sp


def auc(a: Sequence[float], b: Sequence[float]) -> Optional[float]:
    """P(random a > random b) + half the ties (Mann-Whitney)."""
    na, nb = len(a), len(b)
    if not na or not nb:
        return None
    allv = sorted([(x, 0) for x in a] + [(x, 1) for x in b])
    ranks = [0.0] * len(allv)
    i = 0
    while i < len(allv):
        j = i
        while j + 1 < len(allv) and allv[j + 1][0] == allv[i][0]:
            j += 1
        r = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[k] = r
        i = j + 1
    ra = sum(r for r, (_, g) in zip(ranks, allv) if g == 0)
    return (ra - na * (na + 1) / 2.0) / (na * nb)


def find_bursts(bars, move: float, window_s: int, rng_s: tuple[int, int], gap_s: int = 60) -> list[tuple[int, int]]:
    """(second, direction) of each fast burst: the mid moved at least ``move``
    (price) within ``window_s``; the first second of it; at most one per
    direction per ``gap_s`` seconds. Uses only prices up to that second."""
    out = []
    last = {1: -10**9, -1: -10**9}
    lo, hi = rng_s
    for s in range(max(lo, window_s + 1), min(hi, bars.n)):
        if not bars.active(s):
            continue
        m, m0 = bars.mid(s), bars.mid(s - window_s)
        if m != m or m0 != m0:
            continue
        mv = m - m0
        if abs(mv) < move:
            continue
        d = 1 if mv > 0 else -1
        if s - last[d] < gap_s:
            continue
        last[d] = s
        out.append((s, d))
    return out


def compare(a: Sequence[dict], b: Sequence[dict]) -> dict:
    """Group A against group B: effect sizes per feature at each offset, the
    change from -30 s to the moment, and the outcomes after."""
    names = sorted({k for x in list(a) + list(b) for o in x["features"].values() for k in o})
    effects = []
    for off in OFFSETS:
        o = str(off)
        for f in names:
            xa = [x["features"][o][f] for x in a if o in x["features"]]
            xb = [x["features"][o][f] for x in b if o in x["features"]]
            if len(xa) < 2 or len(xb) < 2:
                continue
            effects.append({"feature": f, "offset_s": off, "name": FEATURE_NAMES.get(f, f),
                            "mean_a": round(sum(xa) / len(xa), 4), "mean_b": round(sum(xb) / len(xb), 4),
                            "d": _r(cohen_d(xa, xb)), "auc": _r(auc(xa, xb)), "n_a": len(xa), "n_b": len(xb)})
    changes = []
    for f in names:
        da = [x["features"]["0"][f] - x["features"]["-30"][f] for x in a
              if "0" in x["features"] and "-30" in x["features"]]
        db = [x["features"]["0"][f] - x["features"]["-30"][f] for x in b
              if "0" in x["features"] and "-30" in x["features"]]
        if len(da) < 2 or len(db) < 2:
            continue
        changes.append({"feature": f, "name": FEATURE_NAMES.get(f, f),
                        "change_a": round(sum(da) / len(da), 4), "change_b": round(sum(db) / len(db), 4),
                        "d": _r(cohen_d(da, db)), "auc": _r(auc(da, db)), "n_a": len(da), "n_b": len(db)})
    oc = {}
    for h in [str(x) for x in HORIZONS] + ["mfe5m", "mae5m"]:
        xa = [x["outcomes"][h] for x in a if h in x["outcomes"]]
        xb = [x["outcomes"][h] for x in b if h in x["outcomes"]]
        oc[h] = {"a_mean": _r(sum(xa) / len(xa)) if xa else None, "a_median": _r(median(xa)),
                 "b_mean": _r(sum(xb) / len(xb)) if xb else None, "b_median": _r(median(xb))}
    key = lambda e: -abs(e["d"] or 0.0)      # noqa: E731
    return {"n_a": len(a), "n_b": len(b), "effects": effects, "changes": sorted(changes, key=key),
            "top_at_moment": sorted([e for e in effects if e["offset_s"] == 0], key=key)[:10],
            "top_30s_before": sorted([e for e in effects if e["offset_s"] == -30], key=key)[:10],
            "outcomes": oc}


def combine(day_results: Sequence[dict], min_events: int = 30) -> dict:
    """All days together: base rates, runs vs ordinary moments, bursts that
    became runs vs bursts that died, and latency."""
    samples = [x for r in day_results for x in r.get("samples", [])]
    runs = [x for x in samples if x["kind"] == "run"]
    base = [x for x in samples if x["kind"] == "base"]
    b_ok = [x for x in samples if x["kind"] == "burst_ok"]
    b_fail = [x for x in samples if x["kind"] == "burst_fail"]
    setting = next((r.get("setting") for r in day_results if r.get("setting")), {})
    out: dict = {"setting": setting, "days": len(day_results), "min_events": min_events,
                 "runs": len(runs), "baseline": len(base), "bursts_ok": len(b_ok), "bursts_failed": len(b_fail)}
    rates: dict = {}
    for r in day_results:
        for sym, rows in r.get("base_rates", {}).items():
            for row in rows:
                acc = rates.setdefault(row["key"], {"setting": row["setting"], "market_days": 0, "long_runs": 0,
                                                    "short_runs": 0, "seconds_ok": 0, "seconds_checked": 0})
                acc["market_days"] += 1
                acc["long_runs"] += row["long_runs"]
                acc["short_runs"] += row["short_runs"]
                acc["seconds_ok"] += row["long_seconds_ok"] + row["short_seconds_ok"]
                acc["seconds_checked"] += row["seconds_checked"]
    for acc in rates.values():
        md = max(acc["market_days"], 1)
        acc["runs_per_market_day"] = round((acc["long_runs"] + acc["short_runs"]) / md, 2)
        acc["share_of_seconds"] = round(acc["seconds_ok"] / max(acc["seconds_checked"], 1), 5)
    out["base_rates"] = list(rates.values())
    out["runs_vs_ordinary"] = compare(runs, base)
    out["runs_vs_ordinary"]["enough"] = len(runs) >= min_events and len(base) >= min_events
    out["bursts"] = compare(b_ok, b_fail)
    nb = len(b_ok) + len(b_fail)
    out["bursts"]["enough"] = len(b_ok) >= min_events and len(b_fail) >= min_events
    out["bursts"]["share_became_runs"] = round(len(b_ok) / nb, 4) if nb else None
    lat = []
    for L in LATENCIES_MS:
        rows = [x["latency"][str(L)] for x in b_ok if str(L) in x.get("latency", {})]
        if not rows:
            continue
        lat.append({"latency_ms": L, "runs": len(rows),
                    "still_successful": round(sum(1 for r in rows if r["ok"]) / len(rows), 4),
                    "avg_delay_cost_pips": _r(sum(r["delay_cost_pips"] for r in rows) / len(rows), 3)})
    out["latency"] = lat
    out["what_changed"] = what_changed_lines(out)
    return out


def _r(x: Optional[float], nd: int = 3) -> Optional[float]:
    return None if x is None else round(float(x), nd)


def what_changed_lines(c: dict, top: int = 5) -> list[str]:
    """Plain-English answers, only from measured effect sizes (|d| >= 0.2)."""
    lines: list[str] = []
    rv = c.get("runs_vs_ordinary", {})
    if not rv.get("enough"):
        lines.append(f"Runs against ordinary moments: too few to compare ({c.get('runs', 0)} runs, "
                     f"{c.get('baseline', 0)} ordinary moments; need {c.get('min_events', 30)} of each).")
    else:
        lines.append("Measured at the START of each run - in hindsight the earliest entry that would have worked, "
                     "usually the bottom of a small dip (so very short-term speed points against the run there):")
        n = 0
        for ch in rv.get("changes", []):
            d = ch["d"] or 0.0
            if abs(d) < 0.2 or n >= top:
                continue
            n += 1
            way = "rose" if ch["change_a"] > ch["change_b"] else "fell"
            lines.append(f"  - in the 30 seconds before a run, {ch['name']} {way} ({ch['change_a']:+.3f} on average; "
                         f"ordinary moments {ch['change_b']:+.3f}; d={d:+.2f}, AUC {ch['auc']:.2f})")
        for e in rv.get("top_30s_before", [])[:3]:
            d = e["d"] or 0.0
            if abs(d) >= 0.2:
                lines.append(f"  - already 30 seconds before: {e['name']} averaged {e['mean_a']:.3f} before runs "
                             f"against {e['mean_b']:.3f} at ordinary moments (d={d:+.2f})")
        if n == 0:
            lines.append("  - no feature changed clearly differently (every |d| below 0.2)")
    bu = c.get("bursts", {})
    share = bu.get("share_became_runs")
    if not bu.get("enough"):
        lines.append(f"Bursts that became runs against bursts that died: too few to compare "
                     f"({c.get('bursts_ok', 0)} became runs, {c.get('bursts_failed', 0)} died).")
    else:
        lines.append(f"Of {c['bursts_ok'] + c['bursts_failed']} fast bursts, {100 * share:.0f}% carried on into a full run "
                     f"(entered at once at the touch). What the ones that carried on had at the burst:")
        n = 0
        for e in bu.get("top_at_moment", []):
            d = e["d"] or 0.0
            if abs(d) < 0.2 or n >= top:
                continue
            n += 1
            lines.append(f"  - {e['name']}: {e['mean_a']:.3f} against {e['mean_b']:.3f} for bursts that died "
                         f"(d={d:+.2f}, AUC {e['auc']:.2f})")
        if n == 0:
            lines.append("  - nothing measured separated them clearly (every |d| below 0.2): at the burst, the bot's "
                         "evidence could not tell a real run from noise")
    return lines
