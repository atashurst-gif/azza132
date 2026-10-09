"""Does each feature predict anything? Measured, never assumed.

For every signed feature, on replayed events, at several horizons:

    forward move  m(t, h) = (mid(t + h) - mid(t)) / tick               (ticks, in the futures direction)
    active        |value| >= its threshold
    IC            Pearson correlation of value and m over every sample
    hit rate      share of active samples whose sign(value) == sign(m) (m != 0)
    edge          mean of sign(value) x m over active samples           (ticks)
    net           edge - cost_ticks                                       (after costs)

and the same broken down by regime. A feature with no positive net edge at
any horizon (with enough active samples) is reported as NO EVIDENCE - the
spec says remove what has none, and this is where that decision is made.

Samples taken every second overlap heavily at long horizons, so the counts
overstate the independent evidence; that is stated in the output. On
synthetic scenarios every number here is SYNTHETIC - NOT PERFORMANCE: it
proves the pipeline runs, not that a feature works in the real market.

    python -m mintel.ian.research.featuretest --synthetic all
    python -m mintel.ian.research.featuretest --replay data/ian/recordings/2026-10-08
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from typing import Callable, Optional, Sequence

from .. import SYNTHETIC_LABEL
from ..features import FlowSnapshot
from .harness import Sample, sample


def _sweep(fs: FlowSnapshot) -> float:
    return float(fs.sweep_dir) if fs.sweep_dir and fs.sweep_age_s is not None and fs.sweep_age_s <= 20 else 0.0


FEATURES: dict[str, tuple[Callable[[FlowSnapshot], float], float]] = {
    "book_imbalance": (lambda f: f.imbalance, 0.2),
    "imbalance_persistent": (lambda f: f.imbalance_avg * f.imbalance_persist, 0.1),
    "microprice": (lambda f: f.micro_offset, 0.3),
    "ofi_short": (lambda f: math.tanh(f.ofi_short), 0.3),
    "ofi_medium": (lambda f: math.tanh(f.ofi_medium / 2.0), 0.3),
    "aggression_short": (lambda f: f.aggr_short, 0.4),
    "aggression_medium": (lambda f: f.aggr_medium, 0.3),
    "large_prints": (lambda f: f.large_net, 0.15),
    "absorption": (lambda f: f.absorption_dir * f.absorption_score, 0.5),
    "sweep": (_sweep, 0.5),
    "pulling": (lambda f: f.pulling_signal, 0.3),
    "walls": (lambda f: f.wall_signal, 0.4),
    "refresh": (lambda f: f.refresh_signal, 0.4),
    "consumption": (lambda f: f.consumption_signal, 0.2),
    "resilience": (lambda f: f.resilience_signal, 0.5),
    "delta_slope": (lambda f: math.tanh(f.delta_slope) if f.delta_reliable else 0.0, 0.3),
    "delta_divergence": (lambda f: float(f.delta_divergence), 0.5),
    "footprint": (lambda f: f.footprint_signal, 0.5),
    "profile": (lambda f: f.profile_signal, 0.25),
    "vwap": (lambda f: f.vwap_signal, 0.2),
}


def forward_moves(samples: Sequence[Sample], horizon_s: float, every_s: float) -> list[Optional[float]]:
    k = max(1, int(round(horizon_s / every_s)))
    out: list[Optional[float]] = []
    for i, s in enumerate(samples):
        j = i + k
        if j >= len(samples) or not s.fs.ok or samples[j].fs.mid is None or s.fs.mid is None:
            out.append(None)
            continue
        out.append((samples[j].fs.mid - s.fs.mid) / s.fs.tick)
    return out


def _pearson(xs: list[float], ys: list[float]) -> Optional[float]:
    n = len(xs)
    if n < 10:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx <= 0 or syy <= 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / math.sqrt(sxx * syy)


def _metrics(vals: list[float], moves: list[float], thr: float, cost: float) -> dict:
    act = [(v, m) for v, m in zip(vals, moves) if abs(v) >= thr]
    nz = [(v, m) for v, m in act if m != 0]
    hit = (sum(1 for v, m in nz if (v > 0) == (m > 0)) / len(nz)) if nz else None
    edge = (sum((1 if v > 0 else -1) * m for v, m in act) / len(act)) if act else None
    ic = _pearson(vals, moves)
    return {"samples": len(vals), "active": len(act), "ic": round(ic, 3) if ic is not None else None,
            "hit_rate": round(hit, 3) if hit is not None else None,
            "edge_ticks": round(edge, 3) if edge is not None else None,
            "net_ticks": round(edge - cost, 3) if edge is not None else None}


def evaluate(samples: Sequence[Sample], horizons=(5, 30, 120), every_s: float = 1.0, cost_ticks: float = 1.5,
             min_active: int = 30, data_source: str = "SYNTHETIC") -> dict:
    fwd = {h: forward_moves(samples, h, every_s) for h in horizons}
    result: dict = {"data_source": data_source,
                    "label": SYNTHETIC_LABEL if data_source == "SYNTHETIC" else f"{data_source} - research only",
                    "cost_ticks": cost_ticks, "horizons_s": list(horizons), "samples": len(samples),
                    "note": "samples overlap: counts overstate independent evidence", "features": {}}
    for name, (fn, thr) in FEATURES.items():
        per_h = {}
        best = None
        for h in horizons:
            pairs = [(fn(s.fs), m) for s, m in zip(samples, fwd[h]) if m is not None and s.fs.ok]
            vals = [p[0] for p in pairs]
            moves = [p[1] for p in pairs]
            m = _metrics(vals, moves, thr, cost_ticks)
            by_regime = {}
            for reg in sorted({s.regime.regime for s in samples}):
                rp = [(fn(s.fs), mv) for s, mv in zip(samples, fwd[h]) if mv is not None and s.fs.ok and s.regime.regime == reg]
                if rp:
                    rm = _metrics([p[0] for p in rp], [p[1] for p in rp], thr, cost_ticks)
                    if rm["active"]:
                        by_regime[reg] = rm
            m["by_regime"] = by_regime
            per_h[str(h)] = m
            if m["active"] >= min_active and m["net_ticks"] is not None and (best is None or m["net_ticks"] > best[1]):
                best = (h, m["net_ticks"])
        verdict = (f"EDGE after costs at {best[0]} s ({best[1]:+.2f} ticks)" if best is not None and best[1] > 0
                   else "NO EVIDENCE after costs")
        result["features"][name] = {"threshold": thr, "horizons": per_h, "verdict": verdict}
    return result


def scenario_samples(scenarios: Sequence[str], every_s: float = 1.0, seed: int = 7) -> list[Sample]:
    from ..feeds.synthetic import generate
    out: list[Sample] = []
    for i, sc in enumerate(scenarios):
        evs, _ = generate(sc, seed=seed + i)
        out += sample(evs, every_s=every_s)
    return out


def text(result: dict) -> str:
    lines = [f"FEATURE TEST - {result['label']}",
             f"{result['samples']} samples; cost {result['cost_ticks']} ticks; horizons {result['horizons_s']} s; "
             f"{result['note']}", ""]
    hz = [str(h) for h in result["horizons_s"]]
    lines.append(f"{'feature':22s} " + " ".join(f"{'h=' + h + 's':>26s}" for h in hz) + "  verdict")
    for name, r in result["features"].items():
        cells = []
        for h in hz:
            m = r["horizons"][h]
            cells.append(f"n{m['active']:>5d} hit {m['hit_rate'] if m['hit_rate'] is not None else '-':>5} "
                         f"net {m['net_ticks'] if m['net_ticks'] is not None else '-':>6}")
        lines.append(f"{name:22s} " + " ".join(f"{c:>26s}" for c in cells) + f"  {r['verdict']}")
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m mintel.ian.research.featuretest")
    ap.add_argument("--synthetic", default="", help="comma-separated scenarios, or 'all'")
    ap.add_argument("--replay", default="", help="a recording file or directory")
    ap.add_argument("--instrument", default=None)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    if a.synthetic:
        from ..feeds.synthetic import SCENARIOS
        scs = [s for s in SCENARIOS if s not in ("sequence_gap", "stale_feed", "out_of_order")] \
            if a.synthetic == "all" else a.synthetic.split(",")
        res = evaluate(scenario_samples(scs), data_source="SYNTHETIC")
    elif a.replay:
        from ..feeds.replay import ReplayFeed
        feed = ReplayFeed(a.replay)
        feed.connect()
        evs = []
        while True:
            b = feed.events(50_000)
            if not b:
                break
            evs += b
        res = evaluate(sample(evs, a.instrument), data_source="SYNTHETIC" if feed.synthetic else "REAL RECORDED DATA")
    else:
        ap.print_help()
        return 1
    print(json.dumps(res, indent=2, default=str) if a.json else text(res))
    return 0


if __name__ == "__main__":
    sys.exit(main())
