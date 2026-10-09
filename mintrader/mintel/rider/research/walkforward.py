"""Walk-forward: choose in-sample, judge out-of-sample, never the other way round.

Days are in time order. Each FOLD has an in-sample block (anchored: every
day before it), then a PURGE GAP of ``gap_days`` that is used for nothing
(a trade can run past midnight; the gap keeps an in-sample trade's life
away from the out-of-sample days), then ``oos_days`` out-of-sample days.
The next fold's out-of-sample block starts after a further gap, so no day
is out-of-sample twice::

    days:   0 1 2 3 | 4 | 5 6 | 7 | 8 9
    fold 1: IS IS IS IS  gap  OOS OOS
    fold 2: IS ............... IS  gap  OOS OOS

In each fold the variant is chosen from the SMALL grid (variants.py) using
in-sample trades only: a candidate's net expectancy (pips per trade after
every cost) averaged with its grid neighbours', so one lucky setting
surrounded by poor ones is not chosen; a candidate needs ``min_trades``
in-sample trades to count. The reported result is the chosen variant's
trades on the out-of-sample days - all folds strung together. The best
in-sample figure is never reported as the result.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Sequence

from .stats import summarise
from .variants import Variant, neighbours


@dataclass
class Fold:
    index: int
    is_days: list
    gap_days: list
    oos_days: list

    def to_dict(self) -> dict:
        return {"fold": self.index, "in_sample": [self.is_days[0], self.is_days[-1]] if self.is_days else [],
                "in_sample_days": len(self.is_days), "purged": list(self.gap_days), "out_of_sample": list(self.oos_days)}


def make_folds(days: Sequence[str], is_min: int = 4, oos: int = 2, gap: int = 1, anchored: bool = True) -> list[Fold]:
    days = sorted(days)
    n = len(days)
    out: list[Fold] = []
    start = is_min
    while start + gap + oos <= n:
        is_days = days[:start] if anchored else days[start - is_min:start]
        out.append(Fold(len(out) + 1, list(is_days), list(days[start:start + gap]),
                        list(days[start + gap:start + gap + oos])))
        start += gap + oos
    return out


def _expectancy(trades: list[dict]) -> Optional[float]:
    if not trades:
        return None
    return sum(float(t["net_pips"]) for t in trades) / len(trades)


@dataclass
class FoldResult:
    fold: Fold
    chosen: str
    why: str
    in_sample: dict
    out_of_sample: dict
    candidates: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {**self.fold.to_dict(), "chosen": self.chosen, "why": self.why, "in_sample_result": self.in_sample,
                "out_of_sample_result": self.out_of_sample, "candidates": self.candidates}


def select(trades_by_variant: dict, grid: Sequence[Variant], days: Sequence[str], min_trades: int = 20,
           fallback: str = "RMR-BASE") -> tuple[str, str, list]:
    """Pick from the grid using only ``days`` (in-sample)."""
    dayset = set(days)
    exp: dict[str, Optional[float]] = {}
    counts: dict[str, int] = {}
    for v in grid:
        tr = [t for t in trades_by_variant.get(v.method_id, []) if t["day"] in dayset]
        counts[v.method_id] = len(tr)
        exp[v.method_id] = _expectancy(tr) if len(tr) >= min_trades else None
    rows = []
    best, best_score = None, None
    for v in grid:
        own = exp[v.method_id]
        if own is None:
            rows.append({"variant": v.method_id, "trades": counts[v.method_id], "expectancy": None, "smoothed": None})
            continue
        vals = [own] + [exp[w.method_id] for w in neighbours(v, grid) if exp.get(w.method_id) is not None]
        sm = sum(vals) / len(vals)
        rows.append({"variant": v.method_id, "trades": counts[v.method_id], "expectancy": round(own, 4),
                     "smoothed": round(sm, 4), "neighbours_used": len(vals) - 1})
        if best_score is None or sm > best_score + 1e-12:
            best, best_score = v.method_id, sm
    if best is None:
        return fallback, (f"no grid setting had {min_trades} in-sample trades - kept the baseline"), rows
    why = f"best in-sample expectancy averaged with its grid neighbours ({best_score:+.3f} pips a trade)"
    if best_score <= 0:
        why += " - note: even this lost money in-sample"
    return best, why, rows


def walk_forward(trades_by_variant: dict, grid: Sequence[Variant], days: Sequence[str], *, is_min: int = 4,
                 oos: int = 2, gap: int = 1, min_trades: int = 20) -> dict:
    folds = make_folds(days, is_min, oos, gap)
    if not folds:
        return {"folds": [], "enough_days": False, "oos_trades": [], "oos_result": summarise([]),
                "statement": (f"Not enough days for a walk-forward test: {len(days)} days, need at least "
                              f"{is_min + gap + oos} ({is_min} in-sample, {gap} purged, {oos} out-of-sample).")}
    results = []
    oos_trades: list[dict] = []
    for f in folds:
        chosen, why, rows = select(trades_by_variant, grid, f.is_days, min_trades)
        is_tr = [t for t in trades_by_variant.get(chosen, []) if t["day"] in set(f.is_days)]
        oos_tr = [t for t in trades_by_variant.get(chosen, []) if t["day"] in set(f.oos_days)]
        oos_trades += oos_tr
        results.append(FoldResult(f, chosen, why, summarise(is_tr, len(f.is_days)),
                                  summarise(oos_tr, len(f.oos_days)), rows))
    oos_days = sorted({d for f in folds for d in f.oos_days})
    res = summarise(oos_trades, len(oos_days))
    base_oos = summarise([t for t in trades_by_variant.get("RMR-BASE", []) if t["day"] in set(oos_days)], len(oos_days))
    return {"folds": [r.to_dict() for r in results], "enough_days": True, "oos_days": oos_days,
            "oos_trades": oos_trades, "oos_result": res, "baseline_on_oos_days": base_oos,
            "rules": {"in_sample_min_days": is_min, "purge_gap_days": gap, "out_of_sample_days": oos,
                      "min_in_sample_trades": min_trades, "anchored": True},
            "statement": (f"{len(folds)} fold(s); out-of-sample days: {', '.join(oos_days)}. The result below is "
                          f"ONLY the chosen settings' trades on days they were not chosen on.")}
