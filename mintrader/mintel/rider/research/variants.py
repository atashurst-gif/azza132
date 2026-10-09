"""The variants the research actually replays - a SMALL set, decided before
looking at any result. Each is the core with a few settings changed; the
core's code is never changed for research.

RMR-BASE is the core exactly as built (its default settings, plus whatever
Aaron's data/rider.json says). It is the headline: it was fixed BEFORE the
research, so it was not chosen for looking good on this data.

The walk-forward GRID is two short ordered axes through the baseline:

    trigger_score  55 - 60 - 65          (enter earlier vs. demand more)
    giveback curve loose - balanced - tight (give a winner room vs. protect)

and a choice is judged by its own in-sample result AVERAGED with its grid
neighbours', so a lone lucky setting next to bad ones is not picked (prefer
stable parameter ranges). RMR-CHOP-OFF is not a candidate: it tests whether
the anti-chop gate earns its keep.
"""
from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, field
from typing import Optional, Sequence

from ..config import RiderConfig

GRID_AXES = {"trigger_score": (55, 60, 65), "curve": ("loose", "balanced", "tight")}


@dataclass(frozen=True)
class Variant:
    method_id: str
    description: str
    hypothesis: str
    overrides: dict = field(default_factory=dict)       # RiderConfig fields; "flowlock" merges
    grid: tuple = ()                                    # ((axis, level), ...) - empty: not a walk-forward candidate

    @property
    def coords(self) -> dict:
        return dict(self.grid)

    def config(self, base: RiderConfig) -> RiderConfig:
        d = base.to_dict()
        for k, v in self.overrides.items():
            if k == "flowlock":
                fl = dict(d.get("flowlock") or {})
                fl.update(v)
                d["flowlock"] = fl
            else:
                d[k] = v
        return RiderConfig.from_dict(d)

    def params(self, base: RiderConfig) -> dict:
        c = self.config(base)
        return {"overrides": self.overrides, "trigger_score": c.trigger_score, "ready_score": c.ready_score,
                "max_chop": c.max_chop, "curve": (c.flowlock or {}).get("curve", "balanced")}

    def key(self, base: RiderConfig) -> str:
        blob = json.dumps(self.config(base).to_dict(), sort_keys=True, default=str)
        return hashlib.sha1(blob.encode()).hexdigest()[:12]


VARIANTS = (
    Variant("RMR-BASE", "The core exactly as built: trigger at score 60, balanced giveback curve, anti-chop gate on.",
            "Pre-registered baseline: early entries on clean, fast, confirmed moves, managed by FlowLock X, make "
            "money after spread, slippage and commission.",
            {}, (("trigger_score", 60), ("curve", "balanced"))),
    Variant("RMR-TRIG-55", "Trigger at score 55 (ready at 50): enter earlier on weaker evidence.",
            "Getting on earlier catches more of each run; the extra false starts may cost more than that.",
            {"trigger_score": 55.0, "ready_score": 50.0}, (("trigger_score", 55), ("curve", "balanced"))),
    Variant("RMR-TRIG-65", "Trigger at score 65 (ready at 60): demand stronger evidence before entering.",
            "Fewer, cleaner entries win more often; they may also be later and capture less of each run.",
            {"trigger_score": 65.0, "ready_score": 60.0}, (("trigger_score", 65), ("curve", "balanced"))),
    Variant("RMR-CURVE-LOOSE", "FlowLock X giveback curve 'loose' (gives back 70% young, 45% mature, 30% fading).",
            "More room keeps the big rides alive; the cost is giving back more on the ordinary ones.",
            {"flowlock": {"curve": "loose"}}, (("trigger_score", 60), ("curve", "loose"))),
    Variant("RMR-CURVE-TIGHT", "FlowLock X giveback curve 'tight' (gives back 45% young, 22% mature, 12% fading).",
            "Protecting profit harder raises MFE capture on ordinary scalps; it may cut the big rides short.",
            {"flowlock": {"curve": "tight"}}, (("trigger_score", 60), ("curve", "tight"))),
    Variant("RMR-CHOP-OFF", "The anti-chop gate switched off (everything else as the baseline).",
            "The anti-chop gate saves money: without it there are more trades and a worse result after costs.",
            {"max_chop": 1.01}, ()),
)

EXTRA_VARIANTS = (
    Variant("RMR-CURVE-STEEP", "FlowLock X giveback curve 'steep' (matures fast: 65% young, 15% mature).",
            "Locking in hard once a winner has proved itself captures more of the typical run.",
            {"flowlock": {"curve": "steep"}}, ()),
    Variant("RMR-CURVE-FLAT", "FlowLock X giveback curve 'flat_half' (always gives back half).",
            "A simple fixed giveback is as good as a shaped one (a control for the curve idea).",
            {"flowlock": {"curve": "flat_half"}}, ()),
)


def by_id(method_id: str) -> Optional[Variant]:
    for v in VARIANTS + EXTRA_VARIANTS:
        if v.method_id == method_id:
            return v
    return None


def choose(spec: str) -> list[Variant]:
    """"default" (the six above), "all" (plus the extra curves), "base", or a
    comma list of METHOD_IDs. The baseline is always included."""
    s = (spec or "default").strip()
    if s == "default":
        out = list(VARIANTS)
    elif s == "all":
        out = list(VARIANTS + EXTRA_VARIANTS)
    elif s == "base":
        out = [VARIANTS[0]]
    else:
        out = []
        for name in s.split(","):
            v = by_id(name.strip())
            if v is None:
                raise ValueError(f"unknown variant {name.strip()!r}")
            out.append(v)
    if not any(v.method_id == "RMR-BASE" for v in out):
        out.insert(0, VARIANTS[0])
    seen, uniq = set(), []
    for v in out:
        if v.method_id not in seen:
            seen.add(v.method_id)
            uniq.append(v)
    return uniq


def neighbours(v: Variant, pool: Sequence[Variant]) -> list[Variant]:
    """Grid points one step away along exactly one axis."""
    if not v.grid:
        return []
    c = v.coords
    out = []
    for w in pool:
        if not w.grid or w.method_id == v.method_id:
            continue
        o = w.coords
        diff = [a for a in GRID_AXES if c.get(a) != o.get(a)]
        if len(diff) != 1:
            continue
        a = diff[0]
        levels = GRID_AXES[a]
        try:
            if abs(levels.index(c[a]) - levels.index(o[a])) == 1:
                out.append(w)
        except ValueError:
            continue
    return out


def copy_config(cfg: RiderConfig) -> RiderConfig:
    return copy.deepcopy(cfg)
