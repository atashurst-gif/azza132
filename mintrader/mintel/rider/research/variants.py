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

RMR-GUARDS-ON / RMR-GUARDS-OFF are the pair for the guards added on 9 Oct
(the currency cluster and the loss cooldown), each forced on or off with
everything else as the baseline. RMR-BASE keeps its definition (the core's
defaults plus rider.json), so with the shipped defaults it is the same
settings as RMR-GUARDS-ON; the pipeline replays identical settings once.
Neither is a walk-forward candidate.
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
                "max_chop": c.max_chop, "curve": (c.flowlock or {}).get("curve", "balanced"),
                "currency_cluster_guard": c.currency_cluster_guard, "loss_cooldown_guard": c.loss_cooldown_guard}

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
    Variant("RMR-GUARDS-ON", "The guards added on 9 Oct switched on: no second position on the same side of a "
            "currency (the currency cluster), and the loss cooldown (15 min on a market after a losing exit, 2 h "
            "after two in a row within 2 h, the losing currency side rested 15 min, and no entry anywhere for 15 min after 4 "
            "losing exits within 10 min). Everything else as the baseline.",
            "Taking one idea once (not once per correlated market) and resting after losses cuts the costs of "
            "repeated losing entries by more than it gives up in later winners.",
            {"currency_cluster_guard": True, "loss_cooldown_guard": True}, ()),
    Variant("RMR-GUARDS-OFF", "The guards added on 9 Oct switched off (no currency cluster, no loss cooldown): "
            "the Rider as it traded on its first live morning. Everything else as the baseline.",
            "The control for RMR-GUARDS-ON: without the guards there are more trades, and correlated repeats lose "
            "more after costs.",
            {"currency_cluster_guard": False, "loss_cooldown_guard": False}, ()),
    Variant("RMR-COST-25", "The cost gate as it was until 9 Oct: costs up to a quarter of the expected move "
            "(the live setting is now 15%).",
            "The control for the 15% gate: allowing smaller moves adds trades whose wins commission swallows.",
            {"max_cost_ratio": 0.25}, ()),
    Variant("RMR-PATIENT", "Hold on longer before calling a trade dead: 'never got going' after 5 minutes "
            "instead of 2.5, and 'went against and kept going' at 0.9 R instead of 0.7 R.",
            "Aaron, 9 Oct: wins are too small - the bot is not riding the highs. More patience lets more trades "
            "become runners; the cost is bigger losses on the ones that fail.",
            {"flowlock": {"no_progress_seconds": 300.0, "adverse_r": 0.9}}, ()),
    Variant("RMR-RIDE-LONGER", "Ride the winners longer: the loose giveback curve AND the patient dead-trade rules.",
            "Together, more room and more patience turn the best moves into the big wins the live morning lacked.",
            {"flowlock": {"curve": "loose", "no_progress_seconds": 300.0, "adverse_r": 0.9}}, ()),
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
    """"default" (the eight above), "all" (plus the extra curves), "base", or
    a comma list of METHOD_IDs. The baseline is always included."""
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
