"""Tweaks: tighten-only floors, judged on the same trades.

Spec section 13. "Tweak bots off the back of real results" is done in the one
way that is sound on this much data: a floor that sends a key's weaker
signals to practice. Each floor is tested on the trades the key actually
took (the comparison is paired), and a trade sent away keeps practising, so
a floor can be judged again and reverted.

Pre-registered challengers, written here and nowhere else:

    Trend & Breakout key   min_score = the bot's threshold + 3, + 6 (journal "opportunity")
                           min_rr    = 2.0, 2.2 (journal "reward_risk")
    Financial Ian          min_score = its entry score + 5 (its "score" column)

The spec says "at most 3 per bot" and corrects 0.95 for 3 challengers
(0.983), yet lists four for Trend & Breakout. All four written ones are kept
and the correction is made for the number actually tested: Trend & Breakout's
switch needs P(mu > 0) >= 1 - 0.05 / 4 = 0.9875, never less than 0.983 -
stricter, never looser.

Paired statistic: for an incumbent trade i, delta_i = -x_i if the challenger
would have skipped it, else 0; weights and clusters as in section 4. Prior
mu ~ N(0, 0.10^2); the data variance is cluster-robust (the larger of the
cluster-robust variance and sigma^2 / n_eff, sigma unfloored).

Switch (nightly, released 06:30 UK): >= 40 incumbent trades over >= 8 UK days
with >= 10 would-be-skipped; mean delta >= +0.05 R per incumbent trade;
P(mu > 0) >= the corrected level; at most 1 floor change per bot a night.
Revert: 40 forward incumbent trades (the floored ones counted through their
practice continuation) give P(mu > 0) < 0.5.

A floor is never looser than Aaron's own setting (a challenger at or below
it is not tested), never applies to a pinned key (Formula 1), and never
writes config.json. Loosening ideas belong to the nightly digest.
"""
from __future__ import annotations

import datetime as dt
import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping, Optional, Sequence

from . import (CHALLENGER_MIN_RR, CHALLENGER_SCORE_STEPS, IAN_CHALLENGER_SCORE_STEP, MAX_FLOOR_CHANGES_NIGHT,
               TWEAK_MIN_DAYS, TWEAK_MIN_DELTA_R, TWEAK_MIN_P, TWEAK_MIN_SKIPPED, TWEAK_MIN_TRADES,
               TWEAK_PRIOR_SD_R, TWEAK_REVERT_P, TWEAK_REVERT_TRADES, Floor)
from . import epochs as E
from . import estimate as ES
from . import records as R

MIN_SCORE, MIN_RR = "min_score", "min_rr"
# Re-tested every night on growing data, a challenger would switch on from pure noise far more often than its
# level says (verifier: 21% of made-up null keys within 60 nights). Allowing for the repeats (stricter only):
# a switch needs P(mu > 0) >= RETESTED_MIN_P, and must also have passed on the key's record as it stood
# SWITCH_CONFIRM_BACK and 2 x SWITCH_CONFIRM_BACK traded UK days earlier (inside the spec's 8-day minimum).
# Measured on 160 made-up null keys over 60 nights: 3.8% switched (19.4% before); a planted real effect (scores
# below 76 lose 0.8 R more) still switched on 50% of keys.
SWITCH_CONFIRM_BACK = 4
RETESTED_MIN_P = 0.993
FAMILYWISE = 0.05                      # the 0.95 the switch level corrects
TIER_FIELDS = {"NORMAL": "tier_normal", "STRONG": "tier_strong", "EXCEPTIONAL": "tier_exceptional"}
IAN_DEFAULT_ENTRY_SCORE = 60.0         # mintel.ian.engine's default entry_score


@dataclass(frozen=True)
class Challenger:
    """One pre-registered floor for one key: skip (to practice) when the
    field is below ``value``."""
    field: str                         # min_score | min_rr
    value: float

    def skips(self, trade) -> bool:
        v = trade.score if self.field == MIN_SCORE else trade.rr
        return v is not None and math.isfinite(v) and v < self.value - 1e-9

    @property
    def label(self) -> str:
        return f"{self.field} {self.value:g}"


def switch_level(n_challengers: int) -> float:
    """P(mu > 0) a switch needs: 0.95 corrected for the challengers tested,
    never below 0.983 (the spec's level for 3)."""
    return max(TWEAK_MIN_P, 1.0 - FAMILYWISE / max(1, int(n_challengers)))


# ------------------------------------------------------- the challengers --
def tnb_threshold(cfg, approach: str) -> float:
    """Trend & Breakout's own score floor for an approach: its entry tier's
    threshold, or the approach's own minimum when higher."""
    scan = getattr(cfg, "scan", None)
    tier = str(getattr(scan, "entry_tier", "NORMAL") or "NORMAL").upper()
    base = float(getattr(scan, TIER_FIELDS.get(tier, "tier_normal"), 0.0) or 0.0)
    own = (getattr(scan, "tactic_min_score", None) or {}).get(str(approach).upper())
    try:
        own = float(own) if own is not None else None
    except (TypeError, ValueError):
        own = None
    return max(base, own) if own is not None else base


def ian_threshold(data_dir=None) -> float:
    """Financial Ian's entry score, from data/ian.json (read with json only)."""
    if data_dir is not None:
        try:
            raw = json.loads((Path(data_dir) / "ian.json").read_text())
            v = float(raw.get("entry_score"))
            if math.isfinite(v):
                return v
        except Exception:
            pass
    return IAN_DEFAULT_ENTRY_SCORE


def challengers_for(key: str, cfg, *, data_dir=None) -> list[Challenger]:
    """The pre-registered challengers for one key, each stricter than Aaron's
    own setting (a challenger at or below it would loosen nothing and is not
    tested)."""
    bot, approach, _, _ = R.split_key(key)
    out: list[Challenger] = []
    if bot == "tnb":
        base = tnb_threshold(cfg, approach)
        out += [Challenger(MIN_SCORE, base + step) for step in CHALLENGER_SCORE_STEPS]
        own_rr = float(getattr(getattr(cfg, "scan", None), "min_reward_risk", 0.0) or 0.0)
        out += [Challenger(MIN_RR, rr) for rr in CHALLENGER_MIN_RR if rr > own_rr + 1e-9]
    elif bot == "ian":
        out.append(Challenger(MIN_SCORE, ian_threshold(data_dir) + IAN_CHALLENGER_SCORE_STEP))
    return out


def n_registered(bot: str) -> int:
    """How many challengers are written for a bot (the correction's count)."""
    if bot == "tnb":
        return len(CHALLENGER_SCORE_STEPS) + len(CHALLENGER_MIN_RR)
    if bot == "ian":
        return 1
    return 0


# ------------------------------------------------------ the paired test --
@dataclass(frozen=True)
class Paired:
    """The paired comparison of one challenger on one key's incumbent trades."""
    key: str
    challenger: Challenger
    trades: int
    days: int
    skipped: int
    mean_delta: float                  # weighted mean of delta, R per incumbent trade
    m: float                           # posterior mean of mu
    s: float
    p: float                           # P(mu > 0)
    clusters: int

    @property
    def numbers(self) -> dict:
        return {"key": self.key, "floor": self.challenger.label, "trades": self.trades, "days": self.days,
                "skipped": self.skipped, "mean_delta_r": round(self.mean_delta, 4), "m": round(self.m, 4),
                "s": round(self.s, 4), "p": round(self.p, 4), "clusters": self.clusters}


def paired(key: str, trades: Sequence, challenger: Challenger, as_of: dt.datetime, *,
           haircuts: Optional[Mapping] = None, epochs: Optional[Sequence] = None) -> Paired:
    """delta_i = -x_i when the challenger would have skipped trade i, else 0,
    with the coach's weights and clusters; posterior of mu under N(0, 0.10^2)."""
    obs = ES.prepare(trades, as_of, haircuts=haircuts, epochs=epochs)
    a = ES.Agg()
    skipped = 0
    for o in obs:
        skip = challenger.skips(o.trade)
        skipped += int(skip)
        # the trade's own outcome, without the practice haircut: the comparison is within one key and one
        # record, so taking the haircut off would make skipping practice trades look better by h each
        a.add(o.w, -o.trade.x if skip else 0.0, o.cluster)
    days = len({o.day for o in obs})
    if a.n == 0 or a.sw <= 0:
        return Paired(key, challenger, 0, 0, 0, 0.0, 0.0, TWEAK_PRIOR_SD_R, 0.5, 0)
    W = a.sw
    mean = a.swx / W
    var = max(0.0, a.swx2 / W - mean * mean)
    n_eff = W * W / a.sw2 if a.sw2 > 0 else 0.0
    v_clu = sum((c[0] - mean * c[2]) ** 2 for c in a.clu.values()) / (W * W)
    v_data = max(v_clu, var / n_eff if n_eff > 0 else math.inf)
    v0 = TWEAK_PRIOR_SD_R ** 2
    if v_data > 0 and math.isfinite(v_data):
        m, v = ES.posterior(0.0, v0, mean, v_data)
    else:
        m, v = 0.0, v0                                    # every delta 0: nothing learned
    s = math.sqrt(v)
    return Paired(key, challenger, a.n, days, skipped, mean, m, s, ES.phi(m / s) if s > 0 else 0.5, len(a.clu))


def switch_ok(p: Paired, level: float) -> tuple[bool, tuple[str, ...]]:
    """Section 13's switch test (the night's one-change limit is the caller's)."""
    failed = []
    if p.trades < TWEAK_MIN_TRADES:
        failed.append("trades")
    if p.days < TWEAK_MIN_DAYS:
        failed.append("days")
    if p.skipped < TWEAK_MIN_SKIPPED:
        failed.append("skipped")
    if p.mean_delta < TWEAK_MIN_DELTA_R:
        failed.append("mean_delta")
    if p.p < level:
        failed.append("p")
    return not failed, tuple(failed)


def confirmed(key: str, incumbent: Sequence, challenger: Challenger, as_of: dt.datetime, level: float, *,
              haircuts: Optional[Mapping] = None, epochs: Optional[Sequence] = None,
              back: int = SWITCH_CONFIRM_BACK) -> bool:
    """Did the switch's evidence (mean delta and P(mu > 0); the minimum counts are tonight's) also hold on the
    key's record as it stood ``back`` and ``2 x back`` traded UK days earlier? (Allows for the nightly
    re-testing; stricter only.)"""
    days = sorted({t.uk_day for t in incumbent})
    for k in (back, 2 * back):
        if len(days) <= k:
            return False
        cut = days[-k - 1]
        part = [t for t in incumbent if t.uk_day <= cut]
        q = paired(key, part, challenger, as_of, haircuts=haircuts, epochs=epochs)
        if q.mean_delta < TWEAK_MIN_DELTA_R or q.p < level:          # the evidence then (the counts are now's)
            return False
    return True


def revert_due(p: Paired) -> bool:
    """Revert: 40 forward incumbent trades give P(mu > 0) < 0.5."""
    return p.trades >= TWEAK_REVERT_TRADES and p.p < TWEAK_REVERT_P


# --------------------------------------------------------- the nightly --
@dataclass(frozen=True)
class FloorChange:
    """One floor switched on or reverted tonight, with its numbers."""
    key: str
    action: str                        # switch | revert
    field: str
    value: Optional[float]             # the new floor (None: removed)
    before: Optional[float]
    numbers: dict = field(default_factory=dict)


@dataclass
class FloorState:
    """A floor in force: since when (its forward window starts then)."""
    floor: Floor
    since_utc: dt.datetime


def _incumbent(trades: Iterable, floor: Optional[Floor]) -> list:
    """The trades the current rule takes (those a floor in force would have
    sent to practice are not incumbent for a stricter challenger)."""
    if floor is None:
        return list(trades)
    out = []
    for t in trades:
        if floor.min_score is not None and t.score is not None and t.score < floor.min_score - 1e-9:
            continue
        if floor.min_rr is not None and t.rr is not None and t.rr < floor.min_rr - 1e-9:
            continue
        out.append(t)
    return out


def _is_pinned(key: str, pins: Iterable) -> bool:
    for p in pins or ():
        pat = str(getattr(p, "key", p))
        if E.matches(pat, key) or E.matches(key, pat):
            return True
    return False


def nightly_floors(trades: Iterable, as_of: dt.datetime, cfg, *, current: Optional[Mapping[str, FloorState]] = None,
                   pins: Iterable = (), haircuts: Optional[Mapping] = None, data_dir=None,
                   epochs: Optional[Sequence] = None, bots: Sequence[str] = ("tnb", "ian")) -> list[FloorChange]:
    """Tonight's floor changes: at most one per bot (a switch before a
    revert: tightening first), never on a pinned key, never looser than
    Aaron's settings. ``current``: the floors in force (key -> FloorState)."""
    current = dict(current or {})
    by_key: dict[str, list] = {}
    for t in trades:
        if t.bot in bots and t.closed_utc <= R.to_utc(as_of):
            by_key.setdefault(t.key, []).append(t)
    out: list[FloorChange] = []
    for bot in bots:
        switches, reverts = [], []
        level = max(switch_level(n_registered(bot)), RETESTED_MIN_P)
        for key in sorted(k for k in by_key if R.split_key(k)[0] == bot):
            if _is_pinned(key, pins):
                continue
            rows = by_key[key]
            state = current.get(key)
            floor = state.floor if state is not None else None
            # revert check on the forward window of a floor in force
            if state is not None:
                fwd = [t for t in rows if t.closed_utc > R.to_utc(state.since_utc)]
                for fld in (MIN_SCORE, MIN_RR):
                    val = getattr(floor, fld)
                    if val is None:
                        continue
                    p = paired(key, fwd, Challenger(fld, float(val)), as_of, haircuts=haircuts, epochs=epochs)
                    if revert_due(p):
                        reverts.append(FloorChange(key, "revert", fld, None, float(val), p.numbers))
            inc = _incumbent(rows, floor)
            for ch in challengers_for(key, cfg, data_dir=data_dir):
                have = getattr(floor, ch.field) if floor is not None else None
                if have is not None and ch.value <= float(have) + 1e-9:
                    continue                                 # never looser than the floor in force
                p = paired(key, inc, ch, as_of, haircuts=haircuts, epochs=epochs)
                ok, _ = switch_ok(p, level)
                if ok and confirmed(key, inc, ch, as_of, level, haircuts=haircuts, epochs=epochs):
                    switches.append(FloorChange(key, "switch", ch.field, ch.value, have,
                                                dict(p.numbers, level=round(level, 4))))
        pick = sorted(switches, key=lambda c: (-c.numbers["p"], -c.numbers["mean_delta_r"], c.key, c.field,
                                               c.value))
        if not pick:
            pick = sorted(reverts, key=lambda c: (c.numbers["p"], c.key, c.field))
        out.extend(pick[:MAX_FLOOR_CHANGES_NIGHT])
    return out


def apply_changes(current: Mapping[str, FloorState], changes: Iterable[FloorChange],
                  when: dt.datetime) -> dict:
    """The floors after tonight's changes (key -> FloorState); a revert
    removes that field, a switch sets it (never lower than before)."""
    out = dict(current)
    for c in changes:
        st = out.get(c.key)
        fl = st.floor if st is not None else Floor()
        vals = {"min_score": fl.min_score, "min_rr": fl.min_rr}
        if c.action == "revert":
            vals[c.field] = None
        else:
            before = vals.get(c.field)
            vals[c.field] = c.value if before is None else max(float(before), float(c.value))
        new = Floor(**vals)
        if new.min_score is None and new.min_rr is None:
            out.pop(c.key, None)
        else:
            out[c.key] = FloorState(new, R.to_utc(when))
    return out
