"""Aaron's standing rules, the evidence part of a trial (G1), and the coach's
own luck check.

Spec sections 6.2 (S1, S2, S3), 6.1 (G1's evidence) and 6.3 (the luck check).

S1 (Aaron's rule, exactly as the spec writes it): an approach in a market
condition traded on 3 or more UK days in the current epoch with a net below
zero over those days is banned from real money (practice continues). Real
trades are counted when it has 3 or more real traded days in the epoch,
otherwise its practice trades with the practice haircut taken off.

Read against the record (tests/test_coach_rules.py): this is the reading
Aaron's own notes bear out - on 9 Oct "no standing rule met (session expansion
in a trend still +26.66 over 14 days; momentum continuation in news +22.77
over 4)", although the first had 4 negative days. Counting negative days
instead would have switched off momentum continuation in fast markets
(Formula 1's own condition: 6 negative days of 12, +31.46) and session
expansion in a trend.

S2 (Aaron's rule): a market (symbol) negative on at least two thirds of 6 or
more traded days goes to practice-only for coach-granted keys, and the
nightly digest recommends adding it to ``universe.excluded_symbols``.

S3: nothing is promoted or demoted as a judgement on fewer than 3 UK days of
evidence (S1 and S2 cannot fire on fewer by construction; ``judgement_allowed``
is there for the other judgements).

The luck check (6.3): the key labels of Trend & Breakout's trades are shuffled
among the trades of each UK day, 200 times, and the evidence part of G1 is run
again on every shuffled key. If more than 2% of the made-up keys pass, there is
no promotion of any kind that night. The shuffle is seeded with the date, so
the same night always gives the same answer.

Money here is after the whole commission; nothing reads today's profit or
loss or the £100 line.
"""
from __future__ import annotations

import datetime as dt
import random
from dataclasses import dataclass, field, replace
from typing import Iterable, Mapping, Optional, Sequence

from . import (G1_MAX_BEST_DAY_SHARE, G1_MIN_CLUSTERS, G1_MIN_DAYS, G1_MIN_FORWARD,
               G1_MIN_NEFF, G1_MIN_P_PLUS, G1_MIN_TRADES, JUDGEMENT_MIN_DAYS, LUCK_MAX_PASS_RATE, LUCK_SHUFFLES,
               S1_MIN_DAYS, S2_MIN_DAYS, S2_SHARE)
from . import epochs as E
from . import estimate as ES
from . import records as R

S1, S2, S3, G1 = "S1", "S2", "S3", "G1"

# Stricter than the spec's "m_fwd > 0", as section 18 allows ("thresholds may
# only be made stricter to pass"): the forward-only record (root prior, no
# pooling) must put its mean at least one standard deviation above zero
# (P+_fwd >= 0.84). Measured on 1,000 made-up null keys (mean 0, sd 1.4 R,
# clustered, 2-4 trades a day, the evidence re-tested every night for 60
# days): the spec's G1 as written let 19% of them reach a trial (33% with tau
# at its 0.30 cap), this one 2.3% whatever tau is; a planted +0.3 R key still
# reaches a trial 60% of the time within the 60 days.
G1_FORWARD_Z = 1.0
# Practice is never credited above real (spec 4.2). The haircut's measured value is capped at 0.30 R by the
# same section's formula, so on a key whose practice runs more than 0.30 R above its real record the main
# chain still credits practice above real. G1 therefore also refuses a key whose own real record says no edge:
# a real-only chain of at least G1_REAL_MIN_NEFF effective trades with P+_real below G1_REAL_MIN_P_PLUS
# (stricter only).
G1_REAL_MIN_NEFF = 20.0
G1_REAL_MIN_P_PLUS = 0.5


def money(v: float) -> str:
    """+£5.52 / -£8.52 (the pound sign, never "GBP")."""
    return f"{'-' if v < 0 else '+'}£{abs(v):,.2f}"


# ------------------------------------------------------------- day nets --
def _haircut_gbp(t, haircuts: Optional[Mapping]) -> float:
    if t.real:
        return 0.0
    h = (haircuts or {}).get(t.key, R.HAIRCUT_DEFAULT)
    return float(getattr(h, "h", h)) * t.risk_gbp


def day_nets(trades: Iterable, haircuts: Optional[Mapping] = None) -> dict:
    """Money after costs by UK day (a practice trade has its haircut, in R,
    taken off at its own risk)."""
    out: dict[dt.date, float] = {}
    for t in trades:
        out[t.uk_day] = out.get(t.uk_day, 0.0) + t.net_gbp - _haircut_gbp(t, haircuts)
    return {d: round(v, 2) for d, v in out.items()}


def _choose(trades: Sequence, min_days: int) -> tuple[str, list]:
    """Real trades when they cover ``min_days`` UK days, else practice."""
    real = [t for t in trades if t.real]
    if len({t.uk_day for t in real}) >= min_days:
        return "real", real
    return "practice", [t for t in trades if not t.real]


@dataclass(frozen=True)
class RuleResult:
    """One standing rule on one target, with its numbers."""
    rule: str                          # S1 | S2
    bot: str
    target: str                        # S1: "APPROACH|CONDITION"; S2: the symbol
    fires: bool
    source: str                        # real | practice
    days: int
    negative_days: int
    net_gbp: float
    day_nets: tuple = ()               # ((UK day, net), ...) oldest first
    since_utc: Optional[dt.datetime] = None

    @property
    def numbers(self) -> dict:
        return {"days": self.days, "negative_days": self.negative_days, "net_gbp": self.net_gbp,
                "source": self.source,
                "day_nets": [[d.isoformat(), v] for d, v in self.day_nets]}

    def words(self) -> str:
        what = (f"{self.target.split('|')[0].replace('_', ' ').lower()} in "
                f"{self.target.split('|')[1].replace('_', ' ').lower()} markets" if self.rule == S1 else self.target)
        rec = "real trades" if self.source == "real" else "practice, less the practice allowance"
        return (f"{what}: {money(self.net_gbp)} after costs over {self.days} UK days traded, "
                f"negative on {self.negative_days} of them ({rec})")


def s1(trades: Iterable, as_of: dt.datetime, *, haircuts: Optional[Mapping] = None,
       epochs: Optional[Sequence] = None, bots: Optional[Iterable[str]] = None) -> list[RuleResult]:
    """S1 for every (bot, approach, condition) with trades in its current
    epoch, as of ``as_of`` (trades closed later are not seen)."""
    as_of = R.to_utc(as_of)
    ep = epochs if epochs is not None else E.EPOCHS
    want = set(bots) if bots is not None else None
    groups: dict[tuple[str, str, str], list] = {}
    starts: dict[str, Optional[dt.datetime]] = {}
    for t in trades:
        if t.closed_utc > as_of or (want is not None and t.bot not in want):
            continue
        if t.key not in starts:
            starts[t.key] = E.latest_epoch(t.key, as_of, ep)
        start = starts[t.key]
        if start is not None and t.closed_utc < start:
            continue
        groups.setdefault((t.bot, t.approach, t.condition), []).append(t)
    out = []
    for (bot, approach, condition), rows in sorted(groups.items()):
        source, used = _choose(rows, S1_MIN_DAYS)
        nets = day_nets(used, haircuts)
        neg = sum(1 for v in nets.values() if v < 0)
        since = min((starts.get(t.key) for t in rows if starts.get(t.key) is not None), default=None)
        net = round(sum(nets.values()), 2)
        out.append(RuleResult(S1, bot, f"{approach}|{condition}", _s1(nets), source, len(nets), neg, net,
                              tuple(sorted(nets.items())), since))
    return out


def s1_fires_on(trades: Iterable, as_of: dt.datetime, *, haircuts: Optional[Mapping] = None,
                epochs: Optional[Sequence] = None) -> bool:
    """Does S1 fire on this one record (a key's own trades, taken as one
    approach in one condition)?"""
    as_of = R.to_utc(as_of)
    ep = epochs if epochs is not None else E.EPOCHS
    rows = []
    starts: dict[str, Optional[dt.datetime]] = {}
    for t in trades:
        if t.closed_utc > as_of:
            continue
        if t.key not in starts:
            starts[t.key] = E.latest_epoch(t.key, as_of, ep)
        if starts[t.key] is not None and t.closed_utc < starts[t.key]:
            continue
        rows.append(t)
    _, used = _choose(rows, S1_MIN_DAYS)
    return _s1(day_nets(used, haircuts))


def _s1(nets: Mapping) -> bool:
    """Traded on >= 3 UK days, and the net over them below zero."""
    return len(nets) >= S1_MIN_DAYS and round(sum(nets.values()), 2) < 0


def s2(trades: Iterable, as_of: dt.datetime, *, haircuts: Optional[Mapping] = None,
       bots: Optional[Iterable[str]] = None) -> list[RuleResult]:
    """S2 for every (bot, symbol) on its whole record to ``as_of``: negative
    on at least two thirds of 6 or more traded UK days."""
    as_of = R.to_utc(as_of)
    want = set(bots) if bots is not None else None
    groups: dict[tuple[str, str], list] = {}
    for t in trades:
        if t.closed_utc > as_of or (want is not None and t.bot not in want):
            continue
        groups.setdefault((t.bot, R._core(t.symbol)), []).append(t)
    out = []
    for (bot, sym), rows in sorted(groups.items()):
        source, used = _choose(rows, S2_MIN_DAYS)
        nets = day_nets(used, haircuts)
        neg = sum(1 for v in nets.values() if v < 0)
        fires = len(nets) >= S2_MIN_DAYS and neg >= S2_SHARE * len(nets) - 1e-9
        out.append(RuleResult(S2, bot, sym, fires, source, len(nets), neg, round(sum(nets.values()), 2),
                              tuple(sorted(nets.items()))))
    return out


def judgement_allowed(trades: Iterable) -> bool:
    """S3: a judgement (not a risk limit) needs at least 3 UK days of evidence."""
    return len({t.uk_day for t in trades}) >= JUDGEMENT_MIN_DAYS


# -------------------------------------------------------- G1's evidence --
@dataclass(frozen=True)
class G1Evidence:
    """The evidence part of G1 (section 6.1) for one key; eligibility and the
    night's conditions (luck check, trial slots, budget, brakes) are the
    decision's, not this."""
    key: str
    passed: bool
    failed: tuple = ()                 # the checks it did not meet, by name
    numbers: dict = field(default_factory=dict)


G1_CHECKS = ("trades", "n_eff", "clusters", "days", "forward", "p_plus", "lcb80", "cost15", "forward_edge",
             "recent_net", "recent_days", "best_day", "standing_rule")


def g1_evidence(est: "ES.KeyEstimate", key_trades: Iterable, as_of: dt.datetime, *,
                haircuts: Optional[Mapping] = None, epochs: Optional[Sequence] = None) -> G1Evidence:
    """Section 6.1, G1's evidence: >= 30 trades (any source), n_eff >= 20,
    >= 12 clusters, on >= 8 UK days; >= 15 trades closed after registration in
    the current epoch; P+ >= 0.90, LCB80 > 0, m_cost15 > 0 and the forward-only
    mean at least one standard deviation above zero (stricter than the spec's
    m_fwd > 0: see G1_FORWARD_Z); in its last 30 UK trading days weighted net
    > 0, days up > days down and the best day <= 40% of the net; no standing
    rule fires on its record."""
    failed = []
    if est.n < G1_MIN_TRADES:
        failed.append("trades")
    if est.n_eff < G1_MIN_NEFF:
        failed.append("n_eff")
    if est.clusters < G1_MIN_CLUSTERS:
        failed.append("clusters")
    if est.days < G1_MIN_DAYS:
        failed.append("days")
    if est.n_forward_epoch < G1_MIN_FORWARD:
        failed.append("forward")
    if est.p_plus < G1_MIN_P_PLUS:
        failed.append("p_plus")
    if not est.lcb80 > 0:
        failed.append("lcb80")
    if not est.m_cost15 > 0:
        failed.append("cost15")
    if est.m_fwd is None or est.s_fwd is None or not est.m_fwd - G1_FORWARD_Z * est.s_fwd > 0:
        failed.append("forward_edge")
    if not est.recent_net > 0:
        failed.append("recent_net")
    if not est.recent_up > est.recent_down:
        failed.append("recent_days")
    if not (est.recent_net > 0 and est.recent_best <= G1_MAX_BEST_DAY_SHARE * est.recent_net):
        failed.append("best_day")
    if s1_fires_on(key_trades, as_of, haircuts=haircuts, epochs=epochs):
        failed.append("standing_rule")
    p_real = getattr(est, "p_plus_real", None)
    if (float(getattr(est, "n_real_eff", 0.0) or 0.0) >= G1_REAL_MIN_NEFF and p_real is not None
            and float(p_real) < G1_REAL_MIN_P_PLUS):
        failed.append("real_record")
    numbers = {"trades": est.n, "n_eff": round(est.n_eff, 2), "clusters": est.clusters, "days": est.days,
               "forward": est.n_forward_epoch, "p_plus": round(est.p_plus, 4), "lcb80": round(est.lcb80, 4),
               "m_cost15": round(est.m_cost15, 4),
               "m_fwd": None if est.m_fwd is None else round(est.m_fwd, 4),
               "s_fwd": None if est.s_fwd is None else round(est.s_fwd, 4),
               "recent_net_r": round(est.recent_net, 3), "recent_up": est.recent_up,
               "recent_down": est.recent_down, "recent_best_r": round(est.recent_best, 3),
               "n_real_eff": round(float(getattr(est, "n_real_eff", 0.0) or 0.0), 2),
               "p_plus_real": None if p_real is None else round(float(p_real), 4)}
    return G1Evidence(est.key, not failed, tuple(failed), numbers)


# --------------------------------------------------------- the luck check --
@dataclass(frozen=True)
class LuckResult:
    """Tonight's luck check: how many made-up keys passed G1's evidence."""
    date: str
    shuffles: int
    keys: tuple                        # the real keys whose shuffled copies were tested
    tested: int                        # made-up keys tested (shuffles x keys)
    passed: int
    rate: float
    blocked: bool                      # more than 2% passed: no promotion of any kind tonight
    seed: int

    @property
    def numbers(self) -> dict:
        return {"date": self.date, "shuffles": self.shuffles, "keys": list(self.keys), "tested": self.tested,
                "passed": self.passed, "rate": round(self.rate, 4), "blocked": self.blocked, "seed": self.seed}


def luck_seed(day: dt.date) -> int:
    """The date as an int (20261012), as for the stop-safety bootstrap."""
    return int(day.strftime("%Y%m%d"))


def candidate_keys(trades: Iterable, bot: str = "tnb") -> list[str]:
    """Keys whose shuffled copies can pass on counts: >= 30 trades on >= 8
    UK days. Shuffling labels within each day keeps both counts, so a key
    below them can never pass and is not counted (counting it would dilute
    the rate)."""
    n: dict[str, int] = {}
    days: dict[str, set] = {}
    for t in trades:
        if t.bot != bot:
            continue
        n[t.key] = n.get(t.key, 0) + 1
        days.setdefault(t.key, set()).add(t.uk_day)
    return sorted(k for k in n if n[k] >= G1_MIN_TRADES and len(days[k]) >= G1_MIN_DAYS)


def luck_check(trades: Iterable, as_of: dt.datetime, *, taus: Optional[Mapping[int, float]] = None,
               registered: Optional[Mapping[str, dt.datetime]] = None, live_keys: Iterable[str] = (),
               pins: Iterable = (), haircuts: Optional[Mapping] = None, shuffles: int = LUCK_SHUFFLES,
               seed: Optional[int] = None, epochs: Optional[Sequence] = None, bot: str = "tnb") -> LuckResult:
    """Section 6.3. The key labels (approach, kind, condition) of the bot's
    trades are shuffled among the trades of each UK day; each shuffled key
    that could pass on counts is run through G1's evidence with tonight's
    tau, registrations and haircuts. More than 2% passing blocks every
    promotion tonight."""
    as_of = R.to_utc(as_of)
    day = R.uk_date(as_of)
    seed = luck_seed(day) if seed is None else int(seed)
    rng = random.Random(seed)
    mine = sorted((t for t in trades if t.bot == bot and t.closed_utc <= as_of),
                  key=lambda t: (t.closed_utc, t.source, t.ticket))
    keys = candidate_keys(mine, bot)
    by_day: dict[dt.date, list[int]] = {}
    for i, t in enumerate(mine):
        by_day.setdefault(t.uk_day, []).append(i)
    labels = [(t.approach, t.kind, t.condition) for t in mine]
    tested = passed = 0
    if keys:
        for _ in range(int(shuffles)):
            new = list(labels)
            for idx in by_day.values():
                lab = [labels[i] for i in idx]
                rng.shuffle(lab)
                for i, l in zip(idx, lab):
                    new[i] = l
            shuffled = [replace(t, approach=a, kind=k, condition=c) for t, (a, k, c) in zip(mine, new)]
            fitted = ES.fit(shuffled, as_of, registered=registered, live_keys=live_keys, pins=pins,
                            haircuts=haircuts, taus=taus, reestimate_tau=False, only_keys=keys, epochs=epochs,
                            real_chain=False)
            grouped: dict[str, list] = {}
            for t in shuffled:
                if t.key in fitted.keys:
                    grouped.setdefault(t.key, []).append(t)
            for k in keys:
                est = fitted.keys.get(k)
                if est is None:
                    continue
                tested += 1
                if g1_evidence(est, grouped.get(k, ()), as_of, haircuts=haircuts, epochs=epochs).passed:
                    passed += 1
    rate = passed / tested if tested else 0.0
    return LuckResult(day.isoformat(), int(shuffles), tuple(keys), tested, passed, rate,
                      rate > LUCK_MAX_PASS_RATE, seed)
