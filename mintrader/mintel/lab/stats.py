"""Statistics: how the lab decides whether a result is skill or luck.

Per-day figures are per UK trading day, net pounds, 0 on a day without a
trade. The luck figure everywhere is the same one: a one-sided STUDENTISED
block bootstrap over days (blocks of up to 5, 2,000 resamples, seeded from the
candidate's id so it is deterministic), null hypothesis "the mean day nets
nothing or less". The block is 5 days only when there are 100 or more days to
resample; on the 20 to 55 day windows the lab really has it shrinks (see
``effective_block`` for the measurement that says why). Studentised means each
resample is judged by its mean divided by its own spread (a bootstrap-t), not by
its mean alone: on 20 to 45 days the plain bootstrap passed the 0.005 line two to
ten times too often, worst for ideas that win small and often and lose big and
rarely (see ``block_bootstrap_p``).

The five checks on every row (spec section 5), PAST / FRESH / PRACTICE:

    Enough trades   60 on a window of >= 20 days | 30 | 30 over >= 10 days
    Steady          net > 0, PF >= 1.2, 3 of 4 equal chunks up and the worst no
                    worse than -0.5 x the total, more days up than down, the best
                    day <= 40% of the net, a plateau | net > 0, PF >= 1.1,
                    days up > down | PF >= 1.15, days up > down
    Fresh prices    - | p <= FRESH_P (= 0.15 / the 30 looks a stretch allows) | carried
    Costs           still above zero at 1.5 x the costs; 2 x is shown
    Luck check      "Chance this is luck: about 1 in N", N = round(1 / min(1, p x m))

The constants are written once, in ``mintel/lab/__init__.py``. DSR (the
deflated Sharpe ratio) is SHOWN and used to rank, never used as a gate: at 60
days nothing clears it. Family gates (PBO over combinatorially purged splits)
block a family's forwards when the family looks like it is fitting noise.

Aaron, 10 Oct 2026: "ensure that consistency stays, luck doesn't".
"""
from __future__ import annotations

import hashlib
import math
import random
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Optional, Sequence

from ..research.validation import (cost_stress, cpcv_splits, deflated_sharpe_ratio,
                                   parameter_stability, sharpe)
from . import (BOOTSTRAP_BLOCK, BOOTSTRAP_RESAMPLES, CHECK_COSTS, CHECK_KEYS, CHECK_FRESH, CHECK_LABELS,
               CHECK_LUCK, CHECK_STEADY, CHECK_TRADES, COST_STRESS_PASS, COST_STRESS_SHOWN,
               FAILED, FRESH_DAYS_MIN, FRESH_FAMILYWISE, FRESH_LOOKS_PER_STRETCH, FRESH_MIN_TRADES,
               FRESH_P, FRESH_PF, NOT_YET, PASSED, PAST_ASSUMED_SPREAD_MAX, PAST_BEST_DAY_SHARE,
               PAST_CHUNK_WORST, PAST_CHUNKS, PAST_CHUNKS_UP, PAST_MIN_DAYS,
               PAST_MIN_TRADES, PAST_P_SHOWN, PAST_PF, PBO_GROUPS, PBO_MAX, PBO_MIN_MEMBERS,
               PBO_MIN_TRADES, PBO_TEST_GROUPS, PLATEAU_TOLERANCE, PRACTICE_LOOK_M,
               PRACTICE_MIN_DAYS, PRACTICE_MIN_TRADES, PRACTICE_P, PRACTICE_PF, REASON_EXTEND_GRID,
               REASON_FITS_NOISE, REASON_PLATEAU_PENDING, RUNNING, FRESH_LOST, FRESH_TOO_FEW,
               Result)

__all__ = ["FRESH_P", "CHECK_KEYS", "TestStats", "Check", "Verdict", "summarize", "past_verdict",
           "fresh_verdict", "practice_verdict", "block_bootstrap_p", "luck_line",
           "luck_of_daily", "plateau_state", "missing_siblings", "family_gate", "dsr_of",
           "pass_rates",
           "seed_of", "profit_factor", "chunk_nets", "cost_nets", "effective_block"]

PF_CAP = 99.0                      # profit factor when nothing was lost
_TINY = 1e-18                      # a spread this small is no spread
LUCK_N_CAP = 99999
EXTEND = "extend"


# ------------------------------------------------------------ the numbers ---
def seed_of(ident: str) -> int:
    """The bootstrap seed: ``int(id[4:12], 16)`` for a lab id; a hash of the
    text for any other id (a bot's)."""
    try:
        return int(ident[4:12], 16)
    except ValueError:
        return int(hashlib.sha1(ident.encode("utf-8")).hexdigest()[:8], 16)


def profit_factor(nets: Iterable[float]) -> float:
    win = loss = 0.0
    for x in nets:
        if x > 0:
            win += x
        elif x < 0:
            loss -= x
    if loss <= 0:
        return PF_CAP if win > 0 else 0.0
    return min(PF_CAP, win / loss)


def effective_block(n: int, block: int = BOOTSTRAP_BLOCK) -> int:
    """The block length actually used for ``n`` days: blocks of 5, but never more
    than a twentieth of the days (so single days under 40 days, pairs from 40,
    five only from 100 days).

    A deviation, measured. With a window of 20 to 55 days a block of 5 leaves
    only 4 to 11 blocks to resample, so the bootstrap's own spread is itself
    mostly luck: on pure noise it passed the 0.005 line 3 to 4 times in 100
    (20 to 25 days) and 1 to 2 in 100 (45 to 55 days) instead of half of one,
    and the 0.05 line 8 to 13 times in 100. Single days pass the 0.005 line
    about once in 100 and the 0.05 line about 5 to 6 times in 100."""
    return max(1, min(block, n // 20))


def block_bootstrap_p(daily: Sequence[float], ident: str, *, block: Optional[int] = None,
                      resamples: int = BOOTSTRAP_RESAMPLES) -> float:
    """One-sided p that the mean daily net is above zero by chance.

    A bootstrap-t. The days are centred on their own mean (the null world: a mean
    of nothing), resampled in blocks of consecutive days (wrapping round, so a
    streak stays a streak), and each resample is scored by its mean divided by
    its own standard error. p is the share of resamples scoring at least what the
    real days scored, counting the real one (so it is never exactly 0). Blocks
    are 5 days when there are enough days to resample them (see
    ``effective_block``); pass ``block=5`` for the literal five-day blocks at any
    length. Deterministic: the seed comes from the id. Fewer than two days
    cannot be judged: p = 1.

    Why studentised (measured on 3,000 made-up windows per case, zero-mean payoffs, two
    trades a day, 20 to 45 days; the share that passed the 0.005 line by luck):

        a world where                       plain bootstrap     bootstrap-t
        fair coin-flips or a bell curve     0.7 to 1.4%         0.2 to 0.7%
        winners small and frequent,
          losers big and rare               2 to 11%            0 to 1.4%
        a smoothly lopsided world
          (rare big losses)                 4.5 to 6.3%         1.0 to 1.7%
        winners bigger than losers          0.03 to 0.6%        0.3 to 0.4%

    (nominal 0.5%). The plain bootstrap trusts the spread of the few days it happened to
    see, which is exactly what a lucky run of small wins hides; scoring each resample by
    mean / spread does not. The 1.4% is the extreme case (a loss nine times the win, on
    20 days). The lopsided world is the remaining weak spot: a window of 20 to 45 days
    cannot show a loss that comes once in a hundred trades."""
    n = len(daily)
    if n < 2:
        return 1.0
    mean = sum(daily) / n
    x = [d - mean for d in daily]
    var = sum(v * v for v in x) / (n - 1)
    if var > _TINY * (1.0 + mean * mean):
        t_obs = mean / math.sqrt(var / n)
    else:                                   # every day the same: nothing to measure a spread by
        t_obs = math.inf if mean > 1e-12 else (-math.inf if mean < -1e-12 else 0.0)
    b = max(1, min(block if block else effective_block(n), n))
    nb = -(-n // b)
    ext = x + x[:b - 1]
    pre, pre2 = [0.0], [0.0]
    for v in ext:
        pre.append(pre[-1] + v)
        pre2.append(pre2[-1] + v * v)
    rnd = random.Random(seed_of(ident)).random
    total = nb * b
    hits = 0
    sqrt = math.sqrt
    for _ in range(resamples):
        sm = sq = 0.0
        for _k in range(nb):
            i = int(rnd() * n)
            sm += pre[i + b] - pre[i]
            sq += pre2[i + b] - pre2[i]
        m = sm / total
        v = (sq - sm * sm / total) / (total - 1)
        if v > _TINY:
            t = m / sqrt(v / total)
        else:
            t = 0.0 if abs(m) < 1e-12 else math.copysign(math.inf, m)
        if t >= t_obs - 1e-9:
            hits += 1
    return (hits + 1) / (resamples + 1)


def luck_n(p: float, m: int) -> int:
    """N in 'about 1 in N': round(1 / min(1, p x m)), at least 1."""
    q = min(1.0, max(p, 1e-12) * max(m, 1))
    return int(min(LUCK_N_CAP, max(1, round(1.0 / q))))


def luck_line(p: float, m: int = 1) -> str:
    """The plain-English luck line shown on the page."""
    return f"Chance this is luck: about 1 in {luck_n(p, m)}"


def luck_of_daily(daily: Sequence[float], ident: str, m: int = 1) -> dict:
    """p and the sentence for any series of daily nets (a bot's real days, m=1,
    'picked by hand, not from a grid')."""
    p = block_bootstrap_p(daily, ident)
    return {"p": p, "n": luck_n(p, m), "m": m, "text": luck_line(p, m)}


def chunk_nets(daily: Sequence[float], chunks: int = PAST_CHUNKS) -> list:
    """Net of each of ``chunks`` equal contiguous slices of the days (the first
    ones take the odd days)."""
    n = len(daily)
    base, extra = divmod(n, chunks)
    out, i = [], 0
    for k in range(chunks):
        size = base + (1 if k < extra else 0)
        out.append(sum(daily[i:i + size]))
        i += size
    return out


def cost_nets(trades: Sequence) -> dict:
    """Net pounds at 1, 1.5, 2 and 3 times the costs (``validation.cost_stress``)."""
    rows = [{"cost_gbp": t.cost_gbp, "net_gbp": t.net_gbp} for t in trades]
    out = cost_stress(rows, cost_key="cost_gbp", pnl_key="net_gbp")
    return {float(r["cost_multiple"]): float(r["net"]) for r in out}


_ANNUAL = math.sqrt(252.0)           # annualised Sharpe = per-day Sharpe x this


def sharpe_of(daily: Sequence[float]) -> float:
    return sharpe(list(daily), 252.0) if len(daily) >= 2 else 0.0


def dsr_of(daily: Sequence[float], n_trials: int, trial_sharpes: Optional[Sequence[float]] = None
           ) -> float:
    """Deflated Sharpe ratio of the daily nets against the real number of trials
    (SHOWN and used to rank; never a gate). 0.0 when there is too little data.

    ``trial_sharpes`` are the ANNUALISED Sharpes the ledger holds
    (``LabLedger.family_sharpes``, ``TestStats.sharpe``). The library wants them per
    day, so they are divided by the square root of 252 here: fed as they come, the
    spread of trials is 16 times too wide, the bar to clear is 16 times too high and
    every DSR comes out as exactly 0, which would quietly turn the forwarding rank
    "DSR, then p" into "p" alone."""
    per_day = ([float(x) / _ANNUAL for x in trial_sharpes] if trial_sharpes else None)
    out = deflated_sharpe_ratio(list(daily), n_trials=max(1, int(n_trials)),
                                trial_sharpes=per_day)
    return float(out.get("dsr") or 0.0)


def max_drawdown_r(trades: Sequence) -> float:
    """Worst peak-to-trough of the running R total, trades in the order closed."""
    peak = cur = worst = 0.0
    for t in sorted(trades, key=lambda t: (t.closed_utc, t.opened_utc)):
        cur += t.r
        if cur > peak:
            peak = cur
        if peak - cur > worst:
            worst = peak - cur
    return worst


# ------------------------------------------------------------- the report ---
@dataclass
class TestStats:
    """The figures behind one test, as the ``tests`` table holds them."""
    __test__ = False                   # not a pytest class
    kind: str = "past"
    window_from: str = ""
    window_to: str = ""
    days: int = 0                      # days in the window the candidate was judged on
    days_traded: int = 0
    trades: int = 0
    net_gbp: float = 0.0
    cost_gbp: float = 0.0
    pf: float = 0.0
    win_rate: float = 0.0
    up_days: int = 0
    down_days: int = 0
    best_day_gbp: float = 0.0
    p: float = 1.0
    folds_up: int = 0
    folds_worst: float = 0.0
    plateau: Optional[str] = None
    cost15_net: float = 0.0
    cost20_net: float = 0.0
    dsr: float = 0.0
    max_dd_r: float = 0.0
    assumed_share: float = 0.0
    sharpe: float = 0.0
    expectancy_r: float = 0.0
    synthetic: bool = False
    passed: Optional[bool] = None

    def to_cols(self) -> dict:
        """The columns ``LabLedger.finish_test`` writes."""
        return {"window_from": self.window_from, "window_to": self.window_to,
                "days": self.days, "trades": self.trades, "net_gbp": self.net_gbp,
                "cost_gbp": self.cost_gbp, "pf": self.pf, "win_rate": self.win_rate,
                "up_days": self.up_days, "down_days": self.down_days,
                "best_day_gbp": self.best_day_gbp, "p": self.p, "folds_up": self.folds_up,
                "folds_worst": self.folds_worst, "plateau": self.plateau,
                "cost15_net": self.cost15_net, "cost20_net": self.cost20_net,
                "dsr": self.dsr, "max_dd_r": self.max_dd_r, "assumed_share": self.assumed_share,
                "passed": None if self.passed is None else int(self.passed)}

    def trial_extra(self) -> dict:
        return {"sharpe": self.sharpe, "expectancy_r": self.expectancy_r,
                "max_drawdown_pct": self.max_dd_r}

    @classmethod
    def from_row(cls, row: Mapping, daily: Optional[Mapping] = None) -> "TestStats":
        """Rebuild from a ``tests`` row (for re-grading after the family is done)."""
        s = cls(kind=row.get("kind", "past"), window_from=row.get("window_from") or "",
                window_to=row.get("window_to") or "", days=int(row.get("days") or 0),
                trades=int(row.get("trades") or 0), net_gbp=float(row.get("net_gbp") or 0.0),
                cost_gbp=float(row.get("cost_gbp") or 0.0), pf=float(row.get("pf") or 0.0),
                win_rate=float(row.get("win_rate") or 0.0), up_days=int(row.get("up_days") or 0),
                down_days=int(row.get("down_days") or 0),
                best_day_gbp=float(row.get("best_day_gbp") or 0.0),
                p=float(1.0 if row.get("p") is None else row["p"]),
                folds_up=int(row.get("folds_up") or 0),
                folds_worst=float(row.get("folds_worst") or 0.0), plateau=row.get("plateau"),
                cost15_net=float(row.get("cost15_net") or 0.0),
                cost20_net=float(row.get("cost20_net") or 0.0), dsr=float(row.get("dsr") or 0.0),
                max_dd_r=float(row.get("max_dd_r") or 0.0),
                assumed_share=float(row.get("assumed_share") or 0.0),
                synthetic=bool(row.get("synthetic")))
        if daily:
            s.days_traded = sum(1 for v in daily.values() if v[1] > 0)
        return s


@dataclass
class Check:
    key: str
    state: str                          # Passed | Failed | Running | Not yet | Not tested this way
    text: str
    n: Optional[float] = None
    of: Optional[float] = None

    @property
    def label(self) -> str:
        return CHECK_LABELS[self.key]

    def as_dict(self) -> dict:
        return {"key": self.key, "label": self.label, "state": self.state, "text": self.text,
                "n": self.n, "of": self.of}


@dataclass
class Verdict:
    kind: str                           # past | fresh | practice
    stats: TestStats
    checks: list
    passed: bool
    reasons: list = field(default_factory=list)     # codes: TOO_FEW_TRADES, NOT_STEADY ...
    first_failed: str = ""              # check key; the bin reason for a failed PAST test
    outcome: str = ""                   # fresh: PASS | LOST | TOO_FEW_TRADES
    forward_ok: bool = True             # False when held back (plateau says extend the range)
    why: str = ""                       # one plain sentence for the bin reason / events

    def check(self, key: str) -> Check:
        for c in self.checks:
            if c.key == key:
                return c
        raise KeyError(key)

    def states(self) -> dict:
        return {c.key: c.state for c in self.checks}


def _gbp(x: float, signed: bool = True) -> str:
    sign = ("+" if x > 0 else "-" if x < 0 else "") if signed else ("-" if x < 0 else "")
    return f"{sign}£{abs(x):,.0f}"


def summarize(result: Result, *, n_trials: int = 1, trial_sharpes: Optional[Sequence[float]] = None,
              kind: Optional[str] = None) -> TestStats:
    """All the figures for a Result. Days are the result's own days (every day it
    could be judged on, zeros included)."""
    trades = result.trades
    day_keys = sorted(result.daily)
    daily = [result.daily[d][0] for d in day_keys]
    nets = [t.net_gbp for t in trades]
    wins = sum(1 for x in nets if x > 0)
    net = sum(nets)
    cn = cost_nets(trades) if trades else {}
    p = block_bootstrap_p(daily, result.candidate_id)
    ch = chunk_nets(daily) if daily else [0.0] * PAST_CHUNKS
    s = TestStats(
        kind=kind or result.window.kind, window_from=day_keys[0] if day_keys else "",
        window_to=day_keys[-1] if day_keys else "", days=len(daily),
        days_traded=sum(1 for d in day_keys if result.daily[d][1] > 0), trades=len(trades),
        net_gbp=net, cost_gbp=sum(t.cost_gbp for t in trades), pf=profit_factor(nets),
        win_rate=(wins / len(trades)) if trades else 0.0,
        up_days=sum(1 for d in daily if d > 0), down_days=sum(1 for d in daily if d < 0),
        best_day_gbp=max(daily) if daily else 0.0, p=p,
        folds_up=sum(1 for x in ch if x > 0), folds_worst=min(ch) if ch else 0.0,
        cost15_net=cn.get(COST_STRESS_PASS, 0.0), cost20_net=cn.get(COST_STRESS_SHOWN, 0.0),
        max_dd_r=max_drawdown_r(trades),
        assumed_share=(sum(1 for t in trades if t.spread_assumed) / len(trades)) if trades else 0.0,
        sharpe=sharpe_of(daily), synthetic=bool(result.synthetic),
        expectancy_r=(sum(t.r for t in trades) / len(trades)) if trades else 0.0)
    s.dsr = dsr_of(daily, n_trials, trial_sharpes) if len(daily) >= 8 else 0.0
    return s


# ------------------------------------------------------------------ plateau ---
def plateau_state(candidate, nets: Mapping[str, float]) -> tuple:
    """(state, extend_params) for a candidate from the past nets of its siblings.

    For each sensitive numeric setting the siblings are the identical candidates
    with that setting varied. Three or more values: ``parameter_stability`` at a
    tolerance of 0.5 must call the neighbourhood stable. Two values: the other
    one must also be net positive. 'extend' means the best value sits at the
    edge of the range tried, so the range should be extended before trusting
    it. State is 'na' when the family has no numeric setting, and None while a
    sibling's result is still missing (``missing_siblings`` names the ones to
    run). None is NOT a pass: ``past_verdict`` holds such a candidate back from
    the fresh look until its neighbours exist."""
    from .candidate import family_of
    fam = family_of(candidate)
    state, extend = "na", []
    for param in fam.sensitive:
        vals = fam.axis_values(param)
        if len(vals) < 2:
            continue
        sibs = fam.siblings(candidate, param)
        if any(s.id not in nets for s in sibs):
            return None, []
        if state == "na":
            state = "ok"
        if len(vals) >= 3:
            res = parameter_stability({float(v): float(nets[s.id]) for v, s in zip(vals, sibs)},
                                      plateau_tolerance=PLATEAU_TOLERANCE)
            if res.get("stable") is None:
                extend.append(param)
                if state != "spike":
                    state = EXTEND
            elif not res["stable"]:
                state = "spike"
        else:
            if not all(nets[s.id] > 0 for s in sibs if s.id != candidate.id):
                state = "spike"
    return state, extend


def missing_siblings(candidate, nets: Mapping[str, float]) -> list:
    """The neighbours whose past result ``plateau_state`` is waiting for: the
    siblings (every sensitive setting varied in turn) that are not in ``nets``.
    A neighbour binned in an earlier stretch is never re-run as a candidate, so
    after a roll these have to be tested again as plateau references (they count
    toward N like any test); until then the candidate is not forwarded."""
    from .candidate import family_of
    fam = family_of(candidate)
    out, seen = [], set()
    for param in fam.sensitive:
        if len(fam.axis_values(param)) < 2:
            continue
        for sib in fam.siblings(candidate, param):
            if sib.id not in nets and sib.id not in seen:
                seen.add(sib.id)
                out.append(sib)
    return out


# --------------------------------------------------------------- the verdicts ---
def past_verdict(stats: TestStats, plateau: Optional[str] = "na", *,
                 pbo_ok: Optional[bool] = None) -> Verdict:
    """The PAST test's five checks and the pass/fail decision. ``plateau`` is
    'ok' | 'spike' | 'extend' | 'na', or None = a neighbouring setting's result is
    not there yet. None does not fail the candidate, but it is not forwardable
    either: like 'extend' it passes with ``forward_ok`` False, and the reason
    PLATEAU_PENDING says what it is waiting for. The luck check is shown and used
    to rank, not a gate here."""
    s = stats
    reasons: list = []
    # 1. Enough trades
    ok_t = s.trades >= PAST_MIN_TRADES and s.days >= PAST_MIN_DAYS
    if ok_t:
        t_trades = Check(CHECK_TRADES, PASSED,
                         f"{s.trades} trades over {s.days} days (it needs at least "
                         f"{PAST_MIN_TRADES} trades over at least {PAST_MIN_DAYS} days).",
                         s.trades, PAST_MIN_TRADES)
    else:
        why = (f"Only {s.trades} trades" if s.trades < PAST_MIN_TRADES else
               f"Only {s.days} days of prices")
        t_trades = Check(CHECK_TRADES, FAILED,
                         f"{why} (it needs at least {PAST_MIN_TRADES} trades over at least "
                         f"{PAST_MIN_DAYS} days).", s.trades, PAST_MIN_TRADES)
        reasons.append("TOO_FEW_TRADES")
    # 2. Steady
    problems = []
    if s.net_gbp <= 0:
        problems.append(f"it lost money overall ({_gbp(s.net_gbp)})")
    if s.pf < PAST_PF:
        problems.append(f"it won £{s.pf:.2f} for every £1 lost (needs £{PAST_PF:.2f})")
    if s.folds_up < PAST_CHUNKS_UP:
        problems.append(f"only {s.folds_up} of {PAST_CHUNKS} equal stretches of days "
                        f"{'was' if s.folds_up == 1 else 'were'} up (needs {PAST_CHUNKS_UP})")
    if s.net_gbp > 0 and s.folds_worst < PAST_CHUNK_WORST * s.net_gbp:
        problems.append(f"its worst stretch lost {_gbp(-s.folds_worst, False)}, more than half "
                        f"of the whole gain")
    if s.up_days <= s.down_days:
        problems.append(f"{s.up_days} days up against {s.down_days} down")
    if s.net_gbp > 0 and s.best_day_gbp > PAST_BEST_DAY_SHARE * s.net_gbp:
        problems.append(f"one day made {s.best_day_gbp / s.net_gbp:.0%} of all the profit "
                        f"(limit {PAST_BEST_DAY_SHARE:.0%})")
    if plateau == "spike":
        problems.append("its result changes sharply when its settings are nudged (a spike, "
                        "not a plateau)")
    if problems:
        t_steady = Check(CHECK_STEADY, FAILED, "Not steady: " + "; ".join(problems) + ".")
        reasons.append("NOT_STEADY")
    else:
        pl = {"ok": "it holds up when its settings are nudged",
              "extend": "its settings hit the edge of the range tried, so it is held back until "
                        "the range is extended",
              "na": "it has no setting to nudge, so the fresh test decides",
              None: "the neighbouring settings have not all been tested yet, so it waits for them "
                    "before it gets its fresh test"}.get(plateau, "")
        t_steady = Check(CHECK_STEADY, PASSED,
                         f"{_gbp(s.net_gbp)} after costs, £{s.pf:.2f} won for every £1 lost "
                         f"(needs £{PAST_PF:.2f}); {s.folds_up} of {PAST_CHUNKS} stretches up; "
                         f"{s.up_days} days up, {s.down_days} down; best day "
                         f"{(s.best_day_gbp / s.net_gbp if s.net_gbp > 0 else 0):.0%} of the total "
                         f"(limit {PAST_BEST_DAY_SHARE:.0%}); {pl}.")
    # 3. Fresh prices: not yet
    t_fresh = Check(CHECK_FRESH, NOT_YET, "Not yet: it has not been tried on prices it never saw.")
    # 4. Costs
    cost_problems = []
    if s.cost15_net <= 0:
        cost_problems.append(f"at {COST_STRESS_PASS:g} times the costs it would be "
                             f"{_gbp(s.cost15_net)}")
    if s.assumed_share > PAST_ASSUMED_SPREAD_MAX:
        cost_problems.append(f"{s.assumed_share:.0%} of its trades had to assume a spread "
                             f"(limit {PAST_ASSUMED_SPREAD_MAX:.0%})")
    if cost_problems:
        t_costs = Check(CHECK_COSTS, FAILED, "Costs eat it: " + "; ".join(cost_problems) + ".")
        reasons.append("COSTS")
    else:
        t_costs = Check(CHECK_COSTS, PASSED,
                        f"Still {_gbp(s.cost15_net)} when costs are {COST_STRESS_PASS:g} times higher "
                        f"(it must stay above zero); {_gbp(s.cost20_net)} at {COST_STRESS_SHOWN:g} times.")
    # 5. Luck check (shown on old prices; the fresh test is the real one)
    line = luck_line(s.p, 1)
    t_luck = Check(CHECK_LUCK, PASSED if s.p <= PAST_P_SHOWN else NOT_YET,
                   f"{line} on the old prices (shown for ranking; the fresh test is the real check).")
    checks = [t_trades, t_steady, t_fresh, t_costs, t_luck]
    gates = [t_trades, t_steady, t_costs]
    passed = all(c.state == PASSED for c in gates)
    first = next((c.key for c in gates if c.state != PASSED), "")
    why = next((c.text for c in gates if c.state != PASSED), "")
    fwd = passed and plateau is not None and plateau != EXTEND and pbo_ok is not False
    if passed and plateau == EXTEND:
        reasons.append(REASON_EXTEND_GRID)
    if passed and plateau is None:
        reasons.append(REASON_PLATEAU_PENDING)
    if passed and pbo_ok is False:
        reasons.append(REASON_FITS_NOISE)
    s.plateau = plateau
    s.passed = passed
    return Verdict("past", s, checks, passed, reasons, first, "", fwd, why)


def fresh_verdict(stats: TestStats) -> Verdict:
    """The one fresh look. Outcome PASS, LOST (final) or TOO_FEW_TRADES (one
    retry on a later stretch)."""
    s = stats
    reasons: list = []
    if s.trades < FRESH_MIN_TRADES or s.days < FRESH_DAYS_MIN:
        # Too little to judge either way. The luck figure is only as honest as the days it
        # is built from (it is a bootstrap over days), so a window shorter than the
        # stretch rule's minimum of 20 days is treated like too few trades: not judged,
        # one more go on a later stretch.
        if s.trades < FRESH_MIN_TRADES:
            txt = (f"Only {s.trades} trades on prices it never saw; it needs at least "
                   f"{FRESH_MIN_TRADES} before anyone can tell skill from luck. It gets one "
                   f"more go on a later stretch.")
        else:
            txt = (f"Only {s.days} days of prices it never saw; it needs at least "
                   f"{FRESH_DAYS_MIN} before anyone can tell skill from luck. It gets one "
                   f"more go on a later stretch.")
        checks = [Check(CHECK_TRADES, FAILED, txt, s.trades, FRESH_MIN_TRADES)]
        for key in (CHECK_STEADY, CHECK_FRESH, CHECK_COSTS, CHECK_LUCK):
            checks.append(Check(key, NOT_YET, "Not judged: too little to go on."))
        s.passed = False
        return Verdict("fresh", s, checks, False, [FRESH_TOO_FEW], CHECK_TRADES, FRESH_TOO_FEW,
                       False, txt)
    t_trades = Check(CHECK_TRADES, PASSED,
                     f"{s.trades} trades on prices it never saw (needs at least "
                     f"{FRESH_MIN_TRADES}).", s.trades, FRESH_MIN_TRADES)
    problems = []
    if s.net_gbp <= 0:
        problems.append(f"it lost money ({_gbp(s.net_gbp)})")
    if s.pf < FRESH_PF:
        problems.append(f"it won £{s.pf:.2f} for every £1 lost (needs £{FRESH_PF:.2f})")
    if s.up_days <= s.down_days:
        problems.append(f"{s.up_days} days up against {s.down_days} down")
    if problems:
        t_steady = Check(CHECK_STEADY, FAILED, "Not steady on fresh prices: " + "; ".join(problems) + ".")
    else:
        t_steady = Check(CHECK_STEADY, PASSED,
                         f"{_gbp(s.net_gbp)} after costs, £{s.pf:.2f} won for every £1 lost "
                         f"(needs £{FRESH_PF:.2f}), {s.up_days} days up and {s.down_days} down.")
    luck_ok = s.p <= FRESH_P
    line = (f"(= {FRESH_FAMILYWISE:g} spread over the {FRESH_LOOKS_PER_STRETCH} looks a stretch allows)")
    t_fresh = Check(CHECK_FRESH, PASSED if luck_ok else FAILED,
                    (f"On prices it never saw, the chance that a result this good is just luck is "
                     f"{s.p:.1%}, under the {FRESH_P:.1%} line {line}." if luck_ok else
                     f"On prices it never saw, the chance that a result this good is just luck is "
                     f"{s.p:.1%}; it must be under {FRESH_P:.1%} {line}."))
    if s.cost15_net > 0:
        t_costs = Check(CHECK_COSTS, PASSED,
                        f"Still {_gbp(s.cost15_net)} when costs are {COST_STRESS_PASS:g} times higher.")
    else:
        t_costs = Check(CHECK_COSTS, FAILED,
                        f"At {COST_STRESS_PASS:g} times the costs it would be {_gbp(s.cost15_net)}.")
    t_luck = Check(CHECK_LUCK, PASSED if luck_ok else FAILED,
                   luck_line(s.p, FRESH_LOOKS_PER_STRETCH)
                   + f" (counting the {FRESH_LOOKS_PER_STRETCH} looks a stretch allows).")
    checks = [t_trades, t_steady, t_fresh, t_costs, t_luck]
    passed = all(c.state == PASSED for c in checks)
    first = next((c.key for c in checks if c.state != PASSED), "")
    why = next((c.text for c in checks if c.state != PASSED), "")
    if not passed:
        reasons.append(FRESH_LOST)
    s.passed = passed
    return Verdict("fresh", s, checks, passed, reasons, first, "PASS" if passed else FRESH_LOST,
                   passed, why)


def practice_verdict(stats: TestStats, *, at_look: bool = True, fresh_state: str = PASSED,
                     fresh_text: str = "Passed its one fresh test; carried forward.") -> Verdict:
    """A practice look (two allowed, counted as m = 2 in the luck line). Before a
    look is due, ``at_look=False`` gives 'Running (n of m)' pills and never
    judges. At a look all five must pass: >= 30 trades over >= 10 days, net > 0,
    p <= 0.05, PF >= 1.15, days up > down, still positive at 1.5 x costs."""
    s = stats
    n_ok = s.trades >= PRACTICE_MIN_TRADES and s.days >= PRACTICE_MIN_DAYS
    if n_ok:
        t_trades = Check(CHECK_TRADES, PASSED,
                         f"{s.trades} practice trades over {s.days} days (needs {PRACTICE_MIN_TRADES} "
                         f"over {PRACTICE_MIN_DAYS}).", s.trades, PRACTICE_MIN_TRADES)
    elif at_look:
        t_trades = Check(CHECK_TRADES, FAILED,
                         f"Only {s.trades} practice trades over {s.days} days (needs "
                         f"{PRACTICE_MIN_TRADES} over {PRACTICE_MIN_DAYS}): it trades too rarely.",
                         s.trades, PRACTICE_MIN_TRADES)
    else:
        t_trades = Check(CHECK_TRADES, RUNNING,
                         f"{s.trades} of {PRACTICE_MIN_TRADES} practice trades so far, over "
                         f"{s.days} of {PRACTICE_MIN_DAYS} days.", s.trades, PRACTICE_MIN_TRADES)
    problems = []
    if s.net_gbp <= 0:
        problems.append(f"it is {_gbp(s.net_gbp)} after costs")
    if s.pf < PRACTICE_PF:
        problems.append(f"it won £{s.pf:.2f} for every £1 lost (needs £{PRACTICE_PF:.2f})")
    if s.up_days <= s.down_days:
        problems.append(f"{s.up_days} days up against {s.down_days} down")
    steady_ok = not problems
    if at_look:
        t_steady = Check(CHECK_STEADY, PASSED if steady_ok else FAILED,
                         (f"{_gbp(s.net_gbp)} after costs, £{s.pf:.2f} won for every £1 lost, "
                          f"{s.up_days} days up and {s.down_days} down." if steady_ok
                          else "Not steady in practice: " + "; ".join(problems) + "."))
    else:
        t_steady = Check(CHECK_STEADY, RUNNING,
                         f"So far {_gbp(s.net_gbp)} after costs, {s.up_days} days up and "
                         f"{s.down_days} down; judged at the look.")
    t_fresh = Check(CHECK_FRESH, fresh_state, fresh_text)
    cost_ok = s.cost15_net > 0
    if at_look:
        t_costs = Check(CHECK_COSTS, PASSED if cost_ok else FAILED,
                        (f"Still {_gbp(s.cost15_net)} at {COST_STRESS_PASS:g} times the costs." if cost_ok
                         else f"At {COST_STRESS_PASS:g} times the costs it would be {_gbp(s.cost15_net)}."))
    else:
        t_costs = Check(CHECK_COSTS, RUNNING,
                        f"{_gbp(s.cost15_net)} at {COST_STRESS_PASS:g} times the costs so far.")
    luck_ok = s.p <= PRACTICE_P
    line = luck_line(s.p, PRACTICE_LOOK_M)
    if at_look:
        t_luck = Check(CHECK_LUCK, PASSED if luck_ok else FAILED,
                       f"{line} (two looks are counted); it must be under 1 in "
                       f"{round(1 / PRACTICE_P)} before the doubling.")
    else:
        t_luck = Check(CHECK_LUCK, RUNNING, f"{line} so far; judged at the look.")
    checks = [t_trades, t_steady, t_fresh, t_costs, t_luck]
    passed = at_look and n_ok and steady_ok and cost_ok and luck_ok and s.net_gbp > 0
    first = ""
    why = ""
    reasons: list = []
    if at_look and not passed:
        order = [(t_trades, "TOO_FEW_TRADES"), (t_steady, "NOT_STEADY"), (t_costs, "COSTS"),
                 (t_luck, "LUCK")]
        for chk, code in order:
            if chk.state != PASSED:
                first, why = chk.key, chk.text
                reasons.append(code)
                break
    s.passed = bool(passed) if at_look else None
    return Verdict("practice", s, checks, bool(passed), reasons, first, "", True, why)


# ------------------------------------------------------------- family gates ---
def family_gate(daily_by_id: Mapping[str, Mapping[str, float]], trades_by_id: Mapping[str, int],
                *, days: Optional[Sequence[str]] = None) -> dict:
    """PBO over a family's past results, from combinatorially purged splits.

    ``daily_by_id[cid]`` maps UK day -> net pounds. Members with fewer than 20
    trades are left out; with fewer than 8 members the answer is 'n/a'
    (``pbo`` None, ``blocks`` False). PBO above 0.35 means the family's best
    in-sample candidate usually fails out of sample: it is fitting noise, and
    every forward from the family is blocked that night.

    The splits are ``cpcv_splits(n_days, groups=6, test_groups=2)``: 15 of
    them, each with its purged training days and its test days. For each split
    the member with the best training mean is picked and PBO is how often it
    lands below the median member on the test days.

    NOTE (a deviation, measured): ``validation.probability_of_backtest_overfitting``
    takes a matrix of independent paths and picks the in-sample best from the
    OTHER paths. Fed CPCV's 15 test sets it leaks, because every group of days
    sits in five of the 15 paths, and a family of pure noise scores a PBO of
    about 0.07 instead of about 0.5, so the 0.35 gate would never close. The
    in-sample figure here is the split's own purged training days.

    A limit worth knowing (measured, not a leak): PBO asks whether the best
    member is better than the middle one out of sample. A family in which EVERY
    member carries the same real edge has no stand-out best member, so it scores
    high too (40 members with +0.3 sd a day each averaged 0.5 and were blocked
    about three nights in four). The gate therefore costs some power on families
    that are good all the way through; it never lets noise through. It is the
    spec's rule (0.35), so it is kept, and the page says it blocks forwards
    rather than that the family is worthless."""
    members = sorted(c for c, n in trades_by_id.items() if n >= PBO_MIN_TRADES and c in daily_by_id)
    out = {"pbo": None, "members": len(members), "paths": 0, "blocks": False,
           "note": "n/a"}
    if len(members) < PBO_MIN_MEMBERS:
        out["note"] = f"n/a: needs {PBO_MIN_MEMBERS} members with {PBO_MIN_TRADES}+ trades"
        return out
    day_list = list(days) if days is not None else sorted(
        {d for c in members for d in daily_by_id[c]})
    if len(day_list) < PBO_GROUPS * 2:
        out["note"] = "n/a: too few days"
        return out
    mat = [[float(daily_by_id[c].get(d, 0.0)) for d in day_list] for c in members]
    splits = cpcv_splits(len(day_list), groups=PBO_GROUPS, test_groups=PBO_TEST_GROUPS)
    failures = 0
    for train, test in splits:
        is_scores = [sum(row[i] for i in train) / len(train) for row in mat]
        best = max(range(len(members)), key=lambda k: (is_scores[k], -k))
        oos = [sum(row[i] for i in test) / len(test) for row in mat]
        median = sorted(oos)[len(oos) // 2]
        if oos[best] < median:
            failures += 1
    pbo = round(failures / len(splits), 4)
    out.update(pbo=pbo, paths=len(splits),
               note=("selection looks robust" if pbo <= PBO_MAX else
                     "HIGH over-fitting risk: the in-sample winner usually fails out of sample"))
    out["blocks"] = pbo > PBO_MAX
    return out


def pass_rates(verdicts: Mapping[str, Verdict]) -> dict:
    """How often a family's candidates pass each gate: printed for the control
    family so Aaron can see the gates are not letting noise through."""
    n = len(verdicts)
    if not n:
        return {"n": 0, "past": 0, "trades": 0, "steady": 0, "costs": 0, "past_pass_rate": 0.0}
    cnt = {k: sum(1 for v in verdicts.values() if v.check(k).state == PASSED)
           for k in (CHECK_TRADES, CHECK_STEADY, CHECK_COSTS)}
    past = sum(1 for v in verdicts.values() if v.passed)
    return {"n": n, "past": past, "trades": cnt[CHECK_TRADES], "steady": cnt[CHECK_STEADY],
            "costs": cnt[CHECK_COSTS], "past_pass_rate": past / n}
