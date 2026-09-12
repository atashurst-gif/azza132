"""Validation statistics that make over-fitting visible.

Nothing here improves a strategy.  Everything here makes an *honest* claim
about one possible:

* :func:`walk_forward_splits` - rolling train/test folds in time order.
* :func:`purged_kfold_indices` - k-fold with purging and an embargo, so a
  fold's test set cannot share information with its training set through
  overlapping labels.
* :func:`cpcv_splits` - combinatorially purged cross-validation, which yields
  many backtest paths instead of one.
* :func:`probability_of_backtest_overfitting` - across those paths, how often
  does the in-sample best configuration fail to beat the median out of sample?
* :func:`deflated_sharpe_ratio` / :func:`probabilistic_sharpe_ratio` - is the
  observed Sharpe distinguishable from the best of N lucky tries?
* :func:`monte_carlo_sequences` - what does the equity path look like when the
  same trades arrive in a different order?
* :func:`cost_stress` - what survives when spread and slippage get worse?
* :func:`parameter_stability` - is the chosen value on a plateau or a spike?
"""
from __future__ import annotations

import itertools
import math
import random
from dataclasses import dataclass, field
from typing import Callable, Iterable, Optional, Sequence


# ------------------------------------------------------------------ splits ----

@dataclass(frozen=True)
class Split:
    train: tuple[int, int]      # [start, end)
    test: tuple[int, int]

    def __repr__(self) -> str:
        return (f"Split(train={self.train[0]}:{self.train[1]}, "
                f"test={self.test[0]}:{self.test[1]})")


def walk_forward_splits(n: int, folds: int = 5, min_train: int = 0,
                        anchored: bool = True,
                        embargo: int = 0) -> list[Split]:
    """Time-ordered folds.  Anchored keeps all history; rolling uses a window."""
    if folds < 1 or n < folds + 1:
        return []
    test_size = n // (folds + 1)
    min_train = min_train or test_size
    out: list[Split] = []
    for k in range(folds):
        test_start = min_train + k * test_size
        test_end = min(n, test_start + test_size)
        if test_end - test_start < 1:
            break
        train_end = max(0, test_start - embargo)
        train_start = 0 if anchored else max(0, train_end - min_train)
        if train_end - train_start < 1:
            continue
        out.append(Split((train_start, train_end), (test_start, test_end)))
    return out


def _fold_bounds(n: int, k: int) -> list[tuple[int, int]]:
    size = n // k
    return [(i * size, (i + 1) * size if i < k - 1 else n) for i in range(k)]


def purged_kfold_indices(n: int, k: int = 5, embargo_pct: float = 0.02,
                         label_span: int = 1) -> list[tuple[list[int], list[int]]]:
    """(train_idx, test_idx) per fold, with purging and an embargo.

    Purging removes training samples whose label window overlaps the test
    window; the embargo removes a further band after the test window.  Without
    both, a strategy with multi-bar labels leaks its answers into training and
    every score is inflated.
    """
    embargo = int(n * embargo_pct)
    out = []
    for start, end in _fold_bounds(n, k):
        test = list(range(start, end))
        lo = max(0, start - label_span)
        hi = min(n, end + label_span + embargo)
        train = [i for i in range(n) if i < lo or i >= hi]
        if train and test:
            out.append((train, test))
    return out


def cpcv_splits(n: int, groups: int = 6, test_groups: int = 2,
                embargo_pct: float = 0.02,
                label_span: int = 1) -> list[tuple[list[int], list[int]]]:
    """Combinatorially purged cross-validation.

    Every combination of ``test_groups`` out of ``groups`` becomes one backtest
    path, giving a *distribution* of out-of-sample results rather than the
    single number a plain split produces.  That distribution is what PBO needs.
    """
    bounds = _fold_bounds(n, groups)
    embargo = int(n * embargo_pct)
    out = []
    for combo in itertools.combinations(range(groups), test_groups):
        test: list[int] = []
        for g in combo:
            test.extend(range(*bounds[g]))
        test_set = set(test)
        purge: set[int] = set()
        for g in combo:
            s, e = bounds[g]
            purge.update(range(max(0, s - label_span),
                               min(n, e + label_span + embargo)))
        train = [i for i in range(n) if i not in purge and i not in test_set]
        if train and test:
            out.append((train, sorted(test)))
    return out


# ------------------------------------------------------------- statistics -----

def sharpe(returns: Sequence[float], periods_per_year: float = 252.0) -> float:
    n = len(returns)
    if n < 2:
        return 0.0
    m = sum(returns) / n
    var = sum((r - m) ** 2 for r in returns) / (n - 1)
    sd = math.sqrt(var)
    if sd <= 0:
        return 0.0
    return (m / sd) * math.sqrt(periods_per_year)


def sortino(returns: Sequence[float], periods_per_year: float = 252.0) -> float:
    n = len(returns)
    if n < 2:
        return 0.0
    m = sum(returns) / n
    downside = [r for r in returns if r < 0]
    if not downside:
        return float("inf") if m > 0 else 0.0
    dd = math.sqrt(sum(r * r for r in downside) / len(downside))
    if dd <= 0:
        return 0.0
    return (m / dd) * math.sqrt(periods_per_year)


def _skew_kurt(returns: Sequence[float]) -> tuple[float, float]:
    n = len(returns)
    if n < 4:
        return 0.0, 3.0
    m = sum(returns) / n
    sd = math.sqrt(sum((r - m) ** 2 for r in returns) / (n - 1))
    if sd <= 0:
        return 0.0, 3.0
    g1 = sum(((r - m) / sd) ** 3 for r in returns) / n
    g2 = sum(((r - m) / sd) ** 4 for r in returns) / n
    return g1, g2


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_ppf(p: float) -> float:
    """Inverse normal CDF (Acklam's rational approximation)."""
    if p <= 0.0:
        return -8.0
    if p >= 1.0:
        return 8.0
    a = (-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00)
    b = (-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01)
    c = (-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00)
    d = (7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00)
    plow, phigh = 0.02425, 1 - 0.02425
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
               ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
               ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    q = p - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / \
           (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)


def probabilistic_sharpe_ratio(returns: Sequence[float],
                               benchmark_sr: float = 0.0,
                               periods_per_year: float = 252.0) -> float:
    """Probability the true Sharpe exceeds ``benchmark_sr``.

    Adjusts for sample length, skew and fat tails, all of which make a raw
    Sharpe look better than it is on trading data.
    """
    n = len(returns)
    if n < 8:
        return 0.0
    sr_periodic = sharpe(returns, 1.0)
    bench_periodic = benchmark_sr / math.sqrt(periods_per_year)
    g1, g2 = _skew_kurt(returns)
    denom = math.sqrt(max(1e-12,
                          1.0 - g1 * sr_periodic
                          + (g2 - 1.0) / 4.0 * sr_periodic ** 2))
    z = (sr_periodic - bench_periodic) * math.sqrt(n - 1) / denom
    return round(_norm_cdf(z), 4)


def deflated_sharpe_ratio(returns: Sequence[float], n_trials: int,
                          trial_sharpes: Optional[Sequence[float]] = None,
                          periods_per_year: float = 252.0) -> dict:
    """Sharpe deflated for multiple testing.

    ``n_trials`` must be the *real* number of configurations tried, which is
    exactly what the trial ledger exists to supply.  The expected maximum
    Sharpe under the null grows with the number of trials, so a strategy must
    clear that bar, not zero.
    """
    n = len(returns)
    if n < 8 or n_trials < 1:
        return {"dsr": 0.0, "expected_max_sr": 0.0, "n_trials": n_trials,
                "note": "not enough data"}
    if trial_sharpes and len(trial_sharpes) > 2:
        m = sum(trial_sharpes) / len(trial_sharpes)
        var_sr = sum((s - m) ** 2 for s in trial_sharpes) / (len(trial_sharpes) - 1)
    else:
        var_sr = 1.0 / max(n - 1, 1)
    sd_sr = math.sqrt(max(var_sr, 1e-12))
    gamma = 0.5772156649
    e = math.e
    N = max(n_trials, 2)
    expected_max = sd_sr * ((1 - gamma) * _norm_ppf(1 - 1.0 / N)
                            + gamma * _norm_ppf(1 - 1.0 / (N * e)))
    dsr = probabilistic_sharpe_ratio(
        returns, benchmark_sr=expected_max * math.sqrt(periods_per_year),
        periods_per_year=periods_per_year)
    return {"dsr": dsr,
            "observed_sharpe": round(sharpe(returns, periods_per_year), 4),
            "expected_max_sr_under_null": round(
                expected_max * math.sqrt(periods_per_year), 4),
            "n_trials": n_trials,
            "verdict": ("survives multiple testing" if dsr >= 0.95
                        else "NOT distinguishable from luck given the number "
                             "of trials run")}


def probability_of_backtest_overfitting(
        paths: Sequence[Sequence[float]]) -> dict:
    """PBO over CPCV paths.

    ``paths[i][j]`` is configuration ``j``'s performance on path ``i``.  For
    each path, the configuration that looked best on the *other* paths is
    checked against the median on this one.  A high PBO means the selection
    process is fitting noise.
    """
    if not paths or len(paths) < 2 or len(paths[0]) < 2:
        return {"pbo": None, "note": "need at least 2 paths and 2 configs"}
    n_paths = len(paths)
    n_cfg = len(paths[0])
    failures = 0
    for i in range(n_paths):
        is_scores = [sum(paths[j][c] for j in range(n_paths) if j != i)
                     / (n_paths - 1) for c in range(n_cfg)]
        best = max(range(n_cfg), key=lambda c: is_scores[c])
        oos = sorted(paths[i])
        median = oos[n_cfg // 2]
        if paths[i][best] < median:
            failures += 1
    pbo = failures / n_paths
    return {"pbo": round(pbo, 4), "paths": n_paths, "configs": n_cfg,
            "verdict": ("selection looks robust" if pbo <= 0.35
                        else "HIGH over-fitting risk: the in-sample winner "
                             "usually fails out of sample")}


# ------------------------------------------------------------ monte carlo -----

def max_drawdown(equity: Sequence[float]) -> float:
    peak = -float("inf")
    worst = 0.0
    for v in equity:
        peak = max(peak, v)
        if peak > 0:
            worst = max(worst, (peak - v) / peak)
    return worst * 100.0


def _pct(xs: Sequence[float], p: float) -> float:
    return xs[min(len(xs) - 1, max(0, int(len(xs) * p)))]


def monte_carlo_sequences(trade_returns: Sequence[float], *,
                          iterations: int = 2000, seed: int = 0,
                          starting_equity: float = 10_000.0) -> dict:
    """Two separate questions, answered separately.

    **Permutation** keeps exactly the same trades and only changes their order.
    Compounding is commutative, so the final equity is identical every time -
    that is arithmetic, not a bug - but the *path* is not: the drawdown
    distribution shows how much of the observed equity curve's smoothness was
    the luck of sequencing.

    **Bootstrap** resamples trades with replacement, which does change the
    outcome, and answers the different question of how the strategy might do on
    another sample of the same kind of trades.
    """
    if len(trade_returns) < 5:
        return {"iterations": 0, "note": "too few trades"}
    rng = random.Random(seed)
    series = list(trade_returns)
    n = len(series)

    perm_dds, perm_ruin = [], 0
    for _ in range(iterations):
        rng.shuffle(series)
        eq = [starting_equity]
        for r in series:
            eq.append(eq[-1] * (1.0 + r))
        perm_dds.append(max_drawdown(eq))
        if min(eq) <= starting_equity * 0.5:
            perm_ruin += 1
    perm_dds.sort()

    boot_finals, boot_dds, boot_ruin = [], [], 0
    for _ in range(iterations):
        sample = [series[rng.randrange(n)] for _ in range(n)]
        eq = [starting_equity]
        for r in sample:
            eq.append(eq[-1] * (1.0 + r))
        boot_finals.append(eq[-1])
        boot_dds.append(max_drawdown(eq))
        if min(eq) <= starting_equity * 0.5:
            boot_ruin += 1
    boot_finals.sort()
    boot_dds.sort()

    return {
        "iterations": iterations,
        "trades": n,
        "actual_final_equity": round(
            starting_equity * math.prod(1.0 + r for r in trade_returns), 2),
        "permutation": {
            "median_max_drawdown_pct": round(_pct(perm_dds, 0.5), 2),
            "p95_max_drawdown_pct": round(_pct(perm_dds, 0.95), 2),
            "worst_max_drawdown_pct": round(perm_dds[-1], 2),
            "probability_of_50pct_loss": round(perm_ruin / iterations, 4),
            "note": "same trades, different order - final equity is identical "
                    "by construction, only the path changes",
        },
        "bootstrap": {
            "median_final_equity": round(_pct(boot_finals, 0.5), 2),
            "p05_final_equity": round(_pct(boot_finals, 0.05), 2),
            "p95_final_equity": round(_pct(boot_finals, 0.95), 2),
            "median_max_drawdown_pct": round(_pct(boot_dds, 0.5), 2),
            "p95_max_drawdown_pct": round(_pct(boot_dds, 0.95), 2),
            "probability_of_50pct_loss": round(boot_ruin / iterations, 4),
            "probability_of_losing_money": round(
                sum(1 for f in boot_finals if f < starting_equity)
                / iterations, 4),
        },
    }


def cost_stress(trades: Sequence[dict], *,
                extra_cost_multiples: Sequence[float] = (1.0, 1.5, 2.0, 3.0),
                cost_key: str = "cost_money",
                pnl_key: str = "pnl_money") -> list[dict]:
    """Re-run the P&L with worse spread/slippage.

    An edge that disappears at 1.5x cost was never an edge; it was a rebate on
    an unrealistically tight spread assumption.
    """
    out = []
    for mult in extra_cost_multiples:
        net = 0.0
        wins = 0
        for t in trades:
            cost = float(t.get(cost_key) or 0.0)
            pnl = float(t.get(pnl_key) or 0.0) - cost * (mult - 1.0)
            net += pnl
            wins += 1 if pnl > 0 else 0
        out.append({"cost_multiple": mult, "net": round(net, 2),
                    "win_rate": round(wins / len(trades), 4) if trades else None,
                    "trades": len(trades)})
    return out


def parameter_stability(results: dict[float, float], *,
                        plateau_tolerance: float = 0.25) -> dict:
    """Is the best parameter value on a plateau or a spike?

    A spike means the value was fitted to noise and will not survive live.  A
    plateau means the behaviour is genuinely insensitive nearby, which is what
    should be shipped.
    """
    if len(results) < 3:
        return {"stable": None, "note": "need at least 3 parameter values"}
    keys = sorted(results)
    best_key = max(keys, key=lambda k: results[k])
    best = results[best_key]
    i = keys.index(best_key)
    neighbours = [results[keys[j]] for j in (i - 1, i + 1)
                  if 0 <= j < len(keys)]
    if not neighbours or best == 0:
        return {"stable": None, "note": "no neighbouring values to compare"}
    worst_drop = max((best - v) / abs(best) for v in neighbours)
    values = [results[k] for k in keys]
    monotone = (all(b >= a for a, b in zip(values, values[1:]))
                or all(b <= a for a, b in zip(values, values[1:])))
    at_edge = i in (0, len(keys) - 1)
    out = {
        "best_value": best_key, "best_score": round(best, 4),
        "neighbour_scores": [round(v, 4) for v in neighbours],
        "relative_drop_to_neighbour": round(worst_drop, 4),
        "monotone": monotone,
        "best_at_edge_of_range": at_edge,
    }
    if monotone and at_edge:
        # Not a spike: the metric simply improves in one direction and the
        # tested range stopped too early to find the turning point.
        out["stable"] = None
        out["verdict"] = ("the metric improves monotonically toward the edge "
                          "of the range tested, so the best value here is not "
                          "a plateau OR a spike - extend the range before "
                          "choosing")
        return out
    out["stable"] = worst_drop <= plateau_tolerance
    out["verdict"] = ("sits on a plateau - safe to ship"
                      if worst_drop <= plateau_tolerance
                      else "this is a SPIKE, not a plateau: the value is "
                           "fitted to noise and should not be trusted")
    return out
