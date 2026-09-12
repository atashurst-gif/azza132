"""Performance validation and aggression analysis.

    python3 tools/validate.py                       # synthetic mechanics run
    python3 tools/validate.py --csv EURUSD=path.csv GBPUSD=path2.csv

What this produces, and what it is honest about:

* Every figure is AFTER spread, slippage and commission.
* Synthetic data is labelled ``SYNTHETIC`` and the report states plainly that
  it verifies mechanics only.  A price process we invented cannot tell us
  whether an edge exists, and pretending otherwise would be fabrication.
* Real data (``--csv``) is labelled ``REAL`` and the same statistics then
  actually mean something - still judged against the trial ledger, the
  deflated Sharpe ratio and the probability of backtest overfitting.
* Every configuration tried is recorded in the trial ledger, so the multiple
  testing correction uses the real number of attempts.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mintel.broker.base import CalendarEvent, TF                  # noqa: E402
from mintel.broker.sim import SimBroker                            # noqa: E402
from mintel.config import Config                                   # noqa: E402
from mintel.research.backtest import full_report, run_backtest     # noqa: E402
from mintel.research.replay import Costs, default_spec, load_bars_csv  # noqa: E402
from mintel.research.trial_ledger import TrialLedger               # noqa: E402
from mintel.research.validation import (deflated_sharpe_ratio,      # noqa: E402
                                        parameter_stability,
                                        probability_of_backtest_overfitting)

UTC = dt.timezone.utc

AGGRESSIONS = ("CONSERVATIVE", "NORMAL", "AGGRESSIVE", "MAXIMUM")


def synthetic_data(symbols, bars: int, seed: int):
    """Build a price history from the simulator's regime-block generator."""
    sim = SimBroker(seed=seed,
                    start=dt.datetime(2026, 4, 1, 0, 0, tzinfo=UTC),
                    history_bars=bars)
    sim.connect()
    series = {s: sim.bars(s, TF.M1, bars) for s in symbols}
    specs = {s: sim.spec(s) for s in symbols}
    return series, specs


def real_data(pairs: list[str]):
    series, specs = {}, {}
    for item in pairs:
        if "=" not in item:
            raise SystemExit(f"--csv expects SYMBOL=path, got {item!r}")
        symbol, path = item.split("=", 1)
        bars = load_bars_csv(path)
        if not bars:
            raise SystemExit(f"no bars could be read from {path}")
        series[symbol] = bars
        specs[symbol] = default_spec(symbol, bars[-1].close)
    return series, specs


def calendar_from_csv(path: str) -> list[CalendarEvent]:
    import csv
    out = []
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            try:
                out.append(CalendarEvent(
                    event_id=row.get("event_id") or row.get("id") or "",
                    time_utc=dt.datetime.fromisoformat(
                        row["time_utc"].replace("Z", "+00:00")),
                    currency=row["currency"].upper(),
                    country=row.get("country", ""),
                    name=row.get("name", ""),
                    importance=int(row.get("importance") or 0),
                    actual=_f(row.get("actual")),
                    forecast=_f(row.get("forecast")),
                    previous=_f(row.get("previous")),
                    source="csv"))
            except Exception:
                continue
    return out


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def print_result(r, indent="  "):
    print(f"{indent}{r.headline()}")
    print(f"{indent}  trades/day {r.trades_per_day}   "
          f"profit factor {r.profit_factor if r.profit_factor else 'n/a'}   "
          f"Sharpe {r.sharpe}   Sortino {r.sortino}")
    print(f"{indent}  avg win {r.avg_win}   avg loss {r.avg_loss}   "
          f"largest win {r.largest_win}   largest loss {r.largest_loss}")
    print(f"{indent}  MFE {r.mfe_r_avg}R   MAE {r.mae_r_avg}R   "
          f"MFE captured {r.mfe_capture_avg}%   commission {r.commission}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", nargs="*", default=[],
                    help="SYMBOL=path.csv, repeatable; real broker history")
    ap.add_argument("--calendar-csv", default="",
                    help="calendar archive CSV, so the run is news-aware")
    ap.add_argument("--bars", type=int, default=14_000)
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--cycle-every", type=int, default=10)
    ap.add_argument("--symbols", nargs="*",
                    default=["EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "GER40"])
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    if args.csv:
        series, specs = real_data(args.csv)
        data_kind = "REAL"
    else:
        series, specs = synthetic_data(args.symbols, args.bars, args.seed)
        data_kind = "SYNTHETIC"

    events = calendar_from_csv(args.calendar_csv) if args.calendar_csv else []

    # Realistic retail costs: commission per lot per side, plus slippage that
    # scales with the spread.
    costs = {}
    for symbol, spec in specs.items():
        commission = 3.5 if spec.asset_group.startswith("FX") else 0.0
        costs[symbol] = Costs(commission_per_lot_per_side=commission,
                              slippage_spread_multiple=0.3)

    ledger = TrialLedger(ROOT / "data" / "research_trials.sqlite")
    print()
    print("=" * 74)
    print(f"  PERFORMANCE VALIDATION - {data_kind} DATA")
    print("=" * 74)
    print(f"  instruments : {', '.join(sorted(series))}")
    sample = next(iter(series.values()))
    print(f"  window      : {sample[0].time} to {sample[-1].time}")
    print(f"  bars        : {len(sample)} per instrument")
    print(f"  costs       : spread + slippage + commission, charged per fill")
    if data_kind == "SYNTHETIC":
        print()
        print("  NOTE: this data was generated, not observed. The run below")
        print("  verifies that the machinery works end to end. It says")
        print("  NOTHING about whether the strategy has an edge. For that,")
        print("  re-run with --csv using real broker history.")
    print()

    results = {}
    for aggression in AGGRESSIONS:
        cfg = Config()
        cfg.aggression = aggression
        print(f"  {aggression}")
        r = run_backtest(series, specs, cfg=cfg,
                         label=f"aggression:{aggression}",
                         data_kind=data_kind, events=events, costs=costs,
                         cycle_every=args.cycle_every,
                         ledger=ledger)
        results[aggression] = r
        print_result(r, "    ")
        print()

    # ------------------------------------------------------ the comparison --
    print("=" * 74)
    print("  AGGRESSION ANALYSIS")
    print("=" * 74)
    header = (f"  {'setting':<14}{'trades':>7}{'win%':>7}{'expR':>8}"
              f"{'net':>11}{'maxDD%':>9}{'Sharpe':>8}{'PF':>7}")
    print(header)
    print("  " + "-" * (len(header) - 2))
    for a in AGGRESSIONS:
        r = results[a]
        print(f"  {a:<14}{r.trades:>7}"
              f"{(r.win_rate * 100 if r.win_rate else 0):>7.1f}"
              f"{(r.expectancy_r or 0):>8.3f}{r.net_money:>11.2f}"
              f"{r.max_drawdown_pct:>9.2f}{r.sharpe:>8.2f}"
              f"{(r.profit_factor or 0):>7.2f}")
    print()
    print("  Aggression changes only how far risk climbs toward your ceiling")
    print("  as calibrated confidence rises. It never raises the ceiling, and")
    print("  it never disables a circuit breaker: the same trades are taken,")
    print("  at different sizes.")
    same_trades = len({r.trades for r in results.values()}) == 1
    print(f"  Trade count identical across settings: {same_trades}")

    # --------------------------------------------------- honesty statistics --
    print()
    print("=" * 74)
    print("  OVER-FITTING CHECKS")
    print("=" * 74)
    trial_sharpes = ledger.sharpes()
    report = full_report(results["NORMAL"], n_trials=ledger.count(),
                         trial_sharpes=trial_sharpes)
    print(f"  trials recorded in the ledger : {ledger.count()}")
    dsr = report.get("deflated_sharpe")
    if dsr:
        print(f"  observed Sharpe               : {dsr['observed_sharpe']}")
        print(f"  Sharpe expected from luck     : "
              f"{dsr['expected_max_sr_under_null']}")
        print(f"  deflated Sharpe               : {dsr['dsr']}")
        print(f"  verdict                       : {dsr['verdict']}")
    psr = report.get("probabilistic_sharpe")
    if psr is not None:
        print(f"  probabilistic Sharpe          : {psr}")

    mc = report.get("monte_carlo")
    if mc and mc.get("iterations"):
        print()
        print("  MONTE CARLO")
        print(f"    same trades, reordered: median drawdown "
              f"{mc['permutation']['median_max_drawdown_pct']}%, "
              f"95th percentile {mc['permutation']['p95_max_drawdown_pct']}%, "
              f"worst {mc['permutation']['worst_max_drawdown_pct']}%")
        print(f"    resampled trades      : 5th percentile equity "
              f"{mc['bootstrap']['p05_final_equity']}, median "
              f"{mc['bootstrap']['median_final_equity']}, 95th "
              f"{mc['bootstrap']['p95_final_equity']}")
        print(f"    chance of ending down : "
              f"{mc['bootstrap']['probability_of_losing_money'] * 100:.1f}%")

    stress = report.get("cost_stress")
    if stress:
        print()
        print("  COST STRESS (does the result survive worse execution?)")
        for row in stress:
            print(f"    {row['cost_multiple']}x cost -> net {row['net']}")

    # -------------------------------------------------- parameter stability --
    print()
    print("  PARAMETER STABILITY (plateau or a fitted spike?)")
    stability_scores = {}
    for min_rr in (1.0, 1.15, 1.3, 1.6):
        cfg = Config()
        cfg.scan.min_reward_risk = min_rr
        r = run_backtest(series, specs, cfg=cfg,
                         label="min_reward_risk", data_kind=data_kind,
                         events=events, costs=costs,
                         cycle_every=args.cycle_every, ledger=ledger)
        stability_scores[min_rr] = r.expectancy_r or 0.0
        print(f"    min reward:risk {min_rr:<5} -> {r.trades:>4} trades, "
              f"expectancy {(r.expectancy_r or 0):+.3f}R, "
              f"net {r.net_money:+.2f}")
    stab = parameter_stability(stability_scores)
    print(f"    {stab.get('verdict', stab)}")

    # ------------------------------------------------------------- breakdown -
    normal = results["NORMAL"]
    for title, table in (("BY INSTRUMENT", normal.by_symbol),
                         ("BY SESSION", normal.by_session),
                         ("BY MARKET REGIME", normal.by_regime),
                         ("BY TACTIC", normal.by_tactic),
                         ("AROUND NEWS", normal.around_news)):
        if not table:
            continue
        print()
        print(f"  {title}")
        for key, g in list(table.items())[:10]:
            print(f"    {str(key)[:34]:<36}{g['trades']:>5} trades  "
                  f"win {(g['win_rate'] or 0) * 100:>5.1f}%  "
                  f"expectancy {(g['expectancy_r'] or 0):>+7.3f}R  "
                  f"net {g['net']:>10.2f}")

    print()
    print("=" * 74)
    print("  HONESTY STATEMENT")
    print("=" * 74)
    for k, v in report["honesty"].items():
        print(f"  {k}: {v}")
    print(f"  news_note: {normal.news_note}")
    print()

    if args.out:
        payload = {a: results[a].to_dict() for a in AGGRESSIONS}
        payload["statistics"] = {k: v for k, v in report.items()
                                 if k != "result"}
        payload["parameter_stability"] = stab
        payload["trials_recorded"] = ledger.count()
        Path(args.out).write_text(json.dumps(payload, indent=2, default=str))
        print(f"  full detail written to {args.out}")
    ledger.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
