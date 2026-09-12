"""Backtest runner and performance report.

Runs the **live Trader** over a :class:`~mintel.research.replay.ReplayBroker`,
so the thing being measured is the thing that will trade.  No parallel
"backtest strategy" exists in this codebase, which is why a result here is
worth reading at all.

Everything is reported after costs: spread, slippage and commission are applied
by the replay broker, so there is no gross-profit headline anywhere.

Honesty guards built in:

* If the calendar archive does not cover the test window, the report says
  ``news_aware: False`` and explains why.  It will never present a
  "news-aware" result with no news in it.
* Every run can be written to the trial ledger, so the number of
  configurations tried is available to the deflated-Sharpe and PBO statistics.
* Synthetic data is flagged as synthetic.  Mechanism verification and edge
  measurement are different claims and are labelled differently.
"""
from __future__ import annotations

import datetime as dt
import logging
import math
import shutil
import statistics
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional, Sequence

from ..broker.base import Bar, CalendarEvent, Side, TF
from ..clock import to_utc
from ..config import Config
from ..engine.trader import Trader
from ..news.calendar_store import CalendarStore
from .replay import Costs, ReplayBroker
from .trial_ledger import Trial, TrialLedger
from .validation import (cost_stress, deflated_sharpe_ratio, max_drawdown,
                         monte_carlo_sequences, probabilistic_sharpe_ratio,
                         sharpe, sortino)

log = logging.getLogger("mintel.backtest")


@dataclass
class BacktestResult:
    label: str
    data_kind: str                # "REAL" or "SYNTHETIC"
    news_aware: bool
    news_note: str
    start: Optional[dt.datetime]
    end: Optional[dt.datetime]
    days: float
    starting_equity: float
    ending_equity: float
    trades: int
    wins: int
    losses: int
    win_rate: Optional[float]
    expectancy_r: Optional[float]
    profit_factor: Optional[float]
    avg_win: float
    avg_loss: float
    largest_win: float
    largest_loss: float
    net_money: float
    commission: float
    max_drawdown_pct: float
    sharpe: float
    sortino: float
    trades_per_day: float
    mfe_r_avg: Optional[float]
    mae_r_avg: Optional[float]
    mfe_capture_avg: Optional[float]
    by_symbol: dict
    by_session: dict
    by_regime: dict
    by_tactic: dict
    around_news: dict
    equity_curve: list
    trade_returns: list
    aggression: str = "NORMAL"
    cycles: int = 0
    rejections: int = 0
    notes: tuple[str, ...] = ()

    def headline(self) -> str:
        if self.trades == 0:
            return (f"{self.label}: no trades were taken over "
                    f"{self.days:.1f} days of {self.data_kind.lower()} data.")
        return (f"{self.label}: {self.trades} trades over {self.days:.1f} days "
                f"({self.trades_per_day:.2f}/day), "
                f"{self.win_rate * 100:.1f}% win rate, "
                f"expectancy {self.expectancy_r:+.3f}R, "
                f"net {self.net_money:+.2f}, "
                f"max drawdown {self.max_drawdown_pct:.2f}% "
                f"- AFTER spread, slippage and commission.")

    def to_dict(self) -> dict:
        d = dict(self.__dict__)
        d["start"] = self.start.isoformat() if self.start else None
        d["end"] = self.end.isoformat() if self.end else None
        d["equity_curve"] = self.equity_curve[-500:]
        return d


def _daily_returns(equity_curve: Sequence[tuple[str, float]],
                   starting_equity: float) -> list[float]:
    """Close-to-close returns per calendar day from the equity curve."""
    by_day: dict[str, float] = {}
    for ts, value in equity_curve:
        by_day[ts[:10]] = value           # last equity seen that day
    days = sorted(by_day)
    out, prev = [], starting_equity
    for d in days:
        if prev > 0:
            out.append(by_day[d] / prev - 1.0)
        prev = by_day[d]
    return out


def _group_stats(trades: Sequence[dict], key: Callable[[dict], str]) -> dict:
    out: dict[str, dict] = {}
    for t in trades:
        k = key(t) or "unknown"
        g = out.setdefault(k, {"trades": 0, "wins": 0, "net": 0.0, "sum_r": 0.0})
        g["trades"] += 1
        g["wins"] += 1 if (t.get("realised_r") or 0) > 0 else 0
        g["net"] += t.get("pnl_money") or 0.0
        g["sum_r"] += t.get("realised_r") or 0.0
    for k, g in out.items():
        g["win_rate"] = round(g["wins"] / g["trades"], 3) if g["trades"] else None
        g["expectancy_r"] = round(g["sum_r"] / g["trades"], 3) if g["trades"] else None
        g["net"] = round(g["net"], 2)
    return dict(sorted(out.items(), key=lambda kv: -kv[1]["trades"]))


def run_backtest(series: dict[str, list[Bar]],
                 specs: dict,
                 *, cfg: Optional[Config] = None,
                 label: str = "backtest",
                 data_kind: str = "REAL",
                 events: Sequence[CalendarEvent] = (),
                 costs: Optional[dict[str, Costs]] = None,
                 starting_equity: float = 10_000.0,
                 base_tf: TF = TF.M1,
                 warmup_bars: int = 400,
                 cycle_every: int = 5,
                 max_bars: int = 0,
                 ledger: Optional[TrialLedger] = None,
                 workdir: Optional[str | Path] = None) -> BacktestResult:
    """Replay ``series`` through the live engine.

    ``cycle_every`` controls how often a full decision cycle runs, in base
    bars.  It trades compute for resolution: a 1-minute base with
    ``cycle_every=5`` runs a full multi-timeframe scan every 5 simulated
    minutes, which is close to the live cadence on a quiet symbol.
    """
    cfg = cfg or Config()
    temp = Path(workdir) if workdir else Path(tempfile.mkdtemp(prefix="mintel_bt_"))
    (temp / "data").mkdir(parents=True, exist_ok=True)
    cfg.ops.data_dir = str(temp / "data")
    cfg.ops.log_dir = str(temp / "logs")
    cfg.news.store_path = "calendar.sqlite"

    broker = ReplayBroker(series, specs, base_tf=base_tf,
                          balance=starting_equity, costs=costs, events=events)
    broker.connect()

    # News archive: the ONLY calendar source in a backtest, so news awareness
    # is either genuine or explicitly reported as absent.
    store = CalendarStore(Path(cfg.ops.data_dir) / "calendar.sqlite")
    if events:
        store.upsert(events)
    timeline_start = broker._timeline[0] if broker._timeline else None
    timeline_end = broker._timeline[-1] if broker._timeline else None
    coverage = store.coverage()
    news_aware = bool(events) and bool(
        timeline_start and coverage.covers(timeline_start, timeline_end))
    if not events:
        news_note = ("no calendar data was supplied, so this run is NOT "
                     "news-aware and no news-based conclusion can be drawn "
                     "from it")
    elif not news_aware:
        news_note = (f"calendar data only covers {coverage.start} to "
                     f"{coverage.end}, which does not span the test window - "
                     f"treat news results as incomplete")
    else:
        news_note = (f"{coverage.events} calendar events covering "
                     f"{coverage.start} to {coverage.end}")

    trader = Trader(broker, cfg, clock=lambda: broker.now,
                    use_store_only_news=True, enable_model=False)

    # Warm up: give the engine history before it is allowed to decide anything.
    for _ in range(min(warmup_bars, len(broker._timeline) - 2)):
        broker.step()
    trader.bootstrap()

    equity_curve: list[tuple[str, float]] = []
    cycles = 0
    bars_done = 0
    while not broker.finished:
        if not broker.step():
            break
        bars_done += 1
        if max_bars and bars_done >= max_bars:
            break
        if bars_done % cycle_every:
            # Between full scans, still manage open positions - exactly as the
            # live loop does.  Without this, FlowLock would only see the market
            # once per scan interval and the MFE statistics would be measured
            # far more coarsely than they are in production.
            if broker.positions():
                try:
                    trader.manage_cycle()
                except Exception as exc:
                    log.exception("management step failed at %s: %s",
                                  broker.now, exc)
            continue
        try:
            trader.cycle()
        except Exception as exc:
            log.exception("backtest cycle failed at %s: %s", broker.now, exc)
        cycles += 1
        acct = broker.account()
        equity_curve.append((broker.now.isoformat(), round(acct.equity, 2)))

    # Close anything still open at the end, so the result contains no
    # unrealised wishful thinking.
    for p in list(broker.positions()):
        broker.close(p.ticket, comment="END_OF_TEST")
    trader.manage([], broker.now)

    trades = trader.journal.closed_trades(limit=100_000)
    # Money and pips are recomputed here from broker settlements, because the
    # journal records the decision while the broker records what was paid.
    settle = {c["ticket"]: c for c in broker.closed}
    for t in trades:
        c = settle.get(t["ticket"])
        if c:
            t["pnl_money"] = c["pnl"]
            t["commission"] = c.get("commission", 0.0)

    wins = [t for t in trades if (t.get("realised_r") or 0) > 0]
    losses = [t for t in trades if (t.get("realised_r") or 0) < 0]
    rs = [t.get("realised_r") or 0.0 for t in trades]
    gross_win = sum(t.get("pnl_money") or 0 for t in wins)
    gross_loss = abs(sum(t.get("pnl_money") or 0 for t in losses))
    eq_values = [starting_equity] + [v for _, v in equity_curve]
    # Sharpe and Sortino are computed on DAILY returns.  Annualising
    # per-cycle returns from a short window produces meaninglessly large
    # numbers, which is exactly the sort of flattering statistic this
    # codebase is supposed to refuse to print.
    daily_returns = _daily_returns(equity_curve, starting_equity)
    trade_returns = [(t.get("pnl_money") or 0.0) / max(
        t.get("equity_at_entry") or starting_equity, 1e-9) for t in trades]

    days = 0.0
    if timeline_start and timeline_end:
        days = (timeline_end - timeline_start).total_seconds() / 86400.0

    def _news_bucket(t: dict) -> str:
        ns = (t.get("news_state") or "").lower()
        if "due in" in ns:
            return "just before a release"
        for wave in ("second_wave", "first_wave", "first_spike", "rejected",
                     "whipsaw"):
            if wave in ns:
                return wave.replace("_", " ")
        return "no live release"

    result = BacktestResult(
        label=label, data_kind=data_kind, news_aware=news_aware,
        news_note=news_note, start=timeline_start, end=timeline_end, days=days,
        starting_equity=starting_equity,
        ending_equity=round(broker.account().balance, 2),
        trades=len(trades), wins=len(wins), losses=len(losses),
        win_rate=(len(wins) / len(trades)) if trades else None,
        expectancy_r=(sum(rs) / len(rs)) if rs else None,
        profit_factor=(gross_win / gross_loss) if gross_loss > 0 else None,
        avg_win=round(gross_win / len(wins), 2) if wins else 0.0,
        avg_loss=round(-gross_loss / len(losses), 2) if losses else 0.0,
        largest_win=round(max((t.get("pnl_money") or 0 for t in trades),
                              default=0.0), 2),
        largest_loss=round(min((t.get("pnl_money") or 0 for t in trades),
                               default=0.0), 2),
        net_money=round(broker.account().balance - starting_equity, 2),
        commission=round(broker.commission_paid, 2),
        max_drawdown_pct=round(max_drawdown(eq_values), 2),
        sharpe=round(sharpe(daily_returns, periods_per_year=252.0), 3),
        sortino=round(sortino(daily_returns, periods_per_year=252.0), 3),
        trades_per_day=round(len(trades) / days, 3) if days > 0 else 0.0,
        mfe_r_avg=(round(statistics.mean(t.get("mfe_r") or 0 for t in trades), 3)
                   if trades else None),
        mae_r_avg=(round(statistics.mean(t.get("mae_r") or 0 for t in trades), 3)
                   if trades else None),
        mfe_capture_avg=(round(statistics.mean(t.get("mfe_capture_pct") or 0
                                               for t in trades), 2)
                         if trades else None),
        by_symbol=_group_stats(trades, lambda t: t.get("symbol")),
        by_session=_group_stats(trades, lambda t: t.get("session")),
        by_regime=_group_stats(trades, lambda t: t.get("regime")),
        by_tactic=_group_stats(trades, lambda t: t.get("tactic")),
        around_news=_group_stats(trades, _news_bucket),
        equity_curve=equity_curve, trade_returns=trade_returns,
        aggression=cfg.aggression, cycles=cycles,
        rejections=broker.rejections,
        notes=(("results on SYNTHETIC data verify mechanics only - they say "
                "nothing about whether an edge exists",)
               if data_kind == "SYNTHETIC" else ()))

    if ledger is not None:
        ledger.record(Trial(
            study=label, params={"aggression": cfg.aggression,
                                 "base_risk": cfg.risk.base_risk_pct,
                                 "max_risk": cfg.risk.max_risk_pct,
                                 "min_rr": cfg.scan.min_reward_risk,
                                 "tiers": [cfg.scan.tier_normal,
                                           cfg.scan.tier_strong,
                                           cfg.scan.tier_exceptional]},
            dataset=f"{data_kind}:{','.join(sorted(series))}",
            period_start=str(timeline_start), period_end=str(timeline_end),
            trades=result.trades, sharpe=result.sharpe,
            expectancy_r=result.expectancy_r,
            profit_factor=result.profit_factor,
            max_drawdown_pct=result.max_drawdown_pct,
            net_money=result.net_money, notes=news_note))

    try:
        trader.journal.close()
        store.close()
    except Exception:
        pass
    if workdir is None:
        shutil.rmtree(temp, ignore_errors=True)
    return result


def full_report(result: BacktestResult, *, n_trials: int = 1,
                trial_sharpes: Optional[Sequence[float]] = None) -> dict:
    """Backtest result plus the statistics that keep it honest."""
    out: dict = {"headline": result.headline(), "result": result.to_dict()}
    if result.trades >= 5:
        out["monte_carlo"] = monte_carlo_sequences(
            result.trade_returns, iterations=1000,
            starting_equity=result.starting_equity)
        out["cost_stress"] = cost_stress(
            [{"pnl_money": r * result.starting_equity,
              "cost_money": abs(result.commission) / max(result.trades, 1)}
             for r in result.trade_returns])
    rets = _daily_returns(result.equity_curve, result.starting_equity)
    if len(rets) >= 8:
        out["probabilistic_sharpe"] = probabilistic_sharpe_ratio(rets)
        out["deflated_sharpe"] = deflated_sharpe_ratio(rets, n_trials,
                                                        trial_sharpes)
    out["honesty"] = {
        "data_kind": result.data_kind,
        "news_aware": result.news_aware,
        "news_note": result.news_note,
        "costs_included": "spread, slippage and commission were charged by the "
                          "replay broker; every figure above is net",
        "not_modelled": "queue position, liquidity-driven partial fills, and "
                        "gaps beyond those present in the data",
        "caveats": list(result.notes),
    }
    return out
