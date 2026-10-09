"""Benchmark: does the order flow add anything over price and charts?

Six models are run over the SAME replayed events with the SAME exit engine
(the mechanical trail of :mod:`mintel.ian.trail`, a 6-tick initial stop,
five minutes at most) and the same costs, so the only difference between
them is what decides the entry:

    price_only      the price moved >= 3 ticks over 30 s: follow it
    chart           a fast/slow EMA cross on 5-second bars of the mid
    flow_only       the opportunity score from the futures book alone, no regime tactics
    chart_flow      + the chart component
    chart_flow_news + the calendar (when the replay has releases)
    full            + the regime tactics (the engine's own scoring)

Results per model: trades, trades per day, win rate, net after costs,
expectancy, profit factor, average win / loss, maximum drawdown, MFE, MAE and
MFE capture - in TICKS of the future (the market-neutral unit; money depends
on the spot contract and size). MFE capture = total realised R / total MFE R (an aggregate, so one loser
with a tiny favourable excursion cannot swamp it).

On synthetic scenarios the output is SYNTHETIC - NOT PERFORMANCE: it shows
the pipeline works end to end, not what any model would earn.

    python -m mintel.ian.research.benchmark --synthetic all
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from dataclasses import dataclass
from typing import Optional, Sequence

from ...broker.base import Bar, Side
from .. import SYNTHETIC_LABEL
from ..mapping import root_of
from ..regime import BALANCED, RegimeRead
from ..score import chart_context, news_context, score
from ..trail import FlowRead, FlowTrail, TrailConfig
from .harness import Sample, sample

MODELS = ("price_only", "chart", "flow_only", "chart_flow", "chart_flow_news", "full")


@dataclass
class BenchConfig:
    stop_ticks: float = 6.0
    max_hold_s: float = 300.0
    cost_ticks: float = 1.5          # spread + commission + slippage, round trip
    cooldown_s: float = 30.0
    bar_s: int = 5


def _bars(samples: Sequence[Sample], upto: int, bar_s: int) -> list[Bar]:
    """5-second OHLC bars of the futures mid up to sample ``upto`` (a chart proxy when there are no MT5 bars)."""
    bars: list[Bar] = []
    cur: Optional[list] = None
    for s in samples[: upto + 1]:
        if s.fs.mid is None:
            continue
        key = int(s.ts.timestamp() // bar_s)
        m = s.fs.mid
        if cur is None or cur[0] != key:
            if cur is not None:
                bars.append(Bar(dt.datetime.fromtimestamp(cur[0] * bar_s, dt.timezone.utc), cur[1], cur[2], cur[3], cur[4]))
            cur = [key, m, m, m, m]
        else:
            cur[2], cur[3], cur[4] = max(cur[2], m), min(cur[3], m), m
    return bars


def _ema(vals: list[float], n: int) -> float:
    k = 2.0 / (n + 1.0)
    e = vals[0]
    for v in vals[1:]:
        e += k * (v - e)
    return e


def decide(model: str, samples: Sequence[Sample], i: int, calendar=(), cfg: Optional[BenchConfig] = None) -> int:
    cfg = cfg or BenchConfig()
    s = samples[i]
    fs = s.fs
    if not fs.ok:
        return 0
    if model == "price_only":
        return (1 if fs.move_medium_ticks > 0 else -1) if abs(fs.move_medium_ticks) >= 3 else 0
    if model == "chart":
        mids = [x.fs.mid for x in samples[max(0, i - 120): i + 1] if x.fs.mid is not None]
        if len(mids) < 60:
            return 0
        d = (_ema(mids, 20) - _ema(mids, 60)) / fs.tick
        return (1 if d > 0 else -1) if abs(d) >= 1.0 else 0
    neutral = RegimeRead(BALANCED, ["regime tactics off"], allow_entry=True)
    chart = None
    if model in ("chart_flow", "chart_flow_news", "full"):
        chart = chart_context("FUTURES-MID", _bars(samples, i, cfg.bar_s))
    news = news_context(root_of(fs.instrument), calendar, s.ts) if (calendar and model in ("chart_flow_news", "full")) else None
    reg = s.regime if model == "full" else neutral
    opp = score(fs, reg, spot_symbol="SPOT", chart=chart, news=news)
    if model == "full":
        return opp.direction if opp.tradeable else 0
    # without the regime tactics, the regime's own vetoes are off too: the evidence alone decides
    ok = opp.score >= 60 and len(opp.agree) >= 3 and not opp.against
    return opp.direction if ok else 0


def simulate(samples: Sequence[Sample], model: str, calendar=(), cfg: Optional[BenchConfig] = None) -> list[dict]:
    """Trades in TICKS. Entry at the touch (mid +/- half the cost), exit by the mechanical trail AT the stop
    (never better), or at the mid less half the cost after max_hold_s."""
    cfg = cfg or BenchConfig()
    trades: list[dict] = []
    pos = None
    cool_until: Optional[dt.datetime] = None
    half = cfg.cost_ticks / 2.0
    for i, s in enumerate(samples):
        fs = s.fs
        if fs.mid is None or not fs.tick:
            continue
        mid = fs.mid / fs.tick
        if pos is not None:
            side, entry, trail, t0 = pos
            mark = mid - side.sign * half
            hit = (mark <= trail.stop) if side is Side.BUY else (mark >= trail.stop)
            held = (s.ts - t0).total_seconds()
            if hit or held >= cfg.max_hold_s:
                exit_px = trail.stop if hit else mark
                trail.update(exit_px, FlowRead(available=False))
                r = (exit_px - entry) * side.sign
                trades.append({"model": model, "side": side.value, "entry_ts": t0.isoformat(), "ticks": round(r, 3),
                               "r": round(r / cfg.stop_ticks, 3), "mfe_r": round(trail.mfe_r, 3),
                               "mae_r": round(trail.mae_r, 3), "reason": "STOP" if hit else "TIME",
                               "regime": s.regime.regime})
                pos = None
                cool_until = s.ts + dt.timedelta(seconds=cfg.cooldown_s)
            else:
                trail.update(mark, FlowRead(available=False))
            continue
        if cool_until is not None and s.ts < cool_until:
            continue
        d = decide(model, samples, i, calendar, cfg)
        if d == 0:
            continue
        side = Side.BUY if d > 0 else Side.SELL
        entry = mid + side.sign * half
        trail = FlowTrail(side, entry, entry - side.sign * cfg.stop_ticks, TrailConfig(min_step_points=0.0), 1.0)
        pos = (side, entry, trail, s.ts)
    return trades


def stats(trades: list[dict], days: float) -> dict:
    n = len(trades)
    pnl = [t["ticks"] for t in trades]
    wins = [x for x in pnl if x > 0]
    losses = [x for x in pnl if x < 0]
    eq = peak = dd = 0.0
    for x in pnl:
        eq += x
        peak = max(peak, eq)
        dd = min(dd, eq - peak)
    pos_mfe = sum(t["mfe_r"] for t in trades if t["mfe_r"] > 0)
    avg = lambda xs: round(sum(xs) / len(xs), 3) if xs else None
    return {"trades": n, "per_day": round(n / days, 1) if days > 0 else None,
            "win_rate": round(len(wins) / n, 3) if n else None, "net_ticks": round(sum(pnl), 2),
            "expectancy_ticks": avg(pnl), "profit_factor": round(sum(wins) / -sum(losses), 2) if losses else None,
            "avg_win": avg(wins), "avg_loss": avg(losses), "max_drawdown_ticks": round(dd, 2),
            "mfe_r": avg([t["mfe_r"] for t in trades]), "mae_r": avg([t["mae_r"] for t in trades]),
            "mfe_capture": round(sum(t["r"] for t in trades) / pos_mfe, 3) if pos_mfe > 0 else None}


def run(sample_sets: Sequence[tuple[str, list[Sample], list]], data_source: str = "SYNTHETIC",
        cfg: Optional[BenchConfig] = None) -> dict:
    """``sample_sets``: (name, samples, calendar) per replayed session or scenario."""
    cfg = cfg or BenchConfig()
    res: dict = {"data_source": data_source,
                 "label": SYNTHETIC_LABEL if data_source == "SYNTHETIC" else f"{data_source} - research only",
                 "unit": "ticks of the future", "cost_ticks": cfg.cost_ticks, "sessions": [n for n, _, _ in sample_sets],
                 "models": {}}
    secs = sum((ss[-1].ts - ss[0].ts).total_seconds() for _, ss, _ in sample_sets if len(ss) > 1)
    days = secs / 86400.0
    for m in MODELS:
        allt: list[dict] = []
        by_session = {}
        for name, ss, cal in sample_sets:
            t = simulate(ss, m, cal, cfg)
            by_session[name] = len(t)
            allt += t
        st = stats(allt, days)
        st["trades_by_session"] = by_session
        res["models"][m] = st
    return res


def text(res: dict) -> str:
    lines = [f"BENCHMARK - {res['label']}", f"unit: {res['unit']}; costs {res['cost_ticks']} ticks per round trip; "
             f"sessions: {', '.join(res['sessions'])}", ""]
    lines.append(f"{'model':16s} {'trades':>6s} {'win':>6s} {'net':>8s} {'exp':>7s} {'PF':>6s} {'maxDD':>7s} "
                 f"{'MFE R':>6s} {'MAE R':>6s} {'capt':>6s}")
    for m, s in res["models"].items():
        f = lambda v, w=6: f"{v:>{w}}" if v is not None else f"{'-':>{w}}"
        lines.append(f"{m:16s} {s['trades']:>6d} {f(s['win_rate'])} {f(s['net_ticks'], 8)} {f(s['expectancy_ticks'], 7)} "
                     f"{f(s['profit_factor'])} {f(s['max_drawdown_ticks'], 7)} {f(s['mfe_r'])} {f(s['mae_r'])} {f(s['mfe_capture'])}")
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m mintel.ian.research.benchmark")
    ap.add_argument("--synthetic", default="", help="comma-separated scenarios, or 'all'")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    if not a.synthetic:
        ap.print_help()
        return 1
    from ..feeds.synthetic import SCENARIOS, SyntheticFeed, generate
    scs = [s for s in SCENARIOS if s not in ("sequence_gap", "stale_feed", "out_of_order")] \
        if a.synthetic == "all" else a.synthetic.split(",")
    sets = []
    for i, sc in enumerate(scs):
        evs, _ = generate(sc, seed=7 + i)
        cal = SyntheticFeed(sc, seed=7 + i).calendar_events() if sc == "news_shock" else []
        sets.append((sc, sample(evs, every_s=1.0, calendar=cal), cal))
    res = run(sets, "SYNTHETIC")
    print(json.dumps(res, indent=2, default=str) if a.json else text(res))
    return 0


if __name__ == "__main__":
    sys.exit(main())
