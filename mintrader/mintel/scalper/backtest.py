"""Real-tick backtesting for the Rapid Scalper.

    python -m mintel.scalper.backtest --config data/config.json --symbol EURUSD --days 3
    python -m mintel.scalper.backtest ... --stress          # +25%/+50% spread, slippage, latency, misses
    python -m mintel.scalper.backtest ... --walk-forward    # train / validate / unseen split

Ticks come from the broker's real tick history (MetaTrader's copy_ticks_range
through the bridge). Bid, ask, spread, commission, slippage, order latency,
minimum lots and broker stop levels are all modelled; a tick-by-tick replay,
never a candle approximation. The engine code under test is the live code.
"""
from __future__ import annotations

import argparse
import datetime as dt
import random
import statistics as st
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Optional, Sequence

from ..broker.base import Bar, Side, Tick
from ..clock import UTC, to_utc
from ..contracts import SymbolSpec
from .config import ScalperConfig
from .execution import Fill
from .features import TickBuffer, compute
from .manage import Manager, ScalpTrade
from .risk import plan
from .score import score


@dataclass
class Stress:
    spread_multiplier: float = 1.0
    extra_slippage_points: float = 0.0
    extra_latency_ms: float = 0.0
    miss_probability: float = 0.0
    commission_multiplier: float = 1.0
    seed: int = 7
    label: str = "baseline"


@dataclass
class BTTrade:
    symbol: str
    side: str
    opened: dt.datetime
    closed: dt.datetime
    entry: float
    exit: float
    volume: float
    net: float
    gross: float
    exit_reason: str
    peak_r: float
    mae_r: float
    confidence: float
    state: str
    duration: float


@dataclass
class BTResult:
    label: str
    trades: list = field(default_factory=list)
    rejected: int = 0
    attempted: int = 0

    @property
    def net(self) -> float:
        return sum(t.net for t in self.trades)

    def summary(self) -> dict:
        nets = [t.net for t in self.trades]
        wins = [x for x in nets if x > 0]; losses = [x for x in nets if x < 0]
        return {"label": self.label, "trades": len(nets), "net": round(self.net, 2),
                "win_rate": round(100 * len(wins) / len(nets), 1) if nets else None,
                "avg_win": round(st.mean(wins), 2) if wins else None,
                "avg_loss": round(st.mean(losses), 2) if losses else None,
                "profit_factor": round(sum(wins) / abs(sum(losses)), 2) if losses else None,
                "avg_duration_s": round(st.mean(t.duration for t in self.trades), 1) if nets else None,
                "full_stop_rate": round(100 * sum(1 for t in self.trades if t.exit_reason == "INITIAL_STOP") / len(nets), 1) if nets else None,
                "runners": sum(1 for t in self.trades if t.state == "RUNNER"),
                "rejected": self.rejected, "attempted": self.attempted}


def bars_from_ticks(ticks: Sequence[Tick]) -> list[Bar]:
    """M1 bars built from the ticks themselves (mid price, tick count as volume)."""
    out: list[Bar] = []
    cur: Optional[list] = None
    for t in ticks:
        m = t.time.replace(second=0, microsecond=0)
        if cur is None or cur[0] != m:
            if cur is not None:
                out.append(Bar(cur[0], cur[1], cur[2], cur[3], cur[4], cur[5]))
            cur = [m, t.mid, t.mid, t.mid, t.mid, 1.0]
        else:
            cur[2] = max(cur[2], t.mid); cur[3] = min(cur[3], t.mid); cur[4] = t.mid; cur[5] += 1.0
    if cur is not None:
        out.append(Bar(cur[0], cur[1], cur[2], cur[3], cur[4], cur[5]))
    return out


def replay(symbol: str, ticks: Sequence[Tick], spec: SymbolSpec, cfg: ScalperConfig,
           stress: Optional[Stress] = None) -> BTResult:
    """Tick-by-tick replay through the live scoring, sizing and management code."""
    stress = stress or Stress()
    rng = random.Random(stress.seed)
    res = BTResult(stress.label)
    if not ticks:
        return res
    # stress the spread around the mid
    if stress.spread_multiplier != 1.0:
        ticks = [Tick(t.symbol, t.time, spec.round_price(t.mid - (t.ask - t.bid) * stress.spread_multiplier / 2),
                      spec.round_price(t.mid + (t.ask - t.bid) * stress.spread_multiplier / 2), t.last, t.volume)
                 for t in ticks]
    cfg = replace(cfg, commission_per_lot_round_turn=cfg.commission_per_lot_round_turn * stress.commission_multiplier)
    buf = TickBuffer(symbol)
    bars = bars_from_ticks(ticks)
    bar_idx = 0
    manager = Manager(cfg)
    trade: Optional[ScalpTrade] = None
    pending: Optional[tuple] = None          # (ready_time, side, plan, opp)
    last_eval: Optional[dt.datetime] = None
    last_entry: Optional[dt.datetime] = None
    last_loss: Optional[dt.datetime] = None
    slip_pts = cfg.paper_slippage_points + stress.extra_slippage_points
    latency = dt.timedelta(milliseconds=cfg.paper_latency_ms + stress.extra_latency_ms)

    def fill_price(side: Side, t: Tick) -> tuple[float, float]:
        req = t.ask if side is Side.BUY else t.bid
        return req, spec.normalise_price(req + side.sign * slip_pts * spec.point)

    def close_at(t: Tick, px: float, reason: str, detail: str) -> None:
        nonlocal trade, last_loss
        assert trade is not None
        exit_px = spec.normalise_price(px - trade.side.sign * slip_pts * spec.point)
        gross = spec.money((exit_px - trade.entry_filled) * trade.side.sign, trade.volume)
        net = gross - cfg.commission_per_lot_round_turn * trade.volume
        res.trades.append(BTTrade(symbol, trade.side.value, trade.opened_at, t.time, trade.entry_filled, exit_px,
                                  trade.volume, net, gross, reason, trade.peak_r, trade.mae_r, trade.confidence,
                                  trade.state, (t.time - trade.opened_at).total_seconds()))
        if net < 0:
            last_loss = t.time
        trade = None

    for t in ticks:
        buf.add(t)
        while bar_idx < len(bars) and bars[bar_idx].time <= t.time.replace(second=0, microsecond=0) - dt.timedelta(minutes=1):
            bar_idx += 1
        visible_bars = bars[max(0, bar_idx - 80):bar_idx]
        # pending entry waiting for latency
        if pending is not None and t.time >= pending[0]:
            ready, side, rp, opp = pending
            pending = None
            if rng.random() < stress.miss_probability:
                res.rejected += 1
            else:
                req, px = fill_price(side, t)
                trade = ScalpTrade(len(res.trades) + 1, symbol, side, rp.volume, req, px, t.time, rp.stop, rp.stop,
                                   abs(px - rp.stop), rp.planned_loss, spec.point, spec=spec,
                                   confidence=opp.confidence, components={**opp.contributions, **opp.penalties})
                last_entry = t.time
        if trade is not None:
            hit = (t.bid <= trade.stop) if trade.side is Side.BUY else (t.ask >= trade.stop)
            if hit:
                reason = "INITIAL_STOP" if abs(trade.stop - trade.initial_stop) < 1e-12 else "PROFIT_TRAIL"
                close_at(t, trade.stop, reason, "stop filled")
                continue
            f = compute(symbol, buf, visible_bars, spec.point, t.time)
            d = manager.update(trade, t, f if f.ok else None, t.time)
            if d.close:
                close_at(t, t.bid if trade.side is Side.BUY else t.ask, d.exit_reason, d.reason)
            continue
        if last_eval is not None and (t.time - last_eval).total_seconds() < cfg.scan_interval_seconds:
            continue
        last_eval = t.time
        if last_entry and (t.time - last_entry).total_seconds() < cfg.min_seconds_between_entries:
            continue
        if last_loss and (t.time - last_loss).total_seconds() < cfg.cooldown_after_loss_seconds:
            continue
        f = compute(symbol, buf, visible_bars, spec.point, t.time)
        opp = score(f, cfg)
        if opp.direction == 0:
            continue
        res.attempted += 1
        if not opp.tradable:
            res.rejected += 1
            continue
        side = Side.BUY if opp.direction > 0 else Side.SELL
        spread = t.ask - t.bid
        entry = t.ask if side is Side.BUY else t.bid
        inval = (f.micro_low - cfg.invalidation_buffer_spreads * spread) if side is Side.BUY \
            else (f.micro_high + cfg.invalidation_buffer_spreads * spread)
        rp = plan(spec, side, entry, inval, spread, cfg)
        if not rp.ok:
            res.rejected += 1
            continue
        pending = (t.time + latency, side, rp, opp)
    if trade is not None:
        close_at(ticks[-1], ticks[-1].bid if trade.side is Side.BUY else ticks[-1].ask, "SYSTEM_SHUTDOWN", "end of data")
    return res


STRESSES = [
    Stress(label="baseline"),
    Stress(spread_multiplier=1.25, label="+25% spread"),
    Stress(spread_multiplier=1.5, label="+50% spread"),
    Stress(extra_slippage_points=2.0, label="+2pt slippage"),
    Stress(extra_latency_ms=400.0, label="+400ms latency"),
    Stress(miss_probability=0.2, label="20% missed fills"),
    Stress(commission_multiplier=1.5, label="+50% commission"),
    Stress(spread_multiplier=1.25, extra_slippage_points=1.0, extra_latency_ms=200.0, label="worse everything"),
]


def stress_test(symbol, ticks, spec, cfg) -> list[dict]:
    rows = [replay(symbol, ticks, spec, cfg, s).summary() for s in STRESSES]
    base = rows[0]["net"]
    for r in rows:
        r["fragile"] = bool(base > 0 and r["net"] < 0.25 * base)
    return rows


def neighbourhood(cfg: ScalperConfig) -> list[tuple[str, ScalperConfig]]:
    """Nearby parameter sets: a robust region, not one magic combination."""
    out = []
    for dc in (-5.0, 0.0, 5.0):
        for dg in (-0.1, 0.0, 0.1):
            out.append((f"conf{cfg.min_confidence + dc:.0f}/giveback{cfg.giveback_established + dg:.2f}",
                        replace(cfg, min_confidence=cfg.min_confidence + dc,
                                giveback_established=cfg.giveback_established + dg,
                                giveback_runner=cfg.giveback_runner + dg)))
    return out


def walk_forward(symbol, ticks: Sequence[Tick], spec, cfg) -> dict:
    """Split the days into train / validate / unseen and report each."""
    days = sorted({t.time.date() for t in ticks})
    if len(days) < 3:
        return {"error": "need at least three days of ticks for a walk-forward split"}
    n = len(days)
    train, val, test = days[: max(1, n // 2)], days[max(1, n // 2): max(2, (3 * n) // 4)], days[max(2, (3 * n) // 4):]
    def sub(ds): return [t for t in ticks if t.time.date() in set(ds)]
    out = {"train_days": [str(d) for d in train], "validate_days": [str(d) for d in val], "unseen_days": [str(d) for d in test]}
    rows = []
    for label, c in neighbourhood(cfg):
        rows.append({"params": label,
                     "train": replay(symbol, sub(train), spec, c).summary(),
                     "validate": replay(symbol, sub(val), spec, c).summary(),
                     "unseen": replay(symbol, sub(test), spec, c).summary()})
    out["results"] = rows
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Rapid Scalper real-tick backtest")
    ap.add_argument("--config", default="data/config.json")
    ap.add_argument("--scalper", default="")
    ap.add_argument("--symbol", default="EURUSD")
    ap.add_argument("--days", type=int, default=2)
    ap.add_argument("--stress", action="store_true")
    ap.add_argument("--walk-forward", action="store_true")
    args = ap.parse_args(argv)
    from ..config import Config
    from ..run import build_broker
    cfg = Config.load(args.config)
    scfg = ScalperConfig.load(args.scalper or ScalperConfig.default_path(cfg.ops.data_dir))
    broker = build_broker(cfg)
    if not broker.connect():
        print("Could not connect to the bridge; is the bot running?")
        return 3
    end = to_utc(dt.datetime.now(UTC)); start = end - dt.timedelta(days=args.days)
    ticks = list(broker.ticks_range(args.symbol, start, end))
    spec = broker.spec(args.symbol)
    print(f"{len(ticks)} real ticks for {args.symbol} over {args.days} day(s)")
    if not ticks or spec is None:
        print("no tick history available from the broker for that range")
        return 2
    if args.walk_forward:
        wf = walk_forward(args.symbol, ticks, spec, scfg)
        for k in ("train_days", "validate_days", "unseen_days"):
            print(k, wf.get(k))
        for r in wf.get("results", []):
            print(f"{r['params']:28s} train {r['train']['net']:+8.2f} ({r['train']['trades']})  "
                  f"validate {r['validate']['net']:+8.2f} ({r['validate']['trades']})  "
                  f"unseen {r['unseen']['net']:+8.2f} ({r['unseen']['trades']})")
        return 0
    rows = stress_test(args.symbol, ticks, spec, scfg) if args.stress else [replay(args.symbol, ticks, spec, scfg).summary()]
    for r in rows:
        flag = "  FRAGILE" if r.get("fragile") else ""
        print(f"{r['label']:20s} trades {r['trades']:4d} net {r['net']:+8.2f} win {r['win_rate']} "
              f"pf {r['profit_factor']} full-stop {r['full_stop_rate']}% runners {r['runners']}{flag}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
