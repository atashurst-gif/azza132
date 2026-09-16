"""Plain-English review of a trading day, from the broker's own records.

    python -m mintel.ops.day_review --config data/config.json [--date 2026-09-14]

Every closed position of the bot's (its magic number) is read from the
broker's deal history - price result, commission and swap separately - and
joined with the journal where the journal knows the trade (tactic, regime).
The point is to answer, in words, "what should change tomorrow?".
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from collections import defaultdict
from typing import Optional

from ..clock import UTC, to_utc, utcnow
from ..config import Config


def group_positions(rows: list[dict]) -> list[dict]:
    """Deals -> positions: entry time, exit time, symbol, volume, money."""
    by_pos: dict = {}
    for r in rows:
        pos = r.get("position") or 0
        p = by_pos.setdefault(pos, {
            "position": pos, "symbol": r.get("symbol", ""), "volume": 0.0,
            "gross": 0.0, "commission": 0.0, "opened": None, "closed": None})
        when = r.get("time")
        if r.get("is_entry"):
            p["opened"] = when if p["opened"] is None else min(p["opened"], when)
        else:
            p["closed"] = when if p["closed"] is None else max(p["closed"], when)
            p["volume"] += float(r.get("volume") or 0.0)
            # profit in these rows already includes commission and swap
            p["gross"] += float(r.get("profit") or 0.0) - float(r.get("commission") or 0.0)
        p["commission"] += float(r.get("commission") or 0.0)
    out = []
    for p in by_pos.values():
        if p["closed"] is None:
            continue                      # still open
        p["net"] = round(p["gross"] + p["commission"], 2)
        p["gross"] = round(p["gross"], 2)
        p["commission"] = round(p["commission"], 2)
        if p["opened"] and p["closed"]:
            p["minutes"] = round((p["closed"] - p["opened"]).total_seconds() / 60, 1)
        else:
            p["minutes"] = None
        out.append(p)
    out.sort(key=lambda p: p["closed"])
    return out


def review(positions: list[dict], journal_rows: Optional[list[dict]] = None,
           currency: str = "GBP") -> str:
    if not positions:
        return "No closed trades of the bot's on that day."
    ccy = currency
    n = len(positions)
    gross = sum(p["gross"] for p in positions)
    comm = sum(p["commission"] for p in positions)
    net = sum(p["net"] for p in positions)
    wins = [p for p in positions if p["net"] > 0]
    losses = [p for p in positions if p["net"] < 0]
    durations = [p["minutes"] for p in positions if p["minutes"] is not None]
    quick = [p for p in positions if p["minutes"] is not None and p["minutes"] < 5]
    lines = []
    lines.append("DAY REVIEW (bot trades only, from the broker's records)")
    lines.append("-" * 62)
    lines.append(f"Trades          : {n}   (wins {len(wins)}, losses {len(losses)}, "
                 f"win rate {len(wins) / n * 100:.0f}%)")
    lines.append(f"Price result    : {gross:+.2f} {ccy}   (what the trades made on price)")
    lines.append(f"Commission      : {comm:+.2f} {ccy}   (what the broker charged)")
    lines.append(f"REAL RESULT     : {net:+.2f} {ccy}")
    if wins and losses:
        avg_w = sum(p["net"] for p in wins) / len(wins)
        avg_l = sum(p["net"] for p in losses) / len(losses)
        lines.append(f"Average win     : {avg_w:+.2f} {ccy}   average loss: {avg_l:+.2f} {ccy}"
                     f"   (win/loss size ratio {abs(avg_w / avg_l):.2f})")
    if durations:
        durations.sort()
        med = durations[len(durations) // 2]
        lines.append(f"Time in trade   : median {med:.0f} min; {len(quick)} of {n} lasted under 5 minutes")
    lines.append("")
    lines.append("By market:")
    by_sym: dict = defaultdict(list)
    for p in positions:
        by_sym[p["symbol"]].append(p)
    for sym, ps in sorted(by_sym.items(), key=lambda kv: sum(p["net"] for p in kv[1])):
        w = sum(1 for p in ps if p["net"] > 0)
        lines.append(f"  {sym:<10} {len(ps):>3} trades  net {sum(p['net'] for p in ps):+8.2f}"
                     f"  commission {sum(p['commission'] for p in ps):+7.2f}  wins {w}/{len(ps)}")
    if journal_rows:
        by_tactic: dict = defaultdict(list)
        jr = {int(r.get("ticket") or 0): r for r in journal_rows}
        for p in positions:
            r = jr.get(int(p["position"]))
            if r:
                by_tactic[f"{r.get('tactic', '?')} in {r.get('regime', '?')}"].append(p)
        if by_tactic:
            lines.append("")
            lines.append("By approach (where the journal knows the trade):")
            for name, ps in sorted(by_tactic.items(), key=lambda kv: sum(p["net"] for p in kv[1])):
                w = sum(1 for p in ps if p["net"] > 0)
                lines.append(f"  {name:<44} {len(ps):>3}  net {sum(p['net'] for p in ps):+8.2f}  wins {w}/{len(ps)}")
        by_exit: dict = defaultdict(list)
        for p in positions:
            r = jr.get(int(p["position"]))
            if r:
                by_exit[str(r.get("exit_reason") or "broker stop or target")[:60]].append(p)
        if by_exit:
            lines.append("")
            lines.append("By exit reason (what closed the trade):")
            for name, ps in sorted(by_exit.items(), key=lambda kv: sum(p["net"] for p in kv[1])):
                w = sum(1 for p in ps if p["net"] > 0)
                lines.append(f"  {name:<44} {len(ps):>3}  net {sum(p['net'] for p in ps):+8.2f}  wins {w}/{len(ps)}")
    lines.append("")
    lines.append("What this says:")
    if comm and gross > 0 and net < 0:
        lines.append(f"  - The trades made money on price but commission ({abs(comm):.0f} {ccy}) was "
                     f"bigger than the winnings. Fewer, larger-target trades needed.")
    if durations and len(quick) > n * 0.4:
        lines.append(f"  - {len(quick)} trades were over inside 5 minutes: stops were inside the "
                     f"market's normal noise. Wider (volatility-floored) stops and fewer entries.")
    if wins and losses and abs(sum(p["net"] for p in wins) / len(wins)) < abs(sum(p["net"] for p in losses) / len(losses)):
        lines.append("  - Average win is smaller than average loss: winners are being cut early "
                     "or losers given too much room; check the exit logic and the targets.")
    worst = min(by_sym.items(), key=lambda kv: sum(p["net"] for p in kv[1]))
    if len(worst[1]) >= 3 and sum(p["net"] for p in worst[1]) < 0:
        lines.append(f"  - {worst[0]} cost the most ({sum(p['net'] for p in worst[1]):+.2f} over "
                     f"{len(worst[1])} trades): the per-market daily loss limit will now stop that.")
    if net > 0:
        lines.append(f"  - A profitable day: {net:+.2f} {ccy} after every cost.")
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="plain-English review of a trading day")
    ap.add_argument("--config", default="data/config.json")
    ap.add_argument("--date", default="", help="YYYY-MM-DD (UTC); default today")
    args = ap.parse_args(argv)
    cfg = Config.load(args.config)
    from ..run import build_broker
    broker = build_broker(cfg)
    broker.connect()
    try:
        day = (dt.datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=UTC)
               if args.date else to_utc(utcnow()).replace(hour=0, minute=0, second=0, microsecond=0))
        rows = broker.deals_since(day, cfg.magic, False) or []
        rows = [r for r in rows if r.get("time") and r["time"] < day + dt.timedelta(days=1)]
        try:
            currency = broker.account().currency
        except Exception:
            currency = "GBP"
        journal_rows = []
        try:
            from ..engine.journal import Journal
            from pathlib import Path
            j = Journal(Path(cfg.ops.data_dir) / "journal.sqlite")
            journal_rows = j.closed_trades(limit=2000, since=day)
        except Exception:
            pass
        print(review(group_positions(rows), journal_rows, currency))
        return 0
    finally:
        try:
            broker.disconnect() if hasattr(broker, "disconnect") else None
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
