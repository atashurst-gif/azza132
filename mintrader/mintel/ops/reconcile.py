"""Reconcile a day against MetaTrader's own deal history, per bot.

    python -m mintel.ops.reconcile --config data/config.json --day 2026-10-02

Prints exactly what the broker recorded for that day: for each magic
number (Trend & Breakout, Rapid Scalper, anything else), the closed
positions, the price result, the commission and the net, then the account
total, which must equal the figure at the bottom of MetaTrader's History
tab for the same day. Read-only; nothing is changed.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from collections import defaultdict

from ..clock import to_utc, utcnow
from ..config import Config
from ..scalper import EXISTING_STRATEGY_LABEL, STRATEGY_LABEL
from ..scalper.config import ScalperConfig


def _day_bounds(broker, day: dt.date) -> tuple[dt.datetime, dt.datetime]:
    """The broker's trading day (what MetaTrader's History tab calls a day)."""
    noon = dt.datetime(day.year, day.month, day.day, 12, tzinfo=dt.timezone.utc)
    clock = getattr(broker, "clock", None)
    if clock is not None and hasattr(clock, "day_start_utc"):
        try:
            start = clock.day_start_utc(noon)
            return start, start + dt.timedelta(days=1)
        except Exception:
            pass
    start = dt.datetime(day.year, day.month, day.day, tzinfo=dt.timezone.utc)
    return start, start + dt.timedelta(days=1)


def summarise(rows: list[dict], start: dt.datetime, end: dt.datetime) -> dict:
    """Per-position totals from deal rows (entries and exits), inside [start, end)."""
    per: dict[int, dict] = defaultdict(lambda: {"net": 0.0, "commission": 0.0, "closed": False, "symbol": ""})
    for r in rows:
        t = r.get("time")
        if t is not None and not (start <= to_utc(t) < end):
            continue
        p = per[int(r.get("position") or 0)]
        p["net"] += float(r.get("profit") or 0.0)            # profit + commission + swap
        p["commission"] += float(r.get("commission") or 0.0)
        p["symbol"] = r.get("symbol") or p["symbol"]
        if not r.get("is_entry"):
            p["closed"] = True
    closed = {k: v for k, v in per.items() if v["closed"]}
    nets = [v["net"] for v in closed.values()]
    commission = sum(v["commission"] for v in closed.values())
    return {"positions": len(closed), "wins": sum(1 for x in nets if x > 0),
            "losses": sum(1 for x in nets if x < 0),
            "net": round(sum(nets), 2), "commission": round(commission, 2),
            "price": round(sum(nets) - commission, 2), "by_position": closed}


def reconcile(broker, cfg: Config, day: dt.date, scalper_magic: int) -> dict:
    start, end = _day_bounds(broker, day)
    fetch = lambda magic: broker.deals_since(start, magic, False) or []     # noqa: E731
    everything = summarise(fetch(0), start, end)
    mi = summarise(fetch(cfg.magic), start, end)
    rs = summarise(fetch(scalper_magic), start, end)
    known = set(mi["by_position"]) | set(rs["by_position"])
    other = {k: v for k, v in everything["by_position"].items() if k not in known}
    other_net = round(sum(v["net"] for v in other.values()), 2)
    return {"start": start, "end": end, "all": everything, "mi": mi, "rs": rs,
            "other": {"positions": len(other), "net": other_net,
                      "commission": round(sum(v["commission"] for v in other.values()), 2)}}


def render(res: dict, cfg: Config, day: dt.date, account=None) -> str:
    def row(label, s):
        return (f"  {label:<18} positions {s['positions']:3d}  wins {s['wins']:3d}  losses {s['losses']:3d}  "
                f"price {s['price']:+9.2f}  commission {s['commission']:+8.2f}  NET {s['net']:+9.2f}")
    lines = [f"BROKER RECORD FOR {day.isoformat()}  (MetaTrader day {res['start']:%Y-%m-%d %H:%M} to "
             f"{res['end']:%Y-%m-%d %H:%M} UTC)",
             "-" * 96,
             row(f"{EXISTING_STRATEGY_LABEL} (magic {cfg.magic})", res["mi"]),
             row(f"{STRATEGY_LABEL}", res["rs"]),
             f"  {'other / manual':<18} positions {res['other']['positions']:3d}"
             f"{'':40} commission {res['other']['commission']:+8.2f}  NET {res['other']['net']:+9.2f}",
             "-" * 96,
             row("ACCOUNT", res["all"]),
             "",
             f"  MetaTrader's History tab for this day should show Profit {res['all']['net']:+.2f} at the bottom,",
             f"  with {res['all']['price']:+.2f} on the right (the Profit column before commission).",
             f"  Bots added together: {res['mi']['net'] + res['rs']['net']:+.2f}; "
             f"difference to the account: {res['all']['net'] - res['mi']['net'] - res['rs']['net']:+.2f} "
             f"(anything here is a deal that carried neither bot's magic number)."]
    if account is not None:
        lines.append(f"  Account now: balance {account.balance:.2f}, equity {account.equity:.2f}")
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="reconcile a day against the broker's deal history, per bot")
    ap.add_argument("--config", default="data/config.json")
    ap.add_argument("--day", default="", help="YYYY-MM-DD (default: today, broker day)")
    ap.add_argument("--positions", action="store_true", help="list every position")
    args = ap.parse_args(argv)
    cfg = Config.load(args.config)
    from ..run import build_broker
    scfg = ScalperConfig.load(ScalperConfig.default_path(cfg.ops.data_dir))
    broker = build_broker(cfg)
    if not broker.connect():
        print("FAIL: could not connect to the bridge/MetaTrader", file=sys.stderr)
        return 2
    try:
        day = dt.date.fromisoformat(args.day) if args.day else to_utc(utcnow()).date()
        res = reconcile(broker, cfg, day, scfg.magic)
        try:
            account = broker.account()
        except Exception:
            account = None
        print(render(res, cfg, day, account))
        if args.positions:
            print("\nPOSITIONS (net after commission):")
            for label, s in ((EXISTING_STRATEGY_LABEL, res["mi"]), (STRATEGY_LABEL, res["rs"])):
                for ticket, v in sorted(s["by_position"].items()):
                    print(f"  {label:<18} {ticket:<12} {v['symbol']:<8} commission {v['commission']:+6.2f}  net {v['net']:+8.2f}")
    finally:
        try:
            broker.disconnect()
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
