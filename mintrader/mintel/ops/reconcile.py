"""Reconcile a day against MetaTrader's own deal history, per bot.

    python -m mintel.ops.reconcile --config data/config.json --day 2026-10-02

Prints exactly what the broker recorded for that day: for each magic
number (Trend & Breakout, Rapid Momentum Rider, Momentum Runner, Band
Breaker, Crowd Fader, Financial Ian, the retired Rapid Scalper, the lab
ideas trading on the demo (magics 992001-992999, ``mintel/labdemo``), and
anything else, placed by hand), the closed positions, the price result,
the commission and the net, then the account total, which must equal the
figure at the bottom of MetaTrader's History tab for the same day.
Read-only; nothing is changed.
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


def bots(cfg: Config, scalper_magic: int) -> list[tuple[str, str, int]]:
    """(key, label, magic) for every bot that places its own orders, in the
    order the report shows them. Every magic number is a different bot; no
    two bots ever share one."""
    from ..runner import STRATEGY_LABEL as RUNNER_LABEL
    from ..bandbreaker import STRATEGY_LABEL as BB_LABEL
    from ..crowd import STRATEGY_LABEL as CF_LABEL
    from ..rider import MAGIC as RIDER_MAGIC, STRATEGY_LABEL as RIDER_LABEL
    from ..ian import MAGIC as IAN_MAGIC, STRATEGY_LABEL as IAN_LABEL
    # The Rider's and Financial Ian's magic numbers are fixed in their own
    # code, exactly as their orders carry them: Ian ignores any "magic" in
    # ian.json, so the file is not read here either.
    return [("mi", EXISTING_STRATEGY_LABEL, int(cfg.magic)),
            ("rider", RIDER_LABEL, int(RIDER_MAGIC)),
            ("runner", RUNNER_LABEL, int(cfg.runner.magic)),
            ("bandbreaker", BB_LABEL, int(cfg.bandbreaker.magic)),
            ("crowd", CF_LABEL, int(cfg.crowd.magic)),
            ("ian", IAN_LABEL, int(IAN_MAGIC)),
            ("rs", f"{STRATEGY_LABEL}, retired", int(scalper_magic))]


def _lab_rows(rows: list[dict], fetch, cfg: Config, known: set) -> list[dict]:
    """The deal rows of the lab ideas' positions on the demo (magics
    992001-992999), each position by the magic that opened it, never one a
    bot already claimed. An older bridge whose rows do not say their magic
    is asked per lab magic handed out so far (data/labdemo/magics.json)."""
    from .standing import is_lab_magic, lab_magics_handed_out
    if any("magic" not in r for r in rows):
        return [r for m in sorted(lab_magics_handed_out(cfg)) for r in fetch(m)
                if int(r.get("position") or 0) not in known]
    opener: dict[int, int] = {}
    for r in rows:
        if r.get("is_entry") and r.get("position"):
            opener.setdefault(int(r["position"]), int(r.get("magic") or 0))
    return [r for r in rows if int(r.get("position") or 0) not in known
            and is_lab_magic(opener.get(int(r.get("position") or 0), r.get("magic") or 0))]


def reconcile(broker, cfg: Config, day: dt.date, scalper_magic: int) -> dict:
    """The broker's day split by magic number: one entry per bot (keys "mi",
    "rider", "runner", "bandbreaker", "crowd", "ian", "rs", listed in
    "bots"), "lab" for the lab ideas' positions on the demo (magics
    992001-992999), "other" for every closed position that carried none of
    them (placed by hand), and "all"."""
    start, end = _day_bounds(broker, day)
    fetch = lambda magic: broker.deals_since(start, magic, False) or []     # noqa: E731
    rows = fetch(0)
    everything = summarise(rows, start, end)
    out: dict = {"start": start, "end": end, "all": everything, "bots": []}
    known: set = set()
    for key, label, magic in bots(cfg, scalper_magic):
        s = summarise(fetch(magic), start, end)
        s["label"] = label
        s["magic"] = magic
        out[key] = s
        out["bots"].append(key)
        known |= set(s["by_position"])
    # 14 Oct: the lab ideas the squad's pick promoted trade real demo orders: their line, never "manual"
    out["lab"] = summarise(_lab_rows(rows, fetch, cfg, known), start, end)
    known |= set(out["lab"]["by_position"])
    other = {k: v for k, v in everything["by_position"].items() if k not in known}
    other_net = round(sum(v["net"] for v in other.values()), 2)
    out["other"] = {"positions": len(other), "net": other_net,
                    "commission": round(sum(v["commission"] for v in other.values()), 2)}
    return out


def _bot_keys(res: dict) -> list[str]:
    return list(res.get("bots") or [k for k in ("mi", "rs") if k in res])


def render(res: dict, cfg: Config, day: dt.date, account=None) -> str:
    def row(label, s):
        return (f"  {label:<40} positions {s['positions']:3d}  wins {s['wins']:3d}  losses {s['losses']:3d}  "
                f"price {s['price']:+9.2f}  commission {s['commission']:+8.2f}  NET {s['net']:+9.2f}")
    keys = _bot_keys(res)
    bots_net = round(sum(res[k]["net"] for k in keys), 2)
    lines = [f"BROKER RECORD FOR {day.isoformat()}  (MetaTrader day {res['start']:%Y-%m-%d %H:%M} to "
             f"{res['end']:%Y-%m-%d %H:%M} UTC)",
             "-" * 118]
    for k in keys:
        s = res[k]
        label = s.get("label") or k
        magic = s.get("magic")
        lines.append(row(f"{label} (magic {magic})" if magic else label, s))
    if res.get("lab") is not None:
        lines.append(row("lab ideas on the demo (992001-992999)", res["lab"]))
    lines += [f"  {'other / manual':<40} positions {res['other']['positions']:3d}"
              f"{'':40} commission {res['other']['commission']:+8.2f}  NET {res['other']['net']:+9.2f}",
              "-" * 118,
              row("ACCOUNT", res["all"]),
              "",
              f"  MetaTrader's History tab for this day should show Profit {res['all']['net']:+.2f} at the bottom,",
              f"  with {res['all']['price']:+.2f} on the right (the Profit column before commission).",
              f"  Bots added together: {bots_net:+.2f}; "
              f"difference to the account: {res['all']['net'] - bots_net:+.2f} "
              f"(anything here is the lab ideas' line above, or a deal that carried none of these magic "
              f"numbers: placed by hand)."]
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
            for k in _bot_keys(res):
                s = res[k]
                label = s.get("label") or k
                for ticket, v in sorted(s["by_position"].items()):
                    print(f"  {label:<22} {ticket:<12} {v['symbol']:<8} commission {v['commission']:+6.2f}  net {v['net']:+8.2f}")
    finally:
        try:
            broker.disconnect()
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
