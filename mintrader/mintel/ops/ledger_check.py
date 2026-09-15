"""Check the numbers: is the bridge current, and what does the broker say?

    python -m mintel.ops.ledger_check --config data/config.json

Prints, in plain English, whether the running bridge is the installed code,
what the broker's deal history returns for today and for this strategy, and
what the status page will therefore show. Read-only.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys

from ..clock import to_utc, utcnow
from ..config import Config


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="check the profit figures at source")
    ap.add_argument("--config", default="data/config.json")
    args = ap.parse_args(argv)
    cfg = Config.load(args.config)
    from ..run import build_broker
    from ..broker.stamp import code_stamp
    broker = build_broker(cfg)
    print(f"Installed bridge code stamp : {code_stamp()}")
    print(f"Measuring from              : {cfg.tracking_start_utc or '(midnight)'}  "
          f"[{cfg.tracking_strategy or 'no strategy stamp'}]")
    print(f"Bot's magic number          : {cfg.magic}")
    if not broker.connect():
        print("FAIL: could not connect to the bridge/MetaTrader")
        return 2
    try:
        info = getattr(broker, "ping", lambda: None)() or {}
        if info:
            print(f"Running bridge              : pid {info.get('pid')}, protocol "
                  f"{info.get('protocol')}, code stamp {info.get('code_stamp') or 'UNSTAMPED (old)'}")
            why = getattr(broker, "outdated", lambda: "")()
            print("Bridge up to date           : " + ("yes" if not why else "NO - " + why))
        now = to_utc(utcnow())
        day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        for label, magic in (("all trades on the account", 0),
                             ("the bot's trades only", cfg.magic)):
            try:
                rows = broker.deals_since(day, magic, False) or []
            except Exception as exc:
                print(f"Deals since midnight ({label}): FAIL - {exc}")
                continue
            closes = [r for r in rows if not r.get("is_entry")]
            entries = [r for r in rows if r.get("is_entry")]
            net = sum(float(r.get("profit") or 0) for r in closes)
            print(f"Deals since midnight ({label}): {len(entries)} entries, "
                  f"{len(closes)} exits, net {net:+.2f} incl. commission")
            for r in closes[-5:]:
                print(f"    {str(r.get('time'))[:19]} {r.get('symbol'):8} "
                      f"pos {r.get('position')} {float(r.get('profit') or 0):+.2f}")
        return 0
    finally:
        try:
            broker.disconnect() if hasattr(broker, "disconnect") else None
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
