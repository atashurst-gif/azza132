"""Check the numbers: is the bridge current, and what does the broker say?

    python -m mintel.ops.ledger_check --config data/config.json

Prints, in plain English, whether the running bridge is the installed code,
what the broker's deal history returns for today and for this strategy, and
what the status page will therefore show. Read-only.

When Trend & Breakout is on PAPER (config.json "tnb" -> "mode") its own
line is its REAL deals only (positions left from LIVE: real money), and a
line under it gives its practice deals since midnight from its paper record
(data/tnb_paper.sqlite, read through ``PaperBroker(..., read_only=True)``):
not money, never in the account.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys

from ..clock import to_utc, utcnow
from ..config import Config


def practice_lines(cfg: Config, day: dt.datetime, mode: str) -> list[str]:
    """Trend & Breakout's paper deals since ``day``, from its paper record:
    nothing on LIVE with no paper deal that day (as it always was)."""
    from .standing import TnbPaperRecord
    record = TnbPaperRecord(cfg.ops.data_dir, cfg.magic)
    if record.error:
        return [f"Trend & Breakout's practice : FAIL - {record.error}"]
    rows = record.deals_in(day)
    if mode != "PAPER" and not rows:
        return []
    closes = [r for r in rows if not r.get("is_entry")]
    entries = [r for r in rows if r.get("is_entry")]
    net = sum(float(r.get("profit") or 0) for r in closes)
    out = [f"Practice since midnight (Trend & Breakout's paper record, not money): {len(entries)} entries, "
           f"{len(closes)} exits, net {net:+.2f} incl. commission"]
    for r in closes[-5:]:
        out.append(f"    {str(r.get('time'))[:19]} {r.get('symbol'):8} "
                   f"pos {r.get('position')} {float(r.get('profit') or 0):+.2f} (paper)")
    return out


def print_practice(cfg: Config, day: dt.datetime, mode: str) -> None:
    for line in practice_lines(cfg, day, mode):
        print(line)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="check the profit figures at source")
    ap.add_argument("--config", default="data/config.json")
    args = ap.parse_args(argv)
    cfg = Config.load(args.config)
    from ..run import build_broker
    from ..broker.stamp import code_stamp
    from .standing import tnb_mode
    broker = build_broker(cfg)
    mode = tnb_mode(cfg)
    print(f"Installed bridge code stamp : {code_stamp()}")
    print(f"Measuring from              : {cfg.tracking_start_utc or '(midnight)'}  "
          f"[{cfg.tracking_strategy or 'no strategy stamp'}]")
    print(f"Bot's magic number          : {cfg.magic}")
    if mode == "PAPER":
        print("Trend & Breakout            : PAPER - real prices, simulated orders; its practice is never in "
              "the account")
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
        bot_label = ("Trend & Breakout's REAL trades (left from LIVE)" if mode == "PAPER"
                     else "the bot's trades only")
        for label, magic in (("all trades on the account", 0),
                             (bot_label, cfg.magic)):
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
        print_practice(cfg, day, mode)
        return 0
    finally:
        try:
            broker.disconnect() if hasattr(broker, "disconnect") else None
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
