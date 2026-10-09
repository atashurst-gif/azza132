"""Financial Ian command line.

    python -m mintel.ian --config <data>/config.json --set-key
        asks for the Databento API key and saves it in secrets.json ("databento_api_key")
    python -m mintel.ian --config <data>/config.json --check-feed
        says whether the institutional feed is configured and, if so, whether it connects
    python -m mintel.ian --config <data>/config.json --status
        the bot's last status in plain English
    python -m mintel.ian --config <data>/config.json --demo SCENARIO
        plays a synthetic scenario through the whole pipeline (SYNTHETIC - NOT PERFORMANCE)
"""
from __future__ import annotations

import argparse
import getpass
import json
import sys
import tempfile
import time
from pathlib import Path


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m mintel.ian")
    ap.add_argument("--config", required=True)
    ap.add_argument("--set-key", action="store_true")
    ap.add_argument("--check-feed", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--demo", default="")
    a = ap.parse_args(argv)
    data_dir = Path(a.config).parent
    if a.set_key:
        from .feeds.databento import save_api_key
        key = getpass.getpass("Databento API key (starts with db-, not shown): ").strip()
        if not key:
            print("Nothing saved.")
            return 1
        if not key.startswith("db-"):
            print("That does not look like a Databento key (they start with db-). Saved anyway.")
        p = save_api_key(data_dir, key)
        print(f"Key saved to {p}. Set \"feed\": {{\"vendor\": \"databento\"}} in ian.json and restart Financial Ian.")
        return 0
    if a.check_feed:
        from .engine import IanConfig
        from .feeds.factory import build_feed
        from .mapping import front_month
        import datetime as dt
        cfg = IanConfig.load(IanConfig.default_path(data_dir))
        feed = build_feed(cfg.feed, data_dir, cfg.instruments)
        h = feed.health()
        print(f"feed vendor: {h.vendor}; state: {h.state}; {h.reason}")
        if h.state == "NOT CONFIGURED":
            print("Financial Ian stays DATA-DEGRADED (no signals, no trades) until a feed is configured.")
            print("See docs/ops/financial_ian_data_feed.md.")
            return 2
        today = dt.datetime.now(dt.timezone.utc).date()
        syms = [front_month(r, today, cfg.roll_days).raw_symbol for r in cfg.instruments]
        if feed.connect():
            feed.subscribe(syms)
            time.sleep(10)
            evs = feed.events(100_000)
            print(f"subscribed {', '.join(syms)}: {len(evs)} events in 10 s; state {feed.health().state}: {feed.health().reason}")
            feed.close()
            return 0 if evs else 2
        print(f"could not connect: {feed.health().reason}")
        return 2
    if a.status:
        p = data_dir / "ian-status.json"
        if not p.exists():
            print("No status yet: Financial Ian has not run.")
            return 1
        s = json.loads(p.read_text())
        print(f"{s.get('label')} [{s.get('mode')}] {s.get('status')}")
        print(f"feed: {s['feed']['state']} {s['feed'].get('reason', '')}")
        if s.get("top_opportunity"):
            print("top:", s["top_opportunity"].get("reasoning"))
        for o in s.get("open_positions") or []:
            print(f"open: {o['symbol']} {o['side']} {o['volume']} from {o['entry']} stop {o['stop']} ({o['r_now']:+.2f} R)")
        return 0
    if a.demo:
        from .engine import IanConfig, IanEngine
        from .feeds.synthetic import SyntheticFeed
        with tempfile.TemporaryDirectory() as td:
            eng = IanEngine(td, IanConfig(record=False), None, SyntheticFeed(a.demo))
            eng.run_virtual()
            s = eng.status()
            print(f"{a.demo}: {s['data_label']}")
            for r, o in s["opportunities"].items():
                print(f"  {r}: score {o['score']}, {o['regime']}, {o['spot_side'] or 'no side'}; {o['why_not'] or 'tradeable'}")
            print("  reasoning:", s.get("reasoning") or "-")
            eng.close()
        return 0
    ap.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
