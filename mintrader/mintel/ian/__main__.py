"""Financial Ian command line.

    python -m mintel.ian --config <data>/config.json --set-key
        asks for the Databento API key and saves it in secrets.json ("databento_api_key")
    python -m mintel.ian --config <data>/config.json --check-feed
        says whether the feed is configured and, if so, whether it connects (CME via Databento, or
        Binance's public crypto order book)
    python -m mintel.ian --config <data>/config.json --free-feed
        without a saved Databento key, points ian.json at Binance's PUBLIC crypto order book (no key needed;
        "feed": {"vendor": "binance"}); with one, the CME feed through Databento (unless ian.json already reads
        Binance). Prints what ian.json now reads: "binance" or "cme" (for the installers)
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
    ap.add_argument("--free-feed", action="store_true")
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
    if a.free_feed:
        return free_feed(data_dir)
    if a.check_feed:
        from .engine import IanConfig
        from .feeds.factory import build_feed
        from .instruments import parse_crypto
        from .mapping import subscription_symbol
        import datetime as dt
        cfg = IanConfig.load(IanConfig.default_path(data_dir))
        feed = build_feed(cfg.feed, data_dir, cfg.instruments, parse_crypto(cfg.crypto_instruments))
        h = feed.health()
        print(f"feed vendor: {h.vendor}; state: {h.state}; {h.reason}")
        if h.state == "NOT CONFIGURED":
            print("Financial Ian stays DATA-DEGRADED (no signals, no trades) until a feed is configured.")
            print("See docs/ops/financial_ian_data_feed.md.")
            return 2
        today = dt.datetime.now(dt.timezone.utc).date()
        traded, _ = cfg.universe(h.vendor)
        syms = [subscription_symbol(r, today, cfg.roll_days) for r in traded]
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


def free_feed(data_dir: Path) -> int:
    """No Databento key: Binance's public crypto book becomes the feed. A saved key: the CME feed through
    Databento - already set, or set now (the key is there; --set-key only says to set the vendor by hand) -
    unless ian.json already reads Binance, which is kept. Only ian.json's "feed" -> "vendor" changes;
    everything else in the file, the mode included, is kept. The word printed is what ian.json now READS
    ("cme" only when its feed is Databento), so the installers never name a feed Ian is not reading."""
    from .feeds.databento import read_api_key
    p = Path(data_dir) / "ian.json"
    try:
        raw = json.loads(p.read_text()) if p.exists() else {}
    except Exception as exc:
        print(f"ian.json cannot be read ({exc}); nothing changed", file=sys.stderr)
        return 1
    if not isinstance(raw, dict):
        print("ian.json is not a settings object; nothing changed", file=sys.stderr)
        return 1
    feed = raw.get("feed") if isinstance(raw.get("feed"), dict) else {}
    vendor = str(feed.get("vendor") or "none").strip().lower()
    if read_api_key(data_dir):
        want = vendor if vendor in ("databento", "binance") else "databento"
    else:
        want = "binance"
    if want != vendor or feed.get("vendor") != want:
        raw["feed"] = {**feed, "vendor": want}
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(raw, indent=2))
        tmp.replace(p)
    print("cme" if want == "databento" else "binance")
    return 0


if __name__ == "__main__":
    sys.exit(main())
