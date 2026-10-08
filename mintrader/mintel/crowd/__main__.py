"""Crowd Fader command line.

    python -m mintel.crowd --config ~/MarketBot/data/config.json --set-key
        asks for the Coinversa API key (cvsa_...) and saves it in secrets.json
    python -m mintel.crowd --config ~/MarketBot/data/config.json --probe
        reads gold once with the saved key and prints the read in plain English
    python -m mintel.crowd --config ~/MarketBot/data/config.json --show
        the latest read per market from the bot's own records
"""
from __future__ import annotations

import argparse
import datetime as dt
import getpass
import sys
from pathlib import Path

from ..clock import utcnow


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m mintel.crowd")
    ap.add_argument("--config", required=True)
    ap.add_argument("--set-key", action="store_true", help="save the Coinversa API key into secrets.json")
    ap.add_argument("--probe", action="store_true", help="read one market now with the saved key")
    ap.add_argument("--coin", default="xyz:GOLD")
    ap.add_argument("--show", action="store_true", help="the latest read per market from crowd.sqlite")
    a = ap.parse_args(argv)
    data_dir = Path(a.config).parent
    from .client import CoinversaClient, CoinversaError, read_api_key, save_api_key
    if a.set_key:
        key = getpass.getpass("Coinversa API key (starts with cvsa_, not shown): ").strip()
        if not key:
            print("Nothing saved.")
            return 1
        if not key.startswith("cvsa_"):
            print("That does not look like a Coinversa key (they start with cvsa_). Saved anyway.")
        p = save_api_key(data_dir, key)
        print(f"Key saved to {p}. The bot reads it on its next start; the installer keeps it across reinstalls.")
        return 0
    if a.probe:
        key = read_api_key(data_dir)
        if not key:
            print("No Coinversa key in secrets.json. Run with --set-key first.")
            return 1
        from .signal import summarise
        try:
            c = CoinversaClient(key)
            cohorts = c.cohort_bias(a.coin)
            heat = c.heatmap(a.coin, 3.0, 20)
        except CoinversaError as exc:
            print(f"Coinversa said no: {exc}")
            return 2
        r = summarise(a.coin.split(":")[-1], a.coin, cohorts, heat, utcnow())
        print(f"{a.coin} at {r.price}: {r.reason}")
        for t, v in r.tiers.items():
            print(f"  {v['name']:<10} long {v['long']:>5} / short {v['short']:>5}   money long {v['long_money']:>14,.0f} / short {v['short_money']:>14,.0f}")
        print(f"calls {c.calls}, rate-limit remaining {c.remaining}")
        return 0
    if a.show:
        import sqlite3
        p = data_dir / "crowd.sqlite"
        if not p.exists():
            print("No records yet.")
            return 1
        db = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        db.row_factory = sqlite3.Row
        for r in db.execute("SELECT symbol, MAX(ts_utc) AS ts, reason, score, direction FROM checks GROUP BY symbol ORDER BY symbol"):
            print(f"{r['symbol']:<8} {r['ts']}  {r['reason']}")
        n = db.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        print(f"{n} paper trades on record")
        return 0
    ap.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
