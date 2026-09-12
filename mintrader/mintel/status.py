"""Command-line status, for when a browser is inconvenient.

    python -m mintel.status --config data/config.json
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import urllib.request
from pathlib import Path
from typing import Optional, Sequence

from .config import Config


def fetch(cfg: Config) -> Optional[dict]:
    url = f"http://{cfg.ops.dashboard_host}:{cfg.ops.dashboard_port}/api"
    try:
        with urllib.request.urlopen(url, timeout=8) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


def render(snap: dict) -> str:
    st = snap.get("status") or {}
    health = snap.get("health") or {}
    lines = []
    running = st.get("bot") == "RUNNING" and not health.get("safe_mode")
    lines.append("=" * 58)
    lines.append("  BOT: " + ("RUNNING" if running else "PROBLEM"))
    lines.append("=" * 58)
    def flag(v):
        return "yes" if v else "NO"
    lines += [
        f"  MT5 connected   : {flag(st.get('mt5_connected'))}",
        f"  Broker connected: {flag(st.get('broker_connected'))}",
        f"  Market data live: {flag(st.get('data_live'))}",
        f"  News live       : {flag(st.get('news_live'))}",
        f"  Watchdog healthy: {flag(st.get('watchdog_ok'))}",
        f"  Mode            : {st.get('mode', '?')}",
        f"  Aggression      : {st.get('aggression', '?')}",
        f"  Open trades     : {st.get('open_positions', 0)}",
        f"  Equity          : {st.get('equity', 0):.2f} {st.get('currency', '')}",
        f"  Today           : {st.get('today_pnl', 0):+.2f} "
        f"{st.get('currency', '')}",
        f"  Win rate today  : "
        + ("-" if st.get("win_rate_today") is None
           else f"{st['win_rate_today']:.0f}%"),
        f"  Last scan       : {st.get('last_scan', 'never')}",
        f"  Last trade      : {st.get('last_trade', 'none yet')}",
        f"  Uptime          : {st.get('uptime_minutes', 0):.0f} minutes",
    ]
    problems = [c for c in (health.get("checks") or [])
                if c.get("severity") != "OK"]
    if problems:
        lines.append("")
        lines.append("  WHAT IS WRONG")
        for c in problems:
            lines.append(f"    {c['severity']}: {c['message']}")
    thinking = snap.get("thinking") or []
    if thinking:
        lines.append("")
        lines.append("  WHAT IT IS LOOKING AT")
        for t in thinking[:8]:
            extra = ("  <- " + "; ".join(t["blockers"])) if t.get("blockers") else ""
            lines.append(f"    {t['rank']}. {t['symbol']:<9}"
                         f"{t['direction']:<6}{t['score']:>5.0f}  "
                         f"{t['tier']:<12}{t['tactic'].replace('_',' ').title()}"
                         f"{extra}")
    positions = snap.get("positions") or []
    if positions:
        lines.append("")
        lines.append("  OPEN TRADES")
        for p in positions:
            lines.append(f"    {p['symbol']:<9}{p['side']:<5}"
                         f"{p['volume']:>6} lots  entry {p['entry']}  "
                         f"stop {p['stop']}  {p['profit']:+.2f}  "
                         f"{p.get('flow_state', '')} {p.get('note', '')}")
    results = snap.get("results") or {}
    if results.get("trades"):
        lines.append("")
        lines.append(f"  TODAY: {results['trades']} trades, "
                     f"{results['wins']} won, {results['losses']} lost, "
                     f"net {results['net']:+.2f} {results.get('currency', '')}")
    return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="show the bot's status")
    ap.add_argument("--config", default="data/config.json")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    cfg = Config.load(args.config)
    snap = fetch(cfg)
    if snap is None:
        print("The bot is not answering on "
              f"http://{cfg.ops.dashboard_host}:{cfg.ops.dashboard_port}.")
        print("It may not be running. Start it with START-BOT.ps1")
        return 1
    if args.json:
        print(json.dumps(snap, indent=2, default=str))
    else:
        print(render(snap))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
