"""Entry point for the trading process.

    python -m mintel.run --config data/config.json

Responsibilities beyond starting the Trader:

* write a PID file so the watchdog can see this process;
* start the dashboard in a daemon thread and keep pushing state into it;
* refuse to start twice against the same account (a second trader would
  duplicate every trade);
* shut down cleanly on SIGTERM so the VPS can restart it without leaving a
  stale PID file behind.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import logging.handlers
import os
import signal
import sys
import time
from pathlib import Path
from typing import Optional, Sequence

from .broker.base import Side
from .broker.mt5_adapter import Mt5Broker, mt5_available
from .clock import utcnow
from .config import Config, LIVE_MARKER
from .engine.trader import Trader
from .ops.dashboard import DashboardState, build_results, start_dashboard
from .ops.watchdog import pid_alive

log = logging.getLogger("mintel.run")


def setup_logging(cfg: Config, verbose: bool = False) -> None:
    log_dir = Path(cfg.ops.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-8s %(name)-22s %(message)s")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    root.addHandler(console)
    # Rotating files: a long-running VPS process must not fill the disk, and
    # the disk-space health check would otherwise eventually trip.
    fh = logging.handlers.RotatingFileHandler(
        log_dir / "trader.log", maxBytes=20 * 1024 * 1024, backupCount=5)
    fh.setFormatter(fmt)
    root.addHandler(fh)
    errors = logging.handlers.RotatingFileHandler(
        log_dir / "errors.log", maxBytes=5 * 1024 * 1024, backupCount=3)
    errors.setLevel(logging.WARNING)
    errors.setFormatter(fmt)
    root.addHandler(errors)


class PidFile:
    """Single-instance guard.

    Two traders on one account would double every position, so a live PID for
    this data directory is a hard stop rather than a warning.
    """

    def __init__(self, path: Path):
        self.path = path
        self.acquired = False

    def acquire(self) -> tuple[bool, str]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            try:
                pid = int(self.path.read_text().strip())
            except Exception:
                pid = 0
            if pid and pid != os.getpid() and pid_alive(pid):
                return False, (f"another trader is already running "
                               f"(process {pid}) - refusing to start a second "
                               f"one on the same account")
        self.path.write_text(str(os.getpid()))
        self.acquired = True
        return True, "ok"

    def release(self) -> None:
        if self.acquired:
            try:
                self.path.unlink(missing_ok=True)
            except Exception:
                pass


def push_dashboard(state: DashboardState, trader: Trader) -> None:
    """Copy the trader's current view into the dashboard snapshot."""
    try:
        account = trader.broker.account()
    except Exception:
        account = None
    try:
        positions = trader.broker.positions(trader.cfg.magic)
    except Exception:
        positions = []
    report = trader.health.last_report
    results = {}
    try:
        results = build_results(trader.journal,
                                account.currency if account else "",
                                trader.clock())
    except Exception as exc:
        log.debug("results build failed: %s", exc)

    hb = {}
    try:
        from .ops.health import read_all_heartbeats
        hb = read_all_heartbeats(Path(trader.cfg.ops.data_dir) / "heartbeats",
                                 trader.clock())
    except Exception:
        pass
    watchdog_beat = hb.get("watchdog")
    watchdog_ok = bool(
        watchdog_beat and watchdog_beat.get("age_seconds", 1e9)
        < max(60.0, trader.cfg.ops.watchdog_interval_seconds * 4))

    data_check = next((c for c in (report.checks if report else ())
                       if c.name == "MARKET_DATA"), None)
    news_check = next((c for c in (report.checks if report else ())
                       if c.name == "NEWS"), None)

    pos_rows = []
    for p in positions:
        tracker = trader.flowlock.trackers.get(p.ticket)
        pos_rows.append({
            "symbol": p.symbol, "side": p.side.value, "volume": p.volume,
            "entry": p.entry_price, "stop": p.sl, "profit": p.profit,
            "flow_state": tracker.state.value if tracker else "",
            "note": (f"best {tracker.mfe_r:.2f}R, thesis "
                     f"{tracker.thesis_strength:.0f}/100" if tracker else ""),
        })

    last_trade = "none yet"
    try:
        recent = trader.journal.closed_trades(limit=1)
        if recent:
            last_trade = (f"{recent[0]['symbol']} "
                          f"{(recent[0].get('closed_utc') or '')[:16]}")
    except Exception:
        pass

    state.update(
        status={
            "bot": "RUNNING" if not trader.health.safe_mode else "SAFE MODE",
            "mt5_connected": bool(trader.broker.is_connected()),
            "broker_connected": account is not None,
            "data_live": bool(data_check and data_check.ok),
            "news_live": bool(news_check and news_check.ok),
            "watchdog_ok": watchdog_ok,
            "mode": trader.cfg.effective_mode,
            "aggression": trader.cfg.aggression,
            "open_positions": len(positions),
            "equity": account.equity if account else 0.0,
            "balance": account.balance if account else 0.0,
            "currency": account.currency if account else "",
            "today_pnl": results.get("net", 0.0),
            "win_rate_today": results.get("win_rate"),
            "last_scan": (trader.scanner.last_scan_utc.strftime("%H:%M:%S UTC")
                          if trader.scanner.last_scan_utc else "never"),
            "last_trade": last_trade,
            "cycles": trader.cycles,
            "uptime_minutes": round(
                (utcnow() - trader.started_utc).total_seconds() / 60, 1),
        },
        health=report.to_dict() if report else {},
        thinking=trader.thinking(),
        results=results,
        positions=pos_rows,
        events=trader.journal.recent_events(limit=25))


def build_broker(cfg: Config):
    if not mt5_available():
        raise SystemExit(
            "MetaTrader5 is not available in this Python environment.\n"
            "The trader must run on the Windows VPS where MT5 is installed.\n"
            "For development and testing off-Windows, use the simulator "
            "(see tests/ and mintel.research.backtest).")
    return Mt5Broker(login=cfg.account_login, password=cfg.account_password,
                     server=cfg.account_server,
                     terminal_path=cfg.mt5_terminal_path, magic=cfg.magic)


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="mintel autonomous trader")
    ap.add_argument("--config", default="data/config.json")
    ap.add_argument("--cycles", type=int, default=0,
                    help="stop after N cycles (0 = run forever)")
    ap.add_argument("--no-dashboard", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--check-only", action="store_true",
                    help="start up, run one cycle, print health, exit")
    args = ap.parse_args(argv)

    cfg = Config.load(args.config)
    setup_logging(cfg, args.verbose)
    errors = cfg.validate()
    if errors:
        for e in errors:
            log.error("configuration problem: %s", e)
        return 2

    log.warning("starting in %s mode with %s aggression",
                cfg.effective_mode, cfg.aggression)
    if cfg.is_live:
        log.warning("LIVE TRADING IS ENABLED - real money is at risk")

    pid = PidFile(Path(cfg.ops.data_dir) / "trader.pid")
    ok, msg = pid.acquire()
    if not ok:
        log.error("%s", msg)
        return 3

    httpd = None
    trader = None
    try:
        broker = build_broker(cfg)
        if not broker.connect():
            log.error("could not connect to MetaTrader 5 - is the terminal "
                      "running and logged in?")
            return 4
        trader = Trader(broker, cfg)
        state = DashboardState()
        if not args.no_dashboard:
            httpd = start_dashboard(state, cfg.ops.dashboard_host,
                                    cfg.ops.dashboard_port)
        info = trader.bootstrap()
        log.info("bootstrap: %s", json.dumps(info, default=str)[:2000])

        def shutdown(*_a):
            log.warning("shutdown requested")
            trader.stop()

        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

        if args.check_only:
            result = trader.cycle()
            push_dashboard(state, trader)
            print(json.dumps({"summary": result.summary(),
                              "health": trader.health.last_report.to_dict()
                              if trader.health.last_report else {},
                              "thinking": trader.thinking()[:5]},
                             indent=2, default=str))
            return 0 if not trader.health.safe_mode else 5

        cycles = 0
        last_scan = 0.0
        while not trader._stop.is_set():
            started = time.time()
            scan_due = started - last_scan >= cfg.scan.scan_interval_seconds
            try:
                if scan_due:
                    result = trader.cycle()
                    last_scan = started
                    cycles += 1
                    log.info("%s", result.summary())
                else:
                    # Open positions are managed on the fast clock so FlowLock
                    # is never blind between scans.
                    for note in trader.manage_cycle():
                        log.info("%s", note)
            except Exception as exc:
                log.exception("cycle failed: %s", exc)
            if scan_due:
                try:
                    push_dashboard(state, trader)
                except Exception as exc:
                    log.warning("dashboard update failed: %s", exc)
            if args.cycles and cycles >= args.cycles:
                break
            time.sleep(max(0.0, cfg.scan.manage_interval_seconds
                           - (time.time() - started)))
        return 0
    finally:
        if httpd is not None:
            try:
                httpd.shutdown()
            except Exception:
                pass
        if trader is not None:
            try:
                trader.journal.log_event("SHUTDOWN", "trader stopped", "WARNING")
                trader.journal.close()
            except Exception:
                pass
        pid.release()


if __name__ == "__main__":
    raise SystemExit(main())
