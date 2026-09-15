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
from .clock import to_utc, utcnow
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


_LEDGER_CACHE: dict = {"at": None, "value": {}}


def reset_measurement_for_new_strategy(cfg: Config, path, now: Optional[dt.datetime] = None) -> bool:
    """Start the since-start clock afresh when the strategy has changed.

    Returns True when the clock was reset. The trades of the previous
    strategy stay in the broker's history and the journal; the status page
    simply stops counting them.

    Only the two clock fields are written into the settings file. The
    strategy rules (the ``scan`` and ``flowlock`` sections) belong to the
    code, so any copy of them found in the file is removed: a saved copy of
    an older version's rules would otherwise silently override the new
    build's, which is exactly what happened on day two.
    """
    from .config import STRATEGY_VERSION
    changed = cfg.tracking_strategy != STRATEGY_VERSION
    if changed:
        now = to_utc(now or utcnow()).replace(microsecond=0)
        cfg.tracking_start_utc = now.isoformat()
        cfg.tracking_strategy = STRATEGY_VERSION
        _LEDGER_CACHE["at"] = None
    try:
        from pathlib import Path
        p = Path(path)
        raw = json.loads(p.read_text()) if p.exists() else {}
        if not isinstance(raw, dict):
            raw = {}
        frozen = [k for k in ("scan", "flowlock") if k in raw]
        if changed or frozen:
            raw["tracking_start_utc"] = cfg.tracking_start_utc
            raw["tracking_strategy"] = cfg.tracking_strategy
            for section in frozen:
                raw.pop(section, None)
            tmp = p.with_suffix(p.suffix + ".tmp")
            tmp.write_text(json.dumps(raw, indent=2, sort_keys=True))
            tmp.replace(p)
        if frozen:
            # The in-memory rules are the file's stale copy; use the code's.
            fresh = Config()
            cfg.scan = fresh.scan
            cfg.flowlock = fresh.flowlock
            log.warning("removed a saved copy of the %s rules from the settings "
                        "file - the code's rules apply", " and ".join(frozen))
    except Exception as exc:
        log.warning("could not update the settings file: %s", exc)
    if changed:
        log.info("strategy is now %r - measuring from %s", STRATEGY_VERSION,
                 cfg.tracking_start_utc)
    return changed


def broker_ledger(trader: Trader, now: dt.datetime) -> dict:
    """Today's and since-start results from the BROKER's deal history.

    The journal explains trades; the broker's records are what they earned.
    Only the bot's own deals (its magic number) are counted, so trading by
    hand on the same account does not blur the measurement.  Refreshed at
    most every 30 seconds.
    """
    cached_at = _LEDGER_CACHE["at"]
    if cached_at and (now - cached_at).total_seconds() < 30:
        return _LEDGER_CACHE["value"]
    fn = getattr(trader.broker, "deals_since", None)
    out: dict = {}
    if fn is not None:
        cfg = trader.cfg
        start_txt = cfg.tracking_start_utc or ""
        try:
            start = dt.datetime.fromisoformat(start_txt.replace("Z", "+00:00")) \
                if start_txt else now.replace(hour=0, minute=0, second=0, microsecond=0)
            if start.tzinfo is None:
                start = start.replace(tzinfo=dt.timezone.utc)
        except ValueError:
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        # "Today" never reaches back before the measuring start: on the day
        # the start is set, today begins then, not at midnight.
        today_from = max(day_start, start)
        earliest = min(start, day_start)
        try:
            rows = fn(earliest, cfg.magic, False) or []     # entries too
        except Exception as exc:
            log.debug("deal history unavailable: %s", exc)
            rows = None
        if rows is not None:
            # A position counts only if it was OPENED at or after the start.
            # One opened before (the previous method's trades) is ignored
            # even when it closes afterwards, so the new numbers are the new
            # trading's alone.
            opened_at: dict = {}
            for r in rows:
                if r.get("is_entry") and r.get("time"):
                    pos = r.get("position") or 0
                    opened_at[pos] = min(opened_at.get(pos, r["time"]), r["time"])

            def summarise(since):
                by_pos: dict = {}
                for r in rows:
                    if r.get("is_entry") or not r.get("time"):
                        continue
                    pos = r.get("position") or id(r)
                    when = opened_at.get(pos)
                    if when is None or when < since:
                        continue
                    by_pos[pos] = by_pos.get(pos, 0.0) + float(r["profit"])
                wins = sum(1 for v in by_pos.values() if v > 0)
                return {"net": round(sum(by_pos.values()), 2),
                        "trades": len(by_pos), "wins": wins,
                        "win_rate": round(wins / len(by_pos) * 100, 1) if by_pos else None}
            out = {"today": summarise(today_from),
                   "since_start": summarise(start),
                   "tracking_start": start.isoformat(),
                   "source": "broker"}
    _LEDGER_CACHE["at"] = now
    _LEDGER_CACHE["value"] = out
    return out


def push_dashboard(state: DashboardState, trader: Trader) -> None:
    """Copy the trader's current view into the dashboard snapshot."""
    try:
        account = trader.broker.account()
    except Exception:
        account = None
    ledger = {}
    try:
        ledger = broker_ledger(trader, trader.clock())
    except Exception as exc:
        log.debug("broker ledger failed: %s", exc)
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
            # Broker's figures when it can give them; the journal otherwise.
            "today_pnl": (ledger.get("today") or {}).get("net", results.get("net", 0.0)),
            "win_rate_today": (ledger.get("today") or {}).get("win_rate", results.get("win_rate")),
            "today_trades": (ledger.get("today") or {}).get("trades", results.get("trades", 0)),
            "since_start_pnl": (ledger.get("since_start") or {}).get("net"),
            "since_start_trades": (ledger.get("since_start") or {}).get("trades"),
            "since_start_win_rate": (ledger.get("since_start") or {}).get("win_rate"),
            "tracking_start": ledger.get("tracking_start", trader.cfg.tracking_start_utc),
            "strategy": trader.cfg.tracking_strategy,
            "pnl_source": ledger.get("source", "journal"),
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
    """Direct on Windows, over the Wine bridge on macOS and Linux."""
    if cfg.broker_mode == "bridge":
        from .broker.bridge_client import BridgeBroker
        return BridgeBroker(host=cfg.bridge_host, port=cfg.bridge_port,
                            token_file=cfg.bridge_token_file,
                            magic=cfg.magic)
    if not mt5_available():
        raise SystemExit(
            "MetaTrader5 is not importable in this Python environment.\n"
            "On Windows: install it with `pip install MetaTrader5`.\n"
            "On macOS or Linux: set broker_mode to 'bridge' so MT5 runs inside\n"
            "a Wine prefix and this process talks to it over a socket.\n"
            "For development, use the simulator (see tests/ and "
            "mintel.research.backtest).")
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
    reset_measurement_for_new_strategy(cfg, args.config)
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
            if cfg.broker_mode == "bridge":
                log.error("could not reach the MetaTrader bridge on %s:%d - "
                          "is MetaTrader 5 running inside Wine?",
                          cfg.bridge_host, cfg.bridge_port)
            else:
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
        last_safe_mode = None
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
            # Refresh the page on every scan, and immediately whenever the
            # bot enters or leaves SAFE MODE - that is precisely the moment
            # somebody is looking at it, and waiting for the next scan would
            # leave them staring at a stale green banner.
            if scan_due or trader.health.safe_mode != last_safe_mode:
                last_safe_mode = trader.health.safe_mode
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
