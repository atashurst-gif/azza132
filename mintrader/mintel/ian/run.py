"""Run Financial Ian as its own process.

    python -m mintel.ian.run --config data/config.json

Reads data/ian.json next to the main settings (created with mode PAPER and no
feed on the first start). OFF exits at once. PAPER never sends an order. LIVE
is a deliberate edit of that file.

MetaTrader: when it is installed but cannot be reached (the terminal is not
running, or not logged in) the process waits for it, trying again every 15
seconds; nothing is read or placed meanwhile and the page says "WAITING FOR
METATRADER". Started with --no-broker, or where the MetaTrader5 package is
not installed, it runs without MetaTrader: LIVE then places nothing and the
page says "LIVE needs MetaTrader: nothing can be placed".

With no institutional feed configured the process still runs - it keeps the
heartbeat and the status page up, in the safe DATA-DEGRADED state (no
signals, no trades) - so the page always says plainly what is missing.
With ``"feed": {"vendor": "binance"}`` it reads Binance's public crypto order
book (no key) and trades the broker's BTC and ETH CFDs.

Heartbeat "ian", pid file data/ian.pid (a second copy refuses to start), log
logs/ian.log.
"""
from __future__ import annotations

import argparse
import logging
import logging.handlers
import os
import signal
import sys
import threading
import time
from pathlib import Path

from ..clock import utcnow
from ..config import Config
from ..ops.health import Heartbeat
from ..ops.watchdog import pid_alive
from . import MAGIC, STRATEGY_LABEL
from .engine import IanConfig, IanEngine, write_waiting_status
from .feeds.factory import build_feed

log = logging.getLogger("mintel.ian.run")

CONNECT_RETRY_S = 15.0           # between attempts to reach MetaTrader


def claim_pid(pid_path: Path) -> bool:
    """False when another live Financial Ian holds the pid file."""
    if pid_path.exists():
        try:
            other = int(pid_path.read_text().strip())
            if other != os.getpid() and pid_alive(other):
                return False
        except ValueError:
            pass
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text(str(os.getpid()))
    return True


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Financial Ian")
    ap.add_argument("--config", default="data/config.json")
    ap.add_argument("--ian", default="", help="path of ian.json (default: next to config.json)")
    ap.add_argument("--cycles", type=int, default=0, help="stop after N passes (0 = run until stopped)")
    ap.add_argument("--no-broker", action="store_true", help="run without MetaTrader (status and feed only)")
    args = ap.parse_args(argv)
    cfg = Config.load(args.config)
    data = Path(cfg.ops.data_dir)
    ipath = Path(args.ian) if args.ian else IanConfig.default_path(data)
    if not ipath.exists():
        IanConfig.save_minimal(ipath, "PAPER")
    icfg = IanConfig.load(ipath)

    Path(cfg.ops.log_dir).mkdir(parents=True, exist_ok=True)
    handler = logging.handlers.RotatingFileHandler(Path(cfg.ops.log_dir) / "ian.log", maxBytes=5_000_000, backupCount=5)
    logging.basicConfig(level=logging.INFO, handlers=[handler, logging.StreamHandler()],
                        format="%(asctime)s %(levelname)-8s %(name)-18s %(message)s")
    log.warning("%s starting in %s mode (magic %d)", STRATEGY_LABEL, icfg.mode, MAGIC)
    if icfg.mode == "OFF":
        log.warning("mode is OFF - nothing will be traded; exiting")
        return 0

    pid_path = data / "ian.pid"
    if not claim_pid(pid_path):
        log.error("another %s is already running (%s)", STRATEGY_LABEL, pid_path)
        return 3

    # from here the pid file is ours: whatever happens - including a failure while starting up - it is removed
    feed = None
    engine = None
    try:
        stop = threading.Event()
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        hb = Heartbeat(data / "heartbeats", "ian")
        broker = None
        if not args.no_broker:
            from ..run import build_broker
            try:
                broker = build_broker(cfg)
            except SystemExit as exc:
                log.error("MetaTrader is not available here (%s); running without it: no trades", exc)
                broker = None
            attempt = 0
            while broker is not None and not stop.is_set():
                attempt += 1
                hb.beat({"waiting": "trying to reach MetaTrader", "attempt": attempt})
                write_waiting_status(data, icfg, attempt, utcnow(), CONNECT_RETRY_S)
                try:
                    if broker.connect():
                        break
                except Exception as exc:
                    log.warning("MetaTrader connect failed: %s", exc)
                stop.wait(CONNECT_RETRY_S)
            if stop.is_set():
                return 0
        from .instruments import parse_crypto
        feed = build_feed(icfg.feed, data, icfg.instruments, parse_crypto(icfg.crypto_instruments))
        engine = IanEngine(data, icfg, broker, feed)
        engine.journal.event("START", f"mode {engine.mode}, feed {icfg.feed.get('vendor', 'none')}")
        n = 0
        last_beat = 0.0
        while not stop.is_set():
            t0 = time.monotonic()
            try:
                engine.pump()
                for note in engine.step(utcnow()):
                    log.info("%s", note)
            except Exception as exc:
                log.exception("pass failed: %s", exc)
            if time.monotonic() - last_beat >= 5.0:
                last_beat = time.monotonic()
                hb.beat({"mode": engine.mode, "feed": engine.feed_state, "reason": engine.feed_reason[:200],
                         "open": len(engine.open), "events": engine.events_in})
            n += 1
            if args.cycles and n >= args.cycles:
                break
            stop.wait(max(0.0, icfg.decision_interval_s - (time.monotonic() - t0)))
    finally:
        if engine is not None:
            try:
                engine.write_status(utcnow(), force=True)
            except Exception:
                pass
            try:
                engine.close()
            except Exception:
                pass
        elif feed is not None:                       # the engine never started: do not leave the feed connected
            try:
                feed.close()
            except Exception:
                pass
        pid_path.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
