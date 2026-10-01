"""Run the Rapid Scalper as its own process.

    python -m mintel.scalper.run --config data/config.json

Reads data/scalper.json next to the main settings. OFF exits at once.
PAPER never sends an order. LIVE is a deliberate edit of that file.
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

from ..broker.base import Side  # noqa: F401
from ..clock import to_utc, utcnow
from ..config import Config
from ..ops.health import Heartbeat
from ..ops.watchdog import pid_alive
from .config import ScalperConfig
from .engine import ScalperEngine

log = logging.getLogger("mintel.scalper.run")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Rapid Scalper")
    ap.add_argument("--config", default="data/config.json")
    ap.add_argument("--scalper", default="")
    ap.add_argument("--cycles", type=int, default=0)
    args = ap.parse_args(argv)
    cfg = Config.load(args.config)
    data = Path(cfg.ops.data_dir)
    spath = Path(args.scalper) if args.scalper else ScalperConfig.default_path(data)
    scfg = ScalperConfig.load(spath)
    if not spath.exists():
        scfg.save(spath)                 # the default file, PAPER, so it can be edited

    Path(cfg.ops.log_dir).mkdir(parents=True, exist_ok=True)
    handler = logging.handlers.RotatingFileHandler(Path(cfg.ops.log_dir) / "scalper.log",
                                                   maxBytes=5_000_000, backupCount=5)
    logging.basicConfig(level=logging.INFO, handlers=[handler, logging.StreamHandler()],
                        format="%(asctime)s %(levelname)-8s %(name)-18s %(message)s")
    log.warning("Rapid Scalper starting in %s mode (magic %d)", scfg.mode, scfg.magic)
    if scfg.mode == "OFF":
        log.warning("mode is OFF - nothing will be traded; exiting")
        return 0

    pid_path = data / "scalper.pid"
    if pid_path.exists():
        try:
            other = int(pid_path.read_text().strip())
            if other != os.getpid() and pid_alive(other):
                log.error("another Rapid Scalper is already running (pid %d)", other)
                return 3
        except ValueError:
            pass
    pid_path.write_text(str(os.getpid()))

    from ..run import build_broker
    broker = build_broker(cfg)
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    hb = Heartbeat(data / "heartbeats", "scalper")
    attempt = 0
    while not stop.is_set():
        attempt += 1
        hb.beat({"waiting": "trying to reach MetaTrader", "attempt": attempt})
        if broker.connect():
            break
        stop.wait(15.0)
    if stop.is_set():
        pid_path.unlink(missing_ok=True)
        return 0

    engine = ScalperEngine(cfg, scfg, broker)
    engine.journal.log_event("START", f"mode {scfg.mode}")
    n = 0
    try:
        while not stop.is_set():
            t0 = time.monotonic()
            try:
                for note in engine.cycle():
                    log.info("%s", note)
            except Exception as exc:
                log.exception("cycle failed: %s", exc)
                engine.breakers.record_api_error(to_utc(utcnow()))
            n += 1
            if args.cycles and n >= args.cycles:
                break
            stop.wait(max(0.0, scfg.manage_interval_seconds - (time.monotonic() - t0)))
    finally:
        try:
            engine.shutdown()
        except Exception:
            pass
        try:
            engine.journal.close()
        except Exception:
            pass
        pid_path.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
