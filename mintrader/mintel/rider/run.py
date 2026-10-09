"""Run the Rapid Momentum Rider as its own process.

    python -m mintel.rider.run --config data/config.json

Reads data/rider.json next to the main settings (written with the mode only
when missing). PAPER never sends an order. LIVE places real orders under
magic 990811, each with its stop at the broker. OFF trades nothing: it first
closes any real position LIVE left behind (an open LIVE row in
data/rider.sqlite, or a position at the broker under magic 990811), books it
with the broker's figure and exits; with nothing left from LIVE it exits at
once (see ``_off``). Logs go to logs/rider.log, the pid to data/rider.pid,
the heartbeat is "rider".
"""
from __future__ import annotations

import argparse
import logging
import logging.handlers
import os
import signal
import sqlite3
import sys
import threading
import time
from pathlib import Path
from typing import Optional

from ..clock import utcnow
from ..config import Config
from ..ops.health import Heartbeat
from ..ops.watchdog import pid_alive
from . import MAGIC, STRATEGY_LABEL
from .config import RiderConfig

log = logging.getLogger("mintel.rider.run")

OFF_WIND_DOWN_SECONDS = 600.0      # an OFF start keeps closing what LIVE left for at most this long
OFF_FIGURE_WAIT_SECONDS = 130.0    # ... and waits this long for the broker's figure of a close it booked as an estimate
OFF_PASS_SECONDS = 2.0             # between its passes
OFF_CONNECT_PAUSE_SECONDS = 15.0   # between its tries to reach MetaTrader
OFF_RECHECK_SECONDS = 3.0          # an empty broker list is read again this much later before OFF exits


def _stop_event() -> threading.Event:
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    try:
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
    except (AttributeError, ValueError):
        pass
    return stop


def _claim_pid(pid_path: Path) -> bool:
    """Write our pid, unless another Rider is running."""
    if pid_path.exists():
        try:
            other = int(pid_path.read_text().strip())
            if other != os.getpid() and pid_alive(other):
                log.error("another %s is already running (pid %d)", STRATEGY_LABEL, other)
                return False
        except ValueError:
            pass
    pid_path.write_text(str(os.getpid()))
    return True


def _open_live_rows(path: Path) -> Optional[int]:
    """How many LIVE trades rider.sqlite holds open, read without changing
    anything: 0 with no file, None when it cannot be read."""
    if not path.exists():
        return 0
    try:
        con = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
        try:
            r = con.execute("SELECT COUNT(*) FROM trades WHERE closed_utc IS NULL AND mode='LIVE'").fetchone()
        finally:
            con.close()
        return int(r[0] or 0)
    except Exception as exc:
        log.warning("could not read %s (%s)", path, exc)
        return None


def _held_at_broker(broker) -> Optional[int]:
    """How many positions under the Rider's magic the broker holds (None: unknown)."""
    try:
        return sum(1 for p in (broker.positions(MAGIC) or []) if int(getattr(p, "magic", 0) or 0) == MAGIC)
    except Exception as exc:
        log.warning("could not read the broker's positions (%s)", exc)
        return None


def _let_go(broker) -> None:
    """Close this process's own connection. Never ``shutdown``: over the
    bridge that would log MetaTrader out under the other bots."""
    fn = getattr(broker, "disconnect", None)
    if fn is not None:
        try:
            fn()
        except Exception:
            pass


def _off(cfg: Config, rcfg: RiderConfig, data: Path, cycles: int = 0) -> int:
    """OFF trades nothing. But a real position LIVE left behind would sit at
    the broker with only its last stop and nothing managing it, and its row
    would never close. So, when rider.sqlite holds an open LIVE trade or the
    broker holds a position under magic 990811, the engine is built in OFF
    and runs its leftover pass (close at the broker, book with the broker's
    figure) until nothing is left, trying again on each pass for at most
    OFF_WIND_DOWN_SECONDS; then it exits. Another bot's position is never
    touched. Returns 0 when nothing is left, 1 when something still is (it
    keeps its stop at the broker; the next start tries again), 3 when another
    Rider is running. The watchdog starts it in OFF only while rider.sqlite
    holds an open LIVE trade (a 1 is started again, within the watchdog's
    hourly limit; after a 0 it is left alone)."""
    recorded = _open_live_rows(data / rcfg.journal_file)
    stop = _stop_event()
    from ..run import build_broker
    try:
        broker = build_broker(cfg)
    except (Exception, SystemExit) as exc:       # e.g. MetaTrader not installed in this Python
        broker = None
        log.warning("MetaTrader cannot be used from here (%s)", exc)
    t0 = time.monotonic()
    connected = False
    # with a LIVE trade recorded, the watchdog starts this and judges it by its heartbeat: it reports while
    # it waits for MetaTrader (the engine reports on every pass after that)
    hb = Heartbeat(data / "heartbeats", "rider") if recorded != 0 else None
    attempt = 0
    while broker is not None and not stop.is_set():
        attempt += 1
        if hb is not None:
            hb.beat({"mode": "OFF", "waiting": "trying to reach MetaTrader to close what LIVE left",
                     "attempt": attempt})
        try:
            connected = bool(broker.connect())
        except Exception as exc:
            log.warning("connect failed: %s", exc)
        # with nothing recorded as open, one try is enough: OFF exits at once as before
        if connected or recorded == 0 or time.monotonic() - t0 >= OFF_WIND_DOWN_SECONDS:
            break
        stop.wait(OFF_CONNECT_PAUSE_SECONDS)
    if not connected:
        if recorded == 0:
            log.warning("mode is OFF - nothing will be traded; exiting (MetaTrader was not reached, and no LIVE "
                        "trade is open in the records)")
            return 0
        log.error("mode is OFF, but %s LIVE trade(s) may still be open and MetaTrader could not be reached: "
                  "each keeps its stop at the broker; start the Rider again to close them",
                  "some" if recorded is None else recorded)
        return 1
    held = _held_at_broker(broker)
    if recorded == 0 and held == 0:
        # one empty answer is not proof (MetaTrader can answer "no positions" when it has no answer): read again
        stop.wait(OFF_RECHECK_SECONDS)
        held = _held_at_broker(broker)
    if recorded == 0 and held == 0:
        log.warning("mode is OFF - nothing will be traded; exiting")
        _let_go(broker)
        return 0
    log.warning("mode is OFF - closing what LIVE left first (%s open in the records, %s at the broker)",
                "?" if recorded is None else recorded, "?" if held is None else held)
    pid_path = data / "rider.pid"
    if not _claim_pid(pid_path):
        _let_go(broker)
        return 3
    from .engine import RiderEngine
    code = 1
    engine = None
    try:
        engine = RiderEngine(rcfg, broker, data, clock=utcnow)
        engine.journal.log_event("START", "mode OFF: closing what LIVE left")
        started = utcnow()
        n = 0
        while True:
            try:
                for note in engine.step():
                    log.info("%s", note)
            except Exception as exc:                  # a bad pass is tried again
                log.exception("pass failed: %s", exc)
            n += 1
            spent = time.monotonic() - t0
            waiting = engine.leftovers_outstanding()
            figure = False
            if not waiting and spent < OFF_FIGURE_WAIT_SECONDS:
                try:
                    figure = bool(engine.journal.estimates_since(started))
                except Exception:
                    figure = False
            if not waiting and not figure:
                log.warning("mode is OFF - nothing left from LIVE is open; exiting")
                code = 0
                break
            if stop.is_set() or (cycles and n >= cycles) or spent >= OFF_WIND_DOWN_SECONDS:
                if waiting:
                    log.error("mode is OFF - something left from LIVE is still open; it keeps its stop at the "
                              "broker and the next start tries again")
                else:
                    log.warning("mode is OFF - closed what LIVE left; the broker's figure for a close has not "
                                "come in yet, so it stays marked as an estimate until the Rider runs again")
                    code = 0
                break
            stop.wait(OFF_PASS_SECONDS)
    finally:
        if engine is not None:
            try:
                engine.shutdown()
            except Exception:
                pass
            try:
                engine.journal.close()
            except Exception:
                pass
        pid_path.unlink(missing_ok=True)
        _let_go(broker)
    return code


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=STRATEGY_LABEL)
    ap.add_argument("--config", default="data/config.json")
    ap.add_argument("--rider", default="", help="settings file (default: data/rider.json)")
    ap.add_argument("--cycles", type=int, default=0, help="stop after N passes (0 = run until stopped)")
    args = ap.parse_args(argv)
    cfg = Config.load(args.config)
    data = Path(cfg.ops.data_dir)
    data.mkdir(parents=True, exist_ok=True)
    rpath = Path(args.rider) if args.rider else RiderConfig.default_path(data)
    if not rpath.exists():
        RiderConfig.save_minimal(rpath, "PAPER")        # the mode only, so a person can edit it
    rcfg = RiderConfig.load(rpath)

    Path(cfg.ops.log_dir).mkdir(parents=True, exist_ok=True)
    handler = logging.handlers.RotatingFileHandler(Path(cfg.ops.log_dir) / "rider.log",
                                                   maxBytes=5_000_000, backupCount=5)
    logging.basicConfig(level=logging.INFO, handlers=[handler, logging.StreamHandler()],
                        format="%(asctime)s %(levelname)-8s %(name)-18s %(message)s")
    log.warning("%s starting in %s mode (magic %d, GBP %.2f per pip)", STRATEGY_LABEL, rcfg.mode, MAGIC,
                rcfg.user_pip_value_gbp)
    if rcfg.mode == "OFF":
        return _off(cfg, rcfg, data, args.cycles)

    pid_path = data / "rider.pid"
    if not _claim_pid(pid_path):
        return 3

    from ..run import build_broker
    from .engine import RiderEngine
    broker = build_broker(cfg)
    stop = _stop_event()
    hb = Heartbeat(data / "heartbeats", "rider")
    attempt = 0
    while not stop.is_set():
        attempt += 1
        hb.beat({"waiting": "trying to reach MetaTrader", "attempt": attempt})
        try:
            if broker.connect():
                break
        except Exception as exc:
            log.warning("connect failed: %s", exc)
        stop.wait(15.0)
    if stop.is_set():
        pid_path.unlink(missing_ok=True)
        return 0

    engine = RiderEngine(rcfg, broker, data, clock=utcnow)
    engine.journal.log_event("START", f"mode {rcfg.mode}, GBP {rcfg.user_pip_value_gbp:.2f}/pip")
    n = 0
    try:
        while not stop.is_set():
            t0 = time.monotonic()
            try:
                for note in engine.step():
                    log.info("%s", note)
            except Exception as exc:                      # the loop never dies on one bad pass
                log.exception("pass failed: %s", exc)
            n += 1
            if args.cycles and n >= args.cycles:
                break
            stop.wait(max(0.05, rcfg.scan_interval_seconds - (time.monotonic() - t0)))
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
