"""Independent watchdog process.

Runs as a **separate process** from the trader, because the thing being watched
cannot be trusted to report its own hang.  A deadlocked Python process still
holds its PID, still has an open TCP port and still looks alive to anything
inside it; only its heartbeat stops advancing, and only an outside observer can
notice.

What it does, every ``watchdog_interval_seconds``:

1. Reads every component heartbeat and the trader's PID file.
2. Confirms the MT5 terminal process is running (Windows).
3. Restarts whatever has died, with exponential backoff and an hourly cap.
4. Writes its own heartbeat, so the dashboard can show the watchdog is alive
   and the trader can report if the watchdog itself has stopped.

Run it with::

    python -m mintel.ops.watchdog --config data/config.json
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional, Sequence

from ..clock import to_utc, utcnow
from ..config import Config
from .health import Heartbeat, read_all_heartbeats

log = logging.getLogger("mintel.watchdog")

IS_WINDOWS = os.name == "nt"
IS_MACOS = sys.platform == "darwin"


@dataclass
class ManagedProcess:
    """A process the watchdog is responsible for keeping alive."""
    name: str
    command: Sequence[str]
    heartbeat: str = ""            # heartbeat file name to judge liveness by
    pid_file: str = ""
    cwd: str = ""
    max_restarts_per_hour: int = 12
    grace_seconds: float = 45.0
    # Optional liveness probe, used instead of a heartbeat file for services
    # that answer on a socket.  Returning None means "cannot tell".
    probe: Optional[Callable[[], Optional[bool]]] = None
    restarts: list[dt.datetime] = field(default_factory=list)
    last_start: Optional[dt.datetime] = None
    backoff: float = 0.0
    proc: Optional[subprocess.Popen] = None

    def alive_by_pid(self) -> Optional[bool]:
        """Is the process alive according to its PID?

        Returns ``None`` when we cannot tell, which is treated as "no opinion"
        rather than as a failure - killing a healthy trader because a PID file
        was missing would be worse than the fault it is guarding against.
        """
        if self.proc is not None and self.proc.poll() is None:
            return True
        if self.proc is not None and self.proc.poll() is not None:
            return False
        if not self.pid_file:
            return None
        try:
            pid = int(Path(self.pid_file).read_text().strip())
        except Exception:
            return None
        return pid_alive(pid)

    def can_restart(self, now: dt.datetime) -> bool:
        cutoff = now - dt.timedelta(hours=1)
        self.restarts = [t for t in self.restarts if t >= cutoff]
        if len(self.restarts) >= self.max_restarts_per_hour:
            return False
        if self.last_start and self.backoff:
            if (now - self.last_start).total_seconds() < self.backoff:
                return False
        return True

    def start(self, now: dt.datetime,
              env: Optional[dict] = None) -> tuple[bool, str]:
        try:
            kwargs: dict = {"cwd": self.cwd or None}
            if env:
                kwargs["env"] = env
            if IS_WINDOWS:
                kwargs["creationflags"] = getattr(
                    subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            else:
                kwargs["start_new_session"] = True
            self.proc = subprocess.Popen(list(self.command), **kwargs)
            self.restarts.append(now)
            self.last_start = now
            # Exponential backoff so a component that crashes on start-up does
            # not become a restart loop that hides the real error.
            self.backoff = min(600.0, max(20.0, self.backoff * 2 or 20.0))
            return True, (f"{self.name}: started (pid {self.proc.pid}), "
                          f"next retry no sooner than {self.backoff:.0f}s")
        except Exception as exc:
            return False, f"{self.name}: could not start - {exc}"

    def stop(self) -> None:
        if self.proc is None:
            return
        try:
            self.proc.terminate()
            self.proc.wait(timeout=15)
        except Exception:
            try:
                self.proc.kill()
            except Exception:
                pass


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if IS_WINDOWS:
        try:
            out = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                capture_output=True, text=True, timeout=15).stdout
            return str(pid) in out
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def process_running(image_name: str) -> Optional[bool]:
    """Is a named executable running?  ``None`` when we cannot tell.

    On macOS and Linux this also finds a Windows executable running under
    Wine, because to the operating system ``terminal64.exe`` under Wine is
    simply a process whose command line contains that name.
    """
    if IS_WINDOWS:
        try:
            out = subprocess.run(
                ["tasklist", "/FI", f"IMAGENAME eq {image_name}", "/NH"],
                capture_output=True, text=True, timeout=20).stdout
            return image_name.lower() in out.lower()
        except Exception:
            return None
    if shutil.which("pgrep"):
        try:
            res = subprocess.run(["pgrep", "-f", image_name],
                                 capture_output=True, timeout=10)
            return res.returncode == 0
        except Exception:
            return None
    return None


def port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    """Liveness for a socket service.

    The bridge is judged by whether it actually answers, not by whether a
    process with the right name exists - a hung process still holds its name.
    """
    import socket as _socket
    try:
        with _socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


class Watchdog:
    def __init__(self, cfg: Config, processes: Sequence[ManagedProcess],
                 *, mt5_image: str = "terminal64.exe",
                 mt5_command: Sequence[str] = (),
                 clock=utcnow, env: Optional[dict] = None):
        self.cfg = cfg
        self.env = env
        self.processes = list(processes)
        self.mt5_image = mt5_image
        self.mt5_command = list(mt5_command)
        self.clock = clock
        self.heartbeat_dir = Path(cfg.ops.data_dir) / "heartbeats"
        self.own_heartbeat = Heartbeat(self.heartbeat_dir, "watchdog", clock)
        self.actions: list[str] = []
        self._stop = False
        self._mt5_restarts: list[dt.datetime] = []

    # ---------------------------------------------------------------- checks --
    def check_once(self) -> list[str]:
        now = to_utc(self.clock())
        actions: list[str] = []
        beats = read_all_heartbeats(self.heartbeat_dir, now)
        limit = self.cfg.ops.heartbeat_stale_seconds

        for mp in self.processes:
            reason = ""
            alive = mp.alive_by_pid()
            if alive is False:
                reason = "the process is not running"
            elif mp.probe is not None:
                answered = mp.probe()
                if answered is False and (
                        mp.last_start is None
                        or (now - mp.last_start).total_seconds()
                        > mp.grace_seconds):
                    reason = "it is not answering"
            elif mp.heartbeat:
                beat = beats.get(mp.heartbeat)
                if beat is None:
                    # Only complain once the grace period has passed: a fresh
                    # start has not had time to write a heartbeat yet.
                    if (mp.last_start is None
                            or (now - mp.last_start).total_seconds() > mp.grace_seconds):
                        reason = "it has never reported in"
                elif beat["age_seconds"] > limit:
                    reason = (f"its last report was "
                              f"{beat['age_seconds']:.0f}s ago (limit "
                              f"{limit:.0f}s) - it looks hung")
            if not reason:
                mp.backoff = 0.0      # healthy: clear the backoff
                continue
            if not mp.can_restart(now):
                actions.append(f"{mp.name}: {reason}, but the restart limit "
                               f"has been reached - NEEDS A HUMAN")
                continue
            actions.append(f"{mp.name}: {reason} - restarting")
            if reason != "the process is not running":
                # A hung process must be killed before a replacement is started,
                # or two traders end up fighting over the same account.
                mp.stop()
            ok, msg = mp.start(now, self.env)
            actions.append(msg)

        actions.extend(self._check_mt5(now))

        self.own_heartbeat.beat({"managed": len(self.processes),
                                 "actions": len(actions)})
        if actions:
            self.actions = (self.actions + actions)[-200:]
            self._log_actions(actions)
        return actions

    def _check_mt5(self, now: dt.datetime) -> list[str]:
        if not self.mt5_command:
            return []
        running = process_running(self.mt5_image)
        if running is not False:
            return []
        cutoff = now - dt.timedelta(hours=1)
        self._mt5_restarts = [t for t in self._mt5_restarts if t >= cutoff]
        if len(self._mt5_restarts) >= self.cfg.ops.max_restarts_per_hour:
            return [f"MetaTrader 5 is not running and has already been "
                    f"restarted {len(self._mt5_restarts)} times this hour - "
                    f"NEEDS A HUMAN"]
        try:
            kwargs: dict = {}
            if IS_WINDOWS:
                kwargs["creationflags"] = getattr(
                    subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            else:
                kwargs["start_new_session"] = True
            if self.env:
                kwargs["env"] = self.env
            subprocess.Popen(self.mt5_command, **kwargs)
            self._mt5_restarts.append(now)
            return ["MetaTrader 5 was not running - started it again"]
        except Exception as exc:
            return [f"MetaTrader 5 is not running and could not be started: "
                    f"{exc}"]

    def _log_actions(self, actions: Sequence[str]) -> None:
        for a in actions:
            log.warning("%s", a)
        path = Path(self.cfg.ops.log_dir) / "watchdog.log"
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a") as fh:
                for a in actions:
                    fh.write(f"{utcnow().isoformat()} {a}\n")
        except Exception:
            pass

    # ------------------------------------------------------------------ run --
    def run(self, max_iterations: int = 0) -> int:
        n = 0
        while not self._stop:
            try:
                self.check_once()
            except Exception as exc:
                log.exception("watchdog iteration failed: %s", exc)
            n += 1
            if max_iterations and n >= max_iterations:
                break
            time.sleep(self.cfg.ops.watchdog_interval_seconds)
        return n

    def stop(self, *_args) -> None:
        self._stop = True


def wine_command(cfg: Config, target: str,
                 extra: Sequence[str] = ()) -> list[str]:
    """Command to run a Windows executable inside the configured Wine prefix."""
    wine = shutil.which("wine64") or shutil.which("wine") or "wine"
    return [wine, target, *extra]


def wine_env(cfg: Config) -> dict:
    env = dict(os.environ)
    if cfg.wine_prefix:
        env["WINEPREFIX"] = cfg.wine_prefix
    # Wine's debug chatter is enormous and fills the disk on a long-running
    # machine, which would eventually trip the disk-space health check.
    env.setdefault("WINEDEBUG", "-all")
    return env


def build_default(cfg: Config, config_path: str = "", *, python: str = "",
                  project_dir: str = "") -> Watchdog:
    """The standard production arrangement.

    On Windows: watch the trader and the MT5 terminal.
    On macOS and Linux: watch the trader, the Wine-side bridge, and the MT5
    terminal running under Wine.
    """
    python = python or sys.executable
    project = project_dir or str(Path(__file__).resolve().parents[2])
    data = Path(cfg.ops.data_dir)
    config_path = str(Path(config_path).resolve()) if config_path \
        else str((data / "config.json").resolve())

    processes = [ManagedProcess(
        name="trader",
        command=[python, "-m", "mintel.run", "--config", config_path],
        heartbeat="strategy",
        pid_file=str(data / "trader.pid"),
        cwd=project,
        max_restarts_per_hour=cfg.ops.max_restarts_per_hour)]

    mt5_cmd: list[str] = []
    if cfg.broker_mode == "bridge":
        # The bridge runs the Windows Python that lives inside the Wine prefix.
        if cfg.wine_python:
            bridge_cmd = wine_command(cfg, cfg.wine_python, [
                "-m", "mintel.broker.bridge_server",
                "--port", str(cfg.bridge_port),
                "--token-file", cfg.bridge_token_file,
                "--login", str(cfg.account_login),
                "--server", cfg.account_server,
                "--secrets-file", str(data / "secrets.json"),
                "--terminal", cfg.mt5_terminal_path,
                "--magic", str(cfg.magic)])
            processes.append(ManagedProcess(
                name="bridge",
                command=bridge_cmd,
                pid_file=str(data / "bridge.pid"),
                cwd=project,
                grace_seconds=90.0,
                probe=lambda: port_open(cfg.bridge_host, cfg.bridge_port),
                max_restarts_per_hour=cfg.ops.max_restarts_per_hour))
        if cfg.mt5_terminal_path:
            mt5_cmd = wine_command(cfg, cfg.mt5_terminal_path)
    elif cfg.mt5_terminal_path:
        mt5_cmd = [cfg.mt5_terminal_path]

    wd = Watchdog(cfg, processes, mt5_command=mt5_cmd)
    wd.env = wine_env(cfg) if cfg.broker_mode == "bridge" else None
    return wd


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="mintel watchdog")
    ap.add_argument("--config", default="data/config.json")
    ap.add_argument("--once", action="store_true",
                    help="run a single check and exit (used by tests)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = Config.load(args.config)
    wd = build_default(cfg, args.config)
    signal.signal(signal.SIGINT, wd.stop)
    signal.signal(signal.SIGTERM, wd.stop)
    if args.once:
        for line in wd.check_once():
            print(line)
        return 0

    # Write our own PID so the launcher and the trader can both see that a
    # supervisor exists, and so a second watchdog is not started.
    pid_path = Path(cfg.ops.data_dir) / "watchdog.pid"
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    if pid_path.exists():
        try:
            other = int(pid_path.read_text().strip())
            if other != os.getpid() and pid_alive(other):
                log.error("another watchdog is already running (process %d)",
                          other)
                return 3
        except ValueError:
            pass
    pid_path.write_text(str(os.getpid()))
    log.info("watchdog started, watching %d process(es)", len(wd.processes))
    try:
        wd.run()
    finally:
        try:
            pid_path.unlink(missing_ok=True)
        except OSError:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
