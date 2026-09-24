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
import re
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
    # Where the process's own output goes. Also what makes Windows Python
    # under Wine start at all: handed launchd's /dev/null as standard input
    # it dies with "can't initialize sys standard streams / Invalid handle",
    # so every child gets a pipe for stdin and a real file for its output.
    log_file: str = ""
    # Run the child inside a pseudo-terminal. Windows Python under Wine 8
    # refuses to start unless its standard streams look like a console
    # ("can't initialize sys standard streams: Invalid handle" otherwise),
    # which is exactly what it gets from a Terminal window and exactly what
    # launchd does not give it. Output is pumped from the terminal into the
    # log file by a thread.
    use_pty: bool = False
    # A process pattern (as for pkill -f) that identifies stray copies of this
    # service started by an earlier run; stop() kills them too, so an upgrade
    # never leaves last week's bridge answering the new trader.
    kill_pattern: str = ""
    _log_fh: Optional[object] = field(default=None, repr=False, compare=False)
    _pty_master: int = field(default=-1, repr=False, compare=False)
    _pump: Optional[object] = field(default=None, repr=False, compare=False)
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
            if self.log_file:
                os.makedirs(os.path.dirname(self.log_file) or ".", exist_ok=True)
                self._close_log()
                self._log_fh = open(self.log_file, "ab")
            slave = -1
            if self.use_pty and not IS_WINDOWS:
                import pty
                self._pty_master, slave = pty.openpty()
                kwargs["stdin"] = slave
                kwargs["stdout"] = slave
                kwargs["stderr"] = slave
            else:
                if self._log_fh is not None:
                    kwargs["stdout"] = self._log_fh
                    kwargs["stderr"] = subprocess.STDOUT
                if not IS_WINDOWS:
                    kwargs["stdin"] = subprocess.PIPE
            if IS_WINDOWS:
                kwargs["creationflags"] = getattr(
                    subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            else:
                kwargs["start_new_session"] = True
            try:
                self.proc = subprocess.Popen(list(self.command), **kwargs)
            finally:
                if slave >= 0:
                    os.close(slave)
            if self._pty_master >= 0:
                self._start_pump()
            # Record the pid so "Stop" can find this process even if the
            # watchdog itself is gone (a child that writes its own pid file
            # overwrites this with the same number).
            if self.pid_file:
                try:
                    Path(self.pid_file).write_text(str(self.proc.pid))
                except Exception:
                    pass
            self.restarts.append(now)
            self.last_start = now
            # Exponential backoff so a component that crashes on start-up does
            # not become a restart loop that hides the real error.
            self.backoff = min(600.0, max(20.0, self.backoff * 2 or 20.0))
            return True, (f"{self.name}: started (pid {self.proc.pid}), "
                          f"next retry no sooner than {self.backoff:.0f}s")
        except Exception as exc:
            return False, f"{self.name}: could not start - {exc}"

    def _start_pump(self) -> None:
        """Copy everything the child writes to its terminal into the log."""
        import threading
        master, fh = self._pty_master, self._log_fh

        def pump() -> None:
            try:
                while True:
                    try:
                        chunk = os.read(master, 4096)
                    except OSError:
                        break          # child gone: the terminal is closed
                    if not chunk:
                        break
                    if fh is not None:
                        try:
                            fh.write(chunk)
                            fh.flush()
                        except Exception:
                            pass
            finally:
                try:
                    os.close(master)
                except OSError:
                    pass

        self._pump = threading.Thread(target=pump, name=f"{self.name}-pty", daemon=True)
        self._pump.start()

    def _close_log(self) -> None:
        if self._pump is not None:
            self._pump.join(timeout=2)
            self._pump = None
            self._pty_master = -1
        if self._log_fh is not None:
            try:
                self._log_fh.close()
            except Exception:
                pass
            self._log_fh = None

    def stop(self) -> None:
        if self.proc is not None:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=15)
            except Exception:
                try:
                    self.proc.kill()
                except Exception:
                    pass
        elif self.pid_file:
            # Adopted from a PID file (an earlier watchdog started it).
            try:
                pid = int(Path(self.pid_file).read_text().strip())
                if pid > 0 and pid != os.getpid():
                    os.kill(pid, signal.SIGTERM)
                    for _ in range(30):
                        if not pid_alive(pid):
                            break
                        time.sleep(0.5)
                    if pid_alive(pid):
                        os.kill(pid, signal.SIGKILL)
            except Exception:
                pass
        if self.kill_pattern and not IS_WINDOWS:
            try:
                subprocess.run(["pkill", "-f", self.kill_pattern],
                               capture_output=True, timeout=15)
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


def kill_process(image_name: str) -> None:
    """Terminate every process whose command line names ``image_name``."""
    try:
        if IS_WINDOWS:
            subprocess.run(["taskkill", "/F", "/IM", image_name],
                           capture_output=True, timeout=20)
        elif shutil.which("pkill"):
            subprocess.run(["pkill", "-f", image_name],
                           capture_output=True, timeout=10)
    except Exception:
        pass


def bridge_current(host: str, port: int, token_file: str = "") -> bool:
    """Is a bridge answering on the port AND running the installed code?

    A bridge that survived an upgrade answers the port but not the trader's
    newer questions; reporting it as "not answering" makes the watchdog stop
    it and start the installed code in its place.

    The probe carries the shared token: without it the bridge (rightly)
    refuses to answer, and every check leaves a "bad token" line in its log.
    """
    if not port_open(host, port):
        return False
    try:
        from ..broker.bridge_client import BridgeBroker
        client = BridgeBroker(host=host, port=port, token_file=token_file)
        try:
            why = client.outdated()
        finally:
            try:
                client.disconnect()
            except Exception:
                pass
        if why:
            log.warning("%s", why)
            return False
    except Exception:
        return True          # answering; cannot judge its version
    return True


def bridge_status(host: str, port: int, token_file: str = "") -> Optional[dict]:
    """What the bridge says about itself (its ping), or None if it is silent."""
    if not port_open(host, port):
        return None
    try:
        from ..broker.bridge_client import BridgeBroker
        client = BridgeBroker(host=host, port=port, token_file=token_file)
        try:
            return client.ping()
        finally:
            try:
                client.disconnect()
            except Exception:
                pass
    except Exception:
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
        # The bridge's own view of MetaTrader, refreshed each check by
        # ``bridge_status_fn``; a terminal that is running but has stopped
        # answering the bridge for this long is restarted.
        self.bridge_status_fn: Optional[Callable[[], Optional[dict]]] = None
        self.mt5_unreachable_seconds: float = 600.0
        self._mt5_unreachable_since: Optional[dt.datetime] = None

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
            return self._check_mt5_answers(now)
        self._mt5_unreachable_since = None
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

    def _check_mt5_answers(self, now: dt.datetime) -> list[str]:
        """A running terminal that the bridge cannot talk to is as good as
        dead: MetaTrader's pipe stops answering ("IPC initialize failed") and
        only a restart of the terminal brings it back.  Nothing else can be
        traded until it does, so the watchdog restarts it after
        ``mt5_unreachable_seconds`` of silence."""
        if self.bridge_status_fn is None:
            return []
        try:
            info = self.bridge_status_fn()
        except Exception:
            info = None
        if not info or "mt5_connected" not in info:
            # No bridge, or an old bridge that does not say: not our call.
            self._mt5_unreachable_since = None
            return []
        if info.get("mt5_connected"):
            self._mt5_unreachable_since = None
            return []
        if int(info.get("connect_failures") or 0) < 1:
            return []
        if self._mt5_unreachable_since is None:
            self._mt5_unreachable_since = now
            return []
        silent = (now - self._mt5_unreachable_since).total_seconds()
        if silent < self.mt5_unreachable_seconds:
            return []
        cutoff = now - dt.timedelta(hours=1)
        self._mt5_restarts = [t for t in self._mt5_restarts if t >= cutoff]
        if len(self._mt5_restarts) >= self.cfg.ops.max_restarts_per_hour:
            return [f"MetaTrader 5 is running but has not answered the bridge "
                    f"for {silent:.0f}s ({info.get('mt5_last_error', '')}) and "
                    f"has already been restarted "
                    f"{len(self._mt5_restarts)} times this hour - NEEDS A HUMAN"]
        self._mt5_unreachable_since = None
        self._mt5_restarts.append(now)
        kill_process(self.mt5_image)
        return [f"MetaTrader 5 is running but has not answered the bridge for "
                f"{silent:.0f}s ({info.get('mt5_last_error', '')}) - "
                f"closing it so it can be started fresh"]

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
        if self._stop:
            # Asked to stop (Stop icon, launchctl unload, SIGTERM): take the
            # children with us. Leaving them running with old code, unwatched,
            # is exactly what made an "upgrade" silently run last week's bridge.
            for mp in self.processes:
                try:
                    mp.stop()
                except Exception:
                    pass
        return n

    def stop(self, *_args) -> None:
        self._stop = True


def wine_command(cfg: Config, target: str,
                 extra: Sequence[str] = ()) -> list[str]:
    """Command to run a Windows executable inside the configured Wine prefix.

    The configured binary wins: under launchd PATH is bare, so a which() lookup
    that works in a Terminal finds nothing there.
    """
    wine = (cfg.wine_binary or shutil.which("wine64") or shutil.which("wine")
            or "wine")
    return [wine, target, *extra]


def windows_path(path: str) -> str:
    """A path Windows Python inside Wine can open.

    Windows-style paths pass through unchanged. Unix paths are mapped through
    Wine's Z: drive, which a fresh prefix points at the root of the file system.
    """
    if re.match(r"^[A-Za-z]:[\\/]", path):
        return path
    return "Z:" + path.replace("/", "\\")


def wine_env(cfg: Config) -> dict:
    env = dict(os.environ)
    if cfg.wine_prefix:
        env["WINEPREFIX"] = cfg.wine_prefix
    # Wine's debug chatter is enormous and fills the disk on a long-running
    # machine, which would eventually trip the disk-space health check.
    env.setdefault("WINEDEBUG", "-all")
    # Without these, the first HTML panel MetaTrader opens makes Wine pop a
    # "install Gecko?" dialog that a launchd-started process can never answer.
    env.setdefault("WINEDLLOVERRIDES", "mscoree,mshtml=")
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
        log_file=str(Path(cfg.ops.log_dir) / "trader.out.log"),
        max_restarts_per_hour=cfg.ops.max_restarts_per_hour)]

    mt5_cmd: list[str] = []
    if cfg.broker_mode == "bridge":
        # The bridge runs the Windows Python that lives inside the Wine prefix.
        if cfg.wine_python:
            # By file path, not "-m": the embeddable Windows Python ignores
            # PYTHONPATH and the working directory (its ._pth file puts it in
            # isolated mode), and the script adds the project root itself.
            bridge_script = windows_path(
                str(Path(project) / "mintel" / "broker" / "bridge_server.py"))
            bridge_cmd = wine_command(cfg, cfg.wine_python, [
                bridge_script,
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
                log_file=str(Path(cfg.ops.log_dir) / "bridge.out.log"),
                use_pty=True,
                kill_pattern="bridge_server.py",
                grace_seconds=90.0,
                probe=lambda: bridge_current(cfg.bridge_host, cfg.bridge_port,
                                             cfg.bridge_token_file),
                max_restarts_per_hour=cfg.ops.max_restarts_per_hour))
        if cfg.mt5_terminal_path:
            mt5_cmd = wine_command(cfg, cfg.mt5_terminal_path)
    elif cfg.mt5_terminal_path:
        mt5_cmd = [cfg.mt5_terminal_path]

    wd = Watchdog(cfg, processes, mt5_command=mt5_cmd)
    wd.env = wine_env(cfg) if cfg.broker_mode == "bridge" else None
    if cfg.broker_mode == "bridge":
        wd.bridge_status_fn = lambda: bridge_status(
            cfg.bridge_host, cfg.bridge_port, cfg.bridge_token_file)
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
