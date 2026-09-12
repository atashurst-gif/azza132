"""The macOS production pipeline, end to end, minus Wine.

On a Mac the running system is three processes:

    watchdog  ->  bridge (inside Wine, talking to MT5)
              ->  trader (native, talking to the bridge)  ->  dashboard

These tests start that exact arrangement for real - separate processes, real
sockets, the real config file, the real dashboard - with the bridge pointed at
the simulator instead of MetaTrader 5.  Everything except the Wine hop is
production code on the production path.

What is deliberately NOT claimed here: that Wine works, that MetaTrader 5
installs, or that a broker accepts the orders.  Those need a real Mac and a
real account, and `mintel.verify` is what checks them there.
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(predicate, timeout: float = 60.0, interval: float = 0.5) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if predicate():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def http_json(url: str, timeout: float = 5.0):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


@pytest.fixture
def deployment(tmp_path):
    """A complete installation in a temporary folder."""
    data = tmp_path / "data"
    logs = tmp_path / "logs"
    data.mkdir(parents=True)
    logs.mkdir(parents=True)

    bridge_port = free_port()
    dash_port = free_port()
    token = "pipeline-token"
    (data / "bridge-token.txt").write_text(token)

    config = {
        "version": 1, "mode": "DEMO", "live_marker": "", "magic": 990311,
        "account_login": 0, "account_server": "",
        "broker_mode": "bridge", "bridge_host": "127.0.0.1",
        "bridge_port": bridge_port, "aggression": "NORMAL",
        "risk": {"base_risk_pct": 0.5, "max_risk_pct": 1.5},
        "universe": {"max_symbols": 5},
        "scan": {"scan_interval_seconds": 2.0, "manage_interval_seconds": 1.0},
        "news": {"enabled": True, "store_path": "calendar.sqlite"},
        "ops": {"data_dir": str(data), "log_dir": str(logs),
                "dashboard_host": "127.0.0.1", "dashboard_port": dash_port},
    }
    config_path = data / "config.json"
    config_path.write_text(json.dumps(config, indent=2))

    procs: list[subprocess.Popen] = []

    def spawn(args, name):
        proc = subprocess.Popen(
            [sys.executable, *args], cwd=str(ROOT),
            stdout=open(logs / f"{name}.out.log", "w"),
            stderr=open(logs / f"{name}.err.log", "w"),
            start_new_session=True)
        procs.append(proc)
        return proc

    yield {"config": config_path, "data": data, "logs": logs,
           "bridge_port": bridge_port, "dash_port": dash_port,
           "token": token, "spawn": spawn}

    for proc in procs:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except Exception:
            proc.terminate()
    for proc in procs:
        try:
            proc.wait(timeout=20)
        except Exception:
            proc.kill()


@pytest.mark.slow
class TestThreeProcessPipeline:
    def _start_bridge(self, deployment, live: bool = False):
        args = ["-m", "mintel.broker.bridge_server", "--backend", "sim",
                "--port", str(deployment["bridge_port"]),
                "--token-file", str(deployment["data"] / "bridge-token.txt"),
                "--sim-bars", "3000"]
        if live:
            args += ["--sim-live", "--sim-speed", "30"]
        return deployment["spawn"](args, "bridge")

    def _bridge_up(self, deployment) -> bool:
        with socket.create_connection(
                ("127.0.0.1", deployment["bridge_port"]), timeout=2):
            return True

    def test_bridge_then_trader_then_dashboard(self, deployment):
        self._start_bridge(deployment)
        assert wait_for(lambda: self._bridge_up(deployment), 60), \
            "the bridge never came up"

        deployment["spawn"](
            ["-m", "mintel.run", "--config", str(deployment["config"])],
            "trader")

        url = f"http://127.0.0.1:{deployment['dash_port']}/api"
        assert wait_for(lambda: http_json(url) is not None, 120), \
            f"the dashboard never answered; see {deployment['logs']}"

        snap = http_json(url)
        status = snap["status"]
        assert status["bot"] in ("RUNNING", "SAFE MODE")
        assert status["mt5_connected"] is True, (
            "the trader must see the broker through the bridge")
        assert status["mode"] == "DEMO"

        # It must actually be looking at markets, not merely alive.
        assert wait_for(lambda: http_json(url)["thinking"], 90), \
            "the trader never produced a ranking"
        thinking = http_json(url)["thinking"]
        assert thinking[0]["symbol"]
        assert 0 <= thinking[0]["score"] <= 100

        # And the PID files the launcher relies on must exist.
        assert (deployment["data"] / "trader.pid").exists()

    def test_a_live_feed_reads_as_live(self, deployment):
        """With a wall-clock-tracking feed, every freshness check must pass."""
        self._start_bridge(deployment, live=True)
        assert wait_for(lambda: self._bridge_up(deployment), 60)
        deployment["spawn"](
            ["-m", "mintel.run", "--config", str(deployment["config"])],
            "trader")
        url = f"http://127.0.0.1:{deployment['dash_port']}/api"
        assert wait_for(lambda: http_json(url)["status"].get("data_live"), 120), \
            "fresh prices must read as live, not stale"
        snap = http_json(url)
        assert snap["health"]["safe_mode"] is False
        assert snap["health"]["entries_allowed"] is True, (
            "a healthy live feed must leave the bot free to trade")

    def test_health_endpoint_reflects_the_bridge(self, deployment):
        self._start_bridge(deployment)
        assert wait_for(lambda: self._bridge_up(deployment), 60)
        deployment["spawn"](
            ["-m", "mintel.run", "--config", str(deployment["config"])],
            "trader")
        url = f"http://127.0.0.1:{deployment['dash_port']}/api"
        assert wait_for(lambda: http_json(url)["health"].get("checks"), 120)
        checks = {c["name"]: c for c in http_json(url)["health"]["checks"]}
        assert checks["BROKER_CONNECTION"]["severity"] == "OK"
        assert checks["MARKET_DATA"]["severity"] in ("OK", "WARN")

    def test_a_second_trader_refuses_to_start(self, deployment):
        """Two traders on one account would double every trade."""
        self._start_bridge(deployment)
        assert wait_for(lambda: self._bridge_up(deployment), 60)
        deployment["spawn"](
            ["-m", "mintel.run", "--config", str(deployment["config"])],
            "trader")
        assert wait_for(
            lambda: (deployment["data"] / "trader.pid").exists(), 90)
        second = subprocess.run(
            [sys.executable, "-m", "mintel.run", "--config",
             str(deployment["config"]), "--cycles", "1"],
            cwd=str(ROOT), capture_output=True, text=True, timeout=120)
        assert second.returncode == 3, second.stdout + second.stderr
        assert "already running" in (second.stdout + second.stderr)

    def test_the_trader_survives_the_bridge_restarting(self, deployment):
        self._start_bridge(deployment)
        assert wait_for(lambda: self._bridge_up(deployment), 60)
        deployment["spawn"](
            ["-m", "mintel.run", "--config", str(deployment["config"])],
            "trader")
        url = f"http://127.0.0.1:{deployment['dash_port']}/api"
        assert wait_for(lambda: http_json(url)["thinking"], 120)

        # Kill the Wine-side process, as a Wine crash or an MT5 restart would.
        subprocess.run(["pkill", "-f", "bridge_server"], check=False)
        assert wait_for(
            lambda: http_json(url)["health"].get("safe_mode") is True, 60), \
            "losing the bridge must put the trader into SAFE MODE"

        self._start_bridge(deployment)
        assert wait_for(lambda: self._bridge_up(deployment), 60)
        assert wait_for(
            lambda: http_json(url)["health"].get("safe_mode") is False, 120), \
            "and it must recover by itself once the bridge is back"


@pytest.mark.slow
class TestWatchdogRunsTheWholeThing:
    def test_the_watchdog_starts_and_keeps_the_trader_alive(self, deployment):
        """The LaunchAgent starts only the watchdog; it must do the rest."""
        deployment["spawn"](
            ["-m", "mintel.broker.bridge_server", "--backend", "sim",
             "--port", str(deployment["bridge_port"]),
             "--token-file", str(deployment["data"] / "bridge-token.txt"),
             "--sim-bars", "2000"], "bridge")
        assert wait_for(
            lambda: socket.create_connection(
                ("127.0.0.1", deployment["bridge_port"]), timeout=2), 60)

        deployment["spawn"](
            ["-m", "mintel.ops.watchdog", "--config",
             str(deployment["config"])], "watchdog")

        assert wait_for(
            lambda: (deployment["data"] / "watchdog.pid").exists(), 60), \
            "the watchdog never wrote its PID file"
        assert wait_for(
            lambda: (deployment["data"] / "trader.pid").exists(), 120), \
            "the watchdog never started the trader"

        pid_file = deployment["data"] / "trader.pid"
        first_pid = int(pid_file.read_text().strip())

        # Kill the trader. The watchdog must bring it back.
        os.kill(first_pid, signal.SIGKILL)
        assert wait_for(
            lambda: pid_file.exists()
            and int(pid_file.read_text().strip()) != first_pid, 180), \
            "the watchdog did not restart the trader after it was killed"
        new_pid = int(pid_file.read_text().strip())
        assert new_pid != first_pid
        os.kill(new_pid, 0)          # raises if it is not actually alive


class TestConfigWrittenByTheInstaller:
    """The installer writes JSON by hand; it must load as a real Config."""

    def test_installer_shaped_config_loads(self, tmp_path):
        from mintel.config import Config
        data = tmp_path / "data"
        data.mkdir()
        (data / "config.json").write_text(json.dumps({
            "version": 1, "mode": "DEMO", "live_marker": "", "magic": 990311,
            "account_login": 12345, "account_server": "Broker-Demo",
            "mt5_terminal_path": "C:\\Program Files\\MetaTrader 5\\terminal64.exe",
            "broker_mode": "bridge", "bridge_host": "127.0.0.1",
            "bridge_port": 8790,
            "wine_prefix": "/Users/me/MarketBot/wine",
            "wine_python": "C:\\Python311\\python.exe",
            "aggression": "NORMAL",
            "risk": {"base_risk_pct": 0.5, "max_risk_pct": 1.5,
                     "max_daily_loss_pct": 3.0},
            "universe": {"groups": ["FX_MAJOR", "GOLD"], "max_symbols": 40},
            "news": {"enabled": True, "store_path": "calendar.sqlite"},
            "ops": {"data_dir": str(data), "log_dir": str(tmp_path / "logs"),
                    "dashboard_host": "127.0.0.1", "dashboard_port": 8787},
        }, indent=2))
        cfg = Config.load(data / "config.json")
        assert cfg.validate() == []
        assert cfg.broker_mode == "bridge"
        assert cfg.wine_python.endswith("python.exe")
        assert cfg.effective_mode == "DEMO"
        assert cfg.risk.max_risk_pct == 1.5
        assert cfg.bridge_token_file.endswith("bridge-token.txt")

    def test_live_still_needs_the_marker_from_the_installer(self, tmp_path):
        from mintel.config import Config, LIVE_MARKER
        data = tmp_path / "data"
        data.mkdir()
        base = {"version": 1, "mode": "LIVE", "magic": 990311,
                "broker_mode": "bridge",
                "ops": {"data_dir": str(data), "log_dir": str(data)}}
        (data / "config.json").write_text(json.dumps({**base,
                                                      "live_marker": ""}))
        assert Config.load(data / "config.json").effective_mode == "DEMO"
        (data / "config.json").write_text(
            json.dumps({**base, "live_marker": LIVE_MARKER}))
        assert Config.load(data / "config.json").is_live


class TestLauncherScripts:
    ROOT_DEPLOY = ROOT / "deploy" / "mac"

    def test_scripts_are_syntactically_valid(self):
        for name in ("mintel_mac.sh", "Start Trading Bot.command",
                     "Stop Trading Bot.command"):
            path = self.ROOT_DEPLOY / name
            assert path.exists(), name
            result = subprocess.run(["bash", "-n", str(path)],
                                    capture_output=True, text=True)
            assert result.returncode == 0, f"{name}: {result.stderr}"

    def test_scripts_are_executable(self):
        for name in ("Start Trading Bot.command", "Stop Trading Bot.command"):
            assert os.access(self.ROOT_DEPLOY / name, os.X_OK), name

    def test_the_launcher_is_idempotent_by_construction(self):
        """A routine double-click on a healthy system must not reinstall."""
        text = (self.ROOT_DEPLOY / "Start Trading Bot.command").read_text()
        assert "is_running watchdog && is_running trader" in text
        assert "already running" in text

    def test_the_launcher_never_starts_a_second_trader(self):
        """It starts the SUPERVISOR, which owns the trader and refuses to
        double up.  Spawning ``mintel.run`` from the launcher as well would
        race the watchdog and could put two traders on one account."""
        launcher = (self.ROOT_DEPLOY / "Start Trading Bot.command").read_text()
        lib = (self.ROOT_DEPLOY / "mintel_mac.sh").read_text()
        assert "mintel.ops.watchdog" in lib
        assert "-m mintel.run" not in launcher
        assert "mintel.run" not in lib.split("start_everything()")[1][:1500]

    def test_install_steps_are_all_defined(self):
        lib = (self.ROOT_DEPLOY / "mintel_mac.sh").read_text()
        for fn in ("check_macos", "make_folders", "copy_program",
                   "find_native_python", "make_venv", "ensure_wine",
                   "ensure_wine_python", "ensure_mt5", "write_config",
                   "install_launch_agent", "prevent_idle_sleep", "start_mt5",
                   "start_everything", "show_health", "final_report",
                   "stop_everything"):
            assert f"{fn}()" in lib, fn

    def test_the_launch_agent_restarts_on_crash_and_at_login(self):
        lib = (self.ROOT_DEPLOY / "mintel_mac.sh").read_text()
        assert "<key>RunAtLoad</key><true/>" in lib
        assert "<key>KeepAlive</key><true/>" in lib
        assert "ThrottleInterval" in lib

    def test_the_report_is_honest_about_the_lid(self):
        lib = (self.ROOT_DEPLOY / "mintel_mac.sh").read_text()
        assert "close the lid" in lib
        assert "clamshell" in lib


class TestLauncherRobustness:
    ROOT_DEPLOY = ROOT / "deploy" / "mac"

    def test_find_uses_macos_argument_order(self):
        """BSD find wants -maxdepth before the test; GNU only warns."""
        lib = (self.ROOT_DEPLOY / "mintel_mac.sh").read_text()
        assert "-name terminal64.exe -maxdepth" not in lib
        assert "-maxdepth 5 -name terminal64.exe" in lib

    def test_repeated_runs_do_not_stack_caffeinate(self):
        lib = (self.ROOT_DEPLOY / "mintel_mac.sh").read_text()
        block = lib.split("prevent_idle_sleep()")[1].split("\n}")[0]
        assert "kill -0" in block, "it must reuse a live caffeinate"

    def test_it_installs_a_desktop_icon(self):
        lib = (self.ROOT_DEPLOY / "mintel_mac.sh").read_text()
        launcher = (self.ROOT_DEPLOY / "Start Trading Bot.command").read_text()
        assert "install_desktop_icon()" in lib
        assert "$HOME/Desktop" in lib
        assert "com.apple.quarantine" in lib, (
            "a copy we made ourselves must not be blocked by Gatekeeper")
        assert "install_desktop_icon" in launcher

    def test_the_installed_copy_is_preferred_over_a_stale_download(self):
        launcher = (self.ROOT_DEPLOY / "Start Trading Bot.command").read_text()
        finder = launcher.split("find_source() {")[1].split("\n}")[0]
        installed = finder.index("MarketBot/app/mintel")
        remembered = finder.index("MarketBot/.source")
        assert installed < remembered, (
            "the download folder may have been deleted; prefer what is installed")
        assert '-d "$remembered/mintel"' in finder, (
            "a remembered path must be validated before it is used")

    def test_the_launcher_explains_the_first_run(self):
        launcher = (self.ROOT_DEPLOY / "Start Trading Bot.command").read_text()
        assert "the very first time" in launcher.lower()

    def test_the_self_test_cannot_reach_a_broker(self):
        text = (self.ROOT_DEPLOY / "Self Test.command").read_text()
        assert "--backend sim" in text or '"sim"' in text
        assert "no broker" in text.lower()
        assert "mt5" not in text.lower().split("metatrader")[0].split("bridge")[-1][:200]

    def test_the_self_test_uses_a_live_feed(self):
        text = (self.ROOT_DEPLOY / "Self Test.command").read_text()
        assert "--sim-live" in text, (
            "a frozen clock would make fresh prices read as stale")


class TestBashCompatibility:
    """macOS ships bash 3.2 (2007). The launcher must run on it.

    This is not hypothetical: the first release of these scripts died on the
    very first line it executed, because bash 3.2 does not make the first
    variable in a `local a=... b=...` statement visible to the second
    assignment, and `set -u` turns that into a fatal error rather than an
    empty string. These checks are static because the bash available in CI is
    5.x and simply cannot reproduce 3.2's behaviour.
    """

    ROOT_DEPLOY = ROOT / "deploy" / "mac"
    SCRIPTS = ("mintel_mac.sh", "Start Trading Bot.command",
               "Stop Trading Bot.command", "Self Test.command")

    def _scripts(self):
        for name in self.SCRIPTS:
            yield name, (self.ROOT_DEPLOY / name).read_text()

    def test_no_local_statement_references_its_own_earlier_variable(self):
        import re
        # `local a=1 b=$a` - the exact shape that broke on the user's Mac.
        pattern = re.compile(r"^\s*local\s+(\w+)=[^;]*?\s+\w+=[^;]*?\$\{?\1\b",
                             re.MULTILINE)
        for name, text in self._scripts():
            code = "\n".join(l for l in text.splitlines()
                             if not l.strip().startswith("#"))
            match = pattern.search(code)
            assert match is None, (
                f"{name}: `{match.group(0).strip()}` - bash 3.2 cannot see the "
                f"first variable in the second assignment")

    def test_no_bash_4_only_syntax(self):
        import re
        banned = {
            r"\$\{\w+,,": "${var,,} lowercase expansion is bash 4+",
            r"\$\{\w+\^\^": "${var^^} uppercase expansion is bash 4+",
            r"\bmapfile\b": "mapfile is bash 4+",
            r"\breadarray\b": "readarray is bash 4+",
            r"\bdeclare\s+-A\b": "associative arrays are bash 4+",
            r"\bdeclare\s+-g\b": "declare -g is bash 4+",
            r"&>>": "&>> append redirection is bash 4+",
        }
        for name, text in self._scripts():
            for pattern, why in banned.items():
                assert not re.search(pattern, text), f"{name}: {why}"

    def test_output_helpers_tolerate_no_arguments(self):
        """bash 3.2 + set -u treats an unset "$*" as unbound."""
        lib = (self.ROOT_DEPLOY / "mintel_mac.sh").read_text()
        for helper in ("say()", "step()", "good()", "warn()", "bad()"):
            line = next(l for l in lib.splitlines() if l.strip().startswith(helper))
            assert '"$*"' not in line, (
                f"{helper} uses \"$*\"; use \"${{*:-}}\" so a bare call is safe")

    def test_array_expansions_are_guarded(self):
        """bash 3.2 + set -u dies on "${arr[@]}" when arr is empty."""
        lib = (self.ROOT_DEPLOY / "mintel_mac.sh").read_text()
        lines = lib.splitlines()
        for i, line in enumerate(lines):
            for array in ("WARNINGS", "FAILURES"):
                if f'"${{{array}[@]}}"' in line:
                    window = "\n".join(lines[max(0, i - 4):i])
                    assert f"${{#{array}[@]}}" in window, (
                        f"line {i + 1}: expanding {array} without first "
                        f"checking it is non-empty")

    def test_positional_parameters_are_defaulted(self):
        """`$1` in a function must be written `${1:-}` under set -u."""
        import re
        for name, text in self._scripts():
            for line_no, line in enumerate(text.splitlines(), 1):
                if line.strip().startswith("#"):
                    continue          # comments may quote the bad pattern
                match = re.search(r'"\$(\d)"', line)
                if match is None:
                    continue
                assert "${" in line or "local" not in line, (
                    f"{name} line {line_no}: {line.strip()!r} - use "
                    f'"${{{match.group(1)}:-}}" so an absent argument is not fatal')

    def test_every_script_declares_a_bash_shebang(self):
        for name, text in self._scripts():
            assert text.startswith("#!/bin/bash"), name

    def test_the_launcher_keeps_the_window_open_on_failure(self):
        """A .command window closes on exit; an error would vanish unread."""
        launcher = (self.ROOT_DEPLOY / "Start Trading Bot.command").read_text()
        assert "trap on_unexpected_exit EXIT" in launcher
        assert "BASH_LINENO" in launcher, "it must say WHERE it failed"

    def test_interactive_installers_are_given_a_terminal(self):
        """Homebrew asks for RETURN and a password; </dev/null is an instant
        invisible failure."""
        lib = (self.ROOT_DEPLOY / "mintel_mac.sh").read_text()
        brew_line = next(l for l in lib.splitlines()
                         if "install.sh" in l and "curl" in l)
        assert "</dev/null" not in brew_line
        assert "</dev/tty" in brew_line

    def test_rosetta_failure_is_not_fatal(self):
        lib = (self.ROOT_DEPLOY / "mintel_mac.sh").read_text()
        block = lib.split("Rosetta 2 is needed")[1].split("fi\n  fi")[0]
        assert "warn " in block, "a Rosetta failure must warn, not abort"
        assert ">/dev/null" not in block, (
            "hiding the output hides the password prompt it may show")


class TestLauncherRunsUnderSetU:
    """Actually execute the library's pure functions with `set -u` on."""

    LIB = ROOT / "deploy" / "mac" / "mintel_mac.sh"

    def _run(self, snippet: str, env_extra=None):
        script = f'set -uo pipefail\nsource "{self.LIB}"\n{snippet}\n'
        env = dict(os.environ)
        env["MINTEL_HOME"] = "/tmp/mintel-nonexistent-home"
        env.update(env_extra or {})
        return subprocess.run(["bash", "-c", script], capture_output=True,
                              text=True, env=env, timeout=60)

    def test_library_sources_cleanly(self):
        result = self._run('echo sourced-ok')
        assert result.returncode == 0, result.stderr
        assert "sourced-ok" in result.stdout
        assert "unbound variable" not in result.stderr

    def test_is_running_on_a_missing_pid_file(self):
        result = self._run('if is_running watchdog; then echo yes; '
                           'else echo no; fi')
        assert result.returncode == 0, result.stderr
        assert "unbound variable" not in result.stderr
        assert "no" in result.stdout

    def test_is_running_with_no_argument_is_safe(self):
        result = self._run('if is_running; then echo yes; else echo no; fi')
        assert result.returncode == 0, result.stderr
        assert "unbound variable" not in result.stderr
        assert "no" in result.stdout

    def test_is_running_detects_a_live_process(self, tmp_path):
        pid_dir = tmp_path / "data"
        pid_dir.mkdir()
        (pid_dir / "self.pid").write_text(str(os.getpid()))
        result = self._run(
            f'DATA_DIR="{pid_dir}"; if is_running self; then echo yes; '
            f'else echo no; fi')
        assert "yes" in result.stdout, result.stderr

    def test_is_running_rejects_a_dead_pid(self, tmp_path):
        pid_dir = tmp_path / "data"
        pid_dir.mkdir()
        (pid_dir / "ghost.pid").write_text("4194303")
        result = self._run(
            f'DATA_DIR="{pid_dir}"; if is_running ghost; then echo yes; '
            f'else echo no; fi')
        assert "no" in result.stdout, result.stderr

    def test_output_helpers_with_no_arguments(self):
        result = self._run('say; step; good; warn; bad; echo survived')
        assert result.returncode == 0, result.stderr
        assert "unbound variable" not in result.stderr
        assert "survived" in result.stdout

    def test_final_report_with_empty_arrays(self):
        result = self._run('final_report || true; echo survived')
        assert "unbound variable" not in result.stderr
        assert "survived" in result.stdout

    def test_final_report_with_warnings_and_failures(self):
        result = self._run(
            'warn "a warning"; bad "a failure"; final_report || true; '
            'echo survived')
        assert "unbound variable" not in result.stderr
        assert "a warning" in result.stdout
        assert "a failure" in result.stdout
        assert "survived" in result.stdout
