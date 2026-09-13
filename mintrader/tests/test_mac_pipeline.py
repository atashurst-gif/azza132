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
            "wine_binary": "/Applications/Wine Stable.app/Contents/Resources/wine/bin/wine",
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
        assert cfg.wine_binary.endswith("/bin/wine")
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
                   "find_native_python", "make_venv", "run_logged",
                   "resolve_path", "find_wine", "wine_tool", "wine_wait",
                   "to_windows_path", "ensure_wine",
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
        import re
        lib = (self.ROOT_DEPLOY / "mintel_mac.sh").read_text()
        assert "-name terminal64.exe -maxdepth" not in lib
        assert re.search(r"-maxdepth \d+ -name terminal64\.exe", lib)

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
                    window = "\n".join(lines[max(0, i - 12):i])
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


class TestInstallerNeverHidesAFailure:
    """The Wine install failed on a real Mac with nothing to read.

    Every step that can fail for a reason outside our control must show its
    output live and keep a log. Silence is what turned a diagnosable problem
    into "Wine could not be installed" and nothing else.
    """

    LIB = ROOT / "deploy" / "mac" / "mintel_mac.sh"

    def _code_lines(self):
        return [l for l in self.LIB.read_text().splitlines()
                if not l.strip().startswith("#")]

    def test_no_installer_step_is_silenced(self):
        import re
        for line in self._code_lines():
            if ">/dev/null 2>&1" not in line:
                continue
            for marker in ("brew install", "brew update", "pip install",
                           "$installer", "wineboot", "get-pip", "curl -f"):
                # Whole tokens only: the string "Homebrew installed" contains
                # "brew install" and is not an installer step.
                pattern = r"(?<![A-Za-z])" + re.escape(marker) + r"(?![A-Za-z])"
                assert not re.search(pattern, line), (
                    f"installer step with its output hidden: {line.strip()!r}")

    def test_big_steps_go_through_the_logged_runner(self):
        text = self.LIB.read_text()
        for needle in ("brew install --cask wine-stable",
                       "python-3.11.9-embed-amd64.zip",
                       "get-pip.py",
                       "mt5setup.exe"):
            assert needle in text, needle
        # and each of those is invoked via run_logged, not bare
        for line in self._code_lines():
            if "brew install --cask" in line or "get-pip.py' " in line:
                block_start = text.rfind("run_logged", 0, text.find(line))
                assert block_start != -1

    def test_wine_is_found_in_app_bundles_not_only_on_path(self):
        text = self.LIB.read_text()
        for bundle in ("Wine Stable.app", "Wine Crossover.app"):
            assert bundle in text, (
                f"{bundle}: a cask can install the app and still fail to "
                f"link the command-line binary")

    def test_wine_fallbacks_are_ordered(self):
        """WineHQ's own package first; Homebrew only if that fails."""
        body = self.LIB.read_text().split("ensure_wine_standalone() {")[1].split("\n}\n")[0]
        direct = body.index("ensure_wine_package")
        brew = body.index('"Installing wine-stable with Homebrew (fallback)"')
        assert direct < brew
        assert "wine-crossover" not in body, "that tap no longer ships it"
        assert "gcenx" not in body

    def test_wine_helpers_are_not_assumed_to_be_on_path(self):
        for line in self._code_lines():
            stripped = line.strip()
            assert not stripped.startswith("wineboot "), line
            assert not stripped.startswith("wineserver "), line
            assert not stripped.startswith("WINEDLLOVERRIDES=\"mscoree,mshtml=\" wineboot"), line
        text = self.LIB.read_text()
        assert '"$WINE_BIN" wineboot --init' in text
        assert 'wine_tool wineserver' in text

    def test_windows_python_uses_the_embeddable_build(self):
        """No MSI installer to go wrong under Wine."""
        text = self.LIB.read_text()
        assert "embed-amd64.zip" in text
        assert "import site" in text, "site-packages must be switched on"
        assert "python-3.11.9-amd64.exe" not in text.split("ensure_wine_python()")[1]

    def test_windows_paths_are_converted_correctly(self):
        result = subprocess.run(
            ["bash", "-c",
             f'set -uo pipefail; source "{self.LIB}"; '
             f'WINE_PREFIX=/Users/me/MarketBot/wine; '
             f'to_windows_path "/Users/me/MarketBot/wine/drive_c/Program Files/MetaTrader 5/terminal64.exe"'],
            capture_output=True, text=True, env={**os.environ,
                                                 "MINTEL_HOME": "/tmp/x"})
        assert result.stdout == "C:\\Program Files\\MetaTrader 5\\terminal64.exe", \
            (result.stdout, result.stderr)


class TestWindowsPythonHasATimeZoneDatabase:
    """Run five on the real Mac got all the way to the bridge check and died
    with 'No time zone found with key Asia/Tokyo': Windows Python ships no
    tz database, so zoneinfo needs the tzdata package there.
    """

    LIB = ROOT / "deploy" / "mac" / "mintel_mac.sh"

    def test_tzdata_is_fetched_installed_and_checked(self):
        body = self.LIB.read_text().split("ensure_wine_python() {")[1]
        assert "pip download MetaTrader5 tzdata" in body
        assert "MetaTrader5 tzdata || true" in body
        assert body.count("ZoneInfo('Asia/Tokyo')") >= 2, \
            "both the skip-check and the final check must do a real lookup"

    def test_the_bridge_loads_with_no_system_tz_database(self, tmp_path):
        """Reproduces the Mac failure, then proves the tzdata package fixes it."""
        env = {**os.environ, "PYTHONTZPATH": "/nonexistent"}
        script = str(ROOT / "mintel" / "broker" / "bridge_server.py")
        r = subprocess.run([sys.executable, "-I", script, "--help"],
                           capture_output=True, text=True, timeout=60, env=env)
        if r.returncode == 0:
            pytest.skip("this Python has tzdata installed; cannot reproduce")
        assert "No time zone found" in r.stderr
        target = tmp_path / "site"
        dl = subprocess.run([sys.executable, "-m", "pip", "install", "--quiet",
                             "--no-deps", "--target", str(target), "tzdata"],
                            capture_output=True, text=True, timeout=300)
        if dl.returncode != 0:
            pytest.skip(f"could not fetch tzdata here: {dl.stderr[-200:]}")
        env["PYTHONPATH"] = str(target)
        r = subprocess.run([sys.executable, script, "--help"],
                           capture_output=True, text=True, timeout=60, env=env)
        assert r.returncode == 0, r.stderr
        assert "--backend" in r.stdout


class TestWaitingForWineNeverHangs:
    """Run seven on the real Mac sat forever after installing the packages:
    wine_wait ran `wineserver -w`, and with the user's MetaTrader open that
    server never exits."""

    LIB = ROOT / "deploy" / "mac" / "mintel_mac.sh"

    def _fake_wineserver(self, tmp_path):
        binpath = tmp_path / "wine" / "bin"
        binpath.mkdir(parents=True)
        ws = binpath / "wineserver"
        ws.write_text("#!/bin/sh\nsleep 300\n")
        ws.chmod(0o755)
        wine = binpath / "wine"
        wine.write_text("#!/bin/sh\ntrue\n")
        wine.chmod(0o755)
        return wine

    def _run(self, snippet, tmp_path, extra=None):
        env = {**os.environ, "MINTEL_HOME": str(tmp_path / "home"), **(extra or {})}
        return subprocess.run(
            ["bash", "-c", f'set -uo pipefail; source "{self.LIB}"; {snippet}'],
            capture_output=True, text=True, env=env, timeout=120)

    def test_bounded_even_if_the_server_never_exits(self, tmp_path):
        wine = self._fake_wineserver(tmp_path)
        t0 = time.monotonic()
        r = self._run(f'WINE_BIN="{wine}"; WINE_DIR="{wine.parent}"; wine_wait; echo "rc=$?"', tmp_path)
        assert "rc=0" in r.stdout, (r.stdout, r.stderr)
        assert time.monotonic() - t0 < 60
        exe = "terminal64" + ".exe"     # keep the literal out of this process's argv
        if subprocess.run(["pgrep", "-f", exe], capture_output=True).returncode == 0:
            pytest.skip("something on this box looks like a running terminal")
        assert time.monotonic() - t0 >= 25, "it should still give a normal Wine time to settle"

    def test_skipped_when_using_the_installed_metatrader(self, tmp_path):
        wine = self._fake_wineserver(tmp_path)
        t0 = time.monotonic()
        r = self._run(f'WINE_BIN="{wine}"; WINE_DIR="{wine.parent}"; USING_EXISTING_MT5=yes; '
                      'wine_wait; echo "rc=$?"', tmp_path)
        assert "rc=0" in r.stdout, (r.stdout, r.stderr)
        assert time.monotonic() - t0 < 10


class TestUsesTheMetaTraderAppAlreadyInstalled:
    """MetaQuotes' own MetaTrader 5 for Mac bundles a Wine that MetaTrader is
    tested against. When it is there, use it: no download, no installer, and
    none of the "debugger has been found" trouble newer Wine causes.
    """

    LIB = ROOT / "deploy" / "mac" / "mintel_mac.sh"

    def _layout(self, tmp_path, with_wine=True, with_terminal=True):
        app = tmp_path / "MetaTrader 5.app"
        binpath = app / "Contents" / "SharedSupport" / "wine" / "bin"
        binpath.mkdir(parents=True)
        if with_wine:
            for name in ("wine64", "wineserver"):
                exe = binpath / name
                exe.write_text("#!/bin/sh\necho mq-wine \"$@\"\n")
                exe.chmod(0o755)
        prefix = tmp_path / "net.metaquotes.wine.metatrader5"
        term = prefix / "drive_c" / "Program Files" / "MetaTrader 5"
        term.mkdir(parents=True)
        if with_terminal:
            (term / "terminal64.exe").write_bytes(b"MZ")
        return app, prefix

    def _run(self, snippet, tmp_path, app, prefix):
        env = dict(os.environ)
        env["MINTEL_HOME"] = str(tmp_path / "home")
        env["MINTEL_MT5_APP"] = str(app)
        env["MINTEL_MT5_PREFIX"] = str(prefix)
        return subprocess.run(
            ["bash", "-c", f'set -uo pipefail; source "{self.LIB}"; {snippet}'],
            capture_output=True, text=True, env=env, timeout=120)

    def test_an_installed_app_is_used_for_wine_prefix_and_terminal(self, tmp_path):
        app, prefix = self._layout(tmp_path)
        r = self._run('use_existing_mt5; echo "rc=$?"; echo "bin=$WINE_BIN"; '
                      'echo "prefix=$WINE_PREFIX"; echo "term=$MT5_TERMINAL"; '
                      'echo "ws=$(wine_tool wineserver)"; echo "using=$USING_EXISTING_MT5"',
                      tmp_path, app, prefix)
        assert "rc=0" in r.stdout, (r.stdout, r.stderr)
        assert f"bin={app}/Contents/SharedSupport/wine/bin/wine64" in r.stdout
        assert f"prefix={prefix}" in r.stdout
        assert "term=C:\\Program Files\\MetaTrader 5\\terminal64.exe" in r.stdout
        assert f"ws={app}/Contents/SharedSupport/wine/bin/wineserver" in r.stdout
        assert "using=yes" in r.stdout

    def test_ensure_wine_takes_that_route_and_drops_the_separate_wine(self, tmp_path):
        app, prefix = self._layout(tmp_path)
        home = tmp_path / "home"
        (home / "Wine Devel.app").mkdir(parents=True)
        (home / "wine-devel-11.17.tar.xz").write_bytes(b"x")
        (home / "wine" / "drive_c").mkdir(parents=True)
        r = self._run('ensure_wine; echo "rc=$?"; echo "prefix=$WINE_PREFIX"',
                      tmp_path, app, prefix)
        assert "rc=0" in r.stdout, (r.stdout, r.stderr)
        assert "Using the MetaTrader 5 app you already have" in r.stdout
        assert "Downloading Wine" not in r.stdout
        assert f"prefix={prefix}" in r.stdout
        assert not (home / "Wine Devel.app").exists()
        assert not (home / "wine-devel-11.17.tar.xz").exists()
        assert not (home / "wine").exists()

    def test_ensure_mt5_does_not_run_the_installer_then(self, tmp_path):
        app, prefix = self._layout(tmp_path)
        r = self._run('ensure_wine >/dev/null; ensure_mt5; echo "rc=$?"; echo "term=$MT5_TERMINAL"',
                      tmp_path, app, prefix)
        assert "rc=0" in r.stdout, (r.stdout, r.stderr)
        assert "already installed" in r.stdout
        assert "installer is about to open" not in r.stdout
        assert "term=C:\\Program Files\\MetaTrader 5\\terminal64.exe" in r.stdout

    def test_an_incomplete_app_is_not_used(self, tmp_path):
        app, prefix = self._layout(tmp_path, with_terminal=False)
        r = self._run('use_existing_mt5; echo "rc=$?"', tmp_path, app, prefix)
        assert "rc=1" in r.stdout
        app, prefix = self._layout(tmp_path / "b", with_wine=False)
        r = self._run('use_existing_mt5; echo "rc=$?"', tmp_path / "b", app, prefix)
        assert "rc=1" in r.stdout

    def test_the_real_paths_are_the_defaults(self):
        text = self.LIB.read_text()
        assert 'MT5_APP="${MINTEL_MT5_APP:-/Applications/MetaTrader 5.app}"' in text
        assert ('MT5_APP_PREFIX="${MINTEL_MT5_PREFIX:-$HOME/Library/Application Support/'
                'net.metaquotes.wine.metatrader5}"') in text
        body = text.split("ensure_wine() {")[1].split("\n}\n")[0]
        assert body.lstrip().startswith('step ')
        assert "use_existing_mt5" in body
        # the existing app's environment is never rebooted or re-registered
        assert "wineboot" not in body and "configure_prefix_for_mt5" not in body


class TestWineComesFromWineHQ:
    """Run four on the real Mac: Homebrew's wine-stable cask is disabled
    ("does not pass the macOS Gatekeeper check", 2026-09-01) and the gcenx tap
    no longer carries wine-crossover. The package Homebrew used to install is
    still published by WineHQ's macOS maintainer, so fetch that directly.
    """

    LIB = ROOT / "deploy" / "mac" / "mintel_mac.sh"

    def _code_lines(self):
        return [l for l in self.LIB.read_text().splitlines()
                if not l.strip().startswith("#")]

    def _run(self, snippet, tmp_path, extra_env=None):
        env = dict(os.environ)
        env["MINTEL_HOME"] = str(tmp_path)
        env.update(extra_env or {})
        return subprocess.run(
            ["bash", "-c", f'set -uo pipefail; source "{self.LIB}"; {snippet}'],
            capture_output=True, text=True, env=env, timeout=120)

    def test_the_package_is_pinned_by_version_and_checksum(self):
        text = self.LIB.read_text()
        # 11.17 devel, not 11.0 stable: MetaTrader refuses Wine 10.3-11.0
        # ("A debugger has been found running in your system").
        assert 'WINEHQ_VERSION="11.17"' in text
        assert 'WINEHQ_CHANNEL="devel"' in text
        assert 'WINEHQ_APP="Wine Devel.app"' in text
        import re
        m = re.search(r'WINEHQ_SHA256="\$\{MINTEL_WINEHQ_SHA256:-([0-9a-f]{64})\}"', text)
        assert m, "a full SHA-256 must be pinned"
        assert m.group(1) == "c2b3a8274dbc594deaa64e40469b607cbc4aa8ef5656dec4c5f6f3dac0da770c"
        assert ('WINEHQ_URL="${MINTEL_WINEHQ_URL:-https://github.com/Gcenx/macOS_Wine_builds/releases/'
                'download/${WINEHQ_VERSION}/wine-${WINEHQ_CHANNEL}-${WINEHQ_VERSION}-osx64.tar.xz}"') in text

    def test_no_homebrew_tap_is_relied_on(self):
        for line in self._code_lines():
            assert "gcenx" not in line, line
            assert "brew tap" not in line, line
            assert "brew trust" not in line, line

    def test_our_copy_is_preferred_over_applications(self):
        body = self.LIB.read_text().split("find_wine() {")[1].split("\n}\n")[0]
        assert body.index("$WINE_APP/Contents/Resources/wine/bin/wine") \
            < body.index("/Applications/Wine Stable.app")

    def test_sha256_helper(self, tmp_path):
        import hashlib
        f = tmp_path / "blob"
        f.write_bytes(b"hello wine\n" * 1000)
        r = self._run(f'sha256_of "{f}"', tmp_path)
        assert r.stdout.strip() == hashlib.sha256(f.read_bytes()).hexdigest(), r.stderr

    PLIST = ('<?xml version="1.0" encoding="UTF-8"?>\n<plist version="1.0"><dict>\n'
             '\t<key>CFBundleShortVersionString</key>\n\t<string>{v}</string>\n'
             '\t<key>LSMinimumSystemVersion</key>\n\t<string>10.15</string>\n'
             '</dict></plist>\n')

    def _fake_bundle(self, where, app="Wine Devel.app", version="11.17"):
        """An app laid out like WineHQ's: <app>/Contents/Resources/wine/bin/wine."""
        binpath = where / app / "Contents" / "Resources" / "wine" / "bin"
        binpath.mkdir(parents=True)
        (where / app / "Contents" / "Info.plist").write_text(self.PLIST.format(v=version))
        for name in ("wine", "wineboot", "wineserver", "winepath"):
            exe = binpath / name
            exe.write_text("#!/bin/sh\necho fake-wine \"$@\"\n")
            exe.chmod(0o755)
        return where / app

    def _fake_package(self, tmp_path, version="11.17"):
        import tarfile
        src = tmp_path / "src"
        self._fake_bundle(src, version=version)
        pkg = tmp_path / "pkg.tar.xz"
        with tarfile.open(pkg, "w:xz") as tf:
            tf.add(src / "Wine Devel.app", arcname="Wine Devel.app")
        return pkg

    def test_a_wrong_checksum_is_refused_and_the_file_removed(self, tmp_path):
        import hashlib
        pkg = self._fake_package(tmp_path)
        home = tmp_path / "home"
        home.mkdir()
        wrong = hashlib.sha256(b"not it").hexdigest()
        r = self._run('install_wine_from_winehq; echo "rc=$?"; '
                      'find_wine && echo "found=$WINE_BIN" || echo "found=none"',
                      home, {"MINTEL_WINEHQ_URL": f"file://{pkg}", "MINTEL_WINEHQ_SHA256": wrong})
        assert "rc=1" in r.stdout, (r.stdout, r.stderr)
        assert "did not match its checksum" in r.stdout
        assert not list(home.glob("wine-stable-*.tar.xz")), "a bad download must not linger"
        assert not (home / "Wine Devel.app").exists()
        assert "found=none" in r.stdout

    def test_a_good_package_is_unpacked_and_found(self, tmp_path):
        import hashlib
        pkg = self._fake_package(tmp_path)
        home = tmp_path / "home"
        home.mkdir()
        sha = hashlib.sha256(pkg.read_bytes()).hexdigest()
        r = self._run('install_wine_from_winehq; echo "rc=$?"; '
                      'find_wine && echo "found=$WINE_BIN"; '
                      'echo "dir=$WINE_DIR"; echo "ws=$(wine_tool wineserver)"',
                      home, {"MINTEL_WINEHQ_URL": f"file://{pkg}", "MINTEL_WINEHQ_SHA256": sha})
        assert "rc=0" in r.stdout, (r.stdout, r.stderr)
        assert "SHA-256 matches" in r.stdout
        wine = home / "Wine Devel.app" / "Contents" / "Resources" / "wine" / "bin" / "wine"
        assert wine.exists() and os.access(wine, os.X_OK)
        assert f"found={wine}" in r.stdout
        assert f"ws={wine.parent / 'wineserver'}" in r.stdout
        # second run: the verified download is kept, not fetched again
        r2 = self._run('install_wine_from_winehq; echo "rc=$?"', home,
                       {"MINTEL_WINEHQ_URL": "file:///nonexistent/should-not-be-fetched",
                        "MINTEL_WINEHQ_SHA256": sha})
        assert "rc=0" in r2.stdout, (r2.stdout, r2.stderr)

    def test_an_older_managed_wine_is_replaced(self, tmp_path):
        """The real Mac had 11.0_1 (stable) installed by the previous version of
        this installer; it must be swapped for the pinned build, not kept."""
        import hashlib
        pkg = self._fake_package(tmp_path)
        home = tmp_path / "home"
        home.mkdir()
        old = self._fake_bundle(home, app="Wine Stable.app", version="11.0_1")
        (home / "wine-stable-11.0_1.tar.xz").write_bytes(b"old download")
        sha = hashlib.sha256(pkg.read_bytes()).hexdigest()
        r = self._run('ensure_wine_package; echo "rc=$?"; echo "bin=$WINE_BIN"; '
                      'echo "ver=$(managed_wine_version)"',
                      home, {"MINTEL_WINEHQ_URL": f"file://{pkg}", "MINTEL_WINEHQ_SHA256": sha})
        assert "rc=0" in r.stdout, (r.stdout, r.stderr)
        assert "ver=11.17" in r.stdout
        new = home / "Wine Devel.app" / "Contents" / "Resources" / "wine" / "bin" / "wine"
        assert f"bin={new}" in r.stdout
        assert not old.exists(), "the old copy must not linger"
        assert not (home / "wine-stable-11.0_1.tar.xz").exists()

    def test_a_managed_wine_of_the_wrong_version_is_replaced_too(self, tmp_path):
        import hashlib
        pkg = self._fake_package(tmp_path)
        home = tmp_path / "home"
        home.mkdir()
        self._fake_bundle(home, version="11.6_1")
        sha = hashlib.sha256(pkg.read_bytes()).hexdigest()
        r = self._run('ensure_wine_package; echo "rc=$?"; echo "ver=$(managed_wine_version)"',
                      home, {"MINTEL_WINEHQ_URL": f"file://{pkg}", "MINTEL_WINEHQ_SHA256": sha})
        assert "rc=0" in r.stdout, (r.stdout, r.stderr)
        assert "Replacing it" in r.stdout
        assert "ver=11.17" in r.stdout

    def test_a_current_managed_wine_is_kept_without_downloading(self, tmp_path):
        home = tmp_path / "home"
        home.mkdir()
        self._fake_bundle(home)
        r = self._run('ensure_wine_package; echo "rc=$?"; echo "bin=$WINE_BIN"', home,
                      {"MINTEL_WINEHQ_URL": "file:///nonexistent/must-not-be-fetched",
                       "MINTEL_WINEHQ_SHA256": "0" * 64})
        assert "rc=0" in r.stdout, (r.stdout, r.stderr)
        assert "Downloading" not in r.stdout
        assert "Wine Devel.app/Contents/Resources/wine/bin/wine" in r.stdout

    def test_the_prefix_is_updated_when_wine_changes_and_mt5_settings_applied(self):
        text = self.LIB.read_text()
        body = text.split("ensure_wine_standalone() {")[1].split("\n}\n")[0]
        assert ".mintel-wine-version" in body
        assert '"$WINE_BIN" wineboot --update' in body
        assert body.rstrip().endswith("configure_prefix_for_mt5"), \
            "registry settings go last, after any wineboot rewrote the defaults"
        cfg = text.split("configure_prefix_for_mt5() {")[1].split("\n}\n")[0]
        assert "/d win11" in cfg
        assert "AeDebug' /v Debugger /f" in cfg

    def test_the_download_step_is_visible_and_retried(self):
        body = self.LIB.read_text().split("install_wine_from_winehq() {")[1].split("\n}\n")[0]
        assert 'run_logged "Downloading Wine' in body
        assert "--retry 3" in body
        assert 'run_logged "Unpacking Wine' in body
        assert "xattr -dr com.apple.quarantine" in body


class TestWineInstallOnCurrentHomebrew:
    """The third real-Mac run failed three times in a row on one thing:
    `brew install --cask --no-quarantine` — current Homebrew rejects the flag
    ("Error: invalid option: --no-quarantine") before downloading anything.
    """

    LIB = ROOT / "deploy" / "mac" / "mintel_mac.sh"

    def _code_lines(self):
        return [l for l in self.LIB.read_text().splitlines()
                if not l.strip().startswith("#")]

    def test_no_quarantine_flag_is_never_passed_to_brew(self):
        for line in self._code_lines():
            assert "--no-quarantine" not in line, line

    def test_quarantine_is_cleared_with_xattr_instead(self):
        text = self.LIB.read_text()
        assert "xattr -dr com.apple.quarantine" in text
        body = text.split("ensure_wine_standalone() {")[1].split("\n}\n")[0]
        assert "unquarantine_wine" in body, \
            "the standalone route must clear quarantine after installing"

    def test_windows_wheels_are_fetched_by_the_native_python(self):
        """Nothing for Wine to download: a Wine TLS quirk cannot stall setup."""
        body = self.LIB.read_text().split("ensure_wine_python() {")[1]
        assert "pip download MetaTrader5" in body
        assert "--platform win_amd64" in body
        assert "--only-binary=:all:" in body
        assert "--no-index --find-links" in body

    def test_a_wheel_can_be_unpacked_without_pip(self):
        body = self.LIB.read_text().split("ensure_wine_python() {")[1]
        assert 'unzip -q -o "$whl" -d "$WINE_PY_DIR/Lib/site-packages"' in body

    def test_site_packages_is_named_in_the_pth_file(self):
        body = self.LIB.read_text().split("ensure_wine_python() {")[1]
        assert "Lib\\\\site-packages" in body

    def test_the_bridge_is_proven_to_load_before_config_is_written(self):
        text = self.LIB.read_text()
        body = text.split("ensure_wine_python() {")[1].split("\nto_z_path() {")[0]
        assert "bridge_server.py" in body and "--help" in body
        # and that step is visible + logged, never silenced
        for line in body.splitlines():
            if "bridge_server.py" in line and not line.strip().startswith("#"):
                assert ">/dev/null" not in line

    def test_wine_binary_is_recorded_for_launchd(self):
        text = self.LIB.read_text()
        assert '"wine_binary": os.environ.get("MINTEL_WINEBIN", "")' in text
        assert text.count('MINTEL_WINEBIN="$WINE_BIN"') == 2, \
            "both the first write and the refresh must carry it"
        assert '("wine_binary", "MINTEL_WINEBIN")' in text
        assert "<key>WINEDLLOVERRIDES</key><string>mscoree,mshtml=</string>" in text

    def test_z_paths_are_converted_correctly(self):
        result = subprocess.run(
            ["bash", "-c",
             f'set -uo pipefail; source "{self.LIB}"; '
             f'to_z_path "/Users/me/MarketBot/app/mintel/broker/bridge_server.py"'],
            capture_output=True, text=True, env={**os.environ,
                                                 "MINTEL_HOME": "/tmp/x"})
        assert result.stdout == \
            "Z:\\Users\\me\\MarketBot\\app\\mintel\\broker\\bridge_server.py", \
            (result.stdout, result.stderr)


class TestWatchdogLaunchesTheBridgeUnderLaunchd:
    """launchd starts the watchdog with a bare PATH, and the embeddable Windows
    Python ignores PYTHONPATH and the working directory. Both would have
    broken the very first start after a clean install.
    """

    def _cfg(self, tmp_path):
        from mintel.config import Config
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path)
        cfg.broker_mode = "bridge"
        cfg.wine_python = "C:\\Python311\\python.exe"
        cfg.wine_prefix = str(tmp_path / "wine")
        cfg.account_login = 12345
        cfg.account_server = "Broker-Demo"
        cfg.mt5_terminal_path = "C:\\MT5\\terminal64.exe"
        return cfg

    def test_windows_path_helper(self):
        from mintel.ops.watchdog import windows_path
        assert windows_path("/Users/a b/MarketBot/app/x.py") == \
            "Z:\\Users\\a b\\MarketBot\\app\\x.py"
        assert windows_path("C:\\Python311\\python.exe") == \
            "C:\\Python311\\python.exe"
        assert windows_path("D:/x/y.exe") == "D:/x/y.exe"

    def test_configured_wine_binary_wins_over_path_lookup(self, tmp_path):
        from mintel.ops.watchdog import build_default
        cfg = self._cfg(tmp_path)
        cfg.wine_binary = "/Applications/Wine Stable.app/Contents/Resources/wine/bin/wine"
        wd = build_default(cfg, str(tmp_path / "c.json"), project_dir="/Users/me/MarketBot/app")
        bridge = next(p for p in wd.processes if p.name == "bridge")
        assert bridge.command[0] == cfg.wine_binary
        assert wd.mt5_command[0] == cfg.wine_binary
        assert wd.mt5_command[1] == cfg.mt5_terminal_path

    def test_bridge_is_started_by_script_path_not_dash_m(self, tmp_path):
        from mintel.ops.watchdog import build_default
        cfg = self._cfg(tmp_path)
        cfg.wine_binary = "/opt/homebrew/bin/wine"
        wd = build_default(cfg, str(tmp_path / "c.json"), project_dir="/Users/me/MarketBot/app")
        bridge = next(p for p in wd.processes if p.name == "bridge")
        assert bridge.command[1] == cfg.wine_python
        assert bridge.command[2] == \
            "Z:\\Users\\me\\MarketBot\\app\\mintel\\broker\\bridge_server.py"
        assert "-m" not in bridge.command
        assert "--secrets-file" in bridge.command

    def test_no_gecko_dialog_can_block_a_launchd_start(self, tmp_path):
        from mintel.ops.watchdog import wine_env
        env = wine_env(self._cfg(tmp_path))
        assert env["WINEDLLOVERRIDES"] == "mscoree,mshtml="
        assert env["WINEDEBUG"] == "-all"
        assert env["WINEPREFIX"] == str(tmp_path / "wine")

    def test_the_bridge_script_adds_its_own_project_root(self, tmp_path):
        """What makes running it by file path work at all."""
        import shutil
        # copy the package to a fresh place so nothing on sys.path helps it
        dst = tmp_path / "somewhere" / "app"
        shutil.copytree(ROOT / "mintel", dst / "mintel",
                        ignore=shutil.ignore_patterns("__pycache__"))
        r = subprocess.run(
            [sys.executable, "-P", "-I", str(dst / "mintel" / "broker" / "bridge_server.py"), "--help"],
            capture_output=True, text=True, timeout=60, cwd=str(tmp_path))
        assert r.returncode == 0, r.stderr
        assert "--backend" in r.stdout


class TestLoggedRunner:
    LIB = ROOT / "deploy" / "mac" / "mintel_mac.sh"

    def _run(self, snippet, tmp_path):
        env = dict(os.environ)
        env["MINTEL_HOME"] = str(tmp_path)
        return subprocess.run(
            ["bash", "-c", f'set -uo pipefail; source "{self.LIB}"; {snippet}'],
            capture_output=True, text=True, env=env, timeout=60)

    def test_success_is_quiet_and_returns_zero(self, tmp_path):
        r = self._run('run_logged "a step" true; echo "rc=$?"', tmp_path)
        assert "rc=0" in r.stdout
        assert "failed" not in r.stdout

    def test_failure_shows_the_output_and_where_the_log_is(self, tmp_path):
        r = self._run('run_logged "a step" bash -c "echo the-real-reason; exit 3"; '
                      'echo "rc=$?"', tmp_path)
        assert "rc=3" in r.stdout, "the command's own exit status must survive"
        assert "the-real-reason" in r.stdout, "the output must be visible live"
        assert "failed (exit 3)" in r.stdout
        assert "setup.log" in r.stdout
        log = tmp_path / "logs" / "setup.log"
        assert log.exists()
        assert "the-real-reason" in log.read_text()
        assert "===== a step =====" in log.read_text()

    def test_output_is_appended_across_steps(self, tmp_path):
        self._run('run_logged "one" echo first; run_logged "two" echo second',
                  tmp_path)
        text = (tmp_path / "logs" / "setup.log").read_text()
        assert "first" in text and "second" in text

    def test_resolve_path_follows_relative_symlink_chains(self, tmp_path):
        real = tmp_path / "bin" / "wine-real"
        real.parent.mkdir()
        real.write_text("#!/bin/sh\n")
        (tmp_path / "bin" / "wine").symlink_to("wine-real")
        (tmp_path / "wine-link").symlink_to("bin/wine")
        r = self._run(f'resolve_path "{tmp_path / "wine-link"}"', tmp_path)
        assert r.stdout == str(real), (r.stdout, r.stderr)

    def test_find_wine_locates_a_bundle_binary_and_its_real_directory(self, tmp_path):
        bindir = tmp_path / "Wine Stable.app" / "Contents" / "Resources" / "wine" / "bin"
        bindir.mkdir(parents=True)
        (bindir / "wine").write_text("#!/bin/sh\n")
        (bindir / "wine").chmod(0o755)
        (bindir / "wineserver").write_text("#!/bin/sh\n")
        (bindir / "wineserver").chmod(0o755)
        link = tmp_path / "wine"
        link.symlink_to(bindir / "wine")
        # Put the symlink first on PATH so find_wine takes it, then check it
        # resolves WINE_DIR to the bundle's real bin directory.
        r = self._run(
            f'PATH="{tmp_path}:$PATH"; find_wine; echo "BIN=$WINE_BIN"; '
            f'echo "DIR=$WINE_DIR"; echo "WS=$(wine_tool wineserver)"',
            tmp_path)
        assert f"BIN={link}" in r.stdout, r.stdout
        assert f"DIR={bindir}" in r.stdout, r.stdout
        assert f"WS={bindir / 'wineserver'}" in r.stdout, r.stdout

    def test_find_wine_fails_cleanly_when_absent(self, tmp_path):
        r = self._run('PATH=/nonexistent; if find_wine; then echo found; '
                      'else echo absent; fi', tmp_path)
        assert "absent" in r.stdout
        assert "unbound variable" not in r.stderr


class TestFinalReportHonesty:
    LIB = ROOT / "deploy" / "mac" / "mintel_mac.sh"

    def _run(self, snippet, tmp_path):
        env = dict(os.environ)
        env["MINTEL_HOME"] = str(tmp_path)
        return subprocess.run(
            ["bash", "-c", f'set -uo pipefail; source "{self.LIB}"; {snippet}'],
            capture_output=True, text=True, env=env, timeout=60)

    def test_a_failed_install_does_not_claim_to_be_running(self, tmp_path):
        r = self._run('bad "Wine could not be installed"; final_report; '
                      'echo "rc=$?"', tmp_path)
        assert "rc=1" in r.stdout
        assert "Nothing has been started" in r.stdout
        assert "starts again by itself" not in r.stdout
        assert "Status page" not in r.stdout
        assert "Wine could not be installed" in r.stdout

    def test_a_clean_install_reports_running(self, tmp_path):
        r = self._run('final_report; echo "rc=$?"', tmp_path)
        assert "rc=0" in r.stdout
        assert "THE BOT IS RUNNING" in r.stdout
        assert "starts again by itself" in r.stdout
        assert "close the lid" in r.stdout
