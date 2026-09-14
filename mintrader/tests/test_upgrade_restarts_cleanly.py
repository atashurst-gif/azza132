"""Found on the real Mac: "Stop" left the Wine-side bridge running (no pid
file for it, and the watchdog died without taking its children), and a
re-install with everything still running kept the OLD code in memory. Now
the watchdog records every child's pid and stops them on shutdown, Stop
kills by name as well, and the launcher restarts when the code changed."""
from __future__ import annotations

import datetime as dt
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIB = ROOT / "deploy" / "mac" / "mintel_mac.sh"


def _bash(snippet, home):
    env = {**os.environ, "MINTEL_HOME": str(home)}
    return subprocess.run(["bash", "-c", f'set -uo pipefail; source "{LIB}"; {snippet}'],
                          capture_output=True, text=True, env=env, timeout=120)


class TestCodeFingerprint:
    def test_changed_code_is_detected(self, tmp_path):
        src = tmp_path / "src"
        (src / "mintel").mkdir(parents=True)
        (src / "mintel" / "a.py").write_text("x = 1\n")
        home = tmp_path / "home"
        (home / "data").mkdir(parents=True)
        r = _bash(f'source_changed "{src}" && echo first=changed || echo first=same; '
                  f'code_hash "{src}" > "$DATA_DIR/app.version"; '
                  f'source_changed "{src}" && echo second=changed || echo second=same', home)
        assert "first=changed" in r.stdout, (r.stdout, r.stderr)
        assert "second=same" in r.stdout
        (src / "mintel" / "a.py").write_text("x = 2\n")
        r = _bash(f'source_changed "{src}" && echo third=changed || echo third=same', home)
        assert "third=changed" in r.stdout

    def test_hash_is_stable(self, tmp_path):
        src = tmp_path / "src"
        (src / "mintel").mkdir(parents=True)
        (src / "mintel" / "a.py").write_text("x = 1\n")
        home = tmp_path / "home"
        r = _bash(f'code_hash "{src}"; code_hash "{src}"', home)
        a, b = r.stdout.split()
        assert a == b and len(a) == 64


class TestStopAndStartMechanics:
    def test_stop_kills_the_bridge_by_name_too(self):
        body = LIB.read_text().split("stop_everything() {")[1].split("\n}\n")[0]
        assert 'pkill -f "mintel/broker/bridge_server.py"' in body
        assert 'pkill -f "mintel.run --config"' in body

    def test_new_code_restarts_a_running_bot(self):
        body = LIB.read_text().split("start_everything() {")[1].split("\n}\n")[0]
        assert 'is_running watchdog && [[ -n "$CODE_CHANGED" ]]' in body
        assert "A new version was installed" in body
        assert 'pkill -f "mintel/broker/bridge_server.py"' in body

    def test_launcher_fast_path_checks_the_code(self):
        text = (ROOT / "deploy" / "mac" / "Start Trading Bot.command").read_text()
        assert 'is_running watchdog && is_running trader && ! source_changed "$SOURCE_DIR"' in text

    def test_copy_program_records_the_version(self):
        body = LIB.read_text().split("copy_program() {")[1].split("\n}\n")[0]
        assert 'code_hash "$source_dir" > "$DATA_DIR/app.version"' in body
        assert 'CODE_CHANGED="yes"' in body


class TestWatchdogOwnsItsChildren:
    def test_pid_file_is_written_for_a_child_that_does_not_write_its_own(self, tmp_path):
        from mintel.ops.watchdog import ManagedProcess
        pidf = tmp_path / "bridge.pid"
        mp = ManagedProcess(name="bridge", command=[sys.executable, "-c", "import time; time.sleep(30)"],
                            pid_file=str(pidf), log_file=str(tmp_path / "b.log"), use_pty=True)
        ok, msg = mp.start(dt.datetime.now(dt.timezone.utc))
        assert ok, msg
        assert int(pidf.read_text()) == mp.proc.pid
        mp.stop(); mp._close_log()

    def test_stopping_the_watchdog_stops_the_children(self, tmp_path):
        from mintel.config import Config
        from mintel.ops.watchdog import ManagedProcess, Watchdog
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path)
        cfg.ops.log_dir = str(tmp_path)
        mp = ManagedProcess(name="child", command=[sys.executable, "-c", "import time; time.sleep(60)"],
                            log_file=str(tmp_path / "c.log"))
        mp.start(dt.datetime.now(dt.timezone.utc))
        assert mp.proc.poll() is None
        wd = Watchdog(cfg, [mp], mt5_command=[])
        wd.stop()
        wd.run()
        deadline = time.monotonic() + 20
        while mp.proc.poll() is None and time.monotonic() < deadline:
            time.sleep(0.2)
        assert mp.proc.poll() is not None, "child must be stopped with the watchdog"
        mp._close_log()
