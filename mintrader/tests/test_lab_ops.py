"""The lab's wiring on the Mac: the watchdog, the pulse and the installer.

* the watchdog looks after ONE more process, ``lab`` (the practice process),
  judged by its status file (probed as the bridge is), never by a heartbeat;
  only while data/lab.json does not say OFF and the lab has chosen its markets;
* the pulse publishes reports/live/lab.json (whitelisted, scrubbed, <= 20 KB)
  once the lab has run;
* the installer (bash 3.2): one-time set-up with its marker written only on
  success, the LaunchAgent at 23:10 Monday to Saturday under nice -n 19 with
  one worker, the icon placed once, a missed night started only between 23:10
  and 06:00, a stop line, a health line, the call lines in their place.

EVERY FIGURE HERE IS MADE-UP TEST DATA (fake launchctl, fake Python, made-up files).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import pytest

from mintel.config import Config
from mintel.ops import pulse as PL
from mintel.ops.watchdog import ManagedProcess, Watchdog, build_default

ROOT = Path(__file__).resolve().parents[1]
MAC = ROOT / "deploy" / "mac"
LIB = MAC / "mintel_mac.sh"
UTC = dt.timezone.utc


def _cfg(tmp_path) -> Config:
    c = Config()
    c.ops.data_dir = str(tmp_path / "data")
    c.ops.log_dir = str(tmp_path / "logs")
    return c


# ---------------------------------------------------------------- watchdog ---
def test_the_watchdog_looks_after_the_lab_by_its_status_file(tmp_path):
    cfg = _cfg(tmp_path)
    wd = build_default(cfg, str(tmp_path / "data" / "config.json"), project_dir=str(ROOT))
    names = [p.name for p in wd.processes]
    assert names.index("lab") == names.index("ian") + 1
    lab = wd.processes[names.index("lab")]
    assert lab.command[1:4] == ["-m", "mintel.lab.practice", "--config"]
    assert lab.heartbeat == ""                                        # NO heartbeat
    assert lab.pid_file.endswith("data/lab.pid") and lab.log_file.endswith("logs/lab.out.log")
    assert lab.grace_seconds == 120.0 and lab.kill_pattern == "mintel.lab.practice"
    assert lab.max_restarts_per_hour == cfg.ops.max_restarts_per_hour
    data = Path(cfg.ops.data_dir)
    # enabled only once the lab has chosen its markets, and while it is not OFF
    assert lab.is_enabled() is False
    (data / "lab").mkdir(parents=True)
    (data / "lab" / "universe.json").write_text("{}")
    assert lab.is_enabled() is True
    (data / "lab.json").write_text(json.dumps({"mode": "OFF"}))
    assert lab.is_enabled() is False
    (data / "lab.json").write_text(json.dumps({"mode": "PAPER"}))
    assert lab.is_enabled() is True
    # the probe: the status file's own stamp, at most 3 minutes old
    assert lab.probe() is False
    (data / "lab-status.json").write_text(json.dumps({"updated_utc": dt.datetime.now(UTC).isoformat()}))
    assert lab.probe() is True
    old = dt.datetime.now(UTC) - dt.timedelta(seconds=400)
    (data / "lab-status.json").write_text(json.dumps({"updated_utc": old.isoformat()}))
    assert lab.probe() is False


def test_a_project_without_the_lab_schedules_no_lab(tmp_path):
    wd = build_default(_cfg(tmp_path), str(tmp_path / "c.json"), project_dir=str(tmp_path))
    assert "lab" not in [p.name for p in wd.processes]


def test_the_watchdog_restarts_a_silent_practice_process_and_leaves_a_healthy_one(tmp_path):
    cfg = _cfg(tmp_path)
    status = tmp_path / "data" / "lab-status.json"
    status.parent.mkdir(parents=True)
    script = tmp_path / "practice.py"
    script.write_text(
        "import datetime as dt, json, sys, time\n"
        "n = int(sys.argv[1])\n"
        "for i in range(n):\n"
        f"    open({str(status)!r}, 'w').write(json.dumps({{'updated_utc': dt.datetime.now(dt.timezone.utc).isoformat()}}))\n"
        "    time.sleep(0.05)\n"
        "time.sleep(30)\n")
    from mintel.lab.status import status_fresh
    mp = ManagedProcess("lab", [sys.executable, str(script), "40"], pid_file=str(tmp_path / "data" / "lab.pid"),
                        grace_seconds=0.05, probe=status_fresh(status, 1.0))
    wd = Watchdog(cfg, [mp])
    try:
        actions = wd.check_once()
        assert any("lab: it is not answering - restarting" in a for a in actions)
        time.sleep(0.5)
        assert wd.check_once() == [], "a practice process that writes its status is left alone"
        time.sleep(3.0)                                               # it stopped writing: silent
        actions = wd.check_once()
        assert any("lab: it is not answering - restarting" in a for a in actions)
        assert not (tmp_path / "data" / "heartbeats" / "lab.heartbeat.json").exists()
    finally:
        mp.stop()


# ------------------------------------------------------------------- pulse ---
def test_the_pulse_publishes_the_lab_only_once_it_has_run(tmp_path):
    cfg = _cfg(tmp_path)
    assert PL.lab_json(cfg) is None
    files = PL.pulse_files({"ts_utc": "x"}, ["a"], bots="{}")
    assert PL.LAB_PATH not in files
    data = Path(cfg.ops.data_dir)
    (data / "lab").mkdir(parents=True)
    summary = {"schema": 1, "synthetic": True, "funnel": {"ideas": 632}, "secret_token": "ghp_abcdefghijklmnop",
               "candidates": [{"id": f"lab-{k:010d}", "name": "made up " * 20, "stage": "PRACTICE",
                               "words": ["w" * 200] * 6, "story": [{"why": "s" * 300}] * 8,
                               "headline": "MADE UP login=hunter2", "api_key": "x",
                               "checks": [{"key": "luck", "text": "c" * 700}] * 5} for k in range(60)],
               "binned_recent": [{"id": "x", "reason": "r" * 500}] * 20, "unlisted": "dropped"}
    (data / "lab" / "summary.json").write_text(json.dumps(summary))
    (data / "lab-status.json").write_text(json.dumps({"updated_utc": "now", "feed_ok": True, "token": "t",
                                                      "roster": [], "anything": 1}))
    text = PL.lab_json(cfg)
    assert len(text.encode()) <= PL.LAB_MAX_BYTES == 20_000
    rep = json.loads(text)
    assert rep["summary"]["synthetic"] is True and rep["summary"]["funnel"] == {"ideas": 632}
    assert "secret_token" not in rep["summary"] and "unlisted" not in rep["summary"]
    assert "binned_recent" not in rep["summary"] and rep.get("trimmed")       # trimmed first
    assert len(rep["summary"]["candidates"]) == 12 and rep["summary"]["candidates_left_out"] == 48
    assert all("words" not in c and "story" not in c for c in rep["summary"]["candidates"])
    assert all("api_key" not in c for c in rep["summary"]["candidates"])
    assert "hunter2" not in text and "ghp_" not in text
    assert rep["status"] == {"updated_utc": "now", "feed_ok": True, "roster": []}
    files = PL.pulse_files({"ts_utc": "x"}, ["a"], bots="{}", lab=text)
    assert files[PL.LAB_PATH] == text and PL.LAB_PATH == "reports/live/lab.json"


def test_a_broken_summary_is_said_not_guessed(tmp_path):
    cfg = _cfg(tmp_path)
    (Path(cfg.ops.data_dir) / "lab").mkdir(parents=True)
    (Path(cfg.ops.data_dir) / "lab" / "summary.json").write_text("{broken")
    rep = json.loads(PL.lab_json(cfg))
    assert "not valid JSON" in rep["summary_unavailable"] and "summary" not in rep


def test_pulse_main_publishes_lab_json(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.save(tmp_path / "data" / "config.json")
    (tmp_path / "data" / "lab").mkdir(parents=True)
    (tmp_path / "data" / "lab" / "summary.json").write_text(json.dumps({"schema": 1, "synthetic": True}))
    import mintel.ops.report_upload as RU
    sent = {}
    monkeypatch.setattr(RU, "read_token", lambda p: "made-up")
    monkeypatch.setattr(RU, "upload", lambda files, *a, **k: sent.update(files) or len(files))
    monkeypatch.setattr(PL, "take_pulse", lambda cfg, now=None: {"ts_utc": "2026-10-12T22:00:00Z", "up": True,
                                                                  "why": "", "status": {}})
    monkeypatch.setattr(PL, "pulse_line", lambda p: "2026-10-12T22:00:00Z UP made-up")
    PL.main(["--config", str(tmp_path / "data" / "config.json")])
    assert json.loads(sent[PL.LAB_PATH])["summary"]["synthetic"] is True


# --------------------------------------------------------------- installer ---
def _fake_home(tmp_path, *, launchctl_ok=True, due=True):
    """A MADE-UP MarketBot: a fake launchctl and a fake Python that log what they are
    asked (the Python answers the lab's "due" question and nothing starts)."""
    home = tmp_path / "MarketBot"
    (home / "data").mkdir(parents=True)
    (home / "logs").mkdir()
    (home / "venv" / "bin").mkdir(parents=True)
    (home / "app").symlink_to(ROOT)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "calls.log"
    (bin_dir / "launchctl").write_text(f'#!/bin/bash\necho "launchctl $*" >> "{calls}"\n'
                                       f'[[ "$1" == "load" ]] && exit {0 if launchctl_ok else 1}\nexit 0\n')
    (bin_dir / "launchctl").chmod(0o755)
    py = home / "venv" / "bin" / "python"
    py.write_text("#!/bin/bash\n"
                  f'echo "python $*" >> "{calls}"\n'
                  'if [[ "${1:-}" == "-m" && "${2:-}" == "mintel.lab" && "${3:-}" == "due" ]]; then\n'
                  + ('  echo "due 2026-10-12"; exit 0\n' if due else '  echo "not due 2026-10-12"; exit 1\n') +
                  "fi\n"
                  'if [[ "${1:-}" == "-m" && "${2:-}" == "mintel.lab" && "${3:-}" == "show" ]]; then\n'
                  '  echo "The lab: TEST DATA - not a result."; exit 0\n'
                  "fi\n"
                  'if [[ "${1:-}" == "-m" ]]; then exit 0; fi\n'
                  f'exec "{sys.executable}" "$@"\n')
    py.chmod(0o755)
    desk = tmp_path / "Desktop"
    desk.mkdir()
    return home, calls, bin_dir


def _bash(snippet, home, bin_dir, tmp_path, **env):
    full = {**os.environ, "MINTEL_HOME": str(home), "HOME": str(tmp_path),
            "PATH": f"{bin_dir}:{os.environ['PATH']}", **env}
    return subprocess.run(["bash", "-c", f'set -uo pipefail; source "{LIB}"; {snippet}'],
                          capture_output=True, text=True, env=full, timeout=120)


def _calls(path):
    return path.read_text().splitlines() if path.exists() else []


STEP = 'apply_lab_2026_10_11; echo "CODE=${CODE_CHANGED:-none}"'


def test_the_lab_is_set_up_once_with_its_nightly_agent(tmp_path):
    home, calls, bin_dir = _fake_home(tmp_path)
    r = _bash(STEP, home, bin_dir, tmp_path)
    assert r.returncode == 0, r.stderr
    data = home / "data"
    assert (data / ".lab-2026-10-11").exists() and (data / "lab").is_dir()
    assert json.loads((data / "lab.json").read_text()) == {"mode": "PAPER"}
    assert (data / ".restart-pending").exists() and "CODE=yes" in r.stdout
    assert "The strategy lab is set up" in r.stdout and "[WARN]" not in r.stdout
    plist = (tmp_path / "Library" / "LaunchAgents" / "com.mintel.lab.plist").read_text()
    args = re.findall(r"<string>([^<]*)</string>", plist.split("<key>ProgramArguments</key>")[1].split("</array>")[0])
    assert args == ["/usr/bin/nice", "-n", "19", f"{home}/venv/bin/python", "-m", "mintel.lab.nightly",
                    "--config", f"{data}/config.json", "--upload", "--workers", "1"]
    days = re.findall(r"<key>Weekday</key><integer>(\d)</integer><key>Hour</key><integer>23</integer>"
                      r"<key>Minute</key><integer>10</integer>", plist)
    assert days == ["1", "2", "3", "4", "5", "6"]
    assert f"<string>{home}/logs/lab-nightly.log</string>" in plist
    assert "KeepAlive" not in plist and "RunAtLoad" not in plist
    assert any(c.startswith("launchctl load") and "com.mintel.lab.plist" in c for c in _calls(calls))
    # once only
    again = _bash(STEP, home, bin_dir, tmp_path)
    assert again.stdout.strip() == "CODE=none" and len(_calls(calls)) == 2


def test_the_marker_is_written_only_on_success_and_a_settings_file_is_never_overwritten(tmp_path):
    home, calls, bin_dir = _fake_home(tmp_path, launchctl_ok=False)
    (home / "data" / "lab.json").write_text(json.dumps({"mode": "OFF", "practice_slots": 4}))
    r = _bash(STEP, home, bin_dir, tmp_path)
    assert r.returncode == 0, r.stderr
    assert not (home / "data" / ".lab-2026-10-11").exists() and "CODE=none" in r.stdout
    assert "[WARN]" in r.stdout and "the next double-click tries again" in r.stdout
    assert json.loads((home / "data" / "lab.json").read_text()) == {"mode": "OFF", "practice_slots": 4}
    # macOS takes it next time: now it is set
    (bin_dir / "launchctl").write_text(f'#!/bin/bash\necho "launchctl $*" >> "{calls}"\nexit 0\n')
    r = _bash(STEP, home, bin_dir, tmp_path)
    assert (home / "data" / ".lab-2026-10-11").exists() and "CODE=yes" in r.stdout
    assert json.loads((home / "data" / "lab.json").read_text())["mode"] == "OFF"


def test_the_icon_is_placed_once(tmp_path):
    home, calls, bin_dir = _fake_home(tmp_path)
    r = _bash("install_lab_icon", home, bin_dir, tmp_path)
    assert r.returncode == 0, r.stderr
    icon = tmp_path / "Desktop" / "The Lab.command"
    assert icon.exists() and os.access(icon, os.X_OK) and (home / "data" / ".desktop-icon-lab-placed").exists()
    icon.unlink()                                                     # Aaron moved it away
    _bash("install_lab_icon", home, bin_dir, tmp_path)
    assert not icon.exists()                                         # never put back


@pytest.mark.parametrize("hhmm,started", [("2309", False), ("2310", True), ("2359", True), ("0030", True),
                                          ("0559", True), ("0600", False), ("1500", False)])
def test_a_missed_night_starts_only_between_23_10_and_06_00(tmp_path, hhmm, started):
    home, calls, bin_dir = _fake_home(tmp_path)
    (home / "data" / ".lab-2026-10-11").write_text("x")
    r = _bash("start_lab_nightly_if_missed", home, bin_dir, tmp_path, MINTEL_LAB_HHMM=hhmm)
    assert r.returncode == 0, r.stderr
    time.sleep(0.3)
    nightly = [c for c in _calls(calls) if "mintel.lab.nightly" in c]
    if started:
        assert nightly == [f"python -m mintel.lab.nightly --config {home}/data/config.json --upload --workers 1"]
        assert "started it in the background (nice -n 19)" in r.stdout
    else:
        assert nightly == [] and not any("due" in c for c in _calls(calls))


def test_a_night_already_done_or_running_is_not_started_again(tmp_path):
    home, calls, bin_dir = _fake_home(tmp_path, due=False)
    (home / "data" / ".lab-2026-10-11").write_text("x")
    r = _bash("start_lab_nightly_if_missed", home, bin_dir, tmp_path, MINTEL_LAB_HHMM="2330")
    assert r.returncode == 0 and not [c for c in _calls(calls) if "mintel.lab.nightly" in c]
    # and never before the lab is set up
    home2, calls2, bin2 = _fake_home(tmp_path / "x")
    _bash("start_lab_nightly_if_missed", home2, bin2, tmp_path / "x", MINTEL_LAB_HHMM="2330")
    assert _calls(calls2) == []


def test_the_due_answer_comes_from_the_lab_itself(tmp_path):
    from mintel.lab.__main__ import main
    cfg = _cfg(tmp_path)
    cfg.save(tmp_path / "data" / "config.json")
    assert main(["due", "--config", str(tmp_path / "data" / "config.json")]) == 0
    from mintel.lab import LabPaths
    from mintel.lab.stages import night_of
    p = LabPaths(tmp_path / "data")
    p.run_dir(night_of(dt.datetime.now(UTC))).mkdir(parents=True)
    (p.run_dir(night_of(dt.datetime.now(UTC))) / "done").write_text("x")
    assert main(["due", "--config", str(tmp_path / "data" / "config.json")]) == 1


def test_stop_ends_practice_but_leaves_the_nightly_agent(tmp_path):
    lib = LIB.read_text()
    stop = lib.split("stop_everything() {")[1].split("\n}\n")[0]
    assert 'pkill -f "mintel.lab.practice"' in stop and 'rm -f "$DATA_DIR/lab.pid"' in stop
    assert "com.mintel.lab" not in stop
    health = lib.split("show_health() {")[1].split("\n}\n")[0]
    assert "-m mintel.lab show --one-line" in health


def test_the_launcher_calls_the_lab_steps_in_their_place():
    text = (MAC / "Start Trading Bot.command").read_text()
    body = text.split("write_config         ||")[1]
    order = ["\napply_formula1_choices", "\napply_lab_2026_10_11", "\ninstall_lab_icon", "\nremember_restart",
             "\ninstall_launch_agent", "\nstart_everything", "\nshow_health", "\nstart_lab_nightly_if_missed"]
    pos = [body.index(x) for x in order]
    assert pos == sorted(pos)
    fast = text.split("# ---- Fast path")[1].split("# ---- Otherwise")[0]
    assert '[[ -f "$DATA_DIR/.lab-2026-10-11" ]]' in fast and "start_lab_nightly_if_missed" in fast
    lib = LIB.read_text()
    for fn in ("install_lab_agent", "apply_lab_2026_10_11", "install_lab_icon", "start_lab_nightly_if_missed"):
        assert f"\n{fn}() {{" in lib, fn


def test_the_scripts_are_bash_32():
    for path in (LIB, MAC / "Start Trading Bot.command", MAC / "The Lab.command"):
        r = subprocess.run(["bash", "-n", str(path)], capture_output=True, text=True)
        assert r.returncode == 0, (path.name, r.stderr)
        text = path.read_text()
        assert text.startswith("#!/bin/bash")
        for bad in (r"\$\{\w+,,", r"\$\{\w+\^\^", r"\bmapfile\b", r"\breadarray\b", r"\bdeclare\s+-A\b", "&>>",
                    r"\blocal\s+-n\b"):
            assert not re.search(bad, text), (path.name, bad)
        code = "\n".join(l for l in text.splitlines() if not l.strip().startswith("#"))
        assert not re.search(r"^\s*local\s+(\w+)=[^;]*?\s+\w+=[^;]*?\$\{?\1\b", code, re.MULTILINE), path.name
        for line in text.splitlines():
            if not line.strip().startswith("#") and re.search(r'"\$(\d)"', line):
                assert "${" in line or "local" not in line, (path.name, line)
    assert os.access(MAC / "The Lab.command", os.X_OK)


def test_the_icon_menu_runs_the_lab_gently_and_pauses_it(tmp_path):
    text = (MAC / "The Lab.command").read_text()
    assert "nice -n 19" in text and "mintel.lab.nightly" in text and "--workers 1" in text
    assert "tail -n 0 -f" in text and '/lab"' in text
    assert "1) Open its page" in text and "4) Quit" in text
    # it runs from a made-up install: quits at once on "4"
    home, calls, bin_dir = _fake_home(tmp_path)
    env = {**os.environ, "MINTEL_HOME": str(home), "HOME": str(tmp_path), "PATH": f"{bin_dir}:{os.environ['PATH']}",
           "TERM": "dumb"}
    r = subprocess.run(["bash", "-c", f'printf "4\\n" | bash "{MAC / "The Lab.command"}" < /dev/null'],
                       capture_output=True, text=True, env=env, timeout=60)
    assert r.returncode == 0, r.stderr
    assert "THE LAB" in r.stdout and "The lab: TEST DATA - not a result." in r.stdout


def test_the_operator_page_says_how_it_runs():
    doc = (ROOT / "docs" / "ops" / "lab.md").read_text()
    for words in ("com.mintel.lab", "nice -n 19", "data/lab/STOP", "no heartbeat", "--max-candidates 50",
                  "TEST DATA - not a result", "cannot place an order"):
        assert words in doc, words
