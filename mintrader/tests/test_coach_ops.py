"""The coach's wiring on the Mac: the watchdog, the pulse and the installer (spec sections 3, 14, 15, 19).

* the watchdog looks after ONE more process, ``coach`` (``python -m mintel.coach run`` at nice 10), right
  after the lab's, judged by the freshness of data/coach/status.json (180 s), never by a heartbeat; only
  while data/coach.json exists and does not say OFF;
* the pulse publishes reports/live/coach.json (whitelisted, scrubbed, <= 20 KB) once the coach is
  switched on, and the coach's nightly digest once (the coach holds no token);
* the installer (bash 3.2): one-time step with its marker written only on success, writing Aaron's
  envelope (PILOT, watching only until the end of the first full UK trading day after the install,
  Formula 1 pinned, the demo caps) that the coach's own reader accepts; never overwriting an envelope
  that is there; the icon placed once; a stop line; a health line; the call lines after the lab's.

EVERY FIGURE HERE IS MADE-UP TEST DATA (fake Python, made-up files, tests/fixtures/coach_status_example.json).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from mintel.coach import F1_KEY, MODE_PILOT, MODE_WATCH
from mintel.coach import envelope as E
from mintel.config import Config
from mintel.ops import pulse as PL
from mintel.ops.watchdog import build_default, coach_status_fresh, coach_switched_on

ROOT = Path(__file__).resolve().parents[1]
MAC = ROOT / "deploy" / "mac"
LIB = MAC / "mintel_mac.sh"
FIXTURE = ROOT / "tests" / "fixtures" / "coach_status_example.json"
UTC = dt.timezone.utc


def _cfg(tmp_path) -> Config:
    c = Config()
    c.ops.data_dir = str(tmp_path / "data")
    c.ops.log_dir = str(tmp_path / "logs")
    return c


# ---------------------------------------------------------------- watchdog ---
def test_the_watchdog_runs_the_coach_after_the_lab_with_no_heartbeat(tmp_path):
    cfg = _cfg(tmp_path)
    cfg_path = str(tmp_path / "data" / "config.json")
    wd = build_default(cfg, cfg_path, project_dir=str(ROOT))
    names = [p.name for p in wd.processes]
    assert names.index("coach") == names.index("lab") + 1
    coach = wd.processes[names.index("coach")]
    cmd = list(coach.command)
    tail = ["-m", "mintel.coach", "run", "--config", str(Path(cfg_path).resolve())]
    assert cmd[-6:-5] == [sys.executable] and cmd[-5:] == tail
    if shutil.which("nice"):
        assert cmd[:3] == ["nice", "-n", "10"]
    assert coach.heartbeat == ""                                      # NO heartbeat (C5)
    assert coach.probe is not None
    assert coach.pid_file.endswith("data/coach.pid") and coach.log_file.endswith("logs/coach.out.log")
    assert coach.kill_pattern == "mintel.coach run" and coach.grace_seconds == 120.0
    assert coach.max_restarts_per_hour == cfg.ops.max_restarts_per_hour


def test_the_coach_runs_only_while_its_envelope_exists_and_is_not_off(tmp_path):
    cfg = _cfg(tmp_path)
    wd = build_default(cfg, str(tmp_path / "data" / "config.json"), project_dir=str(ROOT))
    coach = next(p for p in wd.processes if p.name == "coach")
    data = Path(cfg.ops.data_dir)
    data.mkdir(parents=True, exist_ok=True)
    env = data / "coach.json"
    assert coach.is_enabled() is False and coach_switched_on(env) is False      # not switched on yet
    for mode, on in (("PILOT", True), ("WATCH", True), ("GUARD", True), ("OFF", False), ("off", False)):
        env.write_text(json.dumps({"schema": 1, "mode": mode}))
        assert coach.is_enabled() is on, mode
    env.write_text("{broken")                     # there but unreadable: it runs, to say so on the page
    assert coach.is_enabled() is True


def test_the_probe_is_the_status_files_freshness(tmp_path):
    status = tmp_path / "data" / "coach" / "status.json"
    now = dt.datetime(2026, 10, 16, 9, 15, tzinfo=UTC)
    probe = coach_status_fresh(status, 180.0, clock=lambda: now)
    assert probe() is False                                           # missing
    status.parent.mkdir(parents=True)
    for age, fresh in ((0, True), (179, True), (181, False), (900, False)):
        status.write_text(json.dumps({"written_utc": (now - dt.timedelta(seconds=age)).isoformat()}))
        assert probe() is fresh, age
    status.write_text("{bad")                                         # unreadable: the file's own time
    assert coach_status_fresh(status, 180.0)() is True
    os.utime(status, (0, 0))
    assert coach_status_fresh(status, 180.0)() is False
    assert not (tmp_path / "data" / "heartbeats").exists()


def test_a_project_without_the_coach_schedules_no_coach(tmp_path):
    wd = build_default(_cfg(tmp_path), str(tmp_path / "c.json"), project_dir=str(tmp_path))
    assert "coach" not in [p.name for p in wd.processes]


# ------------------------------------------------------------------- pulse ---
def _status_big() -> dict:
    st = json.loads(FIXTURE.read_text())
    st["secret_token"] = "ghp_abcdefghijklmnop"
    st["unlisted"] = "dropped"
    st["keys"] = [dict(st["keys"][0], key=f"tnb|K{k}|INDEX|TREND", why="w" * 300, api_key="x") for k in range(40)]
    st["changes"] = [dict(st["changes"][0], words=f"made up change {k} login=hunter2 " + "c" * 400)
                     for k in range(40)]
    return st


def test_the_pulse_publishes_the_coach_once_it_is_switched_on(tmp_path):
    cfg = _cfg(tmp_path)
    data = Path(cfg.ops.data_dir)
    assert PL.coach_json(cfg) is None
    assert PL.COACH_PATH not in PL.pulse_files({"ts_utc": "x"}, ["a"], bots="{}")
    (data / "coach").mkdir(parents=True)
    (data / "coach.json").write_text(json.dumps({"schema": 1, "mode": "PILOT"}))
    (data / "coach" / "status.json").write_text(json.dumps(_status_big()))
    prog = {"from": "2026-10-12", "ok": True, "total": 22.3, "trading_days": 5, "secret": "x", "other": 1,
            "days": [{"day": "2026-10-12", "net": 10.0}]}
    p = {"status": {"status": {}, "coach_progress": prog}}
    text = PL.coach_json(cfg, p)
    assert len(text.encode()) <= PL.COACH_MAX_BYTES == 20_000
    rep = json.loads(text)
    st = rep["status"]
    assert st["synthetic"] is True and st["mode"] == "PILOT" and st["policy"]["seq"] == 1842
    assert "secret_token" not in st and "unlisted" not in st
    assert st["changes"] == [] and st["changes_left_out"] == 40 and rep.get("trimmed")     # the log goes first
    assert len(st["keys"]) == 12 and st["keys_left_out"] == 28
    assert all("api_key" not in k for k in st["keys"])
    assert "hunter2" not in text and "ghp_" not in text
    assert rep["progress"] == {"from": "2026-10-12", "ok": True, "total": 22.3, "trading_days": 5,
                               "days": [{"day": "2026-10-12", "net": 10.0}]}
    files = PL.pulse_files({"ts_utc": "x"}, ["a"], bots="{}", lab=None, coach=text)
    assert files[PL.COACH_PATH] == text and PL.COACH_PATH == "reports/live/coach.json"


def test_a_small_status_is_not_trimmed_and_a_broken_one_is_said(tmp_path):
    cfg = _cfg(tmp_path)
    data = Path(cfg.ops.data_dir)
    (data / "coach").mkdir(parents=True)
    (data / "coach.json").write_text(json.dumps({"schema": 1, "mode": "WATCH"}))
    (data / "coach" / "status.json").write_text(FIXTURE.read_text())
    rep = json.loads(PL.coach_json(cfg, {"status": {"unavailable": "down"}}))
    assert "trimmed" not in rep and len(rep["status"]["changes"]) == 2
    assert rep["progress_unavailable"] == "the trader's page did not answer"
    (data / "coach" / "status.json").write_text("{broken")
    rep = json.loads(PL.coach_json(cfg))
    assert "not valid JSON" in rep["status_unavailable"] and "status" not in rep


def test_pulse_main_publishes_coach_json_and_the_digest_once(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.save(tmp_path / "data" / "config.json")
    data = tmp_path / "data"
    (data / "coach" / "reports" / "2026-10-15").mkdir(parents=True)
    (data / "coach.json").write_text(json.dumps({"schema": 1, "mode": "PILOT"}))
    (data / "coach" / "status.json").write_text(FIXTURE.read_text())
    (data / "coach" / "reports" / "2026-10-15" / "report.md").write_text("# The coach - MADE UP\n")
    import mintel.ops.report_upload as RU
    sent: list = []
    monkeypatch.setattr(RU, "read_token", lambda p: "made-up")
    monkeypatch.setattr(RU, "upload", lambda files, *a, **k: sent.append((dict(files), k.get("day"))) or len(files))
    monkeypatch.setattr(PL, "take_pulse", lambda cfg, now=None: {
        "ts_utc": "2026-10-16T09:00:00Z", "up": True, "why": "",
        "status": {"status": {}, "coach_progress": {"ok": True, "total": 1.0}}})
    monkeypatch.setattr(PL, "pulse_line", lambda p: "2026-10-16T09:00:00Z UP made-up")
    PL.main(["--config", str(data / "config.json")])
    live, day = sent[0]
    assert day == "live" and json.loads(live[PL.COACH_PATH])["progress"] == {"ok": True, "total": 1.0}
    assert sent[1] == ({"reports/coach/2026-10-15/report.md": "# The coach - MADE UP\n"}, "coach/2026-10-15")
    assert (data / "coach" / "reports" / "2026-10-15" / ".sent").exists()
    sent.clear()
    PL.main(["--config", str(data / "config.json")])
    assert len(sent) == 1                                             # the digest goes once


# --------------------------------------------------------------- installer ---
def _fake_home(tmp_path):
    """A MADE-UP MarketBot: a fake Python that runs nothing with -m and the real one for a script."""
    home = tmp_path / "MarketBot"
    (home / "data").mkdir(parents=True)
    (home / "logs").mkdir()
    (home / "venv" / "bin").mkdir(parents=True)
    (home / "app").symlink_to(ROOT)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "calls.log"
    py = home / "venv" / "bin" / "python"
    py.write_text("#!/bin/bash\n"
                  f'echo "python $*" >> "{calls}"\n'
                  'if [[ "${1:-}" == "-m" && "${2:-}" == "mintel.coach" && "${3:-}" == "show" ]]; then\n'
                  '  echo "The coach: TEST DATA - watching only"; exit 0\n'
                  "fi\n"
                  'if [[ "${1:-}" == "-m" ]]; then exit 0; fi\n'
                  f'exec "{sys.executable}" "$@"\n')
    py.chmod(0o755)
    (tmp_path / "Desktop").mkdir()
    return home, calls, bin_dir


def _bash(snippet, home, bin_dir, tmp_path, **env):
    full = {**os.environ, "MINTEL_HOME": str(home), "HOME": str(tmp_path),
            "PATH": f"{bin_dir}:{os.environ['PATH']}", **env}
    return subprocess.run(["bash", "-c", f'set -uo pipefail; source "{LIB}"; {snippet}'],
                          capture_output=True, text=True, env=full, timeout=120)


STEP = 'apply_coach_2026_10_12; echo "CODE=${CODE_CHANGED:-none}"'


@pytest.mark.parametrize("installed,until,starts", [
    ("2026-10-12T14:00:00+00:00", "2026-10-13T23:00:00+00:00", "Wed 14 Oct"),    # Monday afternoon: Tuesday
    ("2026-10-16T18:00:00+00:00", "2026-10-19T23:00:00+00:00", "Tue 20 Oct"),    # Friday evening: Monday
    ("2026-10-24T10:00:00+00:00", "2026-10-27T00:00:00+00:00", "Tue 27 Oct"),    # a Saturday; then GMT
])
def test_the_coach_is_switched_on_once_with_an_envelope_the_coach_accepts(tmp_path, installed, until, starts):
    home, calls, bin_dir = _fake_home(tmp_path)
    r = _bash(STEP, home, bin_dir, tmp_path, MINTEL_COACH_INSTALL_UTC=installed)
    assert r.returncode == 0, r.stderr
    data = home / "data"
    assert (data / ".coach-2026-10-12").exists() and (data / "coach").is_dir()
    assert (data / ".restart-pending").exists() and "CODE=yes" in r.stdout
    assert "[WARN]" not in r.stdout and f"in charge within your limits from 06:30 UK on {starts}" in r.stdout
    assert "it cannot place an order and never changes config.json" in r.stdout
    assert not (data / "config.json").exists()                        # never written
    st = E.read(data / "coach.json", 1.5, 3.0)                         # the coach's own reader, Aaron's settings
    assert st.ok, st.error
    env = st.envelope
    want_until = dt.datetime.fromisoformat(until)                    # midnight UK at the end of that day
    assert dt.datetime.fromisoformat(env.watch_until_utc) == want_until
    assert env.mode == MODE_PILOT and env.govern == ("tnb", "runner", "ian", "bandbreaker", "crowd")
    assert env.tnb_real_kinds == ("FX_MINOR", "FX_MAJOR", "INDEX", "GOLD")
    assert (env.trial_risk_pct, env.trial_max_gbp, env.max_trials, env.trial_loss_budget_gbp_week) == (0.35, 8.0, 2,
                                                                                                         40.0)
    assert (env.max_step_pct, env.day_line_pct, env.day_pause_pct, env.week_line_pct) == (1.5, 1.5, 2.25, 5.0)
    assert (env.drawdown_line_pct, env.drawdown_pause_pct) == (5.0, 8.0) and env.announce_steps is True
    assert (env.demo_max_step_pct, env.demo_max_daily_stop_pct) == (5.0, 10.0)
    assert [(p.key, p.judge_after) for p in env.pins] == [(F1_KEY, 20)]
    assert env.set_by == "installer, Aaron's request of 12 Oct 2026"
    # watching only on the install day, in charge only from the next 06:30 UK release after the WATCH day
    inst = dt.datetime.fromisoformat(installed)
    assert E.effective_mode(env, inst)[0] == MODE_WATCH
    assert E.effective_mode(env, want_until + dt.timedelta(hours=1))[0] == MODE_WATCH
    assert E.effective_mode(env, want_until + dt.timedelta(hours=8))[0] == MODE_PILOT
    assert not list((data / "coach").iterdir())                       # nothing left behind
    # once only
    again = _bash(STEP, home, bin_dir, tmp_path, MINTEL_COACH_INSTALL_UTC=installed)
    assert again.stdout.strip() == "CODE=none"


def test_the_marker_is_written_only_on_success(tmp_path):
    home, calls, bin_dir = _fake_home(tmp_path)
    data = home / "data"
    (data / "coach").write_text("in the way")                         # its folder cannot be made
    r = _bash(STEP, home, bin_dir, tmp_path)
    assert r.returncode == 0, r.stderr
    assert not (data / ".coach-2026-10-12").exists() and "CODE=none" in r.stdout
    assert "[WARN]" in r.stdout and "the next double-click tries again" in r.stdout
    (data / "coach").unlink()
    # an envelope the coach would refuse is never written: Aaron's settings say a negative ceiling (made up)
    (data / "config.json").write_text(json.dumps({"risk": {"max_risk_pct": -1.0}}))
    r = _bash(STEP, home, bin_dir, tmp_path)
    assert r.returncode == 0, r.stderr
    assert not (data / ".coach-2026-10-12").exists() and not (data / "coach.json").exists()
    assert "CODE=none" in r.stdout and "Could not write the coach's settings" in r.stdout
    assert not (data / ".coach.json.tmp").exists() and not (data / "coach" / ".install.out").exists()
    # fixed: now it is switched on
    (data / "config.json").write_text(json.dumps({"risk": {"max_risk_pct": 1.5, "max_daily_loss_pct": 3.0}}))
    r = _bash(STEP, home, bin_dir, tmp_path)
    assert (data / ".coach-2026-10-12").exists() and "CODE=yes" in r.stdout
    assert E.read(data / "coach.json", 1.5, 3.0).ok


def test_an_envelope_already_there_is_never_overwritten(tmp_path):
    home, calls, bin_dir = _fake_home(tmp_path)
    data = home / "data"
    mine = {"schema": 1, "mode": "OFF", "set_by": "Aaron"}
    (data / "coach.json").write_text(json.dumps(mine))
    r = _bash(STEP, home, bin_dir, tmp_path)
    assert r.returncode == 0, r.stderr
    assert json.loads((data / "coach.json").read_text()) == mine
    assert (data / ".coach-2026-10-12").exists() and "already there and is unchanged" in r.stdout


def test_a_smaller_daily_stop_scales_the_coachs_lines(tmp_path):
    home, calls, bin_dir = _fake_home(tmp_path)
    data = home / "data"
    (data / "config.json").write_text(json.dumps({"risk": {"max_risk_pct": 1.0, "max_daily_loss_pct": 2.0}}))
    r = _bash(STEP, home, bin_dir, tmp_path)
    assert r.returncode == 0, r.stderr
    st = E.read(data / "coach.json", 1.0, 2.0)
    assert st.ok, st.error
    assert (st.envelope.max_step_pct, st.envelope.day_line_pct, st.envelope.day_pause_pct) == (1.0, 1.0, 1.5)


def test_the_icon_is_placed_once(tmp_path):
    home, calls, bin_dir = _fake_home(tmp_path)
    r = _bash("install_coach_icon", home, bin_dir, tmp_path)
    assert r.returncode == 0, r.stderr
    icon = tmp_path / "Desktop" / "The Coach.command"
    assert icon.exists() and os.access(icon, os.X_OK) and (home / "data" / ".desktop-icon-coach-placed").exists()
    assert icon.read_text() == (MAC / "The Coach.command").read_text()
    icon.unlink()                                                     # Aaron moved it away
    _bash("install_coach_icon", home, bin_dir, tmp_path)
    assert not icon.exists()                                         # never put back


def test_stop_ends_the_coach_and_health_shows_its_line():
    lib = LIB.read_text()
    stop = lib.split("stop_everything() {")[1].split("\n}\n")[0]
    assert 'pkill -f "mintel.coach run"' in stop and 'rm -f "$DATA_DIR/coach.pid"' in stop
    assert stop.index("mintel.lab.practice") < stop.index("mintel.coach run")
    health = lib.split("show_health() {")[1].split("\n}\n")[0]
    assert '[[ -f "$DATA_DIR/coach.json" ]]' in health and "-m mintel.coach show --once" in health


def test_health_prints_the_coachs_line(tmp_path):
    home, calls, bin_dir = _fake_home(tmp_path)
    (home / "data" / "coach.json").write_text("{}")
    r = _bash('say "    $(cd "$APP_DIR" && "$VENV_DIR/bin/python" -m mintel.coach show --once --config "$CONFIG" '
              '2>/dev/null | tail -1)"', home, bin_dir, tmp_path)
    assert "The coach: TEST DATA - watching only" in r.stdout


def test_the_launcher_calls_the_coach_steps_after_the_labs():
    text = (MAC / "Start Trading Bot.command").read_text()
    body = text.split("write_config         ||")[1]
    order = ["\napply_formula1_choices", "\napply_lab_2026_10_11", "\ninstall_lab_icon", "\napply_coach_2026_10_12",
             "\ninstall_coach_icon", "\nremember_restart", "\ninstall_launch_agent", "\nstart_everything",
             "\nshow_health"]
    pos = [body.index(x) for x in order]
    assert pos == sorted(pos)
    fast = text.split("# ---- Fast path")[1].split("# ---- Otherwise")[0]
    assert '[[ -f "$DATA_DIR/.coach-2026-10-12" ]]' in fast
    assert fast.index(".lab-2026-10-11") < fast.index(".coach-2026-10-12") < fast.index(".restart-pending")
    lib = LIB.read_text()
    for fn in ("apply_coach_2026_10_12", "install_coach_icon"):
        assert f"\n{fn}() {{" in lib, fn
    step = lib.split("apply_coach_2026_10_12() {")[1].split("\n}\n")[0]
    assert "remember_restart" in step and 'CODE_CHANGED="yes"' in step
    assert step.index('> "$marker"') > step.index("PYEOF\n  then")      # the marker only after the write worked


def test_the_scripts_are_bash_32():
    for path in (LIB, MAC / "Start Trading Bot.command", MAC / "The Coach.command"):
        r = subprocess.run(["bash", "-n", str(path)], capture_output=True, text=True)
        assert r.returncode == 0, (path.name, r.stderr)
        text = path.read_text()
        assert text.startswith("#!/bin/bash")
        for bad in (r"\$\{\w+,,", r"\$\{\w+\^\^", r"\bmapfile\b", r"\breadarray\b", r"\bdeclare\s+-A\b", "&>>",
                    r"\blocal\s+-n\b"):
            assert not re.search(bad, text), (path.name, bad)
        if path.name != "The Coach.command":                         # Builder D's icon: bash -n and the bans
            code = "\n".join(l for l in text.splitlines() if not l.strip().startswith("#"))
            assert not re.search(r"^\s*local\s+(\w+)=[^;]*?\s+\w+=[^;]*?\$\{?\1\b", code, re.MULTILINE), path.name
    step = LIB.read_text().split("apply_coach_2026_10_12() {")[1].split("\n}\n")[0]
    assert "$(" not in step.split("<<'PYEOF'")[0].split("\n")[-1]     # no heredoc inside $( ) (bash 3.2)
    assert os.access(MAC / "The Coach.command", os.X_OK)


def test_the_installer_never_writes_config_or_reads_secrets():
    step = LIB.read_text().split("apply_coach_2026_10_12() {")[1].split("\n}\n")[0]
    assert "secrets.json" not in step and "read_token" not in step and "mintel.ops.modes" not in step
    py = step.split("<<'PYEOF'")[1].split("PYEOF")[0]
    assert "baseline_config" in py and "Config.load" not in py
    writes = re.findall(r"open\(([^,]+), \"w\"", py)
    assert writes == ["tmp"] and 'path = data / "coach.json"' in py
