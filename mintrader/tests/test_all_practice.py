"""13 Oct, Aaron: "put everything out of live and into the lab". The installer's
one-time step: every bot on PAPER through the modes command, no Trend &
Breakout market kept LIVE, and the coach's envelope allowing no real-money
grant (no kind of market, no Formula 1 pin). MADE-UP MarketBot folders only."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from mintel.coach import envelope as E

ROOT = Path(__file__).resolve().parents[1]
MAC = ROOT / "deploy" / "mac"
LIB = MAC / "mintel_mac.sh"
STEP = 'apply_all_practice_2026_10_13; echo "CODE=${CODE_CHANGED:-none}"'
COACH = 'apply_coach_2026_10_12'


def _home(tmp_path, *, modes_fail=False):
    """A MADE-UP MarketBot whose python runs the real modes command and coach code (or fails the modes command)."""
    home = tmp_path / "MarketBot"
    (home / "data").mkdir(parents=True)
    (home / "logs").mkdir()
    (home / "venv" / "bin").mkdir(parents=True)
    (home / "app").symlink_to(ROOT)
    calls = tmp_path / "calls.log"
    py = home / "venv" / "bin" / "python"
    fail = 'if [[ "${1:-}" == "-m" && "${2:-}" == "mintel.ops.modes" ]]; then echo "refused (made up)"; exit 2; fi\n'
    py.write_text("#!/bin/bash\n"
                  f'echo "python $*" >> "{calls}"\n'
                  + (fail if modes_fail else "")
                  + 'if [[ "${1:-}" == "-m" && "${2:-}" != "mintel.ops.modes" ]]; then exit 0; fi\n'
                  f'cd "{ROOT}" && exec "{sys.executable}" "$@"\n')
    py.chmod(0o755)
    (tmp_path / "Desktop").mkdir()
    data = home / "data"
    (data / "config.json").write_text(json.dumps({
        "mode": "DEMO", "risk": {"max_risk_pct": 1.5, "max_daily_loss_pct": 3.0},
        "tnb": {"mode": "PAPER", "live_groups": ["FX_MINOR"], "live_tactics": ["MOMENTUM_CONTINUATION"],
                "live_since_utc": "2026-10-12T07:57:59Z"},
        "runner": {"mode": "LIVE"}, "bandbreaker": {"mode": "PAPER"}, "crowd": {"mode": "PAPER"},
        "ops": {"data_dir": str(data)}}))
    (data / "ian.json").write_text(json.dumps({"enabled": True, "mode": "LIVE"}))
    return home, calls


def _bash(snippet, home, tmp_path):
    env = {**os.environ, "MINTEL_HOME": str(home), "HOME": str(tmp_path),
           "MINTEL_COACH_INSTALL_UTC": "2026-10-12T14:00:00+00:00"}
    return subprocess.run(["bash", "-c", f'set -uo pipefail; source "{LIB}"; {snippet}'],
                          capture_output=True, text=True, env=env, timeout=180)


def test_every_bot_goes_to_practice_and_the_coach_may_grant_no_real_money(tmp_path):
    home, calls = _home(tmp_path)
    data = home / "data"
    assert _bash(COACH, home, tmp_path).returncode == 0
    before = json.loads((data / "coach.json").read_text())
    assert before["pins"] and before["tnb_real_kinds"]
    r = _bash(STEP, home, tmp_path)
    assert r.returncode == 0, r.stderr
    assert "[WARN]" not in r.stdout, r.stdout
    assert (data / ".all-practice-2026-10-13").exists() and (data / ".restart-pending").exists()
    assert "CODE=yes" in r.stdout and "Every bot is on practice now" in r.stdout
    cfg = json.loads((data / "config.json").read_text())
    assert cfg["tnb"]["mode"] == "PAPER" and cfg["tnb"]["live_groups"] == []
    assert cfg["runner"]["mode"] == cfg["bandbreaker"]["mode"] == cfg["crowd"]["mode"] == "PAPER"
    assert json.loads((data / "ian.json").read_text())["mode"] == "PAPER"
    st = E.read(data / "coach.json", 1.5, 3.0)                     # the coach's own reader accepts it
    assert st.ok, st.error
    assert st.envelope.tnb_real_kinds == () and st.envelope.pins == ()
    after = json.loads((data / "coach.json").read_text())
    for k in ("mode", "watch_until_utc", "govern", "max_trials", "demo_max_step_pct", "day_line_pct"):
        assert after[k] == before[k], k                              # everything else kept
    assert "weekly pick" in after["set_by"]
    assert not (data / "coach" / ".practice.out").exists() and not (data / ".coach.json.tmp").exists()
    again = _bash(STEP, home, tmp_path)                              # once only
    assert again.stdout.strip() == "CODE=none"


def test_nothing_is_marked_done_when_the_modes_command_refuses(tmp_path):
    home, calls = _home(tmp_path, modes_fail=True)
    data = home / "data"
    r = _bash(STEP, home, tmp_path)
    assert r.returncode == 0, r.stderr
    assert not (data / ".all-practice-2026-10-13").exists() and "CODE=none" in r.stdout
    assert "[WARN]" in r.stdout and "--paper runner bandbreaker crowd ian --tnb-live-groups none" in r.stdout


def test_without_a_coach_the_bots_still_go_to_practice(tmp_path):
    home, calls = _home(tmp_path)
    data = home / "data"
    r = _bash(STEP, home, tmp_path)
    assert r.returncode == 0, r.stderr
    assert (data / ".all-practice-2026-10-13").exists() and not (data / "coach.json").exists()
    assert json.loads((data / "config.json").read_text())["runner"]["mode"] == "PAPER"


def test_the_launcher_runs_it_after_the_coach_and_the_fast_path_waits_for_it():
    text = (MAC / "Start Trading Bot.command").read_text()
    body = text.split("write_config         ||")[1]
    order = ["\napply_coach_2026_10_12", "\ninstall_coach_icon", "\napply_all_practice_2026_10_13",
             "\nremember_restart", "\nstart_everything"]
    pos = [body.index(x) for x in order]
    assert pos == sorted(pos)
    fast = text.split("# ---- Fast path")[1].split("# ---- Otherwise")[0]
    assert fast.index(".coach-2026-10-12") < fast.index(".all-practice-2026-10-13") < fast.index(".restart-pending")
