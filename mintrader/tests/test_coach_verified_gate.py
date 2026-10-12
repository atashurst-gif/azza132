"""The verifiers' findings on the coach's gate, fixed (12 Oct), each pinned by a test that fails without its fix.

3. When a lease carries a demo block but the demo test fails, the v1 NUMBER of trials applies as well as the
   v1 trial size: only the envelope's max_trials real trials keep their grant (section 21).
4. A money cap on a Trend & Breakout BASELINE entry is never claimed (the trader sizes Aaron's own orders as
   before), so B5 headroom weighs the size really sent.
20. Before the coach is installed (no envelope file), the gate writes no log: the data folder stays as it was.
"""
from __future__ import annotations

from pathlib import Path

from mintel.coach import DemoLimits, Entry, RiskCtx
from mintel.coach import gate as G
from mintel.config import Config, LIVE_MARKER
from tests.test_coach_policy import NOW, coach_cfg, make_policy, put_envelope, put_policy

TRIAL_KEYS = ["tnb|SESSION_EXPANSION|INDEX|TREND", "tnb|BREAKOUT_RETEST|INDEX|TREND",
              "tnb|TREND_PULLBACK|FX_MAJOR|TREND", "tnb|SESSION_EXPANSION|GOLD|TREND"]


def _decide(g, key, *, static_real=False, planned=None, equity=1865.61):
    _, ap, kind, cond = key.split("|")
    return g.decide(ap, kind, cond, "X", 1, 75, 2.0, NOW, static_real,
                    RiskCtx(equity=equity, has_paper=True, planned_gbp=planned))


def test_on_a_real_money_account_only_the_envelopes_number_of_trials_is_granted(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg)                                          # max_trials 2
    ents = {k: Entry("REAL", "LIVE_TRIAL", 0.7, 13.0, step="trial", rule="G1") for k in TRIAL_KEYS}
    put_policy(cfg, make_policy(sha, entries=ents, demo=DemoLimits(max_risk_pct=1.5)))
    cfg.mode, cfg.live_marker = "LIVE", LIVE_MARKER                  # a real-money account: the demo test fails
    g = G.CoachGate("tnb", cfg.ops.data_dir, cfg, clock=lambda: NOW)
    assert not g.demo_test(NOW)[0]
    got = {k: _decide(g, k) for k in TRIAL_KEYS}
    real = sorted(k for k, d in got.items() if d.route == "REAL")
    assert real == sorted(TRIAL_KEYS)[:2]
    assert all(d.money_gbp <= 6.53 for d in got.values() if d.route == "REAL")      # v1 trial size too
    for k in set(TRIAL_KEYS) - set(real):
        assert got[k].route == "BASELINE" and got[k].money_gbp is None and "trials" in got[k].why


def test_a_tnb_baseline_money_cap_does_not_hide_the_size_sent_from_b5(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg)
    put_policy(cfg, make_policy(sha, entries={"tnb|*|*|*": Entry("BASELINE", "PLACED", money_gbp=1.0)},
                                headroom_gbp=5.0))
    g = G.CoachGate("tnb", cfg.ops.data_dir, cfg, clock=lambda: NOW)
    d = _decide(g, "tnb|TREND_PULLBACK|FX_MINOR|TREND", static_real=True, planned=13.0)
    assert d.route == "PRACTICE" and d.rule == "B5"                  # £13 would go out with £5 of headroom
    # within the headroom: Aaron's own settings, and no cap claimed that the trader would not apply
    d = _decide(g, "tnb|TREND_PULLBACK|FX_MINOR|TREND", static_real=True, planned=4.0)
    assert d.route == "BASELINE" and d.money_gbp is None


def test_the_runners_money_cap_is_still_given(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg)
    put_policy(cfg, make_policy(sha, entries={"runner|*|*|*": Entry("BASELINE", "PLACED", money_gbp=3.0)}))
    g = G.CoachGate("tnb", cfg.ops.data_dir, cfg, clock=lambda: NOW)
    d = g.decide("SESSION_EXPANSION", "INDEX", "TREND", "US500", 1, 75, 2.0, NOW, True,
                 RiskCtx(equity=1865.61), bot="runner")
    assert d.route == "BASELINE" and d.money_gbp == 3.0


def test_before_the_coach_is_installed_the_gate_writes_nothing(tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    cfg = Config()
    cfg.ops.data_dir = str(data)
    for bot in ("tnb", "runner", "ian", "bandbreaker", "crowd"):
        g = G.build(bot, data, cfg, clock=lambda: NOW)
        d = g.decide("SESSION_EXPANSION", "INDEX", "TREND", "US500", 1, 75, 2.0, NOW, True, RiskCtx(equity=1000.0))
        assert d.route == "BASELINE" and d.money_gbp is None
        g.note_fill(1_990_000_001, d, symbol="US500")
    assert list(data.iterdir()) == []


def test_once_installed_the_gate_logs_even_when_off(tmp_path):
    cfg = coach_cfg(tmp_path)
    put_envelope(cfg, mode="OFF")
    g = G.CoachGate("tnb", cfg.ops.data_dir, cfg, clock=lambda: NOW)
    d = _decide(g, "tnb|TREND_PULLBACK|FX_MINOR|TREND", static_real=True)
    assert d.route == "BASELINE" and d.source == "off"
    assert (Path(cfg.ops.data_dir) / "coach" / "gate" / "tnb-2026-10-13.jsonl").exists()
