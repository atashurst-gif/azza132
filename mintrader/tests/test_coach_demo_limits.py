"""Section 21, Aaron's 12 Oct instruction: no limits on the demo, step by step.

Lifted values apply only while BOTH hold: config.json is on DEMO and the
broker reports a demo account, as the trader writes it into
``data/coach/weather.json`` (``account_demo``, no older than 180 s). Missing,
stale or contradictory: the real-money (config.json) values. A valid PILOT
lease must carry the demo values; a value is only ever lifted, never lowered.
The coach never writes config.json."""
from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
from pathlib import Path

import pytest

from mintel.broker.base import AccountInfo
from mintel.coach import DemoLimits, Entry, Limits, RiskCtx
from mintel.coach import gate as G
from mintel.config import LIVE_MARKER
from mintel.engine.risk import RiskManager
from tests.test_coach_hooks import US500_KEY, act_on, build, full_policy
from tests.test_coach_policy import NOW, coach_cfg, make_policy, put_envelope, put_policy
from tests.test_tnb_paper_wiring import TNB, entry

DEMO = DemoLimits(max_daily_loss_pct=6.0, max_risk_pct=2.0, max_drawdown_pct=25.0, max_correlated_risk_pct=3.0,
                  max_total_risk_pct=6.0)


def weather(cfg, *, at=NOW, **over):
    doc = {"schema": 1, "bot": "tnb", "as_of_utc": at.isoformat(), "account_demo": True, "account_is_demo": True,
           "account_server": "ICMarketsSC-Demo", "account_login": 0, "config_mode": "DEMO", "symbols": []}
    doc.update(over)
    p = Path(cfg.ops.data_dir) / "coach" / "weather.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(doc))
    return p


def demo_gate(tmp_path, *, demo=DEMO, mode="PILOT", entries=None):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg, mode=mode)
    put_policy(cfg, make_policy(sha, demo=demo, entries=entries if entries is not None else {}))
    weather(cfg)
    return cfg, G.CoachGate("tnb", cfg.ops.data_dir, cfg, clock=lambda: NOW)


def test_the_demo_test_passes_and_lifts(tmp_path):
    cfg, g = demo_gate(tmp_path)
    lim = g.effective_limits(NOW)
    assert lim.demo and lim.max_daily_loss_pct == 6.0 and lim.max_risk_pct == 2.0
    assert lim.max_drawdown_pct == 25.0 and lim.max_correlated_risk_pct == 3.0 and lim.max_total_risk_pct == 6.0
    assert "Every trade still has a stop" in lim.why


@pytest.mark.parametrize("case", [
    "config LIVE", "config mode word", "weather missing", "weather stale", "weather from the future",
    "account not demo", "trade mode not demo", "server not demo", "trader said LIVE", "another account",
    "lease stale", "policy broken", "envelope GUARD", "envelope WATCH", "COACH-OFF", "HALT", "no demo block",
    "not governed"])
def test_every_fail_safe_case_gives_the_real_money_values(tmp_path, case):
    cfg, g = demo_gate(tmp_path)
    now = NOW
    data = Path(cfg.ops.data_dir)
    if case == "config LIVE":
        cfg.mode, cfg.live_marker = "LIVE", LIVE_MARKER
    elif case == "config mode word":
        cfg.mode = "PAPER"
    elif case == "weather missing":
        (data / "coach" / "weather.json").unlink()
    elif case == "weather stale":
        weather(cfg, at=NOW - dt.timedelta(seconds=181))
    elif case == "weather from the future":
        weather(cfg, at=NOW + dt.timedelta(seconds=200))
    elif case == "account not demo":
        weather(cfg, account_demo=False)
    elif case == "trade mode not demo":
        weather(cfg, account_is_demo=False)
    elif case == "server not demo":
        weather(cfg, account_server="ICMarketsSC-Live07")
    elif case == "trader said LIVE":
        weather(cfg, config_mode="LIVE")
    elif case == "another account":
        cfg.account_login = 111
        weather(cfg, account_login=222)
    elif case == "lease stale":
        now = NOW + dt.timedelta(seconds=181)
        weather(cfg, at=now)
    elif case == "policy broken":
        (data / "coach" / "policy.json").write_text("{")
    elif case == "envelope GUARD":
        sha = put_envelope(cfg, mode="GUARD")
        put_policy(cfg, make_policy(sha, seq=2, demo=DEMO, entries={}))
    elif case == "envelope WATCH":
        sha = put_envelope(cfg, mode="WATCH")
        put_policy(cfg, make_policy(sha, seq=2, demo=DEMO, entries={}))
    elif case == "COACH-OFF":
        (data / "COACH-OFF").write_text("YES")
    elif case == "HALT":
        (data / "HALT").write_text("YES")
    elif case == "no demo block":
        sha = put_envelope(cfg)
        put_policy(cfg, make_policy(sha, seq=2, entries={}))
    elif case == "not governed":
        sha = put_envelope(cfg, govern=["runner", "ian"])
        put_policy(cfg, make_policy(sha, seq=2, demo=DEMO, entries={}))
    lim = g.effective_limits(now)
    assert not lim.demo and lim == Limits.of(cfg.risk, lim.why), case
    assert lim.lifted(cfg.risk) == {}


def test_a_value_is_only_ever_lifted(tmp_path):
    cfg, g = demo_gate(tmp_path, demo=DemoLimits(max_daily_loss_pct=3.0, max_drawdown_pct=5.0,
                                                 max_total_risk_pct=1.0))
    lim = g.effective_limits(NOW)
    assert lim.max_daily_loss_pct == 3.0 and lim.max_drawdown_pct == cfg.risk.max_drawdown_pct
    assert lim.max_total_risk_pct == cfg.risk.max_total_risk_pct and lim.lifted(cfg.risk) == {}


def test_the_envelope_bounds_the_demo_ladder(tmp_path):
    from mintel.coach import policy as P
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg, demo_max_daily_stop_pct=5.0, demo_max_step_pct=2.0)
    with pytest.raises(P.PolicyError):
        put_policy(cfg, make_policy(sha, demo=DemoLimits(max_daily_loss_pct=6.0), entries={}))
    with pytest.raises(P.PolicyError):
        put_policy(cfg, make_policy(sha, demo=DemoLimits(max_risk_pct=2.5), entries={}))
    put_policy(cfg, make_policy(sha, demo=DemoLimits(max_daily_loss_pct=5.0, max_risk_pct=2.0), entries={}))
    # the drawdown breaker goes to 25% only once the daily stop is 6% or more
    sha = put_envelope(cfg)
    with pytest.raises(P.PolicyError):
        put_policy(cfg, make_policy(sha, seq=2, demo=DemoLimits(max_daily_loss_pct=5.0, max_drawdown_pct=25.0),
                                    entries={}))
    put_policy(cfg, make_policy(sha, seq=2, demo=DemoLimits(max_daily_loss_pct=6.0, max_drawdown_pct=25.0),
                                entries={}))


# ================================================================ the daily-loss check --
def account(equity):
    return AccountInfo(login=1, server="X-Demo", currency="GBP", balance=10_000.0, equity=equity, margin=0.0,
                       margin_free=equity, margin_level=0.0, leverage=30, trade_allowed=True, is_demo=True)


def test_the_daily_loss_check_reads_the_effective_limit(tmp_path):
    cfg, g = demo_gate(tmp_path)
    rm = RiskManager(cfg)
    rm.realised_today = lambda: -350.0                                     # 3.5% of 10,000 lost today
    plain = rm.snapshot(account(9_650.0), [], NOW)
    assert "DAILY_LOSS" in {b.name for b in plain.breakers}               # config.json's 3%
    rm.limits = lambda: g.effective_limits(NOW)
    lifted = rm.snapshot(account(9_650.0), [], NOW)
    assert "DAILY_LOSS" not in {b.name for b in lifted.breakers}          # the demo's 6%
    rm.realised_today = lambda: -650.0
    s = rm.snapshot(account(9_350.0), [], NOW)
    assert any(b.name == "DAILY_LOSS" and "limit 6.00%" in b.reason for b in s.breakers)
    # the account check goes stale: back to 3% at once
    weather(cfg, at=NOW - dt.timedelta(minutes=10))
    rm.realised_today = lambda: -350.0
    assert "DAILY_LOSS" in {b.name for b in rm.snapshot(account(9_650.0), [], NOW).breakers}


def test_the_default_path_is_byte_identical(tmp_path):
    cfg = coach_cfg(tmp_path)
    g = G.CoachGate("tnb", cfg.ops.data_dir, cfg, clock=lambda: NOW)       # no envelope: off
    a, b = RiskManager(cfg), RiskManager(cfg)
    b.limits = g.effective_limits
    for rm in (a, b):
        rm.realised_today = lambda: -310.0
    assert a.effective_risk() is cfg.risk and b.effective_risk() is cfg.risk
    assert a.snapshot(account(9_690.0), [], NOW) == b.snapshot(account(9_690.0), [], NOW)
    b.limits = lambda: 1 / 0                                               # broken: config.json's own
    assert b.effective_risk() is cfg.risk


def test_the_trader_writes_the_account_check_and_reads_the_lift(tmp_path):
    def coach(cfg, sim):
        put_policy(cfg, full_policy(cfg, sim, demo=DEMO, entries={}), now=sim.now)
    sim, cfg, pb, tr = build(tmp_path, prepare=coach)
    assert tr.risk.limits == tr.coach_gate.effective_limits
    assert tr.risk.effective_risk().max_daily_loss_pct == 3.0               # no account check written yet
    tr.cycle()
    assert tr.risk.effective_risk().max_daily_loss_pct == 6.0
    assert pb.ceiling_pct() == 2.0
    tr.journal.close()
    pb.close_db()
    # the same on a real-money account: nothing is lifted
    def live_account(cfg, sim):
        sim.is_demo = False
        sim.server = "SimBroker-Live"
        coach(cfg, sim)
    sim, cfg, pb, tr = build(tmp_path / "live", prepare=live_account)
    tr.cycle()                     # a live account on DEMO settings: the trader's own check stops it (safe mode)
    w = Path(cfg.ops.data_dir) / "coach" / "weather.json"
    assert not w.exists() or json.loads(w.read_text())["account_demo"] is False
    tr._write_weather(sim.now, [], sim.account())                         # even when it is written
    assert json.loads(w.read_text())["account_demo"] is False
    assert tr.risk.effective_risk() is cfg.risk and pb.ceiling_pct() == 1.5
    tr.journal.close()
    pb.close_db()


def test_a_demo_step_sizes_above_the_v1_ceiling_only_on_the_demo(tmp_path):
    s5 = {US500_KEY: Entry("REAL", "LIVE", 2.0, 200.0, step="S5")}

    def coach(cfg, sim):
        put_policy(cfg, full_policy(cfg, sim, demo=DEMO, entries=s5, headroom_gbp=None), now=sim.now)
    sim, cfg, pb, tr = build(tmp_path, prepare=coach)
    tr.cycle()                                                             # the account check
    for p in list(sim.positions(TNB)):
        sim.close(p.ticket)
    equity = tr.broker.account().equity
    d = tr.coach_gate.decide("SESSION_EXPANSION", "INDEX", "TREND", "US500", "BUY", 80, 2, sim.now, False,
                             RiskCtx(equity=equity))
    assert d.route == "REAL" and d.demo and d.ceiling_pct == 2.0
    assert equity * 0.015 < d.money_gbp <= equity * 0.02 + 1e-9
    assert d.exposure == (3.0, 6.0)
    # on a real-money account the same lease is clamped to the v1 ceiling
    (Path(cfg.ops.data_dir) / "coach" / "weather.json").write_text("{}")
    d = tr.coach_gate.decide("SESSION_EXPANSION", "INDEX", "TREND", "US500", "BUY", 80, 2, sim.now, False,
                             RiskCtx(equity=equity))
    assert d.route == "REAL" and not d.demo and d.ceiling_pct is None and d.money_gbp <= equity * 0.015 + 1e-9
    tr.journal.close()
    pb.close_db()


def test_config_json_is_never_written_in_a_simulated_week(tmp_path):
    def coach(cfg, sim):
        put_policy(cfg, full_policy(cfg, sim, demo=DEMO), now=sim.now)
        cfg.save(Path(cfg.ops.data_dir) / "config.json")
    sim, cfg, pb, tr = build(tmp_path, prepare=coach)
    data = Path(cfg.ops.data_dir)
    watched = {n: hashlib.sha256((data / n).read_bytes()).hexdigest()
               for n in ("config.json", "secrets.json", "coach.json")}
    for day in range(7):
        for _ in range(4):
            sim.advance(30)
            tr.cycle()
        sim.advance(1200)
    for n, digest in watched.items():
        assert hashlib.sha256((data / n).read_bytes()).hexdigest() == digest, n
    assert not (data / "HALT").exists() and not (data / "COACH-OFF").exists()
    written = {p.relative_to(data / "coach").parts[0] for p in (data / "coach").rglob("*") if p.is_file()}
    assert written <= {"policy.json", "policy.prev.json", "weather.json", "gate"}
    tr.journal.close()
    pb.close_db()
