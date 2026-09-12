"""Configuration safety (demo/live) and the operator-facing pages."""
from __future__ import annotations

import datetime as dt
import json

import pytest

from mintel.config import Config, LIVE_MARKER
from mintel.ops.dashboard import (DashboardState, build_results,
                                  render_results, render_status)

UTC = dt.timezone.utc


class TestLiveSwitch:
    def test_default_is_demo(self):
        assert Config().effective_mode == "DEMO"

    def test_live_requires_both_mode_and_marker(self):
        c = Config()
        c.mode = "LIVE"
        assert c.effective_mode == "DEMO"
        assert c.validate(), "and it must complain loudly"
        c.live_marker = LIVE_MARKER
        assert c.is_live and not c.validate()

    def test_a_wrong_marker_does_not_enable_live(self):
        c = Config()
        c.mode = "LIVE"
        c.live_marker = "yes please"
        assert not c.is_live

    def test_a_restart_cannot_silently_become_live(self, workdir):
        c = Config()
        c.mode = "LIVE"
        c.live_marker = LIVE_MARKER
        path = workdir / "config.json"
        c.save(path)
        # Someone edits the marker out; the mode alone must not be enough.
        raw = json.loads(path.read_text())
        raw["live_marker"] = ""
        path.write_text(json.dumps(raw))
        assert Config.load(path).effective_mode == "DEMO"

    def test_deliberate_live_configuration_survives_a_round_trip(self, workdir):
        c = Config()
        c.mode = "LIVE"
        c.live_marker = LIVE_MARKER
        c.account_login = 12345
        c.account_password = "secret"
        path = workdir / "config.json"
        c.save(path)
        loaded = Config.load(path)
        assert loaded.is_live
        assert loaded.account_login == 12345
        assert loaded.account_password == "secret"

    def test_passwords_are_not_written_into_the_main_config(self, workdir):
        c = Config()
        c.account_password = "topsecret"
        path = workdir / "config.json"
        c.save(path)
        assert "topsecret" not in path.read_text()
        assert "topsecret" in (workdir / "secrets.json").read_text()

    def test_api_keys_are_kept_out_of_the_main_config(self, workdir):
        c = Config()
        c.news.api_keys = {"provider": "abc123"}
        path = workdir / "config.json"
        c.save(path)
        assert "abc123" not in path.read_text()
        assert Config.load(path).news.api_keys["provider"] == "abc123"

    def test_environment_overrides_the_stored_password(self, workdir,
                                                       monkeypatch):
        c = Config()
        c.account_password = "stored"
        path = workdir / "config.json"
        c.save(path)
        monkeypatch.setenv("MINTEL_PASSWORD", "from_env")
        assert Config.load(path).account_password == "from_env"


class TestConfigValidation:
    def test_risk_ceiling_must_exceed_base(self):
        c = Config()
        c.risk.max_risk_pct = 0.1
        c.risk.base_risk_pct = 1.0
        assert any("max_risk_pct" in e for e in c.validate())

    def test_absurd_risk_is_refused_outright(self):
        c = Config()
        c.risk.max_risk_pct = 50.0
        assert any("refused" in e for e in c.validate())

    def test_tiers_must_increase(self):
        c = Config()
        c.scan.tier_strong = c.scan.tier_normal - 5
        assert any("tiers" in e for e in c.validate())

    def test_empty_universe_is_refused(self):
        c = Config()
        c.universe.groups = ()
        assert any("universe" in e for e in c.validate())

    def test_unknown_aggression_is_refused(self):
        c = Config()
        c.aggression = "RECKLESS"
        assert any("aggression" in e for e in c.validate())

    def test_missing_file_yields_safe_defaults(self, workdir):
        c = Config.load(workdir / "nope.json")
        assert c.effective_mode == "DEMO"
        assert not c.validate()

    def test_partially_corrupt_sections_fall_back_to_defaults(self, workdir):
        path = workdir / "config.json"
        path.write_text(json.dumps({"risk": "not an object",
                                    "aggression": "AGGRESSIVE"}))
        c = Config.load(path)
        assert c.aggression == "AGGRESSIVE"
        assert c.risk.max_risk_pct == Config().risk.max_risk_pct

    def test_aggression_never_raises_the_ceiling(self):
        for a in ("CONSERVATIVE", "NORMAL", "AGGRESSIVE", "MAXIMUM"):
            c = Config()
            c.aggression = a
            assert c.aggression_scale() > 0
            assert c.risk.max_risk_pct == Config().risk.max_risk_pct


class TestDashboardPages:
    def _snapshot(self, journal) -> dict:
        state = DashboardState()
        state.update(
            status={"bot": "RUNNING", "mt5_connected": True,
                    "broker_connected": True, "data_live": True,
                    "news_live": True, "watchdog_ok": True, "mode": "DEMO",
                    "open_positions": 1, "equity": 10_120.0,
                    "currency": "GBP", "today_pnl": 120.0,
                    "win_rate_today": 66.0, "last_scan": "13:30:05 UTC",
                    "last_trade": "EURUSD 13:22"},
            health={"safe_mode": False, "entries_allowed": True,
                    "summary": "everything is healthy", "checks": []},
            thinking=[{"rank": 1, "symbol": "GBPUSD", "direction": "SHORT",
                       "score": 93.0, "tier": "EXCEPTIONAL",
                       "tactic": "MOMENTUM_CONTINUATION",
                       "regime": "TREND / HIGH VOL", "reason": "x",
                       "blockers": []},
                      {"rank": 2, "symbol": "XAUUSD", "direction": "SHORT",
                       "score": 89.0, "tier": "STRONG",
                       "tactic": "BREAKOUT_RETEST", "regime": "TREND",
                       "reason": "y", "blockers": ["spread 3.1x normal"]}],
            results=build_results(journal, "GBP",
                                  dt.datetime.now(UTC)),
            positions=[{"symbol": "EURUSD", "side": "BUY", "volume": 0.5,
                        "entry": 1.1, "stop": 1.098, "profit": 42.0,
                        "flow_state": "STRONG_FLOW", "note": "best 2.1R"}],
            events=[])
        return state.snapshot()

    def test_status_page_shows_the_headline_state(self, journal):
        html = render_status(self._snapshot(journal))
        assert "BOT: RUNNING" in html
        assert "GBPUSD" in html and "EXCEPTIONAL" in html
        assert "Open trades" in html
        assert "spread 3.1x normal" in html, (
            "the operator must see why a good idea is not being traded")

    def test_problems_are_shown_in_plain_english(self, journal):
        snap = self._snapshot(journal)
        snap["status"]["mt5_connected"] = False
        snap["health"] = {"safe_mode": True, "entries_allowed": False,
                          "summary": "SAFE MODE - MetaTrader 5 is not "
                                     "connected to the broker",
                          "checks": [{"name": "BROKER_CONNECTION",
                                      "severity": "FAIL",
                                      "message": "MetaTrader 5 is not "
                                                 "connected to the broker",
                                      "detail": {}}]}
        html = render_status(snap)
        assert "BOT: PROBLEM" in html
        assert "What is wrong" in html
        assert "MetaTrader 5 is not connected" in html
        assert "NOT CONNECTED" in html

    def test_live_mode_is_visually_obvious(self, journal):
        snap = self._snapshot(journal)
        snap["status"]["mode"] = "LIVE"
        html = render_status(snap)
        assert 'pill bad">LIVE' in html

    def test_results_page_explains_every_trade(self, journal):
        from mintel.broker.base import Position, Side
        from mintel.data.regime import Regime
        from mintel.engine.evidence import Evidence, Family, MarketState
        now = dt.datetime.now(UTC)
        state = MarketState(
            symbol="GBPUSD", as_of=now, regime=Regime.TREND,
            regime_label="TREND / HIGH VOL", direction=-1,
            tactic="MOMENTUM_CONTINUATION",
            evidence={Family.STRUCTURE: Evidence(Family.STRUCTURE, 94.0),
                      Family.MOMENTUM: Evidence(Family.MOMENTUM, 96.0),
                      Family.NEWS: Evidence(Family.NEWS, 88.0, 0.9,
                                            "US CPI above forecast")},
            raw_score=93.0, opportunity=93.0, tier="EXCEPTIONAL",
            entry=1.2700, stop=1.2730, target=1.2640, reward_risk=2.0,
            headroom_pips=37.0, cost_pips=1.1)
        p = Position(1, "GBPUSD", Side.SELL, 0.5, 1.2700, 1.2730, 1.2640, now)
        journal.open_trade(p, state, "k", 1.2, 120.0, 10_000, "GBP",
                           "AGGRESSIVE", "DEMO")
        journal.close_trade(1, exit_price=1.2650, closed_utc=now,
                            pnl_money=250.0, pnl_pips=50.0,
                            exit_reason="FlowLock decay - momentum died",
                            stats={"realised_r": 1.67, "mfe_r": 2.1,
                                   "mae_r": 0.3, "mfe_capture_pct": 79.0,
                                   "final_state": "DECAY"},
                            learned="Kept 79% of the best price it saw.")
        results = build_results(journal, "GBP", now)
        assert results["trades"] == 1 and results["wins"] == 1
        story = results["trade_stories"][0]
        for key in ("why_it_entered", "what_happened", "why_it_exited",
                    "what_it_learned"):
            assert len(story[key]) > 15, key
        assert "trend" in story["why_it_entered"]
        html = render_results(results)
        assert "Why it entered" in html
        assert "What it learned" in html
        assert "Best trade" in html

    def test_results_page_with_no_trades_is_still_valid(self, journal):
        results = build_results(journal, "GBP", dt.datetime.now(UTC))
        assert results["trades"] == 0
        html = render_results(results)
        assert "No completed trades today yet" in html

    def test_pages_escape_untrusted_text(self, journal):
        snap = self._snapshot(journal)
        snap["thinking"][0]["symbol"] = "<script>alert(1)</script>"
        html = render_status(snap)
        assert "<script>alert(1)</script>" not in html
        assert "&lt;script&gt;" in html


class TestDashboardServer:
    def test_server_serves_all_three_pages(self, journal):
        import urllib.request
        from mintel.ops.dashboard import start_dashboard
        state = DashboardState()
        state.update(status={"bot": "RUNNING"},
                     health={"entries_allowed": True, "checks": [],
                             "safe_mode": False, "summary": "ok"},
                     results=build_results(journal, "GBP"))
        httpd = start_dashboard(state, "127.0.0.1", 0)
        port = httpd.server_address[1]
        try:
            for path, needle in (("/", "BOT:"), ("/results", "Today"),
                                 ("/health", "entries_allowed")):
                with urllib.request.urlopen(
                        f"http://127.0.0.1:{port}{path}", timeout=5) as r:
                    body = r.read().decode()
                assert needle in body
        finally:
            httpd.shutdown()

    def test_health_endpoint_reports_503_when_unhealthy(self, journal):
        import urllib.error
        import urllib.request
        from mintel.ops.dashboard import start_dashboard
        state = DashboardState()
        state.update(health={"entries_allowed": False, "checks": [],
                             "safe_mode": True, "summary": "SAFE MODE"})
        httpd = start_dashboard(state, "127.0.0.1", 0)
        port = httpd.server_address[1]
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/health",
                                       timeout=5)
            assert exc.value.code == 503
        finally:
            httpd.shutdown()
