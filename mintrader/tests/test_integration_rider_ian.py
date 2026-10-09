"""9 Oct: the Rapid Momentum Rider and Financial Ian join the running system.

The watchdog looks after both as their own processes (and no longer starts
the retired Rapid Scalper); the status page has a page for each, read-only
from their own files, that renders before they ever ran; push_dashboard and
the attribution carry their figures; the Mac and Windows scripts switch the
line-up once, start nothing twice, and print each bot's health in one line.

Every figure here is SYNTHETIC test data written by the test itself: nothing
in this file measures performance."""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

from mintel.config import Config
from mintel.ops import watchdog as wd
from mintel.ops.dashboard import (DashboardState, render_ian_page, render_rider_page, render_status,
                                  start_dashboard)

ROOT = Path(__file__).resolve().parents[1]
MAC = ROOT / "deploy" / "mac"
WIN = ROOT / "deploy"
UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 9, 10, 0, tzinfo=UTC)


def _cfg(tmp_path: Path) -> Config:
    c = Config()
    c.ops.data_dir = str(tmp_path / "data")
    c.ops.log_dir = str(tmp_path / "logs")
    Path(c.ops.data_dir).mkdir(parents=True, exist_ok=True)
    return c


def _write(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj))


# ================================================================ watchdog --
class TestTheWatchdog:
    def _build(self, tmp_path, **files):
        c = _cfg(tmp_path)
        for name, obj in files.items():
            _write(Path(c.ops.data_dir) / f"{name}.json", obj)
        return c, wd.build_default(c, str(Path(c.ops.data_dir) / "config.json"), python="/py")

    def test_the_rider_and_ian_are_looked_after_and_the_scalper_never_is(self, tmp_path):
        c, w = self._build(tmp_path, scalper={"mode": "PAPER"})
        names = [p.name for p in w.processes]
        assert names[0] == "trader" and "rider" in names and "ian" in names and "scalper" not in names
        data, logs = Path(c.ops.data_dir), Path(c.ops.log_dir)
        cfg_path = str((data / "config.json").resolve())
        by = {p.name: p for p in w.processes}
        assert list(by["rider"].command) == ["/py", "-m", "mintel.rider.run", "--config", cfg_path]
        assert list(by["ian"].command) == ["/py", "-m", "mintel.ian.run", "--config", cfg_path]
        assert (by["rider"].heartbeat, by["ian"].heartbeat) == ("rider", "ian")
        assert by["rider"].pid_file == str(data / "rider.pid") and by["ian"].pid_file == str(data / "ian.pid")
        assert by["rider"].log_file == str(logs / "rider.out.log") and by["ian"].log_file == str(logs / "ian.out.log")
        assert by["rider"].is_enabled() and by["ian"].is_enabled()           # no file: the bots' default, PAPER

    def test_a_bot_whose_file_says_off_is_never_started_and_one_switched_on_is(self, tmp_path, monkeypatch):
        c, w = self._build(tmp_path, rider={"mode": "OFF"}, ian={"mode": "PAPER", "feed": {"vendor": "none"}})
        started: list[str] = []

        def fake_start(self, now, env=None):
            started.append(self.name)
            self.last_start = now
            return True, f"{self.name}: started"
        monkeypatch.setattr(wd.ManagedProcess, "start", fake_start)
        monkeypatch.setattr(wd.ManagedProcess, "alive_by_pid", lambda self: None)
        w.clock = lambda: NOW
        w.check_once()
        assert "rider" not in started and "ian" in started and "trader" in started
        # switched on in its own file: started at the next check, nothing restarted for it
        _write(Path(c.ops.data_dir) / "rider.json", {"mode": "LIVE", "user_pip_value_gbp": 1.0})
        started.clear()
        w.clock = lambda: NOW + dt.timedelta(seconds=15)
        w.check_once()
        assert started == ["rider"]

    def test_a_rider_switched_off_with_a_live_trade_open_is_started_to_close_it(self, tmp_path, monkeypatch):
        """SYNTHETIC rows. A LIVE Rider is stopped for a switch to OFF with a
        real trade open. Before the fix the watchdog never started it while
        rider.json said OFF, so nothing ran the OFF pass and the trade sat at
        the broker with only its last stop. Now an open LIVE row in
        rider.sqlite gets the Rider started (its OFF start closes it and
        exits); an open PAPER row or no open row leaves it OFF."""
        from mintel.rider.journal import RiderJournal
        c, w = self._build(tmp_path, rider={"mode": "OFF"}, ian={"mode": "OFF"})
        started: list[str] = []

        def fake_start(self, now, env=None):
            started.append(self.name)
            self.last_start = now
            return True, f"{self.name}: started"
        monkeypatch.setattr(wd.ManagedProcess, "start", fake_start)
        monkeypatch.setattr(wd.ManagedProcess, "alive_by_pid", lambda self: None)
        j = RiderJournal(Path(c.ops.data_dir) / "rider.sqlite")
        try:
            row = {"symbol": "EURUSD", "side": "BUY", "volume": 0.14, "entry": 1.085, "stop": 1.084,
                   "opened_utc": NOW.isoformat()}
            j.insert_trade({"ticket": 810000001, "mode": "PAPER", **row})
            w.clock = lambda: NOW
            w.check_once()
            assert "rider" not in started                                  # practice only: nothing real is open
            j.insert_trade({"ticket": 5001, "mode": "LIVE", **row})
            started.clear()
            w.clock = lambda: NOW + dt.timedelta(seconds=15)
            w.check_once()
            assert started == ["rider"]                                    # started, in OFF, to close it
            j.update_trade(5001, {"closed_utc": NOW.isoformat()})
            by = {p.name: p for p in w.processes}
            assert not by["rider"].is_enabled()                            # booked: left alone again
            _write(Path(c.ops.data_dir) / "rider.json", {"mode": "PAPER"})
            assert by["rider"].is_enabled()
        finally:
            j.close()

    def test_the_mode_is_read_the_way_the_bots_read_it(self, tmp_path):
        p = tmp_path / "x.json"
        assert wd.own_file_mode(p) == "PAPER"
        for raw, want in (('{"mode": "live"}', "LIVE"), ('{"mode": " OFF "}', "OFF"), ('{"mode": "REAL"}', "PAPER"),
                          ("{broken", "PAPER"), ("[1]", "PAPER"), ('{"mode": null}', "PAPER")):
            p.write_text(raw)
            assert wd.own_file_mode(p) == want, raw
        from mintel.rider.config import RiderConfig
        for raw in ('{"mode": "live"}', '{"mode": "OFF"}', "{broken"):
            p.write_text(raw)
            assert wd.own_file_mode(p) == RiderConfig.load(p).mode

    def test_a_broken_check_never_stops_the_watchdog_looking_after_it(self):
        mp = wd.ManagedProcess(name="x", command=["true"], enabled=lambda: 1 / 0)
        assert mp.is_enabled() is True


# ================================================================ the pages --
def _rider_files(data: Path, now=NOW, mode="LIVE", reason="breaking the 20-second high with the move"):
    from mintel.rider.journal import RiderJournal
    _write(data / "rider.json", {"mode": mode, "user_pip_value_gbp": 1.0})
    _write(data / "rider-status.json", {
        "strategy_id": "momentum_rider", "label": "Rapid Momentum Rider", "tagline": "Gets on early, gets out fast.",
        "mode": mode, "updated_utc": now.isoformat(), "pid": 1234, "user_pip_value_gbp": 1.0,
        "order_flow": "not available on retail MetaTrader FX - never faked",
        "health": {"entries_allowed": True, "summary": "all checks passing",
                   "checks": [{"name": "mt5_alive", "ok": True, "critical": True, "message": "MetaTrader is answering"},
                              {"name": "disk_memory", "ok": False, "critical": False, "message": "120 MB free on disk"}]},
        "scanner": [{"market": "EURUSD", "direction": "NONE", "score": 12.0, "state": "WATCHING", "reason": "quiet"},
                    {"market": "GBPJPY", "direction": "LONG", "score": 71.5, "state": "READY", "reason": reason}],
        "open_positions": [{"ticket": 7, "market": "GBPJPY", "side": "BUY", "volume": 0.07, "entry": 201.5,
                            "stop": 201.42, "pips": 4.2, "money": 4.2, "flowlock_state": "COSTS_COVERED",
                            "flowlock": "costs covered - can no longer lose", "decay": 0.21}],
        "today": {"mode": mode, "trades": 2, "won": 1, "lost": 1, "win_rate": 50.0, "net_pips": 3.4, "net_money": 3.3,
                  "net_money_estimated": False, "money_source": "the broker's figures",
                  "best_trade": "AUDJPY +6.1 pips (+£6.02)"},
        "stories": ["AUDJPY ran up; it got on early and off when the run slowed."],
        "notes": ["GBPJPY: watching a run start"]})
    j = RiderJournal(data / "rider.sqlite")
    j.insert_trade({"ticket": 900001, "mode": mode, "symbol": "AUDJPY", "side": "BUY", "volume": 0.07,
                    "opened_utc": (now - dt.timedelta(minutes=30)).isoformat(),
                    "closed_utc": (now - dt.timedelta(minutes=20)).isoformat(), "realised_pips": 6.1,
                    "net_money": 6.02, "money_source": "broker", "exit_reason": "TRAIL",
                    "explanation": "AUDJPY ran up; it got on early and off when the run slowed."})
    j.insert_trade({"ticket": 900002, "mode": mode, "symbol": "EURUSD", "side": "SELL", "volume": 0.1,
                    "opened_utc": (now - dt.timedelta(minutes=15)).isoformat(),
                    "closed_utc": (now - dt.timedelta(minutes=10)).isoformat(), "realised_pips": -2.7,
                    "net_money": -2.72, "money_source": "estimate", "exit_reason": "STOP",
                    "explanation": "EURUSD turned back <at once>; the stop took it out."})
    j.close()


def _ian_status(data: Path, now=NOW, state="NOT CONFIGURED", ladder=None, **extra):
    st = {"strategy_id": "financial_ian", "label": "Financial Ian", "tagline": "Reads the futures book.",
          "mode": "PAPER", "running": True, "updated": now.isoformat(),
          "status": "DATA-DEGRADED: no institutional feed is configured - no signals, no trades.",
          "data_source": "LIVE FEED", "data_label": "LIVE",
          "feed": {"state": state, "reason": "no feed configured", "vendor": "none", "synthetic": False},
          "mt5": {"connected": True}, "top_opportunity": None, "reasoning": "", "opportunities": {},
          "open_positions": [], "today": {"trades": 0, "wins": 0, "win_rate": None, "net": 0.0,
                                          "data_source": "LIVE FEED"},
          "last_trade": None, "ladder": ladder}
    st.update(extra)
    _write(data / "ian-status.json", st)


LADDER = {"instrument": "6EZ6", "data_class": "INSTITUTIONAL", "tick": 0.00005, "bid": 1.16, "ask": 1.16005,
          "mid": 1.160025, "imbalance": 0.42, "pressure": "buyers (book +0.42, aggression +0.30)",
          "rows": [{"price": 1.1601, "ask": 40, "bid": 0, "bought": 3, "sold": 0, "delta": 3, "flags": ["WALL"]},
                   {"price": 1.16005, "ask": 12, "bid": 0, "bought": 9, "sold": 1, "delta": 8, "flags": []},
                   {"price": 1.16, "ask": 0, "bid": 25, "bought": 0, "sold": 4, "delta": -4, "flags": ["POC"]},
                   {"price": 1.15995, "ask": 0, "bid": 80, "bought": 0, "sold": 0, "delta": 0,
                    "flags": ["<b>odd</b>"]}]}


class TestTheRiderPage:
    def test_it_renders_before_the_rider_ever_ran(self, tmp_path):
        assert "Not started yet" in render_rider_page("", NOW)
        page = render_rider_page(str(tmp_path), NOW)
        assert "RAPID MOMENTUM RIDER" in page and "Not started yet" in page and "No trades today yet" in page
        _write(tmp_path / "rider.json", {"mode": "OFF"})
        assert "OFF: data/rider.json says OFF" in render_rider_page(str(tmp_path), NOW)

    def test_today_the_stories_the_scanner_the_flowlock_and_the_health(self, tmp_path):
        _rider_files(tmp_path, reason="<script>alert(1)</script>")
        page = render_rider_page(str(tmp_path), NOW)
        for needle in ('<span class="pill ok">HEALTHY</span>', "GBP 1.00 per pip",
                       '<div class="k">Trades</div><div class="v ">2</div>', '<div class="k">Won</div>',
                       '<div class="k">Lost</div>', '<div class="k">Win rate</div><div class="v ">50%</div>',
                       '<div class="k">Net pips</div>', "+3.4", '<div class="k">Net money</div>', "+£3.30",
                       '<div class="k">Best trade</div>', "AUDJPY +6.1 pips",
                       "AUDJPY ran up; it got on early", "(estimate)", "COSTS_COVERED",
                       "costs covered - can no longer lose", "momentum decay 0.21",
                       "<th>Market</th><th>Direction</th><th>Score</th><th>State</th><th>Reason</th>",
                       "MetaTrader is answering", "120 MB free on disk", "the broker&#x27;s figures"):
            assert needle in page, needle
        assert page.index("GBPJPY</b></td><td>LONG") < page.index("EURUSD</b></td><td>NONE")   # best score first
        assert "<script>alert(1)</script>" not in page and "&lt;script&gt;" in page
        assert "&lt;at once&gt;" in page

    def test_a_stopped_rider_says_so(self, tmp_path):
        _rider_files(tmp_path, now=NOW - dt.timedelta(minutes=30))
        page = render_rider_page(str(tmp_path), NOW)
        assert "NOT RUNNING</span> last report 30 min ago" in page

    def test_a_mode_change_waiting_for_a_restart_is_named(self, tmp_path):
        _rider_files(tmp_path, mode="PAPER")
        _write(tmp_path / "rider.json", {"mode": "LIVE"})
        assert "running PAPER; data/rider.json says LIVE" in render_rider_page(str(tmp_path), NOW)

    def test_without_a_status_file_today_comes_from_its_records(self, tmp_path):
        _rider_files(tmp_path)
        (tmp_path / "rider-status.json").unlink()
        page = render_rider_page(str(tmp_path), NOW)
        assert "the Rider&#x27;s own records since 00:00 UK" in page          # the UK day, as every page
        assert '<div class="k">Trades</div><div class="v ">2</div>' in page and "+£3.30" in page


class TestTheIanPage:
    def test_it_renders_before_ian_ever_ran(self, tmp_path):
        assert "Not started yet" in render_ian_page("", NOW)
        page = render_ian_page(str(tmp_path), NOW)
        assert "FINANCIAL IAN" in page and "Not started yet" in page and "DATA-DEGRADED" in page

    def test_no_feed_is_said_plainly(self, tmp_path):
        _ian_status(tmp_path)
        page = render_ian_page(str(tmp_path), NOW)
        for needle in ('<span class="pill ok">YES</span>', '<span class="pill warn">NOT CONFIGURED</span>',
                       '<span class="pill ok">CONNECTED</span>', "DATA-DEGRADED: no institutional feed",
                       "None yet: it needs the institutional order book", "No order book",
                       "Today&#x27;s profit", "Win rate", "Last trade", "none yet"):
            assert needle in page, needle

    def test_the_ladder_asks_above_bids_below_with_the_pressure(self, tmp_path):
        top = {"instrument": "6EZ6", "root": "6E", "spot_symbol": "EURUSD", "spot_side": "BUY", "score": 74.0,
               "regime": {"regime": "ABSORPTION_BID"}, "tradeable": True, "why_not": "",
               "reasoning": "Sellers are hitting the 6E bid and it is not giving way."}
        _ian_status(tmp_path, state="LIVE", ladder=LADDER, top_opportunity=top, reasoning=top["reasoning"],
                    open_positions=[{"symbol": "EURUSD", "future": "6E", "side": "BUY", "volume": 0.1, "entry": 1.16,
                                     "stop": 1.159, "r_now": 0.4, "best_r": 0.6, "pnl": 3.1, "trail": "held",
                                     "reasoning": "absorption"}],
                    today={"trades": 3, "wins": 2, "win_rate": 0.667, "net": 5.5, "data_source": "LIVE FEED"},
                    last_trade={"spot_symbol": "GBPUSD", "side": "SELL", "entry": 1.3, "exit_price": 1.299,
                                "net_pnl": 2.2, "realised_r": 0.8, "mode": "PAPER", "data_source": "LIVE FEED",
                                "exit_explanation": "the sellers stopped"})
        page = render_ian_page(str(tmp_path), NOW)
        assert '<span class="pill ok">LIVE</span>' in page and "Sellers are hitting the 6E bid" in page
        assert '<div class="k">Win rate</div><div class="v ">67%</div>' in page and "+£5.50" in page
        assert "GBPUSD SELL +£2.20" in page and "the sellers stopped" in page
        lad = page[page.index('<table class="lad">'):]
        a1, a2, mid, b1, b2 = (lad.index(x) for x in ("1.1601<", "1.16005<", 'class="mid"', ">1.16<", "1.15995<"))
        assert a1 < a2 < mid < b1 < b2                                       # asks above, bids below
        assert "pressure &#9650; buyers" in lad and "book imbalance +0.42" in lad
        assert 'class="bar ask"' in lad and 'class="bar bid"' in lad and 'class="bar buy"' in lad
        assert "&lt;b&gt;odd&lt;/b&gt;" in lad and "<b>odd</b>" not in lad
        assert "WALL" in lad and "POC" in lad

    def test_a_synthetic_run_is_never_shown_as_a_result(self, tmp_path):
        _ian_status(tmp_path, state="LIVE", data_label="SYNTHETIC - NOT PERFORMANCE")
        page = render_ian_page(str(tmp_path), NOW)
        assert "SYNTHETIC - NOT PERFORMANCE" in page and "nothing here is a result" in page

    def test_a_broken_status_file_is_said_not_guessed(self, tmp_path):
        (tmp_path / "ian-status.json").write_text("{half")
        assert "Its status file it could not be read" in render_ian_page(str(tmp_path), NOW)


class TestTheServer:
    def test_the_pages_are_served_and_linked(self, tmp_path):
        import urllib.request
        _rider_files(tmp_path)
        _ian_status(tmp_path)
        state = DashboardState()
        state.update(status={"bot": "RUNNING"}, health={"checks": []}, data_dir=str(tmp_path))
        httpd = start_dashboard(state, "127.0.0.1", 0)
        try:
            port = httpd.server_address[1]
            get = lambda path: urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10).read().decode()  # noqa: E731
            rider, ian, main = get("/rider"), get("/ian"), get("/")
            assert "RAPID MOMENTUM RIDER" in rider and "GBPJPY" in rider
            assert "FINANCIAL IAN" in ian and "NOT CONFIGURED" in ian
            for page in (rider, ian, main, get("/results")):
                assert '<a href="/rider">Rapid Momentum Rider</a>' in page and '<a href="/ian">Financial Ian</a>' in page
            state.update(data_dir="")
            assert "Not started yet" in get("/rider") and "Not started yet" in get("/ian")
        finally:
            httpd.shutdown()


# ============================================================ attribution --
class TestTheFiguresBehindTheCards:
    def test_the_riders_live_money_counts_and_its_estimate_is_flagged(self, tmp_path):
        from mintel.ops.attribution import build_strategies
        _rider_files(tmp_path)

        class J:
            def closed_trades(self, limit=500, since=None):
                return []
        s = build_strategies(J(), [], NOW - dt.timedelta(hours=2), "GBP", tmp_path, None, now=NOW)
        rd = s["momentum_rider"]
        assert rd["trades"] == 2 and rd["live_net"] == pytest.approx(3.3) and rd["mode"] == "LIVE"
        assert rd["estimated_trades"] == 1 and rd["estimated_net"] == -2.72 and rd["net_pips"] == 3.4
        assert rd["unrealised"] == 4.2                                        # its open position, from its status
        assert s["overall"]["net_today"] == pytest.approx(3.3 + 4.2)
        assert s["overall"]["estimated_trades"] == 1
        assert {b["id"] for b in s["overall"]["by_strategy"]} >= {"momentum_rider"}
        assert s["labels"]["momentum_rider"] == "Rapid Momentum Rider" and s["rider"]["present"] is True
        assert s["labels"]["financial_ian"] == "Financial Ian" and s["ian"]["present"] is False

    def test_a_paper_rider_is_practice_and_a_switched_row_is_not_a_trade(self, tmp_path):
        from mintel.ops.attribution import build_strategies_range
        from mintel.ops.standing import practice_figures
        _rider_files(tmp_path, mode="PAPER")
        con = sqlite3.connect(tmp_path / "rider.sqlite")
        con.execute("INSERT INTO trades (ticket, mode, symbol, closed_utc, net_money, exit_reason) VALUES "
                    "(900003, 'PAPER', 'USDJPY', ?, 0.0, 'SWITCHED TO LIVE')", ((NOW - dt.timedelta(minutes=5)).isoformat(),))
        con.commit()
        con.close()
        s = build_strategies_range(tmp_path, NOW - dt.timedelta(hours=3), NOW + dt.timedelta(hours=1), "GBP",
                                   label="Today", now=NOW)
        assert s["momentum_rider"]["trades"] == 2 and s["momentum_rider"]["paper_net"] == pytest.approx(3.3)
        assert s["overall"]["trades"] == 0                                     # practice is never in Overall
        p = practice_figures(tmp_path, NOW - dt.timedelta(hours=3), NOW, "GBP", end=NOW + dt.timedelta(hours=1))
        assert p["momentum_rider"] == pytest.approx(3.3)

    def test_a_synthetic_or_replayed_ian_row_is_never_a_result(self, tmp_path):
        from mintel.ian.journal import Journal as IanJournal
        from mintel.ops.attribution import build_strategies_range
        IanJournal(tmp_path / "ian.sqlite").close()
        con = sqlite3.connect(tmp_path / "ian.sqlite")
        when = (NOW - dt.timedelta(minutes=30)).isoformat()
        for ticket, src, pnl in ((1, "LIVE FEED", 4.0), (2, "SYNTHETIC", 500.0), (3, "REPLAY", -300.0)):
            con.execute("INSERT INTO trades (ticket, spot_symbol, side, closed_utc, net_pnl, mode, data_source, "
                        "exit_reason) VALUES (?,?,?,?,?,?,?,?)", (ticket, "EURUSD", "BUY", when, pnl, "PAPER", src, "TRAIL"))
        con.commit()
        con.close()
        s = build_strategies_range(tmp_path, NOW - dt.timedelta(hours=3), NOW + dt.timedelta(hours=1), "GBP",
                                   label="Today", now=NOW)
        ia = s["financial_ian"]
        assert ia["trades"] == 1 and ia["paper_net"] == 4.0
        assert [t["symbol"] for t in ia["trades_today"]] == ["EURUSD"]

    def test_the_overall_note_names_the_new_bots_once_they_are_installed(self, tmp_path):
        from mintel.ops.attribution import build_strategies, overall_note

        class J:
            def closed_trades(self, limit=500, since=None):
                return []
        s = build_strategies(J(), [], NOW - dt.timedelta(hours=2), "GBP", tmp_path, None, now=NOW)
        assert "Rapid Momentum Rider" not in s["overall"]["note"] and "Ian" not in s["overall"]["note"]
        _write(tmp_path / "rider.json", {"mode": "LIVE"})
        _write(tmp_path / "ian.json", {"mode": "PAPER"})
        _write(tmp_path / "scalper-status.json", {"updated": (NOW - dt.timedelta(hours=5)).isoformat(), "mode": "PAPER"})
        s = build_strategies(J(), [], NOW - dt.timedelta(hours=2), "GBP", tmp_path, None, now=NOW)
        note = s["overall"]["note"]
        assert "Financial Ian are in PAPER" in note and "the Financial Ian" not in note
        assert "Rapid Scalper" not in note                                  # retired and not running: not named
        assert overall_note({"Financial Ian": "OFF"}) == "Financial Ian is OFF."

    def test_the_retired_scalper_tab_only_shows_while_it_has_something(self):
        from mintel.scalper.stats import strategy_stats
        rs = strategy_stats([], [], label="Rapid Scalper", strategy_id="rapid_scalper", scalper=True)
        base = {"overall": {"currency": "GBP"}, "rapid_scalper": rs, "momentum_rider": {"currency": "GBP"},
                "labels": {"overall": "Overall", "rapid_scalper": "Rapid Scalper", "momentum_rider": "Rapid Momentum Rider"},
                "scalper": {"status": "NOT RUNNING", "present": False}}
        page = render_status({"status": {"bot": "RUNNING"}, "health": {}, "strategies": base})
        assert "strategy=momentum_rider" in page and "strategy=rapid_scalper" not in page
        assert "RAPID SCALPER" not in page
        rs2 = dict(rs, trades=1)
        page = render_status({"status": {"bot": "RUNNING"}, "health": {}, "strategies": dict(base, rapid_scalper=rs2)})
        assert "strategy=rapid_scalper" in page


class TestPushDashboard:
    def test_the_cards_get_the_rider_and_ian_lines_and_tabs(self, trader, cfg):
        from mintel.run import push_dashboard
        data = Path(cfg.ops.data_dir)
        now = trader.clock()
        _rider_files(data, now=now)
        _ian_status(data, now=now)
        state = DashboardState()
        push_dashboard(state, trader)
        snap = state.snapshot()
        assert snap["bot_lines"]["momentum_rider"].startswith("HEALTHY, LIVE, scanning 2 markets")
        assert snap["bot_lines"]["financial_ian"].startswith("HEALTHY, PAPER, feed NOT CONFIGURED")
        st = snap["strategies"]
        assert st["momentum_rider"]["trades"] == 2 and st["rider"]["mode"] == "LIVE" and st["ian"]["present"]
        assert state.data_dir == str(data)


# ======================================================== the Mac scripts --
NEW_MAC = ("SETUP-RAPID-RIDER.command", "START-RAPID-RIDER.command", "INSTALL-FINANCIAL-IAN.command",
           "START-FINANCIAL-IAN.command")


def _bash(snippet: str, home: Path, timeout=120):
    env = {**os.environ, "MINTEL_HOME": str(home)}
    return subprocess.run(["bash", "-c", f'set -uo pipefail; source "{MAC / "mintel_mac.sh"}"; {snippet}'],
                          capture_output=True, text=True, env=env, timeout=timeout)


def _home(tmp_path: Path) -> Path:
    """A MarketBot folder whose app is this checkout and whose Python is this one."""
    home = tmp_path / "MarketBot"
    (home / "data").mkdir(parents=True)
    (home / "venv" / "bin").mkdir(parents=True)
    (home / "venv" / "bin" / "python").symlink_to(sys.executable)
    (home / "app").symlink_to(ROOT)
    c = Config()
    c.ops.data_dir = str(home / "data")
    c.ops.log_dir = str(home / "logs")
    c.save(home / "data" / "config.json")
    return home


class TestTheMacScripts:
    def test_every_script_is_valid_bash_32_and_executable(self):
        for name in NEW_MAC + ("mintel_mac.sh", "Start Trading Bot.command"):
            path = MAC / name
            r = subprocess.run(["bash", "-n", str(path)], capture_output=True, text=True)
            assert r.returncode == 0, (name, r.stderr)
            text = path.read_text()
            assert text.startswith("#!/bin/bash"), name
            for bad in (r"\$\{\w+,,", r"\$\{\w+\^\^", r"\bmapfile\b", r"\breadarray\b", r"\bdeclare\s+-A\b", "&>>"):
                assert not re.search(bad, text), (name, bad)
            for line in text.splitlines():
                if not line.strip().startswith("#") and re.search(r'"\$(\d)"', line):
                    assert "${" in line or "local" not in line, (name, line)
            code = "\n".join(l for l in text.splitlines() if not l.strip().startswith("#"))
            assert not re.search(r"^\s*local\s+(\w+)=[^;]*?\s+\w+=[^;]*?\$\{?\1\b", code, re.MULTILINE), name
        for name in NEW_MAC:
            assert os.access(MAC / name, os.X_OK), name

    def test_the_launchers_only_go_through_the_start_machinery(self):
        for name in NEW_MAC:
            text = (MAC / name).read_text()
            assert 'MINTEL_NO_PAUSE=1 bash "$START"' in text, name
            assert "Start Trading Bot.command" in text, name
            for never in ("-m mintel.run", "mintel.rider.run", "mintel.ian.run", "nohup"):
                assert never not in text, (name, never)                      # never a second copy
            assert "wait_bot_line" in text and "pause_close" in text, name
        assert "setup_rider" in (MAC / "SETUP-RAPID-RIDER.command").read_text()
        assert "restart_if_mode_changed rider" in (MAC / "SETUP-RAPID-RIDER.command").read_text()
        assert "setup_ian" in (MAC / "INSTALL-FINANCIAL-IAN.command").read_text()
        for name in ("START-RAPID-RIDER.command", "START-FINANCIAL-IAN.command"):
            text = (MAC / name).read_text()
            assert "setup_" not in text and "modes --config" not in text, name   # a start changes no setting

    def test_the_start_launcher_applies_the_line_up_once_and_its_fast_path_knows(self):
        text = (MAC / "Start Trading Bot.command").read_text()
        body = text.split("write_config         ||")[1]
        assert body.index("apply_rider_live") < body.index("apply_ian_enabled") < body.index("start_everything")
        assert body.index("start_everything") < body.index("start_rider_research")
        fast = text.split("# ---- Fast path")[1].split("# ---- Otherwise")[0]
        assert ".rider-live-2026-10-09" in fast and ".ian-enabled-2026-10-09" in fast and "start_rider_research" in fast
        assert '[[ -n "${MINTEL_NO_PAUSE:-}" ]] && return 0' in text
        assert text.count("read -r -p") == 1                                 # every pause goes through pause_close

    def test_the_line_up_is_set_once_with_the_modes_command(self, tmp_path):
        home = _home(tmp_path)
        data = home / "data"
        r = _bash('apply_scalper_paper; apply_rider_live; apply_ian_enabled; echo "CODE=$CODE_CHANGED"', home)
        assert r.returncode == 0, r.stderr
        assert "CODE=yes" in r.stdout
        assert json.loads((data / "rider.json").read_text()) == {"mode": "LIVE", "user_pip_value_gbp": 1.0}
        assert json.loads((data / "scalper.json").read_text())["mode"] == "OFF"
        assert json.loads((data / "ian.json").read_text())["mode"] == "PAPER"
        for m in (".rider-live-2026-10-09", ".ian-enabled-2026-10-09", ".scalper-paper-2026-10-08"):
            assert (data / m).exists(), m
        # Aaron's later choice is never undone by a reinstall
        _write(data / "rider.json", {"mode": "PAPER", "user_pip_value_gbp": 2.0})
        r = _bash('apply_rider_live; apply_ian_enabled; echo "CODE=${CODE_CHANGED:-none}"', home)
        assert "CODE=none" in r.stdout
        assert json.loads((data / "rider.json").read_text()) == {"mode": "PAPER", "user_pip_value_gbp": 2.0}

    def test_setup_rider_keeps_a_figure_already_set(self, tmp_path):
        home = _home(tmp_path)
        data = home / "data"
        _write(data / "rider.json", {"mode": "PAPER", "user_pip_value_gbp": 2.0})
        r = _bash("setup_rider", home)
        assert r.returncode == 0, r.stdout + r.stderr
        assert json.loads((data / "rider.json").read_text()) == {"mode": "LIVE", "user_pip_value_gbp": 2.0}
        assert (data / ".rider-live-2026-10-09").exists()
        (data / "rider.json").unlink()
        assert _bash("setup_rider", home).returncode == 0
        assert json.loads((data / "rider.json").read_text()) == {"mode": "LIVE", "user_pip_value_gbp": 1.0}

    def test_the_mode_and_health_helpers(self, tmp_path):
        home = _home(tmp_path)
        data = home / "data"
        r = _bash("bot_file_mode rider; bot_file_mode ian", home)
        assert r.stdout.split() == ["PAPER", "PAPER"]
        _write(data / "rider.json", {"mode": "OFF"})
        (data / "ian.json").write_text("{broken")
        assert _bash("bot_file_mode rider; bot_file_mode ian", home).stdout.split() == ["OFF", "PAPER"]
        r = _bash("wait_bot_line rider 0; echo rc=$?", home)
        assert "RAPID MOMENTUM RIDER - OFF" in r.stdout and "rc=2" in r.stdout
        _rider_files(data, now=dt.datetime.now(UTC))
        r = _bash("wait_bot_line rider 0; echo rc=$?", home)
        assert "RAPID MOMENTUM RIDER - HEALTHY, LIVE, scanning 2 markets" in r.stdout and "rc=0" in r.stdout

    def test_the_research_starts_in_the_background_once_a_day(self, tmp_path):
        home = tmp_path / "MarketBot"
        app = home / "app"
        (app / "mintel" / "rider").mkdir(parents=True)
        (app / "mintel" / "rider" / "research.py").write_text("")
        (home / "data").mkdir(parents=True)
        (home / "data" / "config.json").write_text("{}")
        (home / "venv" / "bin").mkdir(parents=True)
        calls = tmp_path / "calls.txt"
        fake = home / "venv" / "bin" / "python"
        fake.write_text(f'#!/bin/bash\necho "$@" >> "{calls}"\n')
        fake.chmod(0o755)
        r = _bash("start_rider_research; start_rider_research", home)
        assert r.returncode == 0, r.stderr
        deadline = time.monotonic() + 10
        while not calls.exists() and time.monotonic() < deadline:
            time.sleep(0.1)
        assert calls.read_text().split() == ["-m", "mintel.rider.research", "--config",
                                             str(home / "data" / "config.json"), "--days", "10", "--upload",
                                             "--workers", "1"]
        today = dt.datetime.now(UTC).strftime("%Y-%m-%d")
        assert (home / "data" / f".rider-research-{today}").exists()
        assert (home / "logs" / "rider-research.log").exists()
        # today's summary already there: nothing starts
        (home / "data" / f".rider-research-{today}").unlink()
        (home / "data" / "rider-research" / today).mkdir(parents=True)
        (home / "data" / "rider-research" / today / "summary.json").write_text("{}")
        calls.unlink()
        _bash("start_rider_research", home)
        time.sleep(0.5)
        assert not calls.exists()
        lib = (MAC / "mintel_mac.sh").read_text()
        body = lib.split("start_rider_research() {")[1].split("\n}\n")[0]
        assert "nohup nice -n 19" in body and "--workers 1" in body       # never starves the trading bots

    def test_reinstalls_carry_the_new_bots_and_stop_them_with_the_rest(self):
        lib = (MAC / "mintel_mac.sh").read_text()
        carry = lib.split("for key in (")[1].split("):")[0]
        assert '"rider"' in carry and '"ian"' in carry
        stop = lib.split("stop_everything() {")[1].split("\n}\n")[0]
        assert "for name in watchdog trader bridge rider ian scalper" in stop
        assert 'pkill -f "mintel.rider.run --config"' in stop and 'pkill -f "mintel.ian.run --config"' in stop
        start = lib.split("start_everything() {")[1].split("\n}\n")[0]
        assert "for name in watchdog trader bridge rider ian scalper" in start
        assert "-m mintel.run" not in start                                   # the supervisor starts them, never this
        ian = lib.split("setup_ian() {")[1].split("\n}\n")[0]
        assert "--set-key" in ian and ian.index("has_key") < ian.index("pip install databento")
        assert "databento_api_key" in ian and "db-" not in ian                 # no key in the source

    def test_the_new_icons_are_placed_once(self):
        lib = (MAC / "mintel_mac.sh").read_text()
        body = lib.split("install_new_bot_icons() {")[1].split("\n}\n")[0]
        assert ".desktop-icons-rider-ian-placed" in body
        for name in ("SETUP-RAPID-RIDER", "START-RAPID-RIDER", "INSTALL-FINANCIAL-IAN", "START-FINANCIAL-IAN"):
            assert f'"{name}"' in body


# ===================================================== the Windows scripts --
NEW_WIN = ("SETUP-RAPID-RIDER", "START-RAPID-RIDER", "INSTALL-FINANCIAL-IAN", "START-FINANCIAL-IAN")


class TestTheWindowsScripts:
    def _ps(self, name):
        return (WIN / name).read_text()

    def test_every_script_and_launcher_is_there_and_plain_ascii(self):
        for n in NEW_WIN + ("SETUP-AND-START", "START-BOT", "STOP-BOT"):
            ps = WIN / f"{n}.ps1"
            cmd = WIN / f"{n}.cmd"
            assert ps.exists() and cmd.exists(), n
            raw = ps.read_bytes()
            assert all(b < 128 for b in raw), f"{n}.ps1: Windows PowerShell 5.1 reads a file with no BOM as ANSI"
            text = ps.read_text()
            for a, b in (("{", "}"), ("(", ")")):
                assert text.count(a) == text.count(b), (n, a)
            c = cmd.read_text()
            assert f'-ExecutionPolicy Bypass -File "%~dp0{n}.ps1"' in c and c.startswith("@echo off")
        text = self._ps("mintel_bots.ps1")
        assert all(ord(ch) < 128 for ch in text)
        for a, b in (("{", "}"), ("(", ")")):
            assert text.count(a) == text.count(b), a

    def test_the_wrappers_use_the_shared_helpers_and_never_start_a_bot_themselves(self):
        for n in NEW_WIN:
            text = self._ps(f"{n}.ps1")
            assert '. (Join-Path $PSScriptRoot "mintel_bots.ps1")' in text, n
            assert "START-BOT.ps1" in text and "Wait-BotLine" in text, n
            for never in ("mintel.rider.run", "mintel.ian.run", "mintel.run\"", "-m\", \"mintel.run"):
                assert never not in text, (n, never)
        assert "Set-RiderLive" in self._ps("SETUP-RAPID-RIDER.ps1")
        assert "SETUP-AND-START.ps1" in self._ps("SETUP-RAPID-RIDER.ps1")
        for n in ("START-RAPID-RIDER.ps1", "START-FINANCIAL-IAN.ps1"):
            assert "Invoke-Modes" not in self._ps(n) and "Set-RiderLive" not in self._ps(n)
        ian = self._ps("INSTALL-FINANCIAL-IAN.ps1")
        assert '"--set-key"' in ian and ian.index("if ($hasKey)") < ian.index('"install", "databento"')
        assert '"--ian-feed", "databento"' in ian and "Enable-Ian" in ian

    def test_the_helpers_switch_the_line_up_through_the_modes_command(self):
        text = self._ps("mintel_bots.ps1")
        assert '@("--off", "scalper", "--live", "rider")' in text and '@("--gbp-per-pip", "1.0")' in text
        assert '@("--paper", "ian")' in text and '"--check", $Name' in text
        assert ".rider-live-2026-10-09" in text and ".ian-enabled-2026-10-09" in text
        research = text.split("function Start-RiderResearch")[1].split("\nfunction ")[0]
        for needle in ('"mintel.rider.research"', '"--days", "10", "--upload"', "summary.json", ".rider-research-$today",
                       "-WindowStyle Hidden", '"--workers", "1"', "-PassThru",
                       "[System.Diagnostics.ProcessPriorityClass]::Idle",
                       "[System.Diagnostics.ProcessPriorityClass]::BelowNormal"):
            assert needle in research, needle
        assert '$ErrorActionPreference = "Continue"' in text.split("function Invoke-MintelPython")[1].split("\nfunction ")[0]

    def test_setup_and_start_keeps_keys_registers_the_tasks_and_restarts_on_new_code(self):
        text = self._ps("SETUP-AND-START.ps1")
        for task in ("MintelTrader", "MintelKeepAlive", "MintelReport", "MintelPulse"):
            assert f'"{task}"' in text, task
        assert "-FromSetup -Restart" in text
        assert "Invoke-LineUpOnce $P" in text
        assert "$oldSecrets.PSObject.Properties" in text and '$secrets = [ordered]@{}' in text
        carry = text.split("foreach ($key in @(")[1].split("))")[0]
        for key in ('"rider"', '"ian"', '"runner"', '"tracking_start_utc"', '"account_reset_utc"'):
            assert key in carry, key
        for item in ('"tools"', '"config"', '"docs"'):
            assert item in text.split("copying program files")[1].split("Good")[0], item

    def test_json_is_written_without_a_byte_order_mark(self):
        """Windows PowerShell 5.1 writes a byte-order mark with -Encoding UTF8;
        Python's json then refuses config.json and the bot loads its defaults."""
        text = self._ps("SETUP-AND-START.ps1")
        assert "Set-Content -Path $configPath" not in text and "Set-Content -Path $secretPath" not in text
        assert text.count("Write-JsonFile $configPath") == 2 and "Write-JsonFile $secretPath" in text
        assert "New-Object System.Text.UTF8Encoding($false)" in text
        with pytest.raises(ValueError):
            json.loads("\ufeff{}")                                          # what a mark would do

    def test_start_and_stop_know_the_new_bots(self):
        start = self._ps("START-BOT.ps1")
        assert '@("watchdog", "trader", "rider", "ian", "scalper")' in start
        assert "Write-BotLines $P" in start and "Start-RiderResearch $P" in start
        assert start.index('Say "Watchdog"') < start.index("Write-BotLines $P")
        stop = self._ps("STOP-BOT.ps1")
        assert '@("watchdog", "trader", "rider", "ian", "scalper")' in stop

    def test_no_secret_is_written_into_any_script(self):
        for path in list(WIN.glob("*.ps1")) + list(WIN.glob("*.cmd")) + list(MAC.glob("*.command")) + [MAC / "mintel_mac.sh"]:
            text = path.read_text()
            assert not re.search(r"db-[A-Za-z0-9]{8,}", text), path
            assert not re.search(r"ghp_[A-Za-z0-9]{8,}", text), path


class TestTheVpsGuide:
    def test_it_covers_the_move_in_plain_steps(self):
        text = (ROOT / "docs" / "ops" / "vps.md").read_text()
        for needle in ("Stop Trading Bot", "launchctl unload", "SETUP-AND-START.cmd", "START-BOT.cmd",
                       "START-RAPID-RIDER.cmd", "INSTALL-FINANCIAL-IAN.cmd", "MintelKeepAlive", "ssh -N -L 8787",
                       "only one copy may ever trade the account"):
            assert needle in text, needle
