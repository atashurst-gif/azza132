"""9 Oct evening: Aaron's line-up. LIVE: the Momentum Runner, the Rapid
Momentum Rider (GBP 1 a pip) and Financial Ian. PAPER: Trend & Breakout,
the Band Breaker and the Crowd Fader.

Trend & Breakout on PAPER keeps deciding on real prices; its orders are
simulated by mintel/broker/paper.py. Everything that reports it must say
PAPER, show its figures as practice and never add them to the account,
while a REAL deal of its own (a position left from LIVE, wound down to its
end) still counts as real money. On LIVE nothing changes.

Every figure here is SYNTHETIC test data made by the tests themselves (a
fake broker and a real PaperBroker writing its own record on the fake
broker's prices): nothing in this file measures performance."""
from __future__ import annotations

import csv
import datetime as dt
import io
import json
import subprocess
import sys
import urllib.request
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from mintel.broker.base import AccountInfo, Side
from mintel.broker.paper import PaperBroker
from mintel.config import Config
from mintel.engine.journal import Journal
from mintel.ops import modes
from mintel.ops import standing as standing_mod
from mintel.ops.attribution import build_strategies, build_strategies_range, make_period_resolver
from mintel.ops.dashboard import DashboardState, render_standing, start_dashboard
from mintel.ops.day_review import build_day_review, group_positions, review
from mintel.ops.ledger_check import practice_lines
from mintel.ops.report_upload import build_bundle
from mintel.ops.standing import TnbPaperRecord, account_standing, period_view, practice_figures, tnb_mode
from tests.test_integration_rider_ian import MAC, WIN, _bash, _home
from tests.test_paper_broker import FakeInner, make, req

UTC = dt.timezone.utc
DAY = dt.datetime(2026, 10, 7, tzinfo=UTC)               # FakeInner's day (a Wednesday)
NOW = dt.datetime(2026, 10, 7, 14, 0, tzinfo=UTC)
TNB, RUNNER = 990_311, 990_511
LEFTOVER = 5_000_001                                       # a REAL Trend & Breakout trade left from LIVE
RUNNER_TICKET = 5_000_002


def _set_tnb(cfg: Config, mode: str) -> Config:
    """Trend & Breakout's mode on a Config, whichever shape its block has."""
    block = getattr(cfg, "tnb", None)
    if block is not None and hasattr(block, "mode"):
        block.mode = mode
    else:
        cfg.tnb = NS(mode=mode)
    return cfg


class Inner(FakeInner):
    """The fake REAL broker, with a balance that is the reset balance plus
    every real deal (so the account figure is exactly MetaTrader's sum)."""

    def account(self):
        bal = 2000.0 + sum(float(r["profit"]) for r in self.real_deals)
        return AccountInfo(login=1, server="ICMarketsSC-Demo", currency="GBP", balance=bal, equity=bal,
                           margin=0.0, margin_free=bal, margin_level=0.0, leverage=30, trade_allowed=True,
                           is_demo=True)


class OldBridgeInner(Inner):
    """An older bridge: its rows do not say which magic they carried, so the
    standing asks per magic - through the wrapper that would add paper deals."""

    def deals_since(self, since, magic=0, closing_only=True):
        return [{k: v for k, v in r.items() if k != "magic"} for r in super().deals_since(since, magic, closing_only)]


def _deal(pos, magic, profit, comm, entry, when, symbol="US30"):
    return {"position": pos, "magic": magic, "symbol": symbol, "volume": 0.1, "profit": profit,
            "commission": comm, "is_entry": entry, "time": when}


def a_day(tmp_path: Path, inner_cls=Inner):
    """One day with: a REAL Trend & Breakout trade left from LIVE (opened
    yesterday, stopped out today), a REAL Momentum Runner trade, and two
    PAPER Trend & Breakout trades booked by a real PaperBroker on the fake
    broker's prices; the journal holds all three Trend & Breakout trades,
    as the trader books them."""
    data = tmp_path / "data"
    data.mkdir(parents=True, exist_ok=True)
    inner = inner_cls()
    inner.real_deals = [
        _deal(LEFTOVER, TNB, -0.30, -0.30, True, DAY - dt.timedelta(hours=9)),
        _deal(LEFTOVER, TNB, -6.30, -0.30, False, DAY + dt.timedelta(hours=12, minutes=30)),
        _deal(RUNNER_TICKET, RUNNER, 0.0, 0.0, True, DAY + dt.timedelta(hours=11)),
        _deal(RUNNER_TICKET, RUNNER, 20.0, 0.0, False, DAY + dt.timedelta(hours=12)),
    ]
    writer = make(inner, data)
    inner.set_tick("EURUSD", 1.10000, 1.10010)
    a = writer.send(req("EURUSD", Side.BUY, 0.5, sl=1.09500, key="a"))
    inner.set_tick("EURUSD", 1.10200, 1.10210, seconds=600)
    assert writer.close(a.ticket).ok
    inner.set_tick("GBPUSD", 1.25000, 1.25010)
    b = writer.send(req("GBPUSD", Side.SELL, 0.2, sl=1.25500, key="b"))
    inner.set_tick("GBPUSD", 1.25100, 1.25110, seconds=900)
    assert writer.close(b.ticket).ok
    closed = {t: writer.closed_deal(t) for t in (a.ticket, b.ticket)}
    writer.close_db()
    assert not inner.order_calls                                  # nothing ever reached the real broker
    j = Journal(data / "journal.sqlite")
    rows = [(LEFTOVER, "US30", "SELL", (DAY - dt.timedelta(hours=9)).isoformat(),
             (DAY + dt.timedelta(hours=12, minutes=30)).isoformat(), -6.30, "BREAKOUT_RETEST", "TREND")]
    for t, c in closed.items():
        rows.append((t, c["symbol"], "BUY" if c["symbol"] == "EURUSD" else "SELL",
                     (c["time"] - dt.timedelta(minutes=10)).isoformat(), c["time"].isoformat(), c["pnl"],
                     "SESSION_EXPANSION", "HIGH_VOL"))
    for r in rows:
        j._exec("INSERT INTO trades (ticket, symbol, side, opened_utc, closed_utc, pnl_money, tactic, regime) "
                "VALUES (?,?,?,?,?,?,?,?)", r)
    cfg = Config()
    cfg.ops.data_dir = str(data)
    cfg.ops.log_dir = str(tmp_path / "logs")
    return NS(data=data, inner=inner, journal=j, cfg=cfg, paper=(a.ticket, b.ticket), closed=closed)


def _paper_net_by_hand(data: Path, start, end) -> float:
    """The paper record's money, summed independently of the code under
    test: every deal of each position whose last exit is in [start, end)."""
    view = PaperBroker(None, data, magic=TNB, read_only=True)
    try:
        rows = view.paper_deals_since(DAY - dt.timedelta(days=30), closing_only=False)
    finally:
        view.close_db()
    last: dict = {}
    for r in rows:
        if not r["is_entry"]:
            last[r["position"]] = max(last.get(r["position"], r["time"]), r["time"])
    keep = {p for p, t in last.items() if start <= t < end}
    return round(sum(r["profit"] for r in rows if r["position"] in keep), 2)


# ===================================================== the modes command --
def a_config(tmp_path: Path) -> Path:
    cfg = Config()
    cfg.ops.data_dir = str(tmp_path)
    p = tmp_path / "config.json"
    cfg.save(p)
    raw = json.loads(p.read_text())
    raw.pop("tnb", None)                       # as an installation from before 9 Oct has it: no block
    raw["note_from_aaron"] = "keep me"
    p.write_text(json.dumps(raw, indent=2))
    return p


class TestTheModesCommand:
    def test_paper_and_back_to_live_in_its_own_block_and_nothing_else_moves(self, tmp_path, capsys):
        p = a_config(tmp_path)
        assert modes.read_modes(p)["tnb"] == "LIVE"                    # no block: LIVE, as it always was
        before = json.loads(p.read_text())
        assert modes.main(["--config", str(p), "--paper", "tnb"]) == 0
        out = capsys.readouterr().out
        line = [l for l in out.splitlines() if l.startswith("Trend & Breakout:")][0]
        assert "PAPER" in line and "real prices, simulated orders" in line and "config.json: tnb.mode" in line
        assert "magic 990311" in line and "the Momentum Runner still rides its index entries" in line
        after = json.loads(p.read_text())
        assert after.pop("tnb") == {"mode": "PAPER"}
        assert after == before                                           # every other key exactly as it was
        assert getattr(getattr(Config.load(p), "tnb", None), "mode", "PAPER") == "PAPER"
        assert tnb_mode(Config.load(p)) == "PAPER"
        assert modes.set_modes(p, {"tnb": "LIVE"})["tnb"] == "LIVE"
        assert json.loads(p.read_text())["tnb"] == {"mode": "LIVE"}

    def test_every_spelling_names_it_and_all_never_does(self):
        for name in ("tnb", "trend", "trend_breakout", "market_intelligence", "TREND-BREAKOUT"):
            assert modes.plan_changes(paper=[name]) == {"tnb": "PAPER"}, name
        assert "tnb" not in modes.plan_changes(live=["all"]) and "tnb" not in modes.plan_changes(off=["all"])

    def test_off_is_refused_in_plain_english_and_nothing_is_written(self, tmp_path, capsys):
        p = a_config(tmp_path)
        stamp = p.read_text()
        msg = "Trend & Breakout picks the Momentum Runner's entries; put it on PAPER instead"
        with pytest.raises(ValueError) as e:
            modes.plan_changes(off=["trend"])
        assert str(e.value) == msg
        with pytest.raises(ValueError, match="put it on PAPER instead"):
            modes.set_modes(p, {"tnb": "OFF"})
        assert modes.main(["--config", str(p), "--off", "tnb", "crowd"]) == 2
        assert f"Nothing changed: {msg}" in capsys.readouterr().err
        assert p.read_text() == stamp                                    # refused as a whole

    def test_its_magic_is_checked_on_the_way_to_live(self, tmp_path):
        p = a_config(tmp_path)
        raw = json.loads(p.read_text())
        raw["runner"]["magic"] = TNB
        p.write_text(json.dumps(raw))
        with pytest.raises(ValueError, match="tnb's magic 990311 is Momentum Runner's"):
            modes.set_modes(p, {"tnb": "LIVE"})
        assert modes.set_modes(p, {"tnb": "PAPER"})["tnb"] == "PAPER"      # the check is only on the way to LIVE

    def test_the_check_line_names_its_mode_and_the_runner(self, tmp_path, capsys):
        p = a_config(tmp_path)
        modes.set_modes(p, {"tnb": "PAPER", "runner": "LIVE"})
        now = dt.datetime(2026, 10, 9, 21, 0, tzinfo=UTC)
        hb = tmp_path / "heartbeats"
        hb.mkdir()

        def beat(age_s, detail=None):
            (hb / "strategy.heartbeat.json").write_text(json.dumps(
                {"name": "strategy", "ts_utc": (now - dt.timedelta(seconds=age_s)).isoformat(), "detail": detail or {}}))
        want = "TREND & BREAKOUT - PAPER (real prices, simulated orders), the Momentum Runner rides its index entries"
        assert modes.bot_health(p, "tnb", now) == (1, want + " - NOT STARTED YET (no report from the trader yet)")
        beat(5)
        assert modes.bot_health(p, "tnb", now) == (0, want)
        beat(5, {"waiting": "trying to reach MetaTrader", "attempt": 2})
        assert modes.bot_health(p, "tnb", now)[1] == want + " - STARTING: trying to reach MetaTrader (attempt 2)"
        beat(600)
        assert modes.bot_health(p, "tnb", now) == (1, want + " - NOT RUNNING (last report 10 min ago)")
        beat(5)
        modes.set_modes(p, {"tnb": "LIVE", "runner": "OFF"})
        assert modes.bot_health(p, "trend", now) == (
            0, "TREND & BREAKOUT - LIVE (real orders on the account), the Momentum Runner is OFF")
        modes.set_modes(p, {"runner": "PAPER"})
        assert modes.bot_health(p, "tnb", now)[1].endswith("the Momentum Runner rides its index entries (PAPER)")

    def test_check_tnb_from_the_command_line(self, tmp_path):
        p = a_config(tmp_path)
        modes.set_modes(p, {"tnb": "PAPER", "runner": "LIVE"})
        r = subprocess.run([sys.executable, "-m", "mintel.ops.modes", "--config", str(p), "--check", "tnb"],
                           capture_output=True, text=True, cwd=str(Path(__file__).resolve().parents[1]), timeout=60)
        assert r.returncode == 1                                          # no trader report yet
        assert r.stdout.startswith("TREND & BREAKOUT - PAPER (real prices, simulated orders), "
                                   "the Momentum Runner rides its index entries")


# ======================================================= the reports --
class TestTheStandingAndTheCards:
    def test_paper_never_reaches_the_account_and_the_cards_still_add_up(self, tmp_path):
        d = a_day(tmp_path)
        cfg = _set_tnb(d.cfg, "PAPER")
        wrapper = PaperBroker(d.inner, d.data, magic=TNB, read_only=True)     # what the trader would hold
        sd = account_standing(wrapper, cfg, NOW, cache_seconds=0)
        assert sd["error"] == ""
        assert sd["made"] == sd["trading"] == 13.40 and sd["adjustments"] == 0.0   # the fake broker's own sum
        by = {b["id"]: b for b in sd["bots"]}
        mi = by["market_intelligence"]
        assert mi["mode"] == "PAPER"
        # its REAL leftover only (real money): a position counts in full, both
        # commissions, on the UK day it closed - opened on the 6th, closed today
        assert (mi["made"], mi["today"], mi["trades"]) == (-6.60, -6.60, 1)
        assert by["momentum_runner"]["made"] == 20.0
        assert round(sum(b["made"] for b in sd["bots"]) + sd["other"]["made"], 2) == sd["trading"]
        assert not any(r["bot_magic"] == TNB and r["position"] in d.paper for r in sd["deals"])
        wrapper.close_db()

    def test_an_older_bridge_asked_per_magic_through_the_wrapper_still_never_sees_paper(self, tmp_path):
        d = a_day(tmp_path, OldBridgeInner)
        cfg = _set_tnb(d.cfg, "PAPER")
        wrapper = PaperBroker(d.inner, d.data, magic=TNB, read_only=True)
        sd = account_standing(wrapper, cfg, NOW, cache_seconds=0)
        mi = {b["id"]: b for b in sd["bots"]}["market_intelligence"]
        assert mi["made"] == -6.60 and sd["trading"] == 13.40
        wrapper.close_db()

    def test_the_practice_figure_is_the_paper_records_and_shows_in_grey(self, tmp_path):
        d = a_day(tmp_path)
        cfg = _set_tnb(d.cfg, "PAPER")
        sd = dict(account_standing(d.inner, cfg, NOW, cache_seconds=0))
        deals = sd.pop("deals")
        p_today = practice_figures(d.data, DAY, NOW, "GBP", end=DAY + dt.timedelta(days=1))
        assert p_today["market_intelligence"] == _paper_net_by_hand(d.data, DAY, DAY + dt.timedelta(days=1))
        assert p_today["market_intelligence"] == TnbPaperRecord(d.data).net(DAY, DAY + dt.timedelta(days=1))
        top = {"sel": period_view(sd, deals, "today", now=NOW), "tot": period_view(sd, deals, "total", now=NOW),
               "p_sel": p_today, "p_tot": p_today}
        html = render_standing({"standing": sd}, top)
        card = html.split("Trend &amp; Breakout")[1].split('<div class="hc')[0]
        assert '<span class="pill ">PAPER</span>' in card
        assert "real prices, simulated orders" in card
        assert "Practice only, not real money" in card and "+£54.48" in card      # in the grey practice line
        assert html.split("Trend &amp; Breakout")[0].endswith('<div class="hc paper"><div class="nm">')
        runner = html.split("Momentum Runner <span")[1].split('<div class="hc')[0]
        assert "rides Trend &amp; Breakout's index entries" in runner
        # the account card is MetaTrader's figure alone
        acct = html.split('<div class="hc acct">')[1].split('<div class="hc')[0]
        assert "+£13.40" in acct and "54.48" not in acct

    def test_on_live_its_card_is_as_it_always_was(self, tmp_path):
        d = a_day(tmp_path)
        (d.data / "tnb_paper.sqlite").unlink()
        sd = dict(account_standing(d.inner, d.cfg, NOW, cache_seconds=0))
        deals = sd.pop("deals")
        mi = {b["id"]: b for b in sd["bots"]}["market_intelligence"]
        assert mi["mode"] == "LIVE" and mi["made"] == -6.60
        top = {"sel": period_view(sd, deals, "today", now=NOW), "tot": period_view(sd, deals, "total", now=NOW),
               "p_sel": {}, "p_tot": {}}
        html = render_standing({"standing": sd}, top)
        card = html.split("Trend &amp; Breakout")[1].split('<div class="hc')[0]
        assert '<span class="pill ok">LIVE</span>' in card
        assert "Practice only" not in card and "simulated" not in card


class TestTheAttribution:
    def test_paper_its_tab_says_so_and_only_the_real_leftover_reaches_overall(self, tmp_path):
        d = a_day(tmp_path)
        s = build_strategies(d.journal, [], DAY, "GBP", d.data, None, now=NOW, tnb_mode="PAPER")
        mi = s["market_intelligence"]
        paper_net = _paper_net_by_hand(d.data, DAY, DAY + dt.timedelta(days=1))
        assert mi["mode"] == "PAPER"
        assert (mi["paper_trades"], mi["paper_net"]) == (2, paper_net)          # the paper record's money
        assert (mi["live_trades"], mi["live_net"]) == (1, -6.30)                 # the real leftover: money
        assert mi["realised"] == round(paper_net - 6.30, 2)
        modes_by_ticket = {t["ticket"]: t["mode"] for t in mi["trades_today"]}
        assert modes_by_ticket == {LEFTOVER: "LIVE", d.paper[0]: "PAPER", d.paper[1]: "PAPER"}
        ov = s["overall"]
        assert (ov["realised"], ov["trades"]) == (-6.30, 1)                      # never the paper money
        assert "Trend & Breakout" in ov["note"] and "PAPER" in ov["note"]
        assert "The Trend" not in ov["note"]
        assert "not in Overall" in mi["source"]

    def test_the_mode_comes_from_the_data_folder_when_it_is_not_passed(self, tmp_path):
        d = a_day(tmp_path)
        (d.data / "config.json").write_text(json.dumps({"tnb": {"mode": "PAPER"}}))
        s = build_strategies(d.journal, [], DAY, "GBP", d.data, None, now=NOW)
        assert s["market_intelligence"]["mode"] == "PAPER" and s["overall"]["realised"] == -6.30

    def test_live_with_no_paper_record_is_exactly_the_old_path(self, tmp_path):
        d = a_day(tmp_path)
        (d.data / "tnb_paper.sqlite").unlink()
        s = build_strategies(d.journal, [], DAY, "GBP", d.data, None, now=NOW, tnb_mode="LIVE")
        mi = s["market_intelligence"]
        total = round(-6.30 + sum(c["pnl"] for c in d.closed.values()), 2)
        assert mi["mode"] == "LIVE" and "live" not in mi
        assert (mi["live_net"], mi["paper_net"], mi["live_trades"], mi["paper_trades"]) == (total, 0.0, 3, 0)
        assert s["overall"]["realised"] == total and s["overall"]["trades"] == 3
        assert "Trend & Breakout" not in s["overall"]["note"]
        ledger = {"net": 1.23, "trades": 3, "wins": 1, "win_rate": 33.3, "commission": 0.5, "before_fees": 1.73,
                  "nets": [1.23]}
        s2 = build_strategies(d.journal, [], DAY, "GBP", d.data, ledger, now=NOW, tnb_mode="LIVE")
        assert s2["market_intelligence"]["realised"] == 1.23 == s2["overall"]["realised"]   # the broker's ledger wins

    def test_live_on_the_day_of_a_switch_keeps_the_paper_trades_out(self, tmp_path):
        d = a_day(tmp_path)
        s = build_strategies(d.journal, [], DAY, "GBP", d.data, None, now=NOW, tnb_mode="LIVE")
        mi = s["market_intelligence"]
        assert mi["mode"] == "LIVE" and mi["paper_trades"] == 2 and s["overall"]["realised"] == -6.30

    def test_open_paper_positions_are_practice_and_real_ones_money(self, tmp_path):
        d = a_day(tmp_path)
        opens = [NS(ticket=d.paper[0], symbol="EURUSD", profit=9.0), NS(ticket=LEFTOVER + 7, symbol="US30", profit=-2.0)]
        s = build_strategies(d.journal, opens, DAY, "GBP", d.data, None, now=NOW, tnb_mode="PAPER")
        assert s["market_intelligence"]["unrealised"] == 7.0
        assert s["overall"]["unrealised"] == -2.0

    def test_another_period_is_split_the_same_way(self, tmp_path):
        d = a_day(tmp_path)
        s = build_strategies_range(d.data, DAY, DAY + dt.timedelta(days=1), "GBP", now=NOW, tnb_mode="PAPER")
        mi = s["market_intelligence"]
        assert mi["mode"] == "PAPER" and mi["paper_trades"] == 2 and mi["live_net"] == -6.30
        assert s["overall"]["realised"] == -6.30
        assert "Trend & Breakout's real trades" in s["overall"]["note"] and "PAPER" in s["overall"]["note"]
        resolve = make_period_resolver(d.data)
        assert getattr(resolve, "takes_tnb_mode", False)
        live = build_strategies_range(d.data, DAY, DAY + dt.timedelta(days=1), "GBP", now=NOW)
        assert live["market_intelligence"]["mode"] == "LIVE"              # the data folder says nothing: LIVE

    def test_the_page_passes_the_mode_to_the_period_view(self, tmp_path):
        calls = []

        def resolver(period, date_from="", date_to="", tnb_mode=None):
            calls.append((period, tnb_mode))
            return {"overall": {"net_today": 0.0, "trades": 0, "currency": "GBP"}, "labels": {"overall": "Overall"},
                    "period": {"key": period, "label": period}}
        resolver.takes_tnb_mode = True
        state = DashboardState()
        state.period_resolver = resolver
        state.update(status={"bot": "RUNNING"}, tnb_mode="PAPER")
        httpd = start_dashboard(state, "127.0.0.1", 0)
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{httpd.server_address[1]}/?period=week").read()
            state.period_resolver = lambda period, date_from="", date_to="": calls.append((period, "three args")) or \
                resolver(period)
            urllib.request.urlopen(f"http://127.0.0.1:{httpd.server_address[1]}/?period=week").read()
        finally:
            httpd.shutdown()
        assert calls[0] == ("week", "PAPER") and ("week", "three args") in calls


class TestTheDayReviewAndTheNightlyReport:
    def test_paper_is_practice_and_the_leftover_is_real_money(self, tmp_path):
        d = a_day(tmp_path)
        cfg = _set_tnb(d.cfg, "PAPER")
        text, positions, rows = build_day_review(cfg, d.inner, DAY)
        paper_net = _paper_net_by_hand(d.data, DAY, DAY + dt.timedelta(days=1))
        assert text.startswith("Trend & Breakout is on PAPER")
        assert f"PRACTICE RESULT : {paper_net:+.2f} GBP   (paper money, not in the account)" in text
        assert "REAL RESULT     : -6.30 GBP" in text
        assert text.index("PRACTICE RESULT") < text.index("REAL MONEY") < text.index("REAL RESULT")
        assert {p["position"]: p["mode"] for p in positions} == {LEFTOVER: "LIVE", d.paper[0]: "PAPER",
                                                                 d.paper[1]: "PAPER"}
        # handed the trader's PAPER wrapper instead of the real broker: the same review
        wrapper = PaperBroker(d.inner, d.data, magic=TNB, read_only=True)
        assert build_day_review(cfg, wrapper, DAY)[0] == text
        wrapper.close_db()

    def test_paper_with_no_real_trade_says_so(self, tmp_path):
        d = a_day(tmp_path)
        d.inner.real_deals = [r for r in d.inner.real_deals if r["position"] != LEFTOVER]
        text, _, _ = build_day_review(_set_tnb(d.cfg, "PAPER"), d.inner, DAY)
        assert "No real Trend & Breakout trade closed that day." in text and "REAL RESULT" not in text

    def test_live_with_no_paper_deal_is_the_review_it_always_was(self, tmp_path):
        d = a_day(tmp_path)
        (d.data / "tnb_paper.sqlite").unlink()
        text, positions, rows = build_day_review(d.cfg, d.inner, DAY)
        real = [r for r in d.inner.deals_since(DAY, TNB, False) if r["time"] < DAY + dt.timedelta(days=1)]
        assert text == review(group_positions(real), rows, "GBP")
        assert "PRACTICE" not in text and all(p["mode"] == "LIVE" for p in positions)

    def test_the_bundle_labels_every_row_and_carries_the_paper_day(self, tmp_path):
        d = a_day(tmp_path)
        cfg = _set_tnb(d.cfg, "PAPER")
        files = build_bundle(cfg, d.inner, DAY, fetch=lambda url: "{}")
        key = "reports/2026-10-07"
        assert "PRACTICE RESULT" in files[f"{key}/day_review.txt"]
        rows = list(csv.DictReader(io.StringIO(files[f"{key}/trades.csv"])))
        assert {int(r["ticket"]): r["mode"] for r in rows} == {LEFTOVER: "LIVE", d.paper[0]: "PAPER", d.paper[1]: "PAPER"}
        paper_money = {int(r["ticket"]): float(r["net_money"]) for r in rows if r["mode"] == "PAPER"}
        assert round(sum(paper_money.values()), 2) == _paper_net_by_hand(d.data, DAY, DAY + dt.timedelta(days=1))
        tnb = json.loads(files[f"{key}/tnb.json"])
        assert tnb["mode"] == "PAPER" and tnb["real"]["net"] == -6.30
        assert tnb["paper"]["net"] == round(sum(paper_money.values()), 2) and len(tnb["paper"]["deals"]) == 4
        assert json.loads(files[f"{key}/rules.json"])["tnb_mode"] == "PAPER"
        assert files["reports/latest/tnb.json"] == files[f"{key}/tnb.json"]

    def test_the_bundle_on_live_is_as_before_with_its_mode_column(self, tmp_path):
        d = a_day(tmp_path)
        (d.data / "tnb_paper.sqlite").unlink()
        files = build_bundle(d.cfg, d.inner, DAY, fetch=lambda url: "{}")
        assert "reports/2026-10-07/tnb.json" not in files
        rows = list(csv.DictReader(io.StringIO(files["reports/2026-10-07/trades.csv"])))
        assert rows and all(r["mode"] == "LIVE" for r in rows)
        assert "REAL RESULT" in files["reports/2026-10-07/day_review.txt"]
        assert json.loads(files["reports/2026-10-07/rules.json"])["tnb_mode"] == "LIVE"

    def test_the_ledger_check_gives_the_practice_apart(self, tmp_path):
        d = a_day(tmp_path)
        lines = practice_lines(d.cfg, DAY, "PAPER")
        closes = [c["pnl"] for c in d.closed.values()]
        assert lines[0] == (f"Practice since midnight (Trend & Breakout's paper record, not money): 2 entries, "
                            f"2 exits, net {sum(closes):+.2f} incl. commission")
        assert all("(paper)" in l for l in lines[1:]) and len(lines) == 3
        (d.data / "tnb_paper.sqlite").unlink()
        assert practice_lines(d.cfg, DAY, "LIVE") == []                     # LIVE: nothing new to say


class TestPushDashboard:
    def test_the_page_and_the_tabs_are_told_trend_and_breakout_is_on_paper(self, trader, cfg):
        from mintel.run import push_dashboard
        standing_mod._CACHE.update(at=None, value=None)
        _set_tnb(trader.cfg, "PAPER")
        state = DashboardState()
        push_dashboard(state, trader)
        snap = state.snapshot()
        assert snap["status"]["tnb_mode"] == "PAPER" and state.tnb_mode == "PAPER"
        assert snap["strategies"]["market_intelligence"]["mode"] == "PAPER"
        assert {b["id"]: b["mode"] for b in state.standing["bots"]}["market_intelligence"] == "PAPER"

    def test_on_live_the_tabs_are_as_before(self, trader, cfg):
        from mintel.run import push_dashboard
        standing_mod._CACHE.update(at=None, value=None)
        _set_tnb(trader.cfg, "LIVE")
        state = DashboardState()
        push_dashboard(state, trader)
        mi = state.snapshot()["strategies"]["market_intelligence"]
        assert state.tnb_mode == "LIVE" and mi["mode"] == "LIVE" and "live" not in mi


# ===================================================== the installers --
LINE_UP = ("apply_bot_modes; apply_scalper_paper; apply_rider_live; apply_ian_enabled; apply_lineup; "
           'echo "CODE=${CODE_CHANGED:-none}"')
WANT = {"tnb": "PAPER", "runner": "LIVE", "bandbreaker": "PAPER", "crowd": "PAPER"}


def _modes(data: Path) -> dict:
    raw = json.loads((data / "config.json").read_text())
    out = {k: (raw.get(k) or {}).get("mode") for k in WANT}
    out["rider"] = json.loads((data / "rider.json").read_text())
    out["ian"] = json.loads((data / "ian.json").read_text())["mode"]
    out["scalper"] = json.loads((data / "scalper.json").read_text())["mode"]
    return out


def _lineup_now(data: Path, pip=1.0) -> dict:
    return {**WANT, "rider": {"mode": "LIVE", "user_pip_value_gbp": pip}, "ian": "LIVE", "scalper": "OFF"}


class TestTheMacLineUp:
    def test_a_fresh_install_ends_on_the_line_up_and_says_it_plainly(self, tmp_path):
        home = _home(tmp_path)
        data = home / "data"
        r = _bash(LINE_UP, home)
        assert r.returncode == 0, r.stderr
        assert _modes(data) == _lineup_now(data)
        assert "CODE=yes" in r.stdout
        assert ("LIVE  - real orders on the account: Momentum Runner, Rapid Momentum Rider (GBP 1.00 a pip), "
                "Financial Ian") in r.stdout
        assert "PAPER - real prices, simulated orders, never in the account: Trend & Breakout, Band Breaker, " \
               "Crowd Fader" in r.stdout
        assert "The Momentum Runner rides Trend & Breakout's index entries with its own real orders." in r.stdout
        assert "Financial Ian can only trade once a CME data feed is configured" in r.stdout
        for m in (".all-bots-live-2026-10-08", ".scalper-paper-2026-10-08", ".rider-live-2026-10-09",
                  ".ian-enabled-2026-10-09", ".lineup-2026-10-09"):
            assert (data / m).exists(), m
        assert r.stdout.index("The bots' line-up") > r.stdout.index("Financial Ian")     # it comes last

    def test_it_never_runs_twice_nor_undoes_a_later_choice(self, tmp_path):
        home = _home(tmp_path)
        data = home / "data"
        assert _bash(LINE_UP, home).returncode == 0
        later = _bash("cd \"$APP_DIR\" && \"$VENV_DIR/bin/python\" -m mintel.ops.modes --config \"$CONFIG\" "
                      "--live tnb crowd --paper ian >/dev/null", home)
        assert later.returncode == 0, later.stderr
        chosen = _modes(data)
        r = _bash(LINE_UP, home)
        assert "CODE=none" in r.stdout and "line-up" not in r.stdout
        assert _modes(data) == chosen and chosen["tnb"] == "LIVE" and chosen["ian"] == "PAPER"

    def test_an_older_step_left_undone_never_undoes_it(self, tmp_path):
        home = _home(tmp_path)
        data = home / "data"
        assert _bash(LINE_UP, home).returncode == 0
        for m in (".all-bots-live-2026-10-08", ".rider-live-2026-10-09", ".ian-enabled-2026-10-09"):
            (data / m).unlink()                                   # as if those steps had failed on this Mac
        before = _modes(data)
        r = _bash(LINE_UP, home)
        assert r.returncode == 0 and "CODE=none" in r.stdout
        assert _modes(data) == before
        for m in (".all-bots-live-2026-10-08", ".rider-live-2026-10-09", ".ian-enabled-2026-10-09"):
            assert (data / m).exists(), m

    def test_a_mac_that_ran_the_8_oct_steps_answering_n_ends_on_the_line_up(self, tmp_path):
        home = _home(tmp_path)
        data = home / "data"
        raw = json.loads((data / "config.json").read_text())
        for b in ("runner", "bandbreaker", "crowd"):
            raw[b]["mode"] = "LIVE"                                   # every bot LIVE since 8 Oct
        raw.pop("tnb", None)
        (data / "config.json").write_text(json.dumps(raw))
        for m in (".all-bots-live-2026-10-08", ".scalper-paper-2026-10-08"):
            (data / m).write_text("2026-10-08T09:00:00Z")
        r = _bash(LINE_UP, home)
        assert r.returncode == 0, r.stderr
        assert _modes(data) == _lineup_now(data) and "CODE=yes" in r.stdout

    def test_a_figure_already_set_for_the_rider_is_kept(self, tmp_path):
        home = _home(tmp_path)
        data = home / "data"
        (data / "rider.json").write_text(json.dumps({"mode": "PAPER", "user_pip_value_gbp": 2.0}))
        (data / ".rider-live-2026-10-09").write_text("x")
        r = _bash(LINE_UP, home)
        assert r.returncode == 0, r.stderr
        assert _modes(data)["rider"] == {"mode": "LIVE", "user_pip_value_gbp": 2.0}
        assert "Rapid Momentum Rider (GBP 2.00 a pip)" in r.stdout

    def test_the_mode_helper_knows_trend_and_breakout_is_live_by_default(self, tmp_path):
        home = _home(tmp_path)
        raw = json.loads((home / "data" / "config.json").read_text())
        raw.pop("tnb", None)
        (home / "data" / "config.json").write_text(json.dumps(raw))
        assert _bash("bot_mode tnb LIVE; bot_mode runner", home).stdout.split() == ["LIVE", "PAPER"]
        raw["tnb"] = {"mode": "PAPER"}
        (home / "data" / "config.json").write_text(json.dumps(raw))
        assert _bash("bot_mode tnb LIVE", home).stdout.split() == ["PAPER"]

    def test_every_path_runs_it_in_order_and_the_fast_path_knows_it(self):
        text = (MAC / "Start Trading Bot.command").read_text()
        body = text.split("write_config         ||")[1]
        assert (body.index("apply_bot_modes") < body.index("apply_rider_live") < body.index("apply_ian_enabled")
                < body.index("apply_lineup") < body.index("install_launch_agent") < body.index("start_everything"))
        fast = text.split("# ---- Fast path")[1].split("# ---- Otherwise")[0]
        assert ".lineup-2026-10-09" in fast
        lib = (MAC / "mintel_mac.sh").read_text()
        carry = lib.split("for key in (")[1].split("):")[0]
        assert '"tnb"' in carry                                       # re-entering the settings keeps it
        lineup = lib.split("apply_lineup() {")[1].split("\n}\n")[0]
        assert 'CODE_CHANGED="yes"' in lineup and "--paper tnb bandbreaker crowd --live runner rider ian" in lineup
        assert "print_lineup" in lineup
        for step in ("apply_bot_modes() {", "apply_rider_live() {", "apply_ian_enabled() {"):
            assert "lineup_done" in lib.split(step)[1].split("\n}\n")[0], step
        final = lib.split("final_report() {")[1].split("\n}\n")[0]
        assert "$(bot_mode tnb LIVE)" in final and "rides Trend & Breakout's index entries" in final
        for path in (MAC / "mintel_mac.sh", MAC / "Start Trading Bot.command"):
            r = subprocess.run(["bash", "-n", str(path)], capture_output=True, text=True)
            assert r.returncode == 0, (path, r.stderr)


class TestTheWindowsLineUp:
    def _ps(self, name):
        return (WIN / name).read_text()

    def test_the_same_line_up_once_after_the_older_steps(self):
        text = self._ps("mintel_bots.ps1")
        assert ('@("--off", "scalper", "--paper", "tnb", "bandbreaker", "crowd", "--live", "runner", "rider", "ian")'
                in text)
        setter = text.split("function Set-LineUp")[1].split("\nfunction ")[0]
        assert ".lineup-2026-10-09" in setter and "Test-RiderHasPipValue" in setter and '"--gbp-per-pip", "1.0"' in setter
        once = text.split("function Invoke-LineUpOnce")[1].split("\nfunction ")[0]
        assert once.index("if (Test-Path $lineUp)") < once.index("Set-RiderLive") < once.index("Enable-Ian") \
            < once.index("Set-LineUp $P")                             # set once it is done: the older steps never run
        assert "Write-LineUp $P" in once
        show = text.split("function Write-LineUp")[1].split("\nfunction ")[0]
        for needle in ("real orders on the account", "real prices, simulated orders, never in the account",
                       'Get-ConfigBotMode $P "tnb" "LIVE"', "CME data feed"):
            assert needle in show, needle

    def test_setup_carries_the_block_and_restarts_after_the_line_up(self):
        text = self._ps("SETUP-AND-START.ps1")
        carry = text.split("foreach ($key in @(")[1].split("))")[0]
        assert '"tnb"' in carry
        assert text.index("Invoke-LineUpOnce $P") < text.index("Write-LineUp $P") < text.index("-FromSetup -Restart")
