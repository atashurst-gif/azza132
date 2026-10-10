"""Formula 1 (9 Oct 2026), the trading core. Aaron: "Sack the rider off and
let's get trend and breakout live risking 0.2 higher" - about 13 a trade
(0.7% instead of 0.5%, the money cap 13 instead of 10), and "Just Formula 1":
only Trend & Breakout's minor currency pair trades use real money; the rest
stays on paper and still feeds the Momentum Runner.

Covers: the settings (tnb.live_since_utc, the new risk defaults), the switch
(python -m mintel.ops.modes --tnb-live-groups / --risk-pct / --risk-money),
the Momentum Runner's own cap (its money per ride unchanged), where every
order goes through the paper wrapper with FX_MINOR live, the top-opportunity
check counting the whole commission, and the status files naming the live
markets. Every price here is synthetic test data."""
from __future__ import annotations

import dataclasses
import datetime as dt
import json
import logging
import re
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from mintel.botexec import MAGIC_RUNNER
from mintel.broker.base import OrderRequest, Position, Side
from mintel.broker.paper import PAPER_FIRST_TICKET, PaperBroker
from mintel.config import (Config, RiskConfig, RunnerConfig as AppRunnerConfig, TnbConfig, tnb_live_groups,
                           tnb_mode_detail)
from mintel.contracts import infer_group
from mintel.engine.journal import Journal
from mintel.engine.risk import RiskManager
from mintel.ops import modes
from mintel.runner import feed
from mintel.runner.shadow import MomentumRunner, RunnerConfig

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 9, 18, 45, 0, tzinfo=UTC)
TNB = 990_311


# =========================================================== the settings --
class TestTheSettings:
    def test_the_live_defaults_are_point_seven_and_thirteen(self):
        r = Config().risk
        assert r.base_risk_pct == 0.7 and r.max_risk_money == 13.0
        # nothing else Aaron did not name has moved
        assert r.max_risk_pct == 1.5 and r.min_risk_pct == 0.1 and r.max_daily_loss_pct == 3.0
        assert r.top_size_enabled and r.top_risk_money == 40.0 and r.top_max_lots == 0.5
        assert r.top_segments == (("MOMENTUM_CONTINUATION", "FX_MINOR"),)
        assert (r.top_min_trades, r.top_min_days, r.top_lookback_trades) == (20, 4, 40)
        assert (r.max_total_risk_pct, r.max_correlated_risk_pct, r.max_open_positions) == (4.0, 2.0, 8)
        assert r.max_drawdown_pct == 12.0 and r.min_margin_level_pct == 300.0
        assert not r.validate()

    def test_the_runner_keeps_ten_and_the_other_bots_their_own(self):
        c = Config()
        assert c.runner.max_risk_money == 10.0 and RunnerConfig().max_risk_money == 10.0
        assert c.bandbreaker.risk_money == 10.0 and c.crowd.risk_money == 10.0 and c.crowd.max_risk_money == 30.0

    def test_the_example_config_says_point_seven(self):
        raw = json.loads((Path(__file__).resolve().parents[1] / "config" / "config.example.json").read_text())
        assert raw["risk"]["base_risk_pct"] == 0.7
        assert raw["risk"]["max_risk_pct"] == 1.5 and raw["risk"]["max_daily_loss_pct"] == 3.0

    def test_live_since_is_blank_by_default_and_read_from_the_file(self, tmp_path):
        assert TnbConfig().live_since_utc == ""
        p = tmp_path / "config.json"
        p.write_text(json.dumps({"tnb": {"mode": "PAPER", "live_groups": ["FX_MINOR"],
                                         "live_since_utc": "2026-10-09T18:45:00Z"}}))
        c = Config.load(p)
        assert c.tnb.live_groups == ("FX_MINOR",) and c.tnb.live_since_utc == "2026-10-09T18:45:00+00:00"

    @pytest.mark.parametrize("bad", ["yesterday", 12345, ["2026-10-09"], {"t": 1}, "2026-13-40T99:00"])
    def test_a_malformed_time_becomes_blank_with_a_log_line_never_a_crash(self, tmp_path, caplog, bad):
        p = tmp_path / "config.json"
        p.write_text(json.dumps({"tnb": {"mode": "PAPER", "live_groups": ["FX_MINOR"], "live_since_utc": bad}}))
        from mintel import config as config_module
        config_module._WARNED.clear()
        with caplog.at_level(logging.WARNING, logger="mintel.config"):
            c = Config.load(p)
        assert c.tnb.live_since_utc == "" and c.tnb.live_groups == ("FX_MINOR",) and c.tnb.mode == "PAPER"
        assert any("live_since_utc" in r.getMessage() for r in caplog.records)

    def test_null_or_no_live_markets_means_blank(self, tmp_path):
        p = tmp_path / "config.json"
        p.write_text(json.dumps({"tnb": {"mode": "PAPER", "live_groups": ["FX_MINOR"], "live_since_utc": None}}))
        assert Config.load(p).tnb.live_since_utc == ""
        p.write_text(json.dumps({"tnb": {"mode": "PAPER", "live_groups": [], "live_since_utc": "2026-10-09T18:45:00"}}))
        assert Config.load(p).tnb.live_since_utc == ""

    def test_the_mode_with_its_live_markets(self):
        assert tnb_mode_detail("PAPER", ("FX_MINOR",)) == "PAPER (FX_MINOR live)"
        assert tnb_mode_detail("PAPER", ()) == "PAPER" and tnb_mode_detail("LIVE", ("FX_MINOR",)) == "LIVE"
        c = Config()
        c.tnb.mode, c.tnb.live_groups = "PAPER", ("fx",)
        assert tnb_live_groups(c) == ("FX_EXOTIC", "FX_MAJOR", "FX_MINOR")       # as the paper broker reads it
        c.tnb.mode = "LIVE"
        assert tnb_live_groups(c) == ()


# ============================================================== the switch --
def a_config(tmp_path: Path, tnb_mode: str = "PAPER") -> Path:
    cfg = Config()
    cfg.ops.data_dir = str(tmp_path)
    cfg.tnb.mode = tnb_mode
    cfg.runner.mode = "LIVE"
    cfg.risk.max_risk_pct = 1.5
    p = tmp_path / "config.json"
    cfg.save(p)
    raw = json.loads(p.read_text())
    raw["note_from_aaron"] = "keep me"
    raw["risk"]["base_risk_pct"], raw["risk"]["max_risk_money"] = 0.5, 10.0     # the file as it was on 9 Oct
    p.write_text(json.dumps(raw, indent=2))
    return p


class TestTheLiveMarketsSwitch:
    def test_formula_1_sets_the_kinds_and_the_time_and_nothing_else(self, tmp_path):
        p = a_config(tmp_path)
        before = json.loads(p.read_text())
        out = modes.set_modes(p, {}, tnb_live_groups=["FX_MINOR"], now=NOW)
        after = json.loads(p.read_text())
        assert out["tnb"] == "PAPER"
        assert after["tnb"]["live_groups"] == ["FX_MINOR"] and after["tnb"]["live_since_utc"] == "2026-10-09T18:45:00+00:00"
        for key in ("live_groups", "live_since_utc"):
            before["tnb"][key] = after["tnb"][key]
        assert after == before                                          # nothing but those two keys moved
        c = Config.load(p)
        assert c.tnb.live_groups == ("FX_MINOR",) and c.tnb.live_since_utc == "2026-10-09T18:45:00+00:00"

    def test_given_alone_it_puts_trend_and_breakout_on_paper(self, tmp_path):
        p = a_config(tmp_path, tnb_mode="LIVE")
        assert modes.set_modes(p, {}, tnb_live_groups=["fx-minor"], now=NOW)["tnb"] == "PAPER"
        raw = json.loads(p.read_text())
        assert raw["tnb"]["mode"] == "PAPER" and raw["tnb"]["live_groups"] == ["FX_MINOR"]

    def test_the_same_set_keeps_its_time_a_new_set_gets_a_new_one(self, tmp_path):
        p = a_config(tmp_path)
        modes.set_modes(p, {}, tnb_live_groups=["FX_MINOR"], now=NOW)
        later = NOW + dt.timedelta(hours=2)
        modes.set_modes(p, {"crowd": "PAPER"}, tnb_live_groups=["fx_minor"], now=later)
        assert json.loads(p.read_text())["tnb"]["live_since_utc"] == "2026-10-09T18:45:00+00:00"
        modes.set_modes(p, {}, tnb_live_groups=["FX_MINOR", "INDICES"], now=later)
        raw = json.loads(p.read_text())
        assert raw["tnb"]["live_groups"] == ["FX_MINOR", "INDEX"] and raw["tnb"]["live_since_utc"] == later.isoformat()

    def test_a_set_kept_through_live_starts_again_when_it_takes_effect(self, tmp_path):
        p = a_config(tmp_path)
        modes.set_modes(p, {}, tnb_live_groups=["FX_MINOR"], now=NOW)
        modes.set_modes(p, {"tnb": "LIVE"}, now=NOW + dt.timedelta(hours=1))        # everything real; kinds kept
        back = NOW + dt.timedelta(hours=3)
        modes.set_modes(p, {"tnb": "PAPER"}, now=back)
        assert json.loads(p.read_text())["tnb"]["live_since_utc"] == back.isoformat()

    def test_none_clears_both_and_leaves_the_mode(self, tmp_path):
        p = a_config(tmp_path)
        modes.set_modes(p, {}, tnb_live_groups=["FX_MINOR"], now=NOW)
        assert modes.set_modes(p, {}, tnb_live_groups=["none"], now=NOW)["tnb"] == "PAPER"
        raw = json.loads(p.read_text())
        assert raw["tnb"]["live_groups"] == [] and raw["tnb"]["live_since_utc"] == ""
        assert Config.load(p).tnb.live_groups == ()

    def test_refused_with_live_tnb_and_nothing_written(self, tmp_path):
        p = a_config(tmp_path)
        stamp = p.read_text()
        with pytest.raises(ValueError, match="--live tnb"):
            modes.set_modes(p, {"tnb": "LIVE"}, tnb_live_groups=["FX_MINOR"], now=NOW)
        assert p.read_text() == stamp
        assert modes.main(["--config", str(p), "--live", "tnb", "--tnb-live-groups", "FX_MINOR"]) == 1
        assert p.read_text() == stamp

    @pytest.mark.parametrize("names", [["FXMINOR"], ["FX_MINOR", "BITCOIN"], ["UNKNOWN"], ["none", "FX_MINOR"], []])
    def test_an_unknown_kind_is_refused_and_nothing_written(self, tmp_path, names):
        p = a_config(tmp_path)
        stamp = p.read_text()
        with pytest.raises(ValueError):
            modes.set_modes(p, {"rider": "OFF"}, tnb_live_groups=names, now=NOW)
        assert p.read_text() == stamp and not (tmp_path / "rider.json").exists()

    def test_every_kind_the_trader_sorts_markets_into_is_accepted(self):
        for kind in ("FX_MINOR", "FX_MAJOR", "FX_EXOTIC", "INDEX", "GOLD", "SILVER", "METAL", "ENERGY", "EQUITY", "CRYPTO"):
            assert modes.check_live_kinds([kind]) == (kind,)
        assert modes.check_live_kinds(["FX"]) == ("FX_MAJOR", "FX_MINOR", "FX_EXOTIC")
        assert modes.check_live_kinds(["metals", "Indexes"]) == ("GOLD", "SILVER", "METAL", "INDEX")
        for symbol in ("EURGBP", "AUDJPY", "EURAUD", "EURUSD", "US500", "XAUUSD", "XAGUSD", "XTIUSD"):
            assert infer_group(symbol) in modes.LIVE_KINDS

    def test_its_magic_is_checked_since_its_orders_there_are_real(self, tmp_path):
        p = a_config(tmp_path)
        raw = json.loads(p.read_text())
        raw["runner"]["magic"] = TNB
        p.write_text(json.dumps(raw))
        stamp = p.read_text()
        with pytest.raises(ValueError, match="tnb's magic 990311"):
            modes.set_modes(p, {}, tnb_live_groups=["FX_MINOR"], now=NOW)
        assert p.read_text() == stamp

    def test_the_show_and_check_lines_name_it_plainly(self, tmp_path, capsys):
        p = a_config(tmp_path)
        assert modes.main(["--config", str(p), "--off", "rider", "--tnb-live-groups", "FX_MINOR"]) == 0
        out = capsys.readouterr().out
        assert re.search(r"Trend & Breakout: PAPER, minor currency pairs LIVE \(Formula 1\) since \d{4}-\d\d-\d\d \d\d:\d\d "
                         r"UTC\. Its orders on minor currency pairs go to the real broker; every other market stays on "
                         r"paper and still feeds the Momentum Runner", out)
        assert json.loads((tmp_path / "rider.json").read_text())["mode"] == "OFF"
        assert modes.main(["--config", str(p), "--show"]) == 0
        show = capsys.readouterr().out
        line = next(x for x in show.splitlines() if x.startswith("Trend & Breakout:"))
        assert "PAPER  minor currency pairs LIVE (Formula 1) since" in line and "tnb.live_groups" in line
        assert "the Momentum Runner still rides its index entries" in line
        code, check = modes.bot_health(p, "tnb", NOW)
        assert check.startswith("TREND & BREAKOUT - PAPER (real prices, simulated orders; minor currency pairs LIVE "
                                "(Formula 1) since ")
        assert "), the Momentum Runner rides its index entries" in check and code == 1     # no trader report yet

    def test_with_indices_live_the_runner_gets_no_feed(self, tmp_path, capsys):
        p = a_config(tmp_path)
        modes.set_modes(p, {}, tnb_live_groups=["FX_MINOR", "INDEX"], now=NOW)
        check = modes.bot_health(p, "tnb", NOW)[1]
        assert "minor currency pairs and indices LIVE since 2026-10-09 18:45 UTC" in check
        assert "(no runner feed: its index orders are real)" in check and "Formula 1" not in check
        modes.main(["--config", str(p), "--show"])
        assert "the Momentum Runner gets no runner feed" in capsys.readouterr().out

    def test_without_live_markets_the_lines_are_as_before(self, tmp_path, capsys):
        p = a_config(tmp_path)
        assert modes.bot_health(p, "tnb", NOW)[1] == ("TREND & BREAKOUT - PAPER (real prices, simulated orders), the "
                                                      "Momentum Runner rides its index entries - NOT STARTED YET (no "
                                                      "report from the trader yet)")
        modes.main(["--config", str(p), "--show"])
        line = next(x for x in capsys.readouterr().out.splitlines() if x.startswith("Trend & Breakout:"))
        assert modes.TNB_WHAT["PAPER"] in line


class TestTheRiskSwitch:
    def test_both_numbers_and_nothing_else(self, tmp_path, capsys):
        p = a_config(tmp_path)
        before = json.loads(p.read_text())
        assert modes.main(["--config", str(p), "--risk-pct", "0.7", "--risk-money", "13"]) == 0
        after = json.loads(p.read_text())
        assert after["risk"]["base_risk_pct"] == 0.7 and after["risk"]["max_risk_money"] == 13.0
        before["risk"]["base_risk_pct"], before["risk"]["max_risk_money"] = 0.7, 13.0
        assert after == before                                          # no other risk key, no other key at all
        out = capsys.readouterr().out
        assert ("Trend & Breakout's risk per trade: 0.7% of the account and never more than 13.00 at its stop "
                "(config.json: risk.base_risk_pct, risk.max_risk_money)") in out
        assert "Only Trend & Breakout sizes from these two numbers" in out
        assert "held to at most 10.00 at its own stop (runner.max_risk_money)" in out
        c = Config.load(p)
        assert c.risk.base_risk_pct == 0.7 and c.risk.max_risk_money == 13.0 and not c.validate()

    def test_each_alone(self, tmp_path):
        p = a_config(tmp_path)
        modes.set_modes(p, {}, risk_pct=0.7)
        raw = json.loads(p.read_text())
        assert raw["risk"]["base_risk_pct"] == 0.7 and raw["risk"]["max_risk_money"] == 10.0
        modes.set_modes(p, {}, risk_money=13)
        raw = json.loads(p.read_text())
        assert raw["risk"]["base_risk_pct"] == 0.7 and raw["risk"]["max_risk_money"] == 13.0

    @pytest.mark.parametrize("pct,money", [(0.05, None), (1.6, None), (0.0, None), (-1.0, None), (float("nan"), None),
                                           (None, 0.0), (None, -13.0), (None, float("inf")), (True, None)])
    def test_out_of_bounds_is_refused_and_nothing_written(self, tmp_path, pct, money):
        p = a_config(tmp_path)
        stamp = p.read_text()
        with pytest.raises(ValueError):
            modes.set_modes(p, {"crowd": "LIVE"}, risk_pct=pct, risk_money=money)
        assert p.read_text() == stamp

    def test_the_bounds_are_the_files_own(self, tmp_path):
        p = a_config(tmp_path)
        raw = json.loads(p.read_text())
        raw["risk"]["max_risk_pct"] = 0.6
        p.write_text(json.dumps(raw))
        with pytest.raises(ValueError, match="between 0.1 .* and 0.6"):
            modes.set_modes(p, {}, risk_pct=0.7)

    def test_only_trend_and_breakout_reads_these_two_numbers(self):
        """Every reader of risk.base_risk_pct and risk.max_risk_money is
        Trend & Breakout's own (checked 9 Oct). A new reader elsewhere must
        be looked at: another bot's money per trade would move with it."""
        root = Path(__file__).resolve().parents[1] / "mintel"
        base_readers, money_readers = set(), set()
        for f in root.rglob("*.py"):
            text = f.read_text()
            rel = str(f.relative_to(root.parent))
            if "base_risk_pct" in text:
                base_readers.add(rel)
            if re.search(r"(risk|\br)\.max_risk_money|getattr\(r, ['\"]max_risk_money", text):
                money_readers.add(rel)
        assert base_readers <= {"mintel/config.py", "mintel/engine/risk.py", "mintel/verify.py",
                                "mintel/research/backtest.py", "mintel/ops/modes.py"}
        assert money_readers <= {"mintel/config.py", "mintel/engine/risk.py", "mintel/ops/modes.py"}


# ======================================================= the Runner's cap --
def sim_us500(balance=10_000.0):
    from mintel.broker.sim import SimBroker
    b = SimBroker(["US500"], start=dt.datetime(2026, 10, 7, 13, 0, tzinfo=UTC), history_bars=300, balance=balance)
    b.connect()
    return b


def live_runner(tmp_path, b, **kw):
    return MomentumRunner(tmp_path, RunnerConfig(mode="LIVE", **kw), spec_fn=b.spec, clock=lambda: b.now, broker=b)


def tnb_trade(b, ticket=1, volume=1.0, risk=30.0):
    t = b.tick("US500")
    return Position(ticket, "US500", Side.BUY, volume, t.ask, t.ask - risk, 0.0, b.now, magic=TNB), t.ask - risk


def at_stop(b, pos) -> float:
    return b.spec(pos.symbol).money_per_lot(abs(pos.entry_price - pos.sl)) * pos.volume


class TestTheRunnersCap:
    def test_a_paper_fed_index_entry_sized_at_thirteen_gives_a_ride_risking_ten_at_most(self, tmp_path):
        from tests.test_tnb_paper_wiring import act, entry, paper_on, setup, trader_on
        sim, cfg = setup(tmp_path, symbols=("EURGBP", "US500"))
        cfg.tnb.live_groups = ("FX_MINOR",)
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        act(tr, sim, entry(sim, "US500", 30.0))
        paper = pb.positions(TNB)[0]
        assert pb.is_paper(paper.ticket)                                # indices stay on paper
        spec = sim.spec("US500")
        paper_risk = spec.money_per_lot(abs(paper.entry_price - paper.sl)) * paper.volume
        assert 10.0 < paper_risk <= 13.0 + 1e-9                         # Trend & Breakout: about 13 a trade
        tr.manage_cycle()
        mine = sim.positions(MAGIC_RUNNER)
        assert len(mine) == 1 and mine[0].volume < paper.volume
        assert at_stop(sim, mine[0]) <= 10.0 + 1e-9                     # the Runner: 10 at most, as before
        assert abs(mine[0].volume / spec.volume_step - round(mine[0].volume / spec.volume_step)) < 1e-6
        assert "held to" in tr.runner.last_note and "under its cap of 10.00" in tr.runner.last_note
        tr.journal.close()
        pb.close_db()

    def test_scaled_down_to_the_step_never_up(self, tmp_path):
        b = sim_us500()
        r = live_runner(tmp_path, b)
        p, sl = tnb_trade(b, volume=1.0)                                # 30 points = 30.00 a lot
        notes = r.observe([p], {1: sl}, b.tick, b.now, tactics={1: "SESSION_EXPANSION"})
        mine = b.positions(MAGIC_RUNNER)[0]
        assert mine.volume == pytest.approx(0.33) and at_stop(b, mine) <= 10.0 + 1e-9
        assert any("held to 0.33 lots" in n and "Trend & Breakout's 1 would risk 30.00" in n for n in notes)

    def test_an_entry_already_under_ten_is_copied_unchanged(self, tmp_path):
        b = sim_us500()
        r = live_runner(tmp_path, b)
        p, sl = tnb_trade(b, volume=0.3)                                # 9.00 at the stop
        notes = r.observe([p], {1: sl}, b.tick, b.now, tactics={1: "SESSION_EXPANSION"})
        mine = b.positions(MAGIC_RUNNER)[0]
        assert mine.volume == pytest.approx(0.3) and [x["req"].volume for x in b.order_log] == [0.3]
        assert not any("held to" in n for n in notes)

    def test_when_even_the_smallest_size_risks_more_it_is_not_taken_and_said(self, tmp_path):
        b = sim_us500()
        b._specs["US500"] = dataclasses.replace(b.spec("US500"), volume_min=0.5, volume_step=0.5)
        r = live_runner(tmp_path, b)
        p, sl = tnb_trade(b, volume=1.0)                                # 0.5 lots would risk 15.00
        notes = r.observe([p], {1: sl}, b.tick, b.now, tactics={1: "SESSION_EXPANSION"})
        assert not b.order_log and not r.open
        assert any("not taken: the smallest size the broker allows (0.5 lots) would risk 15.00" in n for n in notes)
        row = r.db.execute("SELECT why FROM skipped WHERE source_ticket=1").fetchone()
        assert row and "over its cap of 10.00" in row[0]                # said no for good: never hammered

    def test_the_cap_is_measured_at_the_runners_own_entry(self, tmp_path):
        b = sim_us500()
        r = live_runner(tmp_path, b)
        p, sl = tnb_trade(b, volume=0.4)
        p = dataclasses.replace(p, entry_price=p.entry_price - 6.0)    # the price has moved 6 points on (0.25 R)
        assert at_stop(b, p) == pytest.approx(9.6)                      # under 10 at Trend & Breakout's entry ...
        r.observe([p], {1: sl}, b.tick, b.now, tactics={1: "SESSION_EXPANSION"})
        mine = b.positions(MAGIC_RUNNER)[0]
        # ... 12.00 from the Runner's own entry (the touch, 30 points from the stop): held to 0.33
        assert mine.volume == pytest.approx(0.33) and at_stop(b, mine) <= 10.0 + 1e-9

    def test_a_paper_shadow_is_held_the_same_way(self, tmp_path):
        b = sim_us500()
        r = MomentumRunner(tmp_path, RunnerConfig(), spec_fn=b.spec, clock=lambda: b.now)
        p, sl = tnb_trade(b, volume=1.0)
        s = r.adopt(p, sl, b.now, tactic="SESSION_EXPANSION")
        assert s is not None and s.volume == pytest.approx(0.33)

    def test_zero_means_no_cap(self, tmp_path):
        b = sim_us500()
        r = live_runner(tmp_path, b, max_risk_money=0.0)
        p, sl = tnb_trade(b, volume=1.0)
        r.observe([p], {1: sl}, b.tick, b.now, tactics={1: "SESSION_EXPANSION"})
        assert b.positions(MAGIC_RUNNER)[0].volume == pytest.approx(1.0)

    def test_the_runners_own_top_size_is_untouched(self, tmp_path):
        b = sim_us500(balance=10_000.0)
        r = live_runner(tmp_path, b, top_segments=(("BREAKOUT_RETEST", "INDEX"),))
        p, sl = tnb_trade(b, volume=0.33)
        r.observe([p], {1: sl}, b.tick, b.now, tactics={1: "BREAKOUT_RETEST"})
        mine = b.positions(MAGIC_RUNNER)[0]
        assert r.open[mine.ticket].top and mine.volume > 0.33 and at_stop(b, mine) > 10.0     # its own limits only
        assert at_stop(b, mine) <= 40.0 + 1e-9

    def test_the_trader_hands_the_setting_over(self, tmp_path):
        from tests.test_tnb_paper_wiring import paper_on, setup, trader_on
        sim, cfg = setup(tmp_path)
        cfg.runner.max_risk_money = 7.5
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        assert tr.runner.cfg.max_risk_money == 7.5 and tr.runner.status()["max_risk_money"] == 7.5
        assert AppRunnerConfig().max_risk_money == 10.0
        tr.journal.close()
        pb.close_db()


# ======================================================= where orders go --
@pytest.fixture
def inner():
    from tests.test_paper_broker import FakeInner, _spec
    f = FakeInner(now=dt.datetime(2026, 10, 9, 13, 0, tzinfo=UTC))
    f.specs.update({
        "EURGBP": _spec("EURGBP", 5, 0.00001, 1.00, 100_000, group="FX_MINOR", profit_ccy="GBP"),
        "AUDJPY": _spec("AUDJPY", 3, 0.001, 0.50, 100_000, group="FX_MINOR", profit_ccy="JPY"),
        "EURAUD": _spec("EURAUD", 5, 0.00001, 0.52, 100_000, group="FX_MINOR", profit_ccy="AUD"),
        "XAGUSD": _spec("XAGUSD", 3, 0.001, 4.0, 5_000, group="SILVER", profit_ccy="USD"),
        "XTIUSD": _spec("XTIUSD", 2, 0.01, 0.80, 100, group="ENERGY", profit_ccy="USD"),
    })
    return f


PRICES = {"EURGBP": (0.85400, 0.85410), "AUDJPY": (99.500, 99.512), "EURAUD": (1.65000, 1.65015),
          "EURUSD": (1.10000, 1.10010), "US500": (5200.00, 5200.50), "XAUUSD": (2650.00, 2650.30),
          "XAGUSD": (31.000, 31.020), "XTIUSD": (74.00, 74.03)}


def f1_config(tmp_path, live_groups=("FX_MINOR",), **tnb) -> Config:
    cfg = Config()
    cfg.ops.data_dir = str(tmp_path)
    cfg.tnb.mode = "PAPER"
    cfg.tnb.live_groups = tuple(live_groups)
    for k, v in tnb.items():
        setattr(cfg.tnb, k, v)
    return cfg


def wrapper(inner, cfg) -> PaperBroker:
    pb = PaperBroker.from_config(inner, cfg)
    pb._now = lambda: inner.now
    return pb


def buy(inner, pb, symbol, volume=0.1, key="k"):
    bid, ask = PRICES[symbol]
    inner.set_tick(symbol, bid, ask)
    stop = round(bid - (ask - bid) * 40, 5)
    return pb.send(OrderRequest(symbol=symbol, side=Side.BUY, volume=volume, sl=stop, tp=0.0, comment=key,
                                magic=TNB, idempotency_key=key))


class TestWhereEveryOrderGoes:
    @pytest.mark.parametrize("symbol", ["EURGBP", "AUDJPY", "EURAUD"])
    def test_a_minor_pair_goes_to_the_real_broker_under_990311_and_so_do_its_changes(self, inner, tmp_path, symbol):
        pb = wrapper(inner, f1_config(tmp_path))
        inner.allow_orders = True
        res = buy(inner, pb, symbol)
        assert res.ok and res.ticket < PAPER_FIRST_TICKET and not pb.is_paper(res.ticket)
        sent = [c for c in inner.order_calls if c[0] == "send"]
        assert len(sent) == 1 and sent[0][1].magic == TNB and sent[0][1].symbol == symbol
        assert [p.ticket for p in pb.positions(TNB)] == [res.ticket]
        assert pb.modify_stops(res.ticket, 0.0, 0.0).ok                 # a stop move: the real broker
        assert pb.close(res.ticket).ok                                  # a close: the real broker
        assert [c[0] for c in inner.order_calls] == ["send", "modify_stops", "close"]
        pb.close_db()

    @pytest.mark.parametrize("symbol", ["EURUSD", "US500", "XAUUSD", "XAGUSD", "XTIUSD"])
    def test_majors_indices_metals_and_energy_stay_on_paper(self, inner, tmp_path, symbol):
        pb = wrapper(inner, f1_config(tmp_path))
        res = buy(inner, pb, symbol)                                    # inner.send would raise: a leak
        assert res.ok and pb.is_paper(res.ticket) and res.ticket >= PAPER_FIRST_TICKET
        bid, ask = PRICES[symbol]
        assert pb.modify_stops(res.ticket, round(bid - (ask - bid) * 30, 5), 0.0).ok     # a stop move: on paper
        assert pb.close(res.ticket).ok
        assert inner.order_calls == [] and inner.real_positions == []
        pb.close_db()

    def test_the_runner_feed_stays_on_while_indices_are_paper(self, inner, tmp_path):
        cfg = f1_config(tmp_path)
        cfg.runner.feed_tactics = ("BREAKOUT_RETEST",)
        pb = wrapper(inner, cfg)
        assert feed.active(cfg, pb)
        pb.close_db()
        cfg2 = f1_config(tmp_path / "b", live_groups=("FX_MINOR", "INDEX"))
        cfg2.runner.feed_tactics = ("BREAKOUT_RETEST",)
        pb2 = wrapper(inner, cfg2)
        assert not feed.active(cfg2, pb2)                               # index orders would be real
        pb2.close_db()

    def test_each_position_stays_with_the_side_it_was_opened_on(self, inner, tmp_path):
        # before the switch: a paper EURGBP trade (nothing live yet) and a real EURUSD one from LIVE days
        before = wrapper(inner, f1_config(tmp_path, live_groups=()))
        paper_eurgbp = buy(inner, before, "EURGBP", key="p1")
        assert paper_eurgbp.ok and before.is_paper(paper_eurgbp.ticket)
        before.close_db()
        inner.real_positions.append(Position(4_000_001, "EURUSD", Side.BUY, 0.1, 1.1001, 1.0950, 0.0, inner.now,
                                             magic=TNB))
        # the restart after `--tnb-live-groups FX_MINOR`
        pb = wrapper(inner, f1_config(tmp_path))
        assert {p.ticket for p in pb.positions(TNB)} == {paper_eurgbp.ticket, 4_000_001}
        # the paper minor-pair trade is still managed on paper, never sent to the real broker
        assert pb.modify_stops(paper_eurgbp.ticket, 0.85100, 0.0).ok and inner.order_calls == []
        assert pb.close(paper_eurgbp.ticket).ok and inner.order_calls == []
        assert pb.closed_deal(paper_eurgbp.ticket)["reason"].startswith("PAPER")
        # the real leftover is wound down at the real broker (manage_leftovers), never on the paper book
        inner.allow_orders = True
        assert pb.modify_stops(4_000_001, 1.0960, 0.0).ok and inner.order_calls[-1][0] == "modify_stops"
        assert pb.close(4_000_001).ok and inner.order_calls[-1][0] == "close"
        assert not pb.is_paper(4_000_001)
        # a real minor-pair trade opened now is never closed on paper
        real = buy(inner, pb, "EURGBP", key="r1")
        assert not pb.is_paper(real.ticket) and pb.close(real.ticket).ok and inner.order_calls[-1][0] == "close"
        pb.close_db()

    def test_without_manage_leftovers_a_real_leftover_is_left_to_its_own_stop(self, inner, tmp_path):
        inner.real_positions.append(Position(4_000_002, "EURUSD", Side.BUY, 0.1, 1.1001, 1.0950, 0.0, inner.now,
                                             magic=TNB))
        pb = wrapper(inner, f1_config(tmp_path, manage_leftovers=False))
        assert not pb.modify_stops(4_000_002, 1.0960, 0.0).ok and not pb.close(4_000_002).ok
        assert inner.order_calls == []
        pb.close_db()

    def test_the_account_wide_limits_see_the_real_minor_pair_trades(self, tmp_path):
        from tests.test_tnb_paper_wiring import act, entry, paper_on, setup, tnb_orders, trader_on
        sim, cfg = setup(tmp_path, symbols=("EURGBP", "EURUSD", "US500"))
        cfg.tnb.live_groups = ("FX_MINOR",)
        cfg.risk.max_open_positions = 2
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        act(tr, sim, entry(sim, "EURGBP", 0.0030))
        real = sim.positions(TNB)
        assert len(real) == 1 and len(tnb_orders(sim)) == 1 and not pb.is_paper(real[0].ticket)
        act(tr, sim, entry(sim, "EURUSD", 0.0030))
        assert len(sim.positions(TNB)) == 1                             # the major went to paper
        mine = pb.positions(TNB)
        assert {p.ticket for p in mine} >= {real[0].ticket} and len(mine) == 2
        assert real[0].ticket in tr.risk._open_risk                     # total and correlated exposure count it
        snap = tr.risk.snapshot(pb.account(), mine, sim.now)
        assert snap.open_positions == 2 and snap.open_risk_pct > 0
        assert any("POSITION_COUNT" in b for b in snap.blocking_reasons())
        # the daily loss stop counts its real deals (and its paper ones: it only ever stops sooner)
        assert sim.close(real[0].ticket).ok
        day = tr._strategy_day_start(sim.now)
        rows = pb.deals_since(day, TNB, False)
        assert any(int(r["position"]) == real[0].ticket for r in rows)
        tr._realised_cache = {"at": None, "value": 0.0}
        assert tr.realised_today() == pytest.approx(sum(float(r["profit"]) for r in rows))
        tr.journal.close()
        pb.close_db()

    def test_the_setting_is_read_from_config_json_at_start(self, inner, tmp_path):
        p = a_config(tmp_path)
        old = wrapper(inner, Config.load(p))                            # started before the change
        modes.set_modes(p, {}, tnb_live_groups=["FX_MINOR"], now=NOW)
        assert old.live_groups == frozenset()
        res = buy(inner, old, "EURGBP", key="o1")
        assert old.is_paper(res.ticket) and inner.order_calls == []    # the running copy is unchanged ...
        old.close_db()
        from mintel.run import trend_and_breakout_broker
        new = trend_and_breakout_broker(inner, Config.load(p))          # ... a restart applies it
        assert isinstance(new, PaperBroker) and new.live_groups == frozenset({"FX_MINOR"})
        new.close_db()

    def test_which_approaches_can_trade_a_minor_pair_under_version_5(self):
        """Every Trend & Breakout trade on a minor pair goes live, not only
        momentum continuation (Formula 1's own evidence). Under version 5,
        before the score floor (70), excluded markets and every other gate:"""
        from mintel.engine.tactics import TACTICS, blocked_in_group
        scan = Config().scan
        off_cond = set(scan.disabled_tactic_regimes)
        allowed = {}
        names = {t.name: [r.value for r in t.regimes] for t in TACTICS}
        names["BREAKOUT_RETEST"] = names["BREAKOUT_ACCEPTANCE"]          # breakout acceptance's retested form
        for name, conds in names.items():
            if name in scan.disabled_tactics or blocked_in_group(name, "FX_MINOR", scan.disabled_tactic_groups):
                continue
            ok = [c for c in conds if (name, c) not in off_cond]
            if ok:
                allowed[name] = ok
        assert allowed == {"MOMENTUM_CONTINUATION": ["HIGH_VOL", "NEWS"],
                           "BREAKOUT_ACCEPTANCE": ["SQUEEZE", "UNCLEAR"], "BREAKOUT_RETEST": ["UNCLEAR"],
                           "SESSION_EXPANSION": ["TREND", "HIGH_VOL", "UNCLEAR"],
                           "SQUEEZE_RELEASE": ["SQUEEZE", "LOW_VOL"], "RANGE_REJECTION": ["RANGE", "LOW_VOL"],
                           "LIQUIDITY_SWEEP_REVERSAL": ["RANGE", "UNCLEAR"], "VWAP_REVERSION": ["RANGE", "LOW_VOL"]}
        assert {"CHFJPY", "EURNZD", "GBPJPY"} <= set(Config().universe.excluded_symbols)


# ===================================== the top-opportunity check's costs --
def segment(journal: Journal, *, sides, per_trade_win=1.2, loss=-2.0, volume=0.5, days=6, symbol="GBPCHF"):
    """Momentum continuation on a minor pair: each day three winners and one
    loser (+1.5 R / -1 R), ``sides`` of the commission in pnl_money."""
    t = 1
    for d in range(days):
        day = (dt.date(2026, 9, 28) + dt.timedelta(days=d)).isoformat()
        for k in range(4):
            win = k < 3
            r = 1.5 if win else -1.0
            entry, stop = 1.1000, 1.0980
            journal._exec("""INSERT INTO trades (ticket, symbol, side, volume, entry, stop, exit_price, opened_utc,
                             closed_utc, pnl_money, realised_r, regime, tactic, commission_sides)
                             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                          (t, symbol, "BUY", volume, entry, stop, entry + r * (entry - stop), f"{day}T08:00:00+00:00",
                           f"{day}T09:00:00+00:00", per_trade_win if win else loss, r, "HIGH_VOL",
                           "MOMENTUM_CONTINUATION", sides))
            t += 1


def top_rm(tmp_path, sides, **kw):
    j = Journal(tmp_path / "journal.sqlite")
    segment(j, sides=sides, **kw)
    rm = RiskManager(Config(), j)
    rm.round_turn_commission = lambda symbol: 5.50                     # IC Markets Raw, GBP, per lot round turn
    return rm, j


def a_state():
    from tests.test_version5 import state
    return state()


class TestTheTopSizeCountsTheWholeCommission:
    def test_positive_in_the_journal_but_negative_after_the_opening_commission_is_no_top_size(self, tmp_path):
        rm, _ = top_rm(tmp_path, sides=None)                            # rows from before 9 Oct: closing side only
        ok, why = rm.top_opportunity(a_state(), "FX_MINOR", NOW)
        rec = rm.segment_record("MOMENTUM_CONTINUATION", "FX_MINOR")
        # +0.40 a trade in the journal; 0.5 lots x 2.75 a side = 1.375 left out of every row
        assert rec["trades"] == 24 and rec["estimated_commission"] == pytest.approx(24 * 1.38, abs=0.25)
        assert rec["per_trade"] < 0
        assert not ok and "not earning now" in why and "opening commission the older records left out" in why

    def test_a_row_recorded_complete_is_not_adjusted_twice(self, tmp_path):
        rm, _ = top_rm(tmp_path, sides=2)
        ok, why = rm.top_opportunity(a_state(), "FX_MINOR", NOW)
        rec = rm.segment_record("MOMENTUM_CONTINUATION", "FX_MINOR")
        assert rec["estimated_commission"] == 0.0 and rec["per_trade"] == pytest.approx(0.4)
        assert ok and "has earned it" in why and "left out" not in why

    def test_a_row_worked_out_from_prices_misses_both_sides(self, tmp_path):
        rm, j = top_rm(tmp_path, sides=0, days=1)
        net, est = rm.row_net({"pnl_money": 1.2, "volume": 0.5, "symbol": "GBPCHF", "commission_sides": 0})
        assert est == pytest.approx(2.75) and net == pytest.approx(1.2 - 2.75)
        assert rm.row_net({"pnl_money": 1.2, "volume": 0.5, "symbol": "GBPCHF", "commission_sides": 2}) == (1.2, 0.0)
        assert rm.row_net({"pnl_money": 1.2, "volume": 0.5, "symbol": "GBPCHF"})[1] == pytest.approx(1.38, abs=0.01)

    def test_indices_carry_no_commission_and_without_the_brokers_rate_the_fallback_is_used(self, tmp_path):
        rm = RiskManager(Config(), None)
        assert rm.commission_per_side("US500") == 0.0
        assert rm.commission_per_side("EURGBP") == pytest.approx(3.00)  # risk.commission_fallback_per_lot 6.00 / 2
        rm.round_turn_commission = lambda s: 5.5
        assert rm.commission_per_side("EURGBP") == pytest.approx(2.75)

    def test_the_trader_hands_over_the_brokers_own_rate(self, tmp_path):
        from tests.test_tnb_paper_wiring import paper_on, setup, trader_on
        sim, cfg = setup(tmp_path)
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        tr.scanner.commission_per_lot = {"EURGBP": 5.12}
        assert tr.risk.commission_per_side("EURGBP") == pytest.approx(2.56)
        tr.journal.close()
        pb.close_db()

    def test_an_older_journal_gains_the_columns_and_keeps_its_rows(self, tmp_path):
        from mintel.engine.journal import SCHEMA
        old = SCHEMA.replace(", entry_commission REAL, commission_sides INTEGER", "")
        assert old != SCHEMA
        path = tmp_path / "journal.sqlite"
        con = sqlite3.connect(path)
        con.executescript(old)                                          # the schema before 9 Oct
        con.execute("INSERT INTO trades (ticket, symbol, closed_utc, pnl_money) "
                    "VALUES (7, 'EURGBP', '2026-10-08T10:00:00+00:00', 4.5)")
        con.commit()
        con.close()
        j = Journal(path)
        row = j.closed_trades()[0]
        assert row["pnl_money"] == 4.5 and row["commission_sides"] is None and row["entry_commission"] is None
        Journal(path).close()                                          # opening it again changes nothing
        j.close()


class OpeningDealsSim:
    """A SimBroker that records each position's opening deal with IC Markets'
    commission, as MetaTrader does (the simulator alone has no commission)."""

    @staticmethod
    def make(symbols, fee=-1.10):
        from mintel.broker.sim import SimBroker
        from tests.conftest import MID_LONDON

        class _Sim(SimBroker):
            def send(self, req):
                res = super().send(req)
                if res.ok:
                    self.entries.append({"position": res.ticket, "magic": int(req.magic or 0), "symbol": req.symbol,
                                         "volume": req.volume, "profit": fee, "commission": fee, "is_entry": True,
                                         "time": self.now})
                return res

            def deals_since(self, since_utc, magic=0, closing_only=True):
                rows = super().deals_since(since_utc, magic, closing_only)
                if not closing_only:
                    rows += [dict(r) for r in self.entries if r["time"] >= since_utc
                             and (not magic or r["magic"] == magic)]
                return rows

        s = _Sim(list(symbols), seed=101, start=MID_LONDON, history_bars=3000)
        s.entries = []
        s.connect()
        return s


class TestTheJournalHoldsTheWholeCommission:
    def test_a_real_minor_pair_trade_closed_at_the_broker(self, tmp_path):
        from tests.conftest import lifecycle_profile
        from tests.test_tnb_paper_wiring import act, entry, paper_on, trader_on
        sim = OpeningDealsSim.make(("EURGBP", "EURUSD"))
        cfg = lifecycle_profile(Config())
        cfg.ops.data_dir, cfg.ops.log_dir = str(tmp_path / "data"), str(tmp_path / "logs")
        cfg.news.store_path = "calendar.sqlite"
        cfg.account_login, cfg.account_server = sim.login, sim.server
        cfg.tnb.mode, cfg.tnb.live_groups = "PAPER", ("FX_MINOR",)
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        act(tr, sim, entry(sim, "EURGBP", 0.0030))
        ticket = sim.positions(TNB)[0].ticket
        assert sim.close(ticket).ok                                     # its stop, at the broker
        closing = sim.closed_deal(ticket)["pnl"]
        tr.cycle()                                                      # the trader books it
        row = next(t for t in tr.journal.closed_trades() if t["ticket"] == ticket)
        assert row["pnl_money"] == pytest.approx(closing - 1.10)        # the opening deal's commission too
        assert row["commission_sides"] == 2 and row["entry_commission"] == pytest.approx(-1.10)
        # the top-opportunity check reads it as it is: nothing estimated on top
        net, est = tr.risk.row_net({**row, "volume": row["volume"]})
        assert est == 0.0 and net == pytest.approx(row["pnl_money"])
        tr.journal.close()
        pb.close_db()

    def test_when_the_opening_deal_cannot_be_read_the_row_says_so(self):
        from mintel.engine.trader import Trader
        tracker = SimpleNamespace(opened_utc=NOW)

        class Broken:
            def deals_since(self, *a):
                raise ConnectionError("bridge down")

        me = SimpleNamespace(broker=Broken(), cfg=SimpleNamespace(magic=TNB), clock=lambda: NOW)
        assert Trader._opening_commission(me, 5, tracker) is None
        me.broker = SimpleNamespace(deals_since=lambda *a: [{"position": 6, "is_entry": True, "commission": -0.7}])
        assert Trader._opening_commission(me, 5, tracker) is None       # no opening deal for this one: unknown
        assert Trader._opening_commission(me, 6, tracker) == pytest.approx(-0.7)
        me.broker = SimpleNamespace(deals_since=lambda *a: [{"position": 6, "is_entry": True, "commission": 0.0}])
        assert Trader._opening_commission(me, 6, tracker) == 0.0        # an index: none charged, and known

    def test_the_other_bots_executor_still_adds_the_opening_half_once(self):
        """closed_deal itself is unchanged (it sums the closing deals): the
        Runner's, the Rider's and Ian's LIVE executor adds the opening half
        on top, so changing closed_deal would have counted it twice."""
        from mintel.botexec import LiveExecutor
        t = dt.datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
        b = SimpleNamespace(
            closed_deal=lambda ticket: {"pnl": 5.0, "exit_price": 1.0, "time": t},
            deals_since=lambda since, magic, closing_only: [
                {"position": 9, "is_entry": True, "commission": -1.1},
                {"position": 9, "is_entry": False, "commission": -1.1}])
        d = LiveExecutor(b, MAGIC_RUNNER, "RUNNER").closed_deal(9)
        assert d["pnl"] == pytest.approx(3.9) and d["entry_commission"] == pytest.approx(-1.1)


# ============================================================ the status --
class TestTheStatusNamesTheLiveMarkets:
    def test_the_trader_heartbeat_and_page(self, tmp_path):
        from tests.test_tnb_paper_wiring import paper_on, setup, trader_on
        sim, cfg = setup(tmp_path, symbols=("EURGBP", "US500"))
        cfg.tnb.live_groups = ("FX_MINOR",)
        cfg.tnb.live_since_utc = "2026-10-09T18:45:00+00:00"
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        assert tr.tnb_mode_detail == "PAPER (FX_MINOR live)" and tr.tnb_live_groups == ("FX_MINOR",)
        tr.cycle()
        beat = json.loads((Path(cfg.ops.data_dir) / "heartbeats" / "strategy.heartbeat.json").read_text())
        assert beat["detail"]["tnb_mode"] == "PAPER (FX_MINOR live)"
        from mintel.ops.dashboard import DashboardState
        from mintel.run import push_dashboard
        state = DashboardState()
        push_dashboard(state, tr)
        st = state.status
        assert st["tnb_mode"] == "PAPER" and st["tnb_mode_detail"] == "PAPER (FX_MINOR live)"
        assert st["tnb_live_groups"] == ["FX_MINOR"] and st["tnb_live_since_utc"] == "2026-10-09T18:45:00+00:00"
        tr.journal.close()
        pb.close_db()

    def test_the_pulse_and_bots_json(self, tmp_path):
        from mintel.ops import pulse as pl
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path)
        st = {"tnb_mode": "PAPER", "tnb_live_groups": ["FX_MINOR"], "tnb_live_since_utc": "2026-10-09T18:45:00+00:00"}
        live = pl._tnb_live(cfg, st, "PAPER")
        assert live == {"tnb_mode_detail": "PAPER (FX_MINOR live)", "tnb_live_groups": ["FX_MINOR"],
                        "tnb_live_since_utc": "2026-10-09T18:45:00+00:00"}
        assert pl._tnb_live(cfg, {}, "LIVE") == {"tnb_mode_detail": "LIVE", "tnb_live_groups": [],
                                                 "tnb_live_since_utc": ""}
        # an older page: the settings the trader reads
        cfg.tnb.mode, cfg.tnb.live_groups, cfg.tnb.live_since_utc = "PAPER", ("FX_MINOR",), "2026-10-09T18:45:00+00:00"
        assert pl._tnb_live(cfg, {"tnb_mode": "PAPER"}, "PAPER")["tnb_mode_detail"] == "PAPER (FX_MINOR live)"
        p = {"ts_utc": "2026-10-09T19:00:00+00:00", "up": True, "equity": 1877.0, "open_positions": 1,
             "today_pnl": -3.2, "tnb_mode": "PAPER", "tnb_practice_today": 1.1, **live}
        assert pl.pulse_line(p).endswith(" tnb=PAPER tnb_practice=+1.10 tnb_live=FX_MINOR")
        p["tnb_live_groups"] = []
        assert pl.pulse_line(p).endswith(" tnb=PAPER tnb_practice=+1.10")      # as before without live markets
        entry = pl._tnb_bot(cfg, {"up": True}, {"status": st, "health": {}}, True)
        assert entry["mode"] == "PAPER" and entry["mode_detail"] == "PAPER (FX_MINOR live)"
        assert entry["live_groups"] == ["FX_MINOR"] and entry["live_since_utc"] == "2026-10-09T18:45:00+00:00"


# ================================================= 10 Oct review fixes --
# Three verifiers read the change set on 9-10 Oct. What they found in the
# money is fixed here; each test fails without its fix.
def head_sized(eq, spec, pts, tier, conf=0.0):
    """Trend & Breakout's volume as the code before 9 Oct evening sized it
    (0.5% of the account, at most 10 a trade): what the Runner copied."""
    from mintel.broker.base import AccountInfo
    from tests.test_version5 import state as v5_state
    c = Config()
    c.risk.base_risk_pct, c.risk.max_risk_money = 0.5, 10.0
    rm = RiskManager(c, None)
    st = v5_state(symbol="US500", entry=5000.0, stop=5000.0 - pts, tier=tier)
    acct = AccountInfo(login=1, server="x", currency="GBP", balance=eq, equity=eq, margin=0.0, margin_free=eq,
                       margin_level=0.0, leverage=30, trade_allowed=True, is_demo=True)
    pct, _ = rm.risk_pct_for(st, conf)
    return rm.size(st, spec, acct, [], pct, None, 50.0).volume, rm, st, acct


class TestTheRunnersMoneyARideStaysAsItWas:
    """Verifier (money lens): a NORMAL-tier Runner ride grew from about 0.5%
    of the balance (9.30 on 1,877) to the flat cap (9.90), and by up to half
    again as the balance falls. The standing rule: other bots' money per
    trade must not change. The Runner now copies Trend & Breakout's size at
    the old settings (runner.copy_risk_pct 0.5, runner.max_risk_money 10)."""

    @pytest.mark.parametrize("step", [0.1, 0.01])
    @pytest.mark.parametrize("eq", [1877.0, 1700.0, 1600.0, 5000.0])
    @pytest.mark.parametrize("tier,conf", [("NORMAL", 0.0), ("STRONG", 0.6), ("EXCEPTIONAL", 0.9)])
    def test_the_copy_is_exactly_the_old_size(self, eq, step, tier, conf):
        from mintel.contracts import SymbolSpec
        spec = SymbolSpec(name="US500", digits=2, point=0.01, tick_size=0.01, tick_value=0.01, contract_size=1,
                          volume_min=step, volume_max=100.0, volume_step=step, stops_level_points=0,
                          freeze_level_points=0, asset_group="INDEX")
        old, _, st, acct = head_sized(eq, spec, 30.0, tier, conf)
        rm = RiskManager(Config(), None)                               # the new settings: 0.7% and 13
        assert rm.cfg.risk.base_risk_pct == 0.7 and rm.cfg.risk.max_risk_money == 13.0
        pct, _ = rm.risk_pct_for(st, conf)
        new = rm.size(st, spec, acct, [], pct, None, 50.0).volume
        copy = rm.copy_size(st, spec, acct, [], conf, None, None, 50.0, base_pct=0.5, money_cap=10.0)
        assert copy.volume == pytest.approx(old) and copy.volume <= new + 1e-12
        assert rm.cfg.risk.base_risk_pct == 0.7                        # Trend & Breakout's own settings untouched

    def test_through_the_trader_a_normal_ride_on_1877_is_9_30_not_9_90(self, tmp_path):
        from mintel.broker.sim import SimBroker
        from tests.conftest import MID_LONDON
        from tests.test_tnb_paper_wiring import act, entry, paper_on, setup, trader_on
        _, cfg = setup(tmp_path, symbols=("EURGBP", "US500"))
        cfg.tnb.live_groups = ("FX_MINOR",)
        sim = SimBroker(["EURGBP", "US500"], seed=101, start=MID_LONDON, history_bars=3000, balance=1877.0)
        sim.connect()
        cfg.account_login, cfg.account_server = sim.login, sim.server
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        st = dataclasses.replace(entry(sim, "US500", 30.0), tier="NORMAL")
        act(tr, sim, st)
        paper = pb.positions(TNB)[0]
        assert paper.volume == pytest.approx(0.43)                     # Trend & Breakout: 13.00 at its stop
        row = tr.journal.open_trades()[0]
        assert row["runner_volume"] == pytest.approx(0.31)             # 0.5% of 1,877 = 9.39: 0.31 lots, as before
        tr._manage_one = lambda p, now: []                             # keep the made-up trade open this pass
        tr.manage_cycle()
        mine = sim.positions(MAGIC_RUNNER)
        assert len(mine) == 1 and mine[0].volume == pytest.approx(0.31)
        assert at_stop(sim, mine[0]) == pytest.approx(9.30)            # was 9.90 with the flat cap
        assert "sized as before 9 Oct: 0.31 lots" in tr.runner.last_note
        tr.journal.close()
        pb.close_db()

    def test_the_runner_copies_the_smaller_size_it_is_handed(self, tmp_path):
        b = sim_us500()
        r = live_runner(tmp_path, b)
        p, sl = tnb_trade(b, volume=0.43)
        notes = r.observe([p], {1: sl}, b.tick, b.now, tactics={1: "SESSION_EXPANSION"}, copy_volumes={1: 0.31})
        mine = b.positions(MAGIC_RUNNER)[0]
        assert mine.volume == pytest.approx(0.31) and any("sized as before 9 Oct" in n for n in notes)
        # never more than Trend & Breakout's own volume
        p2, sl2 = tnb_trade(b, ticket=2, volume=0.2)
        r.observe([p2], {2: sl2}, b.tick, b.now, tactics={2: "SESSION_EXPANSION"}, copy_volumes={2: 0.31})
        assert sorted(x.volume for x in b.positions(MAGIC_RUNNER)) == pytest.approx([0.2, 0.31])

    def test_a_trade_too_small_at_the_old_size_is_not_ridden_and_said(self, tmp_path):
        b = sim_us500()
        r = live_runner(tmp_path, b)
        p, sl = tnb_trade(b, volume=0.43)
        notes = r.observe([p], {1: sl}, b.tick, b.now, tactics={1: "SESSION_EXPANSION"}, copy_volumes={1: 0.0})
        assert not b.order_log and any("under the broker's smallest size" in n for n in notes)
        paper = MomentumRunner(tmp_path / "p", RunnerConfig(), spec_fn=b.spec, clock=lambda: b.now)
        assert paper.adopt(p, sl, b.now, tactic="SESSION_EXPANSION", copy_volume=0.0) is None

    def test_a_paper_shadow_copies_the_same_size(self, tmp_path):
        b = sim_us500()
        r = MomentumRunner(tmp_path, RunnerConfig(), spec_fn=b.spec, clock=lambda: b.now)
        p, sl = tnb_trade(b, volume=0.43)
        s = r.adopt(p, sl, b.now, tactic="SESSION_EXPANSION", copy_volume=0.31)
        assert s is not None and s.volume == pytest.approx(0.31)

    def test_the_setting_and_no_copy_for_a_top_size_trade(self, tmp_path):
        assert AppRunnerConfig().copy_risk_pct == 0.5
        lines = modes.risk_lines({"risk": {"base_risk_pct": 0.7, "max_risk_money": 13.0}})
        assert ("the Momentum Runner copies the size it would have had at 0.5% of the account "
                "(runner.copy_risk_pct, as before 9 Oct) and each ride is held to at most 10.00") in lines[1]
        from tests.test_tnb_paper_wiring import paper_on, setup, trader_on
        sim, cfg = setup(tmp_path)
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        args = (None, None, None, (), 0.0, None, None, None)
        assert tr._runner_copy_volume(*args, True) is None             # a top-opportunity size did not change
        cfg.runner.copy_risk_pct = 0.0
        assert tr._runner_copy_volume(*args, False) is None            # 0: copy Trend & Breakout's volume
        tr.journal.close()
        pb.close_db()


class TestTheLossLimitsReadTheClosingDealAsBefore:
    """Verifier (money lens): with the opening commission now in the
    journal's pnl_money, a small real winner became a 'loss' for the
    30-minute cool-down and the two-losses-a-market-a-day limit. Aaron did
    not change those limits: they read the closing deals' figure again; the
    journal keeps the honest figure."""

    def test_a_small_real_winner_is_not_a_loss_for_the_cool_down_or_the_day_count(self, tmp_path):
        from tests.conftest import lifecycle_profile
        from tests.test_tnb_paper_wiring import act, entry, move, paper_on, trader_on
        sim = OpeningDealsSim.make(("EURGBP", "EURUSD"), fee=-1.10)
        cfg = lifecycle_profile(Config())
        cfg.ops.data_dir, cfg.ops.log_dir = str(tmp_path / "data"), str(tmp_path / "logs")
        cfg.news.store_path = "calendar.sqlite"
        cfg.account_login, cfg.account_server = sim.login, sim.server
        cfg.tnb.mode, cfg.tnb.live_groups = "PAPER", ("FX_MINOR",)
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        act(tr, sim, entry(sim, "EURGBP", 0.0030))
        pos = sim.positions(TNB)[0]
        spec = sim.spec("EURGBP")
        for _ in range(50):                                             # a small profit on the closing deal
            gain = spec.money(sim.tick("EURGBP").bid - pos.entry_price, pos.volume)
            if 0.05 < gain < 1.0:
                break
            move(sim, "EURGBP", 0.00001 if gain <= 0.05 else -0.00001)
        assert sim.close(pos.ticket).ok
        closing = sim.closed_deal(pos.ticket)["pnl"]
        tr.cycle()
        row = next(t for t in tr.journal.closed_trades() if t["ticket"] == pos.ticket)
        assert closing > 0 > row["pnl_money"]                           # the journal: the whole commission
        assert tr.losses_today("EURGBP") == 0
        assert tr.executor.cooldown_remaining("EURGBP", pos.side) == 0
        tr._losses = {"date": None, "counts": {}}                       # as after a restart: rebuilt from the journal
        assert tr.losses_today("EURGBP") == 0
        tr.journal.close()
        pb.close_db()
