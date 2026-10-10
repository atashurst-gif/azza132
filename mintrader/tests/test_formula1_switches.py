"""10 Oct: the switches for Aaron's four pending decisions (asked about 00:15
UK on 10 Oct), and the daily-loss fix that had to ship before any of them.

A. The daily-loss stop never lets practice offset real money. With Formula 1
   live, Trend & Breakout's day holds REAL deals and positions (its minor
   pairs, at the broker) and PRACTICE ones (the paper record), and the stop
   summed them - so a practice profit let real losses run past 3%. Each side
   now nets its own trades and only a side that is down counts
   (RiskManager.daily_loss_figure); risk.daily_loss_counts_practice False
   counts the real side alone. One side alone is exactly the old figure.
B. Decision 1: tnb.live_tactics (--tnb-live-tactics) - only the listed
   approaches go real on the live markets. Decision 2: --top-size on|off
   (risk.top_size_enabled). Decision 3: --news-before-minutes N
   (news.blackout_before_seconds).
C. The binned Rider's Desktop icon asks before it goes back on LIVE.
D. The Windows START-BOT applies the Formula 1 one-time step too.

Every default keeps what runs today. EVERY FIGURE HERE IS MADE-UP TEST DATA
(a simulator, a fake broker and the tests' own numbers): nothing here
measures performance."""
from __future__ import annotations

import datetime as dt
import inspect
import json
import logging
import math
import os
import re
import subprocess
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from mintel.broker.base import AccountInfo, CalendarEvent, OrderRequest, Position, Side
from mintel.broker.paper import PAPER_FIRST_TICKET, PaperBroker
from mintel.config import Config, NewsConfig, RiskConfig, TnbConfig, tnb_live_tactics, tnb_mode_detail
from mintel.engine.risk import RiskManager
from mintel.engine.tactics import APPROACH_NAMES, approach_of
from mintel.ops import modes
from tests.test_formula1_core import PRICES, a_config, f1_config, wrapper
from tests.test_formula1_core import inner  # noqa: F401  (the fake real broker, a fixture)
from tests.test_integration_rider_ian import MAC, WIN
from tests.test_lineup import F1_CALLS, F1_STEP, FORMULA1, _bash_env, _calls, _fake_python_home
from tests.test_tnb_paper_wiring import act, entry, move, paper_on, setup, trader_on
from tests.conftest import MID_LONDON

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 10, 9, 0, tzinfo=UTC)
TNB = 990_311
MC = "MOMENTUM_CONTINUATION"


def account(equity: float) -> AccountInfo:
    return AccountInfo(login=1, server="x", currency="GBP", balance=equity, equity=equity, margin=0.0,
                       margin_free=equity, margin_level=0.0, leverage=30, trade_allowed=True, is_demo=True)


def pos(ticket: int, profit: float, symbol: str = "EURGBP") -> Position:
    return Position(ticket=ticket, symbol=symbol, side=Side.BUY, volume=0.1, entry_price=0.85, sl=0.84, tp=0.0,
                    open_time=NOW, magic=TNB, profit=profit)


def rm_split(real: float, practice: float, *, counts: bool = True, practice_tickets=()) -> RiskManager:
    """A RiskManager fed (real, practice) as the trader feeds it on PAPER."""
    cfg = Config()
    cfg.risk.daily_loss_counts_practice = counts
    rm = RiskManager(cfg)
    rm.realised_today = lambda: (real, practice)
    tickets = set(practice_tickets)
    rm.is_practice = lambda t: t in tickets
    return rm


def daily(snap):
    return next((b for b in snap.breakers if b.name == "DAILY_LOSS"), None)


# ====================================================== A. the daily-loss stop --
class TestPracticeNeverOffsetsRealMoney:
    def test_a_practice_profit_no_longer_hides_real_losses_past_the_limit(self):
        # a 1,900 day: real money -60 (3.16%), practice +40. The account holds only the real money: 1,840.
        snap = rm_split(-60.0, 40.0).snapshot(account(1_840.0), [], NOW)
        trip = daily(snap)
        assert trip is not None and trip.reason == "real money down 3.16% today (limit 3.00%)"
        assert not snap.entries_allowed and snap.daily_pnl == -60.0
        assert (snap.daily_real_pnl, snap.daily_practice_pnl) == (-60.0, 40.0)
        # the old single pot (-60 + 40 = -20) never tripped: the bug a verifier found on 10 Oct
        old = RiskManager(Config())
        old.realised_today = lambda: -20.0
        assert daily(old.snapshot(account(1_840.0), [], NOW)) is None

    def test_practice_losses_count_as_before_with_the_switch_on(self):
        snap = rm_split(0.0, -66.0).snapshot(account(1_900.0), [], NOW)
        trip = daily(snap)
        assert trip is not None and trip.reason == ("practice down 3.36% today (limit 3.00%; practice losses count "
                                                    "towards the stop)")
        old = RiskManager(Config())
        old.realised_today = lambda: -66.0
        same = old.snapshot(account(1_900.0), [], NOW)
        assert (snap.daily_pnl, snap.daily_pnl_pct) == (same.daily_pnl, same.daily_pnl_pct)   # as today, to the digit

    def test_with_the_switch_off_only_real_money_counts(self):
        assert daily(rm_split(0.0, -66.0, counts=False).snapshot(account(1_900.0), [], NOW)) is None
        assert daily(rm_split(-60.0, 40.0, counts=False).snapshot(account(1_840.0), [], NOW)) is not None
        both = rm_split(-60.0, -66.0, counts=False).snapshot(account(1_840.0), [], NOW)
        assert both.daily_pnl == -60.0 and daily(both).reason == "real money down 3.16% today (limit 3.00%)"
        assert RiskConfig().daily_loss_counts_practice is True                     # the default: practice counts
        assert RiskConfig().max_daily_loss_pct == 3.0                              # the limit never moves

    def test_both_sides_down_add_up_and_the_line_says_how(self):
        snap = rm_split(-30.0, -40.0).snapshot(account(1_870.0), [], NOW)
        assert snap.daily_pnl == -70.0
        assert daily(snap).reason == "down 3.61% today: real money 1.55%, practice 2.06% (limit 3.00%)"

    def test_open_positions_count_on_the_side_they_were_opened_on(self):
        paper_ticket = PAPER_FIRST_TICKET + 4
        positions = [pos(5_000_001, -30.0), pos(paper_ticket, 30.0, "EURUSD")]
        snap = rm_split(-30.0, 0.0, practice_tickets={paper_ticket}).snapshot(account(1_840.0), positions, NOW)
        assert (snap.daily_real_pnl, snap.daily_practice_pnl) == (-60.0, 30.0)
        assert daily(snap).reason == "real money down 3.16% today (limit 3.00%)"
        # a position it cannot place is counted as real money: a real loss is never hidden on paper
        rm = rm_split(0.0, 0.0)
        rm.is_practice = lambda t: (_ for _ in ()).throw(RuntimeError("unreadable"))
        assert rm.snapshot(account(1_900.0), [pos(7, -12.0)], NOW).daily_real_pnl == -12.0

    def test_within_a_side_profits_and_losses_net_as_before(self):
        snap = rm_split(50.0, 0.0).snapshot(account(1_890.0), [pos(5_000_001, -60.0)], NOW)
        assert snap.daily_real_pnl == -10.0 and snap.daily_pnl == -10.0             # never min(+50,0) + min(-60,0)
        up = rm_split(25.0, 15.0).snapshot(account(1_925.0), [], NOW)
        assert up.daily_pnl == 40.0                                                 # both up: their sum, as before

    @pytest.mark.parametrize("seed", range(6))
    def test_one_side_alone_is_exactly_the_old_figure(self, seed):
        import random
        rnd = random.Random(seed)
        realised = round(sum(rnd.uniform(-30, 30) for _ in range(5)), 2)
        positions = [pos(PAPER_FIRST_TICKET + i, round(rnd.uniform(-20, 20), 2)) for i in range(rnd.randint(0, 4))]
        equity = round(1_900.0 + rnd.uniform(-80, 80), 2)
        practice_only = rm_split(0.0, realised, practice_tickets={p.ticket for p in positions})
        real_only = rm_split(realised, 0.0)
        old = RiskManager(Config())
        old.realised_today = lambda: realised
        want = old.snapshot(account(equity), positions, NOW)
        for rm in (practice_only, real_only):
            got = rm.snapshot(account(equity), positions, NOW)
            assert (got.daily_pnl, got.daily_pnl_pct) == (want.daily_pnl, want.daily_pnl_pct)
            assert (daily(got) is None) == (daily(want) is None)

    def test_never_later_than_the_old_single_pot(self):
        for real, practice in ((-60, 40), (-10, -50), (30, -70), (0, -66), (-58, 0), (20, 20), (-80, 79)):
            equity = 1_900.0 + real
            new = rm_split(float(real), float(practice)).snapshot(account(equity), [], NOW)
            old = RiskManager(Config())
            old.realised_today = lambda r=real, p=practice: float(r + p)
            was = old.snapshot(account(equity), [], NOW)
            assert new.daily_pnl <= was.daily_pnl
            if daily(was) is not None:
                assert daily(new) is not None, (real, practice)                     # it trips whenever it did


# ------------------------------------------------ A, through the real trader --
def _close_all(pb, sim):
    for p in list(pb.positions(TNB)):
        if pb.is_paper(p.ticket):
            assert pb.close(p.ticket).ok
        else:
            assert sim.close(p.ticket).ok


class TestTheTraderReadsTheTwoApart:
    def test_fully_paper_is_exactly_the_old_figure(self, tmp_path):
        sim, cfg = setup(tmp_path, symbols=("EURUSD", "US500", "EURGBP"))
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        assert tr.risk.is_practice == pb.is_paper and tr.risk.realised_today == tr.realised_today_parts
        act(tr, sim, entry(sim, "EURUSD", 0.0030))
        act(tr, sim, entry(sim, "EURGBP", 0.0030))
        move(sim, "EURUSD", -0.0012)
        sim.advance(30)
        first = next(p for p in pb.positions(TNB) if p.symbol == "EURUSD")
        assert pb.close(first.ticket).ok
        move(sim, "EURGBP", -0.0008)
        tr._realised_cache = {"at": None, "value": 0.0}
        acct, positions = pb.account(), pb.positions(TNB)
        real, practice = tr.realised_today_parts()
        assert real == 0.0 and practice == tr.realised_today() and practice != 0.0
        got = tr.risk.snapshot(acct, positions, sim.now)
        old = RiskManager(cfg)
        old.realised_today = tr.realised_today                     # the single pot, as before 10 Oct
        want = old.snapshot(acct, positions, sim.now)
        assert (got.daily_pnl, got.daily_pnl_pct) == (want.daily_pnl, want.daily_pnl_pct)
        assert got.daily_real_pnl == 0.0 and got.daily_practice_pnl == want.daily_pnl
        tr.journal.close()
        pb.close_db()

    def test_fully_live_is_the_old_path_untouched(self, tmp_path):
        sim, cfg = setup(tmp_path, tnb="LIVE")
        tr = trader_on(sim, sim, cfg)
        assert tr.risk.realised_today == tr.realised_today and tr.risk.is_practice is None
        tr.risk.realised_today = lambda: -1000.0
        snap = tr.risk.snapshot(sim.account(), [], sim.now)
        assert daily(snap).reason.startswith("down ") and snap.daily_real_pnl is None
        tr.journal.close()

    def test_real_and_practice_deals_are_told_apart_by_ticket(self, tmp_path):
        sim, cfg = setup(tmp_path, symbols=("EURGBP", "EURUSD", "US500"))
        cfg.tnb.live_groups = ("FX_MINOR",)
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        act(tr, sim, entry(sim, "EURGBP", 0.0030))                 # real (Formula 1)
        act(tr, sim, entry(sim, "EURUSD", 0.0030))                 # practice
        move(sim, "EURGBP", -0.0010)
        move(sim, "EURUSD", 0.0010)
        sim.advance(30)
        _close_all(pb, sim)
        day = tr._strategy_day_start(sim.now)
        tr._realised_cache = {"at": None, "value": 0.0}
        real, practice = tr.realised_today_parts()
        assert real == pytest.approx(sum(float(r["profit"]) for r in sim.deals_since(day, TNB, False)))
        assert practice == pytest.approx(sum(float(r["profit"]) for r in pb.paper_deals_since(day, False)))
        assert real + practice == pytest.approx(tr.realised_today()) and real != 0.0 and practice != 0.0
        tr.journal.close()
        pb.close_db()

    def test_every_reader_reads_the_same_figure_and_says_which_part(self, tmp_path):
        from mintel.ops import standing as standing_mod
        from mintel.ops.dashboard import DashboardState
        from mintel.run import push_dashboard
        sim, cfg = setup(tmp_path, symbols=("EURGBP", "EURUSD", "US500"))
        cfg.tnb.live_groups = ("FX_MINOR",)
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        eq = sim.account().equity
        tr.risk.realised_today = lambda: (-0.04 * eq, 0.03 * eq)  # real money down 4%, practice up 3%
        tr.cycle()
        reasons = tr.last_cycle.blocked_reasons
        assert any(r.startswith("DAILY_LOSS: real money down ") for r in reasons), reasons   # Trend & Breakout
        assert tr._bots_may_enter() is False                        # the Runner, Band Breaker and Crowd Fader
        standing_mod._CACHE.update(at=None, value=None)
        state = DashboardState()
        push_dashboard(state, tr)
        assert any("real money down" in r for r in state.snapshot()["status"]["not_trading_because"])   # the page
        # practice alone down 4%: it counts by default ...
        tr.risk.realised_today = lambda: (0.0, -0.04 * eq)
        tr.cycle()
        assert any(r.startswith("DAILY_LOSS: practice down ") for r in tr.last_cycle.blocked_reasons)
        assert tr._bots_may_enter() is False
        # ... and with Aaron's decision 4 (only real money) it never stops the live bots
        cfg.risk.daily_loss_counts_practice = False
        tr.cycle()
        assert not any(r.startswith("DAILY_LOSS") for r in tr.last_cycle.blocked_reasons)
        assert tr._bots_may_enter() is True
        tr.journal.close()
        pb.close_db()

    def test_the_drawdown_stop_reads_the_account_alone_as_before(self):
        rm = rm_split(0.0, -500.0, counts=False)
        rm.snapshot(account(2_000.0), [], NOW)
        snap = rm.snapshot(account(1_700.0), [], NOW + dt.timedelta(hours=1))      # the real account down 15%
        assert any(b.name == "MAX_DRAWDOWN" for b in snap.breakers)
        assert "MAX_DRAWDOWN" in inspect.getsource(RiskManager.snapshot)
        assert "peak_equity - account.equity" in inspect.getsource(RiskManager.snapshot)

    def test_the_setting_loads_and_only_a_clear_false_turns_it_off(self, tmp_path):
        p = tmp_path / "config.json"
        for value, want in ((False, False), (True, True), ("false", True), (None, True)):
            p.write_text(json.dumps({"risk": {"daily_loss_counts_practice": value}} if value is not None else {}))
            cfg = Config.load(p)
            assert RiskManager(cfg).practice_counts() is want, value


# ============================================= B1. which approaches go real --
def send(inner, pb, symbol, tactic, key):
    bid, ask = PRICES[symbol]
    inner.set_tick(symbol, bid, ask)
    stop = round(bid - (ask - bid) * 40, 5)
    return pb.send(OrderRequest(symbol=symbol, side=Side.BUY, volume=0.1, sl=stop, tp=0.0, comment=key, magic=TNB,
                                idempotency_key=key, tactic=tactic))


class TestLiveTacticsRouting:
    def test_by_default_every_approach_on_a_live_market_goes_real_as_today(self, inner, tmp_path):
        pb = wrapper(inner, f1_config(tmp_path))
        inner.allow_orders = True
        for i, tactic in enumerate((MC, "TREND_PULLBACK", "")):
            res = send(inner, pb, "EURGBP", tactic, f"d{i}")
            assert res.ok and not pb.is_paper(res.ticket), tactic
        assert len([c for c in inner.order_calls if c[0] == "send"]) == 3
        pb.close_db()

    def test_only_the_listed_approach_goes_real_and_the_rest_stays_on_paper(self, inner, tmp_path):
        pb = wrapper(inner, f1_config(tmp_path, live_tactics=(MC,)))
        inner.allow_orders = True
        real = send(inner, pb, "EURGBP", MC, "a")
        twin = send(inner, pb, "AUDJPY", MC + "_2X", "b")                          # a twin: the same approach
        assert not pb.is_paper(real.ticket) and not pb.is_paper(twin.ticket)
        for i, tactic in enumerate(("TREND_PULLBACK", "", "momentum", "NOT_AN_APPROACH")):
            res = send(inner, pb, "EURAUD", tactic, f"p{i}")                       # unknown or none: paper
            assert res.ok and pb.is_paper(res.ticket), tactic
        major = send(inner, pb, "EURUSD", MC, "m")                                 # not a live market
        assert pb.is_paper(major.ticket)
        assert [c[1].tactic for c in inner.order_calls if c[0] == "send"] == [MC, MC + "_2X"]
        pb.close_db()

    def test_every_later_action_follows_the_side_it_was_opened_on(self, inner, tmp_path):
        pb = wrapper(inner, f1_config(tmp_path, live_tactics=(MC,)))
        inner.allow_orders = True
        real = send(inner, pb, "EURGBP", MC, "r")
        practice = send(inner, pb, "AUDJPY", "TREND_PULLBACK", "p")
        calls = len(inner.order_calls)
        assert pb.modify_stops(practice.ticket, 99.400, 0.0).ok and pb.close(practice.ticket, 0.05).ok
        assert pb.close(practice.ticket).ok and len(inner.order_calls) == calls     # all on paper, nothing sent
        assert pb.modify_stops(real.ticket, 0.85300, 0.0).ok and pb.close(real.ticket).ok
        assert [c[0] for c in inner.order_calls[calls:]] == ["modify_stops", "close"]
        pb.close_db()

    def test_a_list_naming_no_approach_keeps_every_market_on_paper(self, inner, tmp_path, caplog):
        with caplog.at_level(logging.WARNING, logger="mintel.paper"):
            pb = wrapper(inner, f1_config(tmp_path, live_tactics=("", "  ")))
        assert pb.live_groups == frozenset() and "names no approach" in caplog.text
        res = send(inner, pb, "EURGBP", MC, "x")
        assert pb.is_paper(res.ticket) and inner.order_calls == []
        pb.close_db()

    def test_the_executor_hands_the_approach_over(self, tmp_path):
        sim, cfg = setup(tmp_path, symbols=("EURGBP", "EURJPY", "EURUSD"))
        cfg.tnb.live_groups = ("FX_MINOR",)
        cfg.tnb.live_tactics = (MC,)
        cfg.risk.min_margin_level_pct = 0.0                     # the simulator's margin is not what is tested here
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        assert tr.tnb_live_tactics == (MC,)
        assert tr.tnb_mode_detail == "PAPER (FX_MINOR live, MOMENTUM_CONTINUATION only)"
        act(tr, sim, entry(sim, "EURGBP", 0.0030, tactic=MC))
        act(tr, sim, entry(sim, "EURJPY", 0.30, tactic="TREND_PULLBACK"))
        real = sim.positions(TNB)
        assert [p.symbol for p in real] == ["EURGBP"]                             # only momentum continuation
        mine = pb.positions(TNB)
        assert {p.symbol for p in mine} == {"EURGBP", "EURJPY"}
        assert pb.is_paper(next(p.ticket for p in mine if p.symbol == "EURJPY"))
        assert [o["req"].tactic for o in sim.order_log if int(o["req"].magic or 0) == TNB] == [MC]
        tr.journal.close()
        pb.close_db()

    def test_without_the_setting_the_trader_and_its_status_are_as_today(self, tmp_path):
        sim, cfg = setup(tmp_path, symbols=("EURGBP", "EURUSD", "US500"))
        cfg.tnb.live_groups = ("FX_MINOR",)
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        assert tr.tnb_live_tactics == () and tr.tnb_mode_detail == "PAPER (FX_MINOR live)"
        assert tnb_mode_detail("PAPER", ("FX_MINOR",)) == "PAPER (FX_MINOR live)"
        tr.journal.close()
        pb.close_db()


class TestLiveTacticsSetting:
    def test_empty_by_default(self):
        assert TnbConfig().live_tactics == () and Config().tnb.live_tactics == ()
        assert tnb_live_tactics(Config()) == ()
        assert MC in APPROACH_NAMES and "BREAKOUT_RETEST" in APPROACH_NAMES
        assert approach_of("momentum_continuation_2x") == MC and approach_of("") == ""

    def test_read_from_the_file_and_spelled_as_the_trader_spells_it(self, tmp_path):
        p = tmp_path / "config.json"
        p.write_text(json.dumps({"tnb": {"mode": "PAPER", "live_groups": ["FX_MINOR"],
                                         "live_tactics": "momentum_continuation"}}))
        c = Config.load(p)
        assert c.tnb.live_tactics == (MC,) and c.tnb.live_groups == ("FX_MINOR",)
        assert tnb_live_tactics(c) == (MC,)
        pb = PaperBroker.from_config(NS(), c)
        assert pb.live_tactics == frozenset({MC}) and pb.live_groups == frozenset({"FX_MINOR"})
        pb.close_db()

    def test_an_unknown_name_is_dropped_with_a_log_line(self, tmp_path, caplog):
        p = tmp_path / "config.json"
        p.write_text(json.dumps({"tnb": {"mode": "PAPER", "live_groups": ["FX_MINOR"],
                                         "live_tactics": [MC, "MOMENTUM_CONTINUTION"]}}))
        with caplog.at_level(logging.WARNING, logger="mintel.config"):
            c = Config.load(p)
        assert c.tnb.live_tactics == (MC,) and c.tnb.live_groups == ("FX_MINOR",)
        assert "'MOMENTUM_CONTINUTION' is not an approach" in caplog.text

    @pytest.mark.parametrize("bad", [["MOMENTUM_CONTINUTION"], 7, {"x": 1}])
    def test_a_list_that_names_no_approach_keeps_every_market_on_paper(self, tmp_path, caplog, bad):
        p = tmp_path / "config.json"
        p.write_text(json.dumps({"tnb": {"mode": "PAPER", "live_groups": ["FX_MINOR"], "live_tactics": bad,
                                         "live_since_utc": "2026-10-10T09:00:00+00:00"}}))
        with caplog.at_level(logging.WARNING, logger="mintel.config"):
            c = Config.load(p)
        assert c.tnb.live_groups == () and c.tnb.live_tactics == ()               # never "every approach"
        assert "every market stays on paper" in caplog.text

    def test_it_round_trips_through_the_file(self, tmp_path):
        c = Config()
        c.tnb.mode, c.tnb.live_groups, c.tnb.live_tactics = "PAPER", ("FX_MINOR",), (MC,)
        c.save(tmp_path / "config.json")
        assert json.loads((tmp_path / "config.json").read_text())["tnb"]["live_tactics"] == [MC]
        assert Config.load(tmp_path / "config.json").tnb == c.tnb


def f1_file(tmp_path: Path) -> Path:
    p = a_config(tmp_path)
    modes.set_modes(p, {}, tnb_live_groups=["FX_MINOR"], now=NOW - dt.timedelta(hours=14))
    return p


class TestLiveTacticsSwitch:
    def test_momentum_continuation_only_and_the_trial_starts_again(self, tmp_path, capsys):
        p = f1_file(tmp_path)
        before = json.loads(p.read_text())
        modes.set_modes(p, {}, tnb_live_tactics=["momentum_continuation"], now=NOW)
        after = json.loads(p.read_text())
        assert after["tnb"]["live_tactics"] == [MC] and after["tnb"]["live_since_utc"] == NOW.isoformat()
        for key in ("live_tactics", "live_since_utc"):
            before["tnb"][key] = after["tnb"][key]
        assert after == before                                                    # nothing else moved
        # the same set again keeps its time; a different one (wider here) is a new trial again
        modes.set_modes(p, {}, tnb_live_tactics=[MC], now=NOW + dt.timedelta(hours=1))
        assert json.loads(p.read_text())["tnb"]["live_since_utc"] == NOW.isoformat()
        later = NOW + dt.timedelta(hours=2)
        modes.set_modes(p, {}, tnb_live_tactics=["all"], now=later)
        raw = json.loads(p.read_text())
        assert raw["tnb"]["live_tactics"] == [] and raw["tnb"]["live_since_utc"] == later.isoformat()

    def test_the_lines_say_it_plainly(self, tmp_path, capsys):
        p = f1_file(tmp_path)
        assert modes.main(["--config", str(p), "--tnb-live-tactics", MC]) == 0
        out = capsys.readouterr().out
        assert re.search(r"Trend & Breakout: PAPER, minor currency pairs LIVE for momentum continuation only "
                         r"\(Formula 1\) since \d{4}-\d\d-\d\d \d\d:\d\d UTC\. Its momentum continuation orders on minor "
                         r"currency pairs go to the real broker; its other approaches there, and every other market, "
                         r"stay on paper \(practice\)", out)
        assert "The set of live trades changed, so the live trial starts again" in out
        assert modes.main(["--config", str(p), "--show"]) == 0
        line = next(x for x in capsys.readouterr().out.splitlines() if x.startswith("Trend & Breakout:"))
        assert "minor currency pairs LIVE for momentum continuation only (Formula 1) since" in line
        assert "tnb.live_tactics" in line and "its other approaches there and every other market simulated" in line
        check = modes.bot_health(p, "tnb", NOW)[1]
        assert "minor currency pairs LIVE for momentum continuation only (Formula 1) since" in check
        # the same again: no new trial, so no such line
        assert modes.main(["--config", str(p), "--tnb-live-tactics", MC]) == 0
        assert "live trial starts again" not in capsys.readouterr().out

    def test_in_the_same_call_as_the_live_markets(self, tmp_path):
        p = a_config(tmp_path, tnb_mode="LIVE")
        modes.set_modes(p, {}, tnb_live_groups=["FX_MINOR"], tnb_live_tactics=[MC], now=NOW)
        raw = json.loads(p.read_text())
        assert raw["tnb"]["mode"] == "PAPER" and raw["tnb"]["live_groups"] == ["FX_MINOR"]
        assert raw["tnb"]["live_tactics"] == [MC] and raw["tnb"]["live_since_utc"] == NOW.isoformat()

    @pytest.mark.parametrize("live_first,call", [
        (False, dict(tnb_live_tactics=[MC])),                                     # nothing kept LIVE
        (True, dict(tnb_live_tactics=[MC], tnb_live_groups=["none"])),            # ... nor after this call
        (True, dict(tnb_live_tactics=["MOMENTUM"])),                              # not an approach
        (True, dict(tnb_live_tactics=["all", MC])),
        (True, dict(tnb_live_tactics=[])),
    ])
    def test_refused_and_nothing_written(self, tmp_path, live_first, call):
        p = f1_file(tmp_path) if live_first else a_config(tmp_path)
        stamp = p.read_text()
        with pytest.raises(ValueError):
            modes.set_modes(p, {"rider": "OFF"}, now=NOW, **call)
        assert p.read_text() == stamp and not (tmp_path / "rider.json").exists()

    def test_refused_with_live_tnb_and_by_the_command_line_with_nothing_changed(self, tmp_path, capsys):
        p = f1_file(tmp_path)
        stamp = p.read_text()
        with pytest.raises(ValueError, match="--live tnb"):
            modes.set_modes(p, {"tnb": "LIVE"}, tnb_live_tactics=[MC], now=NOW)
        assert modes.main(["--config", str(p), "--tnb-live-tactics", "BOGUS"]) == 1
        assert capsys.readouterr().err.startswith("Nothing changed: unknown approach BOGUS")
        assert p.read_text() == stamp
        q = a_config(tmp_path / "q")
        assert modes.main(["--config", str(q), "--tnb-live-tactics", MC]) == 1
        assert "Nothing changed: --tnb-live-tactics chooses which approaches" in capsys.readouterr().err

    def test_the_tactics_survive_live_markets_switched_off_and_on(self, tmp_path):
        p = f1_file(tmp_path)
        modes.set_modes(p, {}, tnb_live_tactics=[MC], now=NOW)
        modes.set_modes(p, {}, tnb_live_groups=["none"], now=NOW)
        assert json.loads(p.read_text())["tnb"]["live_tactics"] == [MC]           # never silently wider
        modes.set_modes(p, {}, tnb_live_groups=["FX_MINOR"], now=NOW)
        assert Config.load(p).tnb.live_tactics == (MC,)


class DealingInner:
    """The fake real broker, booking an entry and an exit deal in the MT5
    adapter's shape for every real order (MADE-UP money)."""

    @staticmethod
    def attach(inner):
        send, close = inner.send, inner.close

        def booked_send(req):
            res = send(req)
            inner.real_deals.append({"position": res.ticket, "magic": TNB, "symbol": req.symbol, "volume": req.volume,
                                     "profit": -0.25, "commission": -0.25, "is_entry": True, "time": inner.now})
            return res

        def booked_close(ticket, volume=0.0, comment=""):
            p = next(x for x in inner.real_positions if x.ticket == ticket)
            res = close(ticket, volume, comment)
            inner.real_deals.append({"position": ticket, "magic": TNB, "symbol": p.symbol, "volume": p.volume,
                                     "profit": 4.75, "commission": -0.25, "is_entry": False, "time": inner.now})
            return res
        inner.send, inner.close = booked_send, booked_close
        return inner


class TestFormula1sLiveCountIsExactlyTheRoutedTrades:
    def test_the_page_counts_the_routed_trades_and_nothing_else(self, inner, tmp_path):
        from mintel.ops.verdicts import formula1_live
        DealingInner.attach(inner)
        inner.allow_orders = True
        # 9 Oct, every approach live: a real trend pullback on a minor pair
        old = wrapper(inner, f1_config(tmp_path))
        before = send(inner, old, "EURGBP", "TREND_PULLBACK", "old")
        assert not old.is_paper(before.ticket) and old.close(before.ticket).ok
        old.close_db()
        # 10 Oct, momentum continuation only - a new trial from now
        since = inner.now + dt.timedelta(seconds=1)
        pb = wrapper(inner, f1_config(tmp_path, live_tactics=(MC,), live_since_utc=since.isoformat()))
        inner.set_tick("EURGBP", *PRICES["EURGBP"], seconds=5)
        mc = send(inner, pb, "EURGBP", MC, "mc")
        tp = send(inner, pb, "AUDJPY", "TREND_PULLBACK", "tp")
        assert not pb.is_paper(mc.ticket) and pb.is_paper(tp.ticket)
        assert pb.close(mc.ticket).ok and pb.close(tp.ticket).ok
        rows = [dict(r, bot_magic=r["magic"]) for r in inner.real_deals]
        f1 = formula1_live(rows, [p.ticket for p in inner.real_positions], since)
        assert f1["stats"]["trades"] == 1 and f1["open"] == 0                    # the momentum continuation only
        everything = formula1_live(rows, [], since - dt.timedelta(hours=1))
        assert everything["stats"]["trades"] == 2                                # with the old start: both real ones
        pb.close_db()


# ================================================= B2. the top-opportunity size --
class TestTopSizeSwitch:
    def test_off_and_on_write_that_one_key(self, tmp_path, capsys):
        p = a_config(tmp_path)
        before = json.loads(p.read_text())
        assert before["risk"]["top_size_enabled"] is True                        # the default stays on
        assert modes.main(["--config", str(p), "--top-size", "off"]) == 0
        out = capsys.readouterr().out
        after = json.loads(p.read_text())
        assert after["risk"]["top_size_enabled"] is False
        before["risk"]["top_size_enabled"] = False
        assert after == before                                                   # nothing else, risk.top_* included
        assert ("Top-opportunity size: OFF (config.json: risk.top_size_enabled false). Every Trend & Breakout trade, "
                "Formula 1's included, risks 0.5% of the account and never more than 10.00 at its stop") in out
        assert "The Momentum Runner's own top size (runner.top_size_enabled) is not changed." in out
        assert Config.load(p).runner.top_size_enabled is True
        assert modes.main(["--config", str(p), "--top-size", "on"]) == 0
        out = capsys.readouterr().out
        assert json.loads(p.read_text())["risk"]["top_size_enabled"] is True
        assert ("A trade in momentum continuation on minor currency pairs - Formula 1's - may risk up to 40.00 "
                "(risk.top_risk_money), never more than 1.5% of the account and at most 0.5 lots") in out
        assert "otherwise it risks 0.5% of the account and never more than 10.00 at its stop" in out

    def test_off_means_no_top_size_for_formula_1(self, tmp_path):
        p = a_config(tmp_path)
        modes.set_modes(p, {}, top_size=False, now=NOW)
        rm = RiskManager(Config.load(p))
        state = NS(tactic=MC)
        assert rm.top_opportunity(state, "FX_MINOR", NOW) == (False, "")

    def test_a_value_that_is_not_on_or_off_is_refused(self, tmp_path):
        p = a_config(tmp_path)
        stamp = p.read_text()
        with pytest.raises(ValueError):
            modes.set_modes(p, {}, top_size="off", now=NOW)
        with pytest.raises(SystemExit):
            modes.main(["--config", str(p), "--top-size", "maybe"])
        assert p.read_text() == stamp


# ======================================================== B3. the news window --
class TestNewsWindowSwitch:
    def test_minutes_become_seconds_and_nothing_else_moves(self, tmp_path, capsys):
        p = a_config(tmp_path)
        before = json.loads(p.read_text())
        assert before["news"]["blackout_before_seconds"] == 90.0 == NewsConfig().blackout_before_seconds
        assert modes.main(["--config", str(p), "--news-before-minutes", "15"]) == 0
        out = capsys.readouterr().out
        after = json.loads(p.read_text())
        assert after["news"]["blackout_before_seconds"] == 900.0
        before["news"]["blackout_before_seconds"] = 900.0
        assert after == before
        assert ("News: Trend & Breakout takes no new trade - practice or real - in the 15 minutes before a "
                "high-importance release (importance 3 or more, news.min_importance_for_blackout) on either "
                "currency of the pair") in out
        assert ("The Momentum Runner, Band Breaker, Crowd Fader, Rapid Momentum Rider and Financial Ian do not read "
                "this setting") in out
        assert Config.load(p).news.blackout_before_seconds == 900.0

    @pytest.mark.parametrize("bad", ["0", "-5", "121", "nan", "inf"])
    def test_out_of_bounds_is_refused_and_nothing_written(self, tmp_path, capsys, bad):
        p = a_config(tmp_path)
        stamp = p.read_text()
        assert modes.main(["--config", str(p), "--news-before-minutes", bad]) == 1
        assert capsys.readouterr().err.startswith("Nothing changed: --news-before-minutes")
        assert p.read_text() == stamp

    def test_the_largest_window_is_allowed(self, tmp_path):
        p = a_config(tmp_path)
        modes.set_modes(p, {}, news_before_minutes=120, now=NOW)
        assert json.loads(p.read_text())["news"]["blackout_before_seconds"] == 7200.0


def _news_engine(sim, store, news_cfg, *events):
    from mintel.news.adapters import AdapterRegistry, Mt5CalendarAdapter
    from mintel.news.engine import NewsIntelligenceEngine
    for e in events:
        sim.add_event(e)
    eng = NewsIntelligenceEngine(AdapterRegistry([Mt5CalendarAdapter(sim)]), store, news_cfg)
    eng.refresh(sim.now)
    return eng


def _event(sim, currency, minutes, importance=3, n=1):
    return CalendarEvent(event_id=f"e{currency}{minutes}{importance}{n}", time_utc=sim.now + dt.timedelta(minutes=minutes),
                         currency=currency, country=currency, name="CPI y/y", importance=importance, actual=None,
                         forecast=3.1, previous=3.4)


class TestWhatTheNewsWindowTouches:
    def _verdict(self, eng, sim, symbol):
        from mintel.broker.base import TF
        return eng.verdict(symbol, sim.bars(symbol, TF.M1, 200), sim.tick(symbol), sim.spec(symbol), sim.now)

    @pytest.mark.parametrize("currency", ["EUR", "GBP"])
    def test_either_currency_of_the_pair(self, tmp_path, currency):
        from mintel.broker.sim import SimBroker
        from mintel.news.calendar_store import CalendarStore
        sim = SimBroker(["EURGBP", "USDJPY"], seed=5, start=MID_LONDON, history_bars=600)
        sim.connect()
        store = CalendarStore(tmp_path / "cal.sqlite")
        fifteen = NewsConfig(blackout_before_seconds=900.0)
        eng = _news_engine(sim, store, fifteen, _event(sim, currency, 10))
        assert self._verdict(eng, sim, "EURGBP").blackout                          # 10 minutes ahead, 15 allowed
        assert not self._verdict(eng, sim, "USDJPY").blackout                      # neither of its currencies
        today = _news_engine(sim, CalendarStore(tmp_path / "cal2.sqlite"), NewsConfig())
        assert not self._verdict(today, sim, "EURGBP").blackout                    # 90 seconds: not yet
        store.close()

    def test_only_releases_of_the_importance_it_names(self, tmp_path):
        from mintel.broker.sim import SimBroker
        from mintel.news.calendar_store import CalendarStore
        sim = SimBroker(["EURGBP"], seed=5, start=MID_LONDON, history_bars=600)
        sim.connect()
        store = CalendarStore(tmp_path / "cal.sqlite")
        eng = _news_engine(sim, store, NewsConfig(blackout_before_seconds=900.0), _event(sim, "GBP", 10, importance=2))
        assert not self._verdict(eng, sim, "EURGBP").blackout
        eng2 = _news_engine(sim, CalendarStore(tmp_path / "c2.sqlite"),
                            NewsConfig(blackout_before_seconds=900.0, min_importance_for_blackout=2))
        assert self._verdict(eng2, sim, "EURGBP").blackout
        store.close()

    def test_trend_and_breakouts_paper_and_live_markets_alike(self, tmp_path):
        sim, cfg = setup(tmp_path, symbols=("EURGBP", "GBPUSD", "EURUSD"))
        cfg.tnb.live_groups = ("FX_MINOR",)
        cfg.news.blackout_before_seconds = 900.0
        cfg.news.public_calendar_feed = False
        sim.add_event(_event(sim, "GBP", 12))
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        tr.refresh_news(force=True)
        live = tr.scanner.build_context("EURGBP", sim.now)                       # its orders would be real
        paper = tr.scanner.build_context("GBPUSD", sim.now)                      # its orders would be simulated
        other = tr.scanner.build_context("EURUSD", sim.now)
        assert live.news.blackout and paper.news.blackout and not other.news.blackout
        assert "no new entries" in live.news.reason
        tr.journal.close()
        pb.close_db()

    def test_the_other_bots_never_read_it(self):
        root = Path(__file__).resolve().parents[1] / "mintel"
        for pkg in ("runner", "bandbreaker", "crowd", "rider", "ian"):
            for f in (root / pkg).rglob("*.py"):
                text = f.read_text()
                assert "blackout_before_seconds" not in text and "min_importance_for_blackout" not in text, f
        assert "blackout_before_seconds" not in (root / "botexec.py").read_text()
        # Trend & Breakout's scanner is where it is read (a blocker on the setup, before the order is routed)
        assert "ctx.news.blackout" in (root / "engine" / "scanner.py").read_text()
        assert "news" not in inspect.getsource(PaperBroker.goes_real)


# ================================================== C. the binned Rider's icon --
SETUP_RIDER = MAC / "SETUP-RAPID-RIDER.command"


def _setup_rider(f, answer: str, snippet: str = 'setup_rider; echo "RC=$?"'):
    import os as _os
    env = {**_os.environ, "MINTEL_HOME": str(f.home), "MINTEL_NO_PIP": "1"}
    return subprocess.run(["bash", "-c", f'set -uo pipefail; source "{MAC / "mintel_mac.sh"}"; {snippet}'],
                          input=answer, capture_output=True, text=True, env=env, timeout=120)


class TestTheRiderIconAsksFirst:
    @pytest.mark.parametrize("answer", ["\n", "no\n", "live\n", "yes\n", ""])
    def test_after_formula1_anything_but_live_changes_nothing(self, tmp_path, answer):
        f = _fake_python_home(tmp_path)
        (f.data / FORMULA1).write_text("2026-10-09T19:45:00Z")
        r = _setup_rider(f, answer)
        assert "RC=1" in r.stdout and _calls(f.log) == []
        assert "The Rapid Momentum Rider was switched OFF on 9 Oct" in r.stdout
        assert "its own 10-day test on" in r.stdout and "found no edge" in r.stdout
        assert "Nothing changed" in r.stdout and "the Rapid Momentum Rider stays OFF" in r.stdout
        assert not (f.data / ".rider-live-2026-10-09").exists()

    def test_typing_live_puts_it_back_on(self, tmp_path):
        f = _fake_python_home(tmp_path)
        (f.data / FORMULA1).write_text("2026-10-09T19:45:00Z")
        (f.data / "rider.json").write_text(json.dumps({"mode": "OFF", "user_pip_value_gbp": 1.0}))
        r = _setup_rider(f, "LIVE\n")
        assert "RC=0" in r.stdout, r.stdout + r.stderr
        assert _calls(f.log) == ["--off scalper --live rider"]
        assert "You typed LIVE" in r.stdout

    def test_an_off_rider_without_the_marker_asks_too(self, tmp_path):
        f = _fake_python_home(tmp_path)
        (f.data / "rider.json").write_text(json.dumps({"mode": "OFF"}))
        r = _setup_rider(f, "\n")
        assert "RC=1" in r.stdout and _calls(f.log) == [] and "Nothing changed" in r.stdout

    def test_a_rider_not_switched_off_is_set_up_as_before(self, tmp_path):
        f = _fake_python_home(tmp_path)
        (f.data / "rider.json").write_text(json.dumps({"mode": "PAPER", "user_pip_value_gbp": 2.0}))
        r = _setup_rider(f, "")
        assert "RC=0" in r.stdout and _calls(f.log) == ["--off scalper --live rider"]
        assert "switched OFF on 9 Oct" not in r.stdout

    def test_the_whole_icon_stops_before_anything_runs(self, tmp_path):
        f = _fake_python_home(tmp_path)
        (f.data / FORMULA1).write_text("2026-10-09T19:45:00Z")
        env = {**os.environ, "MINTEL_HOME": str(f.home), "MINTEL_NO_PIP": "1"}
        r = subprocess.run(["bash", str(SETUP_RIDER)], input="no\n", capture_output=True, text=True, env=env,
                           timeout=120)
        assert r.returncode == 1
        assert "Nothing changed" in r.stdout and "the Rapid Momentum Rider stays OFF" in r.stdout
        assert _calls(f.log) == [] and _calls(f.other) == []                      # no modes call, nothing started
        assert "MARKET INTELLIGENCE" not in r.stdout and "HEALTHY" not in r.stdout

    def test_the_icon_says_so_and_is_still_bash_3_2(self):
        text = SETUP_RIDER.read_text()
        assert "only goes on if you type LIVE" in text and "setup_rider || { pause_close; exit 1; }" in text
        lib = (MAC / "mintel_mac.sh").read_text()
        body = lib.split("setup_rider() {")[1].split("\n}\n")[0]
        assert body.strip().startswith("confirm_rider_live || return 1")          # before anything is changed
        ask = lib.split("confirm_rider_live() {")[1].split("\n}\n")[0]
        assert '[[ "$answer" == "LIVE" ]]' in ask
        for path in (SETUP_RIDER, MAC / "mintel_mac.sh"):
            assert subprocess.run(["bash", "-n", str(path)], capture_output=True).returncode == 0
            for bad in (r"\$\{\w+,,", r"\$\{\w+\^\^", r"\bmapfile\b", r"\bdeclare\s+-A\b"):
                assert not re.search(bad, path.read_text()), (path, bad)


class TestTheWindowsRiderSetupAsksFirst:
    """PowerShell 5.1 is not on this machine: these read the scripts."""

    def test_it_asks_before_anything_runs_and_only_live_goes_on(self):
        helpers = (WIN / "mintel_bots.ps1").read_text()
        sacked = helpers.split("function Test-RiderSacked")[1].split("\nfunction ")[0]
        assert '".formula1-2026-10-09"' in sacked and '(Get-BotFileMode $P "rider") -eq "OFF"' in sacked
        ask = helpers.split("function Confirm-RiderLive")[1].split("\nfunction ")[0]
        assert 'if (-not (Test-RiderSacked $P)) { return $true }' in ask
        assert '"$answer".Trim() -ceq "LIVE"' in ask                              # case matters: LIVE only
        assert "its own 10-day test on real prices found no" in ask and "Nothing changed: the Rapid Momentum Rider stays OFF." in ask
        assert "catch { $answer = \"\" }" in ask                                  # no console: nothing changes
        setup_ps = (WIN / "SETUP-RAPID-RIDER.ps1").read_text()
        first = setup_ps.index("if (Test-RiderSacked $P)")
        assert first < setup_ps.index("SETUP-AND-START.ps1\")") < setup_ps.index("Set-RiderLive $P")
        assert setup_ps.index("-not $confirmed -and -not (Confirm-RiderLive $P)") < setup_ps.index("Set-RiderLive $P")
        for name in ("mintel_bots.ps1", "SETUP-RAPID-RIDER.ps1"):
            raw = (WIN / name).read_bytes()
            assert all(b < 128 for b in raw), name
            text = raw.decode()
            for a, b in (("{", "}"), ("(", ")")):
                assert text.count(a) == text.count(b), (name, a)


# ================================================ D. the Windows START-BOT --
class TestStartBotAppliesFormula1:
    def test_once_with_its_marker_and_a_restart_that_never_repeats(self):
        text = (WIN / "START-BOT.ps1").read_text()
        step = text.split("# ------------------------------------------- Formula 1, once (10 Oct) ----")[1]
        step = step.split("# ---------------------------------------------------------------- restart ----")[0]
        assert '$formula1Marker = Join-Path $dataDir ".formula1-2026-10-09"' in step
        assert "if (-not $FromSetup -and -not (Test-Path $formula1Marker))" in step
        assert "Set-Formula1Once $P" in step and "$Restart = $true" in step
        assert ".formula1-2026-10-09-part-restart" in step                       # part set: one restart, not one every 5 minutes
        # the step comes before the restart and before anything is started, after the settings are read
        assert (text.index("$cfg = Get-Content $configPath") < text.index("Set-Formula1Once $P")
                < text.index("if ($Restart) {") < text.index('Say "Watchdog"'))
        assert '. (Join-Path $PSScriptRoot "mintel_bots.ps1")' in text            # where Set-Formula1Once lives
        helpers = (WIN / "mintel_bots.ps1").read_text()
        once = helpers.split("function Set-Formula1Once")[1].split("\nfunction ")[0]
        assert "if (Test-Path $marker) { return $false }" in once                 # the same step as SETUP-AND-START
        raw = (WIN / "START-BOT.ps1").read_bytes()
        assert all(b < 128 for b in raw)
        decoded = raw.decode()
        for a, b in (("{", "}"), ("(", ")")):
            assert decoded.count(a) == decoded.count(b), a
        setup_ps = (WIN / "SETUP-AND-START.ps1").read_text()
        assert setup_ps.index("Set-Formula1Once $P") < setup_ps.index("-FromSetup -Restart")   # setup: its own, first


# ==================================== B1 on the page: which approaches are real --
class TestThePageSaysWhichApproachesAreReal:
    def test_the_setting_as_the_page_reads_it(self, tmp_path):
        from mintel.ops import verdicts as V

        def setting(block):
            (tmp_path / "config.json").write_text(json.dumps({"tnb": block}))
            return V.tnb_live_setting(str(tmp_path))
        base = {"mode": "PAPER", "live_groups": ["FX_MINOR"], "live_since_utc": "2026-10-10T09:00:00Z"}
        assert setting(base) == {"groups": ("FX_MINOR",), "since": NOW}                      # as before: no key
        assert setting(dict(base, live_tactics=["momentum_continuation"])) == {
            "groups": ("FX_MINOR",), "since": NOW, "tactics": (MC,)}
        assert setting(dict(base, live_tactics=["NOT_ONE"]))["groups"] == ()                 # as the trader: all paper
        assert setting(dict(base, live_tactics=[])) == {"groups": ("FX_MINOR",), "since": NOW}

    def test_the_words(self):
        from mintel.ops import attribution as A
        from mintel.ops import verdicts as V
        assert V.live_words(("FX_MINOR",)) == "LIVE: minor pairs (Formula 1) - rest PAPER"           # unchanged
        assert V.live_words(("FX_MINOR",), (MC,)) == ("LIVE: minor pairs (Formula 1, momentum continuation only) "
                                                      "- rest PAPER")
        assert V.live_kinds(("FX_MINOR", "INDEX"), (MC, "TREND_PULLBACK")) == (
            "minor pairs and indices (momentum continuation and trend pullback only)")
        assert "minor pairs (Formula 1, momentum continuation only): those trades are real orders" in \
            A.tnb_paper_source(("FX_MINOR",), (MC,))
        assert A.tnb_paper_source(("FX_MINOR",)) == A.tnb_paper_source(("FX_MINOR",), ())

    def test_the_main_page_row(self, tmp_path):
        from tests.test_formula1_page import _pages, a_formula1_day
        from tests.test_simple_dashboard import row_of
        d = a_formula1_day(tmp_path)
        raw = json.loads((d.data / "config.json").read_text())
        raw["tnb"]["live_tactics"] = [MC]
        (d.data / "config.json").write_text(json.dumps(raw))
        (page,) = _pages(d, "/")
        row = row_of(page, "Trend &amp; Breakout")
        assert ('<span class="s-mode live">LIVE: minor pairs (Formula 1, momentum continuation only) - rest PAPER'
                '</span>') in row


class TestTheStartWindowsSayWhichApproachesAreReal:
    def test_on_the_mac(self, tmp_path):
        from tests.test_integration_rider_ian import _bash, _home
        home = _home(tmp_path)
        cfg = home / "data" / "config.json"
        raw = json.loads(cfg.read_text())

        def words(block):
            raw["tnb"] = block
            cfg.write_text(json.dumps(raw))
            return _bash("tnb_mode_words", home).stdout.strip()
        f1 = {"mode": "PAPER", "live_groups": ["FX_MINOR"]}
        assert words(f1) == "PAPER - minor currency pairs LIVE (Formula 1)"                    # as before
        assert words(dict(f1, live_tactics=[])) == "PAPER - minor currency pairs LIVE (Formula 1)"
        assert words(dict(f1, live_tactics=[MC])) == ("PAPER - minor currency pairs LIVE for momentum continuation "
                                                      "only (Formula 1)")
        assert words(dict(f1, live_tactics=[MC, "TREND_PULLBACK"])) == (
            "PAPER - minor currency pairs LIVE for momentum continuation and trend pullback only")

    def test_on_windows(self):
        text = (WIN / "mintel_bots.ps1").read_text()
        words = text.split("function Get-TnbModeWords")[1].split("\nfunction ")[0]
        assert "$tactics = @(Get-TnbLiveTactics $P)" in words
        assert 'return "PAPER - $($words -join \', \') LIVE$only$f1"' in words
        assert '-join " and ") + " only"' in words
        reader = text.split("function Get-TnbLiveTactics")[1].split("\nfunction ")[0]
        assert "$b.live_tactics" in reader


# ============================== review fixes (10 Oct, second pass): the stop --
RUNNER = 990_511


def _by_magic(sim, runner_tickets):
    """The simulator's deal history filtered by magic number, as MetaTrader's
    adapter does (the simulator ignores the magic it is asked for)."""
    plain = sim.deals_since

    def deals_since(since, magic=0, closing_only=True):
        rows = [dict(r, magic=RUNNER if r["position"] in runner_tickets else TNB)
                for r in plain(since, magic, closing_only)]
        return [r for r in rows if not magic or r["magic"] == int(magic)]
    sim.deals_since = deals_since


class TestTheStopCountsTheRunnersRealMoney:
    """Only real money (decision 4) must still count the live Momentum
    Runner's real money: the stop gates it, and it is the account's main
    real-money bot."""

    def _a_runner_loss(self, tmp_path, *, close=True):
        sim, cfg = setup(tmp_path, symbols=("EURGBP", "EURUSD", "US500"))
        cfg.tnb.live_groups = ("FX_MINOR",)
        cfg.risk.min_margin_level_pct = 0.0
        pb = paper_on(sim, cfg)
        tr = trader_on(pb, sim, cfg)
        start = sim.account().equity
        act(tr, sim, entry(sim, "US500", 30.0))                       # Trend & Breakout's PAPER index entry ...
        tnb_paper = pb.positions(TNB)[0]
        t = sim.tick("US500")
        ride = sim.send(OrderRequest(symbol="US500", side=Side.BUY, volume=float(tnb_paper.volume) * 30,
                                     sl=round(t.bid - 300, 1), tp=0.0, comment="RUNNER", magic=RUNNER,
                                     idempotency_key="runner-1"))     # ... and the Runner's REAL ride, larger
        assert ride.ok
        _by_magic(sim, {ride.ticket})
        move(sim, "US500", -28.0)
        sim.advance(30)
        if close:
            assert sim.close(ride.ticket).ok
        assert pb.close(tnb_paper.ticket).ok
        real_down = (start - sim.account().equity) / start * 100.0
        assert real_down > 3.0                                        # the account's real money is down past 3%
        return sim, cfg, pb, tr

    @pytest.mark.parametrize("close", (True, False))
    def test_with_only_real_money_counting_the_runners_loss_trips_the_stop(self, tmp_path, close):
        sim, cfg, pb, tr = self._a_runner_loss(tmp_path, close=close)
        cfg.risk.daily_loss_counts_practice = False
        tr._realised_cache = {"at": None, "value": 0.0}
        snap = tr.risk.snapshot(pb.account(), pb.positions(TNB), sim.now)
        trip = daily(snap)
        assert trip is not None and trip.reason.startswith("real money down "), snap
        assert tr.risk.others_real == tr.others_real_today and tr.stop_gated_magics() == (RUNNER, 990_611, 990_711)
        assert snap.daily_real_pnl < 0 and not snap.entries_allowed
        tr._last_snapshot = snap
        assert tr._bots_may_enter() is False                          # the Runner itself is stopped
        tr.journal.close()
        pb.close_db()

    def test_by_default_the_figure_is_as_today_and_the_broker_is_not_asked(self, tmp_path):
        sim, cfg, pb, tr = self._a_runner_loss(tmp_path)
        asked = []
        tr.risk.others_real = lambda: asked.append(1) or -1_000.0
        tr._realised_cache = {"at": None, "value": 0.0}
        snap = tr.risk.snapshot(pb.account(), pb.positions(TNB), sim.now)
        old = RiskManager(cfg)
        old.realised_today = tr.realised_today                         # the single pot, as before 10 Oct
        want = old.snapshot(pb.account(), pb.positions(TNB), sim.now)
        assert asked == [] and (snap.daily_pnl, snap.daily_pnl_pct) == (want.daily_pnl, want.daily_pnl_pct)
        tr.journal.close()
        pb.close_db()

    def test_unreadable_other_bots_fall_back_to_the_account_as_before(self):
        rm = rm_split(0.0, 0.0, counts=False)
        rm.others_real = lambda: (_ for _ in ()).throw(RuntimeError("history unavailable"))
        rm.snapshot(account(1_900.0), [], NOW)                         # the day's first snapshot
        snap = rm.snapshot(account(1_840.0), [], NOW + dt.timedelta(minutes=5))
        assert snap.daily_pnl == -60.0 and daily(snap) is not None and snap.daily_real_pnl is None
        rm.others_real = lambda: float("nan")                           # nonsense is never read as nothing lost
        assert rm.snapshot(account(1_840.0), [], NOW + dt.timedelta(minutes=6)).daily_pnl == -60.0


class TestARealProfitStillCoversAPracticeLoss:
    """The default keeps today's figure except where a practice profit would
    hide a real loss (10 Oct review)."""

    def test_real_up_practice_down_is_todays_figure(self):
        snap = rm_split(100.0, -60.0).snapshot(account(1_972.0), [], NOW)
        old = RiskManager(Config())
        old.realised_today = lambda: 40.0
        was = old.snapshot(account(1_972.0), [], NOW)
        assert (snap.daily_pnl, snap.daily_pnl_pct) == (was.daily_pnl, was.daily_pnl_pct) == (40.0, 2.07)
        assert daily(snap) is None and snap.entries_allowed

    def test_when_it_trips_the_line_says_both(self):
        snap = rm_split(20.0, -80.0).snapshot(account(1_920.0), [], NOW)
        assert snap.daily_pnl == -60.0
        assert daily(snap).reason == ("down 3.03% today: practice down 4.04%, real money up 1.01% (limit 3.00%; "
                                      "practice losses count towards the stop)")

    def test_only_a_practice_profit_beside_a_real_loss_changes_the_figure(self):
        for real, practice in ((100, -60), (-30, -40), (25, 15), (0, -66), (-58, 0), (-60, 40), (30, -70)):
            new = rm_split(float(real), float(practice)).snapshot(account(1_900.0 + real), [], NOW)
            want = float(real) if real < 0 < practice else float(real + practice)
            assert new.daily_pnl == want, (real, practice)


class TestOnlyAJsonFalseStopsPracticeCounting:
    def test_null_zero_and_empty_keep_it_on(self, tmp_path):
        p = tmp_path / "config.json"
        for value, want in ((None, True), (0, True), ("", True), ([], True), ({}, True), ("no", True),
                            (1, True), (False, False), (True, True)):
            p.write_text(json.dumps({"risk": {"daily_loss_counts_practice": value}}))
            assert Config.load(p).risk.daily_loss_counts_practice is want, value


# ============================= review fixes (10 Oct, second pass): the modes tool --
def _tactics_in_file(p: Path, value) -> None:
    raw = json.loads(p.read_text())
    raw["tnb"]["live_tactics"] = value
    p.write_text(json.dumps(raw))


class TestAListNamingNoApproachReadsAsTheTraderReadsIt:
    @pytest.mark.parametrize("value", (["FOO"], ["MOMENTUM_CONTINUATON"], 5, {}, [""]))
    def test_show_and_check_say_every_market_is_on_paper(self, tmp_path, capsys, value):
        p = f1_file(tmp_path)
        _tactics_in_file(p, value)
        assert Config.load(p).tnb.live_groups == ()                              # the trader: all on paper
        assert modes.main(["--config", str(p), "--show"]) == 0
        out = capsys.readouterr().out
        line = next(x for x in out.splitlines() if x.startswith("Trend & Breakout:"))
        assert "LIVE" not in line.split("PAPER", 1)[1] and "tnb.live_groups" not in line, line
        assert "tnb.live_tactics names no approach Trend & Breakout trades, so every market stays on paper" in out
        check = modes.bot_health(p, "tnb", NOW)[1]
        assert "minor currency pairs LIVE" not in check and "every market is on paper" in check

    @pytest.mark.parametrize("value", ([], None, "", [MC, "FOO"]))
    def test_a_list_that_names_one_is_read_as_before(self, tmp_path, value):
        p = f1_file(tmp_path)
        _tactics_in_file(p, value)
        assert not modes.tnb_tactics_name_nothing(json.loads(p.read_text()))
        assert "minor currency pairs LIVE" in modes.bot_health(p, "tnb", NOW)[1]

    def test_putting_every_approach_live_from_there_starts_the_trial_again(self, tmp_path, capsys):
        p = f1_file(tmp_path)
        _tactics_in_file(p, ["MOMENTUM_CONTINUATON"])
        was = json.loads(p.read_text())["tnb"]["live_since_utc"]
        assert modes.main(["--config", str(p), "--tnb-live-tactics", "all"]) == 0
        assert json.loads(p.read_text())["tnb"]["live_since_utc"] != was          # nothing live -> every approach
        assert "The set of live trades changed, so the live trial starts again" in capsys.readouterr().out


class TestTheNewsWindowIsNeverRoundedAway:
    @pytest.mark.parametrize("bad", ["1e-9", "0.00001", "0.4"])
    def test_a_window_too_short_to_mean_anything_is_refused(self, tmp_path, capsys, bad):
        p = a_config(tmp_path)
        stamp = p.read_text()
        assert modes.main(["--config", str(p), "--news-before-minutes", bad]) == 1
        assert capsys.readouterr().err.startswith("Nothing changed: --news-before-minutes must be at least 0.5")
        assert p.read_text() == stamp

    def test_half_a_minute_is_the_least(self, tmp_path):
        p = a_config(tmp_path)
        modes.set_modes(p, {}, news_before_minutes=0.5, now=NOW)
        assert json.loads(p.read_text())["news"]["blackout_before_seconds"] == 30.0


class TestTheRiskLineNeverPromisesAnOffTopSize:
    def test_with_the_top_size_off(self, tmp_path, capsys):
        p = a_config(tmp_path)
        assert modes.main(["--config", str(p), "--top-size", "off", "--risk-pct", "0.7", "--risk-money", "13"]) == 0
        out = capsys.readouterr().out
        assert "A top-opportunity trade keeps its own limit" not in out
        assert "The top-opportunity size is OFF (risk.top_size_enabled), so no trade risks more." in out
        assert modes.main(["--config", str(p), "--risk-pct", "0.7"]) == 0             # later, on its own
        assert "A top-opportunity trade keeps its own limit" not in capsys.readouterr().out

    def test_with_the_top_size_on_as_before(self, tmp_path, capsys):
        p = a_config(tmp_path)
        assert modes.main(["--config", str(p), "--risk-pct", "0.7"]) == 0
        assert ("A top-opportunity trade keeps its own limit (risk.top_risk_money 40.00, at most 1.5% of the "
                "account).") in capsys.readouterr().out


class TestClearingTheMarketsSaysTheApproachListIsKept:
    def test_the_line_says_it(self, tmp_path, capsys):
        p = f1_file(tmp_path)
        modes.set_modes(p, {}, tnb_live_tactics=[MC], now=NOW)
        assert modes.main(["--config", str(p), "--tnb-live-groups", "none"]) == 0
        out = capsys.readouterr().out
        assert ("Trend & Breakout: no market kept LIVE - on PAPER every order is simulated (config.json: "
                "tnb.live_groups and tnb.live_since_utc cleared; tnb.live_tactics - momentum continuation only - "
                "is kept, and applies again if markets are kept LIVE later).") in out
        assert json.loads(p.read_text())["tnb"]["live_tactics"] == [MC]

    def test_without_an_approach_list_the_line_is_as_before(self, tmp_path, capsys):
        p = f1_file(tmp_path)
        assert modes.main(["--config", str(p), "--tnb-live-groups", "none"]) == 0
        assert ("Trend & Breakout: no market kept LIVE - on PAPER every order is simulated (config.json: "
                "tnb.live_groups and tnb.live_since_utc cleared).") in capsys.readouterr().out


class TestTheStartWindowsDropNamesTheTraderDoesNotKnow:
    def test_on_the_mac(self, tmp_path):
        from tests.test_integration_rider_ian import _bash, _home
        home = _home(tmp_path)
        cfg = home / "data" / "config.json"
        raw = json.loads(cfg.read_text())

        def words(tactics):
            raw["tnb"] = {"mode": "PAPER", "live_groups": ["FX_MINOR"], "live_tactics": tactics}
            cfg.write_text(json.dumps(raw))
            return _bash("tnb_mode_words", home).stdout.strip()
        nothing = "PAPER - every market (tnb.live_tactics names no approach it trades)"
        for value in (["FOO"], 5, {}, [""], "FOO"):
            assert words(value) == nothing, value
        assert words([MC, "FOO"]) == "PAPER - minor currency pairs LIVE for momentum continuation only (Formula 1)"
        assert words(["momentum_continuation_2x"]) == ("PAPER - minor currency pairs LIVE for momentum continuation "
                                                       "only (Formula 1)")
        for value in ([], None, ""):
            assert words(value) == "PAPER - minor currency pairs LIVE (Formula 1)", value

    @staticmethod
    def _names(text: str, start: str) -> tuple:
        block = text.split(start, 1)[1].split(")", 1)[0]
        return tuple(re.findall(r'"([A-Z_]+)"', block))

    def test_the_scripts_know_the_same_approaches_as_the_trader(self):
        mac = (MAC / "mintel_mac.sh").read_text()
        assert self._names(mac, "NAMES = (") == APPROACH_NAMES
        win = (WIN / "mintel_bots.ps1").read_text()
        reader = win.split("function Get-TnbLiveTactics")[1].split("\nfunction ")[0]
        assert self._names(reader, "$names = @(") == APPROACH_NAMES
        assert 'return @("NONE")' in reader
        words = win.split("function Get-TnbModeWords")[1].split("\nfunction ")[0]
        assert 'return "PAPER - every market (tnb.live_tactics names no approach it trades)"' in words

    def test_on_windows_when_powershell_is_here(self, tmp_path):
        import shutil
        pwsh = shutil.which("pwsh")
        if pwsh is None:
            pytest.skip("PowerShell is not on this machine: the script is read above")
        cfg = tmp_path / "config.json"
        out = {}
        for key, value in (("foo", ["FOO"]), ("five", 5), ("mixed", [MC, "FOO"]), ("empty", [])):
            cfg.write_text(json.dumps({"tnb": {"mode": "PAPER", "live_groups": ["FX_MINOR"], "live_tactics": value}}))
            script = (f'. "{WIN / "mintel_bots.ps1"}"; $P = @{{ Config = "{cfg}" }}; '
                      f'Write-Output (Get-TnbModeWords $P)')
            r = subprocess.run([pwsh, "-NoProfile", "-NonInteractive", "-Command", script], capture_output=True,
                               text=True, timeout=120)
            out[key] = r.stdout.strip().splitlines()[-1] if r.stdout.strip() else r.stderr
        assert out["foo"] == out["five"] == "PAPER - every market (tnb.live_tactics names no approach it trades)"
        assert out["mixed"] == "PAPER - minor currency pairs LIVE for momentum continuation only (Formula 1)"
        assert out["empty"] == "PAPER - minor currency pairs LIVE (Formula 1)"


# =========================== review fixes (10 Oct, second pass): the page's words --
def _f1_day_with(tmp_path, *, tactics=None, top_size=None):
    from tests.test_formula1_page import a_formula1_day
    d = a_formula1_day(tmp_path)
    raw = json.loads((d.data / "config.json").read_text())
    if tactics is not None:
        raw["tnb"]["live_tactics"] = tactics
    if top_size is not None:
        raw["risk"] = {"top_size_enabled": top_size}
    (d.data / "config.json").write_text(json.dumps(raw))
    return d


class TestFormula1sLinesNameTheTradesThatAreReal:
    def test_the_row_the_recommendation_and_the_evidence(self, tmp_path):
        from tests.test_formula1_page import _pages
        from tests.test_simple_dashboard import row_of
        d = _f1_day_with(tmp_path, tactics=[MC])
        home, detail = _pages(d, "/", "/?bot=market_intelligence")
        row = row_of(home, "Trend &amp; Breakout")
        assert "Formula 1 - its momentum continuation trades on minor pairs - is LIVE: judge it after 20" in row
        assert "its minor-pair trades" not in row
        assert "Formula 1 - its momentum continuation trades on minor pairs - is LIVE" in detail
        assert "Formula 1 LIVE - its momentum continuation trades on minor pairs with real money" in detail
        assert "its minor-pair trades" not in detail

    def test_without_the_setting_the_words_are_as_before(self, tmp_path):
        from tests.test_formula1_page import _pages
        from tests.test_simple_dashboard import row_of
        d = _f1_day_with(tmp_path)
        home, detail = _pages(d, "/", "/?bot=market_intelligence")
        assert "Formula 1 - its minor-pair trades - is LIVE: judge it after 20" in row_of(home, "Trend &amp; Breakout")
        assert "Formula 1 LIVE - its minor-pair trades with real money" in detail


class TestThePlanBoxSaysWhatConfigNowHolds:
    def test_both_answers_applied(self, tmp_path):
        from tests.test_formula1_page import _pages, _plan_box
        d = _f1_day_with(tmp_path, tactics=[MC], top_size=False)
        (home,) = _pages(d, "/")
        box = _plan_box(home)
        assert ("In force now (config.json): only its momentum continuation trades on minor pairs use real money "
                "(tnb.live_tactics); the top-opportunity size is OFF, so every trade risks the normal amount, never "
                "the bigger size (risk.top_size_enabled).") in box

    def test_one_answer_applied(self, tmp_path):
        from tests.test_formula1_page import _pages, _plan_box
        d = _f1_day_with(tmp_path, top_size=False)
        box = _plan_box(_pages(d, "/")[0])
        assert "In force now (config.json): the top-opportunity size is OFF" in box and "tnb.live_tactics" not in box

    def test_with_every_default_the_box_is_as_before(self, tmp_path):
        from tests.test_formula1_page import _pages, _plan_box
        d = _f1_day_with(tmp_path, tactics=[], top_size=True)
        assert "In force now" not in _plan_box(_pages(d, "/")[0])


# ========================= review fixes (10 Oct, second pass): the installers --
def _pwsh():
    import shutil
    return shutil.which("pwsh")


def _run_pwsh(script: str) -> str:
    r = subprocess.run([_pwsh(), "-NoProfile", "-NonInteractive", "-Command", script], capture_output=True,
                       text=True, timeout=120)
    assert r.returncode == 0, r.stderr
    return r.stdout


def _start_bot_formula1_block() -> str:
    text = (WIN / "START-BOT.ps1").read_text()
    step = text.split("# ------------------------------------------- Formula 1, once (10 Oct) ----")[1]
    return step.split("# ---------------------------------------------------------------- restart ----")[0]


class TestStartBotTriesFormula1OnceOnItsOwn:
    def test_a_part_set_step_is_never_tried_again_every_five_minutes(self):
        block = _start_bot_formula1_block()
        assert block.index("if (Test-Path $formula1Part)") < block.index("Set-Formula1Once $P")
        assert "it is not tried again on its own. Run SETUP-AND-START.cmd to try again" in block

    @pytest.mark.parametrize("completes", (False, True))
    def test_under_powershell(self, tmp_path, completes):
        if _pwsh() is None:
            pytest.skip("PowerShell is not on this machine: the script is read above")
        data = tmp_path / "data"
        data.mkdir()
        marker = f'Set-Content -Path (Join-Path "{data}" ".formula1-2026-10-09") -Value x; ' if completes else ""
        script = ("$dataDir = '" + str(data) + "'; $FromSetup = $false; $Restart = $false; "
                  "$P = @{ Data = '" + str(data) + "' }; $script:calls = 0; "
                  "function Say { param($m) Write-Output $m }; "
                  "function Test-MintelInstalled { param($P) return $true }; "
                  "function Set-Formula1Once { param($P) $script:calls += 1; " + marker + "return $true }; "
                  + _start_bot_formula1_block() + '; Write-Output "CALLS=$($script:calls) RESTART=$Restart"')
        first = _run_pwsh(script)
        assert "CALLS=1 RESTART=True" in first                                   # tried once, one restart
        later = _run_pwsh(script)                                                # 5 minutes later
        assert "CALLS=0 RESTART=False" in later                                  # never again on its own
        assert ((data / ".formula1-2026-10-09-part-restart").exists()) is (not completes)
        if not completes:
            assert "is not tried again on its own" in later


class TestALiveTypedForTheRiderIsNotUndoneByFormula1:
    def test_on_the_mac(self, tmp_path):
        f = _fake_python_home(tmp_path)
        (f.data / "rider.json").write_text(json.dumps({"mode": "OFF", "user_pip_value_gbp": 1.0}))
        r = _setup_rider(f, "LIVE\n", 'setup_rider; echo "RC=$?"; ' + F1_STEP)
        assert "RC=0" in r.stdout, r.stdout + r.stderr
        assert _calls(f.log) == ["--off scalper --live rider", "--tnb-live-groups FX_MINOR",
                                 "--risk-pct 0.7 --risk-money 13"]               # never "--off rider" after it
        assert (f.data / ".rider-live-confirmed").exists() and (f.data / FORMULA1).exists()
        assert "Rapid Momentum Rider: left as you chose - you typed LIVE for it." in r.stdout
        assert "CODE=yes" in r.stdout and "[WARN]" not in r.stdout

    def test_on_the_mac_a_no_leaves_the_step_as_it_was(self, tmp_path):
        f = _fake_python_home(tmp_path)
        (f.data / "rider.json").write_text(json.dumps({"mode": "OFF"}))
        r = _setup_rider(f, "no\n", 'setup_rider; echo "RC=$?"; ' + F1_STEP)
        assert "RC=1" in r.stdout and not (f.data / ".rider-live-confirmed").exists()
        assert _calls(f.log) == F1_CALLS

    def test_on_windows_read(self):
        helpers = (WIN / "mintel_bots.ps1").read_text()
        ask = helpers.split("function Confirm-RiderLive")[1].split("\nfunction ")[0]
        assert ask.index('-ceq "LIVE"') < ask.index('".rider-live-confirmed"') < ask.index("return $true\n    }")
        once = helpers.split("function Set-Formula1Once")[1].split("\nfunction ")[0]
        assert 'if (Test-Path (Join-Path $P.Data ".rider-live-confirmed"))' in once
        assert "$calls = @($calls[1], $calls[2])" in once

    @pytest.mark.parametrize("typed", (True, False))
    def test_on_windows_under_powershell(self, tmp_path, typed):
        if _pwsh() is None:
            pytest.skip("PowerShell is not on this machine: the script is read above")
        data = tmp_path / "data"
        data.mkdir()
        answer = "LIVE" if typed else "no"
        script = (f'. "{WIN / "mintel_bots.ps1"}"; '
                  "$P = @{ Data = '" + str(data) + "'; Config = 'x' }; $script:calls = @(); "
                  "function Test-RiderSacked { param($P) return $true }; "
                  "function Read-Host { param($m) return '" + answer + "' }; "
                  "function Test-MintelInstalled { param($P) return $true }; "
                  "function Invoke-ModesOrSay { param($P, [string[]] $Arguments) "
                  "$script:calls += ($Arguments -join ' '); return $true }; "
                  "$ok = Confirm-RiderLive $P; $changed = Set-Formula1Once $P; "
                  'Write-Output "OK=$ok"; Write-Output ("CALLS=" + ($script:calls -join "|"))')
        out = _run_pwsh(script)
        assert f"OK={typed}" in out
        want = F1_CALLS[1:] if typed else F1_CALLS
        assert "CALLS=" + "|".join(want) in out
        assert (data / ".rider-live-confirmed").exists() is typed
