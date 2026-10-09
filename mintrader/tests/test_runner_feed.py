"""Strengthening the Momentum Runner: the runner feed (Trend & Breakout on
PAPER still offers chosen approaches on index markets, so the Runner keeps
receiving them) and the Runner's own top size (LIVE).

The simulator's synthetic markets are not where these approaches come from,
so the tactics are replaced by deterministic signals (``market`` fixture)
and the tier and reward:risk gates are opened up for the mechanism to be
seen; each "every other gate" test closes one gate again and shows it bites.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import json

import pytest

from mintel.botexec import MAGIC_RUNNER
from mintel.broker.base import Position, Side
from mintel.broker.paper import PAPER_FIRST_TICKET, PaperBroker
from mintel.broker.sim import SimBroker
from mintel.config import Config, RunnerConfig as AppRunnerConfig
from mintel.engine import tactics as T
from mintel.engine.evidence import MarketState
from mintel.engine.scanner import RunnerFeedState
from mintel.engine.trader import Trader
from mintel.runner import feed
from mintel.runner.shadow import MomentumRunner, RunnerConfig
from tests.conftest import MID_LONDON, lifecycle_profile

TNB = 990_311
ALL_CONDITIONS = tuple(r.value for r in T.Regime)


# ----------------------------------------------------------------- helpers --
def signal(name="BREAKOUT_RETEST", quality=90.0, width=1.5, long_only=True):
    """A tactic that always sees ``name`` long: entry at the touch, the idea
    wrong ``width`` ATR below, room for six ATR."""
    def evaluate(ctx, side):
        if long_only and side < 0:
            return None
        entry = ctx.entry_price(Side.BUY if side > 0 else Side.SELL)
        return T.TacticSignal(name, side, quality, entry - side * width * ctx.atr_ref, entry,
                              target_hint=entry + side * 6.0 * ctx.atr_ref)
    return evaluate


@pytest.fixture
def market(monkeypatch):
    """Only the tactics a test adds exist (for T&B's own rules and the feed alike)."""
    tactics: list = []
    monkeypatch.setattr(T, "candidates_for", lambda regime: tuple(tactics))
    return tactics


def retest(quality=90.0, **kw):
    # BREAKOUT_RETEST is reported by the breakout-acceptance tactic, as in the real registry
    return T.Tactic("BREAKOUT_ACCEPTANCE", tuple(T.Regime), signal("BREAKOUT_RETEST", quality, **kw))


def make(tmp_path, *, tnb="PAPER", wrap=True, live_groups=(), symbols=("EURUSD", "US500")):
    sim = SimBroker(list(symbols), seed=101, start=MID_LONDON, history_bars=3000)
    sim.connect()
    cfg = lifecycle_profile(Config())
    cfg.ops.data_dir = str(tmp_path / "data")
    cfg.ops.log_dir = str(tmp_path / "logs")
    cfg.news.store_path = "calendar.sqlite"
    cfg.account_login, cfg.account_server = sim.login, sim.server
    cfg.tnb.mode = tnb
    cfg.tnb.live_groups = tuple(live_groups)
    cfg.runner.mode = "LIVE"
    cfg.scan.tier_normal, cfg.scan.tier_strong, cfg.scan.tier_exceptional = 1.0, 2.0, 3.0
    cfg.scan.min_reward_risk = 0.1
    cfg.scan.max_cost_fraction_of_stop = 1.0
    # Trend & Breakout's own rules: breakout retest off in every market condition
    cfg.scan.disabled_tactic_regimes = tuple(("BREAKOUT_RETEST", c) for c in ALL_CONDITIONS)
    cfg.runner.feed_tactics = ("BREAKOUT_RETEST",)
    broker = sim
    if wrap:
        broker = PaperBroker.from_config(sim, cfg) if tnb == "PAPER" else PaperBroker(sim, cfg.ops.data_dir)
        broker._now = lambda: sim.now               # the wrapper judges freshness on the simulator's clock
    tr = Trader(broker, cfg, clock=lambda: sim.now, enable_model=False)
    tr.scanner.refresh_universe()
    tr.scanner.timing_confirmation = lambda symbol, direction: (True, "")
    return sim, broker, cfg, tr


def state_on(tr, sim, symbol="US500", side=1, positions=()):
    ctx = tr.scanner.build_context(symbol, sim.now, positions=positions)
    assert ctx is not None
    return ctx, tr.scanner.state_for(ctx, side)


def close_all(*things):
    for x in things:
        fn = getattr(x, "close_db", None) or getattr(x, "close", None)
        try:
            fn()
        except Exception:
            pass


# --------------------------------------------------------------- the feed --
class TestTheSetting:
    def test_empty_by_default_on_the_evidence(self):
        rc = Config().runner
        assert rc.feed_tactics == () and rc.feed_entries() == ()

    def test_approach_and_condition_forms_and_a_bad_entry_left_out(self):
        rc = AppRunnerConfig(feed_tactics=("breakout_retest:trend", "SQUEEZE_RELEASE", ["LIQUIDITY_SWEEP_REVERSAL",
                                                                                     "HIGH_VOL"], "no good!", ""))
        assert rc.feed_entries() == (("BREAKOUT_RETEST", "TREND"), ("SQUEEZE_RELEASE", "*"),
                                     ("LIQUIDITY_SWEEP_REVERSAL", "HIGH_VOL"))

    def test_the_runner_skips_and_approaches_off_everywhere_are_never_fed(self):
        c = Config()
        c.runner.feed_tactics = ("MOMENTUM_CONTINUATION", "NEWS_CONTINUATION", "BREAKOUT_RETEST:TREND")
        assert feed.names_for(c, "TREND") == {"BREAKOUT_RETEST"}
        assert feed.names_for(c, "HIGH_VOL") == frozenset()

    def test_only_index_markets_the_runner_rides_and_never_an_excluded_one(self):
        c = Config()
        assert feed.market_wanted(c, "US500", "INDEX") and feed.market_wanted(c, "US30", "INDEX")
        assert not feed.market_wanted(c, "EURUSD", "FX_MAJOR") and not feed.market_wanted(c, "XAUUSD", "GOLD")
        assert not feed.market_wanted(c, "DE40", "INDEX") and not feed.market_wanted(c, "UK100", "INDEX")
        c.runner.markets = ("US30",)
        assert not feed.market_wanted(c, "US500", "INDEX") and feed.market_wanted(c, "US30", "INDEX")


class TestWhenItOffers:
    def test_an_index_gets_the_listed_approach_while_tnb_is_on_paper(self, tmp_path, market):
        market.append(retest())
        sim, pb, cfg, tr = make(tmp_path)
        assert feed.active(cfg, pb)
        _, st = state_on(tr, sim)
        assert isinstance(st, RunnerFeedState) and st.runner_feed and st.tradable
        assert st.tactic == "BREAKOUT_RETEST" and st.symbol == "US500"
        assert feed.FEED_NOTE in st.notes and st.to_row()["runner_feed"] is True
        close_all(tr.journal, pb)

    def test_without_it_trend_and_breakouts_own_rules_still_say_no(self, tmp_path, market):
        market.append(retest())
        sim, pb, cfg, tr = make(tmp_path)
        cfg.runner.feed_tactics = ()
        assert not feed.active(cfg, pb)
        assert state_on(tr, sim)[1] is None
        close_all(tr.journal, pb)

    def test_never_while_trend_and_breakout_is_live(self, tmp_path, market):
        market.append(retest())
        sim, broker, cfg, tr = make(tmp_path, tnb="LIVE", wrap=False)
        assert not feed.active(cfg, broker) and state_on(tr, sim)[1] is None
        close_all(tr.journal)

    def test_never_when_the_trader_holds_the_real_broker(self, tmp_path, market):
        """The setting alone is not enough: a feed signal must only ever be able to become a PAPER trade."""
        market.append(retest())
        sim, broker, cfg, tr = make(tmp_path, tnb="PAPER", wrap=False)
        assert not feed.active(cfg, broker) and state_on(tr, sim)[1] is None
        close_all(tr.journal)

    def test_never_when_index_orders_are_kept_live(self, tmp_path, market):
        market.append(retest())
        sim, pb, cfg, tr = make(tmp_path, live_groups=("INDEX",))
        assert not feed.active(cfg, pb) and state_on(tr, sim)[1] is None
        close_all(tr.journal, pb)

    def test_never_when_the_runner_is_off(self, tmp_path, market):
        market.append(retest())
        sim, pb, cfg, tr = make(tmp_path)
        cfg.runner.mode = "OFF"
        assert not feed.active(cfg, pb) and state_on(tr, sim)[1] is None
        close_all(tr.journal, pb)

    def test_only_index_markets(self, tmp_path, market):
        market.append(retest())
        sim, pb, cfg, tr = make(tmp_path)
        assert state_on(tr, sim, "EURUSD")[1] is None
        close_all(tr.journal, pb)

    def test_only_listed_approaches_and_conditions(self, tmp_path, market):
        market.append(retest())
        sim, pb, cfg, tr = make(tmp_path)
        ctx = tr.scanner.build_context("US500", sim.now)
        here = ctx.regime.regime.value
        cfg.runner.feed_tactics = ("SQUEEZE_RELEASE",)
        assert tr.scanner.state_for(ctx, 1) is None
        other = next(c for c in ALL_CONDITIONS if c != here)
        cfg.runner.feed_tactics = (f"BREAKOUT_RETEST:{other}",)
        assert tr.scanner.state_for(ctx, 1) is None
        cfg.runner.feed_tactics = (f"BREAKOUT_RETEST:{here}",)
        assert isinstance(tr.scanner.state_for(ctx, 1), RunnerFeedState)
        close_all(tr.journal, pb)

    def test_also_past_a_kind_of_market_switch(self, tmp_path, market):
        market.append(retest())
        sim, pb, cfg, tr = make(tmp_path)
        cfg.scan.disabled_tactic_regimes = ()
        cfg.scan.disabled_tactic_groups = (("BREAKOUT_RETEST", "INDEX"),)
        assert isinstance(state_on(tr, sim)[1], RunnerFeedState)
        cfg.runner.feed_tactics = ()
        assert state_on(tr, sim)[1] is None
        close_all(tr.journal, pb)

    def test_excluded_markets_stay_out(self, tmp_path, market):
        market.append(retest())
        sim, pb, cfg, tr = make(tmp_path)
        cfg.universe.excluded_symbols = ("US500",)
        tr.scanner.refresh_universe()
        assert "US500" not in tr.scanner.universe.symbols
        assert tr.scanner.build_context("US500", sim.now) is None
        assert not any(s.symbol == "US500" for s in tr.scanner.scan(sim.now, []))
        close_all(tr.journal, pb)

    def test_trend_and_breakouts_own_tradable_signal_comes_first_untagged(self, tmp_path, market):
        market.append(retest(quality=95.0))
        market.append(T.Tactic("SESSION_EXPANSION", tuple(T.Regime), signal("SESSION_EXPANSION", 60.0)))
        sim, pb, cfg, tr = make(tmp_path)
        _, st = state_on(tr, sim)
        assert st.tactic == "SESSION_EXPANSION" and type(st) is MarketState and not getattr(st, "runner_feed", False)
        close_all(tr.journal, pb)


class TestEveryOtherGateStillApplies:
    def test_the_score_floor(self, tmp_path, market):
        market.append(retest())
        sim, pb, cfg, tr = make(tmp_path)
        cfg.scan.tier_normal, cfg.scan.tier_strong, cfg.scan.tier_exceptional = 97.0, 98.0, 99.0
        assert state_on(tr, sim)[1] is None
        close_all(tr.journal, pb)

    def test_the_cost_gate(self, tmp_path, market):
        market.append(retest())
        sim, pb, cfg, tr = make(tmp_path)
        cfg.scan.max_cost_fraction_of_stop = 0.0001
        assert state_on(tr, sim)[1] is None
        close_all(tr.journal, pb)

    def test_the_spread_gate(self, tmp_path, market):
        market.append(retest())
        sim, pb, cfg, tr = make(tmp_path)
        sim.fault.spread_multiplier = 10.0
        assert state_on(tr, sim)[1] is None
        close_all(tr.journal, pb)

    def test_a_news_blackout(self, tmp_path, market):
        market.append(retest())
        sim, pb, cfg, tr = make(tmp_path)
        ctx = tr.scanner.build_context("US500", sim.now)
        ctx.news = dataclasses.replace(ctx.news, blackout=True, reason="no new entries around the release")
        assert tr.scanner.state_for(ctx, 1) is None
        close_all(tr.journal, pb)

    def test_a_position_already_open_there(self, tmp_path, market):
        market.append(retest())
        sim, pb, cfg, tr = make(tmp_path)
        t = sim.tick("US500")
        mine = Position(PAPER_FIRST_TICKET, "US500", Side.BUY, 0.1, t.ask, t.ask - 50.0, 0.0, sim.now, magic=TNB)
        assert state_on(tr, sim, positions=[mine])[1] is None
        close_all(tr.journal, pb)

    def test_the_breakers_hold_the_entry(self, tmp_path, market):
        market.append(retest())
        sim, pb, cfg, tr = make(tmp_path)
        tr.bootstrap()
        tr.risk.halt("test: a breaker holds")
        tr.cycle()
        assert pb.positions(TNB) == [] and sim.order_log == []
        close_all(tr.journal, pb)


class TestTagged:
    def test_a_fed_trade_is_paper_tagged_and_the_runner_rides_it_with_a_real_order(self, tmp_path, market):
        market.append(retest())
        sim, pb, cfg, tr = make(tmp_path)
        tr.bootstrap()
        result = tr.cycle()
        assert result.acted is not None and getattr(result.acted, "runner_feed", False), result.blocked_reasons
        mine = pb.positions(TNB)
        assert len(mine) == 1 and mine[0].ticket >= PAPER_FIRST_TICKET and pb.is_paper(mine[0].ticket)
        assert sim.positions(TNB) == [] and not [o for o in sim.order_log if o["req"].magic == TNB]
        rows = tr.journal.open_trades()
        assert [r["ticket"] for r in rows] == [mine[0].ticket] and feed.is_feed_trade(rows[0])
        entry = [e for e in tr.journal.recent_events(50) if e["kind"] == "ENTRY"]
        assert entry and json.loads(entry[0]["detail_json"])["runner_feed"] is True
        tr.manage_cycle()                                   # the Runner sees the fresh index entry
        runner = sim.positions(MAGIC_RUNNER)
        assert len(runner) == 1 and runner[0].symbol == "US500" and runner[0].comment == "RUNNER"
        assert runner[0].volume == pytest.approx(mine[0].volume) and runner[0].sl == pytest.approx(mine[0].sl)
        assert [o["req"].magic for o in sim.order_log] == [MAGIC_RUNNER]
        close_all(tr.journal, pb)

    def test_trend_and_breakouts_own_trades_are_not_tagged(self, tmp_path, market):
        market.append(T.Tactic("SESSION_EXPANSION", tuple(T.Regime), signal("SESSION_EXPANSION", 80.0)))
        sim, pb, cfg, tr = make(tmp_path)
        tr.bootstrap()
        result = tr.cycle()
        assert result.acted is not None and not getattr(result.acted, "runner_feed", False), result.blocked_reasons
        rows = tr.journal.open_trades()
        assert len(rows) == 1 and not feed.is_feed_trade(rows[0])
        close_all(tr.journal, pb)

    def test_is_feed_trade_reads_rows_and_events(self):
        assert feed.is_feed_trade({"entry_state_json": json.dumps({"runner_feed": True})})
        assert feed.is_feed_trade({"runner_feed": True})
        assert not feed.is_feed_trade({"entry_state_json": json.dumps({"tactic": "SESSION_EXPANSION"})})
        assert not feed.is_feed_trade({"entry_state_json": "not json"}) and not feed.is_feed_trade({})


# -------------------------------------------------------- the top size --
T0 = dt.datetime(2026, 10, 7, 13, 0, tzinfo=dt.timezone.utc)
SEGMENT = (("BREAKOUT_RETEST", "INDEX"),)


def index_sim(balance=2_000.0, symbols=("US500",)):
    b = SimBroker(list(symbols), start=T0, history_bars=300, balance=balance)
    b.connect()
    return b


def live(tmp_path, broker, **kw):
    kw.setdefault("top_segments", SEGMENT)
    return MomentumRunner(tmp_path, RunnerConfig(mode="LIVE", **kw), spec_fn=broker.spec, clock=lambda: broker.now,
                          broker=broker)


def real_trade(broker, ticket=1, volume=0.33, risk=30.0, symbol="US500"):
    """Trend & Breakout's fresh trade as the Runner sees it (it is not at the sim)."""
    t = broker.tick(symbol)
    return Position(ticket, symbol, Side.BUY, volume, t.ask, t.ask - risk, 0.0, broker.now, magic=TNB), t.ask - risk


def ride(r, broker, ticket=1, tactic="BREAKOUT_RETEST", regime="", **kw):
    symbol = kw.get("symbol", "US500")
    mid = broker.tick(symbol).mid
    broker.push_bar(symbol, mid, high=mid, low=mid)     # a flat bar: no synthetic wick can reach a tight test stop
    p, sl = real_trade(broker, ticket, **kw)
    notes = r.observe([p], {ticket: sl}, broker.tick, broker.now, tactics={ticket: tactic}, regimes={ticket: regime})
    mine = [x for x in broker.positions(MAGIC_RUNNER) if x.ticket in r.open and r.open[x.ticket].source_ticket == ticket]
    return (mine[0] if mine else None), notes


def seed_rides(r, n, net, tactic="BREAKOUT_RETEST", symbol="US500", start=880_000_001):
    for i in range(n):
        r.db.execute("""INSERT INTO trades (ticket, source_ticket, symbol, side, volume, entry, initial_stop,
                        risk_distance, opened_utc, closed_utc, stop, net_pnl, realised_r, exit_reason, mode, tactic)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                     (start + i, 870_000_001 + i, symbol, "BUY", 1.0, 5000.0, 4970.0, 30.0,
                      (T0 - dt.timedelta(days=1, minutes=i)).isoformat(), (T0 - dt.timedelta(hours=20, minutes=i)).isoformat(),
                      4970.0, net, net / 30.0, "INITIAL_STOP" if net < 0 else "TRAIL_STOP", "LIVE", tactic))
    r.db.commit()


class TestRunnerTopSize:
    def test_off_by_default_on_the_evidence(self, tmp_path):
        assert Config().runner.top_segments == () and RunnerConfig().top_segments == ()
        b = index_sim()
        r = live(tmp_path, b, top_segments=())
        mine, _ = ride(r, b)
        assert mine.volume == pytest.approx(0.33)

    def test_a_qualifying_segment_is_sized_for_its_money_at_the_stop_under_one_and_a_half_percent(self, tmp_path):
        b = index_sim(balance=2_000.0)                      # 1.5% of 2,000 = 30, which binds before 40
        r = live(tmp_path, b)
        mine, notes = ride(r, b)
        spec = b.spec("US500")
        risk = spec.money_per_lot(mine.entry_price - mine.sl) * mine.volume
        assert mine.volume > 0.33 and risk <= 30.0 + 1e-6 and risk > 29.0
        assert r.open[mine.ticket].top and any("top size for breakout retest on index" in n for n in notes)
        row = r.db.execute("SELECT top_size FROM trades WHERE ticket=?", (mine.ticket,)).fetchone()
        assert row[0] == 1

    def test_on_a_larger_account_the_top_money_binds(self, tmp_path):
        b = index_sim(balance=10_000.0)                     # 1.5% = 150: the 40 binds
        r = live(tmp_path, b)
        mine, _ = ride(r, b)
        risk = b.spec("US500").money_per_lot(mine.entry_price - mine.sl) * mine.volume
        assert 39.0 < risk <= 40.0 + 1e-6

    def test_a_ride_outside_the_segment_copies_the_real_volume(self, tmp_path):
        b = index_sim()
        r = live(tmp_path, b)
        mine, notes = ride(r, b, tactic="SESSION_EXPANSION")
        assert mine.volume == pytest.approx(0.33) and not r.open[mine.ticket].top
        assert not any("top size" in n for n in notes)

    def test_a_segment_narrowed_to_one_condition(self, tmp_path):
        b = index_sim()
        r = live(tmp_path, b, top_segments=(("BREAKOUT_RETEST", "INDEX", "TREND"),))
        assert ride(r, b, ticket=1, regime="RANGE")[0].volume == pytest.approx(0.33)
        assert ride(r, b, ticket=2, regime="TREND")[0].volume > 0.33

    def test_the_brokers_volume_limits(self, tmp_path):
        b = index_sim(balance=10_000.0)
        b._specs["US500"] = dataclasses.replace(b.spec("US500"), volume_max=0.5)
        r = live(tmp_path, b)
        mine, _ = ride(r, b)
        assert mine.volume == pytest.approx(0.5)

    def test_never_smaller_than_the_real_trade(self, tmp_path):
        b = index_sim(balance=2_000.0)
        r = live(tmp_path, b)
        mine, notes = ride(r, b, volume=1.5)                # T&B's volume already above what the top size allows
        assert mine.volume == pytest.approx(1.5) and any("top size not used" in n for n in notes)

    def test_outside_indices_at_most_half_a_lot(self, tmp_path):
        b = index_sim(balance=10_000.0, symbols=("EURUSD",))
        r = live(tmp_path, b, markets=("EURUSD",), top_segments=(("BREAKOUT_RETEST", "FX_MAJOR"),))
        mine, notes = ride(r, b, symbol="EURUSD", volume=0.05, risk=0.0005)    # 40 at 5 pips is 0.8 lots
        assert mine.volume == pytest.approx(0.5) and any("held to 0.5 lots" in n for n in notes)

    def test_the_margin_check(self, tmp_path):
        b = index_sim(balance=2_000.0, symbols=("EURUSD",))
        r = live(tmp_path, b, markets=("EURUSD",), top_segments=(("BREAKOUT_RETEST", "FX_MAJOR"),))
        mine, notes = ride(r, b, symbol="EURUSD", volume=0.05, risk=0.0005)
        acct = b.account()
        assert 0.05 < mine.volume < 0.5 and any("margin level" in n for n in notes)
        assert acct.margin_level >= 300.0 - 1e-6

    def test_its_own_negative_rides_put_it_back_to_normal_and_it_says_why(self, tmp_path):
        b = index_sim()
        r = live(tmp_path, b)
        seed_rides(r, 10, -8.0)
        mine, notes = ride(r, b)
        assert mine.volume == pytest.approx(0.33) and not r.open[mine.ticket].top
        assert any("normal size: its own last 10 live rides in breakout retest on index are negative" in n
                   for n in notes)

    def test_fewer_than_ten_of_its_own_rides_do_not_decide(self, tmp_path):
        b = index_sim()
        r = live(tmp_path, b)
        seed_rides(r, 9, -8.0)
        assert ride(r, b)[0].volume > 0.33

    def test_its_own_earning_rides_keep_the_top_size(self, tmp_path):
        b = index_sim()
        r = live(tmp_path, b)
        seed_rides(r, 12, +15.0)
        assert ride(r, b)[0].volume > 0.33

    def test_a_loss_never_makes_the_next_one_larger(self, tmp_path):
        b = index_sim(balance=2_000.0)
        r = live(tmp_path, b)
        seed_rides(r, 9, -8.0)                              # nine of its own losing rides on record
        first, _ = ride(r, b, ticket=1)
        assert first.volume > 0.33
        b.push_bar("US500", first.sl - 5.0, low=first.sl - 6.0)          # stopped at the broker: a loss
        r.observe([], {}, b.tick, b.now)
        assert r.db.execute("SELECT net_pnl FROM trades WHERE ticket=?", (first.ticket,)).fetchone()[0] < 0
        second, notes = ride(r, b, ticket=2)
        assert second.volume <= first.volume                # the tenth loss: back to the copied volume
        assert second.volume == pytest.approx(0.33) and any("normal size" in n for n in notes)

    def test_after_a_loss_with_the_record_still_short_the_size_only_shrinks_with_the_account(self, tmp_path):
        b = index_sim(balance=2_000.0)
        r = live(tmp_path, b)
        first, _ = ride(r, b, ticket=1)
        b.push_bar("US500", first.sl - 5.0, low=first.sl - 6.0)
        r.observe([], {}, b.tick, b.now)
        assert b.account().equity < 2_000.0
        second, _ = ride(r, b, ticket=2)
        assert second.volume <= first.volume

    def test_paper_shadows_keep_the_real_volume(self, tmp_path):
        b = index_sim()
        r = MomentumRunner(tmp_path, RunnerConfig(mode="PAPER", top_segments=SEGMENT), spec_fn=b.spec,
                           clock=lambda: b.now, broker=b)
        p, sl = real_trade(b)
        s = r.adopt(p, sl, b.now, tactic="BREAKOUT_RETEST")
        assert s.volume == pytest.approx(0.33) and b.order_log == []

    def test_the_trader_hands_the_runner_the_operators_ceiling(self, tmp_path):
        sim = index_sim()
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path)
        cfg.runner.mode = "LIVE"
        cfg.risk.max_risk_pct = 1.0
        cfg.runner.top_segments = (["BREAKOUT_RETEST", "INDEX"],)        # as a config file gives it
        tr = Trader(sim, cfg, clock=lambda: sim.now, enable_model=False)
        assert tr.runner.cfg.account_max_risk_pct == 1.0
        assert tr.runner.cfg.account_min_margin_level_pct == cfg.risk.min_margin_level_pct
        assert tr.runner.top_segment("US500", "BREAKOUT_RETEST") == ("BREAKOUT_RETEST", "INDEX", "")
        assert tr.runner.status()["top_size"]["on"] is True
        tr.journal.close()
