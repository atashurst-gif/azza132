"""Day two's lesson: half the pace, wiser choices, and winners held.

Day one and two lost mostly to fees: many NORMAL-tier trades with a reward
barely above the risk, banked early, stopped often.  The defaults now trade
only STRONG setups with at least 2.5:1 after costs, at most four a day,
and give winners room before anything is locked in.
"""
from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pytest

from mintel.broker.base import Bar, Position, Side
from mintel.broker.sim import SimBroker
from mintel.config import Config, FlowLockConfig
from mintel.engine.flowlock import FlowLock, FlowState
from mintel.engine.journal import Journal
from mintel.engine.scanner import Scanner
from mintel.engine.trader import Trader

NOW = dt.datetime(2026, 9, 15, 9, 0, tzinfo=dt.timezone.utc)
SYMS = ("EURUSD", "GBPUSD", "USDJPY")


class TestDefaultsAreSelective:
    def test_entry_rules(self):
        sc = Config().scan
        assert sc.entry_tier == "NORMAL"
        assert sc.min_reward_risk == 1.8
        assert sc.tier_normal >= 60.0
        assert sc.max_cost_fraction_of_stop == 0.20
        assert sc.max_new_positions_per_day == 0       # no count limits
        assert sc.session_caps_utc == ()
        assert sc.max_new_positions_per_hour == 0
        assert sc.min_seconds_between_entries == 60.0  # anti-burst only
        assert sc.max_losses_per_symbol_per_day == 2

    def test_winners_get_room(self):
        fl = Config().flowlock
        assert fl.breakeven_at_r >= 1.5
        assert fl.partial_at_r >= 2.5
        assert fl.partial_fraction <= 0.3
        assert fl.reversal_thesis_floor <= 15.0
        assert fl.structure_break_max_r <= 0.3


class TestOnlyStrongSetupsAreTraded:
    def _scan(self, cfg):
        broker = SimBroker(SYMS, start=NOW - dt.timedelta(days=5))
        broker.connect()
        return Scanner(broker, cfg).scan(broker.now, [])

    def test_normal_tier_is_watched_not_traded(self):
        cfg = Config()
        cfg.scan.entry_tier = "STRONG"
        cfg.scan.min_reward_risk = 0.0
        cfg.scan.max_cost_fraction_of_stop = 0.0
        cfg.scan.tier_normal = 1.0            # everything is at least NORMAL
        cfg.scan.tier_strong = 99.0           # nothing reaches STRONG
        states = self._scan(cfg)
        normal = [s for s in states if s.tier == "NORMAL"]
        assert normal, "the synthetic market should produce NORMAL setups"
        for st in normal:
            assert not st.tradable
            assert any("only STRONG or better" in b for b in st.blockers), st.blockers

    def test_strong_tier_is_not_blocked_by_the_tier_rule(self):
        cfg = Config()
        cfg.scan.entry_tier = "STRONG"
        cfg.scan.min_reward_risk = 0.0
        cfg.scan.max_cost_fraction_of_stop = 0.0
        cfg.scan.tier_normal = 1.0
        cfg.scan.tier_strong = 2.0            # make STRONG easy to reach
        states = self._scan(cfg)
        strong = [s for s in states if s.tier in ("STRONG", "EXCEPTIONAL")]
        assert strong
        for st in strong:
            assert not any("only STRONG" in b for b in st.blockers)

    def test_entry_tier_normal_restores_day_one_behaviour(self):
        cfg = Config()
        cfg.scan.entry_tier = "NORMAL"
        for st in self._scan(cfg):
            assert not any("only STRONG" in b for b in st.blockers)


class TestFourTradesADay:
    def _ns(self, journal=None, **over):
        cfg = Config()
        for k, v in over.items():
            setattr(cfg.scan, k, v)
        cfg.scan.session_caps_utc = ()
        return SimpleNamespace(cfg=cfg, last_entry_utc=None, _entry_times=[],
                               _entries_today=[], journal=journal)

    def test_fifth_entry_is_refused_until_tomorrow(self):
        ns = self._ns(min_seconds_between_entries=0.0, max_new_positions_per_hour=0,
                      max_new_positions_per_day=4)
        t = NOW
        for _ in range(4):
            assert Trader._entry_gate(ns, t) == ""
            Trader._note_entry(ns, t)
            t += dt.timedelta(minutes=20)
        why = Trader._entry_gate(ns, t)
        assert "4 trades taken today" in why and "done for the day" in why
        tomorrow = NOW.replace(hour=1) + dt.timedelta(days=1)
        assert Trader._entry_gate(ns, tomorrow) == ""

    def test_count_survives_a_restart_via_the_journal(self, workdir):
        j = Journal(workdir / "j.sqlite")
        try:
            for i in range(4):
                j._exec("INSERT INTO trades (ticket, symbol, opened_utc) VALUES (?, ?, ?)",
                        (100 + i, "EURUSD", (NOW - dt.timedelta(hours=2)).isoformat()))
            assert j.opened_since(NOW.replace(hour=0)) == 4
            ns = self._ns(journal=j, min_seconds_between_entries=0.0,
                          max_new_positions_per_hour=0, max_new_positions_per_day=4)
            assert "done for the day" in Trader._entry_gate(ns, NOW)
        finally:
            j.close()

    def test_zero_means_no_daily_cap(self):
        ns = self._ns(min_seconds_between_entries=0.0, max_new_positions_per_hour=0,
                      max_new_positions_per_day=0)
        for i in range(10):
            assert Trader._entry_gate(ns, NOW) == ""
            Trader._note_entry(ns, NOW)


def _pos(ticket=1, entry=1.1000, sl=1.0950):
    return Position(ticket, "EURUSD", Side.BUY, 1.0, entry, sl, 0.0, NOW)


def _mom(direction=1):
    return SimpleNamespace(direction=direction, efficiency=0.5, adx=30.0,
                           acceleration_atr=0.0)


class TestWinnersAreHeld:
    def _drive(self, fl, p, prices, structure=None):
        out = []
        t = NOW
        for px in prices:
            bar = Bar(t, px, px + 0.0002, px - 0.0002, px, 100.0)
            d = fl.update(p, price=px, atr=0.0020, bar=bar, momentum=_mom(),
                          structure=structure, now=t)
            if d.new_stop:
                p.sl = d.new_stop
            out.append((px, d))
            t += dt.timedelta(minutes=15)
        return out

    def test_nothing_is_banked_before_two_and_a_half_r(self):
        fl = FlowLock(Config().flowlock)
        # up to +2.4R - a day-one bot would have banked at 1.4R
        out = self._drive(fl, _pos(), [1.1000 + 0.00025 * i for i in range(49)])
        assert not any(d.partial_volume > 0 for _px, d in out)

    def test_partial_fires_at_two_and_a_half_r_and_leaves_most_running(self):
        fl = FlowLock(Config().flowlock)
        out = self._drive(fl, _pos(), [1.1000 + 0.00025 * i for i in range(70)])
        partials = [(d.r_now, d.partial_volume) for _px, d in out if d.partial_volume > 0]
        assert len(partials) == 1
        r, vol = partials[0]
        assert r >= 2.5
        assert vol == pytest.approx(0.3)

    def test_breakeven_lock_waits_for_one_and_a_half_r(self):
        fl = FlowLock(Config().flowlock)
        p = _pos()
        self._drive(fl, p, [1.1000 + 0.00025 * i for i in range(25)])  # to +1.2R
        assert fl.trackers[1].breakeven_done is False
        self._drive(fl, p, [1.1060 + 0.00025 * i for i in range(10)])  # on to +1.6R
        assert fl.trackers[1].breakeven_done is True

    def test_a_late_structure_break_does_not_close_a_winner(self):
        fl = FlowLock(Config().flowlock)
        p = _pos()
        broke = SimpleNamespace(bos_down=True, bos_up=False, bos_bars_ago=1,
                                last_swing_low=None, last_swing_high=None)
        calm = SimpleNamespace(bos_down=False, bos_up=False, bos_bars_ago=None,
                               last_swing_low=None, last_swing_high=None)
        self._drive(fl, p, [1.1000 + 0.00025 * i for i in range(10)], calm)  # +0.45R
        d = fl.update(p, price=1.1023, atr=0.0020, momentum=_mom(),
                      structure=broke, now=NOW + dt.timedelta(hours=3))
        assert not d.close, "at +0.46R a structure break is the stop's business"

    def test_an_early_structure_break_still_closes(self):
        fl = FlowLock(Config().flowlock)
        p = _pos()
        broke = SimpleNamespace(bos_down=True, bos_up=False, bos_bars_ago=1,
                                last_swing_low=None, last_swing_high=None)
        d = fl.update(p, price=1.1005, atr=0.0020, momentum=_mom(),
                      structure=broke, now=NOW)
        assert d.close and d.state is FlowState.REVERSAL


class TestMeasurementFollowsTheStrategy:
    def test_first_start_on_a_new_strategy_resets_the_clock(self, workdir):
        from mintel.config import STRATEGY_VERSION
        from mintel.run import reset_measurement_for_new_strategy
        path = workdir / "config.json"
        cfg = Config()
        cfg.ops.data_dir = str(workdir)
        cfg.tracking_start_utc = "2026-09-14T23:39:00+00:00"
        cfg.tracking_strategy = "2026-09-14 day one rules"
        assert reset_measurement_for_new_strategy(cfg, path, NOW) is True
        assert cfg.tracking_start_utc == NOW.isoformat()
        assert cfg.tracking_strategy == STRATEGY_VERSION
        saved = Config.load(path)
        assert saved.tracking_start_utc == NOW.isoformat()
        assert saved.tracking_strategy == STRATEGY_VERSION

    def test_restarts_on_the_same_strategy_keep_the_clock(self, workdir):
        from mintel.config import STRATEGY_VERSION
        from mintel.run import reset_measurement_for_new_strategy
        cfg = Config()
        cfg.tracking_start_utc = "2026-09-15T09:20:00+00:00"
        cfg.tracking_strategy = STRATEGY_VERSION
        assert reset_measurement_for_new_strategy(cfg, workdir / "c.json", NOW + dt.timedelta(hours=5)) is False
        assert cfg.tracking_start_utc == "2026-09-15T09:20:00+00:00"

    def test_reset_never_freezes_the_rules_into_the_file(self, workdir):
        """Day two: a saved copy of the old rules overrode the new build's."""
        import json
        from mintel.config import STRATEGY_VERSION
        from mintel.run import reset_measurement_for_new_strategy
        path = workdir / "config.json"
        stale = Config()
        stale.tracking_strategy = "old"
        stale.scan.entry_tier = "STRONG"
        stale.scan.min_reward_risk = 2.5
        stale.save(path)                       # the old behaviour: everything
        cfg = Config.load(path)
        assert cfg.scan.entry_tier == "STRONG"  # the file's stale copy wins
        assert reset_measurement_for_new_strategy(cfg, path, NOW) is True
        raw = json.loads(path.read_text())
        assert "scan" not in raw and "flowlock" not in raw
        assert raw["tracking_strategy"] == STRATEGY_VERSION
        assert raw["risk"]["max_open_positions"] == 8     # user's settings kept
        assert cfg.scan.entry_tier == Config().scan.entry_tier
        assert cfg.scan.min_reward_risk == Config().scan.min_reward_risk
        assert Config.load(path).scan.entry_tier == Config().scan.entry_tier

    def test_daily_cap_counts_only_this_strategys_trades(self, workdir):
        from mintel.engine.journal import Journal
        from mintel.engine.trader import Trader
        j = Journal(workdir / "j2.sqlite")
        try:
            for i in range(8):        # eight trades this morning, old rules
                j._exec("INSERT INTO trades (ticket, symbol, opened_utc) VALUES (?, ?, ?)",
                        (200 + i, "EURUSD", (NOW - dt.timedelta(hours=3)).isoformat()))
            cfg = Config()
            cfg.scan.min_seconds_between_entries = 0.0
            cfg.scan.max_new_positions_per_hour = 0
            cfg.scan.max_new_positions_per_day = 8
            cfg.tracking_start_utc = (NOW - dt.timedelta(minutes=30)).isoformat()
            ns = SimpleNamespace(cfg=cfg, last_entry_utc=None, _entry_times=[],
                                 _entries_today=[], journal=j)
            assert Trader._entry_gate(ns, NOW) == ""
            cfg.tracking_start_utc = ""
            assert "done for the day" in Trader._entry_gate(ns, NOW)
        finally:
            j.close()

    def test_frozen_rules_are_stripped_even_when_the_clock_is_current(self, workdir):
        import json
        from mintel.config import STRATEGY_VERSION
        from mintel.run import reset_measurement_for_new_strategy
        path = workdir / "config2.json"
        stale = Config()
        stale.tracking_strategy = STRATEGY_VERSION          # clock already current
        stale.tracking_start_utc = "2026-09-15T12:59:00+00:00"
        stale.scan.entry_tier = "STRONG"
        stale.save(path)
        cfg = Config.load(path)
        assert reset_measurement_for_new_strategy(cfg, path, NOW) is False
        assert cfg.tracking_start_utc == "2026-09-15T12:59:00+00:00"   # kept
        assert cfg.scan.entry_tier == Config().scan.entry_tier          # code's rules
        raw = json.loads(path.read_text())
        assert "scan" not in raw and raw["tracking_start_utc"] == "2026-09-15T12:59:00+00:00"


class TestThePageSaysWhyItIsNotTrading:
    def test_overall_reasons_and_rules_are_shown(self):
        from mintel.ops.dashboard import render_status
        snap = {"status": {"not_trading_because": ["8 trades taken today (limit 8) - done for the day"],
                           "strategy_rules": {"entry_tier": "NORMAL", "min_reward_risk": 1.8,
                                              "max_cost_fraction_of_stop": 0.2,
                                              "max_new_positions_per_hour": 2,
                                              "max_new_positions_per_day": 8}},
                "health": {}, "thinking": [], "results": {}, "positions": [], "events": []}
        page = render_status(snap)
        assert "Why no new trade right now" in page
        assert "done for the day" in page
        assert "NORMAL setups or better, at least 1.8x the risk" in page
        assert "2 an hour and 8 a day" in page
        snap["status"]["strategy_rules"]["sessions"] = ["Asia 2", "London 3"]
        assert "per session: Asia 2, London 3" in render_status(snap)
        snap["status"]["strategy_rules"]["sessions"] = []
        snap["status"]["strategy_rules"]["max_new_positions_per_hour"] = 0
        snap["status"]["strategy_rules"]["max_new_positions_per_day"] = 0
        assert "no limit on the number of trades" in render_status(snap)

    def test_no_overall_block_says_so(self):
        from mintel.ops.dashboard import render_status
        snap = {"status": {"not_trading_because": []}, "health": {}, "thinking": [],
                "results": {}, "positions": [], "events": []}
        assert "Nothing is holding entries back overall" in render_status(snap)


class TestDailyLossStopMeasuresThisStrategysDay:
    def test_old_rules_losses_this_morning_do_not_count(self, workdir):
        from mintel.broker.sim import SimBroker
        from mintel.engine.trader import Trader
        sim = SimBroker(SYMS, start=NOW - dt.timedelta(days=2))
        sim.connect()
        cfg = Config()
        cfg.ops.data_dir = str(workdir)
        cfg.account_login, cfg.account_server = sim.login, sim.server
        t = Trader(sim, cfg, clock=lambda: NOW, enable_model=False)
        sim.closed.append({"ticket": 1, "symbol": "EURUSD", "volume": 0.1,
                           "pnl": -40.0, "time": NOW - dt.timedelta(hours=3),
                           "price": 1.1, "reason": "stop"})
        cfg.tracking_start_utc = ""
        assert t.realised_today() == pytest.approx(-40.0)
        t._realised_cache = {"at": None, "value": 0.0}
        cfg.tracking_start_utc = (NOW - dt.timedelta(minutes=30)).isoformat()
        assert t.realised_today() == pytest.approx(0.0)


class TestAllowancePerSession:
    """Session caps are optional (off by default) but must work when set."""
    CAPS = (("Asia", "00:00-07:00", 2), ("London", "07:00-13:00", 3),
            ("New York", "13:00-21:00", 3), ("late evening", "21:00-00:00", 1))

    def _ns(self, **over):
        cfg = Config()
        cfg.scan.min_seconds_between_entries = 0.0
        cfg.scan.max_new_positions_per_hour = 0
        cfg.scan.session_caps_utc = self.CAPS
        for k, v in over.items():
            setattr(cfg.scan, k, v)
        return SimpleNamespace(cfg=cfg, last_entry_utc=None, _entry_times=[],
                               _entries_today=[], journal=None)

    def test_defaults_have_no_count_limit_at_all(self):
        ns = self._ns(session_caps_utc=(), max_new_positions_per_day=0)
        t = NOW.replace(hour=3)
        for i in range(20):
            assert Trader._entry_gate(ns, t) == "", i
            Trader._note_entry(ns, t)
            t += dt.timedelta(minutes=2)

    def test_asia_spent_does_not_block_london(self):
        ns = self._ns()
        t = NOW.replace(hour=1)
        for _ in range(2):                          # Asia allows 2
            assert Trader._entry_gate(ns, t) == ""
            Trader._note_entry(ns, t)
            t += dt.timedelta(minutes=30)
        assert "Asia session" in Trader._entry_gate(ns, t)
        london = NOW.replace(hour=8)
        for _ in range(3):                          # London allows 3 more
            assert Trader._entry_gate(ns, london) == ""
            Trader._note_entry(ns, london)
        assert "London session" in Trader._entry_gate(ns, london)
        assert Trader._entry_gate(ns, NOW.replace(hour=14)) == ""     # New York

    def test_eight_overnight_trades_leave_london_open(self, workdir):
        from mintel.engine.journal import Journal
        j = Journal(workdir / "s.sqlite")
        try:
            for i in range(8):
                j._exec("INSERT INTO trades (ticket, symbol, opened_utc) VALUES (?, ?, ?)",
                        (300 + i, "USDJPY", NOW.replace(hour=3).isoformat()))
            ns = self._ns()
            ns.journal = j
            assert Trader._entry_gate(ns, NOW.replace(hour=9, minute=17)) == ""
            assert "Asia session" in Trader._entry_gate(ns, NOW.replace(hour=5))
        finally:
            j.close()

    def test_late_evening_window_crosses_midnight(self):
        ns = self._ns()
        t = NOW.replace(hour=21, minute=10)
        assert Trader._entry_gate(ns, t) == ""
        Trader._note_entry(ns, t)
        assert "late evening session" in Trader._entry_gate(ns, t.replace(hour=23, minute=40))


class TestDayThreeLessons:
    def test_counter_trend_reversals_do_not_run_in_a_trend(self):
        from mintel.data.regime import Regime
        from mintel.engine.tactics import candidates_for
        names = {t.name for t in candidates_for(Regime.TREND)}
        assert "LIQUIDITY_SWEEP_REVERSAL" not in names
        assert "FAILED_BREAKOUT_RECLAIM" not in names
        assert "MOMENTUM_CONTINUATION" in names
        assert "LIQUIDITY_SWEEP_REVERSAL" in {t.name for t in candidates_for(Regime.RANGE)}

    def test_stop_floor_is_one_atr(self):
        assert Config().scan.stop_noise_floor_atr == 1.0

    def test_nothing_is_trailed_before_one_r(self):
        fl = FlowLock(Config().flowlock)
        p = _pos()                                   # risk 0.0050
        # climb to +0.8R (PROVING) and wobble: the stop must not move
        prices = [1.1000 + 0.0002 * i for i in range(20)] + [1.1040, 1.1030, 1.1035]
        stops = []
        t = NOW
        for px in prices:
            d = fl.update(p, price=px, atr=0.0020, bar=Bar(t, px, px + 0.0001, px - 0.0001, px, 100.0),
                          momentum=_mom(), now=t)
            if d.new_stop:
                stops.append(d.new_stop)
            t += dt.timedelta(minutes=15)
        assert fl.trackers[1].mfe_r >= 0.75 and fl.trackers[1].mfe_r < 1.0
        assert stops == [], stops

    def test_after_one_r_the_giveback_is_forty_percent(self):
        assert Config().flowlock.mfe_giveback_normal == 0.40
        assert Config().flowlock.proving_trail is False

    def test_day_review_breaks_down_exit_reasons(self, workdir):
        from mintel.ops.day_review import review
        positions = [{"position": 1, "symbol": "US30", "net": -3.5, "gross": -3.5, "commission": 0.0,
                      "opened": NOW, "closed": NOW + dt.timedelta(minutes=3), "volume": 0.2, "minutes": 3},
                     {"position": 2, "symbol": "XAUEUR", "net": 7.5, "gross": 7.6, "commission": -0.1,
                      "opened": NOW, "closed": NOW + dt.timedelta(minutes=40), "volume": 0.01, "minutes": 40}]
        journal_rows = [{"ticket": 1, "tactic": "MOMENTUM_CONTINUATION", "regime": "TREND",
                         "exit_reason": "broker stop or target"},
                        {"ticket": 2, "tactic": "BREAKOUT_RETEST", "regime": "TREND",
                         "exit_reason": "thesis broke: the reasons for the trade have gone"}]
        text = review(positions, journal_rows, "GBP")
        assert "By exit reason" in text and "thesis broke" in text and "broker stop or target" in text


class TestDayThreeEveningReview:
    def test_news_continuation_is_off_by_default(self):
        from mintel.data.regime import Regime
        from mintel.engine.tactics import best_signal
        from mintel.broker.sim import SimBroker
        cfg = Config()
        assert "NEWS_CONTINUATION" in cfg.scan.disabled_tactics
        assert "TREND_PULLBACK" in cfg.scan.disabled_tactics
        assert cfg.scan.tactic_min_score["MOMENTUM_CONTINUATION"] >= 70
        # a disabled tactic never becomes the signal
        broker = SimBroker(SYMS, start=NOW - dt.timedelta(days=5)); broker.connect()
        sc = Scanner(broker, cfg)
        for st in sc.scan(broker.now, []):
            assert st.tactic != "NEWS_CONTINUATION"

    def test_a_tactic_below_its_score_floor_is_blocked(self):
        cfg = Config()
        broker = SimBroker(SYMS, start=NOW - dt.timedelta(days=5)); broker.connect()
        first = Scanner(broker, cfg).scan(broker.now, [])
        assert first
        tactic = first[0].tactic
        cfg.scan.tactic_min_score = {tactic: 99.0}
        states = Scanner(broker, cfg).scan(broker.now, [])
        hit = [s for s in states if s.tactic == tactic]
        assert hit
        for st in hit:
            assert any("needs a score of 99+" in b for b in st.blockers), st.blockers
        cfg.scan.tactic_min_score = {}
        for st in Scanner(broker, cfg).scan(broker.now, []):
            assert not any("needs a score" in b for b in st.blockers)

    def test_exit_kinds_are_readable(self):
        from mintel.ops.day_review import exit_kind
        assert exit_kind("[sl 1.15237]", -12.72) == "stop-loss hit (original or trailed)"
        assert exit_kind("[sl 0.82110]", 8.57) == "trailed stop hit while in profit"
        assert exit_kind("[tp 3733.98]", 7.47) == "target reached"
        assert exit_kind("thesis broke: the reasons for the trade have gone", -1) == "closed by the bot: thesis broke"
        assert exit_kind("", 2.0) == "broker stop or target"


class TestDayFourExitBugs:
    """Winners were clipped at ~0.6R by a stall counter that ticked per second,
    and a pre-entry bar's high was credited as the trade's best price."""

    def test_stall_counts_new_bars_not_management_cycles(self):
        fl = FlowLock(Config().flowlock)
        p = _pos()
        t0 = NOW
        bar = Bar(t0, 1.1010, 1.1012, 1.1008, 1.1010, 50.0)
        fl.update(p, price=1.1010, atr=0.0020, bar=bar, momentum=_mom(), now=t0)
        # forty management cycles inside the SAME bar: no stall progress
        for i in range(40):
            fl.update(p, price=1.1009, atr=0.0020, bar=bar, momentum=_mom(),
                      now=t0 + dt.timedelta(seconds=i + 1))
        assert fl.trackers[1].bars_since_extension == 0
        assert fl.trackers[1].state is not FlowState.DECAY
        # new bars without a new high do advance it, one per bar
        for k in range(1, 4):
            b = Bar(t0 + dt.timedelta(minutes=k), 1.1009, 1.1011, 1.1007, 1.1009, 50.0)
            fl.update(p, price=1.1009, atr=0.0020, bar=b, momentum=_mom(),
                      now=t0 + dt.timedelta(minutes=k))
        assert fl.trackers[1].bars_since_extension == 3

    def test_a_bar_that_opened_before_the_trade_does_not_set_the_best(self):
        fl = FlowLock(Config().flowlock)
        p = _pos()                                    # opened at NOW, risk 0.0050
        stale = Bar(NOW - dt.timedelta(minutes=5), 1.0990, 1.1300, 1.0980, 1.1005, 50.0)  # +6R wick, pre-entry
        d = fl.update(p, price=1.1005, atr=0.0020, bar=stale, momentum=_mom(), now=NOW)
        assert fl.trackers[1].mfe_r == pytest.approx(0.1, abs=0.01)
        assert d.new_stop is None
        fresh = Bar(NOW + dt.timedelta(minutes=1), 1.1005, 1.1040, 1.1004, 1.1030, 50.0)
        fl.update(p, price=1.1030, atr=0.0020, bar=fresh, momentum=_mom(),
                  now=NOW + dt.timedelta(minutes=1))
        assert fl.trackers[1].mfe_r == pytest.approx(0.8, abs=0.01)


class TestDayFiveFloor:
    def test_a_strong_flow_winner_cannot_round_trip_to_zero(self):
        cfg = Config().flowlock
        cfg.profit_floor_giveback = 0.65             # available, off by default
        fl = FlowLock(cfg)
        p = _pos()                                   # risk 0.0050
        t = NOW
        strong = SimpleNamespace(direction=1, efficiency=0.6, adx=35.0, acceleration_atr=0.1)
        # run to +2.3R with strong momentum -> STRONG_FLOW
        for i in range(1, 24):
            px = 1.1000 + 0.0005 * i
            fl.update(p, price=px, atr=0.0020, bar=Bar(t, px, px + 0.0001, px - 0.0001, px, 50.0),
                      momentum=strong, now=t)
            if fl.trackers[1].stop > p.sl:
                p.sl = fl.trackers[1].stop
            t += dt.timedelta(minutes=1)
        tr = fl.trackers[1]
        assert tr.mfe_r >= 2.2
        # the stop must already lock at least 35% of the best gain
        assert tr.stop >= 1.1000 + 0.35 * tr.mfe - 1e-9, (tr.stop, tr.mfe, tr.state)

    def test_cost_gate_is_twelve_percent(self):
        from mintel.engine.scanner import Scanner
        cfg = Config()
        broker = SimBroker(SYMS, start=NOW - dt.timedelta(days=5)); broker.connect()
        sc = Scanner(broker, cfg)
        sc.commission_per_lot = {"*": 60.0}          # make FX expensive
        states = sc.scan(broker.now, [])
        assert any("limit 20%" in b for st in states for b in st.blockers)


class TestMomentumContinuationOffInTrends:
    def test_default_and_effect(self):
        from mintel.data.regime import Regime
        from mintel.engine.tactics import best_signal
        cfg = Config()
        assert ("MOMENTUM_CONTINUATION", "TREND") in cfg.scan.disabled_tactic_regimes
        # 24 Sep standing rule: negative on three of five days, net -13 GBP.
        assert ("BREAKOUT_RETEST", "HIGH_VOL") in cfg.scan.disabled_tactic_regimes
        broker = SimBroker(SYMS, start=NOW - dt.timedelta(days=5)); broker.connect()
        for st in Scanner(broker, cfg).scan(broker.now, []):
            assert not (st.tactic == "MOMENTUM_CONTINUATION" and st.regime is Regime.TREND), st
        # and the same approach is still available outside trends
        cfg2 = Config(); cfg2.scan.disabled_tactic_regimes = (("MOMENTUM_CONTINUATION", "RANGE"),)
        names = {(st.tactic, st.regime) for st in Scanner(broker, cfg2).scan(broker.now, [])}
        assert all(not (t == "MOMENTUM_CONTINUATION" and r is Regime.RANGE) for t, r in names)
