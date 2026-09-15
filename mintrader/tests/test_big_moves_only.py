"""Day two's lesson: few trades, each worth taking, and winners held.

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
        assert sc.entry_tier == "STRONG"
        assert sc.min_reward_risk >= 2.5
        assert sc.max_cost_fraction_of_stop <= 0.15
        assert sc.max_new_positions_per_day == 4
        assert sc.max_new_positions_per_hour == 1
        assert sc.min_seconds_between_entries >= 900
        assert sc.max_losses_per_symbol_per_day == 1

    def test_winners_get_room(self):
        fl = Config().flowlock
        assert fl.breakeven_at_r >= 1.5
        assert fl.partial_at_r >= 3.0
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

    def test_nothing_is_banked_before_three_r(self):
        fl = FlowLock(Config().flowlock)
        # 0.5R steps up to +2.9R - a day-one bot would have banked at 1.4R
        out = self._drive(fl, _pos(), [1.1000 + 0.00025 * i for i in range(59)])
        assert not any(d.partial_volume > 0 for _px, d in out)

    def test_partial_fires_at_three_r_and_leaves_most_running(self):
        fl = FlowLock(Config().flowlock)
        out = self._drive(fl, _pos(), [1.1000 + 0.00025 * i for i in range(70)])
        partials = [(d.r_now, d.partial_volume) for _px, d in out if d.partial_volume > 0]
        assert len(partials) == 1
        r, vol = partials[0]
        assert r >= 3.0
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
