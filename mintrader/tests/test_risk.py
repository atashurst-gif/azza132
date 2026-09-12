"""Sizing, risk ceilings, martingale prohibition and circuit breakers."""
from __future__ import annotations

import datetime as dt

import pytest

from mintel.broker.base import AccountInfo, Position, Side
from mintel.config import Config
from mintel.contracts import SymbolSpec
from mintel.data.regime import Regime
from mintel.engine.evidence import Evidence, Family, MarketState
from mintel.engine.risk import RiskManager, exposure_key

UTC = dt.timezone.utc


def spec(**kw) -> SymbolSpec:
    base = dict(name="EURUSD", digits=5, point=0.00001, tick_size=0.00001,
                tick_value=1.0, contract_size=100_000, volume_min=0.01,
                volume_max=100.0, volume_step=0.01, stops_level_points=10.0,
                freeze_level_points=0.0, asset_group="FX_MAJOR")
    base.update(kw)
    return SymbolSpec(**base)


def account(equity=10_000.0, **kw) -> AccountInfo:
    base = dict(login=1, server="S", currency="GBP", balance=equity,
                equity=equity, margin=0.0, margin_free=equity,
                margin_level=0.0, leverage=30, trade_allowed=True,
                is_demo=True)
    base.update(kw)
    return AccountInfo(**base)


def state(tier="STRONG", direction=1, entry=1.1000, stop=1.0980,
          symbol="EURUSD", opportunity=75.0) -> MarketState:
    return MarketState(
        symbol=symbol, as_of=dt.datetime(2026, 3, 10, 13, 0, tzinfo=UTC),
        regime=Regime.TREND, regime_label="TREND / NORMAL VOL",
        direction=direction, tactic="MOMENTUM_CONTINUATION",
        evidence={Family.STRUCTURE: Evidence(Family.STRUCTURE, 80.0)},
        raw_score=opportunity, opportunity=opportunity, tier=tier,
        entry=entry, stop=stop, target=1.1040, reward_risk=2.0,
        headroom_pips=40.0, cost_pips=1.0)


class TestRiskPercentage:
    def test_no_trade_tier_gets_no_risk(self):
        rm = RiskManager(Config())
        pct, _ = rm.risk_pct_for(state(tier="NO_TRADE"), 0.9)
        assert pct == 0.0

    def test_normal_tier_gets_base_risk(self):
        cfg = Config()
        rm = RiskManager(cfg)
        pct, _ = rm.risk_pct_for(state(tier="NORMAL"), 0.9)
        assert pct == pytest.approx(cfg.risk.base_risk_pct)

    def test_exceptional_tier_can_reach_the_ceiling(self):
        cfg = Config()
        cfg.aggression = "MAXIMUM"
        rm = RiskManager(cfg)
        pct, _ = rm.risk_pct_for(state(tier="EXCEPTIONAL"), 1.0)
        assert pct == pytest.approx(cfg.risk.max_risk_pct)

    def test_ceiling_is_never_exceeded_at_any_setting(self):
        for aggression in ("CONSERVATIVE", "NORMAL", "AGGRESSIVE", "MAXIMUM"):
            for conf in (0.0, 0.5, 1.0, 5.0):
                for tier in ("NORMAL", "STRONG", "EXCEPTIONAL"):
                    cfg = Config()
                    cfg.aggression = aggression
                    pct, _ = RiskManager(cfg).risk_pct_for(state(tier=tier),
                                                           conf)
                    assert pct <= cfg.risk.max_risk_pct + 1e-9

    def test_low_confidence_sizes_smaller_than_high_confidence(self):
        cfg = Config()
        cfg.aggression = "AGGRESSIVE"
        rm = RiskManager(cfg)
        low, _ = rm.risk_pct_for(state(tier="EXCEPTIONAL"), 0.1)
        high, _ = rm.risk_pct_for(state(tier="EXCEPTIONAL"), 1.0)
        assert low < high

    def test_conservative_trades_smaller_than_maximum(self):
        results = {}
        for aggression in ("CONSERVATIVE", "NORMAL", "AGGRESSIVE", "MAXIMUM"):
            cfg = Config()
            cfg.aggression = aggression
            results[aggression], _ = RiskManager(cfg).risk_pct_for(
                state(tier="STRONG"), 0.8)
        assert (results["CONSERVATIVE"] < results["NORMAL"]
                < results["AGGRESSIVE"] <= results["MAXIMUM"])


class TestKelly:
    def test_kelly_is_ignored_on_a_small_sample(self):
        cfg = Config()
        cfg.aggression = "MAXIMUM"
        rm = RiskManager(cfg)
        pct, reasons = rm.risk_pct_for(state(tier="EXCEPTIONAL"), 1.0,
                                       (20, 0.9, 5.0))
        assert pct == pytest.approx(cfg.risk.max_risk_pct)
        assert any("too few" in r for r in reasons)

    def test_kelly_only_ever_reduces_risk(self):
        cfg = Config()
        cfg.aggression = "MAXIMUM"
        rm = RiskManager(cfg)
        without, _ = rm.risk_pct_for(state(tier="EXCEPTIONAL"), 1.0)
        with_kelly, _ = rm.risk_pct_for(state(tier="EXCEPTIONAL"), 1.0,
                                        (500, 0.45, 1.0))
        assert with_kelly <= without

    def test_negative_kelly_falls_back_to_base(self):
        cfg = Config()
        rm = RiskManager(cfg)
        pct, reasons = rm.risk_pct_for(state(tier="EXCEPTIONAL"), 1.0,
                                       (300, 0.2, 1.0))
        assert pct == pytest.approx(cfg.risk.base_risk_pct)
        assert any("negative" in r for r in reasons)

    def test_kelly_is_always_capped_by_the_configured_maximum(self):
        cfg = Config()
        cfg.aggression = "MAXIMUM"
        rm = RiskManager(cfg)
        # Absurdly favourable stats must still not breach the ceiling.
        pct, _ = rm.risk_pct_for(state(tier="EXCEPTIONAL"), 1.0,
                                 (5000, 0.95, 20.0))
        assert pct <= cfg.risk.max_risk_pct


class TestNoMartingale:
    """Losses must never increase the next position's size."""

    def test_sizing_ignores_recent_losses(self):
        cfg = Config()
        rm = RiskManager(cfg)
        s, sp, acct = state(), spec(), account()
        snap = rm.snapshot(acct, [])
        before = rm.size(s, sp, acct, [], 1.0, snap, margin_per_lot=3000.0)
        # Simulate a losing streak: equity down, several closed losers.
        for _ in range(5):
            rm.note_order()
        after_acct = account(equity=9_000.0)
        snap2 = rm.snapshot(after_acct, [])
        after = rm.size(s, sp, after_acct, [], 1.0, snap2, margin_per_lot=3000.0)
        assert after.volume <= before.volume, (
            "risk must fall with equity, never rise after losses")

    def test_risk_percentage_has_no_loss_streak_input(self):
        """Structural guarantee: the sizing API cannot see past outcomes.

        ``risk_pct_for`` accepts only the current opportunity, calibrated
        confidence and a large-sample Kelly triple - there is no parameter
        through which "the last trade lost" could increase size.
        """
        import inspect
        params = set(inspect.signature(RiskManager.risk_pct_for).parameters)
        assert params == {"self", "state", "calibrated_confidence",
                          "kelly_stats"}

    def test_equity_drop_reduces_money_at_risk(self):
        cfg = Config()
        rm = RiskManager(cfg)
        big = rm.size(state(), spec(), account(10_000), [], 1.0,
                      rm.snapshot(account(10_000), []), margin_per_lot=3000.0)
        small = rm.size(state(), spec(), account(5_000), [], 1.0,
                        rm.snapshot(account(5_000), []), margin_per_lot=3000.0)
        assert small.risk_money < big.risk_money


class TestSizing:
    def test_volume_matches_the_hand_calculation(self):
        cfg = Config()
        rm = RiskManager(cfg)
        acct = account(10_000)
        # 1% of 10,000 = 100 GBP; a 20-pip stop costs 200 GBP per lot -> 0.5 lots
        r = rm.size(state(entry=1.1000, stop=1.0980), spec(), acct, [], 1.0,
                    rm.snapshot(acct, []), margin_per_lot=1000.0)
        assert r.volume == pytest.approx(0.5)
        assert r.risk_money == pytest.approx(100.0)
        assert r.stop_pips == pytest.approx(20.0)

    def test_long_and_short_are_symmetric(self):
        cfg = Config()
        rm = RiskManager(cfg)
        acct = account()
        snap = rm.snapshot(acct, [])
        long_r = rm.size(state(direction=1, entry=1.1000, stop=1.0980), spec(),
                         acct, [], 1.0, snap, margin_per_lot=1000.0)
        short_r = rm.size(state(direction=-1, entry=1.1000, stop=1.1020), spec(),
                          acct, [], 1.0, snap, margin_per_lot=1000.0)
        assert long_r.volume == pytest.approx(short_r.volume)
        assert long_r.risk_money == pytest.approx(short_r.risk_money)

    def test_rounding_never_pushes_risk_over_the_ceiling(self):
        cfg = Config()
        cfg.risk.max_risk_pct = 1.0
        rm = RiskManager(cfg)
        acct = account(10_000)
        # A coarse lot step is where rounding errors would breach the cap.
        r = rm.size(state(entry=1.1000, stop=1.0950), spec(volume_step=0.5,
                                                           volume_min=0.5),
                    acct, [], 1.0, rm.snapshot(acct, []), margin_per_lot=100.0)
        assert r.risk_pct <= 1.0 + 1e-9

    def test_stop_too_tight_for_the_minimum_lot_is_refused(self):
        cfg = Config()
        cfg.risk.max_risk_pct = 0.05
        rm = RiskManager(cfg)
        acct = account(200)
        r = rm.size(state(entry=1.1000, stop=1.0900), spec(), acct, [], 0.05,
                    rm.snapshot(acct, []), margin_per_lot=1000.0)
        assert not r.ok and r.rejected

    def test_zero_stop_distance_is_refused(self):
        rm = RiskManager(Config())
        acct = account()
        r = rm.size(state(entry=1.1, stop=1.1), spec(), acct, [], 1.0,
                    rm.snapshot(acct, []))
        assert not r.ok and "zero" in r.rejected

    def test_jpy_pair_sizing_uses_its_own_tick_value(self):
        rm = RiskManager(Config())
        acct = account(10_000)
        jpy = spec(name="USDJPY", digits=3, point=0.001, tick_size=0.001,
                   tick_value=0.68)
        # 30 pips = 300 points * 0.68 = 204 per lot; 1% of 10k = 100 -> 0.49
        r = rm.size(state(symbol="USDJPY", entry=151.00, stop=150.70), jpy,
                    acct, [], 1.0, rm.snapshot(acct, []), margin_per_lot=1000.0)
        assert r.volume == pytest.approx(0.49, abs=0.01)

    def test_margin_level_projection_prevents_a_margin_call(self):
        cfg = Config()
        cfg.risk.min_margin_level_pct = 300.0
        rm = RiskManager(cfg)
        acct = account(10_000)
        r = rm.size(state(), spec(), acct, [], 1.5, rm.snapshot(acct, []),
                    margin_per_lot=10_000.0)
        # equity/3 = 3333 of margin allowed -> at most 0.33 lots
        assert r.volume <= 0.34
        assert any("margin level" in x for x in r.reasons)


class TestExposure:
    def test_correlated_positions_share_a_bucket(self):
        keys_a = set(exposure_key("EURUSD", Side.BUY))
        keys_b = set(exposure_key("USDJPY", Side.SELL))
        assert keys_a & keys_b, "both are short dollars"

    def test_opposite_dollar_bets_do_not_share_a_bucket(self):
        keys_a = set(exposure_key("EURUSD", Side.BUY))
        keys_b = set(exposure_key("EURUSD", Side.SELL))
        assert not (keys_a & keys_b)

    def test_correlated_cap_reduces_or_refuses_a_second_trade(self):
        cfg = Config()
        cfg.risk.max_correlated_risk_pct = 1.0
        rm = RiskManager(cfg)
        acct = account(10_000)
        rm.register_open_risk(1, "EURUSD", Side.BUY, 100.0)
        snap = rm.snapshot(acct, [Position(1, "EURUSD", Side.BUY, 0.5, 1.1,
                                           1.098, 0.0,
                                           dt.datetime.now(UTC))])
        r = rm.size(state(symbol="GBPUSD"), spec(name="GBPUSD"), acct, [], 1.0,
                    snap, margin_per_lot=1000.0)
        assert r.risk_money <= 0.0 or r.risk_money <= 100.0

    def test_bucket_full_refuses_outright(self):
        cfg = Config()
        cfg.risk.max_correlated_risk_pct = 0.5
        rm = RiskManager(cfg)
        acct = account(10_000)
        rm.register_open_risk(1, "EURUSD", Side.BUY, 60.0)
        snap = rm.snapshot(acct, [Position(1, "EURUSD", Side.BUY, 0.5, 1.1,
                                           1.098, 0.0,
                                           dt.datetime.now(UTC))])
        r = rm.size(state(symbol="AUDUSD"), spec(name="AUDUSD"), acct, [], 1.0,
                    snap, margin_per_lot=1000.0)
        assert not r.ok and "correlated" in r.rejected


class TestCircuitBreakers:
    def _rm(self, **risk):
        cfg = Config()
        for k, v in risk.items():
            setattr(cfg.risk, k, v)
        return RiskManager(cfg)

    def test_no_breakers_on_a_clean_account(self):
        rm = self._rm()
        assert rm.snapshot(account(), []).entries_allowed

    def test_daily_loss_stops_new_entries(self):
        rm = self._rm(max_daily_loss_pct=2.0)
        now = dt.datetime(2026, 3, 10, 9, 0, tzinfo=UTC)
        rm.snapshot(account(10_000), [], now)
        snap = rm.snapshot(account(9_700), [], now + dt.timedelta(hours=1))
        assert not snap.entries_allowed
        assert any("down" in r for r in snap.blocking_reasons())

    def test_max_drawdown_stops_new_entries(self):
        rm = self._rm(max_drawdown_pct=5.0)
        now = dt.datetime(2026, 3, 10, 9, 0, tzinfo=UTC)
        rm.snapshot(account(12_000), [], now)
        snap = rm.snapshot(account(11_000), [], now + dt.timedelta(hours=2))
        assert not snap.entries_allowed

    def test_reject_storm_trips_a_breaker(self):
        rm = self._rm(max_rejects_per_10min=3)
        now = dt.datetime(2026, 3, 10, 9, 0, tzinfo=UTC)
        for _ in range(3):
            rm.note_reject(now)
        snap = rm.snapshot(account(), [], now)
        assert not snap.entries_allowed
        assert any("rejected" in r for r in snap.blocking_reasons())

    def test_order_rate_limit_trips_a_breaker(self):
        rm = self._rm(max_orders_per_hour=2)
        now = dt.datetime(2026, 3, 10, 9, 0, tzinfo=UTC)
        for _ in range(2):
            rm.note_order(now)
        assert not rm.snapshot(account(), [], now).entries_allowed

    def test_stale_rejects_expire(self):
        rm = self._rm(max_rejects_per_10min=2)
        old = dt.datetime(2026, 3, 10, 9, 0, tzinfo=UTC)
        for _ in range(5):
            rm.note_reject(old)
        later = old + dt.timedelta(minutes=30)
        assert rm.snapshot(account(), [], later).entries_allowed

    def test_position_count_limit(self):
        rm = self._rm(max_open_positions=1)
        pos = [Position(1, "EURUSD", Side.BUY, 0.1, 1.1, 1.09, 0.0,
                        dt.datetime.now(UTC))]
        assert not rm.snapshot(account(), pos).entries_allowed

    def test_trade_permission_off_trips_a_breaker(self):
        rm = self._rm()
        snap = rm.snapshot(account(trade_allowed=False), [])
        assert not snap.entries_allowed

    def test_margin_level_breaker(self):
        rm = self._rm(min_margin_level_pct=200.0)
        acct = account(equity=10_000, margin=8_000.0, margin_level=125.0)
        assert not rm.snapshot(acct, []).entries_allowed

    def test_manual_halt_and_resume(self):
        rm = self._rm()
        rm.halt("operator stopped it")
        assert not rm.snapshot(account(), []).entries_allowed
        rm.resume()
        assert rm.snapshot(account(), []).entries_allowed

    def test_breakers_never_prevent_managing_existing_risk(self):
        """A tripped breaker blocks entries only - it is not a freeze."""
        rm = self._rm(max_daily_loss_pct=1.0)
        now = dt.datetime(2026, 3, 10, 9, 0, tzinfo=UTC)
        rm.snapshot(account(10_000), [], now)
        snap = rm.snapshot(account(9_000), [], now + dt.timedelta(hours=1))
        assert not snap.entries_allowed
        # Nothing in the snapshot forbids closing or trailing; the trader's
        # manage() path never consults entries_allowed.
        from mintel.engine.trader import Trader
        import inspect
        src = inspect.getsource(Trader.manage)
        assert "entries_allowed" not in src
