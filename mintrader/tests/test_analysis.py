"""Structure, momentum, headroom, currency strength, regime, cross-market."""
from __future__ import annotations

import datetime as dt

import pytest

from mintel.broker.base import Bar, TF
from mintel.contracts import SymbolSpec
from mintel.data import crossmarket, headroom as hr, momentum as mom
from mintel.data import regime as rg, strength as stg, structure as st
from mintel.data.series import atr, adx_dmi, ema, linreg_slope, percentile_rank

UTC = dt.timezone.utc


def series(closes, *, start=None, wick=0.0, step_minutes=5, volume=300.0):
    """Build bars from a close series with optional symmetric wicks."""
    t0 = start or dt.datetime(2026, 3, 10, 8, 0, tzinfo=UTC)
    bars = []
    prev = closes[0]
    for i, c in enumerate(closes):
        o = prev
        bars.append(Bar(time=t0 + dt.timedelta(minutes=step_minutes * i),
                        open=o, high=max(o, c) + wick, low=min(o, c) - wick,
                        close=c, tick_volume=volume))
        prev = c
    return bars


def spec() -> SymbolSpec:
    return SymbolSpec(name="EURUSD", digits=5, point=0.00001,
                      tick_size=0.00001, tick_value=1.0, contract_size=100_000,
                      volume_min=0.01, volume_max=100.0, volume_step=0.01,
                      stops_level_points=10.0, freeze_level_points=0.0,
                      asset_group="FX_MAJOR")


class TestMomentumEfficiency:
    def test_clean_move_beats_thrash_with_the_same_net_change(self):
        """The core requirement: 20 pips straight != 80 pips of chop ending +20."""
        clean = series([1.1000 + 0.0001 * i for i in range(60)], wick=0.00002)
        chop = []
        px = 1.1000
        for i in range(60):
            px += 0.0008 if i % 2 == 0 else -0.0007
            chop.append(px)
        # force the same net displacement
        chop[-1] = clean[-1].close
        thrash = series(chop, wick=0.00002)

        m_clean = mom.measure(clean)
        m_thrash = mom.measure(thrash)
        assert m_clean.efficiency > 0.85
        assert m_thrash.efficiency < 0.35
        assert m_clean.score_0_100() > m_thrash.score_0_100() + 20

    def test_efficiency_is_bounded(self):
        m = mom.measure(series([1.1 + 0.0001 * i for i in range(80)]))
        assert 0.0 <= m.efficiency <= 1.0

    def test_direction_sign(self):
        up = mom.measure(series([1.1 + 0.0001 * i for i in range(60)]))
        down = mom.measure(series([1.1 - 0.0001 * i for i in range(60)]))
        assert up.direction == 1 and down.direction == -1

    def test_acceleration_detects_a_speeding_move(self):
        closes = [1.1000]
        for i in range(60):
            closes.append(closes[-1] + 0.00002 * (1 + i * 0.15))
        m = mom.measure(series(closes))
        assert m.acceleration_atr > 0

    def test_too_little_data_returns_none(self):
        assert mom.measure(series([1.1, 1.1001])) is None


class TestExhaustion:
    def test_exhaustion_discounts_a_late_entry(self):
        assert mom.exhaustion_discount(2.5, None) < 1.0

    def test_normal_range_is_not_discounted(self):
        assert mom.exhaustion_discount(0.8, None) == 1.0

    def test_strong_accelerating_momentum_recovers_most_of_the_discount(self):
        """A genuine trend day must remain tradable past its average range."""
        strong = mom.measure(series([1.1 + 0.0002 * i for i in range(80)],
                                    wick=0.00001))
        plain = mom.exhaustion_discount(2.5, None)
        with_strength = mom.exhaustion_discount(2.5, strong)
        assert with_strength > plain
        assert with_strength > 0.85


class TestStructure:
    def test_uptrend_is_recognised(self):
        closes = []
        px = 1.1000
        for leg in range(6):
            for _ in range(6):
                px += 0.0004
                closes.append(px)
            for _ in range(3):
                px -= 0.00015
                closes.append(px)
        r = st.analyse(series(closes, wick=0.00003))
        assert r.trend is st.Trend.UP
        assert r.higher_highs and r.higher_lows
        assert r.score_0_100(1) > r.score_0_100(-1)

    def test_break_of_structure_is_never_both_ways(self, sim):
        for sym in sim.symbols():
            r = st.analyse(sim.bars(sym, TF.M15, 400))
            if r is None:
                continue
            assert not (r.bos_up and r.bos_down)

    def test_swings_require_confirmation_on_the_right(self):
        bars = series([1.1, 1.101, 1.102, 1.103, 1.102, 1.101, 1.10] * 8)
        swings = st.find_swings(bars, left=2, right=2)
        assert swings, "should find pivots"
        assert max(s.index for s in swings) <= len(bars) - 3

    def test_compression_detected_when_range_shrinks(self):
        wide = [1.1 + 0.002 * ((i % 6) - 3) for i in range(60)]
        tight = [1.1 + 0.00005 * ((i % 4) - 2) for i in range(40)]
        r = st.analyse(series(wide + tight, wick=0.00002))
        assert r.compression > 0.5

    def test_tight_invalidation_stays_within_its_cap(self):
        bars = series([1.1 + 0.0003 * i for i in range(60)], wick=0.00005)
        a = atr(bars, 14)
        inval = st.tight_invalidation(bars, 1, a, min_atr=0.5, max_atr=2.0)
        dist = bars[-1].close - inval
        assert 0 < dist <= 2.0 * a * 1.001

    def test_tight_invalidation_is_symmetric_for_shorts(self):
        bars = series([1.1 - 0.0003 * i for i in range(60)], wick=0.00005)
        a = atr(bars, 14)
        inval = st.tight_invalidation(bars, -1, a)
        assert inval > bars[-1].close


class TestBreakoutQuality:
    def _bars_with_break(self, close_beyond, body_frac):
        base = series([1.1000 + 0.00002 * ((i % 5) - 2) for i in range(50)],
                      wick=0.00003)
        level = max(b.high for b in base)
        rng = 0.0006
        close = level + close_beyond
        open_ = close - rng * body_frac
        bar = Bar(time=base[-1].time + dt.timedelta(minutes=5), open=open_,
                  high=close + rng * (1 - body_frac) * 0.2, low=open_ - 0.00002,
                  close=close, tick_volume=600)
        return base + [bar], level

    def test_wick_only_break_is_rejected(self):
        base = series([1.1000 + 0.00002 * ((i % 5) - 2) for i in range(50)],
                      wick=0.00003)
        level = max(b.high for b in base)
        wick_bar = Bar(time=base[-1].time + dt.timedelta(minutes=5),
                       open=level - 0.0003, high=level + 0.0008,
                       low=level - 0.0004, close=level - 0.0002,
                       tick_volume=600)
        q = st.assess_breakout(base + [wick_bar], level, 1,
                               atr_value=atr(base, 14), median_volume=300)
        assert not q.confirmed
        assert q.rejected, "a poke and reclaim is a failed break"
        assert q.failed

    def test_solid_body_close_beyond_is_confirmed(self):
        bars, level = self._bars_with_break(0.0005, 0.8)
        q = st.assess_breakout(bars, level, 1, atr_value=atr(bars[:-1], 14),
                               median_volume=300)
        assert q.confirmed and q.score > 40

    def test_participation_raises_the_score(self):
        bars, level = self._bars_with_break(0.0005, 0.8)
        a = atr(bars[:-1], 14)
        low_part = st.assess_breakout(bars, level, 1, atr_value=a,
                                      median_volume=2000)
        high_part = st.assess_breakout(bars, level, 1, atr_value=a,
                                       median_volume=200)
        assert high_part.score > low_part.score


class TestHeadroom:
    def test_wall_overhead_reduces_the_multiplier(self):
        s = spec()
        a = 0.0010
        near = hr.measure(1.1000, 1, a, s,
                          named_levels=[("PDH", 1.1001)])
        far = hr.measure(1.1000, 1, a, s,
                         named_levels=[("PDH", 1.1060)])
        assert near.multiplier() < 0.7 < far.multiplier()

    def test_open_space_earns_a_small_bonus(self):
        r = hr.measure(1.1000, 1, 0.0010, spec())
        assert r.multiplier() > 1.0

    def test_a_strong_level_binds_before_a_nearer_weak_one(self):
        from mintel.data.structure import Level
        r = hr.measure(1.1000, 1, 0.0010, spec(),
                       levels=[Level(price=1.1004, kind="SWING_HIGH",
                                     strength=0.3)],
                       named_levels=[("PDH", 1.1008)])
        assert r.nearest.kind == "PDH"

    def test_target_is_not_capped_by_a_round_number(self):
        r = hr.measure(1.1000, 1, 0.0010, spec())
        target = r.realistic_target(objective_atr=2.5)
        assert target == pytest.approx(1.1000 + 0.0025)

    def test_target_respects_a_genuinely_strong_barrier(self):
        r = hr.measure(1.1000, 1, 0.0010, spec(),
                       named_levels=[("PDH", 1.1012)])
        target = r.realistic_target(objective_atr=2.5)
        assert 1.1000 < target < 1.1012

    def test_round_numbers_are_generated_in_the_right_direction(self):
        up = hr.round_numbers(1.1023, spec(), 1)
        down = hr.round_numbers(1.1023, spec(), -1)
        assert all(x > 1.1023 for x in up)
        assert all(x < 1.1023 for x in down)


class TestCurrencyStrength:
    def _pairs(self, usd_strength: float):
        """USD rising against everything by construction."""
        out = {}
        for quote in ("JPY", "CHF", "CAD"):
            out[f"USD{quote}"] = series(
                [1.0 + usd_strength * i for i in range(80)], wick=0.00002)
        for base in ("EUR", "GBP", "AUD", "NZD"):
            out[f"{base}USD"] = series(
                [1.0 - usd_strength * i for i in range(80)], wick=0.00002)
        return out

    def test_broad_dollar_strength_is_detected(self):
        r = stg.compute(self._pairs(0.0002))
        assert r.score("USD") > 70
        for c in ("EUR", "GBP", "JPY"):
            assert r.score(c) < 40
        assert r.pairs_used == 7

    def test_one_pair_cannot_dominate_the_scale(self):
        """A currency seen in a single pair must not print an extreme score."""
        pairs = {"EURNZD": series([1.0 + 0.002 * i for i in range(80)])}
        r = stg.compute(pairs)
        assert 30 < r.score("NZD") < 70
        assert r.confidence("NZD") < 0.5

    def test_confidence_grows_with_the_number_of_pairs(self):
        r = stg.compute(self._pairs(0.0002))
        assert r.confidence("USD") == 1.0
        assert r.confidence("JPY") < 1.0

    def test_no_opinion_when_there_is_no_theme(self):
        flat = {f"{b}USD": series([1.0] * 80) for b in ("EUR", "GBP", "AUD")}
        r = stg.compute(flat)
        assert r.agrees_with("EUR", "USD", 1) is None

    def test_divergence_direction(self):
        r = stg.compute(self._pairs(0.0002))
        assert r.divergence("GBP", "USD") < 0
        assert r.agrees_with("GBP", "USD", -1) is True
        assert r.agrees_with("GBP", "USD", 1) is False


class TestCrossMarket:
    def test_correlated_market_agreement_raises_the_score(self):
        mine = series([1.0 + 0.0002 * i for i in range(160)])
        friend = series([2.0 + 0.0004 * i for i in range(160)])
        r = crossmarket.analyse("A", 1, mine, {"B": friend})
        assert r.score_0_100() > 60

    def test_disagreement_lowers_the_score(self):
        mine = series([1.0 + 0.0002 * i for i in range(160)])
        friend = series([2.0 + 0.0004 * i for i in range(160)])
        r = crossmarket.analyse("A", -1, mine, {"B": friend})
        assert r.score_0_100() < 40

    def test_no_usable_relationship_is_neutral_not_negative(self):
        mine = series([1.0 + 0.0002 * i for i in range(160)])
        r = crossmarket.analyse("A", 1, mine, {})
        assert r.score_0_100() == 50.0

    def test_market_depth_is_never_fabricated(self):
        mine = series([1.0 + 0.0002 * i for i in range(160)])
        r = crossmarket.analyse("A", 1, mine, {})
        assert r.depth_available is False
        assert "depth" in r.note


class TestRegime:
    def test_trending_data_is_classified_as_trend(self):
        closes = []
        px = 1.1
        for leg in range(8):
            for _ in range(7):
                px += 0.0004
                closes.append(px)
            for _ in range(3):
                px -= 0.0001
                closes.append(px)
        bars = series(closes, wick=0.00004)
        r = rg.classify(st.analyse(bars), mom.measure(bars))
        assert r.regime in (rg.Regime.TREND, rg.Regime.HIGH_VOL)
        assert r.direction == 1

    def test_squeeze_is_classified_from_compression(self):
        wide = [1.1 + 0.002 * ((i % 6) - 3) for i in range(80)]
        tight = [1.1 + 0.00004 * ((i % 4) - 2) for i in range(40)]
        bars = series(wide + tight, wick=0.00002)
        r = rg.classify(st.analyse(bars), mom.measure(bars))
        assert r.regime in (rg.Regime.SQUEEZE, rg.Regime.LOW_VOL)

    def test_news_window_dominates(self):
        bars = series([1.1 + 0.0002 * i for i in range(80)])
        r = rg.classify(st.analyse(bars), mom.measure(bars),
                        in_news_window=True, news_importance=3)
        assert r.regime is rg.Regime.NEWS

    def test_insufficient_data_is_reported_honestly(self):
        r = rg.classify(None, None)
        assert r.regime is rg.Regime.UNCLEAR
        assert r.confidence == 0.0
