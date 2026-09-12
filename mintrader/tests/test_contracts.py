"""Money math.  Every bug here costs real money, so the coverage is dense."""
from __future__ import annotations

import pytest

from mintel.contracts import SymbolSpec, classify_fx, infer_group


def fx5(name="EURUSD", **kw) -> SymbolSpec:
    base = dict(name=name, digits=5, point=0.00001, tick_size=0.00001,
                tick_value=1.0, contract_size=100_000, volume_min=0.01,
                volume_max=100.0, volume_step=0.01, stops_level_points=10.0,
                freeze_level_points=0.0, asset_group="FX_MAJOR")
    base.update(kw)
    return SymbolSpec(**base)


def jpy3(**kw) -> SymbolSpec:
    base = dict(name="USDJPY", digits=3, point=0.001, tick_size=0.001,
                tick_value=0.68, contract_size=100_000, volume_min=0.01,
                volume_max=100.0, volume_step=0.01, stops_level_points=10.0,
                freeze_level_points=0.0, asset_group="FX_MAJOR")
    base.update(kw)
    return SymbolSpec(**base)


def gold(**kw) -> SymbolSpec:
    base = dict(name="XAUUSD", digits=2, point=0.01, tick_size=0.01,
                tick_value=1.0, contract_size=100, volume_min=0.01,
                volume_max=50.0, volume_step=0.01, stops_level_points=30.0,
                freeze_level_points=0.0, asset_group="GOLD")
    base.update(kw)
    return SymbolSpec(**base)


class TestPipConversion:
    def test_five_digit_fx_pip_is_ten_points(self):
        s = fx5()
        assert s.points_per_pip == 10
        assert s.pip_size == pytest.approx(0.0001)
        assert s.price_to_pips(0.0025) == pytest.approx(25.0)
        assert s.pips_to_price(25.0) == pytest.approx(0.0025)

    def test_three_digit_jpy_pip_is_ten_points(self):
        s = jpy3()
        assert s.pip_size == pytest.approx(0.01)
        assert s.price_to_pips(0.35) == pytest.approx(35.0)

    def test_four_digit_fx_pip_is_one_point(self):
        s = fx5(digits=4, point=0.0001, tick_size=0.0001, tick_value=10.0)
        assert s.points_per_pip == 1
        assert s.pip_size == pytest.approx(0.0001)

    def test_metals_and_indices_use_points_as_pips(self):
        assert gold().points_per_pip == 1
        assert gold().pip_size == pytest.approx(0.01)

    def test_round_trip_is_lossless(self):
        for spec in (fx5(), jpy3(), gold()):
            for pips in (0.1, 1, 7.5, 130.0):
                assert spec.price_to_pips(spec.pips_to_price(pips)) == pytest.approx(pips)


class TestMoney:
    def test_money_per_lot_matches_hand_calculation(self):
        # 20 pips on EURUSD = 200 points; 1.0 lot at 1.0 per point = 200.
        assert fx5().money_per_lot(0.0020) == pytest.approx(200.0)

    def test_money_scales_with_volume(self):
        s = fx5()
        assert s.money(0.0020, 0.25) == pytest.approx(50.0)
        assert s.money(0.0020, 2.0) == pytest.approx(400.0)

    def test_money_is_signed_so_a_loss_is_negative(self):
        """An unsigned result here would record every loss as a gain."""
        s = fx5()
        assert s.money(0.0020, 1.0) == pytest.approx(200.0)
        assert s.money(-0.0020, 1.0) == pytest.approx(-200.0)

    def test_money_per_lot_is_a_magnitude(self):
        s = fx5()
        assert s.money_per_lot(-0.0020) == s.money_per_lot(0.0020) > 0

    def test_signed_money_scales_correctly_for_shorts(self):
        s = fx5()
        # A short from 1.1000 closed at 1.0980 gained 20 pips.
        moved = (1.0980 - 1.1000) * -1
        assert s.money(moved, 1.0) == pytest.approx(200.0)
        # The same short closed at 1.1020 lost 20 pips.
        moved = (1.1020 - 1.1000) * -1
        assert s.money(moved, 1.0) == pytest.approx(-200.0)

    def test_jpy_tick_value_respected(self):
        s = jpy3()
        # 30 pips = 300 points, 0.68 per point -> 204.0 per lot
        assert s.money_per_lot(0.30) == pytest.approx(204.0)

    def test_gold_contract_size_respected(self):
        # 5.00 of price = 500 ticks at 1.0 -> 500 per lot
        assert gold().money_per_lot(5.00) == pytest.approx(500.0)

    def test_distance_for_money_inverts_money(self):
        s = fx5()
        d = s.price_distance_for_money(200.0, 1.0)
        assert s.money(d, 1.0) == pytest.approx(200.0)


class TestVolume:
    def test_volume_floors_onto_the_step(self):
        s = fx5()
        assert s.normalise_volume(0.178) == pytest.approx(0.17)

    def test_volume_never_rounds_up_past_the_risk_budget(self):
        s = fx5(volume_step=0.1)
        assert s.normalise_volume(0.19) == pytest.approx(0.1)

    def test_below_minimum_becomes_zero(self):
        assert fx5().normalise_volume(0.004) == 0.0

    def test_above_maximum_is_clamped(self):
        assert fx5(volume_max=5.0).normalise_volume(500.0) == pytest.approx(5.0)

    def test_negative_volume_is_zero(self):
        assert fx5().normalise_volume(-1.0) == 0.0


class TestStops:
    def test_min_stop_distance_respects_broker_level(self):
        s = fx5(stops_level_points=20.0)
        assert s.min_stop_distance_price(0.0) == pytest.approx(0.0002)

    def test_min_stop_distance_respects_spread(self):
        s = fx5(stops_level_points=1.0)
        assert s.min_stop_distance_price(0.0005, 2.0) == pytest.approx(0.0010)

    def test_zero_stops_level_still_has_a_floor(self):
        s = fx5(stops_level_points=0.0)
        assert s.min_stop_distance_price(0.0) >= s.tick_size


class TestValidation:
    def test_complete_spec_is_tradable(self):
        assert fx5().is_tradable()

    @pytest.mark.parametrize("field", ["tick_value", "contract_size",
                                       "volume_step", "point"])
    def test_missing_money_field_makes_it_untradable(self, field):
        s = fx5(**{field: 0.0})
        assert not s.is_tradable()
        assert field in s.missing_fields()

    def test_trade_disabled_symbol_is_untradable(self):
        assert not fx5(trade_allowed=False).is_tradable()


class TestClassification:
    @pytest.mark.parametrize("name,base,quote", [
        ("EURUSD", "EUR", "USD"), ("GBPJPY", "GBP", "JPY"),
        ("EURUSD.pro", "EUR", "USD"), ("USDCHFm", "USD", "CHF"),
        ("AUDNZD_raw", "AUD", "NZD"),
    ])
    def test_broker_suffixes_are_stripped(self, name, base, quote):
        b, q, _ = classify_fx(name)
        assert (b, q) == (base, quote)

    def test_non_fx_returns_empty(self):
        assert classify_fx("GER40") == ("", "", "")

    @pytest.mark.parametrize("name,group", [
        ("EURUSD", "FX_MAJOR"), ("EURJPY", "FX_MINOR"), ("XAUUSD", "GOLD"),
        ("XAGUSD", "SILVER"), ("GER40", "INDEX"), ("US500", "INDEX"),
        ("USOIL", "ENERGY"),
    ])
    def test_group_inference(self, name, group):
        assert infer_group(name) == group
