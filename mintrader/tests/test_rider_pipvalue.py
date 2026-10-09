"""Rapid Momentum Rider: GBP per pip -> lots, per instrument.

The tick values are realistic MT5 figures for a GBP account (tick_value =
GBP per tick_size per 1.0 lot). Note on USDJPY: one 0.001 tick on 1.0 lot is
100,000 x 0.001 = 100 JPY, about GBP 0.49 at GBPJPY ~203, so tick_value is
~0.49 (a figure of 0.0049 would be a hundred times too small and would give
~20 lots, not ~0.20)."""
import dataclasses

import pytest

from mintel.rider.pipvalue import fx_pip_size, gbp_per_pip_for, lots_for_gbp_per_pip, round_to_step
from mintel.rider.synth import make_spec


def spec(name, **kw):
    s = make_spec(name)
    return dataclasses.replace(s, **kw) if kw else s


class TestPipDefinition:
    def test_five_digit_fx_pip_is_one_ten_thousandth(self):
        pip, note = fx_pip_size(spec("EURUSD"))
        assert pip == pytest.approx(0.0001) and note == ""

    def test_three_digit_jpy_pip_is_one_hundredth(self):
        pip, note = fx_pip_size(spec("USDJPY"))
        assert pip == pytest.approx(0.01) and note == ""

    def test_four_and_two_digit_quotes(self):
        assert fx_pip_size(spec("EURUSD", digits=4, point=0.0001, tick_size=0.0001))[0] == pytest.approx(0.0001)
        assert fx_pip_size(spec("USDJPY", digits=2, point=0.01, tick_size=0.01))[0] == pytest.approx(0.01)

    def test_a_broker_that_did_not_class_the_symbol_as_fx_is_corrected_and_says_so(self):
        s = spec("EURUSD", asset_group="UNKNOWN")          # spec.pip_size would be the point, 0.00001
        pip, note = fx_pip_size(s)
        assert pip == pytest.approx(0.0001) and "did not match" in note

    def test_not_fx_is_not_sized(self):
        s = dataclasses.replace(spec("EURUSD"), name="XAUUSD", base_currency="XAU", profit_currency="USD")
        v, actual, why = lots_for_gbp_per_pip(s, 1.0)
        assert v == 0.0 and "not an FX pair" in why


class TestLots:
    def test_eurusd_five_digit(self):
        v, actual, why = lots_for_gbp_per_pip(spec("EURUSD", tick_value=0.74), 1.0)
        assert v == pytest.approx(0.14)                       # 1 / 7.40 = 0.135 -> nearest 0.01
        assert actual == pytest.approx(1.036, abs=1e-3) and "0.14 lots" in why

    def test_usdjpy_three_digit(self):
        v, actual, _ = lots_for_gbp_per_pip(spec("USDJPY", tick_value=0.4926), 1.0)
        assert v == pytest.approx(0.20)                       # 1 / 4.926 = 0.203
        assert actual == pytest.approx(0.985, abs=1e-3)

    def test_eurgbp_cross_one_pound_a_tick(self):
        v, actual, _ = lots_for_gbp_per_pip(spec("EURGBP", tick_value=1.0), 1.0)
        assert v == pytest.approx(0.10) and actual == pytest.approx(1.0)

    def test_gbpjpy(self):
        v, actual, _ = lots_for_gbp_per_pip(spec("GBPJPY", tick_value=0.4926), 1.0)
        assert v == pytest.approx(0.20) and actual == pytest.approx(0.985, abs=1e-3)

    def test_audnzd(self):
        v, actual, _ = lots_for_gbp_per_pip(spec("AUDNZD", tick_value=0.43), 1.0)   # 1 NZD ~ GBP 0.43
        assert v == pytest.approx(0.23) and actual == pytest.approx(0.989, abs=1e-3)

    def test_four_digit_broker_gives_the_same_size(self):
        s = spec("EURUSD", digits=4, point=0.0001, tick_size=0.0001, tick_value=7.4)
        assert lots_for_gbp_per_pip(s, 1.0)[0] == pytest.approx(0.14)

    def test_step_min_max(self):
        s = spec("EURUSD", tick_value=0.74, volume_step=0.01, volume_min=0.01, volume_max=50.0)
        assert lots_for_gbp_per_pip(s, 1000.0)[0] == pytest.approx(50.0)          # 135 lots asked: capped
        v, actual, why = lots_for_gbp_per_pip(s, 0.01)
        assert v == pytest.approx(0.01) and "smallest size" in why               # never below the minimum
        assert lots_for_gbp_per_pip(s, 2.0)[0] == pytest.approx(0.27)

    def test_rounds_to_the_nearest_step_not_down(self):
        assert round_to_step(0.1351, 0.01, 0.01, 50.0) == pytest.approx(0.14)
        assert round_to_step(0.1349, 0.01, 0.01, 50.0) == pytest.approx(0.13)
        assert round_to_step(0.1351, 0.1, 0.1, 50.0) == pytest.approx(0.1)
        assert round_to_step(57.0, 0.01, 0.01, 50.0) == pytest.approx(50.0)

    def test_non_gbp_account_converts_with_the_rate_supplied(self):
        usd_acct = spec("EURUSD", tick_value=1.0)              # USD account: 1 USD a tick per lot
        v, actual, why = lots_for_gbp_per_pip(usd_acct, 1.0, "USD", lambda a, b: 0.74 if (a, b) == ("USD", "GBP") else None)
        assert v == pytest.approx(0.14) and actual == pytest.approx(1.036, abs=1e-3)
        v2, _, why2 = lots_for_gbp_per_pip(usd_acct, 1.0, "USD", None)
        assert v2 == 0.0 and "no USD->GBP rate" in why2       # never guessed

    def test_reported_value_matches_gbp_per_pip_for(self):
        s = spec("EURUSD", tick_value=0.74)
        v, actual, _ = lots_for_gbp_per_pip(s, 1.0)
        assert gbp_per_pip_for(s, v) == pytest.approx(actual)
