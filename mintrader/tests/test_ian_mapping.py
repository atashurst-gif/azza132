"""Financial Ian: every futures <-> spot mapping, both directions, the prices and the roll.

An inversion error trades exactly backwards, so each contract is tested on its own."""
import datetime as dt

import pytest

from mintel.broker.base import Side
from mintel.ian import mapping as m

SAME = {"6E": "EURUSD", "6B": "GBPUSD", "6A": "AUDUSD", "6N": "NZDUSD"}
INVERTED = {"6C": "USDCAD", "6J": "USDJPY", "6S": "USDCHF"}


class TestTheTable:
    def test_every_fx_future_has_its_spot_pair_and_the_right_orientation(self):
        for root, spot in SAME.items():
            c = m.CONTRACTS[root]
            assert c.spot == spot and c.inverted is False and c.same_direction
        for root, spot in INVERTED.items():
            c = m.CONTRACTS[root]
            assert c.spot == spot and c.inverted is True and not c.same_direction
        assert set(m.CONTRACTS) == set(SAME) | set(INVERTED)

    def test_the_foreign_currency_is_what_the_future_prices(self):
        expect = {"6E": "EUR", "6B": "GBP", "6A": "AUD", "6N": "NZD", "6C": "CAD", "6J": "JPY", "6S": "CHF"}
        for root, ccy in expect.items():
            assert m.CONTRACTS[root].currency == ccy
            spot = m.CONTRACTS[root].spot
            # the foreign currency is the BASE of a same-direction pair and the QUOTE of an inverted one
            assert (spot[:3] == ccy) == (not m.CONTRACTS[root].inverted)
            assert (spot[3:] == ccy) == m.CONTRACTS[root].inverted

    def test_context_markets_are_never_traded(self):
        for root in ("ES", "ZN", "GC"):
            assert m.ALL[root].context_only


class TestDirections:
    @pytest.mark.parametrize("root", sorted(SAME))
    def test_same_direction_contracts(self, root):
        assert m.spot_direction(root, +1) == +1
        assert m.spot_direction(root, -1) == -1
        assert m.spot_direction(root, 0) == 0
        assert m.futures_direction(root, +1) == +1
        assert m.futures_direction(root, -1) == -1
        assert m.spot_side(root, Side.BUY) is Side.BUY
        assert m.spot_side(root, Side.SELL) is Side.SELL

    @pytest.mark.parametrize("root", sorted(INVERTED))
    def test_inverted_contracts(self, root):
        # the yen (franc, Canadian dollar) futures rising = the dollar falling against it = USDxxx DOWN
        assert m.spot_direction(root, +1) == -1
        assert m.spot_direction(root, -1) == +1
        assert m.futures_direction(root, +1) == -1          # buying USDJPY = selling yen futures
        assert m.futures_direction(root, -1) == +1
        assert m.spot_side(root, Side.BUY) is Side.SELL
        assert m.spot_side(root, Side.SELL) is Side.BUY

    @pytest.mark.parametrize("root", sorted(m.CONTRACTS))
    def test_the_translation_round_trips(self, root):
        for d in (-1, 0, 1):
            assert m.futures_direction(root, m.spot_direction(root, d)) == d
            assert m.spot_direction(root, m.futures_direction(root, d)) == d

    def test_magnitudes_are_reduced_to_a_sign(self):
        assert m.spot_direction("6J", 3.7) == -1 and m.spot_direction("6E", -0.2) == -1

    def test_an_unknown_root_is_an_error_not_a_guess(self):
        with pytest.raises(KeyError):
            m.spot_direction("ZZ", 1)

    def test_news_effects_follow_the_futures_quote(self):
        # good news for the euro lifts 6E; good news for the dollar drops it
        assert m.currency_effect("6E", "EUR", +1) == +1 and m.currency_effect("6E", "USD", +1) == -1
        # same for the yen future: good for JPY lifts 6J (and so USDJPY falls), good for USD drops 6J
        assert m.currency_effect("6J", "JPY", +1) == +1 and m.currency_effect("6J", "USD", +1) == -1
        assert m.spot_direction("6J", m.currency_effect("6J", "USD", +1)) == +1       # strong dollar -> USDJPY up
        assert m.currency_effect("6E", "GBP", +1) == 0


class TestPrices:
    @pytest.mark.parametrize("root,fut,spot", [("6E", 1.0850, 1.0850), ("6B", 1.27, 1.27), ("6A", 0.655, 0.655),
                                               ("6N", 0.605, 0.605)])
    def test_same_direction_prices_are_unchanged(self, root, fut, spot):
        assert m.futures_to_spot_price(root, fut) == pytest.approx(spot)
        assert m.spot_to_futures_price(root, spot) == pytest.approx(fut)

    @pytest.mark.parametrize("root,fut,spot", [("6J", 0.0066667, 150.0), ("6C", 0.7353, 1.36), ("6S", 1.1236, 0.89)])
    def test_inverted_prices_are_one_over(self, root, fut, spot):
        assert m.futures_to_spot_price(root, fut) == pytest.approx(spot, rel=1e-3)
        assert m.spot_to_futures_price(root, spot) == pytest.approx(fut, rel=1e-3)
        assert m.spot_to_futures_price(root, m.futures_to_spot_price(root, fut)) == pytest.approx(fut)

    def test_a_futures_rise_is_a_spot_fall_for_the_yen(self):
        lo, hi = 0.0066, 0.0067
        assert m.futures_to_spot_price("6J", hi) < m.futures_to_spot_price("6J", lo)
        assert m.futures_to_spot_price("6E", 1.09) > m.futures_to_spot_price("6E", 1.08)

    def test_distances_convert_with_the_inversion(self):
        assert m.futures_distance_to_spot("6E", 1.085, 0.0005) == pytest.approx(0.0005)
        # 10 yen-future ticks (0.000005) at 0.0066667 is about 0.11 yen on USDJPY
        d = m.futures_distance_to_spot("6J", 0.0066667, 0.000005)
        assert d == pytest.approx(0.000005 / 0.0066667 ** 2, rel=0.01) and d == pytest.approx(0.1124, rel=0.01)
        assert m.futures_distance_to_spot("6C", 0.7353, -0.0005) > 0

    def test_the_basis_is_spot_minus_the_converted_future(self):
        assert m.basis("6E", 1.0870, 1.0850) == pytest.approx(-0.0020)
        assert m.basis("6J", 0.0066667, 150.2) == pytest.approx(0.2, abs=0.01)

    def test_nonsense_prices_are_refused(self):
        with pytest.raises(ValueError):
            m.futures_to_spot_price("6J", 0.0)


class TestSymbols:
    def test_roots_from_every_spelling(self):
        assert m.root_of("6EZ6") == "6E" and m.root_of("6E.c.0") == "6E" and m.root_of("6E.FUT") == "6E"
        assert m.root_of("6JH7") == "6J" and m.root_of("6EZ6-6EH7") == "6E" and m.root_of("ESZ6") == "ES"
        assert m.root_of("EURUSD") == "" and m.root_of("") == "" and m.root_of("6EX") == ""

    def test_spot_for_future_and_back(self):
        for root, spot in {**SAME, **INVERTED}.items():
            assert m.spot_symbol(root) == spot
            assert m.future_for_spot(spot) == root
        assert m.future_for_spot("EURUSD.a") == "6E" and m.future_for_spot("EURGBP") == ""

    def test_the_brokers_own_spelling_is_used(self):
        assert m.spot_symbol("6E", ["GBPUSD", "EURUSD.a", "USDJPY.a"]) == "EURUSD.a"
        assert m.spot_symbol("6J", ["USDJPY"]) == "USDJPY"
        assert m.spot_symbol("6S", ["EURUSD"]) == ""

    def test_raw_symbols_parse(self):
        assert m.parse_raw_symbol("6EZ6") == ("6E", 12, 6)
        assert m.parse_raw_symbol("6JH7") == ("6J", 3, 7)
        assert m.parse_raw_symbol("EURUSD") is None


class TestTheRoll:
    def test_third_wednesdays(self):
        assert m.third_wednesday(2026, 12) == dt.date(2026, 12, 16)
        assert m.third_wednesday(2027, 3) == dt.date(2027, 3, 17)
        assert m.third_wednesday(2026, 9) == dt.date(2026, 9, 16)

    def test_last_trading_day_is_two_business_days_before_one_for_the_canadian_dollar(self):
        assert m.contract_for("6E", 2026, 12).last_trade == dt.date(2026, 12, 14)        # Mon before Wed 16th
        assert m.contract_for("6C", 2026, 12).last_trade == dt.date(2026, 12, 15)        # Tue
        assert m.contract_for("6E", 2027, 3).last_trade == dt.date(2027, 3, 15)

    def test_front_month_and_its_roll(self):
        fm = m.front_month("6E", dt.date(2026, 10, 8))
        assert fm.raw_symbol == "6EZ6" and fm.month == 12
        assert m.front_month("6E", dt.date(2026, 12, 5)).raw_symbol == "6EZ6"
        # eight days before the last trading day the liquidity has moved to March
        assert m.front_month("6E", dt.date(2026, 12, 6)).raw_symbol == "6EH7"
        assert m.front_month("6J", dt.date(2026, 9, 1)).raw_symbol == "6JU6"
        assert m.front_month("6B", dt.date(2026, 12, 31)).raw_symbol == "6BH7"
        assert m.front_month("6E", dt.datetime(2026, 10, 8, 12, tzinfo=dt.timezone.utc)).raw_symbol == "6EZ6"
