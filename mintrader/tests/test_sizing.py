"""Position sizing and the realistic £1,500 account (Aaron, 14 Oct 2026). Every trade here is MADE UP."""
from __future__ import annotations

import datetime as dt

import pytest

from mintel.contracts import SymbolSpec
from mintel.ops import sizing as Z

UTC = dt.timezone.utc
RATES = {"USD": 0.80, "EUR": 0.85, "JPY": 0.005}


def at(day: int, hour: int = 10, minute: int = 0) -> dt.datetime:
    return dt.datetime(2026, 10, day, hour, minute, tzinfo=UTC)


def trade(r: float, closed: dt.datetime, risk: float = 10.0, opened=None, **kw) -> dict:
    return dict({"closed": closed, "opened": opened or closed - dt.timedelta(minutes=5), "net": round(r * risk, 2),
                 "risk": risk}, **kw)


class TestMoneyMaths:
    def test_pip_value_and_the_currency_conversion(self):
        eu = Z.spec_for("EURUSD", RATES)
        assert eu.tick_value == pytest.approx(100_000 * 0.00001 * 0.80)          # £0.80 a point a lot
        assert Z.pip_value_per_lot(eu) == pytest.approx(8.0)                     # $10 a pip = £8
        uj = Z.spec_for("USDJPY", RATES)
        assert Z.pip_value_per_lot(uj) == pytest.approx(1000 * 0.005)             # ¥1,000 a pip = £5
        assert Z.gbp_rate("GBP") == 1.0 and Z.gbp_rate("XYZ") is None
        assert Z.spec_for("EURUSD", {"USD": 0}).tick_value == pytest.approx(100_000 * 0.00001 * 0.79)  # bad rate ignored

    def test_the_size_comes_from_equity_risk_and_the_stop(self):
        eu = Z.spec_for("EURUSD", RATES)
        s = Z.position_size(1500, 0.5, 0.0015, eu)                     # £7.50 at a 15-pip stop: £0.50 a pip
        assert s.ok and s.risk_target == 7.5
        assert s.volume == 0.06 and s.risk_at_stop == pytest.approx(7.2)       # floored: 0.0625 -> 0.06
        assert s.risk_at_stop <= s.risk_target and s.per_pip == pytest.approx(0.48)

    def test_costs_count_against_the_risk(self):
        eu = Z.spec_for("EURUSD", RATES)
        bare = Z.position_size(3000, 0.5, 0.0015, eu)
        costed = Z.position_size(3000, 0.5, 0.0015, eu, spread_points=10, commission_per_lot=5.6)
        assert costed.volume < bare.volume and costed.costs > 0
        assert costed.risk_with_costs <= costed.risk_target + 1e-9

    def test_below_the_minimum_lot_is_no_trade_never_rounded_up(self):
        gold = Z.spec_for("XAUUSD", RATES)
        s = Z.position_size(200, 0.5, 20.0, gold)                       # £1 of risk; 0.01 lots would risk £16
        assert not s.ok and s.volume == 0 and s.min_lot_risk == pytest.approx(16.0)
        assert "minimum lot" in s.why

    def test_no_stop_no_size(self):
        assert "no stop" in Z.position_size(1500, 0.5, 0.0, Z.spec_for("EURUSD", RATES)).why

    def test_margin_against_free_margin(self):
        eu = Z.spec_for("EURUSD", RATES)
        assert Z.margin_for(eu, 1.0, 1.09, 0.85) == pytest.approx(100_000 * 0.85 / 30, abs=0.01)   # FX: the base ccy
        gold = Z.spec_for("XAUUSD", RATES)
        assert Z.margin_for(gold, 0.1, 2650.0, 0.80) == pytest.approx(0.1 * 100 * 2650 * 0.80 / 20, abs=0.01)
        assert Z.margin_for(eu, 1.0, 1.09, 0.85, margin_per_lot=3000.0) == 3000.0            # the broker's own
        s = Z.position_size(10_000, 0.5, 0.0015, eu, price=1.09, rate_to_gbp=0.85, free_margin=50.0)
        assert not s.ok and s.margin_ok is False and "free margin" in s.why
        ok = Z.position_size(10_000, 0.5, 0.0015, eu, price=1.09, rate_to_gbp=0.85, free_margin=5000.0)
        assert ok.ok and ok.margin_ok

    def test_exposure_scales_with_equity_both_ways(self):
        eu = Z.spec_for("EURUSD", RATES)
        sizes = [Z.position_size(e, 0.5, 0.0015, eu).volume for e in (750, 1500, 3000, 10_000)]
        assert sizes == sorted(sizes) and sizes[0] < sizes[-1]
        assert Z.risk_gbp(3000, 0.5) == 2 * Z.risk_gbp(1500, 0.5) == 15.0
        assert Z.risk_gbp(750, 0.5) == 3.75

    def test_the_combined_cap_and_the_ceiling(self):
        eu = Z.spec_for("EURUSD", RATES)
        s = Z.position_size(10_000, 0.5, 0.0015, eu, open_risk_pct=1.8)
        assert s.risk_target == pytest.approx(20.0)                      # 0.2% left under the 2% cap
        assert not Z.position_size(10_000, 0.5, 0.0015, eu, open_risk_pct=2.0).ok
        assert Z.position_size(10_000, 5.0, 0.0015, eu).risk_target == 150.0     # 5% asked: 1.5% ceiling
        assert Z.combined_ok([0.5, 0.5, 0.5], 0.5) and not Z.combined_ok([0.5, 0.5, 0.5, 0.5], 0.5)

    def test_reference_table_by_balance(self):
        t = Z.balance_table([{"id": "a", "nick": "A", "league": "DEMO", "mean_r": 0.2, "per_day": 3}])
        cells = t["strategies"][0]["cells"]
        assert [c["risk"] for c in cells] == [7.5, 15.0, 25.0, 50.0]
        assert [c["per_day"] for c in cells] == [4.5, 9.0, 15.0, 30.0]
        eur = next(i for i in t["instruments"] if i["name"] == "EURUSD")
        pips = [c["per_pip"] for c in eur["cells"]]
        assert pips == sorted(pips) and all(p > 0 for p in pips)


class TestTheAccount:
    def test_compounds_both_ways(self):
        sim = Z.simulate({"a": [trade(-1, at(12, 10)), trade(1, at(12, 11))]})
        assert sim["taken"] == 2
        assert sim["end"] == pytest.approx(1500 - 7.5 + 1492.5 * 0.005, abs=0.01)     # the 2nd trade risks less
        assert sim["max_dd_pct"] == pytest.approx(0.5)

    def test_the_combined_cap(self):
        opened = at(12, 9)
        five = [trade(1, at(12, 10, i), opened=opened) for i in range(5)]       # five open at once
        sim = Z.simulate({"a": five})
        assert sim["taken"] == 4 and sim["skipped"] == {"combined cap": 1}

    def test_the_daily_safeguard(self):
        losses = [trade(-1, at(12, 10 + i)) for i in range(8)]
        sim = Z.simulate({"a": losses})
        assert sim["taken"] == 5 and sim["skipped"]["daily stop"] == 3        # 2% of the day's start

    def test_the_weekly_and_overall_safeguards(self):
        days = (12, 13, 14, 15, 16, 19, 20, 21, 22, 23, 26, 27)
        bad = [trade(-1, at(d, 9 + i)) for d in days for i in range(4)]
        sim = Z.simulate({"a": bad})
        assert sim["skipped"].get("weekly stop", 0) > 0
        worse = Z.simulate({"a": bad}, safeguards={"weekly_pct": 100.0, "daily_pct": 100.0})
        assert worse["halted"] and worse["skipped"]["drawdown stop"] > 0
        assert worse["max_dd_pct"] >= 10.0 and worse["max_dd_pct"] < 10.6

    def test_risk_unknown_and_the_minimum_lot(self):
        no_risk = {"closed": at(12), "net": 5.0}
        sim = Z.simulate({"a": [no_risk]})
        assert sim["taken"] == 0 and sim["skipped"] == {"no risk recorded": 1}
        sim = Z.simulate({"a": [no_risk]}, fallback_risk={"a": 10.0})
        assert sim["taken"] == 1 and sim["stated_risk"] == 1 and sim["net"] == pytest.approx(3.75)
        big = trade(1, at(12), risk=20.0, volume=0.01)                      # its smallest size risked £20
        assert Z.simulate({"a": [big]})["skipped"] == {"below the minimum lot": 1}

    def test_the_combined_cap_holds_whatever_order_they_close_in(self):
        # rv14: A1 and A2 open 10:00-11:00, B 12:00-12:30, and C, D, E open 10:30 and close after 13:00. B used
        # to cut A1 and A2 out of the book, so C, D and E saw nothing open and all six were taken (2.5% at 10:45)
        o10, o1030 = at(12, 10), at(12, 10, 30)
        trades = {"A1": [trade(1, at(12, 11), opened=o10)], "A2": [trade(1, at(12, 11), opened=o10)],
                  "B": [trade(1, at(12, 12, 30), opened=at(12, 12))], "C": [trade(1, at(12, 13), opened=o1030)],
                  "D": [trade(1, at(12, 13, 5), opened=o1030)], "E": [trade(1, at(12, 13, 10), opened=o1030)]}
        sim = Z.simulate(trades)
        taken = set(sim["by"])
        assert sim["skipped"].get("combined cap", 0) >= 1
        for moment in sorted({v[0]["opened"] for v in trades.values()}):
            open_now = sum(Z.TIER_START for k in taken if trades[k][0]["opened"] <= moment < trades[k][0]["closed"])
            assert open_now <= Z.MAX_COMBINED_PCT, (moment, open_now)

    def test_the_worst_drawdown_is_the_worst_percentage(self):
        # rv14: a later fall can be bigger in £ (off a higher peak) but smaller in %; the worst % is reported
        base = dt.datetime(2026, 1, 5, 10, tzinfo=UTC)
        rs = [-1] * 6 + [1] * 80 + [-1] * 5
        sim = Z.simulate({"x": [trade(r, base + dt.timedelta(days=i)) for i, r in enumerate(rs)]}, check_min_lot=False)
        assert sim["taken"] == 91 and not sim["skipped"]
        eq = peak = 1500.0
        worst = worst_gbp = 0.0
        for r in rs:
            eq += r * eq * Z.TIER_START / 100.0
            peak = max(peak, eq)
            worst, worst_gbp = max(worst, (peak - eq) / peak * 100.0), max(worst_gbp, peak - eq)
        assert sim["max_dd_pct"] == pytest.approx(worst, abs=0.01) and sim["max_dd_pct"] > 2.9    # not 2.48
        assert sim["max_dd_gbp"] == pytest.approx(worst_gbp, abs=0.01)

    def test_strategies_are_listed_apart(self):
        sim = Z.simulate({"a": [trade(1, at(12, 10))], "b": [trade(-0.5, at(12, 11))]})
        assert set(sim["by"]) == {"a", "b"} and sim["by"]["a"]["net"] > 0 > sim["by"]["b"]["net"]


class TestTiers:
    def test_needs_sixty_demo_trades_and_a_positive_lower_bound(self):
        assert Z.tier_recommendation(59, 0.5)["pct"] == 0.5
        assert Z.tier_recommendation(80, -0.1)["pct"] == 0.5
        assert Z.tier_recommendation(80, None)["pct"] == 0.5
        up = Z.tier_recommendation(60, 0.2)
        assert up["pct"] == 1.0 and up["needs_approval"] and "approval" in up["words"]
        top = Z.tier_recommendation(90, 0.2, current_pct=1.0)
        assert top["pct"] == 1.5 and "separate approval" in top["words"]

    def test_a_streak_alone_is_never_enough(self):
        r = Z.tier_recommendation(12, None, streak=9)
        assert r["pct"] == 0.5 and "streak alone is never enough" in r["words"]

    def test_never_above_the_ceiling(self):
        assert Z.tier_recommendation(500, 1.0, current_pct=1.5)["pct"] == 1.5
        assert max(Z.TIERS) == Z.MAX_TIER == 1.5 and Z.MAX_COMBINED_PCT == 2.0


def test_reference_specs_are_real_symbol_specs():
    for name in Z.REFERENCE_ORDER:
        s = Z.spec_for(name)
        assert isinstance(s, SymbolSpec) and not s.missing_fields()
        assert Z.reference_size(name, 10_000).ok
