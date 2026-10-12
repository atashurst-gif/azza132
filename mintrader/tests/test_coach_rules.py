"""Aaron's standing rules, codified (spec sections 6.2 and 18): S1, S2, S3.

The reproduction tests use Trend & Breakout's REAL trades from the reports
branch (tests/fixtures/coach_reports_trades.csv) against the dated entries in
``mintel/config.py`` (``scan.disabled_tactic_regimes`` from 25 Sep onward and
``universe.excluded_symbols``). Every other record here is MADE UP.
"""
from __future__ import annotations

import datetime as dt

import pytest

from mintel.coach import epochs as E
from mintel.coach import records as R
from mintel.coach import rules as RU
from mintel.config import ScanConfig, UniverseConfig
from tests.test_coach_estimate import reports_trades

UTC = dt.timezone.utc
UK = R.TZ_UK


def end_of(day: dt.date) -> dt.datetime:
    """23:59 UK on ``day`` (the evening review), in UTC."""
    return dt.datetime(day.year, day.month, day.day, 23, 59, tzinfo=UK).astimezone(UTC)


@pytest.fixture(scope="module")
def record():
    return reports_trades()


# The dated entries added to scan.disabled_tactic_regimes from 25 Sep onward (config.py's own notes).
DATED_S1 = [
    (dt.date(2026, 9, 25), "BREAKOUT_RETEST", "SQUEEZE"),
    (dt.date(2026, 9, 28), "BREAKOUT_RETEST", "TREND"),
    (dt.date(2026, 9, 30), "BREAKOUT_ACCEPTANCE", "TREND"),
    (dt.date(2026, 9, 30), "LIQUIDITY_SWEEP_REVERSAL", "HIGH_VOL"),
    (dt.date(2026, 10, 2), "SESSION_EXPANSION", "SQUEEZE"),
    (dt.date(2026, 10, 6), "BREAKOUT_ACCEPTANCE", "HIGH_VOL"),
]

# The entries the codified rule does not reproduce from the fixture, each with its reason. The test
# below checks that each really is not reproduced, so the list stays honest.
NAMED_EXCEPTIONS = {
    ("BREAKOUT_RETEST", "SQUEEZE"):
        "net +1.30 over 8 days on the record (Aaron: about +1 over 7): switched off on 25 Sep for its five "
        "negative days, 'a coin toss that pays fees', before the standing rule was written down on 30 Sep. "
        "S1 needs a net below zero.",
    ("LIQUIDITY_SWEEP_REVERSAL", "HIGH_VOL"):
        "net +36.53 over 9 days on the record: switched off on 30 Sep because 'the last five [days were] in a "
        "row' negative - a streak, not a net below zero (and the 29 Sep file is missing).",
    ("SESSION_EXPANSION", "SQUEEZE"):
        "the files for 29 Sep and 2 Oct are missing on the reports branch, and those were two of its three "
        "negative days (25, 29 Sep, 2 Oct): the fixture has 4 days, net +3.36.",
    ("BREAKOUT_ACCEPTANCE", "HIGH_VOL"):
        "Aaron's 6 Oct count used 17, 30 Sep and 1 Oct, before the 5 Oct reset; S1 counts the current epoch "
        "only (epochs.py), so it fires only when the record before the reset is counted (checked below).",
}


def test_s1_reproduces_the_dated_switch_offs_from_25_september(record):
    in_config = set(ScanConfig().disabled_tactic_regimes)
    reproduced = []
    for day, approach, condition in DATED_S1:
        assert (approach, condition) in in_config                     # the entries are really there
        res = {r.target: r for r in RU.s1(record, end_of(day))}.get(f"{approach}|{condition}")
        fired = res is not None and res.fires
        if (approach, condition) in NAMED_EXCEPTIONS:
            assert not fired, f"{approach} {condition} now reproduces: take it off the exception list"
        else:
            assert fired, (day, approach, condition, res)
            reproduced.append((approach, condition))
    assert reproduced == [("BREAKOUT_RETEST", "TREND"), ("BREAKOUT_ACCEPTANCE", "TREND")]
    # the epoch exception: counted across the reset it fires (3 days, all negative, -17.39)
    res = {r.target: r for r in RU.s1(record, end_of(dt.date(2026, 10, 6)), epochs=())}
    ba = res["BREAKOUT_ACCEPTANCE|HIGH_VOL"]
    assert ba.fires and ba.days == 3 and ba.negative_days == 3 and ba.net_gbp == pytest.approx(-17.39)


def test_s1_leaves_alone_what_aaron_kept(record):
    """9 Oct: 'no standing rule met (session expansion in a trend still +26.66 over 14 days; momentum
    continuation in news +22.77 over 4)'. Formula 1's conditions are never switched off by S1 on the
    record either."""
    res = {r.target: r for r in RU.s1(record, end_of(dt.date(2026, 10, 8)), epochs=())}
    for target in ("SESSION_EXPANSION|TREND", "MOMENTUM_CONTINUATION|NEWS", "MOMENTUM_CONTINUATION|HIGH_VOL"):
        assert not res[target].fires and res[target].net_gbp > 0
    assert res["SESSION_EXPANSION|TREND"].negative_days >= 3      # counting negative days would have fired


def test_s2_reproduces_the_excluded_symbols(record):
    in_config = set(UniverseConfig().excluded_symbols)
    # 4 Oct review of 17 Sep - 2 Oct
    four_oct = {"USDJPY", "DE40", "XAUGBP", "CHFJPY", "EURNZD", "UK100", "GBPJPY", "XAUAUD"}
    fired = {r.target for r in RU.s2(record, end_of(dt.date(2026, 10, 2))) if r.fires}
    assert four_oct <= in_config and four_oct <= fired
    # 8 Oct, the record to 7 Oct
    eight_oct = {"USDCHF", "XAUJPY"}
    fired = {r.target for r in RU.s2(record, end_of(dt.date(2026, 10, 7))) if r.fires}
    assert eight_oct <= in_config and eight_oct <= fired
    usdchf = {r.target: r for r in RU.s2(record, end_of(dt.date(2026, 10, 7)))}["USDCHF"]
    assert (usdchf.days, usdchf.negative_days) == (6, 5)


# ---------------------------------------------------------- made-up records --
def day_trade(ticket, day: dt.date, net: float, *, approach="BREAKOUT_RETEST", condition="TREND", kind="FX_MINOR",
              symbol="EURGBP", real=True, bot="tnb", risk=10.0):
    t = dt.datetime(day.year, day.month, day.day, 10, tzinfo=UTC)
    return R.Trade("journal", bot, ticket, approach, kind, condition, symbol, "BUY", t, t + dt.timedelta(minutes=30),
                   real, net, 0.6, risk)


DAYS = [dt.date(2026, 9, 14) + dt.timedelta(days=i) for i in range(5)] + \
       [dt.date(2026, 9, 21) + dt.timedelta(days=i) for i in range(5)]           # 10 UK weekdays
AFTER = end_of(DAYS[-1])


def test_s1_needs_three_days_and_a_net_below_zero():
    two = [day_trade(1, DAYS[0], -5.0), day_trade(2, DAYS[1], -5.0)]
    assert not RU.s1(two, AFTER, epochs=())[0].fires                       # S3: two days is not enough
    three = two + [day_trade(3, DAYS[2], 2.0)]
    r = RU.s1(three, AFTER, epochs=())[0]
    assert r.fires and r.days == 3 and r.negative_days == 2 and r.net_gbp == -8.0
    even = two + [day_trade(3, DAYS[2], 10.0)]
    assert not RU.s1(even, AFTER, epochs=())[0].fires                      # net 0: not below zero
    assert "£" in r.words() and "GBP" not in r.words()


def test_s1_is_per_approach_and_condition_across_kinds():
    rows = [day_trade(1, DAYS[0], -5.0, kind="FX_MINOR"), day_trade(2, DAYS[1], -5.0, kind="INDEX", symbol="US30"),
            day_trade(3, DAYS[2], -5.0, kind="GOLD", symbol="XAUUSD"),
            day_trade(4, DAYS[0], -5.0, condition="HIGH_VOL"), day_trade(5, DAYS[1], -5.0, approach="OTHER")]
    res = {r.target: r for r in RU.s1(rows, AFTER, epochs=())}
    assert res["BREAKOUT_RETEST|TREND"].fires and res["BREAKOUT_RETEST|TREND"].days == 3
    assert not res["BREAKOUT_RETEST|HIGH_VOL"].fires and not res["OTHER|TREND"].fires


def test_s1_counts_the_current_epoch_only():
    rows = [day_trade(i, d, -5.0) for i, d in enumerate(DAYS[:5])] + [day_trade(10, DAYS[6], -5.0)]
    epoch = (E.Epoch(dt.datetime.combine(DAYS[5], dt.time(0), tzinfo=UTC), "made-up", ("tnb|*|*|*",)),)
    assert RU.s1(rows, AFTER, epochs=())[0].fires
    r = RU.s1(rows, AFTER, epochs=epoch)[0]
    assert not r.fires                                                     # one day in the epoch
    rows += [day_trade(11, DAYS[7], -5.0), day_trade(12, DAYS[8], -5.0)]
    r = RU.s1(rows, AFTER, epochs=epoch)[0]
    assert r.fires and r.days == 3 and r.source == "real"
    # an epoch dated after the evening judged is not yet in force
    assert RU.s1(rows, end_of(DAYS[4]), epochs=epoch)[0].fires


def test_s1_uses_real_trades_once_it_has_three_real_days_else_practice_less_the_haircut():
    practice = [day_trade(i, d, 0.5, real=False) for i, d in enumerate(DAYS[:4])]      # +0.05 R a day
    r = RU.s1(practice, AFTER, epochs=())[0]
    assert r.source == "practice" and r.fires                              # 0.05 R less the 0.10 R allowance
    assert r.net_gbp == pytest.approx(4 * (0.5 - 1.0))
    real = [day_trade(100 + i, d, 1.0) for i, d in enumerate(DAYS[5:8])]
    r = RU.s1(practice + real, AFTER, epochs=())[0]
    assert r.source == "real" and not r.fires and r.days == 3
    measured = RU.s1(practice, AFTER, epochs=(), haircuts={"tnb|BREAKOUT_RETEST|FX_MINOR|TREND": 0.05})[0]
    assert not measured.fires                                              # 0.05 R less 0.05 R: zero


def test_s2_a_market_negative_on_two_thirds_of_six_or_more_days():
    four_of_six = [day_trade(i, d, -3.0 if i < 4 else 2.0, symbol="NZDCHF") for i, d in enumerate(DAYS[:6])]
    r = RU.s2(four_of_six, AFTER)[0]
    assert r.fires and (r.days, r.negative_days, r.target) == (6, 4, "NZDCHF")
    three_of_six = [day_trade(i, d, -3.0 if i < 3 else 2.0, symbol="NZDCHF") for i, d in enumerate(DAYS[:6])]
    assert not RU.s2(three_of_six, AFTER)[0].fires
    five_of_five = [day_trade(i, d, -3.0, symbol="NZDCHF") for i, d in enumerate(DAYS[:5])]
    assert not RU.s2(five_of_five, AFTER)[0].fires                         # fewer than 6 days
    # per bot: another bot's days are its own
    mixed = four_of_six[:3] + [day_trade(50 + i, d, -3.0, symbol="NZDCHF", bot="crowd") for i, d in enumerate(DAYS[3:6])]
    assert not any(r.fires for r in RU.s2(mixed, AFTER))


def test_s3_no_judgement_on_fewer_than_three_uk_days():
    assert not RU.judgement_allowed([day_trade(1, DAYS[0], -9.0), day_trade(2, DAYS[1], -9.0),
                                     day_trade(3, DAYS[1], -9.0)])
    assert RU.judgement_allowed([day_trade(i, d, -1.0) for i, d in enumerate(DAYS[:3])])


def test_the_rules_never_see_trades_after_the_evening_judged():
    rows = [day_trade(i, d, -5.0) for i, d in enumerate(DAYS[:3])]
    assert not RU.s1(rows, end_of(DAYS[1]), epochs=())[0].fires
    assert RU.s1(rows, end_of(DAYS[2]), epochs=())[0].fires
