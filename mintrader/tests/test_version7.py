"""Version 7 (16 Oct): CADJPY, EURCAD, EURUSD, GBPAUD and GBPCAD off on the
coach's S2 count of every trade in the market. Superseded on 17 Oct by
version 8 before it ever ran on the Mac (Aaron: "Keep the approaches off and
continue with how we should be if they weren't there", read as: the market
rule counts only the trades today's rules still allow): those five are back
on there (tests/test_version8.py). What version 7 kept still holds."""
from mintel.config import Config, STRATEGY_NAME, STRATEGY_VERSION

V6_GROUPS = (("MOMENTUM_CONTINUATION", "GOLD"), ("MOMENTUM_CONTINUATION", "SILVER"),
             ("MOMENTUM_CONTINUATION", "METAL"), ("MOMENTUM_CONTINUATION", "FX_MAJOR"),
             ("MOMENTUM_CONTINUATION", "INDEX"))
V6_REGIMES = (("MOMENTUM_CONTINUATION", "TREND"), ("BREAKOUT_RETEST", "HIGH_VOL"),
              ("BREAKOUT_RETEST", "SQUEEZE"), ("BREAKOUT_RETEST", "TREND"),
              ("BREAKOUT_ACCEPTANCE", "TREND"), ("LIQUIDITY_SWEEP_REVERSAL", "HIGH_VOL"),
              ("BREAKOUT_ACCEPTANCE", "HIGH_VOL"), ("SESSION_EXPANSION", "SQUEEZE"))
V6_TACTICS = ("NEWS_CONTINUATION", "TREND_PULLBACK", "FAILED_BREAKOUT_RECLAIM")


def test_version_7_name_and_the_clock_is_unchanged():
    assert STRATEGY_NAME in ("Commission First (version 7)", "Commission First (version 8)")
    assert STRATEGY_VERSION.startswith("2026-10-05 complete reset to zero")


def test_every_version_6_approach_rule_is_still_kept():
    c = Config()
    assert tuple(c.scan.disabled_tactic_groups) == V6_GROUPS
    assert tuple(c.scan.disabled_tactic_regimes) == V6_REGIMES
    assert tuple(c.scan.disabled_tactics) == V6_TACTICS
    assert c.scan.tactic_min_score == {"MOMENTUM_CONTINUATION": 70.0}
    assert [tuple(x) for x in c.risk.top_segments] == [("MOMENTUM_CONTINUATION", "FX_MINOR")]


def test_the_coach_market_rule_is_the_ledgers_rule():
    # the rule both versions rest on: the coach's S2, negative on two-thirds or more of 6 or more traded days
    from mintel.coach import rules as RU
    assert RU.S2_MIN_DAYS == 6 and abs(RU.S2_SHARE - 2 / 3) < 1e-9
