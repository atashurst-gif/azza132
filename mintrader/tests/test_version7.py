"""Version 7 (16 Oct): CADJPY, EURCAD, EURUSD, GBPAUD and GBPCAD off, by the
standing market rule (negative on two-thirds or more of 6+ traded days), as
the coach's S2 reads it on the Mac's whole Trend & Breakout record. Every
version 6 rule is kept unchanged."""
import datetime as dt

from mintel.config import Config, STRATEGY_NAME, STRATEGY_VERSION

V6_EXCLUDED = ("USDJPY", "DE40", "XAUGBP", "CHFJPY", "EURNZD", "UK100", "GBPJPY", "XAUAUD", "USDCHF", "XAUJPY")
V7_OFF = ("CADJPY", "EURCAD", "EURUSD", "GBPAUD", "GBPCAD")
V6_GROUPS = (("MOMENTUM_CONTINUATION", "GOLD"), ("MOMENTUM_CONTINUATION", "SILVER"),
             ("MOMENTUM_CONTINUATION", "METAL"), ("MOMENTUM_CONTINUATION", "FX_MAJOR"),
             ("MOMENTUM_CONTINUATION", "INDEX"))
V6_REGIMES = (("MOMENTUM_CONTINUATION", "TREND"), ("BREAKOUT_RETEST", "HIGH_VOL"),
              ("BREAKOUT_RETEST", "SQUEEZE"), ("BREAKOUT_RETEST", "TREND"),
              ("BREAKOUT_ACCEPTANCE", "TREND"), ("LIQUIDITY_SWEEP_REVERSAL", "HIGH_VOL"),
              ("BREAKOUT_ACCEPTANCE", "HIGH_VOL"), ("SESSION_EXPANSION", "SQUEEZE"))
V6_TACTICS = ("NEWS_CONTINUATION", "TREND_PULLBACK", "FAILED_BREAKOUT_RECLAIM")


def test_version_7_name_and_the_clock_is_unchanged():
    assert STRATEGY_NAME == "Commission First (version 7)"
    assert STRATEGY_VERSION.startswith("2026-10-05 complete reset to zero")


def test_version_7_adds_five_markets_off_and_keeps_every_version_6_rule():
    c = Config()
    assert tuple(c.universe.excluded_symbols) == V6_EXCLUDED + V7_OFF
    assert tuple(c.scan.disabled_tactic_groups) == V6_GROUPS
    assert tuple(c.scan.disabled_tactic_regimes) == V6_REGIMES
    assert tuple(c.scan.disabled_tactics) == V6_TACTICS
    assert c.scan.tactic_min_score == {"MOMENTUM_CONTINUATION": 70.0}
    assert [tuple(x) for x in c.risk.top_segments] == [("MOMENTUM_CONTINUATION", "FX_MINOR")]


def test_the_scanner_leaves_the_five_out_and_keeps_the_rest():
    from mintel.broker.sim import SimBroker
    from mintel.engine.scanner import discover
    cfg = Config()
    cfg.universe.max_symbols = 60
    syms = V7_OFF + ("GBPUSD", "AUDJPY", "GBPNZD", "EURAUD", "US30", "XAUUSD")
    broker = SimBroker(syms, start=dt.datetime(2026, 10, 12, 9, tzinfo=dt.timezone.utc) - dt.timedelta(days=5))
    broker.connect()
    uni = discover(broker, cfg)
    for sym in V7_OFF:
        assert sym not in uni.symbols and "evidence" in uni.rejected.get(sym, ""), sym
    for sym in ("GBPUSD", "AUDJPY", "GBPNZD", "EURAUD", "US30", "XAUUSD"):
        assert sym in uni.symbols, sym


def test_the_coach_market_rule_is_the_ledgers_rule():
    # the count version 7 rests on: the coach's S2, negative on two-thirds or more of 6 or more traded days
    from mintel.coach import rules as RU
    assert RU.S2_MIN_DAYS == 6 and abs(RU.S2_SHARE - 2 / 3) < 1e-9
