"""Version 6 (15 Oct): momentum continuation off on the indices, by the
standing approach rule per kind of market (as in versions 4 and 5). Every
version 5 rule is kept unchanged."""
from mintel.config import Config, STRATEGY_NAME, STRATEGY_VERSION
from mintel.contracts import infer_group
from mintel.data.regime import Regime
from mintel.engine import tactics as T
from mintel.engine.tactics import blocked_in_group

V5_GROUPS = (("MOMENTUM_CONTINUATION", "GOLD"), ("MOMENTUM_CONTINUATION", "SILVER"),
             ("MOMENTUM_CONTINUATION", "METAL"), ("MOMENTUM_CONTINUATION", "FX_MAJOR"))
V5_REGIMES = (("MOMENTUM_CONTINUATION", "TREND"), ("BREAKOUT_RETEST", "HIGH_VOL"),
              ("BREAKOUT_RETEST", "SQUEEZE"), ("BREAKOUT_RETEST", "TREND"),
              ("BREAKOUT_ACCEPTANCE", "TREND"), ("LIQUIDITY_SWEEP_REVERSAL", "HIGH_VOL"),
              ("BREAKOUT_ACCEPTANCE", "HIGH_VOL"), ("SESSION_EXPANSION", "SQUEEZE"))
V5_TACTICS = ("NEWS_CONTINUATION", "TREND_PULLBACK", "FAILED_BREAKOUT_RECLAIM")


def test_version_6_name_and_the_clock_is_unchanged():
    assert STRATEGY_NAME == "Commission First (version 6)"
    assert STRATEGY_VERSION.startswith("2026-10-05 complete reset to zero")


def test_version_6_adds_one_switch_off_and_keeps_every_version_5_rule():
    scan = Config().scan
    assert tuple(scan.disabled_tactic_groups) == V5_GROUPS + (("MOMENTUM_CONTINUATION", "INDEX"),)
    assert tuple(scan.disabled_tactic_regimes) == V5_REGIMES
    assert tuple(scan.disabled_tactics) == V5_TACTICS
    assert scan.tactic_min_score == {"MOMENTUM_CONTINUATION": 70.0}


def test_momentum_continuation_is_off_on_the_indices_and_still_on_on_the_minor_pairs():
    groups = Config().scan.disabled_tactic_groups
    # every index the scanner can admit (it classifies by name: scanner.discover)
    for sym in ("US30", "US500", "AUS200", "JP225", "HK50", "NAS100"):
        assert infer_group(sym) == "INDEX", sym
        assert blocked_in_group("MOMENTUM_CONTINUATION", infer_group(sym), groups), sym
    for sym in ("EURAUD", "GBPAUD", "AUDNZD", "GBPCAD"):
        assert infer_group(sym) == "FX_MINOR", sym
        assert not blocked_in_group("MOMENTUM_CONTINUATION", infer_group(sym), groups), sym
    # the indices' other approaches stay on (session expansion in a trend: AUS200 +27.06 on 15 Oct)
    assert not blocked_in_group("SESSION_EXPANSION", "INDEX", groups)


def test_the_scanner_takes_no_momentum_signal_on_an_index_but_still_takes_one_on_a_minor_pair(monkeypatch):
    sig = T.TacticSignal(name="MOMENTUM_CONTINUATION", direction=1, quality=80.0,
                         invalidation=1.0, entry_hint=1.1)
    tac = T.Tactic("MOMENTUM_CONTINUATION", (Regime.HIGH_VOL, Regime.NEWS), lambda ctx, side: sig)
    monkeypatch.setattr(T, "candidates_for", lambda regime: (tac,))
    scan = Config().scan
    kw = dict(disabled=scan.disabled_tactics, disabled_in=scan.disabled_tactic_regimes,
              disabled_in_group=scan.disabled_tactic_groups)
    for regime in (Regime.HIGH_VOL, Regime.NEWS):
        assert T.best_signal(None, 1, regime, group="INDEX", **kw) is None
        assert T.best_signal(None, 1, regime, group="FX_MINOR", **kw) is sig


def test_the_runner_and_the_top_size_are_untouched():
    from mintel.runner import feed
    c = Config()
    # the Runner has not ridden momentum continuation since version 5, so it loses nothing here:
    # even listed for the runner feed, it is never offered (session expansion still is)
    assert "MOMENTUM_CONTINUATION" in {str(t).upper() for t in c.runner.skip_tactics}
    c.runner.feed_tactics = ("MOMENTUM_CONTINUATION", "SESSION_EXPANSION")
    for regime in ("HIGH_VOL", "NEWS"):
        assert "MOMENTUM_CONTINUATION" not in feed.names_for(c, regime)
    assert "SESSION_EXPANSION" in feed.names_for(c, "HIGH_VOL")
    # the top-opportunity size stays on momentum continuation on the minor pairs only
    assert [tuple(x) for x in c.risk.top_segments] == [("MOMENTUM_CONTINUATION", "FX_MINOR")]
