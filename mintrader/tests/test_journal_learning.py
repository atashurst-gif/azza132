"""Journal, calibration, historical learning and research validation."""
from __future__ import annotations

import datetime as dt
import json
import math
import random

import pytest

from mintel.broker.base import Position, Side
from mintel.data.regime import Regime
from mintel.engine.evidence import Evidence, Family, MarketState
from mintel.engine.execution import Intent, IntentState
from mintel.engine.flowlock import FlowLock
from mintel.engine.journal import Journal
from mintel.engine.learning import (HistoricalMatcher, MetaLabeller,
                                    label_from_trade)
from mintel.engine.opportunity import Calibrator, score, tier_for
from mintel.config import ScanConfig
from mintel.research import validation as V
from mintel.research.trial_ledger import Trial, TrialLedger

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 3, 10, 13, 0, tzinfo=UTC)


def ms(symbol="EURUSD", opportunity=75.0, tactic="MOMENTUM_CONTINUATION",
       regime=Regime.TREND, direction=1) -> MarketState:
    return MarketState(
        symbol=symbol, as_of=T0, regime=regime, regime_label="TREND / NORMAL",
        direction=direction, tactic=tactic,
        evidence={Family.STRUCTURE: Evidence(Family.STRUCTURE, 80.0, 1.0, "x"),
                  Family.MOMENTUM: Evidence(Family.MOMENTUM, 70.0),
                  Family.SESSION: Evidence(Family.SESSION, 80.0, 0.8,
                                           "session: LONDON"),
                  Family.EXECUTION: Evidence(Family.EXECUTION, 85.0, 1.0,
                                             "spread 1.2 pips (1.0x normal)"),
                  Family.NEWS: Evidence(Family.NEWS, 50.0, 0.4, "no release")},
        raw_score=opportunity, opportunity=opportunity, tier="STRONG",
        entry=1.1000, stop=1.0980, target=1.1040, reward_risk=2.0,
        headroom_pips=40.0, cost_pips=1.2)


def position(ticket=1, symbol="EURUSD") -> Position:
    return Position(ticket, symbol, Side.BUY, 0.5, 1.1000, 1.0980, 1.1040, T0)


class TestJournalRoundTrip:
    def test_full_trade_lifecycle_is_recorded(self, journal):
        p, state = position(), ms()
        journal.open_trade(p, state, "key1", 0.75, 75.0, 10_000.0, "GBP",
                           "NORMAL", "DEMO")
        assert len(journal.open_trades()) == 1

        fl = FlowLock()
        t = fl.adopt(p, 1.0980)
        t.mfe, t.mfe_r = 0.0040, 2.0
        stats = fl.capture_stats(t, 1.1030)
        journal.close_trade(1, exit_price=1.1030, closed_utc=T0,
                            pnl_money=150.0, pnl_pips=30.0,
                            exit_reason="FlowLock decay", stats=stats,
                            tracker=t, exit_state=state,
                            learned="Kept most of the move.")
        closed = journal.closed_trades()
        assert len(closed) == 1
        row = closed[0]
        assert row["realised_r"] == pytest.approx(1.5)
        assert row["mfe_r"] == pytest.approx(2.0)
        assert row["exit_reason"] == "FlowLock decay"
        assert row["learned"]
        assert json.loads(row["entry_state_json"])["symbol"] == "EURUSD"
        assert json.loads(row["flow_json"])["state"]

    def test_decision_state_is_reproducible(self, journal):
        journal.record_decision(ms(), acted=True)
        rows = journal._exec("SELECT * FROM decisions").fetchall()
        stored = json.loads(rows[0]["state_json"])
        assert stored["evidence"]["STRUCTURE"] == pytest.approx(80.0)
        assert stored["evidence_confidence"]["SESSION"] == pytest.approx(0.8)

    def test_session_and_spread_are_extracted(self, journal):
        journal.open_trade(position(), ms(), "k", 0.5, 50.0, 10_000, "GBP",
                           "NORMAL", "DEMO")
        row = journal.open_trades()[0]
        assert "LONDON" in (row["session"] or "")
        assert row["spread_pips"] == pytest.approx(1.2)

    def test_intents_survive_and_are_queryable(self, journal):
        it = Intent("k", "EURUSD", Side.BUY, 0.1, 1.09, 0.0, T0,
                    IntentState.UNKNOWN)
        journal.save_intent(it)
        assert journal.find_intent("k").state is IntentState.UNKNOWN
        assert [x.key for x in journal.unresolved_intents()] == ["k"]
        it.state = IntentState.FILLED
        journal.save_intent(it)
        assert journal.unresolved_intents() == []

    def test_match_statistics_at_three_widths(self, journal):
        for i in range(8):
            p = position(ticket=i + 1)
            journal.open_trade(p, ms(), f"k{i}", 0.5, 50.0, 10_000, "GBP",
                               "NORMAL", "DEMO")
            journal.close_trade(i + 1, exit_price=1.1030, closed_utc=T0,
                                pnl_money=100.0, pnl_pips=30.0,
                                exit_reason="TP",
                                stats={"realised_r": 1.2, "mfe_r": 1.5,
                                       "mae_r": 0.2, "mfe_capture_pct": 80.0})
        stats = journal.match_stats("EURUSD", "TREND",
                                     "MOMENTUM_CONTINUATION", Side.BUY)
        assert stats["exact"]["n"] == 8
        assert stats["exact"]["expectancy_r"] == pytest.approx(1.2)
        assert stats["tactic"]["n"] == 8

    def test_kelly_statistics_need_both_wins_and_losses(self, journal):
        assert journal.kelly_stats("MOMENTUM_CONTINUATION", "TREND") is None
        for i in range(20):
            p = position(ticket=i + 1)
            journal.open_trade(p, ms(), f"k{i}", 0.5, 50.0, 10_000, "GBP",
                               "NORMAL", "DEMO")
            r = 1.5 if i % 2 else -1.0
            journal.close_trade(i + 1, exit_price=1.1030, closed_utc=T0,
                                pnl_money=r * 100, pnl_pips=30.0,
                                exit_reason="x",
                                stats={"realised_r": r, "mfe_r": max(r, 0),
                                       "mae_r": 0.5, "mfe_capture_pct": 50.0})
        n, win_rate, payoff = journal.kelly_stats("MOMENTUM_CONTINUATION",
                                                   "TREND")
        assert n == 20
        assert win_rate == pytest.approx(0.5)
        assert payoff == pytest.approx(1.5)

    def test_equity_curve_and_events(self, journal):
        journal.record_equity(10_000, 10_050, 1, 0.5)
        assert len(journal.equity_curve()) == 1
        journal.log_event("TEST", "hello", "ERROR")
        journal.log_event("TEST", "quiet", "INFO")
        assert len(journal.recent_events(min_severity="WARNING")) == 1

    def test_decision_pruning(self, journal):
        journal.record_decision(ms(), acted=False)
        journal.prune_decisions(keep_days=0)
        assert journal._exec("SELECT COUNT(*) FROM decisions").fetchone()[0] == 0


class TestCalibration:
    def test_uncalibrated_scores_are_treated_as_opinions(self):
        c = Calibrator()
        assert c.reliability(90.0) == 0.0
        assert c.calibrated_confidence(90.0) < 0.9

    def test_observed_performance_takes_over_with_evidence(self):
        good = Calibrator(min_samples=20)
        for _ in range(40):
            good.observe(90.0, 1.2)
        bad = Calibrator(min_samples=20)
        for _ in range(40):
            bad.observe(90.0, -0.6)
        assert good.calibrated_confidence(90.0) > bad.calibrated_confidence(90.0)
        assert bad.calibrated_confidence(90.0) < 0.35

    def test_reliability_grows_with_samples(self):
        c = Calibrator(min_samples=20)
        for i in range(1, 21):
            c.observe(75.0, 0.5)
            assert c.reliability(75.0) == pytest.approx(min(1.0, i / 20))

    def test_monotonic_violation_is_reported_honestly(self):
        c = Calibrator(min_samples=5)
        for _ in range(10):
            c.observe(60.0, 1.0)       # middle band does well
            c.observe(90.0, -0.5)      # top band does badly
        assert c.monotonic_violation() is not None

    def test_no_violation_when_ordering_is_sensible(self):
        c = Calibrator(min_samples=5)
        for _ in range(10):
            c.observe(60.0, 0.1)
            c.observe(90.0, 0.9)
        assert c.monotonic_violation() is None


class TestScoring:
    def test_reality_checks_only_reduce_except_generous_headroom(self):
        from mintel.data.headroom import HeadroomReading, Obstacle
        base = score(80.0, 80.0, None, None, 0.5, 100.0, 100.0)
        tight = HeadroomReading(1, 1.1, 0.001,
                                Obstacle(1.1002, "PDH", 0.85, 0.0002, 0.2, 2.0),
                                0.24)
        with_wall = score(80.0, 80.0, tight, None, 0.5, 100.0, 100.0)
        assert with_wall.final < base.final
        assert with_wall.notes

    def test_bad_execution_reduces_the_score(self):
        good = score(80.0, 80.0, None, None, 0.5, 100.0, 100.0)
        bad = score(80.0, 80.0, None, None, 0.5, 10.0, 100.0)
        assert bad.final < good.final

    def test_quiet_session_reduces_the_score(self):
        loud = score(80.0, 80.0, None, None, 0.5, 100.0, 100.0)
        quiet = score(80.0, 80.0, None, None, 0.5, 100.0, 30.0)
        assert quiet.final < loud.final

    def test_tiers_are_ordered(self):
        cfg = ScanConfig()
        assert tier_for(cfg.tier_exceptional + 1, cfg) == "EXCEPTIONAL"
        assert tier_for(cfg.tier_strong + 1, cfg) == "STRONG"
        assert tier_for(cfg.tier_normal + 1, cfg) == "NORMAL"
        assert tier_for(cfg.tier_normal - 1, cfg) == "NO_TRADE"


class TestHistoricalMatcher:
    def test_no_history_means_no_influence_not_no_permission(self):
        class Empty:
            def match_stats(self, *a):
                return {"exact": {"n": 0}, "tactic_regime": {"n": 0},
                        "tactic": {"n": 0}}

        r = HistoricalMatcher(Empty()).evaluate("NEWSYM", "TREND", "X",
                                                Side.BUY)
        assert r.score == 50.0 and r.confidence == 0.0
        assert "does not block" in r.note

    def test_tightest_sufficient_match_is_preferred(self):
        class J:
            def match_stats(self, *a):
                return {"exact": {"n": 30, "expectancy_r": 0.8,
                                   "win_rate": 0.6},
                        "tactic_regime": {"n": 200, "expectancy_r": -0.5},
                        "tactic": {"n": 500, "expectancy_r": 0.0}}

        r = HistoricalMatcher(J()).evaluate("EURUSD", "TREND", "X", Side.BUY)
        assert r.width == "exact"
        assert r.score > 60

    def test_broader_match_used_when_the_exact_one_is_empty(self):
        class J:
            def match_stats(self, *a):
                return {"exact": {"n": 0},
                        "tactic_regime": {"n": 40, "expectancy_r": 0.3},
                        "tactic": {"n": 400, "expectancy_r": 0.9}}

        r = HistoricalMatcher(J()).evaluate("EURUSD", "TREND", "X", Side.BUY)
        assert r.width == "tactic_regime"
        assert r.confidence < 1.0

    def test_bad_history_lowers_the_score(self):
        class J:
            def match_stats(self, *a):
                return {"exact": {"n": 50, "expectancy_r": -0.8},
                        "tactic_regime": {"n": 0}, "tactic": {"n": 0}}

        r = HistoricalMatcher(J()).evaluate("EURUSD", "TREND", "X", Side.BUY)
        assert r.score < 30

    def test_one_outlier_cannot_produce_a_perfect_score(self):
        class J:
            def match_stats(self, *a):
                return {"exact": {"n": 6, "expectancy_r": 25.0},
                        "tactic_regime": {"n": 0}, "tactic": {"n": 0}}

        r = HistoricalMatcher(J()).evaluate("EURUSD", "TREND", "X", Side.BUY)
        assert r.score <= 90.0


class TestMetaLabeller:
    def test_refuses_to_train_on_a_small_sample(self):
        m = MetaLabeller()
        out = m.train([{}] * 50, [0] * 50)
        assert not out["trained"] and "need" in out["reason"]

    def test_refuses_to_train_on_one_class(self):
        from mintel.engine.learning import FEATURES
        rows = [{f: 1.0 for f in FEATURES} for _ in range(300)]
        out = MetaLabeller().train(rows, [1] * 300)
        assert not out["trained"]

    def test_trains_and_predicts_calibrated_probabilities(self, tmp_path):
        from mintel.engine.learning import FEATURES
        rng = random.Random(0)
        rows, labels = [], []
        for _ in range(700):
            r = {f: rng.uniform(0, 100) for f in FEATURES}
            good = r["structure"] + r["momentum"] > 110
            labels.append(1 if (good) == (rng.random() < 0.85) else 0)
            rows.append(r)
        m = MetaLabeller(tmp_path / "m.pkl")
        out = m.train(rows, labels)
        assert out["trained"]
        p = m.predict(rows[0])
        assert p is not None and 0.0 <= p <= 1.0
        assert "calibration_error" in out

    def test_model_evidence_confidence_is_capped(self, tmp_path):
        from mintel.engine.learning import FEATURES
        rng = random.Random(1)
        rows = [{f: rng.uniform(0, 100) for f in FEATURES} for _ in range(400)]
        labels = [1 if r["structure"] > 50 else 0 for r in rows]
        m = MetaLabeller(tmp_path / "m2.pkl")
        m.train(rows, labels)
        got = m.evidence_score(rows[0])
        assert got is not None
        _score, conf, note = got
        assert conf <= 0.75, "a model must never dominate the aggregate"
        assert "model probability" in note

    def test_unavailable_model_returns_none(self):
        assert MetaLabeller().predict({}) is None
        assert MetaLabeller().evidence_score({}) is None

    def test_labels_use_favourable_excursion(self):
        assert label_from_trade({"mfe_r": 1.4}) == 1
        assert label_from_trade({"mfe_r": 0.4}) == 0

    def test_feature_vector_contains_no_outcome_information(self):
        from mintel.engine.learning import FEATURES
        banned = ("pnl", "realised", "exit", "result", "mfe", "mae", "won")
        for f in FEATURES:
            assert not any(b in f for b in banned), f


class TestTrialLedger:
    def test_every_trial_is_counted(self, tmp_path):
        led = TrialLedger(tmp_path / "t.sqlite")
        for i in range(25):
            led.record(Trial("study", {"p": i}, sharpe=i * 0.1, trades=100))
        assert led.count("study") == 25
        report = led.honest_report("study")
        assert report["trials_run"] == 25
        assert "best of 25" in report["warning"]
        led.close()

    def test_missing_out_of_sample_is_called_out(self, tmp_path):
        led = TrialLedger(tmp_path / "t2.sqlite")
        led.record(Trial("s", {"a": 1}, sharpe=2.0))
        assert "warning_oos" in led.honest_report("s")
        led.close()

    def test_out_of_sample_trials_are_tracked(self, tmp_path):
        led = TrialLedger(tmp_path / "t3.sqlite")
        led.record(Trial("s", {"a": 1}, sharpe=2.0, is_oos=True))
        r = led.honest_report("s")
        assert r["oos_trials"] == 1
        assert "warning_oos" not in r
        led.close()

    def test_parameter_hash_is_stable(self):
        a = Trial("s", {"x": 1, "y": 2})
        b = Trial("s", {"y": 2, "x": 1})
        assert a.param_hash == b.param_hash


class TestValidationStatistics:
    def test_walk_forward_folds_are_in_time_order(self):
        splits = V.walk_forward_splits(1000, folds=5, embargo=10)
        assert splits
        for s in splits:
            assert s.train[1] <= s.test[0], "training must precede testing"

    def test_purged_folds_remove_overlapping_labels(self):
        folds = V.purged_kfold_indices(500, k=5, embargo_pct=0.05,
                                        label_span=10)
        for train, test in folds:
            assert not (set(train) & set(test))
            gap = min(abs(t - x) for t in test for x in (min(train), max(train)))
            assert gap >= 0

    def test_cpcv_produces_many_paths(self):
        paths = V.cpcv_splits(600, groups=6, test_groups=2)
        assert len(paths) == 15
        for train, test in paths:
            assert not (set(train) & set(test))

    def test_deflated_sharpe_punishes_many_trials(self):
        rng = random.Random(2)
        rets = [rng.gauss(0.001, 0.01) for _ in range(400)]
        one = V.deflated_sharpe_ratio(rets, 1)
        many = V.deflated_sharpe_ratio(rets, 500)
        assert many["dsr"] < one["dsr"]
        assert many["expected_max_sr_under_null"] > one["expected_max_sr_under_null"]

    def test_probability_of_backtest_overfitting_detects_noise_fitting(self):
        rng = random.Random(3)
        noise = [[rng.gauss(0, 1) for _ in range(10)] for _ in range(20)]
        out = V.probability_of_backtest_overfitting(noise)
        assert out["pbo"] is not None and out["pbo"] > 0.3

    def test_pbo_is_low_when_one_config_is_genuinely_better(self):
        rng = random.Random(4)
        paths = [[rng.gauss(0, 0.2) for _ in range(9)] + [rng.gauss(3, 0.2)]
                 for _ in range(20)]
        out = V.probability_of_backtest_overfitting(paths)
        assert out["pbo"] < 0.2

    def test_monte_carlo_separates_path_risk_from_outcome_risk(self):
        rng = random.Random(5)
        rets = [rng.gauss(0.004, 0.02) for _ in range(150)]
        out = V.monte_carlo_sequences(rets, iterations=300)
        assert out["permutation"]["p95_max_drawdown_pct"] >= \
            out["permutation"]["median_max_drawdown_pct"]
        assert out["bootstrap"]["p05_final_equity"] < \
            out["bootstrap"]["p95_final_equity"]
        assert 0.0 <= out["bootstrap"]["probability_of_losing_money"] <= 1.0

    def test_cost_stress_degrades_results(self):
        trades = [{"pnl_money": 10.0, "cost_money": 4.0}] * 40
        out = V.cost_stress(trades)
        nets = [row["net"] for row in out]
        assert nets == sorted(nets, reverse=True)

    def test_parameter_stability_distinguishes_plateau_from_spike(self):
        plateau = V.parameter_stability({1: 0.80, 2: 0.90, 3: 0.88, 4: 0.70})
        spike = V.parameter_stability({1: 0.10, 2: 0.90, 3: 0.12, 4: 0.10})
        assert plateau["stable"] is True
        assert spike["stable"] is False
        assert "SPIKE" in spike["verdict"]

    def test_sharpe_and_sortino_agree_in_sign(self):
        rng = random.Random(6)
        good = [rng.gauss(0.002, 0.01) for _ in range(300)]
        assert V.sharpe(good) > 0 and V.sortino(good) > 0

    def test_probabilistic_sharpe_is_a_probability(self):
        rng = random.Random(7)
        rets = [rng.gauss(0.001, 0.01) for _ in range(200)]
        p = V.probabilistic_sharpe_ratio(rets)
        assert 0.0 <= p <= 1.0

    def test_max_drawdown(self):
        assert V.max_drawdown([100, 120, 90, 130]) == pytest.approx(25.0)
