"""Financial Ian's research tools run end to end on synthetic scenarios and label their output
SYNTHETIC - NOT PERFORMANCE (they prove the pipeline, not an edge)."""
import pytest

from mintel.ian import SYNTHETIC_LABEL
from mintel.ian.feeds.synthetic import SyntheticFeed, generate
from mintel.ian.research import benchmark, featuretest
from mintel.ian.research.harness import sample


@pytest.fixture(scope="module")
def sets():
    out = []
    for i, sc in enumerate(("balanced", "sweep_up", "absorption_buyers", "news_shock")):
        evs, _ = generate(sc, seed=7 + i)
        cal = SyntheticFeed(sc, seed=7 + i).calendar_events() if sc == "news_shock" else []
        out.append((sc, sample(evs, every_s=1.0, calendar=cal), cal))
    return out


def test_featuretest_measures_every_feature_by_horizon_and_regime(sets):
    samples = [s for _, ss, _ in sets for s in ss]
    res = featuretest.evaluate(samples, horizons=(5, 30), data_source="SYNTHETIC")
    assert res["label"] == SYNTHETIC_LABEL and "overlap" in res["note"]
    assert set(res["features"]) == set(featuretest.FEATURES)
    for name, r in res["features"].items():
        assert set(r["horizons"]) == {"5", "30"}
        assert r["verdict"].startswith("EDGE") or r["verdict"] == "NO EVIDENCE after costs"
        m = r["horizons"]["5"]
        assert m["samples"] > 500 and m["active"] >= 0
        if m["active"]:
            assert 0.0 <= m["hit_rate"] <= 1.0 and m["net_ticks"] == pytest.approx(m["edge_ticks"] - 1.5)
    assert res["features"]["sweep"]["horizons"]["5"]["by_regime"]          # broken down by regime
    assert "FEATURE TEST - " + SYNTHETIC_LABEL in featuretest.text(res)


def test_forward_moves_are_measured_in_ticks(sets):
    _, ss, _ = sets[1]
    fwd = featuretest.forward_moves(ss, 5, 1.0)
    i = next(k for k, s in enumerate(ss) if s.fs.ok and fwd[k] is not None)
    assert fwd[i] == pytest.approx((ss[i + 5].fs.mid - ss[i].fs.mid) / ss[i].fs.tick)
    assert fwd[-1] is None


def test_benchmark_runs_all_six_models_on_the_same_data(sets):
    res = benchmark.run(sets, "SYNTHETIC")
    assert res["label"] == SYNTHETIC_LABEL and res["unit"] == "ticks of the future"
    assert list(res["models"]) == list(benchmark.MODELS)
    for m, st in res["models"].items():
        assert set(st["trades_by_session"]) == {"balanced", "sweep_up", "absorption_buyers", "news_shock"}
        for k in ("trades", "per_day", "win_rate", "net_ticks", "expectancy_ticks", "profit_factor", "avg_win",
                  "avg_loss", "max_drawdown_ticks", "mfe_r", "mae_r", "mfe_capture"):
            assert k in st
        assert st["max_drawdown_ticks"] <= 0
    # the order-flow models never trade the balanced book
    assert res["models"]["full"]["trades_by_session"]["balanced"] == 0
    assert res["models"]["flow_only"]["trades_by_session"]["balanced"] == 0
    assert "BENCHMARK - " + SYNTHETIC_LABEL in benchmark.text(res)


def test_trades_exit_at_the_stop_never_better(sets):
    _, ss, _ = sets[1]
    for t in benchmark.simulate(ss, "price_only") + benchmark.simulate(ss, "full"):
        if t["reason"] == "STOP":
            assert t["r"] >= -1.0 - 1e-9            # a stop is a loss of at most 1 R (6 ticks) ...
        assert t["mae_r"] <= 1e-9 and t["mfe_r"] >= -1e-9


def test_the_command_lines(capsys):
    assert featuretest.main(["--synthetic", "balanced,sweep_up"]) == 0
    out = capsys.readouterr().out
    assert SYNTHETIC_LABEL in out and "verdict" in out
    assert benchmark.main(["--synthetic", "sweep_up"]) == 0
    assert SYNTHETIC_LABEL in capsys.readouterr().out
