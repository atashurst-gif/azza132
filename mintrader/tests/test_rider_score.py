"""Rapid Momentum Rider: features, score, entry states, currency strength,
news, learning and settings - on SYNTHETIC seeded tick paths (mintel/rider/
synth.py). These show the machinery behaves as designed; they are not
results and say nothing about real-market performance."""
import datetime as dt
import json

import pytest

from mintel.broker.base import CalendarEvent, Side, Tick
from mintel.rider import MAGIC, STRATEGY_ID, STRATEGY_LABEL, synth
from mintel.rider.config import RiderConfig
from mintel.rider.core import RiderCore
from mintel.rider.features import ORDER_FLOW_AVAILABLE, efficiency, zigzag
from mintel.rider.learning import BucketStats, Outcome
from mintel.rider.score import DEFAULT_WEIGHTS, TRIGGER, weights_from
from mintel.rider.strength import PairMove, compute_strength

UTC = dt.timezone.utc


def replay(core, ticks_by_symbol, start_scan, end=None, every=1.0, on_scan=None):
    """Feed ticks in time order and scan once a second of market time - the
    same cadence the live engine uses. Returns [(seconds, rows)]."""
    allt = sorted(((t.time, s, t) for s, ts in ticks_by_symbol.items() for t in ts), key=lambda x: (x[0], x[1]))
    end = end or allt[-1][0]
    out = []
    i = 0
    now = allt[0][0] + dt.timedelta(seconds=every)
    while now <= end:
        while i < len(allt) and allt[i][0] <= now:
            core.on_tick(allt[i][1], allt[i][2])
            i += 1
        if now >= start_scan:
            rows = core.scan(now)
            out.append((now, rows))
            if on_scan is not None:
                on_scan(now, rows)
        now += dt.timedelta(seconds=every)
    return out


def single(kind, seed, direction=1, symbol="EURUSD", **kw):
    ticks, pivot = synth.scenario(kind, symbol, seed=seed, direction=direction, **kw)
    core = RiderCore(RiderConfig(max_cost_ratio=0.25))
    core.set_spec(symbol, synth.make_spec(symbol))
    scans = replay(core, {symbol: ticks}, pivot - dt.timedelta(seconds=120))
    return core, scans, pivot


def triggers(scans, pivot, symbol="EURUSD"):
    out = []
    for now, rows in scans:
        r = next(x for x in rows if x.symbol == symbol)
        if r.state == TRIGGER:
            out.append(((now - pivot).total_seconds(), r.direction))
    return out


class TestIdentity:
    def test_ids(self):
        assert STRATEGY_ID == "momentum_rider" and STRATEGY_LABEL == "Rapid Momentum Rider" and MAGIC == 990811

    def test_order_flow_is_absent_never_faked(self):
        assert ORDER_FLOW_AVAILABLE is False
        assert not any("order" in k or "flow" in k for k in DEFAULT_WEIGHTS)

    def test_weights_sum_to_100_and_overrides_renormalise(self):
        assert sum(DEFAULT_WEIGHTS.values()) == pytest.approx(100.0)
        w = weights_from({"velocity": 24.0, "bogus": 5, "news": "x"})
        assert sum(w.values()) == pytest.approx(100.0) and w["velocity"] > DEFAULT_WEIGHTS["velocity"]


class TestMeasures:
    def test_trend_efficiency(self):
        assert efficiency([1, 2, 3, 4, 5]) == (1.0, 1)
        e, d = efficiency([1, 3, 1, 3, 1, 2])
        assert e == pytest.approx(1 / 9) and d == 1

    def test_zigzag_finds_micro_swings(self):
        piv = zigzag([0, 1, 2, 3, 2, 1, 2, 3, 4, 3], 1.0)
        assert [(i, k) for i, _, k in piv] == [(0, -1), (3, 1), (5, -1), (8, 1)]


class TestScenarios:
    @pytest.mark.parametrize("direction", [1, -1])
    def test_a_clean_trend_scores_high_and_triggers_its_way(self, direction):
        core, scans, pivot = single("trend", 3, direction, pips_per_minute=6.0)
        trig = triggers(scans, pivot)
        assert trig, "a clean trend must trigger"
        first = trig[0]
        assert first[0] <= 150                                # early in a 5-minute run, not at its end
        want = "LONG" if direction > 0 else "SHORT"
        assert all(d == want for _, d in trig)
        best = max(r.score for now, rows in scans for r in rows if now >= pivot)
        assert best >= 65
        r = next(r for now, rows in scans for r in rows if r.state == TRIGGER)
        assert ("upward" if direction > 0 else "downward") in r.reason and "momentum" in r.reason

    def test_violent_chop_waits(self):
        core, scans, pivot = single("chop", 4)
        late = [(now, rows[0]) for now, rows in scans if (now - pivot).total_seconds() >= 60]
        assert late and not any(r.state == TRIGGER for _, r in late)
        assert sum(1 for _, r in late if "choppy" in r.reason.lower()) >= len(late) * 0.4
        assert any(r.direction == "NONE" and "too choppy" in r.reason for _, r in late)

    @pytest.mark.parametrize("direction", [1, -1])
    def test_a_squeeze_release_triggers_in_the_release_direction(self, direction):
        core, scans, pivot = single("squeeze", 2, direction)
        trig = triggers(scans, pivot)
        assert trig and all(d == ("LONG" if direction > 0 else "SHORT") for _, d in trig)
        assert trig[0][0] >= 0                               # nothing during the squeeze itself

    def test_a_quiet_market_never_triggers(self):
        core, scans, pivot = single("quiet", 5)
        assert not triggers(scans, pivot)
        assert any("Quiet" in rows[0].reason or "No clear direction" in rows[0].reason for _, rows in scans)

    def test_same_ticks_same_decisions(self):
        a = single("trend", 7, 1)[1]
        b = single("trend", 7, 1)[1]
        assert [(n, [(r.symbol, r.direction, r.score, r.state, r.reason) for r in rows]) for n, rows in a] == \
               [(n, [(r.symbol, r.direction, r.score, r.state, r.reason) for r in rows]) for n, rows in b]

    def test_no_look_ahead_a_different_future_changes_nothing_before_it(self):
        ticks, pivot = synth.scenario("trend", "EURUSD", seed=8, direction=1)
        cut = pivot + dt.timedelta(seconds=40)
        past = [t for t in ticks if t.time <= cut]
        c = past[-1].bid
        # the same past, then the future mirrored: up becomes down
        other = past + [Tick(t.symbol, t.time, round(2 * c - t.ask, 5), round(2 * c - t.bid, 5))
                        for t in ticks if t.time > cut]
        assert other != ticks
        res = []
        for path in (ticks, other):
            core = RiderCore(RiderConfig(max_cost_ratio=0.25))
            core.set_spec("EURUSD", synth.make_spec("EURUSD"))
            scans = replay(core, {"EURUSD": path}, pivot - dt.timedelta(seconds=30), end=cut)
            res.append([(r.direction, r.score, r.state) for _, rows in scans for r in rows])
        assert res[0] == res[1]

    def test_velocity_is_normalised_across_instruments(self):
        """The same shape on EURUSD and on USDJPY (pip 0.01) reads the same."""
        out = {}
        for sym in ("EURUSD", "USDJPY"):
            core, scans, pivot = single("trend", 9, 1, symbol=sym)
            out[sym] = [round(rows[0].score) for now, rows in scans if round((now - pivot).total_seconds()) in (30, 60, 90)]
        assert out["EURUSD"] and all(abs(a - b) <= 4 for a, b in zip(out["EURUSD"], out["USDJPY"]))


class TestEntries:
    def _trend_core(self):
        ticks, pivot = synth.scenario("trend", "EURUSD", seed=3, direction=1, pips_per_minute=6.0)
        core = RiderCore(RiderConfig(max_cost_ratio=0.25, cooldown_seconds=20.0))
        core.set_spec("EURUSD", synth.make_spec("EURUSD"))
        got = []
        def on_scan(now, rows):
            for it in core.entries(now):
                got.append((now, it))
        replay(core, {"EURUSD": ticks}, pivot - dt.timedelta(seconds=60), on_scan=on_scan)
        return core, got, pivot

    def test_an_intent_carries_a_real_stop_and_spends_its_trigger(self):
        core, got, pivot = self._trend_core()
        assert got
        ids = [it.trigger_id for _, it in got]
        assert ids == sorted(set(ids))                        # every intent spent its own trigger: never duplicated
        gaps = [(b[0] - a[0]).total_seconds() for a, b in zip(got, got[1:])]
        assert all(g >= 20.0 for g in gaps)                   # and never inside the cooldown
        now, it = got[0]
        assert it.side is Side.BUY and it.stop < it.reference_price
        assert 0.6 * it.atr - 1e-9 <= it.reference_price - it.stop <= min(3.0 * it.atr, 30 * 0.0001) + 1e-9
        assert it.snapshot["categories"] and it.snapshot["families"]["velocity"]["strength"] > 0
        assert it.family and it.score >= RiderConfig(max_cost_ratio=0.25).trigger_score and "momentum" in it.reason

    def test_one_position_per_market_cooldown_and_fresh_trigger(self):
        ticks, pivot = synth.scenario("trend", "EURUSD", seed=3, direction=1, pips_per_minute=6.0)
        core = RiderCore(RiderConfig(max_cost_ratio=0.25, cooldown_seconds=20.0))
        core.set_spec("EURUSD", synth.make_spec("EURUSD"))
        state = {"pos": None, "entries": 0, "closed_at": None, "blocked": 0}

        def on_scan(now, rows):
            spent = core.spent_trigger.get("EURUSD", 0)
            intents = core.entries(now)
            r = rows[0]
            if state["pos"] is not None:
                assert not intents                            # one position per market
                if (now - state["pos"].opened_at).total_seconds() >= 30 and state["closed_at"] is None:
                    core.position_closed(state["pos"], now)
                    state["closed_at"] = now
                    state["pos"] = None
                return
            if state["closed_at"] is not None and r.state == TRIGGER:
                if (now - state["closed_at"]).total_seconds() < 20 or r.trigger_id <= spent:
                    assert not intents                        # cooldown, or the same wave: no duplicate
                    state["blocked"] += 1
                elif intents:
                    assert intents[0].trigger_id > spent      # a re-entry needs a fresh trigger
            for it in intents:
                tk = core.last_tick("EURUSD")
                state["pos"] = core.open_position(it, tk.ask, 0.14, 1000 + state["entries"], now)
                state["entries"] += 1

        replay(core, {"EURUSD": ticks}, pivot - dt.timedelta(seconds=60), on_scan=on_scan)
        assert state["entries"] >= 1 and state["blocked"] >= 1
        # every entry spent a different trigger
        assert core.spent_trigger["EURUSD"] >= state["entries"]

    def test_a_fresh_trigger_after_the_cooldown_may_re_enter(self):
        core, got, pivot = self._trend_core()
        now, it = got[-1]
        pos = core.open_position(it, it.reference_price, 0.14, 1, now)
        later = now + dt.timedelta(seconds=5)
        core.position_closed(pos, later)
        core.states.trigger_id["EURUSD"] += 1                 # a new wave arrives in TRIGGER
        core.last_rows[0].trigger_id = core.states.trigger_id["EURUSD"]
        core.last_rows[0].state = TRIGGER
        core.last_scan_at = (later + dt.timedelta(seconds=1)).timestamp()
        assert core.entries(later + dt.timedelta(seconds=1)) == []          # inside the cooldown
        core.last_scan_at = (later + dt.timedelta(seconds=25)).timestamp()
        again = core.entries(later + dt.timedelta(seconds=25))
        assert len(again) == 1 and again[0].trigger_id == core.states.trigger_id["EURUSD"]

    def test_a_fill_through_the_stop_is_refused(self):
        core, got, pivot = self._trend_core()
        now, it = got[0]
        with pytest.raises(ValueError):
            core.open_position(it, it.stop - 0.0001, 0.14, 2, now)


class TestCurrencyStrength:
    SYMS = ["EURUSD", "GBPUSD", "USDJPY", "EURJPY", "EURGBP", "AUDUSD", "USDCHF", "GBPJPY", "EURCHF", "NZDUSD"]

    def test_least_squares_does_not_hand_one_leg_to_the_other(self):
        moves = [PairMove("EURUSD", "EUR", "USD", -3.0), PairMove("GBPUSD", "GBP", "USD", -1.5),
                 PairMove("USDJPY", "USD", "JPY", 1.5), PairMove("EURGBP", "EUR", "GBP", -1.5),
                 PairMove("EURJPY", "EUR", "JPY", -1.5), PairMove("AUDUSD", "AUD", "USD", -1.5)]
        sm = compute_strength(moves)
        assert sm.strongest() == "USD" and sm.weakest() == "EUR"
        assert abs(sm.raw["AUD"]) < abs(sm.raw["EUR"])        # AUD only fell because the dollar rose

    def test_eur_weakest_usd_strongest_puts_eurusd_short_first(self):
        paths, pivot = synth.currency_paths({"EUR": -2.5, "USD": 2.5}, self.SYMS, seed=5)
        core = RiderCore(RiderConfig(max_cost_ratio=0.25))
        for s in self.SYMS:
            core.set_spec(s, synth.make_spec(s))
        tops, strong = [], []
        def on_scan(now, rows):
            if (now - pivot).total_seconds() >= 60:
                tops.append((rows[0].symbol, rows[0].direction))
                strong.append((core.strength.strongest(), core.strength.weakest()))
        replay(core, paths, pivot - dt.timedelta(seconds=10), on_scan=on_scan)
        assert tops
        share = sum(1 for t in tops if t == ("EURUSD", "SHORT")) / len(tops)
        assert share >= 0.5
        assert sum(1 for s in strong if s == ("USD", "EUR")) / len(strong) >= 0.8
        row = next(r for r in core.last_rows if r.symbol == "EURUSD")
        assert row.direction == "SHORT"


class TestNews:
    def test_news_blocks_before_and_never_chases_the_first_tick(self):
        ticks, pivot = synth.scenario("trend", "EURUSD", seed=3, direction=-1, pips_per_minute=8.0)
        ev = CalendarEvent("nfp", pivot, "USD", "US", "Non-Farm Payrolls", 3, 250.0, 180.0, 150.0)
        core = RiderCore(RiderConfig(max_cost_ratio=0.25))
        core.set_spec("EURUSD", synth.make_spec("EURUSD"))
        core.set_calendar([ev])
        seen = {}
        def on_scan(now, rows):
            seen[round((now - pivot).total_seconds())] = rows[0]
        replay(core, {"EURUSD": ticks}, pivot - dt.timedelta(seconds=100), on_scan=on_scan)
        before = [r for k, r in seen.items() if -100 <= k < 0]
        assert before and all(r.state != TRIGGER for r in before)
        assert any("Non-Farm Payrolls due" in r.reason for r in before)
        assert all(seen[k].state != TRIGGER for k in range(0, 20) if k in seen)       # the first seconds: never chased
        later = [r for k, r in seen.items() if k >= 40]
        assert any(r.state == TRIGGER and r.direction == "SHORT" for r in later)
        fams = core.last_results["EURUSD"].families["news"].values
        assert fams.get("event", {}).get("name") == "Non-Farm Payrolls" and fams.get("surprise") == pytest.approx(70.0)


class TestLearning:
    def test_needs_minimum_samples_and_days_and_shrinks(self):
        b = BucketStats(min_samples=30, window=600, min_days=3)
        for i in range(29):
            b.add(Outcome(f"2026-10-0{1 + i % 5}", 10.0, {"entry_family": "breakout"}))
        assert b.match({"entry_family": "breakout"})[0] == 0.5          # 29 trades: not enough
        for i in range(40):
            b.add(Outcome(f"2026-10-0{1 + i % 5}", -2.0, {"entry_family": "pullback"}))
        b.add(Outcome("2026-10-01", 10.0, {"entry_family": "breakout"}))
        bk = b.bucket("entry_family", "breakout")
        assert bk["qualifies"] and bk["mean"] == pytest.approx(10.0)
        assert b.global_mean() < bk["shrunk"] < bk["mean"]                 # pulled toward the global mean
        hi = b.match({"entry_family": "breakout"})[0]
        lo = b.match({"entry_family": "pullback"})[0]
        assert 0.5 < hi <= 1.0 and 0.0 <= lo < 0.5

    def test_one_day_never_counts(self):
        b = BucketStats(min_samples=5, window=600, min_days=3)
        for _ in range(50):
            b.add(Outcome("2026-10-08", -10.0, {"symbol_group": "USD_MAJOR"}))
        assert not b.bucket("symbol_group", "USD_MAJOR")["qualifies"]
        assert b.match({"symbol_group": "USD_MAJOR"})[0] == 0.5

    def test_rolling_window(self):
        b = BucketStats(min_samples=1, window=10, min_days=1)
        for i in range(25):
            b.add(Outcome("2026-10-0%d" % (1 + i % 3), float(i), {}))
        assert len(b.outcomes) == 10 and b.global_mean() == pytest.approx(sum(range(15, 25)) / 10)


class TestConfig:
    def test_missing_file_gives_defaults(self, tmp_path):
        c = RiderConfig.load(tmp_path / "rider.json")
        assert c.mode == "PAPER" and c.user_pip_value_gbp == 1.0 and c.symbols == () and c.max_concurrent_positions == 4

    def test_malformed_file_gives_defaults(self, tmp_path):
        p = tmp_path / "rider.json"
        p.write_text("{not json")
        assert RiderConfig.load(p) == RiderConfig()

    def test_malformed_entries_keep_their_defaults(self, tmp_path):
        p = tmp_path / "rider.json"
        p.write_text(json.dumps({"mode": "live", "user_pip_value_gbp": "lots", "cooldown_seconds": 30,
                                 "symbols": ["EURUSD", "GBPUSD"], "max_concurrent_positions": "x",
                                 "flowlock": {"curve": "tight", "bad": [1]}, "tick_history": "yes", "nonsense": 1}))
        c = RiderConfig.load(p)
        assert c.mode == "LIVE" and c.user_pip_value_gbp == 1.0 and c.cooldown_seconds == 30.0
        assert c.symbols == ("EURUSD", "GBPUSD") and c.max_concurrent_positions == 4
        assert c.flowlock == {"curve": "tight"} and c.tick_history is True

    def test_save_minimal_writes_only_the_mode(self, tmp_path):
        p = RiderConfig.default_path(tmp_path)
        RiderConfig.save_minimal(p, "live")
        assert json.loads(p.read_text()) == {"mode": "LIVE"}
        with pytest.raises(ValueError):
            RiderConfig.save_minimal(p, "MAYBE")


def test_the_live_cost_gate_is_15_percent_since_9_october():
    """Aaron, 9 Oct: wins were being eaten by commission. The live default
    now asks that costs be at most 15% of the expected move (it was 25%);
    the research keeps the old gate as a control (RMR-COST-25)."""
    from mintel.rider.research.variants import by_id
    assert RiderConfig().max_cost_ratio == 0.15
    assert by_id("RMR-COST-25").config(RiderConfig()).max_cost_ratio == 0.25
    patient = by_id("RMR-PATIENT").config(RiderConfig())
    longer = by_id("RMR-RIDE-LONGER").config(RiderConfig())
    assert patient.flowlock["no_progress_seconds"] == 300.0 and patient.flowlock["adverse_r"] == 0.9
    assert longer.flowlock["curve"] == "loose" and longer.flowlock["no_progress_seconds"] == 300.0
