"""Rapid Momentum Rider research suite: the replay, the event study and the
one-command pipeline.

All prices are SYNTHETIC (mintel/rider/research/synthetic.py, seeded) or
scripted by hand; they show the machinery behaves as designed - the same
ticks give the same trades, the future never changes a past decision, fills
pay the spread, slippage and commission, the stress keeps the same signals -
and say nothing about whether the bot makes money."""
import dataclasses
import datetime as dt
import json
import shutil
from array import array

import pytest

from mintel.broker.base import AccountInfo, CalendarEvent, Side, Tick
from mintel.rider import synth
from mintel.rider.config import RiderConfig
from mintel.rider.core import EntryIntent, RiderCore
from mintel.rider.research import SYNTHETIC_LABEL, synthetic
from mintel.rider.research.__main__ import main, resolve_symbols
from mintel.rider.research.costs import CostModel
from mintel.rider.research.eventstudy import auc, cohen_d, combine, study_day
from mintel.rider.research.registry import Registry
from mintel.rider.research.replay import DayData, DayReplay, ZRecord, load_day, replay_day
from mintel.rider.research.tickstore import DayTicks, TickStore, day_start_ms, ms_to_dt

UTC = dt.timezone.utc
SYMS = ["EURUSD", "GBPUSD", "USDJPY"]
SEED_DAY, DAY = "2026-10-05", "2026-10-06"


@pytest.fixture(scope="module")
def small(tmp_path_factory):
    root = tmp_path_factory.mktemp("rr")
    store = TickStore(root / "ticks")
    specs, events = synthetic.generate(store, [SEED_DAY, DAY], SYMS, seed=9, hours=(7, 8))
    # the 9 Oct guards off: this made-up hour is here to exercise the replay's own machinery (fills, stop moves,
    # stress, no look-ahead) on many trades; the guards cut it to 3 and have their own replay tests
    # (tests/test_rider_guards.py)
    cfg = RiderConfig(max_cost_ratio=0.25, currency_cluster_guard=False, loss_cooldown_guard=False)
    cost = CostModel(slippage_pips=cfg.slippage_pips, latency_ms=cfg.latency_ms)
    data = load_day(store, DAY, SYMS, specs, events, cache_dir=root / "cache")
    # the run also records every committed stop move: when it was sent, and the spread the gate saw then
    seen, moves = {}, []
    due, apply = RiderCore.stop_move_due, DayReplay._apply_stop

    def due_spy(self, pos, new_stop, tick, now):
        ok = due(self, pos, new_stop, tick, now)
        if ok:
            seen[(pos.ticket, int(round(now.timestamp() * 1000)))] = tick.ask - tick.bid
        return ok

    def apply_spy(self, o, tk):
        sent_ms = o.pending_stop[3]
        old = o.pos.flow.stop
        apply(self, o, tk)
        if o.pos.flow.stop != old:
            moves.append({"ticket": o.pos.ticket, "side": o.pos.side.sign, "entry": o.pos.entry, "pip": o.pos.pip,
                          "old": old, "new": o.pos.flow.stop, "sent_ms": sent_ms,
                          "spread": seen[(o.pos.ticket, sent_ms)]})
    RiderCore.stop_move_due, DayReplay._apply_stop = due_spy, apply_spy
    try:
        full = DayReplay(cfg, data, cost, "RMR-BASE").run_full()
    finally:
        RiderCore.stop_move_due, DayReplay._apply_stop = due, apply
    return {"root": root, "store": store, "specs": specs, "events": events, "cfg": cfg, "cost": cost, "data": data,
            "full": full, "moves": moves}


def sig(intents):
    return [(i.signal_time, i.symbol, i.side, i.stop, i.trigger_id) for i in intents]


# ---------------------------------------------------------------- replay --

class TestReplay:
    def test_the_fixture_trades(self, small):
        assert len(small["full"].trades) >= 3                     # otherwise the tests below prove nothing
        assert small["full"].counters["scans"] >= 3600
        t = small["full"].trades[0]
        for k in ("gross_pips", "net_pips", "net_money", "mfe_pips", "mae_pips", "capture", "exit_reason",
                  "flow_states", "latency_ms", "commission_gbp"):
            assert k in t

    def test_same_ticks_same_trades(self, small):
        again = DayReplay(small["cfg"], small["data"], small["cost"], "RMR-BASE").run_full()
        assert again.trades == small["full"].trades
        assert sig(again.intents) == sig(small["full"].intents)

    def test_the_future_never_changes_a_past_decision(self, small):
        data = small["data"]
        cut = day_start_ms(DAY) + 7 * 3_600_000 + 40 * 60_000                 # 07:40
        ticks = {}
        for s, d in data.ticks.items():
            nd = DayTicks(s, array("q", d.times), array("d", d.bids), array("d", d.asks))
            pip = 0.01 if s.endswith("JPY") else 0.0001
            for i, t in enumerate(nd.times):
                if t >= cut:                                                   # a 30-pip jump after the cut
                    nd.bids[i] += 30 * pip
                    nd.asks[i] += 30 * pip
            ticks[s] = nd
        other = DayReplay(small["cfg"], dataclasses.replace(data, ticks=ticks), small["cost"], "RMR-BASE").run_full()
        before = lambda trades: [t for t in trades if t["exit_ms"] < cut]       # noqa: E731
        assert before(small["full"].trades)                                    # trades before the cut exist
        assert before(other.trades) == before(small["full"].trades)
        cut_dt = ms_to_dt(cut)
        assert sig([i for i in other.intents if i.signal_time < cut_dt]) == \
            sig([i for i in small["full"].intents if i.signal_time < cut_dt])
        if any(t["exit_ms"] >= cut for t in small["full"].trades):
            assert other.trades != small["full"].trades                         # the jump did matter afterwards

    def test_stress_keeps_the_same_signals(self, small):
        full = small["full"]
        same = DayReplay(small["cfg"], small["data"], small["cost"], "RMR-BASE").run_fixed(full.intents, full.zrec)
        assert same.trades == full.trades                                       # 1x replays the normal run exactly
        two = DayReplay(small["cfg"], small["data"], small["cost"].stressed(2.0), "RMR-BASE").run_fixed(
            full.intents, full.zrec)
        assert two.counters["intents"] == len(full.intents)
        by = {(t["symbol"], t["signal_ms"]): t for t in full.trades}
        compared = 0
        for t in two.trades:
            a = by.get((t["symbol"], t["signal_ms"]))
            if a is None or t["fill_ms"] != a["fill_ms"]:
                continue
            compared += 1
            worse = (t["entry"] - a["entry"]) * (1 if t["side"] == "LONG" else -1)
            assert worse > 0                                                    # a wider spread fills worse
            assert t["commission_gbp"] == pytest.approx(a["commission_gbp"])    # commission is not stressed
        assert compared >= 1
        res = replay_day(small["cfg"], small["data"], small["cost"], "RMR-BASE",
                         [small["cost"].stressed(1.5), small["cost"].delayed(2000)])
        assert set(res) == {"normal costs", "1.5x spread and slippage", "2000 ms delay", "_notes"}
        assert res["normal costs"]["trades"] == full.trades
        assert all(t["latency_ms"] >= 2000 for t in res["2000 ms delay"]["trades"])

    def test_committed_stop_moves_keep_the_live_step_and_interval(self, small):
        """The replay sends a stop move only when the live engine would
        (RiderCore.stop_move_due): each gains at least 0.3 pip and one spread
        and is sent at least 5 s after the trade's last one (only the first
        move to breakeven may skip the wait). On this SYNTHETIC fixture the
        old every-pass rule committed 266 moves on 16 trades, median 0.1 pip."""
        cfg, moves = small["cfg"], small["moves"]
        assert len(moves) >= 10
        assert sum(t["stop_moves"] for t in small["full"].trades) == len(moves)
        last = {}
        for m in moves:
            gain = (m["new"] - m["old"]) * m["side"]
            assert gain >= max(cfg.stop_min_step_pips * m["pip"], cfg.stop_min_step_spreads * m["spread"]) - 1e-10
            prev = last.get(m["ticket"])
            if prev is not None:
                breakeven = (m["entry"] - m["old"]) * m["side"] > 0
                assert m["sent_ms"] - prev >= cfg.stop_min_modify_seconds * 1000 or breakeven
            last[m["ticket"]] = m["sent_ms"]

    def test_calendar_hides_actuals_until_released(self, small):
        now = day_start_ms(DAY) + 8 * 3_600_000
        evs = [CalendarEvent("past", ms_to_dt(now - 300_000), "USD", "US", "Past", 3, actual=1.2, forecast=1.0),
               CalendarEvent("soon", ms_to_dt(now + 600_000), "USD", "US", "Soon", 3, actual=9.9, forecast=1.0),
               CalendarEvent("far", ms_to_dt(now + 13 * 3_600_000), "USD", "US", "Far", 3, actual=1.0)]
        r = DayReplay(small["cfg"], dataclasses.replace(small["data"], calendar=evs), small["cost"])
        got = {e.event_id: e for e in r._calendar(now)}
        assert got["past"].actual == 1.2 and got["soon"].actual is None and got["soon"].forecast == 1.0
        assert "far" not in got


# ------------------------------------------------- fills on a scripted path --

def scripted(path, symbol="EURUSD"):
    """[(seconds after 08:00, bid, ask)] on DAY -> DayData with no seed bars."""
    t0 = day_start_ms(DAY) + 8 * 3_600_000
    d = DayTicks(symbol)
    for sec, b, a in path:
        d.times.append(t0 + int(round(sec * 1000)))
        d.bids.append(b)
        d.asks.append(a)
    d0 = day_start_ms(DAY)
    return DayData(DAY, [symbol], {symbol: synth.make_spec(symbol)}, [], d0 - 600_000, d0, d0 + 86_400_000,
                   d0 + 86_400_000, {symbol: d}, {}), t0


def intent(symbol, side, stop, t_ms, ref):
    return EntryIntent(symbol, side, stop, "scripted", {}, "breakout", 70.0, 1, 0.0, ms_to_dt(t_ms), ref, 0.0005,
                       0.00002, 0.00008, 0.002, 0.1, 0.6, 2.0, abs(ref - stop) / 0.0001)


class TestFills:
    def test_entry_after_the_delay_at_the_ask_and_a_gapped_stop(self):
        path = [(-5, 1.10000, 1.10002), (0, 1.10000, 1.10002), (0.1, 1.10010, 1.10012), (0.4, 1.10020, 1.10022),
                (10, 1.10010, 1.10012), (20, 1.09900, 1.09902), (25, 1.09900, 1.09902)]
        data, t0 = scripted(path)
        cfg = RiderConfig(max_cost_ratio=0.25)
        cost = CostModel(slippage_pips=0.2, latency_ms=250)
        out = DayReplay(cfg, data, cost, "T").run_fixed([intent("EURUSD", Side.BUY, 1.09950, t0, 1.10002)],
                                                        ZRecord(["EURUSD"]))
        assert len(out.trades) == 1
        t = out.trades[0]
        assert t["fill_ms"] == t0 + 400 and t["latency_ms"] == 400             # first tick at or after +250 ms
        assert t["entry"] == pytest.approx(1.10022 + 0.00002)                  # the ask plus slippage
        assert t["exit_reason"] == "STOP"
        assert t["exit_price"] == pytest.approx(1.09900 - 0.00002)             # gapped through: worse than the stop
        assert t["gross_pips"] == pytest.approx(-12.6)
        assert t["volume"] == 0.14 and t["gbp_per_pip"] == pytest.approx(1.036)
        assert t["commission_gbp"] == pytest.approx(2 * 3.5 * 0.74 * 0.14)
        assert t["net_money"] == pytest.approx(-12.6 * 1.036 - 2 * 3.5 * 0.74 * 0.14, abs=1e-3)
        assert t["mae_pips"] == pytest.approx(12.4) and t["mfe_pips"] == 0.0
        assert t["entry_slip_pips"] == pytest.approx(2.2)                      # the delay cost 2 pips plus slippage

    def test_a_fill_through_the_planned_stop_is_closed_at_once(self):
        path = [(-5, 1.10000, 1.10002), (0, 1.10000, 1.10002), (0.3, 1.09940, 1.09942), (5, 1.09940, 1.09942)]
        data, t0 = scripted(path)
        out = DayReplay(RiderConfig(max_cost_ratio=0.25), data, CostModel(slippage_pips=0.2, latency_ms=250), "T").run_fixed(
            [intent("EURUSD", Side.BUY, 1.09950, t0, 1.10002)], ZRecord(["EURUSD"]))
        assert [t["exit_reason"] for t in out.trades] == ["FILL_THROUGH_STOP"]
        t = out.trades[0]
        assert t["entry"] == pytest.approx(1.09944) and t["exit_price"] == pytest.approx(1.09938)
        assert t["fill_ms"] == t["exit_ms"] == t0 + 300

    def test_a_short_pays_the_spread_the_other_way(self):
        path = [(-5, 1.10000, 1.10002), (0, 1.10000, 1.10002), (0.3, 1.10000, 1.10002),
                (8, 1.10070, 1.10072), (12, 1.10070, 1.10072)]
        data, t0 = scripted(path)
        out = DayReplay(RiderConfig(max_cost_ratio=0.25), data, CostModel(slippage_pips=0.2, latency_ms=250), "T").run_fixed(
            [intent("EURUSD", Side.SELL, 1.10060, t0, 1.10000)], ZRecord(["EURUSD"]))
        t = out.trades[0]
        assert t["side"] == "SHORT" and t["entry"] == pytest.approx(1.09998)
        assert t["exit_reason"] == "STOP" and t["exit_price"] == pytest.approx(1.10074)
        assert t["gross_pips"] == pytest.approx(-7.6)


# ----------------------------------------------------------- event study --

class TestEventStudy:
    def test_effect_size_helpers(self):
        assert cohen_d([1, 2, 3], [1, 2, 3]) == 0.0
        assert cohen_d([2, 3, 4], [0, 1, 2]) == pytest.approx(2.0)
        assert auc([3, 4], [1, 2]) == 1.0 and auc([1, 2], [3, 4]) == 0.0 and auc([1, 2], [1, 2]) == 0.5
        assert cohen_d([1], [2, 3]) is None

    def test_runs_against_ordinary_moments(self, small):
        r = study_day(small["store"], DAY, SYMS, small["specs"], [], small["cfg"], n_baseline=40,
                      cache_dir=small["root"] / "cache")
        kinds = {s["kind"] for s in r["samples"]}
        assert {"run", "base"} <= kinds and ({"burst_ok", "burst_fail"} & kinds)
        run = next(s for s in r["samples"] if s["kind"] == "run" and len(s["features"]) == 3)
        assert set(run["features"]) == {"-30", "-10", "0"}
        for k in ("score", "velocity", "efficiency", "chop", "cross_market", "breakout", "z5"):
            assert k in run["features"]["0"]
        assert set(run["outcomes"]) >= {"10", "30", "60", "300", "mfe5m", "mae5m"}
        assert run["outcomes"]["mfe5m"] >= 10.0                                   # a +10 run by definition
        assert set(r["base_rates"]) == set(SYMS) and len(r["base_rates"]["EURUSD"]) == 5
        c = combine([r], min_events=5)
        assert c["runs"] == sum(1 for s in r["samples"] if s["kind"] == "run")
        assert c["runs_vs_ordinary"]["top_at_moment"]
        assert all(0.0 <= e["auc"] <= 1.0 for e in c["runs_vs_ordinary"]["effects"] if e["auc"] is not None)
        assert c["what_changed"] and isinstance(c["what_changed"][0], str)
        if c["bursts_ok"]:
            assert [x["latency_ms"] for x in c["latency"]] == [0, 250, 500, 1000, 2000]
        small["study"] = r

    def test_features_never_see_the_future(self, small, tmp_path):
        base = small.get("study") or study_day(small["store"], DAY, SYMS, small["specs"], [], small["cfg"],
                                               n_baseline=40, cache_dir=small["root"] / "cache")
        cut = day_start_ms(DAY) + 7 * 3_600_000 + 45 * 60_000
        store2 = TickStore(tmp_path / "ticks")
        for s in SYMS:
            for d in (SEED_DAY, DAY):
                t = small["store"].load(s, d)
                pip = 0.01 if s.endswith("JPY") else 0.0001
                bids = [b + (40 * pip if tm >= cut else 0.0) for tm, b in zip(t.times, t.bids)]
                asks = [a + (40 * pip if tm >= cut else 0.0) for tm, a in zip(t.times, t.asks)]
                store2.write_day(s, d, t.times, bids, asks)
        other = study_day(store2, DAY, SYMS, small["specs"], [], small["cfg"], n_baseline=40,
                          cache_dir=tmp_path / "cache")
        limit = (cut - day_start_ms(DAY)) // 1000 - 310            # labels of these never reach the change
        key = lambda s: (s["symbol"], s["second"], s["side"], s["kind"])        # noqa: E731
        a = {key(s): s for s in base["samples"] if s["kind"] == "run" and s["second"] < limit}
        b = {key(s): s for s in other["samples"] if s["kind"] == "run" and s["second"] < limit}
        assert a and set(a) == set(b)
        for k in a:
            assert a[k]["features"] == b[k]["features"]


# --------------------------------------------------------- one command --

class TestCommand:
    def test_resolve_symbols(self):
        names = ["EURUSD.a", "EURUSD", "GBPUSD.r", "XAUUSD", "USDJPY", "US500", "EURGBP"]
        assert resolve_symbols(names, "auto") == ["EURUSD", "GBPUSD.r", "USDJPY"]
        assert resolve_symbols(names, "all") == ["EURGBP", "EURUSD", "GBPUSD.r", "USDJPY"]
        assert resolve_symbols(names, "gbpusd,EURGBP,AUDUSD") == ["GBPUSD.r", "EURGBP"]

    def test_synthetic_end_to_end_is_labelled_and_published(self, tmp_path, monkeypatch):
        import mintel.rider.research.publish as pub
        sent = {}

        def fake_publish(files, config_path, folder, **kw):
            sent.update({"files": files, "folder": folder})
            return [f"reports/rider-research/{folder}/{n}" for n in files]
        monkeypatch.setattr(pub, "publish", fake_publish)
        data = tmp_path / "data"
        data.mkdir()
        args = ["--config", str(data / "config.json"), "--synthetic", "--days", "1", "--symbols", "EURUSD,GBPUSD",
                "--synthetic-hours", "7-8", "--variants", "base", "--workers", "1", "--upload"]
        assert main(args) == 0
        out = next((data / "rider-research").glob("*-SYNTHETIC"))
        md = (out / "report.md").read_text()
        assert md.startswith(f"# Rapid Momentum Rider - research report ({SYNTHETIC_LABEL})")
        assert f"## Verdict - {SYNTHETIC_LABEL}" in md and "says NOTHING about whether the bot makes money" in md
        assert "## Cost and latency stress" in md and "## Event study" in md and "## Data coverage" in md
        sm = json.loads((out / "summary.json").read_text())
        assert sm["synthetic"] is True and sm["label"] == SYNTHETIC_LABEL and sm["verdict"]["code"] == "SYNTHETIC"
        assert sm["data_source"].startswith("SYNTHETIC")
        assert sm["verdict"]["pipeline_check"]["code"] == "INSUFFICIENT_DATA"     # one day is never enough
        h = sm["headline"]["normal"]
        assert h["trades"] == len((out / "trades.csv").read_text().strip().splitlines()) - 1
        assert set(sm["headline"]["stress"]) == {"1.5x spread and slippage", "2x spread and slippage",
                                                 "1000 ms delay", "2000 ms delay"}
        assert sent["folder"].endswith("-SYNTHETIC") and set(sent["files"]) == {"report.md", "summary.json",
                                                                               "trades.csv"}
        reg = Registry(data / "rider-research" / "SYNTHETIC" / "registry.sqlite")
        rows = reg.rows()
        assert {r["METHOD_ID"] for r in rows} == {"RMR-BASE", "RMR-WF-SELECT"}
        assert all(r["DATA_SOURCE"] == "SYNTHETIC" for r in rows)
        reg.close()
        # again: the made-up ticks and every job come from the cache, with the same answer
        assert main(args[:-1]) == 0
        sm2 = json.loads((out / "summary.json").read_text())
        assert sm2["headline"] == sm["headline"]
        reg = Registry(data / "rider-research" / "SYNTHETIC" / "registry.sqlite")
        assert reg.run_count() == 4                                                # every run is recorded
        reg.close()

    def test_probe_reads_one_past_hour_says_what_came_back_and_stores_nothing(self, tmp_path, monkeypatch, capsys):
        class Broker:
            disconnected = False

            def connect(self):
                return True

            def disconnect(self):
                Broker.disconnected = True

            def symbols(self):
                return ["EURUSD.a", "GBPUSD"]

            def ticks_range(self, s, start, end):
                assert s == "EURUSD.a"                           # the broker's own name for EURUSD
                out, t = [], start
                while t <= end:
                    out.append(Tick(s, t, 1.10000, 1.10002))
                    t += dt.timedelta(seconds=30)
                return out

        class Empty(Broker):
            def ticks_range(self, s, start, end):
                return []

        import time
        import mintel.run as run_mod
        monkeypatch.setattr(time, "sleep", lambda s: None)
        data = tmp_path / "data"
        data.mkdir()
        monkeypatch.setattr(run_mod, "build_broker", lambda cfg: Broker())
        assert main(["--config", str(data / "config.json"), "--probe"]) == 0
        out = capsys.readouterr().out
        assert "past history, as the research reads it" in out and "kept for the hour: 120 ticks" in out
        assert "VERDICT: past ticks ARE reachable." in out and Broker.disconnected
        assert not (data / "rider-research").exists()                           # a probe stores nothing
        monkeypatch.setattr(run_mod, "build_broker", lambda cfg: Empty())
        assert main(["--config", str(data / "config.json"), "--probe"]) == 5
        assert "VERDICT: past ticks are NOT reachable" in capsys.readouterr().out

    def test_real_mode_fetches_through_the_broker_then_disconnects(self, tmp_path, monkeypatch):
        src = TickStore(tmp_path / "src")
        today = dt.datetime.now(UTC).date()
        from mintel.rider.research.tickstore import weekdays_back
        # every weekday the fetch asks for (the replay day and the four days before it), as MetaTrader has them:
        # a weekday with no ticks in its busy hours is never stored as final, so it would be asked again
        days = weekdays_back(today - dt.timedelta(days=1), 5)
        specs, events = synthetic.generate(src, days, ["EURUSD", "GBPUSD"], seed=3, hours=(7, 8))

        class Broker:
            connected = disconnected = False
            calls = 0

            def connect(self):
                Broker.connected = True
                return True

            def disconnect(self):
                Broker.disconnected = True

            def account(self):
                return AccountInfo(1, "demo", "GBP", 1000.0, 1000.0, 0.0, 1000.0, 0.0, 30, True, True)

            def symbols(self):
                return ["EURUSD", "GBPUSD", "XAUUSD"]

            def spec(self, s):
                return specs.get(s)

            def ticks_range(self, s, start, end):
                Broker.calls += 1
                d = src.load_span(s, int(start.timestamp() * 1000), int(end.timestamp() * 1000) + 1)
                return list(d.ticks())

            def calendar(self, start, end, currencies=()):
                return list(events)

        import time
        import mintel.run as run_mod
        from mintel.rider.research import tickstore
        monkeypatch.setattr(run_mod, "build_broker", lambda cfg: Broker())
        monkeypatch.setattr(time, "sleep", lambda s: None)
        # the made-up market trades 07:00-08:00 UTC only: an empty hour outside it is its truth, not MetaTrader
        # still loading (an empty hour inside the busy hours is never stored as final)
        monkeypatch.setattr(tickstore, "BUSY_HOURS", (7, 8))
        data = tmp_path / "data"
        data.mkdir()
        assert main(["--config", str(data / "config.json"), "--days", "1", "--symbols", "EURUSD,GBPUSD",
                     "--variants", "base", "--workers", "1"]) == 0
        assert Broker.connected and Broker.disconnected and Broker.calls > 0
        root = data / "rider-research"
        assert (root / "ticks" / "EURUSD" / f"{days[-1]}.csv.gz").exists()
        assert json.loads((root / "specs.json").read_text())["account_currency"] == "GBP"
        sm = json.loads((root / today.isoformat() / "summary.json").read_text())
        assert sm["synthetic"] is False and sm["label"] == "" and sm["verdict"]["code"] == "INSUFFICIENT_DATA"
        assert sm["data_source"].startswith("REAL")
        calls = Broker.calls
        assert main(["--config", str(data / "config.json"), "--days", "1", "--symbols", "EURUSD,GBPUSD",
                     "--variants", "base", "--workers", "1"]) == 0
        assert Broker.calls == calls                                              # nothing fetched twice
        shutil.rmtree(root / "ticks" / "GBPUSD")
        assert main(["--config", str(data / "config.json"), "--no-fetch", "--days", "1", "--variants", "base",
                     "--workers", "1"]) == 0
