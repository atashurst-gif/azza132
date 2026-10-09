"""9 Oct: "the bots haven't traded for well over an hour" - and nobody could
see why without asking Aaron. Two remote views:

* the Rapid Momentum Rider's "why no trade" record (rider-status.json,
  block ``why_no_trade``): the last hour of scans, per market and in total,
  and for every near miss the first rule that held it back. It only READS
  what the scan and the entry check decided: the same synthetic prices give
  the same orders, stop moves and exits with or without it.
* reports/live/bots.json, published by the ten-minute pulse: one entry per
  bot, read-only from each bot's status file and the trader's page.

Every price here is SYNTHETIC (mintel/rider/synth.py, seeded); nothing is a
performance result.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
import types
from collections import Counter

import pytest

from mintel.broker.base import OrderResult, Side
from mintel.config import Config
from mintel.ops import pulse as pl
from mintel.ops import report_upload as ru
from mintel.rider import synth
from mintel.rider.config import RiderConfig
from mintel.rider.core import EntryIntent, RiderCore, ScanRow, _ts
from mintel.rider.engine import GATE_WORDS, WHY_WINDOW_MINUTES, RiderEngine, WhyNoTrade
from mintel.rider.score import READY, TRIGGER
from tests.test_report_upload import ConflictingGitHub, FakeGitHub
from tests.test_rider_engine import Clock, FakeBroker, last_time, make, run, wave_path

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 10, 9, 9, 0, tzinfo=UTC)
NOW = dt.datetime(2026, 10, 9, 9, 30, tzinfo=UTC)


# ======================================================== the record itself --
def _s(market, score, shown="WATCHING", state=None, direction="LONG", tid=0, **kw) -> dict:
    d = {"market": market, "direction": direction, "score": score, "shown": shown, "state": state or shown,
         "trigger_id": tid}
    d.update(kw)
    return d


def scripted_scans() -> list[tuple[dt.datetime, list[dict]]]:
    """Two minutes, one scan a second, three markets:

    EURUSD  BUILDING at 65 held back by costs for 40 scans (66.5 at 20 s), then WATCHING at 20;
    GBPUSD  READY at 58 for 60 scans, TRIGGER at 62 and entered at 60 s, then ENTERED at 61;
    USDJPY  WATCHING at 10, but TRIGGER at 63 with the health checks failing at 100-104 s."""
    out = []
    for i in range(120):
        when = T0 + dt.timedelta(seconds=i)
        rows = []
        if i < 40:
            sc = 66.5 if i == 20 else 65.0
            rows.append(_s("EURUSD", sc, "BUILDING", gate="cost", detail=f"costs at {i}",
                           numbers={"cost_ratio": 0.4 if i == 20 else 0.3, "expected_move_pips": 3.0}))
        else:
            rows.append(_s("EURUSD", 20.0))
        if i < 60:
            rows.append(_s("GBPUSD", 58.0, READY, gate="below_trigger", detail="score 58.0"))
        elif i == 60:
            rows.append(_s("GBPUSD", 62.0, TRIGGER, tid=1, entered=True))
        else:
            rows.append(_s("GBPUSD", 61.0, "ENTERED", TRIGGER, tid=1, gate="one_position", detail="open"))
        if 100 <= i <= 104:
            rows.append(_s("USDJPY", 63.0, TRIGGER, tid=1, gate="health", detail="new entries paused: x"))
        else:
            rows.append(_s("USDJPY", 10.0, direction="NONE", tid=1 if i > 104 else 0))
        out.append((when, rows))
    return out


class TestTheRecordCountsAScriptedScenario:
    @pytest.fixture
    def why(self):
        w = WhyNoTrade(60.0, started=T0 - dt.timedelta(hours=2))
        for when, rows in scripted_scans():
            w.record(when, rows)
        return w

    def test_states_best_scores_entries_and_triggers(self, why):
        r = why.report(T0 + dt.timedelta(seconds=119))
        assert r["scans"] == 120 and r["failed_scans"] == 0
        assert r["entries"] == 1 and r["triggers"] == 2           # GBPUSD's and USDJPY's arrivals in TRIGGER
        m = r["markets"]
        assert m["EURUSD"]["scans"] == {"BUILDING": 40, "WATCHING": 80}
        assert m["GBPUSD"]["scans"] == {"READY": 60, "TRIGGER": 1, "ENTERED": 59}
        assert m["USDJPY"]["scans"] == {"WATCHING": 115, "TRIGGER": 5}
        assert m["EURUSD"]["best"] == {"score": 66.5, "direction": "LONG",
                                       "time_utc": (T0 + dt.timedelta(seconds=20)).isoformat()}
        assert m["GBPUSD"]["best"]["score"] == 62.0 and m["USDJPY"]["best"]["score"] == 63.0
        assert r["best"]["market"] == "EURUSD" and r["best"]["score"] == 66.5
        assert "covers" in r and r["covers"] == f"the last {WHY_WINDOW_MINUTES} minutes"

    def test_every_near_miss_scan_is_counted_against_its_rule(self, why):
        r = why.report(T0 + dt.timedelta(seconds=119))
        assert r["near_miss_scans"] == 40 + 60 + 59 + 5
        assert [(x["gate"], x["scans"]) for x in r["blocked_by"]] == [
            ("below_trigger", 60), ("one_position", 59), ("cost", 40), ("health", 5)]
        assert all(x["what"] == GATE_WORDS[x["gate"]] for x in r["blocked_by"])
        m = r["markets"]
        assert m["EURUSD"]["blocked_by"] == {"cost": 40}
        assert m["GBPUSD"]["blocked_by"] == {"below_trigger": 60, "one_position": 59}
        assert m["USDJPY"]["blocked_by"] == {"health": 5}

    def test_near_misses_are_runs_with_the_numbers_of_their_best_scan(self, why):
        near = why.report(T0 + dt.timedelta(seconds=119))["near_misses"]
        assert [(n["market"], n["gate"], n["scans"]) for n in near] == [
            ("EURUSD", "cost", 40), ("GBPUSD", "below_trigger", 60), ("GBPUSD", "one_position", 59),
            ("USDJPY", "health", 5)]
        cost = near[0]
        assert cost["score"] == 66.5 and cost["numbers"] == {"cost_ratio": 0.4, "expected_move_pips": 3.0}
        assert cost["detail"] == "costs at 20" and cost["what"] == GATE_WORDS["cost"]
        assert cost["time_utc"] == T0.isoformat() and cost["last_utc"] == (T0 + dt.timedelta(seconds=39)).isoformat()
        assert not any(k.startswith("_") for n in near for k in n)

    def test_last_trigger_and_the_summary_in_plain_words(self, why):
        r = why.report(T0 + dt.timedelta(seconds=119))
        assert r["last_trigger"] == ("last trigger: USDJPY LONG at 09:01:40 UTC (19 s ago), score 63.0, not taken: "
                                     + GATE_WORDS["health"])
        assert r["last_trigger_detail"]["taken"] is False and r["last_trigger_detail"]["held_back_by"] == "health"
        assert r["summary"] == (
            "In the last 60 minutes: 120 scans of 3 markets, 2 triggers, 1 entry. Best score 66.5 (EURUSD LONG at "
            "09:00:20 UTC); the trigger score is 60. 105 near-miss scans with no entry, held back most by: "
            f"{GATE_WORDS['below_trigger']} (60), {GATE_WORDS['cost']} (40), {GATE_WORDS['health']} (5). "
            "(59 more scans scored high on a market already in a trade.)")
        assert r["last_trade"] == "last trade: not known"           # the engine gives it from the records

    def test_the_window_rolls_by_the_minute(self, why):
        assert why.report(T0 + dt.timedelta(minutes=59, seconds=59))["scans"] == 120
        later = why.report(T0 + dt.timedelta(minutes=60))
        assert later["scans"] == 60                                 # the first minute has dropped out
        assert later["markets"]["GBPUSD"]["scans"] == {"ENTERED": 59, "TRIGGER": 1}
        gone = why.report(T0 + dt.timedelta(minutes=61))
        assert gone["scans"] == 0 and gone["near_miss_scans"] == 0 and gone["blocked_by"] == []
        assert "no scans" in gone["summary"]
        assert "59 min ago" in gone["last_trigger"]                 # the last trigger is still said

    def test_a_new_start_says_so(self):
        w = WhyNoTrade(60.0, started=T0)
        r = w.report(T0 + dt.timedelta(minutes=5))
        assert r["covers"].startswith("since the Rider started at 09:00:00 UTC")
        assert r["last_trigger"] == "last trigger: none since the Rider started at 09:00:00 UTC"

    def test_memory_stays_bounded_however_long_it_runs(self):
        w = WhyNoTrade(60.0, started=T0)
        markets = [f"M{i:02d}" for i in range(28)]
        n = 0
        for k in range(0, 3 * 3600, 10):                            # three hours, a scan every 10 s
            when = T0 + dt.timedelta(seconds=k)
            w.record(when, [_s(m, 70.0, "BUILDING", gate="chop" if k % 20 else "cost", detail="x",
                               direction="LONG" if k % 40 else "SHORT") for m in markets])
            n += 1
        assert len(w.minutes) <= WHY_WINDOW_MINUTES + 1 and len(w.near) <= 10
        r = w.report(T0 + dt.timedelta(seconds=3 * 3600 - 10))
        assert r["scans"] == 360 and len(r["markets"]) == 28        # the last hour only
        assert sum(x["scans"] for x in r["blocked_by"]) == 360 * 28
        assert len(json.dumps(r)) < 40_000


# ======================================================= the engine's record --
def two_markets():
    e_ticks, pivot = wave_path(seed=3, waves=((1, 8.0, 180), (-1, 9.0, 150), (1, 7.0, 200)))
    g_ticks, _ = synth.scenario("trend", "GBPUSD", seed=4, direction=-1, warm_minutes=63.0, pips_per_minute=8.0,
                                start=e_ticks[0].time)
    return {"EURUSD": e_ticks, "GBPUSD": g_ticks}, pivot - dt.timedelta(seconds=300)


def run_capturing(eng, broker, clock, tb):
    """Run to the end, the health checks failing for 7 minutes in every 14
    (algo trading switched off), and capture each scan as the engine left it."""
    seen = []

    def hook(now, notes):
        if now.minute % 7 == 3 and now.second == 0:
            broker.trade_allowed = not broker.trade_allowed
        seen.append({"now": now, "notes": list(notes), "allowed": eng.entries_allowed, "sent": len(broker.sent),
                     "rows": [(r.symbol, r.direction, r.score, r.state, r.trigger_id,
                               eng.core.states.state.get(r.symbol)) for r in eng.last_rows]})
    run(eng, broker, clock, last_time(tb), hook=hook)
    return seen


@pytest.fixture(scope="module")
def live_run(tmp_path_factory):
    tb, start = two_markets()
    eng, broker, clock = make(tmp_path_factory.mktemp("rider"), tb, start, max_concurrent_positions=1)
    seen = run_capturing(eng, broker, clock, tb)
    return eng, broker, clock, seen


class TestTheEngineFeedsTheRecord:
    def test_the_counts_match_every_scan(self, live_run):
        eng, broker, clock, seen = live_run
        w = eng.why_no_trade(clock.now)
        assert w["scans"] == len(seen) and w["failed_scans"] == 0
        states: dict = {}
        best: dict = {}
        for s in seen:
            for sym, d, score, shown, _tid, _state in s["rows"]:
                states.setdefault(sym, Counter())[shown] += 1
                best[sym] = max(best.get(sym, -1.0), round(score, 1))
        assert {m: Counter(row["scans"]) for m, row in w["markets"].items()} == states
        assert {m: row["best"]["score"] for m, row in w["markets"].items()} == best
        assert w["entries"] == len(broker.sent) == 2

    def test_near_misses_and_their_rules_match_every_scan(self, live_run):
        eng, broker, clock, seen = live_run
        w = eng.why_no_trade(clock.now)
        near = health = in_trade = 0
        prev_sent = 0
        for s in seen:
            entered = {broker.sent[i].symbol for i in range(prev_sent, s["sent"])}
            prev_sent = s["sent"]
            for sym, d, score, shown, _tid, state in s["rows"]:
                if sym in entered or not (score >= 60.0 or state in (READY, TRIGGER)):
                    continue
                near += 1
                if shown == "ENTERED":
                    in_trade += 1
                elif state == TRIGGER and not s["allowed"]:
                    health += 1
        blocked = {x["gate"]: x["scans"] for x in w["blocked_by"]}
        assert w["near_miss_scans"] == near == sum(blocked.values())
        assert blocked["health"] == health > 0                      # TRIGGER while algo trading was off
        assert blocked["one_position"] == in_trade > 0
        assert set(blocked) <= set(GATE_WORDS)
        for n in w["near_misses"]:
            if n["gate"] == "health":
                assert n["detail"] == "new entries paused: algo trading is switched OFF in MetaTrader"

    def test_last_trade_last_trigger_and_tick_freshness(self, live_run):
        eng, broker, clock, seen = live_run
        w = eng.why_no_trade(clock.now)
        last = max(eng.journal.open_rows() + eng.journal.all_closed(), key=lambda r: r["opened_utc"])
        assert w["last_trade"].startswith(f"last trade: {last['symbol']} {last['side']} opened ")
        assert w["last_trade_detail"]["ticket"] == last["ticket"] and "LIVE" in w["last_trade"]
        assert w["last_trigger"].startswith("last trigger: ") and w["triggers"] > 0
        for sym in ("EURUSD", "GBPUSD"):
            row = w["markets"][sym]
            assert row["tick_age_seconds"] is not None and row["tick_age_seconds"] >= 0
            assert row["ticks_per_minute"] is not None
        assert w["ticks"]["markets"] == 2 and w["ticks"]["no_price_yet"] == []

    def test_the_status_file_carries_it(self, live_run):
        eng, broker, clock, seen = live_run
        eng.write_status(clock.now)
        st = json.loads(eng.status_path.read_text())
        assert st["why_no_trade"]["scans"] == len(seen) and st["why_no_trade"]["summary"]
        assert set(st["scanner"][0]) >= {"market", "direction", "score", "state", "reason", "cost_ratio",
                                         "headroom_atr"}

    def test_a_record_that_fails_never_stops_the_pass(self, tmp_path, monkeypatch):
        ticks, pivot = wave_path()
        tb = {"EURUSD": ticks}
        eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300))

        def broken(*a, **kw):
            raise RuntimeError("bookkeeping bug")
        monkeypatch.setattr(eng.why, "record", broken)
        monkeypatch.setattr(eng.why, "report", broken)
        run(eng, broker, clock, last_time(tb))
        assert broker.sent                                          # it still traded the wave
        assert "unavailable" in eng.status(clock.now)["why_no_trade"]


@pytest.mark.parametrize("setting, gate, word", [
    ({"max_cost_ratio": 0.01}, "cost", "pips against an expected move of"),
    ({"max_chop": 0.0}, "chop", "at or over the 0 limit"),
])
def test_a_gate_that_never_opens_is_named_with_its_numbers(tmp_path, setting, gate, word):
    """A test-only setting closes one gate for good: every near miss is
    counted against it, with the numbers it was judged on, and nothing trades."""
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300), **setting)
    run(eng, broker, clock, last_time(tb))
    w = eng.why_no_trade(clock.now)
    assert not broker.sent and w["entries"] == 0
    assert [x["gate"] for x in w["blocked_by"]] == [gate] and w["near_miss_scans"] > 0
    n = w["near_misses"][-1]
    assert n["gate"] == gate and word in n["detail"] and n["score"] >= 60.0
    assert {"cost_ratio", "expected_move_pips", "cost_pips", "chop", "headroom_atr", "spread_pips",
            "ticks_per_minute", "confirmed"} <= set(n["numbers"])
    if gate == "cost":
        assert n["numbers"]["cost_ratio"] > 0.01 and n["numbers"]["expected_move_pips"] > 0
    assert GATE_WORDS[gate] in w["summary"]


def test_the_cap_is_named_for_every_trigger_it_cut_off():
    """RiderCore.entries stops at the cap; every TRIGGER it did not reach is
    still named, with the rule that applies to it, and nothing else changes."""
    core = RiderCore(RiderConfig(max_concurrent_positions=1))
    core.last_scan_at = _ts(T0)                                 # this scan's rows, as scan() left them
    core.last_rows = [ScanRow("EURUSD", "LONG", 70.0, TRIGGER, "r", trigger_id=1),
                      ScanRow("GBPUSD", "SHORT", 65.0, TRIGGER, "r", trigger_id=2),
                      ScanRow("USDJPY", "LONG", 50.0, "BUILDING", "r")]
    core.open["AUDUSD"] = object()                              # one position open elsewhere: the cap is reached
    core.spent_trigger["GBPUSD"] = 2
    assert core.entries(T0) == []
    assert core.notes == ["EURUSD: trigger skipped - 1 positions already open"]
    assert core.why_not["EURUSD"] == ("max_concurrent", "1 positions open or being opened - the cap is 1")
    assert core.why_not["GBPUSD"][0] == "fresh_trigger"
    assert "USDJPY" not in core.why_not
    assert core.spent_trigger == {"GBPUSD": 2} and core.last_signal == {}


def test_the_entry_rules_are_named(tmp_path):
    """RiderCore.entries says which of its own rules held a TRIGGER back."""
    tb, start = two_markets()
    eng, broker, clock = make(tmp_path, tb, start, max_concurrent_positions=1)
    run(eng, broker, clock, last_time(tb), hook=lambda now, n: False if eng.positions else None)
    core = eng.core
    assert core.open
    sym = next(iter(core.open))
    row = next(r for r in core.last_rows if r.symbol == sym)
    t = _ts(clock.now)
    assert core._skip_rule(sym, row, t)[0] == "one_position"
    other = types.SimpleNamespace(symbol="XXXYYY", trigger_id=3)
    core.spent_trigger["XXXYYY"] = 3
    assert core._skip_rule("XXXYYY", other, t)[0] == "fresh_trigger"
    other.trigger_id = 4
    core.last_signal["XXXYYY"] = t - 5.0
    rule, words = core._skip_rule("XXXYYY", other, t)
    assert rule == "cooldown" and "15 s of the 20 s cooldown left" in words
    core.last_signal["XXXYYY"] = t - 60.0
    assert core._skip_rule("XXXYYY", other, t) is None


class _RefusingBroker(FakeBroker):
    """The broker says no to every order."""
    def send(self, req):
        self.sent.append(req)
        return OrderResult(False, 10006, comment="Request rejected")


class _SilentBroker(FakeBroker):
    """The send itself fails: the order may or may not have reached the broker."""
    def send(self, req):
        self.sent.append(req)
        raise ConnectionError("pipe closed")


def _no_size(eng, monkeypatch):
    monkeypatch.setattr(eng.core, "size", lambda sym: (0.0, 0.0, f"{sym}: no pip value"))


def _executor_raises(eng, monkeypatch):
    def boom(*a, **kw):
        raise TimeoutError("no reply in 5 s")
    monkeypatch.setattr(eng.executor, "open", boom)


@pytest.mark.parametrize("broker_cls, fault, gate, detail", [
    (None, _no_size, "size", "EURUSD: cannot size the trade - EURUSD: no pip value"),
    (_RefusingBroker, None, "order", "EURUSD: order refused - rejected (10006) Request rejected"),
    (_SilentBroker, None, "unknown", "EURUSD: order refused - send failed: pipe closed"),
    (None, _executor_raises, "unknown",
     "EURUSD: order sent, outcome unknown (no reply in 5 s); the broker's list is checked next pass"),
])
def test_an_entry_the_order_step_did_not_open_names_what_stopped_it(tmp_path, monkeypatch, broker_cls, fault, gate,
                                                                    detail):
    """A TRIGGER the order step did not open is counted against what really
    stopped it - sizing, a refusal from the broker, or a send with no answer
    (which may yet show as open) - never all as "the order did not go through"."""
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300), broker_cls=broker_cls)
    if fault is not None:
        fault(eng, monkeypatch)
    seen = []

    def hook(now, notes):                                       # stop on the pass the order step said no
        seen.extend(n for n in notes if n == detail)
        return False if seen else None
    run(eng, broker, clock, last_time(tb), hook=hook)
    assert seen
    w = eng.why_no_trade(clock.now)
    assert w["entries"] == 0 and w["triggers"] > 0 and not eng.positions
    blocked = {x["gate"]: x for x in w["blocked_by"]}
    assert blocked[gate] == {"gate": gate, "what": GATE_WORDS[gate], "scans": 1}
    assert set(blocked) <= set(GATE_WORDS)
    if gate != "order":
        assert "order" not in blocked                               # nothing the broker refused
    assert [(n["gate"], n["detail"], n["what"]) for n in w["near_misses"] if n["gate"] == gate] == [
        (gate, detail, GATE_WORDS[gate])]
    lt = w["last_trigger_detail"]
    assert lt["taken"] is False and lt["held_back_by"] == gate
    if gate == "unknown":
        assert w["last_trigger"].endswith(f"not known yet: {GATE_WORDS['unknown']}")
    else:
        assert w["last_trigger"].endswith(f"not taken: {GATE_WORDS[gate]}")


def test_every_note_of_the_order_step_has_its_own_rule(tmp_path):
    """The notes RiderEngine._enter really returns, each read back to its rule."""
    ticks, pivot = wave_path()
    tb = {"EURUSD": ticks}
    eng, broker, clock = make(tmp_path, tb, pivot - dt.timedelta(seconds=300))
    run(eng, broker, clock, pivot - dt.timedelta(seconds=290))     # prices and the contract are in
    tick = eng.latest["EURUSD"]
    row = ScanRow("EURUSD", "LONG", 70.0, TRIGGER, "r", trigger_id=1)

    def rule(note):
        return eng._held_back_by(row, None, None, TRIGGER, True, False, note)

    no_price = eng._enter(types.SimpleNamespace(symbol="XXXYYY"), clock.now)
    assert no_price == "XXXYYY: no contract or price - no entry"
    assert rule(no_price) == ("no_price", no_price)
    through = eng._enter(types.SimpleNamespace(symbol="EURUSD", side=Side.BUY, stop=tick.ask + 0.0050), clock.now)
    assert through == "EURUSD: price is already through the planned stop - no entry" and not broker.sent
    assert rule(through) == ("through_stop", through)
    assert rule("EURUSD: filled through the planned stop - closed at once")[0] == "through_stop"
    assert rule("EURUSD: trigger skipped - 6 positions already open (the sanity cap)")[0] == "max_concurrent"
    assert rule("") == ("one_position", GATE_WORDS["one_position"])
    assert rule("EURUSD: ticket 5001 already on record - closed again at once")[0] == "not_opened"


# =================================================== decisions are unchanged --
def _entries_before(self, now: dt.datetime) -> list[EntryIntent]:
    """RiderCore.entries exactly as it was before the record (9 Oct, build
    2bc7c1baa9), to run the engine the way it ran before."""
    t = _ts(now)
    if self.last_scan_at != t:
        self.scan(now)
    out: list[EntryIntent] = []
    self.notes = []
    for row in self.last_rows:
        if row.state != TRIGGER:
            continue
        sym = row.symbol
        if sym in self.open:
            continue
        if self.spent_trigger.get(sym, 0) >= row.trigger_id:
            continue
        last = max(self.last_signal.get(sym, -1e18), self.last_exit.get(sym, -1e18))
        if t - last < self.cfg.cooldown_seconds:
            continue
        if len(self.open) + len(out) >= self.cfg.max_concurrent_positions:
            self.notes.append(f"{sym}: trigger skipped - {self.cfg.max_concurrent_positions} positions already open")
            break
        intent, why = self._intent(sym, row, now)
        if intent is None:
            self.notes.append(f"{sym}: {why}")
            continue
        self.spent_trigger[sym] = row.trigger_id
        self.last_signal[sym] = t
        out.append(intent)
    return out


class _EngineBefore(RiderEngine):
    """The engine with no "why no trade" record at all."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.core.entries = types.MethodType(_entries_before, self.core)

    def _diagnose(self, *a, **kw):
        return None

    def why_no_trade(self, now):
        return {}


def _decisions(eng, broker, seen) -> dict:
    trades = [{k: r.get(k) for k in ("ticket", "symbol", "side", "volume", "entry", "initial_stop", "stop",
                                     "exit_price", "exit_reason", "net_money", "opened_utc", "closed_utc",
                                     "flow_states", "trigger_id", "score")}
              for r in eng.journal.all_closed()]
    return {"sent": [(r.symbol, r.side.value, r.volume, r.sl, r.tp, r.magic, r.comment) for r in broker.sent],
            "modified": [(m[0], m[1], m[4]) for m in broker.modified],
            "closed": list(broker.closed_calls),
            "trades": trades,
            "scans": [(s["now"], s["allowed"], [(r[0], r[1], r[2], r[3], r[4]) for r in s["rows"]]) for s in seen],
            "notes": [(s["now"], s["notes"]) for s in seen],
            "open": sorted(eng.positions)}


def test_the_same_prices_give_the_same_orders_stops_and_exits(live_run, tmp_path):
    """Before and after the record, on the same synthetic ticks (two markets,
    the cap at one position, the health checks failing for 7 minutes in every
    14): the same scans, orders, stop moves, closes and booked trades."""
    eng, broker, clock, seen = live_run
    tb, start = two_markets()
    clock0 = Clock(start)
    broker0 = FakeBroker(tb, clock0)
    eng0 = _EngineBefore(RiderConfig(mode="LIVE", max_concurrent_positions=1), broker0, tmp_path, clock=clock0,
                         heartbeat=False)
    seen0 = run_capturing(eng0, broker0, clock0, tb)
    before, after = _decisions(eng0, broker0, seen0), _decisions(eng, broker, seen)
    assert after["sent"] and after["modified"] and after["trades"]          # the scenario trades and manages
    for k in before:
        assert after[k] == before[k], k


# ========================================================= reports/live/bots.json --
def _cfg(workdir) -> Config:
    cfg = Config()
    cfg.ops.data_dir = str(workdir)
    cfg.ops.log_dir = str(workdir)
    cfg.broker_mode = "bridge"
    return cfg


def _why_block() -> dict:
    w = WhyNoTrade(60.0, started=T0 - dt.timedelta(hours=2))
    for when, rows in scripted_scans():
        w.record(when, rows)
    return w.report(T0 + dt.timedelta(seconds=119), last_trade={"line": "last trade: EURUSD BUY opened 08:47:12 UTC"})


def _scanner(n=28) -> list[dict]:
    return [{"market": f"M{i:02d}", "direction": "LONG", "score": 70.0 - i, "state": "BUILDING",
             "reason": "Fast upward momentum, but costs would eat the move", "family": "breakout",
             "cost_ratio": 0.31, "headroom_atr": 2.5} for i in range(n)]


def _write(path, obj) -> None:
    path.write_text(obj if isinstance(obj, str) else json.dumps(obj))


def populate(d) -> None:
    """Every bot's own files, as on Aaron's Mac (with a few things that must never be published)."""
    _write(d / "rider.json", {"mode": "LIVE", "user_pip_value_gbp": 1.0})
    _write(d / "ian.json", {"mode": "LIVE"})
    _write(d / "rider-status.json", {
        "mode": "LIVE", "updated_utc": NOW.isoformat(), "pid": 4242, "user_pip_value_gbp": 1.0,
        "health": {"entries_allowed": True, "summary": "all checks passing",
                   "checks": [{"name": "fresh_ticks", "ok": True, "critical": True, "message": "prices are fresh"}]},
        "scanner": _scanner(), "why_no_trade": _why_block(),
        "open_positions": [{"ticket": 5001, "market": "EURUSD", "side": "BUY", "volume": 0.14, "entry": 1.085,
                            "stop": 1.0841, "pips": 3.2, "money": 3.2, "flowlock": "protecting", "opened_utc": "x"}],
        "today": {"trades": 5, "net_money": -9.99, "money_source": "the broker's figures"},
        "notes": ["09:29:58 watching 28 FX markets", "github_token=ghp_abcdefghijklmnop0123 was pasted here"],
        "api_key": "db-SECRETSECRETSECRET"})
    _write(d / "ian-status.json", {
        "mode": "LIVE", "updated": NOW.isoformat(), "status": "DATA-DEGRADED: no institutional feed is configured",
        "feed": {"state": "NOT CONFIGURED", "reason": "no data-feed key saved", "vendor": "", "synthetic": False},
        "mt5": {"connected": True}, "open_positions": [], "today": {"trades": 0}})
    _write(d / "runner-status.json", {
        "mode": "LIVE", "updated": NOW.isoformat(), "status": "IN TRADE", "note": "riding US500",
        "refused": 0, "skipped": 1,
        "open": [{"ticket": 9001, "source_ticket": 8001, "symbol": "US500", "side": "BUY", "entry": 5800.0,
                  "stop": 5790.0, "initial_stop": 5785.0, "peak_r": 1.2, "r_now": 0.8, "pnl": 12.5,
                  "opened": "2026-10-09T08:10:00+00:00", "trailing": True, "mode": "LIVE"}]})
    _write(d / "crowd-status.json", "{not json")                     # broken: said, never a crash
    # bandbreaker-status.json: missing
    from mintel.broker.paper import SCHEMA
    con = sqlite3.connect(d / "tnb_paper.sqlite")
    con.executescript(SCHEMA)
    con.execute("INSERT INTO positions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (700001, "GER40", "SELL", 0.5, 19000.0, 19050.0, 0.0, "2026-10-09T08:30:00+00:00", "MINTEL", 990311,
                 "2026-10-09T09:29:00+00:00"))
    con.commit()
    con.close()
    _write(d / "secrets.json", {"github_token": "ghp_NEVERNEVERNEVER0000"})


def _bot_row(bid, label, magic, mode, today, n, open_=0.0, n_open=0) -> dict:
    return {"id": bid, "label": label, "magic": magic, "mode": mode, "made": today, "today": today, "trades": n,
            "today_trades": n, "open": open_, "open_trades": n_open, "retired": bid == "rapid_scalper"}


def payload() -> dict:
    """The trader's page (/api), as the pulse reads it."""
    return {
        "status": {"bot": "RUNNING", "mt5_connected": True, "broker_connected": True, "tnb_mode": "PAPER",
                   "build": "2bc7c1baa9", "last_scan": "09:29:58 UTC", "last_trade": "US500 2026-10-09T08:10",
                   "not_trading_because": ["outside its session"], "account_login": 51007777},
        "health": {"summary": "all checks passing", "safe_mode": False},
        "standing": {"start": "2026-10-01T00:00:00+00:00", "day_start": "2026-10-09T00:00:00+00:00",
                     "today": -4.2, "today_trades": 3, "today_commission": 1.4, "open": 12.5, "equity": 1008.3,
                     "balance": 995.8, "currency": "GBP", "made": -4.2, "error": "", "source": "broker",
                     "password": "hunter2-never",
                     "bots": [_bot_row("market_intelligence", "Trend & Breakout", 990311, "PAPER", 0.0, 0),
                              _bot_row("momentum_rider", "Rapid Momentum Rider", 990811, "LIVE", -7.85, 2),
                              _bot_row("momentum_runner", "Momentum Runner", 990511, "LIVE", 3.65, 1, 12.5, 1),
                              _bot_row("band_breaker", "Band Breaker", 990611, "PAPER", 0.0, 0),
                              _bot_row("crowd_fader", "Crowd Fader", 990711, "PAPER", 0.0, 0),
                              _bot_row("financial_ian", "Financial Ian", 990911, "LIVE", 0.0, 0),
                              _bot_row("rapid_scalper", "Rapid Scalper (retired)", 990411, "RETIRED", 0.0, 0)]},
        "strategies": {"market_intelligence": {"paper_net": 2.5, "paper_trades": 1},
                       "band_breaker": {"paper_net": 0.0, "paper_trades": 0}},
        "open_live": [{"bot": "Momentum Runner", "magic": 990511, "ticket": 9001, "symbol": "US500", "side": "BUY",
                       "volume": 1.0, "entry": 5800.0, "stop": 5790.0, "target": 0.0, "profit": 12.5,
                       "opened": "2026-10-09T08:10:00+00:00"}],
        "positions": [{"symbol": "GER40", "side": "SELL", "volume": 0.5, "entry": 19000.0, "stop": 19050.0,
                       "profit": 3.0, "flow_state": "INITIAL", "note": "best 0.10R"}],
        "thinking": [{"rank": i, "symbol": f"IDX{i}", "direction": "LONG", "score": 60 - i, "tier": "B",
                      "tactic": "breakout", "regime": "trend", "reason": "waiting", "blockers": ["spread"],
                      "reward_risk": 2.0} for i in range(1, 9)],
    }


def up_pulse() -> dict:
    return {"ts_utc": NOW.isoformat(), "up": True, "reason": "", "status": payload(), "tnb_mode": "PAPER"}


def _bot(rep, bid) -> dict:
    return next(b for b in rep["bots"] if b["id"] == bid)


class TestBotsJson:
    def test_every_bot_from_its_own_file_and_the_page(self, workdir):
        populate(workdir)
        text = pl.bots_json(_cfg(workdir), up_pulse(), NOW)
        assert len(text.encode()) < pl.BOTS_MAX_BYTES
        rep = json.loads(text)
        assert [b["id"] for b in rep["bots"]] == ["market_intelligence", "momentum_rider", "momentum_runner",
                                                  "band_breaker", "crowd_fader", "financial_ian"]
        assert rep["trader_up"] is True and "trimmed" not in rep
        assert rep["account"]["today"] == -4.2 and rep["account"]["today_trades"] == 3
        assert rep["account"]["day_start"] == "2026-10-09T00:00:00+00:00"

        rider = _bot(rep, "momentum_rider")
        assert rider["mode"] == "LIVE" and rider["mode_set"] == "LIVE" and rider["running"] is True
        assert rider["health"].startswith("RAPID MOMENTUM RIDER - HEALTHY, LIVE, scanning 28 markets, GBP 1.00 per pip")
        assert rider["why_no_trade"] == _why_block()
        assert len(rider["scanner_top10"]) == 10 and rider["scanner_top10"][0] == _scanner()[0]
        assert set(rider["scanner_top10"][0]) >= {"market", "direction", "score", "state", "reason", "cost_ratio",
                                                  "headroom_atr"}
        assert rider["open_trades"][0]["ticket"] == 5001
        # today is the page's standing block, copied - not the Rider's own record, never worked out again
        assert rider["today"]["result"] == -7.85 and rider["today"]["trades"] == 2
        assert rider["today"]["from_utc"] == "2026-10-09T00:00:00+00:00"
        assert rider["own_record_today"]["trades"] == 5

        ian = _bot(rep, "financial_ian")
        assert ian["feed"]["state"] == "NOT CONFIGURED" and ian["feed"]["reason"] == "no data-feed key saved"
        assert "feed NOT CONFIGURED" in ian["health"] and ian["today"]["result"] == 0.0

        tnb = _bot(rep, "market_intelligence")
        assert tnb["mode"] == "PAPER" and tnb["running"] is True
        paper = tnb["open_paper_trades"]
        assert paper["count"] == 1 and paper["trades"][0]["symbol"] == "GER40" and paper["trades"][0]["stop"] == 19050.0
        assert len(tnb["thinking"]) == 5 and tnb["thinking"][0]["symbol"] == "IDX1"
        assert tnb["positions_on_page"][0]["symbol"] == "GER40"
        assert tnb["practice_today"]["result"] == 2.5 and tnb["practice_today"]["trades"] == 1
        assert tnb["today"]["result"] == 0.0

        runner = _bot(rep, "momentum_runner")
        assert runner["open_trades"][0]["symbol"] == "US500" and runner["open_trades"][0]["r_now"] == 0.8
        assert runner["open_on_account"][0]["ticket"] == 9001
        assert runner["today"]["result"] == 3.65 and runner["today"]["open_trades"] == 1
        assert "IN TRADE, LIVE" in runner["health"]

        bb = _bot(rep, "band_breaker")
        assert bb["running"] is False and bb["health"].startswith("not running / not readable: no status file")
        assert bb["practice_today"]["result"] == 0.0
        crowd = _bot(rep, "crowd_fader")
        assert crowd["running"] is False and "crowd-status.json is not valid JSON" in crowd["health"]

    def test_no_secrets_no_login_no_keys(self, workdir):
        populate(workdir)
        text = pl.bots_json(_cfg(workdir), up_pulse(), NOW)
        for secret in ("ghp_abcdefghijklmnop0123", "db-SECRETSECRETSECRET", "hunter2", "51007777",
                       "ghp_NEVERNEVERNEVER0000", "account_login", "api_key", "password"):
            assert secret not in text, secret
        assert "[hidden]" in text                                   # the note was kept, the token blanked

    def test_missing_files_and_a_dead_trader_give_plain_entries(self, workdir):
        cfg = _cfg(workdir)

        def dead(url):
            raise OSError("connection refused")
        p = pl.take_pulse(cfg, NOW, fetch=dead, heartbeats={}, bridge=lambda: None, port_is_open=lambda: False,
                          terminal_running=lambda: False)
        rep = json.loads(pl.bots_json(cfg, p, NOW))
        assert rep["trader_up"] is False and "trader has never reported" in rep["trader_reason"]
        assert "unavailable" in rep["account"]
        for b in rep["bots"]:
            assert b["running"] is False
            assert "unavailable" in b["today"]
            if b["id"] != "market_intelligence":
                assert b["health"].startswith("not running / not readable"), b
        assert _bot(rep, "market_intelligence")["health"].startswith("the trader's page did not answer")

    def test_a_status_file_that_is_not_an_object_or_stale(self, workdir):
        cfg = _cfg(workdir)
        _write(workdir / "ian-status.json", [1, 2, 3])
        _write(workdir / "runner-status.json", {"mode": "LIVE", "updated": "2026-10-09T08:00:00+00:00", "open": []})
        _write(workdir / "rider-status.json", {"mode": "LIVE", "updated_utc": NOW.isoformat(), "scanner": []})
        rep = json.loads(pl.bots_json(cfg, up_pulse(), NOW))
        assert "holds no status" in _bot(rep, "financial_ian")["health"]
        runner = _bot(rep, "momentum_runner")
        assert runner["running"] is False and runner["health"].startswith("NOT RUNNING (last report 90 min ago)")
        rider = _bot(rep, "momentum_rider")
        assert "older build" in rider["why_no_trade"]["unavailable"]   # a Rider that has not been restarted yet

    def test_a_huge_status_is_cut_to_the_cap(self, workdir):
        populate(workdir)
        st = json.loads((workdir / "rider-status.json").read_text())
        why = st["why_no_trade"]
        why["markets"] = {f"MARKET{i:04d}": {"scans": {"WATCHING": 3600}, "best": {"score": float(i % 90)},
                                             "blocked_by": {"chop": 10, "cost": 5}, "tick_age_seconds": 1.0}
                          for i in range(900)}
        st["scanner"] = [dict(r, reason="x" * 700) for r in _scanner(28)]
        st["notes"] = ["y" * 700] * 40
        _write(workdir / "rider-status.json", st)
        text = pl.bots_json(_cfg(workdir), up_pulse(), NOW)
        assert len(text.encode()) <= pl.BOTS_MAX_BYTES
        rep = json.loads(text)
        rider = _bot(rep, "momentum_rider")
        assert rep["trimmed"] and rider["health"] and rider["today"]["result"] == -7.85
        assert len(rider["why_no_trade"]["markets"]) == 10 and rider["why_no_trade"]["markets_trimmed"]
        small = pl.bots_json(_cfg(workdir), up_pulse(), NOW, max_bytes=3_000)
        assert len(small.encode()) <= 3_000 and json.loads(small)

    def test_it_goes_up_with_the_pulse_and_survives_a_clash(self, workdir, monkeypatch, capsys):
        populate(workdir)
        cfg = _cfg(workdir)
        (workdir / "config.json").write_text("{}")
        monkeypatch.setattr(pl.Config, "load", staticmethod(lambda p: cfg))
        monkeypatch.setattr(pl, "take_pulse", lambda c: up_pulse())
        real_upload = ru.upload
        gh = ConflictingGitHub({"reports/live/bots.json": 2})
        gh.files["reports/live/bots.json"] = "{}\n"
        waits: list = []
        monkeypatch.setattr(ru, "upload", lambda files, repo, branch, token, day="": real_upload(
            files, repo, branch, token, opener=gh, day=day, sleep=waits.append))
        assert pl.main(["--config", str(workdir / "config.json")]) == 0
        assert {"reports/live/pulse.json", "reports/live/status.json", "reports/live/uptime.log",
                "reports/live/bots.json"} <= set(gh.files)
        rep = json.loads(gh.files["reports/live/bots.json"])
        assert _bot(rep, "momentum_rider")["why_no_trade"]["scans"] == 120
        assert waits == [0.5, 1.0]                                  # two clashes, retried with a fresh read
        assert "published 4 files" in capsys.readouterr().out
        assert "ghp_" not in gh.files["reports/live/bots.json"]

    def test_a_file_that_still_fails_is_named_and_the_rest_go_up(self, workdir, monkeypatch, capsys):
        populate(workdir)
        cfg = _cfg(workdir)
        (workdir / "config.json").write_text("{}")
        monkeypatch.setattr(pl.Config, "load", staticmethod(lambda p: cfg))
        monkeypatch.setattr(pl, "take_pulse", lambda c: up_pulse())
        real_upload = ru.upload
        gh = ConflictingGitHub({"reports/live/bots.json": 99})
        monkeypatch.setattr(ru, "upload", lambda files, repo, branch, token, day="": real_upload(
            files, repo, branch, token, opener=gh, day=day, sleep=lambda s: None))
        assert pl.main(["--config", str(workdir / "config.json")]) == 1
        out = capsys.readouterr().out
        assert "published 3 of 4 files" in out and "reports/live/bots.json" in out and "after 5 tries" in out
        assert "reports/live/pulse.json" in gh.files and "reports/live/uptime.log" in gh.files
        assert gh.files["reports/live/bots.json"].startswith("written by someone else")   # never ours

    def test_no_token_means_nothing_is_built_or_sent(self, workdir, monkeypatch, capsys):
        cfg = _cfg(workdir)
        (workdir / "config.json").write_text("{}")
        monkeypatch.setattr(pl.Config, "load", staticmethod(lambda p: cfg))
        monkeypatch.setattr(pl, "take_pulse", lambda c: up_pulse())
        gh = FakeGitHub()
        monkeypatch.setattr(ru, "upload", lambda *a, **kw: gh(*a))
        assert pl.main(["--config", str(workdir / "config.json")]) == 0
        assert "no GitHub token" in capsys.readouterr().out and not gh.calls
