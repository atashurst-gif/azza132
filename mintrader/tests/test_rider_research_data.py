"""Rapid Momentum Rider research suite: the data and bookkeeping parts.

Tick store (with a fake broker: hour chunks, resume, retries, the size
budget, the coverage and gap report), the SUCCESSFUL MOMENTUM RUN labeler,
the cost model arithmetic, the trade statistics, the walk-forward folds and
selection, the research registry, publishing with conflict retries and the
verdict rules. Every price here is made up for the test; nothing is a
result."""
import base64
import datetime as dt
import json
import random

import pytest

from mintel.broker.base import Side, Tick
from mintel.rider.config import RiderConfig
from mintel.rider.research import SYNTHETIC_LABEL
from mintel.rider.research.costs import CostModel, trade_money
from mintel.rider.research.labeler import (DEFAULT_SETTINGS, RunSetting, first_reach, label_runs, label_ticks,
                                           run_events, second_bars)
from mintel.rider.research.publish import publish, put_with_retry
from mintel.rider.research.registry import COLUMNS, Registry
from mintel.rider.research.report import verdict
from mintel.rider.research.stats import drawdown, summarise
from mintel.broker.base import TF
from mintel.rider.research.pipeline import same_settings
from mintel.rider.research.tickstore import (DAY_MS, DayTicks, TickStore, day_start_ms, fetch_ticks, ms_to_dt, probe,
                                             weekdays_back)
from mintel.rider.research.variants import VARIANTS, by_id, choose, neighbours
from mintel.rider.research.walkforward import make_folds, select, walk_forward

UTC = dt.timezone.utc
DAY = "2026-10-06"           # a Tuesday
D0 = day_start_ms(DAY)
PIP = 0.0001


# ------------------------------------------------------------ tick store --

class FakeBroker:
    """ticks_range over a scripted tick list; can fail chosen calls."""

    def __init__(self, ticks, fail_starts=(), fail_once=(), fail_big=(), empty_once=()):
        self.ticks = ticks
        self.empty_once = set(empty_once)
        self.calls = []
        self.fail_starts = set(fail_starts)
        self.fail_once = set(fail_once)
        self.fail_big = set(fail_big)

    def ticks_range(self, symbol, start, end):
        self.calls.append((symbol, start, end))
        if start in self.fail_starts:
            raise ConnectionError("bridge timed out")
        if start in self.fail_big and end - start > dt.timedelta(minutes=30):
            raise TimeoutError("too many ticks for one answer")
        if start in self.fail_once:
            self.fail_once.discard(start)
            raise ConnectionError("bridge hiccup")
        if start in self.empty_once:
            self.empty_once.discard(start)
            return []                       # the terminal still syncing its history
        # MetaTrader includes both ends: a tick exactly at `end` comes back twice across chunks
        return [t for t in self.ticks if t.symbol == symbol and start <= t.time <= end]


class Loading(FakeBroker):
    """MetaTrader still loading a past period: the windows starting at
    ``slow`` answer nothing for the first ``empty_first`` asks. It records
    the load requests (the symbol selected, its one-minute bars read)."""

    def __init__(self, ticks, slow=(), empty_first=2):
        super().__init__(ticks)
        self.slow = set(slow)
        self.empty_first = empty_first
        self.asked = {}
        self.selected, self.bar_asks = [], []

    def ticks_range(self, symbol, start, end):
        if start in self.slow:
            self.asked[start] = self.asked.get(start, 0) + 1
            if self.asked[start] <= self.empty_first:
                self.calls.append((symbol, start, end))
                return []
        return super().ticks_range(symbol, start, end)

    def ensure_selected(self, symbol):
        self.selected.append(symbol)
        return True

    def bars(self, symbol, tf, count, end=None):
        self.bar_asks.append((symbol, tf, count, end))
        return []


class LocalTime(FakeBroker):
    """MetaTrader reading the times it is given as local time an hour ahead
    of UTC: every answer is for the hour before the one asked."""

    def ticks_range(self, symbol, start, end):
        return super().ticks_range(symbol, start - dt.timedelta(hours=1), end - dt.timedelta(hours=1))


def scripted_day(symbol="EURUSD", hole=None, every_s=20, day=DAY):
    out = []
    d0 = day_start_ms(day)
    t = d0
    px = 1.08
    rng = random.Random(4)
    while t < d0 + DAY_MS:
        if hole and hole[0] <= t < hole[1]:
            t += every_s * 1000
            continue
        px += rng.gauss(0, 0.00002)
        out.append(Tick(symbol, ms_to_dt(t), round(px, 5), round(px + 0.00002, 5)))
        t += every_s * 1000          # lands exactly on every hour boundary
    return out


class TestTickStore:
    def test_fetch_in_hour_chunks_dedupes_edges_and_stores_compressed(self, tmp_path):
        store = TickStore(tmp_path / "ticks")
        ticks = scripted_day()
        b = FakeBroker(ticks)
        rep = fetch_ticks(b, store, ["EURUSD"], [DAY], now=ms_to_dt(D0 + 2 * DAY_MS), progress=lambda m: None)
        assert len(b.calls) == 24 and rep.days_fetched == 1 and not rep.failed_chunks
        assert all((c[2] - c[1]) == dt.timedelta(hours=1) for c in b.calls)
        path = store.day_path("EURUSD", DAY)
        assert path.exists() and path.name == f"{DAY}.csv.gz" and path.parent.name == "EURUSD"
        got = store.load("EURUSD", DAY)
        assert len(got) == len(ticks)                         # no boundary tick stored twice
        assert list(got.times) == sorted(got.times)
        assert got.bids[0] == ticks[0].bid and got.asks[-1] == ticks[-1].ask
        assert not (store.root / "EURUSD" / ".partial" / DAY).exists()
        assert store.complete("EURUSD", DAY)

    def test_resume_fetches_only_what_is_missing_and_retries(self, tmp_path):
        store = TickStore(tmp_path / "ticks")
        ticks = scripted_day()
        bad = ms_to_dt(D0 + 5 * 3_600_000)
        flaky = ms_to_dt(D0 + 9 * 3_600_000)
        b = FakeBroker(ticks, fail_starts={bad}, fail_once={flaky})
        rep = fetch_ticks(b, store, ["EURUSD"], [DAY], now=ms_to_dt(D0 + 2 * DAY_MS), progress=lambda m: None,
                          sleep=lambda s: None)
        assert rep.days_fetched == 0 and len(rep.failed_chunks) == 1     # the 05:00 hour never came
        assert not store.complete("EURUSD", DAY)                         # a day is never half-stored
        assert rep.chunks_fetched == 23                                  # the flaky hour succeeded on retry
        b2 = FakeBroker(ticks)
        rep2 = fetch_ticks(b2, store, ["EURUSD"], [DAY], now=ms_to_dt(D0 + 2 * DAY_MS), progress=lambda m: None)
        assert [c[1] for c in b2.calls] == [bad]                         # only the missing hour is asked for
        assert rep2.chunks_resumed == 23 and rep2.days_fetched == 1
        assert len(store.load("EURUSD", DAY)) == len(ticks)
        b3 = FakeBroker(ticks)
        rep3 = fetch_ticks(b3, store, ["EURUSD"], [DAY], now=ms_to_dt(D0 + 2 * DAY_MS), progress=lambda m: None)
        assert b3.calls == [] and rep3.days_already_stored == 1          # complete days are never fetched again

    def test_a_chunk_too_big_for_the_bridge_is_fetched_in_quarters(self, tmp_path):
        store = TickStore(tmp_path / "ticks")
        ticks = scripted_day()
        busy = ms_to_dt(D0 + 13 * 3_600_000)
        b = FakeBroker(ticks, fail_big={busy})
        rep = fetch_ticks(b, store, ["EURUSD"], [DAY], now=ms_to_dt(D0 + 2 * DAY_MS), progress=lambda m: None,
                          sleep=lambda s: None)
        assert rep.days_fetched == 1 and not rep.failed_chunks
        quarters = [c for c in b.calls if c[2] - c[1] == dt.timedelta(minutes=15)]
        assert [c[1] for c in quarters] == [busy + dt.timedelta(minutes=15 * k) for k in range(4)]
        assert len(store.load("EURUSD", DAY)) == len(ticks)

    def test_an_empty_answer_in_busy_hours_is_asked_again(self, tmp_path):
        store = TickStore(tmp_path / "ticks")
        ticks = scripted_day()
        h = ms_to_dt(D0 + 9 * 3_600_000)
        b = FakeBroker(ticks, empty_once={h})
        rep = fetch_ticks(b, store, ["EURUSD"], [DAY], now=ms_to_dt(D0 + 2 * DAY_MS), progress=lambda m: None,
                          sleep=lambda s: None)
        assert rep.empty_retried == 1 and [c[1] for c in b.calls].count(h) == 2
        assert len(store.load("EURUSD", DAY)) == len(ticks)

    def test_incomplete_day_and_size_budget(self, tmp_path):
        store = TickStore(tmp_path / "ticks")
        b = FakeBroker(scripted_day())
        rep = fetch_ticks(b, store, ["EURUSD"], [DAY], now=ms_to_dt(D0 + 3_600_000), progress=lambda m: None)
        assert b.calls == [] and "not a complete day" in rep.notes[0]
        rep = fetch_ticks(b, store, ["EURUSD"], [DAY], now=ms_to_dt(D0 + 2 * DAY_MS), max_bytes=1,
                          progress=lambda m: None)
        assert rep.budget_hit and "size budget" in rep.notes[-1]
        assert len(b.calls) <= 1 and not store.complete("EURUSD", DAY)

    def test_coverage_reports_gaps_and_insufficient_history(self, tmp_path):
        store = TickStore(tmp_path / "ticks")
        hole = (D0 + 10 * 3_600_000, D0 + 10 * 3_600_000 + 45 * 60_000)          # 45 minutes at 10:00 UTC
        b = FakeBroker(scripted_day("EURUSD") + scripted_day("GBPUSD", hole=hole))
        fetch_ticks(b, store, ["EURUSD", "GBPUSD"], [DAY], now=ms_to_dt(D0 + 2 * DAY_MS), progress=lambda m: None)
        e = store.entry("GBPUSD", DAY)
        assert e["max_session_gap_s"] == pytest.approx(45 * 60 + 20, abs=21)
        assert e["gaps"][0]["start_utc"].startswith("09:59") or e["gaps"][0]["start_utc"].startswith("10:")
        cov = store.coverage(["EURUSD", "GBPUSD"], [DAY, "2026-10-07"], min_days=5, min_ticks=1000,
                             max_gap_minutes=30, min_symbol_fraction=0.5)
        rows = {(r.symbol, r.day): r for r in cov.rows}
        assert rows[("EURUSD", DAY)].usable
        assert not rows[("GBPUSD", DAY)].usable and "hole" in rows[("GBPUSD", DAY)].why
        assert rows[("EURUSD", "2026-10-07")].why == "no ticks stored"
        assert cov.usable_days == [DAY]
        assert not cov.sufficient and "History insufficient" in cov.statement()
        assert cov.to_dict()["sufficient"] is False

    # ---- 9 Oct: 3,456 past hour chunks came back with ONE tick in total, and no error ----

    def test_an_empty_past_hour_is_loaded_and_asked_again_after_short_waits(self, tmp_path):
        store = TickStore(tmp_path / "ticks")
        ticks = scripted_day()
        slow = [ms_to_dt(D0 + 9 * 3_600_000), ms_to_dt(D0 + 13 * 3_600_000)]
        b = Loading(ticks, slow=slow, empty_first=2)
        waits = []
        rep = fetch_ticks(b, store, ["EURUSD"], [DAY], now=ms_to_dt(D0 + 2 * DAY_MS), progress=lambda m: None,
                          sleep=waits.append)
        assert rep.days_fetched == 1 and len(store.load("EURUSD", DAY)) == len(ticks)
        assert rep.empty_retried == 2 and not rep.empty_busy_chunks
        assert waits == [1.0, 2.0, 1.0, 2.0]                      # short waits, and only for the slow hours
        assert [c[1] for c in b.calls].count(slow[0]) == 3
        assert b.selected == ["EURUSD", "EURUSD"]                 # MetaTrader asked to load each slow hour ...
        assert [(a[1], a[3]) for a in b.bar_asks] == [(TF.M1, s + dt.timedelta(hours=1)) for s in slow]  # ... bars

    def test_a_busy_hour_that_stays_empty_is_never_stored_and_the_next_run_asks_again(self, tmp_path):
        store = TickStore(tmp_path / "ticks")
        ticks = scripted_day()
        nine = ms_to_dt(D0 + 9 * 3_600_000)
        b = Loading(ticks, slow=[nine], empty_first=99)
        waits, said = [], []
        rep = fetch_ticks(b, store, ["EURUSD"], [DAY], now=ms_to_dt(D0 + 2 * DAY_MS), progress=said.append,
                          sleep=waits.append)
        assert rep.days_fetched == 0 and not store.complete("EURUSD", DAY)
        assert waits == [1.0, 2.0, 4.0, 8.0]                      # asked five times over about 15 s
        assert rep.empty_busy_chunks == [{"symbol": "EURUSD", "day": DAY, "start_utc": nine.isoformat(), "asked": 5,
                                          "bars_in_hour": 0}]
        assert any("not stored as final; the next run asks again" in m for m in said)
        assert not (store.partial_dir("EURUSD", DAY) / "0900.csv.gz").exists()   # the hours that came are kept
        assert len(list(store.partial_dir("EURUSD", DAY).glob("*.csv.gz"))) == 23
        b2 = FakeBroker(ticks)                                    # the next run: MetaTrader has it now
        rep2 = fetch_ticks(b2, store, ["EURUSD"], [DAY], now=ms_to_dt(D0 + 2 * DAY_MS), progress=lambda m: None)
        assert [c[1] for c in b2.calls] == [nine] and rep2.days_fetched == 1
        assert len(store.load("EURUSD", DAY)) == len(ticks)

    def test_an_empty_quiet_hour_or_weekend_is_what_the_market_did(self, tmp_path):
        store = TickStore(tmp_path / "ticks")
        hole = (D0 + 21 * 3_600_000, D0 + 22 * 3_600_000)         # 21:00-22:00 UTC: outside the busy hours
        waits = []
        rep = fetch_ticks(FakeBroker(scripted_day(hole=hole)), store, ["EURUSD"], [DAY, "2026-10-10"],
                          now=ms_to_dt(D0 + 6 * DAY_MS), progress=lambda m: None, sleep=waits.append)
        assert rep.days_fetched == 2 and waits == [] and not rep.empty_busy_chunks
        assert store.complete("EURUSD", "2026-10-10") and len(store.load("EURUSD", "2026-10-10")) == 0   # Saturday

    def test_no_history_at_all_stops_the_fetch_after_three_market_days_and_says_how_to_check(self, tmp_path):
        store = TickStore(tmp_path / "ticks")
        b = FakeBroker([])
        waits, said = [], []
        days = ["2026-10-05", "2026-10-06", "2026-10-07", "2026-10-08", "2026-10-09"]
        rep = fetch_ticks(b, store, ["EURUSD"], days, now=ms_to_dt(D0 + 9 * DAY_MS), progress=said.append,
                          sleep=waits.append)
        asked = sorted({c[1].date().isoformat() for c in b.calls})
        assert asked == days[:3] and rep.stopped_no_history and rep.days_fetched == 0
        assert "--probe" in rep.notes[-1] and "Nothing empty was stored" in rep.notes[-1]
        assert sum(waits) == pytest.approx(3 * (15.0 + 18 * 1.0))   # the full wait once a day, then one quick try
        assert not any(store.complete("EURUSD", d) for d in days)

    def test_a_day_the_old_fetch_stored_as_complete_with_empty_busy_hours_is_fetched_again(self, tmp_path):
        """Before 9 Oct the fetch took MetaTrader's empty answers as final and
        stored whole weekdays as complete with about no tick in them. Any
        later fetch (not only the Mac launcher's one-off wipe) asks again."""
        store = TickStore(tmp_path / "ticks")
        ticks = scripted_day()
        one = [t for t in ticks if t.time == ms_to_dt(D0 + 22 * 3_600_000)]          # one tick, outside busy hours
        store.write_day("EURUSD", DAY, [D0 + 22 * 3_600_000], [one[0].bid], [one[0].ask], complete=True,
                        source="broker.ticks_range")                                  # as the old fetch wrote it
        good = scripted_day(day="2026-10-05")
        store.write_day("EURUSD", "2026-10-05", [int(t.time.timestamp() * 1000) for t in good],
                        [t.bid for t in good], [t.ask for t in good], complete=True, source="broker.ticks_range")
        b = FakeBroker(ticks + good)
        said = []
        rep = fetch_ticks(b, store, ["EURUSD"], ["2026-10-05", DAY], now=ms_to_dt(D0 + 2 * DAY_MS),
                          progress=said.append, sleep=lambda s: None)
        assert {c[1].date().isoformat() for c in b.calls} == {DAY}     # the good old day is not asked for again
        assert rep.days_refetched == 1 and rep.days_already_stored == 1 and rep.days_fetched == 1
        assert any("19 busy hours hold no tick" in m for m in said)
        assert len(store.load("EURUSD", DAY)) == len(ticks)
        assert store.entry("EURUSD", DAY)["busy_hours_checked"] is True
        assert store.entry("EURUSD", "2026-10-05")["busy_hours_empty"] == 0              # worked out once, kept
        b2 = FakeBroker(ticks + good)
        rep2 = fetch_ticks(b2, store, ["EURUSD"], ["2026-10-05", DAY], now=ms_to_dt(D0 + 2 * DAY_MS),
                           progress=lambda m: None)
        assert b2.calls == [] and rep2.days_already_stored == 2 and rep2.days_refetched == 0

    def test_the_old_day_is_kept_until_the_new_fetch_is_complete_and_an_old_empty_chunk_is_asked_again(self, tmp_path):
        store = TickStore(tmp_path / "ticks")
        ticks = scripted_day()
        store.write_day("EURUSD", DAY, [], [], [], complete=True, source="broker.ticks_range")
        quiet = [t for t in ticks if not 1 <= t.time.hour < 20]      # MetaTrader still sends no busy hour
        rep = fetch_ticks(FakeBroker(quiet), store, ["EURUSD"], [DAY], now=ms_to_dt(D0 + 2 * DAY_MS),
                          progress=lambda m: None, sleep=lambda s: None)
        assert rep.days_refetched == 1 and rep.days_fetched == 0 and rep.empty_busy_chunks
        assert store.complete("EURUSD", DAY) and len(store.load("EURUSD", DAY)) == 0    # nothing better yet: kept
        nine = store.partial_dir("EURUSD", DAY) / "0900.csv.gz"
        TickStore._write_csv(nine, [], [], [])                       # an empty busy hour an old fetch left half-way
        rep2 = fetch_ticks(FakeBroker(ticks), store, ["EURUSD"], [DAY], now=ms_to_dt(D0 + 2 * DAY_MS),
                           progress=lambda m: None)
        assert rep2.days_fetched == 1 and len(store.load("EURUSD", DAY)) == len(ticks)
        assert store.busy_hours_empty("EURUSD", DAY) == 0

    def test_an_answer_for_the_wrong_hour_is_measured_and_corrected(self, tmp_path):
        """The bridge gives MetaTrader times with no time zone; MetaTrader on
        the Mac can read them as UK local time (BST, an hour ahead of UTC), so
        the answer for 10:00-11:00 holds 09:00-10:00 - and the ticks outside
        the hour are dropped. The lag is measured and corrected."""
        store = TickStore(tmp_path / "ticks")
        ticks = scripted_day(day="2026-10-05") + scripted_day()
        b = LocalTime(ticks)
        said = []
        rep = fetch_ticks(b, store, ["EURUSD"], [DAY], now=ms_to_dt(D0 + 2 * DAY_MS), progress=said.append,
                          sleep=lambda s: None)
        assert rep.days_fetched == 1 and not rep.failed_chunks and rep.time_shift_minutes == 60.0
        got = store.load("EURUSD", DAY)
        mine = [t for t in ticks if D0 <= int(t.time.timestamp() * 1000) < D0 + DAY_MS]
        assert len(got) == len(mine) and got.times[0] == D0 and got.bids[0] == mine[0].bid
        note = next(m for m in said if "60 minutes earlier than asked" in m)
        # the code fix is already in mt5_adapter; what is left is a bridge still running the old copy
        assert "already in mintel/broker/mt5_adapter.py" in note and "restart the bridge" in note
        assert "No code needs to change" in note and "lasting fix" not in note

    def test_the_probe_says_plainly_whether_past_ticks_can_be_read(self):
        now = dt.datetime(2026, 10, 7, 12, 30, tzinfo=UTC)
        both = scripted_day() + scripted_day(day="2026-10-07")
        res = probe(FakeBroker(both), "EURUSD", now, sleep=lambda s: None)
        assert res["reachable"] and [w["window"] for w in res["windows"]] == ["recent", "past"]
        text = "\n".join(res["lines"])
        assert "EURUSD 2026-10-06 10:00-11:00 UTC - past history" in text
        assert "MetaTrader returned 181 ticks (first 10:00:00.000, last 11:00:00.000 UTC)" in text
        assert "kept for the hour: 180 ticks, first 10:00:00.000, last 10:59:40.000 UTC" in text
        assert res["lines"][-1] == "VERDICT: past ticks ARE reachable."
        dead = probe(FakeBroker([]), "EURUSD", now, sleep=lambda s: None)
        assert not dead["reachable"] and dead["lines"][-1].startswith("VERDICT: past ticks are NOT reachable")
        assert "MetaTrader returned 0 ticks, asked 5 times" in "\n".join(dead["lines"])
        past = ms_to_dt(D0 + 10 * 3_600_000)
        loading = probe(Loading(both, slow=[past], empty_first=99), "EURUSD", now, sleep=lambda s: None)
        assert not loading["reachable"] and "the recent hour works and the past hour does not" in loading["lines"][-1]
        lagged = probe(LocalTime(both), "EURUSD", now, sleep=lambda s: None)
        assert lagged["reachable"] and lagged["time_shift_minutes"] == 60.0
        assert "asked 60 minutes later" in lagged["lines"][-1]
        assert any("restart the bridge" in line for line in lagged["lines"])        # the probe says the same

    def test_load_span_crosses_days_and_weekdays(self, tmp_path):
        store = TickStore(tmp_path / "t")
        for day, px in (("2026-10-05", 1.0), ("2026-10-06", 2.0)):
            d0 = day_start_ms(day)
            store.write_day("EURUSD", day, [d0 + 1000, d0 + DAY_MS - 1000], [px, px], [px + 0.1, px + 0.1])
        span = store.load_span("EURUSD", day_start_ms("2026-10-05") + 5000, day_start_ms("2026-10-06") + 5000)
        assert list(span.bids) == [1.0, 2.0]
        assert weekdays_back(dt.date(2026, 10, 12), 3) == ["2026-10-08", "2026-10-09", "2026-10-12"]


# --------------------------------------------------------------- labeler --

def mk(path, symbol="EURUSD", spread_pips=0.2):
    """[(seconds after midnight, mid in pips from 1.1)] -> DayTicks."""
    d = DayTicks(symbol)
    for sec, mp in path:
        mid = 1.1 + mp * PIP
        d.times.append(D0 + int(round(sec * 1000)))
        d.bids.append(mid - spread_pips * PIP / 2)
        d.asks.append(mid + spread_pips * PIP / 2)
    return d


class TestLabeler:
    def test_first_reach_matches_brute_force(self):
        rng = random.Random(1)
        for _ in range(30):
            n = rng.randint(1, 60)
            vals = [rng.choice([float("-inf"), rng.uniform(0, 10)]) for _ in range(n)]
            starts = sorted(rng.sample(range(n), rng.randint(1, n)))
            th = [rng.uniform(0, 11) for _ in starts]
            got = first_reach(vals, th, starts)
            want = [next((j for j in range(s + 1, n) if vals[j] >= t), n) for s, t in zip(starts, th)]
            assert got == want

    def test_long_run_spread_paid_and_short_not(self):
        st = RunSetting(10, 5, 300)
        path = [(0, 0.0)] + [(1 + k, 0.5 * (k + 1)) for k in range(30)]       # +15 pips in 30 s, no dip
        bars = second_bars(mk(path), D0, 400)
        lo = label_runs(bars, st, PIP, 1, start_range=(0, 60))
        sh = label_runs(bars, st, PIP, -1, start_range=(0, 60))
        assert lo.ok[0] == 1 and sh.ok[0] == 0
        # bought at the ask (+0.1), sold at the bid (-0.1): +10 needs mid +10.2 -> second 21
        assert lo.t_fav[0] == 21
        assert lo.ok[25] == 0                                            # too little left after second 25

    def test_adverse_first_and_horizon_and_same_second_ambiguity(self):
        st = RunSetting(10, 5, 60)
        dip = [(0, 0.0), (5, -6.0), (10, 12.0)]                          # adverse 5 hit before +10
        assert label_runs(second_bars(mk(dip), D0, 100), st, PIP, 1, start_range=(0, 1)).ok[0] == 0
        slow = [(0, 0.0), (90, 12.0)]                                    # +10 only after the 60 s horizon
        assert label_runs(second_bars(mk(slow), D0, 200), st, PIP, 1, start_range=(0, 1)).ok[0] == 0
        both = [(0, 0.0), (5.2, -6.0), (5.6, 12.0)]                      # both inside second 6: order unknown
        assert label_runs(second_bars(mk(both), D0, 100), st, PIP, 1, start_range=(0, 1)).ok[0] == 0
        # the tick-level check resolves the order exactly
        good = [(0, 0.0), (5.2, 12.0), (5.6, -6.0)]
        r = label_ticks(mk(good), D0, 1, st, PIP, use_last_touch=True)
        assert r["ok"] is True and r["seconds"] == pytest.approx(5.2)
        r = label_ticks(mk(both), D0, 1, st, PIP, use_last_touch=True)
        assert r["ok"] is False

    def test_latency_entry_and_slippage(self):
        st = RunSetting(10, 5, 300)
        path = [(0, 0.0), (0.3, 1.0), (1.0, 4.0), (20, 14.4)]
        r0 = label_ticks(mk(path), D0, 1, st, PIP, use_last_touch=True)
        r1 = label_ticks(mk(path), D0 + 250, 1, st, PIP, slippage_pips=0.2)
        assert r0["ok"] and r0["fill"] == pytest.approx(1.1 + 0.1 * PIP)
        assert r1["fill_ms"] == D0 + 300 and r1["fill"] == pytest.approx(1.1 + 1.3 * PIP)
        assert r1["ok"] is True
        r2 = label_ticks(mk(path), D0 + 600, 1, st, PIP, slippage_pips=0.2)     # filled at +4.3: needs +14.3
        assert r2["ok"] is True
        r3 = label_ticks(mk(path), D0 + 600, 1, st, PIP, slippage_pips=0.5)     # +4.6 needs +14.6: not reached
        assert r3["ok"] is False

    def test_run_events_are_distinct_runs(self):
        st = RunSetting(5, 2.5, 120)
        path = [(0, 0.0)] + [(1 + k, 0.4 * (k + 1)) for k in range(40)] + [(200, 16.0)] + \
               [(201 + k, 16.0 + 0.4 * (k + 1)) for k in range(40)]
        bars = second_bars(mk(path), D0, 400)
        ev = run_events(label_runs(bars, st, PIP, 1, start_range=(0, 300)))
        assert [e.second for e in ev][:1] == [0]
        assert len(ev) == 2 and ev[1].second >= 190                      # one per leg, not one per second
        assert all(e.side == 1 for e in ev)

    def test_default_settings_cover_the_brief(self):
        assert [s.favourable_pips for s in DEFAULT_SETTINGS] == [5, 10, 20, 30, 50]
        assert all(s.adverse_pips < s.favourable_pips for s in DEFAULT_SETTINGS)
        assert "within 5 min" in DEFAULT_SETTINGS[1].name


# ------------------------------------------------------------------ costs --

class TestCosts:
    def test_spread_stress_keeps_the_mid(self):
        c = CostModel(slippage_pips=0.2).stressed(1.5)
        t = c.tick("EURUSD", ms_to_dt(D0), 1.10000, 1.10010)
        assert (t.bid + t.ask) / 2 == pytest.approx(1.10005)
        assert t.ask - t.bid == pytest.approx(0.00015)
        assert c.slip(PIP) == pytest.approx(0.3 * PIP) and c.label == "1.5x spread and slippage"
        assert CostModel().delayed(1000).latency_ms == 1000.0

    def test_fills_pay_the_spread_and_slippage_and_stops_gap(self):
        c = CostModel(slippage_pips=0.2)
        t = Tick("EURUSD", ms_to_dt(D0), 1.10000, 1.10010)
        assert c.entry_fill(Side.BUY, t, PIP) == pytest.approx(1.10012)
        assert c.entry_fill(Side.SELL, t, PIP) == pytest.approx(1.09998)
        assert c.exit_fill(Side.BUY, t, PIP) == pytest.approx(1.09998)
        assert c.exit_fill(Side.SELL, t, PIP) == pytest.approx(1.10012)
        assert c.stop_touched(Side.BUY, 1.10000, t) and not c.stop_touched(Side.BUY, 1.09990, t)
        assert c.stop_fill(Side.BUY, 1.10000, t, PIP) == pytest.approx(1.09998)        # at the stop
        gap = Tick("EURUSD", ms_to_dt(D0), 1.09900, 1.09910)
        assert c.stop_fill(Side.BUY, 1.10000, gap, PIP) == pytest.approx(1.09898)      # a gap fills worse
        assert c.stop_fill(Side.SELL, 1.09950, t, PIP) == pytest.approx(1.10012)

    def test_money_arithmetic(self):
        # long 0.14 lots at GBP 1.036/pip: +12.0 pips gross, commission 2 x 2.59 x 0.14
        m = trade_money(Side.BUY, 1.10012, 1.10132, PIP, 1.036, 0.14, 2.59)
        assert m["gross_pips"] == pytest.approx(12.0)
        assert m["commission_gbp"] == pytest.approx(0.7252)
        assert m["net_money"] == pytest.approx(12.0 * 1.036 - 0.7252)
        assert m["net_pips"] == pytest.approx((12.0 * 1.036 - 0.7252) / 1.036)
        s = trade_money(Side.SELL, 150.00, 150.05, 0.01, 0.99, 0.2, 2.59)
        assert s["gross_pips"] == pytest.approx(-5.0) and s["net_money"] < -5.0 * 0.99


# ------------------------------------------------------------------ stats --

def tr(day, net, mfe=5.0, gross=None, exit_ms=0, sym="EURUSD", fam="breakout", reason="STOP"):
    return {"day": day, "net_pips": net, "net_money": net, "gross_pips": net if gross is None else gross,
            "mfe_pips": mfe, "mae_pips": 1.0, "exit_ms": exit_ms, "symbol": sym, "family": fam, "exit_reason": reason}


class TestStats:
    def test_summary_figures(self):
        trades = [tr("d1", 10, 12, exit_ms=1), tr("d1", -4, 2, exit_ms=2), tr("d2", -6, 1, exit_ms=3),
                  tr("d2", 8, 10, exit_ms=4)]
        s = summarise(trades)
        assert s["trades"] == 4 and s["wins"] == 2 and s["win_rate"] == 0.5
        assert s["net_pips"] == 8 and s["expectancy_pips"] == 2.0
        assert s["profit_factor"] == pytest.approx(18 / 10)
        assert s["avg_win_pips"] == 9 and s["avg_loss_pips"] == -5
        assert s["max_drawdown_pips"] == 10                              # +10, +6, 0: a 10-pip fall
        assert s["mfe_capture"] == pytest.approx(8 / 25, abs=1e-3)
        assert s["trades_per_day"] == 2.0
        assert summarise([])["win_rate"] is None
        assert drawdown([1, -3, 2, -5]) == 6

    def test_no_losses_has_no_profit_factor_number(self):
        s = summarise([tr("d1", 3)])
        assert s["profit_factor"] is None and s["profit_factor_note"] == "no losing trades"


# ---------------------------------------------------------- walk-forward --

class TestWalkForward:
    def test_folds_do_not_overlap_and_are_purged(self):
        days = [f"2026-09-{d:02d}" for d in range(1, 11)]
        folds = make_folds(days, is_min=4, oos=2, gap=1)
        assert len(folds) == 2
        assert folds[0].oos_days == days[5:7] and folds[0].gap_days == [days[4]]
        assert folds[1].oos_days == days[8:10] and folds[1].gap_days == [days[7]]
        rng = random.Random(3)
        for _ in range(200):
            n = rng.randint(0, 30)
            ds = [f"d{k:03d}" for k in range(n)]
            is_min, oos, gap = rng.randint(1, 6), rng.randint(1, 4), rng.randint(1, 3)
            fs = make_folds(ds, is_min, oos, gap)
            seen_oos = set()
            for f in fs:
                assert f.is_days and f.oos_days and len(f.gap_days) == gap
                assert max(f.is_days) < min(f.gap_days) <= max(f.gap_days) < min(f.oos_days)    # purged, in order
                assert not set(f.is_days) & set(f.oos_days) and not set(f.gap_days) & set(f.oos_days)
                assert not seen_oos & set(f.oos_days)                                           # never OOS twice
                seen_oos |= set(f.oos_days)
        assert make_folds(days[:6], 4, 2, 1) == []

    def test_selection_prefers_a_stable_range_over_a_lone_spike(self):
        v = {x.method_id: x for x in VARIANTS}
        per = {"RMR-BASE": -4.0, "RMR-TRIG-55": 6.0, "RMR-TRIG-65": 3.0, "RMR-CURVE-LOOSE": 2.5,
               "RMR-CURVE-TIGHT": 2.5}
        days = ["a", "b"]
        trades = {k: [tr(days[i % 2], x) for i in range(30)] for k, x in per.items()}
        grid = [x for x in VARIANTS if x.grid]
        chosen, why, rows = select(trades, grid, days, min_trades=20)
        assert chosen != "RMR-TRIG-55"                       # the raw best is a lone spike next to a loser
        best_raw = max(per, key=per.get)
        assert best_raw == "RMR-TRIG-55"
        assert chosen == "RMR-BASE" and "neighbours" in why
        assert {r["variant"] for r in rows} == set(per)
        assert neighbours(v["RMR-TRIG-55"], grid) == [v["RMR-BASE"]]
        assert len(neighbours(v["RMR-BASE"], grid)) == 4
        chosen2, why2, _ = select(trades, grid, days, min_trades=1000)
        assert chosen2 == "RMR-BASE" and "kept the baseline" in why2

    def test_result_is_out_of_sample_only(self):
        days = [f"2026-09-{d:02d}" for d in range(1, 11)]
        good_is = {d: 5.0 for d in days[:5]}
        trades = {}
        for x in VARIANTS:
            if not x.grid:
                continue
            val_is = 5.0 if x.method_id == "RMR-TRIG-65" else 0.5
            trades[x.method_id] = ([tr(d, val_is) for d in days[:8] for _ in range(5)] +
                                   [tr(d, -1.0) for d in days[8:] for _ in range(5)])
        wf = walk_forward(trades, [x for x in VARIANTS if x.grid], days, is_min=4, oos=2, gap=1, min_trades=5)
        assert wf["enough_days"] and len(wf["folds"]) == 2
        assert set(wf["oos_days"]) == set(days[5:7] + days[8:10])
        assert all(t["day"] in wf["oos_days"] for t in wf["oos_trades"])
        assert wf["oos_result"]["trades"] == 20
        assert wf["folds"][1]["out_of_sample_result"]["net_pips"] < 0       # the later days' losses are reported
        assert good_is
        short = walk_forward(trades, [x for x in VARIANTS if x.grid], days[:5])
        assert not short["enough_days"] and "Not enough days" in short["statement"]


# -------------------------------------------------------------- variants --

class TestVariants:
    def test_variant_configs(self):
        base = RiderConfig()
        assert by_id("RMR-TRIG-55").config(base).trigger_score == 55.0
        loose = by_id("RMR-CURVE-LOOSE").config(RiderConfig.from_dict({"flowlock": {"decay_exit": 0.9}}))
        assert loose.flowlock == {"decay_exit": 0.9, "curve": "loose"}
        assert by_id("RMR-CHOP-OFF").config(base).max_chop > 1.0 and not by_id("RMR-CHOP-OFF").grid
        assert by_id("RMR-BASE").config(base).to_dict() == base.to_dict()
        assert [v.method_id for v in choose("default")][0] == "RMR-BASE"
        assert [v.method_id for v in choose("RMR-TRIG-65")] == ["RMR-BASE", "RMR-TRIG-65"]
        assert len(choose("all")) == 13
        with pytest.raises(ValueError):
            choose("NOPE")

    def test_the_guard_pair_keeps_the_baseline_as_it_is(self):
        """RMR-GUARDS-ON / RMR-GUARDS-OFF force the 9 Oct guards on and off
        with everything else as the baseline; RMR-BASE stays "the defaults
        plus rider.json" (no overrides of its own)."""
        base = RiderConfig()
        on, off = by_id("RMR-GUARDS-ON").config(base), by_id("RMR-GUARDS-OFF").config(base)
        assert on.currency_cluster_guard and on.loss_cooldown_guard
        assert not off.currency_cluster_guard and not off.loss_cooldown_guard
        assert by_id("RMR-BASE").overrides == {} and by_id("RMR-BASE").config(base).to_dict() == base.to_dict()
        rest = lambda c: {k: v for k, v in c.to_dict().items()                          # noqa: E731
                          if k not in ("currency_cluster_guard", "loss_cooldown_guard")}
        assert rest(on) == rest(off) == rest(base)
        assert not by_id("RMR-GUARDS-ON").grid and not by_id("RMR-GUARDS-OFF").grid      # not walk-forward picks
        ids = [v.method_id for v in choose("default")]
        assert "RMR-GUARDS-ON" in ids and "RMR-GUARDS-OFF" in ids
        # with the shipped defaults RMR-GUARDS-ON is RMR-BASE's settings: replayed once
        assert same_settings(choose("default"), base) == {**{i: i for i in ids}, "RMR-GUARDS-ON": "RMR-BASE"}
        aaron = RiderConfig.from_dict({"loss_cooldown_guard": False})                   # rider.json switched one off
        assert same_settings(choose("default"), aaron)["RMR-GUARDS-ON"] == "RMR-GUARDS-ON"
        assert same_settings(choose("default"), aaron)["RMR-GUARDS-OFF"] == "RMR-GUARDS-OFF"


# -------------------------------------------------------------- registry --

class TestRegistry:
    def test_round_trip(self, tmp_path):
        p = tmp_path / "registry.sqlite"
        reg = Registry(p)
        reg.seed([{"method_id": v.method_id, "description": v.description, "hypothesis": v.hypothesis,
                   "params": v.params(RiderConfig(max_cost_ratio=0.25))} for v in VARIANTS])
        rid = reg.record({"method_id": "RMR-TRIG-55", "data_source": "SYNTHETIC", "markets": "EURUSD,GBPUSD",
                          "period": "2026-10-01 to 2026-10-02 (2 days)", "trades": 42, "win_rate": 0.45,
                          "expectancy": -0.31, "profit_factor": 0.88, "drawdown": 25.5, "mfe_capture": 0.41,
                          "cost_sensitivity": "1.5x: -0.6 pips a trade; FRAGILE", "oos_result": "12 trades, -3.0 pips",
                          "fragile": True, "notes": SYNTHETIC_LABEL})
        with pytest.raises(KeyError):
            reg.record({"method_id": "NOT-SEEDED", "data_source": "REAL", "markets": "", "period": "", "trades": 0})
        reg.close()
        reg = Registry(p)                                             # it is on disk
        rows = reg.rows("RMR-TRIG-55")
        assert len(rows) == 1 and rows[0]["RUN_ID"] == rid
        r = rows[0]
        for col in COLUMNS:
            assert col in r
        assert r["TRADES"] == 42 and r["WIN RATE"] == 0.45 and r["EXPECTANCY"] == -0.31
        assert r["PROFIT FACTOR"] == 0.88 and r["DRAWDOWN"] == 25.5 and r["MFE CAPTURE"] == 0.41
        assert "FRAGILE" in r["COST SENSITIVITY"] and r["OUT-OF-SAMPLE RESULT"].startswith("12 trades")
        assert r["DATA_SOURCE"] == "SYNTHETIC" and "Trigger at score 55" in r["DESCRIPTION"]
        assert len(reg.methods()) == len(VARIANTS) and reg.run_count() == 1
        reg.seed([{"method_id": "RMR-TRIG-55", "description": "changed", "hypothesis": "h"}])
        assert len(reg.methods()) == len(VARIANTS) and reg.rows("RMR-TRIG-55")[0]["DESCRIPTION"] == "changed"
        assert reg.rows(data_source="REAL") == []
        reg.close()


# ------------------------------------------------------------- publishing --

class FakeGitHub:
    def __init__(self, put_codes, existing=None):
        self.repo = "owner/repo"
        self.put_codes = list(put_codes)
        self.existing = existing
        self.calls = []
        self.sha = 0

    def call(self, method, path, body=None):
        self.calls.append((method, path, body))
        if method == "GET":
            if self.existing is None:
                return 404, {}
            self.sha += 1
            return 200, {"sha": f"sha{self.sha}", "content": base64.b64encode(self.existing.encode()).decode()}
        code = self.put_codes.pop(0)
        return code, {"message": "conflict" if code != 201 else ""}


class TestPublish:
    def test_conflicts_reread_the_sha_and_retry(self):
        gh = FakeGitHub([409, 422, 201], existing="old")
        sleeps = []
        assert put_with_retry(gh, "reports", "reports/rider-research/x/report.md", "new", "m",
                              sleep=sleeps.append) == "written"
        puts = [c for c in gh.calls if c[0] == "PUT"]
        gets = [c for c in gh.calls if c[0] == "GET"]
        assert len(puts) == 3 and len(gets) == 3 and len(sleeps) == 2
        assert [p[2]["sha"] for p in puts] == ["sha1", "sha2", "sha3"]   # a fresh sha each time
        assert base64.b64decode(puts[-1][2]["content"]).decode() == "new"

    def test_gives_up_after_five_and_skips_unchanged(self):
        gh = FakeGitHub([409] * 5, existing=None)
        with pytest.raises(RuntimeError):
            put_with_retry(gh, "reports", "p", "x", "m", attempts=5, sleep=lambda s: None)
        assert sum(1 for c in gh.calls if c[0] == "PUT") == 5
        same = FakeGitHub([], existing="same")
        assert put_with_retry(same, "reports", "p", "same", "m", sleep=lambda s: None) == "unchanged"
        gh = FakeGitHub([500])
        with pytest.raises(RuntimeError):
            put_with_retry(gh, "reports", "p", "x", "m", sleep=lambda s: None)       # not a conflict: no retry
        assert sum(1 for c in gh.calls if c[0] == "PUT") == 1

    def test_publish_uses_the_token_from_secrets_and_the_reports_folder(self, tmp_path):
        cfgp = tmp_path / "config.json"
        cfgp.write_text(json.dumps({"report_repo": "o/r", "report_branch": "reports"}))
        (tmp_path / "secrets.json").write_text(json.dumps({"github_token": "tok"}))
        seen = []

        def opener(method, url, body):
            seen.append((method, url))
            if method == "GET" and url.endswith("/branches/reports"):
                return 200, {}
            if method == "GET":
                return 404, {}
            return 201, {}
        paths = publish({"report.md": "# r", "summary.json": "{}"}, str(cfgp), "2026-10-09", opener=opener,
                        sleep=lambda s: None)
        assert paths == ["reports/rider-research/2026-10-09/report.md",
                         "reports/rider-research/2026-10-09/summary.json"]
        assert any("/contents/reports/rider-research/2026-10-09/report.md" in u for _, u in seen)
        (tmp_path / "secrets.json").write_text("{}")
        with pytest.raises(RuntimeError):
            publish({"a": "b"}, str(cfgp), "x", opener=opener)


# --------------------------------------------------------------- verdict --

class TestVerdict:
    def s(self, n, net):
        return {"trades": n, "net_pips": net, "expectancy_pips": net / n if n else None}

    def test_rules(self):
        wf_ok = {"enough_days": True, "oos_result": self.s(40, 12.0)}
        assert verdict(self.s(100, 50), self.s(100, 20), wf_ok, True, "")["code"] == "PASSED"
        v = verdict(self.s(100, 50), self.s(100, -5), wf_ok, True, "")
        assert v["code"] == "FRAGILE" and v["fragile"] and "FRAGILE" in v["flags"]
        assert verdict(self.s(100, -50), self.s(100, -90), wf_ok, True, "")["code"] == "NO_EDGE"
        assert verdict(self.s(10, 50), None, wf_ok, True, "")["code"] == "INSUFFICIENT_DATA"
        v = verdict(self.s(100, 50), self.s(100, 20), wf_ok, False, "History insufficient: 2 usable day(s)")
        assert v["code"] == "INSUFFICIENT_DATA" and "History insufficient" in v["text"]
        assert verdict(self.s(100, 50), self.s(100, 20), {"enough_days": True, "oos_result": self.s(40, -3)},
                       True, "")["code"] == "NOT_CONFIRMED_OUT_OF_SAMPLE"
        assert verdict(self.s(100, 50), self.s(100, 20), {"enough_days": False}, True, "")["code"] == "UNPROVEN"
        syn = verdict(self.s(100, 50), self.s(100, -5), wf_ok, True, "", synthetic=True)
        assert syn["code"] == "SYNTHETIC" and syn["text"].startswith(SYNTHETIC_LABEL)
        assert "FRAGILE" in syn["pipeline_check"]["text"] and "meaningless for money" in syn["pipeline_check"]["text"]
