"""The lab's fetch: forward top-up, backward backfill, gently.

Driven by the simulated broker through a thin adapter (a duck-typed feed with
``bars``, ``symbols``, ``spec`` and ``ping``) that logs every call with the
fake clock's time, so the pacing, the window, the budget and the paging can be
checked exactly.
"""
from __future__ import annotations

import datetime as dt

import pytest

from mintel.broker.base import TF
from mintel.broker.sim import SimBroker
from mintel.clock import TZ_LONDON
from mintel.lab import LabPaths, day_min, epoch_min, minute_to_dt, synth
from mintel.lab import fetch as F
from mintel.lab.store import BarStore

UTC = dt.timezone.utc
# the sim's history ends at Saturday 10 Oct 2026 00:00 UTC and runs back 10 days
SIM_END = dt.datetime(2026, 10, 10, 0, 0, tzinfo=UTC)
SAT_MORNING = dt.datetime(2026, 10, 10, 9, 0, tzinfo=UTC)          # 10:00 BST on a Saturday: open
FRI_NIGHT = dt.datetime(2026, 10, 8, 22, 30, tzinfo=UTC)           # 23:30 BST on a Thursday night


class Clock:
    def __init__(self):
        self.t = 1000.0
        self.slept = []

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.slept.append(s)
        self.t += s


class Feed:
    """The thin adapter: logs (clock, symbol, count, end) for every bars() call."""
    synthetic = True

    def __init__(self, sim, clock, latency=0.05):
        self.sim, self.clock, self.latency = sim, clock, latency
        self.log = []
        self.connected = True
        self.fail = []                    # exceptions to raise on the next calls, in order

    def ping(self):
        return {"ok": True} if self.connected else None

    def symbols(self):
        return self.sim.symbols()

    def spec(self, s):
        return self.sim.spec(s)

    def bars(self, symbol, tf, count, end=None):
        self.log.append((self.clock(), symbol, tf, count, end))
        self.clock.t += self.latency
        if self.fail:
            raise self.fail.pop(0)
        return self.sim.bars(symbol, tf, count, end)


def make(tmp_path, symbols=("EURUSD", "GBPUSD"), days=10):
    sim = SimBroker(list(symbols), seed=3, start=SIM_END, history_bars=days * 1440)
    sim.connect()
    clock = Clock()
    feed = Feed(sim, clock)
    store = BarStore(LabPaths(tmp_path))
    return sim, feed, store, clock


def run(feed, store, clock, now=SAT_MORNING, **kw):
    kw.setdefault("budget_s", 600)
    kw.setdefault("history_days", 130)
    kw.setdefault("symbols", list(feed.symbols()))
    return F.update(feed, store, now=now, sleep=clock.sleep, clock=clock, **kw)


# ------------------------------------------------------------------ windows ---
@pytest.mark.parametrize("when,kind", [
    ((2026, 10, 12, 3, 0), None),          # Monday 03:00 UK: Sunday night is not a window
    ((2026, 10, 12, 14, 0), None),         # Monday afternoon
    ((2026, 10, 12, 23, 9), None),         # 23:09: not yet
    ((2026, 10, 12, 23, 10), "weeknight"), # 23:10 Monday
    ((2026, 10, 13, 5, 59), "weeknight"),  # Tuesday morning belongs to Monday night
    ((2026, 10, 13, 6, 0), None),
    ((2026, 10, 16, 23, 30), "weeknight"), # Friday night
    ((2026, 10, 17, 0, 30), "weekend"),    # Saturday 00:30
    ((2026, 10, 17, 15, 0), "weekend"),
    ((2026, 10, 18, 19, 59), "weekend"),   # Sunday before 20:00
    ((2026, 10, 18, 20, 0), None),
    ((2026, 12, 15, 23, 30), "weeknight"), # GMT winter
])
def test_the_fetch_windows_are_london_time(when, kind):
    local = dt.datetime(*when, tzinfo=TZ_LONDON)
    assert F.window_kind(local.astimezone(UTC)) == kind
    assert F.in_window(local) is (kind is not None)
    assert F.window_budget_s(local) == {None: 0, "weeknight": 1800, "weekend": 5400}[kind]


def test_a_start_outside_the_window_fetches_nothing(tmp_path):
    sim, feed, store, clock = make(tmp_path)
    mon = dt.datetime(2026, 10, 12, 13, 0, tzinfo=UTC)                 # Monday 14:00 BST
    rep = run(feed, store, clock, now=mon)
    assert rep.skipped == "outside the fetch window" and feed.log == []


def test_not_connected_is_skipped_without_a_single_call(tmp_path):
    sim, feed, store, clock = make(tmp_path)
    feed.connected = False
    rep = run(feed, store, clock)
    assert rep.skipped == "the price feed is not connected" and feed.log == []


def test_no_call_is_made_after_the_window_closes(tmp_path):
    sim, feed, store, clock = make(tmp_path)
    # Tuesday 05:59:40 BST (04:59:40 UTC): the night ends at 06:00 UK
    start = dt.datetime(2026, 10, 13, 4, 59, 40, tzinfo=UTC)
    rep = run(feed, store, clock, now=start, budget_s=3600, pace_s=5.0)
    assert rep.stopped == "the fetch window closed"
    t0 = feed.log[0][0]
    for at, *_ in feed.log:
        wall = start + dt.timedelta(seconds=at - t0)
        assert F.in_window(wall), wall
    assert 3 <= len(feed.log) <= 5                                       # 20 seconds at 5 s a call


# ---------------------------------------------------------------- the paging ---
def test_pages_back_from_the_newest_one_page_a_second(tmp_path):
    sim, feed, store, clock = make(tmp_path)
    rep = run(feed, store, clock, pace_s=1.0)
    # pacing: never closer than a second together
    times = [c[0] for c in feed.log]
    assert all(b - a >= 1.0 - 1e-9 for a, b in zip(times, times[1:]))
    for sym in ("EURUSD", "GBPUSD"):
        calls = [c for c in feed.log if c[1] == sym]
        assert calls[0][4] is None and calls[0][3] == 3000                 # the newest page first
        ends = [c[4] for c in calls[1:]]
        assert ends == sorted(ends, reverse=True) and len(set(ends)) == len(ends)
        # each page ends one minute before the oldest bar already held
        for prev, nxt in zip(calls[1:], calls[2:]):
            assert nxt[4] < prev[4]
    # everything the sim has is now in the store, in points
    for sym in ("EURUSD", "GBPUSD"):
        have = store.bars_in(sym)
        assert have == 10 * 1440
        assert store.index(sym)["source"] == "SYNTHETIC"
    assert sorted(rep.exhausted) == ["EURUSD", "GBPUSD"]
    assert store.index("EURUSD")["history_exhausted"] is True


def test_the_backfill_stops_at_the_wanted_depth_and_logs_depth_days(tmp_path):
    sim, feed, store, clock = make(tmp_path, days=30)
    lines = []
    rep = run(feed, store, clock, history_days=5, log=lines.append)
    assert rep.exhausted == []                                             # it stopped by choice, not by the broker
    for sym in ("EURUSD", "GBPUSD"):
        assert 5 <= rep.depth_days[sym] < 12
        assert any(f"depth_days {sym} {rep.depth_days[sym]}" in ln for ln in lines)
        assert store.index(sym)["depth_days"] == rep.depth_days[sym]


def test_an_empty_page_marks_history_exhausted_and_is_not_asked_again(tmp_path):
    sim, feed, store, clock = make(tmp_path, days=4)
    rep = run(feed, store, clock)
    assert sorted(rep.exhausted) == ["EURUSD", "GBPUSD"]
    assert rep.depth_days["EURUSD"] >= 3
    calls_before = len(feed.log)
    rep2 = run(feed, store, clock, now=SAT_MORNING + dt.timedelta(minutes=5))
    # only the forward top-up runs: no backward page is asked for an exhausted symbol
    new_calls = feed.log[calls_before:]
    assert rep2.exhausted == []
    assert len(new_calls) <= 4 and all(c[4] is None or c[4] >= minute_to_dt(
        store.first_min(c[1]) + 1) for c in new_calls)


def test_the_time_budget_is_honoured(tmp_path):
    sim, feed, store, clock = make(tmp_path, days=40)
    rep = run(feed, store, clock, budget_s=5, history_days=130)
    assert rep.stopped == "the time budget is spent"
    assert 4 <= len(feed.log) <= 7
    assert rep.seconds <= 5 + 1.5


def test_the_backfill_deepens_every_symbol_together(tmp_path):
    sim, feed, store, clock = make(tmp_path, ("EURUSD", "GBPUSD", "USDJPY"), days=40)
    run(feed, store, clock, budget_s=9, history_days=130)
    depth = [store.bars_in(s) for s in ("EURUSD", "GBPUSD", "USDJPY")]
    assert max(depth) - min(depth) <= 3000                                 # one page apart at most


def test_the_store_size_cap_stops_the_backfill(tmp_path):
    sim, feed, store, clock = make(tmp_path, days=30)
    rep = run(feed, store, clock, max_mb=0, history_days=130)
    assert rep.stopped == "the store is full"


# ------------------------------------------------------------------ healing ---
def test_the_last_three_days_are_refetched_so_a_gap_heals(tmp_path):
    sim, feed, store, clock = make(tmp_path, days=10)
    run(feed, store, clock)
    sym = "EURUSD"
    good = store.read_month(sym, "2026-10")
    # punch a 90-minute hole into Friday 9 Oct and damage one close
    hole = range(day_min("2026-10-09") + 600, day_min("2026-10-09") + 690)
    cols = good
    fresh = type(good)()
    for i in range(cols.n):
        if cols.t[i] in hole:
            continue
        c = cols.c[i] + 77 if cols.t[i] == day_min("2026-10-09") + 100 else cols.c[i]
        fresh.append_row(cols.t[i], cols.o[i], cols.h[i], cols.l[i], c, cols.s[i], cols.v[i])
    store._write_month(sym, "2026-10", fresh)
    store._rebuild_days(sym, SAT_MORNING)
    store._write_index(sym)
    assert store.read_month(sym, "2026-10").n == good.n - 90
    rep = run(feed, store, clock, now=SAT_MORNING + dt.timedelta(minutes=10))
    healed = store.read_month(sym, "2026-10")
    assert healed.n == good.n and list(healed.t) == list(good.t)
    assert list(healed.c) == list(good.c)                                   # the damaged close was rewritten too
    assert store.index(sym)["days"]["2026-10-09"]["complete"] is True


def test_new_bars_are_topped_up_on_the_next_run(tmp_path):
    sim, feed, store, clock = make(tmp_path, days=10)
    run(feed, store, clock)
    before = store.bars_in("EURUSD")
    sim.advance(300)
    log0 = len(feed.log)
    rep = run(feed, store, clock, now=SAT_MORNING + dt.timedelta(hours=1))
    assert store.bars_in("EURUSD") == before + 300
    assert store.last_min("EURUSD") == epoch_min(SIM_END) + 300
    first_calls = [c for c in feed.log[log0:] if c[1] == "EURUSD"]
    assert first_calls[0][4] is None                                        # the newest page is always first


# ------------------------------------------------------------------ failures ---
def test_one_timeout_is_retried_once_after_eight_seconds(tmp_path):
    sim, feed, store, clock = make(tmp_path, days=4)
    feed.fail = [TimeoutError("bridge timed out")]
    rep = run(feed, store, clock)
    assert 8.0 in clock.slept
    assert rep.stopped == "" and store.bars_in("EURUSD") == 4 * 1440
    assert feed.log[0][1:] == feed.log[1][1:]                                # the same page asked again


def test_a_second_timeout_ends_the_night_but_keeps_what_was_fetched(tmp_path):
    sim, feed, store, clock = make(tmp_path, days=10)
    # the first symbol's pages succeed; then two timeouts in a row on the next call
    n_ok = 5
    orig = feed.bars

    def flaky(symbol, tf, count, end=None):
        if len(feed.log) >= n_ok:
            feed.log.append((clock(), symbol, tf, count, end))
            raise TimeoutError("down")
        return orig(symbol, tf, count, end)
    feed.bars = flaky
    rep = run(feed, store, clock)
    assert rep.stopped == "the feed timed out twice"
    assert clock.slept.count(8.0) == 1
    assert store.bars_in("EURUSD") > 0                                      # earlier pages are kept


def test_a_torn_month_is_dropped_and_fetched_again(tmp_path):
    sim, feed, store, clock = make(tmp_path, days=10)
    run(feed, store, clock)
    p = tmp_path / "lab" / "bars" / "EURUSD" / "2026-10.c.bin"
    data = bytearray(p.read_bytes())
    data[8:12] = b"\xff\xff\xff\x7f"                                        # damage one close
    p.write_bytes(bytes(data))
    store.refresh()
    assert store.verify("EURUSD") == ["2026-10"]
    rep = run(feed, store, clock, now=SAT_MORNING + dt.timedelta(minutes=5))
    assert rep.repaired == {"EURUSD": ["2026-10"]}
    store.refresh()
    assert store.verify("EURUSD") == []
    assert store.bars_in("EURUSD") == 10 * 1440


# -------------------------------------------------------------------- source ---
def test_a_feed_that_is_not_synthetic_stamps_the_store_as_bridge(tmp_path):
    sim, feed, store, clock = make(tmp_path, days=3)
    feed.synthetic = False
    run(feed, store, clock)
    assert store.index("EURUSD")["source"] == "bridge"
    feed.synthetic = True
    assert F.is_synthetic_feed(feed) is True
    assert F.is_synthetic_feed(sim) is True                                 # the simulated broker itself
    assert F.is_synthetic_feed(object()) is False


def test_every_page_asks_for_minute_bars_only(tmp_path):
    sim, feed, store, clock = make(tmp_path, days=4)
    run(feed, store, clock)
    assert {c[2] for c in feed.log} == {TF.M1}
    assert not [m for m in dir(feed) if m in ("send", "modify_stops", "close")]


# ------------------------------------------- a page must not overwrite good prices ---
class ShiftedFeed(Feed):
    """What the bridge does when the broker's clock changes: it converts every bar with the
    CURRENT offset, so bars older than the change come back an hour late. The change is at
    ``switch``; the hour after it is empty (as a weekend would be), later bars are right."""

    def __init__(self, *a, switch, **kw):
        super().__init__(*a, **kw)
        self.switch = switch

    def bars(self, symbol, tf, count, end=None):
        rows = super().bars(symbol, tf, count, end)
        out = []
        for b in rows:
            if b.time < self.switch:
                out.append(type(b)(b.time + dt.timedelta(hours=1), b.open, b.high, b.low, b.close,
                                   b.tick_volume, b.real_volume, b.spread_points))
            elif b.time >= self.switch + dt.timedelta(hours=1):
                out.append(b)
        return out


def _snapshot(store, sym):
    out = {}
    for ym in store.months(sym):
        c = store.read_month(sym, ym)
        for i in range(c.n):
            out[c.t[i]] = (c.o[i], c.h[i], c.l[i], c.c[i], c.s[i])
    return out


def test_a_broker_clock_change_cannot_overwrite_the_stored_prices(tmp_path):
    """The overlap re-fetch let new rows win. After a clock change the last three days came back an
    hour late and 5,880 of 11,520 stored rows were silently replaced, with 60 duplicate-hour bars
    added and the report saying all was well."""
    sim, feed, store, clock = make(tmp_path, days=8)
    run(feed, store, clock)
    before = _snapshot(store, "EURUSD")
    sim.advance(600)                                                          # ten new hours after the change
    shifted = ShiftedFeed(sim, clock, switch=SIM_END)
    lines = []
    rep = run(shifted, store, clock, now=SAT_MORNING + dt.timedelta(hours=1), log=lines.append)
    store.refresh()
    after = _snapshot(store, "EURUSD")
    assert all(after[t] == v for t, v in before.items())                      # not one stored price changed
    assert len(after) > len(before) + 400                                     # and the new, correct minutes arrived
    new_stamps = set(after) - set(before)
    # not one mislabelled minute either: the last stored hour, echoed an hour late, would sit at
    # SIM_END .. SIM_END + 59, past the last stored bar, where nothing could contradict it
    assert new_stamps <= set(range(epoch_min(SIM_END) + 60, epoch_min(SIM_END) + 601))
    assert not rep.ok and set(rep.clashes) == {"EURUSD", "GBPUSD"}
    cl = rep.clashes["EURUSD"]
    assert "an hour late" in cl["note"] and cl["minutes"] >= 3 * 60
    err = [e for e in rep.errors if e.startswith("EURUSD")]
    assert len(err) == 1 and "left out" in err[0] and "clock changed" in err[0]
    assert any("left out" in ln for ln in lines)
    # the next night, with the old days out of the three-day overlap's way, is clean again
    sim.advance(5 * 1440)
    rep2 = run(feed, store, clock, now=SAT_MORNING + dt.timedelta(days=1))
    assert rep2.ok and rep2.clashes == {}
    store.refresh()
    assert _missing(store, "EURUSD") == []                                  # the empty hour was filled by the right data


def test_small_corrections_from_the_broker_are_still_taken(tmp_path):
    sim, feed, store, clock = make(tmp_path, days=6)
    run(feed, store, clock)
    sym = "EURUSD"
    good = store.read_month(sym, "2026-10")
    bent = type(good)()
    for i in range(good.n):
        c = good.c[i] + 50 if i in (good.n - 100, good.n - 900, good.n - 2000) else good.c[i]   # three damaged closes
        bent.append_row(good.t[i], good.o[i], good.h[i], good.l[i], c, good.s[i], good.v[i])
    store._write_month(sym, "2026-10", bent)
    store._rebuild_days(sym, SAT_MORNING)
    store._write_index(sym)
    rep = run(feed, store, clock, now=SAT_MORNING + dt.timedelta(minutes=10))
    assert rep.clashes == {} and rep.ok
    assert list(store.read_month(sym, "2026-10").c) == list(good.c)


def test_the_overlap_check_answers_for_each_row(tmp_path):
    sim, feed, store, clock = make(tmp_path, days=3)
    run(feed, store, clock)
    c = store.read_month("EURUSD", "2026-10")
    t = [c.t[10], c.t[11], c.t[12], c.t[-1] + 5]
    o = [c.o[10], c.o[11] + 1, c.o[12], 1]
    h = [c.h[10], c.h[11], c.h[12], 1]
    l = [c.l[10], c.l[11], c.l[12], 1]
    cl = [c.c[10], c.c[11], c.c[12], 1]
    assert store.overlap("EURUSD", t, o, h, l, cl) == [True, False, True, None]
    assert store.overlap("EURUSD", [c.t[20] + 60], [c.o[20]], [c.h[20]], [c.l[20]], [c.c[20]], shift=60) == [True]


def test_a_bridge_running_the_simulated_broker_is_stamped_synthetic(tmp_path):
    """A bridge started with --backend sim says so in its ping and nowhere else: its feed carries
    no flag and wraps no SimBroker, and its prices used to reach the board as real."""
    sim, feed, store, clock = make(tmp_path, days=3)
    feed.synthetic = False
    feed.ping = lambda: {"protocol": 3, "backend": "SimBroker", "mt5_connected": True}
    run(feed, store, clock)
    assert store.index("EURUSD")["source"] == "SYNTHETIC" and store.is_synthetic()
    assert F.is_synthetic_feed(feed, {"backend": "SimBroker"}) is True
    assert F.is_synthetic_feed(feed, {"backend": "Mt5Broker"}) is False
    assert F.is_synthetic_feed(feed) is False and F.is_synthetic_feed(feed, None) is False
    # a real broker behind a bridge stays 'bridge'
    sim2, feed2, store2, clock2 = make(tmp_path / "real", days=3)
    feed2.synthetic = False
    feed2.ping = lambda: {"protocol": 3, "backend": "Mt5Broker"}
    run(feed2, store2, clock2)
    assert store2.index("EURUSD")["source"] == "bridge" and not store2.is_synthetic()


# ----------------------------------------------------- a hole is never permanent ---
def _missing(store, sym):
    """Minutes absent between the first and last stored bar (the sim itself has no bar at the
    stroke of its own end, SIM_END, where its history and its advance() meet)."""
    have = set()
    for ym in store.months(sym):
        have.update(store.read_month(sym, ym).t)
    return [m for m in range(min(have), max(have) + 1) if m not in have and m != epoch_min(SIM_END)]


def test_a_top_up_cut_short_is_finished_by_the_next_run(tmp_path):
    """The top-up anchored on last_min, which the first page moves to the newest bar; if the run then
    timed out, nothing ever went back for the days between."""
    sim, feed, store, clock = make(tmp_path, ("EURUSD",), days=8)
    run(feed, store, clock)
    assert _missing(store, "EURUSD") == []
    sim.advance(8640)                                                         # six days more: three pages to catch up
    orig = feed.bars
    calls = []

    def flaky(symbol, tf, count, end=None):
        calls.append(end)
        if len(calls) in (2, 3):                                             # the second page and its retry time out
            raise TimeoutError("down")
        return orig(symbol, tf, count, end)
    feed.bars = flaky
    rep = run(feed, store, clock, now=SAT_MORNING + dt.timedelta(hours=2))
    assert rep.stopped == "the feed timed out twice"
    assert len(_missing(store, "EURUSD")) > 4000                              # a hole, as the page-one merge left it
    assert store.index("EURUSD")["topup_from"] is not None                    # ... and the run remembers how far back to go
    feed.bars = orig
    rep2 = run(feed, store, clock, now=SAT_MORNING + dt.timedelta(hours=26))
    store.refresh()
    assert rep2.ok and _missing(store, "EURUSD") == []
    assert store.last_min("EURUSD") == epoch_min(SIM_END) + 8640
    assert store.index("EURUSD").get("topup_from") is None


def test_a_month_dropped_as_damaged_in_the_middle_is_fetched_again(tmp_path):
    """Torn months are dropped so they are fetched again; only the newest ever was. A hole in the
    middle was never revisited (the backfill only extends the oldest edge, and had stopped)."""
    sim = SimBroker(["EURUSD"], seed=3, start=SIM_END, history_bars=45 * 1440)
    sim.connect()
    clock = Clock()
    feed = Feed(sim, clock)
    store = BarStore(LabPaths(tmp_path))
    run(feed, store, clock, symbols=["EURUSD"])
    assert store.months("EURUSD") == ["2026-08", "2026-09", "2026-10"] and store.bars_in("EURUSD") == 45 * 1440
    p = tmp_path / "lab" / "bars" / "EURUSD" / "2026-09.c.bin"
    data = bytearray(p.read_bytes())
    data[8:12] = b"\xff\xff\xff\x7f"
    p.write_bytes(bytes(data))
    store.refresh()
    rep = run(feed, store, clock, now=SAT_MORNING + dt.timedelta(minutes=5), symbols=["EURUSD"])
    store.refresh()
    assert rep.repaired == {"EURUSD": ["2026-09"]}
    assert store.verify("EURUSD") == [] and store.bars_in("EURUSD") == 45 * 1440
    assert _missing(store, "EURUSD") == []
    assert store.index("EURUSD").get("heal_from") is None
