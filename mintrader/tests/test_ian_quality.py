"""Financial Ian: data quality - a sequence gap, a stale feed, out-of-order events, bad timestamps,
latency, missing depth and the feed's own notices make an instrument DEGRADED (no new signals) or DOWN,
and it recovers on its own once clean data has flowed long enough. SYNTHETIC test data."""
import datetime as dt

from mintel.ian.book import ASK, BID, MODIFY, BookEvent, FeedNotice, OrderBook
from mintel.ian.dataquality import DEGRADED, DOWN, LIVE, NOT_CONFIGURED, DataQualityMonitor, QualityConfig
from mintel.ian.feeds.synthetic import generate

UTC = dt.timezone.utc


def play(scenario, cfg=None, scope="instrument", until=None):
    evs, marks = generate(scenario)
    mon = DataQualityMonitor(cfg or QualityConfig(), scope)
    book = OrderBook("6EZ6")
    timeline = []                       # (received time, state) after every event
    reasons = {}
    for ev in evs:
        t = ev.ts_received if isinstance(ev, BookEvent) else ev.received
        if until is not None and t >= marks[until]:
            break
        mon.observe(ev)
        if isinstance(ev, BookEvent):
            book.apply(ev)
        st = mon.state("6EZ6", t, book)
        timeline.append((t, st.state))
        reasons[t] = st.reasons
    mon.reasons_at = reasons
    return mon, book, marks, timeline, evs


class TestDegradedAndRecovery:
    def test_a_clean_feed_is_live(self):
        mon, book, marks, tl, _ = play("balanced")
        assert all(s == LIVE for _, s in tl[50:])
        st = mon.stats("6EZ6")
        assert st["gaps"] == 0 and st["out_of_order"] == 0 and 0 < st["latency_ms"] < 10

    def test_a_sequence_gap_degrades_then_recovers(self):
        mon, book, marks, tl, _ = play("sequence_gap")
        ev_t = marks["event"]
        after = [(t, s) for t, s in tl if t >= ev_t]
        assert after[0][1] == DEGRADED or after[1][1] == DEGRADED
        degraded = [t for t, s in after if s == DEGRADED]
        assert degraded and (degraded[-1] - degraded[0]).total_seconds() >= QualityConfig().recovery_seconds - 0.5
        assert after[-1][1] == LIVE                                  # recovered on its own
        assert mon.stats("6EZ6")["gaps"] == 1
        assert any("sequence gap: 100 message(s) missing" in r for r in mon.reasons_at[degraded[0]])

    def test_out_of_order_degrades_then_recovers(self):
        mon, book, marks, tl, _ = play("out_of_order")
        after = [s for t, s in tl if t >= marks["event"]]
        assert DEGRADED in after[:3] and after[-1] == LIVE
        assert mon.stats("6EZ6")["out_of_order"] >= 1

    def test_a_stale_feed_degrades_goes_down_and_recovers(self):
        mon, book, marks, tl, evs = play("stale_feed", until="resume")      # stop inside the silence
        last_before = tl[-1][0]
        assert mon_state_at(mon, book, last_before + dt.timedelta(seconds=11)) == DEGRADED
        assert mon_state_at(mon, book, last_before + dt.timedelta(seconds=29)) == DEGRADED
        st = mon.state("6EZ6", last_before + dt.timedelta(seconds=11), book)
        assert any("stale book" in r for r in st.reasons)
        # 60 s of silence would be DOWN
        assert mon.state("6EZ6", last_before + dt.timedelta(seconds=61), book).state == DOWN

    def test_the_silence_itself_is_a_fault_until_clean_data_has_flowed(self):
        mon, book, marks, tl, _ = play("stale_feed")
        resumed = [s for t, s in tl if t >= marks["resume"]]
        assert resumed[0] == DEGRADED and resumed[-1] == LIVE

    def test_missing_depth_and_a_crossed_book(self):
        mon = DataQualityMonitor()
        t = dt.datetime(2026, 10, 7, 12, tzinfo=UTC)
        b = OrderBook("6EZ6")
        for i, (side, p) in enumerate([(BID, 1.0850), (ASK, 1.08505)]):
            ev = BookEvent("6EZ6", t, t, i + 1, side, p, 10, "add")
            mon.observe(ev)
            b.apply(ev)
        st = mon.state("6EZ6", t, b)
        assert st.state == DEGRADED and any("missing depth" in r for r in st.reasons)
        ev = BookEvent("6EZ6", t, t, 3, BID, 1.0851, 10, "add")
        mon.observe(ev)
        b.apply(ev)
        assert any("crossed" in r for r in mon.state("6EZ6", t, b).reasons)

    def test_timestamps_from_the_future_and_latency(self):
        mon = DataQualityMonitor(QualityConfig(max_latency_ms=500))
        t = dt.datetime(2026, 10, 7, 12, tzinfo=UTC)
        mon.observe(BookEvent("6EZ6", t, t - dt.timedelta(seconds=2), 1, BID, 1.085, 10, MODIFY))
        assert "timestamp problem" in mon.inst["6EZ6"].fault
        mon2 = DataQualityMonitor(QualityConfig(max_latency_ms=500))
        mon2.observe(BookEvent("6EZ6", t, t + dt.timedelta(seconds=2), 1, BID, 1.085, 10, MODIFY))
        assert "latency 2000 ms" in mon2.inst["6EZ6"].fault

    def test_vendor_notices(self):
        mon = DataQualityMonitor(sequence_scope="channel")
        t = dt.datetime(2026, 10, 7, 12, tzinfo=UTC)
        mon.observe(BookEvent("6EZ6", t, t, 10, BID, 1.085, 10, MODIFY))
        mon.observe(BookEvent("6EZ6", t, t, 500, BID, 1.085, 11, MODIFY))     # per-channel numbering: not a gap
        assert not mon.inst["6EZ6"].fault
        mon.observe(FeedNotice("6EZ6", t, "BAD_BOOK", "possibly wrong"))
        assert "bad book" in mon.inst["6EZ6"].fault
        mon.observe(FeedNotice("", t, "DISCONNECTED", "socket closed"))
        assert "disconnected" in mon.feed_fault
        mon.observe(FeedNotice("", t, "RECONNECTED", ""))
        assert mon.feed_fault == ""

    def test_not_configured_and_down(self):
        mon = DataQualityMonitor()
        t = dt.datetime(2026, 10, 7, 12, tzinfo=UTC)
        assert mon.state("6EZ6", t, feed_state=NOT_CONFIGURED).state == NOT_CONFIGURED
        assert mon.state("6EZ6", t).state == DOWN                   # nothing received yet
        assert mon.state("6EZ6", t, feed_state=DOWN).state == DOWN

    def test_recovery_attempts_back_off(self):
        mon = DataQualityMonitor(QualityConfig(recover_backoff_s=5, recover_backoff_max_s=20))
        t = dt.datetime(2026, 10, 7, 12, tzinfo=UTC)
        due = [s for s in range(0, 60) if mon.recovery_due("6EZ6", t + dt.timedelta(seconds=s))]
        assert due == [0, 5, 15, 35, 55]


def mon_state_at(mon, book, t):
    return mon.state("6EZ6", t, book).state
