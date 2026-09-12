"""Timezone, DST and session behaviour.

A wrong timezone silently corrupts session levels, opening ranges, news timing
and every staleness check, so these are tested against real DST transition
dates rather than assumed offsets.
"""
from __future__ import annotations

import datetime as dt

import pytest

from mintel.broker.base import TF
from mintel.clock import (SESSIONS, ServerClock, active_sessions,
                          is_weekend_gap, to_utc, UTC)
from mintel.data import sessions as sess


def U(*a) -> dt.datetime:
    return dt.datetime(*a, tzinfo=UTC)


class TestServerClock:
    def test_offset_is_measured_not_assumed(self):
        c = ServerClock.measure(dt.datetime(2026, 7, 1, 17, 0, 3),
                                U(2026, 7, 1, 14, 0, 0))
        assert c.offset_seconds == 3 * 3600

    def test_measurement_rounds_to_whole_minutes(self):
        c = ServerClock.measure(dt.datetime(2026, 7, 1, 17, 0, 29),
                                U(2026, 7, 1, 14, 0, 0))
        assert c.offset_seconds % 60 == 0

    def test_round_trip(self):
        c = ServerClock(2 * 3600)
        utc = U(2026, 3, 10, 12, 0)
        assert c.server_to_utc(c.utc_to_server(utc)) == utc

    def test_naive_datetime_is_rejected(self):
        with pytest.raises(ValueError):
            to_utc(dt.datetime(2026, 1, 1, 0, 0))


class TestDaylightSaving:
    def test_us_cash_open_shifts_with_us_dst(self):
        # Winter: NY is UTC-5 so the cash open is 14:30 UTC.
        assert "US_CASH" not in active_sessions(U(2026, 1, 15, 14, 0))
        assert "US_CASH" in active_sessions(U(2026, 1, 15, 15, 0))
        # Summer: NY is UTC-4 so the cash open is 13:30 UTC.
        assert "US_CASH" in active_sessions(U(2026, 7, 15, 14, 0))

    def test_london_open_shifts_with_uk_dst(self):
        # UK DST begins 29 March 2026; before that London opens at 08:00 UTC.
        assert "LONDON" in active_sessions(U(2026, 3, 20, 8, 30))
        assert "LONDON" not in active_sessions(U(2026, 3, 20, 7, 30))
        # After the change London opens at 07:00 UTC.
        assert "LONDON" in active_sessions(U(2026, 4, 10, 7, 30))

    def test_us_and_uk_dst_change_on_different_dates(self):
        """Between 8 and 29 March the usual 5-hour gap is only 4."""
        gap_before = None
        lon = SESSIONS["LONDON"].bounds_for(U(2026, 3, 20, 12, 0))[0]
        ny = SESSIONS["NEWYORK"].bounds_for(U(2026, 3, 20, 12, 0))[0]
        gap_before = (ny - lon).total_seconds() / 3600
        lon2 = SESSIONS["LONDON"].bounds_for(U(2026, 4, 20, 12, 0))[0]
        ny2 = SESSIONS["NEWYORK"].bounds_for(U(2026, 4, 20, 12, 0))[0]
        gap_after = (ny2 - lon2).total_seconds() / 3600
        assert gap_before == pytest.approx(4.0)
        assert gap_after == pytest.approx(5.0)

    def test_overlap_is_derived_not_hardcoded(self):
        assert "LONDON_NY_OVERLAP" in active_sessions(U(2026, 7, 15, 14, 0))
        assert "LONDON_NY_OVERLAP" not in active_sessions(U(2026, 7, 15, 3, 0))


class TestWeekend:
    @pytest.mark.parametrize("ts,expected", [
        (U(2026, 3, 14, 12, 0), True),    # Saturday
        (U(2026, 3, 13, 22, 0), True),    # Friday after 21:00
        (U(2026, 3, 15, 12, 0), True),    # Sunday before 21:00
        (U(2026, 3, 15, 22, 0), False),   # Sunday after the open
        (U(2026, 3, 11, 12, 0), False),   # Wednesday
    ])
    def test_weekend_detection(self, ts, expected):
        assert is_weekend_gap(ts) is expected


class TestSessionRanges:
    def test_session_levels_are_built_from_bars(self, sim):
        m15 = sim.bars("EURUSD", TF.M15, 800)
        m1 = sim.bars("EURUSD", TF.M1, 3000)
        r = sess.read(m15, m1, sim.now)
        assert "ASIA" in r.ranges and "LONDON" in r.ranges
        asia = r.ranges["ASIA"]
        assert asia.high > asia.low
        assert asia.start < asia.end
        assert r.prev_day_high is not None and r.prev_day_low is not None
        assert r.prev_day_high > r.prev_day_low

    def test_liquidity_is_highest_in_the_overlap(self, sim):
        m15 = sim.bars("EURUSD", TF.M15, 800)
        m1 = sim.bars("EURUSD", TF.M1, 3000)
        r = sess.read(m15, m1, sim.now)
        assert r.is_overlap
        assert r.liquidity_score() == 100.0

    def test_opening_ranges_exist_for_each_configured_length(self, sim):
        m15 = sim.bars("EURUSD", TF.M15, 800)
        m1 = sim.bars("EURUSD", TF.M1, 3000)
        r = sess.read(m15, m1, sim.now, opening_minutes=(1, 5, 15))
        assert set(r.opening_ranges) == {1, 5, 15}
        assert r.opening_ranges[15].width >= r.opening_ranges[1].width

    def test_wick_outside_the_range_is_not_acceptance(self, sim):
        """A poke through the opening range must not read as a breakout."""
        from mintel.broker.base import Bar
        m1 = sim.bars("EURUSD", TF.M1, 3000)
        lon_open, _ = SESSIONS["LONDON"].bounds_for(sim.now)
        inside = [b for b in m1 if b.time < lon_open]
        after = [b for b in m1 if b.time >= lon_open]
        assert after, "fixture must cover the London session"
        first = after[0]
        hi, lo, mid = first.high, first.low, (first.high + first.low) / 2
        poke = Bar(time=first.time + dt.timedelta(minutes=1), open=mid,
                   high=hi + 0.0020, low=lo, close=mid, tick_volume=300)
        series = inside + [first, poke]
        orr = sess.build_opening_range(series, "LONDON", 1,
                                       poke.time + dt.timedelta(minutes=1))
        assert orr is not None
        assert orr.high == hi, "the range itself is the first minute"
        assert not orr.broke_up, "a wick above the range is not a break"
        assert not orr.accepted

    def test_close_beyond_the_range_is_a_break(self, sim):
        from mintel.broker.base import Bar
        m1 = sim.bars("EURUSD", TF.M1, 3000)
        lon_open, _ = SESSIONS["LONDON"].bounds_for(sim.now)
        inside = [b for b in m1 if b.time < lon_open]
        after = [b for b in m1 if b.time >= lon_open]
        first = after[0]
        hi, lo = first.high, first.low
        up = Bar(time=first.time + dt.timedelta(minutes=1), open=hi,
                 high=hi + 0.0030, low=hi - 0.0001, close=hi + 0.0025,
                 tick_volume=400)
        hold = Bar(time=first.time + dt.timedelta(minutes=2), open=hi + 0.0025,
                   high=hi + 0.0040, low=hi + 0.0020, close=hi + 0.0035,
                   tick_volume=400)
        orr = sess.build_opening_range(inside + [first, up, hold], "LONDON", 1,
                                       hold.time + dt.timedelta(minutes=1))
        assert orr.broke_up and orr.accepted and not orr.reclaimed

    def test_break_then_reclaim_is_a_failed_breakout(self, sim):
        from mintel.broker.base import Bar
        m1 = sim.bars("EURUSD", TF.M1, 3000)
        lon_open, _ = SESSIONS["LONDON"].bounds_for(sim.now)
        inside = [b for b in m1 if b.time < lon_open]
        after = [b for b in m1 if b.time >= lon_open]
        first = after[0]
        hi, lo, mid = first.high, first.low, (first.high + first.low) / 2
        up = Bar(time=first.time + dt.timedelta(minutes=1), open=hi,
                 high=hi + 0.0030, low=hi, close=hi + 0.0025, tick_volume=400)
        back = Bar(time=first.time + dt.timedelta(minutes=2), open=hi + 0.0025,
                   high=hi + 0.0026, low=mid - 0.0001, close=mid,
                   tick_volume=500)
        orr = sess.build_opening_range(inside + [first, up, back], "LONDON", 1,
                                       back.time + dt.timedelta(minutes=1))
        assert orr.broke_up
        assert orr.reclaimed, "it broke out and came back inside"
        assert not orr.accepted
