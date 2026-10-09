"""MetaTrader history calls take the broker's clock as integer seconds.

On 9 Oct 2026 the MetaTrader5 package under Wine read naive datetimes in the
Mac's local time (UK summer time), so every tick request returned the hour
before the one asked for: the Rider's live tick history came back empty and its
research fetched nothing. These tests pin the fix: integer seconds on the
broker's clock, and only ticks inside the asked window come back.
"""
from __future__ import annotations

import calendar
import datetime as dt

import numpy as np

from mintel.broker.mt5_adapter import Mt5Broker
from mintel.clock import ServerClock

UTC = dt.timezone.utc
OFFSET = 3 * 3600                                   # IC Markets in summer: UTC+3


def _server_epoch(utc_ts: dt.datetime) -> int:
    return calendar.timegm((utc_ts + dt.timedelta(seconds=OFFSET)).replace(tzinfo=None).timetuple())


class FakeMt5:
    COPY_TICKS_ALL = 1
    TIMEFRAME_M1 = 1

    def __init__(self, tick_epochs):
        self.tick_epochs = tick_epochs              # broker-clock seconds of every tick held
        self.calls = []

    def copy_ticks_range(self, symbol, lo, hi, flags):
        self.calls.append(("ticks", lo, hi))
        assert isinstance(lo, int) and isinstance(hi, int), "bounds must be integer seconds"
        rows = [(t, t * 1000 + 250, 1.1, 1.1001, 0.0, 0.0) for t in self.tick_epochs if lo <= t <= hi]
        return np.array(rows, dtype=[("time", "i8"), ("time_msc", "i8"), ("bid", "f8"),
                                     ("ask", "f8"), ("last", "f8"), ("volume", "f8")])

    def copy_rates_from(self, symbol, tf, when, count):
        self.calls.append(("rates", when, count))
        assert isinstance(when, int), "the end must be integer seconds"
        return None


def _broker(epochs):
    b = Mt5Broker()
    b._mt5 = FakeMt5(epochs)
    b._clock = ServerClock(offset_seconds=OFFSET)
    return b


def test_ticks_come_from_the_window_asked_for_not_the_hour_before():
    start = dt.datetime(2026, 10, 9, 12, 5, 39, tzinfo=UTC)
    end = start + dt.timedelta(seconds=60)
    inside = [_server_epoch(start) + s for s in (1, 20, 59)]
    hour_before = [_server_epoch(start - dt.timedelta(hours=1)) + s for s in (1, 20, 59)]
    b = _broker(hour_before + inside)
    got = b.ticks_range("EURUSD", start, end)
    assert [t.time for t in got] == [start + dt.timedelta(seconds=s, milliseconds=250) for s in (1, 20, 59)]
    kind, lo, hi = b._mt5.calls[0]
    assert lo == _server_epoch(start) - 1 and hi == _server_epoch(end) + 1


def test_ticks_just_outside_the_window_are_dropped():
    start = dt.datetime(2026, 10, 8, 10, 0, 0, tzinfo=UTC)
    end = dt.datetime(2026, 10, 8, 11, 0, 0, tzinfo=UTC)
    b = _broker([_server_epoch(start) - 1, _server_epoch(start), _server_epoch(end), _server_epoch(end) + 1])
    got = b.ticks_range("EURUSD", start, end)
    assert all(start <= t.time <= end for t in got)
    assert len(got) == 1                                 # end + 250 ms is outside; start + 250 ms is in


def test_bars_up_to_a_time_ask_with_integer_seconds():
    from mintel.broker.base import TF
    b = _broker([])
    end = dt.datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
    assert b.bars("EURUSD", TF.M1, 10, end=end) == []
    assert b._mt5.calls[0] == ("rates", _server_epoch(end), 10)
