"""NEWS showed DEGRADED on the real Mac: the MetaTrader5 Python package the
bridge uses has no calendar API, so the bot traded on charts alone. A public
weekly calendar feed is now a second source, and the MT5 adapter steps aside
instead of failing on every refresh."""
from __future__ import annotations

import datetime as dt
import json

import pytest

from mintel.broker.base import BrokerError
from mintel.news.adapters import (AdapterRegistry, ForexFactoryAdapter,
                                  Mt5CalendarAdapter, StoreBackedAdapter)
from mintel.news.calendar_store import CalendarStore
from mintel.news.engine import NewsIntelligenceEngine

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 9, 14, 13, 0, tzinfo=UTC)

THIS_WEEK = [
    {"title": "CPI m/m", "country": "USD", "date": "2026-09-14T08:30:00-04:00",
     "impact": "High", "forecast": "0.3%", "previous": "0.2%", "actual": "0.4%"},
    {"title": "BoE Gov Bailey Speaks", "country": "GBP", "date": "2026-09-14T15:00:00-04:00",
     "impact": "Medium", "forecast": "", "previous": ""},
    {"title": "Bank Holiday", "country": "JPY", "date": "2026-09-15T00:00:00-04:00",
     "impact": "Holiday", "forecast": "", "previous": ""},
    {"title": "Retail Sales m/m", "country": "AUD", "date": "2026-09-20T21:30:00-04:00",
     "impact": "Low", "forecast": "0.1%", "previous": "-0.1%"},
]
NEXT_WEEK = [
    {"title": "FOMC Statement", "country": "USD", "date": "2026-09-22T14:00:00-04:00",
     "impact": "High", "forecast": "", "previous": ""},
]


def fake_opener(url, timeout):
    body = THIS_WEEK if "thisweek" in url else NEXT_WEEK
    return json.dumps(body).encode()


class TestFeedAdapter:
    def test_parses_and_windows_the_week(self):
        ad = ForexFactoryAdapter(opener=fake_opener)
        start, end = NOW - dt.timedelta(hours=36), NOW + dt.timedelta(hours=36)
        events = ad.fetch_calendar(start, end)
        names = [e.name for e in events]
        assert names == ["CPI m/m", "BoE Gov Bailey Speaks", "Bank Holiday"]
        cpi = events[0]
        assert cpi.currency == "USD"
        assert cpi.time_utc == dt.datetime(2026, 9, 14, 12, 30, tzinfo=UTC)
        assert cpi.importance == 3
        assert cpi.forecast == pytest.approx(0.3)
        assert cpi.previous == pytest.approx(0.2)
        assert cpi.actual == pytest.approx(0.4)
        assert cpi.source == "forexfactory"
        speech = events[1]
        assert speech.importance == 2 and speech.actual is None and speech.forecast is None
        assert events[2].importance == 0
        assert ad.health.healthy and ad.health.calls == 2

    def test_next_week_is_included_when_the_window_reaches_it(self):
        ad = ForexFactoryAdapter(opener=fake_opener)
        events = ad.fetch_calendar(NOW, NOW + dt.timedelta(days=10))
        assert "FOMC Statement" in [e.name for e in events]

    def test_a_failing_feed_backs_off_instead_of_hammering(self):
        calls = []
        def broken(url, timeout):
            calls.append(url)
            raise OSError("boom")
        ad = ForexFactoryAdapter(opener=broken)
        with pytest.raises(Exception):
            ad.fetch_calendar(NOW, NOW + dt.timedelta(hours=1))
        with pytest.raises(Exception):
            ad.fetch_calendar(NOW, NOW + dt.timedelta(hours=1))
        assert len(calls) == 2, "the second attempt must be refused during backoff"

    def test_offline_switch_fails_fast(self, monkeypatch):
        from mintel.news.adapters import _http_get
        monkeypatch.setenv("MINTEL_OFFLINE", "1")
        with pytest.raises(Exception):
            _http_get("https://example.invalid/x", 1.0)


class TestMt5AdapterStepsAside:
    def test_no_calendar_api_disables_it_quietly(self):
        class B:
            def calendar(self, s, e):
                raise BrokerError("this MetaTrader5 build has no calendar API; use the stored historical calendar")
        ad = Mt5CalendarAdapter(B())
        assert ad.fetch_calendar(NOW, NOW) == []
        assert ad.health.enabled is False

    def test_other_errors_still_raise(self):
        class B:
            def calendar(self, s, e):
                raise BrokerError("connection lost")
        ad = Mt5CalendarAdapter(B())
        with pytest.raises(BrokerError):
            ad.fetch_calendar(NOW, NOW)
        assert ad.health.enabled is True


class TestEngineIsHealthyOnTheFeedAlone:
    def test_refresh(self, tmp_path):
        class B:
            def calendar(self, s, e):
                raise BrokerError("this MetaTrader5 build has no calendar API")
        store = CalendarStore(tmp_path / "cal.sqlite")
        reg = AdapterRegistry([Mt5CalendarAdapter(B()),
                               ForexFactoryAdapter(opener=fake_opener),
                               StoreBackedAdapter(store)])
        eng = NewsIntelligenceEngine(reg, store)
        assert eng.refresh(NOW, force=True)
        h = eng.health
        assert h.healthy
        assert not h.stale(900.0, NOW)
        assert h.events_known >= 3
        assert h.last_error == ""
