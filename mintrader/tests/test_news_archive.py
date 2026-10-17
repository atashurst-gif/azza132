"""The calendar archive keeps every release (17 Oct fix).

The public weekly feed has no ids, and until 17 Oct a release's id was
"<source>:<time>": every release sharing a time was ONE archive row, with the
first release's currency and the last one's name and importance, swapping on
every refresh. So the 15-minute news window (10 Oct) could miss non-farm
payrolls when Canada's jobs data shared its minute. Now each release has its
own id, two currencies' releases of one name never merge, and the old merged
rows are left out wherever their time's releases are on record one by one.

EVERY EVENT IN THIS FILE IS MADE UP for the test: never a calendar.
"""
from __future__ import annotations

import datetime as dt
import json

from mintel.broker.base import CalendarEvent
from mintel.news.adapters import AdapterRegistry, ForexFactoryAdapter, StoreBackedAdapter
from mintel.news.calendar_store import CalendarStore
from mintel.news.engine import NewsIntelligenceEngine

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 2, 11, 0, tzinfo=UTC)
AT = dt.datetime(2026, 10, 2, 12, 30, tzinfo=UTC)          # 08:30 New York

FRIDAY = [   # one minute, four releases, two currencies (made up)
    {"title": "Non-Farm Employment Change", "country": "USD", "date": "2026-10-02T08:30:00-04:00", "impact": "High"},
    {"title": "Unemployment Rate", "country": "USD", "date": "2026-10-02T08:30:00-04:00", "impact": "High"},
    {"title": "Employment Change", "country": "CAD", "date": "2026-10-02T08:30:00-04:00", "impact": "High"},
    {"title": "Average Hourly Earnings m/m", "country": "USD", "date": "2026-10-02T08:30:00-04:00",
     "impact": "Low"},
    {"title": "CPI y/y", "country": "EUR", "date": "2026-10-02T05:00:00-04:00", "impact": "Medium"},
    {"title": "CPI y/y", "country": "GBP", "date": "2026-10-02T05:00:00-04:00", "impact": "High"},
]


def opener_of(items):
    return lambda url, timeout: json.dumps(items if "thisweek" in url else []).encode()


def engine(store, items=FRIDAY):
    reg = AdapterRegistry([ForexFactoryAdapter(opener=opener_of(items)), StoreBackedAdapter(store)])
    return NewsIntelligenceEngine(reg, store)


def test_every_release_at_a_shared_time_is_kept_and_stays_as_it_is(tmp_path):
    store = CalendarStore(tmp_path / "calendar.sqlite")
    eng = engine(store)
    for k in range(4):                                         # refresh after refresh, as the trader does
        assert eng.refresh(NOW + dt.timedelta(minutes=2 * k), force=True)
        rows = {(e.currency, e.name): e.importance for e in store.query(AT, AT)}
        assert rows == {("USD", "Non-Farm Employment Change"): 3, ("USD", "Unemployment Rate"): 3,
                        ("CAD", "Employment Change"): 3, ("USD", "Average Hourly Earnings m/m"): 1}, k
    both = {e.currency: e.importance for e in store.query(AT - dt.timedelta(hours=3, minutes=30), AT)
            if e.name == "CPI y/y"}
    assert both == {"EUR": 2, "GBP": 3}                         # one name, two currencies: two releases
    store.close()


def test_the_news_window_now_sees_payrolls_when_canada_shares_the_minute(tmp_path):
    store = CalendarStore(tmp_path / "calendar.sqlite")
    eng = engine(store)
    assert eng.refresh(NOW, force=True)
    before = AT - dt.timedelta(minutes=10)
    for ccys in (("EUR", "USD"), ("USD", "CAD"), ("GBP", "CAD"), ("USD", "JPY")):
        assert eng.upcoming(ccys, before, 900.0, 3), ccys       # all four are inside the 15 minutes
    assert not eng.upcoming(("EUR", "AUD"), before, 900.0, 3)
    store.close()


def _merged_row(store, when, currency, name, importance):
    """A row as the archive wrote it before 17 Oct: the id is "<source>:<time>"."""
    store.upsert([CalendarEvent(f"forexfactory:{when.isoformat()}", when, currency, "", name, importance,
                                actual=1.0, source="forexfactory")])


def test_an_old_merged_row_is_left_out_once_its_releases_are_on_record_one_by_one(tmp_path):
    store = CalendarStore(tmp_path / "calendar.sqlite")
    past = dt.datetime(2026, 9, 4, 12, 30, tzinfo=UTC)
    _merged_row(store, past, "CAD", "Non-Farm Employment Change", 3)             # history: all there is, kept
    _merged_row(store, AT, "CAD", "Non-Farm Employment Change", 3)               # this week's, before the fix
    assert [(e.currency, e.name) for e in store.query(AT, AT)] == [("CAD", "Non-Farm Employment Change")]
    assert engine(store).refresh(NOW, force=True)                               # the fixed feed writes each one
    now_rows = sorted((e.currency, e.name) for e in store.query(AT, AT))
    assert ("CAD", "Non-Farm Employment Change") not in now_rows and len(now_rows) == 4
    assert [(e.currency, e.name) for e in store.query(past, past)] == [("CAD", "Non-Farm Employment Change")]
    # and a later refresh does not bring it back through the archive's own adapter
    assert engine(store).refresh(NOW + dt.timedelta(minutes=5), force=True)
    assert ("CAD", "Non-Farm Employment Change") not in {(e.currency, e.name) for e in store.query(AT, AT)}
    # a surprise is sized on the releases one by one, never on the merged row
    store.upsert([CalendarEvent("x#CAD#Non-Farm Employment Change#later", AT + dt.timedelta(days=1), "CAD", "",
                                "Non-Farm Employment Change", 3, source="other")])
    hist = store.historical_values("Non-Farm Employment Change", "CAD", AT + dt.timedelta(days=2))
    assert [e.time_utc for e in hist] == [past]
    store.close()


def test_the_coach_reads_the_archive_by_the_same_rule(tmp_path):
    from mintel.coach import newswin as NW
    from mintel.config import Config
    store = CalendarStore(tmp_path / "calendar.sqlite")
    _merged_row(store, AT, "CAD", "Non-Farm Employment Change", 3)
    cfg = Config()
    cfg.news.blackout_before_seconds = 900.0
    span = (AT - dt.timedelta(days=1), AT + dt.timedelta(days=1))
    store.close()
    win = NW.load(tmp_path, cfg, *span)                         # before the fix: one CAD row, EURUSD not seen
    assert win.inside("GBPCAD", AT - dt.timedelta(minutes=10)) is True
    assert win.inside("EURUSD", AT - dt.timedelta(minutes=10)) is not True
    store = CalendarStore(tmp_path / "calendar.sqlite")
    assert engine(store).refresh(NOW, force=True)
    store.close()
    win = NW.load(tmp_path, cfg, *span)
    assert win.inside("EURUSD", AT - dt.timedelta(minutes=10)) is True
    assert win.release("EURUSD", AT - dt.timedelta(minutes=10)).startswith("USD ")
