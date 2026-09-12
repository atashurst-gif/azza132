"""News intelligence: surprise, price verdict, waves, blackouts, failures."""
from __future__ import annotations

import datetime as dt

import pytest

from mintel.broker.base import Bar, CalendarEvent, TF
from mintel.news.adapters import (AdapterRegistry, JsonCalendarAdapter,
                                  Mt5CalendarAdapter, StoreBackedAdapter)
from mintel.news.calendar_store import CalendarStore
from mintel.news.engine import NewsIntelligenceEngine, Wave
from mintel.news.surprise import family_sign, measure as measure_surprise

UTC = dt.timezone.utc
T = dt.datetime(2026, 3, 10, 13, 30, tzinfo=UTC)


def ev(**kw) -> CalendarEvent:
    base = dict(event_id="e1", time_utc=T, currency="USD", country="US",
                name="CPI y/y", importance=3, actual=None, forecast=3.1,
                previous=3.4)
    base.update(kw)
    return CalendarEvent(**base)


@pytest.fixture
def store(tmp_path) -> CalendarStore:
    s = CalendarStore(tmp_path / "cal.sqlite")
    yield s
    s.close()


class TestSurprise:
    def test_gap_versus_forecast_and_previous(self):
        r = measure_surprise(ev(actual=3.6))
        assert r.gap_vs_forecast == pytest.approx(0.5)
        assert r.gap_vs_previous == pytest.approx(0.2)

    def test_revised_previous_takes_precedence(self):
        r = measure_surprise(ev(actual=3.6, revised_previous=3.0))
        assert r.gap_vs_previous == pytest.approx(0.6)
        assert r.revision == pytest.approx(-0.4)

    def test_normalised_against_the_series_own_history(self):
        history = [ev(event_id=str(i), actual=3.1 + 0.05 * (i % 3),
                      forecast=3.1) for i in range(12)]
        r = measure_surprise(ev(actual=3.6), history)
        assert r.z_vs_history is not None and r.z_vs_history > 3
        assert r.samples == 12
        assert r.is_surprise

    def test_magnitude_is_capped_without_history(self):
        r = measure_surprise(ev(actual=99.0))
        assert r.z_vs_history is None
        assert r.magnitude <= 0.6
        assert "capped" in r.note

    def test_better_number_is_not_automatically_bullish(self):
        """An unemployment beat is currency-NEGATIVE, not positive."""
        up = measure_surprise(ev(name="Unemployment Rate", actual=4.5,
                                 forecast=4.1, previous=4.1))
        assert up.hypothesis_sign == -1
        cpi = measure_surprise(ev(actual=3.6))
        assert cpi.hypothesis_sign == +1

    def test_ambiguous_events_yield_no_hypothesis(self):
        r = measure_surprise(ev(name="Crude Oil Inventories", actual=5.0,
                                forecast=1.0))
        assert r.hypothesis_sign == 0
        assert not r.interpretable

    def test_missing_forecast_is_handled(self):
        r = measure_surprise(ev(actual=3.6, forecast=None))
        assert r.gap_vs_forecast is None
        assert not r.is_surprise

    @pytest.mark.parametrize("name,sign", [
        ("Core CPI m/m", 1), ("Nonfarm Payrolls", 1), ("GDP q/q", 1),
        ("Initial Jobless Claims", -1), ("Unemployment Rate", -1),
        ("Something Unknown", 0),
    ])
    def test_family_signs(self, name, sign):
        assert family_sign(name) == sign


class TestCalendarStore:
    def test_revisions_update_in_place(self, store):
        store.upsert([ev()])
        store.upsert([ev(actual=3.6)])
        rows = store.query(T - dt.timedelta(hours=1), T + dt.timedelta(hours=1))
        assert len(rows) == 1 and rows[0].actual == pytest.approx(3.6)

    def test_a_later_null_does_not_erase_a_known_value(self, store):
        store.upsert([ev(actual=3.6)])
        store.upsert([ev(actual=None)])
        rows = store.query(T - dt.timedelta(hours=1), T + dt.timedelta(hours=1))
        assert rows[0].actual == pytest.approx(3.6)

    def test_coverage_reports_the_stored_window(self, store):
        store.upsert([ev()])
        c = store.coverage()
        assert c.days == 1
        assert c.covers(T, T)
        assert not c.covers(T - dt.timedelta(days=5), T)

    def test_csv_round_trip(self, store, tmp_path):
        store.upsert([ev(actual=3.6)])
        n = store.export_csv(tmp_path / "out.csv")
        assert n == 1
        store2 = CalendarStore(tmp_path / "other.sqlite")
        store2.import_csv(tmp_path / "out.csv")
        assert len(store2.query(T - dt.timedelta(days=1),
                                T + dt.timedelta(days=1))) == 1
        store2.close()

    def test_reconnect_after_close(self, store):
        store.close()
        assert not store.healthy()
        assert store.reconnect()
        assert store.healthy()

    def test_filters_by_importance_and_currency(self, store):
        store.upsert([ev(), ev(event_id="x", currency="EUR", importance=1)])
        assert len(store.query(T - dt.timedelta(hours=1),
                               T + dt.timedelta(hours=1),
                               currencies=["USD"])) == 1
        assert len(store.query(T - dt.timedelta(hours=1),
                               T + dt.timedelta(hours=1),
                               min_importance=2)) == 1


class TestAdapters:
    def test_registry_merges_by_tier(self, store):
        store.upsert([ev(actual=None, previous=3.4)])
        calls = {"n": 0}

        def fake(url, timeout):
            calls["n"] += 1
            return (b'[{"id":"e1","date":"2026-03-10T13:30:00Z",'
                    b'"country":"USD","title":"CPI y/y","impact":"High",'
                    b'"actual":"3.6%","estimate":"3.1%","previous":"3.4%"}]')

        reg = AdapterRegistry([StoreBackedAdapter(store),
                               JsonCalendarAdapter("http://x/{start}/{end}",
                                                   opener=fake, tier=3)])
        merged, errors = reg.fetch_calendar(T - dt.timedelta(days=1),
                                             T + dt.timedelta(days=1))
        assert not errors
        assert len(merged) == 1
        # The lower tier wins on identity, but a missing value is filled in
        # from the higher tier.
        assert merged[0].actual == pytest.approx(3.6)

    def test_adapter_failure_is_reported_not_raised(self, sim):
        sim.fault.calendar_timeout = True
        reg = AdapterRegistry([Mt5CalendarAdapter(sim)])
        merged, errors = reg.fetch_calendar(T - dt.timedelta(days=1),
                                             T + dt.timedelta(days=1))
        assert merged == []
        assert errors and "timeout" in errors[0]

    def test_cache_serves_stale_data_rather_than_nothing(self):
        state = {"fail": False}

        def flaky(url, timeout):
            if state["fail"]:
                raise TimeoutError("provider down")
            return (b'[{"id":"1","date":"2026-03-10T13:30:00Z",'
                    b'"country":"USD","title":"CPI","impact":"High"}]')

        ad = JsonCalendarAdapter("http://x/{start}/{end}", opener=flaky,
                                 cache_seconds=0.0)
        first = ad.fetch_calendar(T, T)
        assert len(first) == 1
        state["fail"] = True
        second = ad.fetch_calendar(T, T)
        assert len(second) == 1, "should fall back to the cached response"
        assert ad.health.consecutive_failures == 1

    def test_social_sources_are_excluded_unless_allowed(self):
        class Social:
            name, tier = "social", 5

            def __init__(self):
                from mintel.news.adapters import AdapterHealth
                self.health = AdapterHealth("social", 5)

            def fetch_calendar(self, a, b):
                return []

            def fetch_headlines(self, since):
                from mintel.news.adapters import Headline
                return [Headline(T, "social", 5, ("USD",), "rumour")]

        reg = AdapterRegistry([Social()])
        assert reg.fetch_headlines(T - dt.timedelta(hours=1)) == []
        allowed = reg.fetch_headlines(T - dt.timedelta(hours=1),
                                      allow_social=True)
        assert len(allowed) == 1
        assert not allowed[0].actionable_alone


class TestEngineVerdict:
    def _engine(self, sim, store, event) -> NewsIntelligenceEngine:
        sim.add_event(event)
        eng = NewsIntelligenceEngine(AdapterRegistry([Mt5CalendarAdapter(sim)]),
                                     store)
        eng.refresh(sim.now)
        return eng

    def test_imminent_release_blocks_new_entries(self, sim, store):
        release = sim.now + dt.timedelta(seconds=45)
        eng = self._engine(sim, store, ev(time_utc=release, actual=None))
        v = eng.verdict("EURUSD", sim.bars("EURUSD", TF.M1, 200),
                        sim.tick("EURUSD"), sim.spec("EURUSD"), sim.now)
        assert v.wave is Wave.PENDING
        assert v.blackout
        assert "no new entries" in v.reason

    def test_price_reaction_outranks_the_headline(self, sim, store):
        """Dollar-bullish data but the dollar is SOLD: follow price."""
        release = sim.now
        eng = self._engine(sim, store, ev(time_utc=release, actual=3.6))
        # EURUSD RISES after bullish-USD data, i.e. the dollar was sold.
        for _ in range(10):
            sim.push_bar("EURUSD", sim.bars("EURUSD", TF.M1, 1)[-1].close + 0.0004)
        v = eng.verdict("EURUSD", sim.bars("EURUSD", TF.M1, 300),
                        sim.tick("EURUSD"), sim.spec("EURUSD"), sim.now)
        assert v.bias == 1, "bias must follow price, not the textbook"
        assert v.hypothesis_contradicted
        assert "follow price" in v.reason

    def test_agreement_between_data_and_tape(self, sim, store):
        eng = self._engine(sim, store, ev(time_utc=sim.now, actual=3.6))
        for _ in range(10):
            sim.push_bar("EURUSD", sim.bars("EURUSD", TF.M1, 1)[-1].close - 0.0004)
        v = eng.verdict("EURUSD", sim.bars("EURUSD", TF.M1, 300),
                        sim.tick("EURUSD"), sim.spec("EURUSD"), sim.now)
        assert v.bias == -1
        assert not v.hypothesis_contradicted
        assert v.support > 50

    def test_wide_spread_keeps_us_in_the_first_spike(self, sim, store):
        eng = self._engine(sim, store, ev(time_utc=sim.now, actual=3.6))
        for _ in range(5):
            sim.push_bar("EURUSD", sim.bars("EURUSD", TF.M1, 1)[-1].close - 0.0004)
        sim.fault.spread_multiplier = 8.0
        v = eng.verdict("EURUSD", sim.bars("EURUSD", TF.M1, 300),
                        sim.tick("EURUSD"), sim.spec("EURUSD"), sim.now)
        assert v.wave is Wave.FIRST_SPIKE
        assert v.blackout, "execution is unpriceable while the spread is blown"

    def test_a_fully_rejected_move_flips_the_bias(self, sim, store):
        eng = self._engine(sim, store, ev(time_utc=sim.now, actual=3.6))
        start = sim.bars("EURUSD", TF.M1, 1)[-1].close
        for _ in range(3):                       # initial spike down
            sim.push_bar("EURUSD", sim.bars("EURUSD", TF.M1, 1)[-1].close - 0.0010)
        for _ in range(6):                       # fully given back and beyond
            sim.push_bar("EURUSD", sim.bars("EURUSD", TF.M1, 1)[-1].close + 0.0010)
        v = eng.verdict("EURUSD", sim.bars("EURUSD", TF.M1, 300),
                        sim.tick("EURUSD"), sim.spec("EURUSD"), sim.now)
        assert v.wave in (Wave.REJECTED, Wave.WHIPSAW, Wave.FIRST_WAVE)
        if v.wave is Wave.REJECTED:
            assert v.bias == 1

    def test_no_recent_release_is_neutral_not_negative(self, sim, store):
        eng = NewsIntelligenceEngine(
            AdapterRegistry([Mt5CalendarAdapter(sim)]), store)
        eng.refresh(sim.now)
        v = eng.verdict("EURUSD", sim.bars("EURUSD", TF.M1, 200),
                        sim.tick("EURUSD"), sim.spec("EURUSD"), sim.now)
        assert v.bias == 0
        assert v.score_for(1) == 50.0 == v.score_for(-1)
        assert not v.blackout

    def test_engine_keeps_working_when_the_feed_dies(self, sim, store):
        # cache_seconds=0 so the dead feed is actually consulted rather than
        # answered from the adapter's short-lived cache.
        sim.add_event(ev(time_utc=sim.now, actual=3.6))
        eng = NewsIntelligenceEngine(
            AdapterRegistry([Mt5CalendarAdapter(sim, cache_seconds=0.0)]),
            store)
        eng.refresh(sim.now)
        adapter = eng.registry.adapters[0]
        sim.fault.calendar_timeout = True
        eng.refresh(sim.now)
        # Serving the last good data beats serving nothing, but it must be
        # visible rather than silent.
        assert adapter.health.consecutive_failures == 1
        assert adapter.health.serving_stale
        assert "failing" in eng.health.degraded_reason
        # And trading continues on the stored calendar.
        v = eng.verdict("EURUSD", sim.bars("EURUSD", TF.M1, 200),
                        sim.tick("EURUSD"), sim.spec("EURUSD"), sim.now)
        assert v.event is not None

    def test_total_feed_failure_still_leaves_the_archive(self, sim, store):
        """With no cache at all, the store is the fallback and refresh fails."""
        store.upsert([ev(time_utc=sim.now, actual=3.6)])
        sim.fault.calendar_timeout = True
        eng = NewsIntelligenceEngine(
            AdapterRegistry([Mt5CalendarAdapter(sim, cache_seconds=0.0),
                             StoreBackedAdapter(store)]), store)
        assert eng.refresh(sim.now) is False
        assert eng.health.degraded_reason
        assert eng.health.events_known >= 1

    def test_currency_mapping_for_non_fx(self, sim, store):
        eng = NewsIntelligenceEngine(
            AdapterRegistry([Mt5CalendarAdapter(sim)]), store)
        assert eng.currencies_for("EURUSD") == ("EUR", "USD")
        assert eng.currencies_for("GER40", sim.spec("GER40"))
