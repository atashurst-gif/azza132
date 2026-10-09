"""Financial Ian: each order-flow feature detects its own synthetic scenario and not the balanced one;
regimes; the opportunity score and its plain-English reasoning. All data here is SYNTHETIC test data."""
import datetime as dt
import functools
import math

import pytest

from mintel.ian.book import ADD, ASK, BID, BUY, CANCEL, MODIFY, SELL, UNKNOWN, BookEvent, OrderBook, TradeEvent
from mintel.ian.features import FlowConfig, InstrumentFlow, session_key
from mintel.ian.feeds.synthetic import generate
from mintel.ian.regime import (ABSORPTION, BALANCED, BREAKOUT, HIGH_VOLATILITY, LIQUIDITY_VACUUM, NEWS_SHOCK,
                               REVERSAL, TRENDING, NewsContext, classify)
from mintel.ian.research.harness import sample
from mintel.ian.score import ChartContext, news_context, score

UTC = dt.timezone.utc


@functools.lru_cache(maxsize=None)
def run(scenario: str, instrument: str = "6EZ6"):
    evs, marks = generate(scenario, instrument=instrument)
    return sample(evs, every_s=0.5), marks


def after(scenario, start="event", end=None, instrument="6EZ6"):
    ss, marks = run(scenario, instrument)
    t0 = marks.get(start, marks["warmup_end"])
    t1 = marks.get(end) if end else None
    return [s for s in ss if s.ts >= t0 and (t1 is None or s.ts < t1) and s.fs.ok]


def ok_samples(scenario):
    ss, marks = run(scenario)
    return [s for s in ss if s.fs.ok]


# ------------------------------------------------------------- the balanced book --

class TestBalancedIsQuiet:
    def test_no_feature_fires_on_the_balanced_book(self):
        ss = ok_samples("balanced")
        assert len(ss) > 300
        for s in ss:
            f = s.fs
            assert f.absorption_score < 0.6
            assert f.sweep_dir == 0 and f.sweeps_medium == 0
            assert f.pull_ask < 0.8 and f.pull_bid < 0.8
            assert not f.refresh_levels and f.refresh_signal == 0
            assert not [e for e in f.wall_events if e["status"] == "CONSUMED"]
            assert abs(f.consumption_signal) < 0.2
            assert f.resilience_signal == 0
            assert f.delta_divergence == 0
            assert not f.stacked_buy and not f.stacked_sell
            assert f.vol_ratio < 2.5 and f.depth_ratio > 0.6 and f.spread_ratio < 1.6
            assert f.labels == []

    def test_the_balanced_book_is_balanced_and_never_tradeable(self):
        ss = ok_samples("balanced")
        assert all(s.regime.regime == BALANCED for s in ss)
        assert not any(s.opp.tradeable for s in ss)


# ------------------------------------------------------------- one per scenario --

class TestEachFeatureFindsItsScenario:
    @pytest.mark.parametrize("scenario,sign", [("sweep_up", 1), ("sweep_down", -1)])
    def test_sweeps(self, scenario, sign):
        ss = after(scenario)
        hits = [s for s in ss if s.fs.sweep_dir == sign]
        assert hits, "the sweep was not seen"
        first = hits[0].fs
        assert first.sweep_levels >= 3 and first.sweep_exec_share >= 0.6
        assert (hits[0].ts - run(scenario)[1]["event"]).total_seconds() <= 1.0     # seen within a second
        assert not [s for s in ss if s.fs.sweep_dir == -sign and s.fs.sweep_age_s is not None and s.fs.sweep_age_s < 5]
        assert any(("SWEEP UP" if sign > 0 else "SWEEP DOWN") in " ".join(s.fs.labels) for s in ss)

    @pytest.mark.parametrize("scenario,sign", [("offer_pulling", 1), ("bid_pulling", -1)])
    def test_liquidity_pulling(self, scenario, sign):
        ss = after(scenario)
        assert max(s.fs.pulling_signal * sign for s in ss) >= 0.8
        side = "pull_ask" if sign > 0 else "pull_bid"
        assert max(getattr(s.fs, side) for s in ss) >= 0.8

    def test_a_wall_hit_refreshed_and_finally_consumed(self):
        ss, marks = run("wall_absorbed_consumed")
        standing = [s for s in ss if marks["wall_placed"] <= s.ts < marks["wall_consumed"] and s.fs.wall_ask]
        assert standing, "the offer wall was not tracked"
        w = max((s.fs.wall_ask for s in standing), key=lambda w: w["size"])
        assert w["ratio"] >= 3.0 and w["size"] >= 300
        late = [s for s in ss if s.ts >= marks["wall_reached"]]
        assert max((s.fs.wall_ask or {}).get("refreshes", 0) for s in late) >= 1
        consumed = [s for s in ss if s.ts >= marks["wall_consumed"]
                    and any(e["status"] == "CONSUMED" and e["side"] == "ASK" for e in s.fs.wall_events)]
        assert consumed and max(s.fs.wall_signal for s in consumed) > 0          # a wall taken out reads bullish
        assert any("OFFER WALL CONSUMED" in " ".join(s.fs.labels) for s in consumed)

    @pytest.mark.parametrize("scenario,sign", [("absorption_buyers", -1), ("absorption_sellers", 1)])
    def test_absorption(self, scenario, sign):
        ss = after(scenario, "event", "after")
        strong = [s for s in ss if s.fs.absorption_score >= 0.6]
        assert strong, "no absorption seen"
        assert all(s.fs.absorption_dir == sign for s in strong)
        f = strong[-1].fs
        assert abs(f.move_short_ticks) <= 1 and f.absorption_pressure >= 2.0
        text = " ".join(strong[-1].fs.labels)
        assert ("sellers absorbing aggressive buyers" if sign < 0 else "buyers absorbing aggressive sellers") in text

    def test_refresh_is_only_ever_possible(self):
        ss = after("refresh_iceberg")
        lv = [r for s in ss for r in s.fs.refresh_levels if r["side"] == "BID"]
        assert lv and all(r["label"] == "POSSIBLE REFRESH / ABSORPTION" for r in lv)
        assert max(r["executed"] / r["max_visible"] for r in lv) >= 1.5
        assert max(s.fs.refresh_signal for s in ss) > 0
        for s in ss:
            for label in s.fs.labels:
                assert "iceberg" not in label.lower()                   # never claimed as a fact

    def test_liquidity_vacuum(self):
        ss = after("liquidity_vacuum")
        assert min(s.fs.depth_ratio for s in ss) <= 0.35
        assert max(s.fs.spread_ratio for s in ss) >= 2.0

    def test_news_shock_velocity(self):
        ss, marks = run("news_shock")
        shock = [s for s in ss if marks["event"] <= s.ts <= marks["recovery"] + dt.timedelta(seconds=3) and s.fs.ok]
        base = ok_samples("balanced")
        assert max(s.fs.intensity for s in shock) >= 4.0
        assert min(s.fs.depth_ratio for s in shock) <= 0.5
        assert max(s.fs.cancels_per_s for s in shock) > 2 * max(s.fs.cancels_per_s for s in base) or \
            max(s.fs.velocity_ratio for s in shock) > max(s.fs.velocity_ratio for s in base)
        assert max(s.fs.spread_change_ticks for s in shock) >= 3

    @pytest.mark.parametrize("scenario,sign", [("trend_up", 1), ("trend_down", -1)])
    def test_trend_flow_consumption_and_footprint(self, scenario, sign):
        ss = after(scenario)
        assert max(s.fs.move_long_ticks * sign for s in ss) >= 8
        assert max(s.fs.aggr_medium * sign for s in ss) >= 0.3
        assert max(s.fs.consumption_signal * sign for s in ss) >= 0.2
        assert any((s.fs.stacked_buy if sign > 0 else s.fs.stacked_sell) for s in ss)
        assert max(s.fs.vwap_signal * sign for s in ss) > 0.3

    def test_high_volatility(self):
        ss = after("high_volatility")
        assert max(s.fs.vol_ratio for s in ss) >= 2.5

    @pytest.mark.parametrize("scenario,sign", [("divergence_bearish", -1), ("divergence_bullish", 1)])
    def test_delta_divergence(self, scenario, sign):
        ss = after(scenario)
        assert any(s.fs.delta_divergence == sign for s in ss)
        assert not any(s.fs.delta_divergence == -sign for s in ss)
        assert all(s.fs.delta_reliable for s in ss)

    def test_resilience_after_a_sweep_that_holds(self):
        ss = after("sweep_up")
        res = [s.fs for s in ss if s.fs.resilience is not None]
        assert res and res[0].resilience_side == "ASK"
        assert max(f.resilience_signal for f in res) == pytest.approx(0.7)   # the new price held: genuine repricing


# --------------------------------------------------------------- exact formulas --

T0 = dt.datetime(2026, 10, 7, 12, 0, tzinfo=UTC)


def bev(t, seq, side, price, size, action=MODIFY, cause=""):
    return BookEvent("6EZ6", t, t, seq, side, price, size, action, cause=cause)


def two_sided(book_or_flow, t, bids, asks, seq=0):
    for p, q in bids:
        seq += 1
        book_or_flow.apply(bev(t, seq, BID, p, q, ADD)) if isinstance(book_or_flow, OrderBook) else \
            book_or_flow.on_book(bev(t, seq, BID, p, q, ADD))
    for p, q in asks:
        seq += 1
        book_or_flow.apply(bev(t, seq, ASK, p, q, ADD)) if isinstance(book_or_flow, OrderBook) else \
            book_or_flow.on_book(bev(t, seq, ASK, p, q, ADD))
    return seq


class TestFormulas:
    def test_microprice_leans_to_the_thin_side(self):
        b = OrderBook("6EZ6")
        two_sided(b, T0, [(1.08500, 90)], [(1.08505, 10)])
        # (Pa*Qb + Pb*Qa)/(Qb+Qa) = (1.08505*90 + 1.085*10)/100
        assert b.microprice() == pytest.approx((1.08505 * 90 + 1.085 * 10) / 100)
        assert b.microprice() > b.mid()

    def test_weighted_imbalance_by_distance(self):
        f = InstrumentFlow("6EZ6", FlowConfig(imbalance_decay=0.5))
        two_sided(f, T0, [(1.08500, 100), (1.08495, 100)], [(1.08505, 100), (1.08510, 300)])
        # distances from the mid 1.085025: bids 0.5 and 1.5 ticks, asks 0.5 and 1.5 ticks
        w1, w2 = math.exp(-0.25), math.exp(-0.75)
        expect = (w1 * 100 + w2 * 100 - w1 * 100 - w2 * 300) / (w1 * 100 + w2 * 100 + w1 * 100 + w2 * 300)
        assert f.weighted_imbalance() == pytest.approx(expect)
        assert f.weighted_imbalance() < 0                  # offer-heavy, but the far size counts for less

    def test_ofi_counts_the_touch(self):
        f = InstrumentFlow("6EZ6")
        seq = two_sided(f, T0, [(1.08500, 50), (1.08495, 40)], [(1.08505, 50)])
        before = f.cur.ofi
        f.on_book(bev(T0, seq + 1, BID, 1.08500, 80))      # bid size up 30: e = +30
        assert f.cur.ofi - before == pytest.approx(30)
        f.on_book(bev(T0, seq + 2, ASK, 1.08505, 20))      # ask size down 30: e = +30
        assert f.cur.ofi - before == pytest.approx(60)
        f.on_book(bev(T0, seq + 3, BID, 1.08500, 0, CANCEL))   # the best bid gone: e = -80 (the old touch size)
        assert f.cur.ofi - before == pytest.approx(60 - 80)

    def test_an_execution_is_not_a_cancellation(self):
        f = InstrumentFlow("6EZ6")
        seq = two_sided(f, T0, [(1.08500, 50)], [(1.08505, 50)])
        f.on_trade(TradeEvent("6EZ6", T0, 1.08505, 20, BUY, seq + 1, T0))
        f.on_book(bev(T0, seq + 2, ASK, 1.08505, 30))      # 20 executed
        f.on_book(bev(T0, seq + 3, ASK, 1.08505, 25))      # 5 cancelled
        assert f.cur.exe_near_a == 20 and f.cur.can_near_a == 5

    def test_cumulative_delta_only_when_the_aggressor_is_known(self):
        f = InstrumentFlow("6EZ6")
        seq = two_sided(f, T0, [(1.08500, 50)], [(1.08505, 50)])
        f.on_trade(TradeEvent("6EZ6", T0, 1.08505, 10, BUY, seq + 1, T0))
        f.on_trade(TradeEvent("6EZ6", T0, 1.08500, 4, SELL, seq + 2, T0))
        assert f.cum_delta == 6
        f.on_trade(TradeEvent("6EZ6", T0, 1.08505, 10, UNKNOWN, seq + 3, T0))
        assert f.cum_delta == 6 and f.cum_unknown == 10    # an unclassified trade never moves the delta

    def test_session_vwap_and_profile(self):
        f = InstrumentFlow("6EZ6", FlowConfig(profile_min_volume=10))
        seq = two_sided(f, T0, [(1.08500, 50)], [(1.08505, 50)])
        prices = [(1.08500, 10), (1.08505, 40), (1.08510, 10), (1.08515, 5), (1.08495, 5)]
        for i, (p, q) in enumerate(prices):
            f.on_trade(TradeEvent("6EZ6", T0, p, q, BUY, seq + 1 + i, T0))
        assert f.pv / f.v == pytest.approx(sum(p * q for p, q in prices) / sum(q for _, q in prices))
        assert max(f.profile, key=f.profile.get) == round(1.08505 / 0.00005)

    def test_sessions_change_at_five_pm_chicago(self):
        before = dt.datetime(2026, 10, 7, 21, 59, tzinfo=UTC)         # 16:59 CDT
        after_ = dt.datetime(2026, 10, 7, 22, 1, tzinfo=UTC)          # 17:01 CDT
        assert session_key(before) == "2026-10-07" and session_key(after_) == "2026-10-08"

    def test_a_depth_window_artefact_is_not_a_cancel(self):
        f = InstrumentFlow("6EZ6")
        seq = two_sided(f, T0, [(1.08500, 50)], [(1.08505, 50)])
        f.on_book(bev(T0, seq + 1, BID, 1.08500, 0, CANCEL, cause="view"))
        assert f.cur.cancels_v == 0 and f.cur.can_near_b == 0


# ------------------------------------------------------------------- regimes --

def regimes(scenario, start="event", end=None, news=None):
    ss, marks = run(scenario)
    t0 = marks.get(start, marks["warmup_end"])
    out = []
    for s in ss:
        if s.ts < t0 or (end and s.ts >= marks[end]) or not s.fs.ok:
            continue
        r = classify(s.fs, news(s.ts, marks) if news else None, ())
        out.append(r)
    return out


class TestRegimes:
    def test_unknown_while_warming_up(self):
        ss, _ = run("balanced")
        assert ss[0].regime.regime == "UNKNOWN" and not ss[0].regime.allow_entry

    def test_breakout_after_a_sweep(self):
        rs = after("sweep_up")[:30]
        assert sum(1 for s in rs if s.regime.regime == BREAKOUT and s.regime.direction == 1) >= 15

    def test_liquidity_vacuum(self):
        rs = [s.regime for s in after("liquidity_vacuum")]
        vac = [r for r in rs if r.regime == LIQUIDITY_VACUUM]
        assert len(vac) >= 0.8 * len(rs) and not any(r.allow_entry for r in vac)

    def test_news_shock_with_the_calendar(self):
        def news(t, marks):
            since = (t - marks["event"]).total_seconds()
            return NewsContext(seconds_since_release=since if since >= 0 else None, event="USD Non-Farm Payrolls")
        rs = regimes("news_shock", news=news)
        shock = [r for r in rs if r.regime == NEWS_SHOCK]
        assert len(shock) >= 20 and not any(r.allow_entry for r in shock)

    def test_absorption(self):
        rs = [s.regime for s in after("absorption_buyers", "event", "after")]
        ab = [r for r in rs if r.regime == ABSORPTION]
        assert len(ab) >= 20 and all(r.direction == -1 for r in ab)

    def test_high_volatility(self):
        rs = [s.regime for s in after("high_volatility")]
        assert sum(1 for r in rs if r.regime == HIGH_VOLATILITY) >= 0.5 * len(rs)

    def test_trending(self):
        rs = [s.regime for s in after("trend_down")]
        tr = [r for r in rs if r.regime == TRENDING]
        assert tr and all(r.direction == -1 for r in tr)

    def test_reversal(self):
        rs = [s.regime for s in after("reversal")]
        rev = [r for r in rs if r.regime == REVERSAL]
        assert rev and all(r.direction == -1 for r in rev)


# --------------------------------------------------------------------- score --

class TestScore:
    def test_a_sweep_is_traded_early_and_the_right_way(self):
        ss = after("sweep_up")
        tr = [s for s in ss if s.opp.tradeable]
        assert tr and all(s.opp.spot_direction == 1 for s in tr)
        assert (tr[0].ts - run("sweep_up")[1]["event"]).total_seconds() <= 10
        o = tr[0].opp
        assert len(o.agree) >= 3 and not o.against
        assert "EUR" in o.reasoning and o.reasoning.endswith("so EURUSD BUY.")

    @pytest.mark.parametrize("inst,spot,side", [("6JZ6", "USDJPY", "SELL"), ("6CZ6", "USDCAD", "SELL"),
                                                ("6SZ6", "USDCHF", "SELL"), ("6BZ6", "GBPUSD", "BUY")])
    def test_the_spot_side_after_the_mapping(self, inst, spot, side):
        evs, marks = generate("trend_up", instrument=inst)
        ss = [s for s in sample(evs, every_s=1.0) if s.ts >= marks["event"] and s.opp.tradeable]
        assert ss
        for s in ss:
            assert s.opp.direction == 1                      # the future is bid
            assert s.opp.spot_symbol == spot
            assert ("BUY" if s.opp.spot_direction > 0 else "SELL") == side
            assert s.opp.reasoning.rstrip(".").split(f"so {spot} ")[1].startswith(side)
        if side == "SELL":
            assert "quoted the other way round" in ss[0].opp.reasoning

    def test_never_with_the_absorbed_aggressor(self):
        ss = after("absorption_buyers", "event", "after")
        assert not [s for s in ss if s.opp.tradeable and s.opp.direction == 1]

    def test_no_trade_into_or_just_after_a_release(self):
        s = next(x for x in after("sweep_up") if x.opp.tradeable)
        assert score(s.fs, s.regime).tradeable
        before = score(s.fs, s.regime, news=NewsContext(seconds_to_next=60, event="USD CPI"))
        assert not before.tradeable and "no new trade into a release" in before.why_not
        just = score(s.fs, s.regime, news=NewsContext(seconds_since_release=20, event="USD CPI"))
        assert not just.tradeable and "never chase the first print" in just.why_not

    def test_poor_spot_execution_blocks(self):
        s = next(x for x in after("sweep_up") if x.opp.tradeable)
        o = score(s.fs, s.regime, exec_quality=0.3, exec_note="spread 4 pips")
        assert not o.tradeable and "spot execution too poor" in o.why_not

    def test_the_chart_is_retail_and_translated(self):
        s = after("sweep_up")[4]
        o = score(s.fs, s.regime, chart=ChartContext("USDJPY", True, value=-0.8, note="the USDJPY chart is falling"))
        # a falling USDJPY chart would be BULLISH evidence for a yen future; on a 6E reading it is just EURUSD
        comp = [c for c in o.components if c.name == "chart"][0]
        assert comp.source == "RETAIL" and comp.value == pytest.approx(-0.8)
        evs, _ = generate("balanced", instrument="6JZ6")
        sj = [x for x in sample(evs, every_s=2.0) if x.fs.ok][-1]
        oj = score(sj.fs, sj.regime, chart=ChartContext("USDJPY", True, value=-0.8, note="falling"))
        assert [c for c in oj.components if c.name == "chart"][0].value == pytest.approx(0.8)

    def test_news_surprises_point_the_future(self):
        from mintel.broker.base import CalendarEvent
        now = dt.datetime(2026, 10, 7, 12, 40, tzinfo=UTC)
        ev = CalendarEvent("x", now - dt.timedelta(minutes=5), "USD", "US", "Non-Farm Payrolls", 3,
                           actual=250.0, forecast=180.0)
        assert news_context("6E", [ev], now).futures_effect == -1      # strong dollar: euro future down
        assert news_context("6J", [ev], now).futures_effect == -1      # and the yen future down (USDJPY up)
        ev2 = CalendarEvent("y", now - dt.timedelta(minutes=5), "EUR", "EU", "CPI y/y", 3, actual=2.9, forecast=2.5)
        assert news_context("6E", [ev2], now).futures_effect == 1
        assert news_context("6J", [ev2], now).futures_effect == 0
