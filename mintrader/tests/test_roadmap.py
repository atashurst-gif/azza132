"""ROAD TO PROFIT (Aaron, 14 Oct 2026): the bootstrap, the milestones and the plain "not supported" words.
Every trade here is MADE UP."""
from __future__ import annotations

import datetime as dt

import pytest

from mintel.ops import roadmap as RM
from tests.test_leagues import demo_run

UTC = dt.timezone.utc


def ev(cid, trades, league="DEMO", risk=10.0):
    return RM.strategy_evidence(cid, cid.title(), league, [dict(t, risk=risk) for t in trades])


class TestBootstrap:
    def test_a_certain_result_has_no_spread(self):
        c = {"rs": [1.0] * 30, "per_day": 2.0}
        sim = RM.bootstrap_days([c], 7.5)
        assert sim["p10"] == sim["p50"] == sim["p90"] == 40 * 7.5 and sim["trades"] == 40
        assert sim["daily_p50"] == 15.0 and sim["dd_p90"] == 0.0

    def test_percentiles_in_order_and_repeatable(self):
        c = {"rs": [2.0, -1.0, -1.0, 1.5, -1.0, 0.5] * 5, "per_day": 3.0}
        a, b = RM.bootstrap_days([c], 7.5), RM.bootstrap_days([c], 7.5)
        assert a == b                                                      # a fixed seed
        assert a["p10"] < a["p50"] < a["p90"] and a["dd_p50"] <= a["dd_p90"]
        mean = sum(c["rs"]) / len(c["rs"]) * 60 * 7.5
        assert a["p10"] < mean < a["p90"]

    def test_nothing_to_resample_is_none(self):
        assert RM.bootstrap_days([{"rs": [], "per_day": 3}], 7.5) is None


class TestMilestones:
    def test_thin_evidence_is_not_supported_and_says_why(self):
        thin = [ev("bot:a", demo_run(6, 2))]
        p = RM.project(thin, 25.0, 1500, 0.5, "DEMO")
        assert not p["supported"] and p["projection"] is None
        assert p["words"].startswith("Not supported yet: 6 demo trades over 2 days is too little to project from")
        assert "£" not in p["words"]                                        # no figure is invented

    def test_no_edge_is_not_supported(self):
        losing = [ev("bot:a", demo_run(40, 10, 2.0, -6.0, every=2))]
        p = RM.project(losing, 25.0, 1500, 0.5, "DEMO")
        assert not p["supported"] and "no demo strategy shows a positive result" in p["words"]

    def test_supported_and_not_from_the_same_evidence(self):
        good = [ev("bot:a", demo_run(60, 10, 8.0, -4.0))]                     # ~+0.4R a trade, 6 a day
        low = RM.project(good, 10.0, 1500, 0.5, "DEMO")
        high = RM.project(good, 100.0, 1500, 0.5, "DEMO")
        assert low["supported"] and low["projection"]["sim"]["p10"] > 0
        assert not high["supported"] and "would take about £" in high["words"]
        need = high["projection"]["equity_needed"]
        assert need == pytest.approx(100.0 / (high["projection"]["r_per_day"] * 0.005), rel=0.01)
        assert high["projection"]["eurusd_lots"] > 0 and high["improve"]
        assert low["contributors"][0]["id"] == "bot:a"

    def test_the_milestones_grow_with_the_account(self):
        assert RM.milestones(1500) == [25, 50, 100]
        assert RM.milestones(5000) == [25, 50, 100, 250] and RM.milestones(12_000)[-1] == 500

    def test_demo_and_paper_are_projected_apart(self, monkeypatch):
        world = {"tests": {"lab:p": {"id": "lab:p", "nick": "Paper One",
                                     "trades": [dict(t, risk=10.0) for t in demo_run(40, 8, start=dt.date(2026, 10, 1))]}},
                 "refresh": None}
        built = {"demo_trades": {"bot:d": [dict(t, risk=10.0) for t in demo_run(5, 2, start=dt.date(2026, 10, 12))]},
                 "demo_known": {"bot:d": True}, "stints": []}
        now = dt.datetime(2026, 10, 14, 12, tzinfo=UTC)
        road = RM.road(world, built, now)
        m = road["milestones"][0]
        assert m["DEMO"]["league"] == "DEMO" and m["PAPER"]["league"] == "PAPER"
        assert [c["id"] for c in m["PAPER"]["contributors"]] == ["lab:p"]
        assert m["DEMO"]["projection"] is None and m["PAPER"]["projection"] is not None
        assert not any("lab:p" == c["id"] for c in m["DEMO"]["contributors"])
        assert road["best"]["league"] == "PAPER" and "to prove it on Demo" in road["best"]["needs"]
        assert any(b["barrier"] == "Too few trades" for b in road["barriers"])

    def test_barriers_seen_in_the_records(self):
        a = ev("bot:a", demo_run(40, 10))
        b = ev("bot:b", demo_run(40, 10))
        heavy = RM.strategy_evidence("bot:c", "Costly", "DEMO",
                                     [dict(t, risk=10.0, costs=4.0) for t in demo_run(40, 10)])
        got = {x["barrier"]: x for x in RM.barriers({"DEMO": [a, b, heavy], "PAPER": []}, [], None)}
        assert "Strategies moving together" in got and "Costs eating the edge" in got
        assert "Costly" in got["Costs eating the edge"]["seen"]
        for x in got.values():
            assert x["doing"]


NOW14 = dt.datetime(2026, 10, 14, 17, tzinfo=UTC)
EMPTY = {"demo_trades": {}, "demo_known": {}, "stints": []}


class TestTheReviewFindings:
    """rv14: every figure on the road is the record's, never more (all trades MADE UP)."""

    def test_trades_a_day_counts_every_trading_day_not_only_the_days_it_traded(self):
        # 35 trades, 7 on each of five days a week apart: 17 Sep to 14 Oct is 20 UK trading days
        days = [dt.date(2026, 9, 17), dt.date(2026, 9, 24), dt.date(2026, 10, 1), dt.date(2026, 10, 8),
                dt.date(2026, 10, 13)]
        trades = [{"closed": dt.datetime(d.year, d.month, d.day, 9 + i, tzinfo=UTC),
                   "net": 8.0 if i % 2 == 0 else -4.0, "risk": 10.0} for d in days for i in range(7)]
        e = RM.strategy_evidence("bot:w", "Weekly", "DEMO", trades, now=NOW14)
        assert e["days"] == 5 and e["per_day"] == 1.75                       # not 7.0 (35 / the 5 days it traded)
        road = RM.road({"tests": {}, "refresh": None},
                       {"demo_trades": {"bot:w": trades}, "demo_known": {"bot:w": True}, "stints": []}, NOW14)
        assert road["best"]["per_day"] == 1.75 and road["best"]["day_at_account"] == 3.75   # £7.50 x 0.2857R x 1.75
        pr = road["milestones"][0]["DEMO"]["projection"]
        assert pr["typical_day"] == 3.75 and pr["equity_needed"] == 10000.0                 # not £15.00 and £2,500

    def test_the_bar_applies_to_the_strategies_the_projection_is_built_from(self):
        losing = ev("bot:a", demo_run(30, 6, 0.0, -10.0, every=1))           # 30 losses over 6 days
        thin = ev("bot:b", demo_run(10, 2, 20.0, -4.0))                      # a good 10 trades over 2 days
        p = RM.project([losing, thin], 25.0, 1500, 0.5, "DEMO")
        assert p["projection"] is None and p["supported"] is False
        assert p["trades"] == 10 and p["days"] == 2
        assert "strategies with an edge have 10 trades over 2 days" in p["words"] and "£" not in p["words"]

    def test_the_days_are_the_distinct_days_across_the_set(self):
        c = ev("bot:c", demo_run(21, 3, start=dt.date(2026, 10, 5)))         # Mon 5 to Wed 7 Oct
        d = ev("bot:d", demo_run(21, 3, start=dt.date(2026, 10, 8)))         # Thu 8, Fri 9, Mon 12 Oct
        p = RM.project([c, d], 10.0, 1500, 0.5, "DEMO")
        assert p["trades"] == 42 and p["days"] == 6 and p["projection"] is not None   # not "over 3 days"

    def test_an_unread_demo_is_waiting_not_empty(self):
        built = {"demo_trades": {"bot:runner": []}, "demo_known": {"bot:runner": False}, "stints": []}
        road = RM.road({"tests": {}, "refresh": None}, built, NOW14)
        assert road["evidence"]["DEMO_UNKNOWN"] == ["bot:runner"]
        seen = {b["barrier"]: b for b in road["barriers"]}["Too few trades"]["seen"]
        assert "waiting for MetaTrader's figures" in seen and "Nothing is on Demo yet" not in seen
        for m in road["milestones"]:
            d = m["DEMO"]
            assert d["projection"] is None and d["supported"] is False and d["improve"] == []
            assert d["words"].startswith("Waiting for MetaTrader's figures") and "0 demo trades" not in d["words"]
        assert "Nothing is on Demo yet" in {b["barrier"]: b for b in RM.road(
            {"tests": {}, "refresh": None}, EMPTY, NOW14)["barriers"]}["Too few trades"]["seen"]   # truly empty

    def test_a_stint_that_ended_before_the_window_is_not_waiting(self):
        # rv14 R5: MetaTrader unread. Two stints ended in August (before the 28-day window: known to be empty
        # here), one ended inside the window, one is running. Only the last two can hold evidence in the window.
        aug = lambda d: dt.datetime(2026, 8, d, 7, tzinfo=UTC)                       # noqa: E731
        stints = [{"id": "bot:crowd", "since": aug(3), "until": aug(20)},
                  {"id": "bot:bandbreaker", "since": aug(4), "until": aug(21)},
                  {"id": "bot:late", "since": aug(10), "until": dt.datetime(2026, 10, 2, 7, tzinfo=UTC)},
                  {"id": "bot:runner", "since": dt.datetime(2026, 10, 12, 6, tzinfo=UTC), "until": None}]
        ids = [s["id"] for s in stints]
        built = {"demo_trades": {i: [] for i in ids}, "demo_known": {i: False for i in ids}, "stints": stints}
        road = RM.road({"tests": {}, "refresh": None}, built, NOW14)
        assert road["evidence"]["DEMO_UNKNOWN"] == ["bot:late", "bot:runner"]          # not the two August ones
        seen = {b["barrier"]: b for b in road["barriers"]}["Too few trades"]["seen"]
        assert "(2 on Demo not read yet)" in seen
        # nobody on Demo in the window, only old stints: the road says so, never "waiting" for figures it has
        old = {"demo_trades": {i: [] for i in ids[:2]}, "demo_known": {i: False for i in ids[:2]},
               "stints": stints[:2]}
        road = RM.road({"tests": {}, "refresh": None}, old, NOW14)
        assert road["evidence"]["DEMO_UNKNOWN"] == []
        seen = {b["barrier"]: b for b in road["barriers"]}["Too few trades"]["seen"]
        assert "Nothing is on Demo yet" in seen and "not read yet" not in seen
        assert not any(m["DEMO"]["words"].startswith("Waiting for MetaTrader's figures") for m in road["milestones"])

    def test_a_lab_ideas_unread_record_is_not_bounded_by_its_stint(self):
        # rv14 S3: a lab idea's demo record (its own sqlite) is not cut at its stint's end: its last position
        # closes after the demote. Made up: demoted 6h before the window, its last trade 40h into it
        start = NOW14 - dt.timedelta(days=RM.WINDOW_DAYS)
        until = start - dt.timedelta(hours=6)
        st = [{"id": cid, "since": until - dt.timedelta(days=10), "until": until} for cid in ("lab:x", "bot:old")]
        tr = [{"closed": start + dt.timedelta(hours=40), "net": 5.0, "risk": 10.0, "r": 0.5}]
        ok = RM.evidence({"tests": {}}, {"stints": st[:1], "demo_trades": {"lab:x": tr},
                                         "demo_known": {"lab:x": True}}, NOW14)
        assert [e["id"] for e in ok["DEMO"]] == ["lab:x"]                  # readable: that trade is demo evidence
        built = {"stints": st, "demo_trades": {"lab:x": [], "bot:old": []},
                 "demo_known": {"lab:x": False, "bot:old": False}}
        road = RM.road({"tests": {}, "refresh": None}, built, NOW14)
        # unreadable: waiting, never "nothing"; a MetaTrader stint that ended before the window is still known empty
        assert road["evidence"]["DEMO"] == [] and road["evidence"]["DEMO_UNKNOWN"] == ["lab:x"]
        seen = {b["barrier"]: b for b in road["barriers"]}["Too few trades"]["seen"]
        assert "Nothing is on Demo yet" not in seen and "(1 on Demo not read yet)" in seen

    def test_a_lab_stint_that_ended_long_ago_is_not_waiting_for_figures(self):
        # rv14 S3 follow-up: the lab exemption is bounded (LAB_GRACE). Made up: lab stints that ended in June and
        # July with labdemo.sqlite unreadable are known empty for this window, not "on Demo not read yet"
        st = [{"id": "lab:june", "since": NOW14 - dt.timedelta(days=130), "until": NOW14 - dt.timedelta(days=120)},
              {"id": "lab:july", "since": NOW14 - dt.timedelta(days=95), "until": NOW14 - dt.timedelta(days=80)}]
        built = {"stints": st, "demo_trades": {"lab:june": [], "lab:july": []},
                 "demo_known": {"lab:june": False, "lab:july": False}}
        road = RM.road({"tests": {}, "refresh": None}, built, NOW14)
        assert road["evidence"]["DEMO_UNKNOWN"] == []

    def test_a_retired_idea_is_not_a_current_contributor(self):
        trades = [dict(t, risk=10.0) for t in demo_run(40, 8, start=dt.date(2026, 10, 1))]
        world = {"tests": {"lab:old": {"id": "lab:old", "nick": "Benched", "retired": True, "trades": trades},
                           "lab:new": {"id": "lab:new", "nick": "Current", "trades": trades}}, "refresh": None}
        road = RM.road(world, EMPTY, NOW14)
        assert [e["id"] for e in road["evidence"]["PAPER"]] == ["lab:new"]
        assert road["best"]["id"] == "lab:new"
        assert all("lab:old" not in [c["id"] for c in m["PAPER"]["contributors"]] for m in road["milestones"])

    def test_the_barriers_speak_of_todays_demo_rules(self):
        thin = ev("bot:t", demo_run(6, 2))
        got = {x["barrier"]: x for x in RM.barriers({"DEMO": [thin], "PAPER": []}, [], None, demo_places=8,
                                                    demo_cap_pct=6.0)}
        doing = got["Too few trades"]["doing"]
        assert "every UK trading day" in doing and "up to 8 at once" in doing
        assert "each week" not in doing and "up to 3" not in doing
        a, b = ev("bot:a", demo_run(40, 10)), ev("bot:b", demo_run(40, 10))
        corr = {x["barrier"]: x for x in RM.barriers({"DEMO": [a, b], "PAPER": []}, [], None,
                                                     demo_cap_pct=6.0)}["Strategies moving together"]["doing"]
        assert "within 6% of equity" in corr and "real-money plan caps planned risk at 2% at once" in corr
        deep = [{"league": "DEMO", "account": 1500.0, "projection": {"dd_pct_p90": 14.0}}]
        dd = {x["barrier"]: x for x in RM.barriers({"DEMO": [a], "PAPER": []}, deep, None)}["Drawdown"]["doing"]
        assert "more up days than down" not in dd and "goes back to Paper" in dd
