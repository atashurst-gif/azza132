"""The leagues, ROAD TO PROFIT, live readiness and a career on the TRADING SQUAD pages (Aaron, 14 Oct 2026).

EVERY FIGURE HERE IS MADE-UP TEST DATA (the made-up world of tests/test_squad_page.py)."""
from __future__ import annotations

import re

import pytest

from mintel.ops import squad as SQ
from mintel.ops import squad_pages as P
from tests.test_page_figures import a_day, serve
from tests.test_squad_page import NOW13, TECHNICAL, card, made_up_world, money, text_of


@pytest.fixture(scope="module")
def pages(tmp_path_factory):
    w = made_up_world(tmp_path_factory.mktemp("lpages"), ideas=120)
    httpd, get = serve(w.d, w.now, health={"checks": [], "summary": "all checks passing"})
    try:
        yield {p: get(p) for p in ("/", "/?t=all", "/leagues", "/leagues?t=all", "/road", "/readiness",
                                   "/bot?id=bot%3Arunner&t=all", "/demo")}
    finally:
        httpd.shutdown()


def line(page: str, league: str) -> str:
    return page.split(f'data-league="{league}">')[1].split("</a>")[0]


class TestTheFrontPage:
    def test_three_league_lines_in_their_place(self, pages):
        page = pages["/"]
        at = [page.index(x) for x in ('id="cards"', 'id="league-lines"', 'id="league"', 'id="factory"', 'id="road"',
                                      'id="readiness-card"')]
        assert at == sorted(at)
        assert re.findall(r'data-league="([A-Z]+)"', page) == ["PAPER", "DEMO", "REAL"]
        assert "Not connected yet" in line(page, "REAL")
        assert "the broker's figures" in line(page, "DEMO") and "1 active" in line(page, "DEMO")

    def test_the_lines_are_the_cards_figures_never_added(self, pages):
        for path in ("/", "/?t=all"):
            page = pages[path]
            paper = money(re.search(r'<span class="lv">(.*?)</span>', line(page, "PAPER")).group(1))
            demo = money(re.search(r'<span class="lv">(.*?)</span>', line(page, "DEMO")).group(1))
            assert paper == money(card(page, "PAPER PROFIT")) and demo == money(card(page, "DEMO PROFIT"))
            assert paper != demo

    def test_the_two_small_cards(self, pages):
        page = pages["/"]
        road = page.split('id="road"')[1].split("</div>", 1)[0]
        assert "ROAD TO PROFIT" in road
        ready = page.split('id="readiness-card"')[1]
        assert "LIVE READINESS" in ready and "NO ACTION REQUIRED YET" in ready

    def test_no_technical_clutter(self, pages):
        for path in ("/", "/?t=all", "/leagues", "/road", "/bot?id=bot%3Arunner&t=all"):
            txt = text_of(pages[path])
            for w in TECHNICAL:
                assert not re.search(rf"\b{w}\b", txt), (path, w)
            for w in ("sqlite", "heartbeat", "JSON", "Traceback", "dashboard error", "None", "nan"):
                assert w not in txt, (path, w)

    def test_the_navigation(self, pages):
        nav = pages["/leagues"].split('<nav class="sq-nav">')[1].split("</nav>")[0]
        assert re.findall(r">([^<]+)</a>", nav)[:3] == ["Squad", "Leagues", "Road to Profit"]
        assert 'href="/leagues" class=on' in nav
        assert 'href="/road" class=on' in pages["/road"]


class TestTheLeaguesPage:
    def test_three_leagues_apart_and_readiness(self, pages):
        page = pages["/leagues?t=all"]
        at = [page.index(x) for x in ('id="paper"', 'id="demo"', 'id="real"', 'id="readiness"')]
        assert at == sorted(at)
        t = text_of(page)
        assert "PAPER LEAGUE" in t and "DEMO LEAGUE" in t and "REAL MONEY LEAGUE" in t
        real = page.split('id="real"')[1].split("</section>")[0]
        assert "Not connected yet" in real and "without your approval" in real
        demo = page.split('id="demo"')[1].split("</section>")[0]
        assert 'data-id="bot:runner"' in demo and "+£11.25" in demo            # the broker's, after all costs
        assert "The bar:" in t and "40+ demo trades" in t
        assert pages["/readiness"] == pages["/leagues"]

    def test_profit_order_with_quality(self, pages):
        page = pages["/leagues?t=all"].split('id="paper"')[1].split("</section>")[0]
        nets = [money(m) for m in re.findall(r'<div class="lg-money">(.*?)</div>', page)]
        assert nets == sorted(nets, reverse=True) and len(nets) == SQ.TOP_SHOWN
        assert "★" in page and 'data-col="updays"' in page


class TestTheRoadPage:
    def test_milestones_evidence_and_the_1500_account(self, pages):
        page = pages["/road"]
        t = text_of(page)
        for k in ("ROAD TO PROFIT", "WHAT THE EVIDENCE SAYS", "MILESTONES", "A REALISTIC £1,500 ACCOUNT",
                  "BY ACCOUNT SIZE", "BARRIERS", "Ambitions, not targets"):
            assert k in t, k
        assert re.findall(r'data-milestone="(\d+)"', page) == ["25", "50", "100"]
        assert 'data-evidence="DEMO"' in page and 'data-evidence="PAPER"' in page
        demo = page.split('data-evidence="DEMO"')[1].split("</div>")[0]
        assert "Not supported yet" in demo and "too little to project from" in demo
        for b_ in ("£1,500", "£3,000", "£5,000", "£10,000"):
            assert b_ in t
        assert "your approval" in t


class TestTheCareer:
    def test_the_career_across_the_leagues(self, pages):
        page = pages["/bot?id=bot%3Arunner&t=all"]
        c = page.split('id="career"')[1]
        assert 'data-career="PAPER"' in c and 'data-career="DEMO"' in c and "REAL MONEY" in c
        demo = c.split('data-career="DEMO"')[1].split("</div></div>")[0]
        assert "+£11.25" in demo
        assert "Promoted to Demo" in c and "passes the bar" in c
        assert "BEST AND WORST CONDITIONS" in c and "VERSIONS" in c


class TestItAlwaysRenders:
    def test_no_data_at_all(self):
        b = SQ.board(None)
        x = P.extras(b, None)
        assert x == {"period": None, "all": None, "road": None, "readiness": None, "career": None, "equity": None}
        b["x"] = x
        for page in (SQ.render_squad(b), P.render_leagues_page(b, x), P.render_road_page(b, x),
                     SQ.render_bot_page(b, "lab:x")):
            assert "</html>" in page
        t = text_of(SQ.render_squad(b))
        assert "Building the evidence" in t and "NO ACTION REQUIRED YET" in t and "Not connected yet" in t
        assert "Building the evidence" in text_of(P.render_road_page(b, x))
        assert "Nobody on Demo in this period" in text_of(P.render_leagues_page(b, x))

    def test_served_with_no_records(self, tmp_path):
        d = a_day(tmp_path, records=False)
        httpd, get = serve(d, status={"bot": "STARTING"})
        try:
            got = [get(p) for p in ("/", "/leagues", "/road", "/readiness", "/bot?id=bot%3Arunner")]
        finally:
            httpd.shutdown()
        assert all("dashboard error" not in p and "</html>" in p for p in got)

    def test_escaped(self):
        bad = "<img src=x onerror=alert(1)>"
        b = SQ.board(None, "today", NOW13)
        x = {"readiness": {"stage": "approaching", "bots": [{
            "id": bad, "version": bad, "nick": bad, "stage": "approaching",
            "demo": {"net": 1.0, "trades": 21, "days": 5, "up_days": 3, "max_dd": 2.0, "mean": 0.1},
            "paper": {"net": 1.0, "trades": 21, "days": 5, "up_days": 3, "max_dd": 2.0, "mean": 0.1},
            "promising": [bad], "needed": [bad], "justified": False, "justified_words": bad, "earliest": bad}]}}
        for page in (P.render_leagues_page(b, x), SQ.render_squad(dict(b, x=x))):
            assert bad not in page and "&lt;img" in page


class TestWhileMetaTraderIsUnread:
    """rv14: an unread demo record is "—" and "waiting for MetaTrader's figures", never £0.00, 0 trades or
    "nothing is on Demo" (the made-up world with the runner on DEMO, MetaTrader's deals not read)."""

    @pytest.fixture(scope="class")
    def unread(self, tmp_path_factory):
        import json
        w = made_up_world(tmp_path_factory.mktemp("unread"), ideas=20)
        (w.data / "weekly.json").write_text(json.dumps({"demo_target": 8, "demo_risk_cap_pct": 6}))
        b = SQ.board(w.data, "all", w.now, sd=w.d.sd, deals=None, cache_seconds=0)
        x = P.extras(b, w.data, w.d.sd, None, test_id="bot:runner", cache_seconds=0)
        return b, x, w

    def test_the_demo_league_is_unknown_not_zero(self, unread):
        b, x, _w = unread
        assert x["all"]["demo_known"]["bot:runner"] is False
        demo = P.render_leagues_page(b, x).split('id="demo"')[1].split("</section>")[0]
        t = text_of(demo)
        assert "waiting for MetaTrader's figures" in t and "Momentum Runner" in t
        assert "£0.00" not in t and not re.search(r"(?<![\d.,])0 trades", t)
        assert "— trades" in t

    def test_the_career_demo_box_is_unknown_not_zero(self, unread):
        _b, x, _w = unread
        box = text_of(P.render_career(x["career"]).split('data-career="DEMO"')[1].split('<div class="box muted">')[0])
        assert "TRADES —" in box and "Worst drawdown —" in box and "£0.00" not in box

    def test_the_road_is_waiting_for_metatrader(self, unread):
        b, x, _w = unread
        t = text_of(P.render_road_page(b, x))
        assert "waiting for MetaTrader's figures" in t and "Waiting for MetaTrader's figures" in t
        assert "Nothing is on Demo yet" not in t and "0 demo trades" not in t

    def test_the_barriers_use_todays_demo_settings(self, unread):
        _b, x, _w = unread
        doing = {r["barrier"]: r for r in x["road"]["barriers"]}["Too few trades"]["doing"]
        assert "up to 8 at once" in doing and "every UK trading day" in doing and "each week" not in doing

    def test_the_readiness_card_keeps_its_last_answer(self, unread):
        from mintel.ops import readiness as RD
        b, _x, w = unread
        figs = {"net": 30.0, "trades": 22, "days": 5, "up_days": 4, "max_dd": 4.0, "mean": 1.36}
        bot = {"id": "bot:runner", "version": "current rules", "nick": "Momentum Runner", "stage": RD.APPROACHING,
               "demo": figs, "paper": figs, "needed": ["18 more demo trades"], "earliest": "2026-10-20",
               "promising": [], "justified": False, "justified_words": "Not yet"}
        RD.write(w.data, {"stage": RD.APPROACHING, "bots": [bot], "fingerprint": RD.fingerprint(RD.APPROACHING, [bot]),
                          "headline": "", "message": ""})                     # the last answer, made up
        x = P.extras(b, w.data, w.d.sd, None, cache_seconds=0)               # MetaTrader still unread
        assert x["readiness"]["stage"] == RD.APPROACHING and x["readiness"]["bots"][0]["stale"] is True
        card_html = P.readiness_card(x)
        assert "NO ACTION REQUIRED YET" not in card_html and "Momentum Runner" in card_html


class TestTheReadinessPace:
    """rv14 R4: the readiness card's expected frequency is trades a UK trading day since its first demo trade
    (the pace its proposed activation is worked out at), never trades on the days it happened to trade."""

    def test_thirty_trades_on_five_days_over_four_weeks(self):
        import datetime as dt
        from mintel.ops import readiness as RD
        utc = dt.timezone.utc
        days = [dt.date(2026, 9, 17), dt.date(2026, 9, 24), dt.date(2026, 10, 1), dt.date(2026, 10, 8),
                dt.date(2026, 10, 13)]
        tr = [{"closed": dt.datetime(d.year, d.month, d.day, 9 + i, tzinfo=utc), "net": 8.0 if i % 2 == 0 else -4.0,
               "risk": 10.0, "costs": 0.5, "symbol": "EURUSD"} for d in days for i in range(7)][:30]   # made up
        now = dt.datetime(2026, 10, 14, 12, tzinfo=utc)
        a = RD.assess("lab:x", tr, tr, None, now.date())
        bot = dict(a, id="lab:x", version="v1", nick="X", desc="", on_demo=True)
        rd = P._with_pace({"stage": a["stage"], "bots": [bot]},
                          {"demo_trades": {"lab:x": tr}, "demo_known": {"lab:x": True}}, now)
        t = text_of(P.render_readiness({"readiness": rd}))
        assert "Expected frequency: 1.5 trades a day" in t          # 30 / the 20 trading days from 17 Sep to 14 Oct
        assert "6.0 trades a day" not in t                           # not 30 / the 5 days it traded
        assert f"Proposed activation: {a['earliest']}" in t
        # a card carried over while its record cannot be read: unknown, never a figure
        stale = P._with_pace({"stage": a["stage"], "bots": [dict(bot, stale=True)]},
                             {"demo_trades": {"lab:x": []}, "demo_known": {"lab:x": False}}, now)
        assert stale["bots"][0]["per_day"] is None
        assert "Expected frequency: —" in text_of(P.render_readiness({"readiness": stale}))

    def test_the_pages_put_the_pace_on_each_card(self, tmp_path, monkeypatch):
        from mintel.ops import leagues as L
        from mintel.ops import readiness as RD
        w = made_up_world(tmp_path / "w", ideas=20)
        figs = {"net": 30.0, "trades": 5, "days": 2, "up_days": 2, "max_dd": 4.0, "mean": 2.0}
        bot = {"id": "bot:runner", "version": "current rules", "nick": "Momentum Runner", "stage": RD.APPROACHING,
               "demo": figs, "paper": figs, "needed": [], "earliest": "2026-11-02", "promising": [],
               "justified": False, "justified_words": "Not yet"}            # made up
        monkeypatch.setattr(RD, "review", lambda *a, **k: {"stage": RD.APPROACHING, "bots": [dict(bot)]})
        b = SQ.board(w.data, "all", w.now, sd=w.d.sd, deals=w.d.deals, cache_seconds=0)
        x = P.extras(b, w.data, w.d.sd, w.d.deals, cache_seconds=0)
        mine = x["all"]["demo_trades"]["bot:runner"]
        assert mine and x["readiness"]["bots"][0]["per_day"] == L.quality(mine, b["now"])["per_day"]
