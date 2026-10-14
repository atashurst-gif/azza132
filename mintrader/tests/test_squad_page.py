"""TRADING SQUAD, the new front page (Aaron, 13 Oct 2026: the full frontend redesign; the spec is
docs/MASTER_MISSION.md, "The dashboard").

EVERY FIGURE HERE IS MADE-UP TEST DATA written by these tests: the fake broker and records of
tests/test_page_figures.py, made-up bots' records, a made-up lab of real registered ideas with made-up practice
trades, a made-up weekly state and history. None of it is a result of any bot.
"""
from __future__ import annotations

import datetime as dt
import json
import random
import re
import sqlite3
from html import unescape
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from mintel.ops import squad as SQ
from mintel.ops.standing import period_view, uk_date
from tests.test_page_figures import DEALS, a_day, serve

UTC = dt.timezone.utc
NOW13 = dt.datetime(2026, 10, 13, 15, 0, tzinfo=UTC)            # Tuesday 13 Oct, 16:00 UK (BST)
RESET = dt.datetime(2026, 10, 5, 11, 30, 42, tzinfo=UTC)
RUNNER = 990_511
SINCE = dt.datetime(2026, 10, 12, 5, 30, tzinfo=UTC)             # Monday 12 Oct 06:30 UK: the runner on DEMO
SYMBOLS = {"GOLD": ("XAUUSD",), "INDEX": ("US500", "DE40", "UK100"), "FX_MAJOR": ("EURUSD", "GBPUSD", "USDJPY"),
           "FX_MINOR": ("EURGBP", "GBPJPY", "AUDJPY")}
TRADING_DAYS = [dt.date(2026, 10, d) for d in (5, 6, 7, 8, 9, 12, 13)]


def text_of(page: str) -> str:
    page = re.sub(r"<(style|script)>.*?</\1>", "", page, flags=re.S)
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", page)))


def money(txt: str) -> float:
    s = unescape(re.sub(r"<[^>]+>", "", txt)).strip()
    m = re.fullmatch(r"([+-]?)£([\d,]+\.\d\d)", s)
    assert m, f"not a money figure: {s!r}"
    v = float(m.group(2).replace(",", ""))
    return -v if m.group(1) == "-" else v


def card(page: str, key: str) -> str:
    """One headline card's figure (its html)."""
    m = re.search(rf'data-card="{re.escape(key)}"[^>]*><div class="sq-k">[^<]*</div><div class="sq-v num">(.*?)</div>',
                  page)
    assert m, key
    return m.group(1)


def rows_of(page: str) -> list[dict]:
    """The league's rows on a page, in order: {id, pos, nick, money, badge}."""
    out = []
    for m in re.finditer(r'<a class="lg-row[^"]*" href="[^"]*" data-id="([^"]*)" data-pos="(\d+)"(.*?)</a>', page,
                         re.S):
        body = m.group(3)
        out.append({"id": unescape(m.group(1)), "pos": int(m.group(2)),
                    "nick": unescape(re.search(r'<div class="lg-name">([^<]*)', body).group(1)),
                    "money": money(re.search(r'<div class="lg-money">(.*?)</div>', body).group(1)),
                    "badge": (re.search(r'data-badge="([A-Z]+)"', body) or [None, ""])[1]})
    return out


# ------------------------------------------------------------- the made-up world --
def _trade(t: dt.datetime, net: float) -> dict:
    return {"closed": t, "net": net, "symbol": "EURUSD"}


def _deal_pair(pos: int, magic: int, net: float, opened: dt.datetime, symbol: str = "US500") -> list[dict]:
    """A closed position as MetaTrader's two deals (the adapter's shape): 0.25 commission each way."""
    return [{"position": pos, "magic": magic, "symbol": symbol, "volume": 1.0, "profit": -0.25, "commission": -0.25,
             "is_entry": True, "time": opened},
            {"position": pos, "magic": magic, "symbol": symbol, "volume": 1.0, "profit": round(net + 0.25, 2),
             "commission": -0.25, "is_entry": False, "time": opened + dt.timedelta(minutes=12)}]


RUNNER_DEMO = [7.5, -2.0, 4.25, 3.0, -1.5]                     # its made-up DEMO results, after costs
RUNNER_DEALS = [d for i, n in enumerate(RUNNER_DEMO)
                for d in _deal_pair(8_100 + i, RUNNER, n, SINCE + dt.timedelta(hours=3 + 5 * i))]


def _own_db(data: Path, name: str, per_day: dict, mode: str = "PAPER") -> None:
    """A bot's own record, made up: {UK date: [nets]}."""
    con = sqlite3.connect(data / name)
    con.execute("CREATE TABLE IF NOT EXISTS trades(ticket INTEGER, symbol TEXT, side TEXT, opened_utc TEXT, "
                "closed_utc TEXT, net_pnl REAL, commission REAL, risk_money REAL, mode TEXT, exit_reason TEXT)")
    k = 50_000 + 1000 * len(name)
    for day, nets in per_day.items():
        for i, n in enumerate(nets):
            o = dt.datetime(day.year, day.month, day.day, 8, 0, tzinfo=UTC) + dt.timedelta(minutes=40 * i)
            con.execute("INSERT INTO trades(ticket, symbol, side, opened_utc, closed_utc, net_pnl, commission, "
                        "risk_money, mode, exit_reason) VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (k, "US500", "BUY", o.isoformat(), (o + dt.timedelta(minutes=25)).isoformat(), n, 0.5, 10.0,
                         mode, "TP"))
            k += 1
    con.commit()
    con.close()


def made_up_world(tmp_path: Path, ideas: int = 640, now: dt.datetime = NOW13, promoted: bool = True,
                  seed: int = 7) -> NS:
    """A big MADE-UP pool: the fake broker of test_page_figures (plus the runner's DEMO trades), the runner on
    DEMO since Monday, a winning Band Breaker, ``ideas`` real registered lab ideas with made-up practice trades
    (a few strong ones that pass the bar, some retired, some added on Saturday), a weekly history and a refresh."""
    from mintel.lab.candidate import registered
    from mintel.lab.ledger import LabLedger
    d = a_day(tmp_path, deals=DEALS + (RUNNER_DEALS if promoted else []), now=now)
    data = d.data
    rnd = random.Random(seed)
    days = [x for x in TRADING_DAYS if x <= uk_date(now)]
    _own_db(data, "runner.sqlite", {x: [6.0, 6.0, 6.0, -4.0, 5.0, 6.0, -2.0] for x in days if x < dt.date(2026, 10, 12)})
    _own_db(data, "crowd.sqlite", {x: [-3.0, 1.0, -2.5] for x in days})
    cands = registered()[:ideas]
    led = LabLedger(data / "lab" / "lab.sqlite")
    sat = dt.datetime(2026, 10, 10, 21, 0, tzinfo=UTC)
    ticket = 900_000_000
    for k, c in enumerate(cands):
        joined = sat if k % 9 == 0 else RESET
        led.register(c, f"long name {k}", now=joined)
        led.execute("UPDATE candidates SET stage='PRACTICE' WHERE id=?", (c.id,))
        star = k % 97 == 5
        mu = 4.0 if star else rnd.uniform(-3.5, 3.5)
        sd = 3.0 if star else 7.0
        for day in days:
            if dt.datetime(day.year, day.month, day.day, 23, tzinfo=UTC) < joined:
                continue
            for i in range(5 if star else rnd.choice((0, 0, 1, 1, 2, 3))):
                ticket += 1
                closed = dt.datetime(day.year, day.month, day.day, rnd.randint(6, 19), rnd.randint(0, 59), tzinfo=UTC)
                if closed > now:
                    continue
                net = round(rnd.gauss(mu, sd), 2)
                led.execute("INSERT INTO practice_trades(ticket, candidate_id, symbol, side, opened_utc, closed_utc, "
                            "net_gbp, commission_gbp, risk_gbp, fill_source) VALUES(?,?,?,?,?,?,?,?,?,?)",
                            (ticket, c.id, rnd.choice(SYMBOLS[c.markets]), 1,
                             (closed - dt.timedelta(minutes=30)).isoformat(), closed.isoformat(), net, 0.4, 13.0,
                             "tick"))
        if k % 31 == 3:
            led.retire(c.id, "a clear loser (made up)", now=sat)
    led.close()
    (data / "lab" / "refresh.json").write_text(json.dumps({
        "night": "2026-10-10", "written_utc": sat.isoformat(), "retired": [{"id": "x"}] * 12,
        "variants": [{"id": "v"}] * 18, "new": [{"id": "n"}] * 53, "pool": ideas, "target": 640}))
    w = data / "weekly"
    w.mkdir(exist_ok=True)
    item = {"id": "bot:runner", "kind": "bot", "bot": "runner", "label": "Momentum Runner", "part": "",
            "magic": RUNNER, "process": "trader", "risk_gbp": 10.0, "since_utc": SINCE.isoformat(),
            "plan": "2026-10-09"}
    (w / "state.json").write_text(json.dumps({"schema": 1, "promoted": [item] if promoted else [],
                                              "restart_pending": []}))
    if promoted:
        (w / "history.jsonl").write_text(json.dumps(dict(item, event="promote", ts_utc=SINCE.isoformat(),
                                                         why="passes the bar")) + "\n")
    return NS(d=d, data=data, now=now, cands=cands)


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    return made_up_world(tmp_path_factory.mktemp("squad"))


def board_of(w: NS, timeline: str = "today", now: dt.datetime = None) -> dict:
    return SQ.board(w.data, timeline, now or w.now, sd=w.d.sd, deals=w.d.deals, cache_seconds=0)


# ================================================================ the timelines --
class TestTimelines:
    def test_today_five_trading_days_a_month_and_all_time(self):
        b = {k: SQ.bounds(k, NOW13, RESET) for k in SQ.TIMELINE_KEYS}
        assert b["today"] == (dt.datetime(2026, 10, 12, 23, tzinfo=UTC), dt.datetime(2026, 10, 13, 23, tzinfo=UTC))
        # Tue 13, Mon 12, Fri 9, Thu 8, Wed 7: the weekend is not a trading day
        assert b["5d"] == (dt.datetime(2026, 10, 6, 23, tzinfo=UTC), dt.datetime(2026, 10, 13, 23, tzinfo=UTC))
        assert b["month"][0] == RESET and b["all"] == (RESET, b["today"][1])      # never before the reset

    def test_a_weekend_looks_back_to_friday_and_monday_compares_with_friday(self):
        sat = dt.datetime(2026, 10, 17, 12, tzinfo=UTC)
        assert SQ.trading_days_back(uk_date(sat), 5) == [dt.date(2026, 10, d) for d in (16, 15, 14, 13, 12)]
        assert SQ.bounds("5d", sat, RESET)[0] == dt.datetime(2026, 10, 11, 23, tzinfo=UTC)
        assert SQ.previous_trading_day(dt.date(2026, 10, 19)) == dt.date(2026, 10, 16)

    def test_uk_days_across_the_clocks_going_back(self):
        mon = dt.datetime(2026, 10, 26, 12, tzinfo=UTC)                   # GMT from Sunday 25 Oct
        a, b = SQ.bounds("5d", mon, RESET)
        assert a == dt.datetime(2026, 10, 19, 23, tzinfo=UTC) and b == dt.datetime(2026, 10, 27, 0, tzinfo=UTC)
        assert SQ.bounds("month", mon, RESET)[0] == RESET                   # 27 Sep is before the reset

    def test_a_month_after_the_reset_starts_a_month_back(self):
        nov = dt.datetime(2026, 11, 20, 12, tzinfo=UTC)
        assert SQ.bounds("month", nov, RESET)[0] == dt.datetime(2026, 10, 21, 0, tzinfo=UTC) - dt.timedelta(hours=1)
        assert SQ.bounds("all", nov, RESET)[0] == RESET

    def test_every_figure_follows_the_timeline(self, world):
        today, five, total = (board_of(world, k) for k in ("today", "5d", "all"))
        for b, key in ((today, "today"), (five, "5d"), (total, "all")):
            start, end = SQ.bounds(key, world.now, RESET)
            for r in b["league"]:
                t = b["tests"][r["id"]]
                mine = [x for x in t["trades"] if start <= x["closed"] < end]
                assert r["trades"] == len(mine) and r["net"] == round(sum(x["net"] for x in mine), 2)
        assert total["head"]["trades"] > five["head"]["trades"] > today["head"]["trades"] > 0
        assert SQ.timeline_of("nonsense") == "today" and SQ.timeline_of("5D") == "5d"

    def test_the_buttons_and_the_address(self, world):
        httpd, get = serve(world.d, world.now)
        try:
            page = get("/?t=5d")
        finally:
            httpd.shutdown()
        for k, w in SQ.TIMELINES:
            assert f'>{w}</a>' in page
        assert '<a href="/?t=5d" class=on data-t="5d">5 DAYS</a>' in page and 'href="/" data-t="today"' in page
        assert "Wed 7 Oct – Tue 13 Oct" in text_of(page)


# ================================================================ league, arrows, streaks --
def a_test(cid: str, by_day: dict, **extra) -> dict:
    trades = [_trade(dt.datetime(day.year, day.month, day.day, 9, i, tzinfo=UTC), n)
              for day, nets in by_day.items() for i, n in enumerate(nets)]
    return dict({"id": cid, "nick": cid.title(), "desc": "", "kind": "lab", "ref": cid, "ready": False,
                 "live": False, "retired": False, "trades": trades}, **extra)


MON, TUE = dt.date(2026, 10, 12), dt.date(2026, 10, 13)


class TestLeagueAndArrows:
    def _tests(self) -> dict:
        return {t["id"]: t for t in (
            a_test("a", {MON: [10.0], TUE: [-8.0]}),     # yesterday #1, today 1 after 2 days: 2.0
            a_test("b", {MON: [5.0], TUE: [20.0]}),      # 25.0
            a_test("c", {MON: [1.0], TUE: [1.0]}),       # 2.0 as well, but won both: the better win rate first
            a_test("d", {MON: [-1.0], TUE: [40.0]}),     # 39.0: from #4 to #1
            a_test("e", {TUE: [0.5]}))}                  # new today

    def test_best_profit_first_and_moves_against_the_previous_day(self):
        tests = self._tests()
        start = SQ.bounds("all", NOW13, RESET)[0]
        cur = SQ.league_of(tests, start, dt.datetime(2026, 10, 13, 23, tzinfo=UTC))
        prev = SQ.league_of(tests, start, dt.datetime(2026, 10, 12, 23, tzinfo=UTC))
        SQ.apply_moves(cur, prev)
        assert [r["id"] for r in cur] == ["d", "b", "c", "a", "e"] and [r["pos"] for r in cur] == [1, 2, 3, 4, 5]
        assert [r["id"] for r in prev] == ["a", "b", "c", "d"]
        moves = {r["id"]: SQ.move_words(r) for r in cur}
        assert moves["d"] == ("↑3", "up", "#4 → #1") and moves["a"] == ("↓3", "down", "#1 → #4")
        assert moves["b"] == ("–", "same", "#2 → #2") and moves["c"][0] == "–" and moves["e"][0] == "NEW"
        assert cur[0]["badge"] == SQ.RISING                    # up 3 places

    def test_the_page_lists_them_best_first_with_the_top_20_and_show_all(self, world):
        b = board_of(world, "all")
        n = len(b["league"])
        assert n > 300
        page = SQ.render_squad(b)
        rows = rows_of(page)
        assert len(rows) == SQ.TOP_SHOWN and [r["pos"] for r in rows] == list(range(1, 21))
        assert [r["money"] for r in rows] == sorted((r["money"] for r in rows), reverse=True)
        assert f'href="/?t=all&all=1#league">Show all {n}</a>' in page
        everything = rows_of(SQ.render_squad(b, show_all=True))
        assert len(everything) == n and everything[:20] == rows
        assert [r["money"] for r in everything] == sorted((r["money"] for r in everything), reverse=True)


class TestStreaks:
    def d(self, day: int, hour: int = 9) -> dt.datetime:
        return dt.datetime(2026, 10, day, hour, tzinfo=UTC)

    def test_consecutive_profitable_days_up_to_the_latest(self):
        t = [_trade(self.d(5), 3), _trade(self.d(6), -1), _trade(self.d(7), 2), _trade(self.d(8), 1),
             _trade(self.d(8), -0.5), _trade(self.d(9), 4)]
        assert SQ.streak_of(t) == 3                             # 7, 8 (+0.5 that day) and 9; the 6th ended it

    def test_a_day_without_a_trade_neither_breaks_nor_counts_and_a_level_day_breaks(self):
        assert SQ.streak_of([_trade(self.d(7), 2), _trade(self.d(9), 2), _trade(self.d(12), 2)]) == 3
        assert SQ.streak_of([_trade(self.d(7), 2), _trade(self.d(8), 1), _trade(self.d(8), -1)]) == 0
        assert SQ.streak_of([]) == 0

    def test_a_trade_late_in_the_evening_counts_on_its_uk_day(self):
        late = dt.datetime(2026, 10, 8, 23, 30, tzinfo=UTC)     # 00:30 UK on Friday 9 Oct (BST)
        assert SQ.day_nets([_trade(late, 5)]) == {dt.date(2026, 10, 9): 5}
        assert SQ.streak_of([_trade(self.d(8, 12), -2), _trade(late, 5)]) == 1

    def test_the_league_and_the_best_streak_card(self, world):
        b = board_of(world, "5d")
        best = max(b["league"], key=lambda r: (r["streak"], r["net"], r["id"]))
        assert b["head"]["streak"] == {"id": best["id"], "nick": best["nick"], "days": best["streak"]}
        page = SQ.render_squad(b)
        assert f'🔥 {best["streak"]} days' in card(page, "BEST STREAK") and f'data-streak="{best["streak"]}"' in page
        assert '<span class="fire">🔥 ' in page


# ================================================================ badges --
class TestBadges:
    def r(self, **kw) -> dict:
        return dict({"ready": False, "move": None, "streak": 0, "retired": False, "trades": 15, "net": 5.0}, **kw)

    def test_each_badge_and_their_order(self):
        b = SQ.badge_of
        assert b(self.r(ready=True, move=5, streak=4, trades=3)) == SQ.CHAMPION
        assert b(self.r(move=3, streak=4)) == SQ.RISING and b(self.r(move=2, streak=4)) == SQ.HOT
        assert b(self.r(streak=3, trades=2)) == SQ.HOT and b(self.r(streak=2, trades=9)) == SQ.TESTING
        assert b(self.r(trades=20, net=-0.01)) == SQ.BENCHED and b(self.r(trades=19, net=-50)) == ""
        assert b(self.r(trades=25, net=0.0)) == "" and b(self.r(retired=True, trades=2)) == SQ.BENCHED
        assert b(self.r()) == ""

    def test_demo_ready_is_passing_the_bar_and_not_on_demo(self):
        rows = SQ.apply_moves([dict(self.r(ready=True), id="x", pos=1), dict(self.r(ready=True, live=True), id="y",
                                                                            pos=2)], [])
        assert [r["demo_ready"] for r in rows] == [True, False]

    def test_on_the_page(self, world):
        b = board_of(world, "all")
        page = SQ.render_squad(b, show_all=True)
        got = {r["id"]: r["badge"] for r in rows_of(page)}
        for r in b["league"]:
            assert got[r["id"]] == r["badge"] == SQ.badge_of(r)
        assert {SQ.CHAMPION, SQ.HOT, SQ.TESTING, SQ.BENCHED} <= set(got.values())
        assert got["bot:runner"] == SQ.CHAMPION                          # passes the weekly pick's bar
        runner = page.split('data-id="bot:runner"')[1].split("</a>")[0]
        assert SQ.ON_DEMO in runner and SQ.DEMO_READY not in runner        # already on DEMO
        champs = [r for r in b["league"] if r["badge"] == SQ.CHAMPION and r["id"] != "bot:runner"]
        assert champs and all(SQ.DEMO_READY in page.split(f'data-id="{r["id"]}"')[1].split("</a>")[0]
                              for r in champs)
        retired = [r for r in b["league"] if r["retired"]]
        assert retired and all(r["badge"] in (SQ.BENCHED, SQ.HOT, SQ.RISING) for r in retired)


# ================================================================ DEMO --
class TestDemo:
    def test_hidden_when_nothing_is_on_demo_and_nothing_traded(self, tmp_path):
        w = made_up_world(tmp_path, ideas=20, promoted=False)
        b = board_of(w, "today")                                       # the account traded on 8-9 Oct only
        page = SQ.render_squad(b)
        assert not b["show_demo"] and 'id="demo-squad"' not in page and 'data-card="DEMO PROFIT"' not in page
        assert "Nobody on Demo yet" in text_of(SQ.render_demo_page(b))

    def test_the_card_alone_when_the_account_traded_but_no_test_is_on_demo(self, tmp_path):
        w = made_up_world(tmp_path, ideas=20, promoted=False)
        b = board_of(w, "all")
        page = SQ.render_squad(b)
        assert 'data-card="DEMO PROFIT"' in page and 'id="demo-squad"' not in page
        assert money(card(page, "DEMO PROFIT")) == period_view(w.d.sd, w.d.deals, "total", now=w.now)["made"]

    def test_shown_with_metatraders_figures_when_a_test_is_on_demo(self, world):
        b = board_of(world, "5d")
        page = SQ.render_squad(b)
        team = page.split('id="demo-squad"')[1].split("</section>")[0]
        assert "DEMO SQUAD · THE FIRST TEAM" in team and "Momentum Runner" in team
        assert money(re.search(r'<div class="tm-money">(.*?)</div>', team).group(1)) == round(sum(RUNNER_DEMO), 2)
        assert "60%" in team and ">5<small> trades" in team                # 3 of the 5 won
        assert 'data-moment="promo:bot:runner:' in team                  # promoted this week: a moment, once
        # the account's own figure for the five days, the broker's: the runner's five and 8-9 Oct's
        view = period_view(world.d.sd, world.d.deals, "custom", "2026-10-07", "2026-10-13", now=world.now)
        assert money(card(page, "DEMO PROFIT")) == view["trading"]

    def test_unknown_until_metatrader_is_read(self, world):
        b = SQ.board(world.data, "5d", world.now, sd={}, deals=None, cache_seconds=0)
        page = SQ.render_squad(b)
        assert b["demo"] is None and b["demo_rows"][0]["net"] is None
        assert "waiting for MetaTrader" in text_of(page)


class TestPaperAndDemoAreNeverAdded:
    def test_the_paper_card_is_the_tests_own_records_alone(self, world):
        b = board_of(world, "all")
        page = SQ.render_squad(b)
        paper, demo = money(card(page, "PAPER PROFIT")), money(card(page, "DEMO PROFIT"))
        own = sum(r["net"] for r in b["league"])
        assert paper == round(own, 2)
        # the lab's own rows, read here straight from the made-up file: they are in the paper figure as they are
        con = sqlite3.connect(world.data / "lab" / "lab.sqlite")
        lab = con.execute("SELECT ROUND(SUM(net_gbp), 2) FROM practice_trades WHERE closed_utc >= ? AND closed_utc <= ?",
                          (RESET.isoformat(), world.now.isoformat())).fetchone()[0]          # since the reset
        con.close()
        assert round(sum(r["net"] for r in b["league"] if r["kind"] == "lab"), 2) == lab
        assert demo == period_view(world.d.sd, world.d.deals, "total", now=world.now)["made"]
        assert paper != round(paper + demo, 2) and round(paper + demo, 2) not in {
            money(m) for m in re.findall(r'<div class="sq-v num">(.*?)</div>', page) if "£" in m}
        # the DEMO trades never enter the league: the runner's paper record stops before its promotion
        runner = next(r for r in b["league"] if r["id"] == "bot:runner")
        assert all(x["closed"] < SINCE for x in b["tests"]["bot:runner"]["trades"]) and runner["net"] > 0


# ================================================================ factory, latest action --
class TestFactory:
    def test_the_counts(self, world):
        b = board_of(world, "5d")
        f = b["factory"]
        from mintel.ops import weekly as W
        pool = W._pool(world.data, world.data / "config.json", world.now, 60)
        assert f["active"] == len(pool) > 600
        assert f["new"] == sum(1 for k in range(len(world.cands)) if k % 9 == 0)         # joined on Saturday
        assert f["promising"] == sum(1 for r in b["league"] if r["net"] > 0 and r["trades"] >= 10)
        assert f["champions"] == sum(1 for c in pool if c.get("passes")) >= 2
        page = SQ.render_squad(b)
        for k, v in f.items():
            assert f'data-count="{k}"><b>{v:,}</b>' in page

    def test_the_pure_counts(self):
        now = NOW13
        rows = [{"net": 5.0, "trades": 10}, {"net": 5.0, "trades": 9}, {"net": -1.0, "trades": 30}]
        lab = [{"created_utc": (now - dt.timedelta(days=2)).isoformat()},
               {"created_utc": (now - dt.timedelta(days=8)).isoformat()}, {"created_utc": None}]
        assert SQ.factory_counts([{"passes": True}, {}], rows, lab, now) == {"active": 2, "new": 1, "promising": 1,
                                                                             "champions": 1}


class TestLatestAction:
    def nick(self, cid, e):
        return {"bot:runner": "Momentum Runner"}.get(cid, cid)

    def test_the_newest_of_promotions_refreshes_and_the_top_ten(self):
        promo = {"event": "promote", "id": "bot:runner", "ts_utc": "2026-10-12T05:30:00+00:00"}
        refresh = {"written_utc": "2026-10-10T21:00:00+00:00", "new": [1] * 40, "variants": [1] * 10, "retired": [1]}
        a = SQ.latest_action([], [], [promo], refresh, self.nick, NOW13)
        assert (a["emoji"], a["text"]) == ("🚀", "Momentum Runner promoted to Demo")
        a = SQ.latest_action([], [], [], refresh, self.nick, NOW13)
        assert a["text"] == "50 fresh challengers entered testing, 1 benched"
        demote = dict(promo, event="demote", ts_utc="2026-10-13T09:00:00+00:00")
        assert SQ.latest_action([], [], [promo, demote], None, self.nick, NOW13)["text"] == \
            "Momentum Runner back to Paper"
        assert SQ.latest_action([], [], [], None, self.nick, NOW13) is None

    def test_a_new_entry_in_the_top_ten_and_a_new_number_one(self):
        def row(cid, pos, net, hour):
            return {"id": cid, "nick": cid.upper(), "pos": pos, "net": net,
                    "last": dt.datetime(2026, 10, 13, hour, tzinfo=UTC)}
        prev = [row(f"t{k}", k, 20 - k, 9) for k in range(1, 13)]
        cur = [row("t1", 1, 30, 9)] + [row(f"t{k}", k, 20 - k, 9) for k in range(2, 6)] + [row("t12", 6, 14, 14)]
        promo = {"event": "promote", "id": "bot:runner", "ts_utc": "2026-10-12T05:30:00+00:00"}
        a = SQ.latest_action(cur, prev, [promo], None, self.nick, NOW13)
        assert (a["emoji"], a["text"]) == ("👀", "T12 enters the Top 10 at #6")
        cur[0], cur[1] = dict(cur[1], pos=1), dict(cur[0], pos=2)
        assert SQ.latest_action(cur, prev, [], None, self.nick, NOW13)["text"] == "T2 has taken top spot"

    def test_on_the_page(self, world):
        b = board_of(world, "5d")
        page = SQ.render_squad(b)
        latest = page.split('id="latest"')[1].split("</div></div></div>")[0]
        assert unescape(b["latest"]["text"]) in unescape(latest)
        assert "LATEST ACTION" in text_of(page) and "STRATEGY FACTORY" in text_of(page)


# ================================================================ nicknames --
class TestNicknames:
    def items(self, cands, when=lambda k: "2026-10-05T11:30:42+00:00"):
        return [{"id": f"lab:{c.id}", "kind": "lab", "family": c.family, "markets": c.markets, "session": c.session,
                 "params": c.pdict(), "order": when(k)} for k, c in enumerate(cands)]

    def test_every_test_has_its_own_name_the_same_every_time(self):
        from mintel.engine.tactics import APPROACH_NAMES
        from mintel.lab.candidate import registered
        cands = registered(True)
        items = self.items(cands)
        items += [{"id": f"bot:{b}", "kind": "bot", "bot": b} for b in SQ.BOT_NAMES]
        items += [{"id": f"tnb:{a}:{k}", "kind": "tnb", "tactic": a, "group": k}
                  for a in APPROACH_NAMES for k in ("FX_MAJOR", "FX_MINOR", "INDEX", "GOLD", "SILVER")]
        names = SQ.assign_nicknames(items)
        assert len(names) == len(items) and len(set(names.values())) == len(items)          # unique
        assert SQ.assign_nicknames(list(reversed(items))) == names                         # order does not matter
        assert all(len(n) <= 34 for n in names.values())
        assert names["bot:runner"] == "Momentum Runner" and names["tnb:MOMENTUM_CONTINUATION:GOLD"] == "Gold Momentum"

    def test_a_newer_idea_never_renames_an_older_one(self):
        from mintel.lab.candidate import registered
        cands = registered(True)
        old = self.items(cands[:400])
        before = SQ.assign_nicknames(old)
        new = self.items(cands[400:], when=lambda k: "2026-10-10T21:00:00+00:00")
        after = SQ.assign_nicknames(old + new)
        assert all(after[k] == v for k, v in before.items())

    def test_names_come_from_the_ideas_own_fields(self):
        t = {"kind": "lab", "family": "RANGE_BREAK", "markets": "GOLD", "session": "LONDON",
             "params": {"lookback_min": 60}}
        assert SQ.nickname_base(t) == "London Gold Breakout 60"
        assert SQ.nickname_base(dict(t, family="MOMENTUM_CONTINUATION", session="BOTH", markets="INDEX",
                                     params={"pullback": "NONE"})) == "Index Rocket"
        assert SQ.nickname_base(dict(t, family="OPENING_RANGE", session="NEWYORK", markets="FX_MAJOR",
                                     params={"range_min": 15})) == "NY Major Opener 15"
        two = [dict(t, id="b", order="2"), dict(t, id="a", order="1"), dict(t, id="c", order="3")]
        assert SQ.assign_nicknames(two) == {"a": "London Gold Breakout 60", "b": "London Gold Breakout 60 Mk2",
                                            "c": "London Gold Breakout 60 Mk3"}

    def test_the_page_shows_the_nickname_and_the_descriptive_name(self, world):
        b = board_of(world, "all")
        page = SQ.render_squad(b)
        r = b["league"][0]
        row = page.split(f'data-id="{r["id"]}"')[1].split("</a>")[0]
        assert f'<div class="lg-name">{r["nick"]}' in unescape(row) and \
            f'<div class="lg-desc">{r["desc"]}</div>' in unescape(row)
        assert len({x["nick"] for x in b["league"]}) == len(b["league"])
        again = board_of(world, "all")
        assert {x["id"]: x["nick"] for x in again["league"]} == {x["id"]: x["nick"] for x in b["league"]}


# ================================================================ a test's own page --
class TestBotPage:
    def test_plain_words_four_sections_and_the_advanced_link(self, world):
        b = board_of(world, "all")
        lab = next(r for r in b["league"] if r["kind"] == "lab" and r["net"] > 0 and r["trades"] >= 10)
        page = SQ.render_bot_page(b, lab["id"])
        t = text_of(page)
        for h in ("WHAT IT DOES", "WHEN IT LIKES TO TRADE", "WHAT IT HAS MADE", "WHY IT IS DOING WELL"):
            assert h in t, h
        for s in ("does", "when", "made", "why"):
            assert f'data-section="{s}"' in page
        assert f'href="/lab?id={lab["id"][4:]}" data-advanced>VIEW ADVANCED DETAILS' in page
        assert lab["nick"] in t and lab["desc"] in t
        made = page.split('data-section="made"')[1].split('data-section="why"')[0]
        assert money(re.search(r'<div class="sq-v num">(.*?)</div>', made).group(1)) == lab["net"]
        assert f'{lab["trades"]} trades' in text_of(made) and "Not on Demo" in text_of(made)
        assert "closed between" in t and "UK" in t

    def test_a_bot_on_demo_shows_paper_and_demo_apart(self, world):
        b = board_of(world, "5d")
        page = SQ.render_bot_page(b, "bot:runner")
        made = page.split('data-section="made"')[1].split('data-section="why"')[0]
        figs = [money(m) for m in re.findall(r'<div class="sq-v num">(.*?)</div>', made)]
        paper = next(r for r in b["league"] if r["id"] == "bot:runner")["net"]
        assert figs == [paper, round(sum(RUNNER_DEMO), 2)]
        assert "PAPER" in text_of(made) and "DEMO · MetaTrader" in text_of(made)
        assert 'href="/?bot=momentum_runner" data-advanced' in page
        assert "Follows Trend &amp; Breakout" in page

    def test_the_words_never_claim_more_than_the_numbers(self):
        early = SQ.figures([_trade(NOW13, 4.0), _trade(NOW13, 3.0)])
        assert SQ.why_lines(early, [])[1][0].startswith("Still early: only 2 trades")
        losing = [_trade(NOW13 - dt.timedelta(minutes=i), n) for i, n in enumerate([5.0, -8.0] * 6)]
        title, lines = SQ.why_lines(SQ.figures(losing), losing)
        assert title == "HOW IT IS DOING" and lines[0].startswith("Not in profit in this period: -£18.00")
        assert "average loss (£8.00) is bigger than its average win (£5.00)" in lines[1]
        few_wins = [_trade(NOW13 - dt.timedelta(minutes=i), n) for i, n in enumerate([12.0, -2.0, -2.0] * 4)]
        title, lines = SQ.why_lines(SQ.figures(few_wins), few_wins)
        assert title == "WHY IT IS DOING WELL" and "wins only 33%" in lines[0] and "6.0× its average loss" in lines[0]
        clean = [_trade(NOW13 - dt.timedelta(minutes=i), 2.0) for i in range(10)]
        assert "has not lost a trade" in SQ.why_lines(SQ.figures(clean), clean)[1][0]

    def test_served_and_an_unknown_id(self, world):
        httpd, get = serve(world.d, world.now)
        try:
            ok, nope = get("/bot?id=bot%3Arunner&t=5d"), get("/bot?id=nope%3Cscript%3E")
        finally:
            httpd.shutdown()
        assert "WHAT IT DOES" in ok and 'class=on data-t="5d"' in ok and "No such test" in nope
        assert "<script>" not in nope.split("</style>")[1].split("<script>")[0]


# ================================================================ the front page itself --
TECHNICAL = ("JSON", "json", "heartbeat", "Heartbeat", "ATR", "ADX", "sqlite", "SQLite", "Traceback", "magic",
             "ticket", "VWAP", "vwap", "stdout", "API", "PID")


class TestTheFrontPage:
    @pytest.fixture(scope="class")
    def pages(self, world):
        httpd, get = serve(world.d, world.now, health={"checks": [], "summary": "all checks passing"})
        try:
            yield {p: get(p) for p in ("/", "/?t=5d", "/?t=all&all=1", "/demo", "/factory", "/advanced")}
        finally:
            httpd.shutdown()

    def test_only_what_the_spec_lists_in_its_order(self, pages):
        page = pages["/?t=5d"]
        order = ['class="sq-hero', 'id="cards"', 'id="league"', 'id="demo-squad"', 'id="latest"', 'id="factory"']
        at = [page.index(x) for x in order]
        assert at == sorted(at)
        for k in ("PAPER PROFIT", "WIN RATE", "TRADES", "WINNING BOTS", "BEST STREAK", "DEMO PROFIT"):
            assert f'data-card="{k}"' in page
        assert "TRADING <span>SQUAD</span>" in page and "<title>Trading Squad</title>" in page
        for absent in ("The coach", "The plan", "Technical detail", "What the bot is looking at", "Balance",
                       "s-checks", "class=\"side\"", "location.reload"):
            assert absent not in page, absent

    def test_no_technical_words(self, pages):
        for path in ("/", "/?t=5d", "/?t=all&all=1", "/demo", "/factory"):
            txt = text_of(pages[path])
            for w in TECHNICAL:
                assert not re.search(rf"\b{w}\b", txt), (path, w)
            for w in ("sqlite", "heartbeat", "JSON", "Traceback"):
                assert w not in pages[path], (path, w)

    def test_the_navigation_and_the_health_dot(self, pages):
        page = pages["/"]
        nav = page.split('<nav class="sq-nav">')[1].split("</nav>")[0]
        # 14 Oct: Leagues and Road to Profit join the navigation
        assert re.findall(r">([^<]+)</a>", nav) == ["Squad", "Leagues", "Road to Profit", "Demo", "Factory", "Advanced"]
        assert 'href="/" class=on' in nav and 'href="/advanced"' in nav
        assert '<span class="sq-health ok" title="The trader"><i></i>running</span>' in page
        assert "Overview" in pages["/advanced"] and "s-acct" in pages["/advanced"]     # the old Home, kept
        assert 'class="on"' in pages["/demo"] or "class=on" in pages["/demo"]

    def test_the_old_addresses_still_open_the_old_pages(self, world):
        httpd, get = serve(world.d, world.now)
        try:
            week, bot, live = get("/?period=week"), get("/?bot=band_breaker"), get("/?show=live")
        finally:
            httpd.shutdown()
        assert "s-acct" in week and "TRADING <span>SQUAD</span>" not in week
        assert "Band Breaker" in bot and "<h1>Real money</h1>" in live

    def test_the_health_dot_says_when_something_is_wrong(self):
        assert SQ.health_of({"status": {"bot": "RUNNING"}}) == ("ok", "running")
        assert SQ.health_of({"status": {"bot": "RUNNING"}, "health": {"safe_mode": True}})[0] == "bad"
        assert SQ.health_of({"status": {"bot": "WAITING FOR METATRADER"}})[0] == "warn"
        assert SQ.health_of({}) == ("warn", "starting")

    def test_the_refresh_swaps_in_place_and_moments_are_shown_once(self, pages):
        page = pages["/"]
        assert "setInterval(swap,10000)" in page and "DOMParser" in page and "location.reload" not in page
        assert "html:not(.settled) .rise" in SQ.SQUAD_CSS and "classList.add('settled')" in page
        assert "localStorage" in page and "try{" in page and "squad-seen" in page
        assert "prefers-color-scheme" in page and 'id="theme-btn"' in page
        assert '<noscript><meta http-equiv="refresh" content="10"></noscript>' in page

    def test_a_record_five_days_and_a_champion_are_moments(self, pages):
        page = pages["/?t=5d"]
        assert 'data-record5="' in page and 'data-moment="champ:' in pages["/?t=all&all=1"]


class TestItAlwaysRenders:
    def test_no_data_at_all(self, tmp_path):
        for b in (SQ.board(None), SQ.board(str(tmp_path / "nothing"), "5d", NOW13),
                  SQ.board(str(tmp_path), "all", NOW13, sd={"start": RESET.isoformat(), "made": 0.0}, deals=[])):
            page = SQ.render_squad(b)
            t = text_of(page)
            assert "No paper trades in this period yet" in t and "All quiet so far" in t
            assert 'id="demo-squad"' not in page and 'data-card="DEMO PROFIT"' not in page
            assert money(card(page, "PAPER PROFIT")) == 0.0 and "WINNING BOTS" in t
            for r in (SQ.render_demo_page(b), SQ.render_factory_page(b), SQ.render_bot_page(b, "lab:x")):
                assert "</html>" in r

    def test_served_with_no_records(self, tmp_path):
        d = a_day(tmp_path, records=False)
        httpd, get = serve(d, status={"bot": "STARTING"})
        try:
            got = [get(p) for p in ("/", "/?t=month", "/demo", "/factory", "/bot?id=bot%3Arunner")]
        finally:
            httpd.shutdown()
        assert all("dashboard error" not in p and "</html>" in p for p in got)
        assert "starting" in got[0]

    def test_escaped(self):
        bad = "<img src=x onerror=alert(1)>"
        tests = {"lab:z": a_test("lab:z", {TUE: [3.0]}, nick=bad, desc=bad)}
        b = SQ.board(None, "today", NOW13)
        rows = SQ.league_of(tests, *SQ.bounds("today", NOW13, RESET))
        SQ.apply_moves(rows, [])
        b.update(league=rows, head=SQ.headline(rows), tests=tests)
        for page in (SQ.render_squad(b), SQ.render_bot_page(b, "lab:z")):
            assert bad not in page and "&lt;img" in page


class TestPhoneWidth:
    def test_no_fixed_width_wider_than_a_phone(self):
        css = re.sub(r"@media\s*\([^)]*\)", "", SQ.SQUAD_CSS)          # a breakpoint is not a width
        for prop, px in re.findall(r"(?<![-\w])((?:min-)?width)\s*:\s*(\d+)px", css):
            assert int(px) <= 390, (prop, px)
        for px in re.findall(r"grid-template-columns:[^;}]*?(\d+)px", css):
            assert int(px) <= 390
        assert "max-width:1180px" in css and "overflow-x:hidden" in css

    def test_the_league_folds_on_a_phone_and_is_never_a_table(self, world):
        css = SQ.SQUAD_CSS
        phone = css.split("@media (max-width:720px){")[1]
        assert 'grid-template-areas:"pos bot money" "pos stats stats"' in phone and ".lg-head{display:none}" in phone
        assert ".sq-cards{grid-template-columns:repeat(2,minmax(0,1fr))" in phone
        page = SQ.render_squad(board_of(world, "5d"))
        assert "<table" not in page and '<meta name="viewport" content="width=device-width,initial-scale=1">' in page
