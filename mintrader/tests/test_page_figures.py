"""The page's figures on UK days, and the bot buttons (Aaron, 9 Oct, 09:49 UK:
"all figures wrong for today ... add the buttons back to switch between
bots"; "I don't want last night 11pm trade showing, just today's").

Every period is a UK calendar day (Europe/London, midnight to midnight, BST
and GMT). Each closed position counts once, on the UK day it CLOSED, with
its full result (every one of its deals: both halves of the commission),
for the bot whose magic opened it. Total and Overall stay the balance less
the balance at the reset. Positions still open count in no period.

Every figure here is SYNTHETIC test data made by these tests (a fake broker
and fake status files and records): the shape of 9 Oct, not its results.
The Crowd Fader's silver short (ticket 1996782951) is modelled as Aaron
described it: opened 8 Oct 15:15 UTC with 0.06 of commission, stopped
8 Oct 22:36:42 UTC (23:36 UK) at -9.30 after its second 0.06 - -9.36 in
full, on Thursday 8 Oct (UK), so Yesterday on Friday 9 Oct and never Today."""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
import urllib.request
from pathlib import Path
from types import SimpleNamespace as NS

from mintel.broker.base import Side
from mintel.config import Config
from mintel.ops.dashboard import (REFRESH_JS, DashboardState, render_rider_page, render_standing, render_status,
                                  start_dashboard)
from mintel.ops.standing import (account_standing, open_rows, period_bounds, period_view, positions_of,
                                 uk_date, uk_midnight)

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 9, 8, 49, tzinfo=UTC)               # Friday 9 Oct, 09:49 UK (BST)
TNB, RUNNER, BAND, CROWD, RIDER, IAN = 990_311, 990_511, 990_611, 990_711, 990_811, 990_911
SILVER, LEFT, R1, R2, R3 = 1_996_782_951, 5_000_001, 7_001, 7_002, 7_003


def t(*a) -> dt.datetime:
    return dt.datetime(*a, tzinfo=UTC)


def deal(pos, magic, profit, comm, entry, when, symbol, volume=0.07):
    """One deal in the MT5 adapter's shape: "profit" is the deal's profit,
    commission, swap and fee together; "commission" is the commission alone."""
    return {"position": pos, "magic": magic, "symbol": symbol, "volume": volume, "profit": profit,
            "commission": comm, "is_entry": entry, "time": when}


DEALS = [
    # the Crowd Fader's LIVE silver short: opened Thursday afternoon, stopped 23:36 UK Thursday
    deal(SILVER, CROWD, -0.06, -0.06, True, t(2026, 10, 8, 15, 15), "XAGUSD", 0.01),
    deal(SILVER, CROWD, -9.30, -0.06, False, t(2026, 10, 8, 22, 36, 42), "XAGUSD", 0.01),
    # a real Trend & Breakout trade left from LIVE: opened 22:30 UK Thursday, closed 08:00 UK Friday
    deal(LEFT, TNB, -0.30, -0.30, True, t(2026, 10, 8, 21, 30), "GBPUSD", 0.3),
    deal(LEFT, TNB, 15.00, -0.30, False, t(2026, 10, 9, 7, 0), "GBPUSD", 0.3),
    # the Rapid Momentum Rider, LIVE since 08:47 UTC: two closed, one open
    deal(R1, RIDER, -0.05, -0.05, True, t(2026, 10, 9, 8, 47, 10), "GBPJPY"),
    deal(R1, RIDER, 10.22, -0.05, False, t(2026, 10, 9, 8, 48), "GBPJPY"),
    deal(R2, RIDER, -0.04, -0.04, True, t(2026, 10, 9, 8, 47, 30), "EURUSD"),
    deal(R2, RIDER, -3.00, -0.04, False, t(2026, 10, 9, 8, 48, 30), "EURUSD"),
    deal(R3, RIDER, -0.05, -0.05, True, t(2026, 10, 9, 8, 48, 50), "AUDJPY"),
]
TODAY = round(14.70 + 10.17 - 3.04, 2)                           # 21.83: closed since 00:00 UK Friday, in full
YESTERDAY = -9.36                                                # the silver short, both commissions
MADE = round(sum(d["profit"] for d in DEALS), 2)                 # 12.42: the balance less the 2,000 at the reset


def open_now():
    return [NS(ticket=R3, symbol="AUDJPY", side=Side.BUY, volume=0.07, entry_price=101.25, sl=101.15, tp=0.0,
               profit=1.40, swap=0.0, magic=RIDER, open_time=t(2026, 10, 9, 8, 48, 50))]


class Broker:
    """The fake REAL broker: the balance is 2,000 plus every deal, as at MetaTrader."""
    clock = None

    def __init__(self, deals=DEALS, positions=None):
        self.deals = list(deals)
        self.pos = open_now() if positions is None else positions
        self.bal = round(2000.0 + sum(d["profit"] for d in self.deals), 2)

    def account(self):
        return NS(balance=self.bal, equity=round(self.bal + sum(p.profit for p in self.pos), 2), currency="GBP")

    def deals_since(self, since, magic=0, closing_only=True):
        assert closing_only is False                             # the commission on the way in counts too
        return [dict(d) for d in self.deals if d["time"] >= since and (not magic or d["magic"] == magic)]

    def positions(self, magic=None):
        return [p for p in self.pos if magic is None or p.magic == magic]


def _schema(path: Path, module: str):
    import importlib
    con = sqlite3.connect(path)
    con.executescript(importlib.import_module(module).SCHEMA)
    return con


def write_records(data: Path, now=NOW):
    """The bots' own records and status files for the same day."""
    from mintel.rider.journal import RiderJournal
    (data / "rider.json").write_text(json.dumps({"mode": "LIVE", "user_pip_value_gbp": 1.0}))
    (data / "ian.json").write_text(json.dumps({"mode": "LIVE", "feed": {"vendor": "none"}}))
    j = RiderJournal(data / "rider.sqlite")
    for ticket, mode, sym, side, entry, exit_, pips, money, reason, closed in (
            (R1, "LIVE", "GBPJPY", "BUY", 201.502, 201.615, 11.3, 10.22, "TRAIL", t(2026, 10, 9, 8, 48)),
            (R2, "LIVE", "EURUSD", "SELL", 1.16410, 1.16453, -4.3, -3.00, "STOP", t(2026, 10, 9, 8, 48, 30)),
            (6_900, "PAPER", "USDJPY", "BUY", 151.10, 151.12, 1.5, 1.50, "TRAIL", t(2026, 10, 9, 6, 0))):
        j.insert_trade({"ticket": ticket, "mode": mode, "symbol": sym, "side": side, "volume": 0.07, "entry": entry,
                        "exit_price": exit_, "opened_utc": (closed - dt.timedelta(minutes=1)).isoformat(),
                        "closed_utc": closed.isoformat(), "realised_pips": pips, "net_money": money,
                        "money_source": "broker" if mode == "LIVE" else "paper", "exit_reason": reason,
                        "explanation": f"{sym} <ran> and it got off."})
    j.close()
    (data / "rider-status.json").write_text(json.dumps({
        "mode": "LIVE", "updated_utc": now.isoformat(), "user_pip_value_gbp": 1.0, "tagline": "Gets on early.",
        "health": {"entries_allowed": True, "summary": "all checks passing",
                   "checks": [{"name": "mt5_alive", "ok": True, "critical": True, "message": "MetaTrader is answering"}]},
        "scanner": [{"market": "GBPJPY", "direction": "LONG", "score": 71.5, "state": "READY",
                     "reason": "<b>breaking</b> the 20-second high"},
                    {"market": "EURUSD", "direction": "NONE", "score": 12.0, "state": "WATCHING", "reason": "quiet"}],
        # its own day as the status file has it (the broker's day, closing deals only): never the page's TODAY
        "today": {"mode": "LIVE", "trades": 12, "won": 4, "lost": 8, "win_rate": 33.3, "net_pips": -9.9,
                  "net_money": -12.86, "money_source": "the broker's figures", "best_trade": "GBPJPY +11.3 pips"},
        "open_positions": [{"ticket": R3, "market": "AUDJPY", "side": "BUY", "volume": 0.07, "entry": 101.25,
                            "stop": 101.15, "pips": 2.0, "money": 1.40, "flowlock_state": "COSTS_COVERED",
                            "flowlock": "costs covered", "decay": 0.1}]}))
    (data / "ian-status.json").write_text(json.dumps({
        "mode": "LIVE", "updated": now.isoformat(), "status": "DATA-DEGRADED: no institutional feed is configured",
        "data_label": "LIVE", "feed": {"state": "NOT CONFIGURED", "reason": "no feed configured", "vendor": "none"},
        "mt5": {"connected": True}, "reasoning": "No book, so no view: it waits.", "top_opportunity": None,
        "open_positions": [], "today": {"trades": 0, "net": 0.0}}))
    con = _schema(data / "crowd.sqlite", "mintel.crowd.engine")
    con.execute("INSERT INTO trades (ticket, symbol, side, volume, entry, exit_price, opened_utc, closed_utc, net_pnl, "
                "exit_reason, mode) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (SILVER, "XAGUSD", "SELL", 0.01, 50.12, 50.75, t(2026, 10, 8, 15, 15).isoformat(),
                 t(2026, 10, 8, 22, 36, 42).isoformat(), -9.30, "STOP", "LIVE"))
    con.commit()
    con.close()
    con = _schema(data / "bandbreaker.sqlite", "mintel.bandbreaker.engine")
    con.execute("INSERT INTO trades (ticket, symbol, side, volume, entry, exit_price, opened_utc, closed_utc, net_pnl, "
                "exit_reason, mode) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (8_001, "US500", "BUY", 1.0, 6650.0, 6654.2, t(2026, 10, 9, 7, 30).isoformat(),
                 t(2026, 10, 9, 8, 0).isoformat(), 4.20, "TARGET", "PAPER"))
    con.commit()
    con.close()


def a_day(tmp_path: Path, records: bool = True, deals=DEALS, positions=None, broker_cls=None) -> NS:
    data = tmp_path / "data"
    data.mkdir(parents=True, exist_ok=True)
    if records:
        write_records(data)
    cfg = Config()
    cfg.ops.data_dir = str(data)
    cfg.ops.log_dir = str(tmp_path / "logs")
    cfg.tnb.mode = "PAPER"
    cfg.runner.mode = "LIVE"
    broker = (broker_cls or Broker)(deals, positions)
    sd = dict(account_standing(broker, cfg, NOW, cache_seconds=0))
    rows = sd.pop("deals")
    return NS(data=data, cfg=cfg, broker=broker, sd=sd, deals=rows, open=open_rows(broker.positions(None), cfg))


def serve(d: NS):
    state = DashboardState()
    state.update(status={"bot": "RUNNING"}, health={"checks": []}, standing=d.sd, deals=d.deals, open_live=d.open,
                 open_at=NOW.isoformat(), data_dir=str(d.data), tnb_mode="PAPER")
    state.clock = lambda: NOW
    httpd = start_dashboard(state, "127.0.0.1", 0)
    port = httpd.server_address[1]

    def get(path):
        return urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=20).read().decode()
    return httpd, get


def card_of(page: str, label: str) -> str:
    """One bot's card: from its name to the next card."""
    return page.split(f'<div class="nm">{label} <span')[1].split('<div class="hc')[0]


def account_card(page: str) -> str:
    return page.split('<div class="hc acct">')[1].split('<div class="hc')[0]


# ================================================================= UK days --
class TestUkDays:
    SD = {"start": "2026-10-05T11:30:42+00:00"}

    def test_today_and_yesterday_are_uk_midnights_in_summer(self):
        a, b, label, key = period_bounds(self.SD, "today", now=NOW)
        assert (a, b, label, key) == (t(2026, 10, 8, 23), t(2026, 10, 9, 23), "Today", "today")   # 00:00 BST = 23:00 UTC
        assert period_bounds(self.SD, "yesterday", now=NOW)[:2] == (t(2026, 10, 7, 23), t(2026, 10, 8, 23))
        assert period_bounds(self.SD, "week", now=NOW)[0] == t(2026, 10, 5, 11, 30, 42)          # Monday, but not before the reset
        later = t(2026, 10, 14, 9, 0)
        assert period_bounds(self.SD, "week", now=later)[0] == t(2026, 10, 11, 23)               # Monday 12 Oct 00:00 UK
        assert period_bounds(self.SD, "custom", "2026-10-08", "2026-10-08", now=NOW)[:2] == (t(2026, 10, 7, 23),
                                                                                               t(2026, 10, 8, 23))

    def test_winter_days_start_at_midnight_utc_and_the_clocks_going_back_make_a_25_hour_day(self):
        winter = t(2026, 12, 1, 10, 0)
        assert period_bounds(self.SD, "today", now=winter)[:2] == (t(2026, 12, 1), t(2026, 12, 2))
        assert period_bounds(self.SD, "month", now=winter)[0] == t(2026, 12, 1)
        assert period_bounds(self.SD, "6m", now=winter)[0] == t(2026, 10, 5, 11, 30, 42)       # never before the reset
        a, b, _, _ = period_bounds(self.SD, "yesterday", now=t(2026, 10, 26, 12, 0))           # Sunday 25 Oct: BST ends
        assert (a, b) == (t(2026, 10, 24, 23), t(2026, 10, 26)) and b - a == dt.timedelta(hours=25)
        assert uk_midnight(dt.date(2026, 3, 29)) == t(2026, 3, 29) and uk_midnight(dt.date(2026, 3, 30)) == t(2026, 3, 29, 23)

    def test_a_trade_closed_at_2330_utc_is_the_next_uk_day_in_summer_but_the_same_day_in_winter(self):
        assert uk_date(t(2026, 10, 8, 23, 30)) == dt.date(2026, 10, 9)                        # 00:30 BST Friday
        assert uk_date(t(2026, 11, 30, 23, 30)) == dt.date(2026, 11, 30)                      # 23:30 GMT Monday
        for close, now, today, yesterday in ((t(2026, 10, 8, 23, 30), t(2026, 10, 9, 9), 5.0, 0.0),
                                             (t(2026, 11, 30, 23, 30), t(2026, 12, 1, 9), 0.0, 5.0)):
            rows = [dict(deal(1, RIDER, 0.0, 0.0, True, close - dt.timedelta(minutes=5), "EURUSD"), bot_magic=RIDER),
                    dict(deal(1, RIDER, 5.0, 0.0, False, close, "EURUSD"), bot_magic=RIDER)]
            sd = {"start": "2026-10-05T11:30:42+00:00", "bots": [{"id": "momentum_rider", "magic": RIDER, "made": 5.0}]}
            assert period_view(sd, rows, "today", now=now)["made"] == today, close
            assert period_view(sd, rows, "yesterday", now=now)["made"] == yesterday, close


# ========================================================== the silver short --
class TestTheSilverShortIsYesterday:
    def test_it_is_yesterday_in_full_and_never_today(self, tmp_path):
        d = a_day(tmp_path)
        today = period_view(d.sd, d.deals, "today", now=NOW)
        yesterday = period_view(d.sd, d.deals, "yesterday", now=NOW)
        assert today["made"] == TODAY and d.sd["today"] == TODAY             # standing["today"]: the pulse reads it
        assert yesterday["made"] == YESTERDAY and yesterday["trades"] == 1
        assert yesterday["bots"]["crowd_fader"] == {"made": -9.36, "trades": 1, "wins": 0, "losses": 1,
                                                    "commission": 0.12}
        assert today["bots"]["crowd_fader"]["made"] == 0.0 and today["bots"]["crowd_fader"]["trades"] == 0
        assert SILVER not in {p["position"] for p in today["closed"]}
        by = {b["id"]: b for b in d.sd["bots"]}
        assert by["crowd_fader"]["today"] == 0.0 and by["crowd_fader"]["made"] == -9.36

    def test_end_to_end_through_the_snapshot_and_the_page(self, tmp_path):
        d = a_day(tmp_path)
        httpd, get = serve(d)
        try:
            page = get("/")
            acct = account_card(page)
            assert '<div class="fk">Today</div><div class="fv ok">+£21.83</div>' in acct
            assert '<div class="fk">Overall</div><div class="fv ok">+£12.42</div>' in acct          # balance less 2,000
            assert "-£9.36" not in acct and "-£9.30" not in acct
            crowd = card_of(page, "Crowd Fader")
            assert '<div class="fk">Today</div><div class="fv ">£0.00</div>' in crowd             # nothing closed today
            assert '<div class="fk">Overall</div><div class="fv bad">-£9.36</div>' in crowd
            assert "Days are UK days, midnight to midnight." in page and page.count("Days are UK days") == 1
            yesterday = get("/?period=yesterday")
            assert '<div class="fk">Yesterday</div><div class="fv bad">-£9.36</div>' in account_card(yesterday)
            assert '<div class="fk">Yesterday</div><div class="fv bad">-£9.36</div>' in card_of(yesterday, "Crowd Fader")
            assert "1 trade yesterday (0 won, 1 lost)" in card_of(yesterday, "Crowd Fader")
        finally:
            httpd.shutdown()

    def test_the_difference_from_the_balance_is_said_in_one_plain_line(self, tmp_path):
        d = a_day(tmp_path)
        today = period_view(d.sd, d.deals, "today", now=NOW)
        assert today["balance_moved"] == round(15.00 + 10.17 - 3.04 - 0.05, 2)              # 22.08 booked today
        assert today["why"] == ("This differs from what trades moved the balance today (+£22.08) because £0.30 of "
                                "commission was charged yesterday on a trade that closed today and £0.05 of commission "
                                "was charged today on a trade still open, which counts on the day it closes.")
        yesterday = period_view(d.sd, d.deals, "yesterday", now=NOW)
        assert yesterday["why"] == ("This differs from what trades moved the balance yesterday (-£9.66) because £0.30 "
                                    "of commission was charged yesterday on a trade that closed after, which counts on "
                                    "the day it closed.")
        html = render_standing({"standing": d.sd}, {"sel": today, "tot": period_view(d.sd, d.deals, "total", now=NOW)})
        assert '<div class="why">This differs from what trades moved the balance today' in account_card(html)
        same = a_day(tmp_path / "b", deals=DEALS[:2], positions=[])                         # nothing straddles: no line
        assert period_view(same.sd, same.deals, "yesterday", now=NOW)["why"] == ""


# ======================================================== across periods --
class TestAPositionAcrossPeriods:
    def test_opened_yesterday_closed_today_counts_today_with_both_commissions(self, tmp_path):
        d = a_day(tmp_path)
        today = period_view(d.sd, d.deals, "today", now=NOW)
        left = [p for p in today["closed"] if p["position"] == LEFT][0]
        assert left["net"] == 14.70 and left["commission"] == 0.60 and left["bot_magic"] == TNB
        assert today["bots"]["market_intelligence"]["made"] == 14.70
        assert LEFT not in {p["position"] for p in period_view(d.sd, d.deals, "yesterday", now=NOW)["closed"]}
        assert {b["id"]: b for b in d.sd["bots"]}["market_intelligence"]["today"] == 14.70

    def test_an_open_position_counts_in_no_period_and_shows_as_open(self, tmp_path):
        d = a_day(tmp_path)
        for key in ("today", "yesterday", "week", "month", "6m", "1y"):
            v = period_view(d.sd, d.deals, key, now=NOW)
            assert R3 not in {p["position"] for p in v["closed"]}, key
        assert d.sd["open"] == 1.40 and {b["id"]: b for b in d.sd["bots"]}["momentum_rider"]["open"] == 1.40
        httpd, get = serve(d)
        try:
            page = get("/")
            assert "Open trades right now - 1 open" in page
            assert "£0.05 has already been taken from the balance for these open trades" in page
            assert "open now <b class=\"ok\">+£1.40</b>" in card_of(page, "Rapid Momentum Rider")
        finally:
            httpd.shutdown()

    def test_without_the_open_list_an_exit_short_of_the_entry_is_still_open(self):
        rows = [dict(deal(9, RIDER, -0.1, -0.1, True, t(2026, 10, 9, 8), "EURUSD", 0.2), bot_magic=RIDER),
                dict(deal(9, RIDER, 3.0, -0.05, False, t(2026, 10, 9, 8, 5), "EURUSD", 0.1), bot_magic=RIDER)]
        assert positions_of(rows, None)[0]["is_closed"] is False                 # half closed, half still open
        assert positions_of(rows, [])[0]["is_closed"] is True                    # MetaTrader says it is gone
        assert positions_of(rows, [9])[0]["is_closed"] is False                  # MetaTrader still has it

    def test_total_is_the_balance_less_2000_and_everything_adds_up(self, tmp_path):
        d = a_day(tmp_path)
        total = period_view(d.sd, d.deals, "total", now=NOW)
        assert d.sd["made"] == MADE == round(d.broker.bal - 2000, 2) == total["made"]
        assert total["adjustments"] == 0.0
        assert round(sum(b["made"] for b in total["bots"].values()) + total["other"], 2) == total["made"]
        for key in ("today", "yesterday", "week", "month"):
            v = period_view(d.sd, d.deals, key, now=NOW)
            assert round(sum(b["made"] for b in v["bots"].values()) + v["other"], 2) == v["made"], key
        assert period_view(d.sd, d.deals, "week", now=NOW)["made"] == round(TODAY + YESTERDAY, 2)
        # a broker charge on the balance: Total is still the balance, and says where it came from
        d.sd["made"] = round(MADE - 1.25, 2)
        total = period_view(d.sd, d.deals, "total", now=NOW)
        assert total["made"] == round(MADE - 1.25, 2) and total["adjustments"] == -1.25
        html = render_standing({"standing": d.sd}, {"sel": total, "tot": total})
        assert "-£1.25 from other changes to the balance" in html


# ============================================================ the Rider --
class TestTheRiderAgreesEverywhere:
    def test_its_card_the_account_and_its_page_carry_the_same_figures(self, tmp_path):
        d = a_day(tmp_path)
        httpd, get = serve(d)
        try:
            page, rider_page, mine = get("/"), get("/rider"), get("/?bot=momentum_rider")
        finally:
            httpd.shutdown()
        card = card_of(page, "Rapid Momentum Rider")
        assert '<div class="fk">Today</div><div class="fv ok">+£7.13</div>' in card            # 10.17 - 3.04
        assert "2 trades today (1 won, 1 lost)" in card
        assert "+£21.83" in account_card(page)                                                # in the account's Today
        today = rider_page.split("<h2>Today (UK day, since 00:00 UK)</h2>")[1].split('<div class="card">')[0]
        for needle in ('<div class="k">Trades</div><div class="v ">2</div>',
                       '<div class="k">Won</div><div class="v ok">1</div>',
                       '<div class="k">Lost</div><div class="v bad">1</div>',
                       '<div class="k">Win rate</div><div class="v ">50%</div>',
                       '<div class="k">Net pips</div><div class="v ok">+7.0</div>',
                       '<div class="k">Net money</div><div class="v ok">+£7.13</div>',
                       "GBPJPY +11.3 pips (+£10.17)"):
            assert needle in today, needle
        assert "-12.86" not in rider_page and "12 trades" not in rider_page                   # never its own broker-day file
        assert "the same figures as its card on the status page" in rider_page
        assert "practice today (PAPER, not money): 1 trade, +£1.50" in rider_page
        assert "+£10.17 (MetaTrader, in full)" in rider_page and "UK</span>" in rider_page
        assert "&lt;ran&gt;" in rider_page and "<ran>" not in rider_page
        # its own view lists the same two trades, LIVE in black, and the practice one in grey
        view = mine.split("Rapid Momentum Rider - trades closed today - 3</h2>")[1].split("</table>")[0]
        assert view.count('<tr class="live">') == 2 and view.count('<tr class="practice">') == 1
        assert "GBPJPY" in view and "EURUSD" in view and "USDJPY" in view and "XAGUSD" not in view
        assert ">+£10.17</td>" in view and ">-£3.04</td>" in view and '+£1.50 <span class="pill">practice</span>' in view
        assert "LIVE: 2 trades, +£7.13" in mine

    def test_without_figures_from_metatrader_the_page_says_where_today_came_from(self, tmp_path):
        d = a_day(tmp_path)
        page = render_rider_page(str(d.data), NOW)
        assert "the Rider&#x27;s status file (MetaTrader&#x27;s records have not reached this page yet)" in page

    def test_rows_that_could_not_be_read_are_not_known_never_a_false_zero(self, tmp_path):
        class OldBridge(Broker):
            """An older bridge (no magic on its rows) that times out on the Rider's magic."""
            def deals_since(self, since, magic=0, closing_only=True):
                if magic == RIDER:
                    raise RuntimeError("bridge timed out")
                return [{k: v for k, v in r.items() if k != "magic"} for r in super().deals_since(since, magic, closing_only)]
        d = a_day(tmp_path, broker_cls=OldBridge)
        assert RIDER in d.sd["unknown_magics"] and d.sd["made"] == MADE
        top = {"sel": period_view(d.sd, d.deals, "today", now=NOW), "tot": period_view(d.sd, d.deals, "total", now=NOW),
               "data_dir": str(d.data), "now": NOW}
        mine = render_standing({"standing": d.sd, "open_live": d.open}, top, bot="momentum_rider")
        assert "trades not known just now" in mine and "could not be read just now" in mine
        assert "None closed today" not in mine
        page = render_rider_page(str(d.data), NOW, sd=d.sd, deals=d.deals)
        assert "MetaTrader&#x27;s records have not reached this page yet" in page        # its own file, said so


# ========================================================== the bot buttons --
class TestTheBotButtons:
    LABELS = ("All bots", "Trend &#38; Breakout", "Rapid Momentum Rider", "Momentum Runner", "Band Breaker",
              "Crowd Fader", "Financial Ian")

    def test_they_sit_under_the_period_buttons_and_keep_the_period(self, tmp_path):
        d = a_day(tmp_path)
        httpd, get = serve(d)
        try:
            page = get("/?period=yesterday")
            bar = page.split('<div class="botbar">')[1].split("</div>")[0]
            for lbl in self.LABELS:
                assert f">{lbl}</a>" in bar, lbl
            assert "Rapid Scalper" not in bar                                   # no real money of its own in the period
            assert page.index('class="tab p on" href="/?period=yesterday"') < page.index('<div class="botbar">')
            assert page.index('<div class="botbar">') < page.index('class="cards"')
            assert '<a class="tab on" href="/?period=yesterday">All bots</a>' in bar
            assert 'href="/?period=yesterday&bot=crowd_fader">Crowd Fader</a>' in bar
            crowd = get("/?period=yesterday&bot=crowd_fader")
            assert 'class="tab p on" href="/?period=yesterday&bot=crowd_fader"' in crowd   # the period stays chosen
            assert 'href="/?period=today&bot=crowd_fader"' in crowd                        # and the bot, on every period
            assert '<input type="hidden" name="bot" value="crowd_fader">' in crowd         # the custom dates too
            assert '<a class="tab on" href="/?period=yesterday&bot=crowd_fader">Crowd Fader</a>' in crowd
            custom = get("/?period=custom&from=2026-10-08&to=2026-10-08&bot=crowd_fader")
            assert 'href="/?period=custom&from=2026-10-08&to=2026-10-08&bot=momentum_rider"' in custom
        finally:
            httpd.shutdown()

    def test_a_bot_alone_shows_its_card_its_trades_its_open_trades_and_its_live_view(self, tmp_path):
        d = a_day(tmp_path)
        httpd, get = serve(d)
        try:
            crowd = get("/?period=yesterday&bot=crowd_fader")
            rider = get("/?bot=momentum_rider")
            ian = get("/?bot=financial_ian")
            tnb = get("/?bot=market_intelligence")
            band = get("/?bot=band_breaker")
        finally:
            httpd.shutdown()
        top = crowd.split('id="more"')[0]
        assert '<div class="hc acct">' not in top and '<div class="cards one">' in top        # that bot alone
        assert top.count('<div class="hc') == 1 and "Crowd Fader <span" in top
        rows = top.split("Crowd Fader - trades closed yesterday - 1</h2>")[1].split("</table>")[0]
        for cell in (">XAGUSD<", ">SELL<", ">0.01<", ">50.12<", ">50.75<", ">-£9.36</td>", ">STOP<"):
            assert cell in rows, cell
        assert "23:36:42" in rows                                                            # closed 23:36 UK
        assert "Crowd Fader - open trades right now" in top and "CROWD FADER" in top
        r = rider.split('id="more"')[0]
        assert "<th>Market</th><th>Direction</th><th>Score</th><th>State</th><th>Reason</th>" in r
        assert "&lt;b&gt;breaking&lt;/b&gt;" in r and "<b>breaking</b>" not in r
        assert "MetaTrader is answering" in r and 'href="/rider"' in r
        assert "Rapid Momentum Rider - open trades right now - 1 open" in r and "AUDJPY" in r
        i = ian.split('id="more"')[0]
        assert "NOT CONFIGURED" in i and "No book, so no view: it waits." in i and 'href="/ian"' in i
        assert "None closed today." in i
        m = tnb.split('id="more"')[0]
        assert "It is on PAPER" in m and "Momentum Runner rides its index entries" in m
        assert "What the bot is looking at right now" in m
        assert ">GBPUSD<" in m and ">+£14.70</td>" in m                                       # its real leftover, today
        b = band.split('id="more"')[0]
        assert '<tr class="practice">' in b and '+£4.20 <span class="pill">practice</span>' in b
        assert "Practice only, not real money: today +£4.20" in b and "BAND BREAKER" in b

    def test_every_bot_alone_renders_with_no_status_files_at_all(self, tmp_path):
        d = a_day(tmp_path, records=False)
        httpd, get = serve(d)
        try:
            for bot in ("market_intelligence", "momentum_rider", "momentum_runner", "band_breaker", "crowd_fader",
                        "financial_ian", "rapid_scalper", "nonsense<script>"):
                for period in ("today", "yesterday", "total"):
                    page = get(f"/?bot={bot}&period={period}")
                    assert "BOT: RUNNING" in page and "dashboard error" not in page, (bot, period)
                    assert "<script>alert" not in page
            assert "Rapid Momentum Rider: not started yet." in get("/?bot=momentum_rider")
            assert "Financial Ian: not started yet." in get("/?bot=financial_ian")
            assert "NOT RUNNING" in get("/?bot=band_breaker")
            assert '<a class="tab on" href="/?period=today">All bots</a>' in get("/?bot=nonsense")
        finally:
            httpd.shutdown()
        # and before MetaTrader has been heard from at all
        for bot in ("market_intelligence", "momentum_rider", "crowd_fader", "financial_ian"):
            page = render_status({"status": {"bot": "WAITING FOR METATRADER"}, "health": {}}, "overall", None, bot)
            assert "has not heard from MetaTrader yet" in page and "open trades right now" in page, bot

    def test_the_retired_scalper_has_a_button_only_with_real_money_in_the_period(self, tmp_path):
        rows = DEALS + [deal(9_001, 990_411, -0.02, -0.02, True, t(2026, 10, 6, 9), "EURUSD"),
                        deal(9_001, 990_411, -8.70, -0.02, False, t(2026, 10, 6, 10), "EURUSD")]
        d = a_day(tmp_path, deals=rows)
        today = period_view(d.sd, d.deals, "today", now=NOW)
        total = period_view(d.sd, d.deals, "total", now=NOW)
        assert "Rapid Scalper (retired)</a>" not in render_standing({"standing": d.sd}, {"sel": today, "tot": total})
        assert "Rapid Scalper (retired)</a>" in render_standing({"standing": d.sd}, {"sel": total, "tot": total})

    def test_the_refresh_keeps_the_chosen_bot_and_never_wipes_a_date_being_typed(self, tmp_path):
        assert "location.reload()" in REFRESH_JS                         # the same address, ?bot= and all
        assert "location.href" not in REFRESH_JS and "location.assign" not in REFRESH_JS
        assert "defaultValue" in REFRESH_JS and "document.activeElement" in REFRESH_JS
        d = a_day(tmp_path)
        page = render_status({"status": {"bot": "RUNNING"}, "health": {}, "standing": d.sd, "open_live": d.open},
                             "overall", {"sel": period_view(d.sd, d.deals, "today", now=NOW),
                                         "tot": period_view(d.sd, d.deals, "total", now=NOW), "now": NOW},
                             "momentum_rider")
        assert REFRESH_JS in page and '<a class="tab on" href="/?period=today&bot=momentum_rider">' in page


# ======================================================== push_dashboard --
class TestThePushPutsTheFoldedTilesOnUkDays:
    def test_trend_and_breakouts_own_tiles_use_the_same_uk_day(self, trader, cfg, monkeypatch):
        from mintel.run import _LEDGER_CACHE, push_dashboard
        from mintel.ops import standing as standing_mod
        _LEDGER_CACHE["at"] = None
        standing_mod._CACHE["at"] = None
        assert int(cfg.magic) == TNB
        day0 = uk_midnight(uk_date(trader.clock()))
        rows = [deal(41, TNB, -0.30, -0.30, True, day0 - dt.timedelta(hours=2), "GBPUSD", 0.3),   # yesterday (UK)
                deal(41, TNB, 6.30, -0.30, False, day0 + dt.timedelta(minutes=30), "GBPUSD", 0.3)]  # today (UK)
        cfg.tracking_start_utc = (day0 - dt.timedelta(days=3)).isoformat()
        cfg.account_reset_utc = (day0 - dt.timedelta(days=3)).isoformat()
        monkeypatch.setattr(trader.broker, "deals_since", lambda since, magic=0, closing_only=True:
                            [dict(r) for r in rows if r["time"] >= since and (not magic or r["magic"] == magic)],
                            raising=False)
        state = DashboardState()
        push_dashboard(state, trader)
        st = state.snapshot()["status"]
        assert st["day_basis"] == "UK"
        assert st["today_pnl"] == 6.00 and st["today_trades"] == 1             # in full, both commissions
        assert [p["label"] for p in st["periods"]] == ["Today", "Yesterday", "This week", "Last 7 days",
                                                         "Last 14 days", "Since start"]
        assert state.standing["today"] == 6.00 and state.standing["day_basis"] == "UK"
        page = render_status(state.snapshot())
        assert "Today (UK day)" in page and "each in full on the UK day it closed" in page
