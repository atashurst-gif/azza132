"""The page's one big figure: the account's standing since the reset, from
the broker alone, and the by-bot rows that add up to it. Every finding of
the 8 Oct check is pinned here."""
import datetime as dt
import json
from types import SimpleNamespace as NS

import pytest

from mintel.broker.base import Side
from mintel.config import Config
from mintel.ops.dashboard import DashboardState, render_standing, render_status
from mintel.ops.standing import account_standing

UTC = dt.timezone.utc
START = dt.datetime(2026, 10, 5, 11, 30, 42, tzinfo=UTC)
NOW = dt.datetime(2026, 10, 8, 12, 0, tzinfo=UTC)


def deal(pos, magic, profit, comm, entry, when):
    return {"position": pos, "magic": magic, "profit": profit, "commission": comm, "is_entry": entry, "time": when}


DEALS = [
    deal(1, 990311, -0.30, -0.30, True, START + dt.timedelta(hours=1)),
    deal(1, 990311, 21.60, -0.30, False, START + dt.timedelta(hours=2)),
    deal(2, 990311, 0.0, 0.0, True, NOW - dt.timedelta(hours=3)),
    deal(2, 990311, 8.28, 0.0, False, NOW - dt.timedelta(hours=2)),
    deal(3, 990611, 0.0, 0.0, True, NOW - dt.timedelta(hours=1)),
    deal(3, 990611, -9.36, 0.0, False, NOW - dt.timedelta(minutes=30)),
    deal(4, 0, -1.0, 0.0, True, NOW - dt.timedelta(hours=5)),              # by hand
    deal(4, 0, 3.0, 0.0, False, NOW - dt.timedelta(hours=4)),
    deal(5, 990511, 0.0, 0.0, True, NOW - dt.timedelta(hours=6)),          # a Runner trade...
    deal(5, 0, 4.0, 0.0, False, NOW - dt.timedelta(hours=5, minutes=30)),  # ...closed by hand on the phone
    deal(9, 990311, 50.0, 0.0, False, START - dt.timedelta(hours=2)),      # before the reset
]
TRADING = round(21.3 + 8.28 - 9.36 + 2.0 + 4.0, 2)


def without_magic(rows):
    return [{k: v for k, v in r.items() if k != "magic"} for r in rows]


class Broker:
    clock = None

    def __init__(self, deals=DEALS, fail=False, fail_magic=None, old_bridge=False, balance=None, positions_fail=False):
        self.deals, self.fail, self.fail_magic, self.old = deals, fail, fail_magic, old_bridge
        self.bal = 2000 + TRADING if balance is None else balance
        self.positions_fail = positions_fail
        self.calls = 0

    def account(self):
        return NS(balance=self.bal, equity=self.bal - 2.13, currency="GBP")

    def deals_since(self, since, magic=0, closing_only=True):
        self.calls += 1
        if self.fail:
            raise RuntimeError("history unavailable")
        if magic and magic == self.fail_magic:
            raise RuntimeError("bridge timed out")
        assert closing_only is False                          # the entry half of the commission counts
        rows = [d for d in self.deals if (not magic or d["magic"] == magic)]
        return without_magic(rows) if self.old else rows

    def positions(self, magic=None):
        if self.positions_fail:
            raise RuntimeError("positions timed out")
        return [NS(magic=990311, profit=-2.0, swap=-0.13)]


def cfg():
    c = Config()
    c.tracking_start_utc = "2026-10-08T06:00:00+00:00"       # a rules change moved this: the account figure ignores it
    c.bandbreaker.mode = "LIVE"
    return c


def standing(**kw):
    return account_standing(Broker(**kw), cfg(), NOW, cache_seconds=0)


class TestTheFigure:
    def test_it_is_the_balance_less_the_balance_at_the_reset(self):
        sd = standing()
        assert sd["made"] == TRADING and sd["trading"] == TRADING and sd["adjustments"] == 0.0
        assert sd["start"] == START.isoformat() and sd["start_balance"] == 2000.0
        assert sd["open"] == -2.13 and sd["made_commission"] == 0.6 and sd["made_trades"] == 5

    def test_a_balance_change_that_is_not_a_trade_gets_its_own_row(self):
        sd = standing(balance=2000 + TRADING - 1.5)              # a broker charge of 1.50
        assert sd["made"] == round(TRADING - 1.5, 2) and sd["adjustments"] == -1.5
        top = _top({k: v for k, v in sd.items() if k != "deals"}, sd.get("deals") or ())
        html = render_standing({"standing": sd}, top)
        assert "other changes to the balance" in html and "-£1.50" in html

    def test_the_rows_add_up_and_a_trade_is_its_openers(self):
        sd = standing()
        by = {b["id"]: b for b in sd["bots"]}
        assert by["market_intelligence"]["made"] == 29.58 and by["market_intelligence"]["today"] == 8.28
        assert by["band_breaker"]["made"] == -9.36 and by["band_breaker"]["mode"] == "LIVE"
        assert by["momentum_runner"]["made"] == 4.0               # closed by hand, still the Runner's
        assert sd["other"]["made"] == 2.0
        assert sum(b["made"] for b in sd["bots"]) + sd["other"]["made"] == pytest.approx(sd["trading"])
        assert sum(b["open"] for b in sd["bots"]) + sd["other"]["open"] == pytest.approx(sd["open"])
        assert sd["other"]["open"] == 0.0                          # open money is never a leftover

    def test_today_starts_at_the_brokers_day(self):
        assert standing()["today"] == round(8.28 - 9.36 + 2.0 + 4.0, 2)

    def test_one_read_of_the_history_when_the_bridge_says_the_magic(self):
        b = Broker()
        account_standing(b, cfg(), NOW, cache_seconds=0)
        assert b.calls == 1


class TestWhenTheBrokerFails:
    def test_no_history_means_no_figure_not_a_wrong_one(self):
        sd = standing(fail=True)
        assert sd["made"] is None and "history unavailable" in sd["error"]
        html = render_standing({"standing": sd}, None)
        assert "could not be read" in html and 'class="fv">-<' in html

    def test_an_older_bridge_is_asked_per_bot_and_a_failed_bot_is_unknown_not_hand_trading(self):
        sd = standing(old_bridge=True, fail_magic=990311)
        by = {b["id"]: b for b in sd["bots"]}
        assert by["market_intelligence"]["made"] is None
        assert sd["other"]["made"] is None and sd["other"]["today"] is None      # never the missing bot's money
        assert "990311" in sd["error"]
        top = _top({k: v for k, v in sd.items() if k != "deals"}, sd.get("deals") or ())
        html = render_standing({"standing": sd}, top)
        assert "could not be read just now" in html and sd["made"] == TRADING     # the big figure still stands

    def test_an_older_bridge_that_answers_still_adds_up(self):
        sd = standing(old_bridge=True)
        by = {b["id"]: b for b in sd["bots"]}
        assert by["market_intelligence"]["made"] == 29.58 and not sd["error"]
        assert sum(b["made"] for b in sd["bots"]) + sd["other"]["made"] == pytest.approx(sd["trading"])

    def test_positions_that_cannot_be_read_leave_the_open_rows_unknown(self):
        sd = standing(positions_fail=True)
        assert all(b["open"] is None for b in sd["bots"]) and sd["other"]["open"] is None
        assert sd["open"] == -2.13 and "positions" in sd["error"]            # the account's own figure still shows


def _top(sd, deals=(), period="today", **kw):
    from mintel.ops.standing import period_view
    return {"sel": period_view(sd, list(deals), period, now=NOW, **kw), "tot": period_view(sd, list(deals), "total", now=NOW),
            "p_sel": {}, "p_tot": {"financial_ian": -57.09}}


class TestThePage:
    def test_the_headlines_reach_the_real_page_through_the_snapshot(self):
        state = DashboardState()
        sd = dict(standing())
        deals = sd.pop("deals")
        state.update(status={"bot": "RUNNING"}, health={"checks": []}, standing=sd)
        page = render_status(state.snapshot(), "overall", _top(sd, deals))
        assert "Account - all bots, after commission" in page
        assert f"+£{TRADING:,.2f}" in page                                   # Overall = the balance less £2,000
        assert "started with £2,000.00 on Mon 05 Oct" in page
        assert "reset" not in page.split('id="more"')[0].lower()            # no "reset" talk at the top
        assert page.index('class="cards"') < page.index('id="more"')

    def test_a_card_per_bot_with_today_and_overall(self):
        sd = dict(standing())
        deals = sd.pop("deals")
        html = render_standing({"standing": sd}, _top(sd, deals))
        for label in ("Trend &amp; Breakout", "Rapid Momentum Rider", "Momentum Runner", "Band Breaker", "Crowd Fader",
                      "Financial Ian"):
            assert label in html, label
        # the account and the six active bots; the retired scalper made nothing here, so it has no line
        assert html.count('<div class="fk">Today</div>') == 7 and html.count('<div class="fk">Overall</div>') == 7
        assert "Rapid Scalper" not in html
        assert "Practice only, not real money" in html and "-£57.09" in html     # paper Financial Ian, in grey
        assert "from trades placed by hand" in html                              # the +2.00 hand trade keeps the sum

    def test_a_page_with_no_figures_yet_still_renders(self):
        page = render_status({"status": {"bot": "WAITING FOR METATRADER", "waiting": "x"}, "health": {}})
        assert "WAITING FOR METATRADER" in page and "has not heard from MetaTrader yet" in page


class TestPeriodsAndOpenTrades:
    def _state(self):
        from mintel.ops.standing import open_rows
        from mintel.broker.base import Side
        b = Broker()
        sd = dict(account_standing(b, cfg(), NOW, cache_seconds=0))
        deals = sd.pop("deals")
        pos = [NS(ticket=7, symbol="US30", side=Side.SELL, volume=0.3, entry_price=46850.2, sl=46910.0, tp=0.0,
                  profit=3.10, swap=-0.1, magic=990311, open_time=NOW - dt.timedelta(minutes=25)),
               NS(ticket=8, symbol="XAUUSD", side=Side.SELL, volume=0.01, entry_price=4123.6, sl=4141.2, tp=4061.0,
                  profit=-0.77, swap=0.0, magic=990711, open_time=NOW - dt.timedelta(minutes=8)),
               NS(ticket=9, symbol="EURUSD", side=Side.BUY, volume=0.01, entry_price=1.1, sl=0.0, tp=0.0,
                  profit=0.05, swap=0.0, magic=0, open_time=NOW - dt.timedelta(minutes=3))]
        return sd, deals, open_rows(pos, cfg())

    def test_every_period_adds_up_and_total_is_the_balance_figure(self):
        from mintel.ops.standing import PERIOD_BUTTONS, period_view
        sd, deals, _ = self._state()
        assert [k for k, _ in PERIOD_BUTTONS] == ["today", "yesterday", "week", "month", "6m", "1y", "total"]
        for key, _label in PERIOD_BUTTONS:
            v = period_view(sd, deals, key, now=NOW)
            assert sum(b["made"] for b in v["bots"].values()) + v["other"] == pytest.approx(v["trading"]), key
        total = period_view(sd, deals, "total", now=NOW)
        assert total["made"] == sd["made"] and total["adjustments"] == 0.0
        today = period_view(sd, deals, "today", now=NOW)
        assert today["made"] == round(8.28 - 9.36 + 2.0 + 4.0, 2) and today["adjustments"] is None
        assert period_view(sd, deals, "yesterday", now=NOW)["made"] == 0.0

    def test_a_custom_range_counts_whole_days_and_never_before_the_reset(self):
        from mintel.ops.standing import period_view
        sd, deals, _ = self._state()
        v = period_view(sd, deals, "custom", "2026-10-01", "2026-10-05", now=NOW)
        assert v["start"] == START.isoformat()                              # clamped to the reset
        assert v["made"] == round(-0.30 + 21.60, 2)                          # only the 5 Oct trade
        both = period_view(sd, deals, "custom", "2026-10-08", "2026-10-05", now=NOW)   # reversed dates
        assert both["made"] == period_view(sd, deals, "total", now=NOW)["trading"]

    def test_open_trades_name_their_bot(self):
        _sd, _deals, rows = self._state()
        assert [(r["bot"], r["symbol"], r["profit"]) for r in rows] == [
            ("Trend & Breakout", "US30", 3.0), ("Crowd Fader", "XAUUSD", -0.77), ("Placed by hand", "EURUSD", 0.05)]

    def test_the_page_has_the_filter_the_open_box_and_the_chosen_period(self):
        import urllib.request
        from mintel.ops.dashboard import start_dashboard
        sd, deals, rows = self._state()
        state = DashboardState()
        state.update(status={"bot": "RUNNING"}, health={"checks": []}, standing=sd, deals=deals,
                     open_live=rows, open_at=NOW.isoformat(), data_dir="")
        httpd = start_dashboard(state, "127.0.0.1", 0)
        try:
            port = httpd.server_address[1]
            page = urllib.request.urlopen(f"http://127.0.0.1:{port}/").read().decode()
            for lbl in ("Today", "Yesterday", "This week", "This month", "Last 6 months", "Last year", "Total", "Custom"):
                assert f">{lbl}<" in page, lbl
            assert 'class="tab p on" href="/?period=today"' in page                   # Today by default
            assert "Open trades right now - 3 open" in page and "Crowd Fader" in page and "Placed by hand" in page
            assert page.index('class="cards"') < page.index("Open trades right now")
            week = urllib.request.urlopen(f"http://127.0.0.1:{port}/?period=week").read().decode()
            assert '<div class="fk">This week</div>' in week and 'class="tab p on" href="/?period=week"' in week
            total = urllib.request.urlopen(f"http://127.0.0.1:{port}/?period=total").read().decode()
            assert '<div class="fk">Today</div>' not in total and '<div class="fk">Overall</div>' in total
            custom = urllib.request.urlopen(
                f"http://127.0.0.1:{port}/?period=custom&from=2026-10-05&to=2026-10-05").read().decode()
            assert "+£21.30" in custom
        finally:
            httpd.shutdown()


class TestTheSecondCheck:
    def test_odd_custom_dates_never_break_the_page(self):
        from mintel.ops.standing import period_view
        sd = dict(standing())
        deals = sd.pop("deals")
        for a, b in (("0001-01-01", ""), ("9999-12-31", "9999-12-31"), ("not-a-date", ""), ("", "")):
            v = period_view(sd, deals, "custom", a, b, now=NOW)
            assert v["made"] is not None, (a, b)
        far = period_view(sd, deals, "custom", "2020-01-01", "2030-01-01", now=NOW)
        assert far["trading"] == period_view(sd, deals, "total", now=NOW)["trading"]

    def test_a_bot_that_could_not_be_read_is_unknown_in_every_period(self):
        from mintel.ops.standing import period_view
        sd = dict(standing(old_bridge=True, fail_magic=990611))
        deals = sd.pop("deals")
        for key in ("today", "week", "total"):
            v = period_view(sd, deals, key, now=NOW)
            assert v["bots"]["band_breaker"]["made"] is None and v["other"] is None, key
            assert v["bots"]["market_intelligence"]["made"] is not None
        assert period_view(sd, deals, "today", now=NOW)["trading"] == round(8.28 - 9.36 + 2.0 + 4.0, 2)  # every deal still counts
        assert period_view(sd, deals, "total", now=NOW)["adjustments"] is None                      # never a false "charge"
        html = render_standing({"standing": sd}, _top(sd, deals))
        assert "trades not known just now" in html and "broker charges" not in html

    def test_a_trade_booked_between_the_reads_is_read_again(self):
        b = Broker()
        real_account, calls = b.account, {"n": 0}
        def account():
            calls["n"] += 1
            a = real_account()
            if calls["n"] >= 2:                         # a +7.40 trade lands after the first read
                return NS(balance=a.balance + 7.40, equity=a.equity + 7.40, currency="GBP")
            return a
        b.account = account
        extra = deal(77, 990711, 7.40, 0.0, False, NOW - dt.timedelta(minutes=1))
        real_deals = b.deals_since
        def deals_since(since, magic=0, closing_only=True):
            rows = real_deals(since, magic, closing_only)
            return rows + ([extra] if calls["n"] >= 2 else [])
        b.deals_since = deals_since
        sd = account_standing(b, cfg(), NOW, cache_seconds=0)
        assert sd["made"] == round(TRADING + 7.40, 2) and sd["adjustments"] == 0.0

    def test_the_page_refresh_never_wipes_a_date_being_picked(self):
        page = render_status({"status": {"bot": "RUNNING"}, "health": {}})
        assert "location.reload" in page and "defaultValue" in page
        assert '<meta http-equiv="refresh"' not in page.replace('<noscript><meta http-equiv="refresh" content="10"></noscript>', "")


class TestTheScalperLeavesNoRealTradeBehind:
    def test_a_paper_scalper_closes_a_real_position_left_from_live(self, tmp_path):
        from mintel.broker.base import OrderRequest
        from tests.test_rapid_scalper import FakeBroker, synthetic_ticks, _engine
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        res = b.send(OrderRequest("EURUSD", Side.BUY, 0.05, sl=1.0, tp=0.0, magic=990411, comment="RS"))
        left = res.ticket
        other = b.send(OrderRequest("EURUSD", Side.BUY, 0.05, sl=1.0, tp=0.0, magic=990311, comment="MI")).ticket
        e = _engine(b, tmp_path, mode="PAPER")
        notes = e.cycle()
        assert left not in b.positions_ and other in b.positions_          # only the scalper's own, never another bot's
        assert any("left open from LIVE" in n for n in notes)
        e.cycle()
        assert len([c for c in b.closed if c[0] == left]) == 1             # once

    def test_a_live_scalper_never_closes_its_own_positions_this_way(self, tmp_path):
        from mintel.broker.base import OrderRequest
        from tests.test_rapid_scalper import FakeBroker, synthetic_ticks, _engine
        b = FakeBroker(); b.ticks_by_symbol["EURUSD"] = synthetic_ticks()
        t = b.send(OrderRequest("EURUSD", Side.BUY, 0.05, sl=1.0, tp=0.0, magic=990411, comment="RS")).ticket
        e = _engine(b, tmp_path, mode="LIVE")
        e.cycle()
        assert not [c for c in b.closed if c[0] == t and c[2] == "RS paper switch"]


class TestTheLineUpOfNineOctober:
    """The Rapid Momentum Rider replaces the Rapid Scalper and Financial Ian
    joins: six cards. The retired scalper's real money (magic 990411, about
    -8.72 since 5 Oct) still belongs to it, so the cards still add up."""

    SCALPER_DEALS = DEALS + [
        deal(11, 990411, -0.02, -0.02, True, START + dt.timedelta(hours=20)),
        deal(11, 990411, -8.70, -0.02, False, START + dt.timedelta(hours=21)),
        deal(12, 990811, -0.07, -0.07, True, NOW - dt.timedelta(minutes=50)),     # the Rider, live today
        deal(12, 990811, 5.10, -0.07, False, NOW - dt.timedelta(minutes=40)),
    ]

    def _cfg(self, tmp_path, rider="LIVE", ian="PAPER"):
        c = cfg()
        c.ops.data_dir = str(tmp_path)
        (tmp_path / "rider.json").write_text(json.dumps({"mode": rider, "user_pip_value_gbp": 1.0}))
        (tmp_path / "ian.json").write_text(json.dumps({"mode": ian, "feed": {"vendor": "none"}}))
        (tmp_path / "scalper.json").write_text(json.dumps({"mode": "OFF"}))
        return c

    def _standing(self, tmp_path, **kw):
        trading = round(TRADING - 8.72 + 5.03, 2)
        return account_standing(Broker(deals=self.SCALPER_DEALS, balance=2000 + trading), self._cfg(tmp_path, **kw),
                                NOW, cache_seconds=0), trading

    def test_six_bots_and_the_retired_scalper_and_the_rows_add_up(self, tmp_path):
        sd, trading = self._standing(tmp_path)
        assert [b["id"] for b in sd["bots"]] == ["market_intelligence", "momentum_rider", "momentum_runner",
                                                 "band_breaker", "crowd_fader", "financial_ian", "rapid_scalper"]
        by = {b["id"]: b for b in sd["bots"]}
        assert by["rapid_scalper"]["made"] == -8.72 and by["rapid_scalper"]["retired"] is True
        assert by["rapid_scalper"]["magic"] == 990411 and by["rapid_scalper"]["mode"] == "RETIRED"
        assert by["rapid_scalper"]["label"] == "Rapid Scalper (retired)"
        assert by["momentum_rider"]["made"] == 5.03 and by["momentum_rider"]["mode"] == "LIVE"
        assert by["momentum_rider"]["magic"] == 990811 and by["financial_ian"]["magic"] == 990911
        assert by["financial_ian"]["mode"] == "PAPER" and by["financial_ian"]["made"] == 0.0
        assert sd["other"]["made"] == 2.0                                      # never the scalper's money
        assert sd["made"] == trading
        assert sum(b["made"] for b in sd["bots"]) + sd["other"]["made"] == pytest.approx(sd["trading"])

    def test_modes_come_from_each_bots_own_file(self, tmp_path):
        sd, _ = self._standing(tmp_path, rider="OFF", ian="LIVE")
        by = {b["id"]: b for b in sd["bots"]}
        assert by["momentum_rider"]["mode"] == "OFF" and by["financial_ian"]["mode"] == "LIVE"
        (tmp_path / "ian.json").write_text("{broken")
        from mintel.ops.standing import bot_modes_and_magics
        c = cfg(); c.ops.data_dir = str(tmp_path)
        assert bot_modes_and_magics(c)["financial_ian"] == (990911, "PAPER")    # Ian's own default for a broken file

    def test_the_page_shows_six_cards_and_a_retired_line_only_when_it_has_money(self, tmp_path):
        sd, _ = self._standing(tmp_path)
        sd = dict(sd)
        deals = sd.pop("deals")
        top = _top(sd, deals)
        page = render_standing({"standing": sd}, top)
        assert page.count('<div class="hc') == 7                                # the account and six bots
        assert page.count('<div class="fk">Overall</div>') == 7
        assert "Rapid Scalper (retired): overall <b" in page and "-£8.72" in page
        assert "kept here so the cards add up to the account" in page
        assert 'class="nm">Rapid Scalper' not in page                          # a line, never a card
        today = render_standing({"standing": sd}, {**top, "sel": top["sel"]})
        assert "today <b" not in today.split("Rapid Scalper (retired)")[1][:80]   # nothing today: not shown as today
        # with no scalper money at all there is no line
        sd2 = dict(standing())
        d2 = sd2.pop("deals")
        assert "Rapid Scalper" not in render_standing({"standing": sd2}, _top(sd2, d2))

    def test_every_period_still_adds_up_with_the_retired_bot(self, tmp_path):
        from mintel.ops.standing import PERIOD_BUTTONS, period_view
        sd, _ = self._standing(tmp_path)
        sd = dict(sd)
        deals = sd.pop("deals")
        for key, _label in PERIOD_BUTTONS:
            v = period_view(sd, deals, key, now=NOW)
            assert sum(b["made"] for b in v["bots"].values()) + v["other"] == pytest.approx(v["trading"]), key
        assert period_view(sd, deals, "total", now=NOW)["bots"]["rapid_scalper"]["made"] == -8.72

    def test_open_trades_name_the_new_bots(self, tmp_path):
        from mintel.ops.standing import open_rows
        pos = [NS(ticket=1, symbol="EURUSD", side=Side.BUY, volume=0.1, entry_price=1.1, sl=1.09, tp=0.0, profit=1.0,
                  swap=0.0, magic=990811, open_time=NOW),
               NS(ticket=2, symbol="GBPUSD", side=Side.SELL, volume=0.1, entry_price=1.3, sl=1.31, tp=0.0, profit=-1.0,
                  swap=0.0, magic=990911, open_time=NOW),
               NS(ticket=3, symbol="USDJPY", side=Side.SELL, volume=0.1, entry_price=150.0, sl=151.0, tp=0.0, profit=0.5,
                  swap=0.0, magic=990411, open_time=NOW)]
        assert [r["bot"] for r in open_rows(pos, self._cfg(tmp_path))] == [
            "Rapid Momentum Rider", "Financial Ian", "Rapid Scalper (retired)"]

    def test_the_cards_are_three_across_on_a_wide_screen(self):
        from mintel.ops.dashboard import CSS
        assert ".cards{display:grid;grid-template-columns:repeat(3,1fr)" in CSS
        assert "@media (max-width:1100px){.cards{grid-template-columns:repeat(2,1fr)}}" in CSS
        assert "@media (max-width:600px){.cards{grid-template-columns:1fr}}" in CSS
