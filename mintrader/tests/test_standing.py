"""The page's one big figure: the account's standing since the reset, from
the broker alone, and the by-bot rows that add up to it. Every finding of
the 8 Oct check is pinned here."""
import datetime as dt
from types import SimpleNamespace as NS

import pytest

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
        html = render_standing({"standing": sd})
        assert "Other changes to the balance" in html and "-£1.50" in html

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
        html = render_standing({"standing": sd})
        assert "could not be read" in html and "£" not in html.split('class="hv')[1][:30]

    def test_an_older_bridge_is_asked_per_bot_and_a_failed_bot_is_unknown_not_hand_trading(self):
        sd = standing(old_bridge=True, fail_magic=990311)
        by = {b["id"]: b for b in sd["bots"]}
        assert by["market_intelligence"]["made"] is None
        assert sd["other"]["made"] is None and sd["other"]["today"] is None      # never the missing bot's money
        assert "990311" in sd["error"]
        html = render_standing({"standing": sd})
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


class TestThePage:
    def test_the_figure_reaches_the_real_page_through_the_snapshot(self):
        state = DashboardState()
        sd = standing()
        sd["practice"] = {"rapid_scalper": -15.03}
        state.update(status={"bot": "RUNNING"}, health={"checks": []}, standing=sd)
        page = render_status(state.snapshot())
        assert f"+£{TRADING:,.2f}" in page and "Made since Mon 05 Oct, 12:30 UK - after commission" in page
        assert page.index('class="hero') < page.index('id="more"')
        assert "£2,000.00 at the reset" in page

    def test_the_table_has_a_row_per_bot_and_the_account_total(self):
        sd = standing()
        sd["practice"] = {"rapid_scalper": -15.03}
        html = render_standing({"standing": sd})
        for label in ("Trend &amp; Breakout", "Rapid Scalper", "Momentum Runner", "Band Breaker", "Crowd Fader",
                      "Anything else (placed by hand)", "Account"):
            assert label in html, label
        assert "-£15.03" in html

    def test_a_page_with_no_standing_yet_still_renders(self):
        page = render_status({"status": {"bot": "WAITING FOR METATRADER", "waiting": "x"}, "health": {}})
        assert "WAITING FOR METATRADER" in page and "has not heard from the broker yet" in page
