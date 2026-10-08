"""The page's one big figure: the account's standing since the reset, from
the broker alone, and the by-bot rows that add up to it."""
import datetime as dt
from types import SimpleNamespace as NS

import pytest

from mintel.config import Config
from mintel.ops.dashboard import render_standing, render_status
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
    deal(9, 990311, 50.0, 0.0, False, START - dt.timedelta(hours=2)),      # before the reset
]
MADE = round(21.3 + 8.28 - 9.36 + 2.0, 2)


class Broker:
    clock = None

    def __init__(self, deals=DEALS, fail=False):
        self.deals, self.fail = deals, fail

    def account(self):
        return NS(balance=2000 + MADE, equity=2000 + MADE - 2.13, currency="GBP")

    def deals_since(self, since, magic=0, closing_only=True):
        if self.fail:
            raise RuntimeError("history unavailable")
        assert closing_only is False                          # the entry half of the commission counts
        return [d for d in self.deals if (not magic or d["magic"] == magic)]

    def positions(self, magic=None):
        return [NS(magic=990311, profit=-2.0, swap=-0.13)]


def cfg():
    c = Config()
    c.tracking_start_utc = START.isoformat()
    c.bandbreaker.mode = "LIVE"
    return c


class TestTheFigure:
    def test_it_is_every_trade_on_the_account_since_the_reset(self):
        sd = account_standing(Broker(), cfg(), NOW, cache_seconds=0)
        assert sd["made"] == MADE and sd["source"] == "broker"
        assert sd["balance"] == pytest.approx(2000 + MADE) and sd["start_balance"] == 2000.0
        assert sd["open"] == -2.13
        assert sd["made_commission"] == 0.6 and sd["made_trades"] == 4

    def test_the_bot_rows_and_the_hand_row_add_up_to_it(self):
        sd = account_standing(Broker(), cfg(), NOW, cache_seconds=0)
        by = {b["id"]: b for b in sd["bots"]}
        assert by["market_intelligence"]["made"] == 29.58 and by["market_intelligence"]["today"] == 8.28
        assert by["band_breaker"]["made"] == -9.36 and by["band_breaker"]["mode"] == "LIVE"
        assert by["rapid_scalper"]["made"] == 0.0 and by["crowd_fader"]["mode"] == "PAPER"
        assert sd["other"]["made"] == 2.0
        total = sum(b["made"] for b in sd["bots"]) + sd["other"]["made"]
        assert total == pytest.approx(sd["made"])
        assert sum(b["open"] for b in sd["bots"]) + sd["other"]["open"] == pytest.approx(sd["open"])

    def test_today_starts_at_the_brokers_day(self):
        sd = account_standing(Broker(), cfg(), NOW, cache_seconds=0)
        assert sd["today"] == round(8.28 - 9.36 + 2.0, 2)

    def test_a_broker_that_cannot_be_read_gives_no_figure_not_a_wrong_one(self):
        sd = account_standing(Broker(fail=True), cfg(), NOW, cache_seconds=0)
        assert sd["made"] is None and "history unavailable" in sd["error"]
        html = render_standing({"standing": sd})
        assert "could not be read" in html and "£" not in html.split("hv")[1][:20]


class TestThePage:
    def _snap(self, sd):
        sd["practice"] = {"rapid_scalper": -15.03}
        return {"standing": sd, "status": {"bot": "RUNNING"}, "health": {"checks": []}, "updated": NOW.isoformat()}

    def test_the_big_box_leads_and_says_where_it_comes_from(self):
        sd = account_standing(Broker(), cfg(), NOW, cache_seconds=0)
        page = render_status(self._snap(sd))
        hero = page.index('class="hero')
        assert hero < page.index('id="more"')                          # the big box comes first, the rest is folded
        assert f"+£{MADE:,.2f}" in page and "Made since Mon 05 Oct, 12:30 UK - after commission" in page
        assert "£2,000.00 at the reset" in page and "MetaTrader" in page

    def test_the_table_has_a_row_per_bot_and_the_account_total(self):
        sd = account_standing(Broker(), cfg(), NOW, cache_seconds=0)
        html = render_standing(self._snap(sd)["standing"] and {"standing": sd})
        for label in ("Trend &amp; Breakout", "Rapid Scalper", "Momentum Runner", "Band Breaker", "Crowd Fader",
                      "Anything else (placed by hand)", "Account"):
            assert label in html, label
        assert "-£15.03" in html                                       # practice, shown in grey, not added
