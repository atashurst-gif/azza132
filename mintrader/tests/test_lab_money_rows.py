"""14 Oct: the lab ideas trading on the DEMO account (mintel/labdemo, magics
992001-992999) are robot trades. The account figures, the page's periods and
the broker reconcile give them their own row; "placed by hand" holds only what
carried no bot's and no lab idea's number. Every row still adds up to the
account."""
import datetime as dt
import json
from types import SimpleNamespace as NS

import pytest

from mintel.config import Config
from mintel.ops import reconcile as RC
from mintel.ops import standing as S

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 14, 15, 0, tzinfo=UTC)
LAB_MAGIC = 992_001
TNB = 990_311


def deal(pos, magic, profit, entry, when, comm=0.0):
    return {"position": pos, "magic": magic, "profit": profit, "commission": comm, "is_entry": entry,
            "time": when, "symbol": "EURUSD", "volume": 1.0}


def at(h, m=0):
    return dt.datetime(2026, 10, 14, h, m, tzinfo=UTC)


DEALS = [
    deal(111, LAB_MAGIC, -0.50, True, at(9), comm=-0.5),            # a lab idea's demo trade
    deal(111, LAB_MAGIC, -150.00, False, at(10), comm=-0.5),
    deal(222, 0, 0.0, True, at(11)),                                 # placed by hand
    deal(222, 0, 4.00, False, at(11, 30)),
    deal(333, TNB, 0.0, True, at(12)),                               # Trend & Breakout
    deal(333, TNB, 10.00, False, at(12, 30)),
]
TRADING = round(-150.50 + 4.00 + 10.00, 2)


class Broker:
    clock = None

    def __init__(self, deals=DEALS, old_bridge=False):
        self.deals, self.old = deals, old_bridge

    def account(self):
        return NS(balance=10_000.0 + TRADING, equity=10_000.0 + TRADING - 2.0, currency="GBP")

    def deals_since(self, since, magic=0, closing_only=True):
        rows = [dict(d) for d in self.deals if not magic or d["magic"] == magic]
        if self.old:                                                 # an older bridge: no magic on its rows
            for r in rows:
                r.pop("magic")
        return rows

    def positions(self, magic=None):
        return [NS(magic=LAB_MAGIC, profit=-2.0, swap=0.0, ticket=444)]     # a lab idea's trade still open


def cfg(tmp_path, handed_out=True):
    c = Config()
    c.ops.data_dir = str(tmp_path / "data")
    c.account_reset_utc = "2026-10-14T00:00:00+00:00"
    c.account_reset_balance = 10_000.0
    if handed_out:
        lab = tmp_path / "data" / "labdemo"
        lab.mkdir(parents=True)
        (lab / "magics.json").write_text(json.dumps({"next": LAB_MAGIC + 1, "by_key": {"lab-1@v1": LAB_MAGIC}}))
    return c


@pytest.mark.parametrize("old_bridge", [False, True], ids=["bridge gives magics", "older bridge"])
def test_lab_demo_money_has_its_own_row_and_is_never_placed_by_hand(tmp_path, old_bridge):
    sd = S.account_standing(Broker(old_bridge=old_bridge), cfg(tmp_path), NOW, cache_seconds=0)
    assert sd["error"] == ""
    assert sd["lab"] == {"label": "Lab ideas on the demo", "made": -150.5, "today": -150.5, "open": -2.0}
    assert sd["other"]["label"] == "Anything else (placed by hand)"
    assert sd["other"]["made"] == 4.0 and sd["other"]["today"] == 4.0 and sd["other"]["open"] == 0.0
    by = {b["id"]: b for b in sd["bots"]}
    assert by["market_intelligence"]["made"] == 10.0
    assert all(b["made"] == 0.0 for bid, b in by.items() if bid != "market_intelligence")
    # every row still adds up to the account
    assert sd["made"] == sd["trading"] == TRADING
    assert sum(b["made"] for b in sd["bots"]) + sd["lab"]["made"] + sd["other"]["made"] == pytest.approx(TRADING)
    assert sum(b["open"] for b in sd["bots"]) + sd["lab"]["open"] + sd["other"]["open"] == pytest.approx(sd["open"])
    assert sd["unknown_magics"] == [] and sd["deals_complete"] is True
    for key in ("total", "today"):
        v = S.period_view(sd, sd["deals"], key, now=NOW)
        assert v["lab"] == -150.5 and v["other"] == 4.0, key
        assert round(sum(b["made"] for b in v["bots"].values()) + v["lab"] + v["other"], 2) == v["made"], key


def test_an_unreadable_lab_magic_leaves_the_lab_and_hand_rows_unknown_never_guessed(tmp_path):
    class Flaky(Broker):
        def deals_since(self, since, magic=0, closing_only=True):
            if magic == LAB_MAGIC:
                raise RuntimeError("bridge timed out")
            return super().deals_since(since, magic, closing_only)
    sd = S.account_standing(Flaky(old_bridge=True), cfg(tmp_path), NOW, cache_seconds=0)
    assert sd["lab"]["made"] is None and sd["other"]["made"] is None
    assert "lab ideas' deals" in sd["error"] and sd["deals_complete"] is False
    assert sd["unknown_magics"] == []                       # no bot's money is unknown
    v = S.period_view(sd, sd["deals"], "total", now=NOW)
    assert v["lab"] is None and v["other"] is None


@pytest.mark.parametrize("old_bridge", [False, True], ids=["bridge gives magics", "older bridge"])
def test_the_broker_reconcile_gives_the_lab_ideas_their_own_line_not_manual(tmp_path, old_bridge):
    c = cfg(tmp_path)
    res = RC.reconcile(Broker(old_bridge=old_bridge), c, NOW.date(), 990_411)
    assert res["lab"]["net"] == -150.5 and res["lab"]["positions"] == 1 and res["lab"]["commission"] == -1.0
    assert res["other"]["positions"] == 1 and res["other"]["net"] == 4.0          # only the hand trade
    assert res["mi"]["net"] == 10.0
    assert res["all"]["net"] == pytest.approx(TRADING)
    text = RC.render(res, c, NOW.date())
    lab_line = next(ln for ln in text.splitlines() if "lab ideas on the demo" in ln)
    assert "NET   -150.50" in lab_line
    manual = next(ln for ln in text.splitlines() if "other / manual" in ln)
    assert "NET     +4.00" in manual


@pytest.mark.parametrize("deals,note", [
    (DEALS, "Overall also includes -£150.50 from the lab ideas trading on the demo and +£4.00 from trades placed "
            "by hand, so the cards add up to the account."),
    ([d for d in DEALS if d["position"] != 222],
     "Overall also includes -£150.50 from the lab ideas trading on the demo, so the cards add up to the account."),
], ids=["lab, hand and a bot", "only lab and a bot"])
def test_the_overview_note_counts_the_lab_ideas_so_the_cards_add_up(tmp_path, deals, note):
    """rv14 R7: period_view moved the lab money out of 'other' into 'lab', and the Overview note (the only reader
    of 'other') did not read 'lab': the lab ideas' demo money dropped off the page and the note's 'the cards add
    up to the account' was false (or, with no hand trade, there was no note at all)."""
    from mintel.ops import dashboard as D

    class Exact(Broker):                                    # the balance is exactly these deals: no adjustments
        def account(self):
            return NS(balance=10_000.0 + round(sum(d["profit"] for d in self.deals), 2), equity=10_000.0,
                      currency="GBP")
    sd = S.account_standing(Exact(deals=deals), cfg(tmp_path), NOW, cache_seconds=0)
    sel = S.period_view(sd, sd["deals"], "today", now=NOW)
    tot = S.period_view(sd, sd["deals"], "total", now=NOW)
    cards = round(sum(b["made"] for b in tot["bots"].values()), 2)
    assert cards == 10.0 and tot["lab"] == -150.5
    assert tot["adjustments"] == 0.0
    assert round(cards + tot["lab"] + tot["other"], 2) == tot["made"]          # what the note claims
    top = {"sel": sel, "tot": tot, "p_sel": {}, "p_tot": {}, "data_dir": str(tmp_path / "data"), "now": NOW}
    page = D.render_standing({"standing": sd, "open_live": []}, top)
    assert f'<div class="small">{note}</div>' in page
    assert page.count("Overall also includes") == 1
