"""Formula 1 on the page (Aaron, 9 Oct about 19:30 UK: "Sack the rider off
and let's get trend and breakout live risking 0.2 higher pip average per
trade" - "Just Formula 1": only Trend & Breakout's minor currency pair
trades use real money, the rest stays on paper and still feeds the Momentum
Runner; judged after 20 live trades).

What the page must do:
* Trend & Breakout's row and its own page say its minor pairs are LIVE and
  the rest is PAPER - never plain "PAPER" - with its REAL money (MetaTrader,
  magic 990311, ``standing.period_view``) leading and its practice apart,
  in grey, labelled.
* The plan's focus line counts Formula 1's LIVE trades from MetaTrader's
  deal records ONLY: Trend & Breakout's real positions on minor currency
  pairs OPENED at or after tnb.live_since_utc - closed ones in full after
  every cost, still-open ones only as open. Never the journal.
* The Rapid Momentum Rider, switched off: its row says so (binned, no edge)
  while its real money still counts in the account; never a "waiting" that
  reads as if it will trade again; the plan flags neither it nor Trend &
  Breakout.

EVERY FIGURE HERE IS MADE-UP TEST DATA written by these tests (a fake
broker's deals in the MT5 adapter's shape, made-up status files, records
and research summaries): the shape of 9 Oct, not anyone's results.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
from html import unescape
from pathlib import Path
from types import SimpleNamespace as NS

from mintel.broker.base import Side
from mintel.config import Config
from mintel.ops import verdicts as V
from mintel.ops.dashboard import formula1_line
from mintel.ops.standing import account_standing, open_rows, period_view, practice_figures
from tests.test_page_figures import DEALS, NOW, TNB, Broker, deal, open_now, serve, t, write_records
from tests.test_simple_dashboard import research_file, row_figs, row_of, text_of

UTC = dt.timezone.utc
LIVE_SINCE = t(2026, 10, 9, 7, 30)                   # MADE-UP: 08:30 UK on Friday 9 Oct
# MADE-UP Trend & Breakout deals on 9 Oct, each position: (ticket, symbol, entry time, entry, exit time, exit)
F1_WIN, F1_LOSS, BEFORE, INDEX, MAJOR, STILL_OPEN = 6_100_001, 6_100_002, 6_100_003, 6_100_004, 6_100_005, 6_100_006
TNB_DEALS = [
    # Formula 1, LIVE: minor pairs opened after it went live - these two are its record
    deal(F1_WIN, TNB, -0.20, -0.20, True, t(2026, 10, 9, 7, 40), "EURGBP", 0.2),
    deal(F1_WIN, TNB, 11.80, -0.20, False, t(2026, 10, 9, 8, 10), "EURGBP", 0.2),          # +11.60 in full
    deal(F1_LOSS, TNB, -0.20, -0.20, True, t(2026, 10, 9, 7, 45), "AUDJPY", 0.2),
    deal(F1_LOSS, TNB, -8.60, -0.20, False, t(2026, 10, 9, 8, 20), "AUDJPY", 0.2),          # -8.80 in full
    # a minor pair OPENED before it went live (closed after): not Formula 1's live record
    deal(BEFORE, TNB, -0.20, -0.20, True, t(2026, 10, 9, 6, 50), "GBPCHF", 0.2),
    deal(BEFORE, TNB, 5.00, -0.20, False, t(2026, 10, 9, 8, 5), "GBPCHF", 0.2),
    # an index and a major pair of its own: not minor pairs, not counted
    deal(INDEX, TNB, 0.0, 0.0, True, t(2026, 10, 9, 7, 50), "US30", 0.1),
    deal(INDEX, TNB, 20.00, 0.0, False, t(2026, 10, 9, 8, 15), "US30", 0.1),
    deal(MAJOR, TNB, -0.20, -0.20, True, t(2026, 10, 9, 7, 55), "EURUSD", 0.2),
    deal(MAJOR, TNB, -3.00, -0.20, False, t(2026, 10, 9, 8, 25), "EURUSD", 0.2),
    # a minor pair still open: counted as open, never as closed
    deal(STILL_OPEN, TNB, -0.20, -0.20, True, t(2026, 10, 9, 8, 30), "EURCHF", 0.2),
]
F1_NET, F1_COSTS = round(11.60 - 8.80, 2), 0.80


def _open_positions():
    return open_now() + [NS(ticket=STILL_OPEN, symbol="EURCHF", side=Side.BUY, volume=0.2, entry_price=0.9400,
                            sl=0.9380, tp=0.0, profit=1.10, swap=0.0, magic=TNB, open_time=t(2026, 10, 9, 8, 30))]


def a_formula1_day(tmp_path: Path, live_groups=("FX_MINOR",), since=LIVE_SINCE, rider="LIVE", deals=None) -> NS:
    """The page fixture's day (tests/test_page_figures.py) plus the MADE-UP Trend & Breakout deals above, with
    config.json in the data folder saying what the installer's Formula 1 step writes: tnb PAPER, its minor pairs
    LIVE since ``since``. ``rider``: the Rider's mode in rider.json."""
    data = tmp_path / "data"
    data.mkdir(parents=True, exist_ok=True)
    write_records(data)
    (data / "rider.json").write_text(json.dumps({"mode": rider, "user_pip_value_gbp": 1.0}))
    tnb = {"mode": "PAPER"}
    if live_groups is not None:
        tnb.update(live_groups=list(live_groups), live_since_utc=since.isoformat() if since else "")
    (data / "config.json").write_text(json.dumps({"tnb": tnb}))
    cfg = Config()
    cfg.ops.data_dir = str(data)
    cfg.ops.log_dir = str(tmp_path / "logs")
    cfg.tnb.mode = "PAPER"
    cfg.runner.mode = "LIVE"
    broker = Broker(DEALS + TNB_DEALS if deals is None else deals, _open_positions())
    sd = dict(account_standing(broker, cfg, NOW, cache_seconds=0))
    rows = sd.pop("deals")
    return NS(data=data, cfg=cfg, broker=broker, sd=sd, deals=rows, open=open_rows(broker.positions(None), cfg))


def _pages(d: NS, *paths):
    httpd, get = serve(d)
    try:
        return [get(p) for p in paths]
    finally:
        httpd.shutdown()


def _plan_box(page: str) -> str:
    return text_of(page.split("<h2>The plan</h2>")[1].split("</section>")[0])


# ================================================ Formula 1's live count --
class TestFormula1sLiveCountIsMetaTradersAlone:
    def test_only_its_minor_pairs_opened_since_it_went_live_and_closed_count(self, tmp_path):
        d = a_formula1_day(tmp_path)
        f1 = V.formula1_live(d.deals, d.sd["open_tickets"], LIVE_SINCE)
        s = f1["stats"]
        assert (s["trades"], s["wins"], s["net"], s["costs"]) == (2, 1, F1_NET, F1_COSTS)   # EURGBP and AUDJPY only
        assert (s["days"], s["up_days"], s["down_days"]) == (1, 1, 0)
        assert f1["open"] == 1 and f1["since"] == LIVE_SINCE                                # EURCHF: open, not closed
        # the same rows with every position before it went live: nothing
        assert V.formula1_live(d.deals, d.sd["open_tickets"], t(2026, 10, 9, 8, 35))["stats"]["trades"] == 0
        assert V.formula1_live(d.deals, d.sd["open_tickets"], None)["stats"]["trades"] == 0  # no start: no count
        # the magic decides whose it is: the Rider's own GBPJPY (a minor pair, 990811) is never in the 2 above
        other = V.formula1_live(d.deals, d.sd["open_tickets"], LIVE_SINCE, magic=990_811)
        assert other["stats"]["trades"] == 1 and other["stats"]["net"] == 10.17 and other["open"] == 1

    def test_the_plan_box_shows_it_and_never_the_journal(self, tmp_path):
        d = a_formula1_day(tmp_path)
        # a MADE-UP journal row for a minor-pair trade closed after it went live, at +999: not in MetaTrader's rows
        from mintel.engine.journal import SCHEMA
        con = sqlite3.connect(d.data / "journal.sqlite")
        con.executescript(SCHEMA)
        con.execute("INSERT INTO trades (ticket, symbol, tactic, regime, opened_utc, closed_utc, pnl_money, side, entry, "
                    "stop, exit_price, mode) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (9_999_999, "GBPCHF", "MOMENTUM_CONTINUATION", "HIGH_VOL", t(2026, 10, 9, 7, 35).isoformat(),
                     t(2026, 10, 9, 8, 0).isoformat(), 999.0, "BUY", 1.0, 0.99, 1.01, "DEMO"))
        con.commit()
        con.close()
        (page,) = _pages(d, "/")
        box = _plan_box(page)
        assert ("Live so far: 2 trades closed since 09 Oct 08:30 UK, 1 won, +£2.80 after all costs (£0.80 of "
                "commission); 1 day up, 0 down - 2 of 20 toward the judgement. From MetaTrader's records. 1 open now, "
                "not counted until it closes.") in box
        assert "999" not in box and "Going forward since" not in box
        assert "Check:" not in box                    # the plan flags neither Trend & Breakout nor anything else here

    def test_no_live_trades_yet_says_so(self, tmp_path):
        d = a_formula1_day(tmp_path, since=t(2026, 10, 9, 8, 45))     # MADE-UP: live from 09:45 UK, after them all
        (page,) = _pages(d, "/")
        assert ("Live so far: no live trades yet (live since 09 Oct 09:45 UK) - 0 of 20 toward the judgement."
                in _plan_box(page))

    def test_when_it_cannot_count_it_says_why_rather_than_guess(self, tmp_path):
        d = a_formula1_day(tmp_path, since=None)                       # live, but no start time recorded
        (page,) = _pages(d, "/")
        assert "Live so far: its live start time is not recorded, so its live trades are not counted yet." in \
            _plan_box(page)
        d = a_formula1_day(tmp_path / "b")
        httpd, get = serve(d, standing={}, deals=[])                   # MetaTrader not read yet
        try:
            box = _plan_box(get("/"))
        finally:
            httpd.shutdown()
        assert ("Live so far: MetaTrader's records could not be read just now - no live figure is shown rather than "
                "a wrong one.") in box
        assert formula1_line({"mode": "LIVE"}).startswith("Trend & Breakout is fully LIVE on this computer")

    def test_the_judgement_after_20_live_trades_follows_the_plans_rule(self):
        def made_up(nets_per_day):                                     # MADE-UP live trades, one list per UK day
            out = []
            for i, nets in enumerate(nets_per_day):
                for j, net in enumerate(nets):
                    out.append({"closed": t(2026, 10, 12 + i, 9, j), "net": net, "costs": 0.4, "kind": "real"})
            return {"stats": V.stats(out), "open": 0, "since": LIVE_SINCE}
        early = made_up([[5.0] * 7])
        assert V.formula1_judgement(early).startswith("Formula 1 - its minor-pair trades - is LIVE: judge it after 20 "
                                                      "live trades (7 so far).")
        keep = made_up([[4.0, -2.0, 3.0, 2.0, -1.0]] * 3 + [[-3.0] * 5])
        assert keep["stats"]["trades"] == 20
        assert V.formula1_judgement(keep) == (
            "Formula 1 has its 20 live trades - time to judge it: +£3.00 after costs over 20 live trades, 3 days up "
            "and 1 down. By the plan's rule that is a keep: keep it and grow it.")
        back = made_up([[2.0, -3.0, 1.0, -1.0, 0.5]] * 4)
        assert V.formula1_judgement(back).endswith("it has not earned its place: back to PAPER, and test the next "
                                                   "candidate.")


# ======================================== Trend & Breakout: part LIVE, never plain PAPER --
class TestTrendAndBreakoutsRow:
    def test_its_row_leads_with_real_money_and_labels_the_practice(self, tmp_path):
        d = a_formula1_day(tmp_path)
        today, total = _pages(d, "/", "/?period=total")
        row = row_of(today, "Trend &amp; Breakout")
        assert '<span class="s-mode live">LIVE: minor pairs (Formula 1) - rest PAPER</span>' in row
        assert '<span class="s-mode">PAPER</span>' not in row and ">PAPER<" not in row
        sel = period_view(d.sd, d.deals, "today", now=NOW)
        tot = period_view(d.sd, d.deals, "total", now=NOW)
        # its REAL money: MetaTrader's, magic 990311, period_view's own figures - as a LIVE bot's row
        assert row_figs(row) == {"Today": sel["bots"]["market_intelligence"]["made"],
                                 "Overall": tot["bots"]["market_intelligence"]["made"]}
        n = sel["bots"]["market_intelligence"]["trades"]
        assert f">{n} real trades, " in row and "s-prac" not in row.split('<div class="s-extra">')[0]
        # its practice: the paper record's, in the grey line, labelled - never added
        p_sel = practice_figures(str(d.data), dt.datetime.fromisoformat(sel["start"]), NOW, "GBP",
                                 end=dt.datetime.fromisoformat(sel["end"]))
        p_tot = practice_figures(str(d.data), dt.datetime.fromisoformat(tot["start"]), NOW, "GBP",
                                 end=dt.datetime.fromisoformat(tot["end"]))
        extra = unescape(row.split('<div class="s-extra">')[1].split("</div>")[0])
        assert extra.startswith("The money above is real (MetaTrader's): its minor pairs (Formula 1) trade LIVE. "
                                "The rest is practice (PAPER), not real money")
        money = lambda v: ("+" if v > 0 else "-" if v < 0 else "") + f"£{abs(v):,.2f}"   # noqa: E731
        assert f"overall {money(p_tot.get('market_intelligence', 0.0))}" in extra
        assert f"today {money(p_sel.get('market_intelligence', 0.0))}" in extra
        trow = row_of(total, "Trend &amp; Breakout")
        assert row_figs(trow) == {"Overall": tot["bots"]["market_intelligence"]["made"]}

    def test_its_own_page_says_the_same_and_leads_with_formula_1(self, tmp_path):
        d = a_formula1_day(tmp_path)
        (raw,) = _pages(d, "/?bot=market_intelligence")
        page = text_of(raw)
        top = raw.split("<h2>Its detail</h2>")[0]
        assert '<span class="s-mode live">LIVE: minor pairs (Formula 1) - rest PAPER</span>' in top
        assert ">PAPER<" not in raw                                   # not in the box, not in its old card
        assert ("Formula 1 LIVE - its minor-pair trades with real money 2 trades over 1 UK day (1 up, 0 down); "
                "+£2.80 after costs") in page
        assert "1 more still open (not counted until it closes). 2 of 20 toward the judgement" in page
        assert ("Source: MetaTrader's records for magic number 990311: positions on minor currency pairs opened since "
                "it went live, each in full after commission - never its journal; dates: since 9 Oct 08:30 UK") in page
        assert ("Recommendation Formula 1 - its minor-pair trades - is LIVE: judge it after 20 live trades (2 so far). "
                "Keep it and grow it only if it is positive after costs with more up days than down; otherwise back to "
                "PAPER and test the next candidate. The rest stays on PAPER.") in page
        sel = period_view(d.sd, d.deals, "today", now=NOW)["bots"]["market_intelligence"]
        # (10 Oct: part LIVE, so its real trades are called real - review fix)
        assert f"{sel['trades']} real trades today, {sel['wins']} won, +£{sel['made']:.2f} after costs." in page
        # its old card and its live view say it too
        card = raw.split("<h2>Its detail</h2>")[1]
        assert '<span class="pill ok">LIVE: minor pairs (Formula 1) - rest PAPER</span>' in card
        assert "The rest is practice (PAPER), not real money" in card
        assert "It is on PAPER except its minor pairs (Formula 1): those orders go to MetaTrader with real money" in \
            unescape(card)

    def test_the_old_details_page_never_calls_it_plain_paper_either(self, tmp_path):
        d = a_formula1_day(tmp_path)
        (raw,) = _pages(d, "/details")
        card = raw.split('<div class="nm">Trend &amp; Breakout <span')[1].split('<div class="hc')[0]
        assert 'class="pill ok">LIVE: minor pairs (Formula 1) - rest PAPER</span>' in card
        assert "minor pairs (Formula 1) LIVE (real money); the rest real prices, simulated orders" in unescape(card)

    def test_without_live_markets_it_is_the_paper_row_it_always_was(self, tmp_path):
        d = a_formula1_day(tmp_path, live_groups=None)
        (page,) = _pages(d, "/")
        row = row_of(page, "Trend &amp; Breakout")
        assert '<span class="s-mode">PAPER</span>' in row and "Overall (practice)" in row
        assert "Live so far: not live on this computer yet" in _plan_box(page)

    def test_the_live_setting_is_read_as_the_trader_reads_it(self, tmp_path):
        def setting(block):
            (tmp_path / "config.json").write_text(json.dumps({"tnb": block}))
            return V.tnb_live_setting(str(tmp_path))
        assert setting({"mode": "PAPER", "live_groups": ["fx_minor"], "live_since_utc": "2026-10-09T18:45:00Z"}) == {
            "groups": ("FX_MINOR",), "since": t(2026, 10, 9, 18, 45)}
        assert setting({"mode": "PAPER", "live_groups": "FX_MINOR"})["groups"] == ("FX_MINOR",)    # a string is one
        assert setting({"mode": "PAPER", "live_groups": ["INDICES"]})["groups"] == ("INDEX",)       # the broker's alias
        assert setting({"mode": "LIVE", "live_groups": ["FX_MINOR"]})["groups"] == ()              # LIVE: all real
        assert setting({"mode": "PAPER", "live_groups": []}) == {"groups": (), "since": None}
        assert V.tnb_live_setting(str(tmp_path / "missing")) == {"groups": (), "since": None}
        assert V.live_words(("FX_MINOR",)) == "LIVE: minor pairs (Formula 1) - rest PAPER"
        assert V.live_words(("INDEX", "FX_MINOR")) == "LIVE: indices and minor pairs (Formula 1) - rest PAPER"
        assert V.live_words(()) == ""


# ================================================ the Rider, switched off --
class TestTheRiderSwitchedOff:
    def test_its_row_says_switched_off_and_binned_and_its_money_still_counts(self, tmp_path):
        d = a_formula1_day(tmp_path, rider="OFF")
        research_file(d.data, "momentum_rider")                          # MADE-UP research: no edge
        today, total = _pages(d, "/", "/?period=total")
        row = row_of(today, "Rapid Momentum Rider")
        assert '<span class="s-mode">OFF</span>' in row and '<span class="s-chip amber">NO EDGE</span>' in row
        sel = period_view(d.sd, d.deals, "today", now=NOW)
        tot = period_view(d.sd, d.deals, "total", now=NOW)
        assert row_figs(row) == {"Today": sel["bots"]["momentum_rider"]["made"],
                                 "Overall": tot["bots"]["momentum_rider"]["made"]}          # real money, not grey
        assert "practice" not in row.split('<div class="s-nums">')[1].split('<div class="s-extra">')[0]
        assert ("Switched off - it places no trades; binned: its own test on real past prices found no edge. Its real "
                "money from its LIVE days still counts in the account's figures.") in row
        assert "<b>What to do:</b> Leave it off - it is binned: do not put money on it." in row
        # the account still holds its money: Overall is the balance less the 2,000 at the reset, its trades in it
        assert tot["made"] == d.sd["made"] and tot["bots"]["momentum_rider"]["made"] != 0.0
        assert "Check:" not in _plan_box(today)                          # the plan bins it: no disagreement
        assert "Check:" not in _plan_box(total)

    def test_never_a_waiting_that_reads_as_if_it_will_trade_again(self, tmp_path):
        d = a_formula1_day(tmp_path, rider="OFF")                        # no research on this computer
        (home, page) = _pages(d, "/", "/?bot=momentum_rider")
        row = unescape(row_of(home, "Rapid Momentum Rider"))
        assert "Switched off: it places no trades, so there is nothing new to judge." in row
        assert "until it can trade" not in row and "until it can trade" not in unescape(page)
        assert "Nothing to judge while it is switched off." in row
        assert "No trades" in text_of(page) or "trades today" in text_of(page)
        v = V.bot_verdict("momentum_rider", "Rapid Momentum Rider", "OFF", 990_811, sel=None, tot=None,
                          data_dir=str(d.data), now=NOW)
        assert v["code"] == V.WAITING and v["headline"].startswith("Switched off")

    def test_an_off_bots_day_is_its_real_trades(self):
        real = V.stats([{"closed": t(2026, 10, 9, 8, 0), "net": -3.0, "costs": 0.1, "kind": "real"}])
        assert V.what_happened("OFF", "today", real, V.stats([]), real, None).startswith(
            "1 real trade today, 0 won, -£3.00 after costs - left from its LIVE days.")
        assert V.what_happened("OFF", "today", V.stats([]), V.stats([]), V.stats([]), None) == (
            "No trades today: it is switched off.")


# ================================================= 10 Oct review fixes --
# Three verifiers read the change set on 9-10 Oct; what they found on the
# page is fixed here, each test failing without its fix. MADE-UP data only.
LATER = t(2026, 10, 13, 10, 0)                       # MADE-UP: Tuesday 13 Oct, 11:00 UK, the FX market open
F1_REAL = 6_200_001
LATER_DEALS = [
    # MADE-UP: a Formula 1 trade (EURGBP, Trend & Breakout's magic) closed at 10:45 UK, 15 minutes ago
    deal(F1_REAL, TNB, -0.30, -0.30, True, t(2026, 10, 13, 9, 0), "EURGBP", 0.3),
    deal(F1_REAL, TNB, 5.70, -0.30, False, t(2026, 10, 13, 9, 45), "EURGBP", 0.3),
    # MADE-UP: the Runner's last ride, on Friday
    deal(6_200_002, 990_511, 0.0, 0.0, True, t(2026, 10, 9, 19, 11), "US500", 0.3),
    deal(6_200_002, 990_511, 8.0, 0.0, False, t(2026, 10, 9, 20, 1), "US500", 0.3),
]


def a_later_day(tmp_path: Path, deals=LATER_DEALS) -> NS:
    data = tmp_path / "data"
    data.mkdir(parents=True, exist_ok=True)
    write_records(data, LATER)
    (data / "rider.json").write_text(json.dumps({"mode": "OFF", "user_pip_value_gbp": 1.0}))
    (data / "config.json").write_text(json.dumps({"tnb": {"mode": "PAPER", "live_groups": ["FX_MINOR"],
                                                          "live_since_utc": "2026-10-09T18:45:00+00:00"}}))
    cfg = Config()
    cfg.ops.data_dir, cfg.ops.log_dir = str(data), str(tmp_path / "logs")
    cfg.tnb.mode, cfg.runner.mode = "PAPER", "LIVE"
    broker = Broker(deals, [])
    sd = dict(account_standing(broker, cfg, LATER, cache_seconds=0))
    rows = sd.pop("deals")
    return NS(data=data, cfg=cfg, broker=broker, sd=sd, deals=rows, open=[])


def _later_pages(d: NS, *paths, **extra):
    httpd, get = serve(d, now=LATER, **extra)
    try:
        return [get(p) for p in paths]
    finally:
        httpd.shutdown()


class TestWhatToDoNeverCountsWhatItCannotCount:
    """Verifier (page lens): with MetaTrader unreadable, or no live start time
    recorded, Trend & Breakout's What to do still said '(0 so far)' while
    the plan box said it could not count - a made-up figure on the page."""

    def test_metatrader_unreadable(self, tmp_path):
        d = a_formula1_day(tmp_path)
        httpd, get = serve(d, standing={}, deals=[])
        try:
            home, page = get("/"), get("/?bot=market_intelligence")
        finally:
            httpd.shutdown()
        row = text_of(row_of(home, "Trend &amp; Breakout"))
        assert "so far)" not in row
        assert ("judge it after 20 live trades (they cannot be counted just now: MetaTrader's records could not be "
                "read)") in row
        rec = text_of(page).split("Recommendation")[1].split("The evidence")[0]
        assert "so far)" not in rec and "cannot be counted just now" in rec

    def test_no_live_start_time(self, tmp_path):
        for i, since in enumerate((None, "9 Oct evening")):
            d = a_formula1_day(tmp_path / str(i), since=None)
            if since:                                                    # a malformed time, as a hand edit leaves it
                raw = json.loads((d.data / "config.json").read_text())
                raw["tnb"]["live_since_utc"] = since
                (d.data / "config.json").write_text(json.dumps(raw))
            (home,) = _pages(d, "/")
            row = text_of(row_of(home, "Trend &amp; Breakout"))
            assert "live trades (0 so far)" not in row, since
            assert "(they are not counted yet: its live start time is not recorded)" in row, since

    def test_counted_it_says_the_count_as_before(self, tmp_path):
        (home,) = _pages(a_formula1_day(tmp_path), "/")
        assert "judge it after 20 live trades (2 so far)" in text_of(row_of(home, "Trend &amp; Breakout"))


class TestTheQuietBannerSeesFormula1:
    """Verifier (page lens): the 'No live trade since ...' banner counted only
    bots whose mode is LIVE, so a Formula 1 trade closed minutes ago was
    ignored on Trend & Breakout's page and on /details."""

    def test_a_formula1_trade_15_minutes_ago_means_no_banner(self, tmp_path):
        d = a_later_day(tmp_path)
        bot, det = _later_pages(d, "/?bot=market_intelligence", "/details", open_live=[])
        for page in (bot, det):
            assert "No live trade" not in text_of(page)

    def test_when_quiet_trend_and_breakout_is_named_among_the_live_bots(self, tmp_path):
        d = a_later_day(tmp_path, deals=LATER_DEALS[2:])                 # no Formula 1 trade yet
        (det,) = _later_pages(d, "/details", open_live=[])
        banner = det.split('<div class="quiet">')[1].split("</div></div>")[0]
        assert "No live trade since Fri 09 Oct 21:01 UK" in banner      # the Runner's ride: the newest live trade
        assert "<b>Trend &amp; Breakout</b>: LIVE: minor pairs (Formula 1) - rest PAPER - " in banner


class TestNeverPlainPaperAnywhere:
    """Verifier (page lens): the 'Now:' line, the /details note and the
    attribution's Trend & Breakout source still said plain PAPER, all
    simulated, and called Formula 1's real money practice."""

    def test_the_now_line(self, tmp_path):
        d = a_later_day(tmp_path)
        bot, det = _later_pages(d, "/?bot=market_intelligence", "/details", open_live=[])
        for page in (bot, det):
            card = text_of(page.split('<div class="nm">Trend &amp; Breakout <span')[1].split('<div class="hc')[0])
            assert "Now: PAPER -" not in card
            assert "Now: LIVE: minor pairs (Formula 1) - rest PAPER - last real trade" in card

    def test_the_details_note_and_the_attribution_source(self, tmp_path):
        d = a_formula1_day(tmp_path)
        httpd, get = serve(d, status={"bot": "RUNNING", "tnb_mode": "PAPER"})
        try:
            det = get("/details")
        finally:
            httpd.shutdown()
        txt = text_of(det)
        assert "is on PAPER: Today, Win rate today, This strategy and Open trades are its practice" not in txt
        assert ("Trend & Breakout is on PAPER except its minor pairs (Formula 1), which trade LIVE with real money: "
                "Today, Win rate today, This strategy and Open trades are its real trades there and its practice "
                "together") in txt
        from mintel.ops import attribution as A
        assert "minor pairs (Formula 1)" in A.tnb_paper_source(("FX_MINOR",))
        assert "its orders are simulated" not in A.tnb_paper_source(("FX_MINOR",))
        assert A.tnb_paper_source(()) == A.TNB_PAPER_SOURCE


class TestTheVerdictSaysWhatItCovers:
    """Verifier (page lens): Trend & Breakout's chip and headline judge its
    whole record (practice and its earlier real trades) next to the Formula
    1 pill, with nothing saying so."""

    def test_the_headline_and_the_action_say_as_a_whole(self, tmp_path):
        d = a_formula1_day(tmp_path)
        (home, page) = _pages(d, "/", "/?bot=market_intelligence")
        row = text_of(row_of(home, "Trend &amp; Breakout"))
        assert "As a whole (practice and real together; Formula 1 is judged on its own below):" in row
        verdict = text_of(page).split("Verdict:")[1].split("What happened")[0]
        assert "for Trend & Breakout as a whole - Formula 1 is judged on its own" in verdict
        v = V.bot_verdict("momentum_runner", "Momentum Runner", "LIVE", 990_511, sel=None, tot=None,
                          data_dir=str(d.data), now=NOW)
        assert not v["headline"].startswith("As a whole")               # other bots as before


class TestWhatHappenedSaysRealWhenPartIsReal:
    def test_a_period_with_practice_only_says_no_real_trades(self):
        made_up = [{"closed": t(2026, 10, 9, 8, j), "net": 4.2, "costs": 0.1, "kind": "practice"} for j in range(3)]
        out = V.what_happened("LIVE", "today", V.stats([]), V.stats(made_up), V.stats(made_up), None, mixed=True)
        assert out.startswith("No real trades today. Also 3 practice trades today (+£12.60, not money).")
        real = V.stats([{"closed": t(2026, 10, 9, 8, 0), "net": 5.4, "costs": 0.6, "kind": "real"}])
        out = V.what_happened("LIVE", "today", real, V.stats([]), real, None, mixed=True)
        assert out.startswith("1 real trade today, 1 won, +£5.40 after costs.")
        assert "left from its LIVE days" not in out
        assert V.what_happened("LIVE", "today", V.stats([]), V.stats([]), V.stats([]), None) == "No trades today."

    def test_on_the_page(self, tmp_path):
        d = a_later_day(tmp_path, deals=LATER_DEALS[2:])
        (page,) = _later_pages(d, "/?bot=market_intelligence", open_live=[])
        assert "What happened - Today No real trades today." in text_of(page)


class TestTheRowStaysPartLiveIfItsVerdictFails:
    """Verifier (page lens): when Trend & Breakout's verdict raised, its row
    fell back to plain PAPER and called Formula 1's real money 'from its
    LIVE days'."""

    def test_the_fallback_keeps_the_live_markets(self, tmp_path, monkeypatch):
        real = V.bot_verdict

        def boom(bid, *a, **k):
            if bid == V.TNB:
                raise ValueError("made-up failure")
            return real(bid, *a, **k)
        monkeypatch.setattr(V, "bot_verdict", boom)
        d = a_formula1_day(tmp_path)
        (home,) = _pages(d, "/")
        row = row_of(home, "Trend &amp; Breakout")
        assert '<span class="s-mode live">LIVE: minor pairs (Formula 1) - rest PAPER</span>' in row
        assert "Overall (practice)" not in row and "from its LIVE days" not in row
        assert "The money above is real (MetaTrader's)" in unescape(row)


class TestThePlanNeverDatesTheStartItself:
    def test_the_focus_leaves_the_start_to_live_so_far(self):
        plan, err = V.load_plan()
        assert not err
        what = plan["focus"]["what"]
        assert "since 9 Oct" not in what and what.startswith("LIVE by Aaron's decision of 9 Oct.")
