"""The simple main page, each bot's verdict, and the plan (Aaron, 9 Oct
16:45 UK: "turn the UI of the bot display into something really easy for me
to read ... Just a simple dashboard showing the results for today and all
other filters (always correctly), the bots' overall logic on what's
happened and its recommendations for each separate bot and a nice simple but
intelligent plan on where we're going, what we're looking to bin off, change
or keep").

EVERY FIGURE IN THESE TESTS IS MADE-UP TEST DATA, written by the tests: the
fake broker, status files and records of tests/test_page_figures.py (the
shape of 9 Oct, not its results), and made-up trades and research summaries
built here. None of it is a result of any bot.
"""
from __future__ import annotations

import datetime as dt
import json
import re
from html import unescape
from pathlib import Path

import pytest

from mintel.ops import verdicts as V
from mintel.ops.dashboard import (REFRESH_JS, DashboardState, render_bot_detail, render_home, start_dashboard)
from mintel.ops.standing import PERIOD_BUTTONS, period_view, practice_figures, uk_midnight
from tests.test_page_figures import (LATER, MADE, NOW, NOW_TAG, TODAY, YESTERDAY, a_day, quiet_day, serve, t)

UTC = dt.timezone.utc
PERIODS = [k for k, _ in PERIOD_BUTTONS] + ["custom"]
CUSTOM = {"custom": ("2026-10-08", "2026-10-08")}


# ----------------------------------------------------------------- helpers --
def money(txt: str) -> float:
    """'+£21.83' / '-£9.36' / '£2,012.42' -> a number."""
    s = unescape(re.sub(r"<[^>]+>", "", txt)).strip()
    m = re.fullmatch(r"([+-]?)£([\d,]+\.\d\d)", s)
    assert m, f"not a money figure: {s!r}"
    v = float(m.group(2).replace(",", ""))
    return -v if m.group(1) == "-" else v


def account_section(page: str) -> str:
    return page.split('<div class="s-acct">')[1].split("</section>")[0]


def account_figs(page: str) -> dict:
    """{label: number} of the account's big figures."""
    sec = account_section(page)
    return {unescape(k): money(v) for k, v in
            re.findall(r'<div class="s-k">([^<]*)</div><div class="s-big">(.*?)</div>', sec)}


def row_of(page: str, label: str) -> str:
    """One bot's row on the main page."""
    name = f'<span class="s-name">{label}</span>'
    assert page.count(name) == 1, label
    start = page.rindex('<a class="s-bot"', 0, page.index(name))
    return page[start:page.index("</a>", page.index(name)) + 4]


def row_figs(row: str) -> dict:
    return {unescape(k): money(v) for k, v in re.findall(r'<div class="s-k">([^<]*)</div><div class="s-m">(.*?)</div>', row)}


def text_of(page: str) -> str:
    page = re.sub(r"<(style|script)>.*?</\1>", "", page, flags=re.S)
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", page)))


def q(key: str) -> str:
    """The address of a period, with the custom dates when it is custom."""
    if key in CUSTOM:
        a, b = CUSTOM[key]
        return f"period=custom&from={a}&to={b}"
    return f"period={key}"


def made_up(per_day: list[list[float]], cost: float = 0.5, start: dt.date = dt.date(2026, 10, 5),
            kind: str = "real") -> list[dict]:
    """MADE-UP trades: one list of per-trade nets (after costs) per UK day, from ``start``."""
    out = []
    for i, nets in enumerate(per_day):
        day = start + dt.timedelta(days=i)
        for j, net in enumerate(nets):
            out.append({"closed": dt.datetime(day.year, day.month, day.day, 9, 0, j % 60, tzinfo=UTC),
                        "net": net, "costs": cost, "symbol": "EURUSD", "ticket": 1000 * i + j, "kind": kind})
    return out


def research_file(data: Path, bot: str = "momentum_rider", code: str = "NO_EDGE", trades: int = 1073,
                  synthetic: bool = False, day: str = "2026-10-09", text: str = "") -> Path:
    """A MADE-UP research summary in the shape each research tool writes."""
    folder = V.RESEARCH_DIRS[bot]
    out = data / folder / day
    out.mkdir(parents=True, exist_ok=True)
    if bot == "momentum_rider":
        sm = {"title": "MADE-UP test summary", "synthetic": synthetic, "data_source": "MADE-UP real-tick shape",
              "days_replayed": [f"2026-09-{d:02d}" for d in range(28, 31)] + [f"2026-10-0{d}" for d in range(1, 8)],
              "verdict": {"code": code, "text": text or f"{code} - made up"},
              "headline": {"normal": {"trades": trades, "net_pips": -1427.1, "expectancy_pips": -1.33,
                                      "profit_factor": 0.46, "avg_win_pips": 3.3, "avg_loss_pips": -3.7,
                                      "avg_mfe_pips": 2.9}},
              "variants": [{"method_id": f"V{i}", "normal": {"net_pips": -10.0 - i}} for i in range(4)]}
    else:
        sm = {"label": "MADE-UP", "synthetic": synthetic, "source": "MADE-UP book shape", "days": 6,
              "period": {"start": "2026-09-01", "end": "2026-09-06"},
              "verdict": code.replace("_", " "), "verdict_reasons": [text or "made up"],
              "stats": {"trades": trades, "net_gbp": -40.0, "expectancy_gbp": -1.0, "profit_factor": 0.7,
                        "avg_win_gbp": 2.0, "avg_loss_gbp": -3.0}}
    (out / "summary.json").write_text(json.dumps(sm))
    return out / "summary.json"


# ======================================== the account: standing's own figures --
class TestTheAccountIsStandingsOwnFigures:
    def test_every_period_shows_exactly_what_period_view_gives(self, tmp_path):
        d = a_day(tmp_path)
        httpd, get = serve(d)
        try:
            pages = {k: get(f"/?{q(k)}") for k in PERIODS}
            pages["custom range"] = get("/?period=custom&from=2026-10-05&to=2026-10-09")
        finally:
            httpd.shutdown()
        tot = period_view(d.sd, d.deals, "total", now=NOW)
        assert tot["made"] == MADE == d.sd["made"]                       # the balance less the 2,000 at the reset
        for k, page in pages.items():
            a, b = CUSTOM.get(k, ("2026-10-05", "2026-10-09") if k == "custom range" else ("", ""))
            v = period_view(d.sd, d.deals, "custom" if k == "custom range" else k, a, b, now=NOW)
            figs = account_figs(page)
            assert figs["Balance"] == d.sd["balance"] == 2012.42, k
            if v["key"] == "total":
                assert figs == {"Balance": 2012.42, "Total (Overall)": tot["made"]}, k
            else:
                assert figs == {"Balance": 2012.42, v["label"]: v["made"], "Overall": tot["made"]}, k
                sub = f"{v['trades']} trade{'' if v['trades'] == 1 else 's'} closed, {v['wins']} won" \
                    if v["trades"] else "no trades closed"
                assert sub in account_section(page), k
            assert text_of(page).count("Days are UK days, midnight to midnight") == 1, k
        assert account_figs(pages["today"])["Today"] == TODAY == 21.83
        assert account_figs(pages["yesterday"])["Yesterday"] == YESTERDAY == -9.36
        assert account_figs(pages["custom"])["Thu 08 Oct 2026"] == -9.36       # the silver short, on its UK day

    def test_each_bots_row_carries_the_same_money_as_the_full_page(self, tmp_path):
        d = a_day(tmp_path)
        httpd, get = serve(d)
        try:
            pages = {k: get(f"/?{q(k)}") for k in PERIODS}
        finally:
            httpd.shutdown()
        tot = period_view(d.sd, d.deals, "total", now=NOW)
        p_tot = practice_figures(str(d.data), dt.datetime.fromisoformat(tot["start"]), NOW, "GBP",
                                 end=dt.datetime.fromisoformat(tot["end"]))
        live = {"momentum_rider": "Rapid Momentum Rider", "momentum_runner": "Momentum Runner",
                "financial_ian": "Financial Ian"}
        paper = {"market_intelligence": "Trend &amp; Breakout", "band_breaker": "Band Breaker",
                 "crowd_fader": "Crowd Fader"}
        for k, page in pages.items():
            a, b = CUSTOM.get(k, ("", ""))
            v = period_view(d.sd, d.deals, k, a, b, now=NOW)
            p_sel = practice_figures(str(d.data), dt.datetime.fromisoformat(v["start"]), NOW, "GBP",
                                     end=dt.datetime.fromisoformat(v["end"]))
            for bid, label in live.items():
                figs = row_figs(row_of(page, label))
                want = {"Overall": tot["bots"][bid]["made"]}
                if v["key"] != "total":
                    want[v["label"]] = v["bots"][bid]["made"]
                assert figs == want, (k, bid)
            for bid, label in paper.items():
                figs = row_figs(row_of(page, label))
                want = {"Overall (practice)": p_tot[bid]}
                if v["key"] != "total":
                    want[f"{v['label']} (practice)"] = p_sel[bid]
                assert figs == want, (k, bid)
        rider = row_of(pages["today"], "Rapid Momentum Rider")
        assert ">2 trades, 50% won<" in rider and ">Trades today<" in rider          # 10.17 won, -3.04 lost
        assert row_figs(rider) == {"Today": 7.13, "Overall": 7.08}

    def test_practice_trades_are_counted_exactly_as_the_practice_figure(self, tmp_path):
        d = a_day(tmp_path)
        for k in ("today", "yesterday", "week", "total"):
            v = period_view(d.sd, d.deals, k, now=NOW)
            a, b = dt.datetime.fromisoformat(v["start"]), dt.datetime.fromisoformat(v["end"])
            figs = practice_figures(str(d.data), a, NOW, "GBP", end=b)
            for bid in ("market_intelligence", "momentum_rider", "momentum_runner", "band_breaker", "crowd_fader",
                        "financial_ian"):
                trades = V.practice_trades(str(d.data), bid, a, b)
                assert round(sum(x["net"] for x in trades), 2) == figs.get(bid, 0.0), (k, bid)

    def test_when_metatrader_has_not_been_read_no_figure_is_shown_rather_than_a_wrong_one(self, tmp_path):
        state = DashboardState()
        state.update(status={"bot": "WAITING FOR METATRADER", "waiting": "no terminal"}, health={})
        httpd = start_dashboard(state, "127.0.0.1", 0)
        try:
            import urllib.request
            page = urllib.request.urlopen(f"http://127.0.0.1:{httpd.server_address[1]}/?period=week").read().decode()
        finally:
            httpd.shutdown()
        assert "MetaTrader's records could not be read just now" in unescape(page)
        acct = page.split('<div class="s-k">Account</div>')[1].split("</section>")[0]
        assert '<div class="s-big">-</div>' in acct and "£" not in acct
        assert "waiting for MetaTrader (no terminal)" in page                       # one line: what is wrong
        assert 'class="s-p on" href="/?period=week"' in page                        # the chosen period still shows
        for label in ("Trend &amp; Breakout", "Rapid Momentum Rider", "Momentum Runner", "Band Breaker",
                      "Crowd Fader", "Financial Ian"):
            assert f'<span class="s-name">{label}</span>' in page


    def test_before_metatrader_is_read_each_bots_practice_is_still_the_chosen_periods(self, tmp_path):
        d = a_day(tmp_path)
        httpd, get = serve(d, standing={}, deals=[])                     # MetaTrader not heard from yet
        try:
            week, yesterday, detail = get("/?period=week"), get("/?period=yesterday"), get("/?bot=band_breaker&period=week")
        finally:
            httpd.shutdown()
        band = row_of(week, "Band Breaker")
        assert ">Trades this week<" in band and ">1 practice trade, 100% won<" in band   # the made-up 9 Oct trade
        assert ">no practice trades<" in row_of(yesterday, "Band Breaker")
        assert "What happened - This week" in detail
        assert "1 practice trade this week, 1 won, +£4.20 after costs (practice, not money)." in unescape(detail)
        rider = row_of(week, "Rapid Momentum Rider")
        assert '<div class="s-m">-</div>' in rider and "No verdict just now" in rider    # never a guess at real money


# ================================ a bot's own page: period_view's own money --
class TestABotsOwnPageCarriesTheSameMoney:
    """MADE-UP deals: the fixture's day plus a Trend & Breakout position OPENED before the 5 Oct 11:30 UTC reset
    (0.40 of commission then) and closed after it at +20.00. The Rider's third trade is still open, its 0.05 of
    commission already on the balance."""

    def _day(self, tmp_path):
        from tests.test_page_figures import DEALS, TNB, deal
        early = deal(5_000_009, TNB, -0.40, -0.40, True, t(2026, 10, 5, 9, 0), "EURGBP", 0.4)
        late = deal(5_000_009, TNB, 20.00, -0.40, False, t(2026, 10, 6, 10, 0), "EURGBP", 0.4)
        return a_day(tmp_path, deals=DEALS + [early, late])

    def test_the_total_on_a_bots_page_is_period_views_figure_and_says_why_it_differs(self, tmp_path):
        d = self._day(tmp_path)
        httpd, get = serve(d)
        try:
            pages = {bid: text_of(get(f"/?bot={bid}&period=total")) for bid in ("momentum_rider", "market_intelligence")}
            home = get("/?period=total")
        finally:
            httpd.shutdown()
        tot = period_view(d.sd, d.deals, "total", now=NOW)
        rider, tnb = tot["bots"]["momentum_rider"]["made"], tot["bots"]["market_intelligence"]["made"]
        assert (rider, tnb) == (7.08, 34.70)                       # the main page's Overall for each
        assert row_figs(row_of(home, "Rapid Momentum Rider"))["Overall"] == rider
        assert "overall +£34.70." in row_of(home, "Trend &amp; Breakout")
        # What happened - Total: the same figure, never the closed trades added up (+7.13 and +34.30)
        assert "2 trades in total, 1 won, +£7.08 after costs." in pages["momentum_rider"]
        assert ("Its 2 closed trades on their own come to +£7.13; the Overall figure (+£7.08) also counts £0.05 of "
                "costs already charged on 1 trade still open.") in pages["momentum_rider"]
        assert "2 real trades in total, 2 won, +£34.70 after costs - left from its LIVE days." in pages["market_intelligence"]
        assert ("Its 2 closed trades on their own come to +£34.30; the Overall figure (+£34.70) leaves out £0.40 of "
                "costs charged before the reset on 1 trade that closed after it.") in pages["market_intelligence"]
        # the Real money evidence: Overall is period_view's, in every period
        for bid, made in (("momentum_rider", rider), ("market_intelligence", tnb)):
            assert f"Real money Overall since 5 Oct: {'+' if made >= 0 else '-'}£{abs(made):.2f}." in pages[bid], bid
            assert "+£7.13 after costs" not in pages[bid] and "+£34.30 after costs" not in pages[bid]

    def test_every_period_on_a_bots_page_matches_period_view(self, tmp_path):
        d = self._day(tmp_path)
        httpd, get = serve(d)
        try:
            for k in PERIODS:
                page = text_of(get(f"/?bot=momentum_rider&{q(k)}"))
                a, b = CUSTOM.get(k, ("", ""))
                v = period_view(d.sd, d.deals, k, a, b, now=NOW)["bots"]["momentum_rider"]
                if v["trades"]:
                    m = re.search(r"(\d+) trades? [^,]*, (\d+) won, ([+-]£[\d,]+\.\d\d) after costs\.", page)
                    assert m and int(m.group(1)) == v["trades"] and money(m.group(3)) == v["made"], k
                else:
                    assert re.search(r"No trades [^.]*\.", page), k
        finally:
            httpd.shutdown()


# ============================================ PAPER is practice, labelled --
class TestPracticeIsLabelled:
    def test_paper_bots_show_practice_in_grey_and_say_so(self, tmp_path):
        d = a_day(tmp_path)
        httpd, get = serve(d)
        try:
            today, yesterday = get("/"), get("/?period=yesterday")
        finally:
            httpd.shutdown()
        band = row_of(today, "Band Breaker")
        assert '<div class=s-prac><div class="s-k">Today (practice)</div><div class="s-m">' in band
        assert row_figs(band) == {"Today (practice)": 4.20, "Overall (practice)": 4.20}
        assert ">1 practice trade, 100% won<" in band and "Practice only (PAPER), not real money." in band
        crowd = row_of(yesterday, "Crowd Fader")
        assert ("Practice only, not real money. Real money from its LIVE days: yesterday -£9.36 (1 trade), "
                "overall -£9.36.") in crowd
        tnb = row_of(today, "Trend &amp; Breakout")
        assert "Real money from its LIVE days: today +£14.70 (1 trade), overall +£14.70." in tnb
        for label in ("Rapid Momentum Rider", "Momentum Runner", "Financial Ian"):        # LIVE: money, no practice
            row = row_of(today, label)
            nums = row.split('<div class="s-nums">')[1]                 # its figures: real money only
            assert "practice" not in nums and "s-prac" not in row, label
            assert '<span class="s-mode live">LIVE</span>' in row
        assert '<span class="s-mode">PAPER</span>' in band


class TestRealAndPracticeAreNamedApart:
    def test_a_live_bot_with_practice_rows_names_both_counts(self, tmp_path):
        """The fixture's Rider: 2 real trades and 1 MADE-UP PAPER trade from before it went live."""
        d = a_day(tmp_path)
        httpd, get = serve(d)
        try:
            home, page = get("/"), text_of(get("/?bot=momentum_rider&period=total"))
        finally:
            httpd.shutdown()
        row = row_of(home, "Rapid Momentum Rider")
        assert "Too early to tell: 3 trades (2 real, 1 practice) so far" in unescape(row)
        assert "Its whole record, real and practice together: 3 trades (2 real, 1 practice) over 1 UK day" in page
        s = V.stats(made_up([[1.0] * 6]) + made_up([[1.0] * 6], kind="practice"))
        assert V.headline(V.PROMISING, s, None, "", (6, 6)).startswith(
            "Promising: +£1.00 a trade after costs over 12 trades (6 real, 6 practice)")
        assert V.headline(V.NOT_ENOUGH, V.stats(made_up([[1.0]], kind="practice")), None, "", (0, 1)) == (
            "Too early to tell: 1 practice trade so far - it needs 30 over 5 UK days.")


# ===================================================== every verdict path --
class TestEveryVerdictPath:
    """Made-up trades only (``made_up``), labelled as such."""

    def test_proven(self):
        s = V.stats(made_up([[4.0, -2.0, 3.0, 2.0, -1.0, 2.0]] * 4 + [[-3.0] * 3 + [1.0] * 3] * 1))
        assert s["trades"] == 30 and s["days"] == 5 and s["up_days"] == 4 and s["down_days"] == 1
        assert V.decide(s) == (V.PROVEN, "enough trades, positive after costs, and more up days than down")
        assert s["net"] == 26.0 and s["profit_factor"] == 2.24
        assert V.headline(V.PROVEN, s, None, "") == "Proven: +£0.87 a trade after costs over 30 trades, 4 of 5 days up."
        assert V.VERDICTS[V.PROVEN] == ("KEEP AND GROW", "blue")

    def test_promising_needs_ten_trades_and_says_what_is_missing(self):
        nine = V.stats(made_up([[1.0] * 9]))
        assert V.decide(nine)[0] == V.NOT_ENOUGH                         # a few lucky trades are not evidence
        ten = V.stats(made_up([[1.0] * 10]))
        code, _ = V.decide(ten)
        assert code == V.PROMISING
        assert V.headline(code, ten, None, "") == ("Promising: +£1.00 a trade after costs over 10 trades - needs 20 more "
                                                   "trades and 4 more UK days to count as proven.")
        # enough trades and days, positive, but too thin to be proven: still PROMISING, and it says why
        solid = V.stats(made_up([[2.0, -1.8, 1.0]] * 10))
        assert solid["trades"] == 30 and solid["days"] == 10 and solid["profit_factor"] == 1.67
        assert V.decide(solid)[0] == V.PROVEN
        thin = V.stats(made_up([[2.0, -1.0, -0.9]] * 10))
        assert thin["profit_factor"] == 1.05 and V.decide(thin)[0] == V.PROMISING
        assert "to win £1.20 for every £1 lost (now £1.05)" in V.headline(V.PROMISING, thin, None, "")
        downs = V.stats(made_up([[5.0]] * 2 + [[-1.0]] * 3 + [[1.0] * 9] * 3))
        assert downs["up_days"] == 5 and downs["down_days"] == 3 and V.decide(downs)[0] == V.PROVEN
        downs = V.stats(made_up([[20.0]] * 2 + [[-1.0] * 9] * 3 + [[1.0]] * 1))
        assert downs["up_days"] == 3 and downs["down_days"] == 3 and V.decide(downs)[0] == V.PROMISING
        assert "more up days than down (now 3 up, 3 down)" in V.headline(V.PROMISING, downs, None, "")

    def test_no_edge_from_its_own_record(self):
        s = V.stats(made_up([[3.0, -4.0, -4.0]] * 10))
        assert s["profit_factor"] == 0.38
        code, why = V.decide(s)
        assert code == V.NO_EDGE and why == "over 30 trades it won back 38p for every £1 it lost"
        assert V.headline(code, s, None, "") == ("No edge: over 30 trades it won back 38p for every £1 it lost "
                                                 "(-£1.67 a trade).")
        assert V.VERDICTS[V.NO_EDGE] == ("BIN (OR BACK TO PAPER)", "amber")

    def test_never_a_verdict_from_fewer_trades_than_the_rule_allows(self):
        s = V.stats(made_up([[-1.0] * 29]))                             # 29 losers: one short of the rule
        assert V.decide(s)[0] == V.NOT_ENOUGH
        assert V.decide(V.stats(made_up([[-1.0] * 30])))[0] == V.NO_EDGE
        assert V.decide(V.stats(made_up([[5.0] * 29] * 1)))[0] == V.PROMISING   # never PROVEN under 30
        assert V.decide(V.stats([]))[0] == V.NOT_ENOUGH

    def test_break_even_and_the_boundary_at_0_9(self):
        s = V.stats(made_up([[1.0, -1.0, 0.8, -0.9, 0.9, -0.9]] * 5))
        assert s["profit_factor"] == 0.96 and s["trades"] == 30 and s["days"] == 5
        assert V.decide(s)[0] == V.BREAK_EVEN
        assert V.headline(V.BREAK_EVEN, s, None, "").startswith("Break-even: ")
        edge = V.stats(made_up([[0.9, -1.0]] * 15))                      # exactly 0.90: not NO EDGE
        assert edge["profit_factor"] == 0.9 and V.decide(edge)[0] == V.BREAK_EVEN

    def test_enough_trades_but_too_few_days_is_not_enough_data(self):
        s = V.stats(made_up([[1.0, -1.05] * 20] * 2))
        assert s["trades"] == 80 and s["days"] == 2 and s["profit_factor"] == 0.95
        assert V.decide(s)[0] == V.NOT_ENOUGH
        assert V.headline(V.NOT_ENOUGH, s, None, "") == ("Too early to tell: 80 trades, but over only 2 UK days - "
                                                         "it needs 5 days.")

    def test_its_own_research_on_real_prices_says_no_edge(self, tmp_path):
        research_file(tmp_path, "momentum_rider")
        r = V.read_research(str(tmp_path), "momentum_rider")
        assert r["code"] == V.NO_EDGE and r["trades"] == 1073 and r["days"] == 10 and r["versions_lost"] == 4
        assert r["source"] == "data/rider-research/2026-10-09/summary.json"
        few = V.stats(made_up([[5.0] * 12]))                            # its own record looks good...
        assert V.decide(few)[0] == V.PROMISING
        assert V.decide(few, r) == (V.NO_EDGE, "its own test on real past prices found no edge")   # ...research wins
        assert V.headline(V.NO_EDGE, few, r, "") == ("No edge: its own 10-day test on real past prices lost 1.33 pips "
                                                     "a trade over 1,073 trades.")      # never "lost -1.33"

    def test_synthetic_or_small_research_is_never_evidence(self, tmp_path):
        research_file(tmp_path, "momentum_rider", synthetic=True)
        r = V.read_research(str(tmp_path), "momentum_rider")
        assert r["synthetic"] and V.decide(V.stats([]), r)[0] == V.NOT_ENOUGH
        research_file(tmp_path / "b", "momentum_rider", trades=12)
        r = V.read_research(str(tmp_path / "b"), "momentum_rider")
        assert V.decide(V.stats([]), r)[0] == V.NOT_ENOUGH
        # the NEWEST run is the one read
        research_file(tmp_path / "c", "momentum_rider", code="PASSED", day="2026-10-01")
        research_file(tmp_path / "c", "momentum_rider", code="NO_EDGE", day="2026-10-09")
        assert V.read_research(str(tmp_path / "c"), "momentum_rider")["code"] == V.NO_EDGE
        research_file(tmp_path / "d", "financial_ian", code="NO EDGE", trades=40)
        ian = V.read_research(str(tmp_path / "d"), "financial_ian")
        assert ian["code"] == V.NO_EDGE and ian["trades"] == 40 and ian["unit"] == "£"
        assert ian["period"] == "2026-09-01 to 2026-09-06"

    def test_a_newer_synthetic_run_never_hides_the_real_research(self, tmp_path):
        research_file(tmp_path, "momentum_rider", code="NO_EDGE", day="2026-10-09")
        research_file(tmp_path, "momentum_rider", code="PASSED", day="2026-10-09-SYNTHETIC")     # its folder says so
        r = V.read_research(str(tmp_path), "momentum_rider")
        assert r["source"] == "data/rider-research/2026-10-09/summary.json" and not r["synthetic"]
        assert V.research_counts(r) and r["code"] == V.NO_EDGE
        research_file(tmp_path, "momentum_rider", code="PASSED", day="2026-10-10", synthetic=True)  # its summary says so
        assert V.read_research(str(tmp_path), "momentum_rider")["source"].endswith("/2026-10-09/summary.json")
        research_file(tmp_path / "only", "momentum_rider", code="PASSED", day="2026-10-09-SYNTHETIC")
        only = V.read_research(str(tmp_path / "only"), "momentum_rider")
        assert only["synthetic"] and not V.research_counts(only)                # no real run: shown, never used

    def test_ians_research_counts_only_on_the_market_it_trades_now(self, tmp_path):
        """MADE-UP: a no-edge test on CME futures, written the way Ian's research tool writes its summary."""
        out = tmp_path / "ian-research" / "2026-10-08"
        out.mkdir(parents=True)
        (out / "summary.json").write_text(json.dumps({
            "label": "MADE-UP", "synthetic": False, "days": 5, "period": {"first": "2026-09-28", "last": "2026-10-02"},
            "source": "Databento GLBX.MDP3 (CME Globex MDP 3.0), downloaded by this tool", "verdict": "NO EDGE",
            "verdict_reasons": ["made up"], "stats": {"trades": 40, "net_gbp": -40.0, "expectancy_gbp": -1.0,
                                                      "profit_factor": 0.7, "avg_win_gbp": 2.0, "avg_loss_gbp": -3.0}}))
        r = V.read_research(str(tmp_path), "financial_ian")
        assert r["period"] == "2026-09-28 to 2026-10-02" and r["market"] == "CME futures (Databento)"
        (tmp_path / "ian.json").write_text(json.dumps({"mode": "LIVE", "feed": {"vendor": "binance"}}))
        v = V.bot_verdict("financial_ian", "Financial Ian", "LIVE", 990_911, sel=None, tot=None,
                          data_dir=str(tmp_path), now=NOW)
        assert v["code"] != V.NO_EDGE                                           # a test of another market
        ev = next(e for e in v["evidence"] if e["label"] == "Its own research on real past prices")
        assert ev["facts"].endswith("Not used for the verdict: this test was on CME futures (Databento); it now "
                                    "trades Binance's public crypto book.")
        assert ev["period"] == "2026-09-28 to 2026-10-02"
        (tmp_path / "ian.json").write_text(json.dumps({"mode": "LIVE", "feed": {"vendor": "databento"}}))
        v = V.bot_verdict("financial_ian", "Financial Ian", "LIVE", 990_911, sel=None, tot=None,
                          data_dir=str(tmp_path), now=NOW)
        assert v["code"] == V.NO_EDGE                                           # the same market: it counts
        assert v["headline"] == "No edge: its own 5-day test on real past prices lost £1.00 a trade over 40 trades."

    def test_waiting_when_it_cannot_trade(self, tmp_path):
        (tmp_path / "ian-status.json").write_text(json.dumps({"feed": {"state": "NOT CONFIGURED"}}))
        blocked = V.cannot_trade(str(tmp_path), "financial_ian", "LIVE")
        assert blocked == "it has no market data feed"
        assert V.decide(V.stats([]), None, blocked) == (V.WAITING, "it has no market data feed")
        assert V.headline(V.WAITING, V.stats([]), None, blocked) == ("Waiting: it has no market data feed - nothing new "
                                                                     "to judge until it can trade.")
        assert V.decide(V.stats(made_up([[1.0] * 12])), None, blocked)[0] == V.PROMISING   # its record still speaks
        assert V.cannot_trade(str(tmp_path), "band_breaker", "OFF") == "it is switched OFF"
        (tmp_path / "crowd-status.json").write_text(json.dumps({"status": "NO KEY: no Coinversa key"}))
        assert V.cannot_trade(str(tmp_path), "crowd_fader", "PAPER") == "no Coinversa key is saved"
        assert V.cannot_trade(str(tmp_path / "missing"), "financial_ian", "LIVE") == ""   # a missing file says nothing

    def test_the_riders_day_reads_plainly(self):
        # MADE-UP: 62 trades, 21 won at 3.77 each, 41 lost at 3.16 each, 0.94 of costs each (58.28 in all)
        day = made_up([[3.77] * 21 + [-3.16] * 41], cost=0.94)
        s = V.stats(day)
        assert (s["trades"], s["wins"], s["avg_win"], s["avg_loss"], s["costs"]) == (62, 21, 3.77, 3.16, 58.28)
        assert s["net"] == -50.39 and s["before_costs"] == 7.89
        r = {"code": V.NO_EDGE, "trades": 1073, "days": 10, "unit": "pips", "expectancy": -1.33, "profit_factor": 0.46,
             "avg_win": 3.3, "avg_loss": 3.7, "best_move": 2.9, "versions": 6, "versions_lost": 6, "synthetic": False}
        txt = V.what_happened("LIVE", "today", s, V.stats([]), s, r)
        assert txt == ("62 trades today, 21 won, -£50.39 after costs. Wins averaged £3.77, losses £3.16, and costs took "
                       "£58.28 - more than the £7.89 it made before costs: the costs are bigger than its whole edge. "
                       "Its own 10-day test on real past prices (1,073 trades) found no edge: -1.33 pips a trade after "
                       "costs; profit factor 0.46; average win 3.3 pips against average loss 3.7 pips; the best move "
                       "after entry only 2.9 pips on average. Every version tested lost.")
        assert V.recommendation(V.NO_EDGE, "LIVE", "Rapid Momentum Rider", s, "").startswith(
            "Take it off live trading - put it back on PAPER (or bin it).")
        assert V.recommendation(V.NO_EDGE, "PAPER", "x", s, "").startswith("Bin it, or leave it on PAPER only")

    def test_a_paper_bot_says_practice_and_its_leftover_real_trade_says_so(self):
        prac = V.stats(made_up([[2.0, -1.0]], kind="practice"))
        assert V.what_happened("PAPER", "today", V.stats([]), prac, prac, None).startswith(
            "2 practice trades today, 1 won, +£1.00 after costs (practice, not money).")
        real = V.stats(made_up([[14.70]]))
        assert V.what_happened("PAPER", "today", real, V.stats([]), real, None) == (
            "1 real trade today, 1 won, +£14.70 after costs - left from its LIVE days. No losing trade; wins averaged "
            "£14.70, and costs took £0.50 of the £15.20 it made before costs.")

    def test_no_verdict_from_half_a_record_when_metatrader_could_not_be_read(self, tmp_path):
        v = V.bot_verdict("momentum_rider", "Rapid Momentum Rider", "LIVE", 990_811, sel=None, tot=None,
                          data_dir=str(tmp_path), now=NOW)
        assert v["code"] == V.NOT_ENOUGH and v["why"] == "MetaTrader's records could not be read just now"
        assert v["headline"].startswith("No verdict just now: MetaTrader's records could not be read")
        assert v["what_happened"].startswith("MetaTrader's records could not be read just now")
        research_file(tmp_path, "momentum_rider")                       # its research does not need MetaTrader
        v = V.bot_verdict("momentum_rider", "Rapid Momentum Rider", "LIVE", 990_811, sel=None, tot=None,
                          data_dir=str(tmp_path), now=NOW)
        assert v["code"] == V.NO_EDGE

    def test_the_rule_is_written_in_one_place(self):
        assert (V.MIN_TRADES, V.MIN_DAYS, V.PROVEN_PROFIT_FACTOR, V.NO_EDGE_PROFIT_FACTOR, V.MIN_TRADES_PROMISING) == \
            (30, 5, 1.2, 0.9, 10)
        assert set(V.VERDICTS) == {V.PROVEN, V.PROMISING, V.BREAK_EVEN, V.NO_EDGE, V.WAITING, V.NOT_ENOUGH}
        assert all(c not in ("green", "red") for _a, c in V.VERDICTS.values())     # green and red are for money
        assert "30+ trades" in V.RULE_TEXT and "5+ UK days" in V.RULE_TEXT


# ============================================== Trend & Breakout's journal --
class TestTrendAndBreakoutsHistory:
    def test_its_journal_before_the_reset_and_its_strongest_part(self, tmp_path):
        """MADE-UP journal rows: momentum continuation on FX minor pairs (the segment), a major-pair trade
        (not in it), one in a trend market (switched off, not in it), all before the reset."""
        import sqlite3
        from mintel.engine.journal import SCHEMA
        d = a_day(tmp_path)
        con = sqlite3.connect(d.data / "journal.sqlite")
        con.executescript(SCHEMA)
        rows = [(101, "GBPCHF", "MOMENTUM_CONTINUATION", "HIGH_VOL", t(2026, 9, 29, 10), 12.0),
                (102, "EURCAD", "MOMENTUM_CONTINUATION", "NEWS", t(2026, 9, 30, 10), -4.0),
                (103, "AUDJPY", "MOMENTUM_CONTINUATION_2X", "HIGH_VOL", t(2026, 10, 1, 10), 8.0),
                (104, "EURUSD", "MOMENTUM_CONTINUATION", "HIGH_VOL", t(2026, 10, 1, 11), -6.0),
                (105, "GBPCHF", "MOMENTUM_CONTINUATION", "TREND", t(2026, 10, 2, 10), -3.0),
                (106, "US500", "SESSION_EXPANSION", "TREND", t(2026, 10, 2, 11), 5.0)]
        for ticket, sym, tactic, regime, closed, pnl in rows:
            con.execute("INSERT INTO trades (ticket, symbol, tactic, regime, opened_utc, closed_utc, pnl_money, side, "
                        "entry, stop, exit_price, mode) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                        (ticket, sym, tactic, regime, (closed - dt.timedelta(minutes=30)).isoformat(),
                         closed.isoformat(), pnl, "BUY", 1.0, 0.99, 1.01, "DEMO"))
        con.commit()
        con.close()
        tot = period_view(d.sd, d.deals, "total", now=NOW)
        sel = period_view(d.sd, d.deals, "today", now=NOW)
        v = V.bot_verdict("market_intelligence", "Trend & Breakout", "PAPER", 990_311, sel=sel, tot=tot,
                          data_dir=str(d.data), now=NOW, since_plan=uk_midnight(dt.date(2026, 10, 9)))
        assert v["segment"]["trades"] == 3 and v["segment"]["net"] == 16.0          # 101, 102, 103 only
        assert v["judged"]["trades"] == 1                    # the 1 real trade since; its journal is evidence only
        labels = [e["label"] for e in v["evidence"]]
        assert "Before the account reset (real trades)" in labels
        seg = next(e for e in v["evidence"] if e["label"].startswith("The Formula 1 candidate"))
        assert seg["facts"].startswith("3 trades over 3 UK days (2 up, 1 down); +£16.00 without the commission "
                                       "charged when each trade opened")
        assert "after costs" not in seg["facts"] and "costs not in the record" not in seg["facts"]
        before = next(e for e in v["evidence"] if e["label"] == "Before the account reset (real trades)")
        assert "the commission charged when each trade opened is not in it" in before["source"]
        assert "not used for the verdict" in before["source"] and "after costs" not in before["facts"]
        assert v["forward"]["real"]["trades"] == 0 and v["forward"]["practice"]["trades"] == 0

    def test_its_journal_never_makes_the_verdict(self, tmp_path):
        """MADE-UP journal: 60 trades before the reset, booked at +12.00 each by their closing deals - money that
        leaves out the commission charged when each opened, so the broker's full figure could be negative. Only
        the fully-costed trades (the 1 real trade since the reset) are judged: never PROVEN or PROMISING from it."""
        import sqlite3
        from mintel.engine.journal import SCHEMA
        d = a_day(tmp_path)
        con = sqlite3.connect(d.data / "journal.sqlite")
        con.executescript(SCHEMA)
        for i in range(60):
            closed = t(2026, 9, 28, 9) + dt.timedelta(days=i // 10, minutes=i % 10)   # 28 Sep - 3 Oct
            con.execute("INSERT INTO trades (ticket, symbol, tactic, regime, opened_utc, closed_utc, pnl_money, side, "
                        "entry, stop, exit_price, mode) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                        (2000 + i, "EURUSD", "SESSION_EXPANSION", "TREND", (closed - dt.timedelta(minutes=5)).isoformat(),
                         closed.isoformat(), 12.0, "BUY", 1.0, 0.99, 1.01, "DEMO"))
        con.commit()
        con.close()
        v = V.bot_verdict("market_intelligence", "Trend & Breakout", "PAPER", 990_311,
                          sel=period_view(d.sd, d.deals, "today", now=NOW),
                          tot=period_view(d.sd, d.deals, "total", now=NOW), data_dir=str(d.data), now=NOW)
        assert v["code"] == V.NOT_ENOUGH and v["judged"]["trades"] == 1 and v["judged"]["journal"] == 0
        assert "earned a live trial" not in v["recommendation"]
        judged = next(e for e in v["evidence"] if e["label"].startswith("What the verdict is judged on"))
        assert "every trade whose full costs are on record" in judged["label"]
        assert "Its journal trades from before the reset are shown below but not judged" in judged["facts"]
        before = next(e for e in v["evidence"] if e["label"] == "Before the account reset (real trades)")
        assert before["facts"].startswith("60 trades over 6 UK days (6 up, 0 down); +£720.00 without the commission "
                                          "charged when each trade opened")
        assert "after costs" not in before["facts"]
        # what the rule would have said had the journal been judged: PROVEN - the reason it never is
        assert V.decide(V.stats(V.tnb_history(str(d.data), dt.datetime.fromisoformat(d.sd["start"]), {})
                                ["before_reset"]))[0] == V.PROVEN

    def test_its_recommendation_follows_what_the_record_says_of_the_formula_1_candidate(self):
        whole = V.stats(made_up([[1.0, -1.0, 0.8, -0.9, 0.9, -0.9]] * 5))           # MADE-UP: break-even overall
        pays = V.stats(made_up([[12.0], [-4.0], [8.0]]))
        loses = V.stats(made_up([[3.0], [-9.0]]))
        assert V.recommendation(V.BREAK_EVEN, "PAPER", "Trend & Breakout", whole, "", "market_intelligence", pays) == (
            "Keep it on PAPER: as a whole it does not pay - look inside it for the part that does. Its momentum "
            "continuation on FX minor pairs, in fast and news markets is the part that pays on its record (+£16.00 "
            "over 3 real trades) - that is Formula 1.")
        assert V.recommendation(V.BREAK_EVEN, "PAPER", "Trend & Breakout", whole, "", "market_intelligence",
                                loses).endswith("(the Formula 1 candidate) is not paying on its record either "
                                                "(-£6.00 over 2 real trades).")
        assert "Formula 1" not in V.recommendation(V.BREAK_EVEN, "PAPER", "Band Breaker", whole, "", "band_breaker", pays)


# =================================================================== the plan --
class TestThePlan:
    def test_the_shipped_plan_is_whole(self):
        plan, err = V.load_plan()
        assert err == "" and plan["aim"] == "Find one winning formula, prove it, grow it - then build the next."
        assert plan["updated"] == "2026-10-09"
        assert plan["focus"]["name"] == "Formula 1" and plan["focus"]["bot"] == "market_intelligence"
        assert "must" in plan["focus"]["caution"] or "prove itself" in plan["focus"]["caution"]
        steps = [s["text"] for s in plan["steps"]]
        assert len(steps) == 5 and steps[0].startswith("Rapid Momentum Rider back to PAPER")
        assert "20 live trades" in steps[2] and "30 trades" in steps[3]
        ids = {"market_intelligence", "momentum_rider", "momentum_runner", "band_breaker", "crowd_fader",
               "financial_ian"}
        for bucket in ("bin", "change", "keep"):
            assert plan[bucket], bucket
            for item in plan[bucket]:
                assert item["text"] and item["why"], item
                assert item.get("bot") is None or item["bot"] in ids
                assert item.get("stance") is None or item["stance"] in V.PLAN_STANCES
        assert V.next_steps(plan) == steps[:3]

    def test_the_fx_majors_item_quotes_what_version_5_switched_off(self):
        """docs/strategies/v2-commission-first.md: version 5 switched off momentum continuation on the FX majors
        (17 trades, -13.87 after 17.48 commission); -83.73 over 74 is every approach on the majors together."""
        plan, _ = V.load_plan()
        item = next(i for i in plan["bin"] if "FX major" in i["text"])
        assert item["text"] == "Trend & Breakout's momentum continuation on FX major pairs"
        assert item["why"].startswith("17 real trades, -£13.87 after £17.48 of commission, down on 5 of 8 days - "
                                      "switched off since version 5.")
        assert "(Every Trend & Breakout approach on the FX majors together: -£84 over 74 real trades.)" in item["why"]
        doc = (Path(V.PLAN_FILE).parent / "strategies" / "v2-commission-first.md").read_text()
        assert "17 trades, -13.87 after 17.48 commission" in doc and "| FX majors | 74 | 49% | -83.73 |" in doc

    def test_the_plan_renders_on_the_main_page(self, tmp_path):
        d = a_day(tmp_path)
        httpd, get = serve(d)
        try:
            page = get("/")
        finally:
            httpd.shutdown()
        plan, _ = V.load_plan()
        raw = page.split("<h2>The plan</h2>")[1].split("</section>")[0]
        box = text_of(raw)
        assert plan["aim"] in box and "The focus now" in box and "Formula 1" in box
        for s in plan["steps"][:3]:
            assert s["text"] in box
        assert plan["steps"][3]["text"] not in box                       # the next three, no more
        for title in ("Bin", "Change", "Keep"):
            assert f"<h3>{title}</h3>" in raw
        for item in plan["bin"] + plan["change"] + plan["keep"]:
            assert item["text"] in box and item["why"] in box
        assert "Going forward since 09 Oct: no trades in it yet" in box
        assert "The plan is docs/plan.json, updated 09 Oct 2026." in box
        assert "Its figures come from: " + "; ".join(plan["sources"]) in box
        assert "Check:" not in box                                       # nothing disagrees on this made-up day

    def test_the_bots_come_first_and_the_plans_reasons_fold_away(self, tmp_path):
        """One screen: the account, then the bots, then the plan - its aim, focus and next steps in view, each
        Bin/Change/Keep item one line with its reason folded under it, and its sources folded at the end."""
        from html import escape
        from mintel.ops.dashboard import KEEP_OPEN_JS
        d = a_day(tmp_path)
        httpd, get = serve(d)
        try:
            page = get("/")
        finally:
            httpd.shutdown()
        plan, _ = V.load_plan()
        assert page.index('<div class="s-acct">') < page.index("<h2>The bots") < page.index("<h2>The plan</h2>")
        raw = page.split("<h2>The plan</h2>")[1].split("</section>")[0]
        for bucket in ("bin", "change", "keep"):
            for k, item in enumerate(plan[bucket]):
                fold = raw.split(f'<details id="plan-{bucket}-{k}"><summary>')[1].split("</details>")[0]
                assert f"<b>{escape(item['text'], quote=True)}</b></summary>" in fold, (bucket, k)
                assert escape(item["why"], quote=True) in fold, (bucket, k)
        sources = raw.split('<details class="s-more" id="plan-sources">')[1].split("</details>")[0]
        assert "The plan is docs/plan.json" in sources and "Its figures come from: " in sources
        for s_ in plan["steps"][:3]:
            assert escape(s_["text"], quote=True) in raw.split("<details")[0]      # the next steps stay in view
        assert KEEP_OPEN_JS in page                                      # an opened reason survives the refresh

    def test_a_bot_whose_record_now_disagrees_is_flagged_next_to_the_plan(self, tmp_path):
        d = a_day(tmp_path)
        research_file(d.data, "financial_ian", code="NO EDGE", trades=40)   # MADE-UP research: no edge
        httpd, get = serve(d)
        try:
            page, ian = get("/"), get("/?bot=financial_ian")
        finally:
            httpd.shutdown()
        flag = "The plan says keep testing Financial Ian, but its record now says NO EDGE."
        box = text_of(page.split("<h2>The plan</h2>")[1].split("</section>")[0])
        assert f"Check: {flag}" in box
        assert flag in unescape(ian)
        assert '<span class="s-chip amber">NO EDGE</span>' in row_of(page, "Financial Ian")

    def test_disagreement_wording_for_each_stance(self):
        plan = {"bin": [{"bot": "momentum_rider", "stance": "bin", "says": "take {name} off live trading", "text": "x"}],
                "keep": [{"bot": "crowd_fader", "stance": "test", "text": "y"},
                         {"bot": "band_breaker", "stance": "grow", "text": "z"}]}
        vs = {"momentum_rider": {"label": "Rapid Momentum Rider", "code": V.PROMISING},
              "crowd_fader": {"label": "Crowd Fader", "code": V.PROVEN},
              "band_breaker": {"label": "Band Breaker", "code": V.BREAK_EVEN}}
        assert [x["text"] for x in V.plan_disagreements(plan, vs)] == [
            "The plan says take Rapid Momentum Rider off live trading, but its record now says PROMISING.",
            "The plan says keep testing Crowd Fader, but its record now says PROVEN.",
            "The plan says keep and grow Band Breaker, but its record now says BREAK-EVEN."]
        vs = {"momentum_rider": {"label": "Rapid Momentum Rider", "code": V.NO_EDGE},
              "crowd_fader": {"label": "Crowd Fader", "code": V.NOT_ENOUGH},
              "band_breaker": {"label": "Band Breaker", "code": V.PROVEN}}
        assert V.plan_disagreements(plan, vs) == []                      # each as the plan expects

    def test_the_mac_installer_copies_the_plan(self, tmp_path):
        """deploy/mac/mintel_mac.sh's copy_program, run for real on a MADE-UP source folder: the docs folder (and
        docs/plan.json with it) reaches the Mac's app folder, as on Windows (SETUP-AND-START.ps1)."""
        import os
        import subprocess
        src, home = tmp_path / "src", tmp_path / "MarketBot"
        (src / "docs").mkdir(parents=True)
        (src / "mintel").mkdir()
        (src / "mintel" / "x.py").write_text("X = 1\n")
        (src / "docs" / "plan.json").write_text('{"aim": "MADE-UP"}')
        (home / "app").mkdir(parents=True)
        lib = Path(__file__).resolve().parent.parent / "deploy" / "mac" / "mintel_mac.sh"
        r = subprocess.run(["bash", "-c", f'set -uo pipefail; source "{lib}"; copy_program "{src}"'],
                           capture_output=True, text=True, timeout=120, env={**os.environ, "MINTEL_HOME": str(home)})
        assert r.returncode == 0, r.stdout + r.stderr
        assert (home / "app" / "docs" / "plan.json").read_text() == '{"aim": "MADE-UP"}'
        assert (home / "app" / "mintel" / "x.py").exists()

    def test_a_missing_or_broken_plan_is_said_not_guessed(self, tmp_path):
        plan, err = V.load_plan(tmp_path / "plan.json")
        assert plan is None and "not on this computer yet" in err
        (tmp_path / "plan.json").write_text("{not json")
        plan, err = V.load_plan(tmp_path / "plan.json")
        assert plan is None and "could not be read" in err
        page = render_home({"standing": {}}, None, {}, None, err)
        assert "No plan to show: the plan file could not be read" in page


# =================================================== one bot, in detail --
class TestOneBotInDetail:
    def test_what_happened_the_recommendation_and_the_evidence_sit_above_its_existing_detail(self, tmp_path):
        d = a_day(tmp_path)
        research_file(d.data, "momentum_rider")                          # MADE-UP research: no edge
        httpd, get = serve(d)
        try:
            page, home = get("/?bot=momentum_rider"), get("/")
        finally:
            httpd.shutdown()
        txt = text_of(page)
        assert '<span class="s-chip amber">NO EDGE</span>' in page and "Verdict: NO EDGE - BIN (OR BACK TO PAPER)" in txt
        assert "What happened - Today" in txt
        assert ("2 trades today, 1 won, +£7.13 after costs. Wins averaged £10.17, losses £3.04, and costs took £0.18 "
                "of the £7.31 it made before costs.") in txt
        assert "Recommendation Take it off live trading - put it back on PAPER (or bin it)." in txt
        assert "Source: MetaTrader's records for magic number 990811: each closed trade in full, after commission" in txt
        assert "Source: data/rider-research/2026-10-09/summary.json" in txt
        assert "The rule: NO EDGE:" in txt
        # then its existing detail: the card with its "Now:" line, its trades, its open trades, its live view
        box_end = page.index("<h2>Its detail</h2>")
        assert page.index("What happened") < box_end < page.index(NOW_TAG)
        assert "Rapid Momentum Rider - trades closed today - 3" in page[box_end:]
        assert "Rapid Momentum Rider - open trades right now - 1 open" in page[box_end:]
        assert "<th>Market</th><th>Direction</th><th>Score</th><th>State</th><th>Reason</th>" in page[box_end:]
        row = row_of(home, "Rapid Momentum Rider")
        assert '<span class="s-chip amber">NO EDGE</span>' in row
        assert "No edge: its own 10-day test on real past prices lost 1.33 pips a trade over 1,073 trades." in row
        assert ("<b>What to do:</b> Take it off live trading - put it back on PAPER (or bin it).") in row

    def test_every_row_on_the_main_page_says_what_to_do(self, tmp_path):
        d = a_day(tmp_path)
        research_file(d.data, "momentum_rider")                          # MADE-UP research: no edge
        httpd, get = serve(d)
        try:
            page = get("/")
        finally:
            httpd.shutdown()
        tot = period_view(d.sd, d.deals, "total", now=NOW)
        sel = period_view(d.sd, d.deals, "today", now=NOW)
        vs = V.all_verdicts(d.sd, sel, tot, str(d.data), NOW, V.load_plan()[0], deals=d.deals)
        for bid, v in vs.items():
            row = row_of(page, v["label"].replace("&", "&amp;"))
            assert v["recommendation"] and (f'<div class="s-rec"><b>What to do:</b> '
                                             f'{v["recommendation"].replace("&", "&amp;")}</div>') in row, bid
        assert "<b>What to do:</b> Take it off live trading" in row_of(page, "Rapid Momentum Rider")

    def test_on_a_phone_each_wide_table_scrolls_by_itself(self, tmp_path):
        """A bot's old detail (its trades, its scanner) keeps its tables; on a 390px phone each scrolls sideways on
        its own, so the cards and text around them stay the width of the screen."""
        d = a_day(tmp_path)
        httpd, get = serve(d)
        try:
            page = get("/?bot=momentum_rider")
        finally:
            httpd.shutdown()
        assert ".s-old table{display:block;max-width:100%;overflow-x:auto}" in page
        assert '<div class="s-old">' in page and "<table" in page.split('<div class="s-old">')[1]

    def test_the_main_page_has_nothing_else(self, tmp_path):
        d = a_day(tmp_path)
        httpd, get = serve(d, thinking=[{"rank": 1, "symbol": "US500", "direction": "LONG", "score": 71.0,
                                         "tier": "NORMAL", "tactic": "MOMENTUM_CONTINUATION", "regime": "TREND"}])
        try:
            page = get("/")
        finally:
            httpd.shutdown()
        for absent in (NOW_TAG, 'class="quiet"', "What the bot is looking at", 'class="tile"', "<table",
                       'id="more"', "BOT: RUNNING", '<div class="hc'):
            assert absent not in page, absent
        assert '<meta name="viewport" content="width=device-width,initial-scale=1">' in page
        assert "background:#fff" in page                                  # white, calm
        assert '<a href="/details?period=today">All the technical detail</a>' in page
        for link in ('href="/rider"', 'href="/ian"', 'href="/health"'):
            assert link in page

    def test_the_quiet_banner_is_on_a_live_bots_view_and_details_only(self, tmp_path):
        d = quiet_day(tmp_path)
        httpd, get = serve(d, LATER)
        try:
            home, rider, band, details = get("/"), get("/?bot=momentum_rider"), get("/?bot=band_breaker"), get("/details")
        finally:
            httpd.shutdown()
        assert '<div class="quiet">' not in home and '<div class="quiet">' not in band
        assert '<div class="quiet">' in rider and '<div class="quiet">' in details
        assert NOW_TAG in rider and NOW_TAG in band and NOW_TAG not in home


# ======================================================= /details: the old page --
class TestDetailsIsTheFullPage:
    def test_details_serves_the_whole_old_page_and_its_links_stay_there(self, tmp_path):
        d = a_day(tmp_path)
        httpd, get = serve(d)
        try:
            page, crowd = get("/details?period=yesterday"), get("/details?bot=crowd_fader")
        finally:
            httpd.shutdown()
        assert "BOT: RUNNING - everything is healthy" in page and '<div class="hc acct">' in page
        assert 'id="more"' in page and NOW_TAG in page and '<div class="botbar">' in page
        assert '<div class="fk">Yesterday</div><div class="fv bad">-£9.36</div>' in page
        assert 'class="tab p on" href="/details?period=yesterday"' in page
        assert 'action="/details"' in page and 'href="/?' not in page
        assert '<div class="cards one">' in crowd and "Crowd Fader <span" in crowd


# ===================================================== missing files, escaping --
class TestItAlwaysRenders:
    BOTS = ("market_intelligence", "momentum_rider", "momentum_runner", "band_breaker", "crowd_fader",
            "financial_ian", "rapid_scalper", "nonsense<script>")

    def test_with_no_status_files_or_records_at_all(self, tmp_path):
        d = a_day(tmp_path, records=False)
        httpd, get = serve(d)
        try:
            for period in ("today", "yesterday", "total", "custom&from=2026-10-08&to=2026-10-09"):
                page = get(f"/?period={period}")
                assert "dashboard error" not in page and '<h1>Trading bots</h1>' in page, period
                for bot in self.BOTS:
                    page = get(f"/?bot={bot}&period={period}")
                    assert "dashboard error" not in page and "<script>alert" not in page, (bot, period)
        finally:
            httpd.shutdown()

    def test_with_no_data_folder_and_no_metatrader(self, tmp_path):
        state = DashboardState()
        state.update(status={"bot": "RUNNING"}, health={}, data_dir="")
        httpd = start_dashboard(state, "127.0.0.1", 0)
        try:
            import urllib.request
            port = httpd.server_address[1]
            for path in ["/", "/?period=total"] + [f"/?bot={b}" for b in self.BOTS]:
                page = urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=20).read().decode()
                assert "dashboard error" not in page, path
        finally:
            httpd.shutdown()

    def test_a_broken_research_file_is_said(self, tmp_path):
        d = a_day(tmp_path)
        out = d.data / "rider-research" / "2026-10-09"
        out.mkdir(parents=True)
        (out / "summary.json").write_text("{broken")
        v = V.bot_verdict("momentum_rider", "Rapid Momentum Rider", "LIVE", 990_811,
                          sel=period_view(d.sd, d.deals, "today", now=NOW),
                          tot=period_view(d.sd, d.deals, "total", now=NOW), data_dir=str(d.data), now=NOW)
        ev = next(e for e in v["evidence"] if e["label"] == "Its own research on real past prices")
        assert ev["facts"] == "the summary could not be read" and v["code"] == V.NOT_ENOUGH


class TestEverythingIsEscaped:
    def test_plan_research_and_status_text_are_escaped(self, tmp_path, monkeypatch):
        bad = "<script>alert(1)</script>"
        plan = {"aim": f"aim {bad}", "updated": "2026-10-09", "written_by": bad,
                "focus": {"name": bad, "what": bad, "evidence": bad, "caution": bad, "bot": "market_intelligence"},
                "steps": [{"text": f"step {bad}"}], "bin": [{"text": bad, "why": bad}],
                "change": [{"text": bad, "why": bad}],
                "keep": [{"bot": "financial_ian", "stance": "test", "says": "<b>{name}</b>", "text": bad, "why": bad}]}
        (tmp_path / "plan.json").write_text(json.dumps(plan))
        monkeypatch.setattr(V, "PLAN_FILE", tmp_path / "plan.json")
        d = a_day(tmp_path / "day")
        research_file(d.data, "financial_ian", code="NO EDGE", trades=40)   # flags the keep item, with its "<b>"
        httpd, get = serve(d)
        try:
            pages = [get("/"), get("/?bot=financial_ian"), get("/?bot=momentum_rider")]
        finally:
            httpd.shutdown()
        for page in pages:
            assert "<script>alert" not in page and "<b>Financial Ian</b>" not in page
        assert "&lt;script&gt;alert(1)&lt;/script&gt;" in pages[0]
        assert "The plan says &lt;b&gt;Financial Ian&lt;/b&gt;, but its record now says NO EDGE." in pages[0]
        assert "&lt;b&gt;breaking&lt;/b&gt;" in pages[2] and "<b>breaking</b>" not in pages[2]   # the Rider's scanner


# ============================================== the refresh keeps the choice --
class TestTheRefreshKeepsTheChoice:
    def test_period_and_bot_stay_chosen_and_a_date_being_typed_is_never_wiped(self, tmp_path):
        d = a_day(tmp_path)
        httpd, get = serve(d)
        try:
            week, crowd, custom = (get("/?period=week"), get("/?period=yesterday&bot=crowd_fader"),
                                   get("/?period=custom&from=2026-10-08&to=2026-10-08&bot=crowd_fader"))
        finally:
            httpd.shutdown()
        for page in (week, crowd, custom):
            assert REFRESH_JS in page and "location.reload()" in page          # the same address, query and all
            assert '<form class="dates" method="get" action="/">' in page      # never wiped while typing a date
            assert 'http-equiv="refresh" content="10;' not in page             # no refresh that drops the query
        assert 'class="s-p on" href="/?period=week"' in week
        assert '<a class="s-bot" href="/?period=week&bot=band_breaker">' in week     # rows keep the period
        assert 'class="s-p on" href="/?period=yesterday&bot=crowd_fader"' in crowd   # buttons keep the bot
        assert 'href="/?period=today&bot=crowd_fader"' in crowd
        assert '<input type="hidden" name="bot" value="crowd_fader">' in crowd
        assert '<a href="/?period=yesterday">&larr; All bots</a>' in crowd
        assert '<a href="/?period=custom&from=2026-10-08&to=2026-10-08">&larr; All bots</a>' in custom
        assert 'name="from" value="2026-10-08"' in custom and 'name="to" value="2026-10-08"' in custom
        assert '<button type="submit" class=on>Show</button>' in custom
        assert '<a href="/details?period=week">All the technical detail</a>' in week
