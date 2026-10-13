"""The coach on the page (spec section 15, with section 21's demo line).

* "/" gets one card, directly after the lab's "Today's standouts", in plain
  words; the lab's "/" pins hold (no <table, no class="tile", the order kept)
  and the account's figures are the same with and without the coach;
* /coach holds the full record (tables only inside .s-old);
* progress() is standing.period_view custom from 12 Oct, the page's own
  arithmetic, with no pace line before the 5th trading day, and it is in /api
  as coach_progress;
* the empty states say so plainly: not switched on, switched off, a broken
  file, watching only, not reporting, MetaTrader unreadable; a made-up status
  shows "TEST DATA - not a result" and no pound sign.

EVERY FIGURE HERE IS MADE-UP TEST DATA: tests/fixtures/coach_status_example.json
(written by hand, "synthetic": true) and the fake broker of
tests/test_page_figures.py. Tests that need the ordinary rendering clear the
fixture's flag IN MEMORY ONLY - its figures stay made up.
"""
from __future__ import annotations

import datetime as dt
import json
import re
from html import unescape
from pathlib import Path

from mintel.ops import coach_page as C
from mintel.ops.standing import period_view
from tests.test_page_figures import a_day, deal, serve, t
from tests.test_simple_dashboard import account_section, text_of

# 13 Oct (the TRADING SQUAD redesign): "/" is the new squad page (mintel/ops/squad.py). The old Home these tests
# pin - the account cards, the bot tiles, the lab and coach lines - is kept, unchanged, at /advanced (ADVANCED >
# Overview); "/?period=", "/?bot=" and "/?show=" still open the old pages. So "/" below reads "/advanced".

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "coach_status_example.json"
UTC = dt.timezone.utc
FRI = t(2026, 10, 16, 9, 15)                       # Fri 16 Oct, 10:15 UK: the 5th trading day since 12 Oct
THU = t(2026, 10, 15, 9, 15)                       # Thu 15 Oct: the 4th
TNB, RUNNER, IAN = 990_311, 990_511, 990_911
BAD = "<script>alert(1)</script>"

# MADE-UP real trades: one before the count starts, one a day from Mon 12 Oct
COACH_DEALS = [
    deal(9_001, TNB, -0.30, -0.30, True, t(2026, 10, 9, 8, 0), "EURGBP"),
    deal(9_001, TNB, 6.00, -0.30, False, t(2026, 10, 9, 9, 0), "EURGBP"),
    deal(9_002, TNB, -0.30, -0.30, True, t(2026, 10, 12, 8, 0), "EURGBP"),
    deal(9_002, TNB, 10.30, -0.30, False, t(2026, 10, 12, 9, 0), "EURGBP"),
    deal(9_003, RUNNER, -0.10, -0.10, True, t(2026, 10, 13, 14, 0), "US500"),
    deal(9_003, RUNNER, -4.90, -0.10, False, t(2026, 10, 13, 15, 0), "US500"),
    deal(9_004, TNB, -0.30, -0.30, True, t(2026, 10, 14, 8, 0), "GBPCAD"),
    deal(9_004, TNB, 12.40, -0.30, False, t(2026, 10, 14, 10, 0), "GBPCAD"),
    deal(9_005, TNB, -0.30, -0.30, True, t(2026, 10, 15, 8, 0), "EURNZD"),
    deal(9_005, TNB, 6.20, -0.30, False, t(2026, 10, 15, 9, 0), "EURNZD"),
    deal(9_006, TNB, -0.30, -0.30, True, t(2026, 10, 16, 7, 0), "AUDNZD"),
    deal(9_006, TNB, -0.40, -0.30, False, t(2026, 10, 16, 8, 0), "AUDNZD"),
]
SINCE_12 = round(10.00 - 5.00 + 12.10 + 5.90 - 0.70, 2)      # 22.30: the five days, each trade in full


# ------------------------------------------------------------------ helpers --
def made_up_status(test_data: bool = False, **changes) -> dict:
    """The MADE-UP status; ``test_data`` False clears its flag in memory only (the figures stay made up)."""
    st = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert st["synthetic"] is True                       # the file itself can never be mistaken for a result
    st["synthetic"] = test_data
    st.update(changes)
    return st


def put_coach(data: Path, status=None, raw_status: str | None = None, envelope=None, raw_envelope: str | None = None,
              off: bool = False) -> None:
    data.mkdir(parents=True, exist_ok=True)
    if raw_envelope is not None:
        (data / "coach.json").write_text(raw_envelope)
    else:
        env = {"schema": 1, "mode": "PILOT", "watch_until_utc": "2026-10-13T23:00:00+00:00"}
        env.update(envelope or {})
        (data / "coach.json").write_text(json.dumps(env))
    (data / "coach").mkdir(exist_ok=True)
    if raw_status is not None:
        (data / "coach" / "status.json").write_text(raw_status)
    elif status is not None:
        (data / "coach" / "status.json").write_text(json.dumps(status))
    if off:
        (data / "COACH-OFF").write_text("made up")


def day(tmp_path, now=FRI, deals=COACH_DEALS):
    return a_day(tmp_path, records=False, deals=deals, positions=[], now=now)


def card_of(page: str) -> str:
    return page.split('<section class="s-card s-coach">')[1].split("</section>")[0]


def lines(card: str) -> str:
    return text_of(card)


def pages(d, now, *paths):
    httpd, get = serve(d, now=now)
    try:
        return [get(p) for p in paths]
    finally:
        httpd.shutdown()


# ================================================================= progress --
class TestProgress:
    def test_progress_is_period_view_custom_from_12_oct(self, tmp_path):
        d = day(tmp_path)
        p = C.progress(d.sd, d.deals, FRI)
        v = period_view(d.sd, d.deals, "custom", "2026-10-12", "2026-10-16", now=FRI)
        assert p["ok"] and p["total"] == v["trading"] == SINCE_12
        assert p["from"] == "2026-10-12" and p["to"] == "2026-10-16" and p["trades"] == 5
        assert [r["net"] for r in p["days"]] == [10.00, -5.00, 12.10, 5.90, -0.70]
        assert round(sum(r["net"] for r in p["days"]), 2) == p["total"]       # the 9 Oct trade never counts
        assert p["trading_days"] == 5 and p["days_left"] == 10 and p["pace"] is True
        assert p["a_day"] == round(SINCE_12 / 5, 2)
        assert p["need_a_day"] == round((100 * 15 - SINCE_12) / 10, 2)

    def test_no_pace_line_before_the_fifth_trading_day(self, tmp_path):
        d = day(tmp_path, now=THU)
        p = C.progress(d.sd, d.deals, THU)
        assert p["trading_days"] == 4 and p["pace"] is False and p["a_day"] is None and p["need_a_day"] is None
        words = C.progress_words(p)
        assert words.endswith("over 4 trading days. Too early to say a pace: 4 trading days so far.")
        assert " a day." not in words.split("Against £100 a day:")[1] and "need" not in words
        five = C.progress_words(C.progress(day(tmp_path / "f").sd, day(tmp_path / "f").deals, FRI))
        assert five == ("Against £100 a day: real money since 12 Oct: +£22.30 over 5 trading days, +£4.46 a day. "
                        "To average £100 a day by Fri 30 Oct, the 10 trading days left need £147.77 a day.")

    def test_metatrader_unreadable_cannot_be_counted(self):
        for sd in (None, {}, {"start": "2026-10-05T12:30:00+00:00", "made": None},
                   {"start": "2026-10-05T12:30:00+00:00", "made": 1.0, "error": "no deals"}):
            p = C.progress(sd, [], FRI)
            assert p["ok"] is False and p["total"] is None
            assert C.progress_words(p) == "Against £100 a day: real money since 12 Oct cannot be counted just now."

    def test_before_12_oct_and_after_30_oct(self, tmp_path):
        early = t(2026, 10, 11, 12, 0)
        p = C.progress(day(tmp_path, now=early).sd, [], early)
        assert p["trading_days"] == 0 and "the count starts on Mon 12 Oct" in C.progress_words(p)
        late = t(2026, 11, 2, 12, 0)
        d = day(tmp_path / "l", now=late)
        p = C.progress(d.sd, d.deals, late)
        assert p["trading_days"] == 15 and p["days_left"] == 0 and p["need_a_day"] is None
        assert "The three weeks to Fri 30 Oct are over." in C.progress_words(p)

    def test_the_api_carries_coach_progress(self, tmp_path):
        d = day(tmp_path)
        (api,) = pages(d, FRI, "/api")
        got = json.loads(api)["coach_progress"]
        want = json.loads(json.dumps(C.progress(d.sd, d.deals, FRI), default=str))
        assert got == want and got["total"] == SINCE_12


# ===================================================================== card --
class TestTheCard:
    def test_the_cards_order_and_wording(self, tmp_path):
        d = day(tmp_path)
        put_coach(d.data, made_up_status(written_utc="2026-10-16T09:14:20+00:00"))
        card = C.render_card(str(d.data), d.sd, d.deals, FRI)
        txt = lines(card)
        order = ["The coach's call - Fri 16 Oct, 10:15 · in charge within your limits · checked 40 seconds ago",
                 "Real money now",
                 "Formula 1 (momentum trades on minor currency pairs): £13.00 a trade - your choice (10 Oct) until its "
                 "20-live-trade judgement. The coach leaves it alone until then.",
                 "Momentum Runner: £10.00 a trade, your choice until it is judged at 30 real trades - waiting for New "
                 "York (14:30 UK).",
                 "Financial Ian: £10.00 a trade",
                 "Practising, no money: Trend & Breakout: breakout acceptance on major pairs, Band Breaker: on "
                 "indices. They keep practising - that is how they earn a place.",
                 "Markets now (London morning): FX minors: 5 of 14 in a fast market.",
                 "Changed today: 09:42 Financial Ian paused until midnight: the account is down £42.10 today",
                 "Demo mode: limits are lifted step by step as the bots earn it (your instruction of 12 Oct). Every "
                 "trade still has a stop. Coach-sized trades today +£64.00; at real-money sizes +£19.00.",
                 "Against £100 a day: real money since 12 Oct: +£22.30 over 5 trading days, +£4.46 a day.",
                 "Source: MetaTrader's record, every bot, after commission",
                 "Honest outlook: not estimated yet",
                 "Needs your yes: (1) Formula 1's judgement after 20 live trades (14 to go). (2) Let the 3% stop also "
                 "count Financial Ian",
                 "(4) Lab ideas ready for live: none yet.",
                 'Stop the coach: double-click "The Coach" on the desktop and choose 2 - everything goes back to '
                 "your own settings at once."]
        at = [txt.index(x) for x in order]
        assert at == sorted(at)
        assert "Announced for 06:30" not in txt                       # yesterday's change: not "today"
        assert '<a href="/coach">The coach in full</a>' in card

    def test_every_money_figure_has_a_source(self, tmp_path):
        d = day(tmp_path)
        put_coach(d.data, made_up_status())
        txt = lines(C.render_card(str(d.data), d.sd, d.deals, FRI))
        # the one real figure, then its source at once
        prog = txt.split("Against £100 a day:")[1]
        assert prog.split("Source:")[0].count("£") == 4
        assert prog.split("Source:")[1].startswith(" MetaTrader's record, every bot, after commission")
        # the account's share of the aim is from MetaTrader's equity, and says so
        assert re.search(r"£100 a day is \d+\.\d% of the account \(MetaTrader's equity\) every day", txt)
        # the demo figures are the ledger's real-money shadow, named as such
        assert "Coach-sized trades today +£64.00; at real-money sizes +£19.00." in txt

    def test_the_outlook_is_always_an_estimate_not_a_result(self, tmp_path):
        d = day(tmp_path)
        put_coach(d.data, made_up_status())
        txt = lines(C.render_card(str(d.data), d.sd, d.deals, FRI))
        assert "Honest outlook: not estimated yet (the nightly review works it out) - when it comes it is an " \
               "estimate, not a result." in txt
        put_coach(d.data, made_up_status(outlook={"lo": 3.0, "hi": 15.0}))
        txt = lines(C.render_card(str(d.data), d.sd, d.deals, FRI))
        assert "Honest outlook: about £3.00 to £15.00 a day on the evidence so far - an estimate, not a result." in txt
        assert "The coach never sizes up to catch up." in txt
        assert "22.30" in txt and "40.30" not in txt                  # the estimate is never added to the total

    def test_a_demo_day_with_no_coach_sized_trade_says_so(self, tmp_path):
        d = day(tmp_path)
        line = ("Demo mode: limits are lifted step by step as the bots earn it (your instruction of 12 Oct). Every "
                "trade still has a stop.")
        put_coach(d.data, made_up_status(demo_line=line))
        txt = lines(C.render_card(str(d.data), d.sd, d.deals, FRI))
        assert line + " No trade was sized by the coach today, so there is no real-money comparison yet." in txt
        put_coach(d.data, made_up_status(demo_line="Real-money limits in force (the broker reports a real account)."))
        txt = lines(C.render_card(str(d.data), d.sd, d.deals, FRI))
        assert "Real-money limits in force (the broker reports a real account)." in txt and "Demo mode" not in txt

    def test_a_pace_line_never_before_the_fifth_trading_day(self, tmp_path):
        d = day(tmp_path, now=THU)
        put_coach(d.data, made_up_status(written_utc=THU.isoformat()))
        txt = lines(C.render_card(str(d.data), d.sd, d.deals, THU))
        assert "Too early to say a pace: 4 trading days so far." in txt
        assert "need" not in txt.split("Against £100 a day:")[1].split("Source:")[0]

    def test_metatrader_unreadable(self, tmp_path):
        d = day(tmp_path)
        put_coach(d.data, made_up_status())
        txt = lines(C.render_card(str(d.data), {}, [], FRI))
        assert "Against £100 a day: real money since 12 Oct cannot be counted just now." in txt


# ============================================================ empty states --
class TestEmptyStates:
    def test_not_switched_on(self, tmp_path):
        d = day(tmp_path)
        txt = lines(C.render_card(str(d.data), d.sd, d.deals, FRI))
        assert "The coach is not switched on yet." in txt and "Stop the coach" not in txt
        assert "Every bot is on your own settings." in txt
        assert "Against £100 a day: real money since 12 Oct: +£22.30" in txt     # the account's figure still shows
        assert "The coach is not switched on yet." in lines(C.render_card("", None, None, FRI))

    def test_switched_off(self, tmp_path):
        d = day(tmp_path)
        put_coach(d.data, made_up_status(), off=True)
        txt = lines(C.render_card(str(d.data), d.sd, d.deals, FRI))
        assert "Switched off - the bots are on your own settings." in txt and "Real money now" not in txt
        put_coach(d.data, made_up_status(), envelope={"mode": "OFF"})
        (d.data / "COACH-OFF").unlink()
        assert "Switched off - the bots are on your own settings." in lines(C.render_card(str(d.data), d.sd, d.deals,
                                                                                         FRI))

    def test_a_broken_file(self, tmp_path):
        d = day(tmp_path)
        put_coach(d.data, made_up_status(), raw_envelope="{broken")
        txt = lines(C.render_card(str(d.data), d.sd, d.deals, FRI))
        assert re.search(r"The coach's instructions could not be read \(not valid JSON \(.*\)\): the bots are on "
                         r"your own settings\.", txt)
        put_coach(d.data, made_up_status(envelope_error="the envelope is invalid: unknown key(s) ['x']"))
        txt = lines(C.render_card(str(d.data), d.sd, d.deals, FRI))
        assert ("The coach's instructions could not be read (the envelope is invalid: unknown key(s) ['x']): the bots "
                "are on your own settings.") in txt

    def test_the_watch_day(self, tmp_path):
        mon = t(2026, 10, 12, 15, 0)
        d = day(tmp_path, now=mon)
        put_coach(d.data, made_up_status(mode="WATCH", written_utc=mon.isoformat(), keys=[], changes=[],
                                         words="Watching only - it changes nothing today (watching only until "
                                               "2026-10-13T23:00:00+00:00 (the WATCH day))."))
        card = C.render_card(str(d.data), d.sd, d.deals, mon)
        txt = lines(card)
        assert txt.strip().startswith("The coach's call - Mon 12 Oct, 16:00 · watching only · checked 0 seconds ago")
        assert ("Watching only - it changes nothing today. It takes charge within your limits at 06:30 UK on Wed 14 "
                "Oct, if today's checks pass.") in txt
        assert "Nothing on real money through the coach - every bot trades on your own settings." in txt
        assert "Changed today: nothing." in txt
        assert "Too early to say a pace: 1 trading day so far." in txt

    def test_status_missing_unreadable_or_stale(self, tmp_path):
        d = day(tmp_path)
        put_coach(d.data)                                             # just installed: no status yet
        txt = lines(C.render_card(str(d.data), d.sd, d.deals, FRI))
        assert "The coach has not reported yet - the bots are on your own settings until it does." in txt
        put_coach(d.data, raw_status="{bad")
        txt = lines(C.render_card(str(d.data), d.sd, d.deals, FRI))
        assert "The coach's status could not be read (not valid JSON" in txt and "Real money now" not in txt
        put_coach(d.data, made_up_status(written_utc="2026-10-16T09:00:00+00:00"))
        txt = lines(C.render_card(str(d.data), d.sd, d.deals, FRI))
        assert ("The coach has not reported for 15 minutes: its grants have lapsed, so the bots are on your own "
                "settings with its last restrictions.") in txt

    def test_made_up_status_shows_test_data_and_no_pound_sign(self, tmp_path):
        d = day(tmp_path)
        put_coach(d.data, made_up_status(test_data=True))
        card = C.render_card(str(d.data), d.sd, d.deals, FRI)
        assert "£" not in card and "&pound;" not in card
        txt = lines(card)
        assert "TEST DATA - not a result" in txt and "This status was made up to check the page" in txt
        assert "Against your daily aim: real money since 12 Oct: TEST DATA - not a result over 5 trading days" in txt
        assert "42.10" not in txt and "64.00" not in txt and "22.30" not in txt
        assert "(1) Formula 1's judgement after 20 live trades (14 to go)." in txt          # words, not figures


# ============================================================ the page ("/") --
class TestOnThePage:
    def test_on_the_main_page_the_coach_is_one_line_linking_to_its_page(self, tmp_path):
        """12 Oct (Aaron: "its just a load of writing"; then "Put the layout similar to the ai hub"): "/" carries
        the coach in ONE line on its own small card under the Bots card - its state and how many changes it made
        today - linking to /coach, where the card is in full."""
        d = day(tmp_path)
        st = made_up_status(written_utc=FRI.isoformat())
        put_coach(d.data, st)
        home, page = pages(d, FRI, "/advanced", "/coach")
        assert home.index('s-acct">') < home.index('<section class="card" id="bots">') < home.index(
            '<a class="card coach-card" href="/coach" id="coach">')
        card = home.split('<a class="card coach-card" href="/coach" id="coach">')[1].split("</a>")[0]
        assert '<div class="mini-title">The coach<span class="go">' in card
        line = unescape(card.split('<div class="lab-one">')[1].split("</div>")[0])
        n = len(C._today_changes(st, FRI, limit=10_000))
        title, _sub = C.headline(C.load(str(d.data), FRI))
        words = f"{title} · " + (f"{n} change{'' if n == 1 else 's'} today - see why" if n else "no changes today")
        assert C.one_line(str(d.data), FRI) == "Coach: " + words
        assert line == words[:1].upper() + words[1:]                 # its card's title says "The coach"
        assert "£" not in line
        for absent in ("<table", 'class="tile"', 's-coach', "The coach's call", "Needs your yes"):
            assert absent not in home, absent
        assert page.count('<section class="s-card s-coach">') == 1          # the card, in full, on /coach

    def test_the_coachs_line_in_each_state(self, tmp_path):
        assert C.one_line("", FRI) == "Coach: not switched on yet"
        d = day(tmp_path)
        put_coach(d.data, made_up_status(), raw_envelope="{broken")
        assert C.one_line(str(d.data), FRI) == "Coach: switched off - the bots are on your own settings"
        put_coach(d.data, made_up_status(test_data=True, written_utc=FRI.isoformat()))
        assert C.one_line(str(d.data), FRI).endswith("(TEST DATA - not a result)")

    def test_the_account_is_the_same_with_and_without_the_coach(self, tmp_path):
        d = day(tmp_path)
        (without,) = pages(d, FRI, "/advanced")
        put_coach(d.data, made_up_status(written_utc=FRI.isoformat()))
        (with_,) = pages(d, FRI, "/advanced")
        assert account_section(without) == account_section(with_)
        strip = re.compile(r'<a class="card coach-card" href="/coach" id="coach">.*?</a>'
                           r'|<span class="stamp">[^<]*</span>', re.S)        # the refresh's own clock aside
        assert strip.sub("", without) == strip.sub("", with_)          # nothing else on "/" moves

    def test_everything_is_escaped(self, tmp_path):
        d = day(tmp_path)
        st = made_up_status(written_utc=FRI.isoformat(), board=BAD, demo_line=BAD,
                            needs_your_yes=[BAD], needs_you=[BAD])
        st["keys"][1].update(words=BAD, why=BAD)
        st["changes"][0]["words"] = BAD
        st["plan"]["items"][0]["words"] = BAD
        st["suggestions"] = [BAD]
        put_coach(d.data, st)
        home, page = pages(d, FRI, "/advanced", "/coach")
        for p in (home, page):
            assert "<script>alert" not in p
        assert "&lt;script&gt;alert(1)&lt;/script&gt;" in page               # its words: on /coach, escaped

    def test_the_coach_page(self, tmp_path):
        d = day(tmp_path)
        put_coach(d.data, made_up_status(written_utc=FRI.isoformat()))
        (page,) = pages(d, FRI, "/coach")
        assert "<h1" in page and "The coach</h1>" in page
        for words in ("Each piece of work", "Tonight's plan", "Decisions", "Brakes, headroom and checks",
                      "The luck check", "The coach's scorecard", "Real money each day since 12 Oct",
                      "The honest arithmetic", "Sources"):
            assert f"<h2>{words}</h2>" in unescape(page), words
        for tbl in re.finditer(r"<table", page):                      # tables only inside .s-old
            assert page.rindex('<div class="s-old">', 0, tbl.start()) > page.rindex("</section>", 0, tbl.start())
        txt = text_of(page)
        assert "Released Fri 16 Oct 06:30 UK unless you veto an item" in txt
        assert "Trend & Breakout: breakout acceptance on major pairs BANNED_REAL PRACTICE - 31% -0.22 R 31 (0 real)" \
               in txt
        assert "0.0% of 200 made-up segments passed the promotion test; promotions were allowed" in txt
        assert "Instruction to the bots: number 1842, written 40 seconds ago" in txt
        assert "an estimate, not a result" in txt and "never a projection" in txt
        # /coach without the coach: says so, still shows the real figure
        put_coach(d.data, made_up_status(), raw_envelope="{broken")
        (page,) = pages(d, FRI, "/coach")
        assert "The coach's instructions could not be read" in text_of(page) and "<table" not in page

    def test_render_functions_never_fail(self):
        assert "The coach is not switched on yet." in text_of(C.render_card(None))
        assert "The coach is not switched on yet." in text_of(C.render_page_body(""))
        assert C.progress(None, None)["ok"] is False
