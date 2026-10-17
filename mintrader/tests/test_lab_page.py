"""The lab in the bot showcase (Aaron, 10 Oct 2026: "Make sure everything is
presented in the bot showcase really simply for me and not over the top, put
it all nice and clear in layman terms and change the ui to suit this" and
"each day the winners stand out and we watch them closely like scouts").

What the page must do (docs/lab/lab-spec.md, section 9):
* "/" keeps every pinned order and string, the account untouched, the bots'
  figures exactly as before; adds Today's standouts (a fixed rule, never
  today's money alone), What changed since yesterday, and puts the bots and
  the lab's candidates on one ladder - Real money, Practising (the lab's
  funnel folded under it), Binned - each row with What to do and the five
  check pills in order.
* /lab and /lab?id=<id> read data/lab/summary.json and data/lab-status.json
  only, and say plainly when the lab has not run, could not be read, or is
  still collecting prices; a summary built from made-up prices shows
  "TEST DATA - not a result" in place of every lab figure.

EVERY FIGURE HERE IS MADE-UP TEST DATA: tests/fixtures/lab_summary_example.json
(written by hand, "synthetic": true) and the fake broker, records and status
files of tests/test_page_figures.py. Tests that need the page's ordinary
(non-test-data) rendering load the same made-up file and clear its flag IN
MEMORY ONLY - the figures stay made up. None of it is a result of any bot or
idea.
"""
from __future__ import annotations

import copy
import datetime as dt
import json
import re
import sqlite3
from html import unescape
from pathlib import Path

from mintel.lab import (BINNED, CHECK_LABELS, FAILED, LIVE_TRIAL, NOT_TESTED, NOT_YET, PASSED, PRACTICE, PROVEN,
                        RUNNING)
from mintel.ops import showcase as S
from mintel.ops import verdicts as V
from mintel.ops.dashboard import HOME_CSS, REFRESH_JS, render_candidate, render_home, render_lab_board
from tests.test_formula1_page import a_formula1_day
from tests.test_page_figures import NOW, a_day, serve
from tests.test_simple_dashboard import account_section, made_up, research_file, row_figs, row_of, text_of

# 13 Oct (the TRADING SQUAD redesign): "/" is the new squad page (mintel/ops/squad.py). The old Home these tests
# pin - the account cards, the bot tiles, the lab and coach lines - is kept, unchanged, at /advanced (ADVANCED >
# Overview); "/?period=", "/?bot=" and "/?show=" still open the old pages. So "/" below reads "/advanced".

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "lab_summary_example.json"
UTC = dt.timezone.utc
A, B, C3, D, E, F, G, H, I = ("lab-be77cce177", "lab-969dcd199b", "lab-3e8ec84fce", "lab-205ef7c5fc",
                              "lab-1c10032568", "lab-0d87407508", "lab-b1c2ad8378", "lab-df45fb0c73",
                              "lab-a3665a2f41")
BOT_LABELS = ("Trend &amp; Breakout", "Rapid Momentum Rider", "Momentum Runner", "Band Breaker", "Crowd Fader",
              "Financial Ian")
PILLS = ("Enough trades", "Steady", "Fresh prices", "Costs", "Luck check")


# ------------------------------------------------------------------ helpers --
def made_up_summary(test_data: bool = False) -> dict:
    """The MADE-UP summary. ``test_data`` False clears its "synthetic" flag in memory, so the page draws it as it
    would draw a real one - its figures are still made up."""
    sm = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert sm["synthetic"] is True                       # the file itself can never be mistaken for a result
    sm["synthetic"] = test_data
    return sm


def put_lab(data: Path, summary=None, raw: str | None = None, status=None) -> Path:
    lab = data / "lab"
    lab.mkdir(parents=True, exist_ok=True)
    if raw is not None:
        (lab / "summary.json").write_text(raw)
    elif summary is not None:
        (lab / "summary.json").write_text(json.dumps(summary))
    if status is not None:
        (data / "lab-status.json").write_text(json.dumps(status))
    return lab


def pages(d, *paths):
    httpd, get = serve(d)
    try:
        return [get(p) for p in paths]
    finally:
        httpd.shutdown()


def lab_rows(page: str) -> dict:
    """{id: its row} of every lab candidate's row on a page: its tile on "/" (the Bots card), or its row on /lab
    (with its five checks - /lab's own tiles, the Ideas card, are left out here; see ``idea_tiles``)."""
    out = {}
    if '<h2>Ideas</h2>' in page:
        page = page.split('<h2>Ideas</h2>')[0] + page.split('<h2>Ideas</h2>')[1].split("</section>", 1)[1]
    for m in re.finditer(r'<a class="(?:s-bot|bt(?: prac)?)" href="/lab\?id=([^"]+)"[^>]*>', page):
        out[unescape(m.group(1))] = page[m.start():page.index("</a>", m.start()) + 4]
    return out


def line_cells(row: str) -> dict:
    """{title: text} of a line's money and trades cells on "/" (tags stripped)."""
    return {unescape(k): unescape(re.sub(r"<[^>]+>", "", v)) for k, v in
            re.findall(r'<div class="bt-[mt][^"]*" title="([^"]*)" data-k="[^"]*">(.*?)</div>', row)}


def line_word(row: str) -> str:
    return unescape(re.search(r'<i class="dot [a-z]+"></i>([^<]*)</div>', row).group(1))


def line_status(row: str) -> str:
    return re.search(r'<span class="bt-st [a-z]+" title="[^"]*">([^<]*)</span>', row).group(1)


def hrefs_in_order(page: str) -> list[str]:
    return [unescape(x) for x in re.findall(r'<a class="bt(?: prac)?" href="([^"]+)"', page)]


def pills(row: str) -> list[tuple[str, str]]:
    """[(label, state words)] of a row's five check pills, in order."""
    box = row.split('<div class="s-checks">')[1].split("</div>")[0]
    found = re.findall(r'<span class="s-ck [a-z]+" title="[^"]*">([^<]*)</span>', box)
    return [tuple(unescape(x).split(": ", 1)) for x in found]


def board_of(page: str) -> str:
    """The leaderboard on "/"."""
    return page.split('<section class="card" id="bots">')[1].split("</section>")[0]


def changed(page: str) -> str:
    """What changed since yesterday - on /coach since 12 Oct."""
    return page.split('<section class="s-card" id="changed">')[1].split("</section>")[0]


def lab_line(page: str) -> str:
    """The words of the lab's small card on "/" (after its title): its count and when it first tests, how many
    days of prices it holds, or its funnel's numbers."""
    card = page.split('<a class="card lab-card" href="/lab" id="lab">')[1].split("</a>")[0]
    return text_of(card.split('</span></div>', 1)[1]).strip()


def idea_tiles(page: str) -> dict:
    """{id: its tile} of /lab's Ideas card (the same tiles as on "/", the lab's ideas only)."""
    box = page.split('<h2>Ideas</h2>')[1].split("</section>", 1)[0]
    return {unescape(m.group(1)): box[m.start():box.index("</a>", m.start()) + 4]
            for m in re.finditer(r'<a class="bt(?: prac)?" href="/lab\?id=([^"]+)"[^>]*>', box)}


def eligible_row(name="x", pf=2.0, trades=12, net=40.0, up=6, down=2, stage=PRACTICE, kind="candidate", **kw):
    """A MADE-UP ladder row that passes every part of the standouts rule."""
    checks = [S.make_check("trades", RUNNING, "", 12, 30), S.make_check("steady", RUNNING, ""),
              S.make_check("fresh", PASSED, ""), S.make_check("costs", PASSED, ""),
              S.make_check("luck", RUNNING, "", p=0.03)]
    row = {"kind": kind, "id": f"lab-{name}", "name": name, "stage": stage, "checks": checks, "verdict": "",
           "what_to_do": "Watch.", "trade_kind": "practice", "synthetic": False, "trades": None,
           "window": {"trades": trades, "net": net, "up_days": up, "down_days": down, "pf": pf}}
    row.update(kw)
    return row


# ====================================================== the standouts rule --
class TestTheStandoutsRule:
    def test_the_made_up_ladder_picks_by_the_rule_alone(self):
        lab = {"state": S.LAB_READY, "summary": made_up_summary(), "synthetic": False}
        rows = [S.candidate_ladder(c) for c in S.candidates_of(lab)]
        picks = S.standouts(rows, NOW)
        assert [p["row"]["id"] for p in picks] == [A, E]           # by profit factor over the 10 days: 3.73, 1.60
        # A's luck check is worked out but not passed (it is judged at its look): the words never say it passed
        assert picks[0]["why"] == ("Ahead +£54.90 after costs over the last 10 trading days: 16 practice trades, "
                                   "7 days up, 2 down; no check has failed; its luck check is worked out but not "
                                   "passed yet.")
        assert picks[1]["why"].endswith("; every check it has reached passed.")      # E: its luck check passed
        assert picks[0]["next"] == "Watch - judged on 21 Oct after 20 trading days and 30+ trades (11 to go)."
        assert picks[1]["next"].startswith("Your call: say the word and it goes live at £13 a trade")
        chosen = {p["row"]["id"] for p in picks}
        assert B not in chosen                                     # best profit factor (2.40), but a Failed check
        assert C3 not in chosen                                    # 5.00, but only 6 trades in the 10 days
        assert D not in chosen                                     # +£2,400, but 3 days up and 5 down
        assert F not in chosen and G not in chosen and H not in chosen     # bench, no record, binned

    def test_one_failed_check_or_fewer_than_ten_trades_is_never_a_pick(self):
        ok = eligible_row("ok")
        assert [p["row"]["name"] for p in S.standouts([ok], NOW)] == ["ok"]
        for key in ("trades", "steady", "fresh", "costs", "luck"):
            bad = copy.deepcopy(ok)
            next(c for c in bad["checks"] if c["key"] == key)["state"] = FAILED
            assert S.standouts([bad], NOW) == [], key
        assert S.standouts([eligible_row("nine", trades=9)], NOW) == []
        assert len(S.standouts([eligible_row("ten", trades=10)], NOW)) == 1

    def test_a_huge_net_alone_is_never_a_pick(self):
        rich = {"net": 1_000_000.0, "pf": 50.0}
        assert S.standouts([eligible_row("few", trades=3, **rich)], NOW) == []
        assert S.standouts([eligible_row("downs", up=2, down=5, **rich)], NOW) == []
        assert S.standouts([eligible_row("evens", up=4, down=4, **rich)], NOW) == []
        no_luck = eligible_row("noluck", **rich)
        no_luck["checks"][4] = S.make_check("luck", NOT_YET, "")              # luck not worked out
        assert S.standouts([no_luck], NOW) == []
        assert S.standouts([eligible_row("bench", stage="ON THE BENCH", **rich)], NOW) == []
        assert S.standouts([eligible_row("binned", stage=BINNED, **rich)], NOW) == []
        assert S.standouts([eligible_row("test", synthetic=True, **rich)], NOW) == []
        assert S.standouts([eligible_row("lost", net=-1.0)], NOW) == []
        unpriced = eligible_row("nopf", pf=None)
        assert S.standouts([unpriced], NOW) == []                  # the lab did not give its profit factor: no guess

    def test_at_most_three_never_padded_and_the_order_never_depends_on_the_input(self):
        rows = [eligible_row(f"r{i}", pf=1.5 + i / 10) for i in range(5)]
        picks = S.standouts(rows, NOW)
        assert [p["row"]["name"] for p in picks] == ["r4", "r3", "r2"]
        assert [p["row"]["name"] for p in S.standouts(list(reversed(rows)), NOW)] == ["r4", "r3", "r2"]
        ties = [eligible_row("b", pf=2.0, trades=12), eligible_row("a", pf=2.0, trades=12),
                eligible_row("c", pf=2.0, trades=20)]
        assert [p["row"]["name"] for p in S.standouts(ties, NOW)] == ["c", "a", "b"]   # then trades, then name
        assert len(S.standouts(rows[:1], NOW)) == 1                # one eligible: one pick, never padded

    def test_a_bot_needs_its_own_verdict_and_ten_trades_in_its_last_ten_trading_days(self):
        """MADE-UP practice trades: 12 over the 10 UK trading days to Fri 9 Oct, 9 days up and 1 down."""
        days = [x for x in (dt.date(2026, 9, 28) + dt.timedelta(days=k) for k in range(12)) if x.weekday() < 5]
        trades = []
        for i, day in enumerate(days):
            nets = [3.0, 2.0] if i < 2 else ([-1.0] if i == 5 else [2.5])
            for j, net in enumerate(nets):
                trades.append({"closed": dt.datetime(day.year, day.month, day.day, 7, j, tzinfo=UTC), "net": net,
                               "costs": 0.4, "kind": "practice"})
        now = dt.datetime(2026, 10, 9, 20, 0, tzinfo=UTC)
        w = S.window_of(trades, now)
        assert (w["trades"], w["net"], w["up_days"], w["down_days"]) == (12, 26.5, 9, 1)

        def row(code):
            v = {"label": "Band Breaker", "mode": "PAPER", "code": code, "why": "made up",
                 "recommendation": "Keep testing on PAPER."}
            return {"kind": "bot", "id": "band_breaker", "name": "Band Breaker", "stage": PRACTICE,
                    "checks": S.bot_checks("band_breaker", v, PRACTICE, "practice", trades, "", now=now),
                    "verdict": code, "what_to_do": v["recommendation"], "trades": trades, "trade_kind": "practice"}
        for code, picked in ((V.PROMISING, True), (V.PROVEN, True), (V.NOT_ENOUGH, False), (V.WAITING, False),
                             (V.BREAK_EVEN, False), (V.NO_EDGE, False)):
            assert bool(S.standouts([row(code)], now)) == picked, code
        assert next(c for c in row(V.BREAK_EVEN)["checks"] if c["key"] == "steady")["state"] == FAILED
        pick = S.standouts([row(V.PROMISING)], now)[0]
        # 12 trades: short of the bots' 30, so its luck check is worked out but cannot pass yet
        assert pick["why"] == ("Ahead +£26.50 after costs over the last 10 trading days: 12 practice trades, 9 days "
                               "up, 1 down; no check has failed; its luck check is worked out but not passed yet.")
        assert pick["next"] == "Keep testing on PAPER."
        # trades before the window never count towards it
        old = [dict(t, closed=t["closed"] - dt.timedelta(days=30)) for t in trades]
        assert S.window_of(old, now)["trades"] == 0
        assert S.standouts([dict(row(V.PROMISING), trades=old)], now) == []

    def test_the_window_is_the_last_ten_uk_weekdays(self):
        assert S.window_start(dt.datetime(2026, 10, 9, 20, tzinfo=UTC)) == dt.datetime(2026, 9, 27, 23, tzinfo=UTC)
        # a Saturday looks back over the ten weekdays to Friday
        assert S.window_start(dt.datetime(2026, 10, 10, 12, tzinfo=UTC)) == dt.datetime(2026, 9, 27, 23, tzinfo=UTC)


# ================================================================ on the page --
class TestTheMainPage:
    def test_the_standouts_are_stars_and_what_changed_is_on_the_coach_page(self, tmp_path):
        """12 Oct: no standouts card - each of today's standouts (the same rule, at most 3) gets a star on its line,
        its reason in the star's title; what changed since yesterday moved to /coach."""
        d = a_day(tmp_path)
        put_lab(d.data, made_up_summary())
        page, coach = pages(d, "/advanced", "/coach")
        rows = lab_rows(page)
        starred = [cid for cid, row in rows.items() if 'class="bt-star"' in row]
        assert sorted(starred) == sorted([A, E])
        assert page.count('class="bt-star"') == 2                    # no bot here has earned one
        assert ('<span class="bt-star" title="Standout: Ahead +£54.90 after costs over the last 10 trading days: 16 '
                'practice trades, 7 days up, 2 down; no check has failed; its luck check is worked out but not passed '
                'yet." aria-label="standout">&#9733;</span>') in rows[A]
        assert "Today's standouts" not in page and "What changed" not in page
        sm = made_up_summary()
        name = {c["id"]: c["name"] for c in sm["candidates"]}
        moved = text_of(changed(coach))
        assert "<h2>What changed since yesterday</h2>" in coach
        assert f"{name[E]}: passed look 1 (+£71 over 34 practice trades) - READY FOR LIVE, your call" in moved
        assert f"{name[F]}: passed its one fresh look - ON THE BENCH" in moved
        assert moved.index(name[E]) < moved.index(name[F])          # newest first
        assert "older than a day" not in moved                       # 6 Oct: more than 24 hours before Fri 09:49
        # one short screen: the account's cards, the Bots card, then the lab's and the coach's small cards
        assert (page.index('s-acct">') < page.index('<section class="card" id="bots">')
                < page.index('<a class="card lab-card" href="/lab" id="lab">')
                < page.index('<a class="card coach-card" href="/coach" id="coach">'))

    def test_no_standouts_and_nothing_moved_say_so(self, tmp_path):
        d = a_day(tmp_path)
        page, coach = pages(d, "/advanced", "/coach")                        # no lab, and no bot with ten trades
        assert 'class="bt-star"' not in page
        assert '<p class="s-none">Nothing moved.</p>' in changed(coach)

    def test_changes_are_capped_at_five_and_formula_1_going_live_is_one(self, tmp_path):
        sm = made_up_summary()
        sm["changes_24h"] = [{"when": f"2026-10-08T2{k}:00:00+00:00", "id": A, "name": f"idea {k}", "text": "moved"}
                             for k in range(4)]
        d = a_formula1_day(tmp_path)
        put_lab(d.data, sm)
        lab = S.load_lab(str(d.data), NOW)
        moved = S.changes(lab, str(d.data), NOW)
        assert len(moved) == 5
        assert moved[0]["text"] == ("Trend & Breakout: its minor pairs (Formula 1) went LIVE with real money "
                                    "(config.json tnb.live_since_utc).")       # 08:30 UK today: the newest
        assert [m["text"] for m in moved[1:]] == ["idea 3: moved", "idea 2: moved", "idea 1: moved", "idea 0: moved"]
        later = NOW + dt.timedelta(days=2)
        assert S.changes(S.load_lab(str(d.data), later), str(d.data), later) == []

    def test_one_line_each_live_first_then_by_overall(self, tmp_path):
        """One list, the bots and the lab's ideas together: LIVE first, then by Overall (highest first, no figure
        last), switched off and binned at the end. Ideas by a short name (family and markets), the full name in
        the line's title and on its page; the idea binned 20 days ago is on /lab, not on "/"."""
        d = a_formula1_day(tmp_path, rider="OFF")
        research_file(d.data, "momentum_rider")                      # MADE-UP research: no edge
        put_lab(d.data, made_up_summary())
        (page,) = pages(d, "/advanced")
        for label in BOT_LABELS:
            assert page.count(f'<span class="bt-name">{label}</span>') == 1, label
        bot = lambda b: f"/?period=today&bot={b}"                     # noqa: E731
        lab = lambda c: f"/lab?id={c}"                                # noqa: E731
        assert hrefs_in_order(board_of(page)) == [
            bot("market_intelligence"), bot("financial_ian"), bot("momentum_runner"),      # LIVE (+38.90, 0, 0)
            lab(D), lab(B), lab(E), lab(A), lab(C3),                  # practice: 2,400, 75, 71, 64.10, 40 overall
            bot("band_breaker"), bot("crowd_fader"),                  # 4.20, 0.00
            lab(G), lab(F),                                           # no figure yet: practising, testing
            bot("momentum_rider"), lab(H)]                            # switched off (+7.08 real), binned 6 Oct
        status = {h: line_status(r) for h, r in zip(hrefs_in_order(page), re.findall(
            r'<a class="bt(?: prac)?" href="[^"]*"[^>]*>.*?</a>', board_of(page), flags=re.S))}
        assert status == {bot("market_intelligence"): "LIVE", bot("financial_ian"): "LIVE",
                          bot("momentum_runner"): "LIVE", lab(D): "PRACTICE", lab(B): "PRACTICE",
                          lab(E): "PRACTICE", lab(A): "PRACTICE", lab(C3): "PRACTICE", bot("band_breaker"): "PRACTICE",
                          bot("crowd_fader"): "PRACTICE", lab(G): "PRACTICE", lab(F): "TESTING",
                          bot("momentum_rider"): "OFF", lab(H): "OFF"}
        assert I not in page                                          # binned 19 Sep: on /lab, not on "/"
        sm = made_up_summary()
        full = {c["id"]: c["name"] for c in sm["candidates"]}
        rows = lab_rows(page)
        assert S.short_name(next(c for c in sm["candidates"] if c["id"] == C3)) == "Trend pullback · major pairs"
        assert '<span class="bt-name">Trend pullback · major pairs</span>' in rows[C3]
        # two ideas that would read the same are numbered, never two identical lines
        assert '<span class="bt-name">Momentum burst · minor pairs (1)</span>' in rows[A]
        assert '<span class="bt-name">Momentum burst · minor pairs (2)</span>' in rows[D]
        for cid, row in rows.items():
            assert f'title="{full[cid]}"'.replace("'", "&#x27;") in row, cid    # the full name on hover
        for row in re.findall(r'<a class="bt(?: prac)?" href.*?</a>', page, flags=re.S):   # one tile: no paragraph
            assert "<p" not in row and "s-checks" not in row and "What to do" not in row

    def test_every_one_has_its_page_with_what_to_do_and_the_five_checks_in_order(self, tmp_path):
        """What to do and the five checks moved off "/" to each one's own page: a bot's (/?bot=...) and an idea's
        row on /lab (and its page /lab?id=...)."""
        d = a_formula1_day(tmp_path)
        put_lab(d.data, made_up_summary())
        home, board = pages(d, "/advanced", "/lab")
        rows = re.findall(r'<a class="bt(?: prac)?" href="([^"]*)"', home)
        assert len(rows) == 6 + 8                                    # the six bots, and 8 ideas (I: binned 20 days ago)
        detail = pages(d, *[unescape(h) for h in rows])
        for href, page in zip(rows, detail):
            if href.startswith("/lab"):
                assert "<h2>What to do</h2>" in page and "<h2>Is it luck? The five checks</h2>" in page, href
                checks = page.split("<h2>Is it luck? The five checks</h2>")[1].split("</ul>")[0]
                assert [unescape(x) for x in re.findall(r"<b>[^ ]+ ([^:<]+): [^<]*</b>", checks)] == list(PILLS)
            else:
                assert "<h2>Recommendation</h2>" in page and "<h2>The five checks</h2>" in page, href
                got = pills(page.split("<h2>The five checks</h2>")[1])
                assert [k for k, _v in got] == list(PILLS), href
                for _k, state in got:
                    assert re.fullmatch(r"Passed|Failed|Not yet|Not tested this way|Running( \(\d+ of \d+( for the "
                                        r"trial)?\))?", state)
        on_board = lab_rows(board)
        for row in on_board.values():
            assert '<div class="s-rec"><b>What to do:</b> ' in row and [k for k, _ in pills(row)] == list(PILLS)
        a = on_board[A]
        assert pills(a) == [("Enough trades", "Running (19 of 30)"), ("Steady", "Running"), ("Fresh prices", "Passed"),
                            ("Costs", "Passed"), ("Luck check", "Running")]
        assert 'class="s-ck run" title="19 practice trades over 11 UK trading days; it needs 30 over 10."' in a
        assert 'class="s-ck ok" title="Still +£41 at one and a half times the costs; +£29 at twice."' in a
        assert pills(on_board[B])[3] == ("Costs", "Failed") and 'class="s-ck no"' in on_board[B]
        assert line_word(lab_rows(home)[B]) == "Failing"              # a failed check: its one word on "/"

    def test_the_bots_on_the_ladder_and_their_not_tested_sentences(self, tmp_path):
        d = a_formula1_day(tmp_path, rider="OFF")
        research_file(d.data, "momentum_rider")                      # MADE-UP research: no edge
        ids = {"Trend &amp; Breakout": "market_intelligence", "Rapid Momentum Rider": "momentum_rider",
               "Momentum Runner": "momentum_runner", "Band Breaker": "band_breaker", "Crowd Fader": "crowd_fader",
               "Financial Ian": "financial_ian"}
        got = dict(zip(BOT_LABELS, pages(d, *[f"/?bot={ids[x]}" for x in BOT_LABELS])))   # each bot's own page
        stage = {lab: re.search(r'<h2>On the ladder</h2><p><span class="s-stage">([^<]*)</span>', got[lab]).group(1)
                 for lab in BOT_LABELS}
        assert stage == {"Trend &amp; Breakout": LIVE_TRIAL, "Rapid Momentum Rider": BINNED,
                         "Momentum Runner": LIVE_TRIAL, "Band Breaker": PRACTICE, "Crowd Fader": PRACTICE,
                         "Financial Ian": LIVE_TRIAL}

        def fresh(label):
            m = re.search(r'<span class="s-ck ([a-z]+)" title="([^"]*)">Fresh prices: ([^<]*)</span>', got[label])
            return m.group(1), unescape(m.group(2)), m.group(3)
        assert fresh("Trend &amp; Breakout")[1].startswith(
            "Not tested this way - it went straight to practice; its idea is in the lab as 'Momentum burst'")
        assert fresh("Band Breaker")[1].startswith("Not tested this way - its idea is in the lab as 'Back to the "
                                                   "average'")
        for label in ("Momentum Runner", "Crowd Fader", "Financial Ian"):
            cls, text, word = fresh(label)
            assert (cls, word) == ("ny", NOT_TESTED), label
            assert text == ("Not tested this way. It was never tried once on prices it had not seen, as the lab's "
                            "ideas are."), label
        cls, text, word = fresh("Rapid Momentum Rider")               # switched off: binned by its research
        assert (cls, word) == ("no", FAILED)
        assert text == ("Failed - its own test on real past prices found no edge over 1,073 trades "
                        "(data/rider-research/2026-10-09/summary.json).")
        assert "Switched off - it places no trades" in got["Rapid Momentum Rider"]
        # Formula 1 counts "n of 20" from MetaTrader's records, as the plan's line does
        tnb = got["Trend &amp; Breakout"]
        assert "Enough trades: Running (2 of 20)" in tnb
        assert "2 live trades for Formula 1 since 9 Oct 08:30 UK (MetaTrader&#x27;s records)" in tnb
        assert "so far" not in tnb.split('<div class="s-checks">')[1].split("</div>")[0]   # never a count of its own

    def test_bot_stage_is_derived_from_mode_and_verdict(self):
        assert S.bot_stage({"mode": "OFF", "code": V.NO_EDGE}) == BINNED
        assert S.bot_stage({"mode": "LIVE", "code": V.PROMISING}) == LIVE_TRIAL
        assert S.bot_stage({"mode": "LIVE", "code": V.PROVEN}) == PROVEN
        assert S.bot_stage({"mode": "PAPER", "code": V.PROVEN}) == PRACTICE
        assert S.bot_stage({"mode": "PAPER", "live_groups": ("FX_MINOR",)}) == LIVE_TRIAL
        assert S.bot_stage({"mode": ""}) == "" and S.bot_stage(None) == ""

    def test_a_bots_luck_check_is_its_own_daily_net_counted_once(self, tmp_path):
        """MADE-UP real trades: 10 UK days, mostly up."""
        trades = made_up([[4.0, -1.0]] * 3 + [[-2.0]] + [[3.0, 1.0]] * 7, cost=0.3)      # 5 to 15 Oct
        now = dt.datetime(2026, 10, 15, 20, tzinfo=UTC)
        v = {"label": "Momentum Runner", "mode": "LIVE", "code": V.PROMISING, "why": "positive after costs, but not "
             "proven yet"}
        checks = S.bot_checks("momentum_runner", v, LIVE_TRIAL, "real", trades, "", now=now)
        luck = checks[4]
        daily = S.daily_nets(trades, now)
        assert daily == [3.0, 3.0, 3.0, -2.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0]   # each UK day, weekend ones too
        from mintel.lab.stats import luck_of_daily
        assert luck["p"] == round(luck_of_daily(daily, "momentum_runner", 1)["p"], 4)
        assert luck["state"] in (PASSED, RUNNING) and "picked by hand, not from a grid" in luck["text"]
        assert luck["text"].startswith("Chance this is luck: about 1 in ")
        assert checks[0]["state"] == PASSED and checks[0]["n"] == 21            # 21 real trades: past the 20
        assert checks[1]["state"] == NOT_YET and "PROMISING" in checks[1]["text"]
        assert checks[3]["state"] == PASSED                                      # costs: 6.30 on +35.00 net
        assert "practice" not in json.dumps(checks)                              # a LIVE bot's checks: real money

    def test_a_candidate_with_no_record_shows_no_figure_and_practice_is_grey(self, tmp_path):
        d = a_day(tmp_path)
        put_lab(d.data, made_up_summary())
        home, total, board = pages(d, "/advanced", "/?period=total", "/lab")
        g = lab_rows(home)[G]
        assert "£" not in g and set(line_cells(g).values()) == {"-"}
        assert all(state == NOT_YET for k, state in pills(lab_rows(board)[G]) if k != "Fresh prices")
        a = lab_rows(home)[A]                                        # one with a record: practice, in grey, labelled
        assert '<div class="bt-m bt-o prac" title="Overall (practice)" data-k="Overall"><span class="pos">+£64.10</span></div>' in a
        # the summary was written last night (8 Oct): it does not hold today's practice - no figure, never a guess
        assert line_cells(a) == {"Today (practice)": "-", "Overall (practice)": "+£64.10",
                                 "Trades today (practice)": "-"}
        assert '<span class="bt-st practice" title="Practice - live prices, no money">PRACTICE</span>' in a
        assert line_cells(lab_rows(total)[A]) == {"Overall (practice)": "+£64.10", "Trades in total (practice)": "19"}

    def test_an_ideas_today_and_this_week_come_from_a_summary_written_today_only(self, tmp_path):
        sm = made_up_summary()
        sm["written_utc"] = (NOW - dt.timedelta(minutes=30)).isoformat()     # written this morning (made up)
        a = next(c for c in sm["candidates"] if c["id"] == A)
        a["figures"].update(today_gbp=5.5, today_trades=2)
        d = a_day(tmp_path)
        put_lab(d.data, sm)
        today, week, month = pages(d, "/advanced", "/?period=week", "/?period=month")
        assert line_cells(lab_rows(today)[A]) == {"Today (practice)": "+£5.50", "Overall (practice)": "+£64.10",
                                                  "Trades today (practice)": "2"}
        assert line_cells(lab_rows(week)[A])["This week (practice)"] == "+£41.70"
        assert line_cells(lab_rows(month)[A])["This month (practice)"] == "-"     # the summary does not hold a month
        assert S.lab_period_figures(a, "today", NOW - dt.timedelta(days=1), NOW) == (None, None)

    def test_a_bench_candidate_never_shows_computer_test_money_on_the_main_page(self, tmp_path):
        d = a_day(tmp_path)
        put_lab(d.data, made_up_summary())
        home, board = pages(d, "/advanced", "/lab")
        f = lab_rows(home)[F]
        assert "£" not in f and "computer test" not in f and line_status(f) == "TESTING"
        assert line_word(f) == "Waiting"
        for row in lab_rows(home).values():
            assert "s-sim" not in row
        # its computer tests stay on /lab (on the bench), never called money made
        bench = lab_rows(board)[F]
        assert '<div class="s-sim">On old prices (computer test): +£312.40 over 143 trades</div>' in bench
        assert '<div class="s-sim">Once on prices it had never seen: +£88.10 over 41 trades</div>' in bench
        for row in lab_rows(board).values():
            for sim in re.findall(r'<div class="s-sim">([^<]*)</div>', row):
                assert "made" not in sim.lower()
        assert '<span class="s-chip sky">PASSED</span><span class="s-stage">ON THE BENCH</span>' in bench
        assert ('<div class="s-rec"><b>What to do:</b> Nothing - it starts practising when a place frees.</div>'
                in bench)

    def test_the_account_and_the_bots_figures_are_the_same_with_and_without_the_lab(self, tmp_path):
        d = a_day(tmp_path)
        before = pages(d, "/advanced", "/?period=week", "/?period=total")
        put_lab(d.data, made_up_summary())
        after = pages(d, "/advanced", "/?period=week", "/?period=total")
        for b, a in zip(before, after):
            assert account_section(b) == account_section(a)
            for label in BOT_LABELS:
                assert row_figs(row_of(b, label)) == row_figs(row_of(a, label)), label
                assert row_of(b, label) == row_of(a, label), label            # lines unchanged

    def test_the_lab_never_enters_the_page_as_anything_but_its_two_files(self, tmp_path, monkeypatch):
        """The page reads data/lab/summary.json and data/lab-status.json and nothing else of the lab's: never
        lab.sqlite, and it writes nothing."""
        d = a_day(tmp_path)
        lab = put_lab(d.data, made_up_summary(), status={"updated_utc": NOW.isoformat(), "mode": "PAPER",
                                                         "roster": [], "feed_ok": True})
        (lab / "lab.sqlite").write_text("not a database: the page must never open it")
        real = sqlite3.connect

        def guard(path, *a, **k):
            assert "lab.sqlite" not in str(path), path
            return real(path, *a, **k)
        monkeypatch.setattr(sqlite3, "connect", guard)
        def lab_files():
            return {p: p.stat().st_mtime_ns for p in [*lab.rglob("*"), *d.data.glob("lab*")]}
        files = lab_files()
        got = pages(d, "/advanced", "/lab", f"/lab?id={A}", "/lab?id=nope")
        assert all("dashboard error" not in p for p in got)
        assert lab_files() == files                                  # nothing of the lab's written, nothing new
        assert "sqlite3" not in Path(S.__file__).read_text()


# ========================================================== test data, empty states --
class TestEmptyAndTestData:
    def test_a_made_up_summary_shows_test_data_and_no_money(self, tmp_path):
        d = a_day(tmp_path)
        put_lab(d.data, made_up_summary(test_data=True))
        home, board, one, coach = pages(d, "/advanced", "/lab", f"/lab?id={A}", "/coach")
        rows = lab_rows(home)
        assert len(rows) == 8
        for cid, row in rows.items():
            assert '<div class="bt-test">TEST DATA - not a result</div>' in row, cid
            assert "£" not in row and "GBP" not in row, cid
            assert "Running (" not in row and 'class="bt-m' not in row, cid    # not even a count
            assert 'class="bt-star"' not in row, cid                 # test data never stands out
            assert line_word(row) == "Test data", cid
        assert lab_line(home) == "TEST DATA - not a result"
        assert len(idea_tiles(board)) == 8 and all(                  # /lab's tiles: test data, no money either
            '<div class="bt-test">TEST DATA - not a result</div>' in t for t in idea_tiles(board).values())
        assert "The lab: TEST DATA - not a result." in text_of(changed(coach))
        assert "£" not in board and "TEST DATA - not a result" in board
        assert '<p class="s-test"><b>TEST DATA - not a result.</b>' in board
        txt = text_of(one)
        assert "What happened TEST DATA - not a result" in txt
        assert not re.search(r"[+-]£\d", one)                        # no result anywhere on its page
        assert "<table" not in one                                   # its trades: test data, not listed

    def test_no_lab_folder_says_it_has_not_run_and_nothing_else_changes(self, tmp_path):
        d = a_day(tmp_path)
        home, board, one = pages(d, "/advanced", "/lab", f"/lab?id={A}")
        # before its first night: how many ideas wait (its registered grid, never a guess) and when it first tests
        from mintel.lab.families import all_families
        n = sum(f.GRID_SIZE for f in all_families())
        assert n == 732 and lab_line(home) == f"{n} ideas waiting first test tonight 23:10"
        assert not lab_rows(home)
        assert "The lab has not run yet." in board
        assert "No such idea." in one

    def test_a_broken_summary_is_said_never_guessed(self, tmp_path):
        d = a_day(tmp_path)
        put_lab(d.data, raw="{not json")
        home, board = pages(d, "/advanced", "/lab")
        line = re.search(r'<div class="lab-one">(The lab&#x27;s summary could not be read \(.*?\)\.)</div>', home)
        # in plain words, never a raw Python error
        assert line and "JSONDecodeError" not in line.group(1)
        assert "it is not complete or not valid - the next nightly run writes it again" in line.group(1)
        assert not lab_rows(home) and "The lab&#x27;s summary could not be read (" in board
        put_lab(d.data, raw=json.dumps(dict(made_up_summary(), schema=2)))
        lab = S.load_lab(str(d.data), NOW)
        assert lab["state"] == S.LAB_BROKEN
        assert lab["line"] == ("The lab's summary could not be read (it is written in format 2; this page reads "
                               "format 1).")
        put_lab(d.data, raw="[1, 2]")
        assert S.load_lab(str(d.data), NOW)["line"] == ("The lab's summary could not be read (it does not hold a "
                                                        "summary).")

    def test_a_short_store_says_how_many_days_it_has(self, tmp_path):
        d = a_day(tmp_path)
        sm = made_up_summary()
        sm["store"]["complete_days"] = 31
        put_lab(d.data, sm)
        (home,) = pages(d, "/advanced")
        assert lab_line(home) == "Collecting prices: 31 of 40 days"
        assert '<div class="prog"><span style="width:78%"></span></div>' in home
        assert not lab_rows(home)
        assert S.load_lab(str(d.data), NOW)["state"] == S.LAB_COLLECTING

    def test_the_lab_line_and_a_missed_night(self, tmp_path):
        d = a_day(tmp_path)
        put_lab(d.data, made_up_summary())
        home, board = pages(d, "/advanced", "/lab")
        assert lab_line(home) == "632 Tested 14 Passed 5 Practising 1 Ready 623 Binned"     # its funnel's numbers
        # the whole funnel, the bench and what was binned lately are on /lab
        assert S.funnel_line(made_up_summary(), NOW) == (
            "The lab: 632 ideas tried on past prices - 14 passed; 12 of 30 fresh looks used, 7 passed; 1 on the "
            "bench, 5 practising, 1 ready for live, 623 binned. Last night: 632 tested in 38 min. Prices: 24 markets, "
            "82 complete trading days.")
        assert ("The lab: 632 ideas tried on past prices - 14 passed; 12 of 30 fresh looks used, 7 passed; 1 on the "
                "bench, 5 practising, 1 ready for live, 623 binned.") in text_of(board)
        bench = board.split('id="lab-bench">')[1].split("</details>")[0]
        assert f'href="/lab?id={F}"' in bench
        binned = board.split('id="lab-binned">')[1].split("</details>")[0]
        assert f'href="/lab?id={H}"' in binned
        sm = made_up_summary()
        sm["nightly"]["date"], sm["nightly"]["started"] = "2026-10-06", "2026-10-06T22:10:00+00:00"
        put_lab(d.data, sm)
        home, board = pages(d, "/advanced", "/lab")
        assert lab_line(home).endswith(" Last night's run missed")
        assert "<b>Last night's run: missed.</b>" in board


# ===================================================================== /lab --
class TestTheLabPages:
    def test_the_board(self, tmp_path):
        d = a_day(tmp_path)
        put_lab(d.data, made_up_summary(), status={"updated_utc": (NOW - dt.timedelta(seconds=12)).isoformat(),
                                                   "mode": "PAPER", "roster": [{"id": A}, {"id": B}], "feed_ok": True})
        (board,) = pages(d, "/lab")
        txt = text_of(board)
        assert "Prices: 24 markets, 82 complete trading days (15 Jun-8 Oct 2026); the lab needs 40 to start." in txt
        assert "Short history: results are weaker (fewer than 90 days of past prices)." in txt
        assert ("14 Sep-8 Oct 2026 is kept back and never used for choosing; 12 of 30 looks used (each idea gets one "
                "look, ever).") in txt
        assert "632 ideas tried; the best of 632 always looks good, so every result is judged against that." in txt
        assert ("The clock-trade control passed the past test 0 times in 32: the gates are not letting noise "
                "through.") in txt
        assert "rather than a pass; it gets one more try on the next stretch." in txt
        assert ("Practice right now: 2 ideas on live prices, no money; prices are coming in. Reported 12 seconds "
                "ago.") in txt
        assert "32 ideas wait for enough news history to test." in txt
        fams = board.split("<h2>The families of ideas</h2>")[1].split("</section>")[0]
        assert "<td>Squeeze release</td><td>36</td><td>1</td><td>0.41</td><td>Yes - nothing forwarded</td>" in fams
        assert "<td>Trend pullback</td><td>48</td><td>1</td><td>n/a</td><td>No</td>" in fams
        assert '<details class="s-more" id="lab-bench"><summary>On the bench (1)</summary>' in board
        binned = board.split('id="lab-binned">')[1].split("</details>")[0]
        assert H in binned and I in binned                           # both binned in the last 30 days
        night = text_of(board.split("<h2>Last night's run</h2>")[1].split("</section>")[0])
        assert "Ideas tested 632" in night and "Took 38 min" in night and "upload: done (made up)" in night
        for tbl in re.finditer(r"<table", board):                     # tables only inside .s-old
            assert board.rindex('<div class="s-old">', 0, tbl.start()) > board.rindex("</section>", 0, tbl.start())

    def test_one_idea(self, tmp_path):
        d = a_day(tmp_path)
        put_lab(d.data, made_up_summary(), status={"updated_utc": NOW.isoformat(), "mode": "PAPER", "roster": [],
                                                   "open_positions": [{"candidate_id": A, "symbol": "EURGBP",
                                                                       "side": 1, "opened_utc": NOW.isoformat()}]})
        (page,) = pages(d, f"/lab?id={A}")
        sm = made_up_summary()
        a = next(c for c in sm["candidates"] if c["id"] == A)
        txt = text_of(page)
        assert '<a class="back" href="/lab">&larr; The lab</a>' in page
        assert (f'<h1>{a["name"]}<span class="s-stage">PRACTICE</span><span class="s-chip grey">TESTING</span></h1>'
                in unescape(page))
        assert "Practising for 11 days: +£64 after costs over 19 practice trades, 9 days up, 2 down." in txt
        order = ["What happened", "What to do", "Is it luck? The five checks", "Its rules in plain words",
                 "Its story", "Its trades", "Sources"]
        at = [page.index(f"<h2>{x}</h2>") for x in order]
        assert at == sorted(at)
        assert ("On old prices (computer test, 15 Jun-11 Sep 2026): +£312.40 after costs over 143 trades; 37 days up, "
                "22 down; won £1.41 for every £1 lost.") in txt
        assert "In practice (live prices, no money) since 24 Sep: +£64.10 after costs over 19 practice trades" in txt
        checks = page.split("<h2>Is it luck? The five checks</h2>")[1].split("</ul>")[0]
        assert [unescape(x) for x in re.findall(r"<b>[^ ]+ ([^:<]+): [^<]*</b>", checks)] == list(PILLS)
        assert "Chance this is luck: about 1 in 33" in unescape(checks)
        for w in a["words"]:
            assert w.replace("'", "&#x27;") in page
        assert "23 Sep 2026: ON THE BENCH &rarr; PRACTICE" in page and "+£88 over 41 trades" in unescape(page)
        trades = page.split("<h2>Its trades</h2>")[1].split("<h2>")[0]
        assert trades.count("<tr>") == 1 + 19
        first = re.findall(r"<tr><td>([^<]*)</td>", trades)[0]
        assert first.startswith("Thu 08 Oct")                         # newest first
        assert "Open now (practice): EURGBP buy since 09:49 UK" in text_of(trades)
        src = text_of(page.split("<h2>Sources</h2>")[1])
        assert "data/lab/lab.sqlite tests 4411 (past) and 4502 (fresh)" in src and "data/lab/summary.json" in src

    def test_an_unknown_id_and_the_refresh_keeps_the_id(self, tmp_path):
        d = a_day(tmp_path)
        put_lab(d.data, made_up_summary())
        unknown, blank, page, board = pages(d, "/lab?id=lab-0000000000", "/lab?id=", f"/lab?id={E}", "/lab")
        assert "No such idea." in unknown and "No such idea." in blank
        for p in (page, board, unknown):
            assert REFRESH_JS in p and "location.reload()" in p        # reloads this very address, ?id= and all
            assert 'http-equiv="refresh" content="10;' not in p
        assert "<b>&#10003; Luck check: Passed</b>" in page
        assert '<a href="/lab"><svg' in pages(d, "/advanced")[0]              # the sidebar's link

    def test_the_sidebar_keeps_its_links_on_every_page(self, tmp_path):
        """12 Oct: the footer's links became the hub-like sidebar, the same on every page; the Rider's and Ian's
        own pages are linked from their bots' pages."""
        d = a_day(tmp_path)
        put_lab(d.data, made_up_summary())
        home, board, one, coach, details, rider, ian, rider_page = pages(
            d, "/advanced", "/lab", f"/lab?id={A}", "/coach", "/details", "/?bot=momentum_rider", "/?bot=financial_ian", "/rider")
        for page, on in ((home, "/advanced"), (board, "/lab"), (one, "/lab"), (coach, "/coach"), (details, "/details")):
            nav = page.split('<nav class="nav">')[1].split("</nav>")[0]
            for link in ('href="/"', 'href="/?show=live"', 'href="/?show=practice"', 'href="/lab"', 'href="/coach"',
                         'href="/details"', 'href="/health"'):
                assert link in nav, (on, link)
            assert nav.count('class="on"') == 1 and f'href="{on}" class="on"' in nav, on
        assert 'href="/rider"' in rider and 'href="/ian"' in ian
        assert '<nav class="nav">' in rider_page and "Rapid Momentum Rider" in rider_page


# ========================================================== escaping and phones --
BAD = "<script>alert(1)</script>"


class TestEscapingAndPhones:
    def test_every_summary_string_is_escaped(self, tmp_path):
        sm = made_up_summary()
        a = next(c for c in sm["candidates"] if c["id"] == A)
        for k in ("name", "headline", "what_to_do"):
            a[k] = f"{a[k]} {BAD}"
        a["chip"]["code"] = BAD
        a["chip"]["colour"] = f'grey" onclick="{BAD}'
        for c in a["checks"]:
            c["text"] += f' "{BAD}'
        a["words"].append(BAD)
        a["story"].append({"when": BAD, "from": BAD, "to": BAD, "why": BAD})
        a["sources"].append(BAD)
        a["practice_trades"][0].update(symbol=BAD, exit_reason=BAD, side=BAD)
        e = next(c for c in sm["candidates"] if c["id"] == E)
        e["id"] = f'lab-"><img src=x onerror=alert(1)>'
        sm["changes_24h"][0]["text"] = BAD
        sm["changes_24h"][0]["name"] = BAD
        sm["binned_recent"][0]["reason"] = BAD
        sm["families"][0]["label"] = BAD
        sm["nightly"]["note"] = BAD
        sm["nightly"]["log_tail"].append(BAD)
        sm["nightly"]["step"] = BAD
        d = a_day(tmp_path)
        put_lab(d.data, sm, status={"updated_utc": NOW.isoformat(), "mode": "PAPER", "roster": [], "last_error": BAD,
                                    "open_positions": [{"candidate_id": A, "symbol": BAD, "side": BAD,
                                                        "opened_utc": BAD}]})
        got = pages(d, "/advanced", "/lab", f"/lab?id={A}", "/lab?id=" + BAD.replace("<", "%3C").replace(">", "%3E"))
        for p in got:
            assert "<script>alert" not in p and "<img src=x" not in p and 'onclick="' not in p
        home, board, one, _bad = got
        assert "&lt;script&gt;alert(1)&lt;/script&gt;" in home and "&lt;script&gt;alert(1)&lt;/script&gt;" in board
        assert "&lt;script&gt;alert(1)&lt;/script&gt;" in one
        assert 'href="/lab?id=lab-%22%3E%3Cimg%20src%3Dx%20onerror%3Dalert%281%29%3E"' in home   # quoted, not raw

    def test_phone_width_works(self, tmp_path):
        d = a_day(tmp_path)
        put_lab(d.data, made_up_summary())
        (home,) = pages(d, "/advanced")
        assert '<meta name="viewport" content="width=device-width,initial-scale=1">' in home
        assert ".s-checks{display:flex;flex-wrap:wrap;gap:6px;grid-column:1/-1;font-size:12.5px}" in HOME_CSS
        assert (".s-ck.ok{color:var(--good)}.s-ck.no{color:var(--bad)}.s-ck.run{color:var(--info)}"
                ".s-ck.ny{color:var(--text-3)}") in HOME_CSS
        phone = HOME_CSS.split("@media (max-width:760px){")[1].split("\n}\n")[0]
        # on a phone the sidebar becomes a top bar (its links in one row) and the tiles stack, one a line
        assert ".side{position:sticky;top:0;z-index:20;width:100%;height:auto;flex-direction:row;" in phone
        assert ".side nav{flex-direction:row;overflow-x:auto;" in phone
        assert ".bt-grid{grid-template-columns:minmax(0,1fr);" in phone
        assert ".bt-name{font-weight:650;font-size:14px;line-height:1.35;min-width:0;overflow-wrap:anywhere}" in HOME_CSS
        assert ".s-old table{display:block;max-width:100%;overflow-x:auto}" in HOME_CSS
        for cls in (".s-stage", ".s-group", ".s-cname", ".s-sim", ".bt", ".bt-st", ".dot", ".bt-star", ".kpi",
                    ".funnel", ".side", ".nav-label"):
            assert cls + "{" in HOME_CSS or cls + " " in HOME_CSS, cls
        for absent in ("<table", 'class="tile"', 'id="more"', '<div class="hc', 'class="quiet"', "BOT: RUNNING"):
            assert absent not in home, absent
        assert "--bg:#eef4f3" in home and "--side:#0b3b38" in home   # its own teal look (13 Oct), not the AI hub's

    def test_render_functions_never_fail_without_a_data_folder(self):
        page = render_home({"standing": {}}, None, {}, None, "none")
        assert "ideas waiting" in page and ">Not switched on yet<" in page
        assert 'class="bt-star"' not in page
        assert "The lab has not run yet." in render_lab_board("", NOW)
        assert "No such idea." in render_candidate("", A, NOW)
        assert CHECK_LABELS["luck"] == "Luck check"
