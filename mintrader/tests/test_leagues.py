"""Three leagues, permanent careers, the daily snapshot store and live readiness (Aaron, 14 Oct 2026).

EVERY FIGURE HERE IS MADE-UP TEST DATA: the made-up world of tests/test_squad_page.py (a fake broker, made-up
bots' records, real registered lab ideas with made-up practice trades, the runner on DEMO since Monday) and
made-up demo trades. None of it is a result of any bot.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3

import pytest

from mintel.ops import leagues as L
from mintel.ops import readiness as RD
from mintel.ops import squad as SQ
from mintel.ops import weekly as W
from tests.test_squad_page import RESET, RUNNER, RUNNER_DEMO, SINCE, made_up_world

UTC = dt.timezone.utc


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    return made_up_world(tmp_path_factory.mktemp("leagues"), ideas=120)


def positions_of(w):
    return W.live_positions_json(w.d.deals, w.d.sd.get("open_tickets"))["positions"]


def built_of(w, positions="mt", start=RESET, end=None):
    world = SQ.load_world(w.data, None, w.now, RESET, cache_seconds=0)
    pos = positions_of(w) if positions == "mt" else positions
    return world, L.build(world, w.data, pos, start, end or w.now + dt.timedelta(seconds=1))


# ================================================================ the leagues are kept apart --
class TestThreeLeagues:
    def test_each_league_has_its_own_total_and_none_is_summed(self, world):
        tests, b = built_of(world)
        assert set(b) == {L.PAPER, L.DEMO, L.REAL, "stints", "demo_trades", "demo_known"}
        paper, demo, real = b[L.PAPER], b[L.DEMO], b[L.REAL]
        assert paper["total"]["net"] == round(sum(r["net"] for r in paper["rows"]), 2)
        assert demo["total"]["net"] == round(sum(RUNNER_DEMO), 2) == 11.25            # the broker's, after costs
        assert [r["id"] for r in demo["rows"]] == ["bot:runner"] and demo["total"]["active"] == 1
        assert real["rows"] == [] and real["total"] is None and real["connected"] is False
        assert "Not connected yet" in real["words"]
        # the runner's paper record is in the paper league, its demo record in the demo league - never mixed
        prow = next(r for r in paper["rows"] if r["id"] == "bot:runner")
        drow = demo["rows"][0]
        assert prow["net"] != drow["net"] and drow["trades"] == len(RUNNER_DEMO)
        assert prow["net"] == round(sum(t["net"] for t in tests["tests"]["bot:runner"]["trades"]), 2)

    def test_sorted_by_profit_with_the_quality_beside_it(self, world):
        _w, b = built_of(world)
        rows = b[L.PAPER]["rows"]
        assert [r["net"] for r in rows] == sorted((r["net"] for r in rows), reverse=True)
        assert [r["pos"] for r in rows] == list(range(1, len(rows) + 1))
        for r in rows:
            q = r["quality"]
            assert set(q["checks"]) == {"profitable", "consistent", "risk_adjusted", "active"}
            assert r["stars"] == (0 if q["early"] else sum(q["checks"].values()))

    def test_demo_is_unknown_not_zero_until_metatrader_is_read(self, world):
        _w, b = built_of(world, positions=None)
        row = next(r for r in b[L.DEMO]["rows"] if r["id"] == "bot:runner")
        assert row["known"] is False and b[L.DEMO]["total"]["known"] is False
        assert b["demo_known"]["bot:runner"] is False

    def test_a_metatrader_demo_trade_keeps_its_open_time(self, tmp_path):
        # rv14: without it the £1,500 replay's combined cap never saw two demo trades open together
        from mintel.ops import sizing as Z
        stint = {"id": "bot:runner", "kind": "bot", "bot": "runner", "magic": RUNNER, "since": SINCE, "until": None,
                 "risk_gbp": 10.0}
        opened = SINCE + dt.timedelta(hours=3)
        positions = [{"position": 7_000 + i, "magic": RUNNER, "symbol": "US500", "net": 7.5, "commission": -0.5,
                      "opened": opened.isoformat(), "closed": (opened + dt.timedelta(hours=1, minutes=i)).isoformat()}
                     for i in range(6)]                                   # six made-up positions open together
        got, known = L.demo_trades([stint], positions, tmp_path, lab_rows={})
        trades = got["bot:runner"]
        assert known["bot:runner"] is True and len(trades) == 6 and all(t["opened"] == opened for t in trades)
        sim = Z.simulate({"bot:runner": trades}, fallback_risk={"bot:runner": 10.0}, check_min_lot=False)
        assert sim["taken"] == 4 and sim["skipped"] == {"combined cap": 2}           # 4 x 0.5% = the 2% cap

    def test_the_quality_figures(self):
        d = lambda day, h, n: {"closed": dt.datetime(2026, 10, day, h, tzinfo=UTC), "net": n}    # noqa: E731
        trades = [d(12, 9, 10), d(12, 10, -4), d(13, 9, -3), d(14, 9, 8), d(14, 10, 2)] + \
                 [d(15, 9 + i, 1.0) for i in range(6)]
        q = L.quality(trades)
        assert q["net"] == 19.0 and q["days"] == 4 and q["up_days"] == 3 and q["consistency"] == 0.75
        assert q["max_dd"] == 7.0 and q["net_to_dd"] == pytest.approx(19 / 7, abs=0.01)
        assert q["per_day"] == 2.75 and q["stars"] == 4 and not q["early"]
        assert L.quality(trades[:3])["early"] and L.quality(trades[:3])["stars"] == 0
        assert L.max_drawdown([d(12, 9, -5)]) == 5.0

    def test_trades_a_day_count_every_trading_day_in_the_window(self):
        # rv14 R4: 3 made-up winning trades every Monday for four weeks. 3 a week is 0.6 a UK trading day, never
        # 3.0 (12 / the 4 days it traded), so it is not "active" (at least a trade a day): three stars, not four
        mons = [dt.date(2026, 9, 21) + dt.timedelta(weeks=k) for k in range(4)]
        trades = [{"closed": dt.datetime(m.year, m.month, m.day, 9 + i, tzinfo=UTC), "net": 5.0}
                  for m in mons for i in range(3)]
        q = L.quality(trades, dt.datetime(2026, 10, 16, 20, tzinfo=UTC))      # to Fri 16 Oct: 20 trading days
        assert q["days"] == 4 and q["per_day"] == 0.6
        assert q["checks"]["active"] is False and q["stars"] == 3 and not q["early"]
        assert L.quality(trades)["per_day"] == 0.75                 # no end given: Mon 21 Sep to Mon 12 Oct, 16 days
        # a league's rows: the pace over the window's trading days, to the window's end
        from mintel.ops.standing import uk_midnight
        rows = L.league_rows({}, {"bot:w": trades}, RESET - dt.timedelta(days=30), uk_midnight(dt.date(2026, 10, 17)))
        assert rows[0]["quality"]["per_day"] == 0.6 and rows[0]["stars"] == 3

    def test_the_pace_never_stops_before_its_last_trade(self):
        # rv14 S2/S3: a lab idea's last demo position closes after its demote. Made up: 3 trades a UK trading day
        # Mon 5 to Fri 16 Oct, its pace end Fri 9 Oct - counted to its last trade (10 days), not to 9 Oct (5)
        days = [dt.date(2026, 10, 5) + dt.timedelta(days=k) for k in range(12)]
        trades = [{"closed": dt.datetime(d.year, d.month, d.day, 9 + i, tzinfo=UTC), "net": 5.0}
                  for d in days if d.weekday() < 5 for i in range(3)]
        q = L.quality(trades, dt.datetime(2026, 10, 9, 20, tzinfo=UTC))
        assert q["per_day"] == 3.0 == L.quality(trades)["per_day"] and q["checks"]["active"] is True


# ================================================================ careers --
class TestCareers:
    def test_one_id_across_every_league(self, world):
        w, b = built_of(world)
        c = L.career("bot:runner", w, world.data, b)
        assert c["id"] == "bot:runner" and c["nick"] == "Momentum Runner"
        paper, demo, real = (c["leagues"][k] for k in (L.PAPER, L.DEMO, L.REAL))
        assert paper["quality"]["trades"] == len(w["tests"]["bot:runner"]["trades"]) > 0
        assert demo["quality"]["trades"] == 5 and demo["quality"]["net"] == 11.25 and demo["on_demo"]
        assert all(t["source"] == "MetaTrader" for t in demo["trades"])
        assert real["connected"] is False
        assert [(e["kind"], e["why"]) for e in c["timeline"]] == [("promote", "passes the bar")]
        assert c["versions"][0]["version"] == "current rules"
        assert paper["conditions"]["best"] and paper["conditions"]["best"][0]["net"] > 0
        assert paper["risk"]["median"] == 10.0

    def test_a_demotion_closes_the_stint_and_keeps_its_reason(self, world, tmp_path):
        w = SQ.load_world(world.data, None, world.now, RESET, cache_seconds=0)
        cut = SINCE + dt.timedelta(hours=14)                              # after the first three demo trades
        hist = list(w["history"]) + [{"ts_utc": cut.isoformat(), "event": "demote", "id": "bot:runner",
                                      "why": "its live loss reached £100.00 (made up)"}]
        w2 = dict(w, history=hist, promoted=[])
        b = L.build(w2, world.data, positions_of(world), RESET, world.now + dt.timedelta(seconds=1))
        c = L.career("bot:runner", w2, world.data, b)
        demo = c["leagues"][L.DEMO]
        assert demo["quality"]["trades"] == 3 and not demo["on_demo"]
        assert demo["stints"][0]["until"] == cut and "live loss" in demo["stints"][0]["why_out"]
        # its demo pace runs over its time on Demo (to the demotion), not to its last trade nor on to today
        far = L.career("bot:runner", w2, world.data, b, now=world.now + dt.timedelta(days=60))
        assert far["leagues"][L.DEMO]["quality"]["per_day"] == L.quality(demo["trades"], cut)["per_day"]
        assert far["leagues"][L.PAPER]["quality"]["per_day"] == L.quality(
            w["tests"]["bot:runner"]["trades"], world.now + dt.timedelta(days=60))["per_day"]   # still on paper
        assert [e["kind"] for e in c["timeline"]] == ["promote", "demote"]
        assert c["timeline"][1]["why"].startswith("its live loss")

    def test_a_strategy_that_has_left_has_the_same_pace_on_its_league_row_and_its_career(self):
        # rv14 S2: made-up trades, 3 a UK trading day Mon 5 to Fri 16 Oct (30), then it left: a retired paper idea,
        # and a lab idea on Demo 5 to 17 Oct then demoted. Five weeks on, /leagues and the career page agree: its
        # pace stops at the end of its time there, never falling every trading day after it left
        reset = dt.datetime(2026, 10, 4, 23, tzinfo=UTC)                            # 00:00 UK Mon 5 Oct
        days = [dt.date(2026, 10, 5) + dt.timedelta(days=k) for k in range(12)]
        tr = [{"closed": dt.datetime(d.year, d.month, d.day, 9 + i, tzinfo=UTC), "net": 5.0 if i else -1.0,
               "risk": 10.0, "symbol": "EURUSD"} for d in days if d.weekday() < 5 for i in range(3)]
        now = dt.datetime(2026, 11, 20, 12, tzinfo=UTC)
        hist = [{"ts_utc": "2026-10-04T23:30:00+00:00", "event": "promote", "id": "lab:b", "why": "passes the bar"},
                {"ts_utc": "2026-10-17T10:00:00+00:00", "event": "demote", "id": "lab:b", "why": "made up"}]
        world = {"tests": {"lab:a": {"id": "lab:a", "nick": "A", "desc": "", "kind": "lab", "retired": True,
                                     "trades": tr},
                           "lab:b": {"id": "lab:b", "nick": "B", "desc": "", "kind": "lab", "retired": False,
                                     "trades": []},
                           "lab:c": {"id": "lab:c", "nick": "C", "desc": "", "kind": "lab", "retired": False,
                                     "trades": tr}},
                 "promoted": [], "history": hist}
        lab_demo = {"lab:b": [dict(t, ticket=100 + k) for k, t in enumerate(tr)]}
        built = L.build(world, None, [], reset, now + dt.timedelta(seconds=1), lab_demo=lab_demo)
        for cid, league in (("lab:a", L.PAPER), ("lab:b", L.DEMO)):
            row = next(r for r in built[league]["rows"] if r["id"] == cid)
            box = L.career(cid, world, None, built, now=now)["leagues"][league]["quality"]
            assert row["quality"]["per_day"] == box["per_day"] == 3.0
            assert row["quality"]["checks"]["active"] is box["checks"]["active"] is True
            assert row["stars"] == box["stars"] == 4
        # still there (not retired): its pace runs on to the window's end, as before
        c = next(r for r in built[L.PAPER]["rows"] if r["id"] == "lab:c")
        assert c["quality"]["per_day"] == 0.86 and c["stars"] == 3
        snap = L.snapshot_of(world, None, [], dt.date(2026, 11, 20), reset, lab_demo, now)
        assert [(r["id"], r["stars"]) for r in snap["demo"]] == [("lab:b", 4)]

    def test_a_lab_idea_keeps_its_versions_and_a_retired_one_its_career(self, world):
        w, b = built_of(world)
        retired = next(t for t in w["tests"].values() if t["kind"] == "lab" and t["retired"])
        c = L.career(retired["id"], w, world.data, b)
        assert c["retired"] and c["version"].startswith("v")
        assert any(e["kind"] == "retired" for e in c["timeline"])
        assert c["leagues"][L.DEMO] is None and c["leagues"][L.PAPER] is not None
        assert L.career("lab:nope", w, world.data, b) is None

    def test_the_lab_demo_record_is_read_whatever_its_shape(self, world, tmp_path):
        assert L.lab_demo_trades(tmp_path) == {}                        # nothing there: nothing
        w = SQ.load_world(world.data, None, world.now, RESET, cache_seconds=0)
        idea = next(t for t in w["tests"].values() if t["kind"] == "lab")
        db = world.data / "lab" / "lab.sqlite"
        con = sqlite3.connect(db)
        con.execute("CREATE TABLE IF NOT EXISTS lab_demo_trades(id INTEGER PRIMARY KEY, idea_id TEXT, version INT, "
                    "symbol TEXT, opened_utc TEXT, closed_utc TEXT, net_gbp REAL, risk_gbp REAL, net_1500_gbp REAL)")
        t = dt.datetime(2026, 10, 13, 9, tzinfo=UTC)
        con.executemany("INSERT INTO lab_demo_trades(idea_id, version, symbol, opened_utc, closed_utc, net_gbp, "
                        "risk_gbp, net_1500_gbp) VALUES(?,?,?,?,?,?,?,?)",
                        [(idea["ref"], 1, "EURUSD", (t + dt.timedelta(minutes=k)).isoformat(),
                          (t + dt.timedelta(minutes=k + 5)).isoformat(), n, 9.0, n * 0.8)
                         for k, n in enumerate((4.0, -2.0, 3.5))])
        con.commit()
        con.close()
        try:
            got = L.lab_demo_trades(world.data)[idea["id"]]
            assert [x["net"] for x in got] == [4.0, -2.0, 3.5] and got[0]["equiv_1500"] == pytest.approx(3.2)
            b = L.build(w, world.data, positions_of(world), RESET, world.now + dt.timedelta(seconds=1))
            row = next(r for r in b[L.DEMO]["rows"] if r["id"] == idea["id"])
            assert row["net"] == 5.5 and row["trades"] == 3
            paper_row = next((r for r in b[L.PAPER]["rows"] if r["id"] == idea["id"]), None)
            assert paper_row is None or paper_row["net"] != 5.5             # its paper record stays its own
        finally:
            con = sqlite3.connect(db)
            con.execute("DROP TABLE lab_demo_trades")
            con.commit()
            con.close()


    def test_the_lab_demo_engines_record_by_idea_and_version(self, tmp_path):
        """data/labdemo/labdemo.sqlite (the lab-demo engine's permanent record, its own schema made up here):
        the broker's net by ticket, read once - never also by the idea's magic number."""
        w = made_up_world(tmp_path, ideas=20)
        world = SQ.load_world(w.data, None, w.now, RESET, cache_seconds=0)
        idea = next(t for t in world["tests"].values() if t["kind"] == "lab")
        (w.data / "labdemo").mkdir()
        con = sqlite3.connect(w.data / "labdemo" / "labdemo.sqlite")
        con.execute("CREATE TABLE demo_trades(ticket INTEGER PRIMARY KEY, idea_id TEXT NOT NULL, version INT NOT NULL, "
                    "magic INT NOT NULL, symbol TEXT, volume REAL, opened_utc TEXT, closed_utc TEXT, net_gbp REAL NOT "
                    "NULL, commission_gbp REAL, risk_gbp REAL NOT NULL, tier_pct REAL, r REAL, equiv_1500_gbp REAL, "
                    "mode TEXT DEFAULT 'DEMO')")
        t = dt.datetime(2026, 10, 13, 8, tzinfo=UTC)
        con.executemany("INSERT INTO demo_trades(ticket, idea_id, version, magic, symbol, volume, opened_utc, closed_utc,"
                        " net_gbp, commission_gbp, risk_gbp, tier_pct, r, equiv_1500_gbp) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,"
                        "?,?)", [(70 + k, idea["ref"], 1, 992_001, "EURUSD", 0.1, (t + dt.timedelta(hours=k)).isoformat(),
                                  (t + dt.timedelta(hours=k, minutes=20)).isoformat(), n, -0.7, 18.4, 1.0, n / 18.4,
                                  round(n / 18.4 * 7.5, 2)) for k, n in enumerate((9.2, -18.4, 13.8))])
        con.commit()
        con.close()
        promo = {"ts_utc": "2026-10-13T05:30:00+00:00", "event": "promote", "id": idea["id"], "kind": "lab",
                 "magic": 992_001, "why": "passes the bar (made up)", "version": 1}
        world = dict(world, history=list(world["history"]) + [promo])
        b = L.build(world, w.data, positions_of(w), RESET, w.now + dt.timedelta(seconds=1))
        row = next(r for r in b[L.DEMO]["rows"] if r["id"] == idea["id"])
        assert row["net"] == 4.6 and row["trades"] == 3 and row["known"]
        got = b["demo_trades"][idea["id"]]
        assert got[0]["source"] == "the broker, by ticket" and got[0]["equiv_1500"] == 3.75 and got[0]["costs"] == 0.7
        c = L.career(idea["id"], world, w.data, b)
        assert c["leagues"][L.DEMO]["quality"]["net"] == 4.6 and c["leagues"][L.DEMO]["risk"]["median"] == 18.4
        assert c["leagues"][L.PAPER]["quality"]["net"] != 4.6                    # its paper record stays its own
        (w.data / "labdemo" / "labdemo.sqlite").write_text("not a database")
        b2 = L.build(world, w.data, positions_of(w), RESET, w.now + dt.timedelta(seconds=1))
        assert b2["demo_known"][idea["id"]] is False                               # unreadable: unknown, not zero


# ================================================================ the daily snapshot store --
class TestSnapshots:
    def test_written_once_never_rewritten(self, tmp_path):
        day = dt.date(2026, 10, 12)
        assert L.write_snapshot(tmp_path, day, {"paper": [{"id": "a", "pos": 1}]})
        before = L.snapshot_path(tmp_path, day).read_text()
        assert not L.write_snapshot(tmp_path, day, {"paper": [{"id": "b", "pos": 1}]})
        assert L.snapshot_path(tmp_path, day).read_text() == before
        assert L.read_snapshots(tmp_path)[0]["paper"][0]["id"] == "a"

    def test_the_pulse_fills_in_the_finished_days_once(self, tmp_path):
        w = made_up_world(tmp_path, ideas=40)
        pos = positions_of(w)
        done = L.daily_snapshots(w.data, None, w.now, pos)
        assert done == ["2026-10-05", "2026-10-06", "2026-10-07", "2026-10-08", "2026-10-09", "2026-10-12"]
        files = {p.name: p.read_text() for p in (w.data / "leagues").iterdir()}
        assert L.daily_snapshots(w.data, None, w.now, pos) == []                       # nothing twice
        assert {p.name: p.read_text() for p in (w.data / "leagues").iterdir()} == files
        snaps = L.read_snapshots(w.data)
        mon = snaps[-1]
        assert mon["date"] == "2026-10-12" and mon["real"]["connected"] is False
        assert [r["id"] for r in mon["demo"]] == ["bot:runner"]                         # its Monday demo trades
        # closed by the end of Monday UK: the fourth closes at 00:30 UK on Tuesday
        assert mon["demo"][0]["net"] == round(sum(RUNNER_DEMO[:3]), 2)
        assert mon["totals"]["paper"]["net"] != mon["totals"]["demo"]["net"]
        assert [r["pos"] for r in mon["paper"]] == list(range(1, len(mon["paper"]) + 1))
        # the career reads its daily positions back by the same id
        world = SQ.load_world(w.data, None, w.now, RESET, cache_seconds=0)
        c = L.career("bot:runner", world, w.data, snapshots=snaps, positions=pos, now=w.now)
        assert [r["date"] for r in c["ranks"]][-1] == "2026-10-12" and c["ranks"][-1]["demo"] == 1

    def test_an_unknown_demo_is_never_frozen_into_a_snapshot(self, tmp_path):
        w = made_up_world(tmp_path, ideas=20)
        assert L.daily_snapshots(w.data, None, w.now, None) == [d for d in
                                                                ("2026-10-05", "2026-10-06", "2026-10-07",
                                                                 "2026-10-08", "2026-10-09")]
        assert not L.snapshot_path(w.data, dt.date(2026, 10, 12)).exists()            # the runner was on DEMO

    def test_a_late_snapshot_counts_that_days_demo_set(self, world):
        # rv14: the runner went on DEMO on Mon 12 Oct; Thursday's snapshot, written late, must not count it
        w = SQ.load_world(world.data, None, world.now, RESET, cache_seconds=0)
        thu = L.snapshot_of(w, world.data, positions_of(world), dt.date(2026, 10, 8), RESET)
        assert thu["demo"] == []
        assert thu["totals"]["demo"]["active"] == 0 and thu["totals"]["demo"]["strategies"] == 0
        unread = L.snapshot_of(w, world.data, None, dt.date(2026, 10, 8), RESET)
        assert unread["totals"]["demo"]["known"] is True and unread["totals"]["demo"]["active"] == 0
        mon = L.snapshot_of(w, world.data, positions_of(world), dt.date(2026, 10, 12), RESET)
        assert mon["totals"]["demo"]["active"] == 1 and [r["id"] for r in mon["demo"]] == ["bot:runner"]

    def test_nothing_in_the_store_removes_a_file(self):
        import inspect
        src = inspect.getsource(L)
        for bad in ("unlink(", "os.remove(", "rmtree(", "os.replace(", ".rename("):
            assert bad not in src, bad


# ================================================================ live readiness --
def demo_run(n: int, days: int, win: float = 6.0, loss: float = -4.0, every: int = 3, start=dt.date(2026, 9, 1)):
    """``n`` made-up demo trades spread over ``days`` UK trading days: every ``every``-th one a loss."""
    out, d, k = [], start, 0
    per = max(1, -(-n // days))
    while k < n:
        if d.weekday() < 5:
            for i in range(per):
                if k >= n:
                    break
                out.append({"closed": dt.datetime(d.year, d.month, d.day, 9 + i, tzinfo=UTC),
                            "net": loss if k % every == every - 1 else win, "symbol": "US500"})
                k += 1
        d += dt.timedelta(days=1)
    return out


TODAY = dt.date(2026, 10, 14)
PAPER = demo_run(40, 8, 5.0, -3.0, start=dt.date(2026, 8, 3))


class TestReadiness:
    def test_the_stages(self):
        assert RD.assess("bot:x", demo_run(8, 3), PAPER, 10.0, TODAY)["stage"] == RD.NONE
        a = RD.assess("bot:x", demo_run(22, 5), PAPER, 10.0, TODAY)
        assert a["stage"] == RD.APPROACHING and a["earliest"] != "no estimate yet"
        p = RD.assess("bot:x", demo_run(32, 8), PAPER, 10.0, TODAY)
        assert p["stage"] == RD.PREPARATION and p["checks"]["lcb"] and p["checks"]["drawdown"]
        q = RD.assess("bot:x", demo_run(45, 12), PAPER, 10.0, TODAY)
        assert q["stage"] == RD.QUALIFIED and q["justified"] and q["needed"] == []
        assert q["earliest"] == "now, once you approve" and q["sim_1500"]["max_dd_pct"] <= 10

    def test_not_qualified_when_losing_or_paper_disagrees_or_risk_unknown(self):
        losing = RD.assess("bot:x", demo_run(45, 12, 3.0, -5.0, every=2), PAPER, 10.0, TODAY)
        assert losing["stage"] == RD.NONE and not losing["justified"]
        bad_paper = demo_run(40, 8, 2.0, -6.0, every=2)
        dis = RD.assess("bot:x", demo_run(45, 12), bad_paper, 10.0, TODAY)
        assert dis["stage"] != RD.QUALIFIED and not dis["checks"]["agree"]
        no_risk = RD.assess("bot:x", demo_run(45, 12), PAPER, None, TODAY)
        assert no_risk["stage"] != RD.QUALIFIED and "its risk a trade on record" in no_risk["needed"]

    def test_too_early_has_no_date(self):
        a = RD.assess("bot:x", demo_run(21, 3), PAPER, 10.0, TODAY)
        assert a["stage"] == RD.APPROACHING and a["earliest"] == "no estimate yet"

    def test_the_earliest_date_is_at_its_real_pace(self):
        # rv14: 30 made-up trades on five days a week apart - it trades on one UK trading day in four, so the five
        # more trading days it needs take about 20 trading days, not 5
        days = [dt.date(2026, 9, 17), dt.date(2026, 9, 24), dt.date(2026, 10, 1), dt.date(2026, 10, 8),
                dt.date(2026, 10, 13)]
        demo = [{"closed": dt.datetime(d.year, d.month, d.day, 9 + i, tzinfo=UTC), "net": -2.0 if i % 3 == 0 else 5.0,
                 "symbol": "US500"} for d in days for i in range(6)]
        a = RD.assess("bot:x", demo, PAPER, 10.0, TODAY)
        assert a["stage"] == RD.APPROACHING
        assert a["earliest"] == RD.add_trading_days(TODAY, 20).isoformat()            # not 2026-10-21

    def test_an_unread_metatrader_keeps_the_last_answer(self, world, tmp_path, monkeypatch):
        # rv14: an outage used to turn a qualified bot into "none" and message both ways
        w, b = built_of(world)
        b = dict(b, demo_trades={"bot:runner": demo_run(45, 12)}, demo_known={"bot:runner": True},
                 stints=[dict(s, risk_gbp=10.0) for s in b["stints"]])
        w = dict(w, tests=dict(w["tests"], **{"bot:runner": dict(w["tests"]["bot:runner"], trades=PAPER)}))
        monkeypatch.setattr(SQ, "load_world", lambda *a, **k: w)
        monkeypatch.setattr(L, "build", lambda *a, **k: b)
        first = RD.refresh(tmp_path, None, world.now)
        assert first["stage"] == RD.QUALIFIED and first["notify"] is True
        RD.mark_notified(tmp_path)
        monkeypatch.setattr(L, "build", lambda *a, **k: dict(b, demo_known={"bot:runner": False}))  # MT unread
        out = RD.refresh(tmp_path, None, world.now)
        assert out["stage"] == RD.QUALIFIED and out["notify"] is False and out["fingerprint"] == first["fingerprint"]
        assert out["bots"][0]["stale"] is True and "No strategy is near real money" not in out["message"]
        again = RD.refresh(tmp_path, None, world.now)                                # still unread: still kept
        assert again["stage"] == RD.QUALIFIED and again["notify"] is False
        monkeypatch.setattr(L, "build", lambda *a, **k: b)                           # MetaTrader back
        back = RD.refresh(tmp_path, None, world.now)
        assert back["stage"] == RD.QUALIFIED and back["notify"] is False and not back["bots"][0].get("stale")

    def _content(self, bots):
        stage = bots[0]["stage"] if bots else RD.NONE
        return {"stage": stage, "bots": bots, "fingerprint": RD.fingerprint(stage, bots), "headline": "",
                "message": ""}

    def test_notify_only_on_a_meaningful_change(self, tmp_path):
        quiet = RD.write(tmp_path, self._content([]))
        assert quiet["notify"] is False and quiet["notified"] is None                 # a quiet start: no message
        bot = {"id": "lab:abc", "version": "v1", "stage": RD.APPROACHING, "demo": {"net": 10.0}}
        first = RD.write(tmp_path, self._content([bot]))
        assert first["notify"] is True
        assert RD.mark_notified(tmp_path)["notify"] is False
        again = RD.write(tmp_path, self._content([dict(bot, demo={"net": 55.0})]))   # only the numbers moved
        assert again["notify"] is False and again["notified"]["fingerprint"] == first["fingerprint"]
        up = RD.write(tmp_path, self._content([dict(bot, stage=RD.QUALIFIED)]))
        assert up["notify"] is True
        RD.mark_notified(tmp_path)
        more = RD.write(tmp_path, self._content([dict(bot, stage=RD.QUALIFIED),
                                                 {"id": "bot:runner", "version": "current rules",
                                                  "stage": RD.APPROACHING}]))
        assert more["notify"] is True
        saved = json.loads((tmp_path / RD.FILE).read_text())
        assert saved["notify"] is True and saved["notified"]["stage"] == RD.QUALIFIED

    def test_the_file_has_what_the_message_needs(self, world, tmp_path):
        w, b = built_of(world)
        stints = b["stints"]
        b = dict(b, demo_trades={"bot:runner": demo_run(45, 12)}, demo_known={"bot:runner": True})
        b["stints"] = [dict(s, risk_gbp=10.0) for s in stints]
        w = dict(w, tests=dict(w["tests"], **{"bot:runner": dict(w["tests"]["bot:runner"], trades=PAPER)}))
        rev = RD.review(w, b, world.data, world.now)
        assert rev["stage"] == RD.QUALIFIED
        bot = rev["bots"][0]
        for k in ("id", "version", "nick", "stage", "paper", "demo", "promising", "needed", "justified",
                  "justified_words", "earliest", "fingerprint"):
            assert k in bot, k
        assert bot["id"] == "bot:runner" and bot["version"] == "current rules"
        assert rev["headline"].startswith("Live readiness: Momentum Runner") and "command line" in rev["message"]
        out = RD.write(tmp_path, rev)
        assert out["notify"] is True and out["real_trading_enabled"] is False
        from mintel.ops.pulse import readiness_json
        published = json.loads(readiness_json(out))
        assert published["stage"] == RD.QUALIFIED and published["notify"] is True
