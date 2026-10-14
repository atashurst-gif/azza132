"""13 Oct: the weekly pick (mintel/ops/weekly.py) and Home's headline numbers.

14 Oct (intended changes, Aaron): the pick is made every UK trading day at 22:30
and released 06:30 the next trading day into the demo's free places
(demo_target, default 10); the early-promotion bar (10/12/15 trades by
frequency over 3+ days, PF 1.3, shrinkage, the control benchmark) replaces the
30-trade bar; the demo judgement is evidence-based (bootstrap bounds after 20,
net after 40, 15x risk, a drawdown guard); lab ideas have a demo path. The
tests whose expectations changed say so; every safeguard test (veto, HALT,
intents, put-back, strict reads, never re-released by the same plan) is
unchanged. The new rules have their own tests in tests/test_promotion_rules.py
and tests/test_lab_demo.py.

Aaron: "put everything out of live and into the lab, once they begin proving
move the best each week into real money and live"; "i want to see headline
numbers on home somehow of all the well performing ones etc.. not a load of
writing"; "they just become part of all of the other tests".

A. the proof bar and the luck check;
B. the ranking;
C. Friday's pick, Monday's release, BST and GMT (the clocks go back on 25 Oct 2026);
D. one promotion a week, the veto, HALT;
E. a lab idea with no live path; a Trend & Breakout part that would widen the
   cross product into an unproven combination;
F. the live judgement: 20 losing live trades, the early exit;
G. idempotent across a restart; the watchdog's restart;
H. Home with and without weekly data; the page's /weekly-live; the pulse.

EVERY FIGURE HERE IS MADE-UP TEST DATA, in temporary folders: nothing here
measures performance."""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path

import pytest

from mintel.ops import weekly as W

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 10, 5, 8, 0, tzinfo=UTC)          # Monday 5 Oct, 09:00 UK
FRI_16 = dt.date(2026, 10, 16)
FRI_23 = dt.date(2026, 10, 23)
PICK_16 = dt.datetime(2026, 10, 16, 21, 30, tzinfo=UTC)   # 22:30 BST
REL_19 = dt.datetime(2026, 10, 19, 5, 30, tzinfo=UTC)     # Monday 06:30 BST
MC, TP = "MOMENTUM_CONTINUATION", "TREND_PULLBACK"


# ------------------------------------------------------------- made-up data --
def trades_of(nets_by_day: list[list[float]], start: dt.datetime = T0, cost: float = 0.5) -> list[dict]:
    """Made-up trades in verdicts' shape, one list of nets per UK day."""
    out, k = [], 0
    for d, nets in enumerate(nets_by_day):
        for i, n in enumerate(nets):
            k += 1
            out.append({"closed": start + dt.timedelta(days=d, minutes=10 * i), "net": float(n), "costs": cost,
                        "symbol": "EURGBP", "ticket": k, "kind": "practice"})
    return out


def good_days(days: int = 10, per_day: int = 4) -> list[list[float]]:
    return [[6.0] * (per_day - 1) + [-4.0] for _ in range(days)]


def a_folder(tmp_path: Path, **modes) -> tuple[Path, Path]:
    data = tmp_path / "data"
    data.mkdir()
    raw = {"ops": {"data_dir": str(data), "dashboard_port": 1}, "tnb": {"mode": "PAPER"},
           "runner": {"mode": modes.get("runner", "PAPER")}, "bandbreaker": {"mode": "PAPER"},
           "crowd": {"mode": "PAPER"}}
    (data / "config.json").write_text(json.dumps(raw))
    return data, data / "config.json"


def own_db(data: Path, name: str, nets_by_day: list[list[float]], start: dt.datetime = T0, mode: str = "PAPER",
           first_ticket: int = 1) -> None:
    """A bot's own record (runner.sqlite and the like), made up."""
    con = sqlite3.connect(data / name)
    con.execute("CREATE TABLE IF NOT EXISTS trades(ticket INTEGER, symbol TEXT, side TEXT, opened_utc TEXT, "
                "closed_utc TEXT, net_pnl REAL, commission REAL, risk_money REAL, mode TEXT, exit_reason TEXT)")
    k = first_ticket
    for d, nets in enumerate(nets_by_day):
        for i, n in enumerate(nets):
            o = start + dt.timedelta(days=d, minutes=30 * i)
            con.execute("INSERT INTO trades VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (k, "US500", "BUY", o.isoformat(), (o + dt.timedelta(minutes=20)).isoformat(), n, 0.5, 10.0,
                         mode, "TP"))
            k += 1
    con.commit()
    con.close()


def lab_db(data: Path, cid: str, nets_by_day: list[list[float]], stage: str = "READY FOR LIVE",
           name: str = "Opening range, minor pairs") -> None:
    from mintel.lab.ledger import LabLedger
    led = LabLedger(data / "lab" / "lab.sqlite")
    led.execute("INSERT INTO candidates(id, family, version, def_json, name, stage) VALUES(?,?,?,?,?,?)",
                (cid, "ORB", 1, "{}", name, stage))
    k = 0
    for d, nets in enumerate(nets_by_day):
        for i, n in enumerate(nets):
            k += 1
            c = T0 + dt.timedelta(days=d, minutes=15 * i)
            led.execute("INSERT INTO practice_trades(ticket, candidate_id, symbol, side, closed_utc, net_gbp, "
                        "commission_gbp, risk_gbp, fill_source) VALUES(?,?,?,?,?,?,?,?,?)",
                        (hash((cid, k)) % 10_000_000, cid, "EURGBP", 1, c.isoformat(), n, 0.4, 13.0, "bridge"))
    led.close()


def config(path: Path) -> dict:
    return json.loads(path.read_text())


# ================================================== A. the bar and the luck --
class TestTheBar:
    def test_a_steady_record_passes(self):
        j = W.judge(trades_of(good_days()), "bot:runner", T0 + dt.timedelta(days=11))
        assert j["verdict"] == "PROVEN" and j["luck"] <= W.LUCK_MAX and j["passes"]
        # 14 Oct (intended change): the score is the shrunk R a trade x trades a week (x the lab-test weight)
        assert j["lcb"] > 0 and j["shrunk_r"] > 0
        assert j["score"] == round(j["shrunk_r"] * j["per_week"] * j["weight"], 3)

    def test_too_few_trades_or_days_is_not_proven(self):
        # 14 Oct (intended change): 32 trades over 4 days used to be too few days (5 were needed); the early bar
        # asks for 3 UK days and 15 trades for a frequent test, so it now passes
        assert W.judge(trades_of(good_days(days=4, per_day=8)), "x", T0 + dt.timedelta(days=5))["passes"]
        j = W.judge(trades_of(good_days(days=2, per_day=8)), "x", T0 + dt.timedelta(days=2))   # 16 trades, 2 days
        assert not j["passes"] and "more UK day" in j["why"]
        j = W.judge(trades_of(good_days(days=3, per_day=4)), "x", T0 + dt.timedelta(days=2, hours=12))
        assert j["per_day"] >= 3 and not j["passes"] and "3 more trades (15 for a frequent test)" in j["why"]

    def test_a_short_winning_streak_is_not_proof(self):
        # 14 Oct (intended change; it was "proven by the rule but it could be luck"): 30 trades over 5 days, 3 big
        # wins and 27 small losses - a profit factor of 1.2 is under 1.3, and the profit hangs on one trade
        days = [[100.0] + [-250 / 27] * 5 for _ in range(3)] + [[-250 / 27] * 6 for _ in range(2)]
        j = W.judge(trades_of(days), "luck", T0 + dt.timedelta(days=6))
        assert j["luck"] > W.LUCK_MAX and not j["passes"]
        assert "profit factor 1.20, under 1.3" in j["why"] and "hangs on one trade" in j["why"]

    def test_the_luck_check_is_fixed_by_its_seed(self):
        nets = [3.0, -2.0, 4.0, -1.0, 2.5, -3.0] * 6
        assert W.bootstrap(nets, "bot:crowd") == W.bootstrap(list(nets), "bot:crowd")
        p, lcb = W.bootstrap([-1.0] * 40, "losing")
        assert p == 1.0 and lcb < 0
        assert W.bootstrap([1.0], "one") == (1.0, None)

    def test_only_the_last_28_days_count(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        own_db(data, "runner.sqlite", good_days(days=10), start=T0)          # 5 to 14 Oct
        now = T0 + dt.timedelta(days=40)                                     # 14 Nov: all of it is stale
        assert W.candidates(data, cfg, now) == []
        assert W.candidates(data, cfg, T0 + dt.timedelta(days=11))[0]["passes"]

    def test_after_a_demotion_only_newer_practice_counts(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        own_db(data, "runner.sqlite", good_days(days=10))
        hist = [{"event": "demote", "id": "bot:runner", "ts_utc": (T0 + dt.timedelta(days=8)).isoformat()}]
        c = W.candidates(data, cfg, T0 + dt.timedelta(days=11), hist)[0]
        assert c["stats"]["trades"] == 8 and not c["passes"]


# ============================================================ B. the ranking --
class TestRanking:
    def test_qualifying_by_score_then_the_rest_by_money(self):
        def c(cid, passes, score, net):
            return {"id": cid, "passes": passes, "score": score, "stats": {"net": net}}
        out = W.rank([c("a", True, 10.0, 500.0), c("b", True, 40.0, 100.0), c("c", False, None, 900.0),
                      c("d", False, 5.0, -20.0)])
        assert [x["id"] for x in out] == ["b", "a", "c", "d"]

    def test_steadier_and_busier_ranks_higher(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        own_db(data, "runner.sqlite", good_days(days=10, per_day=6))           # more trades a week
        own_db(data, "bandbreaker.sqlite", good_days(days=10, per_day=4))
        own_db(data, "crowd.sqlite", [[-3.0, -2.0, 1.0]] * 10)                 # losing
        r = W.candidates(data, cfg, T0 + dt.timedelta(days=11))
        assert [x["id"] for x in r] == ["bot:runner", "bot:bandbreaker", "bot:crowd"]
        assert r[0]["score"] > r[1]["score"] > 0 and not r[2]["passes"]


# ================================================= C. Friday, Monday, BST/GMT --
class TestTiming:
    def test_friday_2230_uk_and_monday_0630_uk(self):
        # 14 Oct (intended change): every UK trading day picks at 22:30; Friday's still releases on Monday
        assert W.uk_at(FRI_16, W.PICK_AT) == PICK_16
        assert W.release_utc(FRI_16) == REL_19
        assert W.last_pick_day(PICK_16 - dt.timedelta(minutes=1)) == dt.date(2026, 10, 15)      # Thursday's
        assert W.last_pick_day(PICK_16) == FRI_16
        assert W.last_pick_day(dt.datetime(2026, 10, 18, 12, tzinfo=UTC)) == FRI_16              # the weekend
        assert W.release_utc(dt.date(2026, 10, 14)) == dt.datetime(2026, 10, 15, 5, 30, tzinfo=UTC)

    def test_the_clocks_go_back_on_25_october(self):
        # Friday 23 Oct is still BST (22:30 = 21:30 UTC); Monday 26 Oct is GMT (06:30 = 06:30 UTC)
        assert W.uk_at(FRI_23, W.PICK_AT) == dt.datetime(2026, 10, 23, 21, 30, tzinfo=UTC)
        assert W.release_utc(FRI_23) == dt.datetime(2026, 10, 26, 6, 30, tzinfo=UTC)
        # and the next pick, Monday 26 Oct, is GMT: 22:30 = 22:30 UTC (14 Oct: daily, it was the Friday)
        assert W.next_pick_utc(dt.datetime(2026, 10, 26, 7, 0, tzinfo=UTC)) == dt.datetime(2026, 10, 26, 22, 30,
                                                                                           tzinfo=UTC)

    def test_the_tick_picks_on_friday_and_releases_on_monday(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        own_db(data, "runner.sqlite", good_days(days=10, per_day=4), start=T0 + dt.timedelta(days=7))
        before = dt.datetime(2026, 10, 23, 21, 29, tzinfo=UTC)
        assert W.tick(data, cfg, before) == [] and not (data / "weekly" / "latest.json").exists()
        acts = W.tick(data, cfg, dt.datetime(2026, 10, 23, 21, 31, tzinfo=UTC))
        assert "Momentum Runner" in acts[0]
        plan = json.loads((data / "weekly" / "plan-2026-10-23.json").read_text())
        assert plan["status"] == W.PLANNED and plan["release_utc"] == "2026-10-26T06:30:00+00:00"
        assert plan == json.loads((data / "weekly" / "latest.json").read_text())
        # Monday 06:29 GMT: not yet; 06:30: released
        assert W.tick(data, cfg, dt.datetime(2026, 10, 26, 6, 29, tzinfo=UTC)) == []
        assert config(cfg)["runner"]["mode"] == "PAPER"
        W.tick(data, cfg, dt.datetime(2026, 10, 26, 6, 30, tzinfo=UTC))
        assert config(cfg)["runner"]["mode"] == "LIVE"
        ev = W.history(data)
        assert [e["event"] for e in ev] == ["promote"] and ev[0]["week"] == "2026-W44"

    def test_a_watchdog_down_on_friday_catches_up_until_the_release(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        own_db(data, "runner.sqlite", good_days(), start=T0 + dt.timedelta(days=1))
        W.tick(data, cfg, dt.datetime(2026, 10, 18, 12, 0, tzinfo=UTC))           # Sunday
        assert (data / "weekly" / "plan-2026-10-16.json").exists()

    def test_no_plan_made_after_its_release_time(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        own_db(data, "runner.sqlite", good_days(), start=T0 + dt.timedelta(days=1))
        assert W.tick(data, cfg, REL_19 + dt.timedelta(hours=3)) == []            # a fresh install on Monday
        assert not (data / "weekly").exists() or not list((data / "weekly").glob("plan-*"))


# ============================================== D. one a week, veto, HALT --
def planned(tmp_path, **modes):
    data, cfg = a_folder(tmp_path, **modes)
    own_db(data, "runner.sqlite", good_days(), start=T0 + dt.timedelta(days=1))
    W.tick(data, cfg, PICK_16 + dt.timedelta(minutes=5))
    return data, cfg


class TestWeekRules:
    def test_a_full_demo_squad_takes_no_more(self, tmp_path):
        # 14 Oct (intended change; it was "at most X promotions a week"): the pick fills the demo's free places,
        # demo_target in data/weekly.json (default 10); with every place taken, nothing is released
        data, cfg = planned(tmp_path)
        (data / "weekly.json").write_text(json.dumps({"demo_target": 1}))
        a_state(data, {"id": "bot:crowd", "kind": "bot", "label": "Crowd Fader", "bot": "crowd", "magic": 990_711,
                       "process": "trader", "plan": "2026-10-09", "since_utc": T0.isoformat()})
        raw = config(cfg)
        raw["crowd"]["mode"] = "LIVE"
        cfg.write_text(json.dumps(raw))
        acts = W.tick(data, cfg, REL_19)
        assert "the demo squad is full (1 of 1 places)" in acts[0] and config(cfg)["runner"]["mode"] == "PAPER"
        assert json.loads((data / "weekly" / "latest.json").read_text())["status"] == W.SKIPPED

    def test_a_free_place_is_filled(self, tmp_path):
        data, cfg = planned(tmp_path)
        (data / "weekly" / "history.jsonl").write_text(json.dumps(
            {"ts_utc": "2026-10-19T05:00:00+00:00", "event": "promote", "id": "bot:crowd", "week": "2026-W43"}) + "\n")
        W.tick(data, cfg, REL_19)
        assert config(cfg)["runner"]["mode"] == "LIVE"
        (data / "weekly.json").write_text(json.dumps({"demo_target": 8}))
        assert W.demo_target(data) == 8
        (data / "weekly.json").write_text(json.dumps({"demo_target": 99}))
        assert W.demo_target(data) == W.DEMO_TARGET                           # never more than the cap allows
        (data / "weekly.json").write_text(json.dumps({"weekly_max_promotions": 1}))
        assert W.max_promotions(data) == 1 and W.demo_target(data) == W.DEMO_TARGET   # the old setting still reads

    def test_veto_before_the_release(self, tmp_path):
        data, cfg = planned(tmp_path)
        assert W.veto(data).startswith("Cancelled: Momentum Runner will not go live")
        W.tick(data, cfg, REL_19 - dt.timedelta(hours=1))
        W.tick(data, cfg, REL_19 + dt.timedelta(minutes=1))
        assert config(cfg)["runner"]["mode"] == "PAPER"
        assert json.loads((data / "weekly" / "latest.json").read_text())["status"] == W.VETOED

    def test_veto_after_the_release_takes_it_back(self, tmp_path):
        data, cfg = planned(tmp_path)
        W.tick(data, cfg, REL_19)
        assert config(cfg)["runner"]["mode"] == "LIVE"
        assert "taken back" in W.veto(data)
        W.tick(data, cfg, REL_19 + dt.timedelta(minutes=10))
        assert config(cfg)["runner"]["mode"] == "PAPER"
        assert [e["event"] for e in W.history(data)] == ["promote", "demote"]
        assert W.load_state(data)["promoted"] == []

    def test_no_pick_to_veto(self, tmp_path):
        assert W.veto(tmp_path) == "There is no pick to cancel."

    def test_halt_means_nothing_is_applied(self, tmp_path):
        data, cfg = planned(tmp_path)
        (data / "HALT").write_text("")
        acts = W.tick(data, cfg, REL_19)
        assert "HALT" in acts[0] and config(cfg)["runner"]["mode"] == "PAPER"
        assert W.tick(data, cfg, REL_19 + dt.timedelta(minutes=5)) == []          # said once
        (data / "HALT").unlink()
        W.tick(data, cfg, REL_19 + dt.timedelta(minutes=10))
        assert config(cfg)["runner"]["mode"] == "LIVE"

    def test_a_pick_waiting_on_halt_lapses_at_the_next_pick(self, tmp_path):
        data, cfg = planned(tmp_path)
        (data / "HALT").write_text("")
        W.tick(data, cfg, REL_19)
        W.tick(data, cfg, dt.datetime(2026, 10, 23, 21, 31, tzinfo=UTC))           # the next Friday's pick
        assert json.loads((data / "weekly" / "plan-2026-10-16.json").read_text())["status"] == W.EXPIRED


# ===================================== E. no live path; the cross product --
class TestLiveEdges:
    def test_the_best_is_a_lab_idea_and_it_now_goes_first(self, tmp_path):
        # 14 Oct (intended change): a lab idea has a demo path (the lab-demo roster), so the best one is picked
        # first; it used to be "picked, needs a live path" and passed over
        data, cfg = a_folder(tmp_path)
        own_db(data, "runner.sqlite", good_days(days=10, per_day=4))
        lab_db(data, "lab-0123456789", good_days(days=10, per_day=8))         # twice as busy: the best
        plan = W.make_plan(data, cfg, T0 + dt.timedelta(days=11), FRI_16)
        assert plan["ranking"][0]["id"] == "lab:lab-0123456789"
        assert plan["lab_pick"]["id"] == "lab:lab-0123456789"
        assert [x["pick"]["id"] for x in plan["picks"]] == ["lab:lab-0123456789", "bot:runner"]
        assert not any(W.NEEDS_LIVE_PATH in r for r in plan["reasons"])

    def test_a_lab_idea_still_practising_is_a_candidate_on_its_own_record(self, tmp_path):
        # 13 Oct (intended change): every lab idea practises and is its own test in the ranking, whatever its stage
        # ("I just want all in practice unless any earn the right"); it used to need READY FOR LIVE. The proof bar
        # on its own practice trades is what stops luck now. 14 Oct (intended change): it is promoted (demo path).
        data, cfg = a_folder(tmp_path)
        lab_db(data, "lab-00000000aa", good_days(), stage="PRACTICE")
        got = W.candidates(data, cfg, T0 + dt.timedelta(days=11))
        assert [c["id"] for c in got] == ["lab:lab-00000000aa"] and got[0]["kind"] == "lab" and got[0]["passes"]
        plan = W.make_plan(data, cfg, T0 + dt.timedelta(days=11), ranking=got)
        assert plan["pick"]["id"] == plan["lab_pick"]["id"] == "lab:lab-00000000aa"

    def test_adding_a_part_to_an_empty_live_set(self):
        add = W.tnb_add(TP, "INDEX", frozenset(), frozenset(), set())
        assert add["ok"] and add["groups"] == {"INDEX"} and add["tactics"] == {TP} and add["extras"] == []

    def test_a_part_that_would_widen_into_an_unproven_combination_is_skipped(self):
        add = W.tnb_add(TP, "INDEX", frozenset({"FX_MINOR"}), frozenset({MC}), {f"tnb:{TP}:INDEX"})
        assert not add["ok"] and "skipped this week" in add["why"]
        assert set(add["extras"]) == {f"tnb:{MC}:INDEX", f"tnb:{TP}:FX_MINOR"}
        # every approach live on minor pairs: a new kind would take every approach live there
        from mintel.engine.tactics import APPROACH_NAMES
        assert not W.tnb_add(MC, "INDEX", frozenset({"FX_MINOR"}), frozenset(APPROACH_NAMES), set())["ok"]

    def test_proven_extras_ride_along(self):
        proven = {f"tnb:{MC}:INDEX", f"tnb:{TP}:FX_MINOR", f"tnb:{TP}:INDEX"}
        add = W.tnb_add(TP, "INDEX", frozenset({"FX_MINOR"}), frozenset({MC}), proven)
        assert add["ok"] and add["groups"] == {"FX_MINOR", "INDEX"} and add["tactics"] == {MC, TP}

    def test_the_plan_skips_a_widening_part_and_writes_it_live_without_replacing(self, tmp_path, monkeypatch):
        data, cfg = a_folder(tmp_path)
        raw = config(cfg)
        raw["tnb"].update(live_groups=["FX_MINOR"], live_tactics=[MC], live_since_utc="2026-10-09T18:45:00+00:00")
        cfg.write_text(json.dumps(raw))
        parts = {(TP, "INDEX"): trades_of(good_days()), (MC, "INDEX"): trades_of(good_days(per_day=5))}
        monkeypatch.setattr(W, "tnb_part_trades", lambda *a, **k: {k2: [dict(t, risk=13.0) for t in v]
                                                                 for k2, v in parts.items()})
        plan = W.make_plan(data, cfg, T0 + dt.timedelta(days=11), FRI_16)
        # momentum continuation on indices adds a kind only: minor pairs and indices x momentum continuation
        assert plan["pick"]["id"] == f"tnb:{MC}:INDEX" and plan["extras"] == []
        got = W.promote(plan["pick"], cfg, T0 + dt.timedelta(days=11), {plan["pick"]["id"]})
        t = config(cfg)["tnb"]
        assert got["changed"] and sorted(t["live_groups"]) == ["FX_MINOR", "INDEX"] and t["live_tactics"] == [MC]
        # trend pullback on indices, ranked second, would also make trend pullback on minor pairs live: skipped
        parts[(MC, "INDEX")] = []
        plan = W.make_plan(data, cfg, T0 + dt.timedelta(days=11), FRI_16)
        assert plan["pick"] is None and any("trend pullback, minor pairs" in r for r in plan["reasons"])


# ===================================================== F. the live judgement --
def live_positions(n: int, net_each: float, since: dt.datetime, magic: int = 990_511) -> list[dict]:
    return [{"position": 900 + i, "magic": magic, "symbol": "US500", "net": net_each, "commission": 0.5,
             "opened": (since + dt.timedelta(minutes=10 * i + 1)).isoformat(),
             "closed": (since + dt.timedelta(minutes=10 * i + 5)).isoformat()} for i in range(n)]


class TestLiveJudgement:
    def test_the_verdict(self):
        # 14 Oct (intended change): evidence, not a fixed 20 - the bootstrap bounds after 20, net after 40,
        # 15 x risk at any point (it was 10 x)
        assert W.live_verdict(trades_of([[-1.0] * 19]), 10.0)[0] == "wait"
        assert W.live_verdict(trades_of([[-1.0] * 20]), 10.0)[0] == "demote"
        assert W.live_verdict(trades_of([[1.0, -0.5] * 10]), 10.0)[0] == "keep"
        assert W.live_verdict(trades_of([[-50.0, -50.0]]), 10.0)[0] == "wait"      # -100 = 10 x: no longer enough
        v, why = W.live_verdict(trades_of([[-75.0, -75.0]]), 10.0)            # 2 trades, -150 = 15 x 10
        assert v == "demote" and "15 x" in why

    def test_twenty_losing_live_trades_go_back_to_paper(self, tmp_path):
        data, cfg = planned(tmp_path)
        W.tick(data, cfg, REL_19)
        later = REL_19 + dt.timedelta(hours=5)
        pos = live_positions(20, -0.5, REL_19)
        acts = W.tick(data, cfg, later, positions_fn=lambda: pos)
        assert config(cfg)["runner"]["mode"] == "PAPER" and "20 demo trades" in acts[-1]
        dem = W.history(data)[-1]
        assert dem["event"] == "demote" and dem["record"]["source"] == "MetaTrader"

    def test_winning_live_trades_stay(self, tmp_path):
        data, cfg = planned(tmp_path)
        W.tick(data, cfg, REL_19)
        W.tick(data, cfg, REL_19 + dt.timedelta(hours=5), positions_fn=lambda: live_positions(20, 1.0, REL_19))
        assert config(cfg)["runner"]["mode"] == "LIVE" and W.history(data)[-1]["event"] == "keep"

    def test_the_early_exit(self, tmp_path):
        data, cfg = planned(tmp_path)
        W.tick(data, cfg, REL_19)
        pos = live_positions(4, -37.5, REL_19)                                    # -150: 15 x its 10 risk (14 Oct)
        W.tick(data, cfg, REL_19 + dt.timedelta(hours=2), positions_fn=lambda: pos)
        assert config(cfg)["runner"]["mode"] == "PAPER"
        assert "15 x" in W.history(data)[-1]["why"]

    def test_trades_from_before_the_promotion_or_other_magics_do_not_count(self, tmp_path):
        data, cfg = planned(tmp_path)
        W.tick(data, cfg, REL_19)
        pos = (live_positions(20, -9.0, REL_19 - dt.timedelta(days=2))
               + live_positions(20, -9.0, REL_19, magic=990_611))
        W.tick(data, cfg, REL_19 + dt.timedelta(hours=5), positions_fn=lambda: pos)
        assert config(cfg)["runner"]["mode"] == "LIVE"

    def test_with_the_page_down_its_own_record_can_take_it_back(self, tmp_path):
        data, cfg = planned(tmp_path)
        W.tick(data, cfg, REL_19)
        own_db(data, "runner.sqlite", [[-40.0] * 4], start=REL_19 + dt.timedelta(minutes=5), mode="LIVE",
               first_ticket=5000)                                               # -160: past 15 x (14 Oct)
        W.tick(data, cfg, REL_19 + dt.timedelta(hours=3), positions_fn=lambda: None)
        assert config(cfg)["runner"]["mode"] == "PAPER"
        assert "own record" in W.history(data)[-1]["record"]["source"]

    def test_demotions_wait_under_halt(self, tmp_path):
        data, cfg = planned(tmp_path)
        W.tick(data, cfg, REL_19)
        (data / "HALT").write_text("")
        W.tick(data, cfg, REL_19 + dt.timedelta(hours=1), positions_fn=lambda: live_positions(20, -1.0, REL_19))
        assert config(cfg)["runner"]["mode"] == "LIVE"

    def test_taking_back_one_part_of_a_cross_product(self):
        keep = {f"tnb:{MC}:INDEX", f"tnb:{TP}:FX_MINOR", f"tnb:{TP}:INDEX"}
        rm = W.tnb_remove(MC, "FX_MINOR", frozenset({"FX_MINOR", "INDEX"}), frozenset({MC, TP}), keep)
        # either way two of the three others survive; a tie drops the kind
        assert rm["groups"] == {"INDEX"} and rm["tactics"] is None and rm["lost"] == [f"tnb:{TP}:FX_MINOR"]
        rm = W.tnb_remove(MC, "FX_MINOR", frozenset({"FX_MINOR"}), frozenset({MC}), set())
        assert rm["groups"] == frozenset() and rm["lost"] == []


# ===================================================== G. idempotent; restart --
class TestIdempotent:
    def test_a_restart_between_writing_and_recording(self, tmp_path):
        data, cfg = planned(tmp_path)
        calls = []
        from mintel.ops.modes import set_modes

        def crash(path, changes, **kw):
            set_modes(path, changes, **kw)                 # config.json is written ...
            calls.append(changes)
            raise SystemExit("power cut")                  # ... and the watchdog dies before recording it
        with pytest.raises(SystemExit):
            W.tick(data, cfg, REL_19, set_modes=crash)
        assert config(cfg)["runner"]["mode"] == "LIVE" and W.history(data) == []
        # the next watchdog: config.json already holds it, so it is recorded, not written again
        restarts = []
        W.tick(data, cfg, REL_19 + dt.timedelta(minutes=5), set_modes=crash,
               restart=lambda name: (restarts.append(name), (True, "started"))[1])
        assert len(calls) == 1 and [e["event"] for e in W.history(data)] == ["promote"]
        assert W.load_state(data)["promoted"][0]["id"] == "bot:runner"
        # the intent was saved before the switch, so the trader's restart is not lost with the crash
        assert restarts == ["trader"] and W.load_state(data)["restart_pending"] == []

    def test_a_failed_switch_changes_nothing_and_is_tried_again(self, tmp_path):
        data, cfg = planned(tmp_path)

        def refuse(*a, **k):
            raise ValueError("runner's magic 990511 is another bot's")
        acts = W.tick(data, cfg, REL_19, set_modes=refuse)
        assert "nothing was changed" in acts[0] and config(cfg)["runner"]["mode"] == "PAPER"
        assert W.load_state(data)["promoted"] == [] and W.history(data) == []
        W.tick(data, cfg, REL_19 + dt.timedelta(minutes=5))
        assert config(cfg)["runner"]["mode"] == "LIVE"

    def test_a_restart_left_pending_is_done_at_the_next_check(self, tmp_path):
        data, cfg = planned(tmp_path)
        W.tick(data, cfg, REL_19)                           # no one to restart it (the CLI)
        assert W.load_state(data)["restart_pending"] == ["trader"]
        W.tick(data, cfg, REL_19 + dt.timedelta(minutes=5), restart=lambda n: (False, "could not start"))
        assert W.load_state(data)["restart_pending"] == ["trader"]
        done = []
        W.tick(data, cfg, REL_19 + dt.timedelta(minutes=10), restart=lambda n: (done.append(n), (True, "ok"))[1])
        assert done == ["trader"] and W.load_state(data)["restart_pending"] == []
        # ticks repeated change nothing more
        W.tick(data, cfg, REL_19 + dt.timedelta(minutes=15), restart=lambda n: (done.append(n), (True, "ok"))[1])
        assert done == ["trader"] and [e["event"] for e in W.history(data)] == ["promote"]

    def test_the_watchdog_restarts_the_named_process(self, tmp_path):
        from mintel.config import Config
        from mintel.ops.watchdog import ManagedProcess, Watchdog
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path)
        cfg.ops.log_dir = str(tmp_path / "logs")
        seen = []

        class Fake(ManagedProcess):
            def stop(self):
                seen.append(("stop", self.name))

            def start(self, now, env=None):
                seen.append(("start", self.name))
                return True, "started"
        wd = Watchdog(cfg, [Fake(name="trader", command=["x"]), Fake(name="ian", command=["y"])])
        assert wd.restart_named("trader", T0) == (True, "started")
        assert seen == [("stop", "trader"), ("start", "trader")]
        assert wd.restart_named("nobody", T0)[0]
        # without weekly_config (a test's watchdog, or one built by hand) the weekly pick is left alone
        assert wd._check_weekly(T0) == []

    def test_the_watchdog_looks_every_five_minutes(self, tmp_path, monkeypatch):
        from mintel.config import Config
        from mintel.ops.watchdog import Watchdog
        cfg = Config()
        cfg.ops.data_dir = str(tmp_path)
        wd = Watchdog(cfg, [])
        wd.weekly_config = str(tmp_path / "config.json")
        ticks = []
        monkeypatch.setattr(W, "tick", lambda *a, **k: ticks.append(a[2]) or [])
        for m in (0, 1, 4, 5, 6):
            wd._check_weekly(T0 + dt.timedelta(minutes=m))
        assert ticks == [T0, T0 + dt.timedelta(minutes=5)]


# =================================================================== H. Home --
def text(page: str) -> str:
    import re
    from html import unescape
    page = re.sub(r"<(style|script)>.*?</\1>", "", page, flags=re.S)
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", page)))


class TestHome:
    def test_fresh_install_no_weekly_data(self, tmp_path):
        from mintel.ops.dashboard import render_home, render_weekly_home
        page = render_home({"standing": {}}, {"data_dir": str(tmp_path), "now": T0}, {}, None, "none")
        t = text(page)
        # 14 Oct (intended change): the pick is daily, so the next is that very Monday's
        assert "Nothing on real money — all testing" in t and "next pick Mon 5 Oct 22:30" in t
        assert W.NOTHING_PROVED in t and "0 tests running" in t
        assert render_weekly_home(None)                    # nothing at all still renders
        assert 'id="weekly"' in render_home({"standing": {}}, None, {}, None, "none")

    def test_one_pool_numbers_and_the_top_three(self, tmp_path):
        from mintel.ops.dashboard import render_weekly_home
        data, cfg = a_folder(tmp_path)
        own_db(data, "runner.sqlite", good_days())                             # making money, ready
        own_db(data, "bandbreaker.sqlite", [[-3.0, -2.0, 1.0]] * 10)           # losing
        own_db(data, "crowd.sqlite", [[2.0, 1.0]] * 2)                         # too early
        lab_db(data, "lab-00000000ab", [[5.0, -1.0]] * 6, stage="PRACTICE", name="Gold breakout")
        board = W.home_board(data, cfg, T0 + dt.timedelta(days=11), cache_seconds=0)
        # making money and losing go by the money alone (13 Oct): the 2-trade crowd test is up, so it counts,
        # marked early beside its name. Financial Ian is on PAPER with no trade: since every lab idea practises
        # (13 Oct) a test with no trade yet is counted as waiting for one, and not listed
        # 14 Oct (intended change): the lab idea (12 trades over 6 days, PF 5) passes the early bar too: 2 ready
        assert board["counts"] == {"running": 5, "making": 3, "losing": 1, "early": 1, "waiting": 1, "ready": 2}
        assert [x["name"] for x in board["waiting"]] == ["Financial Ian"] and board["tests"] == 5
        # 13 Oct (intended change): Home lists the best ten and the worst three, not the best three; with only four
        # tests they are all the top, and the bottom is empty (no row twice)
        assert [x["name"] for x in board["top"]] == ["Momentum Runner", "Gold breakout", "Crowd Fader", "Band Breaker"]
        assert board["bottom"] == []
        assert [x["name"] for x in board["all"]] == ["Momentum Runner", "Gold breakout", "Crowd Fader",
                                                     "Band Breaker"]                  # every test, best first
        tot = board["total"]
        assert tot["net"] == round(sum(x["net"] for x in board["all"]), 2)
        assert tot["trades"] == sum(x["trades"] for x in board["all"])
        page = render_weekly_home(board)
        t = text(page)
        assert "5 tests running" in t and "2 ready for real money" in t and "+£140.00 40 trades · 75% won" in t
        assert "1 waiting for a trade" in t and "See all 5 tests" in t and 'href="/?show=practice"' in page
        assert page.count('<div class="wk-row" data-test=') == 4 and "All tests together" in t
        assert "Crowd Fader early" in t and "Band Breaker" in t and "LOSING" in t

    def test_on_real_money_tile(self, tmp_path):
        from mintel.ops.dashboard import render_weekly_home
        data, cfg = planned(tmp_path)
        W.tick(data, cfg, REL_19)
        now = REL_19 + dt.timedelta(hours=4)
        deals = []
        for i, net in enumerate((4.0, -1.5, 3.0)):
            o = REL_19 + dt.timedelta(minutes=10 + 20 * i)
            deals += [{"time": o, "position": 70 + i, "bot_magic": 990_511, "symbol": "US500", "is_entry": True,
                       "profit": -0.25, "commission": -0.25, "volume": 1.0},
                      {"time": o + dt.timedelta(minutes=5), "position": 70 + i, "bot_magic": 990_511,
                       "symbol": "US500", "is_entry": False, "profit": net + 0.25, "commission": -0.25,
                       "volume": 1.0}]
        open_live = [{"bot": "Momentum Runner", "magic": 990_511, "ticket": 99, "symbol": "US500", "profit": 2.4}]
        board = W.home_board(data, cfg, now, deals=deals, open_tickets=[99], open_live=open_live, cache_seconds=0)
        tile = board["live"][0]
        assert (tile["today"], tile["since"], tile["open"], tile["trades"]) == (5.5, 5.5, 2.4, 3)
        page = render_weekly_home(board)
        assert ('<div data-k="today"><span class="pos">+£5.50</span></div>' in page
                and '<div data-k="open now"><span class="pos">+£2.40</span></div>' in page
                and '<div data-k="live trades">3 of 20</div>' in page)
        t = text(page)
        assert "MAKING MONEY" in t and "This week's pick Momentum Runner LIVE since Mon 19 Oct" in t

    def test_the_page_serves_weekly_live(self, tmp_path):
        import urllib.request
        from mintel.ops.dashboard import DashboardState, start_dashboard
        state = DashboardState()
        o = T0 + dt.timedelta(hours=1)
        deals = [{"time": o, "position": 5, "bot_magic": 990_511, "symbol": "US500", "is_entry": True,
                  "profit": -0.3, "commission": -0.3, "volume": 1.0},
                 {"time": o + dt.timedelta(minutes=9), "position": 5, "bot_magic": 990_511, "symbol": "US500",
                  "is_entry": False, "profit": 7.3, "commission": -0.3, "volume": 1.0}]
        state.update(status={"bot": "RUNNING"}, health={"checks": []},
                     standing={"start": T0.isoformat(), "made": 7.0, "open_tickets": []}, deals=deals,
                     data_dir=str(tmp_path))
        httpd = start_dashboard(state, "127.0.0.1", 0)
        try:
            body = json.loads(urllib.request.urlopen(
                f"http://127.0.0.1:{httpd.server_address[1]}/weekly-live", timeout=20).read())
        finally:
            httpd.shutdown()
        assert body["ok"] and body["positions"] == [
            {"position": 5, "magic": 990_511, "symbol": "US500", "net": 7.0, "commission": 0.6,
             "opened": o.isoformat(), "closed": (o + dt.timedelta(minutes=9)).isoformat()}]

    def test_the_pulse_publishes_weekly_json(self, tmp_path):
        from mintel.config import Config
        from mintel.ops.pulse import WEEKLY_PATH, pulse_files, weekly_json
        data, cfg_path = planned(tmp_path)
        cfg = Config()
        cfg.ops.data_dir = str(data)
        text_ = weekly_json(cfg)
        rep = json.loads(text_)
        assert rep["plan"]["pick"]["id"] == "bot:runner" and rep["live"] == []
        assert pulse_files({"status": {}}, [], weekly=text_)[WEEKLY_PATH] == text_
        cfg.ops.data_dir = str(tmp_path / "nothing")
        assert weekly_json(cfg) is None


# ================================================ I. the review's seven bugs --
def tnb_folder(tmp_path, groups=(), tactics=(), mode="PAPER"):
    data, cfg = a_folder(tmp_path)
    raw = config(cfg)
    raw["tnb"].update(mode=mode, live_groups=list(groups), live_tactics=list(tactics),
                      live_since_utc="2026-10-09T18:45:00+00:00" if groups else "")
    cfg.write_text(json.dumps(raw))
    return data, cfg


def a_state(data: Path, *items) -> None:
    (data / "weekly").mkdir(exist_ok=True)
    (data / "weekly" / "state.json").write_text(json.dumps({"schema": 1, "promoted": list(items),
                                                            "restart_pending": []}))


def tnb_item(tactic, group, plan="2026-10-16", **extra):
    return dict({"id": f"tnb:{tactic}:{group}", "kind": "tnb", "label": "Trend & Breakout",
                 "part": W.part_words(tactic, group), "tactic": tactic, "group": group, "magic": 990_311,
                 "process": "trader", "risk_gbp": 13.0, "since_utc": REL_19.isoformat(), "plan": plan}, **extra)


class TestReviewFixes:
    def test_1_a_broken_config_never_drops_a_live_item(self, tmp_path):
        data, cfg = tnb_folder(tmp_path, ["INDEX"], [MC])
        a_state(data, tnb_item(MC, "INDEX"))
        cfg.write_text("{ broken")
        acts = W.tick(data, cfg, REL_19 + dt.timedelta(hours=1))
        assert "nothing was done this pass" in acts[0]
        assert [p["id"] for p in W.load_state(data)["promoted"]] == [f"tnb:{MC}:INDEX"] and W.history(data) == []

    def test_1_a_broken_state_file_stops_the_pass_and_is_never_overwritten(self, tmp_path):
        data, cfg = planned(tmp_path)
        (data / "weekly" / "state.json").write_text("{ half a file")
        acts = W.tick(data, cfg, REL_19)
        assert "could not be read" in acts[0] and config(cfg)["runner"]["mode"] == "PAPER"
        assert (data / "weekly" / "state.json").read_text() == "{ half a file"
        with pytest.raises(W.Unreadable):
            W.load_state(data, strict=True)

    def test_1_a_broken_own_file_never_reads_as_paper(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        (data / "ian.json").write_text('{"mode": "LIVE"')
        a_state(data, {"id": "bot:ian", "kind": "bot", "label": "Financial Ian", "bot": "ian", "magic": 990_911,
                       "process": "ian", "plan": "2026-10-16", "since_utc": REL_19.isoformat()})
        acts = W.tick(data, cfg, REL_19 + dt.timedelta(hours=1))
        assert any("Financial Ian's own file could not be read" in a for a in acts)
        assert len(W.load_state(data)["promoted"]) == 1 and W.history(data) == []

    def test_2_a_crash_after_a_demotion_is_recorded_as_one_with_its_restart(self, tmp_path):
        data, cfg = planned(tmp_path)
        W.tick(data, cfg, REL_19, restart=lambda n: (True, "ok"))
        from mintel.ops.modes import set_modes

        def crash(path, changes, **kw):
            set_modes(path, changes, **kw)
            raise SystemExit("power cut")
        with pytest.raises(SystemExit):
            W.tick(data, cfg, REL_19 + dt.timedelta(hours=2), set_modes=crash,
                   positions_fn=lambda: live_positions(20, -1.0, REL_19))
        assert config(cfg)["runner"]["mode"] == "PAPER"
        st = W.load_state(data)
        assert st["restart_pending"] == ["trader"] and st["promoted"][0]["demoting"].startswith("20 demo trades")
        done = []
        W.tick(data, cfg, REL_19 + dt.timedelta(hours=2, minutes=5), restart=lambda n: (done.append(n), (True, ""))[1])
        ev = W.history(data)[-1]
        assert ev["event"] == "demote" and ev["id"] == "bot:runner" and done == ["trader"]
        assert W.last_demoted(W.history(data))["bot:runner"] == REL_19 + dt.timedelta(hours=2, minutes=5)

    def test_2_a_crash_before_the_switch_drops_the_intent_and_tries_again(self, tmp_path):
        data, cfg = planned(tmp_path)

        def die(*a, **k):
            raise SystemExit("power cut before anything was written")
        with pytest.raises(SystemExit):
            W.tick(data, cfg, REL_19, set_modes=die)
        assert W.load_state(data)["promoted"][0]["promoting"]
        W.tick(data, cfg, REL_19 + dt.timedelta(minutes=5))
        assert config(cfg)["runner"]["mode"] == "LIVE" and [e["event"] for e in W.history(data)] == ["promote"]
        assert "promoting" not in W.load_state(data)["promoted"][0]

    def test_3_a_bot_switched_off_is_never_promoted(self, tmp_path):
        data, cfg = a_folder(tmp_path, runner="OFF")
        own_db(data, "runner.sqlite", good_days())
        plan = W.make_plan(data, cfg, T0 + dt.timedelta(days=11), FRI_16)
        assert plan["pick"] is None and any("is OFF, not on PAPER" in r for r in plan["reasons"])
        item = {"id": "bot:runner", "kind": "bot", "label": "Momentum Runner", "bot": "runner"}
        with pytest.raises(ValueError, match="not on PAPER"):
            W.promote(item, cfg, T0, {"bot:runner"})
        assert config(cfg)["runner"]["mode"] == "OFF"

    def test_3_no_part_while_trend_and_breakout_is_live_as_a_whole(self, tmp_path):
        data, cfg = tnb_folder(tmp_path, mode="LIVE")
        with pytest.raises(ValueError, match="LIVE as a whole"):
            W.promote(tnb_item(TP, "GOLD"), cfg, T0, {f"tnb:{TP}:GOLD"})

    def test_4_a_part_that_completes_the_approach_list_really_goes_live(self, tmp_path):
        from mintel.engine.tactics import APPROACH_NAMES
        others = [a for a in APPROACH_NAMES if a != TP]
        data, cfg = tnb_folder(tmp_path, ["INDEX"], others)
        proven = {f"tnb:{TP}:INDEX"}
        add = W.tnb_add(TP, "INDEX", frozenset({"INDEX"}), frozenset(others), proven)
        assert add["ok"] and add["tactics"] == frozenset(APPROACH_NAMES)          # written, never left as it was
        assert W.promote(tnb_item(TP, "INDEX"), cfg, T0, proven)["changed"]
        assert W.is_live(f"tnb:{TP}:INDEX", config(cfg), {})

    def test_4_a_switch_the_files_do_not_hold_is_put_back_and_not_counted(self, tmp_path):
        data, cfg = planned(tmp_path)
        before = cfg.read_text()

        def quietly_wrong(path, changes, **kw):                                 # writes, but not what was asked
            raw = config(cfg)
            raw["crowd"]["mode"] = "LIVE"
            cfg.write_text(json.dumps(raw))
        acts = W.tick(data, cfg, REL_19, set_modes=quietly_wrong)
        assert "put back as it was" in acts[0] and cfg.read_text() == before
        assert W.load_state(data)["promoted"] == [] and W.history(data) == []
        assert json.loads((data / "weekly" / "latest.json").read_text())["status"] == W.PLANNED

    def test_5_a_veto_just_after_friday_2230_reaches_last_weeks_live_pick(self, tmp_path):
        data, cfg = planned(tmp_path)
        W.tick(data, cfg, REL_19)
        assert config(cfg)["runner"]["mode"] == "LIVE"
        fri_23 = dt.datetime(2026, 10, 23, 21, 31, tzinfo=UTC)
        W.tick(data, cfg, fri_23)                                                # the new plan: nothing to pick
        assert json.loads((data / "weekly" / "latest.json").read_text())["friday"] == "2026-10-23"
        assert "taken back" in W.veto(data)                                      # against the live pick's own plan
        assert (data / "weekly" / "VETO-2026-10-16").exists()
        W.tick(data, cfg, fri_23 + dt.timedelta(minutes=5))
        assert config(cfg)["runner"]["mode"] == "PAPER" and W.history(data)[-1]["why"] == "Aaron vetoed this pick"

    def test_5_a_veto_file_written_against_an_older_plan_is_seen(self, tmp_path):
        data, cfg = planned(tmp_path)
        W.tick(data, cfg, REL_19)
        W.veto_path(data, FRI_16).write_text("x")
        W.tick(data, cfg, dt.datetime(2026, 10, 23, 21, 40, tzinfo=UTC))
        assert config(cfg)["runner"]["mode"] == "PAPER"

    def test_6_a_veto_takes_back_the_parts_that_went_live_with_the_pick(self, tmp_path):
        data, cfg = tnb_folder(tmp_path, ["FX_MINOR", "INDEX"], [MC, TP])
        a_state(data, tnb_item(MC, "INDEX"), tnb_item(TP, "FX_MINOR", rides_with=f"tnb:{MC}:INDEX"),
                tnb_item(TP, "INDEX", rides_with=f"tnb:{MC}:INDEX"), tnb_item(MC, "FX_MINOR", plan="2026-10-09"))
        W.veto_path(data, FRI_16).write_text("x")
        W.tick(data, cfg, REL_19 + dt.timedelta(hours=1))
        left = [p["id"] for p in W.load_state(data)["promoted"]]
        assert left == [f"tnb:{MC}:FX_MINOR"]                                     # only the other week's part
        t = config(cfg)["tnb"]
        assert t["live_groups"] == ["FX_MINOR"] and t["live_tactics"] == [MC]

    def test_7_open_profit_is_counted_once_per_part(self, tmp_path):
        data, cfg = tnb_folder(tmp_path, ["INDEX"], [MC, TP])
        a_state(data, tnb_item(MC, "INDEX"), tnb_item(TP, "INDEX"))
        con = sqlite3.connect(data / "journal.sqlite")
        con.execute("CREATE TABLE trades(ticket INTEGER, tactic TEXT, opened_utc TEXT, closed_utc TEXT)")
        con.executemany("INSERT INTO trades VALUES(?,?,?,NULL)", [(11, "momentum_continuation", REL_19.isoformat()),
                                                                 (12, "trend_pullback", REL_19.isoformat())])
        con.commit()
        con.close()
        open_live = [{"magic": 990_311, "ticket": 11, "symbol": "US500", "profit": 5.0},
                     {"magic": 990_311, "ticket": 12, "symbol": "US500", "profit": -2.0}]
        tiles = {t["id"]: t for t in W.live_tiles(data, REL_19 + dt.timedelta(hours=1), [], [], open_live)}
        assert tiles[f"tnb:{MC}:INDEX"]["open"] == 5.0 and tiles[f"tnb:{TP}:INDEX"]["open"] == -2.0


class TestReReviewFixes:
    """A: an error after set_modes wrote never forgets a demotion; B: a broken rider.json or ian.json never
    blocks another item's demotion."""

    @pytest.mark.parametrize("where", ["set_modes", "read_back"])
    def test_a_an_error_after_the_write_keeps_the_demotion_and_its_restart(self, tmp_path, monkeypatch, where):
        data, cfg = planned(tmp_path)
        W.tick(data, cfg, REL_19, restart=lambda n: (True, "ok"))
        from mintel.ops.modes import PartialWrite, set_modes
        real_view = W.strict_view

        def wrote_then_failed(path, changes, **kw):
            set_modes(path, changes, **kw)
            if where == "set_modes":
                raise PartialWrite("config.json holds the change, ian.json does not")
            monkeypatch.setattr(W, "strict_view", lambda p: (_ for _ in ()).throw(OSError("disk went away")))
        pos = live_positions(20, -1.0, REL_19)
        acts = W.tick(data, cfg, REL_19 + dt.timedelta(hours=2), set_modes=wrote_then_failed, positions_fn=lambda: pos)
        monkeypatch.setattr(W, "strict_view", real_view)
        assert any("could not be put back on PAPER" in a for a in acts)
        assert config(cfg)["runner"]["mode"] == "LIVE"                       # the files put back as they were
        st = W.load_state(data)
        assert st["promoted"][0]["demoting"].startswith("20 demo trades")   # the intent kept
        # the next check tries again, and the demotion is recorded with its restart
        done = []
        W.tick(data, cfg, REL_19 + dt.timedelta(hours=2, minutes=5), positions_fn=lambda: pos,
               restart=lambda n: (done.append(n), (True, ""))[1])
        assert config(cfg)["runner"]["mode"] == "PAPER" and W.history(data)[-1]["event"] == "demote"
        # (when the read failed too the restart was kept, as asked: it may run once more than needed)
        assert done == (["trader"] if where == "set_modes" else ["trader", "trader"])
        assert W.load_state(data)["promoted"] == [] and W.load_state(data)["restart_pending"] == []

    def test_a_put_back_failing_too_leaves_the_intent_for_the_next_read(self, tmp_path, monkeypatch):
        data, cfg = planned(tmp_path)
        W.tick(data, cfg, REL_19, restart=lambda n: (True, "ok"))
        from mintel.ops.modes import set_modes

        def wrote_then_failed(path, changes, **kw):
            set_modes(path, changes, **kw)
            raise OSError("the second file could not be written")
        monkeypatch.setattr(W, "_put_back", lambda before: (_ for _ in ()).throw(OSError("read-only disk")))
        W.tick(data, cfg, REL_19 + dt.timedelta(hours=2), set_modes=wrote_then_failed,
               positions_fn=lambda: live_positions(20, -1.0, REL_19))
        monkeypatch.undo()
        assert config(cfg)["runner"]["mode"] == "PAPER"                      # the write stayed
        st = W.load_state(data)
        assert st["restart_pending"] == ["trader"] and st["promoted"][0]["demoting"]
        done = []
        W.tick(data, cfg, REL_19 + dt.timedelta(hours=2, minutes=5), restart=lambda n: (done.append(n), (True, ""))[1])
        assert W.history(data)[-1]["event"] == "demote" and done == ["trader"]

    def test_a_a_release_that_fails_after_writing_is_settled_by_a_read(self, tmp_path):
        data, cfg = planned(tmp_path)
        from mintel.ops.modes import set_modes

        def wrote_then_failed(path, changes, **kw):
            set_modes(path, changes, **kw)
            raise OSError("the second file could not be written")
        acts = W.tick(data, cfg, REL_19, set_modes=wrote_then_failed)
        assert "could not be switched LIVE" in acts[-1] and config(cfg)["runner"]["mode"] == "PAPER"
        st = W.load_state(data)
        assert st["promoted"] == [] and st["restart_pending"] == []          # put back, so nothing to restart
        W.tick(data, cfg, REL_19 + dt.timedelta(minutes=5))
        assert config(cfg)["runner"]["mode"] == "LIVE"

    def test_b_a_broken_rider_json_never_blocks_a_demotion(self, tmp_path):
        data, cfg = planned(tmp_path)
        W.tick(data, cfg, REL_19)
        (data / "rider.json").write_text("{ broken")
        W.tick(data, cfg, REL_19 + dt.timedelta(hours=1), positions_fn=lambda: live_positions(4, -125.0, REL_19))
        assert config(cfg)["runner"]["mode"] == "PAPER" and W.history(data)[-1]["event"] == "demote"

    def test_b_a_broken_ian_json_blocks_only_ians_own_decisions(self, tmp_path):
        data, cfg = planned(tmp_path)
        W.tick(data, cfg, REL_19)
        st = W.load_state(data)
        st["promoted"].append({"id": "bot:ian", "kind": "bot", "label": "Financial Ian", "bot": "ian",
                               "magic": 990_911, "process": "ian", "plan": "2026-10-09",
                               "since_utc": REL_19.isoformat()})
        (data / "weekly" / "state.json").write_text(json.dumps(st))
        (data / "ian.json").write_text("{ broken")
        acts = W.tick(data, cfg, REL_19 + dt.timedelta(hours=1),
                      positions_fn=lambda: live_positions(4, -125.0, REL_19))
        assert config(cfg)["runner"]["mode"] == "PAPER"                      # the runner still goes back
        assert any("Financial Ian's own file could not be read" in a for a in acts)
        assert [p["id"] for p in W.load_state(data)["promoted"]] == ["bot:ian"]
        with pytest.raises(ValueError, match="could not be read"):
            W.promote({"id": "bot:ian", "kind": "bot", "label": "Financial Ian", "bot": "ian"}, cfg, T0,
                      {"bot:ian"})


def test_the_cli_ranks_and_vetoes(tmp_path, capsys):
    data, cfg = planned(tmp_path)
    assert W.main(["--config", str(cfg)]) == 0
    assert "Pick of 2026-10-16: Momentum Runner [planned]" in capsys.readouterr().out
    assert W.main(["--config", str(cfg), "veto"]) == 0
    assert "Cancelled" in capsys.readouterr().out


# ============================== I. every lab idea in the pool; the breakdown, winners at the top (13 Oct) --
# Aaron, 13 Oct: "I just want all in practice unless any earn the right, then a breakdown of everything (profit) and
# winners at the top".
def a_real_idea(data: Path, k: int, nets_by_day: list[list[float]], stage: str = "BINNED"):
    """A real registered lab idea (its stored definition), with made-up practice trades."""
    from mintel.lab.candidate import registered
    from mintel.lab.ledger import LabLedger
    c = registered()[k]
    led = LabLedger(data / "lab" / "lab.sqlite")
    led.register(c, f"long name {k}")
    led.execute("UPDATE candidates SET stage=? WHERE id=?", (stage, c.id))
    n = 0
    for d, nets in enumerate(nets_by_day):
        for i, net in enumerate(nets):
            n += 1
            t = T0 + dt.timedelta(days=d, minutes=15 * i)
            led.execute("INSERT INTO practice_trades(ticket, candidate_id, symbol, side, closed_utc, net_gbp, "
                        "commission_gbp, risk_gbp, fill_source) VALUES(?,?,?,?,?,?,?,?,?)",
                        (970_000_000 + 1000 * k + n, c.id, "EURUSD", 1, t.isoformat(), net, 0.4, 13.0, "tick"))
    led.close()
    return c


def fake_pool(n_traded: int, n_waiting: int) -> list[dict]:
    """A made-up pool in candidates' shape: test k made k - 7 pounds over k + 1 trades; then the waiting ones."""
    out = []
    for k in range(n_traded):
        out.append({"id": f"lab:lab-{k:010d}", "kind": "lab", "part": f"idea {k:02d}", "passes": False,
                    "stats": {"trades": k + 1, "net": float(k - 7), "win_rate": 50.0, "wins": 0}})
    for k in range(n_waiting):
        out.append({"id": f"lab:lab-w{k:09d}", "kind": "lab", "part": f"quiet {k}", "passes": False,
                    "stats": {"trades": 0, "net": 0.0, "win_rate": None, "wins": 0}})
    return out


class TestEveryIdea:
    def test_the_pool_reads_every_lab_ideas_practice_trades_binned_ones_included(self, tmp_path):
        from mintel.lab.candidate import short_name_of
        data, cfg = a_folder(tmp_path)
        binned = a_real_idea(data, 3, good_days(), stage="BINNED")
        tested = a_real_idea(data, 40, [[2.0, -1.0]] * 3, stage="PAST TEST")
        quiet = a_real_idea(data, 77, [], stage="IDEA")
        now = T0 + dt.timedelta(days=11)
        got = {c["id"]: c for c in W.candidates(data, cfg, now)}
        assert {f"lab:{binned.id}", f"lab:{tested.id}"} <= set(got) and f"lab:{quiet.id}" not in got
        one = got[f"lab:{binned.id}"]
        assert one["part"] == short_name_of(binned) and one["full"] == "long name 3" and one["stage"] == "BINNED"
        assert one["passes"]                                       # its own practice record is what is judged
        # Home's pool keeps the idea with no trade yet: counted as waiting, never listed
        board = W.home_board(data, cfg, now, cache_seconds=0)
        lab_waiting = [x["id"] for x in board["waiting"] if x["kind"] == "lab"]
        assert lab_waiting == [f"lab:{quiet.id}"]                       # (the bots on PAPER with none wait too)
        assert f"lab:{quiet.id}" not in {x["id"] for x in board["all"]}
        assert board["counts"]["waiting"] == len(board["waiting"]) and board["tests"] == 3 + len(W.BOTS)
        assert {x["name"] for x in board["all"]} == {short_name_of(binned), short_name_of(tested)}

    def test_home_shows_the_top_ten_the_bottom_three_and_a_link_to_all(self):
        from mintel.ops.dashboard import render_weekly_home
        pool = fake_pool(20, 600)
        board = dict(W.breakdown(pool), counts=W.pool_counts(pool), total=W.pool_total(pool))
        assert [x["name"] for x in board["top"]] == [f"idea {k:02d}" for k in range(19, 9, -1)]
        assert [x["name"] for x in board["bottom"]] == ["idea 02", "idea 01", "idea 00"]
        page = render_weekly_home(board)
        t = text(page)
        assert page.count('<div class="wk-row" data-test=') == 13
        assert t.index("idea 10") < t.index("the three at the bottom") < t.index("idea 02")
        assert "See all 620 tests" in t and 'href="/?show=practice"' in page
        assert "600 waiting for a trade" in t and "620 tests running" in t
        assert "quiet" not in t and "idea 09" not in t                # no zero-trade test, nothing in the middle
        assert "<table" not in page

    def test_the_practice_page_lists_every_test_sorted_with_the_waiting_ones_folded(self):
        from mintel.ops.dashboard import render_practice_breakdown
        pool = fake_pool(20, 5)
        board = dict(W.breakdown(pool), counts=W.pool_counts(pool), total=W.pool_total(pool))
        page = render_practice_breakdown(board)
        t = text(page)
        import re
        names = re.findall(r'<div class="wk-name"><a href="[^"]*">([^<]*)</a>', page)
        assert names == [f"idea {k:02d}" for k in range(19, -1, -1)]          # every one, winners at the top
        assert page.count('<div class="wk-row" data-test=') == 20 and "<table" not in page
        assert t.index("All tests together") < t.index("idea 19")
        assert "+£" in t and "MAKING MONEY" in t and "LOSING" in t and " early" in t
        # the five with no trade: one folded line at the bottom
        assert page.count("<details") == 1 and "5 tests waiting for their first trade" in t
        assert page.index('class="wk-wait"') > page.index("idea 00")
        assert 'href="/lab?id=lab-0000000019"' in page                     # its own page, one tap away

    def test_the_practice_page_on_the_dashboard(self, tmp_path):
        from mintel.ops.dashboard import render_home
        data, cfg = a_folder(tmp_path)
        own_db(data, "runner.sqlite", good_days())
        a_real_idea(data, 3, [[5.0, -1.0]] * 6, stage="BINNED")
        a_real_idea(data, 9, [], stage="IDEA")
        page = render_home({"standing": {}}, {"data_dir": str(data), "now": T0 + dt.timedelta(days=11)}, {}, None,
                           "none", show="practice")
        t = text(page)
        assert 'id="breakdown"' in page and "All tests together" in t
        from mintel.lab.candidate import registered, short_name_of
        idea = short_name_of(registered()[3])
        assert t.index("All tests together") < t.index("Momentum Runner +£140.00") < t.index(idea + " +£24.00") < t.index(
            "4 tests waiting for their first trade")                      # winners at the top, waiting folded last
        assert "waiting for their first trade" in t and "Each test's own page" in t
        assert 'href="/?bot=momentum_runner"' in page                      # the bot's own page, one tap away



# ======================================== J. X winners a week (13 Oct: "X amount of winners go live") --
class TestXAWeek:
    def test_the_best_qualifiers_fill_the_free_places_together(self, tmp_path):
        # 14 Oct (intended change): the free places of the demo squad (demo_target, 10) are filled, so the lab idea
        # (which has a demo path now) goes too, first: it is the busiest; it used to be the best three bots
        data, cfg = a_folder(tmp_path)
        start = T0 + dt.timedelta(days=1)
        own_db(data, "runner.sqlite", good_days(per_day=6), start=start)            # best bot
        own_db(data, "bandbreaker.sqlite", good_days(per_day=5), start=start)
        own_db(data, "crowd.sqlite", good_days(per_day=4), start=start)
        lab_db(data, "lab-0123456789", good_days(per_day=8))
        W.tick(data, cfg, PICK_16 + dt.timedelta(minutes=5))
        plan = json.loads((data / "weekly" / "latest.json").read_text())
        assert [x["pick"]["id"] for x in plan["picks"]] == ["lab:lab-0123456789", "bot:runner", "bot:bandbreaker",
                                                            "bot:crowd"]
        assert plan["limit"] == W.DEMO_TARGET == 10
        W.tick(data, cfg, REL_19)
        raw = config(cfg)
        assert (raw["runner"]["mode"], raw["bandbreaker"]["mode"], raw["crowd"]["mode"]) == ("LIVE",) * 3
        from mintel.labdemo.roster import read_roster
        assert list(read_roster(data)) == ["lab-0123456789"]
        ev = W.history(data)
        assert [e["id"] for e in ev if e["event"] == "promote"] == ["lab:lab-0123456789", "bot:runner",
                                                                    "bot:bandbreaker", "bot:crowd"]
        assert json.loads((data / "weekly" / "latest.json").read_text())["status"] == W.APPLIED
        # each is judged on its own: the veto takes the whole pick back
        W.veto(data)
        W.tick(data, cfg, REL_19 + dt.timedelta(minutes=10))
        raw = config(cfg)
        assert (raw["runner"]["mode"], raw["bandbreaker"]["mode"], raw["crowd"]["mode"]) == ("PAPER",) * 3
        assert read_roster(data) == {}

    def test_a_setting_of_two_releases_two(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        (data / "weekly.json").write_text(json.dumps({"demo_target": 2}))           # 14 Oct: the demo target
        start = T0 + dt.timedelta(days=1)
        for name, n in (("runner.sqlite", 6), ("bandbreaker.sqlite", 5), ("crowd.sqlite", 4)):
            own_db(data, name, good_days(per_day=n), start=start)
        W.tick(data, cfg, PICK_16 + dt.timedelta(minutes=5))
        W.tick(data, cfg, REL_19)
        raw = config(cfg)
        assert (raw["runner"]["mode"], raw["bandbreaker"]["mode"], raw["crowd"]["mode"]) == ("LIVE", "LIVE", "PAPER")

    def test_a_failed_pick_leaves_the_others_and_is_tried_again(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        start = T0 + dt.timedelta(days=1)
        own_db(data, "runner.sqlite", good_days(per_day=6), start=start)
        own_db(data, "bandbreaker.sqlite", good_days(per_day=5), start=start)
        W.tick(data, cfg, PICK_16 + dt.timedelta(minutes=5))
        real = W._set_modes
        calls = []

        def flaky(config_path, changes, **kw):
            calls.append(changes)
            if "bandbreaker" in changes and len(calls) < 3:
                raise OSError("disk full (made up)")
            return real(config_path, changes, **kw)
        W.tick(data, cfg, REL_19, set_modes=flaky)
        raw = config(cfg)
        assert (raw["runner"]["mode"], raw["bandbreaker"]["mode"]) == ("LIVE", "PAPER")
        assert json.loads((data / "weekly" / "latest.json").read_text())["status"] == W.PLANNED
        W.tick(data, cfg, REL_19 + dt.timedelta(minutes=5), set_modes=flaky)
        assert config(cfg)["bandbreaker"]["mode"] == "LIVE"
        assert json.loads((data / "weekly" / "latest.json").read_text())["status"] == W.APPLIED
        assert [e["id"] for e in W.history(data) if e["event"] == "promote"] == ["bot:runner", "bot:bandbreaker"]

    def test_a_pick_taken_back_is_never_released_again_while_another_is_retried(self, tmp_path):
        # review, 13 Oct: the plan stays PLANNED while one pick keeps failing; a pick already released by it
        # and then taken back (here by hand) must not be switched LIVE again at the next check
        data, cfg = a_folder(tmp_path)
        start = T0 + dt.timedelta(days=1)
        own_db(data, "runner.sqlite", good_days(per_day=6), start=start)
        own_db(data, "bandbreaker.sqlite", good_days(per_day=5), start=start)
        W.tick(data, cfg, PICK_16 + dt.timedelta(minutes=5))
        real = W._set_modes

        def never_bandbreaker(config_path, changes, **kw):
            if "bandbreaker" in changes and changes["bandbreaker"] == "LIVE":
                raise OSError("disk full (made up)")
            return real(config_path, changes, **kw)
        W.tick(data, cfg, REL_19, set_modes=never_bandbreaker)
        assert config(cfg)["runner"]["mode"] == "LIVE"
        real(cfg, {"runner": "PAPER"})                                       # taken back by hand
        for k in range(1, 4):
            W.tick(data, cfg, REL_19 + dt.timedelta(minutes=5 * k), set_modes=never_bandbreaker)
            assert config(cfg)["runner"]["mode"] == "PAPER", k
        assert [e["id"] for e in W.history(data) if e["event"] == "promote"] == ["bot:runner"]

    def test_two_parts_in_one_week_never_widen_into_an_unproven_combination(self, tmp_path, monkeypatch):
        data, cfg = a_folder(tmp_path)
        raw = config(cfg)
        raw["tnb"].update(live_groups=["FX_MINOR"], live_tactics=[MC], live_since_utc="2026-10-09T18:45:00+00:00")
        cfg.write_text(json.dumps(raw))
        # each is fine alone against today's minor pairs x momentum continuation: momentum continuation on indices
        # adds a kind, trend pullback on minor pairs adds an approach. Together the cross product would also hold
        # trend pullback on indices, which has not proved itself: the second is skipped
        parts = {(MC, "INDEX"): trades_of(good_days(per_day=6)), (TP, "FX_MINOR"): trades_of(good_days(per_day=5))}
        monkeypatch.setattr(W, "tnb_part_trades", lambda *a, **k: {k2: [dict(t, risk=13.0) for t in v]
                                                                 for k2, v in parts.items()})
        now = T0 + dt.timedelta(days=11)
        today = (frozenset({"FX_MINOR"}), frozenset({MC}))
        proven = {f"tnb:{MC}:INDEX", f"tnb:{TP}:FX_MINOR"}
        assert W.tnb_add(MC, "INDEX", *today, proven)["ok"] and W.tnb_add(TP, "FX_MINOR", *today, proven)["ok"]
        plan = W.make_plan(data, cfg, now, FRI_16)
        assert [x["pick"]["id"] for x in plan["picks"]] == [f"tnb:{MC}:INDEX"]
        assert any(r.startswith("Trend & Breakout trend pullback, minor pairs") and "has not proved itself" in r
                   for r in plan["reasons"]), plan["reasons"]
        # once trend pullback on indices has proved itself too, both go: it rides along with the second pick
        parts[(TP, "INDEX")] = trades_of(good_days(per_day=4))
        plan = W.make_plan(data, cfg, now, FRI_16)
        assert [x["pick"]["id"] for x in plan["picks"]] == [f"tnb:{MC}:INDEX", f"tnb:{TP}:FX_MINOR"]
        assert plan["picks"][1]["extras"] == [f"tnb:{TP}:INDEX"]
        assert any("goes live with another of today's picks" in r for r in plan["reasons"])
        # released together, the config ends exactly at the checked cross product
        W._write_atomic(W.plan_path(data, FRI_16), plan)
        state = W.load_state(data)
        W._release(data, cfg, plan, W.plan_path(data, FRI_16), state, [], REL_19, W._set_modes)
        t = config(cfg)["tnb"]
        assert sorted(t["live_groups"]) == ["FX_MINOR", "INDEX"] and sorted(t["live_tactics"]) == sorted([MC, TP])
        assert {p["id"] for p in W.load_state(data)["promoted"]} == {f"tnb:{MC}:INDEX", f"tnb:{TP}:FX_MINOR",
                                                                     f"tnb:{TP}:INDEX"}
