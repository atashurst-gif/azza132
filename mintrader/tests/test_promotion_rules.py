"""14 Oct: the squad's promotion rules (mintel/ops/weekly.py; docs/ops/promotion.md).

Aaron: "promising evidence, potentially after 10-15 trades, depending on strategy
type and trading sessions"; "don't demand full real-money qualification
standards before demo"; "avoid treating short winning streaks as proof"; 8-12
on the demo; lab ideas really trade there; sizes compared by tier.

A. the cadence: every UK trading day at 22:30, released 06:30 the next trading day;
B. the early bar by frequency (10 / 12 / 15 trades, 3+ UK days, PF 1.3, positive
   without the best trade);
C. selection-bias protection: empirical-Bayes shrinkage, the control benchmark,
   the lab tests as a weight;
D. the diversity cap, the spread preference, the demo roster target;
E. the demo judgement: bootstrap bounds after 20, net after 40, 15 x risk, the
   drawdown guard, kept;
F. a lab idea promoted and demoted through the roster: its magic, its tier in
   turn, no restart, every reason in history; a broken roster decides nothing
   for the lab and blocks nothing else; never re-released by the same plan;
G. the £1,500-account equivalent beside the demo money, never in it.

EVERY FIGURE HERE IS MADE-UP TEST DATA, in temporary folders: nothing here
measures performance.
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from mintel.labdemo import record as REC
from mintel.labdemo import roster as LR
from mintel.ops import weekly as W
from tests.test_weekly_pick import (FRI_16, MC, PICK_16, REL_19, T0, TP, a_folder, a_state, config, good_days,
                                    lab_db, own_db, tnb_item, trades_of)

UTC = dt.timezone.utc


def spread(n: int, per_day: float, start: dt.datetime = T0, win: float = 6.0, loss: float = -4.0) -> list[dict]:
    """``n`` made-up trades, ``per_day`` on each UK trading day (a slow test: one every 1/per_day trading days),
    three wins to every loss."""
    out, day, k, made = [], start, 0, 0.0
    while k < n:
        if day.weekday() < 5:
            made += per_day
            while made >= 1.0 - 1e-9 and k < n:
                net = loss if k % 4 == 3 else win
                out.append({"closed": day + dt.timedelta(minutes=10 * (k % 30)), "net": net, "costs": 0.5,
                            "symbol": "EURGBP", "ticket": k + 1, "kind": "practice", "risk": 10.0})
                k += 1
                made -= 1.0
        day += dt.timedelta(days=1)
    return out


def last_day(trades) -> dt.datetime:
    return max(t["closed"] for t in trades) + dt.timedelta(minutes=5)


# ============================================================ A. the cadence --
class TestCadence:
    def test_every_uk_trading_day_at_2230_released_0630_the_next(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        own_db(data, "runner.sqlite", good_days(days=6, per_day=4), start=T0 + dt.timedelta(days=1))
        tue = dt.datetime(2026, 10, 13, 21, 31, tzinfo=UTC)                     # Tuesday 22:31 BST
        W.tick(data, cfg, tue - dt.timedelta(minutes=2))
        assert not (data / "weekly" / "plan-2026-10-13.json").exists()
        W.tick(data, cfg, tue)
        plan = json.loads((data / "weekly" / "plan-2026-10-13.json").read_text())
        assert plan["status"] == W.PLANNED and plan["day"] == "2026-10-13"
        assert plan["release_utc"] == "2026-10-14T05:30:00+00:00"                 # Wednesday 06:30 BST
        assert plan["expires_utc"] == "2026-10-14T21:30:00+00:00"                 # the next pick
        W.tick(data, cfg, dt.datetime(2026, 10, 14, 5, 29, tzinfo=UTC))
        assert config(cfg)["runner"]["mode"] == "PAPER"
        W.tick(data, cfg, dt.datetime(2026, 10, 14, 5, 30, tzinfo=UTC))
        assert config(cfg)["runner"]["mode"] == "LIVE"

    def test_no_pick_at_the_weekend_and_fridays_goes_on_monday(self, tmp_path):
        assert W.last_pick_day(dt.datetime(2026, 10, 17, 22, 0, tzinfo=UTC)) == FRI_16       # Saturday 23:00
        assert W.release_utc(FRI_16) == REL_19
        assert W.next_pick_utc(dt.datetime(2026, 10, 17, 12, tzinfo=UTC)) == dt.datetime(2026, 10, 19, 21, 30,
                                                                                        tzinfo=UTC)
        data, cfg = a_folder(tmp_path)
        own_db(data, "runner.sqlite", good_days(days=6, per_day=4), start=T0 + dt.timedelta(days=4))
        W.tick(data, cfg, dt.datetime(2026, 10, 17, 21, 40, tzinfo=UTC))       # Saturday 22:40: Friday's catch-up
        assert sorted(p.name for p in (data / "weekly").glob("plan-*")) == ["plan-2026-10-16.json"]

    def test_the_veto_and_halt_still_hold_daily(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        own_db(data, "runner.sqlite", good_days(days=6, per_day=4), start=T0 + dt.timedelta(days=1))
        tue = dt.datetime(2026, 10, 13, 21, 31, tzinfo=UTC)
        W.tick(data, cfg, tue)
        (data / "HALT").write_text("")
        W.tick(data, cfg, dt.datetime(2026, 10, 14, 5, 31, tzinfo=UTC))
        assert config(cfg)["runner"]["mode"] == "PAPER"
        W.tick(data, cfg, dt.datetime(2026, 10, 14, 21, 31, tzinfo=UTC))       # Wednesday's pick: Tuesday's lapses
        assert json.loads((data / "weekly" / "plan-2026-10-13.json").read_text())["status"] == W.EXPIRED
        (data / "HALT").unlink()
        assert "will not go live" in W.veto(data)
        W.tick(data, cfg, dt.datetime(2026, 10, 15, 5, 31, tzinfo=UTC))
        assert config(cfg)["runner"]["mode"] == "PAPER"

    def test_a_veto_before_the_release_outlasts_the_next_days_pick(self, tmp_path):
        # review, 14 Oct: a veto recorded nowhere lasted only until the next day's 22:30 pick, which took the same
        # test again on the same trades. Now it restarts the test's evidence window, as a demotion does
        data, cfg = a_folder(tmp_path)
        own_db(data, "runner.sqlite", good_days(days=6, per_day=4), start=T0 + dt.timedelta(days=1))
        W.tick(data, cfg, dt.datetime(2026, 10, 12, 21, 31, tzinfo=UTC))       # Monday 22:31 BST: the pick
        assert "will not go live" in W.veto(data)
        W.tick(data, cfg, dt.datetime(2026, 10, 13, 5, 31, tzinfo=UTC))        # Tuesday 06:31: not released
        W.tick(data, cfg, dt.datetime(2026, 10, 13, 5, 36, tzinfo=UTC))
        assert config(cfg)["runner"]["mode"] == "PAPER"
        assert [(e["event"], e["id"], e["plan"]) for e in W.history(data)] == [("veto", "bot:runner", "2026-10-12")]
        W.tick(data, cfg, dt.datetime(2026, 10, 13, 21, 31, tzinfo=UTC))       # Tuesday's pick: not the runner
        assert json.loads((data / "weekly" / "plan-2026-10-13.json").read_text())["picks"] == []
        W.tick(data, cfg, dt.datetime(2026, 10, 14, 5, 31, tzinfo=UTC))
        assert config(cfg)["runner"]["mode"] == "PAPER"
        assert [e["event"] for e in W.history(data)] == ["veto"]
        # a veto is not a ban: fresh evidence after it can earn the place again
        own_db(data, "runner.sqlite", good_days(days=6, per_day=4), start=dt.datetime(2026, 10, 14, 8, 0, tzinfo=UTC),
               first_ticket=100)
        W.tick(data, cfg, dt.datetime(2026, 10, 20, 21, 31, tzinfo=UTC))       # Tuesday 20 Oct 22:31 BST
        plan = json.loads((data / "weekly" / "plan-2026-10-20.json").read_text())
        assert [x["pick"]["id"] for x in plan["picks"]] == ["bot:runner"]

    def test_a_veto_line_for_the_pick_and_each_part_that_would_have_ridden_with_it(self, tmp_path):
        data, _cfg = a_folder(tmp_path)
        pick = {"id": f"tnb:{MC}:INDEX", "kind": "tnb", "label": "Trend & Breakout", "part": W.part_words(MC, "INDEX")}
        plan = {"friday": FRI_16.isoformat(), "status": W.PLANNED, "ranking": [],
                "picks": [{"pick": pick, "extras": [f"tnb:{TP}:INDEX"]}]}
        ppath = W.plan_path(data, FRI_16)
        W._write_atomic(ppath, plan)
        W.veto_path(data, FRI_16).write_text("x")
        state = {"schema": 1, "promoted": [], "restart_pending": []}
        assert W._vetoes(data, state, plan, ppath, REL_19) == ["weekly: this week's pick was vetoed"]
        ev = W.history(data)
        assert [(e["event"], e["id"], e["label"]) for e in ev] == [("veto", f"tnb:{MC}:INDEX", "Trend & Breakout"),
                                                                  ("veto", f"tnb:{TP}:INDEX", "Trend & Breakout")]
        assert W.last_demoted(ev) == {f"tnb:{MC}:INDEX": REL_19, f"tnb:{TP}:INDEX": REL_19}
        assert W._vetoes(data, state, plan, ppath, REL_19) == [] and len(W.history(data)) == 2   # once


# ================================================== B. the bar by frequency --
class TestEarlyBar:
    @pytest.mark.parametrize("per_day,need", [(0.5, 10), (2.0, 12), (5.0, 15)])
    def test_the_trades_needed_by_how_often_it_trades(self, per_day, need):
        assert W.needed_trades(per_day) == need
        ok = spread(need, per_day)
        j = W.judge(ok, f"x{per_day}", last_day(ok))
        assert j["need"] == need and j["passes"], j["why"]
        few = ok[:-1]
        j = W.judge(few, f"y{per_day}", last_day(few))
        assert not j["passes"] and "more trade" in j["why"]

    def test_three_uk_days_whatever_the_count(self):
        t = spread(30, 15.0)                                                      # 30 trades in 2 days
        j = W.judge(t, "two-days", last_day(t))
        assert j["days_traded"] == 2 and not j["passes"] and "1 more UK day" in j["why"]

    def test_positive_after_costs_and_a_profit_factor_of_1_3(self):
        t = spread(12, 2.0, win=3.95, loss=-9.0)                                  # PF 35.55 / 27 = 1.32
        assert W.judge(t, "pf13", last_day(t))["passes"]
        t = spread(12, 2.0, win=3.8, loss=-9.0)                                   # PF 1.27
        j = W.judge(t, "pf127", last_day(t))
        assert not j["passes"] and "under 1.3" in j["why"]
        t = spread(12, 2.0, win=1.0, loss=-9.0)
        assert "not positive after costs" in W.judge(t, "neg", last_day(t))["why"]

    def test_a_short_winning_streak_is_not_proof(self):
        t = spread(12, 2.0, win=-1.0, loss=-1.0)
        t[5]["net"] = 40.0                                                        # one big winner and nothing else
        j = W.judge(t, "streak", last_day(t))
        assert not j["passes"] and "hangs on one trade" in j["why"]


# =============================================== C. selection-bias protection --
def cand(cid: str, trades: list[dict], now: dt.datetime, **extra) -> dict:
    c = dict(W._raw(trades, cid, now), id=cid, kind="lab", label="The Lab", part=cid, live=False, live_path=True,
             risk_gbp=10.0, **extra)
    c.setdefault("market", "FX_MAJOR")
    c.setdefault("session", "LONDON")
    c.setdefault("direction", "BOTH")
    c["div"] = W.div_key(c)
    return c


class TestSelectionBias:
    def test_the_shrinkage_arithmetic(self):
        assert W.shrink(1.0, 0.0, (0.0, 0.04)) == 1.0                             # no noise: its own mean
        assert W.shrink(1.0, 0.04, (0.0, 0.04)) == pytest.approx(0.5)             # as noisy as the pool: halfway
        assert W.shrink(1.0, 1e9, (-0.1, 0.04)) == pytest.approx(-0.1)            # all noise: the pool's mean
        pool = [{"mean_r": m, "se2_r": 0.01, "stats": {"trades": 20}} for m in (-0.2, -0.1, 0.0, 0.1, 0.2)]
        mu0, tau2 = W.pool_prior(pool)
        assert mu0 == pytest.approx(0.0, abs=1e-9) and tau2 == pytest.approx(0.025 - 0.01, abs=1e-6)
        assert W.pool_prior(pool[:3]) == (0.0, W.TAU2_DEFAULT)                    # too small a pool: the fixed prior

    def test_a_few_lucky_trades_rank_below_a_steady_record(self):
        now = T0 + dt.timedelta(days=13)
        lucky = spread(10, 0.8, win=-5.0, loss=-5.0)                              # a big raw average, very noisy:
        lucky[2]["net"] = lucky[6]["net"] = 50.0                                  # two big wins, eight small losses
        steady = spread(40, 3.0, win=4.0, loss=-3.0)                              # smaller, but 40 trades of it
        pool = [cand("lucky", lucky, now), cand("steady", steady, now)]
        pool += [cand(f"noise{k}", spread(12, 1.0, win=3.0 + k % 3, loss=-6.0 - k % 4), now) for k in range(8)]
        W.score_pool(pool)
        by = {c["id"]: c for c in pool}
        assert by["lucky"]["mean_r"] > by["steady"]["mean_r"]                     # raw: the lucky one looks better
        assert by["steady"]["shrunk_r"] > by["lucky"]["shrunk_r"]                 # shrunk: the steady one is
        assert [c["id"] for c in W.rank(pool) if c["passes"]][0] == "steady"

    def test_it_must_clearly_beat_the_best_clock_trade(self):
        now = T0 + dt.timedelta(days=13)
        idea = cand("idea", spread(15, 2.0, win=4.0, loss=-5.0), now)
        ctl = cand("clock", spread(40, 3.0, win=4.0, loss=-4.0), now, control=True)   # luck, a strong run of it
        pool = [idea, ctl] + [cand(f"n{k}", spread(12, 1.0, win=2.0, loss=-3.0 - k), now) for k in range(6)]
        W.score_pool(pool)
        assert idea["control_bar"] == ctl["shrunk_r"] and idea["shrunk_r"] > 0
        assert not idea["passes"] and "does not clearly beat the best clock trade" in idea["why"]
        assert not ctl["passes"] and "never promoted" in ctl["why"]                # a control is never promoted
        # without that lucky clock, the very same record passes
        pool = [cand("idea", spread(15, 2.0, win=4.0, loss=-5.0), now)] + pool[2:]
        W.score_pool(pool)
        assert pool[0]["passes"] and pool[0]["control_bar"] is None

    def test_the_lab_tests_are_a_weight_never_a_gate(self):
        assert W.lab_weight([]) == (1.0, "")
        w, why = W.lab_weight([{"kind": "past", "trades": 80, "net_gbp": -120.0, "pf": 0.6, "finished_utc": "a"},
                               {"kind": "fresh", "trades": 30, "net_gbp": -5.0, "pf": 0.95, "finished_utc": "a"}])
        assert w == pytest.approx(0.5 * 0.85) and "strongly negative" in why
        w2, _ = W.lab_weight([{"kind": "past", "trades": 80, "net_gbp": 50.0, "pf": 1.4, "finished_utc": "b"},
                              {"kind": "past", "trades": 80, "net_gbp": -120.0, "pf": 0.6, "finished_utc": "a"}])
        assert w2 == 1.0                                                          # only the latest of each kind
        now = T0 + dt.timedelta(days=13)
        t = spread(15, 2.0)
        a, b = cand("a", t, now), cand("b", list(t), now, weight=0.25)
        W.score_pool([a, b])
        assert a["passes"] and b["passes"] and b["score"] == pytest.approx(0.25 * a["score"], abs=1e-3)


# ========================================= D. diversity, spread, the target --
def ranked(n: int, now: dt.datetime, **extra) -> list[dict]:
    out = [cand(f"lab:lab-{k:010d}", spread(15 + 3 * (n - k), 3.0), now, **extra) for k in range(n)]
    return W.rank(W.score_pool(out))


class TestTheSquad:
    def test_at_most_two_per_market_and_direction(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        now = T0 + dt.timedelta(days=13)
        plan = W.make_plan(data, cfg, now, FRI_16, ranking=ranked(3, now))
        assert len(plan["picks"]) == 2
        assert any("2 already on the demo in FX_MAJOR/BOTH - waits" in r for r in plan["reasons"])
        # one already on the demo in that market and direction: one more place there
        a_state(data, {"id": "lab:lab-x", "kind": "lab", "label": "The Lab", "div": "FX_MAJOR/BOTH",
                       "market": "FX_MAJOR", "session": "LONDON", "plan": "2026-10-15"})
        assert len(W.make_plan(data, cfg, now, FRI_16, ranking=ranked(3, now))["picks"]) == 1

    def test_a_part_riding_on_the_demo_counts_toward_the_cap(self, tmp_path):
        # review, 14 Oct: a Trend & Breakout part that went live with a pick trades on the demo like any test, so it
        # counts toward the 2 a market and direction (it still holds no place of the demo target)
        data, cfg = a_folder(tmp_path)
        now = T0 + dt.timedelta(days=13)
        a_state(data, {"id": "lab:lab-x", "kind": "lab", "label": "The Lab", "div": "FX_MAJOR/BOTH",
                       "market": "FX_MAJOR", "session": "LONDON", "plan": "2026-10-15"},
                tnb_item(MC, "FX_MINOR"), tnb_item(TP, "FX_MAJOR", rides_with=f"tnb:{MC}:FX_MINOR"))
        plan = W.make_plan(data, cfg, now, FRI_16, ranking=ranked(3, now))
        assert plan["limit"] == W.DEMO_TARGET - 2 and plan["picks"] == []
        assert any("2 already on the demo in FX_MAJOR/BOTH - waits" in r for r in plan["reasons"])

    def test_a_pick_whose_riding_parts_would_break_the_cap_waits(self, tmp_path, monkeypatch):
        # review, 14 Oct: trend pullback on minor pairs would also take trend pullback on indices live, a third test
        # in INDEX/BOTH: the plan said "waits" for it, yet it went live as a rider
        data, cfg = a_folder(tmp_path)
        raw = config(cfg)
        raw["runner"]["mode"] = raw["bandbreaker"]["mode"] = "LIVE"
        raw["tnb"].update(live_groups=["INDEX"], live_tactics=[MC], live_since_utc="2026-10-09T18:45:00+00:00")
        cfg.write_text(json.dumps(raw))
        a_state(data, *[{"id": f"bot:{b}", "kind": "bot", "label": b, "bot": b, "market": "INDEX", "session": "ALL",
                         "direction": "BOTH", "div": "INDEX/BOTH", "plan": "2026-10-15", "process": "trader"}
                        for b in ("runner", "bandbreaker")])
        parts = {(TP, "FX_MINOR"): trades_of(good_days(per_day=6)), (MC, "FX_MINOR"): trades_of(good_days(per_day=5)),
                 (TP, "INDEX"): trades_of(good_days(per_day=4))}
        monkeypatch.setattr(W, "tnb_part_trades", lambda *a, **k: {k2: [dict(t, risk=13.0) for t in v]
                                                                 for k2, v in parts.items()})
        plan = W.make_plan(data, cfg, T0 + dt.timedelta(days=11), FRI_16)
        assert [(x["pick"]["id"], x["extras"]) for x in plan["picks"]] == [(f"tnb:{MC}:FX_MINOR", [])]
        assert any(r.startswith("Trend & Breakout trend pullback, minor pairs") and
                   "more than 2 would be on the demo in INDEX/BOTH - waits" in r for r in plan["reasons"])
        W._write_atomic(W.plan_path(data, FRI_16), plan)
        W._release(data, cfg, plan, W.plan_path(data, FRI_16), W.load_state(data), [], REL_19, W._set_modes)
        prom = W.load_state(data)["promoted"]
        assert sorted(p["id"] for p in prom if W.slot(p)["div"] == "INDEX/BOTH") == ["bot:bandbreaker", "bot:runner"]
        raw, modes = W.strict_view(cfg)
        assert W.is_live(f"tnb:{MC}:FX_MINOR", raw, modes) and not W.is_live(f"tnb:{TP}:INDEX", raw, modes)

    def test_a_spread_across_markets_and_sessions_is_preferred(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        now = T0 + dt.timedelta(days=13)
        a_state(data, {"id": "lab:lab-x", "kind": "lab", "label": "The Lab", "div": "FX_MAJOR/LONG",
                       "market": "FX_MAJOR", "session": "LONDON", "plan": "2026-10-15"})
        r = ranked(2, now)                                                        # both FX_MAJOR, London
        r[1].update(market="GOLD", session="NEWYORK")
        r[1]["div"] = W.div_key(r[1])
        assert r[0]["score"] > r[1]["score"] > 0.85 * 0.9 * r[0]["score"]
        plan = W.make_plan(data, cfg, now, FRI_16, ranking=r, limit=1)
        assert plan["pick"]["id"] == r[1]["id"]                                   # the new market goes first

    def test_the_demo_roster_target(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        now = T0 + dt.timedelta(days=13)
        r = ranked(6, now)
        for k, c in enumerate(r):
            c.update(market=f"M{k}")
            c["div"] = W.div_key(c)
        assert len(W.make_plan(data, cfg, now, FRI_16, ranking=r)["picks"]) == 6       # under the default 10
        (data / "weekly.json").write_text(json.dumps({"demo_target": 3}))
        a_state(data, {"id": "bot:crowd", "kind": "bot", "label": "Crowd Fader", "plan": "2026-10-15"},
                {"id": "tnb:X:Y", "kind": "tnb", "label": "T&B", "rides_with": "bot:crowd", "plan": "2026-10-15"})
        plan = W.make_plan(data, cfg, now, FRI_16, ranking=r)
        assert plan["limit"] == 2 and len(plan["picks"]) == 2                     # a rider holds no place
        a_state(data, *[{"id": f"bot:{b}", "kind": "bot", "label": b, "plan": "x"} for b in ("crowd", "ian", "runner")])
        plan = W.make_plan(data, cfg, now, FRI_16, ranking=r)
        assert plan["picks"] == [] and any("the demo squad is full (3 of 3)" in x for x in plan["reasons"])


# ===================================================== E. the demo judgement --
def demo(nets: list[float]) -> list[dict]:
    return [{"closed": T0 + dt.timedelta(minutes=10 * k), "net": n} for k, n in enumerate(nets)]


class TestDemoJudgement:
    def test_after_twenty_the_bootstrap_upper_bound(self):
        assert W.live_verdict(demo([-1.0, -2.0] * 10), 10.0, "a")[0] == "demote"
        v, why = W.live_verdict(demo([3.0, -3.5] * 10), 10.0, "b")                # losing a little, still unclear
        assert v == "wait" and "not clear yet" in why
        assert W.live_verdict(demo([-1.0] * 19), 10.0, "c")[0] == "wait"          # not 20 yet

    def test_after_forty_net_at_or_below_zero(self):
        nets = [5.0, -5.2] * 20                                                   # 40 trades, -4.00
        _p, _lo, hi = W.bootstrap_all(nets, "d")
        assert hi > 0                                                             # the bound alone would not do it
        v, why = W.live_verdict(demo(nets), 10.0, "d")
        assert v == "demote" and "not in profit after 40" in why
        assert W.live_verdict(demo(nets[:38]), 10.0, "e")[0] == "wait"

    def test_the_early_exit_and_the_drawdown_guard(self):
        v, why = W.live_verdict(demo([-50.0, -50.0, -50.0]), 10.0, "f")           # -150 = 15 x 10
        assert v == "demote" and "early exit" in why
        assert W.live_verdict(demo([-49.0, -50.0, -50.0]), 10.0, "g")[0] == "wait"
        v, why = W.live_verdict(demo([20.0] * 15 + [-25.0] * 8), 10.0, "h")       # up 300, then 200 back
        assert v == "demote" and "drawdown guard" in why

    def test_kept_on_clear_evidence(self):
        v, why = W.live_verdict(demo([4.0, -1.0] * 10), 10.0, "i")
        assert v == "keep" and "it stays" in why


# ===================================== F. a lab idea through the roster --
def real_idea(data: Path, k: int, nets_by_day, start=T0):
    from mintel.lab.candidate import registered
    from mintel.lab.ledger import LabLedger
    c = registered()[k]
    led = LabLedger(data / "lab" / "lab.sqlite")
    led.register(c, f"long name {k}")
    led.execute("UPDATE candidates SET stage='PRACTICE' WHERE id=?", (c.id,))
    n = 0
    for d, nets in enumerate(nets_by_day):
        for i, net in enumerate(nets):
            n += 1
            t = start + dt.timedelta(days=d, minutes=15 * i)
            led.execute("INSERT INTO practice_trades(ticket, candidate_id, symbol, side, closed_utc, net_gbp, "
                        "commission_gbp, risk_gbp, fill_source) VALUES(?,?,?,?,?,?,?,?,?)",
                        (960_000_000 + 1000 * k + n, c.id, "EURUSD", 1, t.isoformat(), net, 0.4, 13.0, "tick"))
    led.close()
    return c


def three_ideas(data: Path):
    """Three real, non-control ideas on three different markets (so the diversity cap leaves them all)."""
    from mintel.lab.candidate import registered
    want, picked = ["FX_MAJOR", "INDEX", "GOLD"], []
    for k, c in enumerate(registered()):
        if c.family != W.CONTROL_FAMILY and want and c.markets == want[0]:
            picked.append(real_idea(data, k, good_days(days=8, per_day=4 + len(picked)), start=T0 + dt.timedelta(days=1)))
            want.pop(0)
    return picked


def book(data: Path, idea, nets, since: dt.datetime, magic: int, first: int = 1) -> None:
    rec = REC.DemoRecord(data)
    for k, n in enumerate(nets):
        o = since + dt.timedelta(minutes=30 * k + 1)
        rec.book({"ticket": first + k, "idea_id": idea.id, "version": idea.version, "magic": magic, "symbol": "EURUSD",
                  "side": 1, "volume": 0.1, "opened_utc": o.isoformat(),
                  "closed_utc": (o + dt.timedelta(minutes=20)).isoformat(), "net_gbp": n, "risk_gbp": 10.0,
                  "tier_pct": 0.5, "r": n / 10.0, "equiv_1500_gbp": REC.equiv_1500(n / 10.0), "exit_reason": "TIME"})
    rec.close()


class TestLabThroughTheRoster:
    def test_promoted_with_its_own_magic_and_tier_in_turn_with_no_restart(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        ideas = three_ideas(data)
        W.tick(data, cfg, PICK_16 + dt.timedelta(minutes=5))
        plan = json.loads((data / "weekly" / "latest.json").read_text())
        assert {x["pick"]["id"] for x in plan["picks"]} == {f"lab:{c.id}" for c in ideas}
        W.tick(data, cfg, REL_19)
        roster = LR.read_roster(data, strict=True)
        assert set(roster) == {c.id for c in ideas}
        order = [e["id"][4:] for e in W.history(data) if e["event"] == "promote"]
        assert [roster[i]["tier_pct"] for i in order] == [0.5, 1.0, 2.0]          # the tiers in turn
        assert sorted(roster[i]["magic"] for i in order) == [992_001, 992_002, 992_003]
        st = W.load_state(data)
        assert st["restart_pending"] == []                                        # the engine re-reads its roster
        assert {p["id"]: p["tier_pct"] for p in st["promoted"]} == {f"lab:{i}": roster[i]["tier_pct"] for i in order}
        for e in W.history(data):
            assert e["event"] == "promote" and e["why"].startswith("the pick of 2026-10-16: passes the bar:")
            assert e["tier_pct"] in LR.TIERS and LR.MAGIC_FIRST <= e["magic"] <= LR.MAGIC_LAST
        assert all(config(cfg)[b]["mode"] == "PAPER" for b in ("runner", "bandbreaker", "crowd"))

    def test_judged_on_the_brokers_record_and_taken_back(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        ideas = three_ideas(data)
        W.tick(data, cfg, PICK_16 + dt.timedelta(minutes=5))
        W.tick(data, cfg, REL_19)
        loser = ideas[0]
        magic = LR.read_roster(data)[loser.id]["magic"]
        book(data, loser, [-2.0, -1.5] * 10, REL_19, magic)                        # 20 losing demo trades
        book(data, ideas[1], [-60.0] * 3, REL_19 - dt.timedelta(days=3), 992_999, first=500)   # before: not counted
        acts = W.tick(data, cfg, REL_19 + dt.timedelta(hours=12))
        assert loser.id not in LR.read_roster(data) and len(LR.read_roster(data)) == 2
        ev = W.history(data)[-1]
        assert ev["event"] == "demote" and ev["id"] == f"lab:{loser.id}" and "20 demo trades" in ev["why"]
        assert ev["record"]["source"].startswith("MetaTrader") and ev["record"]["trades"] == 20
        # the £1,500 equivalent rides beside the demo money, never in it
        assert ev["record"]["net"] == -35.0 and ev["record"]["equiv_1500"] == pytest.approx(-35.0 / 10.0 * 7.5, abs=0.1)
        assert W.load_state(data)["restart_pending"] == [] and any("back on PAPER" in a for a in acts)
        # its place is filled from the qualifiers at the next release, never by the plan that took it back
        for k in range(1, 4):
            W.tick(data, cfg, REL_19 + dt.timedelta(hours=12, minutes=5 * k))
            assert loser.id not in LR.read_roster(data)

    def test_a_broken_roster_decides_nothing_for_the_lab_and_blocks_nothing_else(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        ideas = three_ideas(data)
        own_db(data, "runner.sqlite", good_days(days=8, per_day=4), start=T0 + dt.timedelta(days=1))
        W.tick(data, cfg, PICK_16 + dt.timedelta(minutes=5))
        LR.roster_path(data).parent.mkdir(parents=True, exist_ok=True)
        LR.roster_path(data).write_text("{ half")
        acts = W.tick(data, cfg, REL_19)
        assert config(cfg)["runner"]["mode"] == "LIVE"                            # the bot still goes
        assert LR.roster_path(data).read_text() == "{ half"                       # never overwritten
        assert any("roster could not be read" in a for a in acts)
        assert {p["id"] for p in W.load_state(data)["promoted"]} == {"bot:runner"}
        assert ideas

    def test_a_lab_pick_taken_back_by_hand_is_never_re_released_by_the_same_plan(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        ideas = three_ideas(data)
        W.tick(data, cfg, PICK_16 + dt.timedelta(minutes=5))
        W.tick(data, cfg, REL_19)
        ros = LR.read_roster(data)
        ros.pop(ideas[0].id)
        LR.write_roster(data, ros)                                                # taken off by hand
        for k in range(1, 4):
            W.tick(data, cfg, REL_19 + dt.timedelta(minutes=5 * k))
            assert ideas[0].id not in LR.read_roster(data)
        assert [e["event"] for e in W.history(data) if e["id"] == f"lab:{ideas[0].id}"] == ["promote", "gone"]

    def test_a_control_idea_is_never_promoted(self, tmp_path):
        data, cfg = a_folder(tmp_path)
        from mintel.lab.candidate import registered
        k = next(k for k, c in enumerate(registered()) if c.family == W.CONTROL_FAMILY)
        c = real_idea(data, k, good_days(days=10, per_day=6))
        got = W.candidates(data, cfg, T0 + dt.timedelta(days=11))
        one = next(x for x in got if x["id"] == f"lab:{c.id}")
        assert one["control"] and not one["passes"] and not one["live_path"]
        with pytest.raises(ValueError, match="never promoted"):
            W.lab_promote_change(dict(one, tier_pct=1.0), {W.LABDEMO: frozenset()}, data)


# ================================================== G. the £1,500 equivalent --
def test_the_1500_account_equivalent():
    assert REC.EQUIV_PER_R == 7.5
    assert REC.equiv_1500(2.0) == 15.0 and REC.equiv_1500(-1.0) == -7.5 and REC.equiv_1500(None) is None
