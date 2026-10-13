"""The weekly refresh of the practice pool (mintel/lab/refresh.py), 13 Oct.

Aaron: "with constant tweaks on each by you to improve their performance and each
week, X amount of winners go live the following week and we add in new
strategies to the mix to replace and continue this cycle".

What must hold: a clear loser (20+ practice trades in 28 days, net <= 0, profit
factor under 0.9) is RETIRED with its reason and keeps every row and its stage;
the bots are only listed; the top winners get neighbours one grid step away,
children of their parent, never twice, judged on their own trades; a new family
file is found with no other change; the pool is kept at its target.

EVERY FIGURE HERE IS MADE-UP TEST DATA.
"""
from __future__ import annotations

import datetime as dt
import json
import sys
import textwrap

import pytest

from mintel.lab import RETIRED, LabPaths, LabSettings
from mintel.lab import families as FAM
from mintel.lab import refresh as RF
from mintel.lab.candidate import registered
from mintel.lab.ledger import LabLedger
from mintel.lab.nightly import REFRESH_WEEKDAY

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 17, 22, 30, tzinfo=UTC)        # Saturday night
NIGHT = "2026-10-17"
TICKET = [970_100_000]


@pytest.fixture
def lab(tmp_path):
    paths = LabPaths(tmp_path / "data")
    paths.root.mkdir(parents=True)
    led = LabLedger(paths.sqlite)
    yield paths, led
    led.close()


def add(led, c, stage="BINNED"):
    led.register(c, FAM.get(c.family).name(c))
    led.execute("UPDATE candidates SET stage=? WHERE id=?", (stage, c.id))


def trades(led, cid, nets, day="2026-10-14"):
    for k, net in enumerate(nets):
        TICKET[0] += 1
        led.execute("INSERT INTO practice_trades(ticket, candidate_id, symbol, side, volume, opened_utc, closed_utc,"
                    " net_gbp, commission_gbp, risk_gbp, r, fill_source) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (TICKET[0], cid, "EURUSD", 1, 0.1, f"{day}T09:{k % 60:02d}:00+00:00",
                     f"{day}T10:{k % 60:02d}:00+00:00", net, 0.2, 13.0, net / 13.0, "tick"))


# ------------------------------------------------------------------ retire ---
def test_the_retire_rule_thresholds():
    lose = {"trades": 20, "net": -1.0, "pf": 0.89}
    assert RF.is_clear_loser(lose)
    assert not RF.is_clear_loser(dict(lose, trades=19))                 # too few trades to call it
    assert RF.is_clear_loser(dict(lose, net=0.0))                       # level after costs is not earning
    assert not RF.is_clear_loser(dict(lose, net=0.01))
    assert not RF.is_clear_loser(dict(lose, pf=0.9))                    # under 0.9, not at it
    assert not RF.is_clear_loser(dict(lose, pf=None))                   # never lost a trade
    assert RF.profit_factor([2.0, -4.0]) == 0.5 and RF.profit_factor([1.0]) is None


def test_a_clear_loser_is_retired_with_its_reason_and_keeps_every_row(lab):
    paths, led = lab
    cs = registered()[:4]
    for c in cs:
        add(led, c)
    loser, few, level_pf, old = cs
    trades(led, loser.id, [1.0] * 8 + [-1.5] * 12)                       # 20 trades, -10, pf 0.44
    trades(led, few.id, [1.0] * 7 + [-1.5] * 12)                         # 19: not yet
    trades(led, level_pf.id, [1.0] * 10 + [-1.05] * 10)                  # net -0.5, pf 0.95: kept
    trades(led, old.id, [-1.0] * 25, day="2026-09-01")                   # outside the 28 days
    events = led.one("SELECT COUNT(*) FROM events")[0]
    rep = RF.refresh_pool(led, paths, LabSettings.from_dict({"pool_target": 4}), night=NIGHT, now=NOW)
    assert [x["id"] for x in rep["retired"]] == [loser.id]
    row = led.candidate(loser.id)
    assert row["retired_utc"] and "20 trades" in row["retired_reason"] and row["stage"] == "BINNED"
    ev = led.query("SELECT * FROM events WHERE candidate_id=? AND kind='retired'", (loser.id,))
    assert len(ev) == 1 and ev[0]["to_stage"] == RETIRED and ev[0]["from_stage"] == "BINNED"
    assert led.one("SELECT COUNT(*) FROM events")[0] == events + 1
    assert led.one("SELECT COUNT(*) FROM practice_trades WHERE candidate_id=?", (loser.id,))[0] == 20   # kept
    assert all(not led.candidate(c.id)["retired_utc"] for c in (few, level_pf, old))
    assert not led.retire(loser.id, "again", now=NOW)                    # once
    # it is off the practice roster
    from mintel.lab.practice import Practice
    eng = Practice.__new__(Practice)
    eng.settings, eng.ledger, eng.universe = LabSettings(), led, {}
    assert loser.id not in {r["id"] for r in eng._roster_rows()} and few.id in {r["id"] for r in eng._roster_rows()}


def test_the_bots_are_listed_never_retired(lab):
    paths, led = lab
    (paths.data_dir / "weekly").mkdir()
    (paths.data_dir / "weekly" / "latest.json").write_text(json.dumps({"bots": [
        {"id": "bot:crowd", "kind": "bot", "label": "Crowd Fader", "part": "", "trades": 31, "net": -40.0,
         "profit_factor": 0.7},
        {"id": "tnb:MOMENTUM_CONTINUATION:INDEX", "kind": "tnb", "label": "Trend & Breakout",
         "part": "momentum continuation, stock indices", "trades": 25, "net": 12.0, "profit_factor": 1.1}]}))
    rep = RF.refresh_pool(led, paths, LabSettings(), night=NIGHT, now=NOW)
    assert [x["id"] for x in rep["losing_needs_a_look"]] == ["bot:crowd"]
    md = (paths.run_dir(NIGHT) / "refresh.md").read_text()
    assert "Losing, needs a look" in md and "Crowd Fader: 31 trades" in md


# ---------------------------------------------------------------- variants ---
def test_winners_get_neighbours_one_step_away_children_of_their_parent_never_twice(lab):
    paths, led = lab
    vf = [c for c in FAM.get("VWAP_FADE").grid() if c.p("sigma") == 2.5 and c.p("stop_atr") == 1.0][0]
    rb = FAM.get("RANGE_BREAK").grid()[0]                                # nothing tunable: no variants
    add(led, vf)
    add(led, rb)
    trades(led, vf.id, [3.0] * 8 + [-1.0] * 4)                           # 12 trades, +20
    trades(led, rb.id, [3.0] * 12)
    settings = LabSettings.from_dict({"pool_target": 2})
    rep = RF.refresh_pool(led, paths, settings, night=NIGHT, now=NOW)
    kids = [led.candidate(x["id"]) for x in rep["variants"]]
    assert len(kids) == 3 and all(k["parent_id"] == vf.id and k["family"] == "VWAP_FADE" for k in kids)
    from mintel.lab.stages import cand_of
    got = [cand_of(k) for k in kids]
    for k in got:
        moved = {p for p in ("sigma", "stop_atr") if k.p(p) != vf.p(p)}
        assert len(moved) == 1                                           # one setting, one step
        assert (k.markets, k.session, k.news, k.exit) == (vf.markets, vf.session, vf.news, vf.exit)
    assert [(k.p("sigma"), k.p("stop_atr")) for k in got] == [(2.25, 1.0), (2.75, 1.0), (2.5, 1.25)]
    assert {x["parent"] for x in rep["variants"]} == {vf.id}
    # never twice: a forced second run adds the next neighbours, not the same ones
    n = led.one("SELECT COUNT(*) FROM candidates")[0]
    again = RF.refresh_pool(led, paths, settings, night=NIGHT, now=NOW, force=True)
    ids = [x["id"] for x in again["variants"]]
    assert not set(ids) & {k["id"] for k in kids}
    assert led.one("SELECT COUNT(*) FROM candidates")[0] == n + len(ids)
    # the same night again without force: nothing new
    assert RF.refresh_pool(led, paths, settings, night=NIGHT, now=NOW) == again


def test_a_variant_is_judged_on_its_own_trades_never_its_parents(tmp_path):
    from mintel.ops import weekly as W
    paths = LabPaths(tmp_path / "data")
    paths.root.mkdir(parents=True)
    led = LabLedger(paths.sqlite)
    vf = [c for c in FAM.get("VWAP_FADE").grid() if c.p("sigma") == 2.5][0]
    add(led, vf)
    trades(led, vf.id, [3.0] * 30, day="2026-10-14")
    rep = RF.refresh_pool(led, paths, LabSettings.from_dict({"pool_target": 1}), night=NIGHT, now=NOW)
    kid = rep["variants"][0]["id"]
    led.close()
    (paths.data_dir / "config.json").write_text(json.dumps({"ops": {"data_dir": str(paths.data_dir)}}))
    pool = {c["id"]: c for c in W.candidates(paths.data_dir, paths.data_dir / "config.json", NOW, keep_empty=True)}
    assert pool[f"lab:{vf.id}"]["stats"]["trades"] == 30
    assert pool[f"lab:{kid}"]["stats"]["trades"] == 0 and not pool[f"lab:{kid}"]["passes"]


def test_neighbour_values():
    assert RF.neighbour_values([2.0, 2.5, 3.0], 2.5, 1.0, 5.0) == [2.25, 2.75]
    assert RF.neighbour_values([2.0, 2.5, 3.0], 2.0, 1.0, 5.0) == [2.25, 1.5]       # and one step past the edge
    assert RF.neighbour_values([2.0, 2.5, 3.0], 2.25, 1.0, 5.0) == [2.125, 2.375]   # a variant's own neighbours
    assert RF.neighbour_values([10, 20], 10, 2, 60) == [15]                         # whole stays whole; 0 < 2
    assert RF.neighbour_values([0.1], 0.1, 0.0, 1.0) == []                          # one value: no step known


# ------------------------------------------------------------- new ideas ---
def test_a_new_family_file_is_found_with_no_other_change(tmp_path, monkeypatch):
    pkg = tmp_path / "newfams"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "dip_buy.py").write_text(textwrap.dedent('''
        from mintel.lab import Signal
        from mintel.lab.candidate import FamilyBase

        class DipBuy(FamilyBase):
            code = "DIP_BUY_TEST"
            label = "Dip buy (test)"
            features = ("atr",)
            GRID_SIZE = 4
            news_axis = False
            AXES = {"drop_atr": [1.0, 2.0], "exit": ["t1", "t2"], "markets": ["FX_MAJOR"], "news": ["IGNORE"]}

            def signals(self, day, p, span, news):
                return []

            def subject(self, c):
                return f"Dip buy ({c.p('drop_atr'):g})"

            def rule_words(self, c):
                return ["Buys a dip."]
    '''))
    (pkg / "broken.py").write_text("raise RuntimeError('half written')\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    found = FAM.discover([str(pkg)], "newfams")
    assert [c.code for c in found] == ["DIP_BUY_TEST"]                  # the broken one is left out
    assert {c.code for c in FAM.discover()} == set(FAM.FAMILIES)          # this folder: the nine, found the same way
    # once registered (as a file in mintel/lab/families/ would be), its grid joins the pool at the refresh
    monkeypatch.setitem(FAM.FAMILIES, "DIP_BUY_TEST", found[0]())
    new = [c for c in registered() if c.family == "DIP_BUY_TEST"]
    assert len(new) == 4
    paths = LabPaths(tmp_path / "data")
    paths.root.mkdir(parents=True)
    led = LabLedger(paths.sqlite)
    for c in registered()[:632]:
        add(led, c)
    rep = RF.refresh_pool(led, paths, LabSettings(), night=NIGHT, now=NOW)
    assert sorted(x["id"] for x in rep["new"]) == sorted(c.id for c in new) and rep["pool"] == 636
    assert sys.modules["newfams.dip_buy"]
    led.close()


def test_the_pool_is_kept_at_its_target(lab):
    paths, led = lab
    cs = registered()
    for c in cs[:50]:
        add(led, c)
    losers = cs[:5]
    for c in losers:
        trades(led, c.id, [-1.0] * 20)
    rep = RF.refresh_pool(led, paths, LabSettings.from_dict({"pool_target": 60}), night=NIGHT, now=NOW)
    assert len(rep["retired"]) == 5 and len(rep["new"]) == 15 and rep["pool"] == RF.pool_size(led) == 60
    assert [x["id"] for x in rep["new"]] == [c.id for c in cs[50:65]]    # the grid's waiting ideas, in order
    assert all(led.candidate(x["id"])["stage"] == "IDEA" for x in rep["new"])
    # an over-full pool adds nothing (nothing is retired to make room)
    rep2 = RF.refresh_pool(led, paths, LabSettings.from_dict({"pool_target": 40}), night="2026-10-24",
                           now=NOW + dt.timedelta(days=7))
    assert rep2["new"] == [] and rep2["pool"] == 60
    # the report: refresh.md for the upload, the block summary.json carries, the pulse's key
    assert (paths.run_dir(NIGHT) / "refresh.md").exists()
    from mintel.lab.report import upload_files
    assert f"reports/lab/{NIGHT}/refresh.md" in upload_files(paths, NIGHT)
    block = RF.summary_block(json.loads((paths.root / "refresh.json").read_text()))
    assert block["pool"] == 60 and block["retired_count"] == 0 and block["night"] == "2026-10-24"
    from mintel.ops.pulse import LAB_SUMMARY_KEYS
    assert "refresh" in LAB_SUMMARY_KEYS


def test_the_nightly_refreshes_on_saturday_only():
    assert REFRESH_WEEKDAY == dt.date.fromisoformat(NIGHT).weekday() == 5
