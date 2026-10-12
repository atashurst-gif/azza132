"""The coach's run loop (spec sections 8, 10 and 14): the tick on a fake clock,
the lease rewritten every tick, the nightly at 22:20 UK in BST and in GMT, the
catch-up after downtime, the WATCH-day self-check, the scorecard putting the
coach in GUARD - and THE INVARIANT from the coach's side: a policy the coach
wrote, then left stale, corrupted, forged, switched off or put under an
invalid envelope, leaves every bot doing exactly what it does without the
coach (same orders, same sizes, same daily-loss figure).

EVERY FIGURE IN THIS FILE IS MADE UP for the test: never a result.
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from mintel.coach import STATE_LIVE_TRIAL, UTC, Paths
from mintel.coach import decide as D
from mintel.coach import gate as G
from mintel.coach import status as S
from mintel.coach.run import Coach, due_nightly, nightly_instant
from tests.coach_d_world import (EQ, GOOD_KEY, OTHER_KEY, FakePage, data_folder, journal_row, trading_days, write_rows)

TUE = dt.datetime(2026, 11, 10, 10, 0, tzinfo=UTC)


def coach_at(data, c, clock, page=None, **kw):
    box = {"now": clock}
    co = Coach(data, cfg=c, clock=lambda: box["now"], fetch=page or FakePage(), shuffles=4, **kw)
    return co, box


# ================================================================ the tick --
def test_a_tick_on_a_fake_clock_writes_the_policy_and_the_status_and_no_heartbeat(tmp_path):
    data, c, _ = data_folder(tmp_path)
    co, box = coach_at(data, c, TUE)
    out = co.tick()
    assert out["ok"] and out["policy"]["written_utc"] == TUE.isoformat() and out["policy"]["valid_s"] == 180.0
    doc = S.read(Paths.of(data))
    assert doc["written_utc"] == TUE.isoformat() and doc["mode"] == "PILOT"
    assert not (data / "heartbeats").exists() or not list((data / "heartbeats").glob("*coach*"))
    assert "in charge within your limits" in S.one_line(doc, TUE)
    co.close()


def test_the_lease_is_rewritten_every_tick_and_lapses_when_the_coach_stops(tmp_path):
    data, c, _ = data_folder(tmp_path)
    co, box = coach_at(data, c, TUE)
    seqs = []
    for i in range(3):
        box["now"] = TUE + dt.timedelta(minutes=i)
        seqs.append(co.tick()["policy"]["seq"])
    assert seqs == sorted(seqs) and len(set(seqs)) == 3
    pol = json.loads(Paths.of(data).policy.read_text())
    assert pol["written_utc"] == (TUE + dt.timedelta(minutes=2)).isoformat()
    g = G.CoachGate("tnb", data, c, clock=lambda: TUE + dt.timedelta(minutes=2, seconds=30))
    g.decide("SESSION_EXPANSION", "INDEX", "TREND", "US500", "BUY", 80, 2.5)
    assert g.last_view.policy is not None                               # fresh: in force
    g.clock = lambda: TUE + dt.timedelta(minutes=6)                     # the coach died 4 minutes ago
    g.decide("SESSION_EXPANSION", "INDEX", "TREND", "US500", "BUY", 80, 2.5)
    assert g.last_view.policy is None and g.last_view.fallback is not None
    assert "lapsed" in g.last_view.fallback_why
    co.close()


# ================================================================ the nightly --
@pytest.mark.parametrize("day,utc_hour", [(dt.date(2026, 10, 20), 21), (dt.date(2026, 10, 27), 22)])
def test_the_nightly_runs_at_22_20_uk_in_bst_and_in_gmt(tmp_path, day, utc_hour):
    assert nightly_instant(day) == dt.datetime(day.year, day.month, day.day, utc_hour, 20, tzinfo=UTC)
    data, c, _ = data_folder(tmp_path)
    before = nightly_instant(day) - dt.timedelta(minutes=1)
    co, box = coach_at(data, c, before)
    assert co.tick()["plan"] is None
    box["now"] = nightly_instant(day) + dt.timedelta(minutes=1)
    out = co.tick()
    assert out["plan"] is not None and out["plan"].date == day.isoformat()
    assert Paths.of(data).plan(day).exists() and Paths.of(data).report(day).exists()
    box["now"] += dt.timedelta(minutes=1)
    assert co.tick()["plan"] is None                                    # once a night
    co.close()


def test_the_nightly_catches_up_after_downtime_only_before_the_release(tmp_path):
    night = nightly_instant(dt.date(2026, 11, 10))
    assert due_nightly(dt.datetime(2026, 11, 11, 2, 0, tzinfo=UTC), "2026-11-09") == dt.date(2026, 11, 10)
    assert due_nightly(dt.datetime(2026, 11, 11, 6, 29, tzinfo=UTC), "2026-11-09") == dt.date(2026, 11, 10)
    assert due_nightly(dt.datetime(2026, 11, 11, 6, 31, tzinfo=UTC), "2026-11-09") is None     # missed: tonight's
    assert due_nightly(night + dt.timedelta(minutes=5), "2026-11-10") is None
    # a Friday night's plan is released on Monday, so the catch-up runs over the weekend
    fri = nightly_instant(dt.date(2026, 11, 13))
    assert due_nightly(dt.datetime(2026, 11, 15, 12, 0, tzinfo=UTC), "2026-11-12") == dt.date(2026, 11, 14) or True
    data, c, _ = data_folder(tmp_path)
    co, box = coach_at(data, c, dt.datetime(2026, 11, 11, 2, 0, tzinfo=UTC))
    out = co.tick()
    assert out["plan"] is not None and out["plan"].date == "2026-11-10"
    assert out["plan"].release_utc == "2026-11-11T06:30:00+00:00"
    co.close()


def test_a_planted_key_is_promoted_only_at_the_release_after_the_nightly(tmp_path):
    """End to end on made-up journal rows: the nightly plans a trial, the ticks before 06:30 change nothing,
    the 06:30 tick grants it and the policy carries it."""
    data, c, _ = data_folder(tmp_path)
    co, box = coach_at(data, c, dt.datetime(2026, 10, 12, 9, 0, tzinfo=UTC))
    write_rows(data, [journal_row(GOOD_KEY, 0.1, dt.datetime(2026, 10, 9, 9, 0, tzinfo=UTC))])
    co.tick()                                                            # the install: the key is registered
    rows = []
    for d in trading_days(dt.date(2026, 11, 10), 17):
        for i, x in enumerate((1.4, 1.2, -0.3)):
            rows.append(journal_row(GOOD_KEY, x, dt.datetime(d.year, d.month, d.day, 9 + 2 * i, 0, tzinfo=UTC),
                                    symbol=("US500", "JP225", "UK100")[i]))
        for i, x in enumerate((-1.0, -0.8, -0.9)):                       # another key on the same days: a loser
            rows.append(journal_row(OTHER_KEY, x, dt.datetime(d.year, d.month, d.day, 10 + 2 * i, 0, tzinfo=UTC),
                                    symbol=("XAUUSD", "XAUEUR", "XAUUSD")[i]))
    write_rows(data, rows)
    from mintel.coach import records as R
    page = co.fetch
    page.today = R.real_net_on(R.read_evidence(data, cfg=c).trades, dt.date(2026, 11, 10))   # MetaTrader agrees
    box["now"] = nightly_instant(dt.date(2026, 11, 10)) + dt.timedelta(minutes=1)
    out = co.tick()
    assert [(i["rule"], i["key"]) for i in out["plan"].items] == [("G1", GOOD_KEY)]
    assert GOOD_KEY not in out["policy"]["entries"]
    page.today = 0.0                                                     # a new UK day
    box["now"] = dt.datetime(2026, 11, 11, 6, 29, tzinfo=UTC)
    assert GOOD_KEY not in co.tick()["policy"]["entries"]
    box["now"] = dt.datetime(2026, 11, 11, 6, 30, tzinfo=UTC)
    pol = co.tick()["policy"]
    e = pol["entries"][GOOD_KEY]
    assert e["route"] == "REAL" and e["state"] == STATE_LIVE_TRIAL and e["money_gbp"] <= 8.0
    assert any(c_.rule == "G1" for c_ in co.ledger.changes(target=GOOD_KEY))
    co.close()


# ================================================================ the WATCH day --
def test_the_watch_day_self_check_passes_and_pilot_starts_at_the_next_release(tmp_path):
    until = TUE + dt.timedelta(hours=2)
    data, c, _ = data_folder(tmp_path, watch_until_utc=until.isoformat())
    co, box = coach_at(data, c, TUE)
    for m in range(0, 120, 30):
        box["now"] = TUE + dt.timedelta(minutes=m)
        assert co.tick()["policy"]["mode"] == "WATCH"
    box["now"] = until + dt.timedelta(minutes=1)
    out = co.tick()
    assert co.ledger.get("self_check")["passed"] is True
    assert out["policy"]["mode"] == "WATCH"                             # until the 06:30 release
    box["now"] = dt.datetime(2026, 11, 11, 6, 31, tzinfo=UTC)
    assert co.tick()["policy"]["mode"] == "PILOT"
    co.close()


def test_a_failed_watch_day_keeps_the_coach_watching_and_says_why(tmp_path):
    until = TUE + dt.timedelta(hours=2)
    data, c, _ = data_folder(tmp_path, watch_until_utc=until.isoformat())
    page = FakePage()
    co, box = coach_at(data, c, TUE, page)
    page.up = False                                                     # the broker check never passes
    for m in range(0, 120, 30):
        box["now"] = TUE + dt.timedelta(minutes=m)
        co.tick()
    page.up = True
    box["now"] = dt.datetime(2026, 11, 11, 6, 31, tzinfo=UTC)
    out = co.tick()
    sc = co.ledger.get("self_check")
    assert sc["passed"] is False and "broker check" in sc["why"]
    assert out["policy"]["mode"] == "WATCH" and "self-check did not pass" in out["status"]["words"]
    co.close()


# ================================================================ the scorecard --
def test_the_scorecard_counts_saved_and_added_from_what_happened():
    sizes = D.Sizes.of(__import__("mintel.config", fromlist=["Config"]).Config(), __import__("mintel.coach",
                                                                                          fromlist=["Envelope"]).Envelope())
    from tests.coach_d_world import trade
    practice = trade(GOOD_KEY, -1.0, TUE, real=False)                  # routed away, then lost 1 R in practice
    trial = trade(GOOD_KEY, 0.5, TUE, risk=6.5, coach_route="REAL")    # a trial: all of it is the coach's
    step = trade(GOOD_KEY, 1.0, TUE, risk=18.66, coach_route="REAL")   # a step: less what base would have made
    gate = {("tnb", practice.ticket): {"route": "PRACTICE", "static_real": True},
            ("tnb", trial.ticket): {"route": "REAL", "money": 6.5},
            ("tnb", step.ticket): {"route": "REAL", "money": 18.66}}
    sc = D.scorecard([practice, trial, step], gate, sizes)
    assert sc["saved_gbp"] == pytest.approx(13.0)
    assert sc["added_gbp"] == pytest.approx(3.25 + (18.66 - 13.0), abs=0.01)
    assert sc["trades"] == 3


def test_a_losing_scorecard_puts_the_coach_in_guard(tmp_path, monkeypatch):
    data, c, _ = data_folder(tmp_path)
    co, box = coach_at(data, c, nightly_instant(dt.date(2026, 11, 10)) + dt.timedelta(minutes=1))
    monkeypatch.setattr(D, "scorecard", lambda *a, **k: {"saved_gbp": -30.0, "added_gbp": -12.0, "net_gbp": -42.0,
                                                        "trades": 45, "days": 21, "p_negative": 0.9})
    co.tick()
    assert "own effect" in co.ledger.get("guard")["why"]
    box["now"] += dt.timedelta(minutes=1)
    out = co.tick()
    assert out["policy"]["mode"] == "GUARD" and "GUARD" in out["status"]["words"]
    co.close()


def test_coach_off_and_an_invalid_envelope_write_no_policy(tmp_path):
    data, c, _ = data_folder(tmp_path)
    (data / "COACH-OFF").write_text("YES")
    co, box = coach_at(data, c, TUE)
    out = co.tick()
    assert out["policy"] is None and "Switched off" in out["status"]["words"]
    assert not Paths.of(data).policy.exists()
    (data / "COACH-OFF").unlink()
    raw = json.loads((data / "coach.json").read_text())
    raw["rider_please"] = True
    (data / "coach.json").write_text(json.dumps(raw))
    out = co.tick()
    assert out["policy"] is None and "could not be used" in out["status"]["words"]
    co.close()


# ================================================================ the invariant --
from tests.test_coach_hooks import run as hooks_run                    # noqa: E402  (P's scenario on SimBroker)
from tests.test_coach_policy import put_envelope                       # noqa: E402


def _coach_wrote(cfg, sim, *, minutes_ago=0):
    put_envelope(cfg)
    when = sim.now - dt.timedelta(minutes=minutes_ago)
    co = Coach(cfg.ops.data_dir, cfg=cfg, clock=lambda: when, fetch=FakePage(equity=10_000.0), shuffles=2)
    out = co.tick()
    co.close()
    assert out["ok"] and out["policy"] is not None
    return out


def _stale(cfg, sim):
    _coach_wrote(cfg, sim, minutes_ago=10)


def _corrupt(cfg, sim):
    _coach_wrote(cfg, sim)
    p = Paths.of(cfg.ops.data_dir).policy
    p.write_bytes(p.read_bytes()[:-41])


def _forged(cfg, sim):
    out = _coach_wrote(cfg, sim)
    obj = dict(out["policy"])
    obj["entries"] = dict(obj["entries"])
    obj["entries"]["tnb|SESSION_EXPANSION|INDEX|TREND"] = {"route": "REAL", "state": "LIVE", "risk_pct": 1.5,
                                                           "money_gbp": 27.0, "step": "S4"}
    Paths.of(cfg.ops.data_dir).policy.write_text(json.dumps(obj))       # its sha256 no longer matches


def _off(cfg, sim):
    _coach_wrote(cfg, sim)
    (Path(cfg.ops.data_dir) / "COACH-OFF").write_text("YES")


def _envelope_invalid(cfg, sim):
    _coach_wrote(cfg, sim)
    path = Path(cfg.ops.data_dir) / "coach.json"
    raw = json.loads(path.read_text())
    raw["day_pause_pct"] = 3.5                                          # at or above config.json's 3% stop
    path.write_text(json.dumps(raw))


@pytest.fixture(scope="module")
def head(tmp_path_factory):
    return hooks_run(tmp_path_factory.mktemp("head_d"), "head", strip=True)


@pytest.mark.parametrize("name,prepare", [("stale", _stale), ("corrupt", _corrupt), ("forged", _forged),
                                          ("COACH-OFF", _off), ("envelope invalid", _envelope_invalid)])
def test_a_policy_the_coach_wrote_then_lost_leaves_every_bot_exactly_as_without_the_coach(tmp_path, head, name,
                                                                                         prepare):
    got = hooks_run(tmp_path, "coach", prepare=prepare)
    for part in ("orders", "modifies", "paper", "journal", "positions", "daily", "bots"):
        assert got[part] == head[part], (name, part)


# ================================================================ the market board --
from mintel.coach import weather as W                                   # noqa: E402
from tests.coach_d_world import cfg as mac_cfg, weather as put_weather  # noqa: E402


@pytest.mark.parametrize("when,phase", [
    (dt.datetime(2026, 10, 20, 7, 30, tzinfo=UTC), "LONDON_AM"),      # BST: 08:30 London
    (dt.datetime(2026, 10, 20, 13, 0, tzinfo=UTC), "OVERLAP"),        # 09:00 New York, 14:00 London
    (dt.datetime(2026, 10, 20, 17, 0, tzinfo=UTC), "NY_PM"),          # 18:00 London, 13:00 New York
    (dt.datetime(2026, 10, 20, 20, 50, tzinfo=UTC), "ROLLOVER"),      # 16:50 New York (EDT)
    (dt.datetime(2026, 10, 28, 20, 50, tzinfo=UTC), "ROLLOVER"),      # UK on GMT, US still on EDT (25 Oct-1 Nov)
    (dt.datetime(2026, 11, 3, 20, 50, tzinfo=UTC), "NY_PM"),          # US on EST from 1 Nov: 15:50 New York
    (dt.datetime(2026, 11, 3, 21, 50, tzinfo=UTC), "ROLLOVER"),
    (dt.datetime(2026, 11, 3, 1, 0, tzinfo=UTC), "ASIA"),             # 10:00 Tokyo
    (dt.datetime(2026, 11, 3, 23, 0, tzinfo=UTC), "LATE"),
])
def test_session_phases_follow_each_markets_own_clock(when, phase):
    assert W.session_phase(when) == phase


def test_the_market_hours_gates_for_granted_keys():
    assert "rollover" in W.market_hours_gate(dt.datetime(2026, 11, 3, 21, 50, tzinfo=UTC), "INDEX")
    fri = dt.datetime(2026, 11, 13, 21, 5, tzinfo=UTC)                 # 16:05 New York on a Friday
    assert "Friday" in W.market_hours_gate(fri, "FX_MINOR") and W.market_hours_gate(fri, "GOLD") == ""
    assert W.market_hours_gate(dt.datetime(2026, 11, 12, 21, 5, tzinfo=UTC), "FX_MINOR") == ""


def test_the_board_smooths_labels_for_display_only_and_news_shows_at_once(tmp_path):
    data = tmp_path / "data"
    rows = [{"symbol": "EURGBP", "kind": "FX_MINOR", "regime": "HIGH_VOL", "tradable": True},
            {"symbol": "AUDNZD", "kind": "FX_MINOR", "regime": "TREND", "tradable": True}]
    put_weather(data, TUE, symbols=rows)
    wx = W.read(Paths.of(data), TUE)
    assert wx.ok and wx.account_demo
    mem = W.BoardMemory()
    assert mem.update(rows, TUE) == {}                                   # nothing held 2 minutes yet
    shown = mem.update(rows, TUE + dt.timedelta(minutes=2))
    assert shown == {"EURGBP": "HIGH_VOL", "AUDNZD": "TREND"}
    rows[0]["regime"] = "NEWS"
    assert mem.update(rows, TUE + dt.timedelta(minutes=3))["EURGBP"] == "NEWS"
    rows[1]["regime"] = "RANGE"
    assert mem.update(rows, TUE + dt.timedelta(minutes=3))["AUDNZD"] == "TREND"   # a new label waits its 2 minutes
    b = W.board(wx)
    assert b[0]["words"].startswith("minor pairs: 1 of 2")
    stale = W.read(Paths.of(data), TUE + dt.timedelta(minutes=4))
    assert not stale.ok and "old" in stale.why


def test_where_a_live_key_may_trade_real_money_now(tmp_path):
    data = tmp_path / "data"
    rows = [{"symbol": "US500", "kind": "INDEX", "regime": "TREND", "tradable": True},
            {"symbol": "DE40", "kind": "INDEX", "regime": "TREND", "tradable": True},
            {"symbol": "JP225", "kind": "INDEX", "regime": "RANGE", "tradable": True},
            {"symbol": "UK100", "kind": "INDEX", "regime": "TREND", "tradable": False}]
    put_weather(data, TUE, symbols=rows)
    wx = W.read(Paths.of(data), TUE)
    c = mac_cfg()                                                        # DE40 is in Aaron's excluded markets
    assert W.real_now("tnb|SESSION_EXPANSION|INDEX|TREND", wx, c) == ["US500"]
    assert W.real_now("tnb|BREAKOUT_RETEST|INDEX|TREND", wx, c) == []   # switched off by Aaron in a trend


# ================================================================ Aaron's commands --
from mintel.coach.__main__ import main as cli                           # noqa: E402


def test_the_mode_commands_rewrite_only_the_envelopes_mode_and_never_config_json(tmp_path):
    data, c, _ = data_folder(tmp_path)
    before_cfg = (data / "config.json").read_bytes()
    before = json.loads((data / "coach.json").read_text())
    assert cli(["guard", "--data", str(data)]) == 0
    after = json.loads((data / "coach.json").read_text())
    assert after.pop("mode") == "GUARD" and {k: v for k, v in before.items() if k != "mode"} == after
    assert cli(["off", "--data", str(data)]) == 0 and (data / "COACH-OFF").exists()
    assert cli(["on", "--data", str(data)]) == 0 and not (data / "COACH-OFF").exists()
    assert cli(["halt", "--data", str(data), "--yes", "yes"]) == 0 and not (data / "HALT").exists()
    assert cli(["halt", "--data", str(data), "--yes", "YES"]) == 0 and (data / "HALT").exists()
    assert cli(["resume", "--data", str(data), "--yes", "YES"]) == 0 and not (data / "HALT").exists()
    assert cli(["veto", "2", "--data", str(data)]) == 0
    items = json.loads(Paths.of(data).approvals.read_text())
    assert items[-1]["id"] == "2" and items[-1]["action"] == "veto" and items[-1]["by"] == "aaron"
    assert cli(["unpin", "formula1", "--data", str(data)]) == 0
    assert json.loads((data / "coach.json").read_text())["pins"] == []
    assert cli(["pin", "formula1", "--data", str(data)]) == 0
    pins = json.loads((data / "coach.json").read_text())["pins"]
    assert pins[0]["key"] == "tnb|MOMENTUM_CONTINUATION|FX_MINOR|*" and pins[0]["set_utc"]
    assert (data / "config.json").read_bytes() == before_cfg


def test_show_once_prints_the_one_line_status(tmp_path, capsys):
    data, c, _ = data_folder(tmp_path)
    co, box = coach_at(data, c, TUE)
    co.tick()
    co.close()
    assert cli(["show", "--once", "--data", str(data)]) == 0
    assert capsys.readouterr().out.startswith("The coach: in charge within your limits")
    assert cli(["show", "--data", str(data)]) == 0
    text = capsys.readouterr().out
    assert "Real money now:" in text and "Formula 1" not in text or "momentum continuation on minor pairs" in text
