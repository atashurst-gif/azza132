"""The coach's brakes (spec section 7, section 21's demo scaling): B1-B9, Ian's
money counted, the saved peak surviving a restart, headroom including Ian,
the trial budget across a week rollover, and no brake released on unreadable
data. Every brake only restricts.

EVERY FIGURE IN THIS FILE IS MADE UP for the test (synthetic pages, synthetic
records): never a result.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from mintel.coach import F1_KEY, STATE_LIVE, STATE_LIVE_TRIAL, STATE_PINNED, STATE_PLACED, UTC, Paths
from mintel.coach import brakes as B
from mintel.coach import decide as D
from mintel.coach import feed as F
from mintel.coach.ledger import Change
from mintel.coach.run import Coach
from tests.coach_d_world import (EQ, GOOD_KEY, NOW, FakePage, cfg, data_folder, env, seg, trade, world)

E = env()
LN = B.lines(E, 3.0)


def acct(today=0.0, equity=EQ, e0=EQ, week=0.0, open_risk=0.0):
    return B.Account(equity, e0, today, open_risk, week)


def ev(prev=None, a=None, **kw):
    kw.setdefault("now", NOW)
    kw.setdefault("data_ok", True)
    kw.setdefault("trial_losses_gbp", 0.0)
    return B.evaluate(prev or B.BrakeState(), a or acct(), LN, **kw)


# ================================================================ B1, B2 --
@pytest.mark.parametrize("today,want", [(-27.97, "OPEN"), (-27.99, "LINE"), (-41.97, "LINE"), (-41.99, "PAUSE")])
def test_b1_and_b2_lines_at_the_spec_figures(today, want):
    st, moves = ev(a=acct(today=today))
    assert st.day == want
    if want != "OPEN":
        assert moves and moves[0][0] == "day" and "£" in moves[0][3]


def test_b1_holds_every_governed_key_at_base_pauses_trials_and_stops_grants():
    st, _ = ev(a=acct(today=-30.0))
    assert st.at_base and st.no_grants and st.trials_blocked and not st.pause_governed
    k2 = "tnb|BREAKOUT_ACCEPTANCE|INDEX|TREND"
    segs = {GOOD_KEY: seg(GOOD_KEY, STATE_LIVE, "S3"), k2: seg(k2, STATE_LIVE_TRIAL, "trial")}
    ents = D.entries(segs, world([], segs, brakes=st))
    assert ents[GOOD_KEY].route == "REAL" and ents[GOOD_KEY].step == "base" and ents[GOOD_KEY].money_gbp == 13.0
    assert ents[k2].route == "PRACTICE"


def test_b2_pauses_governed_keys_the_runner_and_ian_until_midnight_but_never_the_pin():
    st, _ = ev(a=acct(today=-45.0))
    assert st.pause_governed
    c = cfg()
    c.runner.mode = "LIVE"
    segs = {GOOD_KEY: seg(GOOD_KEY, STATE_LIVE, "base"), "runner|*|*|*": seg("runner|*|*|*", STATE_PLACED),
            "ian|*|*|*": seg("ian|*|*|*", STATE_PLACED), F1_KEY: seg(F1_KEY, STATE_PINNED)}
    ents = D.entries(segs, world([], segs, brakes=st, c=c, ian_mode="LIVE"))
    assert ents[GOOD_KEY].route == "PRACTICE" and ents[GOOD_KEY].until_utc == "2026-11-11T00:00:00+00:00"
    assert ents["runner|*|*|*"].route == "SKIP" and ents["ian|*|*|*"].route == "SKIP"
    assert ents[F1_KEY].route == "PIN"


def test_the_day_brake_ends_with_the_uk_day_but_never_on_unreadable_data():
    st, _ = ev(a=acct(today=-30.0))
    tomorrow = NOW + dt.timedelta(days=1)
    kept, moves = ev(st, B.Account(None, None, None, None, None), now=tomorrow, data_ok=False)
    assert kept.day == "LINE" and moves == []                          # unknown data: nothing released
    kept, _ = ev(st, acct(), now=tomorrow, data_ok=False)
    assert kept.day == "LINE"                                          # a failed data check: still nothing released
    fresh, moves = ev(st, acct(), now=tomorrow)
    assert fresh.day == "OPEN" and moves[0][2] == "OPEN"


# ================================================================ B3 --
def test_b3_week_line_and_its_release_above_minus_two_and_a_half_percent():
    st, _ = ev(a=acct(week=-93.30))
    assert st.week == "LINE" and st.no_steps and st.no_grants
    held, _ = ev(st, acct(week=-60.0))
    assert held.week == "LINE"                                         # -3.2%: still below -2.5%
    off, _ = ev(st, acct(week=-40.0))
    assert off.week == "OPEN"


# ================================================================ B4 --
def test_b4_from_the_saved_peak_released_only_at_the_nightly_under_half_the_trigger():
    st, _ = ev(B.BrakeState(peak=2000.0), acct(equity=1895.0))       # 5.25% down
    assert st.drawdown == "LINE" and st.at_base and st.no_grants
    st2, _ = ev(st, acct(equity=1835.0))                               # 8.25% down
    assert st2.drawdown == "PAUSE" and st2.pause_governed
    later, _ = ev(st2, acct(equity=1990.0))                            # 0.5%: not at a tick
    assert later.drawdown == "PAUSE"
    night, _ = ev(st2, acct(equity=1990.0), nightly=True)
    assert night.drawdown == "OPEN"
    half, _ = ev(st2, acct(equity=1930.0), nightly=True)               # 3.5%: under half of 8, over half of 5
    assert half.drawdown == "LINE"


def test_the_saved_peak_survives_a_restart(tmp_path):
    data, c, _ = data_folder(tmp_path)
    page = FakePage(equity=2000.0)
    coach = Coach(data, cfg=c, clock=lambda: NOW, fetch=page, shuffles=2)
    assert coach.tick()["ok"]
    coach.close()
    page.equity = 1890.0                                               # 5.5% under the saved 2,000
    again = Coach(data, cfg=c, clock=lambda: NOW + dt.timedelta(minutes=1), fetch=page, shuffles=2)
    out = again.tick()
    assert out["policy"]["brakes"]["drawdown"] == "LINE" and out["policy"]["account"]["peak"] == 2000.0
    again.close()


# ================================================================ B5 --
def _ian(data: Path, rows):
    con = sqlite3.connect(data / "ian.sqlite")
    con.execute("CREATE TABLE trades (ticket INTEGER, spot_symbol TEXT, side TEXT, opened_utc TEXT, closed_utc TEXT, "
                "net_pnl REAL, risk_money REAL, commission REAL, mode TEXT, data_source TEXT)")
    for r in rows:
        con.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?)", r)
    con.commit()
    con.close()


def test_headroom_counts_every_bots_open_real_risk_ian_included(tmp_path):
    data, c, _ = data_folder(tmp_path)
    _ian(data, [(1, "BTCUSDT", "BUY", NOW.isoformat(), None, None, 10.0, None, "LIVE", "LIVE"),
                (2, "BTCUSDT", "BUY", NOW.isoformat(), None, None, 25.0, None, "PAPER", "LIVE")])
    risk, why = F.open_real_risk(data, c)
    assert risk == 10.0 and why == ""                                  # Ian's real trade counts, its practice does not
    h = B.headroom(acct(today=-10.0, open_risk=risk), 3.0)
    assert h == pytest.approx(0.03 * EQ - 10.0 - 10.0, abs=0.01)
    assert B.headroom(acct(today=+50.0, open_risk=0.0), 3.0) == pytest.approx(0.03 * EQ, abs=0.01)   # profit never adds
    assert B.headroom(B.Account(EQ, EQ, 0.0, None, 0.0), 3.0) is None


def test_ians_money_is_counted_in_the_day(tmp_path):
    """The brakes count the page's account figure (every magic, Financial Ian's too) - here Ian's loss alone
    takes the day past the line."""
    data, c, _ = data_folder(tmp_path)
    today = NOW - dt.timedelta(hours=1)
    _ian(data, [(7, "BTCUSDT", "SELL", (today - dt.timedelta(minutes=30)).isoformat(), today.isoformat(), -30.0, 10.0,
                 0.0, "LIVE", "LIVE")])
    page = FakePage(today=-30.0)
    coach = Coach(data, cfg=c, clock=lambda: NOW, fetch=page, shuffles=2)
    out = coach.tick()
    assert out["policy"]["brakes"]["day"] == "LINE"
    assert out["policy"]["data"]["broker_check"]["records_today"] == -30.0   # the records see Ian's trade too
    coach.close()


# ================================================================ B7 --
def test_the_trial_budget_is_this_iso_weeks_and_rolls_over_on_monday(tmp_path):
    data, c, _ = data_folder(tmp_path)
    coach = Coach(data, cfg=c, clock=lambda: NOW, fetch=FakePage(), shuffles=2)
    friday = dt.datetime(2026, 11, 13, 14, 0, tzinfo=UTC)
    went = Change("state", GOOD_KEY, "made-up: the key went on a live trial", "PRACTICE", "LIVE_TRIAL:trial", "G1",
                  ts_utc=(friday - dt.timedelta(days=3)).isoformat())
    t = [trade(GOOD_KEY, -6.5, friday, risk=6.5, coach_route="REAL")]   # made up: a trial losing £42.25
    assert coach._trial_losses(t, [went], friday) == pytest.approx(42.25)
    st, _ = B.evaluate(B.BrakeState(), acct(), LN, now=friday, data_ok=True, trial_losses_gbp=42.25)
    assert st.trials_blocked and "Monday" in st.why["trial_budget"]
    monday = dt.datetime(2026, 11, 16, 6, 31, tzinfo=UTC)
    assert coach._trial_losses(t, [went], monday) == 0.0
    st2, _ = B.evaluate(st, acct(), LN, now=monday, data_ok=True, trial_losses_gbp=0.0)
    assert not st2.trials_blocked
    coach.close()


# ================================================================ B8 --
def test_b8_failed_data_stops_grants_and_pauses_an_unreadable_governed_bot():
    st, _ = ev(data_ok=False)
    assert st.no_grants
    segs = {GOOD_KEY: seg(GOOD_KEY, STATE_LIVE, "base")}
    w = world([], segs, brakes=st, data_ok=False)
    w.unreadable_bots = ("tnb",)
    ents = D.entries(segs, w)
    assert ents[GOOD_KEY].route == "PRACTICE" and ents[GOOD_KEY].rule == "B8"


def test_no_grant_when_the_page_cannot_be_read(tmp_path):
    data, c, _ = data_folder(tmp_path)
    page = FakePage()
    page.up = False
    coach = Coach(data, cfg=c, clock=lambda: NOW, fetch=page, shuffles=2)
    out = coach.tick()
    assert out["policy"]["grants_allowed"] is False and out["policy"]["data"]["ok"] is False
    coach.close()


# ================================================================ B9 --
def test_b9_three_tick_errors_in_a_row_put_the_coach_in_guard(tmp_path, monkeypatch):
    data, c, _ = data_folder(tmp_path)
    coach = Coach(data, cfg=c, clock=lambda: NOW, fetch=FakePage(), shuffles=2)
    monkeypatch.setattr(F, "read", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("made-up failure")))
    for i in range(3):
        assert coach.tick(NOW + dt.timedelta(minutes=i))["ok"] is False
    assert coach.ledger.get("guard")["why"].startswith("3 ticks in a row failed")
    monkeypatch.undo()
    out = coach.tick(NOW + dt.timedelta(minutes=4))
    assert out["policy"]["mode"] == "GUARD"
    coach.close()


def test_b9_an_audit_finding_puts_the_coach_in_guard(tmp_path):
    data, c, _ = data_folder(tmp_path)
    coach = Coach(data, cfg=c, clock=lambda: NOW, fetch=FakePage(), shuffles=2)
    gl = Paths.of(data).gate_log("tnb", NOW.date())
    gl.parent.mkdir(parents=True, exist_ok=True)
    gl.write_text(json.dumps({"ts": NOW.isoformat(), "bot": "tnb", "key": GOOD_KEY, "route": "REAL", "money": 40.0,
                              "seq": 1, "source": "policy", "why": "made up"}) + "\n")
    out = coach.tick()
    assert out["policy"]["mode"] == "GUARD" and "audit" in coach.ledger.get("guard")["why"]
    coach.close()


def test_b9_too_many_trial_starts_in_a_week(tmp_path):
    data, c, _ = data_folder(tmp_path)
    coach = Coach(data, cfg=c, clock=lambda: NOW, fetch=FakePage(), shuffles=2)
    coach.ledger.add_changes([Change("state", f"tnb|A{i}|INDEX|TREND", "made-up trial start for the test",
                                     "PRACTICE", "LIVE_TRIAL:trial", "G1") for i in range(4)], now=NOW)
    out = coach.tick()
    assert out["policy"]["mode"] == "GUARD" and "trial starts" in coach.ledger.get("guard")["why"]
    coach.close()


# ================================================================ demo scaling --
def test_on_the_demo_the_brakes_follow_the_effective_stop():
    ln = B.lines(E, 3.0, 6.0, demo=True)
    assert (ln.day_line_pct, ln.day_pause_pct) == (3.0, 4.5)
    assert (ln.drawdown_line_pct, ln.drawdown_pause_pct, ln.trial_budget_gbp) == (10.0, 15.0, 150.0)
    ln4 = B.lines(E, 3.0, 4.0, demo=True)
    assert (ln4.day_line_pct, ln4.day_pause_pct, ln4.drawdown_line_pct) == (2.0, 3.0, 5.0)
    v1 = B.lines(E, 3.0, 6.0, demo=False)                              # the demo test failed: the v1 lines
    assert (v1.stop_pct, v1.day_line_pct, v1.day_pause_pct, v1.trial_budget_gbp) == (3.0, 1.5, 2.25, 40.0)


def test_no_brake_ever_raises_a_size():
    segs = {GOOD_KEY: seg(GOOD_KEY, STATE_LIVE, "S2")}
    free = D.entries(segs, world([], segs))[GOOD_KEY].money_gbp
    for st in (B.BrakeState(day="LINE"), B.BrakeState(day="PAUSE"), B.BrakeState(week="LINE"),
               B.BrakeState(drawdown="LINE"), B.BrakeState(drawdown="PAUSE"), B.BrakeState(data_ok=False)):
        e = D.entries(segs, world([], segs, brakes=st)).get(GOOD_KEY)
        assert e is None or e.route != "REAL" or e.money_gbp <= free
