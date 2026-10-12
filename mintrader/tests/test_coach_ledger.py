"""The coach's ledger (spec sections 10.2 and 14): a change without a sentence
is refused; the ledger rows are committed before the policy file is replaced
(crash injection); ``changes`` is append-only and a ``policies`` row may only
be marked valid; section 21's shadow figure.

EVERY FIGURE IN THIS FILE IS MADE UP for the test: never a result.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3

import pytest

from mintel.coach import STATE_LIVE, STATE_LIVE_TRIAL, Paths
from mintel.coach import decide as D
from mintel.coach import policy as P
from mintel.coach.ledger import Change, Ledger, LedgerError, Segment
from mintel.coach.run import Coach
from tests.coach_d_world import EQ, GOOD_KEY, NOW, FakePage, cfg, data_folder, env, trade

SENTENCE = "Made-up change for the ledger's own test."


def test_a_change_without_a_sentence_is_refused(tmp_path):
    led = Ledger(tmp_path / "ledger.sqlite")
    for words in ("", "too short", None):
        with pytest.raises(LedgerError):
            led.add_changes([Change("state", GOOD_KEY, words)])
    with pytest.raises(LedgerError):
        led.commit([Change("state", GOOD_KEY, SENTENCE), Change("state", GOOD_KEY, "")])
    assert led.changes() == []                                          # all or nothing
    # the table refuses it too, whatever the code says
    with pytest.raises(sqlite3.IntegrityError):
        led.con.execute("INSERT INTO changes(ts_utc, kind, target, words, by) VALUES ('x','k','t','short','coach')")
    with pytest.raises(sqlite3.IntegrityError):
        led.con.execute("INSERT INTO changes(ts_utc, kind, target, words, by) VALUES ('x','k','t',?,'robot')",
                        (SENTENCE,))
    led.close()


def test_changes_are_append_only_and_a_policy_row_may_only_be_marked_valid(tmp_path):
    led = Ledger(tmp_path / "ledger.sqlite")
    ids = led.add_changes([Change("state", GOOD_KEY, SENTENCE)])
    for q in ("UPDATE changes SET words = 'something else entirely' WHERE id = ?", "DELETE FROM changes WHERE id = ?"):
        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            led.con.execute(q, (ids[0],))
    led.add_policy(1, NOW.isoformat(), "a" * 64, b"{}")
    with pytest.raises(sqlite3.DatabaseError):
        led.con.execute("UPDATE policies SET sha256 = 'b' WHERE seq = 1")
    led.mark_valid(1)
    with pytest.raises(sqlite3.DatabaseError):
        led.con.execute("UPDATE policies SET valid = 0 WHERE seq = 1")
    assert led.con.execute("SELECT valid FROM policies WHERE seq = 1").fetchone()[0] == 1
    led.close()


def test_the_ledger_is_committed_before_the_policy_file_is_replaced(tmp_path, monkeypatch):
    """Crash injection: the policy write dies after the ledger commit - the changes and segments are in the
    ledger, the policy file on disk is the old one, and its ledger row is not marked valid."""
    data, c, _ = data_folder(tmp_path)
    page = FakePage()
    coach = Coach(data, cfg=c, clock=lambda: NOW, fetch=page, shuffles=2)
    assert coach.tick()["ok"]
    paths = Paths.of(data)
    before = paths.policy.read_bytes()
    n_before = len(coach.ledger.changes(limit=10_000))
    page.up = False                                                     # grants stop: a difference to record

    def crash(*a, **k):
        raise OSError("made-up crash between the ledger and the file")
    monkeypatch.setattr(P, "write", crash)
    out = coach.tick(NOW + dt.timedelta(minutes=1))
    assert out["ok"] is False
    assert paths.policy.read_bytes() == before
    rows = coach.ledger.changes(limit=10_000)
    assert len(rows) > n_before and any(r.kind == "grants" for r in rows)
    last = coach.ledger.con.execute("SELECT seq, valid FROM policies ORDER BY seq DESC LIMIT 1").fetchone()
    assert last[1] == 0
    monkeypatch.undo()
    assert coach.tick(NOW + dt.timedelta(minutes=2))["ok"]
    assert json.loads(paths.policy.read_text())["seq"] > json.loads(before)["seq"]
    coach.close()


def test_every_policy_written_has_its_reasons_in_the_ledger(tmp_path):
    data, c, _ = data_folder(tmp_path)
    page = FakePage()
    coach = Coach(data, cfg=c, clock=lambda: NOW, fetch=page, shuffles=2)
    first = coach.tick()["policy"]
    for k, e in first["entries"].items():
        assert e.get("why_id"), k                                        # each instruction names its sentence
    rows = {r.id: r for r in coach.ledger.changes(limit=1000)}
    for k, e in first["entries"].items():
        assert rows[e["why_id"]].target == k and len(rows[e["why_id"]].words) >= 20
    coach.close()


def test_the_shadow_figure_is_the_v1_sizing_of_the_same_trade():
    """Section 21: a coach-sized demo trade at S7 (3%) is shadowed at the v1 size (S4 at most, 1.5%) and its
    result rescaled to that risk; a demo trial at base is shadowed at the v1 trial (0.35%, never above £8)."""
    sizes = D.Sizes.of(cfg(), env())
    went = [Change("state", GOOD_KEY, SENTENCE, "LIVE:S6", "LIVE:S7", "G3", ts_utc=(NOW - dt.timedelta(days=2)).isoformat(),
                   id=1),
            Change("state", "tnb|A|INDEX|TREND", SENTENCE, "PRACTICE", "LIVE_TRIAL:trial", "G1",
                   ts_utc=(NOW - dt.timedelta(days=2)).isoformat(), id=2)]
    money = round(EQ * 3.0 / 100, 2)
    t = trade(GOOD_KEY, -1.0, NOW, risk=money, coach_route="REAL")
    t2 = trade("tnb|A|INDEX|TREND", 2.0, NOW, risk=13.0, coach_route="REAL")
    gate = {("tnb", t.ticket): {"money": money, "key": GOOD_KEY}, ("tnb", t2.ticket): {"money": 13.0,
                                                                                     "key": "tnb|A|INDEX|TREND"}}
    rows = {r["ticket"]: r for r in D.shadow_rows([t, t2], went, sizes, gate)}
    r = rows[t.ticket]
    eq = money / 0.03
    assert r["real_money_gbp"] == pytest.approx(sizes.v1_money("S7", eq), abs=0.01)
    assert r["real_money_gbp"] == pytest.approx(eq * 1.5 / 100, abs=0.02)
    assert r["real_money_net_gbp"] == pytest.approx(-r["real_money_gbp"], abs=0.02)
    r2 = rows[t2.ticket]
    assert r2["real_money_gbp"] <= 8.0 and r2["real_money_gbp"] == pytest.approx(sizes.v1_money("trial", 13.0 / 0.007,
                                                                                               STATE_LIVE_TRIAL), abs=0.01)
    assert r2["real_money_net_gbp"] == pytest.approx(2.0 * r2["real_money_gbp"], abs=0.02)
