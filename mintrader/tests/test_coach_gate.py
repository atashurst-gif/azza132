"""The coach's gate (spec section 10.3 and 10.4): the full table of switches,
modes and policy states, the cap property over thousands of random and
broken policies, GUARD never above BASELINE, COACH-OFF at the next call, and
the fallback that keeps restrictions and drops grants."""
from __future__ import annotations

import dataclasses
import datetime as dt
import itertools
import json
import math
import random
from pathlib import Path

import pytest

from mintel.coach import (F1_KEY, ROUTES, UTC, Block, Decision, Entry, Envelope, Floor, Gates, Paths, RiskCtx)
from mintel.coach import gate as G
from mintel.coach import policy as P
from tests.test_coach_policy import (INDEX_KEY, NOW, PRACTICE_KEY, coach_cfg, grant, make_policy, put_envelope,
                                     put_policy, put_raw, sealed_obj)

EQ = 1865.61
CTX = RiskCtx(equity=EQ)


def gate_for(cfg, now=NOW, **kw) -> G.CoachGate:
    return G.CoachGate("tnb", cfg.ops.data_dir, cfg, clock=lambda: now, **kw)


def decide(g, key: str, now=NOW, static_real=False, ctx=CTX, symbol="", score=80.0, rr=2.5, side="BUY"):
    bot, approach, kind, cond = key.split("|")
    return g.decide(approach, kind, cond, symbol or {"INDEX": "US500", "FX_MAJOR": "EURUSD",
                                                     "FX_MINOR": "EURGBP"}.get(kind, "XAUUSD"),
                    side, score, rr, now, static_real, ctx, bot=bot)


def touch(cfg, name):
    (Path(cfg.ops.data_dir) / name).write_text("YES")


# ================================================================ the table --
STATES = ("valid", "stale", "corrupt", "absent", "first_install", "rolled_back", "old_envelope")


def prepare(tmp_path, *, halt, off, mode, state, governed):
    """A gate in this situation, and whether a last good policy was seen."""
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg, mode=mode, govern=(["tnb", "runner", "ian"] if governed else ["runner", "ian"]))
    good = make_policy(sha, seq=5)
    g = gate_for(cfg)
    seen_good = False
    if state != "first_install":
        if governed:
            put_policy(cfg, good)
        else:                                                  # the writer would refuse it (V10): put it there
            put_raw(cfg, sealed_obj(good))
        decide(g, INDEX_KEY)                                   # the gate sees a good policy once
        seen_good = True
    now = NOW
    paths = Paths.of(cfg.ops.data_dir)
    if state == "stale":
        now = NOW + dt.timedelta(seconds=400)
    elif state == "corrupt":
        paths.policy.write_bytes(paths.policy.read_bytes()[:-20])
    elif state == "absent":
        paths.policy.unlink()
    elif state == "rolled_back":
        put_raw(cfg, sealed_obj(make_policy(sha, seq=2, entries={INDEX_KEY: grant()})))
    elif state == "old_envelope":
        put_raw(cfg, sealed_obj(make_policy("e" * 64, seq=9)))
    if halt:
        touch(cfg, "HALT")
    if off:
        touch(cfg, "COACH-OFF")
    g.clock = lambda: now
    return cfg, g, now, seen_good


def expected(key, *, halt, off, mode, state, governed, static_real, seen_good):
    pinned = key == F1_KEY
    if halt:
        return "PRACTICE" if static_real else "BASELINE"
    if off or mode == "OFF" or not governed:
        return "BASELINE"
    if pinned:
        return "PIN"
    if mode == "WATCH":
        return "BASELINE"
    usable = state == "valid"
    restrictions = state == "stale" or (state != "valid" and seen_good)
    if key == INDEX_KEY:                                       # a grant
        return "REAL" if usable and mode == "PILOT" else "BASELINE"
    if key == PRACTICE_KEY:                                    # a restriction
        return "PRACTICE" if usable or restrictions else "BASELINE"
    return "BASELINE"


@pytest.mark.parametrize("halt,off", [(False, False), (True, False), (False, True), (True, True)])
@pytest.mark.parametrize("mode", ["OFF", "WATCH", "GUARD", "PILOT"])
@pytest.mark.parametrize("state", STATES)
def test_the_full_table(tmp_path, halt, off, mode, state):
    for governed in (True, False):
        for i, (key, static_real) in enumerate(itertools.product(
                (INDEX_KEY, PRACTICE_KEY, F1_KEY, "tnb|TREND_PULLBACK|INDEX|RANGE"), (False, True))):
            where = tmp_path / f"{governed}-{i}"
            cfg, g, now, seen_good = prepare(where, halt=halt, off=off, mode=mode, state=state, governed=governed)
            d = decide(g, key, now, static_real=static_real)
            want = expected(key, halt=halt, off=off, mode=mode, state=state, governed=governed,
                            static_real=static_real, seen_good=seen_good)
            assert d.route == want, (key, static_real, governed, d)
            if d.route == "REAL":
                assert 0 < d.money_gbp <= min(6.53, 0.35 * EQ / 100) + 1e-9
            if halt:
                assert d.source == "halt"


# ================================================================ the cap property --
def random_number(rng):
    return rng.choice([1e9, -1.0, 0.0, 0.35, 1.5, 1.6, 6.53, 13.0, 27.98, 1000.0, float("nan"), float("inf"),
                       "13", None, True, rng.uniform(-50, 5000), rng.uniform(0, 3)])


def random_policy(rng, sha):
    keys = [INDEX_KEY, "tnb|TREND_PULLBACK|INDEX|TREND", "tnb|SESSION_EXPANSION|INDEX|*", "tnb|*|INDEX|*",
            "tnb|*|*|*", F1_KEY, "tnb|SESSION_EXPANSION|GOLD|TREND", "tnb|X" * rng.randint(1, 3) + "|INDEX|TREND",
            "K" * rng.randint(1, 5000)]
    entries = {}
    if rng.random() < 0.5:                                      # one grant, random numbers: often valid
        entries[rng.choice(keys[:3])] = {"route": "REAL", "state": rng.choice(["LIVE", "LIVE_TRIAL"]),
                                         "risk_pct": random_number(rng), "money_gbp": random_number(rng)}
    for _ in range(rng.randint(0, 6) if rng.random() < 0.5 else 0):
        e = {"route": rng.choice(list(ROUTES) + ["REAL", "REAL", "nope"]),
             "state": rng.choice(["LIVE", "LIVE_TRIAL", "PRACTICE", "PLACED", "?"])}
        if rng.random() < 0.8:
            e["risk_pct"] = random_number(rng)
        if rng.random() < 0.8:
            e["money_gbp"] = random_number(rng)
        if rng.random() < 0.05:
            e["extra"] = 1
        entries[rng.choice(keys)] = e
    nearly = rng.random() < 0.6                                 # most are well formed, so grants do happen
    obj = {"schema": 1 if nearly else rng.choice([1, 2, "1"]), "seq": rng.randint(0, 50),
           "written_utc": NOW.isoformat(), "valid_s": 180 if nearly else rng.choice([180, 60, 1e9, -1, 0]),
           "account_login": 0, "envelope_sha": sha, "build": "",
           "mode": "PILOT" if nearly else rng.choice(["PILOT", "GUARD", "WATCH", "X"]),
           "grants_allowed": True if nearly else rng.choice([True, False, "yes"]), "data": {},
           "account": {"equity": EQ if nearly else random_number(rng)}, "brakes": {},
           "headroom_gbp": random_number(rng), "entries": entries, "floors": {}, "blocks": []}
    return obj


def test_the_cap_property_over_20000_random_policies(tmp_path):
    """Whatever the file says - 1e9, -1, NaN, strings, huge keys, a forged hash - a REAL decision is never
    above min(1.5% of the equity, the envelope's cap, the headroom), and nothing escapes the gate."""
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg)
    rng = random.Random(20261012)
    paths = Paths.of(cfg.ops.data_dir)
    paths.coach_dir.mkdir(parents=True, exist_ok=True)
    grants = 0
    for i in range(20_000):
        obj = random_policy(rng, sha)
        if rng.random() < 0.7:
            try:
                obj["sha256"] = P.sha_of(obj)
            except ValueError:                                  # NaN: cannot be hashed canonically
                obj["sha256"] = "x"
        else:
            obj["sha256"] = rng.choice(["", "0" * 64, 5])
        paths.policy.write_text(json.dumps(obj, allow_nan=True))
        g = gate_for(cfg)                                       # no memory: each file stands alone
        equity = rng.choice([EQ, 50.0, 1e6, rng.uniform(1, 1e5)])
        live_h = rng.choice([None, 0.0, 5.0, 1e9, rng.uniform(0, 100)])
        ctx = RiskCtx(equity=equity, live_headroom_gbp=live_h)
        for key in (INDEX_KEY, "tnb|TREND_PULLBACK|INDEX|TREND", "tnb|SESSION_EXPANSION|INDEX|RANGE"):
            d = decide(g, key, ctx=ctx, static_real=rng.random() < 0.3)
            assert isinstance(d, Decision) and d.route in ROUTES
            if d.route == "REAL":
                grants += 1
                cap = min(1.5 * equity / 100, 1.5 * equity / 100)
                h = obj.get("headroom_gbp")
                if live_h is not None:
                    cap = min(cap, live_h)
                if isinstance(h, (int, float)) and not isinstance(h, bool):
                    cap = min(cap, h)
                assert d.money_gbp is not None and 0 < d.money_gbp <= cap + 1e-9, (obj, d)
                assert d.risk_pct <= 1.5 + 1e-9
    assert grants > 50                                          # the property was exercised


def test_random_byte_corruption_never_produces_a_grant(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg)
    put_policy(cfg, make_policy(sha, entries={INDEX_KEY: grant()}))
    paths = Paths.of(cfg.ops.data_dir)
    good = paths.policy.read_bytes()
    assert decide(gate_for(cfg), INDEX_KEY).route == "REAL"
    rng = random.Random(7)
    remembering = gate_for(cfg)
    assert decide(remembering, INDEX_KEY).route == "REAL"
    for i in range(1500):
        raw = bytearray(good)
        for _ in range(rng.randint(1, 4)):
            raw[rng.randrange(len(raw))] = rng.randrange(256)
        if bytes(raw) == good:
            continue
        paths.policy.write_bytes(bytes(raw))
        assert decide(gate_for(cfg), INDEX_KEY).route != "REAL", raw
        assert decide(remembering, INDEX_KEY).route != "REAL", raw         # the last good one's grant lapses too


def test_guard_never_exceeds_baseline(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg, mode="GUARD")
    rng = random.Random(3)
    for i in range(300):
        entries = {INDEX_KEY: grant(), "tnb|TREND_PULLBACK|INDEX|*": grant(13.0, 0.7, "LIVE", "base"),
                   PRACTICE_KEY: Entry(rng.choice(["PRACTICE", "SKIP", "BASELINE"]), "PRACTICE")}
        put_policy(cfg, make_policy(sha, seq=i + 1, entries=entries, mode=rng.choice(["PILOT", "GUARD"])))
        g = gate_for(cfg)
        for key in (INDEX_KEY, "tnb|TREND_PULLBACK|INDEX|TREND", PRACTICE_KEY):
            d = decide(g, key, static_real=rng.random() < 0.5)
            assert d.route in ("BASELINE", "PRACTICE", "SKIP") and d.money_gbp is None


def test_the_policy_mode_can_only_make_it_stricter(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg, mode="PILOT")
    put_policy(cfg, make_policy(sha, mode="GUARD"))
    assert decide(gate_for(cfg), INDEX_KEY).route == "BASELINE"
    put_policy(cfg, make_policy(sha, seq=2, mode="WATCH"))
    d = decide(gate_for(cfg), PRACTICE_KEY, static_real=True)
    assert d.route == "BASELINE" and d.source == "watch"
    sha2 = put_envelope(cfg, mode="GUARD")
    put_policy(cfg, make_policy(sha2, seq=3, mode="PILOT"))
    assert decide(gate_for(cfg), INDEX_KEY).route == "BASELINE"          # the envelope's GUARD holds


# ================================================================ switches --
def test_coach_off_takes_effect_at_the_next_call(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg)
    put_policy(cfg, make_policy(sha))
    g = gate_for(cfg)
    assert decide(g, INDEX_KEY).route == "REAL"
    assert decide(g, PRACTICE_KEY, static_real=True).route == "PRACTICE"
    touch(cfg, "COACH-OFF")
    assert decide(g, INDEX_KEY).route == "BASELINE"
    assert decide(g, PRACTICE_KEY, static_real=True).route == "BASELINE"
    (Path(cfg.ops.data_dir) / "COACH-OFF").unlink()
    assert decide(g, INDEX_KEY).route == "REAL"


def test_a_changed_envelope_is_read_at_the_next_call(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg)
    put_policy(cfg, make_policy(sha))
    g = gate_for(cfg)
    assert decide(g, INDEX_KEY).route == "REAL"
    put_envelope(cfg, mode="OFF")
    assert decide(g, INDEX_KEY).route == "BASELINE"
    assert decide(g, PRACTICE_KEY, static_real=True).route == "BASELINE"


def test_a_new_policy_is_read_without_a_restart(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg)
    put_policy(cfg, make_policy(sha, seq=1, entries={}))
    g = gate_for(cfg)
    assert decide(g, INDEX_KEY).route == "BASELINE"
    put_policy(cfg, make_policy(sha, seq=2))
    assert decide(g, INDEX_KEY).route == "REAL"


def test_the_fallback_keeps_restrictions_and_drops_grants(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg)
    entries = {INDEX_KEY: grant(), PRACTICE_KEY: Entry("PRACTICE", "BANNED_REAL", rule="S1"),
               "runner|*|*|*": Entry("BASELINE", "PLACED", money_gbp=6.0),
               "tnb|TREND_PULLBACK|FX_MAJOR|*": Entry("SKIP", "PAUSED", rule="D2",
                                                      until_utc=(NOW + dt.timedelta(minutes=5)).isoformat())}
    put_policy(cfg, make_policy(sha, entries=entries))
    g = gate_for(cfg)
    assert decide(g, INDEX_KEY).route == "REAL"
    later = NOW + dt.timedelta(seconds=200)                     # the coach died: its lease lapsed
    assert decide(g, INDEX_KEY, later).route == "BASELINE"
    d = decide(g, PRACTICE_KEY, later, static_real=True)
    assert d.route == "PRACTICE" and d.source == "fallback"
    assert decide(g, "tnb|TREND_PULLBACK|FX_MAJOR|TREND", later, static_real=True).route == "PRACTICE"
    assert decide(g, "tnb|TREND_PULLBACK|FX_MAJOR|TREND", NOW + dt.timedelta(minutes=6),
                  static_real=True).route == "BASELINE"       # its until_utc has passed
    rd = decide(g, "runner|SESSION_EXPANSION|INDEX|TREND", later, static_real=True,
                ctx=RiskCtx(equity=EQ, has_paper=False))
    assert rd.route == "BASELINE" and rd.money_gbp == 6.0      # a cap that only lowers stays too
    # a corrupt file after that: the last good restrictions, never its grants
    Paths.of(cfg.ops.data_dir).policy.write_text("{broken")
    assert decide(g, INDEX_KEY).route == "BASELINE"
    assert decide(g, PRACTICE_KEY, static_real=True).route == "PRACTICE"
    # a new process with no memory: the previous file's restrictions (policy.prev.json passing V1-V5, V13)
    put_policy(cfg, make_policy(sha, seq=2, entries=entries))
    put_policy(cfg, make_policy(sha, seq=3, entries=entries))
    Paths.of(cfg.ops.data_dir).policy.write_text("{broken")
    fresh = gate_for(cfg, now=later)
    assert decide(fresh, PRACTICE_KEY, later, static_real=True).route == "PRACTICE"
    assert decide(fresh, INDEX_KEY, later).route == "BASELINE"


def test_first_install_and_no_envelope_are_aarons_settings(tmp_path):
    cfg = coach_cfg(tmp_path)
    g = gate_for(cfg)
    for key in (INDEX_KEY, PRACTICE_KEY, F1_KEY):
        d = decide(g, key, static_real=True)
        assert d.route == "BASELINE" and d.source == "off"
    put_envelope(cfg)
    d = decide(g, INDEX_KEY, static_real=True)
    assert d.route == "BASELINE" and d.source == "fallback" and "no coach instructions yet" in d.why.lower()


# ================================================================ lookups --
def test_the_key_lookup_order_and_unclear(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg)
    entries = {"tnb|SESSION_EXPANSION|INDEX|TREND": grant(5.0),
               "tnb|SESSION_EXPANSION|INDEX|*": Entry("PRACTICE", "PRACTICE", rule="a"),
               "tnb|*|INDEX|*": Entry("SKIP", "PAUSED", rule="b"),
               "tnb|*|*|*": Entry("PRACTICE", "PRACTICE", rule="c")}
    put_policy(cfg, make_policy(sha, entries=entries))
    g = gate_for(cfg)
    assert decide(g, "tnb|SESSION_EXPANSION|INDEX|TREND").route == "REAL"
    assert decide(g, "tnb|SESSION_EXPANSION|INDEX|RANGE", static_real=True).rule == "a"
    assert decide(g, "tnb|TREND_PULLBACK|INDEX|TREND", static_real=True).rule == "b"
    assert decide(g, "tnb|TREND_PULLBACK|FX_MAJOR|TREND", static_real=True).rule == "c"
    put_policy(cfg, make_policy(sha, seq=2, entries={"tnb|SESSION_EXPANSION|INDEX|*": grant()}))
    assert decide(g, "tnb|SESSION_EXPANSION|INDEX|TREND").route == "REAL"
    assert decide(g, "tnb|SESSION_EXPANSION|INDEX|UNCLEAR").route == "BASELINE"     # never granted


def test_formula1_is_pinned_whatever_the_policy_says(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg)
    put_policy(cfg, make_policy(sha, entries={"tnb|*|*|*": Entry("SKIP", "PAUSED", rule="B2")},
                                blocks=(Block(("tnb",), ("EURGBP",), "SKIP", "S2"),),
                                floors={F1_KEY: Floor(min_score=99.0)}))
    g = gate_for(cfg)
    for cond in ("HIGH_VOL", "NEWS", "TREND", "UNCLEAR"):
        d = decide(g, f"tnb|MOMENTUM_CONTINUATION|FX_MINOR|{cond}", static_real=True, score=60.0)
        assert d.route == "PIN" and d.money_gbp is None
    assert decide(g, "tnb|MOMENTUM_CONTINUATION_2X|FX_MINOR|TREND", static_real=True).route == "PIN"


def test_restrictions_floors_blocks_gates_headroom_and_one_idea(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg)
    put_policy(cfg, make_policy(sha, entries={INDEX_KEY: grant()}, floors={INDEX_KEY: Floor(min_score=76.0)},
                                blocks=(Block(("tnb",), ("US30",), "PRACTICE", "S2"),), headroom_gbp=4.0))
    g = gate_for(cfg)
    assert decide(g, INDEX_KEY, score=70.0).route == "PRACTICE"                    # the floor
    assert decide(g, INDEX_KEY, symbol="US30").route == "PRACTICE"                 # the block
    d = decide(g, INDEX_KEY)
    assert d.route == "REAL" and d.money_gbp == 4.0                                # the headroom clamps
    assert decide(g, INDEX_KEY, ctx=RiskCtx(equity=EQ, live_headroom_gbp=1.5)).money_gbp == 1.5
    assert decide(g, INDEX_KEY, ctx=RiskCtx(equity=EQ, live_headroom_gbp=0.0)).route == "PRACTICE"
    # its own real risk opened after the policy was written comes off the policy's headroom (the 60 s lag)
    before, after = NOW - dt.timedelta(seconds=10), NOW + dt.timedelta(seconds=5)
    soon = NOW + dt.timedelta(seconds=10)
    assert decide(g, INDEX_KEY, soon, ctx=RiskCtx(equity=EQ, own_open=((after, 3.0),))).money_gbp == 1.0
    assert decide(g, INDEX_KEY, soon, ctx=RiskCtx(equity=EQ, own_open=((before, 3.0),))).money_gbp == 4.0
    # one idea, one real ticket
    other = type("P", (), {"symbol": "US500", "side": "SELL", "open_time": NOW - dt.timedelta(hours=2),
                           "magic": 990511})
    assert decide(g, INDEX_KEY, ctx=RiskCtx(equity=EQ, others=lambda: [other])).route == "PRACTICE"
    same = type("P", (), {"symbol": "US500", "side": "BUY", "open_time": NOW - dt.timedelta(minutes=2),
                          "magic": 990511})
    assert decide(g, INDEX_KEY, ctx=RiskCtx(equity=EQ, others=lambda: [same])).route == "PRACTICE"
    old = type("P", (), {"symbol": "US500", "side": "BUY", "open_time": NOW - dt.timedelta(minutes=20),
                         "magic": 990511})
    assert decide(g, INDEX_KEY, ctx=RiskCtx(equity=EQ, others=lambda: [old])).route == "REAL"
    # no new granted entry in the New York rollover (16:45-17:15 NY, 20:45-21:15 UTC until 1 Nov)
    roll = dt.datetime(2026, 10, 13, 20, 50, tzinfo=UTC)
    put_policy(cfg, make_policy(sha, seq=2, now=roll, entries={INDEX_KEY: grant()}), now=roll)
    assert decide(gate_for(cfg, roll), INDEX_KEY, roll).route == "BASELINE"
    fri = dt.datetime(2026, 10, 16, 20, 10, tzinfo=UTC)                           # Friday 16:10 NY
    put_policy(cfg, make_policy(sha, seq=3, now=fri, entries={INDEX_KEY: grant()}), now=fri)
    assert decide(gate_for(cfg, fri), INDEX_KEY, fri).route == "BASELINE"


def test_grants_paused_by_the_data_checks(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg)
    put_policy(cfg, make_policy(sha, grants_allowed=False))
    g = gate_for(cfg)
    assert decide(g, INDEX_KEY).route == "BASELINE"
    assert decide(g, PRACTICE_KEY, static_real=True).route == "PRACTICE"


def test_a_bug_inside_the_gate_is_aarons_settings_and_logged(tmp_path, caplog, monkeypatch):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg)
    put_policy(cfg, make_policy(sha))
    g = gate_for(cfg)
    monkeypatch.setattr(g, "_view", lambda now: 1 / 0)
    d = decide(g, INDEX_KEY, static_real=True)
    assert d.route == "BASELINE" and d.source == "error"
    assert g.bot_may_enter("runner", NOW, static_live=True) is True
    assert g.effective_limits(NOW).max_daily_loss_pct == cfg.risk.max_daily_loss_pct
    assert g.confirm_order(type("R", (), {"symbol": "US500", "tactic": "SESSION_EXPANSION"})) is False


def test_the_gate_log_holds_every_real_decision(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg)
    put_policy(cfg, make_policy(sha))
    g = gate_for(cfg)
    d = decide(g, INDEX_KEY)
    decide(g, PRACTICE_KEY, static_real=False)                  # practice only: not logged
    g.note_fill(1_990_000_001, d, symbol="US500")
    rows = [json.loads(x) for x in (Path(cfg.ops.data_dir) / "coach" / "gate" / "tnb-2026-10-13.jsonl")
            .read_text().splitlines()]
    assert [r.get("event") for r in rows] == [None, "fill"]
    assert rows[0]["key"] == INDEX_KEY and rows[0]["route"] == "REAL" and rows[0]["seq"] == 1
    assert rows[1]["ticket"] == 1_990_000_001 and rows[1]["money"] == d.money_gbp


def test_watch_logs_the_would_be_decision(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg, mode="WATCH")
    put_policy(cfg, make_policy(sha))
    d = decide(gate_for(cfg), INDEX_KEY)
    assert d.route == "BASELINE" and d.source == "watch"
    row = json.loads((Path(cfg.ops.data_dir) / "coach" / "gate" / "tnb-2026-10-13.jsonl").read_text())
    assert row["would"]["route"] == "REAL" and row["route"] == "BASELINE"


def test_the_other_bots_inside_the_trader(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg)
    put_policy(cfg, make_policy(sha, entries={"runner|*|*|*": Entry("SKIP", "PAUSED", rule="B2"),
                                              "bandbreaker|*|*|*": Entry("SKIP", "PAUSED", rule="B2"),
                                              "crowd|*|*|*": Entry("PRACTICE", "PAUSED", rule="B2")}))
    g = gate_for(cfg)
    for name in ("runner", "bandbreaker", "crowd"):
        assert g.bot_may_enter(name, NOW) is True                       # PAPER on Aaron's settings: never gated
        assert g.bot_may_enter(name, NOW, static_live=True) is False
    cfg.runner.mode = "LIVE"
    assert g.bot_may_enter("runner", NOW) is False
    touch(cfg, "COACH-OFF")
    assert g.bot_may_enter("runner", NOW) is True


def test_the_independent_order_check(tmp_path):
    cfg = coach_cfg(tmp_path)
    sha = put_envelope(cfg)
    put_policy(cfg, make_policy(sha))
    g = gate_for(cfg)
    req = lambda sym, tac: type("R", (), {"symbol": sym, "tactic": tac})
    assert g.confirm_order(req("US500", "SESSION_EXPANSION"), kind="INDEX") is True
    assert g.confirm_order(req("EURGBP", "TREND_PULLBACK"), kind="FX_MINOR") is False      # C2
    assert g.confirm_order(req("EURGBP", "MOMENTUM_CONTINUATION"), kind="FX_MINOR") is False
    assert g.confirm_order(req("XAGUSD", "SESSION_EXPANSION"), kind="SILVER") is False
    assert g.confirm_order(req("US500", ""), kind="INDEX") is False                      # says no approach
    cfg.runner.mode = "LIVE"
    assert g.confirm_order(req("US500", "SESSION_EXPANSION"), kind="INDEX") is False      # the Runner's idea
    cfg.runner.mode = "PAPER"
    put_envelope(cfg, mode="GUARD")
    assert g.confirm_order(req("US500", "SESSION_EXPANSION"), kind="INDEX") is False
