"""No chasing (spec sections 6 and 18): making any one trade's outcome worse
never raises any route, state or size; adding a loss never raises a size; the
progress against £100 a day changes nothing (and decide.py cannot even import
the report or the page); and grants and steps appear only in the nightly
plan and take effect only after 06:30 UK, whatever happens during the day.

EVERY TRADE IN THIS FILE IS MADE UP (seeded random streams): never a result.
"""
from __future__ import annotations

import ast
import dataclasses
import datetime as dt
import random
from pathlib import Path

import pytest

from mintel.coach import (BASE_STEP, STATE_LIVE, STATE_LIVE_TRIAL, STATE_PRACTICE, UTC)
from mintel.coach import decide as D
from mintel.coach import records as R
from mintel.coach.run import Coach, nightly_instant
from tests.coach_d_world import (EQ, GOOD_KEY, NOW, OTHER_KEY, FakePage, data_folder, journal_row, seg, series,
                                 trade, trading_days, winner, world, write_rows)

RELEASE = dt.datetime(2026, 11, 11, 6, 30, tzinfo=UTC)
LADDER = ("trial", "base", "S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9")
UP_RULES = ("G1", "G2", "G3", "F1_KEEP", "F1_JUMP", "STOP_UP")


def rank(segs, ents, key):
    e = ents.get(key)
    s = segs.get(key)
    if e is None or e.route != "REAL":
        return (0, 0.0)
    return (1 + LADDER.index(e.step or BASE_STEP), float(e.money_gbp or 0.0))


def outcome(trades, start):
    """Run tonight's review, the morning's release and the day's checks on a copy of ``start``; the key's rank
    and money afterwards."""
    segs = {k: v.copy() for k, v in start.items()}
    w = world(trades, segs)
    D.apply(segs, D.tick(segs, w)[0], w.now)
    downs, plan = D.nightly(segs, w)
    D.apply(segs, downs, w.now)
    wr = world(trades, segs, now=RELEASE)
    moves, _, _ = D.release(segs, {"date": plan.date, "plan": plan.as_dict(), "released_utc": None}, wr, [])
    D.apply(segs, [m for m in moves if m.rule not in ("VETO", "RELEASE_SKIP")], RELEASE)
    D.apply(segs, D.tick(segs, wr)[0], RELEASE)
    ents = D.entries(segs, wr)
    return rank(segs, ents, GOOD_KEY), segs[GOOD_KEY].state


def scenario(rng):
    """A made-up key in a random state with a random (mostly good) record."""
    state = rng.choice(["practice", "trial", "base", "S2"])
    days = rng.randint(8, 14)
    since = dt.datetime.combine(trading_days(NOW.date() - dt.timedelta(days=1), days)[0], dt.time(0), tzinfo=UTC)
    pattern = tuple(round(rng.uniform(-1.0, 1.6), 2) for _ in range(rng.randint(2, 4)))
    trades = winner(days=20, end=since.date() - dt.timedelta(days=1))
    trades += series(GOOD_KEY, pattern, trading_days(NOW.date() - dt.timedelta(days=1), days),
                     risk=6.5 if state == "trial" else 10.0)
    segs = {GOOD_KEY: {"practice": seg(GOOD_KEY),
                       "trial": seg(GOOD_KEY, STATE_LIVE_TRIAL, "trial", since=since),
                       "base": seg(GOOD_KEY, STATE_LIVE, "base", since=since),
                       "S2": seg(GOOD_KEY, STATE_LIVE, "S2", since=since)}[state]}
    return trades, segs


@pytest.mark.parametrize("seed", range(12))
def test_making_any_one_trade_worse_never_raises_a_route_a_state_or_a_size(seed):
    rng = random.Random(seed)
    trades, segs = scenario(rng)
    base = outcome(trades, segs)
    for _ in range(3):
        i = rng.randrange(len(trades))
        worse = list(trades)
        t = worse[i]
        worse[i] = dataclasses.replace(t, net_gbp=round(t.net_gbp - rng.uniform(0.2, 2.0) * t.risk_gbp, 2))
        got = outcome(worse, segs)
        assert got[0] <= base[0], (seed, i, base, got)


@pytest.mark.parametrize("seed", range(12))
def test_adding_a_loss_never_raises_a_size(seed):
    rng = random.Random(100 + seed)
    trades, segs = scenario(rng)
    base = outcome(trades, segs)
    when = NOW - dt.timedelta(hours=rng.randint(1, 60))
    got = outcome(trades + [trade(GOOD_KEY, -rng.uniform(0.3, 2.0), when)], segs)
    assert got[0] <= base[0], (seed, base, got)


# ================================================================ the £100 line --
def test_progress_against_100_a_day_gives_an_identical_policy(tmp_path):
    night = nightly_instant(dt.date(2026, 11, 10)) + dt.timedelta(minutes=1)
    pols = []
    for name, progress in (("none", None),
                           ("far behind", {"days": [{"day": "2026-11-10", "net": -400.0}], "total": -900.0}),
                           ("far ahead", {"days": [{"day": "2026-11-10", "net": 900.0}], "total": 4000.0})):
        data, c, _ = data_folder(tmp_path / name)
        co = Coach(data, cfg=c, clock=lambda: night, fetch=FakePage(progress=progress), shuffles=4)
        pol = co.tick()["policy"]
        co.close()
        pols.append({k: v for k, v in pol.items() if k not in ("seq", "written_utc", "sha256", "envelope_sha")})
    for p in pols[1:]:
        assert {k: v for k, v in p["entries"].items()} == {k: v for k, v in pols[0]["entries"].items()}
        for k in ("blocks", "brakes", "demo", "mode", "grants_allowed", "headroom_gbp", "floors"):
            assert p.get(k) == pols[0].get(k), k


def test_decide_cannot_reach_the_report_the_page_or_the_target():
    src = (Path(__file__).resolve().parents[1] / "mintel" / "coach" / "decide.py").read_text()
    tree = ast.parse(src)
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            names.add(node.module or "")
            names.update(a.name for a in node.names)
        elif isinstance(node, ast.Import):
            names.update(a.name for a in node.names)
    assert not {n for n in names if "report" in n or "coach_page" in n or "dashboard" in n or "pulse" in n}
    for word in ("TARGET_GBP_DAY", "PROGRESS_FROM", "coach_progress", "progress("):
        assert word not in src


# ================================================================ only at night, only at 06:30 --
def test_grants_and_steps_appear_only_in_the_nightly_plan_and_take_effect_only_after_06_30(tmp_path):
    """A seeded random stream of made-up trades during three UK days, ticks at random times: a key's real
    money rises only on the first tick at or after 06:30 UK that released a plan made at 22:20 the night
    before - never during the day, whatever the day's results."""
    rng = random.Random(7)
    data, c, _ = data_folder(tmp_path)
    box = {"now": dt.datetime(2026, 10, 12, 9, 0, tzinfo=UTC)}
    page = FakePage()
    co = Coach(data, cfg=c, clock=lambda: box["now"], fetch=page, shuffles=4)
    write_rows(data, [journal_row(GOOD_KEY, 0.1, dt.datetime(2026, 10, 9, 9, 0, tzinfo=UTC)),
                      journal_row(OTHER_KEY, -0.1, dt.datetime(2026, 10, 9, 10, 0, tzinfo=UTC))])
    co.tick()
    rows = []
    for d in trading_days(dt.date(2026, 11, 9), 16):
        for i, x in enumerate((1.4, 1.2, -0.3)):
            rows.append(journal_row(GOOD_KEY, x, dt.datetime(d.year, d.month, d.day, 9 + 2 * i, 0, tzinfo=UTC),
                                    symbol=("US500", "JP225", "UK100")[i]))
        for i, x in enumerate((-1.0, -0.8, -0.9)):
            rows.append(journal_row(OTHER_KEY, x, dt.datetime(d.year, d.month, d.day, 10 + 2 * i, 0, tzinfo=UTC),
                                    symbol=("XAUUSD", "XAUEUR", "XAUUSD")[i]))
    write_rows(data, rows)
    evidence_today = {}

    def page_today(now):
        trades = R.read_evidence(data, cfg=c, now=now).trades
        return R.real_net_on(trades, R.uk_date(now))

    times = []
    for d in (dt.date(2026, 11, 10), dt.date(2026, 11, 11), dt.date(2026, 11, 12)):
        base = dt.datetime(d.year, d.month, d.day, tzinfo=UTC)
        ts = sorted({base + dt.timedelta(minutes=rng.randint(0, 24 * 60 - 1)) for _ in range(6)}
                    | {nightly_instant(d) + dt.timedelta(minutes=2), dt.datetime(d.year, d.month, d.day, 6, 30,
                                                                                  tzinfo=UTC)})
        times += ts
    last_money: dict = {}
    for now in times:
        # a made-up trade of either key closes since the last tick, good or bad at random
        if rng.random() < 0.7:
            k = rng.choice([GOOD_KEY, OTHER_KEY])
            when = now - dt.timedelta(minutes=rng.randint(1, 30))
            write_rows(data, [journal_row(k, round(rng.uniform(-1.5, 1.5), 2), when,
                                         symbol="US500" if k == GOOD_KEY else "XAUUSD")])
        page.today = page_today(now)
        box["now"] = now
        out = co.tick()
        assert out["ok"], out
        released_now = co.ledger.con.execute("SELECT count(*) FROM plans WHERE released_utc = ?",
                                             (now.isoformat(),)).fetchone()[0]
        for k, e in out["policy"]["entries"].items():
            money = e.get("money_gbp") if e.get("route") == "REAL" else 0.0
            before = last_money.get(k, 0.0)
            if money and money > before + 1e-9:
                uk = now.astimezone(R.TZ_UK)
                assert released_now == 1, (now, k, before, money)
                assert uk.hour == 6 and uk.minute >= 30 or uk.hour > 6, now
            last_money[k] = money or 0.0
        for ch in co.ledger.changes(since=now - dt.timedelta(seconds=1)):
            if ch.kind == "state" and ch.rule in UP_RULES:
                assert released_now == 1, (now, ch.target, ch.rule)
    ups = [c_ for c_ in co.ledger.changes(limit=5000) if c_.kind == "state" and c_.rule in UP_RULES]
    assert ups, "the made-up winner did go live at a release"
    co.close()
