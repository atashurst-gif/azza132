"""The verifiers' findings on the coach's statistics, fixed (12 Oct), each pinned by a test that fails without
its fix. Every record here is made up; none is a result.

6. Practice is never credited above real for a trial: G1 refuses a key whose own real record (20 or more real
   trades) says no edge, even when the capped practice haircut leaves practice credited above real.
8. Floors: the paired test uses each trade's own outcome (no practice haircut), and a switch allows for the
   nightly re-testing (a stricter level, and the evidence must also have held 4 and 8 traded UK days earlier).
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import random

from mintel.coach import decide as D
from mintel.coach import records as R
from mintel.coach import rules as RU
from mintel.coach import tweaks as TW
from mintel.config import Config
from tests.coach_d_world import NOW, seg, series, trading_days, world

UTC = dt.timezone.utc
KEY = "tnb|SESSION_EXPANSION|INDEX|TREND"


# ============================================ 6: real outranks practice in G1 --
def test_a_key_whose_real_record_loses_gets_no_trial_on_practice_alone():
    real = series(KEY, (0.8, -1.0, -0.4), trading_days(dt.date(2026, 10, 9), 20), real=True)       # -0.2 R
    prac_pre = series(KEY, (1.6, -0.6, 0.8), trading_days(dt.date(2026, 10, 9), 20), real=False)   # +0.6 R
    prac_post = [t for t in series(KEY, (1.6, -0.6, 0.8), trading_days(NOW.date() - dt.timedelta(days=1), 18),
                                   real=False) if t.closed_utc > dt.datetime(2026, 10, 13, tzinfo=UTC)]
    trades = R.assign_clusters(real + prac_pre + prac_post)
    h = R.haircut([t for t in trades if t.key == KEY])
    assert h.measured and h.h == 0.30 and h.gap > 0.30           # the spec's cap: practice still above real
    segs = {KEY: seg(KEY, registered=dt.datetime(2026, 10, 13, tzinfo=UTC))}
    w = world(trades, segs)
    w.haircuts = {KEY: h}
    e = w.fit.keys[KEY]
    assert e.p_plus >= 0.90 and e.p_plus_real < 0.5
    ev = RU.g1_evidence(e, w.key_trades(KEY), NOW, haircuts={KEY: h})
    assert ev.failed == ("real_record",) and not ev.passed
    _, plan = D.nightly(segs, w)
    assert plan.items == []


# ============================================ 8: floors ==================
def _day(n, start=dt.date(2026, 9, 14)):
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


def _trade(ticket, day, x, score, real=True, hour=9):
    t = dt.datetime(day.year, day.month, day.day, hour, tzinfo=UTC)
    return R.Trade("journal", "tnb", ticket, "SESSION_EXPANSION", "INDEX", "TREND", "US30", "BUY", t,
                   t + dt.timedelta(minutes=30), real, x * 10.0, 0.0, 10.0, score, 2.5, cluster=f"tnb:{ticket}")


def test_the_paired_test_uses_each_trades_own_outcome_not_the_practice_haircut():
    rows, k = [], 0
    for d in _day(12):
        for j, (x, s) in enumerate([(-1.0, 71.0), (0.5, 71.0), (2.0, 80.0), (-1.0, 80.0), (1.0, 80.0)]):
            k += 1
            rows.append(_trade(k, d, x, s, hour=8 + 2 * j))
    as_of = dt.datetime(2026, 9, 29, 21, 20, tzinfo=UTC)
    ch = TW.Challenger("min_score", 73.0)
    real = TW.paired(KEY, rows, ch, as_of, epochs=())
    prac = TW.paired(KEY, [dataclasses.replace(t, real=False) for t in rows], ch, as_of, epochs=(),
                     haircuts={KEY: 0.30})
    assert prac.mean_delta == real.mean_delta                       # skipping practice earns no haircut


def _null_world(seed, n_keys=20, n_days=60):
    """Made-up keys whose score and reward:risk carry no information (the verifier's exp9 world)."""
    rng = random.Random(seed)
    days = _day(n_days, dt.date(2026, 6, 1))
    kinds = {"GOLD": ["XAUUSD", "XAUEUR"], "FX_MAJOR": ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD"],
             "INDEX": ["US500", "DE40", "UK100", "JP225"]}
    keys = set()
    while len(keys) < n_keys:
        keys.add("tnb|%s|%s|%s" % (rng.choice(["A%d" % i for i in range(5)]), rng.choice(list(kinds)),
                                   rng.choice(["TREND", "HIGH_VOL", "SQUEEZE", "RANGE"])))
    ticket = [1_990_000_000]
    out = []

    def outcome():
        return 2.0 if rng.random() < 1 / 3 else -1.0
    for key in sorted(keys):
        _, ap, kind, cond = key.split("|")
        for d in days:
            moves = []
            for i in range(rng.randint(2, 4)):
                if moves and rng.random() < 0.5:
                    moves[-1].append(i)
                else:
                    moves.append([i])
            for mem in moves:
                mv, side, sym, hr = outcome(), rng.choice(["BUY", "SELL"]), rng.choice(kinds[kind]), rng.randint(7, 19)
                for j, _ in enumerate(mem):
                    x = mv if rng.random() < 0.7 else outcome()
                    ticket[0] += 1
                    t0 = dt.datetime(d.year, d.month, d.day, hr, min(59, 5 * j + rng.randint(0, 4)), tzinfo=UTC)
                    out.append(R.Trade("journal", "tnb", ticket[0], ap, kind, cond, sym, side, t0,
                                       t0 + dt.timedelta(minutes=20), True, round(x * 10.0, 2), 0.6, 10.0))
    out = [dataclasses.replace(t, score=round(rng.uniform(70, 90), 1), rr=round(rng.uniform(1.8, 3.0), 2))
           for t in out]
    return R.assign_clusters(out), days


def test_floors_rarely_switch_on_from_noise_over_60_nights():
    """80 made-up null keys, re-tested every night for 60 nights: at most 5% get a floor (21% before)."""
    switched = total = 0
    for seed in (1, 2, 3, 4):
        trades, days = _null_world(seed)
        cur, keys = {}, set()
        for d in days:
            nt = dt.datetime(d.year, d.month, d.day, 21, 20, tzinfo=UTC)
            seen = [t for t in trades if t.closed_utc <= nt]
            ch = TW.nightly_floors(seen, nt, Config(), current=cur, epochs=(), bots=("tnb",))
            keys |= {c.key for c in ch if c.action == "switch"}
            cur = TW.apply_changes(cur, ch, nt)
        switched += len(keys)
        total += 20
    assert switched <= 0.05 * total


def test_a_switch_needs_its_evidence_to_have_held_earlier_too():
    """A floor whose evidence appeared only in the last few days is not switched on yet."""
    rows, k = [], 0
    ds = _day(12)
    for i, d in enumerate(ds):
        late = i >= len(ds) - 3                                     # the weak signals lose only lately
        for j, (x, s) in enumerate([(-1.0 if late else 1.0, 71.0), (-1.0 if late else 0.5, 71.0), (2.0, 80.0),
                                    (-1.0, 80.0), (1.0, 80.0)]):
            k += 1
            rows.append(_trade(k, d, x, s, hour=8 + 2 * j))
    as_of = dt.datetime(ds[-1].year, ds[-1].month, ds[-1].day, 21, 20, tzinfo=UTC)
    ch = TW.Challenger("min_score", 73.0)
    assert not TW.confirmed(KEY, rows, ch, as_of, 0.99, epochs=())
    assert TW.nightly_floors(rows, as_of, Config(), epochs=()) == []
