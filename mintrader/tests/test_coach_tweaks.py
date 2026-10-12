"""Tweaks: tighten-only floors judged on the same trades (spec section 13).

EVERY RECORD IN THIS FILE IS MADE UP for the test.
"""
from __future__ import annotations

import datetime as dt
import math

import pytest

import mintel.coach as C
from mintel.coach import records as R
from mintel.coach import tweaks as TW
from mintel.config import Config

UTC = dt.timezone.utc
F1 = "tnb|MOMENTUM_CONTINUATION|FX_MINOR|HIGH_VOL"          # under Aaron's Formula 1 pin
SE = "tnb|SESSION_EXPANSION|INDEX|TREND"
IAN = "ian|*|CRYPTO|TREND"


def days(n, start=dt.date(2026, 9, 14)):
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


def trade(ticket, day, x, score, rr=2.5, key=SE, real=True, hour=9):
    bot, approach, kind, condition = key.split("|")
    t = dt.datetime(day.year, day.month, day.day, hour, tzinfo=UTC)
    return R.Trade("journal", bot, ticket, approach, kind, condition, "US30", "BUY", t, t + dt.timedelta(minutes=30),
                   real, x * 10.0, 0.0, 10.0, score, rr, cluster=f"{bot}:{ticket}")


def night(day):
    return dt.datetime(day.year, day.month, day.day, 21, 20, tzinfo=UTC)


def weak_signals_lose(key=SE, n_days=12, low=71.0, start=dt.date(2026, 9, 14), real=True):
    """A made-up key whose signals scoring under 73 lose 1 R each; the rest win on balance."""
    out, k = [], 0
    for d in days(n_days, start):
        for j, (x, s) in enumerate([(-1.0, low), (-1.0, low), (2.0, 80.0), (-1.0, 80.0), (1.0, 80.0)]):
            k += 1
            out.append(trade(k, d, x, s, key=key, real=real, hour=8 + 2 * j))
    return out


# --------------------------------------------------------------- the challengers --
def test_the_challengers_are_the_pre_registered_ones_and_never_looser():
    cfg = Config()                                              # STRONG entries: 70; momentum's own minimum: 70
    got = TW.challengers_for(SE, cfg)
    assert [(c.field, c.value) for c in got] == [("min_score", 73.0), ("min_score", 76.0),
                                                 ("min_rr", 2.0), ("min_rr", 2.2)]
    cfg.scan.min_reward_risk = 2.2                              # Aaron already asks 2.2: no reward:risk challenger
    assert [c.field for c in TW.challengers_for(SE, cfg)] == ["min_score", "min_score"]
    cfg.scan.tactic_min_score = {"SESSION_EXPANSION": 75.0}
    assert [c.value for c in TW.challengers_for(SE, cfg)] == [78.0, 81.0]
    assert [(c.field, c.value) for c in TW.challengers_for(IAN, cfg)] == [("min_score", 65.0)]
    assert TW.challengers_for("bandbreaker|*|INDEX|*", cfg) == []


def test_the_switch_level_is_0_95_corrected_for_the_challengers_tested():
    assert C.TWEAK_MIN_P == 0.983                                # the spec's 0.95 corrected for 3 (0.9833...)
    assert TW.switch_level(3) == pytest.approx(1 - 0.05 / 3) and TW.switch_level(3) >= 0.983
    assert TW.switch_level(4) == pytest.approx(0.9875)          # Trend & Breakout's four written challengers
    assert TW.switch_level(1) == 0.983                          # never below the spec's level
    assert TW.n_registered("tnb") == 4 and TW.n_registered("ian") == 1


# ---------------------------------------------------------------- the paired test --
def test_paired_delta_on_a_hand_fixture():
    d = dt.date(2026, 10, 14)
    trades = [trade(1, d, -1.0, 71.0), trade(2, d, 2.0, 74.0, hour=11), trade(3, d, 1.0, 80.0, hour=13),
              trade(4, d, -1.0, 72.0, hour=15)]
    p = TW.paired(SE, trades, TW.Challenger("min_score", 73.0), night(d), epochs=())
    # delta = -x for the skipped (71 and 72 lost 1 R each: +1, +1), 0 for the others: mean 0.5
    assert (p.trades, p.days, p.skipped, p.clusters) == (4, 1, 2, 4)
    assert p.mean_delta == pytest.approx(0.5)
    v_data = max(4 * 0.25 / 16, 0.25 / 4)                       # cluster-robust, and sigma^2 / n_eff
    v = 1 / (1 / 0.01 + 1 / v_data)
    m = v * (0.5 / v_data)
    assert p.m == pytest.approx(m) == pytest.approx(8 / 116)
    assert p.s == pytest.approx(math.sqrt(v))
    assert p.p == pytest.approx(0.5 * (1 + math.erf(m / math.sqrt(v) / math.sqrt(2))))
    # a challenger that skips nothing learns nothing
    none = TW.paired(SE, trades, TW.Challenger("min_score", 50.0), night(d), epochs=())
    assert none.skipped == 0 and none.mean_delta == 0.0 and none.p == pytest.approx(0.5)


def test_one_move_counts_once_in_the_paired_test():
    d = dt.date(2026, 10, 14)
    apart = [trade(i, d, -1.0, 71.0, hour=8 + i) for i in range(4)] + [trade(10 + i, d, 1.0, 80.0, hour=8 + i)
                                                                         for i in range(4)]
    together = [R.Trade(**{**{f: getattr(t, f) for f in t.__dataclass_fields__},
                           "cluster": "one" if t.score < 73 else t.cluster}) for t in apart]
    a = TW.paired(SE, apart, TW.Challenger("min_score", 73.0), night(d), epochs=())
    b = TW.paired(SE, together, TW.Challenger("min_score", 73.0), night(d), epochs=())
    assert a.mean_delta == b.mean_delta and b.s > a.s and b.p < a.p


# ----------------------------------------------------------------- the nightly --
def test_a_floor_is_switched_on_with_enough_evidence_and_only_then():
    trades = weak_signals_lose()
    as_of = night(days(12)[-1])
    changes = TW.nightly_floors(trades, as_of, Config(), epochs=())
    assert len(changes) == 1
    c = changes[0]
    assert (c.key, c.action, c.field, c.value, c.before) == (SE, "switch", "min_score", 73.0, None)
    assert c.numbers["trades"] == 60 and c.numbers["skipped"] == 24 and c.numbers["days"] == 12
    assert c.numbers["mean_delta_r"] == pytest.approx(0.4) and c.numbers["p"] >= TW.switch_level(4)
    # too few days, too few trades: nothing
    assert TW.nightly_floors(weak_signals_lose(n_days=7), night(days(7)[-1]), Config(), epochs=()) == []
    # the same record on practice trades: the floor is judged on what the key took, practice included
    assert TW.nightly_floors(weak_signals_lose(real=False), as_of, Config(), epochs=())[0].value == 73.0


def test_a_floor_never_applies_to_the_pinned_key():
    trades = weak_signals_lose(key=F1)
    as_of = night(days(12)[-1])
    assert TW.nightly_floors(trades, as_of, Config(), pins=[C.F1_KEY], epochs=()) == []
    assert TW.nightly_floors(trades, as_of, Config(), pins=[C.Pin(C.F1_KEY)], epochs=()) == []
    assert TW.nightly_floors(trades, as_of, Config(), pins=[], epochs=())          # unpinned: it would


def test_never_loosens():
    trades = weak_signals_lose()
    as_of = night(days(12)[-1])
    since = dt.datetime(2026, 9, 1, tzinfo=UTC)
    # a stricter floor already in force: a looser challenger is never tested
    held = {SE: TW.FloorState(C.Floor(min_score=76.0), since)}
    for c in TW.nightly_floors(trades, as_of, Config(), current=held, epochs=()):
        assert c.action == "revert" or (c.field != "min_score" or c.value > 76.0)
    # and applying a change never lowers a floor
    out = TW.apply_changes(held, [TW.FloorChange(SE, "switch", "min_score", 73.0, 76.0)], as_of)
    assert out[SE].floor.min_score == 76.0
    out = TW.apply_changes({}, [TW.FloorChange(SE, "switch", "min_rr", 2.2, None)], as_of)
    assert out[SE].floor == C.Floor(min_rr=2.2) and out[SE].since_utc == as_of
    # config.json's own threshold is never relaxed: every challenger is above it
    cfg = Config()
    for c in TW.challengers_for(SE, cfg):
        own = TW.tnb_threshold(cfg, "SESSION_EXPANSION") if c.field == "min_score" else cfg.scan.min_reward_risk
        assert c.value > own


def test_a_floor_is_reverted_when_its_forward_trades_say_it_hurts():
    since = night(days(12)[-1])
    held = {SE: TW.FloorState(C.Floor(min_score=73.0), since)}
    # 40 forward trades after it: the floored signals (practising on) now WIN 1 R each
    fwd, k = [], 1000
    for d in days(8, dt.date(2026, 10, 1)):
        for j, (x, s) in enumerate([(1.0, 71.0), (1.0, 71.0), (2.0, 80.0), (-1.0, 80.0), (1.0, 80.0)]):
            k += 1
            fwd.append(trade(k, d, x, s, hour=8 + 2 * j, real=s >= 73))
    as_of = night(days(8, dt.date(2026, 10, 1))[-1])
    changes = TW.nightly_floors(weak_signals_lose() + fwd, as_of, Config(), current=held, epochs=())
    assert [(c.action, c.field, c.value, c.before) for c in changes] == [("revert", "min_score", None, 73.0)]
    assert changes[0].numbers["trades"] == 40 and changes[0].numbers["p"] < C.TWEAK_REVERT_P
    assert TW.apply_changes(held, changes, as_of) == {}
    # 39 forward trades: too soon to revert
    assert TW.nightly_floors(weak_signals_lose() + fwd[:-1], as_of, Config(), current=held, epochs=()) == []


def test_at_most_one_floor_change_a_bot_a_night():
    other = "tnb|BREAKOUT_ACCEPTANCE|INDEX|TREND"
    trades = weak_signals_lose() + [R.Trade(**{**{f: getattr(t, f) for f in t.__dataclass_fields__},
                                               "ticket": t.ticket + 5000, "approach": "BREAKOUT_ACCEPTANCE",
                                               "cluster": f"x{t.ticket}"}) for t in weak_signals_lose()]
    ian = [R.Trade(**{**{f: getattr(t, f) for f in t.__dataclass_fields__}, "ticket": t.ticket + 9000, "bot": "ian",
                      "approach": "*", "kind": "CRYPTO", "symbol": "BTCUSD", "cluster": f"i{t.ticket}",
                      "score": 61.0 if t.score < 73 else 80.0}) for t in weak_signals_lose()]
    as_of = night(days(12)[-1])
    changes = TW.nightly_floors(trades + ian, as_of, Config(), epochs=())
    by_bot = {}
    for c in changes:
        by_bot.setdefault(c.key.split("|")[0], []).append(c)
    assert len(by_bot["tnb"]) == 1 and by_bot["tnb"][0].key in (SE, other)
    assert len(by_bot["ian"]) == 1 and by_bot["ian"][0].value == 65.0
