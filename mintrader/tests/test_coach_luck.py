"""The coach's luck check, and how often noise gets a trial (spec sections
6.1, 6.3 and 18).

EVERY TRADE IN THIS FILE IS MADE UP: synthetic keys with a known true edge,
generated from fixed seeds. Outcomes follow Formula 1's shape (a loss of 1 R
two times in three, a win of 2 R one time in three: mean 0, sd 1.41 R), 2-4
trades a UK trading day, clustered (trades of one move share its outcome 70%
of the time), over 60 UK trading days, with the evidence re-tested every
night exactly as the nightly review does.

The planted noise keys are real-money trades with a true edge of zero: the
hardest case for the gate (no practice haircut to hide behind; a practice key
whose haircut is right looks the same).

G2 and G3 belong to decide.py (Builder D); ``_lifecycle`` below transcribes
the spec's rows for them (without the downward moves and the stop-safety
bootstrap, which can only make a promotion rarer), so the rates measured here
are upper bounds.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import random

import pytest

import mintel.coach as C
from mintel.coach import estimate as ES
from mintel.coach import records as R
from mintel.coach import rules as RU

UTC = dt.timezone.utc
KEY = "tnb|MADEUP|FX_MINOR|HIGH_VOL"
REGISTERED = {KEY: dt.datetime(2026, 5, 29, tzinfo=UTC)}           # before the first made-up trade


def trading_days(n, start=dt.date(2026, 6, 1)):
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


DAYS = trading_days(60)


def night(d: dt.date) -> dt.datetime:
    return dt.datetime(d.year, d.month, d.day, 21, 20, tzinfo=UTC)          # 22:20 UK (BST)


def _outcome(rng) -> float:
    return 2.0 if rng.random() < 1 / 3 else -1.0


def made_up_key(rng, edge: float, *, real: bool = True, key=KEY, days=DAYS, risk=10.0) -> list:
    """A made-up key: 2-4 trades a day in 1-4 moves; trades of one move share
    its outcome 70% of the time; ``edge`` R added to every outcome."""
    bot, approach, kind, condition = key.split("|")
    out, ticket = [], 0
    for d in days:
        k = rng.randint(2, 4)
        moves: list[list[int]] = []
        for i in range(k):
            if moves and rng.random() < 0.5:
                moves[-1].append(i)
            else:
                moves.append([i])
        for m, members in enumerate(moves):
            move = _outcome(rng)
            for j, _ in enumerate(members):
                x = (move if rng.random() < 0.7 else _outcome(rng)) + edge
                ticket += 1
                t0 = dt.datetime(d.year, d.month, d.day, 8 + 2 * m, 5 * j, tzinfo=UTC)
                out.append(R.Trade("journal", bot, ticket, approach, kind, condition, "EURGBP", "BUY", t0,
                                   t0 + dt.timedelta(minutes=20), real, round(x * risk, 2), 0.6, risk))
    return R.assign_clusters(out)


def _day_sums(obs, start):
    out = {}
    for o in obs:
        if o.day >= start:
            out[o.day] = out.get(o.day, 0.0) + o.w * o.x
    return out


def _money_days(obs):
    out = {}
    for o in obs:
        out[o.day] = out.get(o.day, 0.0) + o.net_gbp
    return out


def _lifecycle(trades, key=KEY):
    """The first night of a trial, of LIVE(base) and of a step (indexes into
    DAYS), or None. G1's evidence is rules.g1_evidence; G2 and G3 are the
    spec's rows. Weights are computed once at the last night: every G1, G2 and
    G3 quantity is unchanged by a common scale of the weights, so each night's
    answer equals a fresh fit (test_a_fit_on_weights_from_a_later_night_is_the_same)."""
    obs_all = ES.prepare(trades, night(DAYS[-1]), epochs=())
    out = {"trial": None, "base": None, "step": None}
    state, since = "PRACTICE", None
    for di, d in enumerate(DAYS):
        nt = night(d)
        obs = [o for o in obs_all if o.closed_utc <= nt]
        if len(obs) < C.G1_MIN_TRADES or len({o.day for o in obs}) < C.G1_MIN_DAYS:
            continue
        if state == "PRACTICE":
            recent = _day_sums(obs, R.trading_days_back(d, C.G1_LOOKBACK_DAYS))
            if not (sum(recent.values()) > 0 and sum(v > 0 for v in recent.values()) > sum(v < 0 for v in recent.values())):
                continue                                   # G1 needs both: a fit could not pass tonight
            est = ES.fit(None, nt, obs=obs, registered=REGISTERED, epochs=(), reestimate_tau=False,
                         real_chain=False).keys[key]
            if RU.g1_evidence(est, [o.trade for o in obs], nt, epochs=()).passed:
                out["trial"], state, since = di, "TRIAL", d
        elif state == "TRIAL":                             # G2
            trial = [o for o in obs if o.day > since]
            days = _money_days(trial)
            if len(trial) < C.G2_MIN_TRADES or len(days) < C.G2_MIN_DAYS:
                continue
            if not (sum(days.values()) > 0 and sum(v > 0 for v in days.values()) > sum(v < 0 for v in days.values())):
                continue
            est = ES.fit(None, nt, obs=obs, registered=REGISTERED, epochs=(), reestimate_tau=False,
                         live_keys=[key]).keys[key]
            if est.p_plus_real >= C.G2_MIN_P_PLUS_REAL:
                out["base"], state, since = di, "BASE", d
        else:                                              # G3, base -> S1
            live = [o for o in obs if o.day > since]
            days = _money_days(live)
            if len(live) < C.G3_MIN_TRADES or len(days) < C.G3_MIN_DAYS_SINCE_CHANGE:
                continue
            if not (sum(days.values()) > 0 and sum(v > 0 for v in days.values()) > sum(v < 0 for v in days.values())):
                continue
            cum, peak = 0.0, float("-inf")
            for o in sorted(live, key=lambda o: o.closed_utc):
                cum += o.x
                peak = max(peak, cum)
            if cum < peak - 1e-9:
                continue                                   # never grows right after losses
            est = ES.fit(None, nt, obs=obs, registered=REGISTERED, epochs=(), reestimate_tau=False,
                         live_keys=[key]).keys[key]
            if (est.p_plus_real >= C.G3_MIN_P_PLUS_REAL and est.lcb80_real > 0
                    and est.kelly_pct() >= C.step_pct("S1", 0.7)):
                out["step"] = di
                break
    return out


@pytest.fixture(scope="module")
def noise():
    rng = random.Random(20261012)
    return [_lifecycle(made_up_key(rng, 0.0)) for _ in range(1000)]


@pytest.fixture(scope="module")
def planted():
    rng = random.Random(20261013)
    return [_lifecycle(made_up_key(rng, 0.3)) for _ in range(300)]


# ------------------------------------------------------------ the planted tests --
def test_planted_noise_rarely_gets_a_trial_and_almost_never_more(noise):
    """1,000 made-up keys with no edge: P(trial) <= 5%, P(LIVE base) <= 2%,
    P(any step) <= 0.5% (spec section 18)."""
    n = len(noise)
    trial = sum(r["trial"] is not None for r in noise) / n
    base = sum(r["base"] is not None for r in noise) / n
    step = sum(r["step"] is not None for r in noise) / n
    assert trial <= 0.05, trial
    assert base <= 0.02, base
    assert step <= 0.005, step


def test_a_planted_edge_of_0_3_r_reaches_a_trial_at_least_half_the_time_within_60_days(planted):
    rate = sum(r["trial"] is not None for r in planted) / len(planted)
    assert rate >= 0.50, rate
    # and it is found inside the evidence minimums' reach: never before 30 trades over 8 days
    assert min(r["trial"] for r in planted if r["trial"] is not None) >= C.G1_MIN_DAYS - 1


def test_the_spec_forward_test_alone_lets_too_much_noise_through():
    """Why G1's forward bound is stricter than the spec's m_fwd > 0 (it may only
    be made stricter): on the same kind of made-up noise, m_fwd > 0 admits far
    more than 5%. Measured on 200 keys here (19% on 1,000)."""
    rng = random.Random(7)
    loose = 0
    for _ in range(200):
        trades = made_up_key(rng, 0.0)
        obs_all = ES.prepare(trades, night(DAYS[-1]), epochs=())
        for d in DAYS[C.G1_MIN_DAYS:]:
            nt = night(d)
            obs = [o for o in obs_all if o.closed_utc <= nt]
            est = ES.fit(None, nt, obs=obs, registered=REGISTERED, epochs=(), reestimate_tau=False,
                         real_chain=False).keys[KEY]
            g = RU.g1_evidence(est, [o.trade for o in obs], nt, epochs=())
            if set(g.failed) <= {"forward_edge"} and est.m_fwd > 0:
                loose += 1
                break
    assert loose / 200 > 0.05


def test_a_fit_on_weights_from_a_later_night_is_the_same():
    rng = random.Random(3)
    trades = made_up_key(rng, 0.3)
    d = DAYS[30]
    nt = night(d)
    upto = [t for t in trades if t.closed_utc <= nt]
    fresh = ES.fit(upto, nt, registered=REGISTERED, epochs=(), reestimate_tau=False).keys[KEY]
    obs = [o for o in ES.prepare(trades, night(DAYS[-1]), epochs=()) if o.closed_utc <= nt]
    later = ES.fit(None, nt, obs=obs, registered=REGISTERED, epochs=(), reestimate_tau=False).keys[KEY]
    for f in ("n", "n_eff", "clusters", "m", "s", "p_plus", "lcb80", "m_cost15", "m_fwd", "s_fwd",
              "n_forward_epoch", "recent_up", "recent_down"):
        assert getattr(later, f) == pytest.approx(getattr(fresh, f), rel=1e-9, abs=1e-12), f
    assert later.recent_best / later.recent_net == pytest.approx(fresh.recent_best / fresh.recent_net)


# ---------------------------------------------------- one key, by construction --
def _strong_key(n_days=40, per_day=(2.0, -1.0, 2.0), key=KEY, huge=None):
    """A made-up key that earns steadily: 3 trades a day, each its own move."""
    bot, approach, kind, condition = key.split("|")
    out, ticket = [], 0
    for i, d in enumerate(DAYS[:n_days]):
        xs = list(per_day)
        if huge is not None and i == n_days - 1:
            xs = [huge] * 3
        for j, x in enumerate(xs):
            ticket += 1
            t0 = dt.datetime(d.year, d.month, d.day, 8 + 3 * j, 0, tzinfo=UTC)
            out.append(R.Trade("journal", bot, ticket, approach, kind, condition, "EURGBP", "BUY", t0,
                               t0 + dt.timedelta(minutes=20), True, x * 10.0, 0.6, 10.0))
    return R.assign_clusters(out)


def _g1(trades, registered=REGISTERED):
    nt = night(max(t.uk_day for t in trades))
    est = ES.fit(trades, nt, registered=registered, epochs=(), reestimate_tau=False).keys[KEY]
    return RU.g1_evidence(est, trades, nt, epochs=())


def test_a_steady_strong_key_passes_g1_evidence():
    g = _g1(_strong_key())
    assert g.passed, g


def test_a_key_with_one_huge_day_never_passes():
    """Best day over 40% of the last 30 days' net: never a trial, whatever else."""
    flat = _strong_key(per_day=(1.0, -1.0, 0.1), huge=5.0)
    g = _g1(flat)
    assert not g.passed and "best_day" in g.failed
    numbers = g.numbers
    assert numbers["recent_best_r"] > C.G1_MAX_BEST_DAY_SHARE * numbers["recent_net_r"]


def test_fewer_than_15_forward_trades_means_no_trial():
    trades = _strong_key()
    after = sorted(trades, key=lambda t: t.closed_utc)[-15].closed_utc     # registered just before the last 15
    g = _g1(trades, {KEY: after - dt.timedelta(seconds=1)})
    assert "forward" not in g.failed
    g = _g1(trades, {KEY: after})                                          # 14 after it
    assert not g.passed and "forward" in g.failed
    g = _g1(trades, {})                                                    # never registered: no forward record
    assert not g.passed and "forward" in g.failed and "forward_edge" in g.failed


def test_a_standing_rule_on_its_record_stops_a_trial():
    trades = _strong_key()
    # three losing days at the end: net below zero over the epoch the losses start
    bad = []
    for i, d in enumerate(DAYS[40:43]):
        t0 = dt.datetime(d.year, d.month, d.day, 9, tzinfo=UTC)
        bad.append(R.Trade("journal", "tnb", 900 + i, "MADEUP", "FX_MINOR", "HIGH_VOL", "EURGBP", "BUY", t0,
                           t0 + dt.timedelta(minutes=20), True, -30.0, 0.6, 10.0))
    epoch = (dataclasses.replace(RU.E.EPOCHS[0], when_utc=night(DAYS[39]), keys=(KEY,)),)
    nt = night(DAYS[42])
    allt = R.assign_clusters(trades + bad)
    est = ES.fit(allt, nt, registered=REGISTERED, epochs=epoch, reestimate_tau=False).keys[KEY]
    g = RU.g1_evidence(est, allt, nt, epochs=epoch)
    assert "standing_rule" in g.failed and not g.passed


# ------------------------------------------------------------ the luck check --
def _book(rng, keys, edge, n_days=40):
    out = []
    for k in keys:
        out += [dataclasses.replace(t, ticket=t.ticket + 10_000 * keys.index(k))
                for t in made_up_key(rng, edge, key=k, days=DAYS[:n_days])]
    return R.assign_clusters(out)


KEYS = [f"tnb|{a}|FX_MINOR|{c}" for a in ("ALPHA", "BRAVO") for c in ("TREND", "HIGH_VOL")]


def test_the_luck_check_on_noise_passes_and_does_not_block():
    rng = random.Random(11)
    trades = _book(rng, KEYS, 0.0)
    nt = night(DAYS[39])
    reg = {k: dt.datetime(2026, 5, 29, tzinfo=UTC) for k in KEYS}
    res = RU.luck_check(trades, nt, registered=reg, shuffles=40, epochs=())
    assert res.keys == tuple(sorted(KEYS)) and res.tested == 40 * len(KEYS)
    assert res.rate <= C.LUCK_MAX_PASS_RATE and not res.blocked
    again = RU.luck_check(trades, nt, registered=reg, shuffles=40, epochs=())
    assert again == res                                     # seeded with the date: the same night, the same answer
    assert res.seed == 20260724


def test_a_leaky_gate_blocks_every_promotion_that_night():
    """Every key earns, so shuffled labels still pass: the gate cannot tell
    the keys apart, and more than 2% of made-up keys passing blocks the night."""
    rng = random.Random(12)
    trades = _book(rng, KEYS, 1.0)
    nt = night(DAYS[39])
    reg = {k: dt.datetime(2026, 5, 29, tzinfo=UTC) for k in KEYS}
    res = RU.luck_check(trades, nt, registered=reg, shuffles=20, epochs=())
    assert res.rate > C.LUCK_MAX_PASS_RATE and res.blocked
    assert res.numbers["blocked"] is True


def test_the_luck_check_only_counts_keys_that_could_pass_on_counts():
    rng = random.Random(13)
    trades = _book(rng, KEYS, 0.0, n_days=6)                # 6 days: no key can reach 8
    res = RU.luck_check(trades, night(DAYS[5]), shuffles=10, epochs=())
    assert res.keys == () and res.tested == 0 and res.rate == 0.0 and not res.blocked


# --------------------------------------------------------- stricter, never looser --
def test_thresholds_may_only_be_made_stricter():
    """The spec's G1 and luck-check values; every constant in force is at
    least that strict."""
    assert C.G1_MIN_TRADES >= 30 and C.G1_MIN_NEFF >= 20 and C.G1_MIN_CLUSTERS >= 12 and C.G1_MIN_DAYS >= 8
    assert C.G1_MIN_FORWARD >= 15 and C.G1_MIN_P_PLUS >= 0.90 and C.G1_LOOKBACK_DAYS == 30
    assert C.G1_MAX_BEST_DAY_SHARE <= 0.40
    assert RU.G1_FORWARD_Z >= 0.0                          # 0 would be the spec's m_fwd > 0
    assert C.LUCK_SHUFFLES >= 200 and C.LUCK_MAX_PASS_RATE <= 0.02
    assert C.G2_MIN_TRADES >= 20 and C.G2_MIN_DAYS >= 5 and C.G2_MIN_P_PLUS_REAL >= 0.80
    assert C.G3_MIN_TRADES >= 20 and C.G3_MIN_P_PLUS_REAL >= 0.90 and C.KELLY_FRACTION <= 0.25
    assert C.SCEPTICAL_CAP_R <= 0.05 and C.PRIOR_M0_R <= -0.05


def test_the_evidence_never_reads_todays_money_or_the_target():
    """Today's profit or loss and progress against £100 a day never feed
    anything that adds risk: the evidence modules do not name them."""
    import inspect
    from mintel.coach import epochs, tweaks
    for mod in (RU, ES, tweaks, epochs):
        src = inspect.getsource(mod)
        for word in ("TARGET_GBP_DAY", "PROGRESS_FROM", "TARGET_TRADING_DAYS", "status_json", "coach_page",
                     "period_view", "account_standing", "real_today"):
            assert word not in src, (mod.__name__, word)
