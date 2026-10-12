"""The estimator (spec section 5): hand-worked posteriors, leave-one-out
parents, the sceptical cap, DerSimonian-Laird, cluster-robust variance,
Kelly on the lower bound, the forward-only and real-only chains, and
Formula 1's key on the reports fixture.

Every record here is MADE UP, except ``reports_trades()``: Trend & Breakout's
real trades copied from the reports branch (tests/fixtures/coach_reports_trades.csv).
"""
from __future__ import annotations

import csv
import datetime as dt
import math
import statistics
from pathlib import Path

import pytest

import mintel.coach as C
from mintel.coach import estimate as ES
from mintel.coach import records as R
from mintel.engine.tactics import approach_of

UTC = dt.timezone.utc
AS_OF = dt.datetime(2026, 10, 14, 21, 20, tzinfo=UTC)
FIXTURE = Path(__file__).parent / "fixtures" / "coach_reports_trades.csv"
F1_PIN = C.F1_KEY
TAU0 = {1: 0.10, 2: 0.10, 3: 0.15}


def t(ticket, x, *, key="tnb|MADEUP|FX_MINOR|TREND", real=True, cluster=None, closed=None, commission=0.0,
      risk=10.0, score=None, rr=None):
    """A made-up trade with outcome ``x`` R (net = x x risk)."""
    bot, approach, kind, condition = key.split("|")
    closed = closed or AS_OF - dt.timedelta(hours=3)
    return R.Trade("journal", bot, ticket, approach, kind, condition, "EURGBP", "BUY",
                   closed - dt.timedelta(minutes=10), closed, real, x * risk, commission, risk, score, rr,
                   cluster=cluster or f"c{ticket}")


def fit(trades, **kw):
    kw.setdefault("epochs", ())
    kw.setdefault("reestimate_tau", False)
    kw.setdefault("taus", TAU0)
    return ES.fit(trades, AS_OF, **kw)


# ------------------------------------------------------- a hand-worked chain --
def test_prepare_weights_are_the_records_weights():
    trades = reports_trades()
    as_of = dt.datetime(2026, 10, 12, 21, 20, tzinfo=UTC)
    for o in ES.prepare(trades, as_of):
        assert o.w == pytest.approx(R.weight(o.trade, as_of))


def test_a_hand_worked_posterior():
    trades = [t(1, 1.0), t(2, -1.0), t(3, 2.0), t(4, 0.0)]
    e = fit(trades, pins=[F1_PIN]).keys["tnb|MADEUP|FX_MINOR|TREND"]
    # the data: ybar 0.5; sigma sqrt(1.25) (above the 1.0 floor); n_eff 4; v_clu = 5 / 16
    assert e.ybar == pytest.approx(0.5) and e.sigma == pytest.approx(math.sqrt(1.25))
    assert e.n_eff == pytest.approx(4.0) and e.v_clu == pytest.approx(5 / 16)
    assert e.v_data == pytest.approx(max(5 / 16, 1.25 / 4)) and e.cbar == 1.0 and e.clusters == 4
    # the prior: no other key, so every parent is empty: N(-0.05, 0.10^2 + tau1^2 + tau2^2 + tau3^2)
    v_par = 0.10 ** 2 + 0.10 ** 2 + 0.10 ** 2 + 0.15 ** 2
    assert e.m_par == pytest.approx(-0.05) and e.s_par == pytest.approx(math.sqrt(v_par))
    v = 1 / (1 / v_par + 1 / 0.3125)
    m = v * (-0.05 / v_par + 0.5 / 0.3125)
    assert e.m == pytest.approx(m) == pytest.approx(0.029110, abs=1e-6)
    assert e.s == pytest.approx(math.sqrt(v)) == pytest.approx(0.212011, abs=1e-6)
    assert e.p_plus == pytest.approx(0.5 * (1 + math.erf(m / math.sqrt(v) / math.sqrt(2))))
    assert e.lcb80 == pytest.approx(m - 0.8416 * math.sqrt(v))
    assert e.lcb90 == pytest.approx(m - 1.2816 * math.sqrt(v))


def test_the_sigma_floor_is_the_larger_of_one_and_the_bot_kind_pool():
    quiet = [t(i, 0.1 if i % 2 else -0.1, key="tnb|QUIET|FX_MINOR|TREND") for i in range(10)]
    loud = [t(100 + i, 3.0 if i % 2 else -3.0, key="tnb|LOUD|FX_MINOR|TREND") for i in range(10)]
    e = fit(quiet + loud).keys["tnb|QUIET|FX_MINOR|TREND"]
    pooled = math.sqrt(sum(x.x ** 2 for x in quiet + loud) / 20)
    assert e.sigma == pytest.approx(pooled) and pooled > 1.0
    alone = fit(quiet).keys["tnb|QUIET|FX_MINOR|TREND"]
    assert alone.sigma == 1.0                               # its own 0.1 floored at 1.0


# ------------------------------------------------------ leave-one-out parents --
A, B = "tnb|MADEUP|FX_MINOR|TREND", "tnb|MADEUP|FX_MINOR|RANGE"


def test_a_keys_prior_comes_from_its_parent_without_its_own_trades():
    # B is spread wider than the bot x kind pool, so the sigma floor never binds and A's
    # own outcomes cannot reach A's prior even through the floor
    xs = [3.0, -2.0, 2.5, -1.5, 3.0, -2.0]
    b = [t(100 + i, x, key=B) for i, x in enumerate(xs)]
    a1 = [t(i, x, key=A) for i, x in enumerate([0.5, 0.5, 0.5, 0.4])]
    a2 = [t(i, x, key=A) for i, x in enumerate([-0.5, -0.5, -0.5, -0.6])]
    e1 = fit(a1 + b, live_keys=[A]).keys[A]
    e2 = fit(a2 + b, live_keys=[A]).keys[A]
    assert e1.m_par == pytest.approx(e2.m_par) and e1.s_par == pytest.approx(e2.s_par)
    assert e1.m > e2.m
    # by hand: L0 and L1 hold nothing beyond L2; L2 without A is B's six trades
    yb = sum(xs) / 6
    sig2 = sum((x - yb) ** 2 for x in xs) / 6
    vd = max(sum((x - yb) ** 2 for x in xs) / 36, sig2 / 6)
    vp = 0.10 ** 2 + 0.10 ** 2 + 0.10 ** 2
    v = 1 / (1 / vp + 1 / vd)
    m = v * (-0.05 / vp + yb / vd)
    assert e1.m_par == pytest.approx(m)
    assert e1.s_par == pytest.approx(math.sqrt(v + 0.15 ** 2))
    # and B's outcomes do move A's prior
    b_worse = [t(100 + i, x - 1.0, key=B) for i, x in enumerate(xs)]
    assert fit(a1 + b_worse, live_keys=[A]).keys[A].m_par < e1.m_par
    # leave-one-out even through the sigma floor: a wild A cannot widen its parent's spread
    calm_b = [t(100 + i, x, key=B) for i, x in enumerate([0.2, 0.1, 0.3, 0.2, 0.1, 0.3])]
    calm_a = [t(i, x, key=A) for i, x in enumerate([0.2, 0.2, 0.2, 0.2])]
    wild_a = [t(i, x, key=A) for i, x in enumerate([5.0, -3.0, 5.0, -3.0])]
    assert fit(calm_a + calm_b, live_keys=[A]).keys[A].m_par == \
        pytest.approx(fit(wild_a + calm_b, live_keys=[A]).keys[A].m_par)


def test_the_sceptical_cap():
    b = [t(100 + i, 3.0, key=B) for i in range(40)]
    a = [t(i, 0.0, key=A) for i in range(4)]
    capped = fit(a + b).keys[A]
    assert capped.capped and capped.m_par == pytest.approx(C.SCEPTICAL_CAP_R)
    live = fit(a + b, live_keys=[A]).keys[A]
    pinned = fit(a + b, pins=["tnb|MADEUP|FX_MINOR|*"]).keys[A]
    assert not live.capped and live.m_par > C.SCEPTICAL_CAP_R
    assert pinned.m_par == pytest.approx(live.m_par)
    assert capped.m < live.m                                # history used to pick a key never also promotes it


# -------------------------------------------------------------- tau (DL) --
def test_dersimonian_laird_by_hand():
    tau = ES.dersimonian_laird([[(0.0, 0.01), (0.5, 0.01)]])
    # Q = 12.5, df 1, C = 200 - 20000 / 200 = 100: tau^2 = 0.115
    assert tau == pytest.approx(math.sqrt(0.115))
    assert ES.dersimonian_laird([[(0.2, 0.01), (0.2, 0.02)]]) == 0.0
    assert ES.dersimonian_laird([[(0.2, 0.01)]]) is None


def _siblings(means, n=12):
    out, k = [], 0
    for i, mu in enumerate(means):
        key = f"tnb|MADEUP|FX_MINOR|C{chr(65 + i)}"
        for j in range(n):
            k += 1
            out.append(t(k, mu + (1.0 if j % 2 else -1.0), key=key))
    return out


def test_tau_is_reestimated_across_5_or_more_siblings_and_clipped():
    wide = ES.fit(_siblings([-2.0, -1.0, 0.0, 1.0, 2.0]), AS_OF, epochs=())
    assert wide.taus[3] == C.TAU_MAX and wide.tau_estimated[3]
    same = ES.fit(_siblings([0.1] * 5), AS_OF, epochs=())
    assert same.taus[3] == C.TAU_MIN and same.tau_estimated[3]
    few = ES.fit(_siblings([-2.0, -1.0, 1.0, 2.0]), AS_OF, epochs=())
    assert few.taus[3] == C.TAU_DEFAULT[3] and not few.tau_estimated[3]
    held = ES.fit(_siblings([-2.0, -1.0, 1.0, 2.0]), AS_OF, epochs=(), taus={1: 0.2, 2: 0.2, 3: 0.25})
    assert held.taus[3] == 0.25                              # last night's value, not the default
    thin = ES.fit(_siblings([-2.0, -1.0, 0.0, 1.0, 2.0], n=4), AS_OF, epochs=())
    assert not thin.tau_estimated[3]                         # n_eff 4 < 5: not a sibling that counts


# ------------------------------------------------- cluster-robust variance --
def test_cluster_robust_variance():
    trades = [t(i, 2.0, cluster="move-1") for i in range(3)] + [t(10 + i, -1.0, cluster="move-2") for i in range(3)]
    e = fit(trades).keys[A]
    assert e.ybar == pytest.approx(0.5) and e.sigma == pytest.approx(1.5)
    assert e.v_clu == pytest.approx((3 * 1.5) ** 2 * 2 / 36) == pytest.approx(1.125)
    assert e.v_data == pytest.approx(1.125) and 1.125 > 1.5 ** 2 / 6       # three times sigma^2 / n_eff
    assert e.clusters == 2 and e.cbar == 3.0
    apart = fit([t(i, x.x) for i, x in enumerate(trades)]).keys[A]
    assert apart.v_data == pytest.approx(1.5 ** 2 / 6) and apart.cbar == 1.0
    assert apart.s < e.s


def test_the_cluster_ratio_is_clipped_to_three():
    trades = [t(i, 1.0 if i % 2 else -1.0, cluster="one-move") for i in range(10)]
    assert fit(trades).keys[A].cbar == 3.0


# ----------------------------------------------------------------- Kelly --
def test_kelly_on_lcb80_divided_by_the_cluster_size():
    """Section 6.1's illustration: m 0.20, s 0.12, sigma 1.40, c 2.24."""
    lcb80 = 0.20 - 0.8416 * 0.12
    k = ES.kelly_pct(lcb80, 1.40, 2.24)
    assert k == pytest.approx(100 * 0.25 * lcb80 / (1.96 * 2.24)) == pytest.approx(0.564, abs=1e-3)
    assert k < 0.70                                          # below base: "the evidence supports today's size"
    assert ES.kelly_pct(0.1495, 1.40, 2.24) >= 0.85 > ES.kelly_pct(0.148, 1.40, 2.24)   # S1 needs LCB80 >= 0.149
    assert ES.kelly_pct(lcb80, 1.40, 1.0) == pytest.approx(2.24 * k)        # divided by c
    assert ES.kelly_pct(lcb80, 1.40, 2.24, C.DEMO_KELLY_FRACTION) == pytest.approx(2 * k)   # half-Kelly on the demo
    assert ES.kelly_pct(-0.3, 1.40, 2.24) == 0.0


# --------------------------------------------------------- forward and real --
def test_the_forward_only_posterior():
    reg = AS_OF - dt.timedelta(hours=6)                      # every trade on one UK day: equal weights
    before = [t(i, 3.0, closed=reg - dt.timedelta(minutes=i + 1)) for i in range(4)]
    after = [t(10 + i, x, closed=reg + dt.timedelta(minutes=i + 1)) for i, x in enumerate([1.0, -1.0, 2.0, 0.0])]
    e = fit(before + after, registered={A: reg}).keys[A]
    assert e.n_fwd == 4 and e.n_forward_epoch == 4
    # from the root prior N(-0.05, 0.10^2), no pooling; sigma floored at the bot x kind pool of all 8
    pool = before + after
    mean = sum(x.x for x in pool) / 8
    sig = max(1.0, math.sqrt(sum((x.x - mean) ** 2 for x in pool) / 8))
    vd = max(5 / 16, sig ** 2 / 4)
    v = 1 / (1 / 0.01 + 1 / vd)
    assert e.m_fwd == pytest.approx(v * (-0.05 / 0.01 + 0.5 / vd))
    assert e.s_fwd == pytest.approx(math.sqrt(v))
    none = fit(before + after).keys[A]
    assert none.m_fwd is None and none.n_fwd == 0 and none.n_forward_epoch == 0


def test_the_real_only_chain_leaves_practice_out():
    real = [t(1, 1.0), t(2, -1.0), t(3, 2.0), t(4, 0.0)]
    practice = [t(10 + i, 3.0, real=False) for i in range(8)]
    both = fit(real + practice, pins=[F1_PIN]).keys[A]
    alone = fit(real, pins=[F1_PIN]).keys[A]
    assert both.m_real == pytest.approx(alone.m) and both.p_plus_real == pytest.approx(alone.p_plus)
    assert both.lcb80_real == pytest.approx(alone.lcb80)
    assert both.p_plus > both.p_plus_real
    assert both.n_real == 4 and both.n_practice == 8


def test_practice_carries_half_weight_and_the_haircut():
    obs = ES.prepare([t(1, 1.0, real=False), t(2, 1.0)], AS_OF, epochs=())
    prac, real = obs
    assert prac.w == 0.5 and real.w == 1.0
    assert prac.x == pytest.approx(1.0 - 0.10) and real.x == 1.0
    measured = ES.prepare([t(1, 1.0, real=False)], AS_OF, epochs=(), haircuts={A: R.Haircut(0.25, True)})
    assert measured[0].x == pytest.approx(0.75)


def test_commission_x_1_5():
    trades = [t(i, x, commission=1.0) for i, x in enumerate([1.0, -1.0, 2.0, 0.0])]
    e = fit(trades, pins=[F1_PIN]).keys[A]
    plain = fit([t(i, x) for i, x in enumerate([1.0, -1.0, 2.0, 0.0])], pins=[F1_PIN]).keys[A]
    assert e.m == pytest.approx(plain.m)                     # x is already after commission
    assert e.m_cost15 < e.m
    shifted = fit([t(i, x - 0.05) for i, x in enumerate([1.0, -1.0, 2.0, 0.0])], pins=[F1_PIN]).keys[A]
    assert e.m_cost15 == pytest.approx(shifted.m)            # x - 0.5 x 1.0 / 10


def test_the_trade_rate():
    days = [AS_OF - dt.timedelta(days=d) for d in (0, 1, 2)]       # Wed, Tue, Mon
    trades = [t(i, 0.0, closed=d.replace(hour=12)) for i, d in enumerate(days * 2)]
    e = fit(trades).keys[A]
    assert e.lam == pytest.approx((1 + 6) / (1 + 3))


def test_after_a_close_one_key_is_refitted_with_tau_held():
    trades = _siblings([-1.0, -0.5, 0.0, 0.5, 1.0])
    night = ES.fit(trades, AS_OF, epochs=())
    one = ES.refit_key(trades, "tnb|MADEUP|FX_MINOR|CC", AS_OF, night.taus, epochs=())
    assert one.m == pytest.approx(night.keys["tnb|MADEUP|FX_MINOR|CC"].m)
    more = trades + [t(999, -3.0, key="tnb|MADEUP|FX_MINOR|CC")]
    after = ES.refit_key(more, "tnb|MADEUP|FX_MINOR|CC", AS_OF, night.taus, epochs=())
    assert after.m < one.m and after.n == one.n + 1


def test_a_worse_trade_never_raises_the_estimate():
    base = _siblings([0.3, 0.2, 0.0, 0.5, 0.1])
    key = "tnb|MADEUP|FX_MINOR|CA"
    e0 = ES.fit(base, AS_OF, epochs=(), taus=TAU0, reestimate_tau=False).keys[key]
    for i, tr in enumerate(base):
        if tr.key != key:
            continue
        worse = list(base)
        worse[i] = R.Trade(**{**{f: getattr(tr, f) for f in tr.__dataclass_fields__}, "net_gbp": tr.net_gbp - 5.0})
        e1 = ES.fit(worse, AS_OF, epochs=(), taus=TAU0, reestimate_tau=False).keys[key]
        assert e1.m <= e0.m + 1e-12 and e1.p_plus <= e0.p_plus + 1e-12 and e1.m_real <= e0.m_real + 1e-12


def test_the_lab_family_prior_can_only_make_the_coach_more_sceptical():
    assert ES.family_root_mean(-0.05, 0.4) == -0.05
    assert ES.family_root_mean(-0.05, -0.10) == -0.10
    assert ES.family_root_mean(-0.05, -0.9) == C.LAB_FAMILY_FLOOR_R
    trades = [t(1, 1.0), t(2, -1.0), t(3, 2.0), t(4, 0.0)]
    plain = fit(trades).keys[A]
    lower = fit(trades, family_root={"MADEUP": -0.12}).keys[A]
    higher = fit(trades, family_root={"MADEUP": 0.9}).keys[A]
    assert lower.m < plain.m and higher.m == pytest.approx(plain.m)


def test_parent_nodes_are_reported():
    f = fit(_siblings([0.3, 0.2, 0.0, 0.5, 0.1]))
    assert set(f.nodes) == {"tnb", "tnb|MADEUP", "tnb|MADEUP|FX_MINOR"}
    assert f.nodes["tnb"].n == 60 and f.nodes["tnb|MADEUP|FX_MINOR"].level == 2


# ----------------------------------------------- the reports fixture (REAL) --
def reports_trades(until: dt.datetime | None = None) -> list:
    """Trend & Breakout's real trades 17 Sep - 9 Oct from the reports branch.
    The record has no planned risk column, so each trade's risk is its gross
    over its price-R (as section 17 measured it); the 12 trades whose price-R
    is under 0.1 (or whose gross and price-R disagree in sign) take the
    record's median risk instead."""
    rows = [r for r in csv.DictReader(l for l in FIXTURE.open() if not l.startswith("#"))]

    def num(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    risks = [(num(r["net_money"]) - num(r["commission"])) / num(r["realised_r"]) for r in rows
             if abs(num(r["realised_r"])) >= 0.1]
    median = statistics.median([x for x in risks if x > 0])
    out = []
    for r in rows:
        net, com, pr = num(r["net_money"]), num(r["commission"]), num(r["realised_r"])
        risk = (net - com) / pr if abs(pr) >= 0.1 else 0.0
        trade = R.Trade("journal", "tnb", int(r["ticket"]), approach_of(r["tactic"]), R.kind_of(r["symbol"]),
                        r["regime"].upper(), r["symbol"], r["side"], R.parse_utc(r["opened_utc"]),
                        R.parse_utc(r["closed_utc"]), r["mode"] != "PAPER", net, abs(com),
                        risk if risk > 0 else median, num(r["score"]) or None,
                        flags=() if risk > 0 else (R.F_RISK_ESTIMATED,))
        if until is None or trade.closed_utc <= until:
            out.append(trade)
    return R.assign_clusters(out)


F1_HIGH_VOL = "tnb|MOMENTUM_CONTINUATION|FX_MINOR|HIGH_VOL"


def test_formula_1s_key_on_the_reports_fixture():
    """Formula 1's key (momentum continuation on minor pairs in fast markets,
    37 real trades to 8 Oct): m between 0.1 and 0.3 R as the spec expects.
    P+ comes out at 0.83, a little under the spec's expected 0.85-0.97: the
    coach keeps the trades on the nine symbols Aaron excluded later (taking
    them out after the fact would let the history that picked them promote the
    key); without them the same key gives m 0.35 and P+ 0.95."""
    as_of = dt.datetime(2026, 10, 8, 21, 20, tzinfo=UTC)
    trades = reports_trades(as_of)
    assert len(trades) == 436
    e = ES.fit(trades, as_of, pins=[F1_PIN]).keys[F1_HIGH_VOL]
    assert e.n == 37 and not e.capped
    assert 0.10 <= e.m <= 0.30
    assert 0.80 <= e.p_plus <= 0.97
    excluded = {"USDJPY", "DE40", "XAUGBP", "CHFJPY", "EURNZD", "UK100", "GBPJPY", "XAUAUD", "USDCHF", "XAUJPY"}
    kept = [x for x in trades if x.symbol not in excluded]
    e2 = ES.fit(kept, as_of, pins=[F1_PIN]).keys[F1_HIGH_VOL]
    assert 0.85 <= e2.p_plus <= 0.97 and e2.m > e.m
    # its record as section 17 has it: a median risk of about 6.09 a trade
    f1 = [x for x in trades if x.approach == "MOMENTUM_CONTINUATION" and x.kind == "FX_MINOR"
          and x.condition in ("HIGH_VOL", "NEWS") and x.symbol not in excluded]
    assert len(f1) == 38 and round(sum(x.net_gbp for x in f1), 2) == 156.31
    assert statistics.median(x.risk_gbp for x in f1 if R.F_RISK_ESTIMATED not in x.flags) == \
        pytest.approx(6.09, abs=0.01)
