"""The lab's statistics: how skill is told from luck.

The planted tests are the point of this file: a family of candidates with NO
edge must fail the fresh gate at about the rate the arithmetic says, and a
candidate with a planted true edge of 0.3 R a trade must pass the past checks
and usually the fresh test.
"""
from __future__ import annotations

import ast
import math
import random
import re
from pathlib import Path

import pytest

import mintel.lab as lab
from mintel.lab import (CHECK_COSTS, CHECK_FRESH, CHECK_KEYS, CHECK_LUCK, CHECK_STEADY, CHECK_TRADES,
                        FAILED, FRESH_P, NOT_YET, PASSED, RUNNING, Candidate, Result, Trade, WindowInfo,
                        make_params, synth)
from mintel.lab import stats as ST
from mintel.lab import candidate as C
from mintel.lab import families as FAM

RISK = 13.0


def trade(day, net, cost=1.0, assumed=False, sym="EURUSD", hh=10):
    return Trade(sym, 1, 0.1, 1.0, 0.99, 1.02, f"{day}T{hh:02d}:00:00+00:00", f"{day}T{hh + 1:02d}:00:00+00:00",
                 1.0, "STOP", net + cost + 1.0, cost, net, net / RISK, 5, assumed, "bar", day)


def result(cid, days, nets_by_day, *, kind="past", cost=1.0, assumed=0.0, synthetic=True):
    """A Result whose days have the given lists of trade nets."""
    trades, daily = [], {}
    k = 0
    for d in days:
        nets = nets_by_day.get(d, [])
        for n in nets:
            k += 1
            trades.append(trade(d, n, cost, assumed=(random.Random(k).random() < assumed)))
        daily[d] = (sum(nets), len(nets))
    return Result(cid, WindowInfo(kind, days[0], days[-1], len(days)), trades, daily, {}, synthetic, 0.0)


def day_list(n, first="2026-06-01"):
    return synth.weekdays(first, n)


def steady_result(cid="lab-0123456789", n_days=40, per_day=(3.0, 3.0, -2.0), trades_each=3, **kw):
    days = day_list(n_days)
    nets = {d: list(per_day) * 1 for d in days}
    return result(cid, days, nets, **kw)


# ----------------------------------------------------------- written once ---
def test_the_fresh_p_constant_is_written_once_and_is_015_over_30():
    assert FRESH_P == 0.15 / 30 == lab.FRESH_P == ST.FRESH_P
    assert lab.FRESH_LOOKS_PER_STRETCH == 30 and lab.FRESH_FAMILYWISE == 0.15
    root = Path(lab.__file__).parent
    found = {"0.005": [], "0.15": [], "30": []}
    for p in sorted(root.rglob("*.py")):
        tree = ast.parse(p.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, float):
                if node.value == 0.005:
                    found["0.005"].append(p.name)
                if node.value == 0.15:
                    found["0.15"].append(p.name)
    assert found["0.005"] == []                           # never typed as a number: always FRESH_P
    assert [f for f in found["0.15"] if f != "synth.py"] == ["__init__.py"]   # the family-wise level, once
    # (synth.py's 0.15 is the made-up spreads' noise, not a threshold)
    src = (root / "__init__.py").read_text()
    assert len(re.findall(r"^FRESH_P\s*=", src, re.M)) == 1
    assert not [p.name for p in root.glob("*.py") if p.name != "__init__.py"
                and re.search(r"^FRESH_P\s*=", p.read_text(), re.M)]


def test_the_cap_of_thirty_looks_is_the_default_setting_too():
    assert lab.LabSettings().fresh_cap_per_stretch == lab.FRESH_LOOKS_PER_STRETCH == 30
    assert lab.LabSettings().fresh_per_night == 3


def test_the_cap_of_looks_can_be_lowered_in_the_settings_but_never_raised_above_thirty():
    """FRESH_P is 0.15 spread over 30 looks. 500 looks at 0.005 each would be about 2.5 lucky passes a
    stretch while the page still said 'counting the 30 looks'."""
    for asked, got in ((500, 30), (31, 30), (30, 30), (12, 12), (0, 0), (-1, 30), ("lots", 30)):
        assert lab.LabSettings.from_dict({"fresh_cap_per_stretch": asked}).fresh_cap_per_stretch == got, asked
    assert [lab.clamp_fresh_cap(x) for x in (500, 30, 7, -3, None)] == [30, 30, 7, 0, 30]


# ------------------------------------------------------------- bootstrap ---
def test_the_bootstrap_is_deterministic_and_seeded_from_the_id():
    rng = random.Random(1)
    daily = [rng.gauss(1.5, 6.0) for _ in range(40)]
    a = ST.block_bootstrap_p(daily, "lab-00000001ab")
    assert a == ST.block_bootstrap_p(daily, "lab-00000001ab")
    assert ST.seed_of("lab-0123456789") == int("01234567", 16)
    assert ST.seed_of("lab-00000001ab") == 1
    # a different id is a different (but equally valid) stream: close, not identical
    others = {ST.block_bootstrap_p(daily, f"lab-{i:08x}ab") for i in range(2, 8)}
    assert len(others) > 1 and all(abs(o - a) < 0.03 for o in others)
    assert 0.0 < a < 0.5
    # an id that is not a lab id (a bot's) still gets a stable seed
    assert ST.seed_of("momentum_runner") == ST.seed_of("momentum_runner")


@pytest.mark.parametrize("block", [1, 2, 5])
def test_the_bootstrap_matches_an_independent_calculation(block):
    """Centre the days, resample blocks round the end, score every resample by its mean over
    its own standard error (a bootstrap-t), and count the ones scoring at least what the real
    days scored."""
    daily = [4.0, -1.0, 3.0, 2.0, -2.0, 5.0, 1.0, -3.0, 6.0, 2.0, 1.0, -1.0]
    ident = "lab-000000ff00"
    n, b = len(daily), block
    mean = sum(daily) / n
    x = [d - mean for d in daily]
    sd = math.sqrt(sum(v * v for v in x) / (n - 1))
    t_real = mean / (sd / math.sqrt(n))
    rnd = random.Random(0xff).random
    nb = -(-n // b)
    hits = 0
    for _ in range(2000):
        picked = []
        for _k in range(nb):
            i = int(rnd() * n)
            picked.extend(x[(i + j) % n] for j in range(b))
        m = sum(picked) / len(picked)
        sd_r = math.sqrt(sum((v - m) ** 2 for v in picked) / (len(picked) - 1))
        if m / (sd_r / math.sqrt(len(picked))) >= t_real - 1e-9:
            hits += 1
    assert ST.block_bootstrap_p(daily, ident, block=block) == pytest.approx((hits + 1) / 2001)


def test_blocks_of_five_only_once_there_are_a_hundred_days_to_resample():
    assert [ST.effective_block(n) for n in (10, 20, 39, 40, 59, 60, 99, 100, 250)] == [1, 1, 1, 2, 2, 3, 4, 5, 5]
    daily = [3.0, -1.0, 2.0, 5.0, -2.0] * 24                                   # 120 days: five-day blocks
    assert ST.block_bootstrap_p(daily, "lab-00000003ab") == ST.block_bootstrap_p(daily, "lab-00000003ab", block=5)
    short = [3.0, -1.0, 2.0, 5.0, -2.0] * 5                                    # 25 days: single days
    assert ST.block_bootstrap_p(short, "lab-00000003ab") == ST.block_bootstrap_p(short, "lab-00000003ab", block=1)


def test_bootstrap_on_known_series():
    assert ST.block_bootstrap_p([], "lab-00000001ab") == 1.0
    assert ST.block_bootstrap_p([0.0] * 30, "lab-00000001ab") == 1.0              # nothing made, nothing proved
    steady = ST.block_bootstrap_p([2.0] * 30, "lab-00000001ab")
    assert steady == pytest.approx(1 / 2001)                                      # never exactly zero
    losing = ST.block_bootstrap_p([-2.0, -1.0, -3.0, 1.0] * 10, "lab-00000001ab")
    assert losing > 0.5
    rng = random.Random(3)
    noise = [rng.gauss(0, 10) for _ in range(60)]
    m = sum(noise) / 60
    p0 = ST.block_bootstrap_p(noise, "lab-00000001ab")
    assert p0 > 0.05 or m > 0
    strong = [rng.gauss(8, 10) for _ in range(60)]                                # 0.8 sd a day for 60 days
    assert ST.block_bootstrap_p(strong, "lab-00000001ab") < 0.001


def test_a_streak_is_kept_together_so_a_lucky_run_is_not_mistaken_for_consistency():
    """Ten good days in a row then ten bad ones: same total as alternating, but five-day blocks
    keep the run together so the chance of that total is judged less kindly than for iid days."""
    alt = [3.0, -1.0] * 15
    run = [3.0] * 15 + [-1.0] * 15
    assert sum(alt) == sum(run)
    assert ST.block_bootstrap_p(run, "lab-00000009ab", block=5) > ST.block_bootstrap_p(alt, "lab-00000009ab", block=5)


def test_the_bootstrap_is_close_to_honest_on_pure_noise():
    rng = random.Random(11)
    n, hits05, hitsF = 700, 0, 0
    for i in range(n):
        daily = [rng.gauss(0, 10) for _ in range(30)]
        p = ST.block_bootstrap_p(daily, "lab-%010x" % rng.getrandbits(40))
        hits05 += p <= 0.05
        hitsF += p <= FRESH_P
    assert 0.035 <= hits05 / n <= 0.085                    # nominally 5%
    assert hitsF / n <= 0.02                               # nominally 0.5%


def _plain_bootstrap_p(daily, ident, resamples=2000):
    """The bootstrap this module used before it was studentised: centre the days, resample them in
    blocks, count resamples whose MEAN is at least the real one. Kept here as the yardstick."""
    n = len(daily)
    mean = sum(daily) / n
    x = [d - mean for d in daily]
    b = ST.effective_block(n)
    nb = -(-n // b)
    ext = x + x[:b - 1]
    pre = [0.0]
    for v in ext:
        pre.append(pre[-1] + v)
    rnd = random.Random(ST.seed_of(ident)).random
    hits = 0
    for _ in range(resamples):
        total = 0.0
        for _k in range(nb):
            i = int(rnd() * n)
            total += pre[i + b] - pre[i]
        hits += total / (nb * b) >= mean - 1e-12
    return (hits + 1) / (resamples + 1)


def test_a_run_of_small_wins_with_a_few_big_losses_is_not_one_chance_in_two_thousand():
    """25 days from a world with NO edge (small wins most days, a big loss now and then), where the
    big losses happened to come late. 22 days up, +GBP 9 in all. The plain bootstrap trusted the
    narrow spread of the days it saw and called this 1 in 2,000, past the fresh line of 0.005; the
    studentised one scores each resample by its own spread and does not."""
    daily = [0.75, 0.68, 0.6, 0.38, 0.54, -0.64, 0.71, 0.75, 0.58, 0.62, 0.68, 0.6, -2.04, 0.45, 0.51,
             0.68, 0.57, 0.68, 0.48, 0.45, 0.52, 0.53, 0.51, -1.04, 0.61]
    ident = "lab-0000000abc"
    assert _plain_bootstrap_p(daily, ident) <= FRESH_P                      # what it used to say
    p = ST.block_bootstrap_p(daily, ident)
    assert p > 0.03 and p > 5 * FRESH_P
    # while a run that really is steady, with a real spread behind it, still gets its small p
    steady = [2.0 + 0.4 * ((i * 7) % 5 - 2) for i in range(25)]
    assert ST.block_bootstrap_p(steady, ident) <= FRESH_P


@pytest.mark.slow
def test_pure_noise_passes_the_fresh_line_no_more_often_than_it_claims_whatever_its_shape():
    """Measured over thousands of made-up windows (see the docstring of block_bootstrap_p): at 20 to 45
    days the plain bootstrap passed the 0.005 line 1.2% of the time for fair flips and 4 to 10% of
    the time where winners are small and frequent. Zero-mean payoffs, two trades a day."""
    def fair(r):
        return r.choice((1.0, -1.0))

    def small_wins(r):                                      # +0.3R on 77% of trades, -1R on the rest, a little noise
        return 0.2987 * (0.5 + r.random()) if r.random() < 0.77 else -(0.5 + r.random())

    for name, gen, n, reps, plain_floor in (("fair", fair, 20, 450, None), ("small wins", small_wins, 25, 450, 0.015)):
        rng = random.Random(31337)
        new = old = 0
        for _ in range(reps):
            daily = [gen(rng) + gen(rng) for _ in range(n)]
            ident = "lab-%010x" % rng.getrandbits(40)
            new += ST.block_bootstrap_p(daily, ident) <= FRESH_P
            if plain_floor is not None:
                old += _plain_bootstrap_p(daily, ident) <= FRESH_P
        assert new / reps <= 0.008, (name, new / reps)
        if plain_floor is not None:
            assert old / reps >= plain_floor, (name, old / reps)                # and the yardstick is as bad as claimed


def test_why_the_blocks_shrink_on_short_windows():
    """Measured: on 25 days of pure noise, literal five-day blocks pass the 0.005 line several
    times as often as the single-day bootstrap does."""
    rng = random.Random(21)
    n, literal, adaptive = 600, 0, 0
    for i in range(n):
        daily = [rng.gauss(0, 10) for _ in range(25)]
        ident = "lab-%010x" % rng.getrandbits(40)
        literal += ST.block_bootstrap_p(daily, ident, block=5) <= FRESH_P
        adaptive += ST.block_bootstrap_p(daily, ident) <= FRESH_P
    assert literal >= 2 * adaptive and literal / n >= 0.015
    assert adaptive / n <= 0.02


# ------------------------------------------------------------- the words ---
def test_the_luck_line_is_one_in_n():
    assert ST.luck_n(FRESH_P, 30) == 7 and ST.luck_line(FRESH_P, 30) == "Chance this is luck: about 1 in 7"
    assert ST.luck_n(0.05, 2) == 10 and ST.luck_n(0.05, 1) == 20
    assert ST.luck_n(1 / 2001, 30) == 67
    assert ST.luck_n(0.6, 30) == 1 and ST.luck_n(1.0, 1) == 1                 # min(1, p x m)
    assert ST.luck_line(0.2) == "Chance this is luck: about 1 in 5"
    assert ST.luck_n(1e-30, 1) <= 99999


def test_luck_of_a_bots_real_days():
    days = day_list(20)
    out = ST.luck_of_daily([4.0, -1.0] * 10, "momentum_runner", m=1)
    assert set(out) == {"p", "n", "m", "text"} and out["m"] == 1
    assert out["text"].startswith("Chance this is luck: about 1 in ")
    assert out["n"] == ST.luck_n(out["p"], 1)


# ---------------------------------------------------------------- the rules ---
def test_chunks_are_four_equal_contiguous_slices():
    assert ST.chunk_nets([1.0] * 8) == [2.0, 2.0, 2.0, 2.0]
    assert ST.chunk_nets(list(range(10))) == [0 + 1 + 2, 3 + 4 + 5, 6 + 7, 8 + 9]   # the first slices take the odd days
    assert sum(ST.chunk_nets(list(range(41)))) == sum(range(41))
    assert len(ST.chunk_nets([])) == 4


def stats_of(**kw):
    base = dict(kind="past", days=40, trades=100, net_gbp=300.0, cost_gbp=100.0, pf=1.5, win_rate=0.5,
                up_days=24, down_days=14, best_day_gbp=40.0, p=0.01, folds_up=4, folds_worst=40.0,
                cost15_net=250.0, cost20_net=200.0, assumed_share=0.0)
    base.update(kw)
    return ST.TestStats(**base)


def test_the_past_checks_all_pass_for_a_good_candidate():
    v = ST.past_verdict(stats_of(), "ok")
    assert v.passed and v.forward_ok and v.reasons == [] and v.first_failed == ""
    assert v.states() == {CHECK_TRADES: PASSED, CHECK_STEADY: PASSED, CHECK_FRESH: NOT_YET,
                          CHECK_COSTS: PASSED, CHECK_LUCK: PASSED}
    assert [c.key for c in v.checks] == list(CHECK_KEYS)
    assert [c.label for c in v.checks] == ["Enough trades", "Steady", "Fresh prices", "Costs", "Luck check"]


@pytest.mark.parametrize("change,reason,failed", [
    (dict(trades=59), "TOO_FEW_TRADES", CHECK_TRADES),
    (dict(days=19), "TOO_FEW_TRADES", CHECK_TRADES),
    (dict(net_gbp=-1.0, cost15_net=-50.0, best_day_gbp=1.0), "NOT_STEADY", CHECK_STEADY),
    (dict(pf=1.19), "NOT_STEADY", CHECK_STEADY),
    (dict(folds_up=2), "NOT_STEADY", CHECK_STEADY),
    (dict(folds_worst=-151.0), "NOT_STEADY", CHECK_STEADY),                    # worse than -0.5 x 300
    (dict(up_days=14, down_days=14), "NOT_STEADY", CHECK_STEADY),                # strictly more days up than down
    (dict(best_day_gbp=121.0), "NOT_STEADY", CHECK_STEADY),                      # over 40% of 300
    (dict(cost15_net=-0.01), "COSTS", CHECK_COSTS),
    (dict(assumed_share=0.11), "COSTS", CHECK_COSTS),
])
def test_each_past_rule_fails_on_its_own(change, reason, failed):
    v = ST.past_verdict(stats_of(**change), "ok")
    assert not v.passed and reason in v.reasons and v.first_failed == failed
    assert v.check(failed).state == FAILED and v.why


def test_the_rules_sit_exactly_on_their_thresholds():
    ok = lambda **kw: ST.past_verdict(stats_of(**kw), "ok").passed
    assert ok(trades=60, days=20) and ok(pf=1.2) and ok(folds_up=3) and ok(folds_worst=-150.0)
    assert ok(best_day_gbp=120.0) and ok(up_days=15, down_days=14) and ok(cost15_net=0.01)
    assert ok(assumed_share=0.10) and not ok(trades=59) and not ok(pf=1.1999)


def test_the_luck_check_on_old_prices_is_shown_not_a_gate():
    v = ST.past_verdict(stats_of(p=0.4), "ok")
    assert v.passed                                              # a poor past p does not block the forward
    assert v.check(CHECK_LUCK).state == NOT_YET
    v2 = ST.past_verdict(stats_of(p=0.05), "ok")
    assert v2.check(CHECK_LUCK).state == PASSED
    assert "Chance this is luck: about 1 in 20" in v2.check(CHECK_LUCK).text


def test_the_dsr_is_shown_and_never_gates():
    res = steady_result()
    a = ST.summarize(res, n_trials=1)
    b = ST.summarize(res, n_trials=632)
    assert a.dsr > b.dsr >= 0.0                                   # more trials tried, less impressive
    assert ST.past_verdict(b, "na").passed == ST.past_verdict(a, "na").passed
    low = stats_of(dsr=0.0)
    assert ST.past_verdict(low, "ok").passed
    assert "dsr" in ST.TestStats().to_cols()


def test_the_deflated_sharpe_is_fed_per_day_sharpes_though_the_ledger_keeps_annual_ones(tmp_path):
    """The ledger holds ANNUALISED Sharpes (TestStats.sharpe). The library wants per-day ones. Fed as
    they came, the bar to clear was 16 times too high, every DSR was exactly 0.0, and the forwarding
    rank 'DSR, then p' was really 'p' alone."""
    from mintel.lab.ledger import LabLedger
    from mintel.research.validation import deflated_sharpe_ratio
    rng = random.Random(1)
    led = LabLedger(tmp_path / "lab.sqlite", tmp_path / "trials.sqlite")
    fam = FAM.get("OPENING_RANGE")
    for k, c in enumerate(fam.grid()[:40]):                                # 40 noise trials through the real path
        led.register(c, "x")
        noise = [rng.gauss(0.0, 10.0) for _ in range(60)]
        s_ = ST.summarize(result(c.id, day_list(60), {d: [v] for d, v in zip(day_list(60), noise)}))
        tid = led.begin_test(c.id, "past", "F-2026-10", "2026-09-01")
        led.finish_test(tid, s_.to_cols(), {}, reasons=[], skipped={}, synthetic=True, seconds=0.1,
                        trial=s_.trial_extra())
    annual = led.family_sharpes("OPENING_RANGE")
    assert len(annual) == 40 and max(abs(x) for x in annual) > 2.0          # annualised: well above 1
    strong = [rng.gauss(6.0, 10.0) for _ in range(60)]
    got = ST.dsr_of(strong, 40, annual)
    per_day = deflated_sharpe_ratio(strong, 40, [x / math.sqrt(252.0) for x in annual])["dsr"]
    assert got == pytest.approx(per_day) and got > 0.3
    wrong = deflated_sharpe_ratio(strong, 40, annual)["dsr"]                # what it used to do
    assert wrong == 0.0
    # the rank DSR gives is a real rank: a stronger candidate scores higher than a weaker one
    weak = [rng.gauss(1.0, 10.0) for _ in range(60)]
    assert ST.dsr_of(strong, 40, annual) > ST.dsr_of(weak, 40, annual)
    led.close()


def test_a_regrade_from_the_ledger_judges_exactly_what_the_first_verdict_did(tmp_path):
    """assumed_share was not in the ledger, so a re-graded row forgot that 40% of its trades had to
    assume a spread, and a candidate that failed Costs passed."""
    from mintel.lab.ledger import LabLedger
    led = LabLedger(tmp_path / "lab.sqlite", tmp_path / "trials.sqlite")
    c = Candidate("OPENING_RANGE", 1, "FX_MAJOR", "LONDON", "IGNORE", "t1", make_params(k=1))
    led.register(c, "x")
    res = steady_result(c.id, n_days=40, assumed=0.4)
    s_ = ST.summarize(res)
    first = ST.past_verdict(s_, "na")
    assert not first.passed and first.reasons == ["COSTS"] and "assume a spread" in first.check(CHECK_COSTS).text
    tid = led.begin_test(c.id, "past", "F-2026-10", "2026-09-01")
    led.finish_test(tid, s_.to_cols(), res.daily, reasons=first.reasons, skipped={}, synthetic=True,
                    seconds=0.1, trial=s_.trial_extra())
    back = ST.TestStats.from_row(led.test_row(tid), led.daily_of(tid))
    assert back.assumed_share == pytest.approx(s_.assumed_share) and back.assumed_share > 0.3
    again = ST.past_verdict(back, "na")
    assert (again.passed, again.reasons, again.first_failed) == (first.passed, first.reasons, first.first_failed)
    assert again.check(CHECK_COSTS).text == first.check(CHECK_COSTS).text
    led.close()


def test_a_plateau_spike_fails_steady_and_extend_holds_it_back_without_failing_it():
    spike = ST.past_verdict(stats_of(), "spike")
    assert not spike.passed and "spike" in spike.check(CHECK_STEADY).text
    ext = ST.past_verdict(stats_of(), "extend")
    assert ext.passed and not ext.forward_ok and "EXTEND_GRID" in ext.reasons
    na = ST.past_verdict(stats_of(), "na")
    assert na.passed and na.forward_ok and "fresh test decides" in na.check(CHECK_STEADY).text
    blocked = ST.past_verdict(stats_of(), "ok", pbo_ok=False)
    assert blocked.passed and not blocked.forward_ok and "FAMILY_FITS_NOISE" in blocked.reasons


def test_an_unknown_plateau_is_not_a_pass_to_the_fresh_look():
    """With a neighbour's result missing the candidate was waved through as if it had no setting to
    nudge, and the page said so. It now passes its own checks but waits: not forwarded, with its
    reason, and the sentence says what it waits for."""
    v = ST.past_verdict(stats_of(), None)
    assert v.passed and not v.forward_ok
    assert "PLATEAU_PENDING" in v.reasons and lab.REASON_PLATEAU_PENDING == "PLATEAU_PENDING"
    text = v.check(CHECK_STEADY).text
    assert "no setting to nudge" not in text and "not all been tested" in text
    assert v.stats.plateau is None
    # a failure is still a failure, and not also "pending"
    bad = ST.past_verdict(stats_of(net_gbp=-5.0, cost15_net=-9.0), None)
    assert not bad.passed and "PLATEAU_PENDING" not in bad.reasons
    # once the neighbours exist it is judged as before
    assert ST.past_verdict(stats_of(), "ok").forward_ok


# ---------------------------------------------------------------- plateau ---
def nets_for(cands, values):
    return {c.id: v for c, v in zip(cands, values)}


def test_plateau_with_three_values_uses_parameter_stability_at_half_tolerance():
    fam = FAM.get("RANGE_BREAK")
    me = fam.grid()[0]
    sibs = fam.siblings(me, "lookback_min")
    assert [s.p("lookback_min") for s in sibs] == [60, 120, 240]
    assert ST.plateau_state(me, nets_for(sibs, [80.0, 100.0, 90.0])) == ("ok", [])             # a plateau
    assert ST.plateau_state(me, nets_for(sibs, [10.0, 100.0, 12.0]))[0] == "spike"            # a spike
    assert ST.plateau_state(me, nets_for(sibs, [60.0, 100.0, 51.0]))[0] == "ok"               # within 50%
    assert ST.plateau_state(me, nets_for(sibs, [60.0, 100.0, 49.0]))[0] == "spike"            # just outside it
    state, extend = ST.plateau_state(me, nets_for(sibs, [20.0, 50.0, 100.0]))                 # best at the edge, still improving
    assert state == "extend" and extend == ["lookback_min"]
    assert ST.plateau_state(me, {sibs[0].id: 1.0}) == (None, [])                             # a sibling is missing: not yet known


def test_the_neighbours_a_plateau_is_waiting_for_can_be_named():
    fam = FAM.get("RANGE_BREAK")
    me = fam.grid()[0]
    sibs = fam.siblings(me, "lookback_min")
    assert {m.id for m in ST.missing_siblings(me, {})} == {s.id for s in sibs}
    assert [m.id for m in ST.missing_siblings(me, {sibs[1].id: 5.0})] == [sibs[0].id, sibs[2].id]
    assert ST.missing_siblings(me, nets_for(sibs, [1.0, 2.0, 3.0])) == []
    assert ST.missing_siblings(FAM.get("PREV_DAY_LEVELS").grid()[0], {}) == []          # nothing to nudge
    two = FAM.get("VWAP_FADE").grid()[0]                                                # two settings are nudged
    assert len(ST.missing_siblings(two, {})) == 3 + 2 - 1                              # the candidate itself is on both axes


def test_plateau_with_two_values_needs_the_other_one_to_be_positive_too():
    fam = FAM.get("OPENING_RANGE")
    me = fam.grid()[0]
    sibs = fam.siblings(me, "range_min")
    assert [s.p("range_min") for s in sibs] == [15, 30]
    assert ST.plateau_state(me, nets_for(sibs, [50.0, 30.0])) == ("ok", [])
    assert ST.plateau_state(me, nets_for(sibs, [50.0, -1.0]))[0] == "spike"
    assert ST.plateau_state(me, nets_for(sibs, [50.0, 0.0]))[0] == "spike"                    # must be above zero


def test_plateau_across_several_settings_takes_the_worst():
    fam = FAM.get("VWAP_FADE")
    me = fam.grid()[0]
    nets = {}
    for s, v in zip(fam.siblings(me, "sigma"), (48.0, 52.0, 50.0)):
        nets[s.id] = v
    for s in fam.siblings(me, "stop_atr"):
        nets[s.id] = 49.0
    assert ST.plateau_state(me, nets) == ("ok", [])
    other = [s for s in fam.siblings(me, "stop_atr") if s.id != me.id][0]
    nets[other.id] = -5.0                                           # the other stop size loses: a spike
    assert ST.plateau_state(me, nets)[0] == "spike"


def test_families_with_nothing_to_nudge_say_so():
    for code in ("PREV_DAY_LEVELS", "CONTROL_TIME_OF_DAY"):
        c = FAM.get(code).grid()[0]
        assert ST.plateau_state(c, {}) == ("na", [])


# ------------------------------------------------------------ family gates ---
def test_pbo_flags_a_family_that_is_fitting_noise():
    """Members with no edge at all: whichever looks best in-sample is as likely to be below the
    median out of sample as above it, so PBO sits near one half (the 15 splits overlap, so any one
    family wobbles; the average does not) and the 0.35 gate closes on most of them."""
    rng = random.Random(4)
    days = day_list(60)
    ids = [f"lab-{i:010x}" for i in range(12)]
    pbos, blocked = [], 0
    for _ in range(40):
        daily = {c: {d: rng.gauss(0, 10) for d in days} for c in ids}
        g = ST.family_gate(daily, {c: 80 for c in ids}, days=days)
        assert g["members"] == 12 and g["paths"] == 15 and g["pbo"] is not None
        pbos.append(g["pbo"])
        blocked += g["blocks"]
        assert g["blocks"] is (g["pbo"] > 0.35)
    assert 0.40 <= sum(pbos) / len(pbos) <= 0.60
    assert blocked / 40 >= 0.55


def test_pbo_passes_a_family_with_a_real_best_member():
    rng = random.Random(8)
    days = day_list(60)
    ids = [f"lab-{i:010x}" for i in range(12)]
    blocked = 0
    for _ in range(20):
        daily = {c: {d: rng.gauss(0, 2) + (6.0 if i == 0 else 0.0) for d in days} for i, c in enumerate(ids)}
        g = ST.family_gate(daily, {c: 80 for c in ids}, days=days)
        blocked += g["blocks"]
        assert g["pbo"] <= 0.35
    assert blocked == 0


def test_pbo_leave_one_path_out_on_overlapping_splits_would_be_blind_to_noise():
    """Why family_gate does its own in-sample/out-of-sample: the library's leave-one-path-out
    function, fed CPCV's overlapping test sets, calls pure noise 'robust'."""
    from mintel.research.validation import cpcv_splits, probability_of_backtest_overfitting
    rng = random.Random(12)
    n_days, members = 60, 12
    seen = []
    for _ in range(40):
        mat = [[rng.gauss(0, 10) for _ in range(n_days)] for _ in range(members)]
        paths = [[sum(row[i] for i in test) / len(test) for row in mat]
                 for _train, test in cpcv_splits(n_days, groups=6, test_groups=2)]
        seen.append(probability_of_backtest_overfitting(paths)["pbo"])
    assert sum(seen) / len(seen) < 0.2


def test_pbo_is_not_available_with_too_few_members_or_trades():
    days = day_list(60)
    daily = {f"lab-{i:010x}": {d: 1.0 for d in days} for i in range(7)}
    g = ST.family_gate(daily, {c: 80 for c in daily}, days=days)
    assert g["pbo"] is None and g["blocks"] is False and g["members"] == 7 and g["note"].startswith("n/a")
    daily8 = {f"lab-{i:010x}": {d: float(i) for d in days} for i in range(10)}
    g = ST.family_gate(daily8, {c: (80 if i < 7 else 5) for i, c in enumerate(daily8)}, days=days)
    assert g["members"] == 7 and g["pbo"] is None                      # three have under 20 trades
    short = {f"lab-{i:010x}": {d: float(i) for d in days[:8]} for i in range(10)}
    assert ST.family_gate(short, {c: 80 for c in short}, days=days[:8])["pbo"] is None


# ------------------------------------------------------------ cost stress ---
def test_cost_stress_reprices_every_trade_from_its_stored_cost():
    days = day_list(5)
    res = result("lab-0000000001", days, {d: [10.0, -4.0] for d in days}, cost=2.0)
    nets = ST.cost_nets(res.trades)
    net = sum(t.net_gbp for t in res.trades)
    cost = sum(t.cost_gbp for t in res.trades)
    assert nets[1.0] == pytest.approx(net) and nets[1.5] == pytest.approx(net - 0.5 * cost, abs=0.01)
    assert nets[2.0] == pytest.approx(net - cost, abs=0.01) and nets[3.0] == pytest.approx(net - 2 * cost, abs=0.01)
    s = ST.summarize(res)
    assert s.cost15_net == pytest.approx(nets[1.5]) and s.cost20_net == pytest.approx(nets[2.0])


# -------------------------------------------------------------- summaries ---
def test_summarize_counts_days_trades_and_the_profit_factor():
    days = day_list(8)
    per = {days[0]: [10.0, -2.0], days[1]: [-5.0], days[2]: [6.0], days[3]: [], days[4]: [3.0, 3.0],
           days[5]: [-1.0], days[6]: [], days[7]: [4.0]}
    res = result("lab-0000000002", days, per)
    s = ST.summarize(res)
    assert s.trades == 8 and s.days == 8 and s.days_traded == 6
    assert s.net_gbp == pytest.approx(10 - 2 - 5 + 6 + 6 - 1 + 4)
    assert s.up_days == 4 and s.down_days == 2                       # days with no trade are neither
    assert s.pf == pytest.approx((10 + 6 + 3 + 3 + 4) / (2 + 5 + 1))
    assert s.win_rate == pytest.approx(5 / 8)
    assert s.best_day_gbp == pytest.approx(8.0)
    assert s.window_from == days[0] and s.window_to == days[-1]
    assert s.max_dd_r > 0 and s.synthetic is True
    ch = ST.chunk_nets([res.daily[d][0] for d in days])
    assert s.folds_up == sum(1 for x in ch if x > 0) and s.folds_worst == min(ch)


def test_a_profit_factor_with_no_losses_is_capped_and_with_no_trades_is_zero():
    assert ST.profit_factor([1.0, 2.0]) == ST.PF_CAP and ST.profit_factor([]) == 0.0
    assert ST.profit_factor([-1.0]) == 0.0


def test_the_test_row_columns_round_trip_through_the_ledger(tmp_path):
    from mintel.lab.ledger import LabLedger
    led = LabLedger(tmp_path / "lab.sqlite", tmp_path / "trials.sqlite")
    c = Candidate("OPENING_RANGE", 1, "FX_MAJOR", "LONDON", "IGNORE", "t1", make_params(k=1))
    led.register(c, "x")
    res = steady_result(c.id, n_days=30)
    s = ST.summarize(res)
    v = ST.past_verdict(s, "ok")
    tid = led.begin_test(c.id, "past", "F-2026-10", "2026-09-01")
    led.finish_test(tid, s.to_cols(), res.daily, reasons=v.reasons, skipped=res.skipped,
                    synthetic=res.synthetic, seconds=0.1, trial=s.trial_extra())
    row = led.test_row(tid)
    back = ST.TestStats.from_row(row, led.daily_of(tid))
    for f in ("trades", "days", "net_gbp", "pf", "up_days", "down_days", "p", "folds_up", "cost15_net"):
        assert getattr(back, f) == pytest.approx(getattr(s, f)), f
    assert back.days_traded == s.days_traded and back.plateau == "ok"
    assert ST.past_verdict(back, "ok").passed == v.passed
    led.close()


# ------------------------------------------------------------- fresh/practice ---
def fresh_stats(**kw):
    base = dict(kind="fresh", days=25, trades=60, net_gbp=120.0, pf=1.5, up_days=15, down_days=8,
                p=0.002, cost15_net=100.0)
    base.update(kw)
    return ST.TestStats(**base)


def test_the_fresh_look_passes_only_when_every_check_does():
    v = ST.fresh_verdict(fresh_stats())
    assert v.passed and v.outcome == "PASS" and v.reasons == []
    assert all(c.state == PASSED for c in v.checks)
    assert "1 in 7" in v.check(CHECK_LUCK).text or "1 in " in v.check(CHECK_LUCK).text
    lost = [dict(p=FRESH_P + 1e-6), dict(net_gbp=-1.0), dict(pf=1.09), dict(up_days=8, down_days=8),
            dict(cost15_net=0.0)]
    for kw in lost:
        v = ST.fresh_verdict(fresh_stats(**kw))
        assert not v.passed and v.outcome == "LOST" and v.reasons == ["LOST"], kw
    on_the_line = ST.fresh_verdict(fresh_stats(p=FRESH_P, pf=1.1, up_days=9, down_days=8))
    assert on_the_line.passed


def test_too_few_fresh_trades_is_its_own_outcome_with_one_retry_and_judges_nothing_else():
    v = ST.fresh_verdict(fresh_stats(trades=29, p=0.0001, net_gbp=900.0))
    assert not v.passed and v.outcome == "TOO_FEW_TRADES" and v.reasons == ["TOO_FEW_TRADES"]
    assert v.check(CHECK_TRADES).state == FAILED
    assert {v.check(k).state for k in (CHECK_STEADY, CHECK_FRESH, CHECK_COSTS, CHECK_LUCK)} == {NOT_YET}
    assert "one more go" in v.check(CHECK_TRADES).text
    assert ST.fresh_verdict(fresh_stats(trades=30)).passed


def test_a_fresh_window_shorter_than_the_stretch_rule_allows_is_not_judged():
    """p is a bootstrap over days; on 3 days of 40 trades a lucky pass is not rare. The cut rule
    gives every stretch at least 20 fresh days, so this only bites if days go missing, but nothing
    else would stop it."""
    short = ST.fresh_verdict(fresh_stats(days=19, trades=60, p=0.0001))
    assert not short.passed and short.outcome == lab.FRESH_TOO_FEW and short.reasons == [lab.FRESH_TOO_FEW]
    assert short.check(CHECK_TRADES).state == FAILED and "19 days" in short.check(CHECK_TRADES).text
    assert all(short.check(k).state == NOT_YET for k in (CHECK_STEADY, CHECK_FRESH, CHECK_COSTS, CHECK_LUCK))
    assert ST.fresh_verdict(fresh_stats(days=20)).passed
    assert ST.fresh_verdict(fresh_stats(days=3, trades=40, p=0.004)).outcome == lab.FRESH_TOO_FEW


def test_a_practice_look_and_the_running_pills():
    s = ST.TestStats(kind="practice", days=12, trades=34, net_gbp=71.0, pf=1.4, up_days=8, down_days=4,
                     p=0.03, cost15_net=40.0)
    v = ST.practice_verdict(s, at_look=True)
    assert v.passed and v.check(CHECK_FRESH).state == PASSED
    for kw in (dict(trades=29), dict(days=9), dict(net_gbp=-1.0), dict(pf=1.14), dict(p=0.0501),
               dict(up_days=4, down_days=4), dict(cost15_net=-1.0)):
        s2 = ST.TestStats(**{**s.__dict__, **kw})
        v = ST.practice_verdict(s2, at_look=True)
        assert not v.passed and v.first_failed, kw
    # in between looks nothing is judged: Running (n of m)
    mid = ST.TestStats(kind="practice", days=4, trades=18, net_gbp=20.0, pf=1.3, up_days=3, down_days=1, p=0.2)
    v = ST.practice_verdict(mid, at_look=False)
    assert not v.passed and v.stats.passed is None
    assert v.check(CHECK_TRADES).state == RUNNING and v.check(CHECK_TRADES).n == 18 and v.check(CHECK_TRADES).of == 30
    assert {v.check(k).state for k in (CHECK_STEADY, CHECK_COSTS, CHECK_LUCK)} == {RUNNING}
    assert "18 of 30" in v.check(CHECK_TRADES).text
    rare = ST.practice_verdict(ST.TestStats(kind="practice", days=20, trades=12), at_look=True)
    assert "trades too rarely" in rare.check(CHECK_TRADES).text


# ------------------------------------------------------------ control rates ---
def test_pass_rates_for_the_control_family():
    good = ST.past_verdict(stats_of(), "na")
    bad = ST.past_verdict(stats_of(trades=10), "na")
    rates = ST.pass_rates({"a": good, "b": bad, "c": bad, "d": bad})
    assert rates["n"] == 4 and rates["past"] == 1 and rates["past_pass_rate"] == 0.25
    assert rates["trades"] == 1 and ST.pass_rates({})["n"] == 0


# ------------------------------------------------------------- planted tests ---
def planted(cid, days, rng, tpd, mean_r, win=1.0):
    """Trades with a true edge of ``mean_r`` R each: +win R with the right probability, else -1 R."""
    pw = (mean_r + 1.0) / (win + 1.0)
    nets = {}
    for d in days:
        k = sum(1 for _ in range(int(tpd * 3)) if rng.random() < 1 / 3)
        nets[d] = [(win if rng.random() < pw else -1.0) * RISK for _ in range(k)]
    return result(cid, days, nets)


def cid_of(rng):
    return "lab-%010x" % rng.getrandbits(40)


def test_a_family_with_no_edge_fails_the_fresh_gate_at_about_the_expected_rate():
    rng = random.Random(2026)
    days = day_list(25, "2026-08-17")
    n, passed, p05 = 900, 0, 0
    for _ in range(n):
        r = planted(cid_of(rng), days, rng, 2.0, 0.0)
        r.window = WindowInfo("fresh", days[0], days[-1], len(days))
        s = ST.summarize(r)
        v = ST.fresh_verdict(s)
        passed += v.passed
        p05 += s.p <= 0.05
    assert passed / n <= 0.02                        # nominally 0.5%; measured about 1% on 25 days
    assert 0.035 <= p05 / n <= 0.09
    # at most 30 looks a stretch: the expected number of lucky passes per stretch stays small
    assert 30 * (passed / n) <= 0.6


def test_past_gates_pass_some_noise_which_is_why_the_fresh_look_exists():
    rng = random.Random(77)
    days = day_list(54)
    n, past = 400, 0
    for _ in range(n):
        v = ST.past_verdict(ST.summarize(planted(cid_of(rng), days, rng, 2.0, 0.0)), "na")
        past += v.passed
    assert 0.02 <= past / n <= 0.30                  # the past test lets a minority of noise through


def test_a_planted_edge_of_point_three_r_passes_the_past_checks_and_usually_the_fresh_test():
    rng = random.Random(303)
    past_days = day_list(54)
    fresh_days = day_list(40, "2026-08-17")
    n, past_ok, fresh_ok, both = 120, 0, 0, 0
    for _ in range(n):
        cid = cid_of(rng)
        pv = ST.past_verdict(ST.summarize(planted(cid, past_days, rng, 3.0, 0.3)), "na")
        fr = planted(cid, fresh_days, rng, 3.0, 0.3)
        fr.window = WindowInfo("fresh", fresh_days[0], fresh_days[-1], len(fresh_days))
        fv = ST.fresh_verdict(ST.summarize(fr))
        past_ok += pv.passed
        fresh_ok += fv.passed
        both += pv.passed and fv.passed
    assert past_ok / n >= 0.75                       # passes the past checks (nearly always)
    assert fresh_ok / n >= 0.5                       # and usually the fresh test (~120 fresh trades)
    # the price of the 0.005 line: with only ~30 fresh trades the same edge usually does NOT pass
    rng = random.Random(5)
    few = 0
    for _ in range(100):
        r = planted(cid_of(rng), fresh_days[:12], rng, 2.5, 0.3)
        r.window = WindowInfo("fresh", "", "", 12)
        few += ST.fresh_verdict(ST.summarize(r)).passed
    assert few <= 35


def test_a_bigger_edge_is_found_faster():
    rng = random.Random(9)
    days = day_list(25, "2026-08-17")
    res = []
    for mean_r in (0.1, 0.3, 0.6):
        k = 0
        for _ in range(80):
            r = planted(cid_of(rng), days, rng, 3.0, mean_r)
            r.window = WindowInfo("fresh")
            k += ST.fresh_verdict(ST.summarize(r)).passed
        res.append(k)
    assert res[0] < res[1] < res[2]


# ------------------------------------------------------ a real run, end to end ---
def test_the_control_family_on_made_up_prices_is_not_let_through(tmp_path):
    """Candidates with no idea behind them, on random prices, through the real simulator and
    the real checks: the gates must not wave them through."""
    from mintel.lab import LabPaths, sim
    from mintel.lab.store import BarStore
    store = BarStore(LabPaths(tmp_path))
    ents = synth.universe(24)
    days = synth.weekdays("2026-06-01", 40)
    synth.fill_store(store, ents, days, seed=17)
    win = store._build_window("past", store.trading_days([e.name for e in ents]), ents,
                              cut_day="2099-01-01", stretch_id="x")
    fam = FAM.get("CONTROL_TIME_OF_DAY")
    verdicts, traded = {}, 0
    for c in fam.grid():
        res = sim.run(c, win)
        traded += len(res.trades)
        verdicts[c.id] = ST.past_verdict(ST.summarize(res, n_trials=32), "na")
    rates = ST.pass_rates(verdicts)
    assert traded > 500 and rates["n"] == 32
    assert rates["past"] <= 2                        # the clock trades are expected to fail


@pytest.mark.slow
def test_a_planted_edge_is_found_through_the_whole_stack_and_no_edge_is_not(tmp_path):
    """Made-up prices where, after the London opening range, the break carries on for 90 minutes
    (a real edge), through the real store, features, simulator and checks. The London opening-range
    candidates that stop at the far side of the range must pass the past checks and the fresh look;
    the New York ones, on the same prices, have nothing to find and must not."""
    from mintel.lab import LabPaths, sim
    from mintel.lab.store import BarStore
    store = BarStore(LabPaths(tmp_path))
    ents = synth.universe(7, kinds=("FX_MAJOR",))
    synth.fill_store(store, ents, synth.weekdays("2026-04-01", 80), seed=5, or_drift=0.1)
    days = store.trading_days([e.name for e in ents])
    cut = days[-25]
    past = store._build_window("past", [d for d in days if d < cut], ents, cut_day=cut, stretch_id="s")
    fresh = store._build_window("fresh", [d for d in days if d >= cut], ents, cut_day=cut, stretch_id="s")
    fam = FAM.get("OPENING_RANGE")
    london, new_york = [], []
    for c in fam.grid():
        if c.markets != "FX_MAJOR" or c.news != "IGNORE" or c.p("stop") != "RANGE":
            continue
        s = ST.summarize(sim.run(c, past), n_trials=144)
        v = ST.past_verdict(s, "na")
        f = ST.fresh_verdict(ST.summarize(sim.run(c, fresh), kind="fresh"))
        (london if c.session == "LONDON" else new_york).append((v.passed, f.passed, s, f))
    assert len(london) == 6 and len(new_york) == 6
    assert all(p for p, _f, _s, _fv in london)                       # the planted edge passes the past checks ...
    assert sum(f for _p, f, _s, _fv in london) >= 5                  # ... and, with 80+ fresh trades, the fresh look
    assert all(s.expectancy_r > 0.2 for _p, _f, s, _fv in london)
    assert not any(p for p, _f, _s, _fv in new_york)                 # no edge there: nothing passes
    assert not any(f for _p, f, _s, _fv in new_york)
