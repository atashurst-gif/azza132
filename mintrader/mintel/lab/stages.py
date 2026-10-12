"""Stages and moves: the rule's half of the ladder (spec section 6).

    IDEA - PAST TEST - FRESH TEST - ON THE BENCH - PRACTICE - READY FOR LIVE
         - LIVE TRIAL - PROVEN - BINNED

Everything up to READY FOR LIVE, and every BIN, is decided here by the rule;
anything to do with real money or sizes is Aaron's and is never decided here.

    IDEA -> PAST TEST        every registered idea not yet tried, and every one
                             due a retest, is tried that night (binned ones never)
    PAST TEST -> BINNED      any past check failed; the reason is the first one
    PAST TEST -> FRESH TEST  every past check passed, the family is not fitting
                             noise, the settings are not at the edge of the range;
                             ranked by the deflated Sharpe then p; at most 3 a night
                             and 30 a stretch
    FRESH TEST -> BENCH      every fresh check passed
    FRESH TEST -> BINNED     lost (final), or too few trades (one more go on a
                             later stretch, then binned for good)
    BENCH -> PRACTICE        a place is free (6), at most 2 of one family, in the
                             order of the PAST test's deflated Sharpe: a fresh
                             result is NEVER a ranking input
    BENCH -> BINNED          60 days without a place
    PRACTICE -> READY        look 1 (the 10th UK trading day of practice, if it
                             has 30 trades; else the first day it has, up to day
                             20) or look 2 (day 20): every practice check passed.
                             The look days are worked out when it starts and kept
    PRACTICE -> BINNED       look 1 net <= 0; look 2 failed; day 20 with fewer
                             than 30 trades ("trades too rarely"); a practice
                             drawdown over 15 R (GBP 195) at any nightly check

This module and ``store.py`` are the only places that may ask for the fresh
prices (a source-grep test keeps it so): ``fresh_looks`` below is the one caller.

Aaron, 10 Oct 2026: "ensure that consistency stays, luck doesn't".
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import json
from typing import Callable, Optional, Sequence

from ..clock import TZ_LONDON
from . import (BINNED, CHECK_LABELS, COST_STRESS_PASS, COST_STRESS_SHOWN, FRESH_LOOKS_PER_STRETCH,
               FRESH_MIN_TRADES, FRESH_TEST, FRESH_TOO_FEW, HOLD_BACK_REASONS, IDEA, KIND_PAST,
               ON_THE_BENCH, PAST_TEST, PBO_MAX, PRACTICE, PRACTICE_KILL_R, PRACTICE_MIN_TRADES,
               READY_FOR_LIVE, RISK_GBP, STRETCH_GAP_RETEST, UTC, Candidate, add_days,
               clamp_fresh_cap, is_weekday)
from . import sim, stats
from .candidate import family_of, name_of, registered, waiting_for_calendar
from .cut import GridChanged, check_grid
from .families import CONTROL, all_families
from .ledger import FreshRefused
from .store import fresh_once

PRACTISING = (PRACTICE, READY_FOR_LIVE)     # the stages that hold a practice place
MAX_PRACTICE_SYMBOLS = 12                   # distinct markets across the whole practice roster
MAX_PER_FAMILY = 2                          # practising at once from one family
BENCH_MAX_DAYS = 60                         # on the bench this long without a place: binned
LOOK1_DAY, LOOK2_DAY = 10, 20               # UK trading days of practice
CRASHES_BEFORE_BIN = 2                      # a past test that crashes this often in a stretch

Log = Callable[[str], None]


def _quiet(_msg: str) -> None:
    pass


# ------------------------------------------------------------------ helpers --
def cand_of(row) -> Candidate:
    """The frozen definition kept in ``candidates.def_json``, rebuilt; its id is
    checked against the row's so a damaged definition is never run."""
    d = json.loads(row["def_json"])
    c = Candidate(family=d["family"], version=int(d["version"]), markets=d["markets"],
                  session=d["session"], news=d["news"], exit=d["exit"],
                  params=tuple(tuple(p) for p in d.get("params") or ()),
                  risk_gbp=float(d.get("risk_gbp", RISK_GBP)))
    if c.id != row["id"]:
        raise ValueError(f"{row['id']}: its stored definition hashes to {c.id}")
    return c


def gbp(x: float, signed: bool = True) -> str:
    sign = ("+" if x > 0 else "-" if x < 0 else "") if signed else ("-" if x < 0 else "")
    return f"{sign}£{abs(x):,.0f}"


def night_of(now: dt.datetime) -> str:
    """The UK date of the night a run belongs to: 23:10 Monday and 01:30 Tuesday
    are both Monday's night (before noon counts as the night before)."""
    local = now.astimezone(TZ_LONDON)
    d = local.date() if local.hour >= 12 else local.date() - dt.timedelta(days=1)
    return d.isoformat()


def prev_weekday(day: str) -> str:
    """``day`` itself when it is a weekday, else the Friday before it."""
    while not is_weekday(day):
        day = add_days(day, -1)
    return day


def judge_day(now: dt.datetime) -> str:
    """The last UK trading day whose practice is over when the nightly runs."""
    return prev_weekday(night_of(now))


def nth_weekday_after(day: str, n: int) -> str:
    """The n-th weekday after ``day`` (n >= 1)."""
    out, k = day, 0
    while k < n:
        out = add_days(out, 1)
        if is_weekday(out):
            k += 1
    return out


def weekdays_between(first: str, last: str) -> list:
    """Every weekday from ``first`` to ``last``, both included."""
    out, d = [], first
    while d <= last:
        if is_weekday(d):
            out.append(d)
        d = add_days(d, 1)
    return out


def practice_start(since_utc: str) -> str:
    """Day 1 of practice: the first weekday after the night it was given its place."""
    t = dt.datetime.fromisoformat(since_utc)
    if t.tzinfo is None:
        t = t.replace(tzinfo=UTC)
    return nth_weekday_after(night_of(t), 1)


def look_days(since_utc: str) -> tuple:
    """(look 1 due, look 2 due): the 10th and 20th UK trading days of practice."""
    d1 = practice_start(since_utc)
    return nth_weekday_after(add_days(d1, -1), LOOK1_DAY), nth_weekday_after(add_days(d1, -1), LOOK2_DAY)


def _reasons(row) -> list:
    try:
        out = json.loads(row["reasons_json"] or "[]")
        return out if isinstance(out, list) else []
    except (TypeError, ValueError):
        return []


def _family_order() -> dict:
    return {f.code: k for k, f in enumerate(all_families(True))}


def bin_words(kind: str, verdict) -> str:
    """The plain bin reason kept in candidates.bin_reason."""
    label = CHECK_LABELS.get(verdict.first_failed, "")
    head = {"past": "Failed the past test", "fresh": "Failed the fresh test",
            "practice": "Failed its practice look"}[kind]
    return f"{head} ({label}): {verdict.why}" if label else f"{head}: {verdict.why}"


# ----------------------------------------------------- step 6: generate ---
@dataclasses.dataclass
class Generated:
    registered: int = 0                # new ideas added at stage IDEA tonight
    begun: list = dataclasses.field(default_factory=list)      # test ids committed (running)
    waiting: list = dataclasses.field(default_factory=list)    # ideas held back for the calendar
    grid_note: str = ""                # why new ideas wait for the next stretch, if they do
    due: int = 0                       # ideas due a past test this stretch (before any limit)


def due_past(ledger, stretch, *, covered_past_days: int, family: Optional[str] = None) -> tuple:
    """(due, waiting): the candidates owed a past test on this stretch that do not
    have a finished one yet, in family then grid order, and the AVOID_15M ideas
    held back until the calendar covers enough past days. BINNED ideas are never
    due (the only ways back are the one retry and Claude's reopen)."""
    grid = registered(bool(stretch.news_awake))
    order = {c.id: k for k, c in enumerate(grid)}
    fam_order = _family_order()
    due, waiting = [], []
    for row in ledger.candidates(family=family):
        stage = row["stage"]
        if stage == BINNED:
            continue
        past = ledger.past_row(row["id"], stretch.stretch_id)
        if past is not None and past["status"] == "done":
            continue
        owed = stage == IDEA or bool(row["retest_due"]) or (stage == PAST_TEST and past is not None)
        if not owed:
            continue
        try:
            c = cand_of(row)
            fam = family_of(c)
        except (KeyError, ValueError, TypeError):
            continue
        if fam.dormant and not stretch.news_awake:
            continue
        if waiting_for_calendar(c, covered_past_days):
            waiting.append(row["id"])
            continue
        due.append(row)
    due.sort(key=lambda r: (fam_order.get(r["family"], 99), order.get(r["id"], 10 ** 9), r["id"]))
    return due, waiting


def generate(ledger, stretch, *, covered_past_days: int, now: dt.datetime,
             limit: Optional[int] = None, log: Log = _quiet) -> Generated:
    """Nightly step 6: the registered grids become definitions; ids already known
    (binned ones included) are skipped; the past tests due tonight get their
    ``running`` rows committed BEFORE anything is simulated, so N is known first."""
    out = Generated()
    grid = registered(bool(stretch.news_awake))
    try:
        check_grid(stretch, [c.id for c in grid])
        grid_ok = True
    except GridChanged as exc:
        grid_ok = False
        out.grid_note = f"new ideas wait for the next stretch: {exc}"
        log(out.grid_note)
    if grid_ok:
        known = ledger.known_ids()
        for c in grid:
            if c.id not in known and ledger.register(c, name_of(c), now=now):
                out.registered += 1
    due, out.waiting = due_past(ledger, stretch, covered_past_days=covered_past_days)
    out.due = len(due)
    if limit is not None:
        due = due[:max(0, int(limit))]
    for row in due:
        out.begun.append(ledger.begin_test(row["id"], KIND_PAST, stretch.stretch_id, stretch.cut_day,
                                           now=now))
    return out


# --------------------------------------------------------- step 7: past ---
def pending_past(ledger, stretch) -> list:
    """The committed past tests of this stretch not finished yet (running, or
    crashed and to be revived), in family then grid order: (test id, candidate row).
    A crashed row of a BINNED idea is left alone: it was binned for crashing (or is a
    neighbour that ``family_end`` tests again itself when it needs it), and running it
    every night would only crash it again."""
    grid = registered(True)
    order = {c.id: k for k, c in enumerate(grid)}
    fam_order = _family_order()
    rows = ledger.query(
        "SELECT t.id AS test_id, c.* FROM tests t JOIN candidates c ON c.id=t.candidate_id"
        " WHERE t.kind='past' AND t.stretch_id=? AND t.status IN ('running','crashed')"
        " AND NOT (t.status='crashed' AND c.stage=?)",
        (stretch.stretch_id, BINNED))
    rows = [dict(r) for r in rows]
    rows.sort(key=lambda r: (fam_order.get(r["family"], 99), order.get(r["id"], 10 ** 9), r["id"]))
    return [(r.pop("test_id"), r) for r in rows]


def run_past(ledger, row: dict, test_id: int, window, stretch, *, now: dt.datetime,
             log: Log = _quiet):
    """One past test, its ``running`` row already committed. The idea moves IDEA ->
    PAST TEST as it starts; a failed check bins it (the first failed check is the
    reason). A row for an idea past this stage (a neighbour tested only so its
    sibling's plateau can be judged) moves nothing. Returns the Verdict, or None
    when the run crashed (the row stays and still counts)."""
    cid = row["id"]
    stage = row["stage"]
    test_id = ledger.begin_test(cid, KIND_PAST, stretch.stretch_id, stretch.cut_day, now=now)
    if stage == IDEA:
        ledger.move(cid, PAST_TEST, f"tried on past prices (stretch {stretch.stretch_id})", now=now)
        stage = PAST_TEST
    try:
        c = cand_of(row)
        res = sim.run(c, window)
        sharpes = ledger.family_sharpes(c.family)
        s = stats.summarize(res, n_trials=ledger.n_tests(kind=KIND_PAST, family=c.family),
                            trial_sharpes=sharpes or None, kind="past")
        v = stats.past_verdict(s, None)          # the plateau is judged when the family is complete
    except Exception as exc:                      # a fault in the lab, not a verdict on the idea
        ledger.crash_test(test_id, f"{type(exc).__name__}: {exc}", now=now)
        ledger.add_event(cid, "crash", stage, stage, f"its past test crashed: {exc}", now=now)
        log(f"past: {cid} crashed: {type(exc).__name__}: {exc}")
        crashes = ledger.one("SELECT COUNT(*) FROM events WHERE candidate_id=? AND kind='crash'"
                             " AND why LIKE 'its past test crashed%' AND ts_utc>=?",
                             (cid, stretch.set_utc))[0]
        if crashes >= CRASHES_BEFORE_BIN and stage == PAST_TEST:
            ledger.move(cid, BINNED, "its past test crashed twice on this stretch",
                        bin_reason="Its past test crashed twice (a fault in the lab, not a verdict on "
                                   "the idea; Claude can reopen it)", now=now)
        return None
    ledger.finish_test(test_id, s.to_cols(), res.daily, reasons=v.reasons, skipped=res.skipped,
                       synthetic=res.synthetic, seconds=res.seconds, trial=s.trial_extra(), now=now)
    if row.get("retest_due"):
        ledger.clear_retest_due(cid)
    if not v.passed and stage == PAST_TEST:
        ledger.move(cid, BINNED, bin_words("past", v), bin_reason=bin_words("past", v), now=now)
    return v


def family_complete(ledger, family: str, stretch, *, covered_past_days: int) -> bool:
    """A family's grid is complete for the stretch when nothing of it is still owed
    a past test and at least one has been tried."""
    due, _waiting = due_past(ledger, stretch, covered_past_days=covered_past_days, family=family)
    if due:
        return False
    # a crashed row of a binned idea is never run again (see pending_past), so it cannot
    # hold the family's end-of-grid step back for the whole stretch
    left = ledger.one("SELECT COUNT(*) FROM tests t JOIN candidates c ON c.id=t.candidate_id"
                      " WHERE t.kind='past' AND t.stretch_id=? AND c.family=? AND t.status!='done'"
                      " AND NOT (t.status='crashed' AND c.stage=?)",
                      (stretch.stretch_id, family, BINNED))[0]
    done = ledger.one("SELECT COUNT(*) FROM tests t JOIN candidates c ON c.id=t.candidate_id"
                      " WHERE t.kind='past' AND t.stretch_id=? AND c.family=? AND t.status='done'",
                      (stretch.stretch_id, family))[0]
    return left == 0 and done > 0


def _reference_test(ledger, sib: Candidate, window, stretch, now, log) -> bool:
    """A neighbour's past test, run only so a sibling's plateau can be judged. It
    counts toward N like any test and moves nobody."""
    if ledger.candidate(sib.id) is None:
        ledger.register(sib, name_of(sib), now=now)
    past = ledger.past_row(sib.id, stretch.stretch_id)
    if past is not None and past["status"] == "done":
        return True
    tid = ledger.begin_test(sib.id, KIND_PAST, stretch.stretch_id, stretch.cut_day, now=now)
    row = ledger.candidate(sib.id)
    if row["stage"] in (IDEA, PAST_TEST):
        # an idea of this stretch: its own test, with the ordinary moves
        return run_past(ledger, row, tid, window, stretch, now=now, log=log) is not None
    try:
        res = sim.run(sib, window)
        s = stats.summarize(res, n_trials=ledger.n_tests(kind=KIND_PAST, family=sib.family),
                            kind="past")
        v = stats.past_verdict(s, None)
    except Exception as exc:
        ledger.crash_test(tid, f"{type(exc).__name__}: {exc}", now=now)
        log(f"past: neighbour {sib.id} crashed: {exc}")
        return False
    ledger.finish_test(tid, s.to_cols(), res.daily, reasons=v.reasons, skipped=res.skipped,
                       synthetic=res.synthetic, seconds=res.seconds, trial=s.trial_extra(), now=now)
    log(f"past: neighbour {sib.id} tested for its sibling's plateau ({row['stage']})")
    return True


def family_end(ledger, family: str, stretch, *, window=None, now: dt.datetime,
               log: Log = _quiet, should_stop: Callable[[], bool] = lambda: False) -> dict:
    """After a family's grid is complete: the plateau of every past pass (testing any
    neighbour still missing), the family's PBO gate, each pass's deflated Sharpe
    (shown and used to rank, never a gate), then ``regrade_past`` on EVERY past row
    that passed, so the fresh door knows it has been through here. A pass that turns
    into a spike is binned; one held back (edge of the range, a neighbour missing,
    the family fitting noise) waits at PAST TEST."""
    sid = stretch.stretch_id
    rows = ledger.past_rows_for_stretch(sid, family)
    if not rows:
        return {}
    nets = {r["candidate_id"]: float(r["net_gbp"] or 0.0) for r in rows}
    passing = [r for r in rows if r["passed"]]
    defs = {}
    for r in passing:
        crow = ledger.candidate(r["candidate_id"])
        try:
            defs[r["candidate_id"]] = cand_of(crow)
        except (TypeError, ValueError, KeyError):
            continue
    # neighbours still missing (for example binned on an earlier stretch): test them now
    if window is not None:
        tried = set()
        for cid, c in defs.items():
            state, _ext = stats.plateau_state(c, nets)
            if state is not None:
                continue
            for sib in stats.missing_siblings(c, nets):
                if sib.id in tried:
                    continue
                if should_stop():
                    break
                tried.add(sib.id)
                _reference_test(ledger, sib, window, stretch, now, log)
        if tried:
            rows = ledger.past_rows_for_stretch(sid, family)
            nets = {r["candidate_id"]: float(r["net_gbp"] or 0.0) for r in rows}
            passing = [r for r in rows if r["passed"]]
    daily_by_id = {r["candidate_id"]: {d: v[0] for d, v in ledger.daily_of(r["id"]).items()}
                   for r in rows}
    trades_by_id = {r["candidate_id"]: int(r["trades"] or 0) for r in rows}
    gate = stats.family_gate(daily_by_id, trades_by_id)
    n_family = ledger.n_tests(kind=KIND_PAST, family=family)
    sharpes = ledger.family_sharpes(family) or None
    extend: set = set()
    passed_after = 0
    for r in passing:
        cid = r["candidate_id"]
        c = defs.get(cid)
        if c is None:
            try:
                c = cand_of(ledger.candidate(cid))
            except (TypeError, ValueError, KeyError):
                continue
        state, ext = stats.plateau_state(c, nets)
        extend.update(ext)
        daily = ledger.daily_of(r["id"])
        ts = stats.TestStats.from_row(r, daily)
        v = stats.past_verdict(ts, state, pbo_ok=not gate["blocks"])
        values = [daily[d][0] for d in sorted(daily)]
        dsr = stats.dsr_of(values, n_family, sharpes) if len(values) >= 8 else 0.0
        ledger.regrade_past(r["id"], plateau=state, passed=v.passed, reasons=v.reasons, dsr=dsr)
        crow = ledger.candidate(cid)
        if v.passed:
            # a neighbour binned on an earlier stretch, tested again only for this plateau,
            # moves nowhere: it is not counted as a pass of this stretch
            if not (crow and crow["stage"] == BINNED and crow.get("bin_stage") in (IDEA, PAST_TEST)):
                passed_after += 1
            if crow and crow["stage"] == PAST_TEST:
                ledger.set_rank(cid, dsr)
        elif crow and crow["stage"] == PAST_TEST:
            ledger.move(cid, BINNED, bin_words("past", v), bin_reason=bin_words("past", v), now=now)
    control = None
    if family == CONTROL:
        verdicts = {}
        for r in ledger.past_rows_for_stretch(sid, family):
            verdicts[r["candidate_id"]] = stats.past_verdict(
                stats.TestStats.from_row(r), r["plateau"] if r["plateau"] else "na")
        control = stats.pass_rates(verdicts)
    ledger.upsert_family(family, sid, n=len(rows), past_passed=passed_after, pbo=gate["pbo"],
                         extend_grid=sorted(extend), control_pass=control)
    log(f"family {family}: {len(rows)} tested, {passed_after} passed, PBO "
        f"{'n/a' if gate['pbo'] is None else gate['pbo']}"
        + (" (fits noise: its forwards are held back)" if gate["blocks"] else ""))
    return {"family": family, "n": len(rows), "passed": passed_after, "pbo": gate["pbo"],
            "blocks": gate["blocks"], "extend": sorted(extend)}


# -------------------------------------------------------- step 8: fresh ---
def fresh_ranked(ledger, stretch) -> list:
    """The past passes that may have their fresh look, best first: every past check
    passed and gated, plateau known and not at the edge, nothing held back, the
    family's PBO not over 0.35, no fresh look yet (or the one retry after too few
    trades on an earlier stretch). Ranked by the past DSR, then the past p."""
    sid = stretch.stretch_id
    rows = [dict(r) for r in ledger.query(
        "SELECT t.*, c.family AS family, c.stage AS stage FROM tests t"
        " JOIN candidates c ON c.id=t.candidate_id WHERE t.kind='past' AND t.stretch_id=?"
        " AND t.status='done' AND t.passed=1 AND t.gated=1 AND c.stage=?", (sid, PAST_TEST))]
    out = []
    for r in rows:
        if r["plateau"] in (None, "extend"):
            continue
        if any(code in _reasons(r) for code in HOLD_BACK_REASONS):
            continue
        fam = ledger.family_row(r["family"], sid)
        if fam is not None and fam["pbo"] is not None and fam["pbo"] > PBO_MAX:
            continue
        looks = ledger.fresh_rows(r["candidate_id"])
        if looks:
            only = looks[0]
            if not (len(looks) == 1 and only["status"] == "done"
                    and STRETCH_GAP_RETEST in _reasons(only) and only["stretch_id"] != sid):
                continue
        out.append(r)
    out.sort(key=lambda r: (-(r["dsr"] or 0.0), 1.0 if r["p"] is None else r["p"], r["candidate_id"]))
    return out


def fresh_looks(ledger, stretch, store, universe: Sequence, settings, *, calendar=None,
                now: dt.datetime, log: Log = _quiet,
                should_stop: Callable[[], bool] = lambda: False) -> list:
    """Nightly step 8: at most ``fresh_per_night`` (3) fresh looks, best first, while
    the stretch has looks left (never more than 30). Each look is claimed and its
    ``running`` row committed before a single fresh bar is read; its result is final."""
    sid = stretch.stretch_id
    cap = clamp_fresh_cap(min(int(settings.fresh_cap_per_stretch), int(stretch.fresh_cap)))
    look_stretch = dataclasses.replace(stretch, fresh_cap=cap)
    used = ledger.fresh_used(sid)
    room = max(0, min(int(settings.fresh_per_night), cap - used))
    out: list = []
    if room <= 0:
        log(f"fresh: no look tonight ({used} of {cap} used this stretch)")
        return out
    for r in fresh_ranked(ledger, stretch):
        if len(out) >= room or should_stop():
            break
        cid = r["candidate_id"]
        crow = ledger.candidate(cid)
        c = cand_of(crow)
        try:
            win = fresh_once(cid, ledger, look_stretch, store, universe, calendar=calendar, now=now)
        except FreshRefused as exc:
            log(f"fresh: {cid} refused ({exc.code}): {exc}")
            continue
        except Exception as exc:                    # the look is spent; the row says crashed
            ledger.move(cid, BINNED, "its one fresh look could not be built",
                        bin_reason=f"Its one fresh look could not be built ({exc}); the look is spent",
                        now=now)
            out.append({"id": cid, "name": crow["name"], "outcome": "CRASHED", "test_id": None})
            log(f"fresh: {cid} window failed: {exc}")
            continue
        n_look = ledger.fresh_used(sid)
        ledger.move(cid, FRESH_TEST, f"its one look at prices it never saw (look {n_look} of {cap} "
                    f"this stretch)", now=now)
        try:
            res = sim.run(c, win)
            s = stats.summarize(res, kind="fresh")
            v = stats.fresh_verdict(s)
        except Exception as exc:
            ledger.crash_test(win.test_id, f"{type(exc).__name__}: {exc}", now=now)
            ledger.move(cid, BINNED, "its fresh test crashed",
                        bin_reason=f"Its fresh test crashed ({exc}); the one look is spent", now=now)
            out.append({"id": cid, "name": crow["name"], "outcome": "CRASHED", "test_id": win.test_id})
            log(f"fresh: {cid} crashed: {exc}")
            continue
        ledger.finish_test(win.test_id, s.to_cols(), res.daily, reasons=v.reasons, skipped=res.skipped,
                           synthetic=res.synthetic, seconds=res.seconds, trial=s.trial_extra(), now=now)
        figures = f"{gbp(s.net_gbp)} over {s.trades} trades on prices it never saw"
        if v.passed:
            ledger.move(cid, ON_THE_BENCH, f"passed the fresh test: {figures}; "
                        f"{stats.luck_line(s.p, FRESH_LOOKS_PER_STRETCH)}", now=now)
        elif v.outcome == FRESH_TOO_FEW:
            first = len(ledger.fresh_rows(cid)) == 1
            more = ("it gets one more go on the next stretch" if first
                    else "it already had its one more go, so it is binned for good")
            why = f"too few trades on prices it never saw ({s.trades}; it needs {FRESH_MIN_TRADES}): {more}"
            ledger.move(cid, BINNED, why, bin_reason=why[0].upper() + why[1:], now=now)
        else:
            why = f"failed the fresh test ({figures}): {v.why}"
            ledger.move(cid, BINNED, why, bin_reason=bin_words("fresh", v), now=now)
        out.append({"id": cid, "name": crow["name"], "outcome": v.outcome, "test_id": win.test_id,
                    "trades": s.trades, "net_gbp": s.net_gbp, "p": s.p, "passed": bool(v.passed),
                    "synthetic": bool(res.synthetic)})
        log(f"fresh: {cid} {v.outcome}: {figures}")
    return out


LOOK_CUT_SHORT = "Its fresh test was cut short; the one look is spent"


def bin_spent_looks(ledger, *, now: dt.datetime, log: Log = _quiet) -> list:
    """At the start of a night (after the rows left running are marked crashed): an
    idea whose fresh look was claimed but never finished - the process was stopped,
    the Mac shut down - has spent its one look and is binned, as a crash inside the
    run would have done. Without this it would sit at FRESH TEST (or PAST TEST)
    for ever: no rule may give it a second look."""
    out = []
    for stage in (PAST_TEST, FRESH_TEST):
        for row in ledger.candidates(stage=stage):
            looks = ledger.fresh_rows(row["id"])
            if not any(r["status"] == "crashed" for r in looks):
                continue
            if stage == PAST_TEST:
                ledger.move(row["id"], FRESH_TEST, "its one look at prices it never saw was begun", now=now)
            ledger.move(row["id"], BINNED, "its fresh test was cut short", bin_reason=LOOK_CUT_SHORT,
                        now=now)
            out.append(row["id"])
            log(f"fresh: {row['id']} binned: its fresh test was cut short (the look is spent)")
    return out


def retry_too_few(ledger, stretch, *, now: dt.datetime, log: Log = _quiet) -> list:
    """The one way back: an idea binned after its ONE fresh look had too few trades,
    on an earlier stretch, goes back to PAST TEST for a retest on this one."""
    back = []
    for row in ledger.candidates(stage=BINNED):
        if row["bin_stage"] != FRESH_TEST:
            continue
        looks = ledger.fresh_rows(row["id"])
        if (len(looks) == 1 and looks[0]["status"] == "done" and FRESH_TOO_FEW in _reasons(looks[0])
                and looks[0]["stretch_id"] != stretch.stretch_id):
            ledger.retry_too_few(row["id"], now=now)
            back.append(row["id"])
            log(f"retry: {row['id']} back for its one more go (too few trades last stretch)")
    return back


# --------------------------------------------------- step 5: practice ---
@dataclasses.dataclass
class PracticeTrade:
    """A closed practice trade as the statistics read it."""
    net_gbp: float
    cost_gbp: float
    r: float
    opened_utc: str
    closed_utc: str
    day: str


def _pp_of(universe: Sequence) -> dict:
    out = {}
    for e in universe:
        try:
            out[e.name] = sim.Inst(e.name, e.kind, e.spec).pp
        except Exception:
            continue
    return out


def practice_trades(ledger, cid: str, universe: Sequence = ()) -> list:
    """Every closed practice trade of an idea, oldest first, with its costs worked
    out exactly as the simulator charges them: commission plus the spread and the
    slippage paid, in points, at the market's pounds per point."""
    pp = _pp_of(universe)
    out = []
    for r in ledger.query("SELECT * FROM practice_trades WHERE candidate_id=? AND mode='PRACTICE'"
                          " ORDER BY closed_utc, ticket", (cid,)):
        k = pp.get(r["symbol"], 0.0)
        cost = float(r["commission_gbp"] or 0.0) + (float(r["spread_points"] or 0.0) + float(
            r["slippage_points"] or 0.0)) * k * float(r["volume"] or 0.0)
        out.append(PracticeTrade(float(r["net_gbp"] or 0.0), cost, float(r["r"] or 0.0),
                                 r["opened_utc"] or "", r["closed_utc"] or "", _uk_day(r["closed_utc"])))
    return out


def _uk_day(stamp: Optional[str]) -> str:
    if not stamp:
        return ""
    t = dt.datetime.fromisoformat(stamp)
    if t.tzinfo is None:
        t = t.replace(tzinfo=UTC)
    return t.astimezone(TZ_LONDON).date().isoformat()


def practice_stats(ledger, cid: str, since_utc: str, upto_day: str, universe: Sequence = ()):
    """(TestStats, trades, days) of a practice stint from its day 1 to ``upto_day``
    (UK trading days, a day with no trade counting as 0)."""
    first = practice_start(since_utc)
    days = weekdays_between(first, upto_day) if upto_day >= first else []
    # only the trades of the judged days: one closed before day 1 (the evening it was given
    # its place) is not part of the stint the looks judge
    trades = [t for t in practice_trades(ledger, cid, universe) if t.day and first <= t.day <= upto_day]
    daily = {d: [0.0, 0] for d in days}
    for t in trades:
        if t.day in daily:
            daily[t.day][0] += t.net_gbp
            daily[t.day][1] += 1
    values = [daily[d][0] for d in days]
    nets = [t.net_gbp for t in trades]
    cn = stats.cost_nets(trades) if trades else {}
    s = stats.TestStats(
        kind="practice", window_from=days[0] if days else "", window_to=days[-1] if days else "",
        days=len(days), days_traded=sum(1 for d in days if daily[d][1]), trades=len(trades),
        net_gbp=sum(nets), cost_gbp=sum(t.cost_gbp for t in trades), pf=stats.profit_factor(nets),
        win_rate=(sum(1 for x in nets if x > 0) / len(nets)) if nets else 0.0,
        up_days=sum(1 for v in values if v > 0), down_days=sum(1 for v in values if v < 0),
        best_day_gbp=max(values) if values else 0.0,
        p=stats.block_bootstrap_p(values, cid), cost15_net=cn.get(COST_STRESS_PASS, 0.0),
        cost20_net=cn.get(COST_STRESS_SHOWN, 0.0), max_dd_r=stats.max_drawdown_r(trades))
    return s, trades, days


def _look_rows(ledger, cid: str) -> dict:
    return {r["look"]: dict(r) for r in ledger.query("SELECT * FROM looks WHERE candidate_id=?", (cid,))}


def _record_look(ledger, cid: str, look: int, s, passed: Optional[bool], now: dt.datetime) -> None:
    ledger.execute(
        "UPDATE looks SET done_utc=?, trades=?, net_gbp=?, p=?, pf=?, up_days=?, down_days=?,"
        " cost15_net=?, passed=? WHERE candidate_id=? AND look=?",
        (now.astimezone(UTC).replace(microsecond=0).isoformat(), s.trades, s.net_gbp, s.p, s.pf,
         s.up_days, s.down_days, s.cost15_net, None if passed is None else int(passed), cid, look))


def _leave_roster(ledger, cid: str) -> None:
    ledger.execute("DELETE FROM roster WHERE candidate_id=?", (cid,))


def roster(ledger) -> list:
    return [dict(r) for r in ledger.query("SELECT * FROM roster ORDER BY slot, candidate_id")]


def _symbols_json(row) -> list:
    try:
        out = json.loads(row["symbols_json"] or "[]")
        return [str(s) for s in out] if isinstance(out, list) else []
    except (TypeError, ValueError):
        return []


def bench_order(ledger) -> list:
    """The bench in the order places are given: by the PAST test's deflated Sharpe,
    then the past p, then the id. It reads the past rows and nothing else: a fresh
    result is never a ranking input (a property test shuffles fresh results and
    checks the order cannot move)."""
    keyed = []
    for row in ledger.candidates(stage=ON_THE_BENCH):
        past = ledger.one("SELECT dsr, p FROM tests WHERE candidate_id=? AND kind='past'"
                          " AND status='done' AND passed=1 ORDER BY id DESC LIMIT 1", (row["id"],))
        dsr = float(past["dsr"] or 0.0) if past is not None else 0.0
        p = float(1.0 if past is None or past["p"] is None else past["p"])
        keyed.append((-dsr, p, row["id"]))
    keyed.sort()
    return [k[2] for k in keyed]


def practice_symbols(universe: Sequence, kind: str, taken: set,
                     cap: int = MAX_PRACTICE_SYMBOLS) -> list:
    """The markets an idea practises on: its kind's markets in the universe, keeping
    the whole practice roster within ``cap`` distinct markets (those already watched
    come free; new ones only while there is room)."""
    mine = [e.name for e in universe if e.kind == kind]
    room = set(taken)
    chosen = []
    for s in mine:
        if s in room:
            chosen.append(s)
        elif len(room) < cap:
            room.add(s)
            chosen.append(s)
    return chosen


def grade_practice(ledger, universe: Sequence, settings, *, now: dt.datetime,
                   log: Log = _quiet) -> dict:
    """Nightly step 5: the looks due, the drawdown kill, "trades too rarely", the
    bench (60 days without a place is binned), then the promotions into free places
    and the roster the practice process reads. Idempotent: a look already judged is
    never judged again, a place is never given twice."""
    today = judge_day(now)
    out = {"judge_day": today, "looks": [], "binned": [], "ready": [], "promoted": [],
           "bench_binned": []}
    for place in roster(ledger):
        cid = place["candidate_id"]
        crow = ledger.candidate(cid)
        if crow is None or crow["stage"] not in PRACTISING:
            _leave_roster(ledger, cid)
            continue
        s, trades, days = practice_stats(ledger, cid, place["since_utc"], today, universe)
        # the drawdown kill, at every nightly check
        dd = stats.max_drawdown_r(trades)
        if dd > PRACTICE_KILL_R:
            why = (f"practice drawdown of {dd:.1f} R (limit {PRACTICE_KILL_R:g} R, "
                   f"£{PRACTICE_KILL_R * float(settings.risk_gbp):,.0f})")
            ledger.move(cid, BINNED, why, bin_reason=why[0].upper() + why[1:], now=now)
            _leave_roster(ledger, cid)
            out["binned"].append({"id": cid, "why": why})
            log(f"practice: {cid} binned: {why}")
            continue
        looks = _look_rows(ledger, cid)
        l1, l2 = looks.get(1), looks.get(2)
        if l1 is None or l2 is None:                 # a place given before the looks were kept
            d1, d2 = look_days(place["since_utc"])
            for n, d in ((1, d1), (2, d2)):
                ledger.execute("INSERT OR IGNORE INTO looks(candidate_id, look, due_day) VALUES (?,?,?)",
                               (cid, n, d))
            looks = _look_rows(ledger, cid)
            l1, l2 = looks[1], looks[2]
        stage = crow["stage"]
        # look 1: on its day (the 10th trading day) if it has 30 trades, else the first day
        # after it that it has them, while that is still before look 2's day. A look is
        # judged on its own day's figures even when that night was missed (the Mac was off):
        # the trades of later days are never counted in it.
        look1_day = None
        if l1["done_utc"] is None and today >= l1["due_day"]:
            look1_day = _look1_day(trades, l1["due_day"], l2["due_day"], today)
        if look1_day is not None:
            if look1_day != today:
                s, _t, _d = practice_stats(ledger, cid, place["since_utc"], look1_day, universe)
            v = stats.practice_verdict(s, at_look=True)
            _record_look(ledger, cid, 1, s, v.passed, now)
            out["looks"].append({"id": cid, "look": 1, "passed": v.passed})
            if v.passed:
                if stage == PRACTICE:
                    ledger.move(cid, READY_FOR_LIVE, f"passed practice look 1: {s.trades} practice "
                                f"trades over {s.days} days, {gbp(s.net_gbp)} after costs", now=now)
                    out["ready"].append(cid)
            elif s.net_gbp <= 0:
                why = f"failed practice look 1 ({gbp(s.net_gbp)} after costs over {s.trades} trades)"
                ledger.move(cid, BINNED, why, bin_reason=bin_words("practice", v), now=now)
                _leave_roster(ledger, cid)
                out["binned"].append({"id": cid, "why": why})
                log(f"practice: {cid} binned at look 1")
                continue
            continue
        # look 2: day 20, whatever it has, on day 20's figures
        if l2["done_utc"] is None and today >= l2["due_day"]:
            if today != l2["due_day"]:
                s, _t, _d = practice_stats(ledger, cid, place["since_utc"], l2["due_day"], universe)
            if l1["done_utc"] is None:                 # never had its 30 trades before day 20
                _record_look(ledger, cid, 1, s, None, now)
            v = stats.practice_verdict(s, at_look=True)
            _record_look(ledger, cid, 2, s, v.passed, now)
            out["looks"].append({"id": cid, "look": 2, "passed": v.passed})
            if v.passed:
                if stage == PRACTICE:
                    ledger.move(cid, READY_FOR_LIVE, f"passed practice look 2: {s.trades} practice "
                                f"trades over {s.days} days, {gbp(s.net_gbp)} after costs", now=now)
                    out["ready"].append(cid)
            else:
                if s.trades < PRACTICE_MIN_TRADES:
                    why = (f"it trades too rarely ({s.trades} practice trades in {s.days} days; "
                           f"it needs {PRACTICE_MIN_TRADES})")
                    reason = why[0].upper() + why[1:]
                else:
                    why = f"failed practice look 2 ({gbp(s.net_gbp)} after costs over {s.trades} trades)"
                    reason = bin_words("practice", v)
                ledger.move(cid, BINNED, why, bin_reason=reason, now=now)
                _leave_roster(ledger, cid)
                out["binned"].append({"id": cid, "why": why})
                log(f"practice: {cid} binned at look 2: {why}")
    # the bench: 60 days without a place is the end of it
    for row in ledger.candidates(stage=ON_THE_BENCH):
        try:
            since = dt.datetime.fromisoformat(row["stage_since_utc"])
        except (TypeError, ValueError):
            continue
        if since.tzinfo is None:
            since = since.replace(tzinfo=UTC)
        if (now - since).days >= BENCH_MAX_DAYS:
            why = f"{BENCH_MAX_DAYS} days on the bench without a practice place"
            ledger.move(row["id"], BINNED, why, bin_reason=why[0].upper() + why[1:], now=now)
            out["bench_binned"].append(row["id"])
    # promotions into free places
    places = roster(ledger)
    slots = max(0, int(settings.practice_slots))
    used_slots = {p["slot"] for p in places}
    taken_syms = {s for p in places for s in _symbols_json(p)}
    fam_count: dict = {}
    for p in places:
        crow = ledger.candidate(p["candidate_id"])
        if crow:
            fam_count[crow["family"]] = fam_count.get(crow["family"], 0) + 1
    for cid in bench_order(ledger):
        if len(places) >= slots:
            break
        crow = ledger.candidate(cid)
        if fam_count.get(crow["family"], 0) >= MAX_PER_FAMILY:
            continue
        c = cand_of(crow)
        syms = practice_symbols(universe, c.markets, taken_syms)
        if not syms:
            log(f"practice: {cid} waits: no room for its markets ({c.markets})")
            continue
        slot = next(k for k in range(1, slots + 1) if k not in used_slots)
        stamp = now.astimezone(UTC).replace(microsecond=0).isoformat()
        d1, d2 = look_days(stamp)
        ledger.execute("INSERT OR REPLACE INTO roster(candidate_id, slot, since_utc, symbols_json)"
                       " VALUES (?,?,?,?)", (cid, slot, stamp, json.dumps(syms)))
        ledger.execute("DELETE FROM looks WHERE candidate_id=?", (cid,))
        ledger.execute("INSERT INTO looks(candidate_id, look, due_day) VALUES (?,?,?)", (cid, 1, d1))
        ledger.execute("INSERT INTO looks(candidate_id, look, due_day) VALUES (?,?,?)", (cid, 2, d2))
        ledger.move(cid, PRACTICE, f"a practice place: slot {slot} on {', '.join(syms)}; judged on "
                    f"{_short(d1)} (look 1, if it has 30 trades) and {_short(d2)} (look 2)", now=now)
        places.append({"candidate_id": cid, "slot": slot, "since_utc": stamp,
                       "symbols_json": json.dumps(syms)})
        used_slots.add(slot)
        taken_syms.update(syms)
        fam_count[crow["family"]] = fam_count.get(crow["family"], 0) + 1
        out["promoted"].append(cid)
        log(f"practice: {cid} promoted to slot {slot}")
    return out


def _look1_day(trades: Sequence, due: str, look2_due: str, today: str) -> Optional[str]:
    """The day look 1 is judged on: the first UK trading day from its due day (and
    before look 2's) by which the stint has 30 trades, if that day has come."""
    for d in weekdays_between(due, min(today, add_days(look2_due, -1))):
        if sum(1 for t in trades if t.day <= d) >= PRACTICE_MIN_TRADES:
            return d
    return None


def _short(day: str) -> str:
    d = dt.date.fromisoformat(day)
    return f"{d.day} {d.strftime('%b')}"
