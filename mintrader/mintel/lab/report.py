"""The board: what the lab has to show, written once a night (step 9).

    data/lab/summary.json            schema 1, the only lab file the page reads
                                     (with data/lab-status.json)
    data/lab/runs/<date>/report.md   the night in plain words (the nightly review reads it)
    data/lab/runs/<date>/tests.csv   every test touched that night, one line each

Every money figure in the summary says where it came from (a ``source`` naming
the file and the rows), so nothing on the page is a guess. A store filled with
made-up prices makes ``synthetic: true`` here, all the way to the page, which
then shows "TEST DATA - not a result" instead of any lab figure.

``candidates`` lists every idea at ON THE BENCH, PRACTICE, READY FOR LIVE, LIVE
TRIAL and PROVEN, and every idea binned in the last 30 days AFTER it had its
fresh look (the hundreds binned on old prices each night are counts, by family:
listing each one would make the page's file megabytes long).

The practice process refreshes only each practising idea's ``figures`` and
``practice`` blocks, and ``written_utc``, every 30 seconds.

Aaron, 10 Oct 2026: "Make sure everything is presented in the bot showcase
really simply for me and not over the top, put it all nice and clear in
layman terms".
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import json
from pathlib import Path
from typing import Optional, Sequence

from ..clock import TZ_LONDON
from . import (BINNED, CHECK_LABELS, CHECK_LUCK, CHIP_COLOURS, CHIP_FAILED,
               CHIP_PASSED, CHIP_TESTING, CHIP_YOUR_CALL, FRESH_LOOKS_PER_STRETCH, FRESH_MIN_TRADES,
               FRESH_TEST, FRESH_TOO_FEW, IDEA, LIVE_TRIAL, LIVE_TRIAL_TRADES, NEEDED_DAYS,
               NOT_YET, ON_THE_BENCH, FAILED, PAST_TEST, PBO_MAX, PRACTICE,
               PRACTICE_MIN_TRADES, PROVEN, READY_FOR_LIVE, SHORT_HISTORY_DAYS, STAGES,
               SUMMARY_SCHEMA, UTC, LabPaths, add_days, atomic_write, atomic_write_json, is_weekday,
               utc_stamp)
from . import stats
from .candidate import words_of
from .families import CONTROL, all_families
from .stages import (PRACTISING, _look_rows, _reasons, bench_order, cand_of, gbp, judge_day,
                     practice_start, practice_stats, practice_trades, prev_weekday, roster,
                     weekdays_between)
from .status import TEST_DATA, read_json, summary_lock

LISTED_STAGES = (ON_THE_BENCH, PRACTICE, READY_FOR_LIVE, LIVE_TRIAL, PROVEN)
BINNED_LISTED_DAYS = 30
BINNED_RECENT_DAYS = 7
CHANGES_MAX = 30
DB = "data/lab/lab.sqlite"


def _short(day: str) -> str:
    d = dt.date.fromisoformat(day[:10])
    return f"{d.day} {d.strftime('%b')}"


def _uk_day_of(stamp: Optional[str]) -> str:
    if not stamp:
        return ""
    t = dt.datetime.fromisoformat(stamp)
    if t.tzinfo is None:
        t = t.replace(tzinfo=UTC)
    return t.astimezone(TZ_LONDON).date().isoformat()


def _parse(stamp: Optional[str]) -> Optional[dt.datetime]:
    if not stamp:
        return None
    try:
        t = dt.datetime.fromisoformat(stamp)
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=UTC)


def _check(c, p: Optional[float] = None) -> dict:
    d = c.as_dict()
    if c.key == CHECK_LUCK and p is not None:
        d["p"] = p
    return d


# ------------------------------------------------------------ per idea ---
def _test_block(row: Optional[dict], kind: str) -> Optional[dict]:
    if row is None:
        return None
    keys = ("window_from", "window_to", "days", "trades", "net_gbp", "cost_gbp", "pf", "win_rate",
            "up_days", "down_days", "best_day_gbp", "p", "dsr", "plateau", "cost15_net", "cost20_net",
            "max_dd_r", "passed", "status", "stretch_id", "cut_day")
    out = {k: row.get(k) for k in keys}
    out["test_id"] = row["id"]
    out["kind"] = kind
    out["reasons"] = _reasons(row)
    out["synthetic"] = bool(row.get("synthetic"))
    out["source"] = f"{DB} tests {row['id']} ({kind}, {row.get('stretch_id')})"
    return out


def _latest_done(ledger, cid: str, kind: str) -> Optional[dict]:
    r = ledger.one("SELECT * FROM tests WHERE candidate_id=? AND kind=? AND status='done'"
                   " ORDER BY id DESC LIMIT 1", (cid, kind))
    return dict(r) if r else None


def practice_block(ledger, crow: dict, universe: Sequence, now: dt.datetime) -> tuple:
    """(figures, practice) of an idea with a practice record, else (empty figures, None).
    Every figure comes from the practice_trades rows named in its source."""
    cid = crow["id"]
    place = ledger.one("SELECT * FROM roster WHERE candidate_id=?", (cid,))
    trades = practice_trades(ledger, cid, universe)
    if place is None and not trades:
        return _no_figures(), None
    uk_today = now.astimezone(TZ_LONDON).date()
    monday = (uk_today - dt.timedelta(days=uk_today.weekday())).isoformat()
    today = uk_today.isoformat()
    by_day: dict = {}
    for t in trades:
        by_day.setdefault(t.day, [0.0, 0])
        by_day[t.day][0] += t.net_gbp
        by_day[t.day][1] += 1
    nets = [t.net_gbp for t in trades]
    src = f"{DB} practice_trades candidate_id={cid} ({len(trades)} rows, mode PRACTICE)"
    # the last 10 UK trading days (the page's standouts are judged on this window, never on money alone)
    end = prev_weekday(today)
    window = weekdays_between(_back(end, LAST_DAYS), end)
    w_trades = [t for t in trades if window and window[0] <= t.day <= window[-1]]
    w_days = [by_day.get(d, [0.0, 0])[0] for d in window]
    last10 = {"from": window[0] if window else None, "to": end, "trades": len(w_trades),
              "net_gbp": sum(t.net_gbp for t in w_trades),
              "up_days": sum(1 for v in w_days if v > 0), "down_days": sum(1 for v in w_days if v < 0),
              "pf": stats.profit_factor([t.net_gbp for t in w_trades]) if w_trades else None,
              "source": src}
    figures = {"today_gbp": by_day.get(today, [0.0, 0])[0], "today_trades": by_day.get(today, [0.0, 0])[1],
               "week_gbp": sum(v[0] for d, v in by_day.items() if d >= monday),
               "overall_gbp": sum(nets), "trades": len(trades), "wins": sum(1 for x in nets if x > 0),
               "up_days": sum(1 for v in by_day.values() if v[0] > 0),
               "down_days": sum(1 for v in by_day.values() if v[0] < 0),
               "pf": stats.profit_factor(nets) if nets else None, "last10": last10,
               "practice": True, "source": src}
    practice = None
    if place is not None:
        place = dict(place)
        looks = _look_rows(ledger, cid)
        upto = judge_day(now) if now.astimezone(TZ_LONDON).hour < 22 else today
        first = practice_start(place["since_utc"])
        days = weekdays_between(first, max(first, upto)) if upto >= first else []
        try:
            syms = json.loads(place["symbols_json"] or "[]")
        except ValueError:
            syms = []
        practice = {"since": place["since_utc"], "first_day": first, "day": len(days),
                    "slot": place["slot"], "symbols": syms,
                    "look1_due": (looks.get(1) or {}).get("due_day"),
                    "look2_due": (looks.get(2) or {}).get("due_day"),
                    "looks": [_look_out(looks[k]) for k in sorted(looks)],
                    "trades": len(trades), "net_gbp": sum(nets), "source": src}
    return figures, practice


LAST_DAYS = 10                        # the standouts' window, in UK trading days
TRADES_LISTED = 50                    # practice rows carried for the idea's own page


def _back(end: str, n: int) -> str:
    """The weekday ``n - 1`` trading days before ``end`` (so ``n`` days in all)."""
    d, k = end, 1
    while k < n:
        d = add_days(d, -1)
        if is_weekday(d):
            k += 1
    return d


def practice_rows(ledger, cid: str, limit: int = TRADES_LISTED) -> list:
    """The newest practice trades of an idea, for its own page (practice, never money)."""
    return [{k: r[k] for k in ("ticket", "symbol", "side", "volume", "entry", "stop", "target", "opened_utc",
                                "closed_utc", "exit_price", "exit_reason", "net_gbp", "r", "fill_source", "mode")}
            for r in ledger.query("SELECT * FROM practice_trades WHERE candidate_id=? ORDER BY closed_utc DESC,"
                                  " ticket DESC LIMIT ?", (cid, int(limit)))]


def _look_out(row: dict) -> dict:
    keys = ("look", "due_day", "done_utc", "trades", "net_gbp", "p", "pf", "up_days", "down_days",
            "cost15_net", "passed")
    out = {k: row.get(k) for k in keys}
    out["source"] = f"{DB} looks candidate_id={row['candidate_id']} look {row['look']}"
    return out


def _no_figures() -> dict:
    return {"today_gbp": None, "today_trades": None, "week_gbp": None, "overall_gbp": None,
            "trades": None, "wins": None, "up_days": None, "down_days": None, "practice": False,
            "source": None}


def _look_stats(row: dict, since_utc: str) -> stats.TestStats:
    done = _parse(row.get("done_utc"))
    upto = judge_day(done) if done else row["due_day"]
    first = practice_start(since_utc)
    days = len(weekdays_between(first, upto)) if upto >= first else 0
    return stats.TestStats(kind="practice", days=days, trades=int(row.get("trades") or 0),
                           net_gbp=float(row.get("net_gbp") or 0.0), pf=float(row.get("pf") or 0.0),
                           p=float(1.0 if row.get("p") is None else row["p"]),
                           up_days=int(row.get("up_days") or 0), down_days=int(row.get("down_days") or 0),
                           cost15_net=float(row.get("cost15_net") or 0.0))


def _checks(ledger, crow: dict, past: Optional[dict], fresh: Optional[dict], universe, now) -> list:
    """The five checks for an idea, from the verdict of the stage it has reached."""
    stage = crow["stage"]
    at = crow.get("bin_stage") if stage == BINNED else stage
    fresh_v = stats.fresh_verdict(stats.TestStats.from_row(fresh)) if fresh else None
    if at in (PRACTICE, READY_FOR_LIVE, LIVE_TRIAL, PROVEN):
        place = ledger.one("SELECT * FROM roster WHERE candidate_id=?", (crow["id"],))
        since = dict(place)["since_utc"] if place else crow.get("stage_since_utc")
        looks = _look_rows(ledger, crow["id"])
        judged = [looks[k] for k in sorted(looks) if looks[k].get("done_utc") and looks[k].get("passed")
                  is not None]
        if stage == READY_FOR_LIVE and judged:
            passing = [r for r in judged if r["passed"]] or judged
            s = _look_stats(passing[-1], since)
            v = stats.practice_verdict(s, at_look=True)
        elif stage in (BINNED, PRACTICE) and judged and not judged[-1]["passed"]:
            # a look that was judged and failed stays failed on the page: an idea that failed
            # look 1 (but was ahead, so it practises on to look 2) is never shown as all clear
            s = _look_stats(judged[-1], since)
            v = stats.practice_verdict(s, at_look=True)
        elif since:
            s, _t, _d = practice_stats(ledger, crow["id"], since, judge_day(now), universe)
            v = stats.practice_verdict(s, at_look=False)
            if s.trades == 0:
                # no practice record yet: "Not yet", never a figure (and no luck figure)
                out = []
                for c in v.checks:
                    d = c.as_dict()
                    if c.key != "fresh":
                        d.update(state=NOT_YET, text="Not yet: no practice trades yet.", n=None, of=None)
                    out.append(d)
                return out
            if stage == BINNED:
                for c in v.checks:
                    if c.key == "steady":
                        c.state, c.text = FAILED, (crow.get("bin_reason") or "Binned in practice.")
        else:
            v = None
        if v is not None:
            return [_check(c, s.p if c.key == CHECK_LUCK else None) for c in v.checks]
    if at in (FRESH_TEST, ON_THE_BENCH) or (at in (PRACTICE, READY_FOR_LIVE) and fresh_v):
        if fresh_v is not None:
            return [_check(c, fresh["p"] if c.key == CHECK_LUCK else None) for c in fresh_v.checks]
    if past is not None:
        plateau = past.get("plateau")
        v = stats.past_verdict(stats.TestStats.from_row(past), plateau if past.get("gated") else None)
        return [_check(c, past["p"] if c.key == CHECK_LUCK else None) for c in v.checks]
    return [{"key": k, "label": CHECK_LABELS[k], "state": NOT_YET, "text": "Not yet: no test has run.",
             "n": None, "of": None} for k in ("trades", "steady", "fresh", "costs", "luck")]


def _chip(stage: str) -> dict:
    code = {ON_THE_BENCH: CHIP_PASSED, READY_FOR_LIVE: CHIP_YOUR_CALL, BINNED: CHIP_FAILED,
            LIVE_TRIAL: CHIP_PASSED, PROVEN: CHIP_PASSED}.get(stage, CHIP_TESTING)
    return {"code": code, "colour": CHIP_COLOURS[code]}


def _words_for(crow: dict, past, fresh, figures, practice, ctx) -> tuple:
    """(headline, what to do) in the exact wording of the spec."""
    stage = crow["stage"]
    if stage == ON_THE_BENCH:
        order = ctx["bench"]
        pos = order.index(crow["id"]) + 1 if crow["id"] in order else len(order)
        nth = {1: "1st", 2: "2nd", 3: "3rd"}.get(pos, f"{pos}th")
        return (f"Passed both tests; waiting for a practice place ({ctx['taken']} of {ctx['slots']} "
                f"taken, {nth} in line).", "Nothing - it starts practising when a place frees.")
    if stage == PRACTICE and practice is not None:
        n = int(practice["trades"])
        head = (f"Practising for {practice['day']} days: {gbp(practice['net_gbp'])} after costs over {n} "
                f"practice trades, {figures['up_days']} days up, {figures['down_days']} down.")
        if n == 0:
            head = f"Practising since {_short(practice['first_day'])}: no practice trades yet."
        looks = {lk["look"]: lk for lk in practice.get("looks") or []}
        l1 = looks.get(1) or {}
        if l1.get("done_utc") and l1.get("passed") is not None and not l1.get("passed"):
            todo = (f"Watch - it did not pass its first look; judged again on "
                    f"{_short(practice['look2_due'])} after 20 trading days.")
        elif not l1.get("done_utc") and practice.get("look1_due"):
            left = max(0, PRACTICE_MIN_TRADES - n)
            more = f"{left} to go" if left else f"it has {n}"
            todo = (f"Watch - judged on {_short(practice['look1_due'])} after 10 trading days and "
                    f"{PRACTICE_MIN_TRADES}+ trades ({more}).")
        else:
            todo = f"Watch - judged on {_short(practice['look2_due'])} after 20 trading days."
        return head, todo
    if stage == READY_FOR_LIVE:
        looks = [lk for lk in (practice or {}).get("looks") or [] if lk.get("passed")]
        lk = looks[-1] if looks else {}
        days = int((practice or {}).get("day") or 0)
        if practice is not None and lk.get("done_utc"):
            done = _parse(lk["done_utc"])
            days = len(weekdays_between(practice["first_day"], judge_day(done)))
        trades = int(lk.get("trades") or 0)
        return (f"Ready for a live trial: {trades} practice trades over {days} "
                f"days, {gbp(float(lk.get('net_gbp') or 0.0))} after costs, {lk.get('up_days') or 0} days "
                f"up, {lk.get('down_days') or 0} down, every check passed.",
                f"Your call: say the word and it goes live at £{ctx['risk']:g} a trade, judged after "
                f"{LIVE_TRIAL_TRADES} live trades - or leave it practising.")
    if stage == LIVE_TRIAL:
        return ("Live trial: wired by hand, so its real trades are on its own bot's row.",
                f"Keep it at £{ctx['risk']:g} a trade - judged after {LIVE_TRIAL_TRADES} live trades.")
    if stage == PROVEN:
        return ("Proven: it passed its live trial.", "Keep and grow - sizes are your call.")
    if stage == BINNED:
        when = _short(_uk_day_of(crow.get("binned_utc")) or crow.get("binned_utc") or "")
        at = crow.get("bin_stage")
        todo = "Nothing - it stays binned and is not retried."
        if at == FRESH_TEST and fresh is not None:
            reasons = _reasons(fresh)
            if FRESH_TOO_FEW in reasons:
                what = (f"too few trades on prices it never saw ({int(fresh['trades'] or 0)}; it needs "
                        f"{FRESH_MIN_TRADES})")
                if len(ctx["fresh_rows"](crow["id"])) == 1:
                    todo = "Nothing - it gets one more go on the next stretch."
            else:
                what = (f"failed the fresh test ({gbp(float(fresh['net_gbp'] or 0.0))} over "
                        f"{int(fresh['trades'] or 0)} trades on prices it never saw)")
        else:
            why = (crow.get("bin_reason") or "binned").rstrip(".")
            what = why[0].lower() + why[1:] if why else "binned"
        return f"Binned on {when}: {what}.", todo
    if stage == FRESH_TEST:
        return ("Having its one look at prices it never saw.", "Nothing - the result comes tonight.")
    return ("Waiting for its turn.", "Nothing.")


def candidate_entry(ledger, crow: dict, ctx: dict) -> dict:
    cid = crow["id"]
    past = _latest_done(ledger, cid, "past")
    fresh = _latest_done(ledger, cid, "fresh")
    figures, practice = practice_block(ledger, crow, ctx["universe"], ctx["now"])
    head, todo = _words_for(crow, past, fresh, figures, practice, ctx)
    try:
        c = cand_of(crow)
        words = words_of(c)
        family_label = next((f.label for f in all_families(True) if f.code == c.family), c.family)
    except Exception:
        words, family_label = [], crow.get("family") or ""
    story = [{"when": e["ts_utc"], "from": e["from_stage"], "to": e["to_stage"], "why": e["why"],
              "by": e["by"], "kind": e["kind"]}
             for e in ledger.query("SELECT * FROM events WHERE candidate_id=? AND kind IN"
                                   " ('stage','retry','reopen') ORDER BY id", (cid,))]
    sources = []
    for r in ledger.query("SELECT id, kind, stretch_id, status FROM tests WHERE candidate_id=? ORDER BY id",
                          (cid,)):
        sources.append(f"{DB} tests {r['id']} ({r['kind']}, {r['stretch_id']}, {r['status']})")
    if figures.get("source"):
        sources.append(figures["source"])
    if practice is not None:
        sources.append(f"{DB} roster candidate_id={cid}; looks candidate_id={cid}")
    sources.append(f"{DB} events candidate_id={cid}")
    synthetic = bool((past or {}).get("synthetic") or (fresh or {}).get("synthetic")) or ctx["synthetic"]
    return {"id": cid, "name": crow["name"], "family": crow["family"], "family_label": family_label,
            "stage": crow["stage"], "stage_since": crow["stage_since_utc"], "chip": _chip(crow["stage"]),
            "headline": head, "what_to_do": todo, "ready": crow["stage"] == READY_FOR_LIVE,
            "checks": _checks(ledger, crow, past, fresh, ctx["universe"], ctx["now"]),
            "figures": figures, "tests": {"past": _test_block(past, "past"), "fresh": _test_block(fresh, "fresh")},
            "practice": practice, "practice_trades": practice_rows(ledger, cid) if figures.get("practice") else [],
            "words": words, "story": story, "sources": sources,
            "bin_stage": crow.get("bin_stage"), "bin_reason": crow.get("bin_reason"),
            "binned_utc": crow.get("binned_utc"), "synthetic": synthetic}


# ------------------------------------------------------------ the board ---
def _funnel(ledger, *, waiting: int) -> dict:
    counts = ledger.stage_counts()
    one = lambda sql, a=(): int(ledger.one(sql, a)[0])          # noqa: E731
    return {
        "ideas": one("SELECT COUNT(*) FROM candidates"),
        "past_tested": one("SELECT COUNT(DISTINCT candidate_id) FROM tests WHERE kind='past' AND status='done'"),
        # an idea binned on old prices whose neighbour re-test passed moves nowhere: not counted
        "past_passed": one("SELECT COUNT(*) FROM tests t JOIN candidates c ON c.id=t.candidate_id"
                           " WHERE t.kind='past' AND t.status='done' AND t.passed=1"
                           " AND NOT (c.stage=? AND COALESCE(c.bin_stage, '') IN (?,?))"
                           " AND t.id=(SELECT MAX(id) FROM tests u WHERE u.candidate_id=t.candidate_id"
                           " AND u.kind='past' AND u.status='done')", (BINNED, IDEA, PAST_TEST)),
        "fresh_tested": one("SELECT COUNT(DISTINCT candidate_id) FROM tests WHERE kind='fresh' AND status='done'"),
        "fresh_passed": one("SELECT COUNT(DISTINCT candidate_id) FROM tests WHERE kind='fresh' AND status='done'"
                            " AND passed=1"),
        "past_test": counts.get(PAST_TEST, 0), "bench": counts.get(ON_THE_BENCH, 0),
        "practice": counts.get(PRACTICE, 0), "ready": counts.get(READY_FOR_LIVE, 0),
        "live": counts.get(LIVE_TRIAL, 0), "proven": counts.get(PROVEN, 0),
        "binned": counts.get(BINNED, 0), "ideas_waiting": counts.get(IDEA, 0),
        "waiting_for_calendar": int(waiting)}


def _families(ledger, cut) -> list:
    out = []
    sid = cut.stretch_id if cut else None
    for f in all_families(True):
        row = ledger.family_row(f.code, sid) if sid else None
        tested = 0
        if sid:
            tested = int(ledger.one("SELECT COUNT(*) FROM tests t JOIN candidates c ON c.id=t.candidate_id"
                                    " WHERE t.kind='past' AND t.stretch_id=? AND t.status='done'"
                                    " AND c.family=?", (sid, f.code))[0])
        pbo = row["pbo"] if row else None
        try:
            ext = json.loads(row["extend_grid_json"] or "[]") if row else []
        except ValueError:
            ext = []
        out.append({"code": f.code, "label": f.label, "n": f.GRID_SIZE, "tested": tested,
                    "past_passed": int(row["past_passed"] or 0) if row else 0, "pbo": pbo,
                    "fits_noise": bool(pbo is not None and pbo > PBO_MAX), "extend_grid": ext,
                    "complete": row is not None, "control": f.code == CONTROL,
                    "dormant": bool(f.dormant and not (cut and cut.news_awake)),
                    "no_numeric_setting": not any(len(f.axis_values(p)) >= 2 for p in f.sensitive),
                    "source": f"{DB} families code={f.code} stretch={sid}" if row else None})
    return out


def _control(ledger) -> dict:
    def rate(kind):
        r = ledger.one("SELECT COUNT(*), SUM(t.passed) FROM tests t JOIN candidates c ON c.id=t.candidate_id"
                       " WHERE t.kind=? AND t.status='done' AND c.family=?", (kind, CONTROL))
        n, k = int(r[0] or 0), int(r[1] or 0)
        return (None if n == 0 else k / n), n, k
    pr, pn, pk = rate("past")
    fr, fn, fk = rate("fresh")
    return {"past_pass_rate": pr, "past_tested": pn, "past_passed": pk,
            "fresh_pass_rate": fr, "fresh_tested": fn, "fresh_passed": fk,
            "source": f"{DB} tests of family {CONTROL}"}


def _changes(ledger, now: dt.datetime) -> list:
    since = (now - dt.timedelta(hours=24)).astimezone(UTC).replace(microsecond=0).isoformat()
    rows = [dict(r) for r in ledger.query(
        "SELECT e.*, c.name AS name FROM events e LEFT JOIN candidates c ON c.id=e.candidate_id"
        " WHERE e.ts_utc>=? ORDER BY e.id", (since,))]
    out = []
    tried = binned_past = 0
    late = (FRESH_TEST, ON_THE_BENCH, PRACTICE, READY_FOR_LIVE, LIVE_TRIAL, PROVEN)
    for e in rows:
        kind, frm, to = e["kind"], e["from_stage"], e["to_stage"]
        text = ""
        if kind == "stage":
            if frm == IDEA and to == PAST_TEST:
                tried += 1
            elif to == BINNED and frm in (IDEA, PAST_TEST):
                binned_past += 1
            elif to == ON_THE_BENCH:
                text = "Passed its fresh test - now on the bench."
            elif to == PRACTICE:
                text = "Started practising on live prices (no money)."
            elif to == READY_FOR_LIVE:
                text = "Ready for a live trial - your call."
            elif to == BINNED and frm in late:
                text = f"Binned: {e['why']}."
        elif kind == "retry":
            text = "Back for its one more go on this stretch."
        elif kind == "reopen":
            text = f"Reopened by {e['by']}: {e['why']}."
        if text:
            out.append({"when": e["ts_utc"], "id": e["candidate_id"], "name": e.get("name") or "", "text": text})
    if tried or binned_past:
        out.insert(0, {"when": utc_stamp(now), "id": "", "name": "",
                       "text": f"{tried} idea{'' if tried == 1 else 's'} tried on past prices; "
                               f"{binned_past} failed and {'was' if binned_past == 1 else 'were'} binned."})
    return out[-CHANGES_MAX:]


def _binned_recent(ledger, now: dt.datetime) -> list:
    since = (now - dt.timedelta(days=BINNED_RECENT_DAYS)).astimezone(UTC).isoformat()
    out = []
    for r in ledger.query("SELECT * FROM candidates WHERE stage=? AND binned_utc>=? AND bin_stage NOT IN (?,?)"
                          " ORDER BY binned_utc DESC, id LIMIT 20", (BINNED, since, IDEA, PAST_TEST)):
        out.append({"id": r["id"], "name": r["name"], "stage": r["bin_stage"], "reason": r["bin_reason"],
                    "when": r["binned_utc"]})
    return out


def log_tail(path, n: int = 20) -> list:
    """The last ``n`` lines of the nightly's log (the /lab page shows them)."""
    if path is None:
        return []
    try:
        with open(path, "rb") as fh:
            fh.seek(0, 2)
            size = fh.tell()
            fh.seek(max(0, size - 16384))
            lines = fh.read().decode("utf-8", errors="replace").splitlines()
    except OSError:
        return []
    return [ln[:300] for ln in lines[-n:]]


def _nightly(ledger, run_date: Optional[str]) -> dict:
    row = None
    if run_date:
        row = ledger.one("SELECT * FROM runs WHERE date=?", (run_date,))
    if row is None:
        row = ledger.one("SELECT * FROM runs ORDER BY date DESC LIMIT 1")
    if row is None:
        return {}
    row = dict(row)
    started = row.get("started_utc") or ""
    fp = int(ledger.one("SELECT COUNT(*) FROM tests WHERE kind='fresh' AND status='done' AND passed=1"
                        " AND finished_utc>=?", (started,))[0]) if started else 0
    return {"date": row["date"], "started": started, "finished": row.get("finished_utc"),
            "ok": None if row.get("ok") is None else bool(row["ok"]), "step": row.get("step"),
            "tested": row.get("tested"), "fresh": row.get("fresh"), "fresh_passed": fp,
            "seconds": row.get("seconds"), "seconds_per_candidate": row.get("seconds_per_candidate"),
            "deadline_hit": bool(row.get("deadline_hit")), "note": row.get("note") or "",
            "missed": False, "source": f"{DB} runs date={row['date']}"}


def build_summary(ledger, paths: LabPaths, *, settings, universe: Sequence, store=None, cut=None,
                  now: dt.datetime, run_date: Optional[str] = None, waiting: int = 0,
                  news_from: Optional[str] = None) -> dict:
    """summary.json (schema 1), from the ledger, the store and the cut."""
    names = [e.name for e in universe]
    cov = store.coverage(names) if (store is not None and names) else {
        "symbols": len(names), "complete_days": 0, "needed_days": NEEDED_DAYS, "first_day": None,
        "last_day": None, "synthetic": False, "ready": False}
    synthetic_tests = bool(ledger.one("SELECT COUNT(*) FROM tests WHERE synthetic=1")[0])
    synthetic = bool(cov.get("synthetic")) or synthetic_tests
    days = store.trading_days(names) if (store is not None and names) else []
    past = [d for d in days if cut and d < cut.cut_day]
    fresh = [d for d in days if cut and d >= cut.cut_day]
    store_block = {"symbols": cov.get("symbols", len(names)), "first_day": cov.get("first_day"),
                   "last_day": cov.get("last_day"), "complete_days": cov.get("complete_days", 0),
                   "needed_days": NEEDED_DAYS, "ready": bool(cov.get("ready")),
                   "past": [past[0], past[-1]] if past else [], "fresh": [fresh[0], fresh[-1]] if fresh else [],
                   "past_days": len(past), "fresh_days": len(fresh),
                   "stretch_id": cut.stretch_id if cut else None, "cut_day": cut.cut_day if cut else None,
                   "fresh_used": ledger.fresh_used(cut.stretch_id) if cut else 0,
                   "fresh_cap": cut.fresh_cap if cut else FRESH_LOOKS_PER_STRETCH,
                   "short_history": bool(cov.get("complete_days", 0) < SHORT_HISTORY_DAYS),
                   "news_from": news_from, "synthetic": synthetic,
                   "source": "data/lab/bars/*/index.json and data/lab/cut.json"}
    places = roster(ledger)
    ctx = {"universe": list(universe), "now": now, "bench": bench_order(ledger), "taken": len(places),
           "slots": int(settings.practice_slots), "risk": float(settings.risk_gbp),
           "synthetic": synthetic, "fresh_rows": ledger.fresh_rows}
    listed = []
    since = (now - dt.timedelta(days=BINNED_LISTED_DAYS)).astimezone(UTC).isoformat()
    for crow in ledger.candidates():
        if crow["stage"] in LISTED_STAGES or crow["stage"] == FRESH_TEST or (
                crow["stage"] == BINNED and (crow.get("binned_utc") or "") >= since
                and crow.get("bin_stage") not in (IDEA, PAST_TEST, None)):
            listed.append(candidate_entry(ledger, crow, ctx))
    rank = {s: k for k, s in enumerate(STAGES)}
    listed.sort(key=lambda e: (-rank.get(e["stage"], 0) if e["stage"] != BINNED else 99,
                               e["stage_since"] or "", e["id"]))
    trials = {"lab": ledger.n_tests(), "past": ledger.n_tests(kind="past"),
              "fresh": ledger.n_tests(kind="fresh"), "by_family": ledger.n_by_family(),
              "control": _control(ledger), "source": f"{DB} tests (running, crashed and done)"}
    nightly = _nightly(ledger, run_date)
    if nightly:
        nightly["log_tail"] = log_tail(paths.nightly_log)
    return {"schema": SUMMARY_SCHEMA, "written_utc": utc_stamp(now), "synthetic": synthetic,
            "mode": settings.mode, "paused": settings.mode == "OFF",
            "test_data_note": TEST_DATA if synthetic else "",
            "store": store_block, "funnel": _funnel(ledger, waiting=waiting), "trials": trials,
            "families": _families(ledger, cut), "candidates": listed,
            "changes_24h": _changes(ledger, now), "binned_recent": _binned_recent(ledger, now),
            "nightly": nightly, "refresh": _refresh_block(paths)}


def _refresh_block(paths: LabPaths) -> Optional[dict]:
    """The latest weekly refresh of the practice pool (data/lab/refresh.json), short."""
    from .refresh import _read, summary_block
    return summary_block(_read(paths.root / "refresh.json"))


def write_summary(paths: LabPaths, summary: dict) -> None:
    with summary_lock(paths):
        atomic_write_json(paths.summary, summary)


def refresh_practice(paths: LabPaths, ledger, universe: Sequence, now: dt.datetime) -> bool:
    """The practice process's 30-second refresh: each practising idea's figures and
    practice blocks, and written_utc. Nothing else in the file changes."""
    with summary_lock(paths):
        summary, _why = read_json(paths.summary)
        if summary is None:
            return False
        for entry in summary.get("candidates") or []:
            if entry.get("stage") not in PRACTISING:
                continue
            crow = ledger.candidate(entry.get("id"))
            if crow is None:
                continue
            entry["figures"], entry["practice"] = practice_block(ledger, crow, universe, now)
            entry["practice_trades"] = practice_rows(ledger, crow["id"])
        summary["written_utc"] = utc_stamp(now)
        atomic_write_json(paths.summary, summary)
    return True


# --------------------------------------------------------- the text files ---
TEST_COLS = ("id", "candidate_id", "family", "name", "kind", "stretch_id", "cut_day", "status",
             "started_utc", "finished_utc", "seconds", "window_from", "window_to", "days", "trades",
             "net_gbp", "cost_gbp", "pf", "win_rate", "up_days", "down_days", "best_day_gbp", "p",
             "folds_up", "folds_worst", "plateau", "cost15_net", "cost20_net", "dsr", "max_dd_r",
             "assumed_share", "passed", "gated", "reasons_json", "synthetic")


def tests_csv(ledger, since_utc: str) -> str:
    """Every test begun or finished since ``since_utc``, one line each."""
    rows = ledger.query(
        "SELECT t.*, c.family AS family, c.name AS name FROM tests t LEFT JOIN candidates c"
        " ON c.id=t.candidate_id WHERE t.started_utc>=? OR t.finished_utc>=? ORDER BY t.id",
        (since_utc, since_utc))
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(TEST_COLS)
    for r in rows:
        w.writerow(["" if r[k] is None else r[k] for k in TEST_COLS])
    return buf.getvalue()


def _money(x, synthetic: bool) -> str:
    if x is None:
        return "-"
    return f"{gbp(float(x))} (TEST DATA)" if synthetic else gbp(float(x))


def report_md(summary: dict, *, night: str, notes: Sequence[str] = (), fresh_tonight: Sequence = (),
              practice_tonight: Optional[dict] = None) -> str:
    """The night in plain words, for Aaron and the nightly review."""
    syn = bool(summary.get("synthetic"))
    st, fn, night_row = summary["store"], summary["funnel"], summary.get("nightly") or {}
    try:
        d = dt.date.fromisoformat(night[:10])
        title = f"{d.strftime('%A')} {d.day} {d.strftime('%b %Y')}"
    except ValueError:
        title = night
    L = [f"# The lab - the night of {title}", ""]
    if syn:
        L += [f"**{TEST_DATA}.** These prices were made up (a SYNTHETIC store): nothing below is a "
              f"result, every figure is marked TEST DATA.", ""]
    if summary.get("paused"):
        L += ["The lab is paused (data/lab.json says OFF).", ""]
    if not st.get("ready"):
        L += [f"Collecting past prices: {st.get('complete_days', 0)} of {st.get('needed_days', NEEDED_DAYS)} "
              f"complete trading days. Nothing is tested until there are {NEEDED_DAYS}.", ""]
    else:
        L.append(f"Prices: {st.get('symbols')} markets, {st.get('complete_days')} complete trading days "
                 f"({st.get('first_day')} to {st.get('last_day')}).")
        if st.get("past") and st.get("fresh"):
            L.append(f"Past (used for choosing): {st['past'][0]} to {st['past'][1]}. Fresh (kept back, "
                     f"looked at once): {st['fresh'][0]} to {st['fresh'][1]}; {st.get('fresh_used')} of "
                     f"{st.get('fresh_cap')} fresh looks used in stretch {st.get('stretch_id')}.")
        if st.get("short_history"):
            L.append(f"Short history (under {SHORT_HISTORY_DAYS} days): results are weaker; the thresholds are "
                     f"never lowered.")
        L.append("")
    L.append(f"The funnel: {fn['ideas']} ideas, {fn['past_tested']} tried on past prices, {fn['past_passed']} "
             f"passed; {fn['fresh_tested']} fresh looks, {fn['fresh_passed']} passed; {fn['bench']} on the "
             f"bench, {fn['practice']} practising, {fn['ready']} ready for live, {fn['binned']} binned; "
             f"{fn['waiting_for_calendar']} waiting for the calendar.")
    if night_row:
        L.append(f"Last night: {night_row.get('tested') or 0} tested in "
                 f"{round(float(night_row.get('seconds') or 0) / 60)} min "
                 f"({float(night_row.get('seconds_per_candidate') or 0):.1f} s each); "
                 f"{night_row.get('fresh') or 0} fresh looks, {night_row.get('fresh_passed') or 0} passed"
                 + ("; it ran out of time" if night_row.get("deadline_hit") else "") + ".")
    trials = summary.get("trials") or {}
    L.append(f"The honest count: {trials.get('lab', 0)} tests in all ({trials.get('past', 0)} on past prices, "
             f"{trials.get('fresh', 0)} fresh). The best of that many always looks good, so every result is "
             f"judged against that.")
    for n in notes:
        L.append(f"- Note: {n}")
    L += ["", "## Families", ""]
    for f in summary.get("families") or []:
        pbo = "n/a" if f.get("pbo") is None else f"{f['pbo']:.2f}"
        bits = [f"{f['label']} ({f['code']}): {f['n']} ideas, {f['tested']} tested this stretch, "
                f"{f['past_passed']} passed, PBO {pbo}"]
        if f.get("fits_noise"):
            bits.append("fits noise: its passes are held back")
        if f.get("extend_grid"):
            bits.append(f"settings at the edge of the range: {', '.join(f['extend_grid'])}")
        if f.get("dormant"):
            bits.append("asleep until the calendar has enough history")
        if f.get("no_numeric_setting"):
            bits.append("no setting to nudge: the fresh test decides")
        L.append("- " + "; ".join(bits))
    ctl = (trials.get("control") or {})
    if ctl.get("past_tested"):
        L += ["", f"The clock-trade control passed the past test {ctl['past_passed']} times in "
                  f"{ctl['past_tested']}" + (f" and the fresh test {ctl['fresh_passed']} in {ctl['fresh_tested']}"
                                             if ctl.get("fresh_tested") else "")
              + ": it is expected to fail, so this shows whether the gates let noise through."]
    if fresh_tonight:
        L += ["", "## Fresh looks tonight", ""]
        for f in fresh_tonight:
            L.append(f"- {f.get('name')} ({f.get('id')}): {f.get('outcome')}"
                     + (f", {_money(f.get('net_gbp'), syn)} over {f.get('trades')} trades, p {float(f['p']):.4f}"
                        if f.get("p") is not None else ""))
    pr = [c for c in summary.get("candidates") or [] if c["stage"] in PRACTISING + (ON_THE_BENCH,)]
    if pr:
        L += ["", "## Practising and on the bench", ""]
        for c in pr:
            L.append(f"- {c['name']} ({c['id']}), {c['stage']}: {c['headline']} {c['what_to_do']}"
                     + (" [TEST DATA]" if syn else ""))
    if practice_tonight:
        moved = (practice_tonight.get("ready") or []) + [b["id"] for b in practice_tonight.get("binned") or []]
        if moved or practice_tonight.get("promoted"):
            L += ["", f"Practice tonight: {len(practice_tonight.get('promoted') or [])} promoted, "
                      f"{len(practice_tonight.get('ready') or [])} ready, "
                      f"{len(practice_tonight.get('binned') or [])} binned."]
    ch = summary.get("changes_24h") or []
    if ch:
        L += ["", "## What changed in the last 24 hours", ""]
        for c in ch:
            L.append(f"- {c['when'][:16].replace('T', ' ')} {c.get('name') or ''} {c['text']}".replace("  ", " "))
    L += ["", f"Sources: {DB} (tests, events, families, practice_trades, looks, roster, runs); "
              f"data/lab/summary.json; data/lab/cut.json.", ""]
    return "\n".join(L)


def board_text(summary: Optional[dict], why: str = "") -> str:
    """``python -m mintel.lab show``: the board in plain text."""
    if summary is None:
        return f"The lab's summary could not be read ({why})." if why else "The lab has not run yet."
    syn = bool(summary.get("synthetic"))
    lines = [report_md(summary, night=(summary.get("nightly") or {}).get("date") or summary.get("written_utc", "")[:10])]
    if syn:
        lines.insert(0, TEST_DATA)
    return "\n".join(lines)


def write_board(paths: LabPaths, ledger, *, night: str, settings, universe: Sequence, store=None, cut=None,
                now: dt.datetime, run_started: str, waiting: int = 0, news_from: Optional[str] = None,
                notes: Sequence[str] = (), fresh_tonight: Sequence = (),
                practice_tonight: Optional[dict] = None) -> dict:
    """Nightly step 9. Returns {name: path} of what was written."""
    summary = build_summary(ledger, paths, settings=settings, universe=universe, store=store, cut=cut,
                            now=now, run_date=night, waiting=waiting, news_from=news_from)
    write_summary(paths, summary)
    run_dir = paths.run_dir(night)
    md = report_md(summary, night=night, notes=notes, fresh_tonight=fresh_tonight,
                   practice_tonight=practice_tonight)
    atomic_write(run_dir / "report.md", md)
    atomic_write(run_dir / "tests.csv", tests_csv(ledger, run_started))
    return {"summary": paths.summary, "report": run_dir / "report.md", "tests": run_dir / "tests.csv"}


def upload_files(paths: LabPaths, night: str) -> dict:
    """The files the nightly publishes, as {repository path: text}: the summary, the report, the
    tests, and on a Saturday the pool's refresh (refresh.md)."""
    run_dir = paths.run_dir(night)
    out = {}
    for repo_name, local in (("summary.json", paths.summary), ("report.md", run_dir / "report.md"),
                             ("tests.csv", run_dir / "tests.csv"), ("refresh.md", run_dir / "refresh.md")):
        try:
            out[f"reports/lab/{night}/{repo_name}"] = Path(local).read_text(encoding="utf-8")
        except OSError:
            continue
    return out
