"""The weekly refresh of the practice pool: retire clear losers, tweak the winners, add new ideas.

    python -m mintel.lab refresh --config data/config.json [--force]

Run by the nightly on a Saturday night (its ``refresh`` step), after the
trading week has closed and the weekly pick (Friday 22:30) has been made.
Aaron, 13 Oct: "with constant tweaks on each by you to improve their
performance and each week, X amount of winners go live the following week and
we add in new strategies to the mix to replace and continue this cycle".

1. RETIRE CLEAR LOSERS. A lab idea with at least RETIRE_MIN_TRADES (20) practice
   trades in the last WINDOW_DAYS (28) days, net <= 0 after costs and a profit
   factor under RETIRE_PF (0.9) comes off the practice roster: marked RETIRED
   with the reason (``LabLedger.retire``). Its stage, tests and trades are kept;
   nothing is deleted. The old bots and Trend & Breakout's parts are never
   retired here: the ones that fail the same rule are listed under "losing,
   needs a look" for the nightly review (read from data/weekly/latest.json, the
   weekly pick's own figures - the lab never imports the code that can switch a
   bot).
2. TWEAK THE WINNERS. For each of the TWEAK_TOP (10) lab ideas with the most
   practice GBP (positive, at least TWEAK_MIN_TRADES trades), up to
   VARIANTS_EACH (3) neighbours: the same family with one ``TUNABLE`` setting
   moved one step on its grid - halfway to the next value either side (the
   grid's own values are already in the pool), or one grid step past its edge.
   Each is a child of its parent (``parent_id``), never added twice, and starts
   with no record: it is judged only on its own practice trades from now on,
   never on its parent's.
3. ADD NEW IDEAS. Grid ideas not in the ledger yet - most often those of a new
   family module dropped into ``mintel/lab/families/`` - as many as there is
   room for under the pool's target.
4. KEEP THE POOL STEADY. The pool is every idea not retired; the target is
   ``pool_target`` in data/lab.json (640). The report says what happened:
   data/lab/runs/<night>/refresh.md (published as reports/lab/<night>/
   refresh.md) and the ``refresh`` block of summary.json (data/lab/refresh.json).

Variants and new ideas join practice at once (every idea practises); the
nightly's past test also tries them, as information only. Nothing here can
place an order.
"""
from __future__ import annotations

import datetime as dt
import json
from typing import Callable, Optional, Sequence

from . import RETIRED, UTC, Candidate, LabPaths, atomic_write, atomic_write_json, utc_stamp
from .candidate import family_of, name_of, registered, short_name_of

RETIRE_MIN_TRADES = 20
RETIRE_PF = 0.9
WINDOW_DAYS = 28
TWEAK_TOP = 10
TWEAK_MIN_TRADES = 10
VARIANTS_EACH = 3
LISTED = 40                            # names carried in summary.json per list (the .md has them all)

Log = Callable[[str], None]


def _quiet(_msg: str) -> None:
    pass


# ------------------------------------------------------------- the rules --
def profit_factor(nets: Sequence[float]) -> Optional[float]:
    """Money won over money lost; None with no losing trade."""
    won = sum(x for x in nets if x > 0)
    lost = -sum(x for x in nets if x < 0)
    return (won / lost) if lost > 0 else None


def is_clear_loser(rec: dict) -> bool:
    """The retire rule: at least 20 trades in the window, net <= 0 after costs,
    and a profit factor under 0.9 (no losing trade at all is never a loser)."""
    pf = rec.get("pf")
    return (int(rec.get("trades") or 0) >= RETIRE_MIN_TRADES and float(rec.get("net") or 0.0) <= 0
            and pf is not None and pf < RETIRE_PF)


def records(ledger, since_utc: str) -> dict:
    """{idea id: {trades, net, pf}} of each idea's own PRACTICE trades closed since ``since_utc``."""
    by: dict = {}
    for r in ledger.query("SELECT candidate_id, net_gbp FROM practice_trades WHERE mode='PRACTICE'"
                          " AND closed_utc IS NOT NULL AND closed_utc >= ? AND net_gbp IS NOT NULL", (since_utc,)):
        by.setdefault(r[0], []).append(float(r[1]))
    return {cid: {"trades": len(n), "net": round(sum(n), 2), "pf": profit_factor(n)} for cid, n in by.items()}


def neighbour_values(axis: Sequence, v, lo: float, hi: float) -> list:
    """The values one step from ``v`` on a setting's grid: halfway to the next
    value below and above it (``v`` itself may be off the grid, a variant's),
    then one grid step past the edge when ``v`` is at one. Within [lo, hi];
    whole numbers stay whole; never ``v`` or a grid value itself."""
    grid = sorted({float(x) for x in axis})
    whole = all(isinstance(x, int) and not isinstance(x, bool) for x in axis) and isinstance(v, int)
    if len(grid) < 2:
        return []
    pts = sorted(set(grid) | {float(v)})
    k = pts.index(float(v))
    below = pts[k - 1] if k > 0 else None
    above = pts[k + 1] if k < len(pts) - 1 else None
    out = []
    if below is not None:
        out.append((below + v) / 2.0)
    if above is not None:
        out.append((v + above) / 2.0)
    step = min(b - a for a, b in zip(grid, grid[1:]))
    if below is None:
        out.append(v - step)
    if above is None:
        out.append(v + step)
    got = []
    for x in out:
        x = int(round(x)) if whole else round(x, 4)
        if lo <= x <= hi and float(x) != float(v) and float(x) not in grid and x not in got:
            got.append(x)
    return got


def variants_of(c: Candidate, known: set, n: int = VARIANTS_EACH) -> list:
    """Up to ``n`` neighbours of an idea not already ``known``: its family's
    TUNABLE settings in order, each moved one step (``neighbour_values``)."""
    fam = family_of(c)
    out = []
    for param, (lo, hi) in (getattr(fam, "TUNABLE", None) or {}).items():
        v = c.p(param)
        if v is None:
            continue
        for x in neighbour_values(fam.AXES.get(param, ()), v, lo, hi):
            kid = c.replace(params={param: x})
            if kid.id not in known and kid.id not in {k.id for k in out}:
                out.append(kid)
            if len(out) >= n:
                return out
    return out


def waiting_ideas(known: set, news_awake: bool = False) -> list:
    """Grid ideas of every registered family (a new family module included) not in the ledger yet."""
    return [c for c in registered(news_awake) if c.id not in known]


def losing_elsewhere(data_dir) -> list:
    """The old bots and Trend & Breakout's parts that fail the retire rule on the
    weekly pick's own figures (data/weekly/latest.json): listed, never retired."""
    try:
        plan = json.loads((LabPaths(data_dir).data_dir / "weekly" / "latest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    rows = plan.get("bots") if isinstance(plan, dict) else None
    if not isinstance(rows, list):
        rows = [r for r in (plan.get("ranking") or []) if isinstance(r, dict) and r.get("kind") in ("bot", "tnb")]
    out = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        rec = {"trades": r.get("trades"), "net": r.get("net"), "pf": r.get("profit_factor")}
        if is_clear_loser(rec):
            name = f"{r.get('label') or ''} {r.get('part') or ''}".strip() or str(r.get("id"))
            out.append({"id": r.get("id"), "name": name, **rec})
    return out


# ------------------------------------------------------------- the step --
def pool_size(ledger) -> int:
    return int(ledger.one("SELECT COUNT(*) FROM candidates WHERE retired_utc IS NULL")[0])


def refresh_pool(ledger, paths: LabPaths, settings, *, night: str, now: dt.datetime, news_awake: bool = False,
                 force: bool = False, log: Log = _quiet) -> dict:
    """The four steps, once a night (data/lab/refresh.json says which night;
    ``force`` runs it again). Returns the report it wrote."""
    done = _read(paths.root / "refresh.json")
    if not force and isinstance(done, dict) and done.get("night") == night:
        return done
    since = (now - dt.timedelta(days=WINDOW_DAYS)).astimezone(UTC).replace(microsecond=0).isoformat()
    recs = records(ledger, since)
    rows = {r["id"]: r for r in ledger.candidates()}
    live = {cid: r for cid, r in rows.items() if not r.get("retired_utc")}

    # 1. retire clear losers (lab ideas only; the bots are listed, never retired here)
    retired = []
    for cid in sorted(live):
        rec = recs.get(cid)
        if rec and is_clear_loser(rec):
            why = (f"a clear loser on its own practice: {rec['trades']} trades in {WINDOW_DAYS} days, "
                   f"£{rec['net']:,.2f} after costs, profit factor {rec['pf']:.2f} (retired at "
                   f"{RETIRE_MIN_TRADES}+ trades, net <= 0 and under {RETIRE_PF})")
            if ledger.retire(cid, why, now=now):
                retired.append({"id": cid, "name": _short(rows[cid]), "why": why, **rec})
                live.pop(cid, None)
    # 2. tweak the winners
    winners = sorted(((cid, rec) for cid, rec in recs.items() if cid in live and rec["net"] > 0
                      and rec["trades"] >= TWEAK_MIN_TRADES), key=lambda x: (-x[1]["net"], x[0]))[:TWEAK_TOP]
    known = ledger.known_ids()
    variants = []
    for cid, rec in winners:
        try:
            from .stages import cand_of
            parent = cand_of(rows[cid])
            kids = variants_of(parent, known)
        except Exception as exc:
            log(f"refresh: {cid} cannot be tweaked: {exc}")
            continue
        for kid in kids:
            if ledger.register(kid, name_of(kid), parent_id=cid, now=now):
                known.add(kid.id)
                variants.append({"id": kid.id, "name": short_name_of(kid), "parent": cid,
                                 "parent_name": _short(rows[cid]), "parent_net": rec["net"]})
    # 3 and 4. new ideas, as many as there is room for under the target
    size = pool_size(ledger)
    target = int(getattr(settings, "pool_target", 640))
    room = max(0, target - size)
    added = []
    if room:
        for c in waiting_ideas(known, news_awake)[:room]:
            if ledger.register(c, name_of(c), now=now):
                known.add(c.id)
                added.append({"id": c.id, "name": short_name_of(c), "family": c.family})
    size = pool_size(ledger)
    rep = {"night": night, "written_utc": utc_stamp(now), "window_days": WINDOW_DAYS,
           "retired": retired, "variants": variants, "new": added, "pool": size, "target": target,
           "waiting_ideas": max(0, len(waiting_ideas(known, news_awake))),
           "losing_needs_a_look": losing_elsewhere(paths.data_dir),
           "rule": (f"retired: {RETIRE_MIN_TRADES}+ practice trades in {WINDOW_DAYS} days, net <= 0 after costs and "
                    f"a profit factor under {RETIRE_PF}; tweaked: the top {TWEAK_TOP} by practice £ (positive, "
                    f"{TWEAK_MIN_TRADES}+ trades), up to {VARIANTS_EACH} neighbours each, judged on their own trades")}
    atomic_write_json(paths.root / "refresh.json", rep)
    atomic_write(paths.run_dir(night) / "refresh.md", refresh_md(rep))
    log(f"refresh: {len(retired)} retired, {len(variants)} variants, {len(added)} new; pool {size} of {target}")
    return rep


def summary_block(rep: Optional[dict]) -> Optional[dict]:
    """The refresh as summary.json carries it: the counts, and at most LISTED names a list."""
    if not isinstance(rep, dict):
        return None
    out = {k: rep.get(k) for k in ("night", "written_utc", "pool", "target", "waiting_ideas", "rule")}
    for k in ("retired", "variants", "new", "losing_needs_a_look"):
        items = rep.get(k) or []
        out[f"{k}_count"] = len(items)
        out[k] = [{"id": x.get("id"), "name": x.get("name")} for x in items[:LISTED]]
    out["source"] = "data/lab/refresh.json"
    return out


def refresh_md(rep: dict) -> str:
    """reports/lab/<night>/refresh.md: short, plain."""
    L = [f"# Practice pool refresh, {rep['night']}", "",
         f"Pool: {rep['pool']} ideas practising (target {rep['target']}). "
         f"Retired {len(rep['retired'])}, variants added {len(rep['variants'])}, new added {len(rep['new'])}."
         + (f" {rep['waiting_ideas']} grid ideas still waiting." if rep.get("waiting_ideas") else
            " No grid ideas left waiting: a new family module adds more."), "",
         f"Rule: {rep['rule']}.", "", "## Retired (clear losers)", ""]
    L += [f"- {x['name']} ({x['id']}): {x['why']}" for x in rep["retired"]] or ["- none"]
    L += ["", "## Variants of the winners (each judged on its own trades)", ""]
    L += [f"- {x['name']} ({x['id']}), from {x['parent_name']} ({x['parent']}, £{x['parent_net']:,.2f})"
          for x in rep["variants"]] or ["- none"]
    L += ["", "## New ideas", ""]
    L += [f"- {x['name']} ({x['id']})" for x in rep["new"]] or ["- none"]
    L += ["", "## Losing, needs a look (bots and Trend & Breakout parts: never retired automatically)", ""]
    L += [f"- {x['name']}: {x['trades']} trades, £{float(x['net'] or 0):,.2f}, profit factor "
          f"{float(x['pf']):.2f}" for x in rep["losing_needs_a_look"]] or ["- none"]
    L += ["", f"Sources: data/lab/lab.sqlite (practice_trades, candidates, events kind='{RETIRED.lower()}'), "
              "data/weekly/latest.json (the bots' figures)."]
    return "\n".join(L) + "\n"


def _short(row: dict) -> str:
    try:
        from .stages import cand_of
        return short_name_of(cand_of(row))
    except Exception:
        return str(row.get("name") or row.get("id"))


def _read(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
