"""The nightly digest and its upload (spec sections 8, 14 and 15).

Each night the coach writes, under ``data/coach/``:

    report-<date>.md                 the digest in plain words
    reports/<date>/report.md         the same
    reports/<date>/decisions.csv     every change of the day (when, what, why, the rule, the numbers)
    reports/<date>/plan.json         tomorrow's game plan
    reports/<date>/policy.json       the policy in force when the digest was written

and publishes them as ``reports/coach/<date>/`` through
``report_upload.upload(..., day=f"coach/{date}")`` - and only through it
(this module never edits the uploader).

The GitHub token lives only in the secrets file (``data/``), which the coach never
opens. :func:`publish` therefore takes the token from its caller; the coach's
own run loop has none, so it leaves the files in ``data/coach/reports/`` and
says so. The pulse, which already holds the token, can send them with
:func:`upload_files` (Builder G's wiring), as it does the lab's.
"""
from __future__ import annotations

import csv
import io
import json
import os
from pathlib import Path
from typing import Callable, Optional, Sequence

from . import Paths

FLATTERS = "flatters: the segments were picked by people looking at this same record"


def _atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def run_dir(paths: Paths, date: str) -> Path:
    return paths.coach_dir / "reports" / date


def decisions_csv(changes: Sequence) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["ts_utc", "policy_seq", "kind", "target", "from", "to", "rule", "by", "words", "numbers"])
    for c in sorted(changes, key=lambda c: (c.ts_utc or "", c.id or 0)):
        w.writerow([c.ts_utc, c.policy_seq, c.kind, c.target, c.from_state, c.to_state, c.rule, c.by, c.words,
                    json.dumps(c.numbers, sort_keys=True, default=str)])
    return buf.getvalue()


def digest_md(date: str, *, status: dict, plan: dict, changes: Sequence, luck: Optional[dict],
              scorecard: Optional[dict], suggestions: Sequence[str] = ()) -> str:
    """The digest: what changed today and why, tomorrow's plan (with the veto command), the luck check, the
    scorecard, what needs Aaron, and suggestions for Claude's review. Every money figure says where it comes
    from; nothing is a projection."""
    out = [f"# The coach - {date}", "", status.get("words", ""), ""]
    if status.get("demo_line"):
        out += [status["demo_line"], ""]
    out += ["## What changed today", ""]
    if changes:
        for c in sorted(changes, key=lambda c: (c.ts_utc or "", c.id or 0)):
            out.append(f"- {str(c.ts_utc)[:16]} UTC - {c.words} ({c.rule or c.kind})")
    else:
        out.append("- nothing")
    out += ["", "## Tomorrow's plan", ""]
    items = plan.get("items") or []
    if items:
        out.append(f"Released at {plan.get('release_utc')} unless vetoed: "
                   "`python -m mintel.coach veto <item>` (or The Coach icon, option 5).")
        for it in items:
            out.append(f"- ({it.get('item')}) {it.get('words')}")
    else:
        out.append("- nothing moves up tonight.")
    for f in plan.get("floors") or []:
        out.append(f"- ({f.get('item')}) floor {f.get('action')}: {f.get('key')} {f.get('field')} {f.get('value')}")
    out += ["", "## The luck check", ""]
    if luck:
        out.append(f"{luck.get('passed', 0)} of {luck.get('tested', 0)} made-up segments passed tonight's test "
                   f"({float(luck.get('rate', 0.0)):.1%}); "
                   + ("promotions blocked." if luck.get("blocked") else "promotions allowed."))
    else:
        out.append("Not run.")
    out += ["", "## The coach's own scorecard", ""]
    if scorecard:
        out.append(f"Saved {scorecard.get('saved_gbp')} and added {scorecard.get('added_gbp')} (net "
                   f"{scorecard.get('net_gbp')}) over {scorecard.get('trades')} affected trades - from the bots' own "
                   f"records, priced at the normal size.")
    else:
        out.append("Nothing to score yet.")
    for title, name in (("Needs you", "needs_you"), ("Needs your yes", "needs_your_yes")):
        out += ["", f"## {title}", ""]
        out += [f"- {x}" for x in (status.get(name) or ["nothing"])]
    if suggestions:
        out += ["", "## For Claude's review (suggestions only; nothing here is applied)", ""]
        out += [f"- {s}" for s in suggestions]
    out.append("")
    return "\n".join(out)


def write_night(paths: Paths, date: str, *, status: dict, plan: dict, changes: Sequence, luck: Optional[dict],
                scorecard: Optional[dict], policy_obj: Optional[dict], suggestions: Sequence[str] = ()) -> dict:
    md = digest_md(date, status=status, plan=plan, changes=changes, luck=luck, scorecard=scorecard,
                   suggestions=suggestions)
    d = run_dir(paths, date)
    _atomic(paths.report(date), md)
    _atomic(d / "report.md", md)
    _atomic(d / "decisions.csv", decisions_csv(changes))
    _atomic(d / "plan.json", json.dumps(plan, sort_keys=True, indent=1, default=str))
    _atomic(d / "policy.json", json.dumps(policy_obj or {}, sort_keys=True, indent=1, default=str))
    _atomic(paths.plan(date), json.dumps(plan, sort_keys=True, indent=1, default=str))
    return {"report": d / "report.md", "decisions": d / "decisions.csv", "plan": d / "plan.json",
            "policy": d / "policy.json"}


def upload_files(paths: Paths, date: str) -> dict:
    """The night's files as {repository path: text} (``reports/coach/<date>/...``)."""
    d = run_dir(paths, date)
    out = {}
    for name in ("report.md", "decisions.csv", "plan.json", "policy.json"):
        try:
            out[f"reports/coach/{date}/{name}"] = (d / name).read_text(encoding="utf-8")
        except OSError:
            continue
    return out


def publish(files: dict, date: str, *, repo: str, branch: str, token: str,
            upload_fn: Optional[Callable] = None) -> int:
    """Send the night's files through ``report_upload.upload`` (day ``coach/<date>``). The token comes from
    the caller: the coach never reads the secrets file."""
    if not token:
        raise ValueError("no token was handed to the coach's report: the files stay in data/coach/reports")
    if upload_fn is None:
        from ..ops.report_upload import upload as upload_fn          # the one uploader (section 3)
    return upload_fn(files, repo, branch, token, day=f"coach/{date}")
