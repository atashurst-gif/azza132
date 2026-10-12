"""``data/coach/status.json``: what the coach is doing, for the page, the
pulse and the watchdog (spec sections 8, 14 and 15).

Written atomically after every policy write (and on every tick, even when
the coach is off), so its age is the coach's liveness: the watchdog probes
this file's freshness. The coach writes NO heartbeat: a stale heartbeat that
is not on the known list would block Trend & Breakout's entries (C5).

Every money figure carries its source; nothing here is a projection.
"""
from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import Any, Optional

from . import STATUS_SCHEMA, Paths, as_utc

MAX_KEYS = 60
MAX_CHANGES = 40


def write(paths: Paths, doc: dict) -> Path:
    """Atomically: a temporary file, fsync, os.replace."""
    paths.coach_dir.mkdir(parents=True, exist_ok=True)
    out = paths.status
    tmp = out.with_name(out.name + ".tmp")
    body = json.dumps(doc, sort_keys=True, default=str, allow_nan=False, indent=1)
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(body)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, out)
    return out


def read(paths: Paths) -> Optional[dict]:
    try:
        obj = json.loads(paths.status.read_text(encoding="utf-8"))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def base(now: dt.datetime, mode: str, words: str, **extra: Any) -> dict:
    doc = {"schema": STATUS_SCHEMA, "written_utc": as_utc(now).isoformat(), "synthetic": False, "mode": mode,
           "words": words, "sources": {
               "money": "MetaTrader's record through the page's standing block (every bot, after commission)",
               "evidence": "each bot's own closed trades, read only (journal, runner, bandbreaker, crowd, ian)",
               "markets": "data/coach/weather.json, written by Trend & Breakout every 60 s",
               "envelope": "data/coach.json (Aaron's)"}}
    doc.update(extra)
    return doc


def one_line(doc: Optional[dict], now: Optional[dt.datetime] = None) -> str:
    """The icon's and ``show --once``'s line."""
    if not doc:
        return "The coach has not written its status yet."
    when = doc.get("written_utc") or ""
    age = ""
    try:
        t = dt.datetime.fromisoformat(str(when))
        secs = (as_utc(now or dt.datetime.now(dt.timezone.utc)) - as_utc(t)).total_seconds()
        age = f" · checked {int(max(0, secs))} seconds ago"
    except Exception:
        pass
    pol = doc.get("policy") or {}
    seq = f" · instruction {pol.get('seq')}" if pol.get("seq") else ""
    return f"The coach: {doc.get('words', '')}{seq}{age}"


def text(doc: Optional[dict], now: Optional[dt.datetime] = None) -> str:
    """``python -m mintel.coach show``: the status in plain text."""
    if not doc:
        return "The coach has not written its status yet."
    lines = [one_line(doc, now), ""]
    if doc.get("demo_line"):
        lines.append(doc["demo_line"])
    acct = doc.get("account") or {}
    if acct:
        lines.append("Account (MetaTrader, every bot, after commission): " + ", ".join(
            f"{k} {v}" for k, v in acct.items() if v is not None))
    br = doc.get("brakes") or {}
    if br:
        lines.append(f"Brakes: day {br.get('day')}, week {br.get('week')}, drawdown {br.get('drawdown')}; "
                     f"trial budget left £{br.get('trial_budget_left_gbp', 0):.2f}; headroom "
                     f"{'£%.2f' % doc['headroom_gbp'] if doc.get('headroom_gbp') is not None else 'not known'}")
    data = doc.get("data") or {}
    if data:
        lines.append("Data checks: " + ("ok" if data.get("ok") else "FAILED - " + "; ".join(data.get("failed") or [])))
    if doc.get("board"):
        lines.append(doc["board"])
    lines.append("")
    lines.append("Real money now:")
    for k in doc.get("keys") or []:
        if k.get("route") in ("REAL", "PIN") or k.get("state") in ("PLACED", "LIVE", "LIVE_TRIAL", "PINNED"):
            lines.append(f"  {k.get('words')}: {k.get('state')}"
                         + (f" at £{k['money']:.2f}" if k.get("money") else "") + (f" - {k['why']}" if k.get("why") else ""))
    plan = doc.get("plan") or {}
    if plan.get("items"):
        lines.append("")
        lines.append(f"Tonight's plan (released {plan.get('release_utc')} unless you veto an item):")
        for it in plan["items"]:
            lines.append(f"  ({it.get('item')}) {it.get('words')}")
    for title, name in (("Needs you", "needs_you"), ("Needs your yes", "needs_your_yes")):
        if doc.get(name):
            lines.append("")
            lines.append(title + ":")
            lines += [f"  - {x}" for x in doc[name]]
    if doc.get("changes"):
        lines.append("")
        lines.append("Recent decisions:")
        for c in doc["changes"][:15]:
            lines.append(f"  {c.get('ts_utc', '')[:16]} {c.get('words')}")
    return "\n".join(lines)
