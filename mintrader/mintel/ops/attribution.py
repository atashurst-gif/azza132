"""Strategy attribution for the status page: Overall | Trend & Breakout |
Rapid Scalper. Read-only. It reads the existing strategy's own records and
the Rapid Scalper's status file, and adds the money up. It changes
nothing about either strategy."""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Optional

from ..clock import to_utc, utcnow
from ..scalper import (EXISTING_STRATEGY_ID, EXISTING_STRATEGY_LABEL, STRATEGY_ID,
                       STRATEGY_LABEL)
from ..scalper.stats import combine, strategy_stats

STALE_SECONDS = 120.0


def existing_strategy_stats(journal, positions, day_start: dt.datetime, currency: str,
                            ledger_today: Optional[dict] = None) -> dict:
    rows = []
    try:
        rows = journal.closed_trades(limit=500, since=day_start)
    except Exception:
        rows = []
    closed = []
    for r in rows:
        try:
            o = dt.datetime.fromisoformat(r["opened_utc"]); c = dt.datetime.fromisoformat(r["closed_utc"])
            dur = (to_utc(c) - to_utc(o)).total_seconds()
        except Exception:
            dur = None
        closed.append({"net_pnl": r.get("pnl_money") or 0.0, "duration_seconds": dur,
                       "commission": r.get("commission"), "symbol": r.get("symbol"),
                       "ticket": r.get("ticket"), "tactic": r.get("tactic"), "exit_reason": r.get("exit_reason"),
                       "closed_utc": r.get("closed_utc")})
    opens = [{"symbol": p.symbol, "pnl": float(getattr(p, "profit", 0.0) or 0.0)} for p in positions]
    s = strategy_stats(closed, opens, label=EXISTING_STRATEGY_LABEL, strategy_id=EXISTING_STRATEGY_ID, currency=currency)
    if ledger_today:
        # the broker's own figures win where they exist, exactly as the main tiles do
        s["realised"] = round(float(ledger_today.get("net", s["realised"])), 2)
        s["net_today"] = round(s["realised"] + s["unrealised"], 2)
        s["trades"] = int(ledger_today.get("trades", s["trades"]))
        s["wins"] = int(ledger_today.get("wins", s["wins"]))
        s["losses"] = max(0, s["trades"] - s["wins"])
        s["win_rate"] = ledger_today.get("win_rate", s["win_rate"])
    s["trades_today"] = [{"ticket": r["ticket"], "symbol": r["symbol"], "net": r["net_pnl"],
                          "closed": r["closed_utc"], "tactic": r["tactic"], "strategy": EXISTING_STRATEGY_ID}
                         for r in closed[-60:]]
    return s


def read_scalper_status(data_dir: str | Path, status_file: str = "scalper-status.json",
                        now: Optional[dt.datetime] = None) -> dict:
    p = Path(data_dir) / status_file
    now = to_utc(now or utcnow())
    try:
        d = json.loads(p.read_text())
    except Exception:
        return {"strategy_id": STRATEGY_ID, "label": STRATEGY_LABEL, "status": "NOT RUNNING",
                "mode": "OFF", "stats": strategy_stats([], [], label=STRATEGY_LABEL, strategy_id=STRATEGY_ID, scalper=True),
                "present": False}
    try:
        age = (now - to_utc(dt.datetime.fromisoformat(d["updated"]))).total_seconds()
    except Exception:
        age = None
    d["present"] = True
    d["age_seconds"] = age
    if age is not None and age > STALE_SECONDS:
        d["status"] = f"NOT RUNNING (last seen {age / 60:.0f} min ago)"
    return d


def build_strategies(journal, positions, day_start: dt.datetime, currency: str,
                     data_dir: str | Path, ledger_today: Optional[dict] = None,
                     now: Optional[dt.datetime] = None) -> dict:
    mi = existing_strategy_stats(journal, positions, day_start, currency, ledger_today)
    rs_status = read_scalper_status(data_dir, now=now)
    rs = dict(rs_status.get("stats") or {})
    rs.setdefault("id", STRATEGY_ID); rs.setdefault("label", STRATEGY_LABEL); rs.setdefault("currency", currency)
    rs["trades_today"] = rs_status.get("trades_today", [])
    overall = combine([mi, rs])
    overall["trades_today"] = sorted(mi["trades_today"] + rs["trades_today"],
                                     key=lambda t: str(t.get("closed") or ""))
    return {"overall": overall, EXISTING_STRATEGY_ID: mi, STRATEGY_ID: rs, "scalper": rs_status,
            "labels": {"overall": "Overall", EXISTING_STRATEGY_ID: EXISTING_STRATEGY_LABEL, STRATEGY_ID: STRATEGY_LABEL}}
