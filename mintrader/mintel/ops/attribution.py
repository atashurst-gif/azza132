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
                       "regime": r.get("regime"), "closed_utc": r.get("closed_utc")})
    opens = [{"symbol": p.symbol, "pnl": float(getattr(p, "profit", 0.0) or 0.0)} for p in positions]
    s = strategy_stats(closed, opens, label=EXISTING_STRATEGY_LABEL, strategy_id=EXISTING_STRATEGY_ID, currency=currency)
    s["by_approach"] = breakdown(closed, lambda r: approach_name(r.get("tactic"), r.get("regime")))
    s["by_market_type"] = breakdown(closed, lambda r: market_type(r.get("symbol")))
    if ledger_today:
        # the broker's own figures win where they exist, exactly as the main tiles do
        s["realised"] = round(float(ledger_today.get("net", s["realised"])), 2)
        s["net_today"] = round(s["realised"] + s["unrealised"], 2)
        s["trades"] = int(ledger_today.get("trades", s["trades"]))
        s["wins"] = int(ledger_today.get("wins", s["wins"]))
        s["losses"] = max(0, s["trades"] - s["wins"])
        s["win_rate"] = ledger_today.get("win_rate", s["win_rate"])
        if ledger_today.get("commission") is not None:
            s["total_costs"] = round(float(ledger_today["commission"]), 2)
            s["before_fees"] = ledger_today.get("before_fees")
        nets = [float(x) for x in (ledger_today.get("nets") or [])]
        if nets:
            s.update(_money_shape(nets))
    s["trades_today"] = [{"ticket": r["ticket"], "symbol": r["symbol"], "net": r["net_pnl"],
                          "closed": r["closed_utc"], "tactic": r["tactic"], "strategy": EXISTING_STRATEGY_ID}
                         for r in closed[-60:]]
    return s


def _money_shape(nets: list) -> dict:
    """Average and largest win and loss, and profit factor, from per-trade nets."""
    wins = [x for x in nets if x > 0]
    losses = [x for x in nets if x < 0]
    pf = (round(sum(wins) / abs(sum(losses)), 2) if losses else ("no losses" if wins else None))
    return {"avg_win": round(sum(wins) / len(wins), 2) if wins else None,
            "avg_loss": round(sum(losses) / len(losses), 2) if losses else None,
            "largest_win": round(max(wins), 2) if wins else None,
            "largest_loss": round(min(losses), 2) if losses else None,
            "profit_factor": pf}


def approach_name(tactic, regime) -> str:
    t = str(tactic or "unknown").replace("_", " ").lower().capitalize()
    g = str(regime or "").split(" ")[0].replace("_", " ").lower()
    return f"{t} in a {g} market" if g else t


def market_type(symbol) -> str:
    from ..contracts import infer_group
    g = infer_group(str(symbol or ""))
    return {"INDEX": "Indices (no commission)", "GOLD": "Gold", "SILVER": "Silver",
            "ENERGY": "Oil and gas"}.get(g, "FX pairs" if g.startswith("FX") else g.title())


def breakdown(closed, key) -> list[dict]:
    """Trades, wins and net per group, best first (the broker's net where the
    journal has it; the nightly review reconciles to the broker)."""
    groups: dict[str, list[float]] = {}
    for r in closed:
        groups.setdefault(key(r), []).append(float(r.get("net_pnl") or 0.0))
    out = [{"name": k, "trades": len(v), "wins": sum(1 for x in v if x > 0), "net": round(sum(v), 2)}
           for k, v in groups.items()]
    return sorted(out, key=lambda x: -x["net"])


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
    # Overall is the ACCOUNT: money that really moved. A scalper in PAPER is
    # simulated, so it has its own tab but is never added to the account.
    live_rs = str(rs_status.get("mode") or "").upper() == "LIVE"
    overall = combine([mi, rs] if live_rs else [mi])
    overall["trades_today"] = sorted(mi["trades_today"] + (rs["trades_today"] if live_rs else []),
                                     key=lambda t: str(t.get("closed") or ""))
    # the money shape of the account: from every real trade's net
    nets = [float(t.get("net") or 0.0) for t in overall["trades_today"]]
    if nets:
        overall.update(_money_shape(nets))
    if mi.get("before_fees") is not None and not live_rs:
        overall["before_fees"] = mi["before_fees"]
    if not live_rs:
        rs["source"] = rs.get("source") or "simulated (PAPER) - not money, not in Overall"
        overall["note"] = "The Rapid Scalper is in PAPER: its simulated results are on its own tab only."
    return {"overall": overall, EXISTING_STRATEGY_ID: mi, STRATEGY_ID: rs, "scalper": rs_status,
            "labels": {"overall": "Overall", EXISTING_STRATEGY_ID: EXISTING_STRATEGY_LABEL, STRATEGY_ID: STRATEGY_LABEL}}


# ------------------------------------------------------------- periods --
PERIODS = (("today", "Today"), ("yesterday", "Yesterday"), ("week", "This week"),
           ("month", "This month"), ("6m", "Last 6 months"), ("1y", "Last year"))


def period_bounds(period: str, now: Optional[dt.datetime] = None,
                  date_from: str = "", date_to: str = "") -> tuple[dt.datetime, dt.datetime, str]:
    """(start_utc, end_utc, label) for a page period.

    Day boundaries follow the computer's local clock (the page is read on the
    Mac), converted to UTC for the journals. A custom range takes two ISO
    dates (YYYY-MM-DD) and is inclusive of both days."""
    now = to_utc(now or utcnow())
    local = now.astimezone()
    tz = local.tzinfo
    day0 = local.replace(hour=0, minute=0, second=0, microsecond=0)
    one = dt.timedelta(days=1)
    if period == "custom" and (date_from or date_to):
        try:
            a = dt.date.fromisoformat(date_from) if date_from else dt.date.fromisoformat(date_to)
            b = dt.date.fromisoformat(date_to) if date_to else a
        except ValueError:
            a = b = local.date()
        if b < a:
            a, b = b, a
        start = dt.datetime(a.year, a.month, a.day, tzinfo=tz)
        end = dt.datetime(b.year, b.month, b.day, tzinfo=tz) + one
        label = f"{a.isoformat()} to {b.isoformat()}" if a != b else a.isoformat()
    elif period == "yesterday":
        start, end, label = day0 - one, day0, "Yesterday"
    elif period == "week":
        start, end, label = day0 - dt.timedelta(days=day0.weekday()), day0 + one, "This week"
    elif period == "month":
        start, end, label = day0.replace(day=1), day0 + one, "This month"
    elif period == "6m":
        start, end, label = day0 - dt.timedelta(days=182), day0 + one, "Last 6 months"
    elif period == "1y":
        start, end, label = day0 - dt.timedelta(days=365), day0 + one, "Last year"
    else:
        start, end, label = day0, day0 + one, "Today"
    return to_utc(start), to_utc(end), label


def _rows_ro(path: Path, sql: str, args: tuple) -> list[dict]:
    """Read rows from a sqlite file without ever writing to it.

    Opened fresh per call so it is safe from the page's own thread while the
    bots keep their own connections."""
    import sqlite3
    if not path.exists():
        return []
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2.0)
    try:
        con.row_factory = sqlite3.Row
        return [dict(r) for r in con.execute(sql, args).fetchall()]
    except sqlite3.Error:
        return []
    finally:
        con.close()


def build_strategies_range(data_dir: str | Path, start: dt.datetime, end: dt.datetime,
                           currency: str = "GBP", positions=(), scalper_open=(),
                           label: str = "", now: Optional[dt.datetime] = None) -> dict:
    """Strategy figures for a date range, from the bots' own journals.

    Today's figures on the page come from the broker's ledger; any other
    period comes from these records, and the page says so."""
    data = Path(data_dir)
    a, b = to_utc(start).isoformat(), to_utc(end).isoformat()
    includes_now = to_utc(start) <= to_utc(now or utcnow()) < to_utc(end)
    mi_rows = _rows_ro(data / "journal.sqlite",
                       "SELECT * FROM trades WHERE closed_utc IS NOT NULL AND closed_utc >= ? AND closed_utc < ? "
                       "ORDER BY closed_utc ASC LIMIT 5000", (a, b))
    closed = []
    for r in mi_rows:
        try:
            dur = (to_utc(dt.datetime.fromisoformat(r["closed_utc"]))
                   - to_utc(dt.datetime.fromisoformat(r["opened_utc"]))).total_seconds()
        except Exception:
            dur = None
        closed.append({"net_pnl": r.get("pnl_money") or 0.0, "duration_seconds": dur,
                       "commission": r.get("commission"), "symbol": r.get("symbol"),
                       "ticket": r.get("ticket"), "tactic": r.get("tactic"), "exit_reason": r.get("exit_reason"),
                       "closed_utc": r.get("closed_utc")})
    opens = [{"symbol": p.symbol, "pnl": float(getattr(p, "profit", 0.0) or 0.0)} for p in positions] if includes_now else []
    mi = strategy_stats(closed, opens, label=EXISTING_STRATEGY_LABEL, strategy_id=EXISTING_STRATEGY_ID, currency=currency)
    mi["trades_today"] = [{"ticket": r["ticket"], "symbol": r["symbol"], "net": r["net_pnl"], "closed": r["closed_utc"],
                           "tactic": r["tactic"], "strategy": EXISTING_STRATEGY_ID} for r in closed]
    rs_rows = _rows_ro(data / "scalper.sqlite",
                       "SELECT * FROM trades WHERE closed_utc IS NOT NULL AND closed_utc >= ? AND closed_utc < ? "
                       "ORDER BY closed_utc ASC LIMIT 5000", (a, b))
    rs = strategy_stats(rs_rows, list(scalper_open) if includes_now else [], label=STRATEGY_LABEL,
                        strategy_id=STRATEGY_ID, currency=currency, scalper=True)
    rs["trades_today"] = [{"ticket": r["ticket"], "symbol": r["symbol"], "net": r.get("net_pnl"), "closed": r["closed_utc"],
                           "exit_reason": r.get("exit_reason"), "mode": r.get("mode"), "strategy": STRATEGY_ID} for r in rs_rows]
    # the account total only ever counts trades that really happened
    live_rows = [r for r in rs_rows if str(r.get("mode") or "").upper() == "LIVE"]
    rs_live = strategy_stats(live_rows, [], label=STRATEGY_LABEL, strategy_id=STRATEGY_ID,
                             currency=currency, scalper=True)
    overall = combine([mi, rs_live])
    overall["trades_today"] = sorted(mi["trades_today"] + [t for t in rs["trades_today"]
                                                           if str(t.get("mode") or "").upper() == "LIVE"],
                                     key=lambda t: str(t.get("closed") or ""))
    return {"overall": overall, EXISTING_STRATEGY_ID: mi, STRATEGY_ID: rs,
            "scalper": read_scalper_status(data_dir, now=now),
            "labels": {"overall": "Overall", EXISTING_STRATEGY_ID: EXISTING_STRATEGY_LABEL, STRATEGY_ID: STRATEGY_LABEL},
            "period": {"key": "custom" if label and label[:1].isdigit() else label.lower(), "label": label,
                       "start": a, "end": b, "source": "the bots' own records"}}


def make_period_resolver(data_dir: str | Path, currency_fn=None, positions_fn=None):
    """A callable the page uses for any period other than today."""
    def resolve(period: str, date_from: str = "", date_to: str = "") -> dict:
        now = utcnow()
        start, end, label = period_bounds(period, now, date_from, date_to)
        currency = "GBP"
        positions = ()
        try:
            currency = currency_fn() if currency_fn else "GBP"
        except Exception:
            pass
        try:
            positions = positions_fn() if positions_fn else ()
        except Exception:
            pass
        out = build_strategies_range(data_dir, start, end, currency, positions, (), label, now)
        out["period"]["key"] = period
        out["period"]["from"] = date_from
        out["period"]["to"] = date_to
        return out
    return resolve
