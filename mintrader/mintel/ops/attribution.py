"""Strategy attribution for the status page: Overall | Trend & Breakout |
Rapid Momentum Rider | Momentum Runner | Band Breaker | Crowd Fader |
Financial Ian, and the retired Rapid Scalper. Read-only. It reads each bot's
own records and status file and adds the money up. It changes nothing about
any bot.

The Rapid Momentum Rider (rider.sqlite, rider-status.json) books its money
as net_money with money_source ('broker', 'paper' or 'estimate'); Financial
Ian (ian.sqlite, ian-status.json) books net_pnl with a data_source, and a
SYNTHETIC or REPLAY row is test data, never a result: it is left out here.

Overall is the ACCOUNT: money that really moved. Trend & Breakout is real
while it is LIVE (config.json "tnb" -> "mode", LIVE when it says nothing),
exactly as before. On PAPER its orders are simulated (mintel/broker/paper.py)
and its journal holds paper trades: each journal row is PAPER when its
ticket was born on the paper record (data/tnb_paper.sqlite, read-only),
with that record's money (every deal of the position, the entry commission
too), and LIVE otherwise (a real position left from LIVE, still real money).
Only its LIVE rows are added to Overall; the tab says PAPER. Every other
bot's rows carry a mode, LIVE or PAPER (a row with no mode is PAPER:
simulated, never money), and only its LIVE rows are added to Overall, under
the bot's own strategy id. PAPER rows stay on the bot's own tab, where the
figures cover every row for the period and say how much of it was live and
how much paper (live_net, paper_net).

A LIVE row's figure is what the engine booked from the broker's closing
deals (profit, commission and swap on the deal that closed the position).
Where a market charges commission the entry half is not in that figure, so
`python -m mintel.ops.reconcile`, which sums every deal, is the figure that
must match MetaTrader. A row the broker's record could not be read for is
booked at the last price with a '?' on its exit reason: it still counts (it
is the best figure there is) but is counted separately (estimated_trades,
estimated_net) and the page says so."""
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


def row_mode(r: dict) -> str:
    """How a row was traded. The engines write LIVE or PAPER; a row with no
    mode (an older file) is PAPER: simulated, never money."""
    return str(r.get("mode") or "PAPER").upper()


def is_estimated(r: dict) -> bool:
    """A LIVE row booked from the last price because the broker's record was
    not there: the engines put a '?' on the exit reason (STOP?, WINDOW_END?)."""
    return row_mode(r) == "LIVE" and str(r.get("exit_reason") or "").endswith("?")


def estimated_split(rows) -> dict:
    """How many LIVE rows are estimates, and their net."""
    est = [float(r.get("net_pnl") or 0.0) for r in rows if is_estimated(r)]
    return {"estimated_trades": len(est), "estimated_net": round(sum(est), 2)}


def estimated_note(n: int) -> str:
    """The plain statement the page carries when LIVE rows are estimates."""
    return (f"{n} LIVE trade{'' if n == 1 else 's'} booked from the last price because the broker's record "
            f"was not available: check with mintel.ops.reconcile")


def mode_split(rows) -> dict:
    """The live and the paper part of a bot's rows, by net."""
    live = [float(r.get("net_pnl") or 0.0) for r in rows if row_mode(r) == "LIVE"]
    paper = [float(r.get("net_pnl") or 0.0) for r in rows if row_mode(r) != "LIVE"]
    return {"live_net": round(sum(live), 2), "paper_net": round(sum(paper), 2),
            "live_trades": len(live), "paper_trades": len(paper)}


def existing_strategy_stats(journal, positions, day_start: dt.datetime, currency: str,
                            ledger_today: Optional[dict] = None, tnb_mode: str = "LIVE",
                            data_dir: Optional[str | Path] = None) -> dict:
    """Trend & Breakout's tab for today. LIVE with no paper trade in sight:
    exactly as always (the broker's ledger wins where it exists). On PAPER,
    or with a paper trade among the rows (the day of a switch), the rows are
    split by the paper record (``_tnb_split``) and only the real ones are
    money; the ledger is not used then, because once the trader holds the
    PAPER wrapper its deal history holds the paper deals too."""
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
    record = _tnb_record(data_dir)
    if _tnb_split_needed(tnb_mode, record, closed, positions):
        return _tnb_split(closed, positions, record, currency, tnb_mode)
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
    _all_live(s)
    return s


def _all_live(s: dict) -> dict:
    """Trend & Breakout on LIVE with no paper trade in sight: every row is money."""
    s["mode"] = "LIVE"
    s["live_net"], s["paper_net"] = s["realised"], 0.0
    s["live_trades"], s["paper_trades"] = int(s["trades"]), 0
    return s


TNB_PAPER_SOURCE = ("Trend & Breakout is on PAPER: it decides on real prices and its orders are simulated (its paper "
                    "record, tnb_paper.sqlite) - practice, not money, not in Overall; a real trade left from LIVE is "
                    "booked from the broker's closing deal and counts in Overall")
TNB_SPLIT_SOURCE = ("Trend & Breakout's own records: its real trades are booked from the broker's closing deals and "
                    "count in Overall; its PAPER trades (simulated orders on real prices, from its paper record) are "
                    "practice, not money, not in Overall")


def _tnb_record(data_dir):
    """Trend & Breakout's paper record, read-only; None without a data folder."""
    if data_dir is None:
        return None
    from .standing import TnbPaperRecord
    return TnbPaperRecord(data_dir)


def _tnb_split_needed(mode: str, record, closed, positions) -> bool:
    """PAPER, or any row or open position born on the paper record."""
    if str(mode or "LIVE").upper() == "PAPER":
        return True
    if record is None or not record.tickets:
        return False
    return (any(record.is_paper(r.get("ticket")) for r in closed)
            or any(record.is_paper(getattr(p, "ticket", 0)) for p in positions or ()))


def _tnb_split(closed: list, positions, record, currency: str, mode: str, includes_now: bool = True,
               shown: Optional[int] = 60) -> dict:
    """Trend & Breakout's tab with each row marked PAPER (its ticket was born
    on the paper record; its money is the record's, every deal of the
    position) or LIVE (a real trade: money, in Overall), in the shape every
    other bot's tab has (live_net, paper_net, and under "live" the part that
    belongs in Overall)."""
    mode = "PAPER" if str(mode or "LIVE").upper() == "PAPER" else "LIVE"
    money = {p["ticket"]: p for p in (record.positions() if record is not None else [])}
    rows = []
    for r in closed:
        d = dict(r)
        paper = record is not None and record.is_paper(r.get("ticket"))
        d["mode"] = "PAPER" if paper else "LIVE"
        m = money.get(int(r.get("ticket") or 0)) if paper else None
        if m is not None:
            d["net_pnl"], d["commission"] = m["net"], m["commission"]
        rows.append(d)
    opens = []
    if includes_now:
        for p in positions or ():
            paper = record is not None and record.is_paper(getattr(p, "ticket", 0))
            opens.append({"symbol": p.symbol, "pnl": float(getattr(p, "profit", 0.0) or 0.0),
                          "mode": "PAPER" if paper else "LIVE"})
    s = strategy_stats(rows, opens, label=EXISTING_STRATEGY_LABEL, strategy_id=EXISTING_STRATEGY_ID, currency=currency)
    s["by_approach"] = breakdown(rows, lambda r: approach_name(r.get("tactic"), r.get("regime")))
    s["by_market_type"] = breakdown(rows, lambda r: market_type(r.get("symbol")))
    s.update(mode_split(rows))
    s["mode"] = mode

    def trade(r):
        return {"ticket": r["ticket"], "symbol": r["symbol"], "net": r["net_pnl"], "closed": r["closed_utc"],
                "tactic": r["tactic"], "strategy": EXISTING_STRATEGY_ID, "mode": r["mode"]}
    s["trades_today"] = [trade(r) for r in (rows[-shown:] if shown else rows)]
    live_rows = [r for r in rows if r["mode"] == "LIVE"]
    live_opens = [o for o in opens if o["mode"] == "LIVE"]
    live = strategy_stats(live_rows, live_opens, label=EXISTING_STRATEGY_LABEL, strategy_id=EXISTING_STRATEGY_ID,
                          currency=currency)
    live["trades_today"] = [trade(r) for r in live_rows]
    live["in_overall"] = bool(live_rows or live_opens or mode == "LIVE")
    s["live"] = live
    s["source"] = TNB_PAPER_SOURCE if mode == "PAPER" else TNB_SPLIT_SOURCE
    if record is not None and record.error:
        s["source"] += f"; {record.error}"
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


def _read_status(path: Path, strategy_id: str, label: str, now: dt.datetime, mode: str = "PAPER") -> dict:
    """A bot's status file, marked NOT RUNNING when stale or missing. The
    time it was written is "updated" (or "updated_utc", the Rider's name)."""
    try:
        st_ = json.loads(path.read_text())
        age = (now - to_utc(dt.datetime.fromisoformat(st_.get("updated") or st_["updated_utc"]))).total_seconds()
        st_["present"] = True
        st_["age_seconds"] = round(age, 1)
        if age > STALE_SECONDS:
            st_["status"] = f"NOT RUNNING (last seen {age / 60:.0f} min ago)"
            st_["stale"] = True
    except Exception:
        st_ = {"strategy_id": strategy_id, "label": label, "mode": mode, "status": "NOT RUNNING", "open": [], "present": False}
    return st_


def _closed_rows(path: Path, start: dt.datetime, end: Optional[dt.datetime]) -> list[dict]:
    a = to_utc(start).isoformat()
    if end is None:
        return _rows_ro(path, "SELECT * FROM trades WHERE closed_utc IS NOT NULL AND closed_utc >= ? "
                              "ORDER BY closed_utc ASC LIMIT 5000", (a,))
    return _rows_ro(path, "SELECT * FROM trades WHERE closed_utc IS NOT NULL AND closed_utc >= ? "
                          "AND closed_utc < ? ORDER BY closed_utc ASC LIMIT 5000", (a, to_utc(end).isoformat()))


def _trade_row(r: dict, strategy_id: str) -> dict:
    return {"ticket": r["ticket"], "symbol": r["symbol"], "net": r.get("net_pnl"), "closed": r["closed_utc"],
            "exit_reason": r.get("exit_reason"), "mode": row_mode(r), "strategy": strategy_id, "tactic": r.get("tactic"),
            "estimated": is_estimated(r)}


def _bot_tab(data_dir: str | Path, db_name: str, status_name: str, strategy_id: str, label: str,
             start: dt.datetime, end: Optional[dt.datetime], currency: str,
             now: Optional[dt.datetime] = None, source: str = "", paper_source: str = "",
             row_map=None, opens_fn=None, mode_default: str = "PAPER") -> tuple[dict, dict, list]:
    """A bot's figures from its own sqlite and status file: (stats, status, rows).

    The stats cover every row of the period, LIVE and PAPER, and carry the
    split (live_net, paper_net, live_trades, paper_trades) and the count of
    LIVE rows that are estimates (estimated_trades, estimated_net). Under
    "live" is the same shape for the LIVE rows alone: the part that belongs
    in Overall.

    ``row_map`` turns a bot's own row into the common shape (net_pnl,
    symbol, exit_reason ending '?' for an estimate) and may drop a row by
    returning None; ``opens_fn`` reads the open positions from a status file
    that names them differently."""
    data = Path(data_dir)
    now = to_utc(now or utcnow())
    st_ = _read_status(data / status_name, strategy_id, label, now, mode_default)
    mode_now = str(st_.get("mode") or mode_default).upper()
    rows = _closed_rows(data / db_name, start, end)
    if row_map is not None:
        rows = [m for m in (row_map(r) for r in rows) if m is not None]
    includes_now = True if end is None else to_utc(start) <= now < to_utc(end)
    raw_opens = opens_fn(st_) if opens_fn is not None else (st_.get("open") or [])
    opens = [{"symbol": o.get("symbol"), "pnl": o.get("pnl") or 0.0, "mode": str(o.get("mode") or mode_now).upper()}
             for o in raw_opens] if includes_now else []
    stats = strategy_stats(rows, opens, label=label, strategy_id=strategy_id, currency=currency)
    stats.update(mode_split(rows))
    stats["mode"] = mode_now
    stats["trades_today"] = [_trade_row(r, strategy_id) for r in rows]
    peaks = [float(r.get("peak_r") or 0) for r in rows]
    stats["avg_peak_r"] = round(sum(peaks) / len(peaks), 2) if peaks else None
    live_rows = [r for r in rows if row_mode(r) == "LIVE"]
    live_opens = [o for o in opens if o["mode"] == "LIVE"]
    live = strategy_stats(live_rows, live_opens, label=label, strategy_id=strategy_id, currency=currency)
    live["trades_today"] = [_trade_row(r, strategy_id) for r in live_rows]
    # in Overall when it is live now or has real trades in the period
    live["in_overall"] = bool(live_rows or live_opens or mode_now == "LIVE")
    est = estimated_split(live_rows)
    live.update(est)
    stats.update(est)
    stats["live"] = live
    if source:
        stats["source"] = source
    elif live["in_overall"]:
        stats["source"] = (f"{label}'s own records: LIVE trades are booked from the broker's closing deals "
                           f"(where a market charges commission the entry half is not in them; mintel.ops.reconcile "
                           f"has the full figure) and count in Overall; PAPER trades are simulated, not money, not in Overall")
    else:
        stats["source"] = paper_source or f"simulated (PAPER) - {label}'s own records, not money, not in Overall"
    if est["estimated_trades"]:
        stats["source"] += "; " + estimated_note(est["estimated_trades"])
    return stats, st_, rows


def runner_tab(data_dir: str | Path, start: dt.datetime, end: Optional[dt.datetime], currency: str,
               now: Optional[dt.datetime] = None) -> tuple[dict, dict]:
    """The Momentum Runner's figures from its own records (runner.sqlite): (stats, status)."""
    from ..runner import STRATEGY_ID as RUNNER_ID, STRATEGY_LABEL as RUNNER_LABEL
    stats, st_, rows = _bot_tab(data_dir, "runner.sqlite", "runner-status.json", RUNNER_ID, RUNNER_LABEL, start, end, currency,
                                now, paper_source="simulated (PAPER) - a shadow of Trend & Breakout's index trades, "
                                                  "not money, not in Overall")
    stats["reached_trail"] = sum(1 for r in rows if float(r.get("peak_r") or 0) >= float(st_.get("trail_r") or 3.0))
    return stats, st_


def paper_tab(data_dir: str | Path, db_name: str, status_name: str, strategy_id: str, label: str,
              start: dt.datetime, end: Optional[dt.datetime], currency: str,
              now: Optional[dt.datetime] = None, source: str = "") -> tuple[dict, dict]:
    """A bot's figures from its own sqlite and status file: (stats, status).
    The name is from the days every one of them was PAPER; the rows' own
    modes decide what is money."""
    stats, st_, _ = _bot_tab(data_dir, db_name, status_name, strategy_id, label, start, end, currency, now, source)
    return stats, st_


# ------------------------------------------------- the two bots of 9 Oct --
def _rider_row(r: dict) -> Optional[dict]:
    """A Rapid Momentum Rider row in the common shape. Its money is
    net_money; a LIVE row whose money is an estimate (the broker's deal was
    not readable) gets the '?' every other bot puts on its exit reason. A
    practice trade the bot closed in its records when it was switched
    (SWITCHED TO LIVE / SWITCHED OFF, no money) was never a trade."""
    if str(r.get("exit_reason") or "").startswith("SWITCHED"):
        return None
    d = dict(r)
    d["net_pnl"] = r.get("net_money")
    if (row_mode(r) == "LIVE" and str(r.get("money_source") or "") == "estimate"
            and not str(r.get("exit_reason") or "").endswith("?")):
        d["exit_reason"] = f"{r.get('exit_reason') or 'CLOSED'}?"
    return d


def _rider_opens(st_: dict) -> list[dict]:
    mode = str(st_.get("mode") or "PAPER").upper()
    return [{"symbol": o.get("market") or o.get("symbol"), "pnl": o.get("money") or 0.0, "mode": mode}
            for o in (st_.get("open_positions") or [])]


def is_test_data(r: dict) -> bool:
    """A Financial Ian row made from a SYNTHETIC or REPLAY feed: test data, never a result."""
    src = str(r.get("data_source") or "").upper()
    return "SYNTHETIC" in src or "REPLAY" in src


def _ian_row(r: dict) -> Optional[dict]:
    if is_test_data(r):
        return None
    d = dict(r)
    d["symbol"] = r.get("spot_symbol") or r.get("instrument") or r.get("root")
    return d


def _ian_opens(st_: dict) -> list[dict]:
    feed = st_.get("feed") or {}
    if feed.get("synthetic") or str(st_.get("data_label") or "LIVE") != "LIVE":
        return []                                      # a synthetic or replayed run's positions are not money
    return [{"symbol": o.get("symbol"), "pnl": o.get("pnl") or 0.0, "mode": o.get("mode")}
            for o in (st_.get("open_positions") or [])]


def rider_tab(data_dir: str | Path, start: dt.datetime, end: Optional[dt.datetime], currency: str,
              now: Optional[dt.datetime] = None) -> tuple[dict, dict]:
    """The Rapid Momentum Rider's figures from its own records (rider.sqlite,
    rider-status.json): (stats, status). Read-only."""
    from ..rider import STRATEGY_ID as RIDER_ID, STRATEGY_LABEL as RIDER_LABEL
    from .watchdog import own_file_mode
    stats, st_, rows = _bot_tab(data_dir, "rider.sqlite", "rider-status.json", RIDER_ID, RIDER_LABEL, start, end,
                                currency, now, row_map=_rider_row, opens_fn=_rider_opens,
                                mode_default=own_file_mode(Path(data_dir) / "rider.json"))
    pips = [float(r.get("realised_pips") or 0.0) for r in rows]
    stats["net_pips"] = round(sum(pips), 1) if rows else None
    return stats, st_


def ian_tab(data_dir: str | Path, start: dt.datetime, end: Optional[dt.datetime], currency: str,
            now: Optional[dt.datetime] = None) -> tuple[dict, dict]:
    """Financial Ian's figures from its own records (ian.sqlite,
    ian-status.json): (stats, status). Rows from a SYNTHETIC or REPLAY feed
    are left out: they are tests, not results. Read-only."""
    from ..ian import STRATEGY_ID as IAN_ID, STRATEGY_LABEL as IAN_LABEL
    from .watchdog import own_file_mode
    stats, st_, rows = _bot_tab(data_dir, "ian.sqlite", "ian-status.json", IAN_ID, IAN_LABEL, start, end, currency,
                                now, row_map=_ian_row, opens_fn=_ian_opens,
                                mode_default=own_file_mode(Path(data_dir) / "ian.json"))
    return stats, st_


def _known(data_dir: str | Path, status: dict, settings: str) -> bool:
    """Is a bot part of this installation: has it reported, or does it have
    its own settings file? (A bot that never ran here is not named.)"""
    return bool(status.get("present")) or (Path(data_dir) / settings).exists()


NO_ARTICLE = frozenset({"Financial Ian", EXISTING_STRATEGY_LABEL})   # names: never "the Financial Ian"


def _names(labels: list[str]) -> str:
    """'The Momentum Runner, the Band Breaker and the Crowd Fader'
    ('Financial Ian' takes no article)."""
    parts = [str(x) if str(x) in NO_ARTICLE else ("The " if i == 0 else "the ") + str(x)
             for i, x in enumerate(labels)]
    if len(parts) <= 1:
        return "".join(parts)
    return ", ".join(parts[:-1]) + " and " + parts[-1]


def overall_note(modes: dict, estimated: int = 0) -> str:
    """One plain statement of what Overall contains: the bots still in PAPER
    (their simulated results stay on their own tabs), the bots that are OFF,
    or that every bot is live; and, when `estimated` LIVE trades were booked
    without the broker's record, that they are estimates. `modes` maps a
    bot's label to its mode."""
    paper = [label for label, m in modes.items() if str(m or "PAPER").upper() == "PAPER"]
    off = [label for label, m in modes.items() if str(m or "PAPER").upper() not in ("PAPER", "LIVE")]
    parts = []
    if not paper and not off:
        parts.append("Every bot is live: Overall is every bot's broker figures added together.")
    if paper:
        one = len(paper) == 1
        parts.append(f"{_names(paper)} {'is' if one else 'are'} in PAPER: {'its' if one else 'their'} simulated results "
                     f"are on {'its own tab' if one else 'their own tabs'} only.")
    if off:
        parts.append(f"{_names(off)} {'is' if len(off) == 1 else 'are'} OFF.")
    if estimated:
        parts.append(estimated_note(int(estimated)) + ".")
    return " ".join(parts)


def _tnb_overall_part(mi: dict) -> list[dict]:
    """What Trend & Breakout adds to Overall: everything on the LIVE path as
    always; once split, its real part only (when it has one)."""
    if "live" not in mi:
        return [mi]
    return [mi["live"]] if mi["live"]["in_overall"] else []


def build_strategies(journal, positions, day_start: dt.datetime, currency: str,
                     data_dir: str | Path, ledger_today: Optional[dict] = None,
                     now: Optional[dt.datetime] = None, tnb_mode: Optional[str] = None) -> dict:
    """``tnb_mode`` is Trend & Breakout's mode (push_dashboard passes the
    config's); without it, config.json in the data folder says (LIVE when
    it says nothing)."""
    from .standing import tnb_mode as read_tnb_mode
    tnb = str(tnb_mode or read_tnb_mode(None, data_dir)).upper()
    mi = existing_strategy_stats(journal, positions, day_start, currency, ledger_today, tnb, data_dir)
    rs_status = read_scalper_status(data_dir, now=now)
    rs = dict(rs_status.get("stats") or {})
    rs.setdefault("id", STRATEGY_ID); rs.setdefault("label", STRATEGY_LABEL); rs.setdefault("currency", currency)
    rs["trades_today"] = rs_status.get("trades_today", [])
    # The scalper's status file counts the rows of the mode it runs in, so
    # its tab is all live or all paper.
    live_rs = str(rs_status.get("mode") or "").upper() == "LIVE"
    rs["mode"] = str(rs_status.get("mode") or "OFF").upper()
    rs_realised = float(rs.get("realised") or 0.0)
    rs["live_net"], rs["paper_net"] = (rs_realised, 0.0) if live_rs else (0.0, rs_realised)
    rs["live_trades"], rs["paper_trades"] = (int(rs.get("trades") or 0), 0) if live_rs else (0, int(rs.get("trades") or 0))
    from ..runner import STRATEGY_ID as RUNNER_ID, STRATEGY_LABEL as RUNNER_LABEL
    mr, mr_status = runner_tab(data_dir, day_start, None, currency, now)
    from ..bandbreaker import STRATEGY_ID as BB_ID, STRATEGY_LABEL as BB_LABEL
    bb, bb_status = paper_tab(data_dir, "bandbreaker.sqlite", "bandbreaker-status.json", BB_ID, BB_LABEL,
                              day_start, None, currency, now)
    from ..crowd import STRATEGY_ID as CF_ID, STRATEGY_LABEL as CF_LABEL
    cf, cf_status = paper_tab(data_dir, "crowd.sqlite", "crowd-status.json", CF_ID, CF_LABEL,
                              day_start, None, currency, now)
    from ..rider import STRATEGY_ID as RIDER_ID, STRATEGY_LABEL as RIDER_LABEL
    rd, rd_status = rider_tab(data_dir, day_start, None, currency, now)
    from ..ian import STRATEGY_ID as IAN_ID, STRATEGY_LABEL as IAN_LABEL
    ia, ia_status = ian_tab(data_dir, day_start, None, currency, now)
    # Overall is the ACCOUNT: money that really moved. Trend & Breakout (its
    # real trades only while it is on PAPER), the scalper when it runs LIVE,
    # and every other bot's LIVE rows. A bot in PAPER is simulated, so it has
    # its own tab but is never added.
    tnb_parts = _tnb_overall_part(mi)
    parts = tnb_parts + ([rs] if live_rs else []) + [b["live"] for b in (rd, mr, bb, cf, ia) if b["live"]["in_overall"]]
    overall = combine(parts)
    overall["trades_today"] = sorted(overall["trades_today"], key=lambda t: str(t.get("closed") or ""))
    # the money shape of the account: from every real trade's net
    nets = [float(t.get("net") or 0.0) for t in overall["trades_today"]]
    if nets:
        overall.update(_money_shape(nets))
    if mi.get("before_fees") is not None and len(parts) == 1 and tnb_parts and tnb_parts[0] is mi:
        overall["before_fees"] = mi["before_fees"]
    if not live_rs:
        rs["source"] = rs.get("source") or "simulated (PAPER) - not money, not in Overall"
    overall["estimated_trades"] = sum(int(b["live"].get("estimated_trades") or 0) for b in (rd, mr, bb, cf, ia))
    # The note names the bots of this installation: the retired scalper only
    # while it is still running, the Rider and Ian once they have a file here,
    # Trend & Breakout only while it is on PAPER (on LIVE it is the account).
    named: dict = {}
    if mi.get("mode") == "PAPER":
        named[EXISTING_STRATEGY_LABEL] = "PAPER"
    if _known(data_dir, rd_status, "rider.json"):
        named[RIDER_LABEL] = rd["mode"]
    if rs_status.get("present") and not str(rs_status.get("status") or "").startswith("NOT RUNNING"):
        named[STRATEGY_LABEL] = rs["mode"]
    named.update({RUNNER_LABEL: mr["mode"], BB_LABEL: bb["mode"], CF_LABEL: cf["mode"]})
    if _known(data_dir, ia_status, "ian.json"):
        named[IAN_LABEL] = ia["mode"]
    overall["note"] = overall_note(named, overall["estimated_trades"])
    return {"overall": overall, EXISTING_STRATEGY_ID: mi, STRATEGY_ID: rs, RUNNER_ID: mr, BB_ID: bb, CF_ID: cf,
            RIDER_ID: rd, IAN_ID: ia,
            "scalper": rs_status, "runner": mr_status, "bandbreaker": bb_status, "crowd": cf_status,
            "rider": rd_status, "ian": ia_status,
            "labels": {"overall": "Overall", EXISTING_STRATEGY_ID: EXISTING_STRATEGY_LABEL, STRATEGY_ID: STRATEGY_LABEL,
                       RUNNER_ID: RUNNER_LABEL, BB_ID: BB_LABEL, CF_ID: CF_LABEL, RIDER_ID: RIDER_LABEL,
                       IAN_ID: IAN_LABEL}}


# ------------------------------------------------------------- periods --
PERIODS = (("today", "Today"), ("yesterday", "Yesterday"), ("week", "This week"),
           ("month", "This month"), ("6m", "Last 6 months"), ("1y", "Last year"))


def period_bounds(period: str, now: Optional[dt.datetime] = None,
                  date_from: str = "", date_to: str = "") -> tuple[dt.datetime, dt.datetime, str]:
    """(start_utc, end_utc, label) for a page period.

    Day boundaries are UK calendar days (Europe/London, midnight to midnight,
    BST and GMT handled) - the same days as the cards at the top of the page
    (mintel/ops/standing.py period_bounds), whatever the computer's own clock
    says - converted to UTC for the journals. A custom range takes two ISO
    dates (YYYY-MM-DD) and is inclusive of both days."""
    from .standing import _months_back, uk_date, uk_midnight
    now = to_utc(now or utcnow())
    today = uk_date(now)
    one = dt.timedelta(days=1)
    if period == "custom" and (date_from or date_to):
        try:
            a = dt.date.fromisoformat(date_from) if date_from else dt.date.fromisoformat(date_to)
            b = dt.date.fromisoformat(date_to) if date_to else a
        except ValueError:
            a = b = today
        if b < a:
            a, b = b, a
        lo, hi = dt.date(2000, 1, 1), today + one                 # nothing outside the bots' lifetime
        a, b = min(max(a, lo), hi), min(max(b, lo), hi)
        start, end = uk_midnight(a), uk_midnight(b + one)
        label = f"{a.isoformat()} to {b.isoformat()}" if a != b else a.isoformat()
    elif period == "yesterday":
        start, end, label = uk_midnight(today - one), uk_midnight(today), "Yesterday"
    elif period == "week":
        start, end, label = uk_midnight(today - dt.timedelta(days=today.weekday())), uk_midnight(today + one), "This week"
    elif period == "month":
        start, end, label = uk_midnight(today.replace(day=1)), uk_midnight(today + one), "This month"
    elif period == "6m":
        start, end, label = uk_midnight(_months_back(today, 6)), uk_midnight(today + one), "Last 6 months"
    elif period == "1y":
        start, end, label = uk_midnight(_months_back(today, 12)), uk_midnight(today + one), "Last year"
    else:
        start, end, label = uk_midnight(today), uk_midnight(today + one), "Today"
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
                           label: str = "", now: Optional[dt.datetime] = None,
                           tnb_mode: Optional[str] = None) -> dict:
    """Strategy figures for a date range, from the bots' own journals.

    Today's figures on the page come from the broker's ledger; any other
    period comes from these records, and the page says so. Trend &
    Breakout's rows are split by its paper record as on the today view when
    it is on PAPER (``tnb_mode``; without it, config.json in the data folder
    says) or when the period holds a paper trade."""
    from .standing import tnb_mode as read_tnb_mode
    tnb = str(tnb_mode or read_tnb_mode(None, data_dir)).upper()
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
    record = _tnb_record(data_dir)
    if _tnb_split_needed(tnb, record, closed, positions if includes_now else ()):
        mi = _tnb_split(closed, positions, record, currency, tnb, includes_now, shown=None)
    else:
        opens = [{"symbol": p.symbol, "pnl": float(getattr(p, "profit", 0.0) or 0.0)} for p in positions] if includes_now else []
        mi = strategy_stats(closed, opens, label=EXISTING_STRATEGY_LABEL, strategy_id=EXISTING_STRATEGY_ID, currency=currency)
        mi["trades_today"] = [{"ticket": r["ticket"], "symbol": r["symbol"], "net": r["net_pnl"], "closed": r["closed_utc"],
                               "tactic": r["tactic"], "strategy": EXISTING_STRATEGY_ID} for r in closed]
        _all_live(mi)
    rs_rows = _rows_ro(data / "scalper.sqlite",
                       "SELECT * FROM trades WHERE closed_utc IS NOT NULL AND closed_utc >= ? AND closed_utc < ? "
                       "ORDER BY closed_utc ASC LIMIT 5000", (a, b))
    rs = strategy_stats(rs_rows, list(scalper_open) if includes_now else [], label=STRATEGY_LABEL,
                        strategy_id=STRATEGY_ID, currency=currency, scalper=True)
    rs["trades_today"] = [{"ticket": r["ticket"], "symbol": r["symbol"], "net": r.get("net_pnl"), "closed": r["closed_utc"],
                           "exit_reason": r.get("exit_reason"), "mode": r.get("mode"), "strategy": STRATEGY_ID} for r in rs_rows]
    rs.update(mode_split(rs_rows))
    # the account total only ever counts trades that really happened
    live_rows = [r for r in rs_rows if row_mode(r) == "LIVE"]
    rs_live = strategy_stats(live_rows, [], label=STRATEGY_LABEL, strategy_id=STRATEGY_ID,
                             currency=currency, scalper=True)
    rs_live["trades_today"] = [t for t in rs["trades_today"] if str(t.get("mode") or "").upper() == "LIVE"]
    from ..runner import STRATEGY_ID as RUNNER_ID, STRATEGY_LABEL as RUNNER_LABEL
    mr, mr_status = runner_tab(data_dir, start, end, currency, now)
    from ..bandbreaker import STRATEGY_ID as BB_ID, STRATEGY_LABEL as BB_LABEL
    bb, bb_status = paper_tab(data_dir, "bandbreaker.sqlite", "bandbreaker-status.json", BB_ID, BB_LABEL,
                              start, end, currency, now)
    from ..crowd import STRATEGY_ID as CF_ID, STRATEGY_LABEL as CF_LABEL
    cf, cf_status = paper_tab(data_dir, "crowd.sqlite", "crowd-status.json", CF_ID, CF_LABEL,
                              start, end, currency, now)
    from ..rider import STRATEGY_ID as RIDER_ID, STRATEGY_LABEL as RIDER_LABEL
    rd, rd_status = rider_tab(data_dir, start, end, currency, now)
    from ..ian import STRATEGY_ID as IAN_ID, STRATEGY_LABEL as IAN_LABEL
    ia, ia_status = ian_tab(data_dir, start, end, currency, now)
    parts = _tnb_overall_part(mi) + [rs_live] + [x["live"] for x in (rd, mr, bb, cf, ia) if x["live"]["in_overall"]]
    overall = combine(parts)
    overall["trades_today"] = sorted(overall["trades_today"], key=lambda t: str(t.get("closed") or ""))
    paper_n = sum(int(x.get("paper_trades") or 0) for x in (mi, rs, rd, mr, bb, cf, ia))
    tnb_adds = ("Trend & Breakout's real trades" if "live" in mi else "Trend & Breakout")
    overall["note"] = (f"{paper_n} PAPER trade{'' if paper_n == 1 else 's'} in this period "
                       f"{'is' if paper_n == 1 else 'are'} on the bots' own tabs only; Overall adds {tnb_adds} "
                       f"and every LIVE trade of the other bots."
                       if paper_n else
                       "Every trade in this period was live: Overall is every bot's broker figures added together.")
    if mi.get("mode") == "PAPER":
        overall["note"] += (" Trend & Breakout is on PAPER: real prices, simulated orders; the Momentum Runner "
                            "rides its index entries.")
    overall["estimated_trades"] = sum(int(x["live"].get("estimated_trades") or 0) for x in (rd, mr, bb, cf, ia))
    if overall["estimated_trades"]:
        overall["note"] += " " + estimated_note(overall["estimated_trades"]) + "."
    return {"overall": overall, EXISTING_STRATEGY_ID: mi, STRATEGY_ID: rs, RUNNER_ID: mr, BB_ID: bb, CF_ID: cf,
            RIDER_ID: rd, IAN_ID: ia,
            "scalper": read_scalper_status(data_dir, now=now), "runner": mr_status, "bandbreaker": bb_status, "crowd": cf_status,
            "rider": rd_status, "ian": ia_status,
            "labels": {"overall": "Overall", EXISTING_STRATEGY_ID: EXISTING_STRATEGY_LABEL, STRATEGY_ID: STRATEGY_LABEL,
                       RUNNER_ID: RUNNER_LABEL, BB_ID: BB_LABEL, CF_ID: CF_LABEL, RIDER_ID: RIDER_LABEL,
                       IAN_ID: IAN_LABEL},
            "period": {"key": "custom" if label and label[:1].isdigit() else label.lower(), "label": label,
                       "start": a, "end": b, "source": "the bots' own records"}}


def make_period_resolver(data_dir: str | Path, currency_fn=None, positions_fn=None, tnb_mode_fn=None):
    """A callable the page uses for any period other than today. Trend &
    Breakout's mode is the ``tnb_mode`` the page passes, else
    ``tnb_mode_fn()``, else config.json in the data folder."""
    def resolve(period: str, date_from: str = "", date_to: str = "", tnb_mode: Optional[str] = None) -> dict:
        now = utcnow()
        mode = tnb_mode
        if not mode and tnb_mode_fn is not None:
            try:
                mode = tnb_mode_fn()
            except Exception:
                mode = None
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
        out = build_strategies_range(data_dir, start, end, currency, positions, (), label, now, tnb_mode=mode)
        out["period"]["key"] = period
        out["period"]["from"] = date_from
        out["period"]["to"] = date_to
        return out
    resolve.takes_tnb_mode = True
    return resolve
