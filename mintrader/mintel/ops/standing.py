"""The account's standing, from the broker alone.

One figure the page leads with: what the account has made since the reset,
after commission and swap, taken straight from MetaTrader's deal history for
EVERY trade on the account (every bot and anything placed by hand). It
does not depend on any bot's own records, so it cannot be fooled by one: a
paper bot never touches the broker and so can never appear in it.

It is the same number MetaTrader shows: the balance now, less the balance at
the reset. The page shows that sum so it can be checked against the phone.

Under it, the same history split by magic number - one row per bot, plus a
row for anything that carried no bot's number - so the rows always add up to
the big figure. The six active bots always have a row; the retired Rapid
Scalper (magic 990411, replaced by the Rapid Momentum Rider on 9 Oct) keeps
its row too, because its real trades are still part of the account, and
the page shows it only while it has money to show.

Trend & Breakout on PAPER (config.json "tnb" -> "mode"): its orders are
simulated by the PaperBroker wrapper and never reach MetaTrader, so they can
never be in these figures. Its row is still magic 990311's REAL deals (a
position left open from LIVE, wound down to its end), which are real money
and part of the account; its card is marked PAPER and its practice figure
comes from its own paper record (``TnbPaperRecord``: data/tnb_paper.sqlite,
read through ``PaperBroker(..., read_only=True)``), shown apart and never
added. The broker read here is always the real one (``real_broker``), even
if the trader's wrapper is handed in: the whole-account view is MetaTrader's
own.

The page's periods (Aaron, 9 Oct: "I don't want last night 11pm trade
showing, just today's") are UK CALENDAR DAYS: Europe/London, midnight to
midnight, BST and GMT handled by the time-zone database - never the
broker's day (which starts at 22:00 UK in summer). Every period but Total
counts each CLOSED position once, on the UK day it closed, with its full
result as MetaTrader shows a position (every one of its deals: both halves
of the commission, swap, profit, fees), for the bot whose magic OPENED it;
trades and wins are counted by position, never by deal. A position still
open counts in no period (its open profit is shown as open). Total and each
card's Overall stay balance-based (the balance less the balance at the
reset, and each bot's deals since the reset), so they always equal the
account; where a period's figure differs from what the trades moved the
balance in the same UK days, ``period_view`` says why in one plain line.
The daily-loss stop is not this: it keeps its own day (the broker's).
"""
from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from typing import Optional

from ..clock import TZ_LONDON, to_utc

log = logging.getLogger("mintel.standing")

TNB_MAGIC = 990_311
TNB_PAPER_DB = "tnb_paper.sqlite"
_EPOCH = dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc)
UTC = dt.timezone.utc
# the deal history is read from this long before the reset as well, so a
# position that closed after the reset still carries the commission charged
# when it opened (its full result, as MetaTrader shows it)
LOOKBACK = dt.timedelta(days=14)
UK_DAYS_NOTE = "Days are UK days, midnight to midnight"


def uk_date(t: dt.datetime) -> dt.date:
    """The UK calendar date (Europe/London: BST in summer, GMT in winter) of an instant."""
    return to_utc(t).astimezone(TZ_LONDON).date()


def uk_midnight(d: dt.date) -> dt.datetime:
    """00:00 UK on ``d``, in UTC (23:00 UTC the evening before in BST, 00:00 UTC in GMT)."""
    return dt.datetime(d.year, d.month, d.day, tzinfo=TZ_LONDON).astimezone(UTC)


def uk_day_start(now: dt.datetime) -> dt.datetime:
    """00:00 UK today, in UTC."""
    return uk_midnight(uk_date(now))


def tnb_mode(cfg=None, data_dir=None) -> str:
    """Trend & Breakout's mode as every report must treat it: "PAPER" or
    "LIVE". From the config's own ``tnb`` block when it carries one
    (``getattr(getattr(cfg, "tnb", None), "mode", "LIVE")``); otherwise from
    the "tnb" block of config.json in the data folder (where the installers
    and the modes command keep it). LIVE - today's behaviour - unless one of
    them says PAPER. Never raises."""
    block = getattr(cfg, "tnb", None) if cfg is not None else None
    if block is not None:
        mode = block.get("mode") if isinstance(block, dict) else getattr(block, "mode", "LIVE")
        return "PAPER" if str(mode or "").strip().upper() == "PAPER" else "LIVE"
    d = data_dir if data_dir is not None else getattr(getattr(cfg, "ops", None), "data_dir", None)
    if not d:
        return "LIVE"
    try:
        raw = json.loads((Path(d) / "config.json").read_text())
        b = raw.get("tnb") if isinstance(raw, dict) else None
        mode = b.get("mode") if isinstance(b, dict) else ""
        return "PAPER" if str(mode or "").strip().upper() == "PAPER" else "LIVE"
    except Exception:
        return "LIVE"


def real_broker_of(broker):
    """The REAL broker behind the PAPER wrapper (or ``broker`` itself)."""
    try:
        from ..broker.paper import real_broker
        return real_broker(broker)
    except Exception:
        return broker


class TnbPaperRecord:
    """Trend & Breakout's PAPER record (data/tnb_paper.sqlite), read once and
    read-only through ``PaperBroker(None, data_dir, magic, read_only=True)``:
    it can never book a stop or send an order, and it never asks the real
    broker for anything. Empty (no deals, no tickets) when there is no
    record; ``error`` says why when it could not be read.

    ``deals`` are in the MT5 adapter's ``deals_since`` shape (profit
    includes commission and swap; the entry deal carries the entry half of
    the commission). ``tickets`` is every position born on paper."""

    def __init__(self, data_dir, magic: int = TNB_MAGIC):
        self.deals: list[dict] = []
        self.tickets: frozenset = frozenset()
        self.error = ""
        self.present = bool(data_dir) and (Path(data_dir) / TNB_PAPER_DB).exists()
        if not self.present:
            return
        try:
            from ..broker.paper import PaperBroker
            view = PaperBroker(None, data_dir, magic=int(magic or TNB_MAGIC), read_only=True)
            try:
                rows = view.paper_deals_since(_EPOCH, closing_only=False)
            finally:
                view.close_db()
        except Exception as exc:                            # a broken record is said, never guessed
            self.error = f"the paper record could not be read: {exc}"
            log.warning("Trend & Breakout's %s", self.error)
            return
        self.deals = [dict(r, time=to_utc(r["time"])) for r in rows if r.get("time") is not None]
        self.tickets = frozenset(int(r.get("position") or 0) for r in self.deals)

    def is_paper(self, ticket) -> bool:
        try:
            return int(ticket or 0) in self.tickets
        except (TypeError, ValueError):
            return False

    def deals_in(self, start: dt.datetime, end: Optional[dt.datetime] = None) -> list[dict]:
        """Every paper deal booked in [start, end)."""
        a = to_utc(start)
        b = to_utc(end) if end is not None else None
        return [r for r in self.deals if r["time"] >= a and (b is None or r["time"] < b)]

    def positions(self, start: Optional[dt.datetime] = None, end: Optional[dt.datetime] = None) -> list[dict]:
        """Paper positions whose LAST closing deal fell in [start, end), each
        with every one of its deals counted (the entry's commission too), in
        the order they closed: {ticket, symbol, net, commission (a positive
        cost), volume, opened, closed}. A position closed only in part counts
        the part that is closed."""
        by: dict[int, dict] = {}
        for r in self.deals:
            pos = int(r.get("position") or 0)
            p = by.setdefault(pos, {"ticket": pos, "symbol": str(r.get("symbol") or ""), "net": 0.0,
                                    "commission": 0.0, "volume": 0.0, "opened": None, "closed": None})
            p["net"] += float(r.get("profit") or 0.0)
            p["commission"] += abs(float(r.get("commission") or 0.0))
            if r.get("is_entry"):
                p["opened"] = r["time"] if p["opened"] is None else min(p["opened"], r["time"])
            else:
                p["volume"] += float(r.get("volume") or 0.0)
                p["closed"] = r["time"] if p["closed"] is None else max(p["closed"], r["time"])
        a = to_utc(start) if start is not None else None
        b = to_utc(end) if end is not None else None
        out = [dict(p, net=round(p["net"], 2), commission=round(p["commission"], 2)) for p in by.values()
               if p["closed"] is not None and (a is None or p["closed"] >= a) and (b is None or p["closed"] < b)]
        out.sort(key=lambda p: p["closed"])
        return out

    def net(self, start: dt.datetime, end: Optional[dt.datetime] = None) -> float:
        """Practice money of the positions that closed in [start, end)."""
        return round(sum(p["net"] for p in self.positions(start, end)), 2)

_CACHE: dict = {"at": None, "value": None}

BOTS = (  # (id, label): the six active bots in the page's order, then the retired one
    ("market_intelligence", "Trend & Breakout"),
    ("momentum_rider", "Rapid Momentum Rider"),
    ("momentum_runner", "Momentum Runner"),
    ("band_breaker", "Band Breaker"),
    ("crowd_fader", "Crowd Fader"),
    ("financial_ian", "Financial Ian"),
    ("rapid_scalper", "Rapid Scalper (retired)"),
)
ACTIVE_BOTS = BOTS[:6]
RETIRED = frozenset({"rapid_scalper"})     # a known magic, shown only when it has money to show
RIDER_MAGIC, IAN_MAGIC, SCALPER_MAGIC = 990_811, 990_911, 990_411


def _start(cfg, now: dt.datetime) -> dt.datetime:
    """The account's reset (``account_reset_utc``), never moved by a change of
    rules; the strategy's tracking start only if no reset is configured."""
    for txt in (getattr(cfg, "account_reset_utc", "") or "", getattr(cfg, "tracking_start_utc", "") or ""):
        try:
            if txt:
                s = dt.datetime.fromisoformat(txt.replace("Z", "+00:00"))
                return to_utc(s if s.tzinfo else s.replace(tzinfo=dt.timezone.utc))
        except ValueError:
            continue
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def positions_of(deals, open_tickets=None) -> list[dict]:
    """Every position in the deal rows, as MetaTrader shows a position: all
    of its deals added up (the adapter's "profit" already holds the deal's
    profit, commission, swap and fees), credited to the bot that OPENED it
    (the ``bot_magic`` its entry deal was tagged with). ``closed`` is the
    time of its last exit deal; it is CLOSED once it has an exit and is no
    longer open - ``open_tickets`` is MetaTrader's open positions read with
    the deals (None when they could not be read: then its exits must add up
    to its entries' volume, where the rows give a volume). A position still
    open, or only partly closed, is not closed and counts in no period.

    Each: {position, bot_magic, symbol, net, commission (a positive cost),
    volume, opened, closed, is_closed, deals}, in the order they closed."""
    open_set = None if open_tickets is None else {int(t) for t in open_tickets}
    by: dict = {}
    for r in deals or ():
        t = r.get("time")
        if t is None:
            continue
        t = to_utc(t)
        pos = int(r.get("position") or 0)
        key = pos if pos else ("deal", id(r))
        p = by.get(key)
        if p is None:
            p = by[key] = {"position": pos, "bot_magic": r.get("bot_magic", -1), "symbol": "", "net": 0.0,
                           "commission": 0.0, "entry_volume": 0.0, "exit_volume": 0.0, "opened": None,
                           "closed": None, "deals": [], "_entry_tag": False}
        if r.get("is_entry"):
            if not p["_entry_tag"]:
                p["bot_magic"], p["_entry_tag"] = r.get("bot_magic", p["bot_magic"]), True
            p["opened"] = t if p["opened"] is None else min(p["opened"], t)
            p["entry_volume"] += float(r.get("volume") or 0.0)
        else:
            p["closed"] = t if p["closed"] is None else max(p["closed"], t)
            p["exit_volume"] += float(r.get("volume") or 0.0)
        p["symbol"] = p["symbol"] or str(r.get("symbol") or "")
        p["net"] += float(r.get("profit") or 0.0)
        p["commission"] += abs(float(r.get("commission") or 0.0))
        p["deals"].append({"time": t, "profit": float(r.get("profit") or 0.0), "is_entry": bool(r.get("is_entry"))})
    out = []
    for p in by.values():
        if p["closed"] is None:
            closed = False
        elif open_set is not None:
            closed = p["position"] not in open_set
        else:
            closed = not (p["entry_volume"] > 0 and p["exit_volume"] + 1e-9 < p["entry_volume"])
        p.pop("_entry_tag", None)
        out.append(dict(p, net=round(p["net"], 2), commission=round(p["commission"], 2), is_closed=closed,
                        volume=round(p["entry_volume"] or p["exit_volume"], 2)))
    out.sort(key=lambda p: (p["closed"] or p["opened"] or _EPOCH))
    return out


def closed_between(positions, start: dt.datetime, end: dt.datetime) -> list[dict]:
    """The positions that CLOSED in [start, end)."""
    return [p for p in positions if p["is_closed"] and start <= p["closed"] < end]


def tally(positions) -> dict:
    """{made, trades, wins, losses, commission} of closed positions, each
    counted once: a win made money after commission, every other trade is a
    loss (as the Rider's own page counts them)."""
    nets = [float(p["net"]) for p in positions]
    wins = sum(1 for v in nets if v > 0)
    return {"made": round(sum(nets), 2), "trades": len(nets), "wins": wins, "losses": len(nets) - wins,
            "commission": round(sum(float(p["commission"]) for p in positions), 2)}


def bot_modes_and_magics(cfg) -> dict[str, tuple[int, str]]:
    """{bot id: (magic, mode)} as the running bots see them: Trend &
    Breakout (LIVE or PAPER, ``tnb_mode``) and the three config bots from
    config.json, the Rapid Momentum Rider and Financial Ian with their modes
    from their own files (rider.json, ian.json) and their magic numbers
    fixed in their code (``mintel.rider.MAGIC``, ``mintel.ian.MAGIC``: Ian's
    engine ignores any "magic" in ian.json, so it is not read here either),
    and the retired Rapid Scalper as RETIRED on the magic its file names."""
    from .watchdog import own_file_mode
    data = Path(cfg.ops.data_dir)
    out: dict[str, tuple[int, str]] = {"market_intelligence": (int(cfg.magic), tnb_mode(cfg))}
    try:
        from ..rider import MAGIC as rider_magic
    except Exception:
        rider_magic = RIDER_MAGIC
    out["momentum_rider"] = (int(rider_magic), own_file_mode(data / "rider.json"))
    for bid, block, default in (("momentum_runner", "runner", 990_511), ("band_breaker", "bandbreaker", 990_611),
                                ("crowd_fader", "crowd", 990_711)):
        b = getattr(cfg, block, None)
        mode = str(getattr(b, "mode", "PAPER") or "PAPER").upper()
        if b is not None and not getattr(b, "enabled", True):
            mode = "OFF"
        out[bid] = (int(getattr(b, "magic", default) or default), mode)
    try:
        from ..ian import MAGIC as ian_magic          # hard-wired: ian.json cannot change it
    except Exception:
        ian_magic = IAN_MAGIC
    out["financial_ian"] = (int(ian_magic), own_file_mode(data / "ian.json"))
    try:
        from ..scalper.config import ScalperConfig
        out["rapid_scalper"] = (int(ScalperConfig.load(data / "scalper.json").magic), "RETIRED")
    except Exception:
        out["rapid_scalper"] = (SCALPER_MAGIC, "RETIRED")
    return out


def _sum(rows, since: dt.datetime) -> tuple[float, int, float]:
    """(net, closed positions, commission) of trade deals at or after ``since``.
    Every deal counts, the entry included: MetaTrader charges commission on
    the way in as well as on the way out."""
    net = 0.0
    fees = 0.0
    closed: set = set()
    for r in rows:
        when = r.get("time")
        if when is None or to_utc(when) < since:
            continue
        net += float(r.get("profit") or 0.0)
        fees += abs(float(r.get("commission") or 0.0))
        if not r.get("is_entry"):
            closed.add(int(r.get("position") or 0) or id(r))
    return round(net, 2), len(closed), round(fees, 2)


def _split_by_bot(rows, magics: set[int]) -> Optional[dict[int, list]]:
    """Every deal credited to the bot that OPENED its position (the entry
    deal's magic), so a bot's trade closed by hand on the phone still counts
    as that bot's. None if the broker's rows do not say which magic they
    carried (an older bridge): the caller then asks per magic."""
    if any("magic" not in r for r in rows):
        return None
    opener: dict[int, int] = {}
    for r in rows:
        if r.get("is_entry") and r.get("position"):
            opener.setdefault(int(r["position"]), int(r.get("magic") or 0))
    out: dict[int, list] = {}
    for r in rows:
        m = opener.get(int(r.get("position") or 0), int(r.get("magic") or 0))
        out.setdefault(m if m in magics else -1, []).append(r)
    return out


def account_standing(broker, cfg, now: dt.datetime, account=None, positions=None,
                     cache_seconds: float = 30.0) -> dict:
    """The account's figures from the broker. Never raises: a figure the
    broker could not give is None, with the reason in ``error`` (shown on the
    page). One read of the deal history, split locally by magic number, so the
    rows and the total come from the same moment; the balance and the open
    positions are read straight after it.

    ``made`` (and each bot's ``made``) is balance-based: everything since the
    reset. ``today`` (and each bot's ``today``) is the UK day: every position
    that CLOSED since 00:00 UK (``day_start``), each with its full result
    (every one of its deals, the commission charged when it opened too), once,
    for the bot that opened it - the same figure as the page's Today."""
    now = to_utc(now)
    at = _CACHE["at"]
    if at is not None and cache_seconds and (now - at).total_seconds() < cache_seconds and _CACHE["value"]:
        return _CACHE["value"]
    # MetaTrader's own view, always: never the PAPER wrapper, whose answer for
    # Trend & Breakout's magic would add its simulated deals
    broker = real_broker_of(broker)
    start = _start(cfg, now)
    since = start - LOOKBACK                     # the opening deals of positions that closed after the reset
    day = max(uk_day_start(now), start)
    reset_balance = float(getattr(cfg, "account_reset_balance", 0.0) or 0.0)
    out: dict = {"start": start.isoformat(), "day_start": day.isoformat(), "source": "broker",
                 "made": None, "trading": None, "adjustments": None, "today": None, "open": None,
                 "balance": None, "equity": None, "currency": "", "start_balance": reset_balance or None,
                 "bots": [], "other": None, "error": ""}
    errors: list[str] = []

    def done() -> dict:
        out["error"] = "; ".join(errors)
        _CACHE["at"] = now                      # failures are cached too: a wedged bridge is not hammered
        _CACHE["value"] = out
        return out
    fn = getattr(broker, "deals_since", None)
    if fn is None:
        errors.append("the broker gives no deal history")
        return done()
    def balance_now():
        try:
            return broker.account()
        except Exception:
            return None
    before = balance_now()
    try:
        everything = fn(since, 0, False) or []          # magic 0: every trade on the account
    except Exception as exc:
        errors.append(f"deal history not readable: {exc}")
        return done()
    # the balance read on both sides of the deals: a trade booked in between means read once more,
    # so the balance and the deal rows describe the same moment
    after = balance_now()
    if before is not None and after is not None and abs(float(before.balance) - float(after.balance)) >= 0.005:
        try:
            everything = fn(since, 0, False) or []
            after = balance_now() or after
        except Exception as exc:
            errors.append(f"deal history not readable: {exc}")
            return done()
    if after is not None:
        account = after
    elif account is None:
        errors.append("account not readable")
    pos_list = None
    try:
        pos_list = list(broker.positions(None) or [])
    except Exception as exc:
        pos_list = list(positions) if positions is not None else None
        if pos_list is None:
            errors.append(f"open positions not readable: {exc}")
    trading, n_since, fees_since = _sum(everything, start)
    out.update({"trading": trading, "made_trades": n_since, "made_commission": fees_since})
    if account is not None:
        out["balance"] = round(float(account.balance), 2)
        out["equity"] = round(float(account.equity), 2)
        out["currency"] = str(account.currency or "")
    if out["balance"] is not None and reset_balance > 0:
        # exactly what MetaTrader shows: the balance now, less the balance at the reset
        out["made"] = round(out["balance"] - reset_balance, 2)
        out["adjustments"] = round(out["made"] - trading, 2)    # anything on the balance that is not a trade
    else:
        out["made"] = trading
        out["start_balance"] = round(out["balance"] - trading, 2) if out["balance"] is not None else None
    modes = bot_modes_and_magics(cfg)
    magics = {m for m, _mode in modes.values() if m}
    # open positions, per magic, from the one list
    open_by: dict[int, float] = {}
    n_open: dict[int, int] = {}
    if pos_list is not None:
        for q in pos_list:
            m = int(getattr(q, "magic", 0) or 0)
            k = m if m in magics else -1
            open_by[k] = open_by.get(k, 0.0) + float(q.profit or 0.0) + float(getattr(q, "swap", 0.0) or 0.0)
            n_open[k] = n_open.get(k, 0) + 1
        out["open"] = round(sum(open_by.values()), 2)
    elif out["equity"] is not None and out["balance"] is not None:
        out["open"] = round(out["equity"] - out["balance"], 2)
    split = _split_by_bot(everything, magics)
    per: dict[int, Optional[list]] = {}
    if split is not None:
        per = {m: split.get(m, []) for m in magics}
        per[-1] = split.get(-1, [])
    else:
        # an older bridge: ask per magic; any failure leaves that row and the rest unknown
        for m in magics:
            try:
                per[m] = fn(since, m, False) or []
            except Exception as exc:
                per[m] = None
                errors.append(f"magic {m}'s deals not readable: {exc}")
        known = [r for m in magics if per.get(m) is not None for r in per[m]]
        if all(per.get(m) is not None for m in magics):
            keys = {(r.get("position"), str(r.get("time")), r.get("profit")) for r in known}
            per[-1] = [r for r in everything if (r.get("position"), str(r.get("time")), r.get("profit")) not in keys]
        else:
            per[-1] = None
    # every deal, tagged with the bot that opened it, so the page can work out
    # any period from the broker's own rows (held in memory, never published).
    # A deal from before the reset is kept only for a position still going
    # after it (the commission charged when it opened belongs to its result).
    tagged: list[dict] = []
    if split is None and any(v is None for v in per.values()):
        claimed = {(r.get("position"), str(r.get("time")), r.get("profit"))
                   for m, rows_m in per.items() if m != -1 and rows_m is not None for r in rows_m}
        per[-2] = [r for r in everything if (r.get("position"), str(r.get("time")), r.get("profit")) not in claimed]
    for m, rows_m in per.items():
        for r in rows_m or ():
            t = r.get("time")
            if t is None:
                continue
            tagged.append({"time": to_utc(t), "bot_magic": m, "position": r.get("position"),
                           "profit": float(r.get("profit") or 0.0), "commission": float(r.get("commission") or 0.0),
                           "is_entry": bool(r.get("is_entry")), "symbol": str(r.get("symbol") or ""),
                           "volume": float(r.get("volume") or 0.0)})
    after_reset = {r["position"] for r in tagged if r["time"] >= start and r["position"]}
    tagged = [r for r in tagged if r["time"] >= start or (r["position"] and r["position"] in after_reset)]
    tagged.sort(key=lambda r: r["time"])
    # today: the positions that closed since 00:00 UK, each in full, once
    open_tickets = (sorted(int(getattr(q, "ticket", 0) or 0) for q in pos_list) if pos_list is not None else None)
    today_pos = closed_between(positions_of(tagged, open_tickets), day,
                               uk_midnight(uk_date(now) + dt.timedelta(days=1)))

    def today_of(m) -> tuple[float, int]:
        mine = [p for p in today_pos if p["bot_magic"] == m]
        return round(sum(p["net"] for p in mine), 2), len(mine)
    for bid, label in BOTS:
        magic, mode = modes.get(bid, (0, "PAPER"))
        rows = per.get(magic) if magic else []
        if rows is None:
            b = {"made": None, "today": None, "trades": 0, "today_trades": 0}
        else:
            b_since, b_n_since, _ = _sum(rows, start)
            b_today, b_n_today = today_of(magic) if magic else (0.0, 0)
            b = {"made": b_since, "today": b_today, "trades": b_n_since, "today_trades": b_n_today}
        out["bots"].append({"id": bid, "label": label, "magic": magic, "mode": mode, **b,
                            "open": round(open_by.get(magic, 0.0), 2) if pos_list is not None else None,
                            "open_trades": n_open.get(magic, 0), "retired": bid in RETIRED})
    # standing["today"] - the ACCOUNT's figure for the UK day (since 00:00 UK,
    # Europe/London): every position that closed today, every bot's and any
    # placed by hand, each with its full result (every deal of it, the
    # commission charged when it opened too, even if that was yesterday),
    # counted once. The page's Today and the pulse (mintel/ops/pulse.py reads
    # it as today_pnl) carry exactly this figure. Positions still open are not
    # in it; their open profit is "open".
    t_all = tally(today_pos)
    out.update({"today": t_all["made"], "today_trades": t_all["trades"], "today_commission": t_all["commission"]})
    out["day_basis"] = "UK"
    out["open_tickets"] = open_tickets
    out["deals"] = tagged
    out["deals_complete"] = all(v is not None for v in per.values())
    out["unknown_magics"] = [m for m, v in per.items() if v is None and m != -1]
    rest = per.get(-1)
    if rest is None:
        out["other"] = {"label": "Anything else (placed by hand)", "made": None, "today": None,
                        "open": round(open_by.get(-1, 0.0), 2) if pos_list is not None else None}
    else:
        o_since, _, _ = _sum(rest, start)
        out["other"] = {"label": "Anything else (placed by hand)", "made": o_since, "today": today_of(-1)[0],
                        "open": round(open_by.get(-1, 0.0), 2) if pos_list is not None else None}
    return done()


def practice_figures(data_dir, start: dt.datetime, now: dt.datetime, currency: str = "GBP",
                     end: Optional[dt.datetime] = None) -> dict:
    """Each bot's PAPER results from ``start`` (to ``end``, default now), from
    its own records: practice, never money. {bot id: net}. Trend &
    Breakout's is its paper record's: the positions that closed in the
    period, every deal of each (shown on its card only while it is PAPER)."""
    out: dict = {}
    record = TnbPaperRecord(data_dir)
    if not record.error:
        out["market_intelligence"] = record.net(start, end or (to_utc(now) + dt.timedelta(seconds=1)))
    try:
        from .attribution import build_strategies_range
        s = build_strategies_range(data_dir, start, end or (to_utc(now) + dt.timedelta(seconds=1)), currency, now=now)
    except Exception as exc:
        log.debug("practice figures skipped: %s", exc)
        return out
    for bid, _label in BOTS[1:]:
        tab = s.get(bid) or {}
        v = tab.get("paper_net")
        if v is None:
            v = tab.get("net_today")
        if v is not None:
            out[bid] = round(float(v), 2)
    return out


# ------------------------------------------------------------ the page's periods --
PERIOD_BUTTONS = (("today", "Today"), ("yesterday", "Yesterday"), ("week", "This week"), ("month", "This month"),
                  ("6m", "Last 6 months"), ("1y", "Last year"), ("total", "Total"))


def _months_back(d: dt.date, n: int) -> dt.date:
    y, m = d.year, d.month - n
    while m <= 0:
        m += 12
        y -= 1
    import calendar
    return dt.date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def period_bounds(sd: dict, period: str, date_from: str = "", date_to: str = "",
                  now: Optional[dt.datetime] = None) -> tuple[dt.datetime, dt.datetime, str, str]:
    """(start, end, label, key), in UTC, for one of the page's periods on UK
    CALENDAR DAYS (Europe/London, midnight to midnight; BST and GMT are the
    time-zone database's business, never a fixed offset): Today from 00:00
    UK today, Yesterday 00:00-24:00 UK yesterday, This week from Monday 00:00
    UK, This month from the 1st, the last 6 months and the last year from
    that date so many months back, and a custom range from 00:00 UK on its
    first date to 24:00 UK on its last. Never earlier than the reset; Total
    is from the reset. Not the broker's day, which starts at 22:00 UK."""
    now = to_utc(now or dt.datetime.now(UTC))
    try:
        reset = to_utc(dt.datetime.fromisoformat(str(sd["start"])))
    except Exception:
        reset = _EPOCH
    today = uk_date(now)
    one = dt.timedelta(days=1)
    key = period if period in {k for k, _ in PERIOD_BUTTONS} | {"custom"} else "total"
    if key == "custom":
        try:
            a = dt.date.fromisoformat(date_from) if date_from else dt.date.fromisoformat(date_to)
            b = dt.date.fromisoformat(date_to) if date_to else a
            if b < a:
                a, b = b, a
            lo, hi = dt.date(2000, 1, 1), today + one                 # nothing to count after today
            a, b = min(max(a, lo), hi), min(max(b, lo), hi)
            start, end = uk_midnight(a), uk_midnight(b + one)
        except (ValueError, OverflowError):
            key = "total"
        else:
            label = (f"{a.strftime('%d %b %Y')} to {b.strftime('%d %b %Y')}" if a != b else a.strftime("%a %d %b %Y"))
            return max(start, reset), max(end, reset), label, key
    tomorrow = uk_midnight(today + one)
    if key == "today":
        start, end, label = uk_midnight(today), tomorrow, "Today"
    elif key == "yesterday":
        start, end, label = uk_midnight(today - one), uk_midnight(today), "Yesterday"
    elif key == "week":
        start, end, label = uk_midnight(today - dt.timedelta(days=today.weekday())), tomorrow, "This week"
    elif key == "month":
        start, end, label = uk_midnight(today.replace(day=1)), tomorrow, "This month"
    elif key == "6m":
        start, end, label = uk_midnight(_months_back(today, 6)), tomorrow, "Last 6 months"
    elif key == "1y":
        start, end, label = uk_midnight(_months_back(today, 12)), tomorrow, "Last year"
    else:
        start, end, label, key = reset, tomorrow, "Total", "total"
    return max(start, reset), max(end, reset), label, key


# how the "why" line names a period: (in it, before it)
_PHRASES = {"today": ("today", "before today"), "yesterday": ("yesterday", "before yesterday"),
            "week": ("this week", "before this week"), "month": ("this month", "before this month"),
            "6m": ("in the last 6 months", "before the last 6 months"),
            "1y": ("in the last year", "before the last year"), "custom": ("in these dates", "before these dates")}


def _money(v: float, ccy: str = "GBP", signed: bool = True) -> str:
    sym = {"GBP": "£", "USD": "$", "EUR": "€"}.get(str(ccy or ""), "")
    sign = ("+" if v > 0 else ("-" if v < 0 else "")) if signed else ""
    return f"{sign}{sym}{abs(v):,.2f}"


def why_it_differs(key: str, start: dt.datetime, end: dt.datetime, closed: list, positions: list,
                   made: float, moved: float, ccy: str = "GBP") -> str:
    """One plain line saying why a period's figure (each position that closed
    in it, in full) is not what the trades moved the balance in the same UK
    days: commission charged before the period on a trade that closed in it,
    or charged in it on a trade still open or closed later. "" when they agree."""
    if key == "total" or abs(round(made - moved, 2)) < 0.005:
        return ""
    here, before = _PHRASES.get(key, ("in this period", "before this period"))
    in_ids = {id(p) for p in closed}
    early = [(p, d) for p in closed for d in p["deals"] if d["time"] < start]
    still_open, later = [], []
    for p in positions:
        if id(p) in in_ids:
            continue
        for d in p["deals"]:
            if start <= d["time"] < end:
                (later if p["is_closed"] and p["closed"] >= end else still_open).append((p, d))
    day_before = uk_date(start) - dt.timedelta(days=1)
    if early and key in ("today", "yesterday") and all(uk_date(d["time"]) == day_before for _p, d in early):
        before = "yesterday" if key == "today" else "the day before"

    def amount(pairs) -> str:
        v = round(sum(d["profit"] for _p, d in pairs), 2)
        if all(d["is_entry"] for _p, d in pairs) and v <= 0:
            return f"{_money(v, ccy, signed=False)} of commission was charged"
        return f"{_money(v, ccy)} was booked"

    def trades(pairs) -> tuple[str, int]:
        n = len({id(p) for p, _d in pairs})
        return ("a trade" if n == 1 else f"{n} trades"), n
    parts = []
    if early:
        t, _n = trades(early)
        parts.append(f"{amount(early)} {before} on {t} that closed {here}")
    if still_open:
        t, n = trades(still_open)
        parts.append(f"{amount(still_open)} {here} on {t} still open, which "
                     f"{'counts on the day it closes' if n == 1 else 'count on the day they close'}")
    if later:
        t, n = trades(later)
        parts.append(f"{amount(later)} {here} on {t} that closed after, which "
                     f"{'counts on the day it closed' if n == 1 else 'count on the day they closed'}")
    if not parts:
        return ""
    return f"This differs from what trades moved the balance {here} ({_money(moved, ccy)}) because " \
           + " and ".join(parts) + "."


def period_view(sd: dict, deals: list, period: str, date_from: str = "", date_to: str = "",
                now: Optional[dt.datetime] = None) -> dict:
    """The account and each bot for one period, from the broker's deal rows,
    on UK days (``period_bounds``). Any period but Total: every position that
    CLOSED in it, each once, with its full result (every one of its deals,
    whenever booked), for the bot that opened it - ``closed`` holds them, and
    ``why`` says in one line why that differs from what the trades moved the
    balance in the same days (``balance_moved``), if it does. Total is the
    balance less the balance at the reset (as on the phone), and each bot's
    deals since the reset, so the cards always add up to the account.
    Positions still open count in no period."""
    start, end, label, key = period_bounds(sd, period, date_from, date_to, now)
    deals = list(deals or ())
    positions = positions_of(deals, sd.get("open_tickets"))
    closed = closed_between(positions, start, end)
    booked = [r for r in deals if start <= r["time"] < end]
    unknown = set(sd.get("unknown_magics") or ())
    complete = bool(sd.get("deals_complete", True))
    total = key == "total"
    bots = {}
    for b in sd.get("bots") or ():
        m = b.get("magic")
        if m in unknown or b.get("made") is None:
            bots[b["id"]] = {"made": None, "trades": None, "wins": None, "losses": None, "commission": None}
            continue
        t = tally([p for p in closed if p["bot_magic"] == m])
        if total:                                          # balance-based: its deals since the reset
            t["made"] = round(sum(r["profit"] for r in booked if r["bot_magic"] == m), 2)
        bots[b["id"]] = t
    every = tally(closed)
    moved = round(sum(r["profit"] for r in booked), 2)
    if total:
        trading = moved
        other = round(sum(r["profit"] for r in booked if r["bot_magic"] == -1), 2)
    else:
        trading = every["made"]
        other = tally([p for p in closed if p["bot_magic"] == -1])["made"]
    view = {"key": key, "label": label, "start": start.isoformat(), "end": end.isoformat(),
            "trading": trading, "made": trading, "adjustments": None, "trades": every["trades"],
            "wins": every["wins"], "losses": every["losses"], "commission": every["commission"],
            "bots": bots, "other": other if complete else None,     # the hand-trade row is unknown while a bot is
            "complete": complete, "balance_moved": moved, "closed": closed, "why": ""}
    if total and sd.get("made") is not None:
        view["made"] = sd["made"]                          # the balance less the balance at the reset
        view["adjustments"] = round(sd["made"] - trading, 2) if complete else None
    if not total:
        view["why"] = why_it_differs(key, start, end, closed, positions, trading, moved, str(sd.get("currency") or "GBP"))
    return view


def bot_view(view: dict, magic: int) -> dict:
    """One bot's positions that closed in a ``period_view``'s period, and their tally."""
    mine = [p for p in view.get("closed") or () if p.get("bot_magic") == magic]
    return dict(tally(mine), closed=mine)


def open_charged(deals, open_tickets) -> dict:
    """{ticket: money already booked on an open position} - the commission
    charged when it opened (and any part already closed): on the balance now,
    in its result on the day it closes."""
    if not open_tickets:
        return {}
    want = {int(t) for t in open_tickets}
    out: dict = {}
    for r in deals or ():
        pos = int(r.get("position") or 0)
        if pos in want:
            out[pos] = round(out.get(pos, 0.0) + float(r.get("profit") or 0.0), 2)
    return out


def strategy_days(deals, magic: int, tracking_start: dt.datetime, now: dt.datetime, open_tickets=None,
                  extra=()) -> dict:
    """One bot's own "Results by period" (the folded tiles) on UK days, from
    the account's deal rows: its positions OPENED at or after
    ``tracking_start`` (the rules' measuring start) and closed in each
    period, each in full; ``extra`` adds positions of the same shape (Trend
    & Breakout's paper record on PAPER). {"today": {...}, "periods": [...]}
    in the shape the page's tiles read."""
    now = to_utc(now)
    start = to_utc(tracking_start)
    mine = [p for p in positions_of([r for r in deals or () if r.get("bot_magic") == magic], open_tickets)
            if p["is_closed"] and p["opened"] is not None and p["opened"] >= start]
    mine += [p for p in extra or () if p.get("closed") is not None and p.get("opened") is not None
             and to_utc(p["opened"]) >= start]
    today = uk_date(now)
    far = uk_midnight(today + dt.timedelta(days=1))

    def summary(a: dt.datetime, b: dt.datetime = far) -> dict:
        keep = [float(p["net"]) for p in mine if a <= to_utc(p["closed"]) < b]
        fees = sum(float(p.get("commission") or 0.0) for p in mine if a <= to_utc(p["closed"]) < b)
        wins = sum(1 for v in keep if v > 0)
        return {"net": round(sum(keep), 2), "trades": len(keep), "wins": wins,
                "win_rate": round(wins / len(keep) * 100, 1) if keep else None,
                "commission": round(fees, 2), "before_fees": round(sum(keep) + fees, 2),
                "nets": [round(v, 2) for v in keep]}
    d0 = uk_midnight(today)
    periods = [("Today", summary(max(d0, start))),
               ("Yesterday", summary(uk_midnight(today - dt.timedelta(days=1)), d0)),
               ("This week", summary(uk_midnight(today - dt.timedelta(days=today.weekday())))),
               ("Last 7 days", summary(uk_midnight(today - dt.timedelta(days=6)))),
               ("Last 14 days", summary(uk_midnight(today - dt.timedelta(days=13)))),
               ("Since start", summary(start))]
    return {"today": periods[0][1], "since_start": periods[-1][1], "day_start": d0.isoformat(), "day_basis": "UK",
            "periods": [{"label": k, **{kk: vv for kk, vv in v.items() if kk != "nets"}} for k, v in periods]}


def open_rows(positions, cfg) -> list[dict]:
    """Every open position on the account, with the bot that holds it."""
    labels = dict(BOTS)
    by_magic = {m: labels.get(bid, bid) for bid, (m, _mode) in bot_modes_and_magics(cfg).items()}
    out = []
    for q in positions or ():
        m = int(getattr(q, "magic", 0) or 0)
        opened = getattr(q, "open_time", None)
        out.append({"bot": by_magic.get(m, "Placed by hand" if not m else f"magic {m}"), "magic": m,
                    "ticket": int(getattr(q, "ticket", 0) or 0), "symbol": str(q.symbol),
                    "side": getattr(getattr(q, "side", None), "value", str(getattr(q, "side", ""))),
                    "volume": float(q.volume), "entry": float(q.entry_price), "stop": float(q.sl or 0.0),
                    "target": float(q.tp or 0.0),
                    "profit": round(float(q.profit or 0.0) + float(getattr(q, "swap", 0.0) or 0.0), 2),
                    "opened": to_utc(opened).isoformat() if isinstance(opened, dt.datetime) else ""})
    out.sort(key=lambda r: r["opened"])
    return out
