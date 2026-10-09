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
"""
from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from typing import Optional

from ..clock import to_utc

log = logging.getLogger("mintel.standing")

TNB_MAGIC = 990_311
TNB_PAPER_DB = "tnb_paper.sqlite"
_EPOCH = dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc)


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


def _day_start(broker, now: dt.datetime) -> dt.datetime:
    try:
        clock = getattr(broker, "clock", None)
        if clock is not None and hasattr(clock, "day_start_utc"):
            return to_utc(clock.day_start_utc(now))
    except Exception:
        pass
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


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
    positions are read straight after it."""
    now = to_utc(now)
    at = _CACHE["at"]
    if at is not None and cache_seconds and (now - at).total_seconds() < cache_seconds and _CACHE["value"]:
        return _CACHE["value"]
    # MetaTrader's own view, always: never the PAPER wrapper, whose answer for
    # Trend & Breakout's magic would add its simulated deals
    broker = real_broker_of(broker)
    start = _start(cfg, now)
    day = max(_day_start(broker, now), start)
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
        everything = fn(start, 0, False) or []          # magic 0: every trade on the account
    except Exception as exc:
        errors.append(f"deal history not readable: {exc}")
        return done()
    # the balance read on both sides of the deals: a trade booked in between means read once more,
    # so the balance and the deal rows describe the same moment
    after = balance_now()
    if before is not None and after is not None and abs(float(before.balance) - float(after.balance)) >= 0.005:
        try:
            everything = fn(start, 0, False) or []
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
    today, n_today, fees_today = _sum(everything, day)
    out.update({"trading": trading, "made_trades": n_since, "made_commission": fees_since,
                "today": today, "today_trades": n_today, "today_commission": fees_today})
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
                per[m] = fn(start, m, False) or []
            except Exception as exc:
                per[m] = None
                errors.append(f"magic {m}'s deals not readable: {exc}")
        known = [r for m in magics if per.get(m) is not None for r in per[m]]
        if all(per.get(m) is not None for m in magics):
            keys = {(r.get("position"), str(r.get("time")), r.get("profit")) for r in known}
            per[-1] = [r for r in everything if (r.get("position"), str(r.get("time")), r.get("profit")) not in keys]
        else:
            per[-1] = None
    for bid, label in BOTS:
        magic, mode = modes.get(bid, (0, "PAPER"))
        rows = per.get(magic) if magic else []
        if rows is None:
            b = {"made": None, "today": None, "trades": 0, "today_trades": 0}
        else:
            b_since, b_n_since, _ = _sum(rows, start)
            b_today, b_n_today, _ = _sum(rows, day)
            b = {"made": b_since, "today": b_today, "trades": b_n_since, "today_trades": b_n_today}
        out["bots"].append({"id": bid, "label": label, "magic": magic, "mode": mode, **b,
                            "open": round(open_by.get(magic, 0.0), 2) if pos_list is not None else None,
                            "open_trades": n_open.get(magic, 0), "retired": bid in RETIRED})
    # every deal since the reset, tagged with the bot that opened it, so the page
    # can work out any period from the broker's own rows (held in memory, never published)
    tagged: list[dict] = []
    if split is None and any(v is None for v in per.values()):
        claimed = {(r.get("position"), str(r.get("time")), r.get("profit"))
                   for m, rows_m in per.items() if m != -1 and rows_m is not None for r in rows_m}
        per[-2] = [r for r in everything if (r.get("position"), str(r.get("time")), r.get("profit")) not in claimed]
    for m, rows_m in per.items():
        for r in rows_m or ():
            t = r.get("time")
            if t is None or to_utc(t) < start:
                continue
            tagged.append({"time": to_utc(t), "bot_magic": m, "position": r.get("position"),
                           "profit": float(r.get("profit") or 0.0), "commission": float(r.get("commission") or 0.0),
                           "is_entry": bool(r.get("is_entry"))})
    tagged.sort(key=lambda r: r["time"])
    out["deals"] = tagged
    out["deals_complete"] = all(v is not None for v in per.values())
    out["unknown_magics"] = [m for m, v in per.items() if v is None and m != -1]
    rest = per.get(-1)
    if rest is None:
        out["other"] = {"label": "Anything else (placed by hand)", "made": None, "today": None,
                        "open": round(open_by.get(-1, 0.0), 2) if pos_list is not None else None}
    else:
        o_since, _, _ = _sum(rest, start)
        o_today, _, _ = _sum(rest, day)
        out["other"] = {"label": "Anything else (placed by hand)", "made": o_since, "today": o_today,
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
    """(start, end, label, key) on the broker's days - the same days MetaTrader
    and the daily-loss stop use - never earlier than the reset."""
    now = to_utc(now or dt.datetime.now(dt.timezone.utc))
    reset = to_utc(dt.datetime.fromisoformat(sd["start"]))
    try:
        d0 = to_utc(dt.datetime.fromisoformat(sd["day_start"]))
    except Exception:
        d0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if d0 > now:
        d0 -= dt.timedelta(days=1)
    # the broker day boundary is the same clock time every day: work in whole days from it
    while d0 + dt.timedelta(days=1) <= now:
        d0 += dt.timedelta(days=1)
    bdate = (d0 + dt.timedelta(hours=12)).date()           # the broker's calendar date of "today"
    one = dt.timedelta(days=1)

    def day_start(d: dt.date) -> dt.datetime:
        return d0 - dt.timedelta(days=(bdate - d).days)
    key = period if period in {k for k, _ in PERIOD_BUTTONS} | {"custom"} else "total"
    if key == "custom":
        try:
            a = dt.date.fromisoformat(date_from) if date_from else dt.date.fromisoformat(date_to)
            b = dt.date.fromisoformat(date_to) if date_to else a
            if b < a:
                a, b = b, a
            a = max(a, reset.date() - one)                         # nothing to count before the start
            b = min(b, bdate + dt.timedelta(days=1))               # nor after today
            if b < a:
                b = a
            start, end = day_start(a), day_start(b) + one
        except (ValueError, OverflowError):
            key = "total"
        else:
            label = (f"{a.strftime('%d %b %Y')} to {b.strftime('%d %b %Y')}" if a != b else a.strftime("%a %d %b %Y"))
            return max(start, reset), max(end, reset), label, key
    if key == "today":
        start, end, label = d0, d0 + one, "Today"
    elif key == "yesterday":
        start, end, label = d0 - one, d0, "Yesterday"
    elif key == "week":
        start, end, label = day_start(bdate - dt.timedelta(days=bdate.weekday())), d0 + one, "This week"
    elif key == "month":
        start, end, label = day_start(bdate.replace(day=1)), d0 + one, "This month"
    elif key == "6m":
        start, end, label = day_start(_months_back(bdate, 6)), d0 + one, "Last 6 months"
    elif key == "1y":
        start, end, label = day_start(_months_back(bdate, 12)), d0 + one, "Last year"
    else:
        start, end, label, key = reset, d0 + one, "Total", "total"
    return max(start, reset), max(end, reset), label, key


def period_view(sd: dict, deals: list, period: str, date_from: str = "", date_to: str = "",
                now: Optional[dt.datetime] = None) -> dict:
    """The account and each bot for one period, from the broker's deal rows.
    Total is the balance less the balance at the reset (as on the phone); any
    other period is every deal booked in it (profit, commission and swap)."""
    start, end, label, key = period_bounds(sd, period, date_from, date_to, now)
    rows = [r for r in deals or () if start <= r["time"] < end]
    unknown = set(sd.get("unknown_magics") or ())
    bots = {}
    for b in sd.get("bots") or ():
        m = b.get("magic")
        if m in unknown or b.get("made") is None:
            bots[b["id"]] = {"made": None, "trades": None}
            continue
        mine = [r for r in rows if r["bot_magic"] == m]
        bots[b["id"]] = {"made": round(sum(r["profit"] for r in mine), 2),
                         "trades": len({r["position"] for r in mine if not r["is_entry"]})}
    other_rows = [r for r in rows if r["bot_magic"] == -1]
    trading = round(sum(r["profit"] for r in rows), 2)
    view = {"key": key, "label": label, "start": start.isoformat(), "end": end.isoformat(),
            "trading": trading, "made": trading, "adjustments": None,
            "trades": len({r["position"] for r in rows if not r["is_entry"]}),
            "commission": round(sum(abs(r["commission"]) for r in rows), 2),
            "bots": bots, "other": round(sum(r["profit"] for r in other_rows), 2),
            "complete": bool(sd.get("deals_complete", True))}
    if not view["complete"]:
        view["other"] = None                               # the hand-trade row is unknown while a bot is
    if key == "total" and sd.get("made") is not None:
        view["made"] = sd["made"]                          # the balance less the balance at the reset
        view["adjustments"] = round(sd["made"] - trading, 2) if view["complete"] else None
    return view


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
