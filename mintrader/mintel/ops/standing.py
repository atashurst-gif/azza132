"""The account's standing, from the broker alone.

One figure the page leads with: what the account has made since the reset,
after commission and swap, taken straight from MetaTrader's deal history for
EVERY trade on the account (all five bots and anything placed by hand). It
does not depend on any bot's own records, so it cannot be fooled by one: a
paper bot never touches the broker and so can never appear in it.

It is the same number MetaTrader shows: the balance now, less the balance at
the reset. The page shows that sum so it can be checked against the phone.

Under it, the same history split by magic number - one row per bot, plus a
row for anything that carried no bot's number - so the rows always add up to
the big figure.
"""
from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path
from typing import Optional

from ..clock import to_utc

log = logging.getLogger("mintel.standing")

_CACHE: dict = {"at": None, "value": None}

BOTS = (  # (id, label, where its magic and mode come from)
    ("market_intelligence", "Trend & Breakout"),
    ("rapid_scalper", "Rapid Scalper"),
    ("momentum_runner", "Momentum Runner"),
    ("band_breaker", "Band Breaker"),
    ("crowd_fader", "Crowd Fader"),
)


def _start(cfg, now: dt.datetime) -> dt.datetime:
    txt = getattr(cfg, "tracking_start_utc", "") or ""
    try:
        if txt:
            s = dt.datetime.fromisoformat(txt.replace("Z", "+00:00"))
            return to_utc(s if s.tzinfo else s.replace(tzinfo=dt.timezone.utc))
    except ValueError:
        pass
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
    """{bot id: (magic, mode)} as the running bots see them."""
    out: dict[str, tuple[int, str]] = {"market_intelligence": (int(cfg.magic), "LIVE")}
    try:
        from ..scalper.config import ScalperConfig
        sc = ScalperConfig.load(Path(cfg.ops.data_dir) / "scalper.json")
        out["rapid_scalper"] = (int(sc.magic), str(sc.mode).upper())
    except Exception:
        out["rapid_scalper"] = (990_411, "PAPER")
    for bid, block, default in (("momentum_runner", "runner", 990_511), ("band_breaker", "bandbreaker", 990_611),
                                ("crowd_fader", "crowd", 990_711)):
        b = getattr(cfg, block, None)
        mode = str(getattr(b, "mode", "PAPER") or "PAPER").upper()
        if b is not None and not getattr(b, "enabled", True):
            mode = "OFF"
        out[bid] = (int(getattr(b, "magic", default) or default), mode)
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


def account_standing(broker, cfg, now: dt.datetime, account=None, positions=None,
                     cache_seconds: float = 30.0) -> dict:
    """The account's figures from the broker. Never raises: a figure the
    broker could not give is None, with the reason in ``error``."""
    now = to_utc(now)
    at = _CACHE["at"]
    if at is not None and cache_seconds and (now - at).total_seconds() < cache_seconds and _CACHE["value"]:
        return _CACHE["value"]
    start = _start(cfg, now)
    day = max(_day_start(broker, now), start)
    out: dict = {"start": start.isoformat(), "day_start": day.isoformat(), "source": "broker",
                 "made": None, "today": None, "open": None, "balance": None, "equity": None,
                 "currency": "", "start_balance": None, "bots": [], "other": None, "error": ""}
    try:
        account = account if account is not None else broker.account()
    except Exception as exc:
        account = None
        out["error"] = f"account not readable: {exc}"
    if account is not None:
        out["balance"] = round(float(account.balance), 2)
        out["equity"] = round(float(account.equity), 2)
        out["currency"] = str(account.currency or "")
        out["open"] = round(float(account.equity) - float(account.balance), 2)
    fn = getattr(broker, "deals_since", None)
    if fn is None:
        out["error"] = out["error"] or "the broker gives no deal history"
        return out
    try:
        everything = fn(start, 0, False) or []          # magic 0: every trade on the account
    except Exception as exc:
        out["error"] = f"deal history not readable: {exc}"
        return out
    made, n_since, fees_since = _sum(everything, start)
    today, n_today, fees_today = _sum(everything, day)
    out.update({"made": made, "made_trades": n_since, "made_commission": fees_since,
                "today": today, "today_trades": n_today, "today_commission": fees_today})
    if out["balance"] is not None:
        out["start_balance"] = round(out["balance"] - made, 2)
    try:
        open_by_magic: dict[int, float] = {}
        n_open: dict[int, int] = {}
        for p in (positions if positions is not None else broker.positions(None)) or ():
            m = int(getattr(p, "magic", 0) or 0)
            open_by_magic[m] = open_by_magic.get(m, 0.0) + float(p.profit or 0.0) + float(getattr(p, "swap", 0.0) or 0.0)
            n_open[m] = n_open.get(m, 0) + 1
    except Exception:
        open_by_magic, n_open = {}, {}
    rest_since, rest_today = made, today
    rest_open = out["open"]
    modes = bot_modes_and_magics(cfg)
    for bid, label in BOTS:
        magic, mode = modes.get(bid, (0, "PAPER"))
        try:
            rows = (fn(start, magic, False) or []) if magic else []
            b_since, b_n_since, _ = _sum(rows, start)
            b_today, b_n_today, _ = _sum(rows, day)
        except Exception as exc:
            out["error"] = f"{label}'s deals not readable: {exc}"
            b_since = b_today = None
            b_n_since = b_n_today = 0
        b_open = round(open_by_magic.get(magic, 0.0), 2) if magic in open_by_magic else 0.0
        out["bots"].append({"id": bid, "label": label, "magic": magic, "mode": mode,
                            "made": b_since, "today": b_today, "trades": b_n_since, "today_trades": b_n_today,
                            "open": b_open, "open_trades": n_open.get(magic, 0)})
        if b_since is not None:
            rest_since = round(rest_since - b_since, 2)
            rest_today = round(rest_today - b_today, 2)
        if rest_open is not None:
            rest_open = round(rest_open - b_open, 2)
    # anything that carried no bot's magic (a trade placed by hand), so the rows add up
    out["other"] = {"label": "Anything else (placed by hand)", "made": rest_since, "today": rest_today,
                    "open": rest_open}
    _CACHE["at"] = now
    _CACHE["value"] = out
    return out


def practice_figures(data_dir, start: dt.datetime, now: dt.datetime, currency: str = "GBP") -> dict:
    """Each bot's PAPER results since ``start``, from its own records: practice,
    never money. {bot id: net} for the bots that have paper rows."""
    out: dict = {}
    try:
        from .attribution import build_strategies_range
        s = build_strategies_range(data_dir, start, to_utc(now) + dt.timedelta(seconds=1), currency, now=now)
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
