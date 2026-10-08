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
    try:
        everything = fn(start, 0, False) or []          # magic 0: every trade on the account
    except Exception as exc:
        errors.append(f"deal history not readable: {exc}")
        return done()
    # the balance and the open positions, read straight after the deals
    try:
        account = broker.account() or account
    except Exception as exc:
        if account is None:
            errors.append(f"account not readable: {exc}")
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
                            "open_trades": n_open.get(magic, 0)})
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
