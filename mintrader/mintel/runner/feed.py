"""The runner feed: approaches Trend & Breakout still offers, on index
markets, while it is on PAPER - so the Momentum Runner keeps receiving them.

Trend & Breakout's own rules switch an approach off in a market condition
(``scan.disabled_tactic_regimes``) or in a kind of market
(``scan.disabled_tactic_groups``) when it lost with ITS exit. Some of those
approaches earn under the Runner's 3 R trail. While Trend & Breakout trades
on paper, ``runner.feed_tactics`` names approaches its scanner still offers
on index markets even where those two lists would block them. Every other
gate is unchanged: excluded markets are not in the universe at all, and the
score floor, news, spread, cost, risk, exposure caps, breakers and the
daily loss stop apply exactly as to any trade.

Never in LIVE: the feed needs the trader to hold the PAPER wrapper
(``mintel.broker.paper.PaperBroker``) with index orders on paper, so a feed
signal can only ever become a paper Trend & Breakout trade. The Runner's
own order beside it is the only real one.

A feed trade is tagged ``runner_feed`` in the journal row's
``entry_state_json`` (and in its ENTRY event), so Trend & Breakout's own
record can be reported without it: :func:`is_feed_trade`.
"""
from __future__ import annotations

import json
from typing import Any, Mapping, Optional

FEED_KEY = "runner_feed"
FEED_NOTE = ("runner feed: offered for the Momentum Runner while Trend & Breakout is on paper "
             "(its own rules have this approach off here); not part of Trend & Breakout's own record")


def _runner_cfg(cfg):
    return getattr(cfg, "runner", None)


def entries(cfg) -> tuple[tuple[str, str], ...]:
    """(APPROACH, CONDITION or "*") pairs from ``runner.feed_tactics``."""
    rc = _runner_cfg(cfg)
    fn = getattr(rc, "feed_entries", None)
    if callable(fn):
        try:
            return tuple(fn())
        except Exception:
            return ()
    return ()


def active(cfg, broker) -> bool:
    """Is the feed on? Trend & Breakout on PAPER (the setting AND the trader
    really holding the paper wrapper, with index orders on paper), the
    Runner enabled and not OFF, and something listed."""
    from ..config import tnb_mode
    if tnb_mode(cfg) != "PAPER":
        return False
    try:
        from ..broker.paper import PaperBroker
    except Exception:
        return False
    if not isinstance(broker, PaperBroker) or getattr(broker, "read_only", False):
        return False
    if "INDEX" in set(getattr(broker, "live_groups", ()) or ()):
        return False                                   # index orders would be REAL Trend & Breakout orders
    rc = _runner_cfg(cfg)
    if rc is None or not getattr(rc, "enabled", True):
        return False
    if str(getattr(rc, "mode", "PAPER") or "").upper() not in ("LIVE", "PAPER"):
        return False
    return bool(entries(cfg))


def market_wanted(cfg, symbol: str, group: str) -> bool:
    """An index market the Runner rides, and not an excluded one."""
    if str(group or "").upper() != "INDEX":
        return False
    excluded = {str(s).upper() for s in (getattr(getattr(cfg, "universe", None), "excluded_symbols", ()) or ())}
    if str(symbol).upper() in excluded:
        return False
    markets = tuple(getattr(_runner_cfg(cfg), "markets", ()) or ())
    if markets:
        return str(symbol).upper() in {str(m).upper() for m in markets}
    return True


def names_for(cfg, regime: str) -> frozenset[str]:
    """The approaches the feed offers in this market condition: listed for it
    (or for any condition), not ones the Runner skips, and never one switched
    off everywhere (``scan.disabled_tactics``)."""
    cond = str(regime or "").upper()
    skip = {str(t).upper() for t in (getattr(_runner_cfg(cfg), "skip_tactics", ()) or ())}
    off = {str(t).upper() for t in (getattr(getattr(cfg, "scan", None), "disabled_tactics", ()) or ())}
    return frozenset(n for n, c in entries(cfg) if c in ("*", cond) and n not in skip and n not in off)


def is_feed_trade(row: Mapping[str, Any]) -> bool:
    """True for a journal trade row (or an ENTRY event's detail) that the
    runner feed produced."""
    if not row:
        return False
    if row.get(FEED_KEY) in (True, 1, "true", "True"):
        return True
    raw: Optional[Any] = row.get("entry_state_json")
    if not raw:
        return False
    try:
        data = json.loads(raw) if isinstance(raw, str) else dict(raw)
    except Exception:
        return False
    return bool(data.get(FEED_KEY))
