"""Position sizing and the realistic £1,500 account (Aaron, 14 Oct 2026).

Read-only maths. Nothing here sends an order, changes a mode or writes a file: it answers "what size would
this be?" and "what would these recorded trades have done to a £1,500 account at the proposed real-money
settings?".

THE SIZE OF ONE TRADE (``position_size``), from:

* the equity and the risk % (the money at risk = equity x risk %);
* the stop distance (price units);
* the instrument's own money maths - ``contracts.SymbolSpec`` (read, never changed): ``money_per_lot`` turns a
  price distance into account-currency money per 1.0 lot, using the broker's tick value, which already holds
  the contract size and the quote -> account conversion. A spec built here for a GBP account (``spec_for``)
  converts the quote currency to GBP with a stated rate; without a rate there is no size, never a guess;
* the volume grid: ``SymbolSpec.normalise_volume`` floors onto the step and clamps to min/max. A size below the
  minimum lot is NO TRADE (rounding up to the minimum would risk more than asked) - the answer says what the
  minimum lot would risk instead;
* the margin it needs against the free margin (``margin_for``: contract x price x the GBP rate / leverage, the
  ESMA retail leverage by kind of market unless the broker's own margin per lot is given);
* the costs: spread (points x the money of one point) + commission (per lot, round turn, GBP).

EXPOSURE SCALES WITH EQUITY BOTH WAYS: the risk of every trade is a % of the equity at the time, so a smaller
account takes smaller trades and a bigger one bigger trades (``risk_gbp``).

THE PROPOSED REAL-MONEY SETTINGS (review settings, nothing switched on):

* 0.5% risk a trade to start (``TIER_START``);
* 1% for an established performer - shown only as a recommendation (``TIER_ESTABLISHED``);
* 1.5% only with Aaron's separate approval (``TIER_APPROVAL``);
* at most 2% combined planned risk at any moment (``MAX_COMBINED_PCT``);
* safeguards: stop for the day at a 2% loss, for the week at 5%, and stop altogether at a 10% drawdown from
  the peak (``SAFEGUARDS``).

A TIER IS NEVER EARNED BY A STREAK. ``tier_recommendation`` asks for 60+ DEMO trades AND the bootstrap's lower
bound of the per-trade result above zero; even then every increase reads "needs Aaron's approval".

THE £1,500 SIMULATION (``simulate``) replays recorded trades - each as its R multiple (its net after costs /
the money it risked) - in the order they closed, on a £1,500 account at 0.5% a trade, compounding both ways,
with the combined cap and the safeguards applied. A trade whose risk is not recorded uses the strategy's stated
risk a trade (the weekly pick's own ``risk_gbp``) and is counted as such; a trade with no risk at all is left
out and counted. Demo trades and paper trades are simulated SEPARATELY and labelled; they are never added.
"""
from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass, field
from typing import Iterable, Optional, Sequence

from ..contracts import SymbolSpec

# ------------------------------------------------------------------ the proposed settings --
ACCOUNT_GBP = 1500.0
TIER_START = 0.5                 # % of equity a trade, to start
TIER_ESTABLISHED = 1.0           # recommendation only
TIER_APPROVAL = 1.5              # needs Aaron's separate approval
TIERS = (TIER_START, TIER_ESTABLISHED, TIER_APPROVAL)
MAX_TIER = TIER_APPROVAL         # nothing above this, ever, from this code
MAX_COMBINED_PCT = 2.0           # the most planned risk open at once, % of equity
SAFEGUARDS = {"daily_pct": 2.0, "weekly_pct": 5.0, "overall_pct": 10.0}      # review settings
TIER_TRADES = 60                 # demo trades before a higher tier can even be recommended
SIM_DAYS = 20                    # the roadmap's horizon in trading days
BALANCES = (1500.0, 3000.0, 5000.0, 10000.0)

# ESMA retail leverage by kind of market (a UK retail account): what margin a lot needs when the broker's own
# margin figure is not given.
LEVERAGE = {"FX_MAJOR": 30.0, "FX_MINOR": 20.0, "FX_EXOTIC": 20.0, "GOLD": 20.0, "INDEX": 20.0, "SILVER": 10.0,
            "METAL": 10.0, "ENERGY": 10.0, "CRYPTO": 2.0, "EQUITY": 5.0}

# Reference conversion rates into GBP (14 Oct 2026, round figures) used ONLY by the display table and the
# reference specs below. Live sizing always uses the broker's own tick value, which holds the broker's rate.
REFERENCE_GBP = {"GBP": 1.0, "USD": 0.79, "EUR": 0.86, "JPY": 0.0052, "CHF": 0.91, "AUD": 0.52, "CAD": 0.57,
                 "NZD": 0.47}

# Typical instruments for the display table (reference figures, IC Markets-style specs; the live sizing always
# uses the broker's own). name: (kind, digits, point, contract, quote ccy, reference price, typical stop (price),
# typical spread in points, the unit a "pip" means on the table (price), its words)
REFERENCE = {
    "EURUSD": ("FX_MAJOR", 5, 0.00001, 100_000, "USD", 1.09, 0.0015, 2, 0.0001, "pip"),
    "GBPUSD": ("FX_MAJOR", 5, 0.00001, 100_000, "USD", 1.30, 0.0020, 3, 0.0001, "pip"),
    "USDJPY": ("FX_MAJOR", 3, 0.001, 100_000, "JPY", 150.0, 0.20, 3, 0.01, "pip"),
    "XAUUSD": ("GOLD", 2, 0.01, 100, "USD", 2650.0, 5.0, 15, 1.0, "$1 move"),
    "US500": ("INDEX", 1, 0.1, 1, "USD", 5800.0, 15.0, 5, 1.0, "point"),
}
REFERENCE_ORDER = ("EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "US500")
COMMISSION_USD_ROUND_TURN = 7.0      # IC Markets Raw, 3.50 a side, FX and metals (the paper broker's own table)


def gbp_rate(ccy: str, rates: Optional[dict] = None) -> Optional[float]:
    """GBP per one unit of ``ccy``; None when no rate is known (then there is no size, never a guess)."""
    c = str(ccy or "").upper()
    if c == "GBP":
        return 1.0
    table = dict(REFERENCE_GBP)
    table.update({str(k).upper(): float(v) for k, v in (rates or {}).items() if _pos(v)})
    v = table.get(c)
    return float(v) if v and v > 0 else None


def _pos(v) -> bool:
    try:
        return float(v) > 0 and math.isfinite(float(v))
    except (TypeError, ValueError):
        return False


def spec_for(name: str, rates: Optional[dict] = None) -> Optional[SymbolSpec]:
    """A ``SymbolSpec`` for a GBP account from the reference table: its tick value is the contract x the tick in
    the quote currency, converted to GBP. None for an instrument not in the table or without a rate."""
    ref = REFERENCE.get(str(name).upper())
    if ref is None:
        return None
    kind, digits, point, contract, quote = ref[:5]
    rate = gbp_rate(quote, rates)
    if rate is None:
        return None
    base = name[:3] if kind.startswith("FX") else ""
    return SymbolSpec(name=name, digits=digits, point=point, tick_size=point, tick_value=contract * point * rate,
                      contract_size=float(contract), volume_min=0.01, volume_max=100.0, volume_step=0.01,
                      stops_level_points=0.0, freeze_level_points=0.0, base_currency=base, profit_currency=quote,
                      margin_currency=base or quote, asset_group=kind)


def commission_gbp(name: str, volume: float, rates: Optional[dict] = None) -> Optional[float]:
    """Round-turn commission in GBP for ``volume`` lots of a reference instrument (IC Markets Raw: 3.50 a side
    in USD on FX and metals, none on indices, as the paper broker's own table)."""
    ref = REFERENCE.get(str(name).upper())
    if ref is None:
        return None
    kind = ref[0]
    if kind == "INDEX":
        return 0.0
    usd = gbp_rate("USD", rates)
    return None if usd is None else round(COMMISSION_USD_ROUND_TURN * usd * float(volume), 2)


# ------------------------------------------------------------------ one trade's size --
def risk_gbp(equity: float, risk_pct: float) -> float:
    """The money at risk on one trade: a share of the equity NOW, so it shrinks and grows with the account."""
    if not (_pos(equity) and _pos(risk_pct)):
        return 0.0
    return round(float(equity) * float(risk_pct) / 100.0, 2)


def pip_value_per_lot(spec: SymbolSpec) -> float:
    """Account-currency money one pip (``spec.pip_size``) makes on 1.0 lot, via ``money_per_lot``."""
    return float(spec.money_per_lot(spec.pip_size))


def margin_for(spec: SymbolSpec, volume: float, price: float, rate_to_gbp: Optional[float] = None,
               margin_per_lot: Optional[float] = None) -> Optional[float]:
    """The margin ``volume`` lots need, in the account currency: the broker's margin per lot when given, else
    the notional / the ESMA retail leverage of its kind of market. The notional of an FX pair is in its BASE
    currency (contract x lots); of anything else in its quote currency (contract x lots x price).
    ``rate_to_gbp``: GBP per unit of that currency. None when it cannot be worked out honestly."""
    if margin_per_lot is not None and _pos(margin_per_lot):
        return round(float(margin_per_lot) * float(volume), 2)
    lev = LEVERAGE.get(spec.asset_group)
    if not lev or rate_to_gbp is None:
        return None
    notional = float(volume) * float(spec.contract_size)
    if not str(spec.asset_group).startswith("FX"):
        if not _pos(price):
            return None
        notional *= float(price)
    return round(notional * float(rate_to_gbp) / lev, 2)


def margin_currency(spec: SymbolSpec) -> str:
    """The currency a position's notional is in: the base of an FX pair, else the quote."""
    if str(spec.asset_group).startswith("FX") and spec.base_currency:
        return spec.base_currency
    return spec.profit_currency or ""


@dataclass
class Size:
    volume: float = 0.0
    risk_target: float = 0.0          # equity x risk %
    risk_at_stop: float = 0.0         # what the rounded volume loses at the stop, before costs
    costs: Optional[float] = None     # spread + commission at that volume
    risk_with_costs: Optional[float] = None
    per_pip: float = 0.0              # money a pip at that volume
    margin: Optional[float] = None
    margin_ok: Optional[bool] = None
    min_lot_risk: Optional[float] = None   # what the minimum lot would risk (when below it)
    ok: bool = False
    why: str = ""
    notes: list = field(default_factory=list)

    def as_dict(self) -> dict:
        return dict(self.__dict__)


def position_size(equity: float, risk_pct: float, stop_distance: float, spec: SymbolSpec, *,
                  price: Optional[float] = None, rate_to_gbp: Optional[float] = None,
                  margin_per_lot: Optional[float] = None, free_margin: Optional[float] = None,
                  spread_points: Optional[float] = None, commission_per_lot: Optional[float] = None,
                  open_risk_pct: float = 0.0, max_combined_pct: float = MAX_COMBINED_PCT) -> Size:
    """The size of one trade (see the module's notes). ``commission_per_lot``: round turn, account currency.
    ``open_risk_pct``: the planned risk already open; the combined cap trims this trade's risk to what is left.

    Never rounds UP: the volume is floored onto the step, and a size under the minimum lot is no trade."""
    s = Size()
    if spec is None or spec.missing_fields():
        s.why = "the instrument's money maths is incomplete: no size"
        return s
    if not _pos(stop_distance):
        s.why = "no stop: every trade has a stop, so there is no size without one"
        return s
    pct = float(risk_pct or 0.0)
    if pct > MAX_TIER:
        s.notes.append(f"{pct:g}% is above the {MAX_TIER:g}% ceiling: {MAX_TIER:g}% used")
        pct = MAX_TIER
    room = max(0.0, float(max_combined_pct) - float(open_risk_pct or 0.0))
    if pct > room:
        s.notes.append(f"the combined cap of {max_combined_pct:g}% leaves {room:g}% for this trade")
        pct = room
    s.risk_target = risk_gbp(equity, pct)
    if s.risk_target <= 0:
        s.why = "no risk left under the combined cap" if room <= 0 else "no equity or no risk set"
        return s
    per_lot_loss = spec.money_per_lot(stop_distance)
    point_money = spec.money_per_lot(spec.point)
    cost_lot = None
    if spread_points is not None or commission_per_lot is not None:
        cost_lot = float(spread_points or 0.0) * point_money + float(commission_per_lot or 0.0)
    # the costs count against the risk: the stop loss plus the costs stays within the target
    per_lot_total = per_lot_loss + (cost_lot or 0.0)
    raw = s.risk_target / per_lot_total if per_lot_total > 0 else 0.0
    vol = spec.normalise_volume(raw)
    if vol <= 0:
        s.min_lot_risk = round(spec.volume_min * per_lot_total, 2)
        s.why = (f"too small for this market: the minimum lot ({spec.volume_min:g}) would risk "
                 f"£{s.min_lot_risk:,.2f}, more than the £{s.risk_target:,.2f} asked")
        return s
    s.volume = vol
    s.risk_at_stop = round(per_lot_loss * vol, 2)
    s.per_pip = round(pip_value_per_lot(spec) * vol, 4)
    if cost_lot is not None:
        s.costs = round(cost_lot * vol, 2)
        s.risk_with_costs = round(s.risk_at_stop + s.costs, 2)
    s.margin = margin_for(spec, vol, price or 0.0, rate_to_gbp, margin_per_lot)
    if s.margin is not None and free_margin is not None:
        s.margin_ok = s.margin <= float(free_margin)
        if not s.margin_ok:
            s.why = f"not enough free margin: needs £{s.margin:,.2f}, £{float(free_margin):,.2f} free"
            return s
    s.ok = True
    return s


def reference_size(name: str, equity: float, risk_pct: float = TIER_START, rates: Optional[dict] = None) -> Size:
    """``position_size`` for a reference instrument at its typical stop, spread and commission."""
    spec = spec_for(name, rates)
    ref = REFERENCE.get(str(name).upper())
    if spec is None or ref is None:
        return Size(why=f"{name}: no reference figures")
    kind, price, stop, spread = ref[0], ref[5], ref[6], ref[7]
    comm = 0.0 if kind == "INDEX" else COMMISSION_USD_ROUND_TURN * (gbp_rate("USD", rates) or 0.0)
    s = position_size(equity, risk_pct, stop, spec, price=price, rate_to_gbp=gbp_rate(margin_currency(spec), rates),
                      spread_points=spread, commission_per_lot=comm)
    if s.ok:
        s.per_pip = round(spec.money_per_lot(ref[8]) * s.volume, 4)        # the table's own unit
    return s


def combined_ok(open_risk_pcts: Iterable[float], new_pct: float, cap: float = MAX_COMBINED_PCT) -> bool:
    """True when a new trade's planned risk fits under the combined cap with what is already open."""
    return sum(float(x) for x in open_risk_pcts) + float(new_pct) <= cap + 1e-9


# ------------------------------------------------------------------ R multiples --
def r_of(t: dict, fallback_risk: Optional[float] = None) -> tuple[Optional[float], str]:
    """(the trade's R - its net after costs / the money it risked, how the risk was known): its own ``r`` or
    ``risk``/``risk_gbp``, else ``fallback_risk`` (the strategy's stated risk a trade), else None."""
    for k in ("risk", "risk_gbp"):
        if _pos(t.get(k)):
            return float(t["net"]) / float(t[k]), "own"
    if t.get("r") is not None:
        try:
            return float(t["r"]), "own"
        except (TypeError, ValueError):
            pass
    if _pos(fallback_risk):
        return float(t["net"]) / float(fallback_risk), "stated"
    return None, ""


def min_lot_ok(t: dict, proposed_risk: float, slack: float = 1.5) -> bool:
    """False when the trade's own minimum lot would have risked more than ``slack`` x the proposed risk at the
    proposed size (known only where the record holds its volume and its risk). Unknown: True."""
    try:
        vol, risk = float(t.get("volume") or 0.0), float(t.get("risk") or t.get("risk_gbp") or 0.0)
    except (TypeError, ValueError):
        return True
    if vol <= 0 or risk <= 0:
        return True
    min_risk = risk * 0.01 / vol                       # what 0.01 lots of it risked
    return min_risk <= proposed_risk * slack


# ------------------------------------------------------------------ the £1,500 account --
def _uk_day(t: dt.datetime) -> dt.date:
    from .standing import uk_date
    return uk_date(t)


def simulate(trades_by: dict, *, start: float = ACCOUNT_GBP, risk_pct: float = TIER_START,
             cap_pct: float = MAX_COMBINED_PCT, safeguards: Optional[dict] = None,
             fallback_risk: Optional[dict] = None, check_min_lot: bool = True) -> dict:
    """Replay recorded trades on an account of ``start`` at ``risk_pct`` a trade, in the order they closed
    (opened time for the cap where known), compounding both ways. ``trades_by``: {strategy id: [trades]} - ONE
    league's trades (demo or paper, never both). ``fallback_risk``: {id: its stated risk a trade}.

    Returns {start, end, net, ret_pct, max_dd_pct, max_dd_gbp, worst_day, best_day, days, taken, skipped
    (by reason), halted, by (per strategy: taken, net), curve (UK day, equity)}."""
    g = dict(SAFEGUARDS, **(safeguards or {}))
    fallback_risk = fallback_risk or {}
    events = []
    skipped = {"no risk recorded": 0, "combined cap": 0, "daily stop": 0, "weekly stop": 0, "drawdown stop": 0,
               "below the minimum lot": 0}
    for sid, trades in (trades_by or {}).items():
        for t in trades or ():
            if t.get("closed") is None:
                continue
            r, how = r_of(t, fallback_risk.get(sid))
            if r is None or not math.isfinite(r):
                skipped["no risk recorded"] += 1
                continue
            opened = t.get("opened") or t["closed"]
            events.append((opened, t["closed"], sid, r, how, t))
    events.sort(key=lambda e: (e[1], str(e[2])))
    eq = peak = float(start)
    max_dd = max_dd_gbp = 0.0
    day_pnl: dict = {}
    by: dict = {}
    open_book: list = []                 # (closed time, risk pct) of every trade taken: never pruned
    taken = stated = 0
    halted = False
    day_start: dict = {}
    week_start: dict = {}
    curve = []
    for opened, closed, sid, r, how, t in events:
        d = _uk_day(closed)
        wk = d.isocalendar()[:2]
        day_start.setdefault(d, eq)
        week_start.setdefault(wk, eq)
        if halted:
            skipped["drawdown stop"] += 1
            continue
        if eq - day_start[d] <= -g["daily_pct"] / 100.0 * day_start[d]:
            skipped["daily stop"] += 1
            continue
        if eq - week_start[wk] <= -g["weekly_pct"] / 100.0 * week_start[wk]:
            skipped["weekly stop"] += 1
            continue
        # every taken trade still open when this one opened. The book is filtered, never cut down: a trade
        # processed later (it closes later) may have opened earlier and still needs the ones cut away. The
        # last-processed trade open at any moment sees every other trade open then, so the cap holds (at worst
        # it counts an overlap a little early, the safe side).
        if not combined_ok((x[1] for x in open_book if x[0] > opened), risk_pct, cap_pct):
            skipped["combined cap"] += 1
            continue
        risk = eq * risk_pct / 100.0
        if check_min_lot and not min_lot_ok(t, risk):
            skipped["below the minimum lot"] += 1
            continue
        pnl = r * risk
        eq += pnl
        taken += 1
        stated += how == "stated"
        open_book.append((closed, risk_pct))
        day_pnl[d] = day_pnl.get(d, 0.0) + pnl
        b = by.setdefault(sid, {"taken": 0, "net": 0.0})
        b["taken"] += 1
        b["net"] += pnl
        peak = max(peak, eq)
        dd = peak - eq
        max_dd_gbp = max(max_dd_gbp, dd)                 # the worst £ fall ...
        if peak > 0:                                     # ... and the worst % fall, which can be a different one
            max_dd = max(max_dd, dd / peak * 100.0)
        if (peak - eq) >= g["overall_pct"] / 100.0 * peak:
            halted = True
        if not curve or curve[-1][0] != d:
            curve.append([d, round(eq, 2)])
        else:
            curve[-1][1] = round(eq, 2)
    for v in by.values():
        v["net"] = round(v["net"], 2)
    return {"start": round(float(start), 2), "end": round(eq, 2), "net": round(eq - float(start), 2),
            "ret_pct": round((eq / float(start) - 1.0) * 100.0, 2) if start else None,
            "max_dd_pct": round(max_dd, 2), "max_dd_gbp": round(max_dd_gbp, 2),
            "worst_day": round(min(day_pnl.values()), 2) if day_pnl else None,
            "best_day": round(max(day_pnl.values()), 2) if day_pnl else None, "days": len(day_pnl),
            "taken": taken, "stated_risk": stated, "skipped": {k: v for k, v in skipped.items() if v},
            "halted": halted, "by": by, "curve": [(d.isoformat(), e) for d, e in curve],
            "risk_pct": risk_pct, "cap_pct": cap_pct, "safeguards": g}


# ------------------------------------------------------------------ tiers --
def tier_recommendation(demo_trades: int, lcb: Optional[float], current_pct: float = TIER_START,
                        streak: int = 0) -> dict:
    """What the evidence allows, never more: {pct, words, needs_approval}. A higher tier needs TIER_TRADES+ DEMO
    trades and the bootstrap's lower bound of the per-trade result above zero; a streak alone never counts
    (``streak`` is taken only to say so). Every increase needs Aaron's approval."""
    n = int(demo_trades or 0)
    if n < TIER_TRADES or lcb is None or lcb <= 0:
        need = []
        if n < TIER_TRADES:
            need.append(f"{TIER_TRADES - n} more demo trades")
        if lcb is None or lcb <= 0:
            need.append("the cautious per-trade result above zero")
        words = f"Stay at {current_pct:g}%: needs " + " and ".join(need)
        if streak and streak >= 3:
            words += " (a streak alone is never enough)"
        return {"pct": current_pct, "words": words, "needs_approval": False}
    nxt = next((t for t in TIERS if t > current_pct), None)
    if nxt is None:
        return {"pct": current_pct, "words": f"At the {current_pct:g}% ceiling", "needs_approval": False}
    if nxt >= TIER_APPROVAL:
        return {"pct": nxt, "words": f"{nxt:g}% possible: needs Aaron's separate approval", "needs_approval": True}
    return {"pct": nxt, "words": f"Recommend {nxt:g}% (needs Aaron's approval)", "needs_approval": True}


# ------------------------------------------------------------------ the display table --
def balance_table(rows: Sequence[dict], balances: Sequence[float] = BALANCES, risk_pct: float = TIER_START,
                  rates: Optional[dict] = None) -> dict:
    """How the account size changes realistic results, for the display: for each balance, the risk a trade,
    each strategy's realistic £ a trade and £ a day (its mean R x the risk; its trades a day), and the £ a pip
    at the typical stop on the reference instruments. ``rows``: [{id, nick, league, mean_r, per_day}]."""
    out = {"balances": [float(b) for b in balances], "risk_pct": risk_pct, "strategies": [], "instruments": []}
    for r in rows:
        if r.get("mean_r") is None:
            continue
        cells = []
        for b in balances:
            risk = risk_gbp(b, risk_pct)
            per_trade = r["mean_r"] * risk
            cells.append({"risk": risk, "per_trade": round(per_trade, 2),
                          "per_day": round(per_trade * float(r.get("per_day") or 0.0), 2)})
        out["strategies"].append({"id": r.get("id"), "nick": r.get("nick"), "league": r.get("league"),
                                  "cells": cells})
    for name in REFERENCE_ORDER:
        cells = []
        for b in balances:
            s = reference_size(name, b, risk_pct, rates)
            cells.append({"volume": s.volume, "per_pip": s.per_pip if s.ok else None, "ok": s.ok, "why": s.why,
                          "min_lot_risk": s.min_lot_risk})
        ref = REFERENCE[name]
        out["instruments"].append({"name": name, "unit": ref[9], "stop": ref[6], "cells": cells})
    return out
