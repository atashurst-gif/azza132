"""GBP per pip -> lots, per instrument. Never guessed.

Aaron sets the exposure (``user_pip_value_gbp``, about GBP 1 per pip); the
bot only converts it into a lot size for each market:

    per-lot pip value (account currency) = tick_value * (pip_size / tick_size)

``tick_value`` is MetaTrader's ``trade_tick_value``: the account-currency
money one ``tick_size`` of price movement makes on 1.0 lot. It already holds
the contract size and the quote -> account conversion, so nothing here
re-derives exchange rates for a GBP account.

The pip: 5-digit and 4-digit FX quote a pip of 0.0001; 3-digit and 2-digit
JPY pairs a pip of 0.01. ``spec.pip_size`` says the same when the broker has
classed the symbol as FX; it is checked against the quote currency here and,
if they disagree, the FX convention wins and the explanation says so.

Rounding: to the broker's volume step, to the NEAREST step (so GBP 1/pip is
as close to GBP 1 as the step allows), never below ``volume_min``, never
above ``volume_max``. The actual GBP per pip the rounded size gives is
reported with every answer.

A non-GBP account converts with a supplied ``rate_fn(from_ccy, to_ccy)``;
without a rate the answer is "no size" - never a guess.
"""
from __future__ import annotations

import math
from typing import Callable, Optional

from ..contracts import FX_CURRENCIES, classify_fx

RateFn = Callable[[str, str], Optional[float]]


def fx_currencies(spec) -> tuple[str, str]:
    """(base, quote) of an FX pair from the broker's fields, else the name."""
    base = str(getattr(spec, "base_currency", "") or "").upper()
    quote = str(getattr(spec, "profit_currency", "") or "").upper()
    if base in FX_CURRENCIES and quote in FX_CURRENCIES and base != quote:
        return base, quote
    b, q, _ = classify_fx(str(getattr(spec, "name", "") or ""))
    return b, q


def fx_pip_size(spec) -> tuple[float, str]:
    """The pip for an FX pair and a note when the broker's own figure was
    not used. Returns (0.0, why) when the symbol is not an FX pair."""
    base, quote = fx_currencies(spec)
    if not base or not quote:
        return 0.0, f"{getattr(spec, 'name', '?')} is not an FX pair"
    expected = 0.01 if quote == "JPY" else 0.0001
    point = float(getattr(spec, "point", 0.0) or 0.0)
    broker_pip = 0.0
    try:
        broker_pip = float(spec.pip_size)
    except Exception:
        broker_pip = 0.0
    if broker_pip > 0 and math.isclose(broker_pip, expected, rel_tol=1e-6):
        return broker_pip, ""
    if point > 0 and point > expected * 1.0000001:
        return 0.0, f"{spec.name}: the price step {point:g} is coarser than a pip ({expected:g}); cannot size"
    return expected, (f"{spec.name}: the broker's pip ({broker_pip:g}) did not match the FX pip for a "
                      f"{quote} quote; used {expected:g}")


def per_lot_pip_value(spec, pip: float) -> float:
    """Account-currency money one pip makes on 1.0 lot."""
    tick_size = float(getattr(spec, "tick_size", 0.0) or 0.0)
    tick_value = float(getattr(spec, "tick_value", 0.0) or 0.0)
    if tick_size <= 0 or tick_value <= 0 or pip <= 0:
        return 0.0
    return tick_value * (pip / tick_size)


def round_to_step(volume: float, step: float, vmin: float, vmax: float) -> float:
    """Nearest step, never below vmin, never above vmax (re-snapped)."""
    if volume <= 0 or step <= 0:
        return 0.0
    steps = math.floor(volume / step + 0.5 + 1e-9)
    v = steps * step
    if vmin > 0 and v < vmin - 1e-12:
        v = vmin
    if vmax > 0 and v > vmax + 1e-12:
        v = math.floor(vmax / step + 1e-9) * step
    decimals = max(0, min(8, -int(math.floor(math.log10(step))) if step < 1 else 0)) + 2
    return round(v, decimals)


def lots_for_gbp_per_pip(spec, gbp_per_pip: float, account_currency: str = "GBP",
                         rate_fn: Optional[RateFn] = None) -> tuple[float, float, str]:
    """(volume, actual GBP per pip, explanation).

    ``volume`` is 0.0 when the market cannot be sized honestly (not FX, no
    tick value, no conversion rate); the explanation says why.
    """
    name = str(getattr(spec, "name", "?"))
    if not (gbp_per_pip and gbp_per_pip > 0):
        return 0.0, 0.0, "no GBP per pip set"
    pip, note = fx_pip_size(spec)
    if pip <= 0:
        return 0.0, 0.0, note
    per_lot_acct = per_lot_pip_value(spec, pip)
    if per_lot_acct <= 0:
        return 0.0, 0.0, f"{name}: the broker gave no tick value; cannot size"
    acct = str(account_currency or "GBP").upper()
    if acct == "GBP":
        rate = 1.0
    else:
        rate = None
        if rate_fn is not None:
            try:
                rate = rate_fn(acct, "GBP")
            except Exception:
                rate = None
        if not rate or rate <= 0:
            return 0.0, 0.0, f"{name}: no {acct}->GBP rate to convert the pip value; cannot size"
    per_lot_gbp = per_lot_acct * float(rate)
    raw = float(gbp_per_pip) / per_lot_gbp
    step = float(getattr(spec, "volume_step", 0.0) or 0.0)
    vmin = float(getattr(spec, "volume_min", 0.0) or 0.0)
    vmax = float(getattr(spec, "volume_max", 0.0) or 0.0)
    if step <= 0:
        return 0.0, 0.0, f"{name}: the broker gave no volume step; cannot size"
    vol = round_to_step(raw, step, vmin, vmax)
    actual = vol * per_lot_gbp
    why = (f"{name}: 1 pip = {pip:g}; 1.0 lot makes {per_lot_gbp:.4f} GBP a pip"
           f"{'' if acct == 'GBP' else f' ({per_lot_acct:.4f} {acct} at {float(rate):.5f} GBP per {acct})'}; "
           f"GBP {gbp_per_pip:g}/pip needs {raw:.4f} lots -> {vol:g} lots (step {step:g}, "
           f"min {vmin:g}, max {vmax:g}) = GBP {actual:.4f} a pip")
    if vmin > 0 and raw < vmin:
        why += "; the broker's smallest size is larger than asked"
    if vmax > 0 and raw > vmax:
        why += "; capped at the broker's largest size"
    if note:
        why += f" ({note})"
    return vol, actual, why


def gbp_per_pip_for(spec, volume: float, account_currency: str = "GBP",
                    rate_fn: Optional[RateFn] = None) -> float:
    """GBP one pip makes on ``volume`` lots (0.0 when unknown)."""
    pip, _ = fx_pip_size(spec)
    per_lot = per_lot_pip_value(spec, pip)
    if per_lot <= 0:
        return 0.0
    acct = str(account_currency or "GBP").upper()
    rate = 1.0
    if acct != "GBP":
        rate = rate_fn(acct, "GBP") if rate_fn else None
        if not rate:
            return 0.0
    return per_lot * float(rate) * float(volume)


def account_per_profit_unit(spec) -> float:
    """Account-currency value of ONE unit of the symbol's profit currency,
    from the broker's own tick value: tick_value / (tick_size * contract_size).
    0.0 when the broker does not give the fields."""
    ts = float(getattr(spec, "tick_size", 0.0) or 0.0)
    tv = float(getattr(spec, "tick_value", 0.0) or 0.0)
    cs = float(getattr(spec, "contract_size", 0.0) or 0.0)
    if ts <= 0 or tv <= 0 or cs <= 0:
        return 0.0
    return tv / (ts * cs)
