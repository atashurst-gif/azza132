"""Contract specifications and money math.

Every number that turns "price distance" into "money" lives here.  Getting this
wrong is the single most expensive class of bug in an automated trader, so the
module is deliberately explicit and fully unit-tested.

Terminology used consistently throughout the codebase:

* ``point``  - the smallest price increment the broker quotes (``spec.point``).
* ``pip``    - the conventional unit a trader speaks in.  For a 5-digit FX pair
  one pip is 10 points; for a 3-digit JPY pair one pip is 10 points; for metals
  and indices we treat one pip as one point because that is how brokers quote
  their stop levels.
* ``tick_value`` - money (in account currency) gained per ``tick_size`` of price
  movement, per 1.0 lot.  Brokers publish this; it already includes contract
  size and the quote->account conversion.

An instrument is only admitted to the universe when every field needed for the
money math is present and self-consistent (:meth:`SymbolSpec.is_tradable`).  A
symbol we cannot size is a symbol we refuse to trade -- never one we guess at.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Optional

FX_CURRENCIES = ("USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD")


@dataclass(frozen=True)
class SymbolSpec:
    name: str
    digits: int
    point: float
    tick_size: float
    tick_value: float           # account currency per tick_size per 1.0 lot
    contract_size: float
    volume_min: float
    volume_max: float
    volume_step: float
    stops_level_points: float   # broker minimum SL/TP distance, in points
    freeze_level_points: float
    base_currency: str = ""
    profit_currency: str = ""
    margin_currency: str = ""
    margin_initial: float = 0.0
    swap_long: float = 0.0
    swap_short: float = 0.0
    trade_allowed: bool = True
    filling_modes: tuple[str, ...] = ("IOC", "FOK")
    asset_group: str = "UNKNOWN"
    typical_spread_points: float = 0.0

    # ----------------------------------------------------------- validation --
    def missing_fields(self) -> list[str]:
        bad = []
        for f in ("point", "tick_size", "tick_value", "contract_size",
                  "volume_min", "volume_step"):
            if float(getattr(self, f) or 0.0) <= 0.0:
                bad.append(f)
        if self.digits < 0:
            bad.append("digits")
        if self.volume_max and self.volume_max < self.volume_min:
            bad.append("volume_max")
        if not self.filling_modes:
            bad.append("filling_modes")
        return bad

    def is_tradable(self) -> bool:
        return self.trade_allowed and not self.missing_fields()

    # ------------------------------------------------------------ pip logic --
    @property
    def points_per_pip(self) -> float:
        """How many broker points make one conventional pip."""
        if self.asset_group in ("FX_MAJOR", "FX_MINOR", "FX_EXOTIC"):
            # 5-digit majors and 3-digit JPY crosses quote a fractional pip.
            return 10.0 if self.digits in (3, 5) else 1.0
        return 1.0

    @property
    def pip_size(self) -> float:
        return self.point * self.points_per_pip

    def price_to_pips(self, price_distance: float) -> float:
        return price_distance / self.pip_size

    def pips_to_price(self, pips: float) -> float:
        return pips * self.pip_size

    def price_to_points(self, price_distance: float) -> float:
        return price_distance / self.point

    def round_price(self, price: float) -> float:
        return round(price, self.digits)

    def normalise_price(self, price: float) -> float:
        """Snap to the broker's tick grid, then to the quoted precision."""
        snapped = round(price / self.tick_size) * self.tick_size
        return round(snapped, self.digits)

    # -------------------------------------------------------------- volume --
    def normalise_volume(self, volume: float) -> float:
        """Floor onto the broker's volume grid and clamp to the allowed band.

        Flooring (not rounding) matters: rounding up can silently exceed the
        configured risk cap by up to half a lot step.
        """
        if volume <= 0:
            return 0.0
        steps = int((volume + 1e-12) / self.volume_step)
        v = steps * self.volume_step
        v = round(v, 8)
        if v < self.volume_min:
            return 0.0
        if self.volume_max:
            v = min(v, self.volume_max)
        # re-snap after the clamp
        steps = int((v + 1e-12) / self.volume_step)
        return round(steps * self.volume_step, 8)

    # --------------------------------------------------------------- money --
    def money_per_lot(self, price_distance: float) -> float:
        """MAGNITUDE of account-currency P&L for 1.0 lot moving that distance.

        Deliberately unsigned: this is used for risk sizing, where only the
        size of the move matters.  Use :meth:`money` when the direction of the
        result matters.
        """
        return (abs(price_distance) / self.tick_size) * self.tick_value

    def money(self, price_distance: float, volume: float) -> float:
        """SIGNED account-currency P&L for ``volume`` lots.

        The sign follows ``price_distance``, so callers pass the move already
        oriented in the position's favour - ``(exit - entry) * side.sign`` -
        and a losing trade returns a negative number.  An unsigned result here
        would record every loss as a gain, which is not a rounding error but a
        completely fictitious equity curve.
        """
        magnitude = self.money_per_lot(price_distance) * volume
        return -magnitude if price_distance < 0 else magnitude

    def price_distance_for_money(self, money: float, volume: float) -> float:
        if volume <= 0:
            return 0.0
        per_lot = self.tick_value / self.tick_size
        return money / (per_lot * volume)

    def min_stop_distance_price(self, spread_price: float = 0.0,
                                spread_multiple: float = 1.0) -> float:
        """Smallest stop distance the broker will accept, plus spread padding.

        A stop placed inside ``stops_level`` is rejected; one placed just
        outside it but inside the spread is hit instantly on the next tick.
        """
        broker_min = self.stops_level_points * self.point
        return max(broker_min, spread_price * spread_multiple, self.tick_size)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["filling_modes"] = list(self.filling_modes)
        return d


# ------------------------------------------------------- currency inference --

def classify_fx(name: str) -> tuple[str, str, str]:
    """Best-effort (base, quote, group) from a symbol name.

    Broker suffixes (``EURUSD.pro``, ``EURUSD_raw``, ``EURUSDm``) are stripped.
    Returns empty strings when the name is not an FX pair, in which case the
    broker's own base/profit currency fields are authoritative.
    """
    core = "".join(ch for ch in name.upper() if ch.isalpha())
    for cut in range(6, len(core) + 1):
        head = core[:cut]
        if len(head) != 6:
            continue
        base, quote = head[:3], head[3:]
        if base in FX_CURRENCIES and quote in FX_CURRENCIES:
            majors = {"EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD",
                      "AUDUSD", "NZDUSD"}
            group = "FX_MAJOR" if head in majors else "FX_MINOR"
            return base, quote, group
    return "", "", ""


METAL_HINTS = {"XAU": "GOLD", "GOLD": "GOLD", "XAG": "SILVER", "SILVER": "SILVER"}
INDEX_HINTS = ("GER", "DAX", "US30", "US500", "SPX", "NAS", "UK100", "FTSE",
               "JP225", "NIK", "US100", "DE40", "EU50", "AUS200", "HK50")
ENERGY_HINTS = ("WTI", "BRENT", "USOIL", "UKOIL", "XTI", "XBR", "OIL")


def infer_group(name: str, base_ccy: str = "", profit_ccy: str = "",
                path: str = "") -> str:
    """Classify an instrument, preferring the symbol name then the broker path."""
    up = name.upper()
    for hint, group in METAL_HINTS.items():
        if up.startswith(hint) or hint in up:
            return group
    if any(h in up for h in INDEX_HINTS):
        return "INDEX"
    if any(h in up for h in ENERGY_HINTS):
        return "ENERGY"
    _, _, fx_group = classify_fx(name)
    if fx_group:
        return fx_group
    pl = (path or "").upper()
    if "FOREX" in pl:
        return "FX_EXOTIC"
    if "METAL" in pl:
        return "GOLD" if "XAU" in up else "METAL"
    if "INDIC" in pl or "INDEX" in pl or "CASH" in pl:
        return "INDEX"
    if "ENERG" in pl or "OIL" in pl:
        return "ENERGY"
    if "CRYPTO" in pl:
        return "CRYPTO"
    return "UNKNOWN"
