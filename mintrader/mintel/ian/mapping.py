"""CME futures <-> spot symbols, directions and prices - and the generic
(non-CME) instruments, such as Binance's crypto books traded through a CFD.

CME FX futures are quoted in US DOLLARS PER ONE UNIT OF THE FOREIGN CURRENCY.
For EUR, GBP, AUD and NZD that is the same way round as the spot pair
(EURUSD = dollars per euro), so the direction is the same. For JPY, CAD and
CHF the spot pair is quoted the other way (USDJPY = yen per dollar), so the
futures price is 1 / spot and the direction INVERTS:

    6J (Japanese yen futures) UP  = the yen strengthening = USDJPY DOWN.

An inversion error here trades exactly backwards, which is why every entry
and both directions of every translation have their own unit test
(tests/test_ian_mapping.py).

The contract sizes and tick sizes below are reference values for showing
distances in ticks and describing the market; the feature code measures the
real price grid from the book itself and uses the smaller of the two, so a
changed exchange tick never silently distorts the features. Check the
exchange's own contract specifications before relying on them for money.

GENERIC INSTRUMENTS (``rolls=False``): one order book that never rolls, read
from another venue and traded through a broker symbol that tracks it - the
crypto set reads Binance's public BTCUSDT / ETHUSDT books and trades the
broker's BTC/USD and ETH/USD CFDs. The same definition carries them through
the book, features, regime, score, signal, trail and bridge code:

* the feed symbol is the root (``BTCUSDT``) and the subscription symbol;
* the broker symbol is looked up in the broker's own list (``BTCUSD``,
  ``BTCUSD.a`` ...) and an instrument whose CFD is not found is not traded;
* never inverted, price multiplier 1 (spot price = 1 x the book's price);
* tick size 0 here: the feed reports the price step it uses;
* no front month, no roll.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

MONTH_CODES = {1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M", 7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"}
CODE_MONTHS = {v: k for k, v in MONTH_CODES.items()}
QUARTERLY = (3, 6, 9, 12)


@dataclass(frozen=True)
class FuturesContract:
    root: str                   # CME Globex root, e.g. "6E"
    name: str                   # "Euro FX"
    currency: str               # the foreign currency the future prices (EUR); USD for non-FX context markets
    spot: str                   # the matching spot symbol (EURUSD), "" if none
    inverted: bool              # True when spot = 1 / futures price (JPY, CAD, CHF)
    contract_size: float        # units of the foreign currency per contract (reference)
    tick_size: float            # minimum price increment (reference; the book's own grid wins if finer)
    asset: str = "FX"           # FX, INDEX, RATES, METAL, CRYPTO
    context_only: bool = False  # watched as cross-market evidence, never traded
    last_trade_days_before_third_wed: int = 2
    rolls: bool = True          # a dated future with a quarterly front month; False: one book that never rolls
    venue: str = "CME Globex"   # where the book is read
    price_multiplier: float = 1.0   # spot price = multiplier x book price (generic instruments; never inverted)
    trades_24_7: bool = False   # the market (and its broker CFD) trades through the weekend

    @property
    def same_direction(self) -> bool:
        return not self.inverted

    @property
    def generic(self) -> bool:
        """A non-CME instrument: no roll, never inverted, the feed symbol is the root."""
        return not self.rolls


# The FX futures traded through their spot pair (MODE A).
CONTRACTS: dict[str, FuturesContract] = {
    "6E": FuturesContract("6E", "Euro FX", "EUR", "EURUSD", False, 125_000, 0.00005),
    "6B": FuturesContract("6B", "British Pound", "GBP", "GBPUSD", False, 62_500, 0.0001),
    "6A": FuturesContract("6A", "Australian Dollar", "AUD", "AUDUSD", False, 100_000, 0.00005),
    "6N": FuturesContract("6N", "New Zealand Dollar", "NZD", "NZDUSD", False, 100_000, 0.00005),
    "6C": FuturesContract("6C", "Canadian Dollar", "CAD", "USDCAD", True, 100_000, 0.00005,
                          last_trade_days_before_third_wed=1),
    "6J": FuturesContract("6J", "Japanese Yen", "JPY", "USDJPY", True, 12_500_000, 0.0000005),
    "6S": FuturesContract("6S", "Swiss Franc", "CHF", "USDCHF", True, 125_000, 0.00005),
}

# Cross-market context: evidence, not a requirement, and never traded by this bot.
CONTEXT: dict[str, FuturesContract] = {
    "ES": FuturesContract("ES", "E-mini S&P 500", "USD", "US500", False, 50, 0.25, "INDEX", True),
    "ZN": FuturesContract("ZN", "10-Year T-Note", "USD", "", False, 100_000, 1 / 64, "RATES", True),
    "GC": FuturesContract("GC", "Gold", "USD", "XAUUSD", False, 100, 0.10, "METAL", True),
}

# Generic instruments: Binance's public spot books (a crypto EXCHANGE, not CME), traded through the broker's CFD.
# The tick size is 0: the feed reports the price step it uses (see mintel/ian/feeds/binance.py).
CRYPTO: dict[str, FuturesContract] = {
    "BTCUSDT": FuturesContract("BTCUSDT", "Bitcoin (Binance BTCUSDT)", "BTC", "BTCUSD", False, 1.0, 0.0, "CRYPTO",
                               rolls=False, venue="Binance", trades_24_7=True),
    "ETHUSDT": FuturesContract("ETHUSDT", "Ether (Binance ETHUSDT)", "ETH", "ETHUSD", False, 1.0, 0.0, "CRYPTO",
                               rolls=False, venue="Binance", trades_24_7=True),
}

ALL: dict[str, FuturesContract] = {**CONTRACTS, **CONTEXT, **CRYPTO}


def register_instrument(symbol: str, spot: str, name: str = "", currency: str = "", asset: str = "CRYPTO",
                        venue: str = "Binance", trades_24_7: bool = True,
                        price_multiplier: float = 1.0) -> FuturesContract:
    """Add (or replace) a generic instrument: one book read as ``symbol`` and traded through the broker
    symbol ``spot``. Never inverted, no roll. A CME root can never be replaced this way."""
    root = str(symbol or "").strip().upper()
    if not root or root in CONTRACTS or root in CONTEXT:
        raise ValueError(f"{symbol!r} cannot be registered as a generic instrument")
    if float(price_multiplier) <= 0:
        raise ValueError("the price multiplier must be positive")
    cur = (currency or root.replace("USDT", "").replace("USDC", "").replace("USD", "") or root).upper()
    c = FuturesContract(root, name or f"{cur} ({venue} {root})", cur, str(spot or "").strip().upper(), False, 1.0, 0.0,
                        asset, rolls=False, venue=venue, price_multiplier=float(price_multiplier),
                        trades_24_7=bool(trades_24_7))
    CRYPTO[root] = c
    ALL[root] = c
    return c


# ------------------------------------------------------------------ lookup --

def root_of(symbol: str) -> str:
    """'6EZ6' -> '6E', '6E.c.0' -> '6E', '6E.FUT' -> '6E', '6E' -> '6E'.
    Calendar spreads ('6EZ6-6EH7') give their first leg's root."""
    s = str(symbol or "").strip().upper()
    if not s:
        return ""
    s = s.split("-")[0].split(".")[0].split(" ")[0]
    for r in sorted(ALL, key=len, reverse=True):
        if s.startswith(r):
            rest = s[len(r):]
            if rest == "" or (len(rest) >= 2 and rest[0] in CODE_MONTHS and rest[1:].isdigit()):
                return r
    return ""


def contract(symbol_or_root: str) -> Optional[FuturesContract]:
    return ALL.get(root_of(symbol_or_root))


def _strict(root: str) -> FuturesContract:
    c = contract(root)
    if c is None:
        raise KeyError(f"no futures mapping for {root!r}")
    return c


def spot_symbol(root: str, broker_symbols: Optional[Iterable[str]] = None) -> str:
    """The spot symbol a future trades through. With the broker's symbol list,
    the broker's own spelling (EURUSD, EURUSD.a, EURUSDm ...); '' if the
    broker has none."""
    c = contract(root)
    if c is None or not c.spot:
        return ""
    if broker_symbols is None:
        return c.spot
    names = list(broker_symbols)
    if c.spot in names:
        return c.spot
    for n in sorted(names, key=len):
        letters = "".join(ch for ch in n.upper() if ch.isalnum())
        if letters.startswith(c.spot) and len(letters) <= len(c.spot) + 4:
            return n
    return ""


def future_for_spot(spot: str) -> str:
    """'EURUSD' (or 'EURUSD.a') -> '6E'; '' for a spot with no traded future."""
    s = "".join(ch for ch in str(spot).upper() if ch.isalnum())
    for r, c in CONTRACTS.items():
        if c.spot and s.startswith(c.spot):
            return r
    return ""


# -------------------------------------------------------------- directions --

def spot_direction(root: str, futures_direction: int) -> int:
    """+1 futures bullish -> +1 spot for EUR/GBP/AUD/NZD, -1 spot for JPY/CAD/CHF."""
    c = _strict(root)
    d = (futures_direction > 0) - (futures_direction < 0)
    return -d if c.inverted else d


def futures_direction(root: str, spot_dir: int) -> int:
    """The inverse of :func:`spot_direction` (the translation is its own inverse)."""
    c = _strict(root)
    d = (spot_dir > 0) - (spot_dir < 0)
    return -d if c.inverted else d


def spot_side(root: str, futures_side):
    """A broker Side for the spot order, from the futures view's Side."""
    from ..broker.base import Side
    d = spot_direction(root, futures_side.sign)
    return Side.BUY if d > 0 else Side.SELL


def currency_effect(root: str, currency: str, sign: int) -> int:
    """What news that is good (+1) or bad (-1) for ``currency`` means for the
    FUTURES price. Good for the foreign currency -> futures up; good for the
    dollar -> futures down (more dollars per unit is a weaker dollar). Other
    currencies: no direct effect (0)."""
    c = _strict(root)
    s = (sign > 0) - (sign < 0)
    cur = str(currency or "").upper()
    if c.asset != "FX":
        return 0
    if cur == c.currency:
        return s
    if cur == "USD":
        return -s
    return 0


# ------------------------------------------------------------------ prices --

def futures_to_spot_price(root: str, futures_price: float) -> float:
    """The spot-orientation equivalent of a futures price (before the basis):
    6E 1.0850 -> 1.0850; 6J 0.0066667 -> 150.0 (1/x)."""
    c = _strict(root)
    f = float(futures_price)
    if c.inverted:
        if f <= 0:
            raise ValueError("an inverted future needs a positive price")
        return 1.0 / f
    return f * float(c.price_multiplier or 1.0)


def spot_to_futures_price(root: str, spot_price: float) -> float:
    c = _strict(root)
    s = float(spot_price)
    if c.inverted:
        if s <= 0:
            raise ValueError("an inverted pair needs a positive price")
        return 1.0 / s
    return s / float(c.price_multiplier or 1.0)


def futures_distance_to_spot(root: str, futures_price: float, futures_distance: float) -> float:
    """A futures price DISTANCE expressed in spot units (always >= 0).
    Same direction: unchanged. Inverted: |d(1/f)| = d / f^2 to first order,
    computed exactly as |1/f - 1/(f + d)|."""
    c = _strict(root)
    d = abs(float(futures_distance))
    if not c.inverted:
        return d * float(c.price_multiplier or 1.0)
    f = float(futures_price)
    if f <= 0 or f + d <= 0:
        raise ValueError("an inverted future needs a positive price")
    return abs(1.0 / f - 1.0 / (f + d))


def basis(root: str, futures_price: float, spot_mid: float) -> float:
    """spot - converted futures price: the forward points plus any noise. The
    stop is placed in SPOT terms, so the basis never moves a stop by itself."""
    return float(spot_mid) - futures_to_spot_price(root, futures_price)


# --------------------------------------------------------------- the roll --

def third_wednesday(year: int, month: int) -> dt.date:
    d = dt.date(year, month, 1)
    first_wed = d + dt.timedelta(days=(2 - d.weekday()) % 7)
    return first_wed + dt.timedelta(days=14)


def business_days_before(day: dt.date, n: int) -> dt.date:
    """n weekdays before ``day`` (exchange holidays are not known here)."""
    d = day
    while n > 0:
        d -= dt.timedelta(days=1)
        if d.weekday() < 5:
            n -= 1
    return d


@dataclass(frozen=True)
class FrontMonth:
    root: str
    year: int
    month: int
    code: str
    raw_symbol: str             # Globex style: 6EZ6
    last_trade: dt.date
    roll_date: dt.date          # from this date the NEXT contract is the front

    def to_dict(self) -> dict:
        return {"root": self.root, "raw_symbol": self.raw_symbol, "year": self.year, "month": self.month,
                "last_trade": self.last_trade.isoformat(), "roll_date": self.roll_date.isoformat()}


def subscription_symbol(root: str, today: dt.date, roll_days: int = 8) -> str:
    """What the feed is asked for: the front-month contract of a CME future (6EZ6), the root itself for a
    generic instrument that never rolls (BTCUSDT)."""
    c = _strict(root)
    if c.generic:
        return c.root
    return front_month(root, today, roll_days).raw_symbol


def contract_for(root: str, year: int, month: int) -> FrontMonth:
    c = _strict(root)
    if c.generic:
        raise ValueError(f"{root} is not a dated future: it has no contract months")
    last = business_days_before(third_wednesday(year, month), c.last_trade_days_before_third_wed)
    return FrontMonth(c.root, year, month, MONTH_CODES[month], f"{c.root}{MONTH_CODES[month]}{year % 10}",
                      last, last - dt.timedelta(days=8))


def front_month(root: str, today: dt.date, roll_days: int = 8,
                cycle: Sequence[int] = QUARTERLY) -> FrontMonth:
    """The quarterly contract that carries the liquidity on ``today``: the
    nearest one whose roll date (``roll_days`` calendar days before its last
    trading day) has not yet arrived. Exchange holidays are not modelled, so
    the last trading day can be a day early or late around a holiday; the
    roll happens a week before it, so that never matters for which contract
    is the front."""
    if isinstance(today, dt.datetime):
        today = today.date()
    y = today.year
    for _ in range(3):
        for m in cycle:
            fm = contract_for(root, y, m)
            fm = FrontMonth(fm.root, fm.year, fm.month, fm.code, fm.raw_symbol, fm.last_trade,
                            fm.last_trade - dt.timedelta(days=int(roll_days)))
            if today < fm.roll_date:
                return fm
        y += 1
    raise RuntimeError("no front month found")  # unreachable


def parse_raw_symbol(raw: str) -> Optional[tuple[str, int, int]]:
    """'6EZ6' -> ('6E', 12, 6): the root, the month and the last digit of the year."""
    r = root_of(raw)
    if not r:
        return None
    rest = str(raw).strip().upper()[len(r):]
    if len(rest) < 2 or rest[0] not in CODE_MONTHS or not rest[1:].isdigit():
        return None
    return r, CODE_MONTHS[rest[0]], int(rest[1:]) % 10
