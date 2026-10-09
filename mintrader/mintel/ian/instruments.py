"""The generic (non-CME) instruments Financial Ian can trade, and their CFD rules.

Today that is the crypto set: Binance's PUBLIC spot order books (no account,
no key) for BTCUSDT and ETHUSDT, traded through the broker's BTC/USD and
ETH/USD CFDs. Binance is a crypto exchange; its book is a real central limit
order book with every trade's aggressor side, which is what Ian's order-flow
features need - but it is NOT the CME, and nothing here ever calls it that.

ian.json, all optional (these are the defaults):

    "crypto_instruments": {
        "BTCUSDT": {"broker": "BTCUSD", "name": "Bitcoin", "currency": "BTC",
                    "min_stop_bp": 8, "max_stop_bp": 60, "max_cost_share": 0.3, "target_r": 1.0,
                    "commission_per_lot": 0.0, "grid_bp": 2.0, "price_grid": 0.0},
        "ETHUSDT": {"broker": "ETHUSD", ...}
    }

* ``broker``            the CFD symbol to look for in the broker's own list (its spelling there wins:
                        BTCUSD, BTCUSD.a ...). Not found or not tradable -> that instrument is not traded.
* ``min_stop_bp`` / ``max_stop_bp``   the stop's distance limits in basis points of the price (a CFD's
                        "pip" is one point, so the FX pip limits mean nothing here). A stop that would be
                        further than the maximum is no trade: it is never squeezed to fit.
* ``max_cost_share``    no entry when the CFD's live spread plus the commission is more than this share
                        of the target move (``target_r`` x the stop distance).
* ``commission_per_lot`` round-turn commission per 1.0 lot in the account currency, when the broker
                        charges one (added to the spread in the cost check).
* ``grid_bp`` / ``price_grid``   the price step the features count in. Binance quotes BTC to 0.01 USDT,
                        far finer than anything the order-flow rules mean by a "tick"; the feed groups the
                        book into steps sized from the instrument's own recent volatility (about a quarter
                        of its typical two-minute move, which is what one tick is to a CME FX future),
                        ``grid_bp`` of the price when that cannot be measured, or exactly ``price_grid``.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from typing import Optional

from . import EXCHANGE  # noqa: F401  (re-exported: the data class of Binance's book)
from .mapping import CRYPTO, register_instrument

BINANCE = "binance"


@dataclass(frozen=True)
class CryptoInstrument:
    symbol: str                       # the Binance symbol (the book, the root): BTCUSDT
    broker: str                       # the broker CFD to look for: BTCUSD
    name: str = ""
    currency: str = ""
    min_stop_bp: float = 8.0
    max_stop_bp: float = 60.0
    max_cost_share: float = 0.3
    target_r: float = 1.0
    commission_per_lot: float = 0.0
    max_against_frac: float = 0.15    # the CFD moving this share of the stop distance against the signal = no confirmation
    grid_bp: float = 2.0
    price_grid: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


DEFAULT_CRYPTO: dict[str, dict] = {
    "BTCUSDT": {"broker": "BTCUSD", "name": "Bitcoin", "currency": "BTC", "min_stop_bp": 8.0, "max_stop_bp": 60.0,
                "grid_bp": 2.0},
    "ETHUSDT": {"broker": "ETHUSD", "name": "Ether", "currency": "ETH", "min_stop_bp": 10.0, "max_stop_bp": 80.0,
                "grid_bp": 3.0},
}


def default_crypto_config() -> dict:
    return {k: dict(v) for k, v in DEFAULT_CRYPTO.items()}


def parse_crypto(raw: Optional[dict]) -> dict[str, CryptoInstrument]:
    """``crypto_instruments`` from ian.json -> definitions (registered with the mapping, so the whole engine
    knows them). A bad entry is skipped, never guessed at."""
    out: dict[str, CryptoInstrument] = {}
    names = {f.name for f in fields(CryptoInstrument)}
    for sym, spec in dict(raw if isinstance(raw, dict) else DEFAULT_CRYPTO).items():
        try:
            key = str(sym).strip().upper()
            d = dict(DEFAULT_CRYPTO.get(key, {}))
            d.update(spec if isinstance(spec, dict) else {"broker": str(spec)})
            d = {k: v for k, v in d.items() if k in names and k != "symbol"}
            for k in ("min_stop_bp", "max_stop_bp", "max_cost_share", "target_r", "commission_per_lot",
                      "max_against_frac", "grid_bp", "price_grid"):
                if k in d:
                    d[k] = float(d[k])
            inst = CryptoInstrument(key, str(d.pop("broker", "") or key.replace("USDT", "USD")).upper(), **d)
            if inst.min_stop_bp <= 0 or inst.max_stop_bp <= inst.min_stop_bp or not (0 < inst.max_cost_share <= 1):
                continue
            c = CRYPTO.get(key)
            if c is None or c.spot != inst.broker or (inst.name and inst.currency and c.currency != inst.currency):
                register_instrument(key, inst.broker, f"{inst.name} (Binance {key})" if inst.name else "",
                                    inst.currency)
            out[key] = inst
        except (TypeError, ValueError):
            continue
    return out
