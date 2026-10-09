"""The replay's cost model: spread, slippage, commission, delay - and their stress.

* SPREAD is paid because a long is bought at the ASK and sold at the BID
  (and the reverse for a short). Under stress the spread of EVERY tick is
  multiplied around the mid: bid' = mid - k * spread / 2, ask' = mid + k *
  spread / 2. The mid - and so every price feature - is unchanged.
* SLIPPAGE is added to every market fill (entry, exit, and a stop, which
  becomes a market order when it triggers), ``slippage_pips`` per side,
  times the stress multiplier.
* A STOP fills at the stop or WORSE: if the price jumped through it, at the
  first price beyond it (a gap is never filled at the stop).
* COMMISSION is per lot per side in the account currency (GBP), from the
  core's own conversion of the broker's commission; it is not stressed.
* DELAY: an order sent at time t fills at the first tick at or after
  t + ``latency_ms`` (the live engine's signal -> broker round trip).
"""
from __future__ import annotations

from dataclasses import dataclass, replace

from ...broker.base import Side, Tick


@dataclass(frozen=True)
class CostModel:
    spread_mult: float = 1.0
    slippage_pips: float = 0.2
    slippage_mult: float = 1.0
    latency_ms: float = 250.0
    label: str = "normal costs"

    def stressed(self, mult: float, label: str = "") -> "CostModel":
        return replace(self, spread_mult=self.spread_mult * mult, slippage_mult=self.slippage_mult * mult,
                       label=label or f"{mult:g}x spread and slippage")

    def delayed(self, latency_ms: float, label: str = "") -> "CostModel":
        return replace(self, latency_ms=float(latency_ms), label=label or f"{latency_ms:g} ms delay")

    # ------------------------------------------------------------ prices --
    def tick(self, symbol: str, time, bid: float, ask: float) -> Tick:
        k = self.spread_mult
        if k != 1.0:
            mid = 0.5 * (bid + ask)
            half = 0.5 * (ask - bid) * k
            bid, ask = mid - half, mid + half
        return Tick(symbol, time, bid, ask)

    def slip(self, pip: float) -> float:
        return self.slippage_pips * self.slippage_mult * pip

    def entry_fill(self, side: Side, tick: Tick, pip: float) -> float:
        return tick.ask + self.slip(pip) if side is Side.BUY else tick.bid - self.slip(pip)

    def exit_fill(self, side: Side, tick: Tick, pip: float) -> float:
        """Closing at the market: a long sells at the bid, a short buys at the ask."""
        return tick.bid - self.slip(pip) if side is Side.BUY else tick.ask + self.slip(pip)

    @staticmethod
    def stop_touched(side: Side, stop: float, tick: Tick) -> bool:
        return tick.bid <= stop if side is Side.BUY else tick.ask >= stop

    def stop_fill(self, side: Side, stop: float, tick: Tick, pip: float) -> float:
        if side is Side.BUY:
            return min(stop, tick.bid) - self.slip(pip)
        return max(stop, tick.ask) + self.slip(pip)

    def to_dict(self) -> dict:
        return {"spread_mult": self.spread_mult, "slippage_pips": self.slippage_pips,
                "slippage_mult": self.slippage_mult, "latency_ms": self.latency_ms, "label": self.label}


def trade_money(side: Side, entry: float, exit_price: float, pip: float, gbp_per_pip: float,
                volume: float, commission_gbp_per_lot_side: float) -> dict:
    """Gross pips (spread and slippage are already in the fill prices),
    the round-trip commission, and the net result in GBP and in pips."""
    gross = (exit_price - entry) * side.sign / pip if pip else 0.0
    comm = 2.0 * float(commission_gbp_per_lot_side or 0.0) * float(volume)
    net_money = gross * gbp_per_pip - comm
    net_pips = net_money / gbp_per_pip if gbp_per_pip > 0 else gross
    return {"gross_pips": gross, "commission_gbp": comm, "net_money": net_money, "net_pips": net_pips,
            "commission_pips": (comm / gbp_per_pip) if gbp_per_pip > 0 else 0.0}
