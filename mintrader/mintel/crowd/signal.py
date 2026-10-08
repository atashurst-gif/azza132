"""The positioning read, as plain arithmetic on what Coinversa returns.

Tiers (Coinversa's names, by lifetime profitability): Apex and Sharps are
the proven winners; Grinders and Scrapers are in between; The Crowd,
Bleeders, Trapped and Blown Out are the losing crowd. The API still emits
the old slugs, which is what the tables below key on.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Optional, Sequence

WINNERS = ("money_printer", "smart_money")                                  # Apex, Sharps
CROWD = ("exit_liquidity", "semi_rekt", "full_rekt", "giga_rekt")          # The Crowd, Bleeders, Trapped, Blown Out
TIER_NAMES = {"money_printer": "Apex", "smart_money": "Sharps", "grinder": "Grinders", "humble_earner": "Scrapers",
              "exit_liquidity": "The Crowd", "semi_rekt": "Bleeders", "full_rekt": "Trapped", "giga_rekt": "Blown Out",
              "apex": "Apex", "sharps": "Sharps", "grinders": "Grinders", "scrapers": "Scrapers", "crowd": "The Crowd",
              "bleeders": "Bleeders", "trapped": "Trapped", "blown_out": "Blown Out"}
ALIASES = {"apex": "money_printer", "sharps": "smart_money", "grinders": "grinder", "scrapers": "humble_earner",
           "crowd": "exit_liquidity", "bleeders": "semi_rekt", "trapped": "full_rekt", "blown_out": "giga_rekt"}


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def _bias(long: float, short: float) -> float:
    tot = long + short
    return (long - short) / tot if tot > 0 else 0.0


@dataclass
class Read:
    symbol: str
    coin: str
    ts: dt.datetime
    price: float = 0.0                  # Hyperliquid's mark, as the heatmap reports it
    crowd_long: int = 0
    crowd_short: int = 0
    crowd_bias: float = 0.0             # by wallet count, -1..+1
    crowd_bias_money: float = 0.0       # by notional
    winners_bias: float = 0.0           # Apex + Sharps by notional, -1..+1
    winners_bias_count: float = 0.0
    winners_long_money: float = 0.0
    winners_short_money: float = 0.0
    fuel_below: float = 0.0             # long notional that liquidates within the range below
    fuel_above: float = 0.0             # short notional that liquidates within the range above
    cluster_below: Optional[float] = None   # the heaviest bucket's mid price each side
    cluster_above: Optional[float] = None
    all_long: int = 0
    all_short: int = 0
    direction: int = 0                  # +1 lean long, -1 lean short, 0 nothing
    score: float = 0.0                  # 0..100
    reason: str = ""
    tiers: dict = field(default_factory=dict)

    @property
    def crowd_long_share(self) -> float:
        tot = self.crowd_long + self.crowd_short
        return self.crowd_long / tot if tot else 0.0

    @property
    def fuel_share(self) -> float:
        """The share of nearby liquidation fuel on the side a fade would run into."""
        tot = self.fuel_below + self.fuel_above
        if tot <= 0:
            return 0.5
        return (self.fuel_below if self.direction < 0 else self.fuel_above) / tot if self.direction else 0.5

    def to_row(self) -> dict:
        return {"ts_utc": self.ts.isoformat(), "symbol": self.symbol, "coin": self.coin, "price": self.price,
                "crowd_long": self.crowd_long, "crowd_short": self.crowd_short, "crowd_bias": round(self.crowd_bias, 4),
                "crowd_bias_money": round(self.crowd_bias_money, 4), "winners_bias": round(self.winners_bias, 4),
                "winners_bias_count": round(self.winners_bias_count, 4), "fuel_below": round(self.fuel_below, 2),
                "fuel_above": round(self.fuel_above, 2), "cluster_below": self.cluster_below, "cluster_above": self.cluster_above,
                "direction": self.direction, "score": round(self.score, 1), "reason": self.reason}


def _m(x: float) -> str:
    x = float(x or 0.0)
    return f"{x / 1e6:.1f}m" if abs(x) >= 1e6 else (f"{x / 1e3:.0f}k" if abs(x) >= 1e3 else f"{x:.0f}")


def summarise(symbol: str, coin: str, cohorts: Sequence[dict], heatmap: Optional[dict], ts: dt.datetime,
              stretch: float = 0.5, agree_cap: float = 0.1, min_wallets: int = 200) -> Read:
    r = Read(symbol, coin, ts)
    cl = cs = 0
    clm = csm = 0.0
    wl = ws = 0
    wlm = wsm = 0.0
    for row in cohorts or ():
        tier = str(row.get("pnlTier") or row.get("tier") or "").lower()
        tier = ALIASES.get(tier, tier)
        lc, sc = int(row.get("longCount") or 0), int(row.get("shortCount") or 0)
        ln, sn = float(row.get("longNotional") or 0.0), float(row.get("shortNotional") or 0.0)
        r.tiers[tier] = {"long": lc, "short": sc, "long_money": ln, "short_money": sn, "name": TIER_NAMES.get(tier, tier)}
        r.all_long += lc
        r.all_short += sc
        if tier in CROWD:
            cl += lc; cs += sc; clm += ln; csm += sn
        elif tier in WINNERS:
            wl += lc; ws += sc; wlm += ln; wsm += sn
    r.crowd_long, r.crowd_short = cl, cs
    r.crowd_bias, r.crowd_bias_money = _bias(cl, cs), _bias(clm, csm)
    r.winners_bias, r.winners_bias_count = _bias(wlm, wsm), _bias(wl, ws)
    r.winners_long_money, r.winners_short_money = wlm, wsm
    if heatmap:
        try:
            r.price = float(heatmap.get("currentPrice") or 0.0)
            best_b = best_a = 0.0
            for b in heatmap.get("buckets") or ():
                lo, hi = float(b.get("priceLow") or 0.0), float(b.get("priceHigh") or 0.0)
                mid = (lo + hi) / 2.0
                ln_, sn_ = float(b.get("longNotionalAtRisk") or 0.0), float(b.get("shortNotionalAtRisk") or 0.0)
                if r.price and hi <= r.price * (1 + 1e-9):
                    r.fuel_below += ln_
                    if ln_ > best_b:
                        best_b, r.cluster_below = ln_, mid
                elif r.price and lo >= r.price * (1 - 1e-9):
                    r.fuel_above += sn_
                    if sn_ > best_a:
                        best_a, r.cluster_above = sn_, mid
        except Exception:
            pass
    # the decision
    wallets = cl + cs
    if wallets < min_wallets:
        r.reason = f"too few crowd wallets to read ({wallets} < {min_wallets})"
        return r
    sign = 1 if r.crowd_bias > 0 else -1
    stretched = abs(r.crowd_bias) >= stretch
    winners_with_crowd = r.winners_bias * sign                  # > 0: the winners agree with the crowd
    fade_ok = stretched and winners_with_crowd <= agree_cap
    r.direction = -sign if fade_ok else 0
    stretch_part = 50.0 * _clamp(abs(r.crowd_bias) / 0.8)
    winners_part = 30.0 * _clamp((-winners_with_crowd) / 0.4)
    tot_fuel = r.fuel_below + r.fuel_above
    if tot_fuel > 0:
        share = (r.fuel_below if sign > 0 else r.fuel_above) / tot_fuel   # fuel on the side a fade would run into
        fuel_part = 20.0 * _clamp((share - 0.5) / 0.4)
    else:
        fuel_part = 10.0
    r.score = round(stretch_part + winners_part + fuel_part, 1) if stretched else round(stretch_part, 1)
    crowd_txt = f"crowd {max(r.crowd_long_share, 1 - r.crowd_long_share) * 100:.0f}% {'long' if sign > 0 else 'short'} ({wallets:,} wallets)"
    wb = r.winners_bias
    win_txt = (f"Apex and Sharps {abs(wb) * 100:.0f}% {'long' if wb > 0 else 'short'} by money"
               if abs(wb) >= 0.05 else "Apex and Sharps balanced by money")
    fuel_txt = f"{_m(r.fuel_below)} of longs liquidate below vs {_m(r.fuel_above)} of shorts above" if tot_fuel > 0 else "no liquidation map"
    if not stretched:
        verdict = "not stretched: nothing to fade"
    elif not fade_ok:
        verdict = "stretched, but the winners are with the crowd: stand aside"
    else:
        verdict = f"lean {'SHORT' if r.direction < 0 else 'LONG'}, score {r.score:.0f}/100"
    r.reason = f"{crowd_txt}; {win_txt}; {fuel_txt}; {verdict}"
    return r
