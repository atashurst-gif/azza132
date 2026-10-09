"""Currency strength (family 18) and cross-market confirmation (family 19).

Every pair the bot watches gives a normalised move of its base currency
against its quote currency (``PairMove.z``: the tick velocity z-score
blended with the five-minute rate of change in ATRs, so a 30-pip GBPJPY move
is not three times as meaningful as a 10-pip EURCHF move). Each pair credits
its base and debits its quote; a currency's strength is the mean over every
pair it appears in. USD EUR GBP JPY CHF CAD AUD NZD.

Cross-market confirmation separates a BROAD currency move ("the dollar is
rising against everything") from a PAIR-SPECIFIC one: for a trade on
base/quote, every OTHER pair containing the base or the quote is checked for
moving the way the trade implies.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Optional

from ..contracts import FX_CURRENCIES
from .features import Evidence, clip, sign

NAMES = {"USD": "US dollar", "EUR": "euro", "GBP": "pound", "JPY": "yen", "CHF": "Swiss franc",
         "CAD": "Canadian dollar", "AUD": "Australian dollar", "NZD": "New Zealand dollar"}
MIN_MOVE = 0.4          # |z| a pair must show to count as confirming or contradicting


@dataclass
class PairMove:
    symbol: str
    base: str
    quote: str
    z: float


def pair_move_z(velocity: float, roc5_atr: float) -> float:
    """One normalised figure per pair: base up against quote is positive."""
    return 0.6 * max(-6.0, min(6.0, velocity)) + 0.4 * max(-6.0, min(6.0, roc5_atr / 1.4))


@dataclass
class StrengthMap:
    raw: dict = field(default_factory=dict)            # currency -> mean normalised move
    counts: dict = field(default_factory=dict)         # currency -> pairs used
    moves: dict = field(default_factory=dict)          # symbol -> PairMove

    def ranked(self) -> list[tuple[str, float]]:
        return sorted(self.raw.items(), key=lambda kv: kv[1], reverse=True)

    def strongest(self) -> Optional[str]:
        r = self.ranked()
        return r[0][0] if r else None

    def weakest(self) -> Optional[str]:
        r = self.ranked()
        return r[-1][0] if r else None

    def to_dict(self) -> dict:
        return {c: {"strength": round(v, 3), "pairs": self.counts.get(c, 0)} for c, v in self.ranked()}

    # ---------------------------------------------------------- family 18 --
    def pair_alignment(self, base: str, quote: str, d: int) -> Evidence:
        nb, nq = self.counts.get(base, 0), self.counts.get(quote, 0)
        if d == 0 or nb < 2 or nq < 2:
            return Evidence("currency_strength", 0.5, 0, "not enough pairs to read currency strength",
                            {"pairs_base": nb, "pairs_quote": nq})
        diff = self.raw.get(base, 0.0) - self.raw.get(quote, 0.0)
        aligned = diff * d
        strength = clip(aligned / 1.5) if aligned > 0 else 0.0
        strong, weak = (base, quote) if diff > 0 else (quote, base)
        phrase = f"{NAMES.get(strong, strong)} strong, {NAMES.get(weak, weak)} weak"
        return Evidence("currency_strength", strength, sign(diff, 0.1), phrase,
                        {"base": self.raw.get(base, 0.0), "quote": self.raw.get(quote, 0.0), "diff": diff,
                         "pairs_base": nb, "pairs_quote": nq})

    # ---------------------------------------------------------- family 19 --
    def cross_market(self, symbol: str, base: str, quote: str, d: int) -> Evidence:
        if d == 0:
            return Evidence("cross_market", 0.5, 0, "", {})
        legs = {base: [0, 0, 0], quote: [0, 0, 0]}        # confirm, contradict, seen
        for sym, m in self.moves.items():
            if sym == symbol or {m.base, m.quote} == {base, quote}:
                continue
            for ccy, want in ((base, d), (quote, -d)):
                if m.base == ccy:
                    implied = sign(m.z, MIN_MOVE)
                elif m.quote == ccy:
                    implied = -sign(m.z, MIN_MOVE)
                else:
                    continue
                legs[ccy][2] += 1
                if implied == want:
                    legs[ccy][0] += 1
                elif implied == -want:
                    legs[ccy][1] += 1
        seen = legs[base][2] + legs[quote][2]
        if seen < 2:
            return Evidence("cross_market", 0.5, 0, "no other pairs to compare", {"pairs": seen})
        bconf = legs[base][0] / legs[base][2] if legs[base][2] else 0.5
        qconf = legs[quote][0] / legs[quote][2] if legs[quote][2] else 0.5
        bcontra = legs[base][1] / legs[base][2] if legs[base][2] else 0.0
        qcontra = legs[quote][1] / legs[quote][2] if legs[quote][2] else 0.0
        strength = clip(0.5 * (bconf + qconf) - 0.25 * (bcontra + qcontra))
        bw = "rising" if d > 0 else "falling"
        qw = "falling" if d > 0 else "rising"
        if bconf >= 0.6 and qconf >= 0.6:
            phrase = f"broad move: {NAMES.get(base, base)} {bw} and {NAMES.get(quote, quote)} {qw} across the board"
        elif bconf >= 0.6:
            phrase = f"the {NAMES.get(base, base)} is {bw} against most currencies"
        elif qconf >= 0.6:
            phrase = f"the {NAMES.get(quote, quote)} is {qw} against most currencies"
        else:
            phrase = "a move in this pair only"
        return Evidence("cross_market", strength, d if strength >= 0.5 else 0, phrase,
                        {"base_confirm": bconf, "quote_confirm": qconf, "base_contra": bcontra,
                         "quote_contra": qcontra, "pairs": seen})

    def confirms(self, symbol: str, base: str, quote: str, d: int) -> Optional[bool]:
        """For FlowLock X: True confirmed, False confirmation gone, None unknown."""
        e = self.cross_market(symbol, base, quote, d)
        if e.values.get("pairs", 0) < 2:
            return None
        if e.strength >= 0.4:
            return True
        if e.strength < 0.2:
            return False
        return None


def compute_strength(moves: Iterable[PairMove], iterations: int = 30, ridge: float = 0.5) -> StrengthMap:
    """Least-squares currency strengths: each pair says z ~= s(base) - s(quote).
    Solved by a few Gauss-Seidel sweeps, centred on zero, with a small ridge
    so a currency seen in one pair only is not handed the other leg's move
    (AUDUSD falling because the DOLLAR is strong does not make AUD weak)."""
    sm = StrengthMap()
    pairs = []
    for m in moves:
        if m.base not in FX_CURRENCIES or m.quote not in FX_CURRENCIES or m.base == m.quote:
            continue
        sm.moves[m.symbol] = m
        pairs.append(m)
    ccys = sorted({m.base for m in pairs} | {m.quote for m in pairs})
    if not ccys:
        return sm
    st = {c: 0.0 for c in ccys}
    for _ in range(max(1, iterations)):
        for c in ccys:
            acc, n = 0.0, 0
            for m in pairs:
                if m.base == c:
                    acc += m.z + st[m.quote]
                    n += 1
                elif m.quote == c:
                    acc += st[m.base] - m.z
                    n += 1
            st[c] = acc / (n + ridge) if n else 0.0
        mean = sum(st.values()) / len(st)
        for c in ccys:
            st[c] -= mean
    for c in ccys:
        sm.raw[c] = st[c]
        sm.counts[c] = sum(1 for m in pairs if c in (m.base, m.quote))
    return sm
