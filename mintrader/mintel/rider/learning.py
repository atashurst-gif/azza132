"""What the bot learns from its own closed trades: bucket statistics.

Buckets: entry family, session, symbol group, volatility regime, efficiency
band, cost-ratio band. Each bucket's expectancy (net pips per trade) is
SHRUNK toward the global mean,

    shrunk = (n * bucket_mean + k * global_mean) / (n + k),  k = min_samples

over a ROLLING window of the most recent trades. A bucket counts only with
at least ``min_samples`` trades spread over at least ``min_days`` different
days, so one day can never rewrite anything.

What it feeds: the HISTORICAL MATCH category of the score (a few points out
of 100, neutral at half marks when nothing qualifies). It never bans a
market, never changes a threshold, a stop or a size; history changes
confidence only.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Iterable

DIMENSIONS = ("entry_family", "session", "symbol_group", "vol_regime", "efficiency_band", "cost_band")


def efficiency_band(eff: float) -> str:
    return "low" if eff < 0.35 else "mid" if eff < 0.6 else "high"


def cost_band(ratio: float) -> str:
    return "cheap" if ratio < 0.10 else "fair" if ratio < 0.20 else "dear"


def symbol_group(base: str, quote: str) -> str:
    if "USD" in (base, quote):
        return "USD_" + ("JPY" if "JPY" in (base, quote) else "MAJOR")
    if "JPY" in (base, quote):
        return "JPY_CROSS"
    return "CROSS"


@dataclass
class Outcome:
    day: str                       # the UTC date the trade closed
    net_pips: float
    buckets: dict = field(default_factory=dict)


class BucketStats:
    def __init__(self, min_samples: int = 30, window: int = 600, min_days: int = 3):
        self.min_samples = max(1, int(min_samples))
        self.window = max(10, int(window))
        self.min_days = max(1, int(min_days))
        self.outcomes: deque[Outcome] = deque(maxlen=self.window)

    def add(self, o: Outcome) -> None:
        self.outcomes.append(o)

    def load(self, outcomes: Iterable[Outcome]) -> None:
        for o in outcomes:
            self.add(o)

    def global_mean(self) -> float:
        n = len(self.outcomes)
        return sum(o.net_pips for o in self.outcomes) / n if n else 0.0

    def global_sd(self) -> float:
        n = len(self.outcomes)
        if n < 2:
            return 0.0
        m = self.global_mean()
        return math.sqrt(sum((o.net_pips - m) ** 2 for o in self.outcomes) / (n - 1))

    def bucket(self, dim: str, value: str) -> dict:
        rows = [o for o in self.outcomes if o.buckets.get(dim) == value]
        n = len(rows)
        g = self.global_mean()
        mean = sum(o.net_pips for o in rows) / n if n else 0.0
        k = self.min_samples
        shrunk = (n * mean + k * g) / (n + k) if (n + k) else g
        days = len({o.day for o in rows})
        return {"dimension": dim, "value": value, "n": n, "days": days, "mean": mean, "shrunk": shrunk,
                "win_rate": (sum(1 for o in rows if o.net_pips > 0) / n) if n else 0.0,
                "qualifies": n >= self.min_samples and days >= self.min_days}

    def match(self, buckets: dict) -> tuple[float, str, list[dict]]:
        """(strength 0..1, phrase, the buckets that counted). 0.5 = neutral."""
        g = self.global_mean()
        sd = max(self.global_sd(), 1.0)
        used = []
        for dim in DIMENSIONS:
            val = buckets.get(dim)
            if val is None:
                continue
            b = self.bucket(dim, str(val))
            if b["qualifies"]:
                used.append(b)
        if not used:
            return 0.5, "not enough history yet", []
        edge = sum(b["shrunk"] - g for b in used) / len(used)
        strength = 0.5 + 0.5 * math.tanh(edge / sd * 2.0)
        phrase = ("similar set-ups have done better than average" if strength >= 0.6 else
                  "similar set-ups have done worse than average" if strength <= 0.4 else
                  "similar set-ups have been average")
        return strength, phrase, used

    def table(self) -> list[dict]:
        out = []
        for dim in DIMENSIONS:
            vals = sorted({str(o.buckets.get(dim)) for o in self.outcomes if o.buckets.get(dim) is not None})
            for v in vals:
                out.append(self.bucket(dim, v))
        return out
