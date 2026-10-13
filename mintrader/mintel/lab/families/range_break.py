"""Breakout: a close beyond the last N minutes' high or low."""
from __future__ import annotations

from .. import Signal
from ..candidate import SESSION_NAMES, FamilyBase


class RangeBreak(FamilyBase):
    code = "RANGE_BREAK"
    label = "Breakout"
    features = ("hl", "atr")
    sensitive = ("lookback_min",)
    GRID_SIZE = 108
    AXES = {"lookback_min": [60, 120, 240], "session": ["LONDON", "NEWYORK"],
            "exit": ["t2", "tr1", "tr2"], "markets": ["FX_MAJOR", "FX_MINOR", "INDEX"],
            "news": ["IGNORE", "AVOID_15M"]}

    def signals(self, day, p, span, news):
        ser = day.series
        lo, hi = span
        n = p["lookback_min"]
        top, bot = ser.f(f"hh{n}"), ser.f(f"ll{n}")
        c = ser.c
        out = []
        for i in range(max(lo, 1), hi):
            a, b = top[i], bot[i]
            if not a:
                continue
            cl = c[i]
            if cl > a and c[i - 1] <= a:
                out.append(Signal(i, 1, None, "new high"))
            elif cl < b and c[i - 1] >= b:
                out.append(Signal(i, -1, None, "new low"))
        return out

    def key_words(self, c):
        from ..candidate import SESSION_SHORT
        return f"{c.p('lookback_min')} min, {SESSION_SHORT[c.session]}"

    def subject(self, c):
        return f"{c.p('lookback_min')}-minute breakout in the {SESSION_NAMES[c.session]} session"

    def rule_words(self, c):
        return [f"Watches the highest high and lowest low of the last {c.p('lookback_min')} minutes "
                f"during the {SESSION_NAMES[c.session]} session.",
                "Buys when a minute closes above that high, sells when it closes below that low "
                "(only the first close beyond it).", self.stop_words(c)]
