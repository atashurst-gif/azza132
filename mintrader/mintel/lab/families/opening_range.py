"""Opening range: the break of the first minutes of London or New York.

Band Breaker's structure applied to the markets the lab covers. After the
first 15 or 30 minutes of the London open (08:00 London time) or the New York
cash open (09:30 New York time), a one-minute close beyond the range, by a
small buffer, is the trade. One trade a day at most, the first break.
"""
from __future__ import annotations

from math import ceil

from .. import Signal
from ..candidate import SESSION_NAMES, FamilyBase


class OpeningRange(FamilyBase):
    code = "OPENING_RANGE"
    label = "Opening range"
    features = ("sess", "atr")
    sensitive = ("range_min",)
    GRID_SIZE = 144
    TUNABLE = {"buffer_atr": (0.0, 1.0)}
    AXES = {"session": ["LONDON", "NEWYORK"], "range_min": [15, 30], "buffer_atr": [0.1],
            "stop": ["RANGE", "ATR"], "exit": ["t1", "t2", "tr1"],
            "markets": ["FX_MAJOR", "FX_MINOR", "INDEX"], "news": ["IGNORE", "AVOID_15M"]}

    def signals(self, day, p, span, news):
        ser = day.series
        lo, hi = span
        key = ("L" if p["session"] == "LONDON" else "N") + str(p["range_min"])
        top, bot = ser.f(f"or_{key}_hi"), ser.f(f"or_{key}_lo")
        atr, c = ser.f("atr5"), ser.c
        i = lo
        while i < hi and top[i] == 0:              # the range is not finished yet
            i += 1
        k = p["buffer_atr"]
        far_side = p["stop"] == "RANGE"
        while i < hi:
            a = atr[i]
            if a > 0:
                buf = k * a
                cl = c[i]
                if cl > top[i] + buf:
                    return [Signal(i, 1, ceil(cl - bot[i]) if far_side else None, "broke the high")]
                if cl < bot[i] - buf:
                    return [Signal(i, -1, ceil(top[i] - cl) if far_side else None, "broke the low")]
            i += 1
        return []

    def key_words(self, c):
        from ..candidate import SESSION_SHORT
        return (f"{SESSION_SHORT[c.session]} {c.p('range_min')} min, "
                f"{'range' if c.p('stop') == 'RANGE' else 'swing'} stop")

    def subject(self, c):
        stop = "stop at the far side" if c.p("stop") == "RANGE" else "stop 1 swing"
        return f"{SESSION_NAMES[c.session]} opening range ({c.p('range_min')} min), {stop}"

    def rule_words(self, c):
        bell = "08:00 London time" if c.session == "LONDON" else "09:30 New York time (the US stock market's open)"
        stop = ("Stops at the far side of the opening range." if c.p("stop") == "RANGE"
                else self.stop_words(c))
        return [f"Measures the first {c.p('range_min')} minutes after {bell}.",
                "Buys if a minute closes above that range (plus a small buffer), sells if it "
                "closes below it. One trade a day at most: the first break.", stop]
