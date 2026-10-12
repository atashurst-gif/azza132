"""Squeeze release: quiet first, then the break."""
from __future__ import annotations

from .. import Signal
from ..candidate import FamilyBase, swings


class SqueezeRelease(FamilyBase):
    code = "SQUEEZE_RELEASE"
    label = "Squeeze release"
    features = ("bb", "hl", "atr")
    sensitive = ("width_pct", "buffer_atr")
    GRID_SIZE = 36
    news_axis = False
    AXES = {"width_pct": [10, 20], "buffer_atr": [0.0, 0.25], "exit": ["t2", "tr2", "time4h"],
            "markets": ["FX_MAJOR", "FX_MINOR", "INDEX"], "news": ["IGNORE"]}

    def signals(self, day, p, span, news):
        ser = day.series
        lo, hi = span
        pct, atr15 = ser.f("bb_pct"), ser.f("atr15")
        top, bot = ser.f("hh240"), ser.f("ll240")
        c = ser.c
        w, k = p["width_pct"], p["buffer_atr"]
        out = []
        for i in range(max(lo, 1), hi):
            q = pct[i]
            if q < 0 or q > w:
                continue
            a = atr15[i]
            if a <= 0 or not top[i]:
                continue
            up, dn = top[i] + k * a, bot[i] - k * a
            cl = c[i]
            if cl > up and c[i - 1] <= up:
                out.append(Signal(i, 1, None, "released upwards"))
            elif cl < dn and c[i - 1] >= dn:
                out.append(Signal(i, -1, None, "released downwards"))
        return out

    @staticmethod
    def buffer_words(c, sign="plus"):
        k = c.p("buffer_atr")
        return f" ({sign} {swings(k)} of 15-minute movement)" if k else ""

    def subject(self, c):
        buf = f"buffer {swings(c.p('buffer_atr'))}" if c.p("buffer_atr") else "no buffer"
        return f"Squeeze release (quietest {c.p('width_pct')}%, {buf})"

    def rule_words(self, c):
        return [f"Waits for the price to go unusually quiet: how widely it has been swinging, "
                f"measured on 15-minute bars, is among the quietest {c.p('width_pct')}% of its last "
                f"100 readings.",
                f"Then buys if a minute closes above the last 4 hours' high{self.buffer_words(c)}, "
                f"sells if it closes below the 4-hour low{self.buffer_words(c, 'minus')}.",
                self.stop_words(c)]
