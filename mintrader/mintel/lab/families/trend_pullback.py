"""Trend pullback: in a trend, buy the dip to the fast average."""
from __future__ import annotations

from .. import Signal
from ..candidate import FamilyBase, swings


class TrendPullback(FamilyBase):
    code = "TREND_PULLBACK"
    label = "Trend pullback"
    features = ("ema", "atr")
    sensitive = ("touch_atr",)
    GRID_SIZE = 48
    TUNABLE = {"touch_atr": (0.05, 2.0)}
    news_axis = False
    AXES = {"tf": ["M15", "H1"], "emas": ["20/50", "50/200"], "touch_atr": [0.25, 0.5],
            "exit": ["t2", "tr2"], "markets": ["FX_MAJOR", "FX_MINOR", "INDEX"], "news": ["IGNORE"]}

    def signals(self, day, p, span, news):
        ser = day.series
        lo, hi = span
        tfm = 15 if p["tf"] == "M15" else 60
        fast, slow = p["emas"].split("/")
        tb = ser.tf(tfm)
        a, b = tb.range_for(lo, hi)
        ef, es = ser.f(f"ema{fast}_{p['tf']}"), ser.f(f"ema{slow}_{p['tf']}")
        atr = ser.f("atr15")
        touch = p["touch_atr"]
        out = []
        for k in range(max(a, 1), b):
            if tb.t[k] - tb.t[k - 1] != tfm:
                continue
            e, e0 = tb.end_i[k], tb.end_i[k - 1]
            f1, s1, f0, at = ef[e], es[e], ef[e0], atr[e0]
            if f1 <= 0 or s1 <= 0 or f0 <= 0 or at <= 0:
                continue
            tol = touch * at
            if f1 > s1:
                if tb.l[k - 1] <= f0 + tol and tb.c[k] > tb.o[k] and tb.c[k] > f1:
                    out.append(Signal(e, 1, None, "pullback held"))
            elif f1 < s1:
                if tb.h[k - 1] >= f0 - tol and tb.c[k] < tb.o[k] and tb.c[k] < f1:
                    out.append(Signal(e, -1, None, "pullback held"))
        return out

    BARS = {"M15": "15-minute", "H1": "1-hour"}

    def key_words(self, c):
        return f"{c.p('tf')} {c.p('emas')}, touch {c.p('touch_atr'):g}"

    def subject(self, c):
        fast, slow = c.p("emas").split("/")
        return (f"Trend pullback on {self.BARS[c.p('tf')]} bars ({fast} and {slow} bar averages), "
                f"touching within {swings(c.p('touch_atr'))}")

    def rule_words(self, c):
        fast, slow = c.p("emas").split("/")
        return [f"Uses two moving averages of the {self.BARS[c.p('tf')]} closing prices ({fast} and "
                f"{slow} bars). The fast one above the slow one is an uptrend, below is a downtrend.",
                f"Waits for one bar to dip back to the fast average (within {swings(c.p('touch_atr'))}) "
                f"and the next bar to close back in the trend's direction, then trades with the trend.",
                self.stop_words(c)]
