"""Yesterday's high and low: break of them, or rejection at them."""
from __future__ import annotations

from .. import Signal
from ..candidate import SESSION_NAMES, FamilyBase


class PrevDayLevels(FamilyBase):
    code = "PREV_DAY_LEVELS"
    label = "Yesterday's high and low"
    features = ("pdl", "atr")
    sensitive = ()
    GRID_SIZE = 48
    news_axis = False
    AXES = {"mode": ["BREAK", "FADE"], "confirm": ["M5_CLOSE", "M15_CLOSE"],
            "session": ["LONDON", "NEWYORK"], "exit": ["t2", "tr1"],
            "markets": ["FX_MAJOR", "INDEX", "GOLD"], "news": ["IGNORE"]}

    def signals(self, day, p, span, news):
        ser = day.series
        lo, hi = span
        tfm = 5 if p["confirm"] == "M5_CLOSE" else 15
        tb = ser.tf(tfm)
        a, b = tb.range_for(lo, hi)
        pdh, pdl = ser.f("pdh"), ser.f("pdl")
        brk = p["mode"] == "BREAK"
        out = []
        for k in range(max(a, 1), b):
            e = tb.end_i[k]
            up, dn = pdh[e], pdl[e]
            if not up or not dn:
                continue
            c1, c0 = tb.c[k], tb.c[k - 1]
            if brk:
                if c1 > up and c0 <= up:
                    out.append(Signal(e, 1, None, "closed above yesterday's high"))
                elif c1 < dn and c0 >= dn:
                    out.append(Signal(e, -1, None, "closed below yesterday's low"))
            else:
                if tb.h[k] > up and c1 < up:
                    out.append(Signal(e, -1, None, "rejected at yesterday's high"))
                elif tb.l[k] < dn and c1 > dn:
                    out.append(Signal(e, 1, None, "rejected at yesterday's low"))
        return out

    def subject(self, c):
        m = "Break of" if c.p("mode") == "BREAK" else "Rejection at"
        tf = "5-minute" if c.p("confirm") == "M5_CLOSE" else "15-minute"
        return f"{m} yesterday's high and low ({tf} close), {SESSION_NAMES[c.session]} session"

    def rule_words(self, c):
        tf = "5-minute" if c.p("confirm") == "M5_CLOSE" else "15-minute"
        if c.p("mode") == "BREAK":
            r = (f"Buys when a {tf} bar closes above yesterday's high, sells when one closes "
                 f"below yesterday's low.")
        else:
            r = (f"Sells when a {tf} bar pokes above yesterday's high but closes back under it, "
                 f"buys when one pokes below yesterday's low but closes back above it.")
        return [f"Watches yesterday's high and low (UK day) during the {SESSION_NAMES[c.session]} session.",
                r, self.stop_words(c), "Has no number to tune, so it relies on the fresh test alone."]
