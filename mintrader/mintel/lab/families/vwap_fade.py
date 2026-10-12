"""Back to the average (Band Breaker's idea): fade a stretch from the VWAP."""
from __future__ import annotations

from math import ceil

from .. import Signal
from ..candidate import SESSION_NAMES, SWING_NOTE, FamilyBase, swings


class VwapFade(FamilyBase):
    code = "VWAP_FADE"
    label = "Back to the average"
    features = ("vwap", "atr")
    sensitive = ("sigma", "stop_atr")
    GRID_SIZE = 108
    AXES = {"sigma": [2.0, 2.5, 3.0], "session": ["ASIA", "LONDON", "US_CASH"],
            "stop_atr": [1.0, 1.5], "exit": ["vwap"],
            "markets": ["FX_MAJOR", "INDEX", "GOLD"], "news": ["IGNORE", "AVOID_15M"]}
    WARM_MIN = 30                   # minutes into the session before the average means anything

    def signals(self, day, p, span, news):
        ser = day.series
        lo, hi = span
        s = p["session"]
        vw, sg = ser.f(f"vwap_{s}"), ser.f(f"sig_{s}")
        atr, c = ser.f("atr5"), ser.c
        k, stop_k = p["sigma"], p["stop_atr"]
        out = []
        for i in range(lo + self.WARM_MIN, hi):
            sd, a = sg[i], atr[i]
            if sd <= 0 or a <= 0:
                continue
            dev = c[i] - vw[i]
            thr = k * sd
            pdev = c[i - 1] - vw[i - 1] if sg[i - 1] > 0 else 0.0
            stop = ceil(stop_k * a)
            if dev >= thr and pdev < thr:
                out.append(Signal(i, -1, stop, "stretched above the average"))
            elif dev <= -thr and pdev > -thr:
                out.append(Signal(i, 1, stop, "stretched below the average"))
        return out

    def subject(self, c):
        short = {"ASIA": "Asia", "LONDON": "London", "US_CASH": "US"}[c.session]
        return (f"Back to the average, {short} session (strays {c.p('sigma'):g}x "
                f"the usual, stop {swings(c.p('stop_atr'))})")

    def rule_words(self, c):
        k = f"{c.p('sigma'):g}"
        return [f"Tracks the {SESSION_NAMES[c.session]} session's average price (weighted by how much "
                f"was traded) and how far prices usually stray from it.",
                f"After the first half hour, sells when a minute closes more than {k} times that "
                f"usual distance above the average and buys when it closes that far below it.",
                f"Stops {swings(c.p('stop_atr'))} away ({SWING_NOTE})."]
