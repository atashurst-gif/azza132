"""After the news: with or against the first 5-minute move after a big release.

DORMANT until the calendar holds at least 40 covered trading days in the past
stretch: a family that trades the news cannot be judged on a handful of
releases. It is registered (and counted in N) only once it wakes, at a stretch
roll.
"""
from __future__ import annotations

from .. import Signal
from ..candidate import FamilyBase


class NewsMove(FamilyBase):
    code = "NEWS_MOVE"
    label = "After the news"
    features = ("atr",)
    sensitive = ("delay_min",)
    dormant = True
    reads_news = True
    GRID_SIZE = 32
    TUNABLE = {"delay_min": (2, 30)}
    news_axis = False
    AXES = {"direction": ["WITH", "AGAINST"], "delay_min": [5, 15], "stop": ["ATR", "SPIKE"],
            "exit": ["t15w", "tr1"], "markets": ["FX_MAJOR", "FX_MINOR"], "news": ["IGNORE"]}
    MIN_SPIKE_ATR = 0.5

    def signals(self, day, p, span, news):
        if news is None:
            return []
        ser = day.series
        lo, hi = span
        t, o, h, l, c = ser.t, ser.o, ser.h, ser.l, ser.c
        atr = ser.f("atr5")
        delay = p["delay_min"]
        out = []
        for e in range(max(lo, 1), hi - delay):
            if news[e] != 0 or news[e - 1] == 0:
                continue                                   # not the minute of a release
            if t[e + 4] - t[e] != 4:
                continue
            m = c[e + 4] - o[e]
            at = atr[e]
            if at <= 0 or abs(m) < self.MIN_SPIKE_ATR * at:
                continue
            i = e + delay - 1
            if t[i] - t[e] != delay - 1:
                continue
            spike_up = m > 0
            side = (1 if spike_up else -1) * (1 if p["direction"] == "WITH" else -1)
            stop = None
            if p["stop"] == "SPIKE":
                if side > 0:
                    stop = c[i] - min(l[e:e + 5]) + 1
                else:
                    stop = max(h[e:e + 5]) - c[i] + 1
                if stop <= 0:
                    continue
            out.append(Signal(i, side, int(stop) if stop else None, "the first move after the news"))
        return out

    def key_words(self, c):
        return (f"{c.p('direction').lower()} after {c.p('delay_min')} min, "
                f"{'spike' if c.p('stop') == 'SPIKE' else 'swing'} stop")

    def subject(self, c):
        d = "with" if c.p("direction") == "WITH" else "against"
        s = "stop beyond the spike" if c.p("stop") == "SPIKE" else "stop 1 swing"
        return f"Trades {d} the first 5-minute move after big news, {c.p('delay_min')} min after, {s}"

    def rule_words(self, c):
        d = ("in the same direction as" if c.p("direction") == "WITH"
             else "the opposite way to")
        s = ("Stops just beyond the news spike." if c.p("stop") == "SPIKE" else self.stop_words(c))
        return [f"Waits for a high-importance news release on one of the pair's two currencies and "
                f"watches the first 5 minutes after it.",
                f"{c.p('delay_min')} minutes after the release it trades {d} that move, if it was "
                f"at least half a swing.", s]
