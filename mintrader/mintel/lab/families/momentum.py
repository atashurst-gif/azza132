"""Momentum burst (Formula 1's idea).

A strong, steady push over six 5-minute bars (at least four closing in its
direction, the whole move at least k ATR), then the trade on the break of that
push's extreme within the next hour, with or without a shallow pullback first.
"""
from __future__ import annotations

from .. import Signal
from ..candidate import SWING_NOTE, FamilyBase, swings


class MomentumContinuation(FamilyBase):
    code = "MOMENTUM_CONTINUATION"
    label = "Momentum burst"
    features = ("atr",)
    sensitive = ("impulse_atr",)
    GRID_SIZE = 108
    TUNABLE = {"impulse_atr": (0.5, 6.0)}
    AXES = {"impulse_atr": [1.5, 2.0, 2.5], "pullback": ["NONE", "HALF"], "session": ["BOTH"],
            "exit": ["t15w", "tr1", "tr2"], "markets": ["FX_MINOR", "FX_MAJOR", "INDEX"],
            "news": ["IGNORE", "AVOID_15M"]}
    EXPIRE_MIN = 60                  # the idea is dead an hour after the push
    SHALLOW_MIN, SHALLOW_MAX = 0.25, 0.5

    def signals(self, day, p, span, news):
        ser = day.series
        lo, hi = span
        if hi <= lo:
            return []
        tb = ser.tf(5)
        a, b = tb.range_for(lo, hi)
        atr, t, h, l, c = ser.f("atr5"), ser.t, ser.h, ser.l, ser.c
        need = p["impulse_atr"]
        shallow = p["pullback"] == "HALF"

        def fire_of(e, up, ext, size, until):
            """The first minute after the push (known at minute ``e``) that takes the
            trade, or None: the push expires, fails, or nothing happens before ``until``."""
            stop_t = t[e] + self.EXPIRE_MIN
            dip = 0
            i = e + 1
            while i < until and t[i] <= stop_t:
                if (c[i] > ext) if up else (c[i] < ext):
                    if not shallow or dip >= self.SHALLOW_MIN * size:
                        return i
                    return None                                 # broke before a big enough pullback: gone
                d = (ext - l[i]) if up else (h[i] - ext)
                if d > dip:
                    dip = d
                if shallow and dip > self.SHALLOW_MAX * size:
                    return None                                 # too deep: the push has failed
                i += 1
            return None

        # Each signal depends only on the bars up to it (what practice sees live): the trade is
        # taken on the FIRST minute at which any live push breaks its extreme, and that signal
        # spends every push already known by then. Pushes are read in order; one known at or
        # after the best minute found so far cannot fire before it, so that minute is final.
        # (Read push by push, a later push firing first was dropped for an earlier push that
        # broke afterwards - bars live practice had not yet seen.)
        out = []
        last = -1                       # minute of the last signal: pushes known by then are spent
        best = None                     # (minute, side): the earliest break among pushes not yet spent
        for k in range(max(a, 5), b):
            e = tb.end_i[k]
            if best is not None and e >= best[0]:
                out.append(Signal(best[0], best[1], None, "broke the push's extreme"))
                last, best = best[0], None
            if e <= last:
                continue                                        # known by the last signal: spent
            if tb.t[k] - tb.t[k - 5] != 25:                     # a gap inside the six bars
                continue
            at = atr[e]
            move = tb.c[k] - tb.o[k - 5]
            if at <= 0 or abs(move) < need * at:
                continue
            up = move > 0
            aligned = 0
            for j in range(k - 5, k + 1):
                if (tb.c[j] > tb.o[j]) if up else (tb.c[j] < tb.o[j]):
                    aligned += 1
            if aligned < 4:
                continue
            ext = max(tb.h[k - 5:k + 1]) if up else min(tb.l[k - 5:k + 1])
            # a break at or after the best minute so far is too late (a tie goes to the earlier push)
            fired = fire_of(e, up, ext, abs(move), hi if best is None else best[0])
            if fired is not None:
                best = (fired, 1 if up else -1)
        if best is not None:
            out.append(Signal(best[0], best[1], None, "broke the push's extreme"))
        return out

    def key_words(self, c):
        return f"{c.p('impulse_atr'):g} swings, {'pullback' if c.p('pullback') == 'HALF' else 'first break'}"

    def subject(self, c):
        pb = "after a shallow pullback" if c.p("pullback") == "HALF" else "on the first break"
        return f"Momentum burst ({swings(c.p('impulse_atr'))}), {pb}"

    def rule_words(self, c):
        pb = ("Waits for a shallow pullback (a quarter to a half of the push) before the break."
              if c.p("pullback") == "HALF" else "Enters as soon as the push's extreme is broken.")
        return [f"Looks for a strong, steady push: six 5-minute bars, at least four closing the same "
                f"way, together moving at least {swings(c.p('impulse_atr'))} ({SWING_NOTE}).",
                "Trades in the direction of the push when a minute closes beyond its extreme "
                "within the next hour, in the London or New York sessions.", pb, self.stop_words(c)]
