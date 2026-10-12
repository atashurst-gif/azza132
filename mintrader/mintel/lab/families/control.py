"""Clock trade (the control): buy or sell at a fixed UK hour, hold 1 or 4 hours.

THIS FAMILY IS EXPECTED TO FAIL. It has no idea behind it, only a clock, so
any pass it earns is luck. Its pass rate at every gate is printed on the lab
page: if the clock trades start passing, the gates are letting noise through
and nothing else the lab says can be trusted.
"""
from __future__ import annotations

from .. import Signal, uk_day_min
from ..candidate import FamilyBase


class ControlTimeOfDay(FamilyBase):
    code = "CONTROL_TIME_OF_DAY"
    label = "Clock trade (control)"
    features = ("atr",)
    sensitive = ()
    GRID_SIZE = 32
    news_axis = False
    is_control = True
    AXES = {"hour_uk": [8, 10, 13, 15], "side": ["BUY", "SELL"], "exit": ["time1h", "time4h"],
            "markets": ["FX_MAJOR", "FX_MINOR"], "news": ["IGNORE"]}

    def signals(self, day, p, span, news):
        ser = day.series
        target = uk_day_min(day.uk) + int(p["hour_uk"]) * 60
        i = day.index_at(target)
        # On a day still being traded live (practice) the newest bar is flat_i, and its entry
        # bar is still to come, so only a finished day rules out i == flat_i (fix, 12 Oct: as
        # written it never fired on a live day; a finished day is judged exactly as before).
        if i >= day.i1 or (i >= day.flat_i and not day.open_ended) or ser.t[i] - target > 2 \
                or i < span[0]:
            return []
        return [Signal(i, 1 if p["side"] == "BUY" else -1, None, "the clock")]

    def subject(self, c):
        return f"Clock trade: {c.p('side').lower()} at {c.p('hour_uk'):02d}:00 UK"

    def rule_words(self, c):
        return [f"A control with no idea behind it: it {c.p('side').lower()}s at {c.p('hour_uk'):02d}:00 "
                f"UK time every day, whatever the market is doing.",
                "It is EXPECTED to fail. If it ever passes the lab's tests, the tests are too easy.",
                self.stop_words(c)]
