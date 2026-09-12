"""Time handling.

Three clocks exist and must never be confused:

* ``local``  - the operator's machine (a MacBook, a VPS in some region).  Never
  used for any trading decision.
* ``server`` - the broker's trade-server clock.  MT5 bar timestamps and the MT5
  economic calendar are expressed in this clock.
* ``utc``    - the canonical internal clock.  Every stored timestamp is UTC.

Sessions are defined in real exchange timezones (Asia/Tokyo, Europe/London,
America/New_York) so daylight-saving transitions are handled by the tz database
rather than by hard-coded offsets.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from zoneinfo import ZoneInfo

UTC = dt.timezone.utc

TZ_TOKYO = ZoneInfo("Asia/Tokyo")
TZ_LONDON = ZoneInfo("Europe/London")
TZ_NEWYORK = ZoneInfo("America/New_York")
TZ_SYDNEY = ZoneInfo("Australia/Sydney")


def utcnow() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


def to_utc(ts: dt.datetime) -> dt.datetime:
    if ts.tzinfo is None:
        raise ValueError("naive datetime is ambiguous; attach a tzinfo first")
    return ts.astimezone(UTC)


@dataclass(frozen=True)
class ServerClock:
    """Maps between broker trade-server time and UTC.

    ``offset_seconds`` is measured live by comparing a broker tick's server
    timestamp against UTC, so it tracks the broker's own DST changes without us
    guessing which timezone the broker runs in.
    """

    offset_seconds: int = 0

    def server_to_utc(self, naive_server_ts: dt.datetime) -> dt.datetime:
        if naive_server_ts.tzinfo is not None:
            naive_server_ts = naive_server_ts.replace(tzinfo=None)
        return (naive_server_ts - dt.timedelta(seconds=self.offset_seconds)).replace(tzinfo=UTC)

    def utc_to_server(self, ts: dt.datetime) -> dt.datetime:
        return (to_utc(ts) + dt.timedelta(seconds=self.offset_seconds)).replace(tzinfo=None)

    @staticmethod
    def measure(server_ts_naive: dt.datetime, utc_ts: dt.datetime) -> "ServerClock":
        """Derive the offset, rounded to the nearest minute.

        Brokers publish whole-minute offsets; rounding stops tick latency from
        polluting the estimate.
        """
        delta = server_ts_naive.replace(tzinfo=UTC) - to_utc(utc_ts)
        minutes = round(delta.total_seconds() / 60.0)
        return ServerClock(offset_seconds=int(minutes * 60))


# ---------------------------------------------------------------- sessions ---

@dataclass(frozen=True)
class SessionWindow:
    name: str
    tz: ZoneInfo
    open_hm: tuple[int, int]
    close_hm: tuple[int, int]

    def bounds_for(self, ts: dt.datetime) -> tuple[dt.datetime, dt.datetime]:
        """UTC [open, close) of the session instance containing or preceding ``ts``."""
        local = to_utc(ts).astimezone(self.tz)
        day = local.date()
        o = dt.datetime.combine(day, dt.time(*self.open_hm), tzinfo=self.tz)
        c = dt.datetime.combine(day, dt.time(*self.close_hm), tzinfo=self.tz)
        if c <= o:  # window crosses local midnight
            c += dt.timedelta(days=1)
        if local < o:  # we are before today's open -> use yesterday's instance
            o -= dt.timedelta(days=1)
            c -= dt.timedelta(days=1)
        return to_utc(o), to_utc(c)


SESSIONS: dict[str, SessionWindow] = {
    "ASIA": SessionWindow("ASIA", TZ_TOKYO, (9, 0), (15, 0)),
    "LONDON": SessionWindow("LONDON", TZ_LONDON, (8, 0), (16, 30)),
    "NEWYORK": SessionWindow("NEWYORK", TZ_NEWYORK, (8, 0), (17, 0)),
    "US_CASH": SessionWindow("US_CASH", TZ_NEWYORK, (9, 30), (16, 0)),
}

# London/New-York overlap, derived rather than hard-coded, so it follows both
# DST schedules independently (they shift on different dates).
OVERLAP_SESSIONS = ("LONDON", "NEWYORK")


def active_sessions(ts: dt.datetime) -> tuple[str, ...]:
    ts = to_utc(ts)
    out = []
    for name, win in SESSIONS.items():
        o, c = win.bounds_for(ts)
        if o <= ts < c:
            out.append(name)
    if all(n in out for n in OVERLAP_SESSIONS):
        out.append("LONDON_NY_OVERLAP")
    return tuple(out)


def is_weekend_gap(ts: dt.datetime) -> bool:
    """FX is closed from Fri 21:00 UTC to Sun 21:00 UTC (approximately).

    Used only to avoid alarming on stale ticks over the weekend; never to
    authorise a trade.
    """
    ts = to_utc(ts)
    wd, hour = ts.weekday(), ts.hour
    if wd == 5:
        return True
    if wd == 4 and hour >= 21:
        return True
    if wd == 6 and hour < 21:
        return True
    return False
