"""Today's news window on the record (17 Oct), for the standing rules' count.

Since 10 Oct Trend & Breakout takes no new trade, practice or real, in the
``news.blackout_before_seconds`` (15 minutes on the Mac) before a release of
importance ``news.min_importance_for_blackout`` or more on either currency of
the pair (``news.engine.NewsIntelligence.verdict``). The standing rules count
only the trades today's rules still allow, so a trade on record entered inside
that window does not count either. This reads the Mac's own calendar archive
(``news.store_path``, ``data/calendar.sqlite``, found as the trader finds
it; every event the bot has seen, written by ``news.calendar_store``) read only, and says for one trade whether it was:

* ``True``: entered inside today's window before such a release;
* ``False``: not, on a day the archive covers;
* ``None``: not known - the archive does not cover that day, so the trade
  counts as before.

What it cannot see: the broker's own currencies for an index or a metal (the
scanner asks the broker; here a fixed table, else USD as the scanner's own
fallback), and the short wait after a release, which turns on the spread at
that moment. Never raises: no archive, an unreadable one or news switched off
gives no window at all, and every trade counts.
"""
from __future__ import annotations

import bisect
import datetime as dt
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Iterable, Optional

from ..clock import to_utc
from ..contracts import classify_fx
from ..news.calendar_store import NOT_MERGED          # the trader's own rule for rows merged before 17 Oct

CALENDAR = "calendar.sqlite"
# The 15-minute window's first day (ledger, 10 Oct News row; coach.epochs "news block"). Every trade from then on
# was taken with the window in force, so the scanner already let it through: only trades before it are checked.
# If the window is ever widened, record the day in coach.epochs and here.
WINDOW_SINCE = dt.datetime(2026, 10, 10, tzinfo=dt.timezone.utc)

# The currencies the broker's spec gives these (base and profit currency), as the scanner reads them for news.
OTHER_CURRENCIES = {
    "US30": ("USD",), "US500": ("USD",), "USTEC": ("USD",), "NAS100": ("USD",), "US100": ("USD",),
    "DE40": ("EUR",), "GER40": ("EUR",), "STOXX50": ("EUR",), "EU50": ("EUR",), "F40": ("EUR",), "ES35": ("EUR",),
    "UK100": ("GBP",), "JP225": ("JPY",), "AUS200": ("AUD",), "HK50": ("HKD",),
}


def currencies_of(symbol: str) -> tuple[str, ...]:
    """Whose releases close the window for ``symbol``: an FX pair's two currencies, a metal's (XAUUSD: XAU and
    USD), an index's own, else USD (news.engine.NewsIntelligence.currencies_for)."""
    base, quote, _ = classify_fx(symbol)
    if base and quote:
        return (base, quote)
    core = "".join(ch for ch in str(symbol or "").upper() if ch.isalnum())
    if core[:3] in ("XAU", "XAG", "XPT", "XPD") and len(core) >= 6:
        return (core[:3], core[3:6])
    for name, ccys in OTHER_CURRENCIES.items():
        if core.startswith(name):
            return ccys
    return ("USD",)


class NewsWindow:
    """The archive's releases that close the window, by currency, and the UTC days it covers."""

    def __init__(self, window_s: float, events: Iterable[tuple[str, dt.datetime, str]], days: Iterable[dt.date]):
        self.window_s = float(window_s)
        self.days = frozenset(days)
        by: dict[str, list[tuple[dt.datetime, str]]] = {}
        for ccy, when, name in events:
            by.setdefault(str(ccy).upper(), []).append((to_utc(when), str(name or "")))
        self.by = {c: sorted(v) for c, v in by.items()}
        self.times = {c: [t for t, _ in v] for c, v in self.by.items()}

    def release(self, symbol: str, opened_utc: dt.datetime) -> Optional[str]:
        """The first release inside the window after an entry at ``opened_utc`` ("CAD Employment Change at
        12:30 UTC"), else None."""
        start = to_utc(opened_utc)
        end = start + dt.timedelta(seconds=self.window_s)
        best = None
        for ccy in currencies_of(symbol):
            times = self.times.get(ccy)
            if not times:
                continue
            i = bisect.bisect_left(times, start)
            if i < len(times) and times[i] <= end:
                when, name = self.by[ccy][i]
                if best is None or when < best[0]:
                    best = (when, f"{ccy} {name} at {when:%H:%M} UTC".replace("  ", " "))
        return best[1] if best else None

    def inside(self, symbol: str, opened_utc: dt.datetime) -> Optional[bool]:
        """True: entered inside today's window (a release on record closes it); False: not, on days the
        archive covers; None: no release on record and a day of the window not covered - not known."""
        start = to_utc(opened_utc)
        if self.release(symbol, start) is not None:
            return True
        end = start + dt.timedelta(seconds=self.window_s)
        if start.date() not in self.days or end.date() not in self.days:
            return None
        return False


def _read(path: Path, lo: str, hi: str, importance: int) -> Optional[tuple[list, list]]:
    """The releases from ``lo`` to ``hi`` of at least ``importance``, and the covered days. Read only, and
    nothing is made beside the archive (nothing outside data/coach/ is ever written): while the trader holds it
    open in WAL mode its -wal and -shm files are there and are read with it; otherwise the file is whole by
    itself and is read as ``immutable``. None when it cannot be read."""
    side = [Path(f"{path}-wal"), Path(f"{path}-shm")]
    at = Path(path).resolve().as_uri()                         # escaped: a '#', '?' or '%' in the folder is kept
    uris = ([f"{at}?mode=ro"] if all(p.exists() for p in side) else []) + [f"{at}?immutable=1"]
    for uri in uris:
        try:
            with closing(sqlite3.connect(uri, uri=True, timeout=5.0)) as c:
                rows = c.execute("SELECT currency, time_utc, name FROM events WHERE time_utc >= ? AND time_utc <= ? "
                                 "AND importance >= ? AND " + NOT_MERGED, (lo, hi, int(importance))).fetchall()
                days = c.execute("SELECT day FROM coverage").fetchall()
                return rows, days
        except Exception:
            continue
    return None


def load(data_dir, cfg, start: dt.datetime, end: dt.datetime) -> Optional[NewsWindow]:
    """Today's window from ``cfg.news`` over the Mac's archive, for entries from ``start`` to ``end``. None when
    news is off, the window is zero, or there is no readable archive."""
    try:
        news = getattr(cfg, "news", None)
        if news is None or not bool(getattr(news, "enabled", True)) or data_dir is None:
            return None
        window_s = float(getattr(news, "blackout_before_seconds", 0.0) or 0.0)
        if window_s <= 0:
            return None
        named = Path(str(getattr(news, "store_path", "") or CALENDAR))   # where the trader keeps it
        path = named if named.is_absolute() else Path(data_dir) / named.name
        if not path.is_file():
            return None
        lo = to_utc(start) - dt.timedelta(days=1)
        hi = to_utc(end) + dt.timedelta(days=1, seconds=window_s)
        got = _read(path, lo.isoformat(), hi.isoformat(), int(getattr(news, "min_importance_for_blackout", 3)))
        if got is None:
            return None
        rows, days = got
        events = []
        for ccy, when, name in rows:
            try:
                at = dt.datetime.fromisoformat(str(when))
            except Exception:
                continue
            events.append((ccy, at if at.tzinfo else at.replace(tzinfo=dt.timezone.utc), name))
        covered = []
        for (day,) in days:
            try:
                covered.append(dt.date.fromisoformat(str(day)))
            except Exception:
                continue
        return NewsWindow(window_s, events, covered)
    except Exception:
        return None
