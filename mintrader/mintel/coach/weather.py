"""The market board (spec section 8): what each kind of market is doing now,
the session phase in each market's own time zone, and where each live key may
trade with real money now.

Source: ``data/coach/weather.json``, written atomically every 60 s by the
trader from its latest scan states (one entry per scanned symbol:
``{symbol, kind, regime, confidence, vol_state, spread_points, news_state,
tradable, as_of_utc}``), plus the broker's demo-account check (section 21).

Smoothing is for display only: a label must hold for 2 consecutive minutes
before the board shows it (NEWS shows at once). Gating always uses the raw
label at entry, because the evidence was recorded with it.

Session phases are computed in each market's own time zone through
``mintel.clock``, so the UK and US clock changes (25 Oct and 1 Nov) need no
special handling:

    ASIA       Tokyo 09:00-15:00
    LONDON_AM  London 08:00-12:00
    OVERLAP    New York 08:00 to London 16:30
    NY_PM      until New York 16:45
    ROLLOVER   New York 16:45-17:15
    LATE       after the rollover (and any hour no window above names)

Imports: the pure registry ``engine.tactics.TACTICS`` only (section 3).
"""
from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, Sequence

from ..clock import TZ_LONDON, TZ_NEWYORK, TZ_TOKYO
from . import (BOARD_HOLD_MINUTES, DEMO_WEATHER_MAX_AGE_S, FRIDAY_CUTOFF_NY, ROLLOVER_NY, SESSION_PHASES, WILD,
               Paths, as_utc, parse_utc)

PHASE_WORDS = {"ASIA": "Asia session", "LONDON_AM": "London morning", "OVERLAP": "London and New York both open",
               "NY_PM": "New York afternoon", "ROLLOVER": "the New York rollover", "LATE": "late, quiet hours"}
REGIME_WORDS = {"TREND": "trending", "RANGE": "ranging", "SQUEEZE": "in a squeeze", "HIGH_VOL": "in a fast market",
                "LOW_VOL": "in a slow market", "NEWS": "moving on news", "UNCLEAR": "unclear"}
KIND_PLURAL = {"FX_MINOR": "minor pairs", "FX_MAJOR": "major pairs", "FX_EXOTIC": "exotic pairs",
               "INDEX": "indices", "GOLD": "gold markets", "SILVER": "silver markets", "METAL": "metal markets",
               "ENERGY": "energy markets", "CRYPTO": "crypto markets", "EQUITY": "shares", "UNKNOWN": "other markets"}
MAX_BYTES = 2_000_000


def _hm(text: str) -> int:
    return int(text[:2]) * 60 + int(text[3:5])


def _minutes(t: dt.datetime) -> int:
    return t.hour * 60 + t.minute


# ------------------------------------------------------------ the phases --
def session_phase(now: dt.datetime) -> str:
    """The session phase now (section 8), each window in its market's own clock."""
    now = as_utc(now)
    ny, ldn, tky = now.astimezone(TZ_NEWYORK), now.astimezone(TZ_LONDON), now.astimezone(TZ_TOKYO)
    n, l, t = _minutes(ny), _minutes(ldn), _minutes(tky)
    london_close = 16 * 60 + 30
    if _hm(ROLLOVER_NY[0]) <= n < _hm(ROLLOVER_NY[1]):
        return "ROLLOVER"
    # New York is behind London, so during the working day its clock reads less than London's
    if 8 * 60 <= n < l and l < london_close:
        return "OVERLAP"
    if 8 * 60 <= n < _hm(ROLLOVER_NY[0]) and l >= london_close:
        return "NY_PM"
    if 8 * 60 <= l < 12 * 60:
        return "LONDON_AM"
    if 9 * 60 <= t < 15 * 60:
        return "ASIA"
    return "LATE"


def market_hours_gate(now: dt.datetime, kind: str, rollover: Sequence[str] = ROLLOVER_NY,
                      friday_cutoff: str = FRIDAY_CUTOFF_NY) -> str:
    """The two market-hours gates for coach-granted keys ("" when neither applies; pinned and placed keys
    are never affected): no new granted entry in the New York rollover, and no new granted FX or index
    entry after 16:00 New York on a Friday. The gate in each bot applies the same two."""
    ny = as_utc(now).astimezone(TZ_NEWYORK)
    m = _minutes(ny)
    if _hm(rollover[0]) <= m < _hm(rollover[1]):
        return "no new granted entry in the New York rollover"
    if ny.weekday() == 4 and m >= _hm(friday_cutoff) and (str(kind).startswith("FX") or kind == "INDEX"):
        return "no new granted FX or index entry after 16:00 New York on a Friday"
    return ""


# ---------------------------------------------------------- weather.json --
@dataclass
class Weather:
    """What ``weather.json`` said, and whether it is fresh."""
    ok: bool
    why: str = ""
    as_of: Optional[dt.datetime] = None
    age_s: Optional[float] = None
    symbols: list = field(default_factory=list)
    raw: dict = field(default_factory=dict)

    @property
    def account_demo(self) -> bool:
        return bool(self.ok and self.raw.get("account_demo") is True)


def read(paths: Paths, now: dt.datetime, max_age_s: float = DEMO_WEATHER_MAX_AGE_S) -> Weather:
    """``data/coach/weather.json`` (read only). Never raises."""
    try:
        with open(paths.weather, "rb") as fh:
            raw = fh.read(MAX_BYTES + 1)
    except FileNotFoundError:
        return Weather(False, "the market board has not been written yet (weather.json)")
    except OSError as exc:
        return Weather(False, f"the market board could not be read ({exc})")
    if len(raw) > MAX_BYTES:
        return Weather(False, "the market board is too large")
    try:
        obj = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        return Weather(False, f"the market board is not JSON ({exc})")
    if not isinstance(obj, dict):
        return Weather(False, "the market board is not a table")
    when = parse_utc(obj.get("as_of_utc"))
    if when is None:
        return Weather(False, "the market board has no time", raw=obj)
    age = (as_utc(now) - when).total_seconds()
    rows = [r for r in (obj.get("symbols") or []) if isinstance(r, dict) and r.get("symbol")]
    if age > max_age_s:
        return Weather(False, f"the market board is {age:.0f} s old", when, age, rows, obj)
    return Weather(True, "", when, age, rows, obj)


# ---------------------------------------------------------- the board --
class BoardMemory:
    """Display smoothing: a symbol's label shows once it has held for 2 minutes (NEWS at once)."""

    def __init__(self):
        self._seen: dict[str, tuple[str, dt.datetime]] = {}      # symbol -> (raw label, since)
        self._shown: dict[str, str] = {}

    def update(self, rows: Iterable[dict], now: dt.datetime) -> dict[str, str]:
        now = as_utc(now)
        for r in rows:
            sym = str(r.get("symbol"))
            label = str(r.get("regime") or "")
            if not label:
                continue
            last = self._seen.get(sym)
            if last is None or last[0] != label:
                self._seen[sym] = (label, now)
                last = self._seen[sym]
            held = (now - last[1]).total_seconds() / 60.0
            if label == "NEWS" or held >= BOARD_HOLD_MINUTES:
                self._shown[sym] = label
        return dict(self._shown)


def board(weather: Weather, shown: Optional[dict] = None) -> list[dict]:
    """One line per kind of market: how many of its symbols are in which condition (the smoothed labels in
    ``shown`` when given, else the raw ones)."""
    if not weather.symbols:
        return []
    kinds: dict[str, dict[str, int]] = {}
    totals: dict[str, int] = {}
    for r in weather.symbols:
        kind = str(r.get("kind") or "UNKNOWN")
        label = (shown or {}).get(str(r.get("symbol"))) if shown is not None else str(r.get("regime") or "")
        totals[kind] = totals.get(kind, 0) + 1
        if label:
            kinds.setdefault(kind, {})
            kinds[kind][label] = kinds[kind].get(label, 0) + 1
    out = []
    for kind in sorted(totals):
        counts = kinds.get(kind, {})
        top = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
        bits = [f"{n} of {totals[kind]} {REGIME_WORDS.get(lab, lab.lower())}" for lab, n in top[:2]]
        words = f"{KIND_PLURAL.get(kind, kind.lower())}: " + ("; ".join(bits) if bits else "no labels yet")
        out.append({"kind": kind, "total": totals[kind], "counts": counts, "words": words})
    return out


def _approach_regimes() -> dict[str, tuple[str, ...]]:
    from ..engine.tactics import TACTICS                     # the pure registry (section 3)
    out = {t.name: tuple(r.value for r in t.regimes) for t in TACTICS}
    out.setdefault("BREAKOUT_RETEST", out.get("BREAKOUT_ACCEPTANCE", ()))
    return out


def real_now(key: str, weather: Weather, cfg: Any) -> list[str]:
    """For one live key, the symbols where it may trade with real money now: its kind, its condition (or
    any), an approach that trades that condition (TACTICS), and not switched off by Aaron's
    ``scan.disabled_tactic_regimes``, ``scan.disabled_tactic_groups`` or ``universe.excluded_symbols``
    (all read from config.json)."""
    parts = (str(key).split("|") + [WILD] * 4)[:4]
    _bot, approach, kind, cond = parts
    regimes = _approach_regimes().get(approach, ())
    scan = getattr(cfg, "scan", None)
    off_regimes = {(str(a).upper(), str(b).upper()) for a, b in (getattr(scan, "disabled_tactic_regimes", ()) or ())}
    off_groups = {(str(a).upper(), str(b).upper()) for a, b in (getattr(scan, "disabled_tactic_groups", ()) or ())}
    excluded = {str(s).upper() for s in (getattr(getattr(cfg, "universe", None), "excluded_symbols", ()) or ())}
    out = []
    for r in weather.symbols:
        sym = str(r.get("symbol") or "").upper()
        k = str(r.get("kind") or "")
        reg = str(r.get("regime") or "")
        if not sym or sym in excluded or (kind != WILD and k != kind):
            continue
        if cond != WILD and reg != cond:
            continue
        if approach != WILD and reg not in regimes:
            continue
        if (approach, reg) in off_regimes or (approach, k) in off_groups:
            continue
        if r.get("tradable") is False:
            continue
        out.append(sym)
    return sorted(out)


def words(weather: Weather, now: dt.datetime, shown: Optional[dict] = None) -> str:
    """One sentence for the page: the phase and the board."""
    phase = session_phase(now)
    if not weather.ok:
        return f"Markets now: {PHASE_WORDS[phase]}; the market board cannot be read just now ({weather.why})."
    lines = [b["words"] for b in board(weather, shown)]
    return f"Markets now ({PHASE_WORDS[phase]}): " + ("; ".join(lines) if lines else "nothing scanned yet") + "."


assert set(PHASE_WORDS) == set(SESSION_PHASES)
