"""The strategy lab: a machine that tries trading ideas on old prices, then
once on prices it has never seen, then on live prices with no money, and
tells Aaron plainly which ones earned a place.

This file is the frozen interface that the rest of the lab is built on. Other
builders read it and never edit it. It holds only things that must not move:

* where every file lives (``LabPaths``) and the one settings file
  (``LabSettings``, ``data/lab.json``);
* the stage names, the five check names and their state words, the chip
  codes, the summary schema version;
* every threshold of the luck rules, written ONCE here so that the nightly,
  the stages and the page can never disagree about them;
* the small data classes that travel between the simulator, the statistics
  and the ledger (``Candidate``, ``Signal``, ``Trade``, ``Result``).

Aaron, 10 Oct 2026: "ensure that consistency stays, luck doesn't".
Also his rules, which the lab keeps: never invent performance; legal market
behaviour only; the daily-loss stop is never touched; secrets stay in the one
secrets file, which the lab never opens.

THE LAB CANNOT PLACE AN ORDER. Nothing under ``mintel/lab/`` imports an order
type or a broker that can trade, and nothing here can reach one. Its prices
come from a read-only feed. Lab "trades" are rows in ``data/lab/lab.sqlite``
with ``mode='PRACTICE'`` and nowhere else.

Runtime is the standard library only (no numpy or pandas on the Mac).
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from dataclasses import asdict, dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

from ..ops.standing import uk_midnight

UTC = dt.timezone.utc

# ---------------------------------------------------------------- versions ---
SUMMARY_SCHEMA = 1                  # data/lab/summary.json "schema"
LEDGER_SCHEMA = 1                   # PRAGMA user_version of lab.sqlite
STORE_FORMAT = 1                    # the column files and index.json

# ------------------------------------------------------------------ stages ---
# Exact words (Aaron reads these). Everything up to READY FOR LIVE and every
# BIN is the rule's; anything to do with real money or sizes is Aaron's.
IDEA = "IDEA"
PAST_TEST = "PAST TEST"
FRESH_TEST = "FRESH TEST"
ON_THE_BENCH = "ON THE BENCH"
PRACTICE = "PRACTICE"
READY_FOR_LIVE = "READY FOR LIVE"
LIVE_TRIAL = "LIVE TRIAL"
PROVEN = "PROVEN"
BINNED = "BINNED"
STAGES = (IDEA, PAST_TEST, FRESH_TEST, ON_THE_BENCH, PRACTICE, READY_FOR_LIVE,
          LIVE_TRIAL, PROVEN, BINNED)

# ------------------------------------------------------------------ checks ---
# Five checks on every row, bots included, in this order and wording.
CHECK_TRADES, CHECK_STEADY, CHECK_FRESH, CHECK_COSTS, CHECK_LUCK = (
    "trades", "steady", "fresh", "costs", "luck")
CHECK_KEYS = (CHECK_TRADES, CHECK_STEADY, CHECK_FRESH, CHECK_COSTS, CHECK_LUCK)
CHECK_LABELS = {CHECK_TRADES: "Enough trades", CHECK_STEADY: "Steady",
                CHECK_FRESH: "Fresh prices", CHECK_COSTS: "Costs",
                CHECK_LUCK: "Luck check"}
# State words. "Running" carries "n of m" in the check's n / of fields.
PASSED, FAILED, RUNNING, NOT_YET, NOT_TESTED = (
    "Passed", "Failed", "Running", "Not yet", "Not tested this way")
CHECK_STATES = (PASSED, FAILED, RUNNING, NOT_YET, NOT_TESTED)

# ------------------------------------------------------------------- chips ---
CHIP_TESTING, CHIP_PASSED, CHIP_YOUR_CALL, CHIP_FAILED = (
    "TESTING", "PASSED", "YOUR CALL", "FAILED")
CHIP_COLOURS = {CHIP_TESTING: "grey", CHIP_PASSED: "sky",
                CHIP_YOUR_CALL: "sand", CHIP_FAILED: "amber"}

# --------------------------------------------------------------- test kinds ---
KIND_PAST, KIND_FRESH = "past", "fresh"
# why a fresh look ended the way it did (the reasons_json codes)
FRESH_LOST, FRESH_TOO_FEW = "LOST", "TOO_FEW_TRADES"
# why a past pass is held back from the fresh look (reasons_json codes of a PAST row that
# passed its own checks): the settings sit at the edge of the range tried, the family looks
# to be fitting noise, or a neighbouring setting has not been tested yet
REASON_EXTEND_GRID, REASON_FITS_NOISE, REASON_PLATEAU_PENDING = (
    "EXTEND_GRID", "FAMILY_FITS_NOISE", "PLATEAU_PENDING")
HOLD_BACK_REASONS = (REASON_EXTEND_GRID, REASON_FITS_NOISE, REASON_PLATEAU_PENDING)

# --------------------------------------------------------------- the data ---
KINDS = ("FX_MAJOR", "FX_MINOR", "INDEX", "GOLD")      # the lab's kinds of market
NEEDED_DAYS = 40                    # complete trading days before the lab starts
TRADING_DAY_SHARE = 0.80            # share of symbols that must be complete on a day
COMPLETE_SHARE = 0.85               # a day is complete at >= 85% of the median day
WARMUP_BARS = 400                   # M1 bars before a window's first day
HISTORY_DAYS = 130                  # fetch until this many complete days are held
FRESH_DAYS_SHARE, FRESH_DAYS_MIN, FRESH_DAYS_MAX = 0.25, 20, 45
SHORT_HISTORY_DAYS = 90             # under this the fresh stretch is short
UNIVERSE_CAP = 24
SPREAD_MAX = {"FX_MINOR": 12.0, "INDEX": 4.0}   # minors in points, indices in index points

# ------------------------------------------------------- the luck rules ---
# Written ONCE. Aaron: "ensure that consistency stays, luck doesn't".
FRESH_LOOKS_PER_STRETCH = 30        # the most fresh looks in one stretch
FRESH_FAMILYWISE = 0.15             # chance we accept a chance result in a stretch
FRESH_P = FRESH_FAMILYWISE / FRESH_LOOKS_PER_STRETCH     # one fresh look must beat this
FRESH_PER_NIGHT = 3
LUCK_M_PAST_RANK = 1                # past p is shown and used for ranking only
PAST_P_SHOWN = 0.05                 # past luck check "passed" at or under this (not a gate)
PRACTICE_P = 0.05                   # a practice look must beat this ...
PRACTICE_LOOK_M = 2                 # ... with this many looks counted
BOOTSTRAP_BLOCK = 5                 # days in a block
BOOTSTRAP_RESAMPLES = 2000

PAST_MIN_TRADES, PAST_MIN_DAYS = 60, 20       # trades, and days in the window
PAST_PF = 1.2
PAST_CHUNKS, PAST_CHUNKS_UP, PAST_CHUNK_WORST = 4, 3, -0.5   # worst chunk >= -0.5 x total
PAST_BEST_DAY_SHARE = 0.40
PAST_ASSUMED_SPREAD_MAX = 0.10      # more than this share of assumed spreads cannot pass
FRESH_MIN_TRADES, FRESH_PF = 30, 1.1
PRACTICE_MIN_TRADES, PRACTICE_MIN_DAYS, PRACTICE_PF = 30, 10, 1.15
COST_STRESS_PASS = 1.5              # net must stay above zero at 1.5 x the costs
COST_STRESS_SHOWN = 2.0
PLATEAU_TOLERANCE = 0.5
PBO_GROUPS, PBO_TEST_GROUPS = 6, 2
PBO_MAX = 0.35
PBO_MIN_MEMBERS, PBO_MIN_TRADES = 8, 20
PRACTICE_KILL_R = 15.0              # practice drawdown (in R) that bins a candidate
LIVE_TRIAL_TRADES = 20



def clamp_fresh_cap(n) -> int:
    """A fresh-look cap as the lab will honour it: never below 0 and never above
    ``FRESH_LOOKS_PER_STRETCH``. The fresh line FRESH_P is 0.15 spread over exactly
    that many looks, so a hand-edited setting or cut.json cannot buy more looks."""
    try:
        n = int(n)
    except (TypeError, ValueError):
        return FRESH_LOOKS_PER_STRETCH
    return max(0, min(n, FRESH_LOOKS_PER_STRETCH))


# --------------------------------------------------------------- the model ---
RISK_GBP = 13.0                     # Formula 1's figure: practice reads like live
EXITS = ("t1", "t2", "t15w", "tr1", "tr2", "time4h", "vwap", "time1h")
SESSIONS_USED = ("LONDON", "NEWYORK", "US_CASH", "ASIA", "BOTH", "ALL")
NEWS_MODES = ("IGNORE", "AVOID_15M")
NEWS_AVOID_MINUTES = 15
EXIT_REASONS = ("STOP", "TARGET", "TRAIL", "TIME", "SESSION_END", "FEED_GAP")

# the simulator's caps and costs (spec section 3)
MAX_OPEN_PER_CANDIDATE = 3
COOLDOWN_BARS = 5
MAX_ENTRIES_PER_DAY = 6
SLIP_MIN_POINTS, SLIP_SPREAD_SHARE = 1.0, 0.25
SPREAD_SKIP_MULT = 3.0              # skip when spread > 3 x the symbol-day median
COST_SKIP_SHARE = 0.20              # skip when spread + commission > 20% of the stop
MIN_LOT_RISK_MULT = 1.5
TARGET_REACH_POINTS = 1.0           # touching a target is not a fill
TRAIL_ACTIVATE_R = 1.0
FLAT_UK = (21, 55)                  # flat by this UK time ...
FLAT_UK_FRIDAY = (21, 0)            # ... and by this on a Friday
FEED_GAP_MINUTES = 5                # practice: a longer gap closes an open position
STRETCH_GAP_RETEST = "TOO_FEW_TRADES"
NEWS_MOVE_MIN_DAYS = 40             # covered trading days in the past stretch before NEWS_MOVE wakes


# ------------------------------------------------------------------- time ---
def epoch_min(ts: dt.datetime) -> int:
    """UTC epoch minutes of an aware datetime (floored to the minute)."""
    return int(ts.timestamp()) // 60


def minute_to_dt(m: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(m * 60, tz=UTC)


def iso_utc(m: int) -> str:
    """'2026-09-17T08:31:00+00:00' for epoch minutes ``m``."""
    return minute_to_dt(m).isoformat()


def day_min(day: str) -> int:
    """Epoch minutes of 00:00 UTC on an ISO date."""
    d = dt.date.fromisoformat(day)
    return int(dt.datetime(d.year, d.month, d.day, tzinfo=UTC).timestamp()) // 60


def min_to_day(m: int) -> str:
    """The UTC ISO date of epoch minutes ``m``."""
    return dt.date.fromordinal(719163 + m // 1440).isoformat()


@lru_cache(maxsize=4096)
def uk_day_min(day: str) -> int:
    """Epoch minutes of 00:00 UK on an ISO date (BST and GMT by the tz database)."""
    return int(uk_midnight(dt.date.fromisoformat(day)).timestamp()) // 60


def is_weekday(day: str) -> bool:
    return dt.date.fromisoformat(day).weekday() < 5


def add_days(day: str, n: int) -> str:
    return (dt.date.fromisoformat(day) + dt.timedelta(days=n)).isoformat()


def utc_stamp(now: Optional[dt.datetime] = None) -> str:
    now = now or dt.datetime.now(UTC)
    return now.astimezone(UTC).replace(microsecond=0).isoformat()


def atomic_write(path: Path, text: str) -> None:
    """Write ``text`` whole (tmp + replace): a reader never sees half a file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(text)
        fh.flush()
        try:
            os.fsync(fh.fileno())
        except OSError:
            pass
    os.replace(tmp, path)


def atomic_write_json(path: Path, obj: Any) -> None:
    atomic_write(path, json.dumps(obj, indent=1, sort_keys=True, default=str))


# ------------------------------------------------------------------ paths ---
class LabPaths:
    """Every file the lab owns, from the one data directory.

    ``LabPaths(cfg.ops.data_dir)``; pass ``log_dir`` to get the two log files.
    Nothing here creates a file; callers make their own directories.
    """

    def __init__(self, data_dir, log_dir=None):
        d = Path(data_dir)
        self.data_dir = d
        self.root = d / "lab"
        self.bars = self.root / "bars"
        self.cache = self.root / "cache"
        self.feature_cache = self.cache / "features"
        self.universe = self.root / "universe.json"
        self.cut = self.root / "cut.json"
        self.sqlite = self.root / "lab.sqlite"
        self.summary = self.root / "summary.json"
        self.runs = self.root / "runs"
        self.lock = self.root / "nightly.lock"
        self.stop = self.root / "STOP"
        self.mode_file = d / "lab.json"
        self.status = d / "lab-status.json"
        self.pid = d / "lab.pid"
        self.trials = d / "research_trials.sqlite"
        self.calendar = d / "calendar.sqlite"
        self.heartbeats = d / "heartbeats"
        ld = Path(log_dir) if log_dir else None
        self.log_dir = ld
        self.practice_log = (ld / "lab.out.log") if ld else None
        self.nightly_log = (ld / "lab-nightly.log") if ld else None

    def run_dir(self, date: str) -> Path:
        return self.runs / date

    def symbol_dir(self, symbol: str) -> Path:
        return self.bars / symbol


# --------------------------------------------------------------- settings ---
@dataclass(frozen=True)
class LabSettings:
    """``data/lab.json``. Every key optional; a bad value falls back to its
    default. A mode of LIVE is read as PAPER: the lab has no live path."""
    mode: str = "PAPER"                       # PAPER | OFF
    risk_gbp: float = RISK_GBP
    practice_slots: int = 6
    fresh_cap_per_stretch: int = FRESH_LOOKS_PER_STRETCH
    fresh_per_night: int = FRESH_PER_NIGHT
    history_days: int = HISTORY_DAYS
    max_mb: int = 300
    deadline_local: str = "06:00"
    symbols: Optional[tuple] = None           # None = the universe file

    @classmethod
    def load(cls, data_dir) -> "LabSettings":
        try:
            raw = json.loads(Path(data_dir, "lab.json").read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raw = {}
        except (OSError, ValueError):
            raw = {}
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: dict) -> "LabSettings":
        d = cls()

        def num(key, kind, default, lo, hi):
            try:
                v = kind(raw[key])
            except (KeyError, TypeError, ValueError):
                return default
            return v if lo <= v <= hi else default

        mode = str(raw.get("mode", d.mode)).strip().upper()
        if mode == "LIVE":
            mode = "PAPER"
        elif mode not in ("PAPER", "OFF"):
            mode = "OFF"        # an unknown word does nothing, as for the other bots
        dl = str(raw.get("deadline_local", d.deadline_local))
        try:
            hh, mm = dl.split(":")
            assert 0 <= int(hh) <= 23 and 0 <= int(mm) <= 59
        except Exception:
            dl = d.deadline_local
        syms = raw.get("symbols")
        if isinstance(syms, (list, tuple)) and syms:
            syms = tuple(str(s) for s in syms)
        else:
            syms = None
        # The fresh line FRESH_P is 0.15 spread over 30 looks. A cap above 30 would break that
        # arithmetic (500 looks at 0.005 each is about 2.5 lucky passes a stretch), so the
        # setting can lower the cap but never raise it: anything above 30 reads as 30.
        return cls(mode=mode, risk_gbp=num("risk_gbp", float, d.risk_gbp, 0.5, 1000.0),
                   practice_slots=num("practice_slots", int, d.practice_slots, 0, 50),
                   fresh_cap_per_stretch=num("fresh_cap_per_stretch", int,
                                             d.fresh_cap_per_stretch, 0, FRESH_LOOKS_PER_STRETCH),
                   fresh_per_night=num("fresh_per_night", int, d.fresh_per_night, 0, 100),
                   history_days=num("history_days", int, d.history_days, 5, 1000),
                   max_mb=num("max_mb", int, d.max_mb, 10, 100000),
                   deadline_local=dl, symbols=syms)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["symbols"] = list(self.symbols) if self.symbols else None
        return d

    def save(self, data_dir) -> None:
        """Atomic (tmp + replace). The only caller is ``python -m mintel.lab mode``."""
        atomic_write_json(Path(data_dir, "lab.json"), self.to_dict())


# -------------------------------------------------------- the data classes ---
@dataclass(frozen=True)
class Candidate:
    """A frozen trading idea: a family, a version and its settings.

    Never fitted. The id is the hash of the definition ONLY (never data, dates
    or results), so the same idea has the same id on every machine and every
    night, and a BINNED id can be recognised for ever.
    """
    family: str
    version: int
    markets: str                      # FX_MAJOR | FX_MINOR | INDEX | GOLD
    session: str                      # LONDON | NEWYORK | US_CASH | ASIA | BOTH | ALL
    news: str                         # IGNORE | AVOID_15M
    exit: str                         # one of EXITS
    params: tuple = ()                # sorted ((name, value), ...)
    risk_gbp: float = RISK_GBP

    @property
    def id(self) -> str:
        d = self.__dict__
        got = d.get("_id")
        if got is None:
            got = d["_id"] = candidate_id(self)
        return got

    def p(self, name: str, default: Any = None) -> Any:
        for k, v in self.params:
            if k == name:
                return v
        return default

    def pdict(self) -> dict:
        return dict(self.params)

    def definition(self) -> dict:
        d = asdict(self)
        d["params"] = [[k, v] for k, v in self.params]
        return d

    def replace(self, **changes) -> "Candidate":
        """A copy with some top-level fields changed; ``params={...}`` merges."""
        d = {f: getattr(self, f) for f in ("family", "version", "markets", "session",
                                           "news", "exit", "params", "risk_gbp")}
        extra = changes.pop("params", None)
        d.update(changes)
        if extra is not None:
            merged = dict(d["params"])
            merged.update(dict(extra))
            d["params"] = tuple(sorted(merged.items()))
        return Candidate(**d)


def candidate_id(c: Candidate) -> str:
    """'lab-' + the first 10 hex digits of the sha1 of the sorted definition.
    Computed from the definition's own text (so 1 and 1.0 are different ideas
    here, as they are on disk), never from data, dates or results."""
    blob = json.dumps(asdict(c), sort_keys=True, separators=(",", ":"))
    return "lab-" + hashlib.sha1(blob.encode("utf-8")).hexdigest()[:10]


def make_params(**kw) -> tuple:
    """Sorted parameter tuple for a Candidate."""
    return tuple(sorted(kw.items()))


@dataclass(frozen=True, slots=True)
class Signal:
    """A family's decision, known at the CLOSE of M1 bar ``i`` of a series.

    ``side`` is +1 (buy) or -1 (sell). ``stop_points`` (optional) is the
    distance in points from that close to the stop, overriding the exit's own
    ATR stop (the other side of a range, say). ``ref`` says in a word what
    triggered it.
    """
    i: int
    side: int
    stop_points: Optional[int] = None
    ref: str = ""


@dataclass(slots=True)
class Trade:
    symbol: str
    side: int
    volume: float
    entry: float
    stop: float
    target: Optional[float]
    opened_utc: str
    closed_utc: str
    exit_price: float
    exit_reason: str                  # one of EXIT_REASONS
    gross_gbp: float                  # from the fills (spread and slip are inside it)
    cost_gbp: float                   # spread + slippage + commission (re-priced by cost_stress)
    net_gbp: float                    # gross less commission
    r: float                          # net / risk_gbp
    bars_held: int
    spread_assumed: bool              # a bar had no recorded spread; the default was used
    fill_source: str = "bar"          # "bar" in tests, "tick" in practice
    day: str = ""                     # the UK day it closed on (ISO date)


@dataclass(frozen=True)
class WindowInfo:
    """Which prices a result was built from."""
    kind: str                         # past | fresh | practice
    day_from: str = ""
    day_to: str = ""
    days: int = 0
    cut_day: str = ""
    stretch_id: str = ""
    symbols: int = 0
    note: str = ""


@dataclass
class Result:
    candidate_id: str
    window: WindowInfo
    trades: list = field(default_factory=list)
    daily: dict = field(default_factory=dict)       # UK day -> (net_gbp, trades), every window day
    skipped: dict = field(default_factory=dict)     # reason -> count
    synthetic: bool = False
    seconds: float = 0.0

    @property
    def net_gbp(self) -> float:
        return sum(t.net_gbp for t in self.trades)
