"""The coach's evidence store: every closed trade it may learn from, read-only.

Spec section 4. Sources (each opened ``file:...?mode=ro`` with a 2-second busy
timeout; nothing is ever written to any of them):

    data/journal.sqlite        Trend & Breakout ("tnb"). Real or practice by
                               membership of its paper book
                               (``PaperBroker(data_dir, read_only=True).is_paper``),
                               never by ticket number: IC Markets' real tickets
                               (about 1.94e9-2.0e9) sit above the paper start.
                               Rows tagged ``runner_feed`` are left out.
    data/runner.sqlite         Momentum Runner, ``mode`` LIVE = real.
    data/bandbreaker.sqlite    Band Breaker, ``mode`` column.
    data/crowd.sqlite          Crowd Fader, ``mode`` column.
    data/ian.sqlite            Financial Ian, ``mode`` column; rows from a
                               synthetic or replayed feed are tests, never results.
    data/lab/lab.sqlite        the lab's practice trades, if the file is there.
    data/coach/gate/*.jsonl    the gate's decision per entry: joins a
                               coach-granted ticket to its key and route.

The unit of every outcome is R after costs: ``x = net after commission /
planned risk``, clipped to [-3, +5]. ``realised_r`` in the journal is price-R
before commission and is never used. Each column read is checked with
``PRAGMA table_info``: a missing column means the fallback the spec names plus
a flag on the row, never a guess.

Every trade carries its key ``bot|APPROACH|KIND|CONDITION`` (the condition is
the raw label at entry) and its cluster: trades of the same bot and
direction, sharing a leg, entered within 60 minutes of the cluster's first
entry, are one bet.

This module never opens the secrets file: config.json is read with ``json``
alone (``baseline_config``), not through ``Config.load``, which reads the
secrets file beside it.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import math
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field, replace
from functools import cached_property
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from ..contracts import classify_fx, infer_group
from . import (BOT_FALLBACK_RISK_GBP, CLUSTER_MINUTES, DATA_MAX_EXCLUDED_SHARE, DECAY_HALF_LIFE_DAYS,
               FUTURE_SKEW_SECONDS, HAIRCUT_DEFAULT_R, HAIRCUT_MAX_R, HAIRCUT_MIN_EACH, HAIRCUT_MIN_R,
               HAIRCUT_MOVE_SE, INDEX_LEGS as _SPEC_INDEX_LEGS, PRACTICE_WEIGHT, R_CLIP_LO, R_CLIP_HI,
               RECONCILE_MIN_GBP, RECONCILE_SHARE, RUNNER_FALLBACK_RISK_GBP, SQLITE_BUSY_TIMEOUT_S, TZ_UK, UTC,
               Paths)

log = logging.getLogger("mintel.coach.records")

# --- sources -------------------------------------------------------------
JOURNAL, RUNNER, BANDBREAKER, CROWD, IAN, LAB = "journal", "runner", "bandbreaker", "crowd", "ian", "lab"
SOURCES = (JOURNAL, RUNNER, BANDBREAKER, CROWD, IAN, LAB)
SOURCE_FILES = {JOURNAL: "journal.sqlite", RUNNER: "runner.sqlite", BANDBREAKER: "bandbreaker.sqlite",
                CROWD: "crowd.sqlite", IAN: "ian.sqlite", LAB: "lab/lab.sqlite"}
SOURCE_BOT = {JOURNAL: "tnb", RUNNER: "runner", BANDBREAKER: "bandbreaker", CROWD: "crowd", IAN: "ian", LAB: "lab"}
SOURCE_WORDS = {JOURNAL: "Trend & Breakout's journal", RUNNER: "the Momentum Runner's record",
                BANDBREAKER: "the Band Breaker's record", CROWD: "the Crowd Fader's record",
                IAN: "Financial Ian's record", LAB: "the lab's practice record"}
# A source whose file is not there yet has no trades to judge (a bot that has
# never run). Trend & Breakout's journal is the exception: it always exists.
REQUIRED_SOURCES = (JOURNAL,)

BUSY_TIMEOUT_S = SQLITE_BUSY_TIMEOUT_S
R_MIN, R_MAX = R_CLIP_LO, R_CLIP_HI   # x is clipped to this range
MAX_EXCLUDED_SHARE = DATA_MAX_EXCLUDED_SHARE   # under 5% of rows may be excluded
FUTURE_TOLERANCE_S = FUTURE_SKEW_SECONDS       # a closed time this far ahead of the clock is "in the future"
RECONCILE_FLOOR_GBP = RECONCILE_MIN_GBP        # broker check: within max(3, 10% of the page's figure)

# Status of a source
OK, STALE, UNREADABLE = "ok", "stale", "unreadable"

# Flags a trade may carry
F_COMMISSION_ESTIMATED = "commission_estimated"   # the opening commission was estimated (before 9 Oct)
F_RISK_ESTIMATED = "risk_estimated"               # no risk column: the bot's configured risk was used
F_COMMISSION_UNKNOWN = "commission_unknown"       # the record holds no commission figure (counted 0)
F_ESTIMATED_EXIT = "estimated_exit"               # a LIVE row booked from the last price ("?" exit)
F_COACH_REAL = "coach_real"                       # the gate routed this ticket REAL on a coach grant

# Index names contracts.infer_group does not know (its INDEX_HINTS lack them);
# the trader classifies them from the broker's spec, which the coach has not got.
KNOWN_INDEX_NAMES = ("USTEC", "NAS100", "NDX", "SPX500", "US2000", "FRA40", "ESP35", "STOXX50", "CHINA50", "HK50")

# Legs (section 4.3): which indices move as one market (the spec's table, plus
# other broker names for the same markets)
INDEX_LEGS = {**_SPEC_INDEX_LEGS, "NAS100": "USEQ", "US100": "USEQ", "SPX500": "USEQ", "GER40": "EUEQ"}


# ------------------------------------------------------------------ time --
def to_utc(t: dt.datetime) -> dt.datetime:
    return t.astimezone(UTC) if t.tzinfo is not None else t.replace(tzinfo=UTC)


def parse_utc(v: Any) -> Optional[dt.datetime]:
    """An ISO time (or datetime) as UTC; None when it is not one."""
    if v is None or v == "":
        return None
    if isinstance(v, dt.datetime):
        return to_utc(v)
    try:
        return to_utc(dt.datetime.fromisoformat(str(v).strip().replace("Z", "+00:00")))
    except (TypeError, ValueError):
        return None


def uk_date(t: dt.datetime) -> dt.date:
    """The UK calendar date (Europe/London, BST and GMT) of an instant."""
    return to_utc(t).astimezone(TZ_UK).date()


def is_trading_day(d: dt.date) -> bool:
    """UK trading days are the weekdays."""
    return d.weekday() < 5


def trading_days_between(a: dt.date, b: dt.date) -> int:
    """UK trading days after ``a`` up to and including ``b`` (0 when b <= a):
    the age, in trading days, of a trade that closed on ``a`` seen on ``b``."""
    if b <= a:
        return 0
    days = (b - a).days
    weeks, rem = divmod(days, 7)
    n = weeks * 5
    wd = a.weekday()
    for i in range(1, rem + 1):
        if (wd + i) % 7 < 5:
            n += 1
    return n


def trading_days_back(end: dt.date, n: int) -> dt.date:
    """The first of the last ``n`` UK trading days up to ``end`` (``end``
    counted when it is a weekday)."""
    d, left = end, max(1, int(n))
    while True:
        if is_trading_day(d):
            left -= 1
            if left == 0:
                return d
        d -= dt.timedelta(days=1)


# ------------------------------------------------------------- the trade --
@dataclass(frozen=True)
class Trade:
    """One closed trade, from any source, in the coach's unit."""
    source: str                       # journal | runner | bandbreaker | crowd | ian | lab
    bot: str                          # tnb | runner | bandbreaker | crowd | ian | lab
    ticket: int
    approach: str
    kind: str
    condition: str
    symbol: str
    side: str                         # BUY | SELL
    opened_utc: dt.datetime
    closed_utc: dt.datetime
    real: bool                        # real money (False: practice, bot or lab)
    net_gbp: float                    # after the whole commission
    commission_gbp: float             # the whole commission, a positive number (0 when unknown)
    risk_gbp: float                   # planned risk at entry
    score: Optional[float] = None     # the bot's own score at entry (journal "opportunity", Ian "score")
    rr: Optional[float] = None        # reward:risk at entry (journal "reward_risk")
    flags: tuple[str, ...] = ()
    cluster: str = ""
    policy_seq: Optional[int] = None  # from the gate log, when the coach routed it
    coach_route: str = ""

    @cached_property
    def key(self) -> str:
        return make_key(self.bot, self.approach, self.kind, self.condition)

    @cached_property
    def uk_day(self) -> dt.date:
        return uk_date(self.closed_utc)

    @property
    def x_raw(self) -> float:
        return self.net_gbp / self.risk_gbp

    @cached_property
    def x(self) -> float:
        """R after costs, clipped to [-3, +5]."""
        return clip_r(self.net_gbp / self.risk_gbp)

    @cached_property
    def x_cost15(self) -> float:
        """The outcome with commission x 1.5: ``x - 0.5 x commission / risk``."""
        return self.x - 0.5 * self.commission_gbp / self.risk_gbp

    @property
    def uid(self) -> tuple[str, int]:
        return (self.source, self.ticket)


def clip_r(v: float) -> float:
    return max(R_MIN, min(R_MAX, float(v)))


def make_key(bot: str, approach: str, kind: str, condition: str) -> str:
    return "|".join((str(bot or "").strip().lower(), _part(approach), _part(kind), _part(condition)))


def split_key(key: str) -> tuple[str, str, str, str]:
    parts = [p.strip() for p in str(key or "").split("|")]
    parts += ["*"] * (4 - len(parts))
    return parts[0].lower(), parts[1].upper(), parts[2].upper(), parts[3].upper()


def _part(v: Any) -> str:
    """One part of a key, as the policy's Key spells it: upper case, letters,
    digits and "_" ("lab-0123456789" is "LAB_0123456789"); "*" stays "*"."""
    s = str(v or "").strip().upper()
    if s == "*":
        return s
    s = "".join(ch if ch.isalnum() else "_" for ch in s)
    return s or "UNKNOWN"


def kind_of(symbol: str) -> str:
    """The kind of market: ``contracts.infer_group``, with the few index names
    it does not know (USTEC) read as INDEX, as the trader reads them from the
    broker's spec."""
    g = infer_group(str(symbol or ""))
    if g == "UNKNOWN":
        core = _core(symbol)
        if core in KNOWN_INDEX_NAMES or core in INDEX_LEGS:
            return "INDEX"
    return g


def _core(symbol: str) -> str:
    s = str(symbol or "").upper()
    for sep in (".", "_", "-", "#"):
        s = s.split(sep)[0]
    return s


def legs(symbol: str, side: str) -> tuple[str, ...]:
    """The signed legs of a position (section 4.3): a BUY of EURGBP is
    ("EUR+", "GBP-"); US500, US30 and USTEC are one leg (USEQ); DE40 EUEQ,
    UK100 UKEQ, JP225 JPEQ; other indices their own symbol; XAU* XAU, XAG*
    XAG; BTC and ETH separate legs."""
    up, dn = ("+", "-") if str(side or "").upper() in ("BUY", "LONG", "1") else ("-", "+")
    core = _core(symbol)
    if core.startswith("XAU") or core.startswith("GOLD"):
        return ("XAU" + up,)
    if core.startswith("XAG") or core.startswith("SILVER"):
        return ("XAG" + up,)
    for coin in ("BTC", "ETH"):
        if coin in core:
            return (coin + up,)
    base, quote, _ = classify_fx(core)
    if base and quote:
        return (base + up, quote + dn)
    if core in INDEX_LEGS:
        return (INDEX_LEGS[core] + up,)
    return (core + up,)


# ---------------------------------------------------------- the checks --
@dataclass
class SourceCheck:
    """What one source's data checks found (section 4.4)."""
    source: str
    path: str
    status: str = OK                  # ok | stale | unreadable
    present: bool = True
    rows: int = 0                     # closed rows read (before exclusions)
    used: int = 0                     # trades kept
    excluded: int = 0                 # risk <= 0 or missing, or a non-finite net
    duplicates: int = 0               # repeated tickets, counted once
    skipped: int = 0                  # left out by rule: runner-feed rows, test data
    newest_closed_utc: Optional[dt.datetime] = None
    flags: dict = field(default_factory=dict)       # flag -> rows carrying it
    failed: list = field(default_factory=list)      # plain-English reasons

    def fail(self, status: str, why: str) -> None:
        # unreadable outranks stale
        if self.status != UNREADABLE:
            self.status = status
        self.failed.append(why)

    @property
    def ok(self) -> bool:
        return self.status == OK

    def as_dict(self) -> dict:
        return {"source": self.source, "status": self.status, "present": self.present, "rows": self.rows,
                "used": self.used, "excluded": self.excluded, "duplicates": self.duplicates,
                "skipped": self.skipped,
                "newest_closed_utc": self.newest_closed_utc.isoformat() if self.newest_closed_utc else None,
                "flags": dict(self.flags), "failed": list(self.failed)}


@dataclass
class Evidence:
    """Everything read on one pass: the trades (clustered) and each source's checks."""
    trades: list
    checks: dict                      # source -> SourceCheck
    as_of: dt.datetime
    gate_log: dict = field(default_factory=dict)    # (bot, ticket) -> the gate's line

    @property
    def data_ok(self) -> bool:
        return all(c.ok for c in self.checks.values())

    @property
    def failed(self) -> list[str]:
        return [f"{SOURCE_WORDS.get(s, s)}: {why}" for s, c in self.checks.items() for why in c.failed]

    def unreadable_bots(self) -> list[str]:
        """Bots whose own record failed a check (a governed one is paused)."""
        return sorted({SOURCE_BOT[s] for s, c in self.checks.items() if not c.ok})

    def counts(self) -> dict:
        """Closed rows per source, for the next pass's "has not shrunk" check."""
        return {s: c.rows for s, c in self.checks.items() if c.present}

    def by_key(self) -> dict:
        out: dict[str, list] = {}
        for t in self.trades:
            out.setdefault(t.key, []).append(t)
        return out

    def real_today(self, now: Optional[dt.datetime] = None) -> float:
        return real_net_on(self.trades, uk_date(now or self.as_of))


# --------------------------------------------------------------- config --
def baseline_config(data_dir) -> Any:
    """Aaron's settings from ``<data_dir>/config.json``, read with ``json``
    alone (never ``Config.load``, which opens the secrets file). Sections the
    file lacks keep the code's defaults; an unreadable file gives defaults."""
    from ..config import (BandBreakerConfig, Config, CrowdConfig, FlowLockConfig, NewsConfig, OpsConfig,
                          RiskConfig, RunnerConfig, ScanConfig, TnbConfig, UniverseConfig, _build)
    cfg = Config()
    try:
        raw = json.loads((Path(data_dir) / "config.json").read_text())
    except Exception:
        raw = None
    if isinstance(raw, dict):
        nested = {"risk": RiskConfig, "universe": UniverseConfig, "scan": ScanConfig, "flowlock": FlowLockConfig,
                  "news": NewsConfig, "ops": OpsConfig, "runner": RunnerConfig, "bandbreaker": BandBreakerConfig,
                  "crowd": CrowdConfig, "tnb": TnbConfig}
        for name, kls in nested.items():
            sub = raw.get(name)
            if isinstance(sub, dict):
                if name == "tnb":
                    sub = dict(sub)
                    for k in ("live_groups", "live_tactics"):
                        if isinstance(sub.get(k), str):
                            sub[k] = [sub[k]]
                        elif sub.get(k) is None and k in sub:
                            sub[k] = []
                setattr(cfg, name, _build(kls, sub))
        for name in ("mode", "magic", "aggression"):
            if name in raw:
                try:
                    setattr(cfg, name, type(getattr(cfg, name))(raw[name]))
                except Exception:
                    pass
    cfg.ops.data_dir = str(data_dir)
    return cfg


def _tnb_mode(cfg) -> str:
    mode = str(getattr(getattr(cfg, "tnb", None), "mode", "LIVE") or "LIVE").strip().upper()
    return "PAPER" if mode == "PAPER" else "LIVE"


# --------------------------------------------------------------- sqlite --
def connect_ro(path: Path) -> sqlite3.Connection:
    """Open a sqlite file read-only (never created, never written)."""
    uri = Path(path).absolute().as_uri() + "?mode=ro"
    con = sqlite3.connect(uri, uri=True, timeout=BUSY_TIMEOUT_S, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {str(r[1]) for r in con.execute(f"PRAGMA table_info({table})")}


def _num(v: Any) -> Optional[float]:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _side(v: Any) -> str:
    s = str(v if v is not None else "").strip().upper()
    if s in ("BUY", "LONG", "1", "+1"):
        return "BUY"
    if s in ("SELL", "SHORT", "-1"):
        return "SELL"
    n = _num(v)
    if n is not None and n != 0:
        return "BUY" if n > 0 else "SELL"
    return s or "BUY"


def _is_test_row(r: Mapping) -> bool:
    """A row from a synthetic or replayed feed: a test, never a result."""
    for col in ("data_source", "source", "fill_source"):
        src = str(r.get(col) or "").upper()
        if "SYNTH" in src or "REPLAY" in src:
            return True
    return False


# -------------------------------------------------------------- readers --
class _Reader:
    """Reads one source into trades and fills its SourceCheck."""

    def __init__(self, source: str, data_dir: Path, cfg, now: dt.datetime, paper=None):
        self.source = source
        self.bot = SOURCE_BOT[source]
        self.path = Path(data_dir) / SOURCE_FILES[source]
        self.cfg = cfg
        self.now = now
        self.paper = paper
        self.check = SourceCheck(source, str(self.path))
        self.trades: list[Trade] = []
        self._risk_manager = None

    # -- helpers
    def _flag(self, flags: list, name: str) -> None:
        flags.append(name)
        self.check.flags[name] = self.check.flags.get(name, 0) + 1

    def _keep(self, t: Optional[Trade]) -> None:
        if t is None:
            self.check.excluded += 1
        else:
            self.trades.append(t)

    def read(self) -> "_Reader":
        if not self.path.exists():
            self.check.present = False
            if self.source in REQUIRED_SOURCES:
                self.check.fail(UNREADABLE, "the file is not there")
            return self
        try:
            con = connect_ro(self.path)
        except sqlite3.Error as exc:
            self.check.fail(UNREADABLE, f"it could not be opened read-only ({exc})")
            return self
        with closing(con):
            try:
                self._read(con)
            except sqlite3.Error as exc:
                self.trades = []
                self.check.fail(UNREADABLE, f"it could not be read ({exc})")
        return self

    def _rows(self, con, table: str, need: Sequence[str]) -> Optional[tuple[list[dict], set[str]]]:
        cols = table_columns(con, table)
        if not cols:
            if self.source == LAB:                  # the lab has not made its practice table yet
                return [], cols
            self.check.fail(UNREADABLE, f"it has no {table} table")
            return None
        missing = [c for c in need if c not in cols]
        if missing:
            self.check.fail(UNREADABLE, f"its {table} table has no {', '.join(missing)} column")
            return None
        rows = [dict(r) for r in con.execute(f"SELECT * FROM {table} WHERE closed_utc IS NOT NULL "
                                             f"AND closed_utc != '' ORDER BY closed_utc")]
        return rows, cols

    def _read(self, con) -> None:
        getattr(self, "_read_" + self.source)(con)

    def _base(self, r: Mapping, *, approach: str, kind: str, condition: str, symbol: str, side: str,
              real: bool, net: Optional[float], commission: float, risk: Optional[float], flags: list,
              score=None, rr=None) -> Optional[Trade]:
        closed = parse_utc(r.get("closed_utc"))
        opened = parse_utc(r.get("opened_utc")) or closed
        if closed is None or net is None or risk is None or risk <= 0:
            return None
        try:
            ticket = int(r.get("ticket") or 0)
        except (TypeError, ValueError):
            return None
        return Trade(self.source, self.bot, ticket, approach, kind, condition, symbol, side, opened, closed,
                     bool(real), float(net), abs(float(commission or 0.0)), float(risk),
                     _num(score), _num(rr), tuple(flags))

    # -- Trend & Breakout's journal
    def _rm(self):
        if self._risk_manager is None:
            from ..engine.risk import RiskManager
            self._risk_manager = RiskManager(self.cfg)
        return self._risk_manager

    def _read_journal(self, con) -> None:
        got = self._rows(con, "trades", ("ticket", "symbol", "side", "closed_utc", "pnl_money", "tactic",
                                        "regime", "risk_money"))
        if got is None:
            return
        rows, cols = got
        from ..engine.tactics import approach_of
        from ..runner.feed import is_feed_trade
        rm = self._rm()
        self.check.rows = len(rows)
        for r in rows:
            if "entry_state_json" in cols and is_feed_trade(r):
                self.check.skipped += 1                    # the Runner's feed: not Trend & Breakout's own record
                continue
            flags: list[str] = []
            sym = str(r.get("symbol") or "")
            if "commission_sides" not in cols:
                r = dict(r, commission_sides=None)
            if "volume" not in cols:
                r = dict(r, volume=0.0)
            net = None
            raw_net = _num(r.get("pnl_money"))
            if raw_net is not None:
                try:
                    net, est = rm.row_net(r)                # the opening commission, estimated as the trader does
                except Exception:
                    net, est = None, 0.0
                if est > 0:
                    self._flag(flags, F_COMMISSION_ESTIMATED)
            ec = _num(r.get("entry_commission")) if "entry_commission" in cols else None
            if ec is not None and ec != 0:
                commission = 2.0 * abs(ec)
            else:
                vol = _num(r.get("volume")) or 0.0
                try:
                    commission = 2.0 * rm.commission_per_side(sym) * vol
                except Exception:
                    commission = 0.0
            tactic = str(r.get("tactic") or "")
            ticket = int(r.get("ticket") or 0)
            real = not self.paper.is_paper(ticket)
            coach_real = getattr(self.paper, "is_coach_real", None)
            if real and callable(coach_real) and coach_real(ticket):
                self._flag(flags, F_COACH_REAL)            # sent real on a coach grant (the paper record's table)
            self._keep(self._base(r, approach=approach_of(tactic) or "UNKNOWN", kind=kind_of(sym),
                                  condition=_part(r.get("regime")), symbol=sym, side=_side(r.get("side")),
                                  real=real, net=net, commission=commission, risk=_num(r.get("risk_money")),
                                  flags=flags, score=r.get("opportunity") if "opportunity" in cols else None,
                                  rr=r.get("reward_risk") if "reward_risk" in cols else None))

    # -- the bots with a mode column
    def _mode_real(self, r: Mapping) -> bool:
        return str(r.get("mode") or "PAPER").strip().upper() == "LIVE"

    def _fallback_risk(self) -> float:
        if self.source == RUNNER:
            v = float(getattr(getattr(self.cfg, "runner", None), "max_risk_money", 0.0) or 0.0)
            return v if v > 0 else RUNNER_FALLBACK_RISK_GBP
        v = float(getattr(getattr(self.cfg, self.source, None), "risk_money", 0.0) or 0.0)
        return v if v > 0 else BOT_FALLBACK_RISK_GBP

    def _risk_or_fallback(self, r: Mapping, cols: set, col: str, flags: list) -> Optional[float]:
        if col in cols:
            v = _num(r.get(col))
            if v is not None:
                return v
        self._flag(flags, F_RISK_ESTIMATED)
        return self._fallback_risk()

    def _commission(self, r: Mapping, cols: set, col: str, flags: list) -> float:
        v = _num(r.get(col)) if col in cols else None
        if v is None:
            self._flag(flags, F_COMMISSION_UNKNOWN)
            return 0.0
        return abs(v)

    def _read_runner(self, con) -> None:
        got = self._rows(con, "trades", ("ticket", "symbol", "side", "closed_utc", "net_pnl"))
        if got is None:
            return
        rows, cols = got
        from ..engine.tactics import approach_of
        self.check.rows = len(rows)
        for r in rows:
            flags: list[str] = []
            real = "mode" in cols and self._mode_real(r)
            if real and str(r.get("exit_reason") or "").endswith("?"):
                self._flag(flags, F_ESTIMATED_EXIT)
            approach = approach_of(str(r.get("tactic") or "")) if "tactic" in cols else ""
            condition = _part(r.get("regime")) if "regime" in cols and r.get("regime") else "*"
            self._keep(self._base(r, approach=approach or "*", kind="INDEX", condition=condition,
                                  symbol=str(r.get("symbol") or ""), side=_side(r.get("side")), real=real,
                                  net=_num(r.get("net_pnl")), commission=self._commission(r, cols, "commission", flags),
                                  risk=self._risk_or_fallback(r, cols, "risk_money", flags), flags=flags))

    def _read_simple(self, con) -> None:
        """Band Breaker and Crowd Fader: ``bot|*|KIND|*``."""
        got = self._rows(con, "trades", ("ticket", "symbol", "side", "closed_utc", "net_pnl"))
        if got is None:
            return
        rows, cols = got
        self.check.rows = len(rows)
        for r in rows:
            flags: list[str] = []
            real = "mode" in cols and self._mode_real(r)
            if real and str(r.get("exit_reason") or "").endswith("?"):
                self._flag(flags, F_ESTIMATED_EXIT)
            sym = str(r.get("symbol") or "")
            self._keep(self._base(r, approach="*", kind=kind_of(sym), condition="*", symbol=sym,
                                  side=_side(r.get("side")), real=real, net=_num(r.get("net_pnl")),
                                  commission=self._commission(r, cols, "commission", flags),
                                  risk=self._risk_or_fallback(r, cols, "risk_money", flags), flags=flags,
                                  score=r.get("score") if "score" in cols else None))

    _read_bandbreaker = _read_simple
    _read_crowd = _read_simple

    def _read_ian(self, con) -> None:
        got = self._rows(con, "trades", ("ticket", "closed_utc", "net_pnl", "risk_money"))
        if got is None:
            return
        rows, cols = got
        self.check.rows = len(rows)
        for r in rows:
            if _is_test_row(r):
                self.check.skipped += 1                    # a synthetic or replayed feed: a test, not a result
                continue
            flags: list[str] = []
            real = "mode" in cols and self._mode_real(r)
            sym = str(r.get("spot_symbol") or r.get("instrument") or r.get("root") or "")
            condition = _part(r.get("regime")) if "regime" in cols and r.get("regime") else "*"
            self._keep(self._base(r, approach="*", kind="CRYPTO", condition=condition, symbol=sym,
                                  side=_side(r.get("side")), real=real, net=_num(r.get("net_pnl")),
                                  commission=self._commission(r, cols, "commission", flags),
                                  risk=_num(r.get("risk_money")), flags=flags,
                                  score=r.get("score") if "score" in cols else None))

    def _read_lab(self, con) -> None:
        got = self._rows(con, "practice_trades", ("ticket", "candidate_id", "symbol", "side", "closed_utc",
                                                  "net_gbp", "risk_gbp"))
        if got is None:
            return
        rows, cols = got
        self.check.rows = len(rows)
        for r in rows:
            if _is_test_row(r):
                self.check.skipped += 1
                continue
            flags: list[str] = []
            sym = str(r.get("symbol") or "")
            cand = str(r.get("candidate_id") or "").strip() or "UNKNOWN"
            self._keep(self._base(r, approach=cand, kind=kind_of(sym), condition="*", symbol=sym,
                                  side=_side(r.get("side")), real=False, net=_num(r.get("net_gbp")),
                                  commission=self._commission(r, cols, "commission_gbp", flags),
                                  risk=_num(r.get("risk_gbp")), flags=flags))


def _dedupe(trades: list[Trade], check: SourceCheck) -> list[Trade]:
    """One trade per ticket (the last row read wins)."""
    seen: dict[int, Trade] = {}
    for t in trades:
        if t.ticket in seen:
            check.duplicates += 1
        seen[t.ticket] = t
    return sorted(seen.values(), key=lambda t: (t.closed_utc, t.ticket))


def _paper_book(data_dir: Path, cfg, check: SourceCheck):
    """Trend & Breakout's paper book, read-only, or None (the journal check
    then fails: real and practice could not be told apart)."""
    path = Path(data_dir) / "tnb_paper.sqlite"
    if not path.exists() and _tnb_mode(cfg) == "PAPER":
        check.fail(UNREADABLE, "Trend & Breakout is on PAPER but its paper record is not there, "
                               "so real and practice trades cannot be told apart")
        return None
    try:
        from ..broker.paper import PaperBroker          # read-only: it never books and refuses every order
        return PaperBroker(None, data_dir, magic=int(getattr(cfg, "magic", 990_311) or 990_311), read_only=True)
    except Exception as exc:
        check.fail(UNREADABLE, f"its paper record could not be read ({exc}), so real and practice "
                               f"trades cannot be told apart")
        return None


# ----------------------------------------------------------- gate logs --
def read_gate_log(data_dir) -> dict:
    """The gate's decisions that name a ticket: (bot, ticket) -> the line
    (``{ts, ticket, key, route, money, seq, source, why}``). Unreadable lines
    are skipped; the last line for a ticket wins."""
    out: dict = {}
    folder = Paths.of(data_dir).gate_dir
    if not folder.is_dir():
        return out
    for f in sorted(folder.glob("*.jsonl")):
        bot = f.stem.rsplit("-", 3)[0] if f.stem.count("-") >= 3 else f.stem.split("-")[0]
        try:
            text = f.read_text(errors="replace")
        except OSError:
            continue
        for line in text.splitlines():
            try:
                d = json.loads(line)
                ticket = int(d.get("ticket") or 0)
            except Exception:
                continue
            if ticket:
                out[(str(bot).lower(), ticket)] = d
    return out


def _join_gate(trades: list[Trade], gate: Mapping) -> list[Trade]:
    if not gate:
        return trades
    out = []
    for t in trades:
        d = gate.get((t.bot, t.ticket))
        if d is None:
            out.append(t)
            continue
        route = str(d.get("route") or "").upper()
        try:
            seq = int(d.get("seq")) if d.get("seq") is not None else None
        except (TypeError, ValueError):
            seq = None
        flags = t.flags + ((F_COACH_REAL,) if route == "REAL" and t.real and F_COACH_REAL not in t.flags else ())
        out.append(replace(t, coach_route=route, policy_seq=seq, flags=flags))
    return out


# ------------------------------------------------------------- clusters --
def assign_clusters(trades: Iterable[Trade], minutes: float = CLUSTER_MINUTES) -> list[Trade]:
    """Section 4.3, greedily in time order: a trade joins the oldest open
    cluster of the same bot and direction that shares one of its signed legs
    and whose first entry is no more than ``minutes`` before it; otherwise
    it starts a new cluster. Ids are ``<bot>:<n>``."""
    window = dt.timedelta(minutes=float(minutes))
    ordered = sorted(trades, key=lambda t: (t.bot, t.opened_utc, t.source, t.ticket))
    out: list[Trade] = []
    open_: dict[tuple[str, str], list[list]] = {}       # (bot, side) -> [[first entry, legs set, id]]
    counters: dict[str, int] = {}
    for t in ordered:
        mine = set(legs(t.symbol, t.side))
        groups = open_.setdefault((t.bot, t.side), [])
        groups[:] = [g for g in groups if t.opened_utc - g[0] <= window]
        hit = next((g for g in groups if g[1] & mine), None)
        if hit is None:
            counters[t.bot] = counters.get(t.bot, 0) + 1
            hit = [t.opened_utc, set(), f"{t.bot}:{counters[t.bot]}"]
            groups.append(hit)
        hit[1].update(mine)
        out.append(replace(t, cluster=hit[2]))
    return sorted(out, key=lambda t: (t.closed_utc, t.source, t.ticket))


# --------------------------------------------------------------- reading --
def read_evidence(data_dir, *, cfg=None, now: Optional[dt.datetime] = None,
                  previous_counts: Optional[Mapping[str, int]] = None,
                  sources: Sequence[str] = SOURCES) -> Evidence:
    """Read every source read-only, run the data checks (section 4.4) and
    return the trades with their keys and clusters. Never raises: a source
    that cannot be read is marked so and contributes no trades.

    ``cfg``: Aaron's settings as read from config.json (``baseline_config``
    when not given). ``previous_counts``: the closed-row counts of the last
    pass (``Evidence.counts()``), for the "has not shrunk" check."""
    data_dir = Path(data_dir)
    now = to_utc(now or dt.datetime.now(UTC))
    cfg = cfg if cfg is not None else baseline_config(data_dir)
    prev = dict(previous_counts or {})
    checks: dict[str, SourceCheck] = {}
    trades: list[Trade] = []
    for source in sources:
        try:
            paper = None
            check0 = SourceCheck(source, str(data_dir / SOURCE_FILES[source]))
            if source == JOURNAL:
                paper = _paper_book(data_dir, cfg, check0)
                if paper is None:
                    check0.present = (data_dir / SOURCE_FILES[source]).exists()
                    checks[source] = check0
                    continue
            reader = _Reader(source, data_dir, cfg, now, paper=paper)
            try:
                reader.read()
            finally:
                if paper is not None:
                    paper.close_db()
            check = reader.check
            got = _dedupe(reader.trades, check)
            check.used = len(got)
            _final_checks(check, got, now, prev.get(source))
            checks[source] = check
            trades.extend(got)            # kept even when a check failed: data_ok stops every grant
        except Exception as exc:                          # never raises: the source is unreadable
            log.warning("coach records: %s could not be read: %s", source, exc)
            c = SourceCheck(source, str(data_dir / SOURCE_FILES.get(source, source)))
            c.fail(UNREADABLE, f"it could not be read ({exc})")
            checks[source] = c
    gate = read_gate_log(data_dir)
    trades = assign_clusters(_join_gate(trades, gate))
    return Evidence(trades, checks, now, gate)


def _final_checks(check: SourceCheck, trades: Sequence[Trade], now: dt.datetime,
                  previous: Optional[int]) -> None:
    if not check.present:
        return
    if previous is not None and check.rows < int(previous):
        check.fail(STALE, f"it holds {check.rows} closed trades, fewer than the {int(previous)} read before")
    if trades:
        newest = max(t.closed_utc for t in trades)
        check.newest_closed_utc = newest
        if (newest - now).total_seconds() > FUTURE_TOLERANCE_S:
            check.fail(STALE, f"its newest trade closed in the future ({newest.isoformat()})")
    counted = check.rows - check.skipped
    if counted > 0 and check.excluded / counted >= MAX_EXCLUDED_SHARE:
        check.fail(UNREADABLE, f"{check.excluded} of {counted} trades have no usable risk or result "
                               f"(5% or more)")


def coach_real_keys(trades: Iterable[Trade]) -> set:
    """Keys that have been real under the coach (a trade sent real on a coach
    grant): the sceptical cap no longer applies to them."""
    return {t.key for t in trades if F_COACH_REAL in t.flags}


# ------------------------------------------------------ the broker check --
def real_net_on(trades: Iterable[Trade], day: dt.date) -> float:
    """Real money, after commission, of every trade that closed on UK day ``day``."""
    return round(sum(t.net_gbp for t in trades if t.real and t.uk_day == day), 2)


def reconcile(trades: Iterable[Trade], page_today: Optional[float],
              now: Optional[dt.datetime] = None) -> dict:
    """Section 4.4's broker check: the records' real net today (UK day) must
    equal the page's ``standing.today`` within max(3, 10% of it). The page
    figure missing means not ok (no grants)."""
    now = to_utc(now or dt.datetime.now(UTC))
    mine = real_net_on(trades, uk_date(now))
    if page_today is None or _num(page_today) is None:
        return {"ok": False, "records_today": mine, "page_today": None, "tolerance": None,
                "why": "the page's figure for today could not be read"}
    page = float(page_today)
    tol = round(max(RECONCILE_FLOOR_GBP, RECONCILE_SHARE * abs(page)), 2)
    ok = abs(mine - page) <= tol + 1e-9
    return {"ok": ok, "records_today": mine, "page_today": round(page, 2), "tolerance": tol,
            "why": "" if ok else (f"the records say {mine:+.2f} today and MetaTrader's page says {page:+.2f}, "
                                  f"more than {tol:.2f} apart")}


# --------------------------------------------------------------- weights --
def weight(trade: Trade, as_of: dt.datetime, *, practice_weight: float = PRACTICE_WEIGHT,
           half_life_days: float = DECAY_HALF_LIFE_DAYS, epochs=None) -> float:
    """``w = a x d x v`` (section 4.2): ``a`` 1 for real, ``practice_weight``
    (0.5) for bot or lab practice; ``d = 0.5 ** (age in UK trading days /
    30)``; ``v`` 0.5 when the trade closed before its key's latest epoch."""
    from . import epochs as E
    a = 1.0 if trade.real else float(practice_weight)
    age = trading_days_between(trade.uk_day, uk_date(as_of))
    d = 0.5 ** (age / float(half_life_days))
    v = E.epoch_weight(trade.key, trade.closed_utc, as_of, epochs if epochs is not None else E.EPOCHS)
    return a * d * v


# -------------------------------------------------------------- haircut --
@dataclass(frozen=True)
class Haircut:
    """The practice haircut for one key (section 4.2): ``h`` R taken off
    every practice outcome. ``measured``: from the key's own overlapping
    real and practice weeks (else the default 0.10)."""
    h: float = HAIRCUT_DEFAULT_R
    measured: bool = False
    gap: Optional[float] = None       # mean(practice) - mean(real), in R
    se: Optional[float] = None        # its standard error
    real_n: int = 0
    practice_n: int = 0


HAIRCUT_DEFAULT, HAIRCUT_MIN, HAIRCUT_MAX = HAIRCUT_DEFAULT_R, HAIRCUT_MIN_R, HAIRCUT_MAX_R


def haircut(trades: Iterable[Trade], previous: Optional[Haircut] = None) -> Haircut:
    """The haircut for one key's trades. 0.10 R until it has at least 20 real
    and 20 practice trades in overlapping ISO weeks; then
    ``max(0.05, min(0.30, mean(practice) - mean(real)))``, moved only when
    that gap has moved by more than 2 standard errors since ``previous``."""
    by_week: dict[tuple[int, int], tuple[list, list]] = {}
    for t in trades:
        wk = t.uk_day.isocalendar()[:2]
        real, prac = by_week.setdefault(wk, ([], []))
        (real if t.real else prac).append(t.x)
    real_x = [x for r, p in by_week.values() if r and p for x in r]
    prac_x = [x for r, p in by_week.values() if r and p for x in p]
    if len(real_x) < HAIRCUT_MIN_EACH or len(prac_x) < HAIRCUT_MIN_EACH:
        return Haircut(HAIRCUT_DEFAULT, False, None, None, len(real_x), len(prac_x))
    gap = _mean(prac_x) - _mean(real_x)
    se = math.sqrt(_var(prac_x) / len(prac_x) + _var(real_x) / len(real_x))
    h = max(HAIRCUT_MIN, min(HAIRCUT_MAX, gap))
    if previous is not None and previous.measured and previous.gap is not None:
        if abs(gap - previous.gap) <= HAIRCUT_MOVE_SE * max(se, 1e-12):
            return replace(previous, real_n=len(real_x), practice_n=len(prac_x))
    return Haircut(round(h, 6), True, gap, se, len(real_x), len(prac_x))


def _mean(xs: Sequence[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _var(xs: Sequence[float]) -> float:
    if len(xs) < 2:
        return 0.0
    m = _mean(xs)
    return sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
