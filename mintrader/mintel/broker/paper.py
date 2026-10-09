"""PAPER broker for Trend & Breakout: the same code decides; only the order goes nowhere.

:class:`PaperBroker` wraps a REAL broker (the MT5 adapter or the bridge
client, called ``inner`` here) and presents exactly the
:class:`~mintel.broker.base.Broker` surface the trader already uses.

Wiring (done 9 Oct 2026; each step lives in a file this module does not own)
--------------------------------------------------------------------------
It is not a one-line swap. These steps work together:

1. ``mintel/run.py`` ``main()``, after ``wait_for_broker``: the trader is
   built as ``Trader(trend_and_breakout_broker(broker, cfg), cfg)``, which
   is ``PaperBroker.from_config(broker, cfg)`` on PAPER and ``broker``
   itself on LIVE. The wrapper is made there and only there. If the paper
   record cannot be opened on PAPER, the trader does not start (exit code
   6) rather than trade Trend & Breakout live. Never inside
   ``build_broker()``: Financial Ian (``mintel/ian/run.py``), the scalper
   (``mintel/scalper/run.py``) and the ops tools (reconcile, day_review,
   report_upload, ledger_check, potential) all call it. Each would get its
   own booking wrapper on the same file: their real orders refused, and the
   same paper stop booked twice.
2. ``mintel/engine/trader.py``: ``Trader._bot_kwargs`` hands the Momentum
   Runner, the Band Breaker and the Crowd Fader ``real_broker(self.broker)``
   (from this module), never the wrapper, so the LIVE Runner's orders reach
   the broker under its own magic (990511) and its real trades are still
   trailed and closed at the 8-hour mark. ``Trader.real_broker`` (the same
   real broker) also feeds the account health checks and the broker's
   calendar. Trend & Breakout's own executor and scanner keep the wrapper.
   A dry-run Trader (the installer's check, ``mintel/verify.py``) is given
   ``dry_run_view``: a ``read_only=True`` wrapper on the same file that
   never books a stop and refuses every order, and a dry run starts none of
   the other bots.
3. Reports. Each one reads Trend & Breakout's paper figures from the record
   READ-ONLY (``PaperBroker(..., read_only=True)``, through
   ``mintel.ops.standing.TnbPaperRecord``) and its real ones from the real
   broker, and never adds paper to the account:
   ``mintel/ops/attribution.py`` marks each of its rows PAPER or LIVE by
   the record and leaves the PAPER ones out of Overall;
   ``mintel/ops/standing.py`` (the page's Account card and bot rows) reads
   MetaTrader alone (``real_broker_of``), names its mode from the setting
   and shows its practice figure apart; ``mintel/ops/day_review.py``,
   ``mintel/ops/ledger_check.py`` and ``mintel/ops/report_upload.py`` show
   its real deals from the broker and its practice apart;
   ``mintel/ops/pulse.py`` reports the account's day and, on PAPER, a
   separate ``tnb_practice_today``; ``mintel/ops/reconcile.py`` keeps the
   real broker alone (it checks the account). Not wired: no report leaves
   out the Momentum Runner's feed trades yet (``runner_feed``,
   ``mintel.runner.feed.is_feed_trade``); ``runner.feed_tactics`` is empty
   by default, so no such trade is made.
4. ``mintel/config.py``: the ``tnb`` block (``TnbConfig``), ``"mode":
   "LIVE"`` by default or ``"PAPER"``, read through ``config.tnb_mode(cfg)``
   (anything malformed is LIVE, with a warning). Its other fields match
   :class:`PaperConfig`; ``cfg.tnb_paper`` is the block while the mode is
   PAPER and ``None`` otherwise, and ``from_config`` reads it. The Mac
   installer's one-time line-up step (marker ``.lineup-2026-10-09``) sets
   it to PAPER with ``python -m mintel.ops.modes --paper tnb``.

What passes straight through to the real broker (read-only)
-----------------------------------------------------------
Symbols, specifications, ticks, bars, tick history, the calendar, the
server clock, connection and health calls, the margin calculator and
``account()``. The account is the REAL account, so sizing is exactly as in
live.

What is simulated (PAPER) and never reaches the real broker
-----------------------------------------------------------
``send``, ``modify_stops`` and ``close`` (partial closes included). There
are no pending orders on paper, so there is nothing to cancel. With the
default settings no order-sending or order-changing call can reach
``inner``: unknown names are passed through only if they are not order
calls (see ``_BLOCKED``).

* A market order fills at the current REAL tick read from ``inner``: the ask
  for a buy, the bid for a sell, plus an adverse slippage in points
  (``slippage_points``, default 1.0 like the other paper bots). The same
  slippage is taken on every market fill: closes, and stops (a triggered
  stop is a market order at the broker); never on a target. It is refused, in the same shape the real
  adapter returns, when there is no tick (PRICE_OFF "no tick"), when the
  last tick is not fresh (PRICE_OFF, or MARKET_CLOSED when the FX market is
  shut), when the symbol is not open for trading (MARKET_CLOSED), when the
  volume is off the spec's min/max/step (INVALID_VOLUME) and when a stop or
  target is on the wrong side of the price or inside the stops level
  (INVALID_STOPS), as MetaTrader does.
* The stop and target are held "at the broker" by this class: re-checked
  every time a tick for that symbol is read through the wrapper, and on
  every ``positions()`` / deal read (which fetch the current tick). When
  ``replay_ticks`` is on (the default) the real tick history since the last
  full check is replayed too, so a brief spike through a stop between two
  reads is not missed. Each position remembers the last tick time it was
  judged up to (``checked``), and that only moves past ticks that were
  really judged: a read sooner than ``replay_min_seconds`` after the last
  replay judges the current tick and leaves the gap for the next replay; a
  history request that fails leaves the rest for the next read. The replay
  always runs (whatever the timing) before a stop or target change and
  before a close, so earlier ticks are judged against the old levels and
  size, never the new ones; if it cannot run then, the change is refused
  (CONNECTION, nothing changed) and the trader tries again. It also runs
  before a fill on the current tick, so the earliest event wins. A long gap
  (a restart, a sleeping Mac) is read in pieces of ``replay_chunk_minutes``
  back to ``replay_max_hours`` (72); anything older is judged on one-minute
  bars (bid prices; a sell's ask is the bid plus the larger of the bar's
  and the typical spread; a bar that opens through the stop fills at its
  open; a bar that reaches both levels is taken as the stop). A tick no
  newer than a position's last check is never judged again (a late read
  from another thread can never fill a stop set after it). Only ticks
  already printed are ever used (no look-ahead).
  A buy's stop fills when bid <= stop, at the stop or at the bid if the
  price gapped through it (never better), less the slippage; a sell's when
  ask >= stop, at the stop or the worse ask, plus the slippage. A target
  fills only when the price has traded THROUGH it (bid > target for a buy,
  ask < target for a sell) and then exactly at the target, never better.
* ``modify_stops`` keeps broker semantics: any valid stop on the correct
  side of the market is accepted (the engine's own never-widen rule lives
  above, in the executor); an invalid one is refused with INVALID_STOPS like
  MetaTrader; an unchanged request gets "No changes" (10025). A stop of 0
  KEEPS the existing stop on paper (MetaTrader would remove it; paper never
  removes a protective stop); a target of 0 removes the target, as in MT5.
* Money is in the account currency: price profit from ``inner.calc_profit``
  when the broker offers one, otherwise the spec's tick_value / tick_size
  (``SymbolSpec.money``, what the rest of the system uses), rounded to the
  cent like a broker deal. Commission is charged per lot per side by kind of
  market (IC Markets Raw by default: FX and metals USD 3.50, indices 0,
  anything else USD 3.50 so it is never assumed free), converted to the
  account currency with the broker's own rate (see ``_fx_rate``). Swap is
  NOT simulated (always 0).
* Deal rows (``deals_since``) and closed-deal lookups (``closed_deal``) have
  exactly the shape the MT5 adapter returns, so the trader's journal, its
  daily-loss stop and the page read a paper trade exactly as a real one.
  Like the adapter, ``closed_deal`` sums the CLOSING deals only (the entry
  half of the commission sits on the entry deal, which ``deals_since`` with
  ``closing_only=False`` includes).

Whose positions are shown (``positions(magic)``)
------------------------------------------------
* ``magic == self.magic`` (990311 by default, the trader's own number): the
  simulated book, plus any REAL position carrying that same magic (a trade in
  a ``live_groups`` market, or one left open from before the switch to
  paper), so the trader sees its own trades exactly as before. Real ones in
  a live group are managed at the real broker; real ones outside the live
  groups are shown read-only: every change to them is refused (nothing is
  sent) and their own broker-held stop and target close them; their closing
  deal is the broker's. ``manage_leftovers=True`` lets the trader wind those
  down at the real broker instead (stop changes and closes, never an order).
* Any other magic, ``0`` (hand trades) and ``None`` (the whole account): the
  REAL broker's answer, unchanged. Paper is never added to the whole-account
  view (the page's account standing must stay the broker's own figure), and
  another bot's real position can never be modified or closed through this
  wrapper: a ticket that is not on the paper book and not a real position of
  this magic in a live group is refused as "position not found".
* ``send`` with any magic other than this one (or 0, which means this one)
  is refused: another bot (the Momentum Runner, 990511) must be given the
  REAL broker, never this wrapper (``real_broker()``; wiring step 2).

live_groups
-----------
Kinds of market (``contracts.infer_group``, e.g. ``("INDEX",)``) whose
orders go straight to the real broker unchanged. Default: empty, everything
on paper. Orders, stop changes and closes are routed by where the ticket was
born: a paper ticket never reaches ``inner``; a real one in a live group
always does.

Tickets
-------
Paper tickets (positions and deals share one sequence) start at
960_000_001, apart from the other paper sequences in this repository
(700_000_001 Runner / Band Breaker paper executor, 800_000_001 Crowd Fader,
900_000_001 Rapid Scalper), and a new ticket is skipped if a real position
happens to carry that number. Where a ticket was born is decided by the
paper book itself (persisted), never by the number alone.

Persistence
-----------
``data_dir/tnb_paper.sqlite`` holds the open book, every paper deal and the
ticket sequence; a new PaperBroker on the same file restores everything. All
access to the book is under one lock, so the trader's threads can share it.
Only the trader's own instance may book trades: any other process (a report,
a day review) opens the file with ``read_only=True``, which never books a
stop and refuses every order.
"""
from __future__ import annotations

import datetime as dt
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from ..clock import UTC, fx_market_open, to_utc, utcnow
from ..contracts import SymbolSpec, infer_group
from .base import (AccountInfo, Bar, CalendarEvent, OrderRequest, OrderResult,
                   Position, RetCode, Side, TF, Tick)

log = logging.getLogger("mintel.paper")

PAPER_MAGIC = 990_311                 # Trend & Breakout
PAPER_FIRST_TICKET = 960_000_001      # PAPER tickets; see the module notes
DB_NAME = "tnb_paper.sqlite"
LABEL = "Trend & Breakout"

NO_CHANGES = 10025                    # MT5 TRADE_RETCODE_NO_CHANGES
FROZEN = 10029                        # MT5 TRADE_RETCODE_FROZEN

# Per lot PER SIDE, by kind of market: (amount, currency). IC Markets Raw:
# USD 3.50 a side on FX and metals (about 5.50 GBP a lot round turn on the
# GBP account, as the broker's own deals showed 1-5 Oct), nothing on
# indices. "*" is anything else: never assumed free.
DEFAULT_COMMISSION: dict[str, tuple[float, str]] = {
    "FX_MAJOR": (3.50, "USD"), "FX_MINOR": (3.50, "USD"), "FX_EXOTIC": (3.50, "USD"),
    "GOLD": (3.50, "USD"), "SILVER": (3.50, "USD"), "METAL": (3.50, "USD"),
    "INDEX": (0.0, "USD"),
    "*": (3.50, "USD"),
}
# Used only when the broker itself cannot give a rate (see _fx_rate).
DEFAULT_FALLBACK_FX = {"USDGBP": 0.79}

GROUP_ALIASES = {
    "INDICES": ("INDEX",), "INDEXES": ("INDEX",), "INDEX": ("INDEX",),
    "FX": ("FX_MAJOR", "FX_MINOR", "FX_EXOTIC"), "FOREX": ("FX_MAJOR", "FX_MINOR", "FX_EXOTIC"),
    "METALS": ("GOLD", "SILVER", "METAL"),
}

# Names an unknown attribute is never forwarded under: order calls, and the
# MT5 adapter's raw MetaTrader5 module (private names, such as the bridge's
# raw RPC, are never forwarded either). Anything else unknown (clock_skew,
# reconnect, ping, ...) is read-only and passes through.
_BLOCKED = frozenset({"send", "modify_stops", "close", "order_send", "order_check",
                      "cancel", "cancel_order", "close_by", "close_all", "positions_close", "mt5"})

SCHEMA = """
CREATE TABLE IF NOT EXISTS positions (
    ticket INTEGER PRIMARY KEY, symbol TEXT, side TEXT, volume REAL,
    entry REAL, sl REAL, tp REAL, open_utc TEXT, comment TEXT, magic INTEGER,
    checked_utc TEXT
);
CREATE TABLE IF NOT EXISTS deals (
    deal INTEGER PRIMARY KEY, position INTEGER, magic INTEGER, symbol TEXT,
    side TEXT, volume REAL, price REAL, entry_price REAL, profit REAL,
    commission REAL, swap REAL, is_entry INTEGER, time_utc TEXT, reason TEXT
);
CREATE INDEX IF NOT EXISTS ix_paper_deals_time ON deals(time_utc);
CREATE INDEX IF NOT EXISTS ix_paper_deals_position ON deals(position);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
"""


def market_kind(symbol: str, spec: Optional[SymbolSpec] = None) -> str:
    """The kind of market, as the trader classifies it (RiskManager.market_kind):
    ``contracts.infer_group`` on the name, else the spec's own asset group."""
    name = getattr(spec, "name", "") or symbol
    try:
        g = infer_group(name, getattr(spec, "base_currency", "") or "",
                        getattr(spec, "profit_currency", "") or "",
                        getattr(spec, "path", "") or "")
    except Exception:
        g = ""
    if (not g or g == "UNKNOWN") and spec is not None:
        g = str(getattr(spec, "asset_group", "") or g or "UNKNOWN")
    return str(g or "UNKNOWN").upper()


def _groups(names: Sequence[str]) -> frozenset[str]:
    out: set[str] = set()
    for n in names or ():
        key = str(n or "").strip().upper()
        if key:
            out.update(GROUP_ALIASES.get(key, (key,)))
    return frozenset(out)


def _iso(ts: dt.datetime) -> str:
    return _utc(ts).isoformat()


def _utc(ts: dt.datetime) -> dt.datetime:
    return to_utc(ts) if ts.tzinfo is not None else ts.replace(tzinfo=UTC)


def _parse(txt: Optional[str]) -> Optional[dt.datetime]:
    if not txt:
        return None
    return _utc(dt.datetime.fromisoformat(txt))


class _Unreadable(Exception):
    """The real broker could not give the price history asked for."""


def _floor_minute(ts: dt.datetime) -> dt.datetime:
    return ts.replace(second=0, microsecond=0)


def _ceil_minute(ts: dt.datetime) -> dt.datetime:
    floor = _floor_minute(ts)
    return floor if floor == ts else floor + dt.timedelta(minutes=1)


@dataclass
class PaperConfig:
    """Settings for the PAPER wrapper (all optional)."""
    magic: int = PAPER_MAGIC
    # Adverse, on every market fill: entries, closes and stops (a triggered
    # stop is a market order); never on a target. 1.0 like the other paper
    # bots (Band Breaker and Crowd Fader's paper_slippage_points).
    slippage_points: float = 1.0
    commission: dict = field(default_factory=dict)      # {kind: (per lot per side, currency)} over the defaults
    fx_rates: dict = field(default_factory=dict)        # {"USD": 0.79}: account currency per unit; overrides the broker
    fallback_fx_rates: dict = field(default_factory=lambda: dict(DEFAULT_FALLBACK_FX))
    live_groups: tuple[str, ...] = ()           # kinds of market sent to the REAL broker; empty = all paper
    # A REAL position of this magic outside the live groups (left open from
    # before the switch to paper) is read-only by default: its broker stop and
    # target end it and nothing is sent. True lets the trader wind it down at
    # the real broker (stop changes and closes only; never a new order).
    manage_leftovers: bool = False
    max_tick_age_seconds: float = 120.0         # an older tick is not a price to fill at
    replay_ticks: bool = True                   # replay the real tick history between checks
    # A replay runs once at least this long has passed since the last full
    # check (and always before a stop change, a close or a fill). A sooner
    # read judges the current tick and leaves the gap for the next replay.
    replay_min_seconds: float = 1.0
    replay_chunk_minutes: float = 60.0          # one tick-history request covers at most this long
    replay_max_hours: float = 72.0              # tick history this far back; older: one-minute bars
    replay_retry_seconds: float = 5.0           # after a failed request, wait this long before the next
    # A history that has failed for this long is given up for that stretch
    # (with a warning), so the book can still be managed. 0 = never.
    replay_give_up_seconds: float = 900.0
    first_ticket: int = PAPER_FIRST_TICKET
    db_name: str = DB_NAME
    # A view for another process (a report, a day review): the file is opened
    # read-only, nothing is ever booked and every order is refused. Only the
    # trader's own instance may book; two booking instances on one file would
    # book the same stop twice.
    read_only: bool = False


@dataclass
class _Open:
    """One PAPER position on the book."""
    ticket: int
    symbol: str
    side: Side
    volume: float
    entry: float
    sl: float
    tp: float
    opened: dt.datetime
    comment: str
    magic: int
    checked: dt.datetime                        # the last tick time this position was judged against
    saved_checked: Optional[dt.datetime] = None


@dataclass
class _Deal:
    """One PAPER deal (an entry, or a full or partial exit)."""
    deal: int
    position: int
    magic: int
    symbol: str
    side: Side                                  # the POSITION's side
    volume: float
    price: float
    entry_price: float
    profit: float                               # price profit only, account currency
    commission: float                           # negative
    swap: float
    is_entry: bool
    time: dt.datetime
    reason: str


class PaperBroker:
    """A real broker for prices; a simulated one (PAPER) for orders."""

    def __init__(self, inner, data_dir: str | Path = "data", *,
                 config: Optional[PaperConfig] = None,
                 now_fn: Callable[[], dt.datetime] = utcnow, **overrides):
        cfg = config or PaperConfig()
        if overrides:
            cfg = replace(cfg, **overrides)
        self.inner = inner
        self.cfg = cfg
        self.magic = int(cfg.magic)
        self.live_groups = _groups(cfg.live_groups)
        self.slippage_points = float(cfg.slippage_points or 0.0)
        self._commission = {**DEFAULT_COMMISSION, **{str(k).upper(): v for k, v in (cfg.commission or {}).items()}}
        self._now = now_fn
        self._lock = threading.RLock()
        self._book: dict[int, _Open] = {}
        self._deals: list[_Deal] = []
        self._paper_tickets: set[int] = set()       # every position ever born on paper
        self._last_tick: dict[str, Tick] = {}
        self._replay_failing: dict[str, dt.datetime] = {}  # symbol -> when its history first failed to read
        self._replay_wait: dict[str, dt.datetime] = {}     # symbol -> no unforced retry before this
        self._replay_gave_up: set[str] = set()
        self._fx_cache: dict[str, tuple[float, float]] = {}
        self._ccy_cache: tuple[float, str] = (0.0, "")
        self.read_only = bool(cfg.read_only)
        self.path = Path(data_dir) / cfg.db_name
        if self.read_only:
            if self.path.exists():
                self.db = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True, check_same_thread=False,
                                          timeout=10.0)
            else:                                       # nothing on record yet: an empty view
                self.db = sqlite3.connect(":memory:", check_same_thread=False)
                self.db.executescript(SCHEMA)
        else:
            Path(data_dir).mkdir(parents=True, exist_ok=True)
            self.db = sqlite3.connect(str(self.path), check_same_thread=False, timeout=10.0)
            self.db.executescript(SCHEMA)
            self.db.commit()
        self.db.row_factory = sqlite3.Row
        self._next = int(cfg.first_ticket)
        self._restore()

    @classmethod
    def from_config(cls, inner, cfg, **overrides) -> "PaperBroker":
        """Build from the main Config: the trader's magic and data folder, plus an
        optional ``cfg.paper`` (or ``cfg.tnb_paper``) block whose field names
        match :class:`PaperConfig`. Without one: every default."""
        block = getattr(cfg, "tnb_paper", None) or getattr(cfg, "paper", None)
        kw = {}
        if block is not None:
            for name in PaperConfig.__dataclass_fields__:
                src = block.get(name) if isinstance(block, dict) else getattr(block, name, None)
                if src is not None:
                    kw[name] = tuple(src) if name == "live_groups" else src
        kw.setdefault("magic", int(getattr(cfg, "magic", PAPER_MAGIC)))
        kw.update(overrides)
        data_dir = getattr(getattr(cfg, "ops", None), "data_dir", "data")
        return cls(inner, data_dir, config=PaperConfig(**kw))

    # ------------------------------------------------------------ storage --
    def reload(self) -> None:
        """Read the book again from the file (for a read-only view)."""
        self._restore()

    def _restore(self) -> None:
        with self._lock:
            self._book.clear()
            self._deals.clear()
            self._paper_tickets.clear()
            for r in self.db.execute("SELECT * FROM positions"):
                opened = _parse(r["open_utc"]) or self._now_utc()
                checked = _parse(r["checked_utc"]) or opened
                self._book[int(r["ticket"])] = _Open(
                    int(r["ticket"]), str(r["symbol"]), Side(r["side"]), float(r["volume"]), float(r["entry"]),
                    float(r["sl"] or 0.0), float(r["tp"] or 0.0), opened, str(r["comment"] or ""),
                    int(r["magic"] or self.magic), checked, checked)
                self._paper_tickets.add(int(r["ticket"]))
            for r in self.db.execute("SELECT * FROM deals ORDER BY deal"):
                self._deals.append(_Deal(
                    int(r["deal"]), int(r["position"]), int(r["magic"] or self.magic), str(r["symbol"]),
                    Side(r["side"]), float(r["volume"]), float(r["price"]), float(r["entry_price"] or 0.0),
                    float(r["profit"] or 0.0), float(r["commission"] or 0.0), float(r["swap"] or 0.0),
                    bool(r["is_entry"]), _parse(r["time_utc"]) or self._now_utc(), str(r["reason"] or "")))
                self._paper_tickets.add(int(r["position"]))
            row = self.db.execute("SELECT value FROM meta WHERE key='next_ticket'").fetchone()
            top = max([self._next - 1, int(row[0]) - 1 if row else 0]
                      + list(self._book) + [d.deal for d in self._deals] + [d.position for d in self._deals])
            self._next = top + 1

    def _save_open(self, p: _Open) -> None:
        self.db.execute("INSERT OR REPLACE INTO positions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (p.ticket, p.symbol, p.side.value, p.volume, p.entry, p.sl, p.tp, _iso(p.opened),
                         p.comment, p.magic, _iso(p.checked)))
        p.saved_checked = p.checked

    def _save_deal(self, d: _Deal) -> None:
        self.db.execute("INSERT INTO deals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (d.deal, d.position, d.magic, d.symbol, d.side.value, d.volume, d.price, d.entry_price,
                         d.profit, d.commission, d.swap, int(d.is_entry), _iso(d.time), d.reason))

    def _save_next(self) -> None:
        self.db.execute("INSERT OR REPLACE INTO meta VALUES ('next_ticket', ?)", (str(self._next),))

    def _take_ticket(self, avoid: frozenset[int] = frozenset()) -> int:
        while self._next in avoid or self._next in self._book:
            self._next += 1
        t = self._next
        self._next += 1
        return t

    def close_db(self) -> None:
        with self._lock:
            try:
                self.db.close()
            except Exception:
                pass

    # -------------------------------------------------------------- helpers --
    def _now_utc(self) -> dt.datetime:
        return _utc(self._now())

    def _spec(self, symbol: str) -> Optional[SymbolSpec]:
        try:
            return self.inner.spec(symbol)
        except Exception:
            return None

    def _tick(self, symbol: str) -> Optional[Tick]:
        """The current REAL tick, or None when the broker has none (never raises)."""
        try:
            return self.inner.tick(symbol)
        except Exception:
            return None

    def _in_live_group(self, symbol: str, spec: Optional[SymbolSpec] = None) -> bool:
        if not self.live_groups:
            return False
        return market_kind(symbol, spec if spec is not None else self._spec(symbol)) in self.live_groups

    def _fresh(self, tick: Tick) -> bool:
        try:
            age = (self._now_utc() - _utc(tick.time)).total_seconds()
        except Exception:
            return False
        return age <= float(self.cfg.max_tick_age_seconds)

    def _account_currency(self) -> str:
        at, ccy = self._ccy_cache
        if ccy and time.monotonic() - at < 600:
            return ccy
        try:
            ccy = str(self.inner.account().currency or "").upper()
        except Exception:
            ccy = ccy or ""
        if ccy:
            self._ccy_cache = (time.monotonic(), ccy)
        return ccy

    def _fx_rate(self, ccy: str, spec: Optional[SymbolSpec]) -> float:
        """Account currency per one unit of ``ccy``. In order: the configured
        rate; the broker's own conversion (a ``convert``/``fx_rate`` call if it
        has one, else the tick value of the traded symbol when it is quoted in
        ``ccy``, which already holds the broker's rate, else the cross from a
        live tick); last of all the fallback table."""
        ccy = str(ccy or "").upper()
        acc = self._account_currency()
        if not ccy or (acc and ccy == acc):
            return 1.0
        if ccy in (self.cfg.fx_rates or {}):
            return float(self.cfg.fx_rates[ccy])
        hit = self._fx_cache.get(ccy)
        if hit and time.monotonic() - hit[0] < 600:
            return hit[1]
        rate = 0.0
        for name in ("fx_rate", "convert_rate"):
            fn = getattr(self.inner, name, None)
            if callable(fn) and acc:
                try:
                    rate = float(fn(ccy, acc) or 0.0)
                except Exception:
                    rate = 0.0
                if rate > 0:
                    break
        if rate <= 0 and spec is not None and str(spec.profit_currency or "").upper() == ccy:
            try:
                if spec.tick_value > 0 and spec.tick_size > 0 and spec.contract_size > 0:
                    rate = float(spec.tick_value) / (float(spec.tick_size) * float(spec.contract_size))
            except Exception:
                rate = 0.0
        if rate <= 0 and acc:
            for name, invert in ((ccy + acc, False), (acc + ccy, True)):
                t = self._tick(name)
                if t is not None and t.bid > 0 and t.ask > 0:
                    mid = (t.bid + t.ask) / 2.0
                    rate = 1.0 / mid if invert else mid
                    break
        if rate <= 0:
            rate = float((self.cfg.fallback_fx_rates or {}).get(ccy + (acc or "GBP"), 0.0) or 0.0)
            if rate <= 0:
                log.warning("PAPER: no %s->%s rate from the broker or the settings; commission counted 1:1",
                            ccy, acc or "?")
                rate = 1.0
        self._fx_cache[ccy] = (time.monotonic(), rate)
        return rate

    def commission_per_side(self, symbol: str, volume: float, spec: Optional[SymbolSpec] = None) -> float:
        """PAPER commission for one side of ``volume`` lots, in the account
        currency, as a NEGATIVE number (how a broker deal records it)."""
        spec = spec if spec is not None else self._spec(symbol)
        rule = self._commission.get(market_kind(symbol, spec), self._commission.get("*", (0.0, "")))
        if isinstance(rule, (int, float)):
            amount, ccy = float(rule), ""                  # a bare number is already in the account currency
        else:
            amount, ccy = float(rule[0]), str(rule[1] if len(rule) > 1 else "")
        if amount <= 0 or volume <= 0:
            return 0.0
        return -round(amount * float(volume) * self._fx_rate(ccy, spec), 2)

    def _price_profit(self, symbol: str, side: Side, volume: float, entry: float, exit_price: float,
                      spec: Optional[SymbolSpec]) -> float:
        """Price profit in the account currency, as the real broker would book it."""
        fn = getattr(self.inner, "calc_profit", None)
        if callable(fn):
            try:
                v = fn(symbol, side, float(volume), float(entry), float(exit_price))
                if v is not None:
                    return round(float(v), 2)
            except Exception:
                pass
        if spec is None:
            return 0.0
        return round(spec.money((exit_price - entry) * side.sign, volume), 2)

    def _refuse(self, code: RetCode | int, comment: str, req: Optional[OrderRequest] = None,
                ticket: int = 0, requested: float = 0.0) -> OrderResult:
        return OrderResult(False, int(code), ticket=ticket, comment=comment,
                           requested_volume=float(requested or (req.volume if req is not None else 0.0)),
                           idempotency_key=req.idempotency_key if req is not None else "")

    def _closed_market(self) -> bool:
        """A stale tick while the FX market is shut is a closed market."""
        try:
            return not fx_market_open(self._now_utc())
        except Exception:
            return False

    # ------------------------------------------------- stops held "at the broker" --
    def _hit(self, p: _Open, t: Tick) -> Optional[tuple[float, str]]:
        """(trigger price, reason) when this tick closes the position, else
        None. The stop is checked first: one tick cannot fill both, and if one
        ever seemed to, the worse outcome (the stop) is the honest one. The
        slippage is added when the fill is booked (``_book_hit``)."""
        bid, ask = float(t.bid), float(t.ask)
        if bid <= 0 or ask <= 0:
            return None
        if p.side is Side.BUY:
            if p.sl and bid <= p.sl:
                return min(p.sl, bid), "sl"
            if p.tp and bid > p.tp:
                return p.tp, "tp"
        else:
            if p.sl and ask >= p.sl:
                return max(p.sl, ask), "sl"
            if p.tp and ask < p.tp:
                return p.tp, "tp"
        return None

    @staticmethod
    def _hit_bar(p: _Open, bar: Bar, spread: float) -> Optional[tuple[float, str]]:
        """The same rules on a one-minute bar (bid prices; the ask is the bid
        plus ``spread``). A bar that opens through the stop fills at its open;
        a bar that reaches both levels is taken as the stop (the worse one)."""
        o, h, lo = float(bar.open), float(bar.high), float(bar.low)
        if o <= 0 or h <= 0 or lo <= 0:
            return None
        if p.side is Side.BUY:
            if p.sl and lo <= p.sl:
                return min(p.sl, o), "sl"
            if p.tp and h > p.tp:
                return p.tp, "tp"
        else:
            if p.sl and h + spread >= p.sl:
                return max(p.sl, o + spread), "sl"
            if p.tp and lo + spread < p.tp:
                return p.tp, "tp"
        return None

    def _book_hit(self, p: _Open, hit: tuple[float, str], when: dt.datetime) -> None:
        """Book a stop or target fill (caller commits). A stop is a market
        order once triggered: it fills the slippage worse. A target fills
        exactly at the target."""
        price, kind = hit
        spec = self._spec(p.symbol)
        level = p.sl if kind == "sl" else p.tp
        if kind == "sl":
            price = float(price) - p.side.sign * self.slippage_points * float(getattr(spec, "point", 0.0) or 0.0)
        if spec is not None:
            price = spec.normalise_price(price)
        digits = spec.digits if spec is not None else 5
        self._settle(p, p.volume, price, f"[{kind} {level:.{digits}f}] PAPER", when, spec)
        log.info("PAPER %s %s %s %g closed by its %s at %s", LABEL, p.symbol, p.side.value,
                 p.volume, "stop" if kind == "sl" else "target", price)

    def _can_replay(self) -> bool:
        return bool(self.cfg.replay_ticks) and callable(getattr(self.inner, "ticks_range", None))

    def _replay(self, symbol: str, todo: list[_Open], now_t: dt.datetime) -> bool:
        """Judge every price printed after each position's last check and
        before ``now_t``, oldest first, booking the first stop or target hit.
        True when the whole stretch was read. False when a request failed:
        what was read is judged, ``checked`` stays at the last tick judged,
        and the rest is read again next time (never skipped), unless it has
        failed for ``replay_give_up_seconds``, when that stretch is given up
        with a warning so the book can still be managed."""
        try:
            self._replay_window(symbol, todo, now_t)
        except _Unreadable as exc:
            now = self._now_utc()
            first = self._replay_failing.setdefault(symbol, now)
            self._replay_wait[symbol] = now + dt.timedelta(seconds=float(self.cfg.replay_retry_seconds))
            give_up = float(self.cfg.replay_give_up_seconds or 0.0)
            if give_up > 0 and (now - first).total_seconds() >= give_up:
                if symbol not in self._replay_gave_up:
                    self._replay_gave_up.add(symbol)
                    log.warning("PAPER %s: the price history for %s has not been readable for %.0f minutes (%s). "
                                "The stretch that could not be read is NOT checked for stops; only the "
                                "current price is, until the history can be read again.",
                                LABEL, symbol, (now - first).total_seconds() / 60.0, exc)
                return True
            if first == now:
                log.warning("PAPER %s: could not read the price history for %s (%s). The current price is "
                            "still checked; the gap will be read again before anything is changed.",
                            LABEL, symbol, exc)
            return False
        if symbol in self._replay_failing:
            log.info("PAPER %s: the price history for %s can be read again", LABEL, symbol)
        self._replay_failing.pop(symbol, None)
        self._replay_wait.pop(symbol, None)
        self._replay_gave_up.discard(symbol)
        return True

    def _replay_window(self, symbol: str, todo: list[_Open], now_t: dt.datetime) -> None:
        """The stretch in order: ticks back to ``replay_max_hours``, read in
        pieces; anything older on one-minute bars (with the first part
        minute before the bars on ticks). Raises ``_Unreadable`` when the
        broker cannot be read (a booking error is not one: it propagates)."""
        since = min(p.checked for p in todo)
        cap = now_t - dt.timedelta(hours=float(self.cfg.replay_max_hours))
        segments: list[tuple[str, dt.datetime, dt.datetime]] = []
        ticks_from = since
        if since < cap:
            bars_from = _ceil_minute(max(p.checked for p in todo))
            bars_to = _floor_minute(cap)
            if bars_from < bars_to:
                segments += [("ticks", since, bars_from), ("bars", bars_from, bars_to)]
                ticks_from = bars_to
        segments.append(("ticks", ticks_from, now_t))
        live = list(todo)
        for kind, lo, hi in segments:
            if lo >= hi:
                continue
            if kind == "bars":
                self._replay_bars(symbol, live, lo, hi)
            else:
                self._replay_ticks(symbol, live, lo, hi, now_t)
            live = [p for p in live if p.ticket in self._book]
            if not live:
                return

    def _replay_ticks(self, symbol: str, live: list[_Open], lo: dt.datetime, hi: dt.datetime,
                      now_t: dt.datetime) -> None:
        step = dt.timedelta(minutes=max(1.0, float(self.cfg.replay_chunk_minutes)))
        a = lo
        while a < hi and live:
            b = min(a + step, hi)
            try:
                rows = self.inner.ticks_range(symbol, a, b) or []
            except Exception as exc:
                raise _Unreadable(exc) from exc
            ticks = sorted((t for t in rows if _utc(t.time) < now_t), key=lambda t: _utc(t.time))
            for p in live:
                for t in ticks:
                    when = _utc(t.time)
                    if when <= p.checked:
                        continue
                    hit = self._hit(p, t)
                    if hit is not None:
                        self._book_hit(p, hit, when)
                        break
                    p.checked = when                      # judged up to here
            live = [p for p in live if p.ticket in self._book]
            a = b

    def _replay_bars(self, symbol: str, live: list[_Open], lo: dt.datetime, hi: dt.datetime) -> None:
        """Whole one-minute bars from ``lo`` to ``hi`` (both on the minute)."""
        spec = self._spec(symbol)
        point = float(getattr(spec, "point", 0.0) or 0.0)
        typical = float(getattr(spec, "typical_spread_points", 0.0) or 0.0)
        step = dt.timedelta(days=7)
        a = lo
        while a < hi and live:
            b = min(a + step, hi)
            count = int((b - a).total_seconds() // 60) + 2
            try:
                rows = self.inner.bars(symbol, TF.M1, count, b - dt.timedelta(seconds=1)) or []
            except Exception as exc:
                raise _Unreadable(exc) from exc
            bars = sorted((r for r in rows if a <= _utc(r.time) and _utc(r.time) + dt.timedelta(minutes=1) <= b),
                          key=lambda r: _utc(r.time))
            for p in live:
                for bar in bars:
                    when = _utc(bar.time)
                    if when < p.checked:
                        continue
                    spread = max(float(getattr(bar, "spread_points", 0.0) or 0.0), typical) * point
                    hit = self._hit_bar(p, bar, spread)
                    if hit is not None:
                        self._book_hit(p, hit, when)
                        break
                if p.ticket in self._book:
                    p.checked = max(p.checked, b - dt.timedelta(microseconds=1))
            live = [p for p in live if p.ticket in self._book]
            a = b

    def _evaluate(self, symbol: str, tick: Optional[Tick] = None, force: bool = False) -> Optional[Tick]:
        """Judge every PAPER position on ``symbol`` against the prices since
        its last check and the current one; book the stop or target fills.
        ``force`` runs the replay whatever the timing (before a change)."""
        with self._lock:
            if tick is None:
                tick = self._tick(symbol)
            if tick is None:
                return None
            now_t = _utc(tick.time)
            last = self._last_tick.get(symbol)
            if last is None or _utc(last.time) <= now_t:
                self._last_tick[symbol] = tick
            if self.read_only:                            # a read-only view never books anything
                return tick
            # A tick no newer than a position's last check was judged already,
            # or is a late read from another thread: it is never judged again,
            # and never against a stop set after it.
            todo = [p for p in self._book.values() if p.symbol == symbol and now_t > p.checked]
            if not todo:
                return tick
            covered = True                                # no replay: only the current tick counts
            if self._can_replay():
                since = min(p.checked for p in todo)
                waiting = self._replay_wait.get(symbol)
                due = (force or any(self._hit(p, tick) for p in todo)      # a fill now: look back first
                       or ((now_t - since).total_seconds() >= float(self.cfg.replay_min_seconds)
                           and (waiting is None or self._now_utc() >= waiting)))
                covered = self._replay(symbol, todo, now_t) if due else False
            changed = False
            for p in todo:
                if p.ticket not in self._book:            # filled during the replay
                    changed = True
                    continue
                hit = self._hit(p, tick)
                if hit is not None:
                    self._book_hit(p, hit, now_t)
                    changed = True
                    continue
                if covered:
                    p.checked = max(p.checked, now_t)
                if p.saved_checked is None or abs((p.checked - p.saved_checked).total_seconds()) >= 5.0:
                    self._save_open(p)
                    changed = True
            if changed:
                self.db.commit()
            return tick

    def _unjudged(self, p: _Open, tick: Tick) -> Optional[OrderResult]:
        """A refusal when prices before ``tick`` are still unjudged for ``p``
        (its history could not be read): nothing may change until they are."""
        if p.checked >= _utc(tick.time):
            return None
        return OrderResult(False, RetCode.CONNECTION.value, ticket=p.ticket,
                           comment="PAPER: the price history since the last check could not be read, so the "
                                   "stop could not be checked first; nothing was changed (try again)")

    def _evaluate_all(self) -> None:
        with self._lock:
            for symbol in sorted({p.symbol for p in self._book.values()}):
                self._evaluate(symbol)

    def _settle(self, p: _Open, volume: float, price: float, reason: str, when: dt.datetime,
                spec: Optional[SymbolSpec]) -> _Deal:
        """Book a PAPER exit of ``volume`` lots at ``price`` (caller commits)."""
        volume = min(float(volume), p.volume)
        d = _Deal(self._take_ticket(), p.ticket, p.magic, p.symbol, p.side, volume, float(price), p.entry,
                  self._price_profit(p.symbol, p.side, volume, p.entry, float(price), spec),
                  self.commission_per_side(p.symbol, volume, spec), 0.0, False, _utc(when), reason)
        self._deals.append(d)
        self._save_deal(d)
        left = round(p.volume - volume, 8)
        if left <= 1e-9:
            del self._book[p.ticket]
            self.db.execute("DELETE FROM positions WHERE ticket=?", (p.ticket,))
        else:
            p.volume = left
            self._save_open(p)
        self._save_next()
        return d

    # --------------------------------------------------------- pass-through --
    def connect(self) -> bool:
        return self.inner.connect()

    def shutdown(self) -> None:
        self.inner.shutdown()

    def is_connected(self) -> bool:
        return self.inner.is_connected()

    def account(self) -> AccountInfo:
        """The REAL account: sizing stays exactly as in live."""
        return self.inner.account()

    def server_time(self) -> dt.datetime:
        return self.inner.server_time()

    @property
    def clock(self):
        return getattr(self.inner, "clock", None)

    @property
    def timeout(self):
        return getattr(self.inner, "timeout", None)

    @timeout.setter
    def timeout(self, value) -> None:
        if hasattr(self.inner, "timeout"):
            self.inner.timeout = value

    def symbols(self) -> Sequence[str]:
        return self.inner.symbols()

    def spec(self, symbol: str) -> Optional[SymbolSpec]:
        return self.inner.spec(symbol)

    def ensure_selected(self, symbol: str) -> bool:
        return self.inner.ensure_selected(symbol)

    def bars(self, symbol: str, tf: TF, count: int, end: Optional[dt.datetime] = None) -> list[Bar]:
        return self.inner.bars(symbol, tf, count, end)

    def ticks_range(self, symbol: str, start: dt.datetime, end: dt.datetime) -> list[Tick]:
        return self.inner.ticks_range(symbol, start, end)

    def calendar(self, start: dt.datetime, end: dt.datetime, currencies: Sequence[str] = ()) -> list[CalendarEvent]:
        return self.inner.calendar(start, end, currencies)

    def calc_margin(self, symbol: str, side: Side, volume: float, price: float) -> Optional[float]:
        fn = getattr(self.inner, "calc_margin", None)
        return fn(symbol, side, volume, price) if callable(fn) else None

    def tick(self, symbol: str) -> Optional[Tick]:
        """The REAL tick, unchanged. Reading it also lets any PAPER stop or
        target on that symbol fill, as it would at the broker."""
        t = self.inner.tick(symbol)
        if t is not None and self._has_open(symbol):
            self._evaluate(symbol, t)
        return t

    def ticks(self, symbols: Sequence[str]) -> dict:
        fn = getattr(self.inner, "ticks", None)
        out = fn(list(symbols)) if callable(fn) else {s: self.inner.tick(s) for s in symbols}
        for s, t in (out or {}).items():
            if t is not None and self._has_open(s):
                self._evaluate(s, t)
        return out

    def __getattr__(self, name: str) -> Any:
        # Read-only extras of the real broker (clock_skew, reconnect, outdated,
        # ping, disconnect, last_error, ...). Never an order call, never private.
        if name.startswith("_") or name in _BLOCKED:
            raise AttributeError(name)
        inner = self.__dict__.get("inner")
        if inner is None:
            raise AttributeError(name)
        return getattr(inner, name)

    # ---------------------------------------------------------- the book --
    def is_paper(self, ticket: int) -> bool:
        """Was this ticket born on paper (open or closed)?"""
        with self._lock:
            return int(ticket) in self._paper_tickets

    def _has_open(self, symbol: str) -> bool:
        with self._lock:
            return any(p.symbol == symbol for p in self._book.values())

    def _as_position(self, p: _Open) -> Position:
        spec = self._spec(p.symbol)
        t = self._last_tick.get(p.symbol)
        profit = 0.0
        if t is not None and spec is not None:
            mark = float(t.bid if p.side is Side.BUY else t.ask)
            if mark > 0:
                profit = self._price_profit(p.symbol, p.side, p.volume, p.entry, mark, spec)
        return Position(ticket=p.ticket, symbol=p.symbol, side=p.side, volume=p.volume, entry_price=p.entry,
                        sl=p.sl, tp=p.tp, open_time=p.opened, profit=profit, swap=0.0,
                        comment=p.comment, magic=p.magic)

    def _real_own(self) -> list[Position]:
        """REAL positions carrying this magic (live groups, and any left from
        before the switch). Raises when the broker cannot be read."""
        return [p for p in (self.inner.positions(self.magic) or []) if int(p.magic) == self.magic
                and not self.is_paper(p.ticket)]

    def positions(self, magic: Optional[int] = None) -> list[Position]:
        """This magic: the PAPER book plus its own REAL positions. Any other
        magic, 0 or None (the whole account): the real broker's answer only."""
        if magic is None or int(magic) != self.magic:
            return list(self.inner.positions(magic) or [])
        real = self._real_own()                           # a broker that cannot be read raises, as in live
        with self._lock:
            self._evaluate_all()
            paper = [self._as_position(p) for p in sorted(self._book.values(), key=lambda q: q.ticket)]
        return real + paper

    # ------------------------------------------------------------- orders --
    def _route_real(self, ticket: int) -> Optional[Position]:
        """The REAL position this wrapper may pass a stop change or close for:
        carrying this magic and in a live group (or any market, with
        ``manage_leftovers``). None for everything else."""
        if not self.live_groups and not self.cfg.manage_leftovers:
            return None
        try:
            for p in self.inner.positions(self.magic) or []:
                if int(p.ticket) == int(ticket) and int(p.magic) == self.magic:
                    ok = self.cfg.manage_leftovers or self._in_live_group(p.symbol)
                    return p if ok else None
        except Exception:
            return None
        return None

    def _read_only_refusal(self, ticket: int = 0, req: Optional[OrderRequest] = None) -> OrderResult:
        return self._refuse(RetCode.REJECT, "PAPER read-only view: nothing can be changed or sent from here",
                            req, ticket=int(ticket))

    def _not_found(self, ticket: int) -> OrderResult:
        return OrderResult(False, RetCode.REJECT.value, ticket=int(ticket),
                           comment="position not found (PAPER: only this bot's paper positions, or its "
                                   "real ones in a live market, can be changed here; nothing was sent)")

    def send(self, req: OrderRequest) -> OrderResult:
        """Open a position: PAPER, unless its market is a live group."""
        if self.read_only:
            return self._read_only_refusal(req=req)
        magic = int(req.magic or self.magic)
        if magic != self.magic:
            return self._refuse(RetCode.REJECT,
                                f"PAPER wrapper for magic {self.magic} refused an order for magic {magic}: "
                                f"that bot must be given the real broker; nothing was sent", req)
        spec = self._spec(req.symbol)
        if spec is None:
            return self._refuse(RetCode.REJECT, "no symbol spec", req)
        if self._in_live_group(req.symbol, spec):
            return self.inner.send(req)                   # LIVE group: the real order, unchanged
        with self._lock:
            tick = self._evaluate(req.symbol)             # PAPER: the book first, then the fill
            if tick is None:
                return self._refuse(RetCode.PRICE_OFF, "no tick", req)
            if not self._fresh(tick):
                age = (self._now_utc() - _utc(tick.time)).total_seconds()
                if self._closed_market():
                    return self._refuse(RetCode.MARKET_CLOSED, f"PAPER: market closed (last price {age:.0f}s old)", req)
                return self._refuse(RetCode.PRICE_OFF, f"PAPER: no fresh price (last tick {age:.0f}s old)", req)
            if not getattr(spec, "trade_allowed", True):
                return self._refuse(RetCode.MARKET_CLOSED, "PAPER: market closed for new orders", req)
            vol = float(req.volume or 0.0)
            why = self._bad_volume(vol, spec)
            if why:
                if "below" in why:
                    return self._refuse(RetCode.INVALID_VOLUME, "volume below minimum", req)
                return self._refuse(RetCode.INVALID_VOLUME, f"PAPER: invalid volume ({why})", req)
            slip = self.slippage_points * float(spec.point or 0.0)
            touch = float(tick.ask if req.side is Side.BUY else tick.bid)
            price = spec.normalise_price(touch + req.side.sign * slip)
            sl = spec.normalise_price(req.sl) if req.sl else 0.0
            tp = spec.normalise_price(req.tp) if req.tp else 0.0
            bad = self._bad_stops(req.side, sl, tp, tick, spec)
            if bad:
                return self._refuse(RetCode.INVALID_STOPS, f"PAPER: invalid stops ({bad})", req)
            try:
                avoid = frozenset(int(p.ticket) for p in (self.inner.positions(None) or []))
            except Exception:
                avoid = frozenset()
            ticket = self._take_ticket(avoid)
            when = _utc(tick.time)
            comment = ("PAPER " + (req.comment or req.idempotency_key or ""))[:31].rstrip()
            p = _Open(ticket, req.symbol, req.side, vol, price, sl, tp, when, comment, self.magic, when)
            entry = _Deal(self._take_ticket(avoid), ticket, self.magic, req.symbol, req.side, vol, price, price,
                          0.0, self.commission_per_side(req.symbol, vol, spec), 0.0, True, when,
                          "PAPER " + (req.comment or "open"))
            self._book[ticket] = p
            self._paper_tickets.add(ticket)
            self._deals.append(entry)
            self._save_open(p)
            self._save_deal(entry)
            self._save_next()
            self.db.commit()
            log.info("PAPER %s %s %s %g filled at %s (stop %s, target %s), ticket %s", LABEL, req.symbol,
                     req.side.value, vol, price, sl, tp, ticket)
            return OrderResult(True, RetCode.DONE.value, ticket=ticket, deal=entry.deal, filled_volume=vol,
                               price=price, requested_volume=vol, comment="PAPER fill",
                               idempotency_key=req.idempotency_key,
                               raw={"paper": True, "touch": touch, "slippage_points": self.slippage_points})

    @staticmethod
    def _bad_volume(volume: float, spec: SymbolSpec) -> str:
        """Why MetaTrader would refuse this volume ("" when it would not)."""
        step = float(spec.volume_step or 0.0)
        if volume <= 0 or volume < float(spec.volume_min) - 1e-9:
            return f"{volume:g} lots is below the minimum {spec.volume_min:g}"
        if spec.volume_max and volume > float(spec.volume_max) + 1e-9:
            return f"{volume:g} lots is above the maximum {spec.volume_max:g}"
        if step > 0:
            steps = volume / step
            if abs(steps - round(steps)) > 1e-6:
                return f"{volume:g} lots is not a multiple of the step {step:g}"
        return ""

    @staticmethod
    def _bad_stops(side: Side, sl: float, tp: float, tick: Tick, spec: SymbolSpec) -> str:
        """MetaTrader's stop rules: a buy's levels are judged against the bid, a
        sell's against the ask, and each must be at least the stops level away
        on its own side. "" when they are fine."""
        level = float(spec.stops_level_points or 0.0) * float(spec.point or 0.0)
        eps = float(spec.point or 0.0) * 1e-3
        if side is Side.BUY:
            ref = float(tick.bid)
            if sl and not (ref - sl > 0 and ref - sl >= level - eps):
                return f"stop {sl} must be below the bid {ref} by at least {level:g}"
            if tp and not (tp - ref > 0 and tp - ref >= level - eps):
                return f"target {tp} must be above the bid {ref} by at least {level:g}"
        else:
            ref = float(tick.ask)
            if sl and not (sl - ref > 0 and sl - ref >= level - eps):
                return f"stop {sl} must be above the ask {ref} by at least {level:g}"
            if tp and not (ref - tp > 0 and ref - tp >= level - eps):
                return f"target {tp} must be below the ask {ref} by at least {level:g}"
        return ""

    def modify_stops(self, ticket: int, sl: float, tp: float) -> OrderResult:
        """Move a PAPER position's stop/target (broker semantics); a real one in
        a live group goes to the real broker; anything else is not found."""
        ticket = int(ticket)
        if self.read_only:
            return self._read_only_refusal(ticket)
        with self._lock:
            p = self._book.get(ticket)
            if p is not None:
                return self._modify_paper(p, sl, tp)
            if self.is_paper(ticket):
                return self._not_found(ticket)            # closed on paper; never sent anywhere
        if self._route_real(ticket) is not None:
            return self.inner.modify_stops(ticket, sl, tp)
        return self._not_found(ticket)

    def _modify_paper(self, p: _Open, sl: float, tp: float) -> OrderResult:
        spec = self._spec(p.symbol)
        tick = self._evaluate(p.symbol, force=True)       # every earlier price judged on the OLD levels first
        if p.ticket not in self._book:                    # those prices (or this one) took it out
            return self._not_found(p.ticket)
        if spec is None:
            return OrderResult(False, RetCode.REJECT.value, ticket=p.ticket, comment="no symbol spec")
        if tick is None:
            return OrderResult(False, RetCode.PRICE_OFF.value, ticket=p.ticket,
                               comment="PAPER: no price to check the stop against")
        refused = self._unjudged(p, tick)
        if refused is not None:
            return refused
        new_sl = spec.normalise_price(sl) if sl else p.sl  # 0 keeps the stop: paper never removes one
        new_tp = spec.normalise_price(tp) if tp else 0.0
        if abs(new_sl - p.sl) < spec.point * 1e-3 and abs(new_tp - p.tp) < spec.point * 1e-3:
            return OrderResult(False, NO_CHANGES, ticket=p.ticket, comment="No changes")
        freeze = float(spec.freeze_level_points or 0.0) * float(spec.point or 0.0)
        if freeze > 0:
            ref = float(tick.bid if p.side is Side.BUY else tick.ask)
            if (p.sl and abs(ref - p.sl) <= freeze) or (p.tp and abs(p.tp - ref) <= freeze):
                return OrderResult(False, FROZEN, ticket=p.ticket, comment="PAPER: frozen (price inside the freeze level)")
        bad = self._bad_stops(p.side, new_sl, new_tp, tick, spec)
        if bad:
            return OrderResult(False, RetCode.INVALID_STOPS.value, ticket=p.ticket, comment=f"PAPER: invalid stops ({bad})")
        p.sl, p.tp = new_sl, new_tp
        self._save_open(p)
        self.db.commit()
        log.info("PAPER %s %s ticket %s stop %s target %s", LABEL, p.symbol, p.ticket, new_sl, new_tp)
        return OrderResult(True, RetCode.DONE.value, ticket=p.ticket, comment="PAPER modify")

    def close(self, ticket: int, volume: float = 0.0, comment: str = "") -> OrderResult:
        """Close all or part of a PAPER position at the current bid (buy) or
        ask (sell) less the adverse slippage; a real one in a live group goes
        to the real broker; anything else is not found."""
        ticket = int(ticket)
        if self.read_only:
            return self._read_only_refusal(ticket)
        with self._lock:
            p = self._book.get(ticket)
            if p is not None:
                return self._close_paper(p, volume, comment)
            if self.is_paper(ticket):
                return self._not_found(ticket)
        if self._route_real(ticket) is not None:
            return self.inner.close(ticket, volume, comment)
        return self._not_found(ticket)

    def _close_paper(self, p: _Open, volume: float, comment: str) -> OrderResult:
        spec = self._spec(p.symbol)
        tick = self._evaluate(p.symbol, force=True)       # every earlier price judged on the FULL size first
        if p.ticket not in self._book:                    # its stop or target filled first
            return self._not_found(p.ticket)
        if spec is None:
            return OrderResult(False, RetCode.REJECT.value, ticket=p.ticket, comment="no symbol spec")
        if tick is None:
            return OrderResult(False, RetCode.PRICE_OFF.value, ticket=p.ticket, comment="no tick")
        refused = self._unjudged(p, tick)
        if refused is not None:
            return refused
        if not self._fresh(tick):
            code = RetCode.MARKET_CLOSED if self._closed_market() else RetCode.PRICE_OFF
            return OrderResult(False, code.value, ticket=p.ticket, comment="PAPER: no fresh price to close at")
        if volume and volume > 0:
            vol = min(spec.normalise_volume(volume), p.volume)
            if vol <= 0:
                return OrderResult(False, RetCode.INVALID_VOLUME.value, ticket=p.ticket,
                                   comment="PAPER: partial volume below the lot step")
            if 0 < round(p.volume - vol, 8) < float(spec.volume_min) - 1e-9:
                vol = p.volume                            # no sub-minimum remainder is left open
        else:
            vol = p.volume
        slip = self.slippage_points * float(spec.point or 0.0)
        mark = float(tick.bid if p.side is Side.BUY else tick.ask)
        price = spec.normalise_price(mark - p.side.sign * slip)
        self._settle(p, vol, price, f"PAPER {comment or 'close'}", _utc(tick.time), spec)
        self.db.commit()
        log.info("PAPER %s %s ticket %s closed %g at %s (%s)", LABEL, p.symbol, p.ticket, vol, price,
                 comment or "close")
        return OrderResult(True, RetCode.DONE.value, ticket=p.ticket, filled_volume=vol, price=price,
                           requested_volume=vol, comment="PAPER close")

    # ---------------------------------------------------------------- deals --
    @staticmethod
    def _row(d: _Deal) -> dict:
        """One deal in exactly the MT5 adapter's ``deals_since`` shape."""
        return {"position": d.position, "magic": d.magic, "symbol": d.symbol, "volume": d.volume,
                "profit": round(d.profit + d.commission + d.swap, 2), "commission": d.commission,
                "is_entry": d.is_entry, "time": d.time}

    def paper_deals_since(self, since_utc: dt.datetime, closing_only: bool = True) -> list[dict]:
        """The PAPER deals alone, in the adapter's shape."""
        since = _utc(since_utc)
        with self._lock:
            self._evaluate_all()
            return [self._row(d) for d in self._deals
                    if d.time >= since and not (closing_only and d.is_entry)]

    def deals_since(self, since_utc: dt.datetime, magic: int = 0, closing_only: bool = True) -> list[dict]:
        """This magic: its PAPER deals plus its own REAL deals (live groups,
        and anything from before the switch). Any other magic, or 0 (the whole
        account): the real broker's record only - paper is never in it."""
        m = int(magic or 0)
        fn = getattr(self.inner, "deals_since", None)
        if m != self.magic:
            if fn is None:
                raise AttributeError("the real broker has no deal history")
            return fn(since_utc, magic, closing_only)
        real = []
        if fn is not None:
            real = [r for r in (fn(since_utc, m, closing_only) or [])
                    if int(r.get("magic", m) or m) == m and not self.is_paper(int(r.get("position") or 0))]
        rows = real + self.paper_deals_since(since_utc, closing_only)
        rows.sort(key=lambda r: _utc(r["time"]) if isinstance(r.get("time"), dt.datetime) else _utc(dt.datetime.min))
        return rows

    def closed_deal(self, ticket: int) -> Optional[dict]:
        """What happened to a closed position, in the adapter's shape: PAPER
        from the book; a real one from the real broker (read-only)."""
        ticket = int(ticket)
        with self._lock:
            if self.is_paper(ticket):
                p = self._book.get(ticket)
                if p is not None:
                    self._evaluate(p.symbol)
                closing = [d for d in self._deals if d.position == ticket and not d.is_entry]
                if not closing:
                    return None
                volume = sum(d.volume for d in closing)
                pnl = sum(d.profit + d.commission + d.swap for d in closing)
                exit_price = (sum(d.price * d.volume for d in closing) / volume if volume else closing[-1].price)
                return {"exit_price": exit_price, "pnl": round(pnl, 2), "volume": volume,
                        "symbol": closing[-1].symbol, "position": ticket, "time": closing[-1].time,
                        "reason": closing[-1].reason}
        fn = getattr(self.inner, "closed_deal", None)
        return fn(ticket) if callable(fn) else None

    @property
    def closed(self) -> list[dict]:
        """PAPER exits in the simulator's ``closed`` shape (the trader's last
        resort for an exit price)."""
        with self._lock:
            return [{"ticket": d.position, "symbol": d.symbol, "side": d.side.value, "volume": d.volume,
                     "entry": d.entry_price, "exit": d.price, "pnl": round(d.profit + d.commission + d.swap, 2),
                     "reason": d.reason, "time": d.time}
                    for d in self._deals if not d.is_entry]

    # -------------------------------------------------------------- summary --
    def _day_start(self, now: dt.datetime) -> dt.datetime:
        clock = self.clock
        try:
            if clock is not None and hasattr(clock, "day_start_utc"):
                return _utc(clock.day_start_utc(now))
        except Exception:
            pass
        return now.replace(hour=0, minute=0, second=0, microsecond=0)

    @staticmethod
    def _by_position(rows: list[dict], day_start: dt.datetime) -> list[dict]:
        """Positions whose last closing deal fell at or after ``day_start``,
        with every deal of the position counted (the entry's commission too)."""
        net: dict[int, float] = {}
        fees: dict[int, float] = {}
        last: dict[int, dt.datetime] = {}
        sym: dict[int, str] = {}
        for r in rows:
            pos = int(r.get("position") or 0)
            net[pos] = net.get(pos, 0.0) + float(r.get("profit") or 0.0)
            fees[pos] = fees.get(pos, 0.0) + float(r.get("commission") or 0.0)
            sym[pos] = str(r.get("symbol") or sym.get(pos, ""))
            if not r.get("is_entry") and r.get("time") is not None:
                t = _utc(r["time"])
                last[pos] = max(last.get(pos, t), t)
        return [{"ticket": p, "symbol": sym[p], "net": round(net[p], 2), "commission": round(fees[p], 2),
                 "closed": last[p].isoformat()}
                for p in sorted(last, key=lambda k: last[k]) if last[p] >= day_start]

    def summary(self, now: Optional[dt.datetime] = None) -> dict:
        """For status pages: open positions (each marked PAPER or REAL) and
        today's closed trades and net, paper and real apart."""
        now = _utc(now) if now is not None else self._now_utc()
        day_start = self._day_start(now)
        with self._lock:
            self._evaluate_all()
            paper_open = [{"origin": "PAPER", "ticket": p.ticket, "symbol": p.symbol, "side": p.side.value,
                           "volume": p.volume, "entry": p.entry, "stop": p.sl, "target": p.tp,
                           "profit": self._as_position(p).profit, "opened": p.opened.isoformat()}
                          for p in sorted(self._book.values(), key=lambda q: q.ticket)]
            touched = {d.position for d in self._deals if not d.is_entry and d.time >= day_start}
            rows = [self._row(d) for d in self._deals if d.position in touched]
        paper_today = self._by_position(rows, day_start)
        out: dict = {
            "label": LABEL, "mode": "PAPER", "magic": self.magic,
            "live_groups": sorted(self.live_groups), "day_start": day_start.isoformat(),
            "open": paper_open,
            "paper_today": {"trades": paper_today, "closed": len(paper_today),
                            "wins": sum(1 for t in paper_today if t["net"] > 0),
                            "net": round(sum(t["net"] for t in paper_today), 2),
                            "commission": round(sum(t["commission"] for t in paper_today), 2)},
            "real_today": None, "error": "",
        }
        try:
            real_open = self._real_own()
            for p in real_open:
                live = self._in_live_group(p.symbol)
                managed = live or bool(self.cfg.manage_leftovers)
                out["open"].append({"origin": "REAL", "ticket": p.ticket, "symbol": p.symbol, "side": p.side.value,
                                    "volume": p.volume, "entry": p.entry_price, "stop": p.sl, "target": p.tp,
                                    "profit": p.profit, "opened": _utc(p.open_time).isoformat(),
                                    "managed": managed,
                                    "note": ("live market: managed at the real broker" if live else
                                             "left from before the switch to paper: being wound down at the real "
                                             "broker" if managed else
                                             "left from before the switch to paper: its stop and target stay at "
                                             "the broker; nothing is sent")})
            fn = getattr(self.inner, "deals_since", None)
            real_rows = [r for r in (fn(day_start, self.magic, False) or []) if not self.is_paper(int(r.get("position") or 0))] if fn else []
            real_today = self._by_position(real_rows, day_start)
            out["real_today"] = {"trades": real_today, "closed": len(real_today),
                                 "net": round(sum(t["net"] for t in real_today), 2)}
        except Exception as exc:
            out["error"] = f"the real broker could not be read: {exc}"
        n_open = sum(1 for r in out["open"] if r["origin"] == "PAPER")
        pt = out["paper_today"]
        where = (f" Orders in {', '.join(g.lower().replace('_', ' ') for g in sorted(self.live_groups))} markets "
                 f"go to the real broker; everything else is simulated." if self.live_groups else
                 " Every order is simulated.")
        out["note"] = (f"{LABEL} is on PAPER: it decides on real prices and its orders never reach the broker."
                       f"{where} {n_open} paper trade{'s' if n_open != 1 else ''} open; {pt['closed']} closed "
                       f"today, net {pt['net']:+.2f} (paper money, never in the account).")
        return out


def real_broker(broker):
    """The REAL broker behind a :class:`PaperBroker`, or ``broker`` itself.
    The other bots (Momentum Runner, Band Breaker, Crowd Fader) must always
    be given this, never the wrapper: ``Trader._bot_kwargs`` should pass
    ``real_broker(self.broker)`` (wiring step 2 in the module notes)."""
    return broker.inner if isinstance(broker, PaperBroker) else broker
