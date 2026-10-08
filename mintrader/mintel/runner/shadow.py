"""The Momentum Runner: Trend & Breakout's index entries ridden with a 3 R
trail, in PAPER as a shadow of the real trade or in LIVE as the Runner's
own position beside it.

PAPER is pessimistic by construction and unchanged from the trial: the
shadow takes the real trade's own fill and ticket, closes AT its stop level
(never better), a buy is marked on the bid and a sell on the ask, and the
stop only ever moves in the trade's favour. Nothing in PAPER touches the
broker: the shadow's entry is the real trade's fill by definition, so no
paper order is ever simulated.

LIVE sends the Runner's OWN order through the shared executor
(``mintel.botexec``): same market, side and size as the real trade, the
real trade's ORIGINAL stop, no target, at the current price. Only a fresh
trade is entered (a short age, the price still close to the real entry):
a late entry would be a different trade at the same size, so it is refused
for good and written to the ``skipped`` table, which a restart reads back.
Every order carries the Runner's magic number and the comment RUNNER, so no
bot ever sees or touches another's positions, and the real trade is never
touched; a magic number that belongs to another bot is refused before
anything is sent. The broker holds the stop; the Runner only trails it and
closes at the window's end. The broker's own record of a closed position
(profit + commission + swap) is the figure kept for every LIVE trade; a row
written without it says so with a ``?`` on its reason. A position that is
missing from one read is not judged gone: only a closing deal, or three
reads in a row without it, ends the row, and a position found again after
an estimate reopens its row. A position carrying the Runner's magic with no
row here is closed at once and recorded as ORPHAN_CLOSED. Under PAPER or
OFF a LIVE row left open from before a switch is watched read-only: nothing
is sent, and the broker's record ends it. OFF has no executor at all:
nothing can ever be sent.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Sequence

from ..botexec import MAGIC_BANDBREAKER, MAGIC_CROWD, MAGIC_RUNNER, LiveExecutor, make_executor
from ..broker.base import Side, Tick
from ..clock import to_utc, utcnow
from ..contracts import infer_group
from . import STRATEGY_ID, STRATEGY_LABEL, TAGLINE

TAG = "RUNNER"                                  # the order comment on every LIVE order
PAPER_FIRST_TICKET = 700_000_001                # the paper executor's own sequence (unused by the shadow itself)
ORPHAN_SWEEP_SECONDS = 30.0                     # how often LIVE looks for stray positions when nothing is open
MISSES_BEFORE_ESTIMATE = 3                      # reads in a row without the position (and no deal) before a row is estimated

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    ticket INTEGER PRIMARY KEY,          -- PAPER: the real trade's ticket; LIVE: the Runner's own broker ticket
    source_ticket INTEGER,               -- the real trade this one rides (PAPER: = ticket; an orphan: 0)
    symbol TEXT, side TEXT, volume REAL,
    entry REAL, initial_stop REAL, risk_distance REAL,
    opened_utc TEXT, closed_utc TEXT,
    stop REAL, peak_price REAL, peak_r REAL,
    exit_price REAL, realised_r REAL, net_pnl REAL, commission REAL,
    exit_reason TEXT, mode TEXT, duration_seconds REAL, tactic TEXT
);
CREATE INDEX IF NOT EXISTS ix_runner_closed ON trades(closed_utc);
CREATE TABLE IF NOT EXISTS skipped (
    source_ticket INTEGER PRIMARY KEY,   -- a real trade the LIVE Runner said no to, for good
    symbol TEXT, when_utc TEXT, why TEXT
);
"""
ADDED_COLUMNS = {"mode": "TEXT", "source_ticket": "INTEGER"}     # older files gain these on open


def _foreign_magics() -> set[int]:
    """The other bots' magic numbers: Trend & Breakout's, the Rapid
    Scalper's, the Band Breaker's and the Crowd Fader's. The Runner must
    never go LIVE under one of them: it closes every position it does not
    recognise under its own number."""
    out = {MAGIC_BANDBREAKER, MAGIC_CROWD}
    try:
        from ..config import Config
        out.add(int(Config().magic))
    except Exception:
        out.add(990_311)
    try:
        from ..scalper.config import ScalperConfig
        out.add(int(ScalperConfig().magic))
    except Exception:
        out.add(990_411)
    return out


@dataclass
class RunnerConfig:
    enabled: bool = True
    mode: str = "PAPER"                 # PAPER shadows the real trade; LIVE opens its own position; OFF does nothing
    magic: int = MAGIC_RUNNER           # the Runner's own orders; never another bot's number
    trail_r: float = 3.0                # once this far ahead, trail this far behind the best price
    window_hours: float = 8.0           # out at the price after this long (the replay's window)
    markets: tuple[str, ...] = ()       # empty = every index market the main bot trades
    max_entry_age_seconds: float = 120.0    # LIVE: a real trade older than this is not entered (too late)
    max_entry_drift_r: float = 0.5          # LIVE: nor one whose price has left the real entry by more than this many R


@dataclass
class Shadow:
    ticket: int
    symbol: str
    side: Side
    volume: float
    entry: float
    initial_stop: float
    opened: dt.datetime
    tactic: str = ""
    stop: float = 0.0
    peak_price: float = 0.0
    peak_r: float = 0.0
    last_price: float = 0.0
    source_ticket: int = 0              # the real trade's ticket (PAPER: the same as ticket)
    mode: str = "PAPER"                 # how this row was opened; a LIVE row is managed at the broker
    misses: int = 0                     # LIVE: reads in a row that did not show the position (and no deal)

    @property
    def risk_distance(self) -> float:
        return abs(self.entry - self.initial_stop)

    def r_of(self, price: float) -> float:
        return ((price - self.entry) * self.side.sign) / self.risk_distance if self.risk_distance else 0.0

    def stop_reason(self) -> str:
        if not self.stop:
            return "BROKER_CLOSED"
        return "INITIAL_STOP" if abs(self.stop - self.initial_stop) < 1e-12 else "TRAIL_STOP"


class _BrokerReader:
    """A read-only view of the Runner's own positions and closed deals at the
    broker, for a LIVE row left open under PAPER or OFF. It can only read:
    nothing is ever sent through it."""

    def __init__(self, broker, magic: int):
        self._x = LiveExecutor(broker, magic, TAG)

    def positions(self):
        return self._x.positions()

    def closed_deal(self, ticket: int) -> Optional[dict]:
        return self._x.closed_deal(ticket)

    @property
    def last_error(self) -> str:
        return self._x.last_error


class MomentumRunner:
    def __init__(self, data_dir: str | Path, cfg: Optional[RunnerConfig] = None,
                 spec_fn: Optional[Callable[[str], object]] = None,
                 clock: Callable[[], dt.datetime] = utcnow, broker=None,
                 entries_allowed: Optional[Callable[[], bool]] = None, executor=None):
        self.cfg = cfg or RunnerConfig()
        self._foreign_magics = _foreign_magics()
        if self.cfg.enabled and str(self.cfg.mode or "").upper() == "LIVE" and executor is None:
            if not self._magic_is_own():
                raise ValueError(f"Runner magic {self.cfg.magic} is another bot's number (or not a positive number); "
                                 f"refusing to go LIVE")
        self.spec_fn = spec_fn or (lambda s: None)
        self.clock = clock
        self.broker = broker
        self.entries_allowed = entries_allowed or (lambda: True)
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.data_dir / "runner.sqlite"
        self.status_path = self.data_dir / "runner-status.json"
        self._lock = threading.Lock()
        self.db = sqlite3.connect(str(self.path), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self._migrate()
        self.open: dict[int, Shadow] = {}
        self._last_status = 0.0
        self.last_note = ""
        self._why = ""                              # why the last adopt() said no, for the notes
        self._noted: set[tuple] = set()             # (source ticket, note) already written, so passes do not repeat it
        self._attempted: set[int] = set()           # LIVE: real trades said no to for good (refused or too late); never hammered
        self._refused: list[dt.datetime] = []
        self._pending_notes: list[str] = []         # what the restart found, handed out on the first pass
        self.orphans: list[dict] = []
        self._last_sweep = 0.0
        self._broker_reader: Optional[_BrokerReader] = None
        if executor is not None:
            self.executor = executor
        else:
            top = self.db.execute("SELECT MAX(ticket) FROM trades").fetchone()[0]
            first = max(PAPER_FIRST_TICKET, int(top or 0) + 1)
            self.executor = make_executor(self.cfg.mode, broker, self.cfg.magic, TAG, 0.0, first) if self.cfg.enabled else None
        self._restore()

    # ------------------------------------------------------------ helpers --
    @property
    def active(self) -> bool:
        """Enabled with somewhere to send decisions. OFF (or disabled) is not."""
        return bool(self.cfg.enabled) and self.executor is not None

    @property
    def live(self) -> bool:
        return self.executor is not None and getattr(self.executor, "mode", "") == "LIVE"

    @property
    def mode(self) -> str:
        return (self.cfg.mode or "PAPER").upper() if self.active else "OFF"

    def _magic_is_own(self) -> bool:
        """A positive number that is not another bot's."""
        try:
            magic = int(self.cfg.magic or 0)
        except (TypeError, ValueError):
            return False
        return magic > 0 and magic not in self._foreign_magics

    def wants(self, symbol: str) -> bool:
        if self.cfg.markets:
            return str(symbol).upper() in {m.upper() for m in self.cfg.markets}
        return infer_group(str(symbol)) == "INDEX"

    def _migrate(self) -> None:
        """Older record files gain the columns this version writes; nothing is dropped."""
        have = {row[1] for row in self.db.execute("PRAGMA table_info(trades)")}
        for col, kind in ADDED_COLUMNS.items():
            if col not in have:
                self.db.execute(f"ALTER TABLE trades ADD COLUMN {col} {kind}")
        self.db.execute("UPDATE trades SET source_ticket=ticket WHERE source_ticket IS NULL")
        self.db.commit()

    def _shadow_from_row(self, r) -> Shadow:
        return Shadow(int(r["ticket"]), r["symbol"], Side(r["side"]), float(r["volume"]), float(r["entry"]),
                      float(r["initial_stop"]), to_utc(dt.datetime.fromisoformat(r["opened_utc"])),
                      tactic=r["tactic"] or "", stop=float(r["stop"] or 0.0), peak_price=float(r["peak_price"] or 0),
                      peak_r=float(r["peak_r"] or 0), source_ticket=int(r["source_ticket"] or r["ticket"]),
                      mode=str(r["mode"] or "PAPER").upper())

    def _restore(self) -> None:
        self._attempted = {int(r[0]) for r in self.db.execute("SELECT source_ticket FROM skipped")}
        for r in self.db.execute("SELECT * FROM trades WHERE closed_utc IS NULL"):
            s = self._shadow_from_row(r)
            self.open[s.ticket] = s
        if self.broker is not None and (self.live or self._unmanaged_live()):
            self._reconcile_start()

    def _save(self, s: Shadow) -> None:
        with self._lock:
            self.db.execute("""INSERT INTO trades (ticket, source_ticket, symbol, side, volume, entry, initial_stop,
                               risk_distance, opened_utc, stop, peak_price, peak_r, mode, tactic)
                               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                               ON CONFLICT(ticket) DO UPDATE SET stop=excluded.stop, peak_price=excluded.peak_price,
                               peak_r=excluded.peak_r""",
                            (s.ticket, s.source_ticket, s.symbol, s.side.value, s.volume, s.entry, s.initial_stop,
                             s.risk_distance, s.opened.isoformat(), s.stop, s.peak_price, s.peak_r, s.mode, s.tactic))
            self.db.commit()

    def _spec(self, symbol: str):
        try:
            return self.spec_fn(symbol)
        except Exception:
            return None

    def _broker_tick(self, symbol: str) -> Optional[Tick]:
        try:
            return self.broker.tick(symbol) if self.broker is not None else None
        except Exception:
            return None

    def _shadowing(self, source_ticket: int) -> bool:
        return any(s.ticket == source_ticket or s.source_ticket == source_ticket for s in self.open.values())

    def _unmanaged_live(self) -> bool:
        """A LIVE row still open while this run is PAPER or OFF (opened
        before a switch): watched read-only, never managed here."""
        return not self.live and any(s.mode == "LIVE" for s in self.open.values())

    def _reader(self):
        """What reads the broker: the LIVE executor, or a read-only view of
        the Runner's magic for a LIVE row left open under PAPER or OFF."""
        if self.live:
            return self.executor
        if self.broker is None or not self._magic_is_own():
            return None                                  # never read under another bot's number either
        if self._broker_reader is None:
            self._broker_reader = _BrokerReader(self.broker, int(self.cfg.magic))
        return self._broker_reader

    def _refused_today(self, now: dt.datetime) -> int:
        day = to_utc(now).date()
        self._refused = [t for t in self._refused if t.date() >= day]
        return sum(1 for t in self._refused if t.date() == day)

    def _skipped_today(self, now: dt.datetime) -> int:
        start = to_utc(now).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        try:
            return int(self.db.execute("SELECT COUNT(*) FROM skipped WHERE when_utc >= ?", (start,)).fetchone()[0])
        except Exception:
            return 0

    def _skip(self, source_ticket: int, symbol: str, why: str, now: dt.datetime) -> None:
        """Say no to a real trade for good, on record: a restart reads it back
        and never retries it."""
        self._attempted.add(int(source_ticket))
        with self._lock:
            self.db.execute("INSERT OR REPLACE INTO skipped (source_ticket, symbol, when_utc, why) VALUES (?,?,?,?)",
                            (int(source_ticket), symbol, to_utc(now).isoformat(), why))
            self.db.commit()

    # --------------------------------------------------------------- flow --
    def adopt(self, position, initial_stop: float, now: dt.datetime, tactic: str = "",
              tick: Optional[Tick] = None) -> Optional[Shadow]:
        """Ride a real trade, once. PAPER starts a shadow of it; LIVE sends the
        Runner's own order at ``tick``. Returns the row, or None (``_why`` says why)."""
        self._why = ""
        if not self.active or not self.wants(position.symbol):
            return None
        src = int(position.ticket)
        if self._shadowing(src):
            return None
        if self.live and src in self._attempted:
            return None                                  # said no for good (refused, or too late): never hammered
        if self.db.execute("SELECT 1 FROM trades WHERE ticket=? OR source_ticket=?", (src, src)).fetchone():
            return None                                  # already finished with this one
        stop = float(initial_stop or position.sl or 0.0)
        if stop <= 0 or abs(position.entry_price - stop) <= 0:
            return None
        if not self.entries_allowed():
            self._why = self.last_note = "entries paused by the account gate"
            return None
        if self.live:
            return self._adopt_live(position, stop, now, tactic, tick)
        s = Shadow(src, position.symbol, position.side, float(position.volume),
                   float(position.entry_price), stop, to_utc(position.open_time or now), tactic=tactic, stop=stop,
                   source_ticket=src, mode="PAPER")
        self.open[s.ticket] = s
        self._save(s)
        return s

    def _too_late(self, position, side: Side, stop: float, now: dt.datetime, tick: Optional[Tick]) -> str:
        """Only a fresh entry is like for like with the real trade. Past a
        short age, or once the price has left the real entry by more than a
        fraction of its risk, the Runner would be buying a different trade at
        the same size (a multiple of the money at risk, or a hair-trigger
        stop), so it says no for good. Empty when the entry is fine."""
        opened = getattr(position, "open_time", None)
        age = 0.0
        if isinstance(opened, dt.datetime) and opened.tzinfo is not None:
            age = (to_utc(now) - to_utc(opened)).total_seconds()
        if age > float(self.cfg.max_entry_age_seconds):
            return (f"too late to enter: real trade {position.ticket} is {age / 60:.1f} min old "
                    f"(limit {float(self.cfg.max_entry_age_seconds) / 60:.1f} min)")
        if tick is None:
            return ""
        touch = float(tick.ask if side is Side.BUY else tick.bid)
        entry = float(position.entry_price)
        risk = abs(entry - stop)
        drift = abs(touch - entry)
        if risk > 0 and drift > float(self.cfg.max_entry_drift_r) * risk:
            return (f"too late to enter: the price {touch} is {drift / risk:.2f} R from the real entry {entry} "
                    f"(limit {float(self.cfg.max_entry_drift_r)} R)")
        if (touch - stop) * side.sign <= 0:
            return (f"too late to enter: the stop {stop} is not {'below' if side is Side.BUY else 'above'} "
                    f"the price {touch}")
        return ""

    def _adopt_live(self, position, stop: float, now: dt.datetime, tactic: str, tick: Optional[Tick]) -> Optional[Shadow]:
        src = int(position.ticket)
        side = Side(position.side)
        late = self._too_late(position, side, stop, now, tick)
        if late:
            self._skip(src, position.symbol, late, now)
            self._why = self.last_note = late
            return None
        spec = self._spec(position.symbol)
        if tick is None or spec is None or not hasattr(spec, "normalise_price"):
            self._why = f"no price or contract details for {position.symbol}; not entering yet"
            return None                                  # not an attempt: tried again next pass (while still fresh)
        fill = self.executor.open(position.symbol, side, float(position.volume), stop, 0.0, tick, spec,
                                  to_utc(now), comment=TAG)
        if not fill.ok:
            self._refused.append(to_utc(now))
            self._why = self.last_note = f"order refused: {fill.message}"
            self._skip(src, position.symbol, self._why, now)
            return None
        s = Shadow(int(fill.ticket), position.symbol, side, float(fill.volume or position.volume), float(fill.price),
                   stop, to_utc(now), tactic=tactic, stop=stop, source_ticket=src, mode="LIVE")
        self.open[s.ticket] = s
        self._save(s)
        self.last_note = f"{s.symbol} {s.side.value} {s.volume} opened at {s.entry} (stop {s.stop}) beside real trade {src}"
        return s

    def _trail(self, s: Shadow, price: float) -> Optional[float]:
        """The best price is noted; past +trail_r the stop wants to sit trail_r
        behind it. Returns the new stop when it would improve, else None."""
        sign = s.side.sign
        if (price - s.peak_price) * sign > 0 or s.peak_price == 0.0:
            s.peak_price = price
            s.peak_r = max(s.peak_r, s.r_of(price))
        if s.peak_r < self.cfg.trail_r:
            return None
        cand = s.peak_price - sign * self.cfg.trail_r * s.risk_distance
        if (cand - s.stop) * sign <= 0:                   # only ever in the trade's favour
            return None
        spec = self._spec(s.symbol)
        if spec is not None and hasattr(spec, "normalise_price"):
            cand = spec.normalise_price(cand)
        return cand if (cand - s.stop) * sign > 0 else None

    def update(self, s: Shadow, bid: float, ask: float, now: dt.datetime) -> Optional[str]:
        """One PAPER price update. Returns the exit reason when the shadow closes."""
        sign = s.side.sign
        price = bid if s.side is Side.BUY else ask            # what we could close at
        s.last_price = price
        # the stop first: a touch closes AT the stop, never better
        if (price - s.stop) * sign <= 0:
            return self._close(s, s.stop, s.stop_reason(), now)
        if (now - s.opened).total_seconds() >= self.cfg.window_hours * 3600:
            return self._close(s, price, "WINDOW_END", now)
        cand = self._trail(s, price)
        if cand is not None:
            s.stop = cand
        self._save(s)
        return None

    def _update_live(self, s: Shadow, tick: Tick, now: dt.datetime) -> Optional[str]:
        """One LIVE pass for a position that is still at the broker. The
        broker holds the stop; here only the window and the trail."""
        price = float(tick.bid if s.side is Side.BUY else tick.ask)
        s.last_price = price
        spec = self._spec(s.symbol)
        if (now - s.opened).total_seconds() >= self.cfg.window_hours * 3600:
            fill = self.executor.close(s.ticket, tick, spec, comment=f"{TAG} window")
            if not fill.ok:
                self.last_note = f"{s.symbol}: close refused ({fill.message}); keeping the position and retrying next pass"
                self._why = self.last_note
                return None
            deal = self.executor.closed_deal(s.ticket)
            if deal and deal.get("pnl") is not None:
                return self._close(s, float(deal.get("exit_price") or fill.price), "WINDOW_END", now, net=float(deal["pnl"]))
            return self._close(s, float(fill.price), "WINDOW_END?", now)      # estimated from the fill: say so
        cand = self._trail(s, price)
        if cand is not None:
            if self.executor.modify_stop(s.ticket, cand):
                s.stop = cand
            else:                                            # the stop stays where the broker has it; tried again next pass
                self._why = self.last_note = (f"{s.symbol}: stop move to {cand} refused "
                                              f"({getattr(self.executor, 'last_error', '') or 'no reason given'}); will retry")
        self._save(s)
        return None

    def _finalise_from_broker(self, s: Shadow, tick: Optional[Tick], now: dt.datetime) -> str:
        """The position is not in the broker's list. With its closing deal,
        the broker's record is the figure. Without one it is not judged gone
        yet (a read can come back empty when the terminal hiccups): the row
        is kept and counted, and only after MISSES_BEFORE_ESTIMATE reads in a
        row is it estimated at the last mark and marked so. Returns the exit
        reason when the row was closed, else an empty string."""
        reason = s.stop_reason()
        deal = self._reader().closed_deal(s.ticket)
        if deal and deal.get("pnl") is not None:
            s.misses = 0
            exit_price = float(deal.get("exit_price") or s.stop or s.last_price)
            return self._close(s, exit_price, reason, now, net=float(deal["pnl"]))
        s.misses += 1
        if s.misses < MISSES_BEFORE_ESTIMATE:
            return ""
        if tick is not None:
            mark = float(tick.bid if s.side is Side.BUY else tick.ask)
        else:
            mark = float(s.last_price or s.stop)
        return self._close(s, mark, reason + "?", now)

    def _close(self, s: Shadow, exit_price: float, reason: str, now: dt.datetime, net: Optional[float] = None) -> str:
        """Write the row's end. ``net`` is the broker's own figure when there is
        one (LIVE); otherwise the spec's money for the move (PAPER, estimates)."""
        commission = 0.0                                   # indices: none at this broker; LIVE: inside the broker's figure
        if net is None:
            spec = self._spec(s.symbol)
            move = (exit_price - s.entry) * s.side.sign
            net = float(spec.money(move, s.volume)) if spec is not None and hasattr(spec, "money") else 0.0
        with self._lock:
            self.db.execute("""UPDATE trades SET closed_utc=?, stop=?, peak_price=?, peak_r=?, exit_price=?,
                               realised_r=?, net_pnl=?, commission=?, exit_reason=?, duration_seconds=? WHERE ticket=?""",
                            (to_utc(now).isoformat(), s.stop, s.peak_price, s.peak_r, exit_price,
                             round(s.r_of(exit_price), 4), round(net - commission, 2), commission, reason,
                             (to_utc(now) - s.opened).total_seconds(), s.ticket))
            self.db.commit()
        self.open.pop(s.ticket, None)
        return reason

    def _reopen(self, row, p, now: dt.datetime) -> str:
        """A row closed on an estimate whose position is still at the broker:
        the estimate was wrong. The row is reopened and managed on; the
        broker's stop is the truth."""
        old = str(row["exit_reason"] or "")
        ticket = int(row["ticket"])
        with self._lock:
            self.db.execute("""UPDATE trades SET closed_utc=NULL, exit_price=NULL, realised_r=NULL, net_pnl=NULL,
                               commission=NULL, exit_reason=NULL, duration_seconds=NULL WHERE ticket=?""", (ticket,))
            self.db.commit()
        s = self._shadow_from_row(self.db.execute("SELECT * FROM trades WHERE ticket=?", (ticket,)).fetchone())
        if float(getattr(p, "sl", 0.0) or 0.0):
            s.stop = float(p.sl)
        self.open[s.ticket] = s
        self._save(s)
        self._noted.discard((s.ticket, "missing"))
        return (f"{STRATEGY_LABEL}: {s.symbol} ticket {s.ticket} is still at the broker; its row (closed {old}) "
                f"was an estimate, reopened and managed on")

    # --------------------------------------------------------- the broker --
    def _live_positions(self, now: dt.datetime) -> Optional[dict]:
        """The Runner's own positions at the broker, by ticket; None when the
        broker could not be read, or nothing needed reading (then nothing is
        judged gone). Under PAPER or OFF only a LIVE row left open is reason
        to read, and the read-only view does it."""
        rd = self._reader()
        if rd is None:
            return None
        if not self.live:
            if not self._unmanaged_live():
                return None
        elif not self.open and now.timestamp() - self._last_sweep < ORPHAN_SWEEP_SECONDS:
            return None
        self._last_sweep = now.timestamp()
        return {p.ticket: p for p in rd.positions()}

    def _stray(self, p, now: dt.datetime) -> str:
        """A position carrying the Runner's magic that is not open here. One
        with a row in the records was closed on an estimate: reopened. One
        never on record is an orphan: closed at once."""
        row = self.db.execute("SELECT * FROM trades WHERE ticket=?", (int(p.ticket),)).fetchone()
        if row is not None:
            return self._reopen(row, p, now)
        return self._close_orphan(p, now)

    def _close_orphan(self, p, now: dt.datetime) -> str:
        """A position carrying the Runner's magic that has never been on
        record is closed at once, never adopted (an unknown position is not
        evidence), and the close is recorded as ORPHAN_CLOSED with the
        broker's figure, so the money is still in the book."""
        t = self._broker_tick(p.symbol) or Tick(p.symbol, to_utc(now), float(p.entry_price), float(p.entry_price))
        fill = self.executor.close(p.ticket, t, self._spec(p.symbol), comment=f"{TAG} orphan")
        rec = {"ticket": int(p.ticket), "symbol": p.symbol, "side": getattr(p.side, "value", str(p.side)),
               "volume": float(p.volume), "when": to_utc(now).isoformat(),
               "result": "orphan closed" if fill.ok else f"orphan close refused: {fill.message}"}
        self.orphans.append(rec)
        head = f"{STRATEGY_LABEL}: {p.symbol} ticket {p.ticket} {rec['result']}"
        if not fill.ok:
            return head + " (still at the broker with its stop; tried again next pass)"
        opened = p.open_time if isinstance(getattr(p, "open_time", None), dt.datetime) and p.open_time.tzinfo else now
        s = Shadow(int(p.ticket), p.symbol, Side(p.side), float(p.volume), float(p.entry_price), float(p.sl or 0.0),
                   to_utc(opened), stop=float(p.sl or 0.0), source_ticket=0, mode="LIVE")
        self._save(s)
        deal = self.executor.closed_deal(p.ticket)
        if deal and deal.get("pnl") is not None:
            reason = self._close(s, float(deal.get("exit_price") or fill.price), "ORPHAN_CLOSED", now, net=float(deal["pnl"]))
        else:
            reason = self._close(s, float(fill.price), "ORPHAN_CLOSED?", now)
        row = self.db.execute("SELECT exit_price, net_pnl FROM trades WHERE ticket=?", (int(p.ticket),)).fetchone()
        rec["net_pnl"] = float(row["net_pnl"] or 0.0)
        return head + f"; recorded {reason} at {row['exit_price']}: {rec['net_pnl']:+.2f}"

    def _reconcile_start(self) -> None:
        """After a restart with LIVE rows (or in LIVE): rows the broker closed
        while the bot was down are finalised from its record; in LIVE, stray
        positions with the Runner's magic are reopened (when on record) or
        closed. A broker that cannot be read is tried again on the first
        pass, and nothing is judged gone until it answers. Under PAPER or OFF
        nothing is sent."""
        now = to_utc(self.clock())
        try:
            live = {p.ticket: p for p in self._reader().positions()}
        except Exception as exc:
            self._pending_notes.append(f"{STRATEGY_LABEL}: could not read the broker's positions at start: {exc}")
            return
        self._last_sweep = now.timestamp()
        for s in list(self.open.values()):
            if s.mode == "LIVE" and s.ticket not in live:
                reason = self._finalise_from_broker(s, self._broker_tick(s.symbol), now)
                if reason:
                    self._pending_notes.append(f"{STRATEGY_LABEL}: {s.symbol} ticket {s.ticket} was closed at the broker "
                                               f"while the bot was down: {reason}")
                else:
                    self._pending_notes.append(f"{STRATEGY_LABEL}: {s.symbol} ticket {s.ticket} is not at the broker and "
                                               f"has no closing deal yet; kept and checked again on the next passes")
        if self.live:
            for ticket, p in live.items():
                if ticket not in self.open:
                    self._pending_notes.append(self._stray(p, now))

    def _watch_live_row(self, s: Shadow, t: Optional[Tick], live: Optional[dict], now: dt.datetime) -> list[str]:
        """A LIVE row under PAPER or OFF: its stop is at the broker and nothing
        is sent. The broker's record ends it; until then the status says so."""
        notes: list[str] = []
        self.last_note = f"{s.symbol} ticket {s.ticket} was opened LIVE and is not managed in {self.mode}; its stop is at the broker"
        key = (s.ticket, "not managed")
        if key not in self._noted:
            self._noted.add(key)
            notes.append(f"{STRATEGY_LABEL}: {self.last_note}")
        if t is not None:
            s.last_price = float(t.bid if s.side is Side.BUY else t.ask)
        if live is None:
            return notes
        if s.ticket in live:
            s.misses = 0
            return notes
        reason = self._finalise_from_broker(s, t, now)
        if reason:
            self.last_note = f"{s.symbol} ticket {s.ticket} closed at the broker {reason} (from its record; nothing was sent)"
            notes.append(f"{STRATEGY_LABEL}: {self.last_note}")
        return notes

    def _watch_unmanaged(self, tick_fn: Callable[[str], object], now: dt.datetime) -> list[str]:
        """OFF (no executor) with a LIVE row still open: watched read-only."""
        notes: list[str] = []
        if not self._unmanaged_live():
            return notes
        live = None
        try:
            live = self._live_positions(now)
        except Exception as exc:
            notes.append(f"{STRATEGY_LABEL}: could not read the broker's positions: {exc}")
        for s in list(self.open.values()):
            if s.mode != "LIVE":
                continue
            try:
                t = tick_fn(s.symbol)
            except Exception:
                t = None
            try:
                notes.extend(self._watch_live_row(s, t, live, now))
            except Exception as exc:
                notes.append(f"{STRATEGY_LABEL}: {s.symbol} error {exc}")
        return notes

    # --------------------------------------------------------------- pass --
    def observe(self, positions: Sequence, initial_stops: dict, tick_fn: Callable[[str], object],
                now: dt.datetime, tactics: Optional[dict] = None) -> list[str]:
        """One pass: ride new index trades, then move every open one on."""
        notes: list[str] = []
        if self._pending_notes:
            notes.extend(self._pending_notes)
            self._pending_notes = []
        if not self.active:
            notes.extend(self._watch_unmanaged(tick_fn, now))
            return notes
        for p in positions:
            try:
                tick = tick_fn(p.symbol) if self.live and self.wants(p.symbol) else None
                s = self.adopt(p, float(initial_stops.get(p.ticket) or p.sl or 0.0), now,
                               tactic=str((tactics or {}).get(p.ticket) or ""), tick=tick)
                if s is not None:
                    self._noted = {k for k in self._noted if k[0] != int(p.ticket)}
                    if s.mode == "LIVE":
                        notes.append(f"{STRATEGY_LABEL}: opened {s.symbol} {s.side.value} {s.volume} at {s.entry} "
                                     f"(stop {s.stop}) beside real trade {s.source_ticket}")
                    else:
                        notes.append(f"{STRATEGY_LABEL}: shadowing {s.symbol} {s.side.value} from {s.entry} (stop {s.stop})")
                elif self._why and (int(p.ticket), self._why) not in self._noted:
                    self._noted.add((int(p.ticket), self._why))
                    notes.append(f"{STRATEGY_LABEL}: {p.symbol} not entered: {self._why}")
            except Exception as exc:                       # never let the Runner disturb the real bot
                notes.append(f"{STRATEGY_LABEL}: could not ride {getattr(p, 'symbol', '?')}: {exc}")
        live: Optional[dict] = None
        if self.live or self._unmanaged_live():
            try:
                live = self._live_positions(now)
            except Exception as exc:
                notes.append(f"{STRATEGY_LABEL}: could not read the broker's positions: {exc}")
                live = None
        known = set(self.open)                             # on the books as this pass starts (a row closed below is not a stray)
        for s in list(self.open.values()):
            try:
                t = tick_fn(s.symbol)
                if s.mode == "LIVE":
                    if not self.live:
                        notes.extend(self._watch_live_row(s, t, live, now))
                        continue
                    if live is None:
                        continue                           # the broker did not answer: nothing is judged gone
                    if s.ticket not in live:
                        reason = self._finalise_from_broker(s, t, now)
                        if reason:
                            notes.append(f"{STRATEGY_LABEL}: {s.symbol} closed at the broker {reason}")
                        elif (s.ticket, "missing") not in self._noted:
                            self._noted.add((s.ticket, "missing"))
                            notes.append(f"{STRATEGY_LABEL}: {s.symbol} ticket {s.ticket} is not in the broker's list and "
                                         f"has no closing deal yet; kept and checked again")
                        continue
                    s.misses = 0
                    self._noted.discard((s.ticket, "missing"))
                    if t is None:
                        continue
                    self._why = ""
                    reason = self._update_live(s, t, now)
                    if reason:
                        notes.append(f"{STRATEGY_LABEL}: {s.symbol} closed {reason} at the broker")
                    elif self._why and (s.ticket, self._why) not in self._noted:
                        self._noted.add((s.ticket, self._why))       # a refusal is said once, not every pass
                        notes.append(f"{STRATEGY_LABEL}: {self._why}")
                    continue
                if t is None:
                    continue
                reason = self.update(s, float(t.bid), float(t.ask), now)
                if reason:
                    notes.append(f"{STRATEGY_LABEL}: {s.symbol} shadow closed {reason} at {s.stop if 'STOP' in reason else s.last_price}")
            except Exception as exc:
                notes.append(f"{STRATEGY_LABEL}: {s.symbol} error {exc}")
        if self.live and live is not None:
            for ticket, p in live.items():
                if ticket not in self.open and ticket not in known:
                    try:
                        notes.append(self._stray(p, now))
                    except Exception as exc:
                        notes.append(f"{STRATEGY_LABEL}: could not deal with stray position {ticket}: {exc}")
        self.write_status(now)
        return notes

    # ------------------------------------------------------------- status --
    def closed_since(self, since: dt.datetime) -> list[dict]:
        rows = self.db.execute("SELECT * FROM trades WHERE closed_utc IS NOT NULL AND closed_utc >= ? "
                               "ORDER BY closed_utc ASC", (to_utc(since).isoformat(),)).fetchall()
        return [dict(r) for r in rows]

    def skipped_since(self, since: dt.datetime) -> list[dict]:
        """The real trades the LIVE Runner said no to for good, and why."""
        rows = self.db.execute("SELECT * FROM skipped WHERE when_utc >= ? ORDER BY when_utc ASC",
                               (to_utc(since).isoformat(),)).fetchall()
        return [dict(r) for r in rows]

    def open_rows(self) -> list[dict]:
        out = []
        for s in self.open.values():
            spec = self._spec(s.symbol)
            pnl = (float(spec.money((s.last_price - s.entry) * s.side.sign, s.volume))
                   if spec is not None and s.last_price and hasattr(spec, "money") else 0.0)
            out.append({"ticket": s.ticket, "source_ticket": s.source_ticket, "symbol": s.symbol, "side": s.side.value,
                        "entry": s.entry, "stop": s.stop, "initial_stop": s.initial_stop, "peak_r": round(s.peak_r, 2),
                        "r_now": round(s.r_of(s.last_price), 2) if s.last_price else 0.0, "pnl": round(pnl, 2),
                        "opened": s.opened.isoformat(), "trailing": s.peak_r >= self.cfg.trail_r, "mode": s.mode})
        return out

    def status(self, now: Optional[dt.datetime] = None) -> dict:
        now = to_utc(now or self.clock())
        unmanaged = [{"ticket": s.ticket, "symbol": s.symbol, "side": s.side.value, "mode": s.mode,
                      "note": f"opened LIVE, not managed in {self.mode}; its stop is at the broker"}
                     for s in self.open.values() if s.mode == "LIVE" and not self.live]
        return {"strategy_id": STRATEGY_ID, "label": STRATEGY_LABEL, "tagline": TAGLINE,
                "mode": self.mode, "live": self.live, "magic": int(self.cfg.magic),
                "status": ("IN TRADE" if self.open else "WATCHING") if self.active else "OFF",
                "executor_error": str(getattr(self.executor, "last_error", "") or ""),
                "refused": self._refused_today(now), "skipped": self._skipped_today(now), "note": self.last_note,
                "orphans": list(self.orphans[-10:]), "unmanaged": unmanaged,
                "updated": now.isoformat(), "trail_r": self.cfg.trail_r, "window_hours": self.cfg.window_hours,
                "markets": list(self.cfg.markets) or "every index market the main bot trades",
                "open": self.open_rows()}

    def write_status(self, now: dt.datetime, every_seconds: float = 2.0) -> None:
        t = now.timestamp()
        if t - self._last_status < every_seconds:
            return
        self._last_status = t
        try:
            tmp = self.status_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.status(now), indent=2, default=str))
            tmp.replace(self.status_path)
        except Exception:
            pass

    def close(self) -> None:
        try:
            self.db.close()
        except Exception:
            pass
