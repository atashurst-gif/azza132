"""Band Breaker engine: the noise band, the half-hour checks, the positions.

Runs inside the main bot's loop (like the Momentum Runner), one ``step`` per
pass. It fetches bars through callables so it can be tested on made-up
sessions. The engine decides; the shared executor (mintel/botexec.py) is the
only place its decisions touch the broker.

PAPER is pessimistic by construction: fills at the touch plus slippage, a
stop closes AT the stop level, a buy is marked on the bid and a sell on the
ask. Nothing in PAPER touches the broker's orders. LIVE sends real orders
carrying the Band Breaker's own magic number with the stop at the broker
(fail closed: no stop, no position), and the broker's own record of every
closed trade (profit + commission + swap) is the figure that goes in the
book. OFF sends nothing at all.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
import statistics as st
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional, Sequence

from ..botexec import MAGIC_BANDBREAKER, make_executor
from ..broker.base import Position, Side, TF, Tick
from ..clock import TZ_NEWYORK, to_utc, utcnow
from . import STRATEGY_ID, STRATEGY_LABEL, TAGLINE

TAG = "BB"                               # the order comment on every live order
MISSING_PASSES = 3                       # a ticket absent from this many broker reads in a row ...
MISSING_SECONDS = 60.0                   # ... over at least this long, with no deal, is booked as an estimate

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    ticket INTEGER PRIMARY KEY, symbol TEXT, side TEXT, volume REAL,
    entry REAL, initial_stop REAL, opened_utc TEXT, closed_utc TEXT,
    stop REAL, exit_price REAL, realised_r REAL, net_pnl REAL, commission REAL,
    exit_reason TEXT, mode TEXT, duration_seconds REAL,
    session_date TEXT, band_upper REAL, band_lower REAL, vwap_at_entry REAL, sigma REAL,
    day_open REAL, prev_close REAL, peak_r REAL
);
CREATE INDEX IF NOT EXISTS ix_bb_closed ON trades(closed_utc);
CREATE TABLE IF NOT EXISTS checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, symbol TEXT, check_ny TEXT,
    close REAL, band_upper REAL, band_lower REAL, vwap REAL, sigma REAL, decision TEXT
);
"""


@dataclass
class BandBreakerConfig:
    enabled: bool = True
    mode: str = "PAPER"                  # PAPER simulates; LIVE places its own orders; OFF does nothing
    magic: int = MAGIC_BANDBREAKER       # the Band Breaker's own orders, never another bot's number
    markets: tuple[str, ...] = ("US500", "US30", "USTEC")
    lookback_days: int = 14              # sessions in the noise average
    check_minutes: int = 30              # entries only at these closes
    session_open: str = "09:30"          # New York cash hours
    session_close: str = "16:00"
    first_check: str = "10:00"           # the first half-hour close that may enter
    last_entry: str = "15:30"
    flat_before_close_minutes: int = 5   # everything closed by 15:55
    max_entries_per_day: int = 3         # per market, counting reversals
    risk_money: float = 10.0             # money at the initial stop
    paper_slippage_points: float = 1.0


@dataclass
class Bands:
    session_date: dt.date
    day_open: float
    prev_close: float
    sigma: dict                          # "HH:MM" (NY) -> average |move from open|
    sessions_used: int

    def at(self, hhmm: str) -> tuple[float, float, float]:
        s = float(self.sigma.get(hhmm, 0.0))
        return max(self.day_open, self.prev_close) * (1 + s), min(self.day_open, self.prev_close) * (1 - s), s


@dataclass
class Trade:
    """One open position, paper or live. In LIVE ``ticket`` is the broker's."""
    ticket: int
    symbol: str
    side: Side
    volume: float
    entry: float
    initial_stop: float
    opened: dt.datetime
    session_date: str
    stop: float = 0.0
    peak_r: float = 0.0
    last_price: float = 0.0
    mode: str = "PAPER"
    broker_pnl: Optional[float] = None   # the broker's running figure while open (LIVE)
    missing_passes: int = 0              # LIVE: passes in a row the broker's list has not shown it
    missing_since: Optional[dt.datetime] = None
    meta: dict = field(default_factory=dict)

    def r_of(self, price: float) -> float:
        d = abs(self.entry - self.initial_stop)
        return ((price - self.entry) * self.side.sign) / d if d else 0.0


Paper = Trade                            # the old name


def _hhmm(t: str) -> dt.time:
    h, m = t.split(":")
    return dt.time(int(h), int(m))


def ny(ts: dt.datetime) -> dt.datetime:
    return to_utc(ts).astimezone(TZ_NEWYORK)


class BandBreaker:
    def __init__(self, data_dir: str | Path, cfg: Optional[BandBreakerConfig] = None,
                 spec_fn: Optional[Callable[[str], object]] = None,
                 clock: Callable[[], dt.datetime] = utcnow,
                 broker=None, entries_allowed: Optional[Callable[[], bool]] = None, executor=None):
        self.cfg = cfg or BandBreakerConfig()
        self.spec_fn = spec_fn or (lambda s: None)
        self.clock = clock
        self.broker = broker
        self.entries_allowed = entries_allowed or (lambda: True)
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.status_path = self.data_dir / "bandbreaker-status.json"
        self._lock = threading.Lock()
        self.db = sqlite3.connect(str(self.data_dir / "bandbreaker.sqlite"), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self._ensure_columns()
        self.open: dict[str, Trade] = {}            # one position per market
        self.bands: dict[str, Bands] = {}
        self.entries_today: dict[tuple[str, str], int] = {}
        self.refused: dict[str, int] = {}           # NY date -> orders the broker refused
        self.unmanaged: list[dict] = []             # open rows this mode cannot manage (see _restore)
        self.last_minute: dict[str, str] = {}
        self.last_check: dict[str, str] = {}
        self.notes_by_market: dict[str, str] = {}
        self._pending: list[str] = []               # notes from the restart, given out on the first pass
        self._last_status = 0.0
        self._last_sweep = ""                       # the minute the orphan sweep last ran (LIVE)
        self._sweep_due = False                     # an order whose outcome is unknown: sweep before any entry
        # paper tickets continue the paper sequence already on record so they never collide;
        # LIVE rows carry the broker's own numbers and must never pull the paper range into them
        base = max(700_000_000, int(self.db.execute(
            "SELECT COALESCE(MAX(ticket), 700000000) FROM trades WHERE mode IS NULL OR mode='PAPER'").fetchone()[0]))
        if not self.cfg.enabled:
            self.executor = None
        elif executor is not None:
            self.executor = executor
        else:
            self.executor = make_executor(self.cfg.mode, broker, self.cfg.magic, TAG,
                                          self.cfg.paper_slippage_points, base + 1)
        self._restore()

    def _ensure_columns(self) -> None:
        cols = {r[1] for r in self.db.execute("PRAGMA table_info(trades)")}
        if "mode" not in cols:
            self.db.execute("ALTER TABLE trades ADD COLUMN mode TEXT")
            self.db.commit()

    @property
    def active(self) -> bool:
        return self.executor is not None

    @property
    def live(self) -> bool:
        return self.executor is not None and getattr(self.executor, "mode", "") == "LIVE"

    @property
    def mode(self) -> str:
        return self.executor.mode if self.executor is not None else "OFF"

    # ---------------------------------------------------------- the session --
    def in_session(self, now: dt.datetime) -> bool:
        n = ny(now)
        return n.weekday() < 5 and _hhmm(self.cfg.session_open) <= n.time() < _hhmm(self.cfg.session_close)

    def flat_time(self) -> dt.time:
        c = _hhmm(self.cfg.session_close)
        m = c.hour * 60 + c.minute - self.cfg.flat_before_close_minutes
        return dt.time(m // 60, m % 60)

    def compute_bands(self, bars: Sequence, today: dt.date) -> Optional[Bands]:
        """From M30 bars (UTC open times): today's open, yesterday's close and
        the average absolute move from the open at each half hour, over the
        last ``lookback_days`` complete sessions before today."""
        open_t = _hhmm(self.cfg.session_open)
        close_t = _hhmm(self.cfg.session_close)
        days: dict[dt.date, dict] = {}
        for b in bars:
            n = ny(b.time)
            if n.weekday() >= 5:
                continue
            d = days.setdefault(n.date(), {"open": None, "closes": {}, "last_close": None})
            t = n.time()
            if t == open_t:
                d["open"] = float(b.open)
            if open_t <= t < close_t:
                end = (n + dt.timedelta(minutes=self.cfg.check_minutes)).time()
                d["closes"][end.strftime("%H:%M")] = float(b.close)
                d["last_close"] = float(b.close)
        if today not in days or days[today]["open"] is None:
            return None
        prior = [d for d in sorted(days) if d < today and days[d]["open"] and days[d]["closes"]]
        if len(prior) < 3:
            return None
        used = prior[-self.cfg.lookback_days:]
        sigma: dict[str, float] = {}
        keys = set()
        for d in used:
            keys |= set(days[d]["closes"])
        for k in sorted(keys):
            moves = [abs(days[d]["closes"][k] / days[d]["open"] - 1.0) for d in used if k in days[d]["closes"]]
            if moves:
                sigma[k] = st.mean(moves)
        prev_close = days[prior[-1]]["last_close"]
        return Bands(today, float(days[today]["open"]), float(prev_close), sigma, len(used))

    @staticmethod
    def vwap(m1_bars: Sequence, session_date: dt.date, open_t: dt.time) -> float:
        num = den = 0.0
        for b in m1_bars:
            n = ny(b.time)
            if n.date() != session_date or n.time() < open_t:
                continue
            v = float(getattr(b, "tick_volume", 0.0) or 1.0)
            num += (b.high + b.low + b.close) / 3.0 * v
            den += v
        return num / den if den else 0.0

    # ----------------------------------------------------------- the step --
    def step(self, now: dt.datetime, tick_fn: Callable[[str], object],
             bars_fn: Callable[[str, TF, int], Sequence]) -> list[str]:
        """One pass. Cheap unless a minute or a half hour has just turned."""
        notes: list[str] = list(self._pending)
        self._pending.clear()
        if not self.active:
            self.write_status(now)
            return notes
        n = ny(now)
        minute_key = n.strftime("%Y-%m-%d %H:%M")
        # LIVE, once a minute (at once after an order whose outcome is unknown): one
        # read of the broker's list serves the whole pass; a position with our magic
        # and no row here is closed and booked, never left to run unmanaged until
        # the next restart
        at_broker: Optional[dict[int, Position]] = None
        if self.live and (self._sweep_due or self._last_sweep != minute_key):
            self._last_sweep = minute_key
            self._sweep_due = False
            try:
                at_broker = {q.ticket: q for q in self.executor.positions()}
            except Exception as exc:
                notes.append(f"{STRATEGY_LABEL}: could not read the broker's positions ({exc}); the sweep runs again next minute")
            else:
                notes.extend(self._sweep_orphans(now, at_broker))
        for sym in self.cfg.markets:
            try:
                notes.extend(self._step_market(sym, now, n, minute_key, tick_fn, bars_fn, at_broker))
            except Exception as exc:                       # one market's trouble never stops the others
                notes.append(f"{STRATEGY_LABEL}: {sym} skipped this pass ({exc})")
        self.write_status(now)
        return notes

    def _step_market(self, sym: str, now: dt.datetime, n: dt.datetime, minute_key: str,
                     tick_fn, bars_fn, at_broker: Optional[dict] = None) -> list[str]:
        notes: list[str] = []
        pos = self.open.get(sym)
        tick = tick_fn(sym)
        if tick is None:
            return notes
        bid, ask = float(tick.bid), float(tick.ask)
        try:
            age = (to_utc(now) - to_utc(tick.time)).total_seconds()
        except Exception:
            age = 0.0
        if ask <= bid or age > 120.0:
            self.notes_by_market[sym] = "no usable price (stale or crossed quote)"
            return notes
        # every tick: a stop is a stop
        if pos is not None:
            price = bid if pos.side is Side.BUY else ask
            pos.last_price = price
            pos.peak_r = max(pos.peak_r, pos.r_of(price))
            if self.live:
                # the broker holds the stop; what we do is notice when it has closed us
                note = self._reconcile(pos, tick, now, at_broker)
                if note:
                    notes.append(note)
            elif (price - pos.stop) * pos.side.sign <= 0:
                notes.append(self._close(pos, tick, "BAND_STOP" if abs(pos.stop - pos.initial_stop) < 1e-9 else "TRAIL_STOP",
                                         now, level=pos.stop))
            pos = self.open.get(sym)
        if not self.in_session(now):
            if pos is not None:
                notes.append(self._close(pos, tick, "SESSION_CLOSE", now))
            self.notes_by_market[sym] = "outside the New York session"
            return notes
        if pos is not None and n.time() >= self.flat_time():
            notes.append(self._close(pos, tick, "FLAT_BEFORE_CLOSE", now))
            pos = self.open.get(sym)
        # once a minute: bands, VWAP, the trailing stop
        if self.last_minute.get(sym) == minute_key:
            return notes
        self.last_minute[sym] = minute_key
        today = n.date()
        bands = self.bands.get(sym)
        if bands is None or bands.session_date != today:
            bands = self.compute_bands(bars_fn(sym, TF.M30, 48 * (self.cfg.lookback_days + 6)), today)
            if bands is None:
                self.notes_by_market[sym] = "waiting for today's open and enough history"
                return notes
            self.bands[sym] = bands
        tmin = n.replace(second=0, microsecond=0)              # decisions are made on the minute
        hhmm = self._latest_check(n)
        if hhmm is None:
            self.notes_by_market[sym] = f"first check at {self.cfg.first_check} New York"
            return notes
        ub, lb, sigma = bands.at(hhmm)
        vw = self.vwap(bars_fn(sym, TF.M1, 420), today, _hhmm(self.cfg.session_open))
        if pos is not None:
            cand = max(lb, vw) if pos.side is Side.BUY else min(ub, vw) if vw > 0 else ub
            if pos.side is Side.BUY and vw <= 0:
                cand = lb
            if (cand - pos.stop) * pos.side.sign > 0 and (pos.last_price - cand) * pos.side.sign > 0:
                notes.extend(self._trail(pos, self._norm(sym, cand)))
        self.notes_by_market[sym] = (f"band {lb:.2f} - {ub:.2f} (noise {sigma:.2%} by {hhmm} NY), VWAP {vw:.2f}, "
                                     f"price {(bid + ask) / 2:.2f}")
        # at a half-hour close: the decision, once per boundary, never lost to a
        # slow pass, never taken more than a few minutes late
        check_key = f"{today.isoformat()} {hhmm}"
        if self.last_check.get(sym) == check_key:
            return notes
        boundary = _hhmm(hhmm)
        if boundary < _hhmm(self.cfg.first_check) or boundary > _hhmm(self.cfg.last_entry):
            return notes
        self.last_check[sym] = check_key                       # the check is spent, whatever happens next
        late = (tmin.hour * 60 + tmin.minute) - (boundary.hour * 60 + boundary.minute)
        if late > 5:
            self._record_check(sym, now, hhmm, None, ub, lb, vw, sigma, f"missed: first seen {late} min after the close")
            return notes
        m30 = bars_fn(sym, TF.M30, 4)
        closed = [b for b in m30 if ny(b.time).date() < today or ny(b.time).time() < boundary]
        if not closed:
            self._record_check(sym, now, hhmm, None, ub, lb, vw, sigma, "no completed half-hour bar yet")
            return notes
        close = float(closed[-1].close)
        want = Side.BUY if close > ub else (Side.SELL if close < lb else None)
        key = (sym, today.isoformat())
        decision = "inside the band: noise, no trade"
        try:
            if want is None:
                pass
            elif pos is not None and pos.side is want:
                decision = f"already {want.value.lower()}"
            elif self.entries_today.get(key, 0) >= self.cfg.max_entries_per_day:
                decision = "signal, but no more entries today"
                self.notes_by_market[sym] += "; no more entries today"
            else:
                if pos is not None:                            # a close through the other band: out first
                    notes.append(self._close(pos, tick, "REVERSAL", now))
                    pos = self.open.get(sym)
                if pos is not None:                            # the close was refused: never two positions in one market
                    decision = f"reversal signal, but the close was refused: {self._executor_error() or 'no reason given'}"
                elif not self._may_enter():
                    decision = "signal, but entries paused by the account gate"
                    self.notes_by_market[sym] = "entries paused by the account gate"
                else:
                    stop = (max(lb, vw) if vw > 0 else lb) if want is Side.BUY else (min(ub, vw) if vw > 0 else ub)
                    mark = bid if want is Side.BUY else ask        # the stop must clear the price we would be closed at
                    if (mark - stop) * want.sign <= 0:
                        stop = lb if want is Side.BUY else ub
                    if (mark - stop) * want.sign <= 0:
                        decision = "signal, but no room for a stop"
                        self.notes_by_market[sym] += "; no room for a stop"
                    else:
                        ok, msg, decision = self._open(sym, want, tick, stop, now, today,
                                                       {"band_upper": ub, "band_lower": lb, "vwap_at_entry": vw, "sigma": sigma,
                                                        "day_open": bands.day_open, "prev_close": bands.prev_close})
                        if ok:
                            self.entries_today[key] = self.entries_today.get(key, 0) + 1
                        notes.append(msg)
        finally:
            self._record_check(sym, now, hhmm, close, ub, lb, vw, sigma, decision)
        return notes

    def _may_enter(self) -> bool:
        """The account-level gate shared with Trend & Breakout. Fails closed."""
        try:
            return bool(self.entries_allowed())
        except Exception:
            return False

    def _executor_error(self) -> str:
        return str(getattr(self.executor, "last_error", "") or "")

    def _record_check(self, sym: str, now: dt.datetime, hhmm: str, close, ub: float, lb: float,
                      vw: float, sigma: float, decision: str) -> None:
        try:
            with self._lock:
                self.db.execute("INSERT INTO checks (ts_utc, symbol, check_ny, close, band_upper, band_lower, vwap, sigma, decision) "
                                "VALUES (?,?,?,?,?,?,?,?,?)",
                                (to_utc(now).isoformat(), sym, hhmm, close, ub, lb, vw, sigma, decision))
                self.db.commit()
        except Exception:
            pass

    def checks_since(self, since: dt.datetime) -> list[dict]:
        return [dict(r) for r in self.db.execute(
            "SELECT * FROM checks WHERE ts_utc >= ? ORDER BY ts_utc ASC", (to_utc(since).isoformat(),))]

    def _latest_check(self, n: dt.datetime) -> Optional[str]:
        first = _hhmm(self.cfg.first_check)
        m = (n.hour * 60 + n.minute) // self.cfg.check_minutes * self.cfg.check_minutes
        t = dt.time(m // 60, m % 60)
        if t < first:
            return None
        return t.strftime("%H:%M")

    def _norm(self, sym: str, price: float) -> float:
        spec = self.spec_fn(sym)
        return spec.normalise_price(price) if spec is not None and hasattr(spec, "normalise_price") else price

    # ------------------------------------------------------------ positions --
    def _open(self, sym: str, side: Side, tick, stop: float, now: dt.datetime,
              today: dt.date, meta: dict) -> tuple[bool, str, str]:
        """Size at the stop and send the order through the executor. Returns
        (entered, the note for the log, the decision for the checks table)."""
        spec = self.spec_fn(sym)
        if spec is None:
            return False, f"{STRATEGY_LABEL}: {sym} has no contract specification", "signal, but no contract specification"
        point = float(getattr(spec, "point", 0.0) or 0.0)
        touch = float(tick.ask if side is Side.BUY else tick.bid)
        # sized on the touch plus the paper slippage in both modes: in PAPER that is
        # the fill; in LIVE it is a cautious guess at it, the broker's price is the entry
        plan = spec.normalise_price(touch + side.sign * self.cfg.paper_slippage_points * point)
        stop = spec.normalise_price(stop)
        dist = abs(plan - stop)
        per_lot = spec.money_per_lot(dist)
        if per_lot <= 0:
            return False, f"{STRATEGY_LABEL}: {sym} cannot value the stop", "signal, but cannot value the stop"
        volume = spec.normalise_volume(self.cfg.risk_money / per_lot)
        if volume < getattr(spec, "volume_min", 0.01):
            return (False, f"{STRATEGY_LABEL}: {sym} skipped - the smallest size would risk {per_lot * spec.volume_min:.2f}, "
                           f"over {self.cfg.risk_money:.0f}", "signal, but the smallest size would risk too much")
        day = ny(now).date().isoformat()
        try:
            fill = self.executor.open(sym, side, volume, stop, 0.0, tick, spec, now, comment=TAG)
        except Exception as exc:
            # the order may or may not be at the broker: count it as refused, say so
            # plainly, and sweep the broker's list before any entry is considered
            self.refused[day] = self.refused.get(day, 0) + 1
            self._sweep_due = True
            return (False, f"{STRATEGY_LABEL}: {sym} order sent, state unknown: {exc}; the broker's list is checked next pass",
                    f"order sent, state unknown: {exc}")
        if not fill.ok:
            self.refused[day] = self.refused.get(day, 0) + 1
            return False, f"{STRATEGY_LABEL}: {sym} order refused: {fill.message}", f"order refused: {fill.message}"
        p = Trade(int(fill.ticket), sym, side, float(fill.volume or volume), float(fill.price), float(stop), to_utc(now),
                  today.isoformat(), stop=float(stop), last_price=touch, mode=self.executor.mode, meta=meta)
        try:
            self._save(p, new=True)
        except sqlite3.IntegrityError:
            # a ticket already on record: fail closed rather than carry a position with no row of its own
            self.refused[day] = self.refused.get(day, 0) + 1
            undo = self.executor.close(p.ticket, tick, spec, comment=f"{TAG} dup ticket")
            if not undo.ok:
                self._sweep_due = True                         # still at the broker with its stop: the sweep takes it
            what = "closed again at once" if undo.ok else f"close refused ({undo.message}); the sweep will take it"
            return (False, f"{STRATEGY_LABEL}: {sym} order refused: ticket {p.ticket} already on record, {what}",
                    f"order refused: ticket {p.ticket} already on record")
        self.open[sym] = p
        decision = "entered " + side.value.lower() + (f" (live, ticket {p.ticket})" if self.live else "")
        note = (f"{STRATEGY_LABEL}: {sym} {side.value} {p.volume:g} lots at {p.entry} stop {stop} "
                f"(risk {per_lot * p.volume:.2f}, band {meta['band_lower']:.2f}-{meta['band_upper']:.2f})")
        if self.live:
            note += f", live ticket {p.ticket}"
        return True, note, decision

    def _trail(self, p: Trade, new_stop: float) -> list[str]:
        """Re-set the stop, only ever in the trade's favour. In LIVE the broker
        must take it; if it refuses, the engine's stop stays where the broker's
        is and the next minute tries again."""
        old = p.stop
        if self.executor.modify_stop(p.ticket, new_stop):
            p.stop = new_stop
            self._save(p)
            return []
        p.stop = old
        return [f"{STRATEGY_LABEL}: {p.symbol} stop move to {new_stop} refused "
                f"({self._executor_error() or 'no reason given'}); stays at {old}, will retry"]

    def _close(self, p: Trade, tick, reason: str, now: dt.datetime, level: Optional[float] = None) -> str:
        """The engine's own exit. PAPER: at the stop level when one is given
        (never better), else at the mark less slippage. LIVE: a market close at
        the broker; the broker's deal is the figure. A refused close keeps the
        position and is tried again next pass."""
        spec = self.spec_fn(p.symbol)
        at = float(level) if level is not None else None
        if spec is None and at is None:
            at = float(tick.bid if p.side is Side.BUY else tick.ask)
        fill = self.executor.close(p.ticket, tick, spec, at_price=at, comment=f"{TAG} {reason}"[:31])
        if not fill.ok:
            return (f"{STRATEGY_LABEL}: {p.symbol} {p.side.value} close ({reason}) refused: "
                    f"{fill.message or self._executor_error() or 'no reason given'}; still open, will retry")
        exit_px = float(fill.price)
        net = float(spec.money((exit_px - p.entry) * p.side.sign, p.volume)) if spec is not None else 0.0
        closed_at = to_utc(now)
        if self.live:
            exit_px, net, when = self._deal_figures(p, fill, now)
            if when is None:
                reason += "?"                                  # estimated from the fill: the broker's record was not there yet
            else:
                closed_at = when
        self._finalise(p, exit_px, net, reason, closed_at)
        return f"{STRATEGY_LABEL}: {p.symbol} {p.side.value} closed {reason} at {exit_px}: {net:+.2f}"

    def _reconcile(self, p: Trade, tick, now: dt.datetime, mine: Optional[dict] = None) -> str:
        """LIVE, every pass: is the position still at the broker? If not, the
        broker closed it (the stop) and its record is the figure. ``mine`` is
        this pass's read of the broker's list when the sweep already took one."""
        if mine is None:
            mine = {q.ticket: q for q in self.executor.positions()}
        q = mine.get(p.ticket)
        if q is not None:
            p.broker_pnl = float(getattr(q, "profit", 0.0) or 0.0) + float(getattr(q, "swap", 0.0) or 0.0)
            note = ""
            if p.missing_passes:
                note = f"{STRATEGY_LABEL}: {p.symbol} ticket {p.ticket} is back in the broker's list; managed on"
                p.missing_passes, p.missing_since = 0, None
            better = self._adopt_broker_stop(p, q)
            return note or better
        return self._finalise_from_broker(p, tick, now)

    def _adopt_broker_stop(self, p: Trade, q: Position) -> str:
        """The broker's stop is the stop. One that is better for the trade than
        the engine's (tightened by hand, or trailed before a restart that lost
        the save) is adopted, so the trail can only ever move it in favour."""
        sl = float(getattr(q, "sl", 0.0) or 0.0)
        if sl and (sl - p.stop) * p.side.sign > 0:
            old = p.stop
            p.stop = sl
            self._save(p)
            return f"{STRATEGY_LABEL}: {p.symbol} the broker holds a better stop ({sl} vs {old}): adopted"
        return ""

    def _finalise_from_broker(self, p: Trade, tick, now: dt.datetime) -> str:
        """The ticket is not in the broker's list. With the broker's closing
        deal the row is closed ONCE from it. Without one the position is kept
        and checked again (one empty read is not a close: the adapter answers
        an error with an empty list); only after MISSING_PASSES reads in a row
        over MISSING_SECONDS is it booked at the last mark with a '?' on the
        reason, so the review can see it was estimated."""
        spec = self.spec_fn(p.symbol)
        if tick is not None:
            mark = float(tick.bid if p.side is Side.BUY else tick.ask)
        else:
            mark = float(p.stop or p.entry)                   # no price at all: the stop is the pessimistic guess
        deal = self.executor.closed_deal(p.ticket)
        if deal:
            exit_px = float(deal.get("exit_price") or mark)
            net = float(deal["pnl"])
            reason = self._broker_reason(p, deal.get("reason"))
            self._finalise(p, exit_px, net, reason, self._deal_time(deal, now))
            return f"{STRATEGY_LABEL}: {p.symbol} {p.side.value} closed by the broker ({reason}) at {exit_px}: {net:+.2f}"
        p.missing_passes += 1
        if p.missing_since is None:
            p.missing_since = to_utc(now)
        absent = (to_utc(now) - p.missing_since).total_seconds()
        if p.missing_passes < MISSING_PASSES or absent < MISSING_SECONDS:
            if p.missing_passes == 1:
                return (f"{STRATEGY_LABEL}: {p.symbol} ticket {p.ticket} not in the broker's list and no closing deal yet; "
                        "kept and checked again")
            return ""
        exit_px = mark
        net = float(spec.money((exit_px - p.entry) * p.side.sign, p.volume)) if spec is not None else 0.0
        reason = self._broker_reason(p, "") + "?"
        self._finalise(p, exit_px, net, reason, to_utc(now))
        return (f"{STRATEGY_LABEL}: {p.symbol} {p.side.value} gone from the broker for {absent:.0f} s with no deal record; "
                f"booked {reason} at the last mark {exit_px}: {net:+.2f} (estimated)")

    @staticmethod
    def _broker_reason(p: Trade, hint) -> str:
        """Why the broker closed it: its own word when it gives one (MT5 writes
        '[sl ...]' or '[tp ...]' on the closing deal); any other word is not a
        stop; with no word at all, the level the exit must have been (the Band
        Breaker only ever has a stop)."""
        h = str(hint or "").strip().lower()
        if "sl" in h or "stop" in h:
            return "STOP"
        if "tp" in h or "take" in h:
            return "TARGET"
        if h:
            return "MANUAL" if "manual" in h else "BROKER_CLOSED"
        return "STOP" if p.stop else "BROKER_CLOSED"

    @staticmethod
    def _deal_time(deal: dict, now: dt.datetime) -> dt.datetime:
        t = deal.get("time")
        try:
            return to_utc(t) if isinstance(t, dt.datetime) else to_utc(now)
        except Exception:
            return to_utc(now)

    def _finalise(self, p: Trade, exit_px: float, net: float, reason: str, closed_at: dt.datetime) -> None:
        # commission stays 0 in the row: in LIVE the broker's net already holds profit + commission + swap
        with self._lock:
            self.db.execute("""UPDATE trades SET closed_utc=?, stop=?, exit_price=?, realised_r=?, net_pnl=?, commission=0,
                               exit_reason=?, duration_seconds=?, peak_r=? WHERE ticket=?""",
                            (closed_at.isoformat(), p.stop, exit_px, round(p.r_of(exit_px), 4), round(net, 2), reason,
                             (closed_at - p.opened).total_seconds(), round(p.peak_r, 4), p.ticket))
            self.db.commit()
        if self.open.get(p.symbol) is p:                       # only this trade, never another one on the same market
            self.open.pop(p.symbol)

    def _save(self, p: Trade, new: bool = False) -> None:
        with self._lock:
            if new:
                self.db.execute("""INSERT INTO trades (ticket, symbol, side, volume, entry, initial_stop, opened_utc, stop, mode,
                                   session_date, band_upper, band_lower, vwap_at_entry, sigma, day_open, prev_close, peak_r)
                                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)""",
                                (p.ticket, p.symbol, p.side.value, p.volume, p.entry, p.initial_stop, p.opened.isoformat(),
                                 p.stop, p.mode, p.session_date, p.meta.get("band_upper"), p.meta.get("band_lower"),
                                 p.meta.get("vwap_at_entry"), p.meta.get("sigma"), p.meta.get("day_open"), p.meta.get("prev_close")))
            else:
                self.db.execute("UPDATE trades SET stop=?, peak_r=? WHERE ticket=?", (p.stop, round(p.peak_r, 4), p.ticket))
            self.db.commit()

    # -------------------------------------------------------------- restart --
    def _broker_tick(self, sym: str):
        try:
            return self.broker.tick(sym) if self.broker is not None else None
        except Exception:
            return None

    def _restore(self) -> None:
        """Open rows come back as positions. PAPER rows are adopted by the paper
        executor. LIVE rows are checked against the broker: one still there is
        managed on; one gone is finalised from the broker's deal. A position at
        the broker with this bot's magic and no row here is closed at once
        (fail closed; never adopted). A row from the other mode cannot be
        managed by this one and is listed as unmanaged, never touched."""
        now = to_utc(self.clock())
        for sym, day, n in self.db.execute("SELECT symbol, session_date, COUNT(*) FROM trades GROUP BY symbol, session_date"):
            if sym and day:
                self.entries_today[(sym, day)] = int(n)
        self._restore_checks(now)
        rows = [dict(r) for r in self.db.execute("SELECT * FROM trades WHERE closed_utc IS NULL ORDER BY opened_utc")]
        at_broker: Optional[dict[int, Position]] = None
        if self.live:
            try:
                at_broker = {q.ticket: q for q in self.executor.positions()}
            except Exception as exc:
                self._pending.append(f"{STRATEGY_LABEL}: could not read the broker's positions at start ({exc}); "
                                     "open rows are kept and checked on the first pass")
        for r in rows:
            p = self._trade_from_row(r)
            if not self.active or p.mode != self.mode:
                self._leave_unmanaged(p, f"opened in {p.mode}, this run is {self.mode}: not managed here",
                                      f"was opened in {p.mode} and this run is {self.mode}: left as it is, close it by hand or switch back")
                continue
            if p.symbol in self.open:                          # never two positions in one market
                self._pending.append(self._settle_extra(p, at_broker, now))
                continue
            if self.live:
                self.open[p.symbol] = p                        # managed on; taken out again below if the broker closed it
                if at_broker is None:
                    continue
                q = at_broker.get(p.ticket)
                if q is None:
                    self._pending.append(self._finalise_from_broker(p, self._broker_tick(p.symbol), now))
                else:
                    note = self._adopt_broker_stop(p, q)
                    if note:
                        self._pending.append(note)
            else:
                self.executor.adopt(Position(p.ticket, p.symbol, p.side, p.volume, p.entry, p.stop, 0.0, p.opened, comment=TAG))
                self.open[p.symbol] = p
        if self.live and at_broker is not None:
            self._pending.extend(self._sweep_orphans(now, at_broker))

    def _restore_checks(self, now: dt.datetime) -> None:
        """The half-hour checks already taken today come back from the checks
        table, so a restart minutes after a close cannot run the same check
        (and send the same order) twice."""
        today = ny(now).date()
        try:
            rows = self.db.execute("SELECT symbol, check_ny, MAX(ts_utc) AS ts FROM checks GROUP BY symbol").fetchall()
        except Exception:
            return
        for r in rows:
            try:
                if ny(dt.datetime.fromisoformat(r["ts"])).date() == today:
                    self.last_check[r["symbol"]] = f"{today.isoformat()} {r['check_ny']}"
            except Exception:
                continue

    @staticmethod
    def _trade_from_row(r: dict) -> Trade:
        return Trade(int(r["ticket"]), r["symbol"], Side(r["side"]), float(r["volume"]), float(r["entry"]),
                     float(r["initial_stop"]), to_utc(dt.datetime.fromisoformat(r["opened_utc"])), r["session_date"] or "",
                     stop=float(r["stop"] or 0.0), peak_r=float(r["peak_r"] or 0), mode=str(r.get("mode") or "PAPER").upper())

    def _leave_unmanaged(self, p: Trade, why: str, note: str) -> None:
        self.unmanaged.append({"ticket": p.ticket, "symbol": p.symbol, "side": p.side.value, "mode": p.mode, "note": why})
        self._pending.append(f"{STRATEGY_LABEL}: ticket {p.ticket} {p.symbol} {p.side.value} {note}")

    def _settle_extra(self, p: Trade, at_broker: Optional[dict], now: dt.datetime) -> str:
        """A second open row on a market that already has one (the record of an
        earlier fault). LIVE and still at the broker: closed now and booked
        with the broker's figure. Gone from the broker: booked from its deal.
        Anything else is left as it is and listed as unmanaged."""
        head = f"{STRATEGY_LABEL}: ticket {p.ticket} is a second open row for {p.symbol} (ticket {self.open[p.symbol].ticket} is managed)"
        if self.live and at_broker is not None:
            q = at_broker.get(p.ticket)
            if q is not None:
                tick = self._broker_tick(p.symbol) or Tick(p.symbol, now, float(q.entry_price), float(q.entry_price))
                fill = self.executor.close(p.ticket, tick, self.spec_fn(p.symbol), comment=f"{TAG} second position")
                if fill.ok:
                    exit_px, net, when = self._deal_figures(p, fill, now)
                    self._finalise(p, exit_px, net, "DUPLICATE_CLOSED" if when is not None else "DUPLICATE_CLOSED?", when or now)
                    return head + f": closed at the broker at {exit_px}: {net:+.2f}"
                self._leave_unmanaged(p, "a second open row for this market; still at the broker, close it by hand",
                                      f"is a second open row for {p.symbol}; the close was refused ({fill.message}), close it by hand")
                return head + f": close refused ({fill.message}); still at the broker with its stop, close it by hand"
            deal = self.executor.closed_deal(p.ticket)
            if deal:
                exit_px = float(deal.get("exit_price") or p.stop or p.entry)
                reason = self._broker_reason(p, deal.get("reason"))
                self._finalise(p, exit_px, float(deal["pnl"]), reason, self._deal_time(deal, now))
                return head + f": closed by the broker ({reason}) at {exit_px}: {float(deal['pnl']):+.2f}"
        self._leave_unmanaged(p, "a second open row for this market: not managed here",
                              f"is a second open row for {p.symbol}: left as it is, check it by hand")
        return head + ": left as it is, check it by hand"

    def _deal_figures(self, p: Trade, fill, now: dt.datetime) -> tuple[float, float, Optional[dt.datetime]]:
        """After our own close at the broker: the deal's exit, net and time, or
        the fill price valued through the contract (time None) when the deal
        has not come back after the executor's retries."""
        spec = self.spec_fn(p.symbol)
        deal = self.executor.closed_deal(p.ticket)
        if deal:
            return float(deal.get("exit_price") or fill.price), float(deal["pnl"]), self._deal_time(deal, now)
        exit_px = float(fill.price)
        net = float(spec.money((exit_px - p.entry) * p.side.sign, p.volume)) if spec is not None else 0.0
        return exit_px, net, None

    def _sweep_orphans(self, now: dt.datetime, at_broker: dict) -> list[str]:
        """Every position at the broker with our magic that no open row here
        accounts for is closed at once and booked (fail closed; never adopted).
        Run at restart and once a minute in LIVE, on the pass's read of the list."""
        known = {p.ticket for p in self.open.values()}
        return [self._close_orphan(q, now) for q in at_broker.values() if q.ticket not in known]

    def _close_orphan(self, q: Position, now: dt.datetime) -> str:
        """A position with our magic that no open row accounts for: close it
        now and keep the broker's figure, so the money is still in the book. A
        row already carrying that ticket (an earlier estimate, or a row the
        engine lost track of) gets the broker's figure instead of a new row."""
        tick = self._broker_tick(q.symbol) or Tick(q.symbol, now, float(q.entry_price), float(q.entry_price))
        spec = self.spec_fn(q.symbol)
        fill = self.executor.close(q.ticket, tick, spec, comment=f"{TAG} orphan")
        head = f"{STRATEGY_LABEL}: orphan closed - ticket {q.ticket} {q.symbol} {q.side.value} {q.volume:g} lots at the broker with no record here"
        if not fill.ok:
            return head + f" (close refused: {fill.message}; still at the broker with its stop, close it by hand)"
        day = ny(now).date().isoformat()
        p = Trade(int(q.ticket), q.symbol, q.side, float(q.volume), float(q.entry_price), float(q.sl or 0.0),
                  to_utc(q.open_time) if isinstance(q.open_time, dt.datetime) else now, day,
                  stop=float(q.sl or 0.0), mode="LIVE")
        try:
            self._save(p, new=True)
            self.entries_today[(q.symbol, day)] = self.entries_today.get((q.symbol, day), 0) + 1
        except sqlite3.IntegrityError:
            row = self.db.execute("SELECT * FROM trades WHERE ticket=?", (q.ticket,)).fetchone()
            row = dict(row) if row is not None else {}
            if str(row.get("mode") or "PAPER").upper() != "LIVE":
                exit_px, net, _ = self._deal_figures(p, fill, now)
                return head + (f"; closed at {exit_px}: {net:+.2f}, but a PAPER row already holds ticket number {q.ticket}, "
                               "so the figure is only in this note")
            was = str(row.get("exit_reason") or "open")
            p = self._trade_from_row(row)
            head += f" (its row, booked {was}, now holds the broker's figure)"
        exit_px, net, when = self._deal_figures(p, fill, now)
        self._finalise(p, exit_px, net, "ORPHAN_CLOSED" if when is not None else "ORPHAN_CLOSED?", when or now)
        return head + f"; closed at {exit_px}: {net:+.2f}"

    # --------------------------------------------------------------- status --
    def closed_since(self, since: dt.datetime) -> list[dict]:
        return [dict(r) for r in self.db.execute(
            "SELECT * FROM trades WHERE closed_utc IS NOT NULL AND closed_utc >= ? ORDER BY closed_utc ASC",
            (to_utc(since).isoformat(),))]

    def open_rows(self) -> list[dict]:
        out = []
        for p in self.open.values():
            spec = self.spec_fn(p.symbol)
            if p.broker_pnl is not None:
                pnl = p.broker_pnl                              # the broker's running figure
            else:
                pnl = float(spec.money((p.last_price - p.entry) * p.side.sign, p.volume)) if spec is not None and p.last_price else 0.0
            out.append({"ticket": p.ticket, "symbol": p.symbol, "side": p.side.value, "entry": p.entry, "stop": p.stop,
                        "initial_stop": p.initial_stop, "r_now": round(p.r_of(p.last_price), 2) if p.last_price else 0.0,
                        "peak_r": round(p.peak_r, 2), "pnl": round(pnl, 2), "opened": p.opened.isoformat(), "mode": p.mode})
        return out

    def status(self, now: Optional[dt.datetime] = None) -> dict:
        now = to_utc(now or self.clock())
        today_ny = ny(now).date().isoformat()
        return {"strategy_id": STRATEGY_ID, "label": STRATEGY_LABEL, "tagline": TAGLINE,
                "mode": self.mode, "live": self.live, "magic": int(self.cfg.magic),
                "executor_error": self._executor_error(), "refused": int(self.refused.get(today_ny, 0)),
                "status": ("OFF" if not self.active else "IN TRADE" if self.open else
                           ("WATCHING" if self.in_session(now) else "OUTSIDE SESSION")),
                "updated": now.isoformat(), "markets": list(self.cfg.markets),
                "session": f"{self.cfg.session_open}-{self.cfg.session_close} New York, flat by {self.flat_time().strftime('%H:%M')}",
                "rule": f"average noise of the last {self.cfg.lookback_days} sessions; checks every {self.cfg.check_minutes} min "
                        f"from {self.cfg.first_check}; at most {self.cfg.max_entries_per_day} entries a day per market; "
                        f"{self.cfg.risk_money:.0f} at the stop",
                "markets_now": dict(self.notes_by_market), "open": self.open_rows(), "unmanaged": list(self.unmanaged),
                "bands_today": {s: {"session": b.session_date.isoformat(), "open": b.day_open, "prev_close": b.prev_close,
                                    "sessions_used": b.sessions_used, "noise_10:00": round(b.sigma.get("10:00", 0.0), 5),
                                    "noise_15:30": round(b.sigma.get("15:30", 0.0), 5)} for s, b in self.bands.items()},
                "checks_today": self.checks_since(now.replace(hour=0, minute=0, second=0, microsecond=0))[-24:]}

    def write_status(self, now: dt.datetime, every_seconds: float = 2.0) -> None:
        t = to_utc(now).timestamp()
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
