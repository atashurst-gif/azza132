"""Crowd Fader engine: reads positioning, waits for the price to turn, keeps
positions with a structural stop and a liquidation-cluster target.

Runs inside the main bot's loop like the Band Breaker, one ``step`` per
pass. The Coinversa calls happen on their own thread so the trading loop
never waits on the internet; a step only ever reads the latest result.
The engine decides; the shared executor (mintel/botexec.py) is the only
thing that touches a position. PAPER is pessimistic by construction: fills
at the touch plus slippage, a stop closes AT the stop, a target AT the
target, a buy is marked on the bid and a sell on the ask. LIVE sends the
bot's own orders (magic 990711, comment CROWD) with the stop AND the target
at the broker, and the broker's own record of every closed position
(profit + commission + swap) is the figure. OFF has no executor at all.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import sqlite3
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from ..botexec import MAGIC_CROWD, make_executor
from ..broker.base import Position, Side, TF, Tick
from ..clock import to_utc, utcnow
from . import STRATEGY_ID, STRATEGY_LABEL, TAGLINE
from .client import CoinversaClient, CoinversaError, read_api_key
from .signal import Read, summarise

log = logging.getLogger("mintel.crowd")

TAG = "CROWD"                                   # the order comment; the magic is cfg.magic (990711)
PAPER_FIRST_TICKET = 800_000_001                # paper tickets start here and continue the records
MISSING_PASSES = 3                              # a ticket absent from this many broker reads in a row ...
MISSING_SECONDS = 60.0                          # ... over at least this long, with no closing deal, is booked as an estimate
ESTIMATE_WINDOW_HOURS = 24.0                    # a row booked with a '?' this recently is re-read from the broker's history

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    ticket INTEGER PRIMARY KEY, symbol TEXT, coin TEXT, side TEXT, volume REAL,
    entry REAL, initial_stop REAL, target REAL, opened_utc TEXT, closed_utc TEXT,
    stop REAL, exit_price REAL, realised_r REAL, net_pnl REAL, commission REAL,
    exit_reason TEXT, mode TEXT, duration_seconds REAL, session_date TEXT,
    score REAL, crowd_bias REAL, winners_bias REAL, fuel_share REAL, read_ts TEXT, reason TEXT, peak_r REAL,
    risk_money REAL
);
CREATE INDEX IF NOT EXISTS ix_cf_closed ON trades(closed_utc);
CREATE TABLE IF NOT EXISTS checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, symbol TEXT, coin TEXT, price REAL,
    crowd_long INTEGER, crowd_short INTEGER, crowd_bias REAL, crowd_bias_money REAL,
    winners_bias REAL, winners_bias_count REAL, fuel_below REAL, fuel_above REAL,
    cluster_below REAL, cluster_above REAL, direction INTEGER, score REAL, reason TEXT
);
CREATE INDEX IF NOT EXISTS ix_cf_checks ON checks(symbol, ts_utc);
"""

CHECK_COLUMNS = ("ts_utc", "symbol", "coin", "price", "crowd_long", "crowd_short", "crowd_bias", "crowd_bias_money",
                 "winners_bias", "winners_bias_count", "fuel_below", "fuel_above", "cluster_below", "cluster_above",
                 "direction", "score", "reason")


@dataclass
class CrowdConfig:
    enabled: bool = True
    mode: str = "PAPER"                 # PAPER simulates; LIVE places its own orders (magic below); OFF does nothing
    magic: int = MAGIC_CROWD            # the Crowd Fader's own orders; never another bot's number
    # "BROKER_SYMBOL=coinversa coin"; any the broker lacks is skipped
    markets: tuple[str, ...] = ("XAUUSD=xyz:GOLD", "XAGUSD=xyz:SILVER", "US500=xyz:SP500", "JP225=xyz:JP225",
                                "EURUSD=xyz:EUR", "GBPUSD=xyz:GBP", "XTIUSD=xyz:CL", "XBRUSD=xyz:BRENTOIL")
    poll_minutes: float = 10.0
    stretch: float = 0.5                # crowd net bias by wallet count (0.5 = 75% one way)
    agree_cap: float = 0.1              # the winners may lean at most this far WITH the crowd
    min_score: float = 60.0
    min_wallets: int = 200
    heatmap_range_pct: float = 3.0
    heatmap_buckets: int = 20
    trigger_bars: int = 8               # the trigger close must be beyond the last N M15 closes
    stop_bars: int = 4                  # the stop sits beyond the last N M15 bars (an hour)
    min_stop_pct: float = 0.15
    max_stop_pct: float = 1.5
    target_r: float = 2.0
    cluster_min_r: float = 1.0
    cluster_max_r: float = 4.0
    hold_hours: float = 24.0
    max_entries_per_day: int = 2
    risk_money: float = 10.0
    max_risk_money: float = 30.0        # the smallest size the broker allows may risk up to this
    paper_slippage_points: float = 1.0
    entry_hours_utc: tuple[int, ...] = (7, 20)
    weekend_flat_utc: str = "20:30"     # Friday: everything closed, no new entries after

    def mapping(self) -> dict[str, str]:
        out: dict[str, str] = {}
        for m in self.markets:
            if "=" in str(m):
                sym, coin = str(m).split("=", 1)
                if sym.strip() and coin.strip():
                    out[sym.strip()] = coin.strip()
        return out


@dataclass
class Trade:
    ticket: int
    symbol: str
    coin: str
    side: Side
    volume: float
    entry: float
    initial_stop: float
    target: float
    opened: dt.datetime
    session_date: str
    stop: float = 0.0
    peak_r: float = 0.0
    last_price: float = 0.0
    mode: str = "PAPER"
    broker_pnl: Optional[float] = None  # LIVE: the broker's running figure for the open position
    missing_passes: int = 0             # LIVE: broker reads in a row that did not show it (and no closing deal)
    missing_since: Optional[dt.datetime] = None
    meta: dict = field(default_factory=dict)

    def r_of(self, price: float) -> float:
        d = abs(self.entry - self.initial_stop)
        return ((price - self.entry) * self.side.sign) / d if d else 0.0


Paper = Trade                           # the old name


def _hhmm(t: str) -> dt.time:
    h, m = str(t).split(":")
    return dt.time(int(h), int(m))


class CrowdFader:
    def __init__(self, data_dir: str | Path, cfg: Optional[CrowdConfig] = None,
                 client: Optional[CoinversaClient] = None, spec_fn: Optional[Callable[[str], object]] = None,
                 clock: Callable[[], dt.datetime] = utcnow, threaded: bool = True, api_key: Optional[str] = None,
                 broker=None, entries_allowed: Optional[Callable[[], bool]] = None, executor=None):
        self.cfg = cfg or CrowdConfig()
        self.spec_fn = spec_fn or (lambda s: None)
        self.clock = clock
        self.threaded = threaded
        self.broker = broker
        self.entries_allowed = entries_allowed or (lambda: True)
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.status_path = self.data_dir / "crowd-status.json"
        self._lock = threading.Lock()
        self.db = sqlite3.connect(str(self.data_dir / "crowd.sqlite"), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self._ensure_columns()
        self.key_present = False
        self.client = client
        if self.client is None:
            key = api_key if api_key is not None else read_api_key(self.data_dir)
            if key:
                try:
                    self.client = CoinversaClient(key)
                except CoinversaError:
                    self.client = None
        self.key_present = self.client is not None
        self.latest: dict[str, Read] = {}
        self.open: dict[str, Trade] = {}
        self.entries_today: dict[tuple[str, str], int] = {}
        self.acted_on: dict[str, str] = {}
        self.refused: dict[str, int] = {}           # UTC date -> orders the broker refused
        self.unmanaged: list[dict] = []             # open rows this mode cannot manage (see _restore)
        self.notes_by_market: dict[str, str] = {}
        self._pending: list[str] = []               # notes from the restart, given out on the first pass
        self._sweep_due = False                     # LIVE: sweep the broker's list at once (an order's outcome is unknown)
        self._last_minute = ""                      # LIVE: the minute the orphan sweep and the '?' check last ran
        self.last_poll: Optional[dt.datetime] = None
        self.poll_error: str = ""
        self.polls = 0
        self._poll_thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._last_status = 0.0
        # paper tickets continue the sequence already on record so they never collide
        base = int(self.db.execute("SELECT COALESCE(MAX(ticket), ?) FROM trades", (PAPER_FIRST_TICKET - 1,)).fetchone()[0])
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

    # ------------------------------------------------------------- polling --
    def poll_due(self, now: dt.datetime) -> bool:
        if self.client is None:
            return False
        if self.last_poll is None:
            return True
        return (to_utc(now) - self.last_poll).total_seconds() >= self.cfg.poll_minutes * 60.0

    def poll_once(self, now: Optional[dt.datetime] = None) -> list[str]:
        """Read every market from Coinversa once. Safe to call from a thread."""
        now = to_utc(now or self.clock())
        notes: list[str] = []
        if self.client is None:
            return notes
        errors: list[str] = []
        for sym, coin in self.cfg.mapping().items():
            try:
                cohorts = self.client.cohort_bias(coin)
                try:
                    heat = self.client.heatmap(coin, self.cfg.heatmap_range_pct, self.cfg.heatmap_buckets)
                except CoinversaError as exc:
                    heat = None
                    errors.append(f"{coin} heatmap: {exc}")
                r = summarise(sym, coin, cohorts, heat, now, self.cfg.stretch, self.cfg.agree_cap, self.cfg.min_wallets)
                with self._lock:
                    self.latest[sym] = r
                self._record(r)
                notes.append(f"{STRATEGY_LABEL}: {sym} ({coin}) {r.reason}")
            except CoinversaError as exc:
                errors.append(f"{coin}: {exc}")
                if exc.status in (401, 403):
                    break                                   # a bad key fails every market the same way
            except Exception as exc:                        # never let the thread die on a surprise
                errors.append(f"{coin}: {type(exc).__name__}: {exc}")
        self.last_poll = now
        self.polls += 1
        self.poll_error = "; ".join(errors)[:400]
        return notes

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                for n in self.poll_once():
                    log.info("%s", n)
            except Exception as exc:
                self.poll_error = f"{type(exc).__name__}: {exc}"
            self._stop.wait(max(60.0, self.cfg.poll_minutes * 60.0))

    def start(self) -> None:
        """The poll thread. Never started in OFF: with no executor there is
        nothing a read could lead to, so Coinversa is not called at all."""
        if self.active and self.threaded and self.client is not None and self._poll_thread is None:
            self._poll_thread = threading.Thread(target=self._poll_loop, name="crowd-fader-poll", daemon=True)
            self._poll_thread.start()

    def _record(self, r: Read) -> None:
        row = r.to_row()
        with self._lock:
            self.db.execute(f"INSERT INTO checks ({', '.join(CHECK_COLUMNS)}) VALUES ({', '.join('?' * len(CHECK_COLUMNS))})",
                            tuple(row.get(c) for c in CHECK_COLUMNS))
            self.db.commit()

    def checks_since(self, since: dt.datetime, symbol: Optional[str] = None) -> list[dict]:
        q = "SELECT * FROM checks WHERE ts_utc >= ?"
        args: tuple = (to_utc(since).isoformat(),)
        if symbol:
            q += " AND symbol = ?"
            args += (symbol,)
        with self._lock:
            return [dict(r) for r in self.db.execute(q + " ORDER BY ts_utc ASC", args)]

    def context_for(self, symbol: str, when: dt.datetime, max_age_hours: float = 2.0) -> Optional[dict]:
        """The latest read for a market at or before ``when``: what the crowd
        looked like when another bot took a trade. For the nightly review."""
        return context_from_db(self.db, symbol, when, max_age_hours, lock=self._lock)

    # ---------------------------------------------------------------- step --
    def in_entry_hours(self, now: dt.datetime) -> bool:
        n = to_utc(now)
        if n.weekday() >= 5:
            return False
        if n.weekday() == 4 and n.time() >= _hhmm(self.cfg.weekend_flat_utc):
            return False
        hrs = tuple(self.cfg.entry_hours_utc)
        lo, hi = (int(hrs[0]), int(hrs[1])) if len(hrs) >= 2 else (0, 24)
        return lo <= n.hour < hi

    def _may_enter(self) -> bool:
        """The account-level gate shared with Trend & Breakout. Fails closed."""
        try:
            return bool(self.entries_allowed())
        except Exception:
            return False

    def _executor_error(self) -> str:
        return str(getattr(self.executor, "last_error", "") or "")

    def step(self, now: dt.datetime, tick_fn: Callable[[str], object],
             bars_fn: Callable[[str, TF, int], list]) -> list[str]:
        notes: list[str] = list(self._pending)
        self._pending.clear()
        if not self.active:
            self.write_status(now)
            return notes
        now = to_utc(now)
        if self.threaded:
            self.start()
        elif self.poll_due(now):
            notes += self.poll_once(now)
        # LIVE, once a minute (at once after an order whose outcome is unknown): a
        # position with our magic and no row here is closed and booked, never left
        # to run unmanaged until the next restart; and a row booked with a '?'
        # takes the broker's figure once its history has caught up
        minute_key = now.strftime("%Y-%m-%d %H:%M")
        if self.live and (self._sweep_due or self._last_minute != minute_key):
            self._last_minute = minute_key
            notes.extend(self._sweep_orphans(now))
            notes.extend(self._correct_estimates(now))
        for sym, coin in self.cfg.mapping().items():
            try:
                notes.extend(self._step_market(sym, coin, now, tick_fn, bars_fn))
            except Exception as exc:                        # one market's trouble never stops the others
                notes.append(f"{STRATEGY_LABEL}: {sym} skipped this pass ({exc})")
        self.write_status(now)
        return notes

    def _step_market(self, sym: str, coin: str, now: dt.datetime, tick_fn, bars_fn) -> list[str]:
        notes: list[str] = []
        today = now.date()
        weekend_flat = now.weekday() == 4 and now.time() >= _hhmm(self.cfg.weekend_flat_utc)
        tick = tick_fn(sym)
        if tick is None:
            self.notes_by_market[sym] = "no price from the broker"
            return notes
        bid, ask = float(tick.bid), float(tick.ask)
        if ask <= 0 or bid <= 0:
            return notes
        p = self.open.get(sym)
        if p is not None:
            if self.live:
                gone, note = self._reconcile(p, tick, now)   # the broker's stop or target may have closed it
                if note:
                    notes.append(note)
                if gone:
                    return notes
                if p.missing_passes:                         # not in the broker's list this pass: nothing can be closed
                    return notes
            mark = bid if p.side is Side.BUY else ask
            p.last_price = mark
            p.peak_r = max(p.peak_r, p.r_of(mark))
            # PAPER honours the stop and the target here, AT the level. In LIVE both sit
            # at the broker, which closes the position, and _reconcile books it; the
            # target is also watched here as a backstop (a market close) in case the
            # broker's take-profit did not fire, since the executor only vouches for the stop
            hit_stop = (not self.live) and (mark <= p.stop if p.side is Side.BUY else mark >= p.stop)
            hit_target = mark >= p.target if p.side is Side.BUY else mark <= p.target
            held = (now - p.opened).total_seconds() / 3600.0
            if hit_stop:
                notes.append(self._close(p, tick, "STOP", now, level=p.stop))
            elif hit_target:
                notes.append(self._close(p, tick, "TARGET", now, level=None if self.live else p.target))
            elif held >= self.cfg.hold_hours:
                notes.append(self._close(p, tick, "TIME", now))
            elif weekend_flat:
                notes.append(self._close(p, tick, "WEEKEND", now))
            else:
                self._save(p)
                self.notes_by_market[sym] = (f"in trade: {p.side.value} from {p.entry}, {p.r_of(mark):+.2f} R, "
                                             f"stop {p.stop}, target {p.target}")
            return notes
        r = self.latest.get(sym)
        if r is None:
            self.notes_by_market[sym] = "no read from Coinversa yet" if self.client is not None else "no Coinversa key saved"
            return notes
        age_min = (now - r.ts).total_seconds() / 60.0
        if age_min > 3 * self.cfg.poll_minutes + 1:
            self.notes_by_market[sym] = f"last read {age_min:.0f} min old: waiting for a fresh one"
            return notes
        if r.direction == 0 or r.score < self.cfg.min_score:
            self.notes_by_market[sym] = r.reason
            return notes
        if not self.in_entry_hours(now):
            self.notes_by_market[sym] = f"{r.reason}; outside entry hours"
            return notes
        if self.entries_today.get((sym, today.isoformat()), 0) >= self.cfg.max_entries_per_day:
            self.notes_by_market[sym] = f"{r.reason}; no more entries today"
            return notes
        if self.acted_on.get(sym) == r.ts.isoformat():
            self.notes_by_market[sym] = f"{r.reason}; already acted on this read"
            return notes
        if not self._may_enter():
            self.notes_by_market[sym] = "entries paused by the account gate"
            return notes
        ok, why = self._try_enter(sym, coin, r, now, tick, bars_fn)
        self.notes_by_market[sym] = why
        if ok:
            self.acted_on[sym] = r.ts.isoformat()
            self.entries_today[(sym, today.isoformat())] = self.entries_today.get((sym, today.isoformat()), 0) + 1
            notes.append(why)
        elif why.startswith(f"{STRATEGY_LABEL}: {sym} order"):      # refused, or sent with the outcome unknown
            notes.append(why)
        return notes

    def _try_enter(self, sym: str, coin: str, r: Read, now: dt.datetime, tick,
                   bars_fn: Callable[[str, TF, int], list]) -> tuple[bool, str]:
        bid, ask = float(tick.bid), float(tick.ask)
        want = max(self.cfg.trigger_bars, self.cfg.stop_bars) + 3
        try:
            bars = list(bars_fn(sym, TF.M15, want) or [])
        except Exception as exc:
            return False, f"{r.reason}; no M15 bars ({exc})"
        closed = [b for b in bars if to_utc(b.time) + dt.timedelta(minutes=15) <= now]
        if len(closed) < self.cfg.trigger_bars + 1 or len(closed) < self.cfg.stop_bars + 1:
            return False, f"{r.reason}; waiting for enough closed M15 bars"
        last = closed[-1]
        prior = closed[-1 - self.cfg.trigger_bars:-1]
        side = Side.SELL if r.direction < 0 else Side.BUY
        if side is Side.SELL:
            turned = float(last.close) < min(float(b.close) for b in prior)
        else:
            turned = float(last.close) > max(float(b.close) for b in prior)
        if not turned:
            return False, f"{r.reason}; waiting for the price to turn (a 15-min close beyond the last {self.cfg.trigger_bars})"
        recent = closed[-self.cfg.stop_bars:]
        spread = max(ask - bid, 0.0)
        touch = bid if side is Side.SELL else ask
        if side is Side.SELL:
            stop = max(float(b.high) for b in recent) + spread
        else:
            stop = min(float(b.low) for b in recent) - spread
        price_ref = (bid + ask) / 2.0
        dist = abs(touch - stop)
        min_d, max_d = price_ref * self.cfg.min_stop_pct / 100.0, price_ref * self.cfg.max_stop_pct / 100.0
        if dist < min_d:
            stop = touch + (min_d if side is Side.SELL else -min_d)
            dist = min_d
        if dist > max_d:
            return False, f"{r.reason}; the structural stop would be {dist / price_ref * 100:.2f}% away, over {self.cfg.max_stop_pct}%: skipped"
        cluster = r.cluster_below if side is Side.SELL else r.cluster_above
        target = None
        if cluster and r.price:
            # the cluster is a Hyperliquid price; carry it over as a percentage move
            move_pct = (cluster - r.price) / r.price
            cand = touch * (1 + move_pct)
            rr = abs(cand - touch) / dist if dist else 0.0
            if self.cfg.cluster_min_r <= rr <= self.cfg.cluster_max_r and (cand - touch) * side.sign > 0:
                target = cand
        if target is None:
            target = touch + side.sign * self.cfg.target_r * dist
        return self._open(sym, coin, side, tick, stop, target, now, r)

    # ----------------------------------------------------------- positions --
    def _open(self, sym: str, coin: str, side: Side, tick, stop: float, target: float,
              now: dt.datetime, r: Read) -> tuple[bool, str]:
        """Size at the stop and send the order through the executor: PAPER
        fills at the touch plus slippage; LIVE sends the order with the stop
        and the target at the broker and the broker's price is the entry."""
        spec = self.spec_fn(sym)
        if spec is None:
            return False, f"{STRATEGY_LABEL}: {sym} has no contract specification"
        point = float(getattr(spec, "point", 0.0) or 0.0)
        touch = float(tick.bid if side is Side.SELL else tick.ask)
        # sized on the touch plus the paper slippage in both modes: in PAPER that is
        # the fill; in LIVE it is a cautious guess at it, the broker's price is the entry
        plan = spec.normalise_price(touch + side.sign * self.cfg.paper_slippage_points * point)
        stop = spec.normalise_price(stop)
        target = spec.normalise_price(target)
        dist = abs(plan - stop)
        per_lot = spec.money_per_lot(dist)
        if per_lot <= 0:
            return False, f"{STRATEGY_LABEL}: {sym} cannot value the stop"
        volume = spec.normalise_volume(self.cfg.risk_money / per_lot)
        vmin = float(getattr(spec, "volume_min", 0.01) or 0.01)
        if volume < vmin:
            # a wide structural stop on gold or an index can put 10 below the
            # smallest size: take the smallest size if the risk stays modest
            if per_lot * vmin <= self.cfg.max_risk_money:
                volume = vmin
            else:
                return False, (f"{STRATEGY_LABEL}: {sym} skipped - the smallest size would risk "
                               f"{per_lot * vmin:.2f}, over {self.cfg.max_risk_money:.0f}")
        risk = per_lot * volume
        day = now.date().isoformat()
        try:
            fill = self.executor.open(sym, side, volume, stop, target, tick, spec, now, comment=TAG)
        except Exception as exc:
            # the order may or may not be at the broker (a read-back that failed after
            # a fill): this read is spent, and the broker's list is swept before
            # anything else so a stray position is closed and booked, never left
            # to run with no row of its own
            self.refused[day] = self.refused.get(day, 0) + 1
            self.acted_on[sym] = r.ts.isoformat()
            self._sweep_due = True
            return False, (f"{STRATEGY_LABEL}: {sym} order sent, state unknown: {exc}; "
                           "the broker's list is checked for a stray position next pass")
        if not fill.ok:
            # a clean no: nothing recorded, this read is spent so the broker is not hammered
            self.refused[day] = self.refused.get(day, 0) + 1
            self.acted_on[sym] = r.ts.isoformat()
            if str(fill.message).startswith("send failed"):
                self._sweep_due = True          # the send itself failed: it may have reached the broker
            return False, f"{STRATEGY_LABEL}: {sym} order refused: {fill.message}"
        p = Trade(int(fill.ticket), sym, coin, side, float(fill.volume or volume), float(fill.price), float(stop), float(target),
                  to_utc(now), day, stop=float(stop), last_price=touch, mode=self.executor.mode,
                  meta={"score": r.score, "crowd_bias": r.crowd_bias, "winners_bias": r.winners_bias,
                        "fuel_share": r.fuel_share, "read_ts": r.ts.isoformat(), "reason": r.reason, "risk_money": risk})
        try:
            self._save(p, new=True)
        except sqlite3.IntegrityError:
            # a ticket already on record (a PAPER row from before): fail closed rather
            # than carry a position with no row of its own, and never touch that row
            self.refused[day] = self.refused.get(day, 0) + 1
            self.acted_on[sym] = r.ts.isoformat()
            undo = self.executor.close(p.ticket, tick, spec, comment=f"{TAG} dup ticket")
            if not undo.ok:
                self._sweep_due = True                     # still at the broker with its stop: the sweep takes it
            what = "closed again at once" if undo.ok else f"close refused ({undo.message}); the sweep will take it"
            return False, f"{STRATEGY_LABEL}: {sym} order refused: ticket {p.ticket} is already on record, {what}"
        self.open[sym] = p
        rr = abs(target - p.entry) / dist if dist else 0.0
        note = (f"{STRATEGY_LABEL}: {sym} {side.value} {p.volume:g} lots at {p.entry} stop {stop} target {target} "
                f"({rr:.1f} R; risk {risk:.2f}; {r.reason})")
        if self.live:
            note += f", live ticket {p.ticket}"
        return True, note

    def _close(self, p: Trade, tick, reason: str, now: dt.datetime, level: Optional[float] = None) -> str:
        """The engine's own exit. PAPER: AT the stop or target level when one
        is given (never better), else at the mark less slippage. LIVE: a market
        close at the broker; the broker's deal is the figure. A refused close
        keeps the position and is tried again next pass."""
        spec = self.spec_fn(p.symbol)
        at = float(level) if level is not None else None
        if spec is None and at is None:
            at = float(tick.bid if p.side is Side.BUY else tick.ask)
        fill = self.executor.close(p.ticket, tick, spec, at_price=at, comment=f"{TAG} {reason}"[:31])
        if not fill.ok:
            why = fill.message or self._executor_error() or "no reason given"
            self.notes_by_market[p.symbol] = f"close ({reason}) refused: {why}; still open, will retry"
            return f"{STRATEGY_LABEL}: {p.symbol} {p.side.value} close ({reason}) refused: {why}; still open, will retry"
        exit_px = float(fill.price)
        net = float(spec.money((exit_px - p.entry) * p.side.sign, p.volume)) if spec is not None else 0.0
        closed_at = to_utc(now)
        if self.live:
            exit_px, net, when = self._deal_figures(p, fill, now)
            if when is None:
                reason += "?"                              # estimated from the fill: the broker's record was not there yet
            else:
                closed_at = when
        self._finalise(p, exit_px, net, reason, closed_at)
        return f"{STRATEGY_LABEL}: {p.symbol} {p.side.value} closed {reason} at {exit_px}: {net:+.2f}"

    def _reconcile(self, p: Trade, tick, now: dt.datetime) -> tuple[bool, str]:
        """LIVE, every pass: is the position still at the broker? Returns
        (gone, note). Still there: the broker's running figure is taken. Not
        there: the broker may have closed it (the stop or the target) and its
        record is the figure - see _finalise_from_broker, which never books a
        row on one empty read."""
        mine = {q.ticket: q for q in self.executor.positions()}
        q = mine.get(p.ticket)
        if q is not None:
            p.broker_pnl = float(getattr(q, "profit", 0.0) or 0.0) + float(getattr(q, "swap", 0.0) or 0.0)
            if p.missing_passes:
                p.missing_passes, p.missing_since = 0, None
                return False, f"{STRATEGY_LABEL}: {p.symbol} ticket {p.ticket} is back in the broker's list; managed on"
            return False, ""
        return self._finalise_from_broker(p, tick, now)

    def _finalise_from_broker(self, p: Trade, tick, now: dt.datetime) -> tuple[bool, str]:
        """The ticket is not in the broker's list. With the broker's closing
        deal the row is closed ONCE from it, the reason the broker's own word
        (its stop, its take-profit, a close by hand, or one of this bot's own
        closes finalised late). Without a deal the position is kept and
        checked again: one empty read is not a close (the adapter answers a
        terminal error with an empty list), and booking it would drop a live
        position from management and let a second one open on the market.
        Only after MISSING_PASSES reads in a row over MISSING_SECONDS is it
        booked at the last mark with a '?' on the reason, and the '?' check
        puts the broker's figure in later. Returns (gone, note)."""
        spec = self.spec_fn(p.symbol)
        if tick is not None:
            mark = float(tick.bid if p.side is Side.BUY else tick.ask)
        else:
            mark = float(p.last_price or p.stop or p.entry)
        deal = self.executor.closed_deal(p.ticket)
        if deal:
            exit_px = float(deal.get("exit_price") or mark)
            net = float(deal["pnl"])
            reason = self._broker_reason(p, exit_px, deal.get("reason"))
            self._finalise(p, exit_px, net, reason, self._deal_time(deal, now))
            return True, f"{STRATEGY_LABEL}: {p.symbol} {p.side.value} closed by the broker ({reason}) at {exit_px}: {net:+.2f}"
        p.missing_passes += 1
        if p.missing_since is None:
            p.missing_since = to_utc(now)
        absent = (to_utc(now) - p.missing_since).total_seconds()
        if p.missing_passes < MISSING_PASSES or absent < MISSING_SECONDS:
            self.notes_by_market[p.symbol] = (f"ticket {p.ticket} not in the broker's list and no closing deal yet "
                                              f"({p.missing_passes} read{'s' if p.missing_passes != 1 else ''}): kept, checked again next pass")
            if p.missing_passes == 1:
                return False, (f"{STRATEGY_LABEL}: {p.symbol} ticket {p.ticket} not in the broker's list and no closing deal yet; "
                               "kept and checked again")
            return False, ""
        exit_px = mark
        net = float(spec.money((exit_px - p.entry) * p.side.sign, p.volume)) if spec is not None else 0.0
        reason = self._broker_reason(p, exit_px, "") + "?"
        self._finalise(p, exit_px, net, reason, to_utc(now))
        return True, (f"{STRATEGY_LABEL}: {p.symbol} {p.side.value} gone from the broker for {absent:.0f} s with no deal record; "
                      f"booked {reason} at the last mark {exit_px}: {net:+.2f} (estimated; the broker's figure goes in when it comes)")

    @staticmethod
    def _broker_reason(p: Trade, exit_px: float, hint="") -> str:
        """Why the position closed. The broker's own word first: MT5 writes
        '[sl ...]' or '[tp ...]' on the closing deal (the sim says SL or TP);
        a comment of this bot's own (CROWD TIME, CROWD WEEKEND, CROWD TARGET,
        CROWD orphan) is one of its closes finalised late; any other word is
        a close by hand or by the broker, never a stop. Only with no word at
        all is it the level the exit price is nearer to, or BROKER_CLOSED
        when neither level is set."""
        h = str(hint or "").strip()
        low = h.lower()
        if low.startswith(TAG.lower()):
            words = h[len(TAG):].strip().split()
            word = words[0].upper().rstrip("?") if words else ""
            if word.startswith("ORPHAN"):
                return "ORPHAN_CLOSED"
            return word if word in ("TIME", "WEEKEND", "TARGET", "STOP") else "BROKER_CLOSED"
        if "sl" in low or "stop" in low:
            return "STOP"
        if "tp" in low or "take" in low:
            return "TARGET"
        if low:
            return "MANUAL" if "manual" in low else "BROKER_CLOSED"
        levels = []
        if p.stop:
            levels.append((abs(float(exit_px) - float(p.stop)), "STOP"))
        if p.target:
            levels.append((abs(float(exit_px) - float(p.target)), "TARGET"))
        if not levels:
            return "BROKER_CLOSED"
        return min(levels)[1]

    @staticmethod
    def _deal_time(deal: dict, now: dt.datetime) -> dt.datetime:
        t = deal.get("time")
        try:
            return to_utc(t) if isinstance(t, dt.datetime) else to_utc(now)
        except Exception:
            return to_utc(now)

    def _finalise(self, p: Trade, exit_px: float, net: float, reason: str, closed_at: dt.datetime, note: bool = True) -> None:
        # commission stays 0 in the row: in LIVE the broker's net already holds profit + commission + swap
        with self._lock:
            self.db.execute("""UPDATE trades SET closed_utc=?, stop=?, exit_price=?, realised_r=?, net_pnl=?, commission=0,
                               exit_reason=?, duration_seconds=?, peak_r=? WHERE ticket=?""",
                            (closed_at.isoformat(), p.stop, exit_px, round(p.r_of(exit_px), 4), round(net, 2), reason,
                             (closed_at - p.opened).total_seconds(), round(p.peak_r, 4), p.ticket))
            self.db.commit()
        if self.open.get(p.symbol) is p:                       # only this trade, never another one on the same market
            self.open.pop(p.symbol)
        if note:
            self.notes_by_market[p.symbol] = f"closed {reason} at {exit_px}: {net:+.2f}"

    def _deal_figures(self, p: Trade, fill, now: dt.datetime) -> tuple[float, float, Optional[dt.datetime]]:
        """After a close of our own at the broker: the deal's exit, net and
        time, or the fill price valued through the contract (time None) when
        the deal has not come back after the executor's retries."""
        spec = self.spec_fn(p.symbol)
        deal = self.executor.closed_deal(p.ticket)
        if deal:
            return float(deal.get("exit_price") or fill.price), float(deal["pnl"]), self._deal_time(deal, now)
        exit_px = float(fill.price)
        net = float(spec.money((exit_px - p.entry) * p.side.sign, p.volume)) if spec is not None else 0.0
        return exit_px, net, None

    def _deal_quick(self, ticket: int) -> Optional[dict]:
        """The broker's closing deal with no waiting: the once-a-minute check
        of '?' rows must not hold the pass up."""
        try:
            return self.executor.closed_deal(ticket, tries=1)
        except TypeError:
            return self.executor.closed_deal(ticket)
        except Exception:
            return None

    def _correct_estimates(self, now: dt.datetime) -> list[str]:
        """Every LIVE row booked with a '?' (the broker's history had not
        caught up when it closed) is re-read from the broker, and once its
        deal is there the row takes the broker's figure and loses the '?'.
        Runs at start and once a minute, over rows closed in the last day."""
        since = (to_utc(now) - dt.timedelta(hours=ESTIMATE_WINDOW_HOURS)).isoformat()
        with self._lock:
            rows = [dict(r) for r in self.db.execute(
                "SELECT * FROM trades WHERE mode='LIVE' AND exit_reason LIKE '%?' AND closed_utc >= ? ORDER BY closed_utc",
                (since,))]
        notes: list[str] = []
        for r in rows:
            deal = self._deal_quick(int(r["ticket"]))
            if not deal:
                continue
            p = self._trade_from_row(r)
            was = f"{r['exit_reason']} at {r['exit_price']}: {float(r['net_pnl'] or 0.0):+.2f}"
            exit_px = float(deal.get("exit_price") or r["exit_price"] or p.entry)
            net = float(deal["pnl"])
            reason = (self._broker_reason(p, exit_px, deal.get("reason")) if deal.get("reason")
                      else str(r["exit_reason"]).rstrip("?"))
            closed_at = self._deal_time(deal, dt.datetime.fromisoformat(r["closed_utc"]))
            self._finalise(p, exit_px, net, reason, closed_at, note=False)
            notes.append(f"{STRATEGY_LABEL}: ticket {p.ticket} {p.symbol} {p.side.value} now holds the broker's record: "
                         f"{reason} at {exit_px}: {net:+.2f} (was booked {was}, estimated)")
        return notes

    def _save(self, p: Trade, new: bool = False) -> None:
        with self._lock:
            if new:
                self.db.execute("""INSERT INTO trades (ticket, symbol, coin, side, volume, entry, initial_stop, target, opened_utc,
                                   stop, mode, session_date, score, crowd_bias, winners_bias, fuel_share, read_ts, reason, peak_r,
                                   risk_money)
                                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?)""",
                                (p.ticket, p.symbol, p.coin, p.side.value, p.volume, p.entry, p.initial_stop, p.target,
                                 p.opened.isoformat(), p.stop, p.mode, p.session_date, p.meta.get("score"),
                                 p.meta.get("crowd_bias"), p.meta.get("winners_bias"), p.meta.get("fuel_share"),
                                 p.meta.get("read_ts"), p.meta.get("reason"), p.meta.get("risk_money")))
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
        managed on; one gone is finalised from the broker's deal, and with no
        deal yet it is kept and checked on the first passes (never booked on
        one read). A position at the broker with this bot's magic and no row
        here is closed at once (fail closed; never adopted). A row from the
        other mode cannot be managed by this one and is listed as unmanaged,
        never touched. Rows booked with a '?' are re-read from the broker."""
        now = to_utc(self.clock())
        for sym, day, n in self.db.execute("SELECT symbol, session_date, COUNT(*) FROM trades GROUP BY symbol, session_date"):
            if sym and day:
                self.entries_today[(sym, day)] = int(n)
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
            if self.live:
                if p.symbol in self.open:                      # never two positions in one market
                    self._pending.append(self._settle_extra(p, at_broker, now))
                    continue
                self.open[p.symbol] = p                        # managed on; taken out again if the broker closed it
                if at_broker is not None and p.ticket not in at_broker:
                    gone, note = self._finalise_from_broker(p, self._broker_tick(p.symbol), now)
                    if note:
                        self._pending.append(note)
            else:
                self.executor.adopt(Position(p.ticket, p.symbol, p.side, p.volume, p.entry, p.stop, p.target, p.opened,
                                             comment=TAG))
                self.open[p.symbol] = p
        if self.live and at_broker is not None:
            self._pending.extend(self._sweep_orphans(now, at_broker))
        if self.live:
            self._pending.extend(self._correct_estimates(now))
        for sym in self.cfg.mapping():
            row = self.db.execute("SELECT * FROM checks WHERE symbol=? ORDER BY ts_utc DESC LIMIT 1", (sym,)).fetchone()
            if row is not None:
                self.latest[sym] = _read_from_row(dict(row))

    @staticmethod
    def _trade_from_row(r: dict) -> Trade:
        return Trade(int(r["ticket"]), r["symbol"], r["coin"] or "", Side(r["side"]), float(r["volume"]), float(r["entry"]),
                     float(r["initial_stop"]), float(r["target"] or 0.0), to_utc(dt.datetime.fromisoformat(r["opened_utc"])),
                     r["session_date"] or "", stop=float(r["stop"] or 0.0), peak_r=float(r["peak_r"] or 0),
                     mode=str(r.get("mode") or "PAPER").upper())

    def _leave_unmanaged(self, p: Trade, why: str, note: str) -> None:
        self.unmanaged.append({"ticket": p.ticket, "symbol": p.symbol, "side": p.side.value, "mode": p.mode, "note": why})
        self._pending.append(f"{STRATEGY_LABEL}: ticket {p.ticket} {p.symbol} {p.side.value} {note}")

    def _settle_extra(self, p: Trade, at_broker: Optional[dict], now: dt.datetime) -> str:
        """A second open row on a market that already has one (the record of
        an earlier fault). Still at the broker: the orphan sweep closes it and
        its row takes the broker's figure. Gone from the broker with a deal:
        booked from it. Anything else is left as it is and listed as unmanaged."""
        head = f"{STRATEGY_LABEL}: ticket {p.ticket} is a second open row for {p.symbol} (ticket {self.open[p.symbol].ticket} is managed)"
        if at_broker is not None and p.ticket in at_broker:
            return head + ": still at the broker, so it is closed now and its row takes the broker's figure"
        deal = self.executor.closed_deal(p.ticket) if at_broker is not None else None
        if deal:
            exit_px = float(deal.get("exit_price") or p.stop or p.entry)
            reason = self._broker_reason(p, exit_px, deal.get("reason"))
            self._finalise(p, exit_px, float(deal["pnl"]), reason, self._deal_time(deal, now), note=False)
            return head + f": closed by the broker ({reason}) at {exit_px}: {float(deal['pnl']):+.2f}"
        self._leave_unmanaged(p, "a second open row for this market: not managed here",
                              f"is a second open row for {p.symbol}: left as it is, check it by hand")
        return head + ": left as it is, check it by hand"

    def _sweep_orphans(self, now: dt.datetime, at_broker: Optional[dict] = None) -> list[str]:
        """Every position at the broker with our magic that no open row here
        accounts for is closed at once and booked (fail closed; never adopted).
        Run at start, once a minute in LIVE, and at once after an order whose
        outcome is unknown. A read that fails sweeps nothing and is tried
        again next pass; an empty list closes nothing."""
        if at_broker is None:
            try:
                at_broker = {q.ticket: q for q in self.executor.positions()}
            except Exception as exc:
                return [f"{STRATEGY_LABEL}: could not read the broker's positions ({exc}); the sweep runs again next pass"]
        self._sweep_due = False
        known = {p.ticket for p in self.open.values()}
        return [self._close_orphan(q, now) for q in at_broker.values() if q.ticket not in known]

    def _close_orphan(self, q: Position, now: dt.datetime) -> str:
        """A position with our magic that no open row accounts for: close it
        now and keep the broker's figure, so the money is still in the book. A
        row already carrying that ticket (an earlier estimate, or a row the
        engine lost track of) takes the broker's figure instead of a new row;
        a PAPER row with that number is never touched."""
        tick = self._broker_tick(q.symbol) or Tick(q.symbol, now, float(q.entry_price), float(q.entry_price))
        spec = self.spec_fn(q.symbol)
        fill = self.executor.close(q.ticket, tick, spec, comment=f"{TAG} orphan")
        head = f"{STRATEGY_LABEL}: orphan closed - ticket {q.ticket} {q.symbol} {q.side.value} {q.volume:g} lots at the broker with no record here"
        if not fill.ok:
            return head + f" (close refused: {fill.message}; still at the broker with its stop, close it by hand)"
        day = now.date().isoformat()
        p = Trade(int(q.ticket), q.symbol, self.cfg.mapping().get(q.symbol, ""), q.side, float(q.volume), float(q.entry_price),
                  float(q.sl or 0.0), float(q.tp or 0.0),
                  to_utc(q.open_time) if isinstance(q.open_time, dt.datetime) else now, day,
                  stop=float(q.sl or 0.0), mode="LIVE", meta={"reason": "orphan: at the broker with no record here"})
        try:
            self._save(p, new=True)
            self.entries_today[(q.symbol, day)] = self.entries_today.get((q.symbol, day), 0) + 1
        except sqlite3.IntegrityError:
            with self._lock:
                row = self.db.execute("SELECT * FROM trades WHERE ticket=?", (q.ticket,)).fetchone()
            row = dict(row) if row is not None else {}
            if str(row.get("mode") or "PAPER").upper() != "LIVE":
                exit_px, net, _ = self._deal_figures(p, fill, now)
                return head + (f"; closed at {exit_px}: {net:+.2f}, but a PAPER row already holds ticket number {q.ticket}, "
                               "so the figure is only in this note and in the broker's history")
            was = str(row.get("exit_reason") or "open")
            p = self._trade_from_row(row)
            head += f" (its row, booked {was}, now holds the broker's figure)"
        exit_px, net, when = self._deal_figures(p, fill, now)
        self._finalise(p, exit_px, net, "ORPHAN_CLOSED" if when is not None else "ORPHAN_CLOSED?", when or now, note=False)
        return head + f"; closed at {exit_px}: {net:+.2f}"

    # --------------------------------------------------------------- status --
    def closed_since(self, since: dt.datetime) -> list[dict]:
        with self._lock:
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
                        "target": p.target, "initial_stop": p.initial_stop,
                        "r_now": round(p.r_of(p.last_price), 2) if p.last_price else 0.0,
                        "peak_r": round(p.peak_r, 2), "pnl": round(pnl, 2), "opened": p.opened.isoformat(), "mode": p.mode})
        return out

    def status(self, now: Optional[dt.datetime] = None) -> dict:
        now = to_utc(now or self.clock())
        if not self.active:
            state = "OFF"
        elif self.client is None:
            state = "NO KEY - run: python -m mintel.crowd --config <data>/config.json --set-key"
        elif self.open:
            state = "IN TRADE"
        elif self.poll_error and not self.latest:
            state = "COINVERSA UNAVAILABLE"
        elif self.in_entry_hours(now):
            state = "WATCHING"
        else:
            state = "OUTSIDE HOURS (managing only)"
        reads = {}
        for sym, r in self.latest.items():
            reads[sym] = {"coin": r.coin, "at": r.ts.isoformat(), "crowd_long_pct": round(r.crowd_long_share * 100, 1),
                          "crowd_wallets": r.crowd_long + r.crowd_short, "crowd_bias": round(r.crowd_bias, 2),
                          "winners_bias": round(r.winners_bias, 2), "fuel_below": round(r.fuel_below), "fuel_above": round(r.fuel_above),
                          "direction": r.direction, "score": r.score, "reason": r.reason, "hl_price": r.price}
        with self._lock:                                  # LIVE rows booked with a '?', still waiting for the broker's record
            estimated = int(self.db.execute("SELECT COUNT(*) FROM trades WHERE mode='LIVE' AND exit_reason LIKE '%?'").fetchone()[0])
        return {"strategy_id": STRATEGY_ID, "label": STRATEGY_LABEL, "tagline": TAGLINE,
                "mode": self.mode, "live": self.live, "magic": int(self.cfg.magic),
                "executor_error": self._executor_error(), "refused": int(self.refused.get(now.date().isoformat(), 0)),
                "estimated_rows": estimated,
                "status": state, "updated": now.isoformat(),
                "key_present": self.key_present, "markets": list(self.cfg.mapping().keys()),
                "rule": (f"crowd {self.cfg.stretch:.0%} net one way and the winners not with them; score {self.cfg.min_score:.0f}+; "
                         f"entry on a 15-min close beyond the last {self.cfg.trigger_bars}; stop beyond the last {self.cfg.stop_bars} "
                         f"bars; target the nearest cluster or {self.cfg.target_r:g} R; out after {self.cfg.hold_hours:g} h; "
                         f"at most {self.cfg.max_entries_per_day} a day per market; {self.cfg.risk_money:.0f} at the stop "
                         f"(the smallest size up to {self.cfg.max_risk_money:.0f}); reads every {self.cfg.poll_minutes:g} min"),
                "last_poll": self.last_poll.isoformat() if self.last_poll else None, "polls": self.polls,
                "poll_error": self.poll_error, "api_calls": self.client.calls if self.client is not None else 0,
                "api_remaining": self.client.remaining if self.client is not None else None,
                "markets_now": dict(self.notes_by_market), "reads": reads, "open": self.open_rows(),
                "unmanaged": list(self.unmanaged),
                "entries_today": {s: n for (s, d), n in self.entries_today.items() if d == now.date().isoformat()}}

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
        self._stop.set()
        try:
            self.db.close()
        except Exception:
            pass


def _read_from_row(row: dict) -> Read:
    r = Read(row.get("symbol") or "", row.get("coin") or "", to_utc(dt.datetime.fromisoformat(row["ts_utc"])),
             price=float(row.get("price") or 0.0), crowd_long=int(row.get("crowd_long") or 0),
             crowd_short=int(row.get("crowd_short") or 0), crowd_bias=float(row.get("crowd_bias") or 0.0),
             crowd_bias_money=float(row.get("crowd_bias_money") or 0.0), winners_bias=float(row.get("winners_bias") or 0.0),
             winners_bias_count=float(row.get("winners_bias_count") or 0.0), fuel_below=float(row.get("fuel_below") or 0.0),
             fuel_above=float(row.get("fuel_above") or 0.0), cluster_below=row.get("cluster_below"),
             cluster_above=row.get("cluster_above"), direction=int(row.get("direction") or 0),
             score=float(row.get("score") or 0.0), reason=str(row.get("reason") or ""))
    return r


def context_from_db(db, symbol: str, when: dt.datetime, max_age_hours: float = 2.0, lock=None) -> Optional[dict]:
    """The Crowd Fader's latest read for ``symbol`` at or before ``when``, if
    it is recent enough to describe that moment. ``db`` is an open sqlite
    connection (or a path to crowd.sqlite)."""
    own = None
    if isinstance(db, (str, Path)):
        p = Path(db)
        if not p.exists():
            return None
        own = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        own.row_factory = sqlite3.Row
        db = own
    try:
        w = to_utc(when)
        q = "SELECT * FROM checks WHERE symbol=? AND ts_utc <= ? ORDER BY ts_utc DESC LIMIT 1"
        if lock is not None:
            with lock:
                row = db.execute(q, (symbol, w.isoformat())).fetchone()
        else:
            row = db.execute(q, (symbol, w.isoformat())).fetchone()
        if row is None:
            return None
        d = dict(row)
        age = (w - to_utc(dt.datetime.fromisoformat(d["ts_utc"]))).total_seconds() / 3600.0
        if age > max_age_hours:
            return None
        d["age_minutes"] = round(age * 60.0, 1)
        return d
    except Exception:
        return None
    finally:
        if own is not None:
            own.close()
