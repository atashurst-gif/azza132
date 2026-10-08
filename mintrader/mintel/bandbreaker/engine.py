"""Band Breaker engine: the noise band, the half-hour checks, paper positions.

Runs inside the main bot's loop (like the Momentum Runner), one ``step`` per
pass. It fetches bars through callables so it can be tested on made-up
sessions. Pessimistic by construction: fills at the touch plus slippage, a
stop closes AT the stop level, a buy is marked on the bid and a sell on the
ask. Nothing here touches the broker's orders.
"""
from __future__ import annotations

import datetime as dt
import itertools
import json
import sqlite3
import statistics as st
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional, Sequence

from ..broker.base import Side, TF
from ..clock import TZ_NEWYORK, to_utc, utcnow
from . import STRATEGY_ID, STRATEGY_LABEL, TAGLINE

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
    mode: str = "PAPER"
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
class Paper:
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
    meta: dict = field(default_factory=dict)

    def r_of(self, price: float) -> float:
        d = abs(self.entry - self.initial_stop)
        return ((price - self.entry) * self.side.sign) / d if d else 0.0


def _hhmm(t: str) -> dt.time:
    h, m = t.split(":")
    return dt.time(int(h), int(m))


def ny(ts: dt.datetime) -> dt.datetime:
    return to_utc(ts).astimezone(TZ_NEWYORK)


class BandBreaker:
    def __init__(self, data_dir: str | Path, cfg: Optional[BandBreakerConfig] = None,
                 spec_fn: Optional[Callable[[str], object]] = None,
                 clock: Callable[[], dt.datetime] = utcnow):
        self.cfg = cfg or BandBreakerConfig()
        self.spec_fn = spec_fn or (lambda s: None)
        self.clock = clock
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.status_path = self.data_dir / "bandbreaker-status.json"
        self._lock = threading.Lock()
        self.db = sqlite3.connect(str(self.data_dir / "bandbreaker.sqlite"), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self.open: dict[str, Paper] = {}            # one position per market
        self.bands: dict[str, Bands] = {}
        self.entries_today: dict[tuple[str, str], int] = {}
        self.last_minute: dict[str, str] = {}
        self.last_check: dict[str, str] = {}
        self.notes_by_market: dict[str, str] = {}
        self._seq = itertools.count(int(self.db.execute("SELECT COALESCE(MAX(ticket), 700000000) FROM trades").fetchone()[0]) + 1)
        self._last_status = 0.0
        self._restore()

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
        notes: list[str] = []
        if not self.cfg.enabled:
            return notes
        n = ny(now)
        minute_key = n.strftime("%Y-%m-%d %H:%M")
        for sym in self.cfg.markets:
            try:
                notes.extend(self._step_market(sym, now, n, minute_key, tick_fn, bars_fn))
            except Exception as exc:                       # one market's trouble never stops the others
                notes.append(f"{STRATEGY_LABEL}: {sym} skipped this pass ({exc})")
        self.write_status(now)
        return notes

    def _step_market(self, sym: str, now: dt.datetime, n: dt.datetime, minute_key: str,
                     tick_fn, bars_fn) -> list[str]:
        notes: list[str] = []
        pos = self.open.get(sym)
        tick = tick_fn(sym)
        if tick is None:
            return notes
        bid, ask = float(tick.bid), float(tick.ask)
        # every tick: a stop is a stop
        if pos is not None:
            price = bid if pos.side is Side.BUY else ask
            pos.last_price = price
            pos.peak_r = max(pos.peak_r, pos.r_of(price))
            if (price - pos.stop) * pos.side.sign <= 0:
                notes.append(self._close(pos, pos.stop, "BAND_STOP" if abs(pos.stop - pos.initial_stop) < 1e-9 else "TRAIL_STOP", now))
                pos = None
        if not self.in_session(now):
            if pos is not None:
                notes.append(self._close(pos, bid if pos.side is Side.BUY else ask, "SESSION_CLOSE", now))
            self.notes_by_market[sym] = "outside the New York session"
            return notes
        if pos is not None and n.time() >= self.flat_time():
            notes.append(self._close(pos, bid if pos.side is Side.BUY else ask, "FLAT_BEFORE_CLOSE", now))
            pos = None
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
                pos.stop = self._norm(sym, cand)
                self._save(pos)
        self.notes_by_market[sym] = (f"band {lb:.2f} - {ub:.2f} (noise {sigma:.2%} by {hhmm} NY), VWAP {vw:.2f}, "
                                     f"price {(bid + ask) / 2:.2f}")
        # at a half-hour close: the decision
        if n.minute % self.cfg.check_minutes != 0 or self.last_check.get(sym) == minute_key:
            return notes
        if n.time() < _hhmm(self.cfg.first_check) or n.time() > _hhmm(self.cfg.last_entry):
            return notes
        self.last_check[sym] = minute_key
        m30 = bars_fn(sym, TF.M30, 4)
        closed = [b for b in m30 if ny(b.time).time() < n.time() or ny(b.time).date() < today]
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
                if pos is not None:
                    notes.append(self._close(pos, bid if pos.side is Side.BUY else ask, "REVERSAL", now))
                stop = (max(lb, vw) if vw > 0 else lb) if want is Side.BUY else (min(ub, vw) if vw > 0 else ub)
                entry_px = ask if want is Side.BUY else bid
                mark = bid if want is Side.BUY else ask        # the stop must clear the price we would be closed at
                if (mark - stop) * want.sign <= 0:
                    stop = lb if want is Side.BUY else ub
                if (mark - stop) * want.sign <= 0:
                    decision = "signal, but no room for a stop"
                    self.notes_by_market[sym] += "; no room for a stop"
                else:
                    ok, msg = self._open(sym, want, entry_px, stop, now, today,
                                         {"band_upper": ub, "band_lower": lb, "vwap_at_entry": vw, "sigma": sigma,
                                          "day_open": bands.day_open, "prev_close": bands.prev_close})
                    decision = ("entered " + want.value.lower()) if ok else ("signal, but " + msg)
                    if ok:
                        self.entries_today[key] = self.entries_today.get(key, 0) + 1
                    notes.append(msg)
        finally:
            self._record_check(sym, now, hhmm, close, ub, lb, vw, sigma, decision)
        return notes

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
    def _open(self, sym: str, side: Side, touch: float, stop: float, now: dt.datetime,
              today: dt.date, meta: dict) -> tuple[bool, str]:
        spec = self.spec_fn(sym)
        if spec is None:
            return False, f"{STRATEGY_LABEL}: {sym} has no contract specification"
        point = float(getattr(spec, "point", 0.0) or 0.0)
        price = spec.normalise_price(touch + side.sign * self.cfg.paper_slippage_points * point)
        stop = spec.normalise_price(stop)
        dist = abs(price - stop)
        per_lot = spec.money_per_lot(dist)
        if per_lot <= 0:
            return False, f"{STRATEGY_LABEL}: {sym} cannot value the stop"
        volume = spec.normalise_volume(self.cfg.risk_money / per_lot)
        if volume < getattr(spec, "volume_min", 0.01):
            return False, f"{STRATEGY_LABEL}: {sym} skipped - the smallest size would risk {per_lot * spec.volume_min:.2f}, over {self.cfg.risk_money:.0f}"
        p = Paper(next(self._seq), sym, side, float(volume), float(price), float(stop), to_utc(now), today.isoformat(),
                  stop=float(stop), last_price=touch, meta=meta)
        self.open[sym] = p
        self._save(p, new=True)
        return True, (f"{STRATEGY_LABEL}: {sym} {side.value} {volume:g} lots at {price} stop {stop} "
                      f"(risk {per_lot * volume:.2f}, band {meta['band_lower']:.2f}-{meta['band_upper']:.2f})")

    def _close(self, p: Paper, level: float, reason: str, now: dt.datetime) -> str:
        spec = self.spec_fn(p.symbol)
        point = float(getattr(spec, "point", 0.0) or 0.0) if spec is not None else 0.0
        exit_px = level if "STOP" in reason else level - p.side.sign * self.cfg.paper_slippage_points * point
        net = float(spec.money((exit_px - p.entry) * p.side.sign, p.volume)) if spec is not None else 0.0
        with self._lock:
            self.db.execute("""UPDATE trades SET closed_utc=?, stop=?, exit_price=?, realised_r=?, net_pnl=?, commission=0,
                               exit_reason=?, duration_seconds=?, peak_r=? WHERE ticket=?""",
                            (to_utc(now).isoformat(), p.stop, exit_px, round(p.r_of(exit_px), 4), round(net, 2), reason,
                             (to_utc(now) - p.opened).total_seconds(), round(p.peak_r, 4), p.ticket))
            self.db.commit()
        self.open.pop(p.symbol, None)
        return f"{STRATEGY_LABEL}: {p.symbol} {p.side.value} closed {reason} at {exit_px}: {net:+.2f}"

    def _save(self, p: Paper, new: bool = False) -> None:
        with self._lock:
            if new:
                self.db.execute("""INSERT INTO trades (ticket, symbol, side, volume, entry, initial_stop, opened_utc, stop, mode,
                                   session_date, band_upper, band_lower, vwap_at_entry, sigma, day_open, prev_close, peak_r)
                                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)""",
                                (p.ticket, p.symbol, p.side.value, p.volume, p.entry, p.initial_stop, p.opened.isoformat(),
                                 p.stop, self.cfg.mode, p.session_date, p.meta.get("band_upper"), p.meta.get("band_lower"),
                                 p.meta.get("vwap_at_entry"), p.meta.get("sigma"), p.meta.get("day_open"), p.meta.get("prev_close")))
            else:
                self.db.execute("UPDATE trades SET stop=?, peak_r=? WHERE ticket=?", (p.stop, round(p.peak_r, 4), p.ticket))
            self.db.commit()

    def _restore(self) -> None:
        for r in self.db.execute("SELECT * FROM trades WHERE closed_utc IS NULL"):
            p = Paper(int(r["ticket"]), r["symbol"], Side(r["side"]), float(r["volume"]), float(r["entry"]),
                      float(r["initial_stop"]), to_utc(dt.datetime.fromisoformat(r["opened_utc"])), r["session_date"] or "",
                      stop=float(r["stop"]), peak_r=float(r["peak_r"] or 0))
            self.open[p.symbol] = p

    # --------------------------------------------------------------- status --
    def closed_since(self, since: dt.datetime) -> list[dict]:
        return [dict(r) for r in self.db.execute(
            "SELECT * FROM trades WHERE closed_utc IS NOT NULL AND closed_utc >= ? ORDER BY closed_utc ASC",
            (to_utc(since).isoformat(),))]

    def open_rows(self) -> list[dict]:
        out = []
        for p in self.open.values():
            spec = self.spec_fn(p.symbol)
            pnl = float(spec.money((p.last_price - p.entry) * p.side.sign, p.volume)) if spec is not None and p.last_price else 0.0
            out.append({"ticket": p.ticket, "symbol": p.symbol, "side": p.side.value, "entry": p.entry, "stop": p.stop,
                        "initial_stop": p.initial_stop, "r_now": round(p.r_of(p.last_price), 2) if p.last_price else 0.0,
                        "peak_r": round(p.peak_r, 2), "pnl": round(pnl, 2), "opened": p.opened.isoformat()})
        return out

    def status(self, now: Optional[dt.datetime] = None) -> dict:
        now = to_utc(now or self.clock())
        return {"strategy_id": STRATEGY_ID, "label": STRATEGY_LABEL, "tagline": TAGLINE,
                "mode": self.cfg.mode if self.cfg.enabled else "OFF",
                "status": ("OFF" if not self.cfg.enabled else "IN TRADE" if self.open else
                           ("WATCHING" if self.in_session(now) else "OUTSIDE SESSION")),
                "updated": now.isoformat(), "markets": list(self.cfg.markets),
                "session": f"{self.cfg.session_open}-{self.cfg.session_close} New York, flat by {self.flat_time().strftime('%H:%M')}",
                "rule": f"average noise of the last {self.cfg.lookback_days} sessions; checks every {self.cfg.check_minutes} min "
                        f"from {self.cfg.first_check}; at most {self.cfg.max_entries_per_day} entries a day per market; "
                        f"{self.cfg.risk_money:.0f} at the stop",
                "markets_now": dict(self.notes_by_market), "open": self.open_rows(),
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
