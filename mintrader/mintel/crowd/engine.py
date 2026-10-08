"""Crowd Fader engine: reads positioning, waits for the price to turn, keeps
paper positions with a structural stop and a liquidation-cluster target.

Runs inside the main bot's loop like the Band Breaker, one ``step`` per
pass. The Coinversa calls happen on their own thread so the trading loop
never waits on the internet; a step only ever reads the latest result.
Pessimistic by construction: fills at the touch plus slippage, a stop
closes AT the stop, a target AT the target, a buy is marked on the bid
and a sell on the ask. Nothing here touches the broker's orders.
"""
from __future__ import annotations

import datetime as dt
import itertools
import json
import logging
import sqlite3
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from ..broker.base import Side, TF
from ..clock import to_utc, utcnow
from . import STRATEGY_ID, STRATEGY_LABEL, TAGLINE
from .client import CoinversaClient, CoinversaError, read_api_key
from .signal import Read, summarise

log = logging.getLogger("mintel.crowd")

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
    mode: str = "PAPER"
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
class Paper:
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
    meta: dict = field(default_factory=dict)

    def r_of(self, price: float) -> float:
        d = abs(self.entry - self.initial_stop)
        return ((price - self.entry) * self.side.sign) / d if d else 0.0


def _hhmm(t: str) -> dt.time:
    h, m = str(t).split(":")
    return dt.time(int(h), int(m))


class CrowdFader:
    def __init__(self, data_dir: str | Path, cfg: Optional[CrowdConfig] = None,
                 client: Optional[CoinversaClient] = None, spec_fn: Optional[Callable[[str], object]] = None,
                 clock: Callable[[], dt.datetime] = utcnow, threaded: bool = True, api_key: Optional[str] = None):
        self.cfg = cfg or CrowdConfig()
        self.spec_fn = spec_fn or (lambda s: None)
        self.clock = clock
        self.threaded = threaded
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.status_path = self.data_dir / "crowd-status.json"
        self._lock = threading.Lock()
        self.db = sqlite3.connect(str(self.data_dir / "crowd.sqlite"), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
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
        self.open: dict[str, Paper] = {}
        self.entries_today: dict[tuple[str, str], int] = {}
        self.acted_on: dict[str, str] = {}
        self.notes_by_market: dict[str, str] = {}
        self.last_poll: Optional[dt.datetime] = None
        self.poll_error: str = ""
        self.polls = 0
        self._poll_thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._seq = itertools.count(int(self.db.execute("SELECT COALESCE(MAX(ticket), 800000000) FROM trades").fetchone()[0]) + 1)
        self._last_status = 0.0
        self._restore()

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
        if self.threaded and self.client is not None and self._poll_thread is None:
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

    def step(self, now: dt.datetime, tick_fn: Callable[[str], object],
             bars_fn: Callable[[str, TF, int], list]) -> list[str]:
        notes: list[str] = []
        if not self.cfg.enabled:
            return notes
        now = to_utc(now)
        if self.threaded:
            self.start()
        elif self.poll_due(now):
            notes += self.poll_once(now)
        today = now.date()
        weekend_flat = now.weekday() == 4 and now.time() >= _hhmm(self.cfg.weekend_flat_utc)
        for sym, coin in self.cfg.mapping().items():
            tick = tick_fn(sym)
            if tick is None:
                self.notes_by_market[sym] = "no price from the broker"
                continue
            bid, ask = float(tick.bid), float(tick.ask)
            if ask <= 0 or bid <= 0:
                continue
            p = self.open.get(sym)
            if p is not None:
                mark = bid if p.side is Side.BUY else ask
                p.last_price = mark
                p.peak_r = max(p.peak_r, p.r_of(mark))
                hit_stop = mark <= p.stop if p.side is Side.BUY else mark >= p.stop
                hit_target = mark >= p.target if p.side is Side.BUY else mark <= p.target
                held = (now - p.opened).total_seconds() / 3600.0
                if hit_stop:
                    notes.append(self._close(p, p.stop, "STOP", now))
                elif hit_target:
                    notes.append(self._close(p, p.target, "TARGET", now))
                elif held >= self.cfg.hold_hours:
                    notes.append(self._close(p, mark, "TIME", now))
                elif weekend_flat:
                    notes.append(self._close(p, mark, "WEEKEND", now))
                else:
                    self._save(p)
                    self.notes_by_market[sym] = (f"in trade: {p.side.value} from {p.entry}, {p.r_of(mark):+.2f} R, "
                                                 f"stop {p.stop}, target {p.target}")
                continue
            r = self.latest.get(sym)
            if r is None:
                self.notes_by_market[sym] = "no read from Coinversa yet" if self.client is not None else "no Coinversa key saved"
                continue
            age_min = (now - r.ts).total_seconds() / 60.0
            if age_min > 3 * self.cfg.poll_minutes + 1:
                self.notes_by_market[sym] = f"last read {age_min:.0f} min old: waiting for a fresh one"
                continue
            if r.direction == 0 or r.score < self.cfg.min_score:
                self.notes_by_market[sym] = r.reason
                continue
            if not self.in_entry_hours(now):
                self.notes_by_market[sym] = f"{r.reason}; outside entry hours"
                continue
            if self.entries_today.get((sym, today.isoformat()), 0) >= self.cfg.max_entries_per_day:
                self.notes_by_market[sym] = f"{r.reason}; no more entries today"
                continue
            if self.acted_on.get(sym) == r.ts.isoformat():
                self.notes_by_market[sym] = f"{r.reason}; already acted on this read"
                continue
            ok, why = self._try_enter(sym, coin, r, now, bid, ask, bars_fn)
            self.notes_by_market[sym] = why
            if ok:
                self.acted_on[sym] = r.ts.isoformat()
                self.entries_today[(sym, today.isoformat())] = self.entries_today.get((sym, today.isoformat()), 0) + 1
                notes.append(why)
        self.write_status(now)
        return notes

    def _try_enter(self, sym: str, coin: str, r: Read, now: dt.datetime, bid: float, ask: float,
                   bars_fn: Callable[[str, TF, int], list]) -> tuple[bool, str]:
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
        return self._open(sym, coin, side, touch, stop, target, now, r)

    # --------------------------------------------------------------- paper --
    def _open(self, sym: str, coin: str, side: Side, touch: float, stop: float, target: float,
              now: dt.datetime, r: Read) -> tuple[bool, str]:
        spec = self.spec_fn(sym)
        if spec is None:
            return False, f"{STRATEGY_LABEL}: {sym} has no contract specification"
        point = float(getattr(spec, "point", 0.0) or 0.0)
        price = spec.normalise_price(touch + side.sign * self.cfg.paper_slippage_points * point)
        stop = spec.normalise_price(stop)
        target = spec.normalise_price(target)
        dist = abs(price - stop)
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
        p = Paper(next(self._seq), sym, coin, side, float(volume), float(price), float(stop), float(target), to_utc(now),
                  now.date().isoformat(), stop=float(stop), last_price=touch,
                  meta={"score": r.score, "crowd_bias": r.crowd_bias, "winners_bias": r.winners_bias,
                        "fuel_share": r.fuel_share, "read_ts": r.ts.isoformat(), "reason": r.reason, "risk_money": risk})
        self.open[sym] = p
        self._save(p, new=True)
        rr = abs(target - price) / dist if dist else 0.0
        return True, (f"{STRATEGY_LABEL}: {sym} {side.value} {volume:g} lots at {price} stop {stop} target {target} "
                      f"({rr:.1f} R; risk {risk:.2f}; {r.reason})")

    def _close(self, p: Paper, level: float, reason: str, now: dt.datetime) -> str:
        spec = self.spec_fn(p.symbol)
        point = float(getattr(spec, "point", 0.0) or 0.0) if spec is not None else 0.0
        exit_px = level if reason in ("STOP", "TARGET") else level - p.side.sign * self.cfg.paper_slippage_points * point
        net = float(spec.money((exit_px - p.entry) * p.side.sign, p.volume)) if spec is not None else 0.0
        with self._lock:
            self.db.execute("""UPDATE trades SET closed_utc=?, stop=?, exit_price=?, realised_r=?, net_pnl=?, commission=0,
                               exit_reason=?, duration_seconds=?, peak_r=? WHERE ticket=?""",
                            (to_utc(now).isoformat(), p.stop, exit_px, round(p.r_of(exit_px), 4), round(net, 2), reason,
                             (to_utc(now) - p.opened).total_seconds(), round(p.peak_r, 4), p.ticket))
            self.db.commit()
        self.open.pop(p.symbol, None)
        self.notes_by_market[p.symbol] = f"closed {reason} at {exit_px}: {net:+.2f}"
        return f"{STRATEGY_LABEL}: {p.symbol} {p.side.value} closed {reason} at {exit_px}: {net:+.2f}"

    def _save(self, p: Paper, new: bool = False) -> None:
        with self._lock:
            if new:
                self.db.execute("""INSERT INTO trades (ticket, symbol, coin, side, volume, entry, initial_stop, target, opened_utc,
                                   stop, mode, session_date, score, crowd_bias, winners_bias, fuel_share, read_ts, reason, peak_r,
                                   risk_money)
                                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?)""",
                                (p.ticket, p.symbol, p.coin, p.side.value, p.volume, p.entry, p.initial_stop, p.target,
                                 p.opened.isoformat(), p.stop, self.cfg.mode, p.session_date, p.meta.get("score"),
                                 p.meta.get("crowd_bias"), p.meta.get("winners_bias"), p.meta.get("fuel_share"),
                                 p.meta.get("read_ts"), p.meta.get("reason"), p.meta.get("risk_money")))
            else:
                self.db.execute("UPDATE trades SET stop=?, peak_r=? WHERE ticket=?", (p.stop, round(p.peak_r, 4), p.ticket))
            self.db.commit()

    def _restore(self) -> None:
        for sym, day, n in self.db.execute("SELECT symbol, session_date, COUNT(*) FROM trades GROUP BY symbol, session_date"):
            if sym and day:
                self.entries_today[(sym, day)] = int(n)
        for r in self.db.execute("SELECT * FROM trades WHERE closed_utc IS NULL"):
            p = Paper(int(r["ticket"]), r["symbol"], r["coin"] or "", Side(r["side"]), float(r["volume"]), float(r["entry"]),
                      float(r["initial_stop"]), float(r["target"] or 0.0), to_utc(dt.datetime.fromisoformat(r["opened_utc"])),
                      r["session_date"] or "", stop=float(r["stop"]), peak_r=float(r["peak_r"] or 0))
            self.open[p.symbol] = p
        for sym in self.cfg.mapping():
            row = self.db.execute("SELECT * FROM checks WHERE symbol=? ORDER BY ts_utc DESC LIMIT 1", (sym,)).fetchone()
            if row is not None:
                self.latest[sym] = _read_from_row(dict(row))

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
            pnl = float(spec.money((p.last_price - p.entry) * p.side.sign, p.volume)) if spec is not None and p.last_price else 0.0
            out.append({"ticket": p.ticket, "symbol": p.symbol, "side": p.side.value, "entry": p.entry, "stop": p.stop,
                        "target": p.target, "initial_stop": p.initial_stop,
                        "r_now": round(p.r_of(p.last_price), 2) if p.last_price else 0.0,
                        "peak_r": round(p.peak_r, 2), "pnl": round(pnl, 2), "opened": p.opened.isoformat()})
        return out

    def status(self, now: Optional[dt.datetime] = None) -> dict:
        now = to_utc(now or self.clock())
        if not self.cfg.enabled:
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
        return {"strategy_id": STRATEGY_ID, "label": STRATEGY_LABEL, "tagline": TAGLINE,
                "mode": self.cfg.mode if self.cfg.enabled else "OFF", "status": state, "updated": now.isoformat(),
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
