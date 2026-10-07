"""The Momentum Runner shadow: paper trades that mirror the main bot's index
entries and ride them with a 3 R trail.

Pessimistic by construction: a shadow closes AT its stop level (never
better), a buy is marked on the bid and a sell on the ask, and the stop only
ever moves in the trade's favour. Nothing here touches the broker.
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional, Sequence

from ..broker.base import Side
from ..clock import to_utc, utcnow
from ..contracts import infer_group
from . import STRATEGY_ID, STRATEGY_LABEL, TAGLINE

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    ticket INTEGER PRIMARY KEY,          -- the real trade's ticket: one shadow per trade
    symbol TEXT, side TEXT, volume REAL,
    entry REAL, initial_stop REAL, risk_distance REAL,
    opened_utc TEXT, closed_utc TEXT,
    stop REAL, peak_price REAL, peak_r REAL,
    exit_price REAL, realised_r REAL, net_pnl REAL, commission REAL,
    exit_reason TEXT, mode TEXT, duration_seconds REAL, tactic TEXT
);
CREATE INDEX IF NOT EXISTS ix_runner_closed ON trades(closed_utc);
"""


@dataclass
class RunnerConfig:
    enabled: bool = True
    mode: str = "PAPER"                 # PAPER only for now; LIVE is not implemented
    trail_r: float = 3.0                # once this far ahead, trail this far behind the best price
    window_hours: float = 8.0           # out at the price after this long (the replay's window)
    markets: tuple[str, ...] = ()       # empty = every index market the main bot trades


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

    @property
    def risk_distance(self) -> float:
        return abs(self.entry - self.initial_stop)

    def r_of(self, price: float) -> float:
        return ((price - self.entry) * self.side.sign) / self.risk_distance if self.risk_distance else 0.0


class MomentumRunner:
    def __init__(self, data_dir: str | Path, cfg: Optional[RunnerConfig] = None,
                 spec_fn: Optional[Callable[[str], object]] = None,
                 clock: Callable[[], dt.datetime] = utcnow):
        self.cfg = cfg or RunnerConfig()
        self.spec_fn = spec_fn or (lambda s: None)
        self.clock = clock
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.data_dir / "runner.sqlite"
        self.status_path = self.data_dir / "runner-status.json"
        self._lock = threading.Lock()
        self.db = sqlite3.connect(str(self.path), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self.open: dict[int, Shadow] = {}
        self._last_status = 0.0
        self._restore()

    # ------------------------------------------------------------ helpers --
    def wants(self, symbol: str) -> bool:
        if self.cfg.markets:
            return str(symbol).upper() in {m.upper() for m in self.cfg.markets}
        return infer_group(str(symbol)) == "INDEX"

    def _restore(self) -> None:
        for r in self.db.execute("SELECT * FROM trades WHERE closed_utc IS NULL"):
            s = Shadow(int(r["ticket"]), r["symbol"], Side(r["side"]), float(r["volume"]), float(r["entry"]),
                       float(r["initial_stop"]), to_utc(dt.datetime.fromisoformat(r["opened_utc"])),
                       tactic=r["tactic"] or "", stop=float(r["stop"]), peak_price=float(r["peak_price"] or 0),
                       peak_r=float(r["peak_r"] or 0))
            self.open[s.ticket] = s

    def _save(self, s: Shadow) -> None:
        with self._lock:
            self.db.execute("""INSERT INTO trades (ticket, symbol, side, volume, entry, initial_stop, risk_distance,
                               opened_utc, stop, peak_price, peak_r, mode, tactic) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                               ON CONFLICT(ticket) DO UPDATE SET stop=excluded.stop, peak_price=excluded.peak_price,
                               peak_r=excluded.peak_r""",
                            (s.ticket, s.symbol, s.side.value, s.volume, s.entry, s.initial_stop, s.risk_distance,
                             s.opened.isoformat(), s.stop, s.peak_price, s.peak_r, self.cfg.mode, s.tactic))
            self.db.commit()

    # --------------------------------------------------------------- flow --
    def adopt(self, position, initial_stop: float, now: dt.datetime, tactic: str = "") -> Optional[Shadow]:
        """Start a shadow for a real trade, once. Returns it, or None if not wanted."""
        if not self.cfg.enabled or position.ticket in self.open or not self.wants(position.symbol):
            return None
        if self.db.execute("SELECT 1 FROM trades WHERE ticket=?", (position.ticket,)).fetchone():
            return None                                  # already finished with this one
        stop = float(initial_stop or position.sl or 0.0)
        if stop <= 0 or abs(position.entry_price - stop) <= 0:
            return None
        s = Shadow(position.ticket, position.symbol, position.side, float(position.volume),
                   float(position.entry_price), stop, to_utc(position.open_time or now), tactic=tactic, stop=stop)
        self.open[s.ticket] = s
        self._save(s)
        return s

    def update(self, s: Shadow, bid: float, ask: float, now: dt.datetime) -> Optional[str]:
        """One price update. Returns the exit reason when the shadow closes."""
        sign = s.side.sign
        price = bid if s.side is Side.BUY else ask            # what we could close at
        s.last_price = price
        dist = s.risk_distance
        # the stop first: a touch closes AT the stop, never better
        if (price - s.stop) * sign <= 0:
            return self._close(s, s.stop, "INITIAL_STOP" if abs(s.stop - s.initial_stop) < 1e-12 else "TRAIL_STOP", now)
        if (now - s.opened).total_seconds() >= self.cfg.window_hours * 3600:
            return self._close(s, price, "WINDOW_END", now)
        if (price - s.peak_price) * sign > 0 or s.peak_price == 0.0:
            s.peak_price = price
            s.peak_r = max(s.peak_r, s.r_of(price))
        if s.peak_r >= self.cfg.trail_r:
            cand = s.peak_price - sign * self.cfg.trail_r * dist
            if (cand - s.stop) * sign > 0:                  # only ever in the trade's favour
                spec = self.spec_fn(s.symbol)
                if spec is not None and hasattr(spec, "normalise_price"):
                    cand = spec.normalise_price(cand)
                s.stop = cand
        self._save(s)
        return None

    def _close(self, s: Shadow, exit_price: float, reason: str, now: dt.datetime) -> str:
        spec = self.spec_fn(s.symbol)
        move = (exit_price - s.entry) * s.side.sign
        net = float(spec.money(move, s.volume)) if spec is not None and hasattr(spec, "money") else 0.0
        commission = 0.0                                   # indices: none at this broker
        with self._lock:
            self.db.execute("""UPDATE trades SET closed_utc=?, stop=?, peak_price=?, peak_r=?, exit_price=?,
                               realised_r=?, net_pnl=?, commission=?, exit_reason=?, duration_seconds=? WHERE ticket=?""",
                            (to_utc(now).isoformat(), s.stop, s.peak_price, s.peak_r, exit_price,
                             round(s.r_of(exit_price), 4), round(net - commission, 2), commission, reason,
                             (to_utc(now) - s.opened).total_seconds(), s.ticket))
            self.db.commit()
        self.open.pop(s.ticket, None)
        return reason

    def observe(self, positions: Sequence, initial_stops: dict, tick_fn: Callable[[str], object],
                now: dt.datetime, tactics: Optional[dict] = None) -> list[str]:
        """One pass: adopt new index trades, then move every open shadow on."""
        notes: list[str] = []
        if not self.cfg.enabled:
            return notes
        for p in positions:
            try:
                s = self.adopt(p, float(initial_stops.get(p.ticket) or p.sl or 0.0), now,
                               tactic=str((tactics or {}).get(p.ticket) or ""))
                if s is not None:
                    notes.append(f"{STRATEGY_LABEL}: shadowing {s.symbol} {s.side.value} from {s.entry} (stop {s.stop})")
            except Exception as exc:                       # never let the shadow disturb the real bot
                notes.append(f"{STRATEGY_LABEL}: could not shadow {getattr(p, 'symbol', '?')}: {exc}")
        for s in list(self.open.values()):
            try:
                t = tick_fn(s.symbol)
                if t is None:
                    continue
                reason = self.update(s, float(t.bid), float(t.ask), now)
                if reason:
                    notes.append(f"{STRATEGY_LABEL}: {s.symbol} shadow closed {reason} at {s.stop if 'STOP' in reason else s.last_price}")
            except Exception as exc:
                notes.append(f"{STRATEGY_LABEL}: {s.symbol} shadow error {exc}")
        self.write_status(now)
        return notes

    # ------------------------------------------------------------- status --
    def closed_since(self, since: dt.datetime) -> list[dict]:
        rows = self.db.execute("SELECT * FROM trades WHERE closed_utc IS NOT NULL AND closed_utc >= ? "
                               "ORDER BY closed_utc ASC", (to_utc(since).isoformat(),)).fetchall()
        return [dict(r) for r in rows]

    def open_rows(self) -> list[dict]:
        out = []
        for s in self.open.values():
            spec = self.spec_fn(s.symbol)
            pnl = (float(spec.money((s.last_price - s.entry) * s.side.sign, s.volume))
                   if spec is not None and s.last_price and hasattr(spec, "money") else 0.0)
            out.append({"ticket": s.ticket, "symbol": s.symbol, "side": s.side.value, "entry": s.entry,
                        "stop": s.stop, "initial_stop": s.initial_stop, "peak_r": round(s.peak_r, 2),
                        "r_now": round(s.r_of(s.last_price), 2) if s.last_price else 0.0, "pnl": round(pnl, 2),
                        "opened": s.opened.isoformat(), "trailing": s.peak_r >= self.cfg.trail_r})
        return out

    def status(self, now: Optional[dt.datetime] = None) -> dict:
        now = to_utc(now or self.clock())
        return {"strategy_id": STRATEGY_ID, "label": STRATEGY_LABEL, "tagline": TAGLINE,
                "mode": self.cfg.mode if self.cfg.enabled else "OFF",
                "status": ("IN TRADE" if self.open else "WATCHING") if self.cfg.enabled else "OFF",
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
