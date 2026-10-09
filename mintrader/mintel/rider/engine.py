"""RiderEngine: RiderCore wired to the broker.

Every pass (about once a second) it:

1. refreshes the account, the market list and the contract specs (every few
   minutes) and the MetaTrader calendar;
2. reads prices for every market (``broker.ticks`` when the bridge offers
   it, else ``broker.tick``), and with ``tick_history`` every tick since the
   last pass (``ticks_range``) - the same ticks a replay would feed (the
   latest price is not fed ahead of the history, so no tick is skipped);
3. LIVE: reconciles with the broker (a position gone is finalised from the
   broker's own closing deal; a position carrying magic 990811 with no row
   here is CLOSED as an orphan - never adopted; a position whose broker
   stop is missing or looser than FlowLock X's gets the stop sent again, and
   is closed at market if the broker will not hold it); not LIVE: closes
   anything LIVE left behind at the broker (booked at the close price,
   marked '?', while its deal is not readable; a LIVE trade gone from the
   broker's list with no deal - for a minute over several reads, as in
   LIVE - is estimated at the worse of its stop and the price), and reads
   the broker's list again once a minute after that; a refused close of
   an orphan or a leftover is tried again a minute later while the FX
   market is open, not on every pass; in
   every mode a LIVE estimate takes the broker's figure (and its story is
   written again) once its deal comes in. This bookkeeping never stops the
   pass: a part that fails (a database that cannot be written, say) is
   noted and tried again, and step 4 runs all the same (a trade row that
   could not be written is kept and written on a later pass or minute);
4. manages every open position with FlowLock X - ALWAYS, even when a health
   check has failed: a stop move goes to the broker with ``modify_stop`` when
   ``RiderCore.stop_move_due`` says it is worth sending (a minimum step and
   a minimum time between moves; a refusal is rolled back and tried again
   after that wait), an exit with ``close`` (a refusal keeps the position,
   with its broker stop, and retries); a stop FlowLock X sees reached that
   the broker still has not taken a pass later is closed at market; PAPER
   checks every new tick against the stop in force at that tick's time, as
   the replay does (a tick from before the entry price is not checked), and
   books a close on such a tick with the stop it met and the best and worst
   prices seen up to it;
5. runs the health checks; a failed CRITICAL check stops NEW entries only;
6. scans, and when healthy opens the core's entry intents: size from
   ``user_pip_value_gbp`` (never changed by the bot), the stop at the broker
   with the order, one position per market, a sanity cap on concurrent
   positions, re-entry only on a fresh trigger after a short cooldown, no
   quotas and no daily caps;
7. writes data/rider-status.json atomically (every ~2 s) and a heartbeat.

The executor is the shared one (mintel/botexec.py): PAPER simulates
pessimistically, LIVE sends real orders with magic 990811 and fails closed
when the broker does not hold the stop. In LIVE the money figure for a
closed trade is the broker's (closing deals + entry commission + swap).
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import shutil
import sqlite3
import time
from collections import Counter, deque
from pathlib import Path
from typing import Callable, Optional

from ..botexec import Fill, LiveExecutor, make_executor
from ..broker.base import Position, Side, TF, Tick
from ..clock import fx_market_open, to_utc, utcnow
from ..contracts import FX_CURRENCIES, classify_fx
from ..ops.health import Heartbeat
from . import MAGIC, STRATEGY_ID, STRATEGY_LABEL, TAG, TAGLINE
from .config import MODES, RiderConfig
from .core import EntryIntent, RiderCore, RiderPosition, ScanRow
from .features import ORDER_FLOW_AVAILABLE
from .flowlock_x import PLAIN, RUNNER, FlowState, never_widen
from .journal import RiderJournal, to_json
from .learning import BucketStats, Outcome
from .pipvalue import fx_pip_size

log = logging.getLogger("mintel.rider")

PAPER_FIRST_TICKET = 810_000_001
MISSING_PASSES = 3
MISSING_SECONDS = 60.0
CLOSE_RETRY_SECONDS = 60.0              # a refused close of an orphan or a leftover: tried again this much later
ESTIMATE_WINDOW_HOURS = 24.0
FLOWLOCK_STALE_SECONDS = 10.0
TICK_HISTORY_MAX_SECONDS = 120.0
TICK_HISTORY_MARGIN_SECONDS = 1.0       # the history is asked a little past the latest price's time
TICK_HISTORY_WAIT_SECONDS = 5.0         # a history behind the latest price this long: the latest price is fed


def _px(t: Tick) -> tuple[float, float]:
    return float(t.bid), float(t.ask)


def _pair(sym: str) -> str:
    b, q, _ = classify_fx(sym)
    return f"{b}/{q}" if b else sym


def _money(x: Optional[float]) -> str:
    if x is None:
        return "?"
    return f"{'+' if x >= 0 else '-'}£{abs(x):.2f}"


def explain_trade(symbol: str, side: Side, family: str, visited: list, reason: str, realised_pips: float,
                  money: Optional[float], source: str, mode: str, initial_stop: float, final_stop: float,
                  news_name: str = "") -> str:
    """The trade, explained as to a child."""
    pair = _pair(symbol)
    long = side is Side.BUY
    move = "rising" if long else "falling"
    did = "bought" if long else "sold"
    openers = {
        "breakout": f"{pair} broke {'above' if long else 'below'} the edge of where it had been stuck and was moving fast, so the bot {did} it.",
        "squeeze_release": f"{pair} had been very quiet, then burst {'upward' if long else 'downward'}, so the bot {did} it.",
        "session_breakout": f"{pair} broke out of the range it had kept to earlier in the day, so the bot {did} it.",
        "opening_range": f"{pair} broke out of its range from the first minutes of the session, so the bot {did} it.",
        "pullback": f"{pair} was {move} strongly, paused for a moment, then started {move} again, so the bot {did} it.",
        "first_retest": f"{pair} broke through a level, came back to touch it once, then moved away again, so the bot {did} it.",
        "sweep": f"{pair} poked past an old {'low' if long else 'high'}, then snapped straight back, so the bot {did} it.",
        "news": f"After the {news_name or 'news'}, {pair} kept {move}, so the bot {did} it.",
    }
    text = openers.get(family, f"{pair} suddenly started {move} much faster than normal, so the bot {did} it.")
    moved = abs(final_stop - initial_stop) > 1e-12
    if reason == "THESIS_DEAD":
        text += " But the move did not carry on - it turned back - so the bot got out quickly."
    elif reason in ("STOP", "STOP?") and not moved:
        text += " But the price turned round and reached the safety stop."
    elif RUNNER in visited:
        text += (" It kept going fast, so the bot let it run and kept moving the safety stop behind the price "
                 "(it never moves it back).")
    elif moved:
        text += " It moved far enough for the bot to move the safety stop up behind it" if long else \
            " It moved far enough for the bot to move the safety stop down behind it"
        text += " (it never moves it back)."
    if reason.startswith("MOMENTUM_DIED"):
        end = "When the move ran out of puff, the bot closed it"
    elif reason.startswith("PROTECT_LEVEL") or (reason.startswith("STOP") and moved):
        end = "When the price came back to the stop, the trade closed"
    elif reason.startswith("THESIS_DEAD") or reason.startswith("STOP"):
        end = "It closed"
    elif reason.startswith("ORPHAN"):
        end = "It was closed because the bot had no record of it"
    elif reason.startswith("NO_BROKER_STOP"):
        end = "It was closed because the broker was not holding its safety stop"
    elif reason.startswith("SWITCHED"):
        end = "It was closed because the bot's mode was switched"
    else:
        end = "It closed"
    tag = ""
    if source == "estimate":
        tag = ", estimated until the broker's figure comes in"
    elif mode == "PAPER":
        tag = ", practice money"
    return f"{text} {end} at {realised_pips:+.1f} pips ({_money(money)}{tag})."


class RiderEngine:
    def __init__(self, cfg: RiderConfig, broker, data_dir: str | Path, *,
                 clock: Callable[[], dt.datetime] = utcnow, executor=None,
                 journal: Optional[RiderJournal] = None, heartbeat: bool = True):
        self.cfg = cfg
        self.broker = broker
        self.clock = clock
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.journal = journal or RiderJournal(self.data_dir / cfg.journal_file)
        self.learning = BucketStats(cfg.learning_min_samples, cfg.learning_window_trades, cfg.learning_min_days)
        self._load_learning()
        self.core = RiderCore(cfg, self.learning)
        self.mode = cfg.mode if cfg.mode in MODES else "PAPER"
        base = self.journal.max_ticket(PAPER_FIRST_TICKET - 1)
        if executor is not None:
            self.executor = executor
        else:
            # the paper slippage in broker points: 10 points a pip on 5- and 3-digit quotes
            self.executor = make_executor(self.mode, broker, MAGIC, TAG, cfg.slippage_pips * 10.0,
                                          max(base + 1, PAPER_FIRST_TICKET))
        self.positions: dict[int, RiderPosition] = {}
        self.universe: list[str] = []
        self.specs: dict[str, object] = {}
        self.latest: dict[str, Tick] = {}
        self.new_ticks: dict[str, list[Tick]] = {}
        self._last_tick_time: dict[str, dt.datetime] = {}
        self._fed_at_last: dict[str, Counter] = {}          # (bid, ask) of the ticks already fed at that time
        self._held_back: dict[str, tuple[Tick, dt.datetime]] = {}  # a latest price not yet in the history, since
        self._seeded: set[str] = set()
        self._t_specs = -1e18
        self._t_cal = -1e18
        self._t_scan = -1e18
        self._t_status = -1e18
        self._t_snapshot = -1e18
        self._t_account = -1e18
        self._t_reconnect = -1e18
        self._t_probe = -1e18
        self._minute = ""
        self.account = None
        self.account_ok = False
        self.calendar_ok = True
        self.rejects: deque = deque(maxlen=200)
        self.health: list[dict] = []
        self.entries_allowed = False
        self.notes: deque = deque(maxlen=40)
        self.last_rows: list[ScanRow] = []
        self.reconcile_ok = True
        self.last_manage: dict[int, float] = {}
        self.missing: dict[int, tuple[int, float]] = {}
        self.stop_seen: dict[int, float] = {}               # LIVE: FlowLock X saw the stop reached (time)
        self.stop_restored: dict[int, float] = {}           # LIVE: the stop was re-sent to the broker (time)
        self.tick_feed = "every tick" if cfg.tick_history else "snapshots"
        self._sweep_due = self.mode == "LIVE"
        self._leftovers_done = broker is None
        self._close_retry: dict[int, float] = {}            # a refused orphan/leftover close: when it is tried again
        self._leftover_missing: dict[int, tuple[int, float]] = {}   # not LIVE: a LIVE row not in the broker's list
        self._pending_rows: dict[int, dict] = {}            # a trade row that could not be written: written later
        self.status_path = self.data_dir / cfg.status_file
        self.hb = Heartbeat(self.data_dir / "heartbeats", "rider", clock) if heartbeat else None
        self.started = to_utc(self.clock())
        self.cycle_ms = 0.0
        self._restore()

    # ------------------------------------------------------------ helpers --
    @property
    def pip_value_gbp(self) -> float:
        """Aaron's exposure setting. Read-only: nothing in the bot sets it."""
        return float(self.cfg.user_pip_value_gbp)

    def _note(self, msg: str) -> str:
        if msg:
            self.notes.append(f"{to_utc(self.clock()).strftime('%H:%M:%S')} {msg}")
            log.info("%s", msg)
        return msg

    def _spec(self, symbol: str):
        s = self.specs.get(symbol)
        if s is None and self.broker is not None:
            try:
                s = self.broker.spec(symbol)
            except Exception:
                s = None
            if s is not None:
                self.specs[symbol] = s
                self.core.set_spec(symbol, s)
        return s

    @staticmethod
    def _snap(spec, price: float, side: Side) -> float:
        """Onto the tick grid, rounding AWAY from the market (a long's stop
        down, a short's up) so the snap can never bring it closer."""
        if spec is None:
            return price
        ts = float(getattr(spec, "tick_size", 0.0) or getattr(spec, "point", 0.0) or 0.0)
        if ts <= 0:
            return price
        import math
        k = price / ts
        k = math.floor(k + 1e-9) if side is Side.BUY else math.ceil(k - 1e-9)
        return round(k * ts, int(getattr(spec, "digits", 5)))

    def _load_learning(self) -> None:
        try:
            rows = list(reversed(self.journal.all_closed(self.cfg.learning_window_trades)))
        except Exception:
            rows = []
        for r in rows:
            try:
                b = json.loads(r.get("buckets_json") or "{}")
            except Exception:
                b = {}
            pips = self._net_pips_row(r)
            if pips is None or not b:
                continue
            self.learning.add(Outcome(str(r.get("closed_utc") or "")[:10], pips, b))

    @staticmethod
    def _net_pips_row(r: dict) -> Optional[float]:
        gpp = float(r.get("gbp_per_pip") or 0.0)
        if r.get("net_money") is not None and gpp > 0:
            return float(r["net_money"]) / gpp
        if r.get("realised_pips") is not None:
            return float(r["realised_pips"])
        return None

    # ------------------------------------------------------------- refresh --
    def _refresh_account(self, now: dt.datetime) -> None:
        t = now.timestamp()
        if self.broker is None or t - self._t_account < 10.0:
            return
        self._t_account = t
        try:
            self.account = self.broker.account()
            self.account_ok = self.account is not None
            if self.account is not None and getattr(self.account, "currency", ""):
                self.core.set_account_currency(self.account.currency)
        except Exception as exc:
            self.account_ok = False
            self._note(f"could not read the account: {exc}")

    def _refresh_universe(self, now: dt.datetime) -> list[str]:
        t = now.timestamp()
        if self.broker is None or (self.universe and t - self._t_specs < self.cfg.specs_refresh_seconds):
            return []
        self._t_specs = t
        notes: list[str] = []
        try:
            names = list(self.cfg.symbols) if self.cfg.symbols else list(self.broker.symbols())
        except Exception as exc:
            return [self._note(f"could not list the broker's markets: {exc}")]
        chosen: dict[tuple[str, str], str] = {}
        specs: dict[str, object] = {}
        for name in names:
            b, q, _ = classify_fx(name)
            if not b or b not in FX_CURRENCIES or q not in FX_CURRENCIES:
                continue
            try:
                spec = self.broker.spec(name)
            except Exception:
                spec = None
            if spec is None or not spec.is_tradable():
                continue
            pip, _why = fx_pip_size(spec)
            if pip <= 0:
                continue
            key = (b, q)
            if key in chosen and len(chosen[key]) <= len(name):
                continue
            chosen[key] = name
            specs[name] = spec
        uni = sorted(chosen.values())
        for p in self.positions.values():
            if p.symbol not in specs:
                sp = self._spec(p.symbol)
                if sp is not None:
                    specs[p.symbol] = sp
                    if p.symbol not in uni:
                        uni.append(p.symbol)
        for name, spec in specs.items():
            self.specs[name] = spec
            self.core.set_spec(name, spec)
            try:
                self.broker.ensure_selected(name)
            except Exception:
                pass
        if uni != self.universe:
            notes.append(self._note(f"watching {len(uni)} FX markets"))
        self.universe = uni
        for name in uni:
            if name in self._seeded:
                continue
            self._seeded.add(name)
            for tf, n in ((TF.M1, self.cfg.seed_bars_m1), (TF.M5, self.cfg.seed_bars_m5)):
                try:
                    bars = list(self.broker.bars(name, tf, int(n)) or [])
                    self.core.seed_bars(name, tf, bars, now)
                except Exception:
                    pass
        return notes

    def _fetch_ticks(self, now: dt.datetime) -> None:
        self.new_ticks = {}
        if self.broker is None or not self.universe:
            return
        latest: dict[str, Optional[Tick]] = {}
        fn = getattr(self.broker, "ticks", None)
        try:
            if fn is not None:
                latest = dict(fn(list(self.universe)) or {})
            else:
                for s in self.universe:
                    latest[s] = self.broker.tick(s)
        except Exception:
            for s in self.universe:
                try:
                    latest[s] = self.broker.tick(s)
                except Exception:
                    latest[s] = None
        hist_fn = getattr(self.broker, "ticks_range", None) if self.cfg.tick_history else None
        for s in self.universe:
            fresh: list[Tick] = []
            last_t = self._last_tick_time.get(s)
            tk = latest.get(s)
            history_ok = False
            if hist_fn is not None and last_t is not None:
                start = max(last_t, now - dt.timedelta(seconds=TICK_HISTORY_MAX_SECONDS))
                # up to the latest price's own time too (the broker's clock can run ahead of this one),
                # plus a margin so a history cut at whole seconds does not stop short of it
                end = max(now, to_utc(tk.time)) if tk is not None else now
                try:
                    fresh = [x for x in (hist_fn(s, start, end + dt.timedelta(seconds=TICK_HISTORY_MARGIN_SECONDS))
                                         or []) if x.time >= last_t]
                    history_ok = True
                except Exception:
                    fresh = []
            # with a working history the latest price is NOT fed: fed ahead of the history, the ticks
            # before it would be skipped next pass. It is used for prices now and the history brings
            # it, in order. Only when the history has still not reached a price held back
            # TICK_HISTORY_WAIT_SECONDS ago is the latest price fed, as with no history at all.
            feed_latest = not history_ok
            if history_ok and tk is not None:
                held = self._held_back.get(s)
                if held is not None and self._history_reached(held[0], fresh):
                    held = None
                if self._history_behind(s, tk, fresh, last_t):
                    held = held or (tk, now)
                    feed_latest = (now - held[1]).total_seconds() >= TICK_HISTORY_WAIT_SECONDS
                if held is None:
                    self._held_back.pop(s, None)
                else:
                    self._held_back[s] = held
            if tk is not None and feed_latest and (not fresh or tk.time > fresh[-1].time or
                                                   (tk.time == fresh[-1].time and
                                                    _px(tk) not in {_px(x) for x in fresh if x.time == tk.time})):
                fresh.append(tk)                           # the latest price, unless the history already has it
            # the history is read again from the newest millisecond fed: a tick of that millisecond
            # already fed on an earlier pass is not fed again, so the core sees each tick once (as the replay)
            done = Counter(self._fed_at_last.get(s) or ())
            feed: list[Tick] = []
            for x in fresh:
                if last_t is not None and x.time == last_t and done[_px(x)] > 0:
                    done[_px(x)] -= 1
                    continue
                feed.append(x)
            added = [x for x in feed if self.core.on_tick(s, x)]
            t_new = added[-1].time if added else last_t
            if t_new is not None:
                fed = Counter(self._fed_at_last.get(s) or ()) if t_new == last_t else Counter()
                fed.update(_px(x) for x in feed if x.time == t_new)
                self._fed_at_last[s] = fed
            if added:
                self._last_tick_time[s] = added[-1].time
                self.latest[s] = tk if tk is not None and tk.time > added[-1].time else added[-1]
            elif tk is not None:
                self.latest[s] = tk
                if s not in self._last_tick_time:
                    self._last_tick_time[s] = tk.time
            self.new_ticks[s] = added

    def _history_behind(self, s: str, tk: Tick, fresh: list[Tick], last_t: Optional[dt.datetime]) -> bool:
        """True when the latest price ``tk`` is not in the tick history yet
        (nor already fed): newer than the newest tick, or of that millisecond
        with prices it does not hold."""
        newest = fresh[-1].time if fresh else last_t
        if newest is None or tk.time > newest:
            return True
        if tk.time < newest or _px(tk) in {_px(x) for x in fresh if x.time == tk.time}:
            return False
        return not (tk.time == last_t and (self._fed_at_last.get(s) or Counter())[_px(tk)] > 0)

    @staticmethod
    def _history_reached(tk: Tick, fresh: list[Tick]) -> bool:
        """The history holds ``tk`` or a newer tick."""
        return bool(fresh) and (fresh[-1].time > tk.time or
                                any(x.time == tk.time and _px(x) == _px(tk) for x in fresh))

    def _refresh_calendar(self, now: dt.datetime) -> None:
        t = now.timestamp()
        if self.broker is None or t - self._t_cal < self.cfg.calendar_refresh_seconds:
            return
        self._t_cal = t
        try:
            events = self.broker.calendar(now - dt.timedelta(minutes=self.cfg.news_window_minutes + 5),
                                          now + dt.timedelta(hours=12), list(FX_CURRENCIES))
            self.core.set_calendar(list(events or []))
            self.calendar_ok = True
        except Exception as exc:
            self.calendar_ok = False
            self._note(f"could not read the calendar: {exc}")

    # ---------------------------------------------------------------- step --
    def step(self) -> list[str]:
        """One pass. Returns plain-English notes."""
        m0 = time.monotonic()
        now = to_utc(self.clock())
        notes: list[str] = []
        self._refresh_account(now)
        notes += self._refresh_universe(now)
        self._fetch_ticks(now)
        self._refresh_calendar(now)
        minute = now.strftime("%Y-%m-%d %H:%M")
        # the bookkeeping below never stops management: each part that fails is noted and tried again,
        # and the open positions are managed all the same
        if self._pending_rows and minute != self._minute:
            notes += self._guarded("writing the records", self._write_pending, now)
        if self.mode == "LIVE":
            if self._sweep_due or minute != self._minute or self._close_retry_due(now):
                notes += self._guarded("the orphan check", self._sweep_orphans, now)
                notes += self._guarded("the estimate check", self._correct_estimates, now)
            notes += self._guarded("the check against the broker", self._reconcile, now)
        else:
            # every minute even with nothing left: one empty answer from the broker is not proof
            if not self._leftovers_done or minute != self._minute:
                notes += self._guarded("closing what LIVE left", self._close_live_leftovers, now)
            if minute != self._minute and self.broker is not None:
                # a LIVE trade booked as an estimate takes the broker's figure in any mode (reads only)
                notes += self._guarded("the estimate check", self._correct_estimates, now,
                                       LiveExecutor(self.broker, MAGIC, TAG))
        self._minute = minute
        notes += self._manage(now)
        self.check_health(now)
        t = now.timestamp()
        if t - self._t_scan >= self.cfg.scan_interval_seconds - 1e-9:
            self._t_scan = t
            try:
                self.last_rows = self.core.scan(now)
            except Exception as exc:
                self.last_rows = []
                notes.append(self._note(f"scan failed: {exc}"))
            if self.executor is not None and self.entries_allowed:
                try:
                    intents = self.core.entries(now)
                except Exception as exc:
                    intents = []
                    notes.append(self._note(f"entry check failed: {exc}"))
                for intent in intents:
                    notes.append(self._enter(intent, now))
            if t - self._t_snapshot >= self.cfg.scan_snapshot_seconds and self.last_rows:
                self._t_snapshot = t
                self.journal.record_scan(self.last_rows, now)
        if t - self._t_status >= self.cfg.status_interval_seconds:
            self._t_status = t
            self.write_status(now)
        if self.hb is not None:
            self.hb.beat({"mode": self.mode, "open": len(self.positions), "entries_allowed": self.entries_allowed,
                          "markets": len(self.universe)})
        self.cycle_ms = (time.monotonic() - m0) * 1000.0
        return [n for n in notes if n]

    def _guarded(self, what: str, fn: Callable[..., list[str]], *args) -> list[str]:
        """Run one piece of bookkeeping; an error is noted (and it is tried
        again on a later pass) instead of stopping the pass before the open
        positions are managed."""
        try:
            return list(fn(*args) or [])
        except Exception as exc:
            if fn == self._reconcile:
                self.reconcile_ok = False
            elif fn == self._sweep_orphans:
                self._sweep_due = True
            return [self._note(f"{what} failed ({exc}); open positions are still managed, tried again")]

    def _close_retry_due(self, now: dt.datetime) -> bool:
        """A refused orphan/leftover close is due to be tried again (LIVE:
        the orphan check runs for it)."""
        t = now.timestamp()
        return any(t >= due for due in self._close_retry.values()) and fx_market_open(now)

    def _write_pending(self, now: dt.datetime) -> list[str]:
        """Trade rows that could not be written when they were made (a
        database briefly locked, say): written now, else tried again next
        minute. A row already there is kept as it is."""
        notes: list[str] = []
        for ticket, row in list(self._pending_rows.items()):
            try:
                self.journal.insert_trade(row)
            except sqlite3.IntegrityError:
                pass
            except sqlite3.Error as exc:
                notes.append(self._note(f"ticket {ticket} {row.get('symbol', '')}: still could not be recorded "
                                        f"({exc}); tried again next minute"))
                continue
            self._pending_rows.pop(ticket, None)
            notes.append(self._note(f"ticket {ticket} {row.get('symbol', '')}: now recorded"))
        return notes

    # ------------------------------------------------------------- entries --
    def _enter(self, intent: EntryIntent, now: dt.datetime) -> str:
        sym = intent.symbol
        spec = self._spec(sym)
        tick = self.latest.get(sym)
        if spec is None or tick is None:
            return self._note(f"{sym}: no contract or price - no entry")
        if any(p.symbol == sym for p in self.positions.values()):
            return ""
        if len(self.positions) >= self.cfg.max_concurrent_positions:
            return self._note(f"{sym}: trigger skipped - {len(self.positions)} positions already open (the sanity cap)")
        volume, gbp_pp, why = self.core.size(sym)
        if volume <= 0:
            self.journal.log_event("NO_SIZE", why, {"symbol": sym}, now)
            return self._note(f"{sym}: cannot size the trade - {why}")
        d = intent.side.sign
        touch = float(tick.ask if intent.side is Side.BUY else tick.bid)
        if (touch - intent.stop) * d <= 0:
            return self._note(f"{sym}: price is already through the planned stop - no entry")
        send_at = to_utc(self.clock())
        m0 = time.monotonic()
        try:
            fill: Fill = self.executor.open(sym, intent.side, volume, intent.stop, 0.0, tick, spec, now, comment=TAG)
        except Exception as exc:
            self.rejects.append(now)
            self._sweep_due = True
            self.journal.log_event("SEND_UNKNOWN", f"{sym}: {exc}", {"symbol": sym}, now)
            return self._note(f"{sym}: order sent, outcome unknown ({exc}); the broker's list is checked next pass")
        ack_at = to_utc(self.clock())
        send_ack_ms = (time.monotonic() - m0) * 1000.0
        if not fill.ok:
            self.rejects.append(now)
            if str(fill.message).startswith("send failed"):
                self._sweep_due = True
            self.journal.log_event("REFUSED", f"{sym}: {fill.message}", {"symbol": sym, "volume": volume}, now)
            return self._note(f"{sym}: order refused - {fill.message}")
        if (float(fill.price) - intent.stop) * d <= 0:
            # filled at or through the stop: fail closed, at once
            undo = self.executor.close(fill.ticket, tick, spec, comment=f"{TAG} thru stop")
            self.journal.log_event("FILL_THROUGH_STOP", f"{sym}: filled {fill.price} through stop {intent.stop}",
                                   {"ticket": fill.ticket, "closed": bool(undo.ok)}, now)
            if not undo.ok and self.mode == "LIVE":
                self._sweep_due = True
            return self._note(f"{sym}: filled through the planned stop - closed at once")
        pos = self.core.open_position(intent, float(fill.price), float(fill.volume or volume), int(fill.ticket),
                                      ack_at, self.mode, gbp_pp)
        pos.meta["opened_on"] = tick.time                  # the price it was opened on (the PAPER stop check)
        pos.meta["marks"] = [(tick.time, pos.flow.best, pos.flow.worst)]   # best/worst so far, by price time
        pip = pos.pip
        row = {
            "ticket": pos.ticket, "mode": self.mode, "symbol": sym, "side": intent.side.value, "volume": pos.volume,
            "gbp_per_pip": gbp_pp, "pip": pip, "signal_utc": intent.signal_time.isoformat(),
            "send_utc": send_at.isoformat(), "ack_utc": ack_at.isoformat(), "opened_utc": ack_at.isoformat(),
            "reference_price": intent.reference_price, "entry": pos.entry, "initial_stop": intent.stop,
            "stop": intent.stop, "score": intent.score, "entry_family": intent.family, "entry_reason": intent.reason,
            "trigger_id": intent.trigger_id, "state_json": to_json(intent.snapshot),
            "flow_json": to_json(pos.flow.to_dict()), "flow_states": ",".join(pos.flow.visited),
            "buckets_json": to_json(intent.snapshot.get("buckets", {})),
            "latency_signal_send_ms": round((send_at - intent.signal_time).total_seconds() * 1000.0, 1),
            "latency_send_ack_ms": round(send_ack_ms, 1),
            "latency_signal_fill_ms": round((ack_at - intent.signal_time).total_seconds() * 1000.0, 1),
            "fill_slippage_pips": round((pos.entry - intent.reference_price) * d / pip, 2) if pip else 0.0,
            "session_date": ack_at.date().isoformat(),
        }
        try:
            self.journal.insert_trade(row)
        except sqlite3.IntegrityError:
            self.core.position_closed(pos, now)
            undo = self.executor.close(pos.ticket, tick, spec, comment=f"{TAG} dup ticket")
            if not undo.ok and self.mode == "LIVE":
                self._sweep_due = True
            return self._note(f"{sym}: ticket {pos.ticket} already on record - closed again at once")
        except sqlite3.Error as exc:
            # the trade exists; it is managed in memory and its row is written on a later pass (_save_unsaved)
            pos.meta["unsaved_row"] = row
            self._note(f"{sym}: could not record the trade ({exc}); tried again on the next pass")
        self.positions[pos.ticket] = pos
        self.last_manage[pos.ticket] = now.timestamp()
        self.journal.log_event("ENTRY", f"{sym} {intent.side.value} {pos.volume:g} at {pos.entry}",
                               {"ticket": pos.ticket, "score": intent.score, "family": intent.family}, now)
        return self._note(f"{STRATEGY_LABEL}: {sym} {intent.side.value} {pos.volume:g} lots at {pos.entry} "
                          f"(GBP {gbp_pp:.2f}/pip), stop {intent.stop} ({intent.stop_pips:.1f} pips) - {intent.reason}")

    # ---------------------------------------------------------- management --
    def _manage(self, now: dt.datetime) -> list[str]:
        notes: list[str] = []
        for ticket, pos in list(self.positions.items()):
            try:
                notes.extend(self._manage_one(pos, now))
            except Exception as exc:                       # one position's trouble never stops the others
                notes.append(self._note(f"{pos.symbol}: management error ({exc}); retrying"))
        return notes

    def _manage_one(self, pos: RiderPosition, now: dt.datetime) -> list[str]:
        tick = self.latest.get(pos.symbol) or self.core.last_tick(pos.symbol)
        if tick is None:
            return []
        if pos.ticket in self.missing:
            return []                                      # not in the broker's list this pass: nothing to move
        spec = self._spec(pos.symbol)
        if self.mode == "PAPER":
            # the history can bring ticks older than the latest price acted on: each meets the stop in
            # force at its own time, as in the replay (none before the price the trade was opened on)
            for tk in (self.new_ticks.get(pos.symbol) or [tick]):
                stop = self._paper_stop_at(pos, tk)
                if stop is not None and (tk.bid <= stop if pos.side is Side.BUY else tk.ask >= stop):
                    # booked with the stop that tick met and what had been seen up to it, not later
                    return [self._close(pos, tk, "STOP", "the stop was reached", now,
                                        at_price=self._paper_stop_fill(pos, tk, stop), met_stop=stop,
                                        met_at=tk.time)]
        d = self.core.manage(pos, tick, now)
        self.last_manage[pos.ticket] = now.timestamp()
        if self.mode == "PAPER":
            # best/worst as of this price's time; one at or before the ticks already fed is all a
            # later tick from the history can need
            fed = self._last_tick_time.get(pos.symbol)
            marks = pos.meta.get("marks", [])
            pos.meta["marks"] = [m for m in marks if fed is not None and m[0] <= fed][-1:] + \
                [m for m in marks if fed is None or m[0] > fed] + [(tick.time, pos.flow.best, pos.flow.worst)]
        if d.exit:
            if d.exit_reason == "STOP":
                if self.mode == "LIVE":
                    # the broker's own stop takes it and reconcile books it - but if the broker
                    # still lists it a pass later with the price still through the stop, fail closed
                    if pos.ticket in self.stop_seen:
                        return [self._close(pos, tick, "STOP", "the stop was reached but the broker had not "
                                            "closed the trade - closed at market", now)]
                    self.stop_seen[pos.ticket] = now.timestamp()
                    return []
                # PAPER: the latest price reached the stop before the tick history brought that tick
                # (the paper book's check above runs on the history's ticks)
                return [self._close(pos, tick, "STOP", d.why, now, at_price=self._paper_stop_fill(pos, tick))]
            return [self._close(pos, tick, d.exit_reason, d.why, now)]
        self.stop_seen.pop(pos.ticket, None)
        notes: list[str] = []
        if d.new_stop is not None:
            old = pos.flow.stop
            new = self._snap(spec, d.new_stop, pos.side)
            if self.core.stop_move_due(pos, new, tick, now):     # min step and min interval, as the replay
                self.core.stop_move_sent(pos, now)
                # tp 0.0: the Rider never uses a take-profit (and the executor need not read positions first)
                if self.executor.modify_stop(pos.ticket, new, 0.0):
                    if self.core.confirm_stop(pos, new, now):
                        # the price this move was made on, and the stop before it (the PAPER stop check);
                        # a move older than every tick still to come is no longer needed
                        fed = self._last_tick_time.get(pos.symbol)
                        pos.meta["moves"] = [m for m in pos.meta.get("moves", ()) if fed is None or m[0] > fed] \
                            + [(tick.time, old)]
                    try:
                        self.journal.record_stop_change(pos.ticket, old, new, d.state, d.why, now)
                    except sqlite3.Error:
                        pass
                    notes.append(self._note(f"{pos.symbol}: stop {old} -> {new} ({PLAIN.get(d.state, d.state)})"))
                else:
                    # the broker did not take it: nothing is committed, the old stop stands, tried again later
                    err = getattr(self.executor, "last_error", "") or "refused"
                    self.rejects.append(now)
                    notes.append(self._note(f"{pos.symbol}: broker refused stop {new} ({err}); keeping {old}, "
                                            f"trying again in {self.cfg.stop_min_modify_seconds:g} s"))
        self._save_open(pos)
        return notes

    @staticmethod
    def _paper_stop_at(pos: RiderPosition, tk: Tick) -> Optional[float]:
        """PAPER: the stop in force when ``tk`` was quoted. None before the
        price the trade was opened on; before the price a stop move was made
        on, the stop that move replaced; otherwise the stop now. (After a
        restart nothing is recorded and every tick meets the stop now.)"""
        opened_on = pos.meta.get("opened_on")
        if opened_on is not None and tk.time < opened_on:
            return None
        for moved_on, before in pos.meta.get("moves", ()):
            if tk.time < moved_on:
                return float(before)
        return float(pos.flow.stop)

    def _paper_stop_fill(self, pos: RiderPosition, tk: Tick, stop: Optional[float] = None) -> float:
        """PAPER: a stop fills at the stop or worse - at the price of the tick
        that hit it when the price jumped through - less the slippage, as the
        replay's CostModel.stop_fill (a gap is never filled at the stop).
        ``stop``: the stop that tick met (default: the stop now)."""
        slip = float(self.cfg.slippage_pips) * float(pos.pip or 0.0)
        stop = float(pos.flow.stop if stop is None else stop)
        if pos.side is Side.BUY:
            return min(stop, float(tk.bid)) - slip
        return max(stop, float(tk.ask)) + slip

    def _save_unsaved(self, pos: RiderPosition) -> bool:
        """A trade whose row could not be written when it opened: written now.
        True when its row is there (or always was)."""
        row = pos.meta.get("unsaved_row")
        if row is None:
            return True
        try:
            self.journal.insert_trade(row)
        except sqlite3.IntegrityError:
            pass                                           # the first write went in after all
        except sqlite3.Error:
            return False
        pos.meta.pop("unsaved_row", None)
        self._note(f"{pos.symbol}: trade {pos.ticket} is now recorded")
        return True

    def _save_open(self, pos: RiderPosition) -> None:
        if not self._save_unsaved(pos):
            return
        try:
            self.journal.update_trade(pos.ticket, {"stop": pos.flow.stop, "flow_json": to_json(pos.flow.to_dict()),
                                                   "flow_states": ",".join(pos.flow.visited),
                                                   "mfe_pips": round(pos.flow.mfe / pos.pip, 2) if pos.pip else 0.0,
                                                   "mae_pips": round(pos.flow.mae / pos.pip, 2) if pos.pip else 0.0})
        except sqlite3.Error:
            pass

    def _commission_money(self, volume: float) -> float:
        per = self.core.commission_account_per_lot_side()
        return 2.0 * float(per or 0.0) * float(volume)

    def _paper_money(self, pos: RiderPosition, exit_px: float) -> Optional[float]:
        return self._estimate_money(pos.symbol, pos.side.sign, pos.volume, pos.entry, exit_px)

    def _estimate_money(self, symbol: str, sign: int, volume: float, entry: float, exit_px: float) -> Optional[float]:
        """The money from the contract and the configured (or learned) commission."""
        spec = self._spec(symbol)
        if spec is None or not volume:
            return None
        return float(spec.money((exit_px - entry) * sign, volume)) - self._commission_money(volume)

    def _learn_commission(self, deal: dict, volume: float) -> None:
        if not self.cfg.learn_commission_from_deals:
            return
        ec = deal.get("entry_commission")
        if ec is None or not volume:
            return
        per_side = abs(float(ec)) / float(volume)
        if per_side > 0:
            self.core.set_learned_commission(per_side)

    def _close(self, pos: RiderPosition, tick: Tick, reason: str, detail: str, now: dt.datetime,
               at_price: Optional[float] = None, met_stop: Optional[float] = None,
               met_at: Optional[dt.datetime] = None) -> str:
        """``met_stop``/``met_at`` (PAPER): the stop the closing tick met and
        that tick's time, when it may be older than the latest price."""
        spec = self._spec(pos.symbol)
        fill = self.executor.close(pos.ticket, tick, spec, at_price=at_price, comment=f"{TAG} {reason}"[:31])
        if not fill.ok:
            self.rejects.append(now)
            held = "WITHOUT a stop the bot trusts" if reason == "NO_BROKER_STOP" else "with its stop at the broker"
            return self._note(f"{pos.symbol}: close ({reason}) refused - {fill.message}; still open {held}, retrying")
        exit_px = float(fill.price)
        if self.mode == "LIVE":
            deal = self.executor.closed_deal(pos.ticket)
            if deal and deal.get("pnl") is not None:
                exit_px = float(deal.get("exit_price") or exit_px)
                self._learn_commission(deal, pos.volume)
                return self._finalise(pos, exit_px, float(deal["pnl"]), "broker", reason, detail, now,
                                      close_tick=tick)
            return self._finalise(pos, exit_px, self._paper_money(pos, exit_px), "estimate", reason + "?", detail, now,
                                  close_tick=tick)
        return self._finalise(pos, exit_px, self._paper_money(pos, exit_px), "paper", reason, detail, now,
                              met_stop=met_stop, met_at=met_at, close_tick=tick)

    @staticmethod
    def _news_name(snapshot: dict) -> str:
        try:
            return str(((snapshot.get("families") or {}).get("news") or {}).get("values", {})
                       .get("event", {}).get("name", ""))
        except Exception:
            return ""

    @staticmethod
    def _extremes_at(pos: RiderPosition, when: Optional[dt.datetime]) -> tuple[float, float]:
        """PAPER: FlowLock X's best and worst price as of ``when`` (the last
        look at a price no newer than it); the latest when nothing is
        recorded (after a restart)."""
        if when is not None:
            seen = [m for m in pos.meta.get("marks", ()) if m[0] <= when]
            if seen:
                return float(seen[-1][1]), float(seen[-1][2])
        return float(pos.flow.best), float(pos.flow.worst)

    def _finalise(self, pos: RiderPosition, exit_px: float, money: Optional[float], source: str, reason: str,
                  detail: str, closed_at: dt.datetime, met_stop: Optional[float] = None,
                  met_at: Optional[dt.datetime] = None, close_tick: Optional[Tick] = None) -> str:
        """``met_stop``/``met_at`` (PAPER): the stop the closing tick met and
        its time. A tick from the history can be older than the latest price,
        so the stop it met can be one a later move replaced: the row and the
        story then hold the stop that was met and the best/worst seen up to
        that tick; FlowLock X's own stop is never touched. ``close_tick``: the
        price the trade was closed on; its closing side (the bid for a long,
        the ask for a short) counts in the best and worst, as the replay's
        _mark (a gap through the stop is a loss the MAE shows)."""
        pip = pos.pip or 1.0
        d = pos.side.sign
        realised = (exit_px - pos.entry) * d / pip
        best, worst = self._extremes_at(pos, met_at) if met_at is not None else (pos.flow.best, pos.flow.worst)
        if close_tick is not None:
            mark = float(close_tick.bid if d > 0 else close_tick.ask)
            best = mark if (mark - best) * d > 0 else best
            worst = mark if (mark - worst) * d < 0 else worst
        mfe = max(0.0, (best - pos.entry) * d) / pip
        mae = max(0.0, (pos.entry - worst) * d) / pip
        gpp = pos.gbp_per_pip
        capture = round(realised / mfe, 3) if mfe > 0 else None
        final_stop = float(pos.flow.stop if met_stop is None else met_stop)
        if abs(final_stop - float(pos.flow.stop)) > 1e-12:
            detail = (f"{detail} - the stop in force at that price was {final_stop}; the later stop move to "
                      f"{pos.flow.stop} came after it and does not count")
        story = explain_trade(pos.symbol, pos.side, pos.family, list(pos.flow.visited), reason, realised, money,
                              source, pos.mode, pos.flow.initial_stop, final_stop, self._news_name(pos.snapshot))
        fields = {"closed_utc": to_utc(closed_at).isoformat(), "exit_price": exit_px, "stop": final_stop,
                  "exit_reason": reason, "exit_detail": detail, "mfe_pips": round(mfe, 2), "mae_pips": round(mae, 2),
                  "mfe_money": round(mfe * gpp, 2), "mae_money": round(-mae * gpp, 2),
                  "realised_pips": round(realised, 2), "net_money": round(money, 2) if money is not None else None,
                  "money_source": source, "mfe_capture": capture, "flow_json": to_json(pos.flow.to_dict()),
                  "flow_states": ",".join(pos.flow.visited),
                  "duration_seconds": round((to_utc(closed_at) - to_utc(pos.opened_at)).total_seconds(), 1),
                  "explanation": story, "commission": round(self._commission_money(pos.volume), 2)}
        if not self._save_unsaved(pos):
            # its row was never written: the whole trade is written by the minute's bookkeeping (_write_pending)
            self._pending_rows[pos.ticket] = {**pos.meta["unsaved_row"], **fields}
            self._note(f"{pos.symbol}: could not record trade {pos.ticket}; tried again next minute")
        else:
            try:
                self.journal.update_trade(pos.ticket, fields)
            except sqlite3.Error as exc:
                self._note(f"{pos.symbol}: could not record the close ({exc})")
        try:
            b = json.loads(to_json(pos.snapshot.get("buckets", {})))
        except Exception:
            b = {}
        net_pips = (money / gpp) if (money is not None and gpp > 0) else realised
        if b:
            self.learning.add(Outcome(to_utc(closed_at).date().isoformat(), net_pips, b))
        self.core.position_closed(pos, closed_at)
        self.positions.pop(pos.ticket, None)
        self.last_manage.pop(pos.ticket, None)
        self.missing.pop(pos.ticket, None)
        self.stop_seen.pop(pos.ticket, None)
        self.stop_restored.pop(pos.ticket, None)
        self.journal.log_event("EXIT", story, {"ticket": pos.ticket, "reason": reason, "source": source}, closed_at)
        return self._note(f"{STRATEGY_LABEL}: {pos.symbol} {pos.side.value} closed ({reason}) at {exit_px}: "
                          f"{realised:+.1f} pips {_money(money)} - {story}")

    # ---------------------------------------------------- reconciliation --
    def _broker_reason(self, pos: RiderPosition, exit_px: float, hint: str) -> str:
        return self._reason_of(hint, exit_px, pos.flow.stop, pos.pip)

    @staticmethod
    def _reason_of(hint: str, exit_px: float, stop: float, pip: float) -> str:
        """The exit reason from the closing deal's comment (``hint``)."""
        h = str(hint or "").lower()
        if h.startswith(TAG.lower()):
            word = h[len(TAG):].strip().split()
            w = word[0].upper() if word else ""
            return w or "BROKER_CLOSED"
        if "sl" in h or "stop" in h:
            return "STOP"
        if "tp" in h:
            return "TARGET"
        if h:
            return "BROKER_CLOSED"
        return "STOP" if stop and abs(exit_px - stop) <= 3 * pip else "BROKER_CLOSED"

    def _reconcile(self, now: dt.datetime) -> list[str]:
        notes: list[str] = []
        try:
            at_broker = {p.ticket: p for p in self.executor.positions()}
            self.reconcile_ok = True
        except Exception as exc:
            self.reconcile_ok = False
            return [self._note(f"could not read the broker's positions ({exc}); positions kept, checked again")]
        for ticket, pos in list(self.positions.items()):
            q = at_broker.get(ticket)
            if q is not None:
                self.missing.pop(ticket, None)
                notes.extend(self._check_broker_stop(pos, q, now))
                continue
            deal = self._deal_quick(ticket)
            if deal and deal.get("pnl") is not None:
                exit_px = float(deal.get("exit_price") or pos.flow.stop)
                self._learn_commission(deal, pos.volume)
                reason = self._broker_reason(pos, exit_px, deal.get("reason", ""))
                notes.append(self._finalise(pos, exit_px, float(deal["pnl"]), "broker", reason,
                                            "closed at the broker", self._deal_time(deal, now)))
                continue
            n, since = self.missing.get(ticket, (0, now.timestamp()))
            n += 1
            self.missing[ticket] = (n, since)
            if n >= MISSING_PASSES and now.timestamp() - since >= MISSING_SECONDS:
                exit_px = self._estimate_exit(pos.symbol, pos.side.sign, pos.flow.stop)
                notes.append(self._finalise(pos, exit_px, self._paper_money(pos, exit_px), "estimate", "BROKER_CLOSED?",
                                            "gone from the broker with no deal record yet", now))
            elif n == 1:
                notes.append(self._note(f"{pos.symbol}: ticket {ticket} not in the broker's list and no closing deal "
                                        "yet - kept and checked again"))
        return notes

    def _check_broker_stop(self, pos: RiderPosition, q: Position, now: dt.datetime) -> list[str]:
        """LIVE: the broker must hold at least the stop FlowLock X has
        committed. A tighter broker stop is adopted (never widened); a
        missing or looser one is re-sent at once; if the broker refuses it,
        or still does not show it after accepting, or the price is already
        through it, the trade is closed at market (fail closed)."""
        s = pos.side.sign
        want = float(pos.flow.stop)
        have = float(getattr(q, "sl", 0.0) or 0.0)
        spec = self._spec(pos.symbol)
        grid = float(getattr(spec, "tick_size", 0.0) or getattr(spec, "point", 0.0) or 0.0) if spec else 0.0
        tol = max(0.5 * grid, 1e-9)
        if have > 0 and (have - want) * s >= -tol:
            self.stop_restored.pop(pos.ticket, None)
            if (have - want) * s > tol and self.core.confirm_stop(pos, have):
                self._save_open(pos)
                return [self._note(f"{pos.symbol}: the broker holds a tighter stop ({have}) than recorded ({want}) "
                                   "- following the broker's")]
            return []
        what = "no stop" if have <= 0 else f"a looser stop ({have})"
        tick = self.latest.get(pos.symbol) or self.core.last_tick(pos.symbol)
        if tick is None:
            self.rejects.append(now)
            return [self._note(f"{pos.symbol}: the broker shows {what} on ticket {pos.ticket} (should be {want}) and "
                               "there is no price yet - checked again next pass")]
        mark = float(tick.bid if pos.side is Side.BUY else tick.ask)
        through = (mark - want) * s <= 0
        notes: list[str] = []
        if not through and pos.ticket not in self.stop_restored:
            if self.executor.modify_stop(pos.ticket, want, 0.0):
                self.stop_restored[pos.ticket] = now.timestamp()
                self.journal.log_event("STOP_RESTORED", f"{pos.symbol} {pos.ticket}: broker showed {what}; re-sent {want}",
                                       {"ticket": pos.ticket, "broker_sl": have, "stop": want}, now)
                return [self._note(f"{pos.symbol}: the broker showed {what} on ticket {pos.ticket} - "
                                   f"the stop {want} was sent again")]
            self.rejects.append(now)
            err = getattr(self.executor, "last_error", "") or "refused"
            notes.append(self._note(f"{pos.symbol}: the broker refused to hold the stop {want} ({err})"))
        if through:
            why = f"the broker showed {what} and the price is already through the stop {want}"
        elif pos.ticket in self.stop_restored:
            why = f"the broker still showed {what} after the stop {want} was sent again"
        else:
            why = f"the broker showed {what} and would not take the stop {want}"
        self.journal.log_event("NO_BROKER_STOP", f"{pos.symbol} {pos.ticket}: {why} - closing at market",
                               {"ticket": pos.ticket, "broker_sl": have, "stop": want}, now)
        notes.append(self._close(pos, tick, "NO_BROKER_STOP", why + " - closed at market", now))
        return notes

    def _estimate_exit(self, symbol: str, sign: int, stop: float) -> float:
        """A trade gone from the broker with no deal yet: the worse of the stop
        and the price now (the stop when there is no price). Never better than
        the stop, and a price gapped through it is not booked at it (the
        broker's deal replaces this estimate)."""
        tk = self.latest.get(symbol)
        if tk is None:
            return float(stop)
        mark = float(tk.bid if sign > 0 else tk.ask)
        return min(float(stop), mark) if sign > 0 else max(float(stop), mark)

    def _deal_quick(self, ticket: int, ex=None) -> Optional[dict]:
        ex = self.executor if ex is None else ex
        try:
            return ex.closed_deal(ticket, tries=1)
        except TypeError:
            try:
                return ex.closed_deal(ticket)
            except Exception:
                return None
        except Exception:
            return None

    @staticmethod
    def _deal_time(deal: dict, now: dt.datetime) -> dt.datetime:
        t = deal.get("time")
        try:
            return to_utc(t) if isinstance(t, dt.datetime) else to_utc(now)
        except Exception:
            return to_utc(now)

    def _sweep_orphans(self, now: dt.datetime) -> list[str]:
        """LIVE: a position carrying magic 990811 that this bot has no row for
        is CLOSED at once and booked - never adopted, never left to run."""
        self._sweep_due = False
        try:
            at_broker = list(self.executor.positions())
        except Exception as exc:
            self._sweep_due = True
            self.reconcile_ok = False
            return [self._note(f"could not read the broker's positions for the orphan check ({exc})")]
        listed = {p.ticket for p in at_broker}
        for gone in [k for k in self._close_retry if k not in listed]:
            self._close_retry.pop(gone, None)
        notes: list[str] = []
        for p in at_broker:
            if p.ticket in self.positions:
                continue
            notes.append(self._close_foreign(p, now, "ORPHAN_CLOSED",
                                             "a position with the rider's number but no record")[0])
        return notes

    def _close_foreign(self, p: Position, now: dt.datetime, reason: str, why: str,
                       ex=None) -> tuple[str, bool, Optional[float]]:
        """Close ``p`` at the broker. Returns (note, closed, the close price
        the broker gave or None); a position with no row here is booked. A
        refused close is tried again CLOSE_RETRY_SECONDS later, and only while
        the FX market is open (not on every pass); until then nothing is sent
        and (note "", closed False) comes back."""
        ex = ex or self.executor
        t = now.timestamp()
        due = self._close_retry.get(p.ticket)
        if due is not None and (t < due or not fx_market_open(now)):
            return "", False, None
        unknown = False
        try:
            r = self.broker.close(p.ticket, 0.0, f"{TAG} {reason.split()[0].lower()}"[:31])
            ok = bool(getattr(r, "ok", False))
        except Exception as exc:
            ok, r, unknown = False, exc, True
        if not ok:
            self._close_retry[p.ticket] = t + CLOSE_RETRY_SECONDS
            if unknown:
                self._sweep_due = True                     # the outcome is unknown: the broker's list is read again
            self.journal.log_event("ORPHAN_REFUSED", f"{p.symbol} {p.ticket}: {r}", {"ticket": p.ticket}, now)
            return (self._note(f"{p.symbol}: could not close position {p.ticket} ({why}): {r}; it keeps its broker "
                               f"stop, tried again in {CLOSE_RETRY_SECONDS:.0f} s (while the FX market is open)"),
                    False, None)
        self._close_retry.pop(p.ticket, None)
        fill_px = float(getattr(r, "price", 0.0) or 0.0) or None
        deal = None
        try:
            deal = ex.closed_deal(p.ticket) if hasattr(ex, "closed_deal") else self.broker.closed_deal(p.ticket)
        except Exception:
            deal = None
        exit_px = float((deal or {}).get("exit_price") or fill_px or p.entry_price)
        money = float(deal["pnl"]) if deal and deal.get("pnl") is not None else None
        spec = self._spec(p.symbol)
        pip, _ = fx_pip_size(spec) if spec is not None else (0.0, "")
        realised = (exit_px - p.entry_price) * p.side.sign / pip if pip else 0.0
        try:
            known = self.journal.trade(p.ticket) is not None
        except sqlite3.Error:
            known = False                                  # the insert below tells (a row already there is kept)
        if not known:
            # no deal readable yet: an estimate from the close price, marked '?' until the broker's figure comes in
            est = money if money is not None else self._estimate_money(p.symbol, p.side.sign, p.volume,
                                                                      p.entry_price, exit_px)
            row = {
                "ticket": p.ticket, "mode": "LIVE", "symbol": p.symbol, "side": p.side.value, "volume": p.volume,
                "pip": pip, "opened_utc": to_utc(p.open_time).isoformat() if p.open_time else now.isoformat(),
                "closed_utc": now.isoformat(), "entry": p.entry_price, "initial_stop": p.sl, "stop": p.sl,
                "exit_price": exit_px, "exit_reason": reason if money is not None else reason + "?",
                "exit_detail": why,
                "realised_pips": round(realised, 2), "net_money": round(est, 2) if est is not None else None,
                "money_source": "broker" if money is not None else "estimate",
                "explanation": f"{_pair(p.symbol)}: {why}, so it was closed at {realised:+.1f} pips ({_money(est)}).",
                "session_date": now.date().isoformat()}
            try:
                self.journal.insert_trade(row)
            except sqlite3.IntegrityError:
                pass                                       # already recorded
            except sqlite3.Error as exc:
                # written by the minute's bookkeeping (_write_pending) once the database takes it
                self._pending_rows[p.ticket] = row
                self._note(f"{p.symbol}: could not record closed position {p.ticket} ({exc}); "
                           "tried again next minute")
        self.journal.log_event(reason, f"{p.symbol} {p.ticket} closed: {why}", {"ticket": p.ticket, "pnl": money}, now)
        return self._note(f"{p.symbol}: closed position {p.ticket} ({why}) at {exit_px}: {_money(money)}"), True, fill_px

    def _story_from_row(self, r: dict, reason: str, realised: Optional[float], money: Optional[float],
                        source: str) -> str:
        """The child's story for a recorded trade (an orphan's own short one)."""
        pips = float(realised or 0.0)
        if not r.get("entry_family"):
            why = r.get("exit_detail") or "it was closed at the broker"
            return f"{_pair(r['symbol'])}: {why}, so it was closed at {pips:+.1f} pips ({_money(money)})."
        try:
            snap = json.loads(r.get("state_json") or "{}")
        except Exception:
            snap = {}
        visited = [s for s in str(r.get("flow_states") or "").split(",") if s]
        initial = float(r.get("initial_stop") or r.get("stop") or 0.0)
        final = float(r.get("stop") or initial)
        side = Side.BUY if r.get("side") == "BUY" else Side.SELL
        return explain_trade(r["symbol"], side, r["entry_family"], visited, reason, pips, money, source,
                             r.get("mode") or "LIVE", initial, final, self._news_name(snap if isinstance(snap, dict) else {}))

    def _correct_estimates(self, now: dt.datetime, ex=None) -> list[str]:
        """LIVE rows booked as an estimate take the broker's figure once its
        deal comes in, and the story is written again from it. ``ex``: what
        reads the deals (not LIVE: a LiveExecutor, which only reads here). A
        row that cannot be written is noted and tried again next minute."""
        notes: list[str] = []
        try:
            rows = self.journal.estimates_since(now - dt.timedelta(hours=ESTIMATE_WINDOW_HOURS))
        except Exception:
            return notes
        for r in rows:
            deal = self._deal_quick(int(r["ticket"]), ex)
            if not deal or deal.get("pnl") is None:
                continue
            exit_px = float(deal.get("exit_price") or r["exit_price"] or 0.0)
            pip = float(r.get("pip") or 0.0)
            d = 1 if r["side"] == "BUY" else -1
            realised = (exit_px - float(r["entry"])) * d / pip if pip else float(r.get("realised_pips") or 0.0)
            reason = str(r.get("exit_reason") or "").rstrip("?")
            pnl = float(deal["pnl"])
            try:
                self.journal.update_trade(int(r["ticket"]), {
                    "exit_price": exit_px, "net_money": round(pnl, 2), "realised_pips": round(realised, 2),
                    "money_source": "broker", "exit_reason": reason,
                    "explanation": self._story_from_row(r, reason, realised, pnl, "broker")})
            except sqlite3.Error as exc:
                notes.append(self._note(f"ticket {r['ticket']} {r['symbol']}: could not record the broker's figure "
                                        f"({exc}); tried again next minute"))
                continue
            notes.append(self._note(f"ticket {r['ticket']} {r['symbol']} now holds the broker's figure: "
                                    f"{_money(pnl)} (was an estimate)"))
        return notes

    def _has_live_rows(self) -> bool:
        try:
            return any(r.get("mode") == "LIVE" for r in self.journal.open_rows())
        except Exception:
            return False

    def leftovers_outstanding(self) -> bool:
        """Not LIVE: something LIVE left is still to be closed at the broker
        or booked (a refused close, a broker list that could not be read, or
        an open LIVE row). The OFF start (run.py) waits on this."""
        return self.mode != "LIVE" and (not self._leftovers_done or self._has_live_rows())

    def _book_leftover(self, r: dict, exit_px: float, deal: Optional[dict], reason: str, detail: str,
                       now: dt.datetime) -> Optional[str]:
        """Close a LIVE row in the records: with the broker's figure when its
        deal is readable, else an estimate (``reason`` marked '?', the money
        from the price, corrected later by _correct_estimates). Returns None,
        or a note when the row could not be written (tried again next minute)."""
        pip = float(r.get("pip") or 0.0)
        dd = 1 if r["side"] == "BUY" else -1
        got = bool(deal) and deal.get("pnl") is not None
        realised = (exit_px - float(r["entry"])) * dd / pip if pip else None
        if got:
            money, source = float(deal["pnl"]), "broker"
        else:
            money, source = self._estimate_money(r["symbol"], dd, float(r.get("volume") or 0.0), float(r["entry"]),
                                                 exit_px), "estimate"
            reason = reason if reason.endswith("?") else reason + "?"
        try:
            self.journal.update_trade(int(r["ticket"]), {
                "closed_utc": now.isoformat(), "exit_price": exit_px, "exit_reason": reason, "exit_detail": detail,
                "realised_pips": round(realised, 2) if realised is not None else None,
                "net_money": round(money, 2) if money is not None else None, "money_source": source,
                "explanation": self._story_from_row(r, reason, realised, money, source)})
        except sqlite3.Error as exc:
            return self._note(f"{r['symbol']}: ticket {r['ticket']} could not be recorded as closed ({exc}); "
                              "tried again next minute")
        return None

    def _close_live_leftovers(self, now: dt.datetime) -> list[str]:
        """Not LIVE: a real position carrying the rider's magic was left from
        LIVE and nothing here would manage it. Close it at the broker and book
        it with the broker's figure (an estimate from the close price, marked
        '?', while its deal is not readable yet); a refused close is tried
        again a minute later, a row that cannot be written next minute. A LIVE
        row the broker does not list is booked from its deal, or - missing for
        a minute over several reads, with no deal - as an estimate. Runs on
        every pass while something is outstanding, else once a minute."""
        if self.broker is None:
            self._leftovers_done = True
            return []
        try:
            mine = [p for p in (self.broker.positions(MAGIC) or []) if int(getattr(p, "magic", 0) or 0) == MAGIC]
        except Exception as exc:
            return [self._note(f"could not check for real positions left from LIVE ({exc})")]
        notes: list[str] = []
        label = "SWITCHED TO PAPER" if self.mode == "PAPER" else "SWITCHED OFF"
        why = f"left open from LIVE (now {self.mode})"
        winder = LiveExecutor(self.broker, MAGIC, TAG)
        all_ok = True
        for p in mine:
            row = self.journal.trade(p.ticket)
            note, closed, fill_px = self._close_foreign(p, now, label, why, winder)
            notes.append(note)
            if not closed:
                all_ok = False
            elif row is not None and row.get("closed_utc") is None:
                deal = None
                try:
                    deal = winder.closed_deal(p.ticket)
                except Exception:
                    deal = None
                dd = 1 if row["side"] == "BUY" else -1
                exit_px = float((deal or {}).get("exit_price") or fill_px or 0.0)
                if not exit_px:                                # no price from the close: as _reconcile estimates
                    exit_px = (self._estimate_exit(row["symbol"], dd, float(row["stop"])) if row.get("stop")
                               else float(row["entry"]))
                failed = self._book_leftover(row, exit_px, deal, label, why + " - closed at the broker", now)
                if failed:                                     # the row stays open: booked from the deal next minute
                    notes.append(failed)
        # LIVE rows the broker no longer holds: booked from its deal (or as an estimate)
        try:
            rows = [r for r in self.journal.open_rows() if r.get("mode") == "LIVE"]
        except Exception:
            rows = []
        held = {p.ticket for p in mine}
        for k in [k for k in self._close_retry if k not in held]:
            self._close_retry.pop(k, None)
        for k in [k for k in self._leftover_missing if k in held]:
            self._leftover_missing.pop(k, None)
        t = now.timestamp()
        waiting = False
        for r in rows:
            ticket = int(r["ticket"])
            if ticket in held:
                continue
            deal = None
            try:
                deal = winder.closed_deal(ticket, tries=1)
            except Exception:
                deal = None
            if not deal:
                # one empty answer is not proof it is gone (MetaTrader can answer "no positions" when it has no
                # answer): as _reconcile, it is estimated only once missing on MISSING_PASSES reads over
                # MISSING_SECONDS; until then the row stays open and the broker's list is read on every pass
                n, since = self._leftover_missing.get(ticket, (0, t))
                n += 1
                self._leftover_missing[ticket] = (n, since)
                if n < MISSING_PASSES or t - since < MISSING_SECONDS:
                    waiting = True
                    if n == 1:
                        notes.append(self._note(f"{r['symbol']}: ticket {ticket} not in the broker's list and no "
                                                "closing deal yet - kept open and checked again"))
                    continue
            pip = float(r.get("pip") or 0.0)
            dd = 1 if r["side"] == "BUY" else -1
            if deal or not r.get("stop"):
                exit_px = float((deal or {}).get("exit_price") or r.get("stop") or r.get("entry") or 0.0)
            else:
                exit_px = self._estimate_exit(r["symbol"], dd, float(r["stop"]))   # as _reconcile
            if deal:
                reason = self._reason_of(deal.get("reason", ""), exit_px, float(r.get("stop") or 0.0), pip)
                reason = label if reason == "SWITCHED" else reason   # its close here was booked late
            else:
                reason = "BROKER_CLOSED?"
            failed = self._book_leftover(r, exit_px, deal, reason, "closed at the broker", now)
            if failed:                                         # the row stays open: tried again next minute
                notes.append(failed)
            else:
                self._leftover_missing.pop(ticket, None)
        self._leftovers_done = all_ok and not waiting
        return notes

    # -------------------------------------------------------------- restart --
    def _pos_from_row(self, r: dict) -> Optional[RiderPosition]:
        try:
            flow = FlowState.from_dict(json.loads(r.get("flow_json") or "{}"))
        except Exception:
            return None
        try:
            snap = json.loads(r.get("state_json") or "{}")
        except Exception:
            snap = {}
        side = Side.BUY if r.get("side") == "BUY" else Side.SELL
        opened = dt.datetime.fromisoformat(r["opened_utc"]) if r.get("opened_utc") else to_utc(self.clock())
        sig = dt.datetime.fromisoformat(r["signal_utc"]) if r.get("signal_utc") else None
        return RiderPosition(int(r["ticket"]), r["symbol"], side, float(r.get("volume") or 0.0), float(r["entry"]),
                             opened, flow, r.get("entry_family") or "", float(r.get("score") or 0.0),
                             r.get("entry_reason") or "", snap, int(r.get("trigger_id") or 0),
                             float(r.get("pip") or 0.0), float(r.get("gbp_per_pip") or 0.0), r.get("mode") or "PAPER",
                             sig)

    def _restore(self) -> None:
        """Open rows come back. PAPER rows are adopted by the paper executor
        (PAPER only). LIVE rows are checked against the broker: still there,
        managed on (a tighter broker stop is adopted, a looser or missing one
        is sent again by the first pass's reconcile); gone, booked from the
        broker's deal (or kept and checked when there is no deal yet). A row
        from the other mode is closed in the records (PAPER -> LIVE) or its
        real position closed at the broker (LIVE -> PAPER/OFF, at the first
        pass). Positions at the broker with no row: closed at the first pass."""
        now = to_utc(self.clock())
        try:
            rows = self.journal.open_rows()
        except Exception:
            rows = []
        at_broker: Optional[dict[int, Position]] = None
        if self.mode == "LIVE":
            try:
                at_broker = {p.ticket: p for p in self.executor.positions()}
            except Exception as exc:
                self._note(f"could not read the broker's positions at start ({exc}); open rows kept and checked")
        for r in rows:
            pos = self._pos_from_row(r)
            if pos is None:
                continue
            spec = self._spec(pos.symbol)
            if spec is not None and not pos.pip:
                pos.pip, _ = fx_pip_size(spec)
            if pos.mode == "PAPER":
                if self.mode == "PAPER" and self.executor is not None:
                    self.executor.adopt(Position(pos.ticket, pos.symbol, pos.side, pos.volume, pos.entry, pos.flow.stop,
                                                 0.0, pos.opened_at, magic=0, comment=TAG))
                    self.positions[pos.ticket] = pos
                    self.core.restore_position(pos)
                    self._note(f"{pos.symbol}: paper position {pos.ticket} restored after a restart")
                else:
                    label = "SWITCHED TO LIVE" if self.mode == "LIVE" else "SWITCHED OFF"
                    try:
                        self.journal.update_trade(pos.ticket, {
                            "closed_utc": now.isoformat(), "exit_price": pos.entry, "exit_reason": label,
                            "realised_pips": 0.0, "net_money": 0.0, "money_source": "paper",
                            "explanation": f"A practice trade on {_pair(pos.symbol)} was closed in the records because "
                                           f"the bot was switched to {self.mode}. No money involved."})
                    except sqlite3.Error as exc:                # no money involved: closed at the next start
                        self._note(f"{pos.symbol}: could not close practice trade {pos.ticket} in the records ({exc})")
                continue
            if self.mode != "LIVE":
                continue                                   # the first pass closes it at the broker (_close_live_leftovers)
            q = at_broker.get(pos.ticket) if at_broker is not None else None
            if at_broker is None or q is not None:
                broker_sl = float(getattr(q, "sl", 0.0) or 0.0) if q is not None else 0.0
                if broker_sl and never_widen(pos.side.sign, pos.flow.stop, broker_sl) != pos.flow.stop:
                    pos.flow.stop = broker_sl              # a TIGHTER broker stop is adopted - never a looser one
                # a missing or looser broker stop: the first pass's reconcile sends the recorded one again
                # (or closes the trade at market if the broker will not hold it)
                self.positions[pos.ticket] = pos
                self.core.restore_position(pos)
                self.last_manage[pos.ticket] = now.timestamp()
                self._note(f"{pos.symbol}: live position {pos.ticket} restored after a restart")
                continue
            deal = self._deal_quick(pos.ticket)
            if deal and deal.get("pnl") is not None:
                exit_px = float(deal.get("exit_price") or pos.flow.stop)
                self._finalise(pos, exit_px, float(deal["pnl"]), "broker",
                               self._broker_reason(pos, exit_px, deal.get("reason", "")),
                               "closed at the broker while the bot was stopped", self._deal_time(deal, now))
            else:
                self.positions[pos.ticket] = pos
                self.core.restore_position(pos)
                self.missing[pos.ticket] = (1, now.timestamp())
        if self.mode == "PAPER" and self.executor is not None:
            self.executor.reserve(self.journal.max_ticket(PAPER_FIRST_TICKET - 1))

    # --------------------------------------------------------------- health --
    def check_health(self, now: dt.datetime) -> list[dict]:
        checks: list[dict] = []

        def add(name: str, ok: bool, msg: str, critical: bool = True) -> None:
            checks.append({"name": name, "ok": bool(ok), "critical": critical, "message": msg})

        t = now.timestamp()
        connected = False
        if self.broker is not None:
            try:
                connected = bool(self.broker.is_connected())
            except Exception:
                connected = False
            if not connected and t - self._t_reconnect >= 15.0:
                self._t_reconnect = t
                try:
                    connected = bool(self.broker.connect())    # recover automatically
                except Exception:
                    connected = False
        add("mt5_alive", connected, "MetaTrader is answering" if connected else "MetaTrader is not answering")
        add("broker_connected", connected and self.account_ok,
            "connected to the broker" if self.account_ok else "no account details from the broker")
        allowed = bool(getattr(self.account, "trade_allowed", False)) if self.account is not None else False
        add("algo_trading", allowed, "algo trading is enabled" if allowed else "algo trading is switched OFF in MetaTrader",
            critical=self.mode == "LIVE")
        newest = max((x.time for x in self.latest.values()), default=None)
        age = (now - to_utc(newest)).total_seconds() if newest is not None else None
        fresh = age is not None and age <= self.cfg.stale_tick_seconds
        if fresh:
            msg = f"prices are fresh ({age:.0f} s old)"
        elif not fx_market_open(now):
            msg = "the FX market is closed - no fresh prices"
        else:
            msg = "no fresh prices" if age is None else f"prices are {age:.0f} s old"
        add("fresh_ticks", fresh, msg)
        add("symbols", bool(self.universe), f"{len(self.universe)} FX markets available" if self.universe
            else "no FX markets available")
        valid = [s for s in self.universe if (lambda x: x is not None and x.ask > x.bid > 0)(self.latest.get(s))]
        frac = len(valid) / len(self.universe) if self.universe else 0.0
        add("spread_valid", frac >= self.cfg.max_spread_valid_fraction,
            f"{len(valid)} of {len(self.universe)} markets have a valid bid/ask")
        cut = now - dt.timedelta(minutes=10)
        recent = sum(1 for x in self.rejects if x >= cut)
        add("execution", recent < self.cfg.max_rejects_per_10min,
            f"{recent} refusals in the last 10 minutes" if recent else "orders and stop moves are going through")
        add("positions_reconciled", self.reconcile_ok,
            "positions match the broker" if self.reconcile_ok else "could not check positions against the broker")
        stale = [p.symbol for tk, p in self.positions.items()
                 if t - self.last_manage.get(tk, 0.0) > FLOWLOCK_STALE_SECONDS and tk not in self.missing]
        add("flowlock_alive", not stale, "FlowLock X is managing every open position" if not stale
            else f"FlowLock X has not looked at {', '.join(stale)} recently")
        if t - self._t_probe >= 30.0:
            self._t_probe = t
            self.journal.probe()
        add("database_writable", self.journal.last_write_ok,
            "the records can be written" if self.journal.last_write_ok else f"cannot write the records ({self.journal.last_error})")
        disk_ok, disk_msg = True, ""
        try:
            free = shutil.disk_usage(str(self.data_dir)).free / 1e6
            disk_ok = free >= self.cfg.min_free_disk_mb
            disk_msg = f"{free:,.0f} MB free on disk"
        except Exception as exc:
            disk_msg = f"disk space unknown ({exc})"
        mem = _free_memory_mb()
        mem_ok = mem is None or mem >= self.cfg.min_free_memory_mb
        mem_msg = "memory not measured on this machine" if mem is None else f"{mem:,.0f} MB memory free"
        add("disk_memory", disk_ok and mem_ok, f"{disk_msg}; {mem_msg}")
        self.health = checks
        self.entries_allowed = (self.executor is not None and self.mode in ("PAPER", "LIVE")
                                and all(c["ok"] for c in checks if c["critical"]))
        return checks

    # --------------------------------------------------------------- status --
    def day_start(self, now: dt.datetime) -> dt.datetime:
        day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        clk = getattr(self.broker, "clock", None)
        if clk is not None and hasattr(clk, "day_start_utc"):
            try:
                day = to_utc(clk.day_start_utc(now))
            except Exception:
                pass
        return day

    def today(self, now: dt.datetime) -> dict:
        rows = [r for r in self.journal.closed_since(self.day_start(now), self.mode)
                if not str(r.get("exit_reason") or "").startswith("SWITCHED")]
        money = [float(r["net_money"]) for r in rows if r.get("net_money") is not None]
        won = sum(1 for r in rows if (r.get("net_money") if r.get("net_money") is not None else r.get("realised_pips") or 0) > 0)
        lost = len(rows) - won
        best = None
        for r in rows:
            v = r.get("net_money") if r.get("net_money") is not None else r.get("realised_pips")
            if v is None:
                continue
            if best is None or v > best[0]:
                best = (v, r)
        estimated = any(r.get("money_source") == "estimate" for r in rows)
        return {
            "mode": self.mode, "trades": len(rows), "won": won, "lost": lost,
            "win_rate": round(100.0 * won / len(rows), 1) if rows else None,
            "net_pips": round(sum(float(r.get("realised_pips") or 0.0) for r in rows), 1),
            "net_money": round(sum(money), 2) if rows else 0.0,
            "net_money_estimated": estimated,
            "money_source": "the broker's figures" if self.mode == "LIVE" else "practice (paper) figures",
            "best_trade": (f"{best[1]['symbol']} {float(best[1].get('realised_pips') or 0.0):+.1f} pips "
                           f"({_money(best[1].get('net_money'))})") if best else None,
        }

    def status(self, now: dt.datetime) -> dict:
        positions = []
        for p in self.positions.values():
            tk = self.latest.get(p.symbol)
            mark = float(tk.bid if p.side is Side.BUY else tk.ask) if tk else p.entry
            pips = p.pips_of(mark)
            positions.append({"ticket": p.ticket, "market": p.symbol, "side": p.side.value, "volume": p.volume,
                              "entry": p.entry, "stop": p.flow.stop, "pips": round(pips, 1),
                              "money": round(pips * p.gbp_per_pip, 2), "money_note": "approximate: pips x GBP per pip",
                              "flowlock_state": p.flow.state, "flowlock": PLAIN.get(p.flow.state, p.flow.state),
                              "decay": round(p.flow.decay, 2), "opened_utc": to_utc(p.opened_at).isoformat()})
        stories = [r.get("explanation") for r in self.journal.recent_closed(8) if r.get("explanation")]
        crit = [c["message"] for c in self.health if not c["ok"] and c["critical"]]
        warn = [c["message"] for c in self.health if not c["ok"] and not c["critical"]]
        if crit:
            summary = "new entries paused (open positions are still managed): " + "; ".join(crit)
        elif warn:
            summary = "warnings: " + "; ".join(warn)
        else:
            summary = "all checks passing"
        return {
            "strategy_id": STRATEGY_ID, "label": STRATEGY_LABEL, "tagline": TAGLINE, "magic": MAGIC,
            "mode": self.mode, "updated_utc": now.isoformat(), "pid": os.getpid(),
            "user_pip_value_gbp": self.pip_value_gbp, "tick_feed": self.tick_feed,
            "order_flow": "not available on retail MetaTrader FX - never faked" if not ORDER_FLOW_AVAILABLE else "available",
            "health": {"entries_allowed": self.entries_allowed, "summary": summary, "checks": self.health},
            "scanner": [r.to_dict() for r in self.last_rows],
            "open_positions": positions,
            "today": self.today(now),
            "stories": stories,
            "currency_strength": self.core.strength.to_dict(),
            "notes": list(self.notes)[-15:],
            "cycle_ms": round(self.cycle_ms, 1),
        }

    def write_status(self, now: dt.datetime) -> None:
        try:
            data = self.status(now)
            tmp = self.status_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(data, indent=1, default=str))
            tmp.replace(self.status_path)
        except Exception as exc:
            log.warning("status write failed: %s", exc)

    def shutdown(self) -> None:
        """Positions are NOT closed on a shutdown: each has its stop at the
        broker, and the next start restores and manages them."""
        try:
            self.write_status(to_utc(self.clock()))
        except Exception:
            pass
        try:
            self.journal.log_event("STOP", f"stopped with {len(self.positions)} open", {}, to_utc(self.clock()))
        except Exception:
            pass


def _free_memory_mb() -> Optional[float]:
    try:
        import psutil  # type: ignore
        return psutil.virtual_memory().available / 1e6
    except Exception:
        pass
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemAvailable:"):
                    return float(line.split()[1]) / 1024.0
    except Exception:
        pass
    return None
