"""Financial Ian's engine: DATA -> INTELLIGENCE -> EXECUTION, one pass at a time.

Each pass:

1. takes the events the feed has delivered, checks them (data quality),
   records them, and applies them to each instrument's book and flow state;
2. works out the feed state: NOT CONFIGURED / LIVE / DEGRADED / DOWN. Only an
   instrument whose data is LIVE can produce a signal, and nothing new is
   entered on any future unless the feed as a whole is LIVE; DEGRADED names
   the futures at fault, asks the feed to recover (with a back-off) and keeps
   managing what is open;
3. for every instrument: features -> regime -> opportunity score (with the
   RETAIL spot chart, the MT5 calendar and the other futures as context);
4. hands the best tradeable opportunities to the MT5 bridge as signals, which
   validates them and places them with the broker stop at once (idempotent);
5. manages open positions with the order-flow trail (never widens), exits
   when the thesis dies, and books every close with the broker's own figure
   in LIVE;
6. writes data/ian-status.json for the page.

Safety: OFF has no executor. A synthetic or replayed feed never drives a LIVE
order (and its results are labelled SYNTHETIC - NOT PERFORMANCE / REPLAY).
The bot's own daily loss limit stops NEW entries for the day only; it does
not touch the account-level daily-loss logic of the main bot.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import math
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Callable, Optional

from ..botexec import make_executor
from ..broker.base import Position, Side, TF, Tick
from ..clock import to_utc, utcnow
from ..scalper.features import TickBuffer
from . import INSTITUTIONAL, MAGIC, RETAIL, STRATEGY_ID, STRATEGY_LABEL, SYNTHETIC_LABEL, TAG, TAGLINE
from .book import FeedNotice, received_at
from .bridge import FILLED_S, BridgeConfig, ExecutionReport, Mt5Bridge, TimedBroker
from .dataquality import DEGRADED, DOWN, LIVE, NOT_CONFIGURED, DataQualityMonitor, QualityConfig
from .features import FlowConfig, FlowSnapshot, InstrumentFlow
from .feeds.base import FINISHED, FeedAdapter, NotConfiguredFeed
from .journal import ABSENT, ADOPTED, FILLED, REJECTED, SENDING, UNKNOWN, Journal
from .mapping import (ALL, CONTRACTS, contract, front_month, futures_direction, root_of, spot_symbol)
from .recorder import Recorder, recording_dir
from .regime import classify
from .score import ChartContext, Opportunity, ScoreConfig, chart_context, cross_market_value, news_context, score
from .signal import StopConfig, build_signal
from .trail import FlowRead, FlowTrail, TrailConfig

log = logging.getLogger("mintel.ian")

PAPER_FIRST_TICKET = 900_000_001
MISSING_PASSES = 3
MISSING_SECONDS = 60.0
RECHECK_SECONDS = 10.0                        # a trade missing that long: its closing deal is looked for this often
ENTRY_LOOKBACK = dt.timedelta(minutes=10)     # the deal history is searched from this long before an unknown send
STOP_NOTE = f"{STRATEGY_LABEL}: broker stop"


@dataclass
class IanConfig:
    mode: str = "PAPER"                       # OFF / PAPER / LIVE
    magic: int = MAGIC
    instruments: tuple = ("6E", "6B", "6A", "6N", "6C", "6J", "6S")
    context: tuple = ("ES", "GC", "ZN")       # watched as evidence when the feed carries them; never traded
    feed: dict = field(default_factory=lambda: {"vendor": "none"})
    roll_days: int = 8
    decision_interval_s: float = 0.5
    status_interval_s: float = 2.0
    record: bool = True
    risk_money: float = 10.0
    max_risk_money: float = 20.0
    max_lots: float = 0.5
    max_open: int = 3
    max_entries_per_day: int = 6              # per future
    cooldown_s: float = 120.0                 # after a close, per future
    max_daily_loss_money: float = 50.0        # this bot only: no NEW entries for the rest of the day
    signal_bucket_s: int = 60
    entry_score: float = 60.0
    min_groups: int = 3
    hold_max_minutes: float = 240.0
    weekend_flat_utc: str = "20:30"
    paper_slippage_points: float = 1.0
    bars_refresh_s: float = 30.0
    calendar_refresh_s: float = 300.0
    max_quote_age_s: float = 5.0
    max_spread_pips: float = 2.5
    min_stop_pips: float = 6.0
    max_stop_pips: float = 30.0
    allow_paper_on_synthetic: bool = False    # research and tests only: a synthetic feed normally places nothing

    @classmethod
    def default_path(cls, data_dir: str | Path) -> Path:
        return Path(data_dir) / "ian.json"

    @classmethod
    def load(cls, path: str | Path) -> "IanConfig":
        """Tolerant: unknown keys ignored, a bad value keeps the default, a broken file gives PAPER with no feed."""
        c = cls()
        p = Path(path)
        try:
            raw = json.loads(p.read_text()) if p.exists() else {}
            if not isinstance(raw, dict):
                raw = {}
        except Exception:
            raw = {}
        for f in fields(cls):
            if f.name not in raw:
                continue
            v = raw[f.name]
            cur = getattr(c, f.name)
            try:
                if isinstance(cur, tuple):
                    v = tuple(v)
                elif isinstance(cur, dict):
                    v = dict(v)
                elif isinstance(cur, bool):
                    v = bool(v)
                elif isinstance(cur, (int, float)) and not isinstance(cur, bool):
                    v = type(cur)(v)
                else:
                    v = str(v)
                setattr(c, f.name, v)
            except Exception:
                pass
        c.mode = str(c.mode or "PAPER").upper()
        if c.mode not in ("OFF", "PAPER", "LIVE"):
            c.mode = "PAPER"
        if c.magic != MAGIC:
            # the LIVE sweep closes any position with this magic it has no record of: another bot's magic
            # here would close that bot's trades, so Ian's own number is fixed
            log.warning("%s: ian.json sets magic %r; this bot's magic is fixed at %d - ignored",
                        STRATEGY_LABEL, raw.get("magic"), MAGIC)
            c.magic = MAGIC
        return c

    @staticmethod
    def save_minimal(path: str | Path, mode: str = "PAPER") -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({"mode": mode, "feed": {"vendor": "none"}}, indent=2))


@dataclass
class IanTrade:
    ticket: int
    signal_id: str
    root: str
    instrument: str
    spot_symbol: str
    side: Side
    volume: float
    entry: float
    initial_stop: float
    opened: dt.datetime
    trail: FlowTrail
    mode: str
    data_source: str
    session_date: str
    regime: str = ""
    score: float = 0.0
    reasoning: str = ""
    last_mark: float = 0.0
    last_reason: str = ""
    broker_pnl: Optional[float] = None
    missing_passes: int = 0
    missing_since: Optional[dt.datetime] = None
    missing_noted: bool = False
    missing_checked: Optional[dt.datetime] = None

    @property
    def stop(self) -> float:
        return self.trail.stop


def _hhmm(t: str) -> dt.time:
    h, m = str(t).split(":")
    return dt.time(int(h), int(m))


def _clip(x: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return lo if x < lo else hi if x > hi else x


class IanEngine:
    def __init__(self, data_dir: str | Path, cfg: Optional[IanConfig] = None, broker=None,
                 feed: Optional[FeedAdapter] = None, clock: Callable[[], dt.datetime] = utcnow, executor=None,
                 spec_fn: Optional[Callable[[str], object]] = None, wall: Callable[[], dt.datetime] = utcnow):
        self.cfg = cfg or IanConfig()
        if str(self.cfg.mode).upper() == "LIVE" and int(self.cfg.magic) != MAGIC:
            # checked before anything is opened or any LIVE executor is built: the sweep closes every
            # position carrying this magic that it has no record of
            raise ValueError(f"{STRATEGY_LABEL} trades LIVE only with its own magic number {MAGIC}, "
                             f"not {self.cfg.magic}")
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.broker = broker
        self.feed = feed or NotConfiguredFeed()
        self.clock = clock
        self.wall = wall
        self.spec_fn = spec_fn or (lambda s: broker.spec(s) if broker is not None else None)
        self.journal = Journal(self.data_dir / "ian.sqlite")
        self.status_path = self.data_dir / "ian-status.json"
        self.feed_state = NOT_CONFIGURED
        self.feed_reason = ""
        self.degraded: list[str] = []                  # the futures whose data is not LIVE (named on the page)
        # unknown sends the positions reads have not shown: signal id -> (reads in a row, time of the first)
        self._misses: dict[str, tuple[int, dt.datetime]] = {}
        self._no_exec = ""                             # why nothing can be placed, when that is not the mode's choice
        self._link_down = False
        self._subscribe()                    # first: a replay only knows what it holds once it has opened it
        self.synthetic = bool(getattr(self.feed, "synthetic", False))
        self.virtual = bool(getattr(self.feed, "virtual_time", False))
        self.data_source = ("SYNTHETIC" if self.synthetic else "REPLAY" if self.virtual else "LIVE FEED")
        self.quality = DataQualityMonitor(QualityConfig(), getattr(self.feed, "sequence_scope", "instrument"))
        self.flows: dict[str, InstrumentFlow] = {}
        self.raw_of: dict[str, str] = {}
        self.latest: dict[str, FlowSnapshot] = {}
        self.opps: dict[str, Opportunity] = {}
        self.open: dict[str, IanTrade] = {}            # spot symbol -> trade
        self.notes: dict[str, str] = {}
        self.spot_ticks: dict[str, TickBuffer] = {}
        self._bars: dict[str, tuple] = {}
        self._calendar: tuple = (None, [])
        self._last_step: Optional[dt.datetime] = None
        self._last_status: Optional[dt.datetime] = None
        self._last_minute = ""
        self.cooldown: dict[str, dt.datetime] = {}
        self.events_in = 0
        self.unknown_instruments: set[str] = set()
        self.last_report: Optional[dict] = None
        self.last_signal_text = ""
        self._pending_notes: list[str] = []
        self.recorder = Recorder(recording_dir(self.data_dir)) if (self.cfg.record and not self.virtual) else None
        # the executor: OFF none; a synthetic/replay feed never trades LIVE (and places nothing unless allowed)
        mode = self.cfg.mode
        if self.virtual and mode == "LIVE":
            self._pending_notes.append(f"{STRATEGY_LABEL}: a {self.data_source.lower()} feed never trades LIVE; running PAPER")
            mode = "PAPER"
        if self.synthetic and not self.cfg.allow_paper_on_synthetic:
            mode = "OFF" if mode != "OFF" else mode
        base = self.journal.max_ticket(PAPER_FIRST_TICKET - 1)
        self.timed = TimedBroker(broker, wall) if (broker is not None and mode == "LIVE") else None
        if executor is not None:
            self.executor = executor
        elif mode == "LIVE" and broker is None:
            # LIVE without MetaTrader: run (status, feed, records) but nothing can ever be sent
            self.executor = None
            self._no_exec = "LIVE needs MetaTrader: nothing can be placed"
            self._pending_notes.append(f"{STRATEGY_LABEL}: {self._no_exec}")
        else:
            self.executor = make_executor(mode, self.timed or broker, self.cfg.magic, TAG,
                                          self.cfg.paper_slippage_points, max(base + 1, PAPER_FIRST_TICKET))
        self.bridge = Mt5Bridge(self.executor, self.journal, broker,
                                BridgeConfig(max_quote_age_s=self.cfg.max_quote_age_s,
                                             max_spread_pips=self.cfg.max_spread_pips, risk_money=self.cfg.risk_money,
                                             max_risk_money=self.cfg.max_risk_money, max_lots=self.cfg.max_lots,
                                             paper_slippage_points=self.cfg.paper_slippage_points),
                                self.spec_fn, self.timed, self.cfg.magic)
        self.score_cfg = ScoreConfig(entry_score=self.cfg.entry_score, min_groups=self.cfg.min_groups)
        self.stop_cfg = StopConfig(min_stop_pips=self.cfg.min_stop_pips, max_stop_pips=self.cfg.max_stop_pips)
        self.flow_cfg = FlowConfig()
        self._restore()

    # ------------------------------------------------------------ properties --
    @property
    def mode(self) -> str:
        return getattr(self.executor, "mode", "OFF") if self.executor is not None else "OFF"

    @property
    def live(self) -> bool:
        return self.mode == "LIVE"

    def _now(self) -> dt.datetime:
        if self.virtual:
            t = self.feed.now()
            if t is not None:
                return t
        return to_utc(self.clock())

    # ----------------------------------------------------------------- feed --
    def _wanted(self, day: dt.date) -> list[str]:
        out = []
        for r in list(self.cfg.instruments) + list(self.cfg.context):
            if r in ALL:
                try:
                    out.append(front_month(r, day, self.cfg.roll_days).raw_symbol)
                except Exception:
                    pass
        return out

    def _check_roll(self, now: dt.datetime) -> list[str]:
        """Once an hour: has the front month of any future rolled? Then subscribe to the new contracts
        (the old contract's flow state is dropped when the new one's first event arrives)."""
        key = now.strftime("%Y-%m-%d %H")
        if getattr(self, "_roll_key", "") == key or self.virtual:
            return []
        self._roll_key = key
        wanted = self._wanted(now.date())
        if wanted == self.subscribed:
            return []
        old, self.subscribed = self.subscribed, wanted
        try:
            self.feed.subscribe(wanted)
        except Exception as exc:
            return [f"{STRATEGY_LABEL}: the roll to {', '.join(sorted(set(wanted) - set(old)))} could not be subscribed ({exc})"]
        msg = f"{STRATEGY_LABEL}: rolled to {', '.join(sorted(set(wanted) - set(old)))}"
        self.journal.event("ROLL", msg, now=now)
        return [msg]

    def _subscribe(self) -> None:
        # a replay or a scenario (virtual time) plays in full: the front month by TODAY's date says nothing
        # about a recording made before a roll, and filtering by it would make a replay depend on the day
        # it is run
        virtual = bool(getattr(self.feed, "virtual_time", False))
        wanted = [] if virtual else self._wanted(to_utc(self.clock()).date())
        self.subscribed = wanted
        try:
            if self.feed.connect() and not virtual:
                self.feed.subscribe(wanted)
        except Exception as exc:
            self.feed_reason = f"the feed would not start: {exc}"

    def _flow_for(self, instrument: str) -> Optional[InstrumentFlow]:
        f = self.flows.get(instrument)
        if f is not None:
            return f
        if "-" in instrument:                        # a calendar spread, not an outright
            return None
        root = root_of(instrument)
        if not root or root not in (set(self.cfg.instruments) | set(self.cfg.context)):
            self.unknown_instruments.add(instrument)
            return None
        cur = self.raw_of.get(root)
        if cur is not None and cur != instrument:
            # the roll: a new contract for the same future replaces the old one's state
            self.flows.pop(cur, None)
            self.latest.pop(root, None)
        self.raw_of[root] = instrument
        f = InstrumentFlow(instrument, self.flow_cfg, aggressor_reliable=True)
        self.flows[instrument] = f
        return f

    def ingest(self, events) -> int:
        n = 0
        for ev in events:
            n += 1
            self.events_in += 1
            try:
                self.quality.observe(ev)
            except Exception as exc:
                log.warning("quality check failed: %s", exc)
            if self.recorder is not None:
                self.recorder.write(ev)
            if isinstance(ev, FeedNotice):
                self.journal.event("FEED", f"{ev.notice}: {ev.detail}", ev.instrument, received_at(ev))
                continue
            f = self._flow_for(ev.instrument)
            if f is not None:
                try:
                    f.on_event(ev)
                except Exception as exc:
                    log.warning("flow update failed for %s: %s", ev.instrument, exc)
        return n

    def pump(self, max_events: int = 20_000) -> int:
        try:
            evs = self.feed.events(max_events)
        except Exception as exc:
            self.feed_reason = f"the feed failed: {exc}"
            return 0
        return self.ingest(evs)

    def _feed_state(self, now: dt.datetime) -> tuple[str, str]:
        self._link_down = True                   # the feed's own connection, not the data's quality
        self.degraded = []
        try:
            h = self.feed.health()
        except Exception as exc:
            return DOWN, f"the feed cannot report its health: {exc}"
        if h.state == NOT_CONFIGURED:
            self._link_down = False
            return NOT_CONFIGURED, h.reason
        if h.state in (DOWN, "CONNECTING"):
            return DOWN, h.reason or "not connected"
        self._link_down = False
        roots = [root_of(i) for i in self.flows if root_of(i) in self.cfg.instruments]
        states = [self.quality.state(i, now, f.book, LIVE if h.state != FINISHED else FINISHED)
                  for i, f in self.flows.items() if root_of(i) in self.cfg.instruments]
        if not states:
            return DEGRADED, f"{h.reason}; no order-book data received yet"
        if all(s.state == DOWN for s in states):
            return DOWN, "; ".join(sorted({r for s in states for r in s.reasons}))[:300]
        bad = [s for s in states if s.state != LIVE]
        if bad:
            self.degraded = sorted({r for r, s in zip(roots, states) if s.state != LIVE})
            return DEGRADED, "; ".join(sorted({r for s in bad for r in s.reasons}))[:300]
        return LIVE, h.reason

    # ---------------------------------------------------------------- retail --
    def _tick(self, symbol: str) -> Optional[Tick]:
        if self.broker is None or not symbol:
            return None
        try:
            t = self.broker.tick(symbol)
        except Exception:
            return None
        if t is not None:
            buf = self.spot_ticks.get(symbol)
            if buf is None:
                buf = self.spot_ticks[symbol] = TickBuffer(symbol, seconds=120.0)
            buf.add(t)
        return t

    def _chart(self, symbol: str, now: dt.datetime) -> Optional[ChartContext]:
        if self.broker is None or not symbol:
            return None
        cached = self._bars.get(symbol)
        if cached is not None and (now - cached[0]).total_seconds() < self.cfg.bars_refresh_s:
            return cached[1]
        try:
            m1 = self.broker.bars(symbol, TF.M1, 60)
            m5 = self.broker.bars(symbol, TF.M5, 60)
            h1 = self.broker.bars(symbol, TF.H1, 24)
            ctx = chart_context(symbol, m1, m5, h1)
            self._bars[symbol] = (now, ctx, m1)
        except Exception as exc:
            ctx = ChartContext(symbol, False, note=f"no bars ({exc})")
            self._bars[symbol] = (now, ctx, [])
        return ctx

    def _m1(self, symbol: str) -> list:
        c = self._bars.get(symbol)
        return list(c[2]) if c is not None and len(c) > 2 else []

    def _events(self, now: dt.datetime) -> list:
        at, evs = self._calendar
        if at is not None and (now - at).total_seconds() < self.cfg.calendar_refresh_s:
            return evs
        out = []
        if self.broker is not None:
            ccys = sorted({c.currency for c in CONTRACTS.values()} | {"USD"})
            try:
                out = list(self.broker.calendar(now - dt.timedelta(hours=1), now + dt.timedelta(hours=2), ccys))
            except Exception:
                out = []
        extra = getattr(self.feed, "calendar_events", None)
        if callable(extra):
            try:
                out += list(extra())
            except Exception:
                pass
        self._calendar = (now, out)
        return out

    def _exec_quality(self, symbol: str, tick: Optional[Tick], now: dt.datetime) -> tuple[float, str]:
        if self.broker is None:
            return 1.0, "no MetaTrader connection: spot execution not checked"
        spec = self._spec(symbol)
        if tick is None or spec is None:
            return 0.0, "no spot quote"
        age = (now - to_utc(tick.time)).total_seconds()
        if age > self.cfg.max_quote_age_s:
            return 0.3, f"spot quote {age:.0f} s old"
        pip = spec.pip_size
        sp = (tick.ask - tick.bid) / pip if pip else 0.0
        typ = float(spec.typical_spread_points or 0.0) * spec.point / pip if pip else 0.0
        if typ <= 0 or sp <= 1.5 * typ:
            return 1.0, f"spread {sp:.1f} pips"
        return _clip(1.5 * typ / sp, 0.2, 1.0), f"spread {sp:.1f} pips vs typical {typ:.1f}"

    def _spec(self, symbol: str):
        try:
            return self.spec_fn(symbol)
        except Exception:
            return None

    def _broker_symbols(self) -> Optional[list]:
        if self.broker is None:
            return None
        try:
            return list(self.broker.symbols())
        except Exception:
            return None

    # ------------------------------------------------------------------ step --
    def step(self, now: Optional[dt.datetime] = None) -> list[str]:
        now = to_utc(now or self._now())
        notes: list[str] = list(self._pending_notes)
        self._pending_notes.clear()
        notes += self._check_roll(now)
        self.feed_state, self.feed_reason = self._feed_state(now)
        snaps: dict[str, FlowSnapshot] = {}
        quality: dict[str, str] = {}
        tried = False
        for inst, f in list(self.flows.items()):
            root = root_of(inst)
            q = self.quality.state(inst, now, f.book, LIVE)
            quality[root] = q.text()
            try:
                fs = f.compute(now)
            except Exception as exc:
                log.warning("features failed for %s: %s", inst, exc)
                continue
            self.latest[root] = fs
            if q.ok:
                snaps[root] = fs
            elif self.quality.needs_recovery(inst) and self.quality.recovery_due(inst, now):
                ok = False
                try:
                    ok = self.feed.recover(q.text())
                except Exception:
                    ok = False
                tried = True
                self.journal.event("RECOVERY", f"{q.text()}; recovery attempt {'made' if ok else 'failed'}", inst, now)
        # the feed's own connection is down (it never came up at start-up, or a reconnect failed): retry it
        # with the same back-off, whether or not any instrument has data yet
        if not self._link_down:
            self.quality.recovered("_feed")
        elif not tried and self.quality.recovery_due("_feed", now):
            ok = False
            try:
                ok = bool(self.feed.recover(self.feed_reason))
            except Exception:
                ok = False
            self.journal.event("RECOVERY", f"the feed is down ({self.feed_reason}); reconnect attempt "
                                           f"{'made' if ok else 'failed'}", "", now)
        self.quality_text = quality
        # opportunities
        events = self._events(now)
        broker_syms = self._broker_symbols()
        for root in list(self.cfg.instruments):
            fs = snaps.get(root)
            if fs is None:
                lf = self.latest.get(root)
                why = quality.get(root) or ("no data" if self.feed_state != NOT_CONFIGURED else "no feed configured")
                self.notes[root] = f"no signals: {why}" if lf is not None else f"waiting: {why}"
                self.opps.pop(root, None)
                continue
            sym = spot_symbol(root, broker_syms) or (contract(root).spot if contract(root) else "")
            tick = self._tick(sym)
            news = news_context(root, events, now)
            reg = classify(fs, news, self.flows[self.raw_of[root]].absorption_episodes)
            chart = self._chart(sym, now)
            cross = cross_market_value(root, snaps)
            eq, eq_note = self._exec_quality(sym, tick, now)
            opp = score(fs, reg, spot_symbol=sym, chart=chart, news=news, cross=cross, exec_quality=eq,
                        exec_note=eq_note, history_mult=1.0, cfg=self.score_cfg)
            if opp.direction:
                hist, hist_note = self.journal.history(root, reg.regime, opp.direction, data_source=self.data_source)
                if hist != 1.0:
                    opp = score(fs, reg, spot_symbol=sym, chart=chart, news=news, cross=cross, exec_quality=eq,
                                exec_note=eq_note, history_mult=hist, history_note=hist_note, cfg=self.score_cfg)
            if self.broker is None and opp.tradeable:
                opp.tradeable = False
                opp.why_not = "no MetaTrader connection: nothing can be placed"
            self.opps[root] = opp
            self.notes[root] = opp.reasoning if opp.tradeable else f"{opp.regime.regime}: {opp.why_not or opp.reasoning}"
        # manage what is open (always, whatever the feed state)
        if self.executor is not None:
            minute = now.strftime("%Y-%m-%d %H:%M")
            if self.live and minute != self._last_minute:
                self._last_minute = minute
                notes += self._sweep(now)
            for sym, tr in list(self.open.items()):
                try:
                    n = self._manage(tr, now)
                    if n:
                        notes.append(n)
                except Exception as exc:
                    notes.append(f"{STRATEGY_LABEL}: {sym} management failed this pass ({exc})")
        # new entries: only while the feed as a whole is LIVE. Any traded future at fault - one that has sent
        # data and is now DEGRADED or DOWN, for any reason, a quiet tape included - stops new entries on every
        # future until its data is clean, and the headline names it. A traded future that has sent nothing
        # yet cannot signal and does not count. What is open is still managed above
        if self.executor is not None and self.feed_state == LIVE:
            notes += self._entries(now, snaps)
        elif self.feed_state == DEGRADED:
            why = f"the data is degraded on {', '.join(self.degraded) or 'the feed'}; no new trades until it is clean"
            for root, opp in self.opps.items():
                if opp.tradeable and root in snaps:
                    self.notes[root] = f"{opp.reasoning} Not entered: {why}"
        self._last_step = now
        self.write_status(now)
        return notes

    def _entries(self, now: dt.datetime, snaps: dict) -> list[str]:
        notes = []
        cand = sorted((o for r, o in self.opps.items() if o.tradeable and r in snaps), key=lambda o: -o.score)
        if not cand:
            return notes
        for opp in cand:
            root = opp.root
            # asked again for every candidate: a fill closed at once earlier in this pass is booked as a loss
            # and can take this bot past its own daily loss limit
            why_all = self._entry_block(now)
            if why_all:
                self.notes[root] = f"{opp.reasoning} Not entered: {why_all}"
                continue
            why = self._market_block(opp, now)
            if why:
                self.notes[root] = f"{opp.reasoning} Not entered: {why}"
                continue
            fs = snaps[root]
            tick = self._tick(opp.spot_symbol)
            spec = self._spec(opp.spot_symbol)
            if tick is None or spec is None:
                self.notes[root] = f"{opp.reasoning} Not entered: no spot quote or contract for {opp.spot_symbol}"
                continue
            received = self.quality.inst.get(fs.instrument)
            sig, why = build_signal(opp, fs, tick, spec, now, self._m1(opp.spot_symbol), self.stop_cfg,
                                    self.data_source, received.last_rx if received else None,
                                    self.cfg.signal_bucket_s)
            if sig is None:
                self.notes[root] = f"{opp.reasoning} Not entered: {why}"
                continue
            why = self._unknown_send(opp.spot_symbol, sig.signal_id)
            if why:
                self.notes[root] = f"{opp.reasoning} Not entered: {why}"
                continue
            self.journal.record_signal(sig, "NEW", mode=self.mode)
            buf = self.spot_ticks.get(opp.spot_symbol)
            hist = list(buf.ticks) if buf is not None else []
            rep = self.bridge.submit(sig, tick, now, hist, spec)
            self.last_report = rep.to_dict()
            self.journal.signal_update(sig.signal_id, rep.status, rep.message, rep.to_dict(), rep.latency)
            if rep.ok and rep.status == FILLED_S and opp.spot_symbol not in self.open:
                note = self._opened(sig, rep, opp, fs, now)
                if rep.message and rep.message not in ("filled", "paper fill"):
                    note += f" Broker: {rep.message}."             # an adoption, an unconfirmed stop: said plainly
                notes.append(note)
                self.last_signal_text = note
            else:
                self.notes[root] = f"{opp.reasoning} Not entered: {rep.message}"
                if rep.status not in ("REFUSED", "DUPLICATE"):
                    notes.append(f"{STRATEGY_LABEL}: {opp.spot_symbol} order {rep.status.lower()}: {rep.message}")
                if rep.status == UNKNOWN:
                    self._last_minute = ""         # the sweep runs on the next pass, not up to a minute later
                elif rep.status == REJECTED and rep.ticket:
                    booked = self._book_no_stop(sig, rep, opp, now)        # it did fill: a real round trip
                    if booked:
                        notes.append(booked)
            if len(self.open) >= self.cfg.max_open:
                break
        return notes

    def _entry_block(self, now: dt.datetime) -> str:
        if self.synthetic and not self.cfg.allow_paper_on_synthetic:
            return "synthetic data: nothing is ever placed"
        if len(self.open) >= self.cfg.max_open:
            return f"{len(self.open)} positions open (the most at once)"
        if now.weekday() >= 5 or (now.weekday() == 4 and now.time() >= _hhmm(self.cfg.weekend_flat_utc)):
            return "the weekend"
        day0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
        rows = self.journal.closed_since(day0, self.mode)
        rows = [r for r in rows if r.get("data_source") == self.data_source]
        lost = sum(float(r.get("net_pnl") or 0.0) for r in rows)
        if lost <= -abs(self.cfg.max_daily_loss_money):
            return f"this bot has lost {lost:.2f} today (its own limit {self.cfg.max_daily_loss_money:g}); no new trades until tomorrow"
        return ""

    def _market_block(self, opp: Opportunity, now: dt.datetime) -> str:
        if opp.spot_symbol in self.open:
            return f"already in {opp.spot_symbol}"
        cd = self.cooldown.get(opp.root)
        if cd is not None and now < cd:
            return f"cooling down after the last trade ({(cd - now).total_seconds():.0f} s)"
        day = now.date().isoformat()
        if self.journal.entries_on(day, opp.root) >= self.cfg.max_entries_per_day:
            return f"{self.cfg.max_entries_per_day} entries in {opp.root} today already"
        return ""

    def _unknown_send(self, symbol: str, signal_id: str = "") -> str:
        """LIVE: an earlier order on this spot pair whose outcome is unknown (or that was cut off while being
        sent) may be at the broker, so no order with another signal id is sent on the pair until it is
        adopted (the bridge or the sweep finds it) or the sweep marks it ABSENT in the orders table (see
        ``_sweep``). The same signal id goes on to the bridge, which never sends it twice and adopts it if
        the broker has it."""
        if not self.live:
            return ""
        for r in self.journal.orders_in(UNKNOWN, SENDING):
            if r.get("symbol") == symbol and r.get("mode") == "LIVE" and r["signal_id"] != signal_id:
                sent = str(r.get("created_utc") or "")[11:19]
                return (f"an earlier order's outcome is still unknown (sent at {sent} UTC); nothing new on {symbol} "
                        "until it is found at the broker or shown not to be there")
        return ""

    def _opened(self, sig, rep: ExecutionReport, opp: Opportunity, fs: FlowSnapshot, now: dt.datetime) -> str:
        spec = self._spec(sig.spot_symbol)
        point = float(getattr(spec, "point", 0.0) or 0.0)
        entry = float(rep.price)
        stop = float(rep.stop or sig.intended_stop)
        if (entry - stop) * sig.side.sign <= 0:                    # a fill through the stop: protect at once
            stop = entry - sig.side.sign * max(sig.stop_distance, point * 10)
        trail = FlowTrail(sig.side, entry, stop, TrailConfig(), point)
        root = sig.root
        f = self.flows.get(self.raw_of.get(root, ""))
        cached = self._bars.get(sig.spot_symbol)
        book = {"features": fs.as_dict(), "ladder": f.ladder(fs, 10) if f is not None else {},
                "regime": opp.regime.to_dict(), "score": opp.to_dict(), "data_class": INSTITUTIONAL,
                "chart": asdict(cached[1]) if cached is not None and cached[1] is not None else None,
                "chart_data_class": RETAIL, "news": next((c.note for c in opp.components if c.name == "news"), None)}
        tr = IanTrade(int(rep.ticket), sig.signal_id, root, sig.instrument, sig.spot_symbol, sig.side, float(rep.volume),
                      entry, stop, now, trail, self.mode, self.data_source, now.date().isoformat(), opp.regime.regime,
                      opp.score, opp.reasoning, last_mark=entry)
        self.journal.open_trade({"ticket": tr.ticket, "signal_id": sig.signal_id, "instrument": sig.instrument,
                                 "root": root, "spot_symbol": sig.spot_symbol, "side": sig.side.value,
                                 "volume": tr.volume, "entry": entry, "initial_stop": stop, "stop": stop,
                                 "opened_utc": now.isoformat(), "regime": opp.regime.regime, "score": opp.score,
                                 "confidence": opp.confidence, "reasoning": opp.reasoning,
                                 "book_json": json.dumps(book, default=str), "latency_json": json.dumps(rep.latency, default=str),
                                 "mode": self.mode, "data_source": self.data_source,
                                 "session_date": tr.session_date, "risk_money": rep.risk_money})
        self.open[sig.spot_symbol] = tr
        live = f", live ticket {tr.ticket}" if self.live else ""
        return (f"{STRATEGY_LABEL}: {sig.spot_symbol} {sig.direction} {tr.volume:g} lots at {entry} stop {stop} "
                f"(risk {rep.risk_money:.2f}; score {opp.score:.0f}, {opp.regime.regime}){live}. {opp.reasoning}")

    # --------------------------------------------------------------- manage --
    def _flow_read(self, tr: IanTrade, now: dt.datetime) -> FlowRead:
        fs = self.latest.get(tr.root)
        inst = self.raw_of.get(tr.root, "")
        q = self.quality.state(inst, now, self.flows[inst].book if inst in self.flows else None) if inst else None
        if fs is None or not fs.ok or q is None or not q.ok:
            return FlowRead(available=False, note=q.text() if q is not None else "no book")
        d = futures_direction(tr.root, tr.side.sign)              # the trade's direction in the futures book
        support = d * _clip(0.6 * fs.aggr_short + 0.4 * math.tanh(fs.ofi_short))
        opp_abs = fs.absorption_score if fs.absorption_dir == -d else 0.0
        opp_abs = max(opp_abs, max(0.0, -d * fs.refresh_signal) * 0.6)
        regen = max(0.0, -d * fs.consumption_signal)
        opp = self.opps.get(tr.root)
        dead = bool(opp is not None and opp.direction == -d and opp.score >= self.cfg.entry_score)
        return FlowRead(support, d * fs.imbalance_avg, opp_abs, regen, dead, True,
                        opp.reasoning if dead and opp is not None else "")

    def _manage(self, tr: IanTrade, now: dt.datetime) -> str:
        tick = self._tick(tr.spot_symbol)
        if self.live:
            gone, note = self._reconcile(tr, tick, now)
            if gone or tr.missing_passes or note.startswith(STOP_NOTE):
                return note
        if tick is None:
            return ""
        spec = self._spec(tr.spot_symbol)
        mark = float(tick.bid if tr.side is Side.BUY else tick.ask)
        tr.last_mark = mark
        if not self.live and self.executor.stop_hit(tr.ticket, tick):
            return self._close(tr, tick, "STOP", now, level=tr.stop,
                               why=f"the stop at {tr.stop} was reached ({tr.last_reason or 'initial stop'})")
        held = (now - tr.opened).total_seconds() / 60.0
        if held >= self.cfg.hold_max_minutes:
            return self._close(tr, tick, "TIME", now, why=f"held {held:.0f} minutes, the most this bot holds a trade")
        if now.weekday() == 4 and now.time() >= _hhmm(self.cfg.weekend_flat_utc):
            return self._close(tr, tick, "WEEKEND", now, why="flat for the weekend")
        flow = self._flow_read(tr, now)
        try:
            min_d = spec.min_stop_distance_price(tick.ask - tick.bid) if spec is not None else 0.0
        except Exception:
            min_d = 0.0
        old = tr.trail.stop
        dec = tr.trail.update(mark, flow, min_d)
        if dec.exit:
            return self._close(tr, tick, "THESIS", now, why=dec.reason)
        if dec.moved:
            new = spec.normalise_price(dec.stop) if spec is not None else dec.stop
            new = max(old, new) if tr.side is Side.BUY else min(old, new)        # never widens, after rounding too
            if new == old:
                tr.trail.stop = old
                return ""
            ok = self.executor.modify_stop(tr.ticket, new)
            if not ok:
                tr.trail.stop = old                                             # the broker still holds the old one
                return f"{STRATEGY_LABEL}: {tr.spot_symbol} stop move to {new} refused ({getattr(self.executor, 'last_error', '')}); kept {old}"
            tr.trail.stop = new
            tr.last_reason = dec.reason
            self.journal.trade_stop(tr.ticket, new)
            return f"{STRATEGY_LABEL}: {tr.spot_symbol} stop tightened {old} -> {new} ({dec.reason.split(': ', 1)[-1]})"
        return ""

    def _close(self, tr: IanTrade, tick: Tick, reason: str, now: dt.datetime, level: Optional[float] = None,
               why: str = "") -> str:
        spec = self._spec(tr.spot_symbol)
        fill = self.executor.close(tr.ticket, tick, spec, at_price=level, comment=f"{TAG} {reason}"[:31])
        if not fill.ok:
            return f"{STRATEGY_LABEL}: {tr.spot_symbol} close ({reason}) refused: {fill.message}; still open, retried next pass"
        exit_px = float(fill.price)
        net = float(spec.money((exit_px - tr.entry) * tr.side.sign, tr.volume)) if spec is not None else 0.0
        when = now
        if self.live:
            deal = self.executor.closed_deal(tr.ticket)
            if deal:
                exit_px = float(deal.get("exit_price") or exit_px)
                net = float(deal["pnl"])
            else:
                reason += "?"
        return self._finalise(tr, exit_px, net, reason, when, why)

    def _finalise(self, tr: IanTrade, exit_px: float, net: float, reason: str, when: dt.datetime, why: str = "") -> str:
        t = tr.trail
        if (exit_px - t.best) * tr.side.sign > 0:
            t.best = exit_px
        if (exit_px - t.worst) * tr.side.sign < 0:
            t.worst = exit_px
        rr = t.r_of(exit_px)
        mfe, mae = t.mfe_r, t.mae_r
        cap = (rr / mfe) if mfe > 0 else None
        expl = (f"Closed {reason} at {exit_px} for {net:+.2f} ({rr:+.2f} R; best {mfe:+.2f} R, worst {mae:+.2f} R"
                + (f", {cap:.0%} of the best captured" if cap is not None else "") + ")"
                + (f": {why}." if why else "."))
        self.journal.close_trade(tr.ticket, closed_utc=when.isoformat(), exit_price=exit_px, net_pnl=round(net, 2),
                                 realised_r=round(rr, 4), mfe_r=round(mfe, 4), mae_r=round(mae, 4),
                                 capture=round(cap, 4) if cap is not None else None, exit_reason=reason,
                                 exit_explanation=expl, stop=t.stop,
                                 duration_seconds=(when - tr.opened).total_seconds())
        if self.open.get(tr.spot_symbol) is tr:
            self.open.pop(tr.spot_symbol)
        self.cooldown[tr.root] = when + dt.timedelta(seconds=self.cfg.cooldown_s)
        return f"{STRATEGY_LABEL}: {tr.spot_symbol} {tr.side.value} {expl}"

    def _reconcile(self, tr: IanTrade, tick, now: dt.datetime) -> tuple[bool, str]:
        """LIVE: is the position still at the broker? Gone -> the broker's closing deal is the figure. An
        empty read is never a close: with no closing deal the trade is kept (and said so once it has been
        missing MISSING_PASSES reads over MISSING_SECONDS), however long that takes."""
        try:
            mine = {q.ticket: q for q in self.executor.positions()}
        except Exception as exc:
            return False, f"{STRATEGY_LABEL}: cannot read the broker's positions ({exc}); {tr.spot_symbol} kept"
        q = mine.get(tr.ticket)
        if q is not None:
            tr.broker_pnl = float(getattr(q, "profit", 0.0) or 0.0) + float(getattr(q, "swap", 0.0) or 0.0)
            tr.missing_passes, tr.missing_since, tr.missing_noted, tr.missing_checked = 0, None, False, None
            held = float(q.sl or 0.0)
            if held and ((held - tr.trail.stop) * tr.side.sign > 0):
                tr.trail.stop = held                            # the broker holds a tighter stop: adopt it, never the reverse
                return False, ""
            tol = 0.5 * float(getattr(self._spec(tr.spot_symbol), "point", 0.0) or 0.0)
            if held and (tr.trail.stop - held) * tr.side.sign <= tol:
                return False, ""                                # the broker holds our stop
            return self._repair_stop(tr, held, tick, now)
        if tr.missing_noted and tr.missing_checked is not None and \
                (now - tr.missing_checked).total_seconds() < RECHECK_SECONDS:
            return False, ""                                    # long missing: the deal history is asked every few seconds
        tr.missing_checked = now
        deal = self.executor.closed_deal(tr.ticket)
        if deal:
            hint = str(deal.get("reason") or "")
            reason = "STOP" if ("sl" in hint.lower() or "stop" in hint.lower()) else ("BROKER_CLOSED" if hint else "STOP")
            return True, self._finalise(tr, float(deal.get("exit_price") or tr.trail.stop), float(deal["pnl"]), reason, now,
                                        "closed at the broker")
        # not in the positions read and no closing deal. Never booked as closed on that alone: the MT5 adapter
        # returns an EMPTY list when its positions read fails (a terminal restart or update), and the position
        # is then still open with its broker stop. It is kept, its stop is not moved, and it is booked only when
        # the broker's closing deal shows how it closed
        tr.missing_passes += 1
        tr.missing_since = tr.missing_since or now
        if tr.missing_passes < MISSING_PASSES or (now - tr.missing_since).total_seconds() < MISSING_SECONDS:
            return False, ""
        if tr.missing_noted:
            return False, ""
        tr.missing_noted = True
        msg = (f"{STRATEGY_LABEL}: ticket {tr.ticket} {tr.spot_symbol} has not shown in the broker's positions for "
               f"{(now - tr.missing_since).total_seconds():.0f} s and the broker has no closing deal for it: kept as "
               "open with its broker stop, not moved, until the broker shows how it closed")
        self.journal.event("NOT_SEEN", msg, tr.spot_symbol, now)
        return False, msg

    def _repair_stop(self, tr: IanTrade, held: float, tick: Optional[Tick], now: dt.datetime) -> tuple[bool, str]:
        """LIVE: the broker holds no stop (or a looser one than ours) for an open trade - a fill whose stop
        was never confirmed, or an order found after an unknown send. Place our stop at once; if the broker
        refuses it, or the price is already through it, close at market. Never left without a stop."""
        sym, want = tr.spot_symbol, float(tr.trail.stop)
        what = "no stop at the broker" if not held else f"a looser stop at the broker ({held} vs {want})"
        through = tick is not None and ((float(tick.bid) <= want) if tr.side is Side.BUY else (float(tick.ask) >= want))
        if through:
            why = f"the price is already through its stop {want}"
        elif self.executor.modify_stop(tr.ticket, want):
            msg = f"{STOP_NOTE} put right: ticket {tr.ticket} {sym} had {what}; its stop {want} was placed at once"
            self.journal.trade_stop(tr.ticket, want)
            self.journal.event("STOP_REPAIR", msg, sym, now)
            return False, msg
        else:
            why = f"the broker refused the stop {want} ({getattr(self.executor, 'last_error', '') or 'no reason given'})"
        mark = tr.last_mark or tr.entry
        t = tick if tick is not None else Tick(sym, now, mark, mark)
        note = self._close(tr, t, "NO_STOP", now, why=f"it had {what} and {why}, so it was closed at market")
        gone = self.open.get(sym) is not tr
        if not gone:
            note = (f"{STOP_NOTE} missing: ticket {tr.ticket} {sym} had {what} and {why}; the close at market was "
                    f"refused ({getattr(self.executor, 'last_error', '') or 'no reason given'}); tried again next pass")
        self.journal.event("STOP_REPAIR", note, sym, now)
        return gone, note

    def _sweep(self, now: dt.datetime) -> list[str]:
        """LIVE, once a minute (and on the pass right after a send whose outcome is unknown): a position with
        our magic that this run is not managing. One whose comment carries the id of one of our orders - a
        send with an UNKNOWN outcome (or one marked ABSENT), or one FILLED or ADOPTED - is ours: with its stop
        it is adopted and managed, unless its pair already has an open trade (then it is closed, so the pair
        never holds two). Any other is closed at once. Every close is booked with the broker's own figure, so
        this bot's daily loss limit counts it. Unknown sends this read does not show are counted
        (``_absence``)."""
        try:
            at = list(self.executor.positions())
        except Exception as exc:
            return [f"{STRATEGY_LABEL}: cannot read the broker's positions ({exc})"]
        known = {t.ticket for t in self.open.values()}
        here = {str(p.comment or "")[len(TAG) + 1:len(TAG) + 11] for p in at if str(p.comment or "").startswith(TAG + " ")}
        notes = self._absence({r["signal_id"][:10]: r for r in self.journal.orders_in(UNKNOWN, SENDING)}, here, now)
        ours = {r["signal_id"][:10]: r for r in self.journal.orders_in(UNKNOWN, SENDING, ABSENT, FILLED, ADOPTED)}
        for p in at:
            if p.ticket in known:
                continue
            cm = str(p.comment or "")
            sid10 = cm[len(TAG) + 1:len(TAG) + 11] if cm.startswith(TAG + " ") else ""
            row = ours.get(sid10)
            tick = self._tick(p.symbol)
            if row is not None and p.sl and p.symbol not in self.open:
                notes.append(self._adopt(p, row, now))
                continue
            spec = self._spec(p.symbol)
            t = tick or Tick(p.symbol, now, float(p.entry_price), float(p.entry_price))
            fill = self.executor.close(p.ticket, t, spec, comment=f"{TAG} orphan")
            if row is None:
                what = f"orphan ticket {p.ticket} {p.symbol} with our magic and no record here"
            elif not p.sl:
                what = f"orphan ticket {p.ticket} {p.symbol} with our magic and no stop at the broker"
            else:                                                     # ours, with its stop, but the pair is taken
                what = (f"ticket {p.ticket} {p.symbol} is one of our orders (signal {row['signal_id']}), but "
                        f"{p.symbol} already has an open trade and the pair never holds two")
            notes.append(f"{STRATEGY_LABEL}: {what}: "
                         + (self._book_orphan(p, row, fill, now) if fill.ok
                            else f"close refused ({fill.message}); it keeps its broker stop"))
            self.journal.event("ORPHAN", notes[-1], p.symbol, now)
        return notes

    def _adopt(self, p, row: dict, now: dt.datetime) -> str:
        """LIVE: a position of ours, with its stop, that this run is not managing (its comment carries the id
        of one of our orders). Adopted and managed from the stop the broker holds, never closed as an orphan."""
        state = str(row.get("state") or "")
        sid = row["signal_id"]
        rec = self.journal.trade(int(p.ticket)) or {}
        srow = self.journal.signal(sid) or {}
        entry, held, sign = float(p.entry_price), float(p.sl), p.side.sign
        point = float(getattr(self._spec(p.symbol), "point", 0.0) or 0.0)
        first = float(rec.get("initial_stop") or row.get("stop") or 0.0) or held
        if (entry - first) * sign <= 0:                               # a fill through its stop: 1 R as at entry
            first = entry - sign * max(abs(entry - first), point * 10, abs(entry) * 1e-4)
        trail = FlowTrail(p.side, entry, first, TrailConfig(), point)
        trail.stop = held                                             # what the broker holds
        root = str(rec.get("root") or srow.get("root") or "")
        try:
            opened = to_utc(dt.datetime.fromisoformat(str(rec.get("opened_utc"))))
        except (TypeError, ValueError):
            try:
                opened = to_utc(p.open_time)                          # no row: the broker's own open time
            except Exception:
                opened = now
        why = ("a send whose outcome was unknown had reached the broker" if state in (UNKNOWN, SENDING, ABSENT)
               else f"its order is on record as {state.lower()} but this run was not managing it")
        tr = IanTrade(int(p.ticket), sid, root, str(rec.get("instrument") or self.raw_of.get(root, "")), p.symbol,
                      p.side, float(p.volume), entry, first, opened, trail, "LIVE", self.data_source,
                      str(rec.get("session_date") or opened.date().isoformat()), str(rec.get("regime") or ""),
                      float(rec.get("score") or 0.0), str(rec.get("reasoning") or f"adopted: {why}"), last_mark=entry)
        if state in (UNKNOWN, SENDING, ABSENT):
            self.journal.order_update(sid, ADOPTED, now, ticket=int(p.ticket), price=entry, volume=float(p.volume),
                                      message="found at the broker by the sweep")
        if not rec:
            self.journal.open_trade({"ticket": tr.ticket, "signal_id": sid, "instrument": tr.instrument, "root": root,
                                     "spot_symbol": p.symbol, "side": p.side.value, "volume": tr.volume, "entry": entry,
                                     "initial_stop": first, "stop": held, "opened_utc": opened.isoformat(), "mode": "LIVE",
                                     "data_source": self.data_source, "session_date": tr.session_date,
                                     "reasoning": f"adopted: {why}"})
        else:
            if rec.get("closed_utc"):
                self.journal.reopen_trade(tr.ticket)                  # booked as closed, but the broker still holds it
            self.journal.trade_stop(tr.ticket, held)
        self.open[p.symbol] = tr
        msg = f"{STRATEGY_LABEL}: ticket {p.ticket} {p.symbol} found at the broker ({why}); adopted with its stop {held} and managed"
        self.journal.event("ADOPT", msg, p.symbol, now)
        return msg

    def _book_orphan(self, p, row: Optional[dict], fill, now: dt.datetime) -> str:
        """A position of ours closed by the sweep: booked with the broker's closing deal (an estimate from the
        close price, marked '?', only when the broker has no deal yet), so this bot's own daily loss limit
        counts it."""
        spec = self._spec(p.symbol)
        exit_px = float(fill.price or p.entry_price)
        net = float(spec.money((exit_px - float(p.entry_price)) * p.side.sign, float(p.volume))) if spec is not None else 0.0
        reason, source = "ORPHAN?", "estimated at the close price (no deal record yet)"
        deal = self.executor.closed_deal(p.ticket)
        if deal:
            exit_px, net = float(deal.get("exit_price") or exit_px), float(deal["pnl"])
            reason, source = "ORPHAN", "the broker's figure"
        rec = self.journal.trade(int(p.ticket))
        sid = str((row or {}).get("signal_id") or "")
        if rec is None:
            srow = self.journal.signal(sid) if sid else None
            try:
                opened = to_utc(p.open_time)
            except Exception:
                opened = now
            self.journal.open_trade({"ticket": int(p.ticket), "signal_id": sid, "root": str((srow or {}).get("root") or ""),
                                     "spot_symbol": p.symbol, "side": p.side.value, "volume": float(p.volume),
                                     "entry": float(p.entry_price), "initial_stop": float(p.sl or 0.0),
                                     "stop": float(p.sl or 0.0), "opened_utc": opened.isoformat(), "mode": "LIVE",
                                     "data_source": self.data_source, "session_date": now.date().isoformat(),
                                     "reasoning": "not managed here: closed by the sweep"})
            opened_at = opened
        else:
            try:
                opened_at = to_utc(dt.datetime.fromisoformat(str(rec.get("opened_utc"))))
            except (TypeError, ValueError):
                opened_at = now
        expl = f"Closed {reason} at {exit_px} for {net:+.2f} ({source}): a position with our magic that this run was not managing."
        self.journal.close_trade(int(p.ticket), closed_utc=now.isoformat(), exit_price=exit_px, net_pnl=round(net, 2),
                                 exit_reason=reason, exit_explanation=expl,
                                 duration_seconds=max(0.0, (now - opened_at).total_seconds()))
        return f"closed at once and booked at {net:+.2f} ({source})"

    def _book_no_stop(self, sig, rep: ExecutionReport, opp: Opportunity, now: dt.datetime) -> str:
        """An order that filled and was closed at once because the broker held no stop for it (the executor's
        close the broker confirms, or the bridge's close of a position found with no stop). A real round
        trip: booked with the broker's closing deal (an estimate from the current price, marked '?', only
        when the broker has no deal yet), so this bot's own daily loss limit, its entries per day and today's
        figures count it. Never booked twice."""
        ticket = int(rep.ticket)
        if self.journal.trade(ticket) is not None:
            return ""
        spec = self._spec(sig.spot_symbol)
        entry = float(rep.price or sig.entry_ref)
        vol = float(rep.volume or (self.journal.order(sig.signal_id) or {}).get("volume") or 0.0)
        tick = self._tick(sig.spot_symbol)
        exit_px = float(tick.bid if sig.side is Side.BUY else tick.ask) if tick is not None else entry
        net = float(spec.money((exit_px - entry) * sig.side.sign, vol)) if spec is not None else 0.0
        reason, source = "NO_STOP?", "estimated at the current price (no deal record yet)"
        deal = self.executor.closed_deal(ticket) if self.executor is not None else None
        if deal:
            exit_px, net = float(deal.get("exit_price") or exit_px), float(deal["pnl"])
            reason, source = "NO_STOP", "the broker's figure"
        self.journal.open_trade({"ticket": ticket, "signal_id": sig.signal_id, "instrument": sig.instrument,
                                 "root": sig.root, "spot_symbol": sig.spot_symbol, "side": sig.side.value,
                                 "volume": vol, "entry": entry, "initial_stop": float(sig.intended_stop),
                                 "stop": float(sig.intended_stop), "opened_utc": now.isoformat(),
                                 "regime": opp.regime.regime, "score": opp.score, "confidence": opp.confidence,
                                 "reasoning": opp.reasoning, "latency_json": json.dumps(rep.latency, default=str),
                                 "mode": self.mode, "data_source": self.data_source,
                                 "session_date": now.date().isoformat(), "risk_money": rep.risk_money})
        expl = (f"Closed {reason} at {exit_px} for {net:+.2f} ({source}): it filled, but the broker held no stop "
                f"for it, so it was closed at once ({rep.message}).")
        self.journal.close_trade(ticket, closed_utc=now.isoformat(), exit_price=exit_px, net_pnl=round(net, 2),
                                 exit_reason=reason, exit_explanation=expl, duration_seconds=0.0)
        self.cooldown[sig.root] = now + dt.timedelta(seconds=self.cfg.cooldown_s)
        msg = f"{STRATEGY_LABEL}: {sig.spot_symbol} ticket {ticket} {sig.side.value} {expl}"
        self.journal.event("NO_STOP", msg, sig.spot_symbol, now)
        return msg

    def _book_unknown_fill(self, row: dict, now: dt.datetime) -> str:
        """LIVE: an order on record as UNKNOWN with the ticket of the broker's fill - the executor reported it
        'closed at once' and the broker's closing deal was not yet in its history when the bridge looked.
        Once that closing deal shows, it is a real round trip: booked with the broker's figure (as
        ``_book_no_stop`` books one), the cool-down started and the order marked REJECTED (nothing is open),
        so this bot's own daily loss limit, its entries per day and today's figures count it. Until the deal
        shows, nothing: the position may still be open, and the sweep finds it. Never booked twice."""
        ticket = int(row.get("ticket") or 0)
        if not ticket or self.executor is None or self.journal.trade(ticket) is not None:
            return ""
        deal = self.executor.closed_deal(ticket)
        if not deal:
            return ""
        sid = row["signal_id"]
        s = self.journal.signal(sid) or {}
        sym = str(row.get("symbol") or s.get("spot_symbol") or "")
        try:
            side = Side(str(row.get("side") or s.get("direction")))
        except ValueError:
            return ""                                   # no side on record: left UNKNOWN, its pair stays blocked
        stop = float(row.get("stop") or s.get("intended_stop") or 0.0)
        entry = float(row.get("price") or 0.0) or stop + side.sign * float(s.get("stop_distance") or 0.0)
        exit_px, net = float(deal.get("exit_price") or entry), float(deal["pnl"])
        try:
            opened = to_utc(dt.datetime.fromisoformat(str(row.get("created_utc"))))
        except (TypeError, ValueError):
            opened = now
        when = deal.get("time")
        closed_at = to_utc(when) if isinstance(when, dt.datetime) else now
        hint = str(deal.get("reason") or "").lower()
        reason = "STOP" if hint.startswith(("sl", "[sl")) else "NO_STOP"
        how = ("its broker stop closed it" if reason == "STOP"
               else "the broker held no stop for it, so it was closed at once")
        root = str(s.get("root") or "")
        self.journal.open_trade({"ticket": ticket, "signal_id": sid, "instrument": s.get("instrument"), "root": root,
                                 "spot_symbol": sym, "side": side.value,
                                 "volume": float(deal.get("volume") or row.get("volume") or 0.0), "entry": entry,
                                 "initial_stop": stop, "stop": stop, "opened_utc": opened.isoformat(),
                                 "regime": s.get("regime"), "score": s.get("score"), "confidence": s.get("confidence"),
                                 "reasoning": s.get("reasoning"), "latency_json": s.get("latency_json"),
                                 "mode": self.mode, "data_source": self.data_source,
                                 "session_date": opened.date().isoformat()})
        expl = (f"Closed {reason} at {exit_px} for {net:+.2f} (the broker's figure): it filled, then {how}; "
                "booked once the broker's record of the close showed.")
        self.journal.close_trade(ticket, closed_utc=now.isoformat(), exit_price=exit_px, net_pnl=round(net, 2),
                                 exit_reason=reason, exit_explanation=expl,
                                 duration_seconds=max(0.0, (closed_at - opened).total_seconds()))
        if root:
            self.cooldown[root] = now + dt.timedelta(seconds=self.cfg.cooldown_s)
        self.journal.order_update(sid, REJECTED, now, message=expl)
        msg = f"{STRATEGY_LABEL}: {sym} ticket {ticket} {side.value} {expl}"
        self.journal.event(reason, msg, sym, now)
        return msg

    def _absence(self, unknown: dict, here: set, now: dt.datetime) -> list[str]:
        """Unknown sends that this successful positions read does not show. One empty read is never taken as
        'not there': the MT5 adapter returns an EMPTY list when its positions read fails. A send counts as not
        at the broker only when MISSING_PASSES reads in a row over at least MISSING_SECONDS have not shown it
        (a read that shows it starts the count again) AND the broker's deal history since the send shows no
        entry of ours on its pair that is still open. Then it is marked ABSENT in the orders table - so a
        restart keeps the outcome - and no longer holds up new orders on its pair. A read taken while the
        terminal is not connected to the trade server is its own stale copy: it counts for nothing and the
        count starts again. An unknown send that carries the ticket of the broker's fill is booked as soon as
        that ticket's closing deal shows (``_book_unknown_fill``), on any pass, never marked ABSENT unbooked."""
        notes = []
        connected = self._trade_server_connected()
        for sid10, r in unknown.items():
            sid = r["signal_id"]
            booked = self._book_unknown_fill(r, now) if sid10 not in here else ""
            if booked:                                  # it filled and its closing deal now shows: a real trade
                self._misses.pop(sid, None)
                notes.append(booked)
                continue
            if sid10 in here or not connected:
                self._misses.pop(sid, None)
                continue
            n, first = self._misses.get(sid, (0, now))
            n += 1
            self._misses[sid] = (n, first)
            if n < MISSING_PASSES or (now - first).total_seconds() < MISSING_SECONDS:
                continue
            why, how = self._entry_since(r)
            if why:
                log.info("%s: %s order %s still blocks its pair: %s", STRATEGY_LABEL, r.get("symbol"), sid, why)
                continue
            self._misses.pop(sid, None)
            booked = self._book_unknown_fill(r, now)   # its closing deal may have reached the history only now
            if booked:
                notes.append(booked)
                continue
            sym = str(r.get("symbol") or "")
            msg = (f"{STRATEGY_LABEL}: the {sym} order sent at {str(r.get('created_utc') or '')[11:19]} UTC, whose "
                   f"outcome was unknown, is not open at the broker ({n} positions reads in a row over "
                   f"{(now - first).total_seconds():.0f} s did not show it, and the deal history agrees{how}); "
                   f"new orders on {sym} are allowed again")
            self.journal.order_update(sid, ABSENT, now, message=msg)
            self.journal.event("ABSENT", msg, sym, now)
            notes.append(msg)
        live = {r["signal_id"] for r in unknown.values()}
        for sid in [s for s in self._misses if s not in live]:
            del self._misses[sid]
        return notes

    def _trade_server_connected(self) -> bool:
        """MetaTrader says it is connected to the trade server right now (False when it cannot say)."""
        fn = getattr(self.broker, "is_connected", None)
        if fn is None:
            return False
        try:
            return bool(fn())
        except Exception:
            return False

    def _entry_since(self, row: dict) -> tuple[str, str]:
        """The broker's deal history since an unknown send: ('', detail) when it holds no entry of ours on
        that pair which is still open (entries of trades on record here are not this order); (why it cannot
        be taken as absent, '') otherwise - including when the history cannot be read."""
        sym = str(row.get("symbol") or "")
        fn = getattr(self.broker, "deals_since", None)
        if fn is None:
            return "the broker's deal history cannot be read", ""
        try:
            sent = to_utc(dt.datetime.fromisoformat(str(row.get("created_utc"))))
        except (TypeError, ValueError):
            return "its send time is not on record", ""
        try:
            # the send time is this machine's clock, the deal times the broker's: look well before the send
            deals = list(fn(sent - ENTRY_LOOKBACK, self.cfg.magic, False) or [])
        except Exception as exc:
            return f"the broker's deal history cannot be read ({exc})", ""
        closed_there = []
        for d in deals:
            if not d.get("is_entry") or str(d.get("symbol") or "") != sym:
                continue
            pos = int(d.get("position") or 0)
            if pos and self.journal.trade(pos) is not None:
                continue
            try:
                done = self.broker.closed_deal(pos) if pos else None
            except Exception:
                done = None
            if not done:
                return f"the deal history shows an entry on {sym} (position {pos or 'unknown'}) that is not closed", ""
            closed_there.append(str(pos))
        if closed_there:
            return "", f"; it did reach the broker as position {', '.join(closed_there)} and was closed there"
        return "", ""

    # -------------------------------------------------------------- restart --
    def _restore(self) -> None:
        now = to_utc(self.clock())
        rows = self.journal.open_trades()
        for r in rows:
            mode = str(r.get("mode") or "PAPER").upper()
            if self.executor is None or mode != self.mode or r.get("data_source") != self.data_source:
                self._pending_notes.append(f"{STRATEGY_LABEL}: ticket {r['ticket']} {r['spot_symbol']} was opened in "
                                           f"{mode} ({r.get('data_source')}); this run is {self.mode} ({self.data_source}): left as it is")
                continue
            side = Side(r["side"])
            stop = float(r["stop"] or r["initial_stop"])
            try:
                trail = FlowTrail(side, float(r["entry"]), float(r["initial_stop"]), TrailConfig(),
                                  float(getattr(self._spec(r["spot_symbol"]), "point", 0.0) or 0.0))
                trail.stop = stop
            except ValueError:
                continue
            tr = IanTrade(int(r["ticket"]), r["signal_id"] or "", r["root"] or "", r["instrument"] or "", r["spot_symbol"],
                          side, float(r["volume"]), float(r["entry"]), float(r["initial_stop"]),
                          to_utc(dt.datetime.fromisoformat(r["opened_utc"])), trail, mode, r.get("data_source") or "",
                          r.get("session_date") or "", r.get("regime") or "", float(r.get("score") or 0.0),
                          r.get("reasoning") or "", last_mark=float(r["entry"]))
            if not self.live:
                self.executor.adopt(Position(tr.ticket, tr.spot_symbol, side, tr.volume, tr.entry, stop, 0.0, tr.opened,
                                             comment=TAG))
            self.open[tr.spot_symbol] = tr
        if rows:
            self.journal.event("RESTORE", f"{len(self.open)} open trade(s) restored", now=now)

    # --------------------------------------------------------------- virtual --
    def run_virtual(self, max_events: Optional[int] = None) -> list[str]:
        """Drive a replay or synthetic feed as if live: before each batch of events, a decision pass at
        every ``decision_interval_s`` of event time - so staleness during a silent gap is seen exactly as
        it would have been live. Deterministic: the same events give the same passes."""
        notes: list[str] = []
        nxt: Optional[dt.datetime] = None
        step = dt.timedelta(seconds=self.cfg.decision_interval_s)
        done = 0
        while True:
            try:
                batch = self.feed.events(1)
            except Exception:
                batch = []
            if not batch:
                break
            ev = batch[0]
            t = received_at(ev)
            if nxt is None:
                nxt = t
            gaps = 0
            while t >= nxt:
                notes += self.step(nxt)
                gaps += 1
                nxt = nxt + (step if gaps < 240 else dt.timedelta(seconds=30))
            self.ingest(batch)
            done += 1
            if max_events is not None and done >= max_events:
                break
        if nxt is not None:
            notes += self.step(nxt)
        return notes

    # --------------------------------------------------------------- status --
    def _today(self, now: dt.datetime) -> dict:
        day0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
        # LIVE without MetaTrader has no executor (its mode reads OFF), but today's records are still LIVE ones
        mode = self.cfg.mode if self._no_exec else self.mode
        rows = [r for r in self.journal.closed_since(day0) if r.get("mode") == mode]
        st = self.journal.stats(rows)
        st["data_source"] = self.data_source
        return st

    def status(self, now: Optional[dt.datetime] = None) -> dict:
        now = to_utc(now or self._now())
        state = self.feed_state
        if state == NOT_CONFIGURED:
            headline = ("DATA-DEGRADED: no institutional feed is configured - no signals, no trades. "
                        "See docs/ops/financial_ian_data_feed.md.")
        elif state == LIVE:
            headline = "WATCHING the futures order book" if not self.open else f"IN {len(self.open)} TRADE(S)"
        elif state == DEGRADED:
            where = f" on {', '.join(self.degraded)}" if self.degraded else ""
            headline = (f"DATA-DEGRADED{where}: {self.feed_reason} - no new trades on any future until the data "
                        "is clean; open positions are still managed")
        else:
            headline = f"FEED DOWN: {self.feed_reason} - no new signals; open positions keep their broker stops"
        if self._no_exec:
            headline = f"{self._no_exec} - " + headline
        elif self.mode == "OFF":
            headline = "OFF - " + headline
        top = None
        tradeables = sorted(self.opps.values(), key=lambda o: -o.score)
        if tradeables:
            top = tradeables[0].to_dict()
        ladder = None
        best_root = tradeables[0].root if tradeables else next(iter(self.latest), None)
        if best_root is not None:
            inst = self.raw_of.get(best_root)
            f = self.flows.get(inst) if inst else None
            if f is not None:
                ladder = f.ladder(self.latest.get(best_root), 10)
        mt5 = False
        try:
            mt5 = bool(self.broker is not None and self.broker.is_connected())
        except Exception:
            mt5 = False
        last = None
        r = self.journal.last_closed()
        if r is not None:
            last = {k: r.get(k) for k in LAST_TRADE_KEYS}
        open_rows = []
        for t in self.open.values():
            spec = self._spec(t.spot_symbol)
            pnl = t.broker_pnl if t.broker_pnl is not None else (
                float(spec.money((t.last_mark - t.entry) * t.side.sign, t.volume)) if spec is not None and t.last_mark else 0.0)
            # kept while the broker's positions do not show it (NOT_SEEN): the page says so, until it is seen again
            unseen = t.missing_since if t.missing_noted else None
            trail = (f"not shown in the broker's positions since {unseen:%H:%M:%S} UTC; kept with its broker stop, "
                     "not moved; the P&L is from the last read that showed it") if unseen is not None else t.last_reason
            open_rows.append({"ticket": t.ticket, "symbol": t.spot_symbol, "future": t.root, "side": t.side.value,
                              "volume": t.volume, "entry": t.entry, "stop": t.stop, "initial_stop": t.initial_stop,
                              "r_now": round(t.trail.r_of(t.last_mark), 2) if t.last_mark else 0.0,
                              "best_r": round(t.trail.mfe_r, 2), "pnl": round(pnl, 2), "opened": t.opened.isoformat(),
                              "reasoning": t.reasoning, "trail": trail, "mode": t.mode,
                              "not_seen_since": unseen.isoformat() if unseen is not None else None})
        h = None
        try:
            h = self.feed.health().to_dict()
        except Exception:
            h = {}
        return {
            "strategy_id": STRATEGY_ID, "label": STRATEGY_LABEL, "tagline": TAGLINE, "magic": int(self.cfg.magic),
            "mode": self.mode, "running": True, "updated": now.isoformat(), "status": headline,
            "data_source": self.data_source,
            "data_label": SYNTHETIC_LABEL if self.synthetic else ("REPLAY - NOT LIVE" if self.virtual else "LIVE"),
            "feed": {"state": state, "reason": self.feed_reason, "vendor": h.get("vendor", ""),
                     "data_class": INSTITUTIONAL, "synthetic": self.synthetic, "events": self.events_in,
                     "subscribed": list(getattr(self, "subscribed", [])), "quality": dict(getattr(self, "quality_text", {})),
                     "latency_ms": {r: self.quality.stats(i).get("latency_ms") for r, i in self.raw_of.items()}},
            "mt5": {"connected": mt5, "data_class": RETAIL,
                    "note": "MT5 spot quotes, bars and calendar: RETAIL data, used for the chart, confirmation and execution only"},
            "top_opportunity": top,
            "reasoning": (top or {}).get("reasoning") or "",
            "opportunities": {r: {"score": o.score, "direction": o.direction, "spot": o.spot_symbol,
                                  "spot_side": "BUY" if o.spot_direction > 0 else "SELL" if o.spot_direction < 0 else "",
                                  "regime": o.regime.regime, "tradeable": o.tradeable, "why_not": o.why_not}
                              for r, o in self.opps.items()},
            "markets_now": dict(self.notes),
            "open_positions": open_rows,
            "today": self._today(now),
            "last_trade": last,
            "last_report": self.last_report,
            "ladder": ladder,
        }

    def write_status(self, now: dt.datetime, force: bool = False) -> None:
        if not force and self._last_status is not None and \
                (now - self._last_status).total_seconds() < self.cfg.status_interval_s:
            return
        self._last_status = now
        try:
            tmp = self.status_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.status(now), indent=2, default=str))
            tmp.replace(self.status_path)
        except Exception as exc:
            log.warning("status write failed: %s", exc)

    def close(self) -> None:
        if self.recorder is not None:
            self.recorder.close()
        try:
            self.feed.close()
        except Exception:
            pass
        self.journal.close()


LAST_TRADE_KEYS = ("ticket", "spot_symbol", "side", "entry", "exit_price", "net_pnl", "realised_r", "exit_reason",
                   "exit_explanation", "reasoning", "closed_utc", "mode", "data_source")


def write_waiting_status(data_dir: str | Path, cfg: IanConfig, attempt: int, now: dt.datetime,
                         retry_s: float = 15.0) -> None:
    """The page while the process waits for MetaTrader (the terminal is not running, or not logged in).
    The engine has not started, so nothing is read, scored or placed - and the page says so plainly instead
    of showing whatever an earlier run last wrote. Today's figures, the last trade and any open positions
    come from the bot's own records."""
    data = Path(data_dir)
    now = to_utc(now)
    mode = str(cfg.mode).upper()
    today: dict = {}
    last, opens = None, []
    try:
        j = Journal(data / "ian.sqlite")
        try:
            day0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
            today = j.stats([r for r in j.closed_since(day0) if r.get("mode") == mode])
            r = j.last_closed()
            last = {k: r.get(k) for k in LAST_TRADE_KEYS} if r is not None else None
            held = "it keeps its broker stop" if mode == "LIVE" else "a PAPER position, not managed meanwhile"
            opens = [{"ticket": r["ticket"], "symbol": r["spot_symbol"], "future": r.get("root"), "side": r["side"],
                      "volume": r["volume"], "entry": r["entry"], "stop": r.get("stop") or r.get("initial_stop"),
                      "initial_stop": r.get("initial_stop"), "opened": r.get("opened_utc"),
                      "reasoning": r.get("reasoning") or "", "mode": r.get("mode"),
                      "trail": f"MetaTrader cannot be reached: {held}"}
                     for r in j.open_trades() if str(r.get("mode") or "").upper() == mode]
        finally:
            j.close()
    except Exception as exc:
        log.warning("%s: records not read for the waiting status (%s)", STRATEGY_LABEL, exc)
    headline = (f"WAITING FOR METATRADER: it cannot be reached yet (attempt {attempt}) - is the terminal running and "
                f"logged in? It tries again every {retry_s:g} seconds; nothing is placed until it connects")
    if opens and mode == "LIVE":
        headline += f". {len(opens)} open position(s) keep their broker stops meanwhile"
    st = {
        "strategy_id": STRATEGY_ID, "label": STRATEGY_LABEL, "tagline": TAGLINE, "magic": MAGIC,
        "mode": mode, "running": True, "updated": now.isoformat(), "status": headline,
        "waiting": "waiting for MetaTrader", "data_source": "", "data_label": "LIVE",
        "feed": {"state": "NOT STARTED", "reason": "waiting for MetaTrader: the feed starts once it is connected",
                 "vendor": str((cfg.feed or {}).get("vendor", "none")), "data_class": INSTITUTIONAL,
                 "synthetic": False, "events": 0, "subscribed": [], "quality": {}, "latency_ms": {}},
        "mt5": {"connected": False, "data_class": RETAIL, "note": "MetaTrader cannot be reached yet"},
        "top_opportunity": None, "reasoning": "", "opportunities": {}, "markets_now": {},
        "open_positions": opens, "today": today, "last_trade": last, "last_report": None, "ladder": None,
    }
    path = data / "ian-status.json"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(st, indent=2, default=str))
        tmp.replace(path)
    except Exception as exc:
        log.warning("status write failed: %s", exc)
