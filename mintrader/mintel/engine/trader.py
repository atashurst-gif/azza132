"""The trader: everything wired together.

One cycle does all of this, in this order:

1. **Health** - check itself, MT5, the broker, every feed.  If anything critical
   is wrong, enter SAFE MODE: keep recovering, keep positions protected, keep
   reconciling, do not send new orders.
2. **Reconcile** - the broker is the source of truth.  Adopt anything it holds
   that we do not know about, and make sure every position has a broker-side
   stop.
3. **Manage** - re-score every open position's thesis and run FlowLock: cut
   what has failed, give room to what is exceptional, tighten as moves die.
4. **Scan** - read news, then every instrument across every useful timeframe;
   classify regime; route to the fitting tactic; score; rank.
5. **Act** - take the strongest opportunity that clears the tier, the
   reward:risk test, the risk breakers and the execution-quality gate.  Size it
   between base and maximum risk according to calibrated confidence.
6. **Record** - journal the decision, the trade and, at exit, what it learned.
7. **Look again immediately.**  There is no daily quota, no per-symbol limit,
   no holding period and no approval list.

Nothing in this file is optional decoration: if a component were not wired in
here, it would not affect a single trade, and it would not be worth having.
"""
from __future__ import annotations

import datetime as dt
import logging
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional, Sequence

from ..broker.base import Bar, Broker, Position, Side, TF, Tick
from ..clock import to_utc, utcnow
from ..config import Config
from ..data.series import atr
from ..news.adapters import AdapterRegistry, ForexFactoryAdapter, Mt5CalendarAdapter, StoreBackedAdapter
from ..news.calendar_store import CalendarStore
from ..news.engine import NewsIntelligenceEngine
from ..ops.health import Heartbeat, HealthSupervisor
from .evidence import Family, MarketState
from .execution import Executor, IntentState
from .flowlock import FlowDecision, FlowLock, FlowState
from .journal import Journal
from .learning import HistoricalMatcher, MetaLabeller, features_from, label_from_trade
from .opportunity import Calibrator
from .risk import RiskManager
from .scanner import Scanner
from .thesis import ThesisTracker

log = logging.getLogger("mintel.trader")


@dataclass
class CycleResult:
    as_of: dt.datetime
    healthy: bool
    safe_mode: bool
    entries_allowed: bool
    top: tuple[MarketState, ...] = ()
    acted: Optional[MarketState] = None
    action_message: str = ""
    managed: tuple[str, ...] = ()
    blocked_reasons: tuple[str, ...] = ()
    scanned: int = 0

    def summary(self) -> str:
        if self.safe_mode:
            return "SAFE MODE - not trading, still protecting and recovering"
        if self.acted is not None:
            return f"opened {self.acted.summary()}: {self.action_message}"
        if self.top:
            return (f"watching {self.scanned} markets; best idea "
                    f"{self.top[0].summary()}")
        return f"watching {self.scanned} markets; nothing worth trading"


class Trader:
    """Owns the components and the cycle.  Safe to run in one thread."""

    def __init__(self, broker: Broker, cfg: Config, *,
                 clock: Callable[[], dt.datetime] = utcnow,
                 journal: Optional[Journal] = None,
                 news: Optional[NewsIntelligenceEngine] = None,
                 use_store_only_news: bool = False,
                 enable_model: bool = True,
                 dry_run: bool = False):
        self.broker = broker
        self.cfg = cfg
        self.clock = clock
        # In dry-run everything is evaluated exactly as normal - regime,
        # tactics, scoring, sizing, stop placement, idempotency - and the
        # order is simply never sent.  It is what makes the verification smoke
        # test genuinely safe to run against a live account, rather than
        # merely claimed to be.
        self.dry_run = bool(dry_run)
        self.would_have_traded: list[dict] = []
        data_dir = Path(cfg.ops.data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)

        self.journal = journal or Journal(data_dir / "journal.sqlite")
        self.calendar_store = CalendarStore(
            Path(cfg.news.store_path) if Path(cfg.news.store_path).is_absolute()
            else data_dir / Path(cfg.news.store_path).name)
        if news is not None:
            self.news = news
        else:
            adapters = []
            if use_store_only_news:
                # Backtests read ONLY the archive, so a "news-aware backtest"
                # is genuinely news-aware and never silently empty.
                adapters.append(StoreBackedAdapter(self.calendar_store))
            elif cfg.news.enabled:
                if cfg.news.use_mt5_calendar:
                    adapters.append(Mt5CalendarAdapter(broker))
                if cfg.news.public_calendar_feed:
                    adapters.append(ForexFactoryAdapter())
                adapters.append(StoreBackedAdapter(self.calendar_store))
            self.news = NewsIntelligenceEngine(AdapterRegistry(adapters),
                                               self.calendar_store, cfg.news)

        self.calibrator = Calibrator()
        self.matcher = HistoricalMatcher(self.journal)
        self.model = (MetaLabeller(data_dir / "meta_model.pkl")
                      if enable_model else None)
        self.scanner = Scanner(broker, cfg, self.news, self.calibrator,
                               history_match=self.matcher)
        self.risk = RiskManager(cfg, self.journal)
        self.risk.realised_today = self.realised_today
        self.last_entry_utc: Optional[dt.datetime] = None
        self._entry_times: list[dt.datetime] = []
        self._entries_today: list[dt.datetime] = []
        self._realised_cache: dict = {"at": None, "value": 0.0}
        self.scanner.losses_today = self.losses_today
        self.executor = Executor(broker, cfg, self.journal, self.risk, clock)
        self.flowlock = FlowLock(cfg.flowlock)
        self.thesis = ThesisTracker()

        expected_login = cfg.account_login
        try:
            expected_login = expected_login or broker.account().login
        except Exception:
            pass
        self.health = HealthSupervisor(cfg, broker, self.journal, self.news,
                                       self.executor,
                                       expected_login=expected_login,
                                       expected_server=cfg.account_server,
                                       clock=clock,
                                       own_heartbeats=("strategy",
                                                       "execution", "news"))
        hb_dir = data_dir / "heartbeats"
        self.hb_strategy = Heartbeat(hb_dir, "strategy", clock)
        self.hb_exec = Heartbeat(hb_dir, "execution", clock)
        self.hb_news = Heartbeat(hb_dir, "news", clock)

        self.last_news_refresh: Optional[dt.datetime] = None
        self.last_commission_refresh: Optional[dt.datetime] = None
        # losing trades per symbol today, rebuilt from the journal on a new day
        self._losses: dict = {"date": None, "counts": {}}
        self.last_cycle: Optional[CycleResult] = None
        self.top_opportunities: tuple[MarketState, ...] = ()
        self.cycles = 0
        self._cycle_marks: list[dt.datetime] = []
        self._last_cycle_at: Optional[dt.datetime] = None
        self.sleep_events: list[dict] = []
        self.started_utc = to_utc(clock())
        self._stop = threading.Event()
        self._bootstrapped = False

    # --------------------------------------------------------------- startup --
    def bootstrap(self) -> dict:
        """Prepare to trade: universe, calibration, recovered state.

        Recovery happens here and nowhere else: trackers are reloaded, the
        broker is reconciled, and any unprotected position gets a stop before a
        single new order is considered.
        """
        out: dict = {}
        try:
            uni = self.scanner.refresh_universe()
            out["universe"] = list(uni.symbols)
            out["rejected"] = uni.rejected
        except Exception as exc:
            out["universe_error"] = str(exc)

        try:
            self.calibrator.load(self.journal.calibration_rows())
            out["calibration"] = self.calibrator.report()
            violation = self.calibrator.monotonic_violation()
            if violation:
                out["calibration_warning"] = violation
                self.journal.log_event("CALIBRATION", violation, "WARNING")
        except Exception as exc:
            out["calibration_error"] = str(exc)

        try:
            self.flowlock.restore(self.journal.load_trackers())
            out["trackers_restored"] = len(self.flowlock.trackers)
        except Exception as exc:
            out["tracker_error"] = str(exc)

        try:
            out["reconcile"] = self.executor.reconcile()
        except Exception as exc:
            out["reconcile_error"] = str(exc)

        try:
            positions = self.broker.positions(self.cfg.magic)
            for p in positions:
                self.flowlock.adopt(p, p.sl, to_utc(self.clock()))
                risk_money = self._risk_money_for(p)
                self.risk.register_open_risk(p.ticket, p.symbol, p.side,
                                             risk_money)
            out["adopted_positions"] = [p.ticket for p in positions]
            problems = self.executor.enforce_stops(positions)
            if problems:
                out["unprotected"] = problems
                self.journal.log_event("UNPROTECTED", "; ".join(problems),
                                       "CRITICAL")
        except Exception as exc:
            out["position_error"] = str(exc)

        try:
            self.refresh_news(force=True)
        except Exception as exc:
            out["news_error"] = str(exc)

        self._bootstrapped = True
        self.journal.log_event("BOOTSTRAP", "trader started",
                               "INFO", {k: str(v)[:400] for k, v in out.items()})
        return out

    def _risk_money_for(self, position: Position) -> float:
        spec = self.broker.spec(position.symbol)
        if spec is None or not position.sl:
            return 0.0
        return spec.money(abs(position.entry_price - position.sl),
                          position.volume)

    # ------------------------------------------------------------------ news --
    def refresh_news(self, force: bool = False) -> bool:
        now = to_utc(self.clock())
        if not self.cfg.news.enabled:
            return True
        self.hb_news.beat({"events": self.news.health.events_known,
                           "degraded": self.news.health.degraded_reason})
        due = (force or self.last_news_refresh is None
               or (now - self.last_news_refresh).total_seconds()
               >= self.cfg.scan.news_refresh_seconds)
        if not due:
            return True
        ok = self.news.refresh(now, force=force)
        self.last_news_refresh = now
        return ok

    # ----------------------------------------------------- broker's ledger --
    def realised_today(self) -> float:
        """Today's realised P&L of the bot's trades, from the broker (30s cache)."""
        now = to_utc(self.clock())
        at = self._realised_cache["at"]
        if at is not None and (now - at).total_seconds() < 30:
            return float(self._realised_cache["value"])
        fn = getattr(self.broker, "deals_since", None)
        if fn is None:
            raise RuntimeError("broker has no deal history")
        start = self._strategy_day_start(now)
        rows = fn(start, self.cfg.magic) or []
        value = float(sum(float(r.get("profit") or 0.0) for r in rows))
        self._realised_cache = {"at": now, "value": value}
        return value

    def _strategy_day_start(self, now: dt.datetime) -> dt.datetime:
        """Midnight UTC, or the strategy's start if that was later today.

        The daily-loss stop and the daily entry cap measure THIS strategy's
        day: the day the rules change, the earlier rules' losses must not
        switch the new ones off before they have placed a trade.
        """
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start_txt = getattr(self.cfg, "tracking_start_utc", "") or ""
        if start_txt:
            try:
                start = dt.datetime.fromisoformat(start_txt.replace("Z", "+00:00"))
                day_start = max(day_start, to_utc(start))
            except ValueError:
                pass
        return day_start

    # --------------------------------------------------------- entry pacing --
    def _entry_gate(self, now: dt.datetime) -> str:
        """Why no new entry may be placed right now ("" when one may)."""
        sc = self.cfg.scan
        now = to_utc(now)
        for window in sc.no_entry_utc_windows:
            try:
                a, b = window.split("-")
                ah, am = (int(x) for x in a.split(":"))
                bh, bm = (int(x) for x in b.split(":"))
            except ValueError:
                continue
            t = now.hour * 60 + now.minute
            lo, hi = ah * 60 + am, bh * 60 + bm
            inside = lo <= t < hi if lo <= hi else (t >= lo or t < hi)
            if inside:
                return f"no new entries during the rollover window ({window} UTC)"
        if self.last_entry_utc is not None:
            gap = (now - self.last_entry_utc).total_seconds()
            if gap < sc.min_seconds_between_entries:
                return (f"spacing entries out: {sc.min_seconds_between_entries - gap:.0f}s "
                        f"until the next one may be placed")
        hour_ago = now - dt.timedelta(hours=1)
        self._entry_times = [t for t in self._entry_times if t >= hour_ago]
        if sc.max_new_positions_per_hour > 0 and len(self._entry_times) >= sc.max_new_positions_per_hour:
            return (f"{len(self._entry_times)} new positions in the last hour "
                    f"(limit {sc.max_new_positions_per_hour})")
        limit_d = int(getattr(sc, "max_new_positions_per_day", 0) or 0)
        if limit_d > 0:
            # Only THIS strategy's trades count: the day the rules change,
            # the earlier rules' trades must not use up the new allowance.
            day_start = Trader._strategy_day_start(self, now)
            n_today = sum(1 for t in getattr(self, "_entries_today", ()) if t >= day_start)
            try:
                n_today = max(n_today, int(self.journal.opened_since(day_start)))
            except Exception:
                pass  # no journal (tests) - the in-memory count still holds
            if n_today >= limit_d:
                return (f"{n_today} trades taken today (limit {limit_d}) - "
                        f"done for the day, no more entries until tomorrow")
        return ""

    def _note_entry(self, now: dt.datetime) -> None:
        now = to_utc(now)
        self.last_entry_utc = now
        self._entry_times.append(now)
        self._entries_today.append(now)

    # --------------------------------------------------- losses per market --
    def losses_today(self, symbol: str) -> int:
        """Losing trades on ``symbol`` since midnight UTC (journal-backed)."""
        today = to_utc(self.clock()).date()
        if self._losses["date"] != today:
            counts: dict = {}
            try:
                start = dt.datetime.combine(today, dt.time(0, 0), tzinfo=dt.timezone.utc)
                for t in self.journal.closed_trades(limit=1000, since=start):
                    if (t.get("pnl_money") or 0.0) < 0:
                        counts[t["symbol"]] = counts.get(t["symbol"], 0) + 1
            except Exception:
                counts = {}
            self._losses = {"date": today, "counts": counts}
        return int(self._losses["counts"].get(symbol, 0))

    def _after_close(self, tracker, pnl_money: float, now: dt.datetime) -> None:
        """Bookkeeping that shapes re-entry: a loss earns a long cooldown in
        the same direction and counts toward the day's limit on that market."""
        if pnl_money >= 0:
            return
        today = to_utc(now).date()
        if self._losses["date"] != today:
            self.losses_today(tracker.symbol)     # rebuilds for the new day
        counts = self._losses["counts"]
        counts[tracker.symbol] = counts.get(tracker.symbol, 0) + 1
        try:
            self.executor.set_cooldown(tracker.symbol, tracker.side,
                                       self.cfg.scan.cooldown_seconds_after_loss)
        except Exception:
            pass

    def refresh_commissions(self, now: dt.datetime, force: bool = False) -> None:
        """Learn what the broker really charges per lot, from its own deals.

        MT5 does not publish commission per symbol, but every deal records
        what was charged.  Round-trip commission per lot per symbol is
        summed |commission| over all of a position's deals divided by the
        closed volume, over the last 30 days of this bot's trades.  A
        symbol not traded yet uses the volume-weighted average of the rest.
        Refreshed every 10 minutes; overridden by risk.commission_per_lot.
        """
        if float(getattr(self.cfg.risk, "commission_per_lot", 0.0) or 0.0) > 0:
            return
        if (not force and self.last_commission_refresh is not None
                and (now - self.last_commission_refresh).total_seconds() < 600):
            return
        self.last_commission_refresh = now
        fn = getattr(self.broker, "deals_since", None)
        if fn is None:
            return
        try:
            rows = fn(now - dt.timedelta(days=30), self.cfg.magic, False) or []
        except Exception as exc:
            log.debug("commission refresh failed: %s", exc)
            return
        fees: dict[str, float] = {}
        closed: dict[str, float] = {}
        for r in rows:
            sym = str(r.get("symbol") or "")
            if not sym:
                continue
            fees[sym] = fees.get(sym, 0.0) + abs(float(r.get("commission") or 0.0))
            if not r.get("is_entry"):
                closed[sym] = closed.get(sym, 0.0) + float(r.get("volume") or 0.0)
        learned: dict[str, float] = {}
        for sym, vol in closed.items():
            if vol > 0 and fees.get(sym, 0.0) > 0:
                learned[sym] = round(fees[sym] / vol, 4)
        total_vol = sum(v for s_, v in closed.items() if s_ in learned)
        if total_vol > 0:
            learned["*"] = round(sum(fees[s_] for s_ in learned) / total_vol, 4)
        if learned:
            self.scanner.commission_per_lot = learned
            log.info("commission per lot learned from the broker: %s",
                     {k: v for k, v in learned.items()})

    # ------------------------------------------------------------- one cycle --
    def cycle(self) -> CycleResult:
        now = to_utc(self.clock())
        if not self._bootstrapped:
            self.bootstrap()
        self.cycles += 1
        slept = self._detect_sleep(now)
        self._cycle_marks = ([] if slept else self._cycle_marks + [now])[-12:]
        self._last_cycle_at = now

        # 1 - health -----------------------------------------------------------
        try:
            positions = self.broker.positions(self.cfg.magic)
            positions_known = True
        except Exception as exc:
            # Crucially NOT an empty position list: "I cannot see the
            # positions" and "there are no positions" are completely different
            # facts, and confusing them would make the bot finalise trades
            # that are still open at the broker.
            positions, positions_known = [], False
            log.error("could not read positions: %s", exc)
        try:
            account = self.broker.account()
            snapshot = self.risk.snapshot(account, positions, now)
        except Exception as exc:
            account, snapshot = None, None
            log.error("could not read the account: %s", exc)

        # Beat BEFORE checking: this thread is demonstrably alive right now, so
        # its own heartbeat should reflect that.  Real liveness testing of this
        # process is the independent watchdog's job precisely because a hung
        # process cannot be trusted to report its own hang.
        self.hb_strategy.beat({"cycle": self.cycles})
        report = self.health.run_checks(
            positions=positions, risk_snapshot=snapshot,
            last_scan=self.scanner.last_scan_utc,
            symbols_expected=len(self.scanner.universe.symbols)
            if self.scanner.universe else 0,
            scan_stale_limit=self._scan_stale_limit())
        if report.failures:
            self.health.heal(report)

        # 2 - reconcile --------------------------------------------------------
        # Done every cycle, not only at startup: a silent disconnect can leave
        # local state and broker state disagreeing at any moment.
        if positions_known and (positions or any(
                i.state is IntentState.UNKNOWN
                for i in self.executor.intents.values())):
            try:
                self.executor.reconcile()
                positions = self.broker.positions(self.cfg.magic)
            except Exception as exc:
                log.error("reconciliation failed: %s", exc)
        naked = (self.executor.enforce_stops([p for p in positions if not p.sl])
                 if positions_known else [])
        if naked:
            self.journal.log_event("UNPROTECTED", "; ".join(naked), "CRITICAL")

        # 3 - manage open positions -------------------------------------------
        self.hb_exec.beat({"positions": len(positions),
                           "visible": positions_known})
        managed = (self.manage(positions, now) if positions_known
                   else ["positions are not visible this cycle; holding all "
                         "existing state untouched"])

        if report.safe_mode:
            result = CycleResult(now, False, True, False, managed=tuple(managed),
                                 blocked_reasons=(self.health.safe_mode_reason,))
            self.last_cycle = result
            return result

        # 4 - news, then scan --------------------------------------------------
        self.refresh_news()
        self.refresh_commissions(to_utc(self.clock()))
        try:
            states = self.scanner.scan(now, positions)
        except Exception as exc:
            log.error("scan failed: %s", exc)
            states = []
        self.top_opportunities = tuple(states[:self.cfg.scan.top_n_reported])

        blocked: list[str] = []
        entries_allowed = report.entries_allowed
        if snapshot is not None and not snapshot.entries_allowed:
            entries_allowed = False
            blocked.extend(snapshot.blocking_reasons())
        if not report.entries_allowed:
            blocked.extend(report.plain_english())

        # 5 - act --------------------------------------------------------------
        acted: Optional[MarketState] = None
        message = ""
        if entries_allowed and account is not None and snapshot is not None:
            acted, message, extra = self.act(states, account, snapshot,
                                             positions, now)
            blocked.extend(extra)

        # 6 - record -----------------------------------------------------------
        try:
            if account is not None and snapshot is not None:
                self.journal.record_equity(account.balance, account.equity,
                                           len(positions),
                                           snapshot.open_risk_pct)
            for s in states[:5]:
                self.journal.record_decision(s, acted is not None
                                             and s is acted)
        except Exception as exc:
            log.warning("journalling failed: %s", exc)

        result = CycleResult(
            as_of=now, healthy=not report.failures, safe_mode=False,
            entries_allowed=entries_allowed, top=self.top_opportunities,
            acted=acted, action_message=message, managed=tuple(managed),
            blocked_reasons=tuple(dict.fromkeys(blocked))[:8],
            scanned=len(self.scanner.universe.symbols)
            if self.scanner.universe else 0)
        self.last_cycle = result
        return result

    def _detect_sleep(self, now: dt.datetime) -> bool:
        """Notice that the machine was suspended, and say so.

        On a laptop this is routine rather than exceptional: the lid closes,
        the Mac sleeps, and the process resumes hours later with a wildly
        stale view of the world.  Left undetected it looks like a mysterious
        outage; detected, it is a known state with a known response - throw
        away the cached view, re-read the calendar, and reconcile against the
        broker before considering any trade.

        The regular health checks would catch the stale data anyway.  This
        exists so the cause is recorded honestly and the recovery is immediate
        rather than incidental.
        """
        previous = self._last_cycle_at
        if previous is None:
            return False
        gap = (now - previous).total_seconds()
        expected = max(self.cfg.scan.scan_interval_seconds * 10, 120.0)
        if gap < expected:
            return False
        minutes = gap / 60.0
        message = (f"this machine appears to have been asleep or suspended "
                   f"for about {minutes:.0f} minutes; discarding the stale "
                   f"view and re-checking everything before trading again")
        log.warning("%s", message)
        self.sleep_events.append({"woke_utc": now.isoformat(),
                                  "gap_seconds": round(gap, 1)})
        self.sleep_events = self.sleep_events[-50:]
        try:
            self.journal.log_event("WAKE", message, "WARNING",
                                   {"gap_seconds": round(gap, 1)})
        except Exception:
            pass
        # Everything cached is now suspect.
        self.last_news_refresh = None
        self.scanner.last_scan_utc = None
        self.top_opportunities = ()
        try:
            self.executor.reconcile()
        except Exception as exc:
            log.error("reconciliation after waking failed: %s", exc)
        return True

    def _scan_stale_limit(self) -> Optional[float]:
        """Staleness limit scaled to how fast this instance actually cycles.

        Live, cycles are a few seconds apart and the limit stays tight.  In a
        replay each cycle can advance the clock by minutes, and a fixed limit
        would report a perfectly healthy scan as stale.
        """
        if len(self._cycle_marks) < 3:
            # During warm-up there is no observed cadence to compare against,
            # so a tight limit would flag a perfectly normal first few cycles.
            # The check answers "has scanning STOPPED", which needs history.
            return max(300.0, self.cfg.scan.scan_interval_seconds * 12)
        gaps = [(self._cycle_marks[i] - self._cycle_marks[i - 1]).total_seconds()
                for i in range(1, len(self._cycle_marks))]
        gaps = [g for g in gaps if g > 0]
        if not gaps:
            return None
        gaps.sort()
        median = gaps[len(gaps) // 2]
        return max(60.0, self.cfg.scan.scan_interval_seconds * 12, median * 3.0)

    def manage_cycle(self) -> list[str]:
        """Position management only - no universe scan.

        Open trades need attention far more often than the whole market needs
        re-ranking: a full multi-timeframe scan of sixty instruments is
        expensive, while re-checking eight open positions is cheap.  Running
        them on the same clock would either make scanning wasteful or make
        FlowLock blind between scans, and a stop that only moves every scan
        interval gives back profit for no reason.
        """
        now = to_utc(self.clock())
        try:
            positions = self.broker.positions(self.cfg.magic)
        except Exception as exc:
            log.error("could not read positions: %s", exc)
            return [f"positions unreadable: {exc}"]
        self.hb_exec.beat({"positions": len(positions), "light": True})
        if not positions:
            return []
        naked = self.executor.enforce_stops([p for p in positions if not p.sl])
        if naked:
            self.journal.log_event("UNPROTECTED", "; ".join(naked), "CRITICAL")
        return self.manage(positions, now)

    # --------------------------------------------------------------- manage --
    def manage(self, positions: Sequence[Position],
               now: dt.datetime) -> list[str]:
        """Re-assess every open position: thesis, then FlowLock.

        ``positions`` must be a list the broker actually returned.  Passing an
        empty list because the broker could not be read would finalise every
        open trade; the caller guards against that.
        """
        notes: list[str] = []
        live = {p.ticket for p in positions}
        for ticket in list(self.flowlock.trackers):
            if ticket not in live:
                self._finalise(ticket, now)
        for p in positions:
            try:
                notes.extend(self._manage_one(p, now))
            except Exception as exc:
                log.error("managing %s failed: %s", p.symbol, exc)
                notes.append(f"{p.symbol}: management error {exc}")
        return notes

    def _manage_one(self, position: Position, now: dt.datetime) -> list[str]:
        notes: list[str] = []
        spec = self.broker.spec(position.symbol)
        if spec is None:
            return [f"{position.symbol}: no contract specification"]
        tracker = self.flowlock.adopt(position, position.sl, now)

        ctx = None
        state = None
        try:
            ctx = self.scanner.build_context(position.symbol, now,
                                             positions=[position])
        except Exception as exc:
            log.warning("could not rebuild context for %s: %s",
                        position.symbol, exc)
        if ctx is not None:
            side = position.side.sign
            # Score the direction we are IN - scoring the other way round would
            # invert every conclusion about whether to hold.
            evidence = self.scanner.evidence_for(ctx, side)
            from .evidence import aggregate
            from ..data.regime import Regime
            regime = ctx.regime.regime if ctx.regime else Regime.UNCLEAR
            state = MarketState(
                symbol=position.symbol, as_of=now, regime=regime,
                regime_label=ctx.regime.label if ctx.regime else regime.value,
                direction=side, tactic="HOLD", evidence=evidence,
                raw_score=aggregate(evidence, regime), opportunity=0.0,
                tier="", entry=position.entry_price, stop=position.sl,
                target=position.tp or None, reward_risk=None,
                headroom_pips=0.0, cost_pips=0.0)

        tick = None
        try:
            tick = self.broker.tick(position.symbol)
        except Exception:
            pass
        price = (tick.bid if position.side is Side.BUY else tick.ask) \
            if tick else position.entry_price
        atr_value = ctx.atr_ref if ctx else 0.0
        if not atr_value:
            try:
                atr_value = atr(self.broker.bars(position.symbol, TF.M15, 120), 14)
            except Exception:
                atr_value = abs(position.entry_price - (position.sl or 0)) or 1e-9
        bars = ctx.data.get(ctx.decision_tf, 2) if ctx else []
        bar = bars[-1] if bars else None

        r_now = tracker.r_of(price)
        reading = self.thesis.evaluate(position, state, now, r_now)
        decision = self.flowlock.update(
            position, price=price, atr=atr_value, bar=bar,
            momentum=ctx.mo(ctx.decision_tf) if ctx else None,
            structure=ctx.st(ctx.decision_tf) if ctx else None,
            thesis_strength=reading.strength, now=now)

        if decision.close:
            res = self.executor.close_position(position, decision.close_reason)
            if res.ok:
                notes.append(f"closed {position.symbol}: "
                             f"{decision.close_reason}")
                self.journal.log_event(
                    "CLOSE", f"{position.symbol} {decision.close_reason}",
                    "INFO", {"ticket": position.ticket,
                             "thesis": reading.describe()})
                self._finalise(position.ticket, now, exit_price=res.price,
                               reason=decision.close_reason, state=state,
                               thesis=reading)
            else:
                notes.append(f"{position.symbol}: close was rejected "
                             f"({res.retcode})")
            return notes

        if decision.partial_volume > 0:
            # Snap to the broker's lot grid and make sure the runner that is
            # left behind is itself tradable; otherwise banking part of the
            # position is not actually possible and the whole idea is skipped.
            part = spec.normalise_volume(decision.partial_volume)
            remainder = round(position.volume - part, 8)
            if part <= 0 or remainder < spec.volume_min:
                tracker.partial_done = True     # not viable at this size
                notes.append(f"{position.symbol}: position too small to bank "
                             f"part of it, leaving it whole")
                part = 0.0
            res = (self.executor.close_position(position, "PARTIAL", part)
                   if part > 0 else None)
            if res is None:
                pass
            elif res.ok:
                notes.append(f"banked {part:g} lots of {position.symbol} at "
                             f"{decision.r_now:.2f}R and left "
                             f"{remainder:g} lots to run")
                self.journal.log_event(
                    "PARTIAL", f"{position.symbol} {part:g} lots",
                    "INFO", {"ticket": position.ticket, "r": decision.r_now})
            else:
                tracker.partial_done = False     # allow a retry next cycle

        if decision.new_stop:
            res = self.executor.move_stop(position, decision.new_stop, spec)
            if res is not None and res.ok:
                notes.append(f"{position.symbol}: stop moved to "
                             f"{decision.new_stop:.{spec.digits}f} "
                             f"({decision.state.value})")
                # Open risk shrinks as the stop tightens; the exposure breakers
                # should see that immediately rather than at the next entry.
                self.risk.register_open_risk(
                    position.ticket, position.symbol, position.side,
                    self._risk_money_for(position))
        try:
            self.journal.save_tracker(tracker)
        except Exception:
            pass
        return notes

    def _finalise(self, ticket: int, now: dt.datetime,
                  exit_price: float = 0.0, reason: str = "",
                  state: Optional[MarketState] = None,
                  thesis=None) -> None:
        """Close out bookkeeping for a position that is gone.

        Reached both when the bot closes a trade and when the broker's own stop
        or target did - in the latter case the exit price is recovered from the
        broker rather than guessed.
        """
        tracker = self.flowlock.forget(ticket)
        self.thesis.forget(ticket)
        self.risk.forget(ticket)
        self.matcher.invalidate()
        try:
            self.journal.drop_tracker(ticket)
        except Exception:
            pass
        if tracker is None:
            return
        # The broker is the authority on what a trade earned: it knows the
        # fills, the commission and the swap.  Only if it cannot tell us do we
        # reconstruct the figure from price and volume.
        deal = None
        try:
            if hasattr(self.broker, "closed_deal"):
                deal = self.broker.closed_deal(ticket)
        except Exception:
            deal = None
        if deal and not self._deal_matches(deal, tracker):
            # A deal for some other symbol or position is not this trade's
            # result.  Better to reconstruct from prices than to book it.
            log.warning("%s: closing deal from the broker does not match this "
                        "position (%s) - ignoring it", tracker.symbol, deal)
            deal = None
        if deal:
            # Prefer the broker's VOLUME-WEIGHTED exit over the price of the
            # final fill.  A position that banked a partial at +1.4R and then
            # stopped out on the remainder did not lose 1R - it lost a
            # fraction of one - and using only the last fill price would put
            # the R multiple and the money in flat contradiction.
            weighted = float(deal.get("exit_price") or 0.0)
            if weighted > 0:
                exit_price = weighted
            if not reason:
                reason = str(deal.get("reason") or "")
        if not exit_price:
            exit_price = self._recover_exit_price(tracker)
        stats = self.flowlock.capture_stats(tracker, exit_price)
        try:
            spec = self.broker.spec(tracker.symbol)
        except Exception:
            spec = None
        pnl_money = pnl_pips = 0.0
        moved = (exit_price - tracker.entry) * tracker.side.sign
        if spec is not None:
            pnl_pips = moved / max(spec.pip_size, 1e-12)
        if deal and deal.get("pnl") is not None:
            pnl_money = float(deal["pnl"])
        elif spec is not None and tracker.volume > 0:
            pnl_money = spec.money(moved, tracker.volume)
        learned = self._lesson(tracker, stats, reason, thesis)
        self._after_close(tracker, pnl_money, now)
        try:
            self.journal.close_trade(
                ticket, exit_price=exit_price, closed_utc=now,
                pnl_money=pnl_money, pnl_pips=round(pnl_pips, 2),
                exit_reason=reason or "broker stop or target",
                stats=stats, tracker=tracker, exit_state=state,
                learned=learned)
            self.calibrator.load(self.journal.calibration_rows())
        except Exception as exc:
            log.warning("could not finalise trade %s: %s", ticket, exc)

    @staticmethod
    def _deal_matches(deal: dict, tracker) -> bool:
        """Is this closing deal plausibly the result of ``tracker``?"""
        symbol = str(deal.get("symbol") or "")
        if symbol and symbol != tracker.symbol:
            return False
        try:
            exit_price = float(deal.get("exit_price") or 0.0)
        except (TypeError, ValueError):
            return False
        if exit_price <= 0 or tracker.entry <= 0:
            return False
        # No instrument moves by more than a quarter of its price between
        # entry and exit within one trade; anything like that is a mix-up.
        return abs(exit_price - tracker.entry) / tracker.entry < 0.25

    def _recover_exit_price(self, tracker) -> float:
        """Ask the broker what actually happened, do not assume the stop price."""
        closed = getattr(self.broker, "closed", None)
        if closed:
            for row in reversed(closed):
                if row.get("ticket") == tracker.ticket:
                    return float(row.get("exit") or 0.0)
        return tracker.stop or tracker.entry

    def _lesson(self, tracker, stats: dict, reason: str, thesis) -> str:
        """A one-line, plain-English takeaway stored with every trade."""
        bits = []
        r = stats.get("realised_r", 0.0)
        capture = stats.get("mfe_capture_pct", 0.0)
        if r > 0:
            bits.append(f"Won {r:.2f}R.")
            if capture < 40 and stats.get("mfe_r", 0) >= 1.5:
                bits.append(f"It was worth {stats['mfe_r']:.1f}R at best and "
                            f"only {capture:.0f}% was kept - the exit gave "
                            f"too much back.")
            else:
                bits.append(f"Kept {capture:.0f}% of the best price it saw.")
        else:
            bits.append(f"Lost {abs(r):.2f}R.")
            mfe_r = stats.get("mfe_r", 0.0)
            if mfe_r >= 1.0:
                bits.append(f"It had been {mfe_r:.1f}R in profit first, so the "
                            f"exit logic gave back a winner.")
            elif mfe_r < 0.3:
                bits.append("It never worked at all, so the entry itself was "
                            "the problem, not the exit.")
            else:
                bits.append(f"It only ever reached {mfe_r:.2f}R in favour, so "
                            f"it never paid for the risk it was taking - the "
                            f"setup looked better than the follow-through.")
            mae_r = stats.get("mae_r", 0.0)
            if mae_r >= 0.95:
                bits.append("The stop was hit, which is the stop doing its "
                            "job.")
        if reason:
            bits.append(f"Exited because {reason}.")
        if thesis is not None and getattr(thesis, "verdict", "") == "BROKEN":
            bits.append("The reason for the trade had broken.")
        return " ".join(bits)

    # ------------------------------------------------------------------ act --
    def act(self, states: Sequence[MarketState], account, snapshot,
            positions: Sequence[Position],
            now: dt.datetime) -> tuple[Optional[MarketState], str, list[str]]:
        """Take the best tradable opportunity, if there is one."""
        blocked: list[str] = []
        gate = self._entry_gate(now)
        if gate:
            blocked.append(gate)
            return None, "", blocked
        per_symbol: dict[str, int] = {}
        for p in positions:
            per_symbol[p.symbol] = per_symbol.get(p.symbol, 0) + 1

        for state in states:
            if not state.tradable:
                continue
            if per_symbol.get(state.symbol, 0) >= self.cfg.risk.max_positions_per_symbol:
                blocked.append(f"{state.symbol}: already at the position limit "
                               f"for this symbol")
                continue
            cd = self.executor.cooldown_remaining(state.symbol, state.side)
            if cd > 0:
                # A cooldown is anti-machine-gun protection, not a ban: a
                # genuinely new setup a few minutes later is welcome.
                blocked.append(f"{state.symbol}: cooling down for {cd:.0f}s")
                continue

            spec = self.scanner.universe.specs.get(state.symbol) if \
                self.scanner.universe else self.broker.spec(state.symbol)
            if spec is None:
                continue

            # Entry timing, on the finest view available, for this candidate
            # only.  Everything above decided WHAT to trade; this decides
            # whether to press the button in the next few seconds.
            timing_ok, timing_note = self.scanner.timing_confirmation(
                state.symbol, state.direction)
            if not timing_ok:
                blocked.append(f"{state.symbol}: {timing_note}")
                continue

            state = self._add_model_evidence(state)
            conf = self.calibrator.calibrated_confidence(state.opportunity)
            kelly = None
            try:
                kelly = self.journal.kelly_stats(state.tactic,
                                                 state.regime.value)
            except Exception:
                pass
            risk_pct, reasons = self.risk.risk_pct_for(state, conf, kelly)
            if risk_pct <= 0:
                blocked.append(f"{state.symbol}: {reasons[0] if reasons else 'no risk allocated'}")
                continue
            margin_per_lot = None
            try:
                if hasattr(self.broker, "calc_margin"):
                    margin_per_lot = self.broker.calc_margin(
                        state.symbol, state.side, 1.0, state.entry)
            except Exception:
                margin_per_lot = None
            sizing = self.risk.size(state, spec, account, positions, risk_pct,
                                    snapshot, margin_per_lot)
            if not sizing.ok:
                blocked.append(f"{state.symbol}: {sizing.rejected}")
                continue

            if self.dry_run:
                intended = {
                    "symbol": state.symbol, "side": state.side.value,
                    "volume": sizing.volume, "entry": state.entry,
                    "stop": state.stop, "target": state.target,
                    "risk_pct": sizing.risk_pct,
                    "risk_money": sizing.risk_money,
                    "opportunity": state.opportunity, "tier": state.tier,
                    "tactic": state.tactic,
                }
                self.would_have_traded.append(intended)
                blocked.append(
                    f"{state.symbol}: DRY RUN - would have {state.side.value} "
                    f"{sizing.volume:g} lots risking {sizing.risk_pct:.2f}%, "
                    f"but no order was sent")
                log.warning("DRY RUN: would have opened %s", intended)
                continue

            report = self.executor.submit(state, spec, sizing.volume)
            if not report.ok:
                blocked.append(f"{state.symbol}: {report.message}")
                self.journal.log_event("ENTRY_FAILED", report.message,
                                       "WARNING", {"symbol": state.symbol})
                continue

            position = report.position
            self.risk.register_open_risk(position.ticket, state.symbol,
                                         state.side, sizing.risk_money)
            tracker = self.flowlock.adopt(position, state.stop, now)
            strength = self.thesis.register(position.ticket, state)
            tracker.thesis_strength = strength
            try:
                self.journal.open_trade(
                    position, state, report.intent.key, sizing.risk_pct,
                    sizing.risk_money, account.equity, account.currency,
                    self.cfg.aggression, self.cfg.effective_mode)
                self.journal.save_tracker(tracker)
                self.journal.log_event(
                    "ENTRY",
                    f"{state.symbol} {state.side.value} {sizing.volume:g} lots",
                    "INFO",
                    {"opportunity": state.opportunity, "tier": state.tier,
                     "tactic": state.tactic, "risk_pct": sizing.risk_pct,
                     "explain": state.explain(),
                     "sizing_reasons": list(reasons) + list(sizing.reasons),
                     "entry_timing": timing_note,
                     "protective_stop_confirmed": report.protective_stop_confirmed})
            except Exception as exc:
                log.warning("could not journal the entry: %s", exc)
            msg = (f"{sizing.volume:g} lots risking "
                   f"{sizing.risk_pct:.2f}% ({sizing.risk_money:.2f} "
                   f"{account.currency}) with a {sizing.stop_pips:.1f} pip "
                   f"stop; {report.message}")
            self._note_entry(now)
            return state, msg, blocked
        return None, "", blocked

    def _add_model_evidence(self, state: MarketState) -> MarketState:
        """Fold the calibrated model in as one more evidence family.

        It cannot override a safety check and its confidence is capped, so a
        confident model cannot on its own push a weak setup into a large trade.
        """
        if self.model is None or not self.model.available:
            return state
        try:
            ctx = None
            row = features_from(state, ctx)
            got = self.model.evidence_score(row)
            if got is None:
                return state
            score, conf, note = got
            from .evidence import Evidence
            state.evidence[Family.HISTORICAL] = Evidence(
                Family.HISTORICAL,
                (state.family(Family.HISTORICAL) + score) / 2.0,
                max(conf, state.evidence.get(Family.HISTORICAL,
                                             Evidence(Family.HISTORICAL, 50.0,
                                                      0.0)).confidence),
                note)
        except Exception as exc:
            log.debug("model evidence skipped: %s", exc)
        return state

    # --------------------------------------------------------------- retrain --
    def retrain_model(self) -> dict:
        """Refit the meta-labeller from the journal, chronologically."""
        if self.model is None:
            return {"trained": False, "reason": "model disabled"}
        try:
            import json as _json
            trades = self.journal.closed_trades(limit=5000)
            trades.sort(key=lambda t: t.get("opened_utc") or "")
            rows, labels = [], []
            for t in trades:
                raw = t.get("entry_state_json")
                if not raw:
                    continue
                try:
                    st = _json.loads(raw)
                except Exception:
                    continue
                ev = st.get("evidence", {})
                rows.append({
                    "structure": ev.get("STRUCTURE", 50.0),
                    "momentum": ev.get("MOMENTUM", 50.0),
                    "volatility": ev.get("VOLATILITY", 50.0),
                    "participation": ev.get("PARTICIPATION", 50.0),
                    "session": ev.get("SESSION", 50.0),
                    "multi_tf": ev.get("MULTI_TF", 50.0),
                    "cross_market": ev.get("CROSS_MARKET", 50.0),
                    "news": ev.get("NEWS", 50.0),
                    "execution": ev.get("EXECUTION", 50.0),
                    "regime_conf": 50.0,
                    "headroom_atr": 0.0,
                    "range_consumed": 0.0,
                    "reward_risk": st.get("reward_risk") or 0.0,
                    "opportunity": st.get("opportunity") or 0.0,
                    "cost_over_atr": 0.0,
                    "stop_atr": 0.0,
                    "hour_utc": float((t.get("opened_utc") or "T00")[11:13] or 0),
                    "vol_percentile": 0.5, "adx": 0.0, "efficiency": 0.0,
                })
                labels.append(label_from_trade(t))
            result = self.model.train(rows, labels)
            self.journal.log_event("MODEL", str(result)[:400], "INFO")
            return result
        except Exception as exc:
            return {"trained": False, "reason": str(exc)}

    # ------------------------------------------------------------------ run --
    def run(self, *, max_cycles: int = 0,
            sleep: Optional[Callable[[float], None]] = None) -> int:
        """The main loop.  Never exits on a recoverable error.

        Open positions are managed on the fast ``manage_interval_seconds``
        clock; the full market scan runs on the slower
        ``scan_interval_seconds`` clock.
        """
        sleeper = sleep or time.sleep
        n = 0
        last_scan = 0.0
        while not self._stop.is_set():
            started = time.time()
            due_for_scan = (started - last_scan
                            >= self.cfg.scan.scan_interval_seconds)
            try:
                if due_for_scan:
                    result = self.cycle()
                    last_scan = started
                    n += 1
                    log.info("%s", result.summary())
                else:
                    for note in self.manage_cycle():
                        log.info("%s", note)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                log.exception("cycle failed: %s", exc)
                try:
                    self.journal.log_event("CYCLE_ERROR", str(exc), "ERROR")
                except Exception:
                    pass
            if max_cycles and n >= max_cycles:
                break
            elapsed = time.time() - started
            sleeper(max(0.0, self.cfg.scan.manage_interval_seconds - elapsed))
        return n

    def stop(self) -> None:
        self._stop.set()

    # -------------------------------------------------------------- reports --
    def thinking(self) -> list[dict]:
        """What the bot is looking at right now, for the dashboard."""
        out = []
        for i, s in enumerate(self.top_opportunities, 1):
            out.append({
                "rank": i, "symbol": s.symbol,
                "direction": "LONG" if s.direction > 0 else "SHORT",
                "score": s.opportunity, "tier": s.tier, "tactic": s.tactic,
                "regime": s.regime_label, "reason": s.explain(),
                "blockers": list(s.blockers),
                "reward_risk": s.reward_risk,
            })
        return out
