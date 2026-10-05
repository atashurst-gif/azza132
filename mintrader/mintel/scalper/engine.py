"""The Rapid Scalper engine: its own loop, its own state, its own ledger.

It shares the broker connection and nothing else with the existing bot:
every order carries the scalper's magic, every position it manages is one
it opened (or one at the broker carrying its magic), and it never looks
at, let alone touches, the other strategy's positions except to COUNT
their risk for account-level safety.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import time
from pathlib import Path
from typing import Optional

from ..broker.base import Side, Tick, TF
from ..clock import to_utc, utcnow
from ..config import Config
from ..ops.health import Heartbeat
from ..version import RUNNING_STAMP
from . import STRATEGY_ID, STRATEGY_LABEL, TAGLINE
from .config import ScalperConfig
from .execution import Fill, make_executor
from .features import TickBuffer, compute
from .journal import ScalperJournal
from .manage import Manager, ScalpTrade
from .risk import Breakers, account_safety, plan
from .score import Opportunity, score
from .stats import strategy_stats

log = logging.getLogger("mintel.scalper")


class ScalperEngine:
    def __init__(self, cfg: Config, scfg: ScalperConfig, broker, *,
                 clock=None, journal: Optional[ScalperJournal] = None,
                 executor=None, data_dir: Optional[Path] = None):
        self.cfg = cfg
        self.scfg = scfg
        self.broker = broker
        self.clock = clock or utcnow
        data = Path(data_dir or cfg.ops.data_dir)
        data.mkdir(parents=True, exist_ok=True)
        self.data_dir = data
        self.journal = journal or ScalperJournal(data / scfg.journal_file)
        self.executor = executor if executor is not None else make_executor(scfg, broker)
        self.manager = Manager(scfg)
        self.breakers = Breakers(scfg)
        self.buffers: dict[str, TickBuffer] = {s: TickBuffer(s) for s in scfg.symbols}
        self.specs: dict = {}
        self.bars: dict[str, list] = {}
        self._bars_at: dict[str, float] = {}
        self.trades: dict[int, ScalpTrade] = {}
        self.last_opportunities: list[Opportunity] = []
        self.best: Optional[Opportunity] = None
        self.last_entry_at: Optional[dt.datetime] = None
        self.last_loss_at: Optional[dt.datetime] = None
        self.last_loss_by_symbol: dict[str, dt.datetime] = {}
        self.last_scan_at: float = 0.0
        self.last_tick_at: Optional[dt.datetime] = None
        self.latency_ms: float = 0.0
        self.hb = Heartbeat(data / "heartbeats", "scalper", self.clock)
        self.started = to_utc(self.clock())
        self.status_path = data / scfg.status_file
        self.blocked_because: list[str] = []
        self._news_cache: tuple[float, list] = (0.0, [])
        # a reset to zero (or a new day) starts the streak afresh; paper and
        # live never share one
        self.breakers.consecutive_losses = self.journal.consecutive_losses(self.day_start(), scfg.mode)

    # ------------------------------------------------------------ helpers --
    def spec(self, symbol: str):
        s = self.specs.get(symbol)
        if s is None:
            try:
                s = self.broker.spec(symbol)
            except Exception:
                s = None
            if s is not None:
                self.specs[symbol] = s
        return s

    def _bars_for(self, symbol: str, now_mono: float) -> list:
        if now_mono - self._bars_at.get(symbol, 0.0) > 30.0:
            try:
                self.bars[symbol] = list(self.broker.bars(symbol, TF.M1, 80))
            except Exception as exc:
                self.breakers.record_api_error(to_utc(self.clock()))
                log.warning("bars %s: %s", symbol, exc)
                self.bars.setdefault(symbol, [])
            self._bars_at[symbol] = now_mono
        return self.bars.get(symbol, [])

    def _ticks(self) -> dict[str, Optional[Tick]]:
        t0 = time.monotonic()
        out: dict[str, Optional[Tick]] = {}
        fn = getattr(self.broker, "ticks", None)
        try:
            if fn is not None:
                out = dict(fn(list(self.scfg.symbols)))
            else:
                for s in self.scfg.symbols:
                    out[s] = self.broker.tick(s)
        except Exception as exc:
            self.breakers.record_api_error(to_utc(self.clock()))
            log.warning("ticks: %s", exc)
            return {}
        self.latency_ms = (time.monotonic() - t0) * 1000.0
        return out

    def day_start(self) -> dt.datetime:
        """The broker's day, or the measuring start if that is later (a reset
        to zero starts the scalper's figures at the same moment as the page)."""
        now = to_utc(self.clock())
        day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        clock = getattr(self.broker, "clock", None)
        if clock is not None and hasattr(clock, "day_start_utc"):
            try:
                day = clock.day_start_utc(now)
            except Exception:
                pass
        # the main bot writes the measuring start into config.json when it
        # resets; the scalper may have started first, so re-read it each minute
        mono = time.monotonic()
        if mono - getattr(self, "_start_read_at", -1e9) > 60.0:
            self._start_read_at = mono
            try:
                raw = json.loads((Path(self.cfg.ops.data_dir) / "config.json").read_text())
                if raw.get("tracking_start_utc"):
                    self.cfg.tracking_start_utc = str(raw["tracking_start_utc"])
            except Exception:
                pass
        start_txt = getattr(self.cfg, "tracking_start_utc", "") or ""
        if start_txt:
            try:
                start = to_utc(dt.datetime.fromisoformat(start_txt.replace("Z", "+00:00")))
                if start > day and start <= now:
                    day = start
            except ValueError:
                pass
        return day

    def _news_blackout(self, symbol: str, now: dt.datetime) -> str:
        c = self.scfg
        if not c.news_filter_enabled:
            return ""
        at, events = self._news_cache
        if time.monotonic() - at > 60.0:
            try:
                events = list(self.broker.calendar(now - dt.timedelta(minutes=10),
                                                   now + dt.timedelta(minutes=15)))
            except Exception:
                events = []
            self._news_cache = (time.monotonic(), events)
        spec = self.spec(symbol)
        ccys = {getattr(spec, "base_currency", "") or symbol[:3], getattr(spec, "profit_currency", "") or symbol[3:6]}
        for e in events:
            if getattr(e, "importance", 0) < c.news_min_importance:
                continue
            if getattr(e, "currency", "") not in ccys:
                continue
            delta = (to_utc(e.time_utc) - now).total_seconds()
            if -c.news_blackout_after_seconds <= delta <= c.news_blackout_before_seconds:
                return f"high-impact {e.currency} news ({e.name}) in {delta / 60:+.0f} min"
        return ""

    # -------------------------------------------------------------- cycle --
    def cycle(self) -> list[str]:
        """One pass: refresh ticks, manage, scan. Returns notes."""
        now = to_utc(self.clock())
        self._cycle_t0 = time.monotonic()
        notes: list[str] = []
        ticks = self._ticks()
        fresh_any = False
        for sym, t in ticks.items():
            if t is None or sym not in self.buffers:
                continue
            if self.buffers[sym].add(t):
                fresh_any = True
            if self.last_tick_at is None or t.time > self.last_tick_at:
                self.last_tick_at = t.time
        tick_age = (now - self.last_tick_at).total_seconds() if self.last_tick_at else None

        if self.executor is not None:
            notes.extend(self._reconcile(now))
            notes.extend(self._manage(now, ticks))

        mono = time.monotonic()
        if mono - self.last_scan_at >= self.scfg.scan_interval_seconds:
            self.last_scan_at = mono
            notes.extend(self._scan(now, ticks, tick_age, mono))

        self.hb.beat({"mode": self.scfg.mode, "open": len(self.trades), "tick_age": tick_age})
        self.write_status(now, tick_age)
        self.cycle_ms = (time.monotonic() - getattr(self, "_cycle_t0", time.monotonic())) * 1000.0
        return notes

    # ---------------------------------------------------------- reconcile --
    def _reconcile(self, now: dt.datetime) -> list[str]:
        notes: list[str] = []
        try:
            live = {p.ticket: p for p in self.executor.positions()}
        except Exception as exc:
            self.breakers.record_api_error(now)
            self.breakers.reconciliation_ok = False
            return [f"could not read positions: {exc}"]
        self.breakers.reconciliation_ok = True
        # tracked but gone at the broker: the server-side stop (or a manual close) took it
        for ticket, trade in list(self.trades.items()):
            if ticket not in live and self.executor.mode == "LIVE":
                exit_price, pnl = trade.stop, None
                try:
                    deal = self.broker.closed_deal(ticket)
                    if deal:
                        exit_price = float(deal.get("exit_price") or exit_price)
                        pnl = deal.get("pnl")
                except Exception:
                    pass
                reason = "INITIAL_STOP" if abs(trade.stop - trade.initial_stop) < 1e-12 else "PROFIT_TRAIL"
                notes.append(self._finalise(trade, now, Fill(True, ticket, exit_price, exit_price, 0.0, 0.0),
                                            reason, "closed at the broker", pnl_override=pnl))
        # at the broker but not tracked: adopt (after a restart) - never ignore a live position
        for ticket, p in live.items():
            if ticket not in self.trades:
                spec = self.spec(p.symbol)
                risk = abs(p.entry_price - p.sl) if p.sl else 0.0
                tr = ScalpTrade(ticket, p.symbol, p.side, p.volume, p.entry_price, p.entry_price,
                                to_utc(p.open_time), p.sl or 0.0, p.sl or 0.0, risk,
                                spec.money_per_lot(risk) * p.volume if (spec and risk) else self.scfg.planned_max_trade_risk_gbp,
                                spec.point if spec else 0.0001, spec=spec, entry_reason="adopted after restart")
                if not p.sl:
                    # fail closed: a scalper position without a stop is not allowed to exist
                    t = self.buffers[p.symbol].last if p.symbol in self.buffers else None
                    if t is None:
                        try:
                            t = self.broker.tick(p.symbol)
                        except Exception:
                            t = None
                    if t is not None and spec is not None:
                        self.executor.close(ticket, t, spec)
                        notes.append(f"{p.symbol}: closed an adopted position that had no stop")
                        continue
                self.trades[ticket] = tr
                notes.append(f"{p.symbol}: adopted position {ticket} from the broker")
        return notes

    # ------------------------------------------------------------- manage --
    def _manage(self, now: dt.datetime, ticks: dict) -> list[str]:
        notes: list[str] = []
        for ticket, trade in list(self.trades.items()):
            tick = ticks.get(trade.symbol) or (self.buffers[trade.symbol].last if trade.symbol in self.buffers else None)
            if tick is None:
                continue
            spec = trade.spec or self.spec(trade.symbol)
            if self.executor.stop_hit(ticket, tick):
                reason = "INITIAL_STOP" if abs(trade.stop - trade.initial_stop) < 1e-12 else "PROFIT_TRAIL"
                fill = self.executor.close(ticket, tick, spec, at_price=trade.stop)
                notes.append(self._finalise(trade, now, fill, reason, "protective stop filled"))
                continue
            f = None
            if trade.symbol in self.buffers:
                f = compute(trade.symbol, self.buffers[trade.symbol], self.bars.get(trade.symbol, []),
                            trade.point, now)
                if not f.ok:
                    f = None
            d = self.manager.update(trade, tick, f, now)
            if d.close:
                fill = self.executor.close(ticket, tick, spec)
                notes.append(self._finalise(trade, now, fill, d.exit_reason, d.reason))
                continue
            if d.partial_volume > 0:
                part = spec.normalise_volume(d.partial_volume) if spec else d.partial_volume
                if part > 0 and trade.volume - part >= (spec.volume_min if spec else 0.01):
                    fill = self.executor.close(ticket, tick, spec, volume=part)
                    if fill.ok:
                        trade.volume = round(trade.volume - part, 8)
                        self.journal.log_event("PARTIAL", f"{trade.symbol} banked {part:g} lots", now=now)
            if d.new_stop is not None:
                if self.executor.modify_stop(ticket, d.new_stop):
                    ch = trade.stop_changes[-1] if trade.stop_changes else {}
                    self.journal.record_stop_change(ticket, ch.get("from", 0.0), d.new_stop, trade.state,
                                                    ch.get("why", ""), now)
                    notes.append(f"{trade.symbol}: stop -> {d.new_stop} ({ch.get('why', '')})")
            self.journal.update_trade(trade)
        return notes

    def _finalise(self, trade: ScalpTrade, now: dt.datetime, fill: Fill, reason: str,
                  detail: str, pnl_override=None) -> str:
        spec = trade.spec or self.spec(trade.symbol)
        exit_price = fill.price if fill.ok and fill.price else trade.stop
        gross = spec.money((exit_price - trade.entry_filled) * trade.side.sign, trade.volume) if spec else 0.0
        commission = self.scfg.commission_for(trade.symbol) * trade.volume
        if pnl_override is None and self.executor is not None and self.executor.mode == "LIVE":
            # the broker's record of profit + commission + swap is the truth
            for _ in range(3):
                try:
                    deal = self.broker.closed_deal(trade.ticket)
                except Exception:
                    deal = None
                if deal and deal.get("pnl") is not None:
                    pnl_override = deal["pnl"]
                    if deal.get("exit_price"):
                        exit_price = float(deal["exit_price"])
                    break
                time.sleep(0.2)
        if pnl_override is not None:
            net = float(pnl_override)
            gross = net + commission
        else:
            net = gross - commission
        last = self.buffers[trade.symbol].last if trade.symbol in self.buffers else None
        spread_now = ((last.ask - last.bid) / trade.point) if (last and trade.point) else 0.0
        slip_cost = spec.money_per_lot(abs(fill.slippage_points) * trade.point) * trade.volume if spec else 0.0
        self.journal.close_trade(trade, closed_at=now, exit_requested=fill.requested or exit_price,
                                 exit_filled=exit_price, exit_reason=reason, exit_detail=detail,
                                 spread_at_exit=spread_now, gross=gross, commission=commission,
                                 spread_cost=spec.money_per_lot(spread_now * trade.point) * trade.volume if spec else 0.0,
                                 slippage_cost=slip_cost, net=net, exit_slip=fill.slippage_points,
                                 exit_latency=fill.latency_ms)
        self.journal.record_slippage(trade.ticket, "exit", fill.requested or exit_price, exit_price,
                                     fill.slippage_points, fill.latency_ms, spread_now, now)
        self.breakers.record_slippage(fill.slippage_points, spread_now)
        self.breakers.record_result(net, now)
        self._broker_day_dirty = True
        if net < 0:
            self.last_loss_at = now
            self.last_loss_by_symbol[trade.symbol] = now
        self.trades.pop(trade.ticket, None)
        msg = (f"{trade.symbol} {trade.side.value} closed {reason}: {net:+.2f} after {trade.duration(now):.1f}s "
               f"(peak {trade.peak_r:.2f}R, {detail})")
        self.journal.log_event("EXIT", msg, {"ticket": trade.ticket, "reason": reason, "net": net}, now)
        log.info("%s", msg)
        return msg

    # --------------------------------------------------------------- scan --
    def _scan(self, now: dt.datetime, ticks: dict, tick_age: Optional[float], mono: float) -> list[str]:
        notes: list[str] = []
        opps: list[Opportunity] = []
        for sym in self.scfg.symbols:
            buf = self.buffers.get(sym)
            spec = self.spec(sym)
            if buf is None or spec is None or not buf.ticks:
                continue
            if not getattr(spec, "trade_allowed", True):
                continue
            f = compute(sym, buf, self._bars_for(sym, mono), spec.point, now)
            opp = score(f, self.scfg)
            opps.append(opp)
        opps.sort(key=lambda o: -o.confidence)
        self.last_opportunities = opps[:10]
        self.best = opps[0] if opps else None
        if self.executor is None:
            self.blocked_because = ["Rapid Scalper is OFF"]
            return notes
        connected = True
        try:
            connected = bool(self.broker.is_connected())
        except Exception:
            connected = False
        blocked = self.breakers.check(now, tick_age_seconds=tick_age, connected=connected,
                                      latency_ms=self.latency_ms)
        if self.last_entry_at and (now - self.last_entry_at).total_seconds() < self.scfg.min_seconds_between_entries:
            blocked.append("spacing between entries")
        if self.last_loss_at and (now - self.last_loss_at).total_seconds() < self.scfg.cooldown_after_loss_seconds:
            blocked.append("cooling down after a loss")
        hours = tuple(int(h) for h in (self.scfg.entry_hours_utc or ()))
        if hours and now.hour not in hours:
            blocked.append(f"outside its trading hours ({', '.join(f'{h:02d}:00' for h in hours)} UTC)")
        self.blocked_because = blocked
        if blocked:
            return notes
        for opp in opps:
            if opp.direction == 0:
                continue
            if not opp.tradable:
                if opp.confidence >= 40:
                    self.journal.record_rejected(opp, opp.features.spread_points if opp.features else 0.0, now, self.scfg.rejected_log_seconds)
                continue
            why = self._try_open(opp, now, ticks)
            notes.append(why)
            break                      # one decision per scan
        return notes

    def _try_open(self, opp: Opportunity, now: dt.datetime, ticks: dict) -> str:
        sym = opp.symbol
        f = opp.features
        spec = self.spec(sym)
        tick = ticks.get(sym) or self.buffers[sym].last
        if tick is None or spec is None or f is None:
            return f"{sym}: no tick"
        side = Side.BUY if opp.direction > 0 else Side.SELL
        black = self._news_blackout(sym, now)
        if black:
            opp.blockers.append(black); self.journal.record_rejected(opp, f.spread_points, now, self.scfg.rejected_log_seconds)
            return f"{sym}: {black}"
        # thesis invalidation: beyond the micro extreme by a spread
        spread = tick.ask - tick.bid
        entry = tick.ask if side is Side.BUY else tick.bid
        buf = self.scfg.invalidation_buffer_spreads * spread
        invalidation = (f.micro_low - buf) if side is Side.BUY else (f.micro_high + buf)
        rp = plan(spec, side, entry, invalidation, spread, self.scfg)
        if not rp.ok:
            opp.blockers.append(rp.reason); self.journal.record_rejected(opp, f.spread_points, now, self.scfg.rejected_log_seconds)
            return f"{sym}: {rp.reason}"
        # cost awareness: a trade whose costs eat the planned loss, or whose
        # expected move barely covers its costs, is a loser before it starts
        cost = rp.commission + rp.spread_cost + rp.slippage_allowance
        # measured against the money at the raw stop distance, not the padded
        # planned loss: on a one-pip stop the commission IS the trade
        at_stop = spec.money_per_lot(rp.stop_distance_points * spec.point) * rp.volume
        if at_stop > 0 and cost > self.scfg.max_cost_fraction_of_risk * at_stop:
            why = (f"costs {cost:.2f} would be {cost / at_stop:.0%} of the {at_stop:.2f} at risk to the stop "
                   f"(limit {self.scfg.max_cost_fraction_of_risk:.0%}); stop too tight to carry the commission")
            opp.blockers.append(why); self.journal.record_rejected(opp, f.spread_points, now, self.scfg.rejected_log_seconds)
            return f"{sym}: {why}"
        cost_points = (cost / rp.volume / spec.money_per_lot(spec.point)) if (rp.volume and spec.money_per_lot(spec.point)) else 0.0
        if cost_points and opp.expected_move_points < self.scfg.min_expected_move_over_cost * cost_points:
            why = (f"expected move {opp.expected_move_points:.1f} points is under {self.scfg.min_expected_move_over_cost:.0f}x "
                   f"the {cost_points:.1f}-point round-trip cost")
            opp.blockers.append(why); self.journal.record_rejected(opp, f.spread_points, now, self.scfg.rejected_log_seconds)
            return f"{sym}: {why}"
        # account-level safety: may block; never touches the other strategy
        try:
            account = self.broker.account()
            others = [p for p in self.broker.positions() if p.magic != self.scfg.magic]
        except Exception as exc:
            self.breakers.record_api_error(now)
            return f"{sym}: account state unknown ({exc})"
        other_risk = 0.0
        for p in others:
            s2 = self.spec(p.symbol)
            if s2 and p.sl:
                other_risk += s2.money_per_lot(abs(p.entry_price - p.sl)) * p.volume
        mine = self.executor.positions()
        my_risk = sum(t.planned_loss for t in self.trades.values())
        day = self.day_start()
        rs_day = sum(float(r.get("net_pnl") or 0) for r in self.journal.closed_since(day))
        acct_day = rs_day + self._other_day_pnl(day)
        ok, why = account_safety(self.scfg, account, others, mine, other_risk, my_risk, rs_day, acct_day, sym, side)
        if not ok:
            opp.blockers.append(why); self.journal.record_rejected(opp, f.spread_points, now, self.scfg.rejected_log_seconds)
            return f"{sym}: {why}"
        # duplicate protection: never two of the same symbol/side
        for t in self.trades.values():
            if t.symbol == sym:
                return f"{sym}: already in a trade here"
        # no going straight back into a market that just took money off us
        lost_at = self.last_loss_by_symbol.get(sym)
        if lost_at and (now - lost_at).total_seconds() < self.scfg.symbol_pause_after_loss_seconds:
            left = (self.scfg.symbol_pause_after_loss_seconds - (now - lost_at).total_seconds()) / 60
            why = f"lost here {((now - lost_at).total_seconds()) / 60:.0f} min ago - {sym} rests for another {left:.0f} min"
            opp.blockers.append(why); self.journal.record_rejected(opp, f.spread_points, now, self.scfg.rejected_log_seconds)
            return f"{sym}: {why}"
        fill = self.executor.open(sym, side, rp.volume, rp.stop, tick, spec, now)
        if not fill.ok:
            self.breakers.record_reject(now)
            self.journal.log_event("ENTRY_FAILED", f"{sym} {fill.message}", now=now)
            return f"{sym}: {fill.message}"
        trade = ScalpTrade(fill.ticket, sym, side, fill.volume or rp.volume, fill.requested, fill.price, now,
                           rp.stop, rp.stop, abs(fill.price - rp.stop), rp.planned_loss, spec.point, spec=spec,
                           confidence=opp.confidence, components={**opp.contributions, **opp.penalties},
                           entry_reason=opp.explain())
        self.trades[fill.ticket] = trade
        self.last_entry_at = now
        self.breakers.record_slippage(fill.slippage_points, f.spread_points)
        self.journal.open_trade(trade, self.scfg.mode, f.spread_points, f.realised_vol_points,
                                {"velocity_5s": f.velocity_5s, "trend_bias": f.trend_bias, "vwap": f.vwap,
                                 "ema_fast": f.ema_fast, "ema_slow": f.ema_slow, "volume_ratio": f.volume_ratio,
                                 "efficiency": f.efficiency, "chop": opp.chop, "expected_move": opp.expected_move_points},
                                fill.slippage_points, fill.latency_ms)
        self.journal.record_slippage(fill.ticket, "entry", fill.requested, fill.price, fill.slippage_points,
                                     fill.latency_ms, f.spread_points, now)
        msg = (f"{sym} {side.value} {rp.volume:g} lots at {fill.price} stop {rp.stop} "
               f"(planned loss {rp.planned_loss:.2f}, confidence {opp.confidence:.0f})")
        self.journal.log_event("ENTRY", msg, {"ticket": fill.ticket, "components": trade.components}, now)
        log.info("%s", msg)
        return msg

    def _other_day_pnl(self, day: dt.datetime) -> float:
        fn = getattr(self.broker, "deals_since", None)
        if fn is None:
            return 0.0
        try:
            # "profit" is profit + commission + swap per deal; entries carry half the commission
            rows = fn(day, self.cfg.magic, False) or []
            return float(sum(float(r.get("profit", 0) or 0) for r in rows))
        except Exception:
            return 0.0

    # ------------------------------------------------------------- status --
    def _broker_day(self, day: dt.datetime) -> Optional[dict]:
        """Cached for broker_day_cache_seconds; refreshed at once after a close."""
        cached = getattr(self, "_broker_day_cache", None)
        mono = time.monotonic()
        if (cached and cached[0] == day and not getattr(self, "_broker_day_dirty", False)
                and mono - cached[1] < self.scfg.broker_day_cache_seconds):
            return cached[2]
        out = self._broker_day_uncached(day)
        if out is not None:
            self._broker_day_cache = (day, mono, out)
            self._broker_day_dirty = False
        return out

    def _broker_day_uncached(self, day: dt.datetime) -> Optional[dict]:
        """The scalper's day as the BROKER records it (its magic only).

        None when not LIVE or the broker could not be asked - never a
        made-up zero."""
        if self.executor is None or self.executor.mode != "LIVE":
            return None
        fn = getattr(self.broker, "deals_since", None)
        if fn is None:
            return None
        try:
            rows = fn(day, self.scfg.magic, False) or []
        except Exception as exc:
            self.breakers.record_api_error(to_utc(self.clock()))
            log.warning("deal history unavailable: %s", exc)
            return None
        per_pos: dict[int, dict] = {}
        for r in rows:
            p = per_pos.setdefault(int(r.get("position") or 0), {"net": 0.0, "commission": 0.0, "closed": False,
                                                                   "symbol": r.get("symbol"), "time": r.get("time")})
            p["net"] += float(r.get("profit") or 0.0)
            p["commission"] += float(r.get("commission") or 0.0)
            if not r.get("is_entry"):
                p["closed"] = True
                p["time"] = r.get("time")
        closed = [p for p in per_pos.values() if p["closed"]]
        nets = [p["net"] for p in closed]
        return {"trades": len(nets), "wins": sum(1 for x in nets if x > 0), "losses": sum(1 for x in nets if x < 0),
                "realised": round(sum(nets), 2), "costs": round(abs(sum(p["commission"] for p in closed)), 2),
                "avg_win": (round(sum(x for x in nets if x > 0) / max(1, sum(1 for x in nets if x > 0)), 2) if any(x > 0 for x in nets) else None),
                "avg_loss": (round(sum(x for x in nets if x < 0) / max(1, sum(1 for x in nets if x < 0)), 2) if any(x < 0 for x in nets) else None),
                "largest_win": round(max(nets), 2) if nets and max(nets) > 0 else None,
                "largest_loss": round(min(nets), 2) if nets and min(nets) < 0 else None,
                "by_position": {k: round(v["net"], 2) for k, v in per_pos.items() if v["closed"]}}

    def stats_today(self) -> dict:
        day = self.day_start()
        # only this mode's rows: paper trades from earlier in the day never
        # mix with live money, and vice versa
        closed = [r for r in self.journal.closed_since(day) if r.get("mode") == self.scfg.mode]
        opens = []
        for t in self.trades.values():
            last = self.buffers[t.symbol].last if t.symbol in self.buffers else None
            px = (last.bid if t.side is Side.BUY else last.ask) if last else t.entry_filled
            opens.append({"symbol": t.symbol, "pnl": round(t.money_of(px), 2)})
        try:
            currency = self.broker.account().currency
        except Exception:
            currency = "GBP"
        s = strategy_stats(closed, opens, label=STRATEGY_LABEL, strategy_id=STRATEGY_ID,
                           currency=currency, scalper=True)
        mode = self.executor.mode if self.executor is not None else "OFF"
        s["mode"] = mode
        s["source"] = "simulated (PAPER)" if mode == "PAPER" else "the bot's own records"
        if mode == "LIVE":
            b = self._broker_day(day)
            if b is not None:
                # the broker's money wins over anything we worked out ourselves
                s.update({k: b[k] for k in ("trades", "wins", "losses", "realised", "avg_win", "avg_loss",
                                            "largest_win", "largest_loss")})
                s["total_costs"] = b["costs"]
                s["net_today"] = round(s["realised"] + s["unrealised"], 2)
                s["win_rate"] = round(100.0 * s["wins"] / s["trades"], 1) if s["trades"] else None
                wins_sum = sum(v for v in b["by_position"].values() if v > 0)
                loss_sum = abs(sum(v for v in b["by_position"].values() if v < 0))
                s["profit_factor"] = (round(wins_sum / loss_sum, 2) if loss_sum else ("no losses" if wins_sum else None))
                nets = list(b["by_position"].values())
                peak = dd = cum = 0.0
                for x in nets:
                    cum += x; peak = max(peak, cum); dd = min(dd, cum - peak)
                s["max_drawdown"] = round(dd, 2)
                s["source"] = "the broker's own deal history (scalper magic number)"
                s["broker_by_position"] = b["by_position"]
            else:
                s["source"] = "the bot's own records (broker history unavailable)"
        return s

    def status(self, now: dt.datetime, tick_age: Optional[float]) -> dict:
        mode = self.scfg.mode
        if mode == "OFF":
            state = "OFF"
        elif self.trades:
            state = "IN TRADE"
        elif self.blocked_because:
            state = "PAUSED"
        else:
            state = "SCANNING"
        best = self.best
        position = None
        for t in self.trades.values():
            last = self.buffers[t.symbol].last if t.symbol in self.buffers else None
            px = (last.bid if t.side is Side.BUY else last.ask) if last else t.entry_filled
            position = {
                "symbol": t.symbol, "side": "LONG" if t.side is Side.BUY else "SHORT",
                "opened": t.opened_at.strftime("%H:%M:%S.%f")[:-3], "duration_seconds": round(t.duration(now), 1),
                "entry": t.entry_filled, "current_pnl": round(t.money_of(px), 2),
                "peak_pnl": round(t.high_water_money, 2), "planned_max_risk": round(t.planned_loss, 2),
                "protected_floor": round(t.protected_floor_money, 2) if t.state != "INITIAL_RISK" else None,
                "mode": t.state, "runner_probability": t.runner_probability,
                "exit_tolerance": t.exit_tolerance, "stop": t.stop, "reason": t.entry_reason,
                "ticket": t.ticket, "volume": t.volume,
            }
        stats = self.stats_today()
        by_pos = stats.get("broker_by_position") or {}
        trades_today = [{"ticket": r["ticket"], "symbol": r["symbol"], "side": r["side"],
                         "opened": r["opened_utc"], "closed": r["closed_utc"],
                         "net": by_pos.get(int(r["ticket"]), r.get("net_pnl")),
                         "exit_reason": r.get("exit_reason"), "duration_seconds": r.get("duration_seconds"),
                         "confidence": r.get("confidence"), "peak_r": r.get("peak_r"),
                         "mode": r.get("mode"), "strategy": STRATEGY_ID}
                        for r in self.journal.closed_since(self.day_start())[-60:]
                        if r.get("mode") == self.scfg.mode]
        return {
            "strategy_id": STRATEGY_ID, "label": STRATEGY_LABEL, "tagline": TAGLINE,
            "status": state, "mode": mode, "build": RUNNING_STAMP,
            "updated": now.isoformat(), "started": self.started.isoformat(),
            "markets_monitored": len(self.scfg.symbols),
            "best_opportunity": best.symbol if best else None,
            "best_direction": ("LONG" if best.direction > 0 else "SHORT" if best.direction < 0 else "NONE") if best else None,
            "confidence": best.confidence if best else None,
            "spread_quality": _quality(best, "Spread", self.scfg.weights.spread) if best else None,
            "momentum": _quality(best, "Momentum", self.scfg.weights.momentum) if best else None,
            "best_breakdown": ({**best.contributions, **best.penalties} if best else {}),
            "best_blockers": (best.blockers if best else []),
            "blocked_because": self.blocked_because,
            "position": position, "tick_age_seconds": tick_age, "latency_ms": round(self.latency_ms, 1),
            "cycle_ms": round(getattr(self, "cycle_ms", 0.0), 1),
            "breakers": {"consecutive_losses": self.breakers.consecutive_losses,
                         "day_pnl": round(self.breakers.day_pnl, 2),
                         "avg_slippage_points": round(self.breakers.avg_slippage(), 2),
                         "tripped": self.breakers.tripped},
            "stats": stats,
            "trades_today": trades_today,
            "events": [{"ts": e["ts_utc"], "kind": e["kind"], "message": e["message"]}
                       for e in self.journal.recent_events(15)],
            "planned_max_trade_risk_gbp": self.scfg.planned_max_trade_risk_gbp,
        }

    def write_status(self, now: dt.datetime, tick_age: Optional[float], force: bool = False) -> None:
        mono = time.monotonic()
        if not force and mono - getattr(self, "_status_at", 0.0) < self.scfg.status_interval_seconds:
            return
        self._status_at = mono
        try:
            tmp = self.status_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.status(now, tick_age), default=str))
            tmp.replace(self.status_path)
        except Exception as exc:
            log.warning("status write failed: %s", exc)

    # ---------------------------------------------------------- shutdown --
    def shutdown(self, reason: str = "SYSTEM_SHUTDOWN") -> None:
        """Paper positions are closed on shutdown; live ones keep their broker stop."""
        now = to_utc(self.clock())
        if self.executor is not None and self.executor.mode == "PAPER":
            for ticket, trade in list(self.trades.items()):
                last = self.buffers[trade.symbol].last if trade.symbol in self.buffers else None
                if last is not None:
                    fill = self.executor.close(ticket, last, trade.spec or self.spec(trade.symbol))
                    self._finalise(trade, now, fill, reason, "process stopping")
        self.journal.log_event("SHUTDOWN", reason, now=now)
        try:
            self.write_status(now, None)
        except Exception:
            pass


def _quality(opp: Opportunity, key: str, max_points: float) -> str:
    v = opp.contributions.get(key, 0.0)
    frac = v / max_points if max_points else 0.0
    return "GOOD" if frac >= 0.7 else ("OK" if frac >= 0.4 else ("STRONG" if key == "Momentum" and frac >= 0.7 else "WEAK"))
