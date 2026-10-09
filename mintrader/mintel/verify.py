"""Startup verification: prove every dependency actually works.

Run by the setup command, and useful any time:

    python -m mintel.verify --config data/config.json

It checks, in order, the things that silently break automated trading:
configuration, MetaTrader 5, the account, trade permission, live prices, the
symbol universe, the economic calendar, the database, the heartbeats and the
dashboard.  Then it runs a **safe smoke test** that exercises the full decision
path - sizing, stop calculation, order-request construction, idempotency - and
deliberately stops short of sending an order.

The smoke test runs beside the real trader, on the same account and the same
journal, so it is kept away from anything real:

* It is a dry-run Trader: an entry is worked out and never sent.
* It is handed the broker through :class:`NoOrders`, which refuses every call
  that would open, change or close a position. A dry run still manages the
  open positions it can see (Trend & Breakout's own), and on LIVE it would
  otherwise move or close them at the real broker.
* With Trend & Breakout on PAPER (config.json "tnb" -> "mode") the Trader
  reads its paper record through a READ-ONLY view (``dry_run_view`` in
  ``mintel/engine/trader.py``): it never books a paper stop and never writes
  the record; only the running trader does. It reads the record again once
  it has started up, so a paper trade opened meanwhile by the running trader
  is never marked closed in the journal they share.
* The other bots (Momentum Runner, Band Breaker, Crowd Fader) are never
  started by a dry run: they already run inside the real trader, and the
  check says so.
* A position found already open is adopted by reconciliation as
  ``adopted:<ticket>``. That is a record of a trade the running trader holds,
  never an order, and :func:`sent_any_order` does not count it.

Exit code 0 means everything needed to trade is working.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import urllib.request
from pathlib import Path
from typing import Any, Optional, Sequence

from .broker.base import OrderResult, RetCode, Side
from .clock import utcnow
from .config import Config

# The executor's record of a position reconciliation found already open (a
# trade the running trader holds), keyed "adopted:<ticket>". Never an order.
ADOPTED_PREFIX = "adopted:"

# Broker names a dry run is never given: every call that could open, change
# or close a position, and the MT5 adapter's raw MetaTrader5 module (which
# could do any of them). The same list as the PAPER wrapper's.
ORDER_CALLS = frozenset({"send", "modify_stops", "close", "order_send", "order_check", "cancel",
                         "cancel_order", "close_by", "close_all", "positions_close", "mt5"})


class NoOrders:
    """The broker a dry-run Trader is handed: the real one for everything it
    reads (prices, the account, positions, history, the calendar, the
    connection), and a refusal for every call that could open, change or
    close a position, which never reaches the broker. ``refused`` lists, in
    plain words, what the dry run would have done."""

    def __init__(self, inner):
        self.inner = inner
        self.refused: list[str] = []

    def _refusal(self, what: str, ticket: int = 0, req: Any = None) -> OrderResult:
        self.refused.append(what)
        return OrderResult(False, RetCode.REJECT.value, ticket=int(ticket or 0),
                           requested_volume=float(getattr(req, "volume", 0.0) or 0.0),
                           idempotency_key=str(getattr(req, "idempotency_key", "") or ""),
                           comment="verification dry run: nothing is sent to the broker")

    def send(self, req) -> OrderResult:
        side = getattr(getattr(req, "side", None), "value", "")
        return self._refusal(f"open {side} {float(getattr(req, 'volume', 0.0) or 0.0):g} lots of "
                             f"{getattr(req, 'symbol', '?')}", req=req)

    def modify_stops(self, ticket: int, sl: float, tp: float) -> OrderResult:
        return self._refusal(f"set ticket {ticket}'s stop to {sl:g} and target to {tp:g}", ticket)

    def close(self, ticket: int, volume: float = 0.0, comment: str = "") -> OrderResult:
        return self._refusal(f"close ticket {ticket}" + (f" ({volume:g} lots)" if volume else ""), ticket)

    def __getattr__(self, name: str) -> Any:
        if name == "inner":                       # not set yet (a copy being built): never recurse
            raise AttributeError(name)
        if name in ORDER_CALLS:
            def refused(*args, **kwargs):
                return self._refusal(f"call {name}")
            return refused
        return getattr(self.inner, name)


class Report:
    def __init__(self):
        self.rows: list[tuple[str, bool, str]] = []

    def add(self, name: str, ok: bool, detail: str = "") -> bool:
        self.rows.append((name, ok, detail))
        mark = "OK  " if ok else "FAIL"
        print(f"  [{mark}] {name}" + (f" - {detail}" if detail else ""))
        return ok

    def warn(self, name: str, detail: str) -> None:
        self.rows.append((name, True, f"warning: {detail}"))
        print(f"  [WARN] {name} - {detail}")

    @property
    def failures(self) -> list[tuple[str, bool, str]]:
        return [r for r in self.rows if not r[1]]


def verify(config_path: str, *, smoke: bool = True) -> int:
    print()
    print("VERIFYING THE TRADING SYSTEM")
    print("-" * 62)
    r = Report()

    # ------------------------------------------------------- configuration --
    cfg = Config.load(config_path)
    errors = cfg.validate()
    r.add("configuration file", not errors,
          "; ".join(errors) if errors else
          f"{cfg.effective_mode} mode, {cfg.aggression} aggression, "
          f"risk {cfg.risk.base_risk_pct}%-{cfg.risk.max_risk_pct}%")
    if errors:
        return 1
    if cfg.is_live:
        r.warn("trading mode", "LIVE - real money is at risk")

    # ---------------------------------------------------------- MetaTrader --
    if cfg.broker_mode == "bridge":
        from .broker.bridge_client import BridgeBroker
        broker = BridgeBroker(host=cfg.bridge_host, port=cfg.bridge_port,
                              token_file=cfg.bridge_token_file,
                              magic=cfg.magic)
        info = broker.ping()
        if not r.add("MetaTrader bridge", info is not None,
                     f"{cfg.bridge_host}:{cfg.bridge_port}"
                     + (f" (backend {info['backend']})" if info else
                        " - not answering; is MetaTrader 5 running in Wine?")):
            return 1
    else:
        from .broker.mt5_adapter import Mt5Broker, mt5_available
        if not r.add("MetaTrader5 Python package", mt5_available(),
                     "" if mt5_available() else
                     "not installed, or this is not Windows"):
            return 1
        broker = Mt5Broker(login=cfg.account_login,
                           password=cfg.account_password,
                           server=cfg.account_server,
                           terminal_path=cfg.mt5_terminal_path,
                           magic=cfg.magic)
    if not r.add("MetaTrader 5 connection", broker.connect(),
                 "check the terminal is running and logged in"):
        return 1

    try:
        account = broker.account()
    except Exception as exc:
        r.add("account details", False, str(exc))
        return 1
    r.add("account details", True,
          f"{'DEMO' if account.is_demo else 'LIVE'} {account.login} on "
          f"{account.server}, {account.balance:.2f} {account.currency}")

    expected_ok = (not cfg.account_login
                   or int(account.login) == int(cfg.account_login))
    r.add("correct account connected", expected_ok,
          "" if expected_ok else
          f"connected to {account.login}, configuration expects "
          f"{cfg.account_login}")

    mode_ok = (cfg.is_live == (not account.is_demo))
    r.add("mode matches the account", mode_ok,
          "" if mode_ok else
          f"configuration says {cfg.effective_mode} but this is a "
          f"{'demo' if account.is_demo else 'live'} account")

    r.add("trading permitted", account.trade_allowed,
          "" if account.trade_allowed else
          "switch on 'Algo Trading' in MetaTrader 5")

    # ------------------------------------------------------ clock & prices --
    srv = broker.server_time()
    skew = abs((utcnow() - broker.clock.server_to_utc(srv)).total_seconds())
    r.add("clock agrees with the broker", skew <= cfg.ops.max_clock_skew_seconds,
          f"{skew:.0f}s apart")

    # ---------------------------------------------------------- universe ----
    from .engine.scanner import discover
    uni = discover(broker, cfg)
    r.add("tradable universe", len(uni.symbols) >= 3,
          f"{len(uni.symbols)} instruments: "
          f"{', '.join(uni.symbols[:8])}"
          + ("..." if len(uni.symbols) > 8 else ""))
    if uni.rejected:
        sample = list(uni.rejected.items())[:3]
        r.warn("rejected instruments",
               f"{len(uni.rejected)} skipped, e.g. "
               + "; ".join(f"{k}: {v}" for k, v in sample))

    fresh = []
    for sym in uni.symbols[:6]:
        t = broker.tick(sym)
        if t:
            fresh.append((utcnow() - t.time).total_seconds())
    r.add("live prices arriving", bool(fresh) and min(fresh) < 120,
          f"freshest price {min(fresh):.0f}s old" if fresh
          else "no ticks received")

    bars_ok = True
    for sym in uni.symbols[:3]:
        from .broker.base import TF
        n = len(broker.bars(sym, TF.M5, 200))
        if n < 100:
            bars_ok = False
            r.warn("chart history", f"{sym} only returned {n} M5 bars")
    r.add("chart history", bars_ok)

    # ------------------------------------------------------------ calendar --
    from .news.adapters import AdapterRegistry, Mt5CalendarAdapter, StoreBackedAdapter
    from .news.calendar_store import CalendarStore
    from .news.engine import NewsIntelligenceEngine
    store_path = Path(cfg.ops.data_dir) / Path(cfg.news.store_path).name
    store = CalendarStore(store_path)
    r.add("trade database / calendar store", store.healthy(), str(store_path))
    from .news.adapters import ForexFactoryAdapter
    adapters = [Mt5CalendarAdapter(broker)]
    if cfg.news.public_calendar_feed:
        adapters.append(ForexFactoryAdapter())
    adapters.append(StoreBackedAdapter(store))
    news = NewsIntelligenceEngine(AdapterRegistry(adapters), store, cfg.news)
    news_ok = news.refresh(force=True)
    if news_ok and news.health.events_known:
        r.add("economic calendar", True,
              f"{news.health.events_known} events known")
    elif news.health.events_known:
        r.warn("economic calendar",
               f"degraded: {news.health.degraded_reason or news.health.last_error}")
    else:
        r.warn("economic calendar",
               "no events returned. Some brokers and some MT5 builds do not "
               "expose the calendar; the bot will trade on charts alone and "
               "use the stored archive.")

    from .engine.journal import Journal
    journal = Journal(Path(cfg.ops.data_dir) / "journal.sqlite")
    r.add("trade journal", journal.healthy())

    # ------------------------------------------------------- reconciliation --
    # The REAL broker's positions under Trend & Breakout's number. On PAPER
    # those are only real trades left open from LIVE; its paper trades are
    # simulated and have no broker-side stop to check.
    positions = broker.positions(cfg.magic)
    naked = [p for p in positions if not p.sl]
    r.add("open positions protected", not naked,
          f"{len(positions)} open, all with a broker-side stop" if not naked
          else f"{len(naked)} position(s) have NO stop loss")

    # -------------------------------------------------------- smoke test ----
    if smoke:
        print()
        print("SAFE SMOKE TEST (no order will be sent)")
        print("-" * 62)
        smoke_test(r, broker, cfg, journal, news, account, positions)

    # ----------------------------------------------------------- dashboard --
    url = f"http://{cfg.ops.dashboard_host}:{cfg.ops.dashboard_port}/health"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            payload = json.loads(resp.read().decode())
        r.add("dashboard", True,
              f"{url} - {payload.get('health', {}).get('summary', '')}")
    except Exception as exc:
        r.warn("dashboard", f"not answering on {url} ({exc}). It starts with "
                            f"the trader; check again in a minute.")

    # Close OUR connection only.  Calling shutdown() on a shared bridge would
    # log MetaTrader out from underneath a trader that is running right now.
    if hasattr(broker, "disconnect"):
        broker.disconnect()
    else:
        broker.shutdown()
    journal.close()
    store.close()

    print()
    print("-" * 62)
    if r.failures:
        print(f"RESULT: {len(r.failures)} PROBLEM(S) FOUND")
        for name, _, detail in r.failures:
            print(f"  - {name}: {detail}")
        return 1
    print("RESULT: EVERYTHING NEEDED TO TRADE IS WORKING")
    return 0


def smoke_test(r: Report, broker, cfg: Config, journal, news, account, positions) -> None:
    """The dry-run Trader beside the real one: one full decision cycle, the
    sizing and the order key worked out, and proof that nothing was sent."""
    from .broker.paper import PaperBroker
    from .config import tnb_mode
    from .engine.trader import Trader
    paper = tnb_mode(cfg) == "PAPER"
    guard = NoOrders(broker)
    orders_before = len(getattr(broker, "order_log", None) or ())
    # dry_run: the cycle below does everything a real cycle does EXCEPT
    # send the order.  Without it this "safe smoke test" could open a
    # position on a live account, which is the opposite of what it says on
    # the tin.  NoOrders: the dry run also manages the positions it can see
    # (Trend & Breakout's own), and on LIVE that would move or close them at
    # the real broker; every such call is refused before it gets there.
    try:
        trader = Trader(guard, cfg, journal=journal, news=news, dry_run=True)
    except Exception as exc:
        r.add("dry-run trader", False, f"could not be built: {exc}")
        return
    try:
        if paper:
            # Built by the Trader itself (dry_run_view): a READ-ONLY view of
            # the paper record, which never books a stop or writes the file.
            view = trader.broker
            ok = isinstance(view, PaperBroker) and view.read_only
            try:
                n = sum(1 for p in view.positions(cfg.magic) if view.is_paper(p.ticket)) if ok else 0
                held = f"{n} paper trade{'' if n == 1 else 's'} open"
            except Exception as exc:
                held = f"its open paper trades could not be listed ({exc})"
            r.add("Trend & Breakout on PAPER", ok,
                  f"it only reads its paper record ({held}); nothing is booked or sent from here" if ok
                  else "the dry run is not reading the paper record read-only")
        started = [name for name, attr in (("Momentum Runner", "runner"), ("Band Breaker", "bandbreaker"),
                                           ("Crowd Fader", "crowd"))
                   if getattr(trader, attr, None) is not None]
        r.add("other bots not started twice", not started,
              "the Momentum Runner, Band Breaker and Crowd Fader run only inside the running trader"
              if not started else f"a second copy of {', '.join(started)} was started by the dry run")

        # Intents restored from the journal belong to the LIVE trader running
        # alongside: those already in memory, and the unfinished ones the
        # first reconciliation loads. Only intents created during this run
        # would mean a leak.
        intents_before = set(trader.executor.intents)
        try:
            intents_before |= {i.key for i in journal.unresolved_intents()}
        except Exception:
            pass
        info = trader.bootstrap()
        r.add("engine bootstrap", "universe" in info,
              f"{len(info.get('universe', []))} instruments, "
              f"{info.get('trackers_restored', 0)} tracked positions restored")

        if paper and hasattr(trader.broker, "reload"):
            # The view read the paper record when the Trader was built, but
            # the trackers came from the shared journal only now, at start-up.
            # A paper trade the running trader opened in between would have a
            # tracker here and no position in the view, and the cycle below
            # would mark it closed in the journal at a made-up loss. Reading
            # the record again puts every tracked paper trade in view.
            try:
                trader.broker.reload()
            except Exception as exc:
                r.add("Trend & Breakout's paper record read again", False,
                      f"it could not be read again ({exc}), so the decision cycle was skipped: "
                      f"it could have marked an open paper trade as closed")
                return

        result = trader.cycle()
        r.add("full decision cycle", result is not None,
              result.summary() if result is not None else "no result")
        r.add("heartbeats written",
              trader.hb_strategy.read() is not None)

        states = list(trader.top_opportunities)
        r.add("market scan produced rankings", bool(states),
              f"best: {states[0].summary()}" if states else "nothing ranked")

        # Exercise sizing and order construction without sending anything,
        # against the positions the trader itself sees (on PAPER: its paper
        # trades and any real ones left from LIVE).
        if states:
            try:
                own = trader.broker.positions(cfg.magic)
            except Exception:
                own = positions
            best = states[0]
            spec = trader.scanner.universe.specs.get(best.symbol)
            snapshot = trader.risk.snapshot(account, own)
            risk_pct, _ = trader.risk.risk_pct_for(best, 0.5)
            margin = broker.calc_margin(best.symbol, best.side, 1.0, best.entry)
            sizing = trader.risk.size(best, spec, account, own,
                                      max(risk_pct, cfg.risk.base_risk_pct),
                                      snapshot, margin)
            r.add("position sizing", sizing.volume > 0 or bool(sizing.rejected),
                  f"{sizing.volume:g} lots risking {sizing.risk_pct:.2f}% "
                  f"({sizing.stop_pips:.1f} pip stop)" if sizing.volume > 0
                  else f"declined: {sizing.rejected}")
            from .engine.execution import make_key
            key = make_key(best, spec)
            r.add("idempotency key generated", len(key) == 24, key)
        if trader.would_have_traded:
            intended = trader.would_have_traded[0]
            print(f"  [ OK ] it would have opened {intended['side']} "
                  f"{intended['volume']:g} lots of {intended['symbol']} "
                  f"risking {intended['risk_pct']:.2f}% - not sent")
        for what in guard.refused[:5]:
            print(f"  [ OK ] it would have asked the broker to {what} - not sent")
        r.add("no order was sent", not sent_any_order(broker, trader, intents_before, orders_before),
              "the smoke test ran in dry-run mode"
              + ("; Trend & Breakout's paper record was only read" if paper else ""))
    finally:
        if trader.broker is not guard and hasattr(trader.broker, "close_db"):
            trader.broker.close_db()              # the read-only paper view's file handle


def sent_any_order(broker, trader, intents_before=frozenset(), orders_before: int = 0) -> bool:
    """Belt and braces: confirm the dry run really sent nothing.

    Only intents that appeared during this run count: the executor loads
    the journal's intents at start-up, and those are the live trader's.
    An ``adopted:<ticket>`` record never counts: reconciliation makes one for
    a position it found already open (a trade the running trader holds, real
    or on Trend & Breakout's paper record), and no order is behind it. Any
    order the dry run sent would carry its own intent, under its own key.

    A broker that keeps a log of the orders it received (the simulator)
    must have no more than the ``orders_before`` it had when the run began.
    """
    new = set(trader.executor.intents) - set(intents_before)
    if any(not str(key).startswith(ADOPTED_PREFIX) for key in new):
        return True
    log = getattr(broker, "order_log", None)
    return bool(log) and len(log) > int(orders_before or 0)


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="verify the trading system")
    ap.add_argument("--config", default="data/config.json")
    ap.add_argument("--no-smoke", action="store_true")
    args = ap.parse_args(argv)
    try:
        return verify(args.config, smoke=not args.no_smoke)
    except Exception as exc:
        print(f"\nVERIFICATION STOPPED: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
