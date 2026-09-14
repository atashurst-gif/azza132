"""Production broker adapter for MetaTrader 5.

The ``MetaTrader5`` package is Windows-only, so this module imports it lazily.
On Linux/macOS the import simply reports unavailable, which is what lets the
whole intelligence stack be developed and tested off-Windows against
:class:`~mintel.broker.sim.SimBroker`.

Two MT5 details that cause real money to be lost if handled sloppily, and are
handled explicitly here:

1. **Bar and calendar timestamps are trade-server time, not UTC.**  Every
   timestamp crossing this boundary goes through :class:`ServerClock`, whose
   offset is measured live from a tick rather than assumed.
2. **A successful API call is not a successful trade.**  ``order_send`` returning
   a result object means the request reached the server; only ``retcode`` says
   what the server did with it.
"""
from __future__ import annotations

import datetime as dt
import logging
import threading
import time
from typing import Optional, Sequence

from ..clock import UTC, ServerClock, utcnow
from ..contracts import SymbolSpec, classify_fx, infer_group
from .base import (AccountInfo, Bar, BrokerError, CalendarEvent, NotConnected,
                   OrderRequest, OrderResult, Position, RetCode, Side, TF, Tick)

log = logging.getLogger("mintel.mt5")

_TF_MAP_NAMES = {
    TF.M1: "TIMEFRAME_M1", TF.M5: "TIMEFRAME_M5", TF.M15: "TIMEFRAME_M15",
    TF.M30: "TIMEFRAME_M30", TF.H1: "TIMEFRAME_H1", TF.H4: "TIMEFRAME_H4",
    TF.D1: "TIMEFRAME_D1",
}


def mt5_available() -> bool:
    try:
        import MetaTrader5  # noqa: F401
        return True
    except Exception:
        return False


class Mt5Broker:
    """Thread-safe MT5 wrapper.

    All MT5 calls are serialised through one lock: the terminal's Python bridge
    is not re-entrant, and concurrent calls from the strategy thread and the
    FlowLock thread otherwise return each other's results.
    """

    def __init__(self, *, login: int = 0, password: str = "", server: str = "",
                 terminal_path: str = "", magic: int = 990_311,
                 timeout_ms: int = 20_000, portable: bool = False):
        self.login = int(login or 0)
        self.password = password
        self.server = server
        self.terminal_path = terminal_path
        self.magic = magic
        self.timeout_ms = timeout_ms
        self.portable = portable
        self._mt5 = None
        self._lock = threading.RLock()
        self._clock = ServerClock(0)
        self._spec_cache: dict[str, tuple[float, SymbolSpec]] = {}
        self._spec_ttl = 300.0
        self._connected = False

    # ------------------------------------------------------------ lifecycle --
    @property
    def mt5(self):
        if self._mt5 is None:
            try:
                import MetaTrader5 as mt5
            except Exception as exc:  # pragma: no cover - platform dependent
                raise BrokerError(
                    "MetaTrader5 package unavailable (Windows only). "
                    "Run the trader on the Windows VPS.") from exc
            self._mt5 = mt5
        return self._mt5

    def connect(self) -> bool:
        with self._lock:
            mt5 = self.mt5
            kwargs: dict = {"timeout": self.timeout_ms}
            if self.terminal_path:
                kwargs["path"] = self.terminal_path
            if self.portable:
                kwargs["portable"] = True
            if self.login:
                kwargs.update(login=self.login, password=self.password,
                              server=self.server)
            if not mt5.initialize(**kwargs):
                log.error("mt5.initialize failed: %s", mt5.last_error())
                self._connected = False
                return False
            if self.login:
                ti = mt5.terminal_info()
                ai = mt5.account_info()
                if ai is None or int(ai.login) != self.login:
                    log.error("connected to the wrong account: wanted %s got %s",
                              self.login, getattr(ai, "login", None))
                    mt5.shutdown()
                    self._connected = False
                    return False
            self._connected = True
            self._sync_clock()
            return True

    def shutdown(self) -> None:
        with self._lock:
            if self._mt5 is not None:
                try:
                    self._mt5.shutdown()
                except Exception:
                    pass
            self._connected = False

    def is_connected(self) -> bool:
        with self._lock:
            if not self._connected:
                return False
            try:
                ti = self.mt5.terminal_info()
                return bool(ti and ti.connected)
            except Exception:
                return False

    def reconnect(self, attempts: int = 1, base_delay: float = 0.0) -> bool:
        """Shut down cleanly and re-initialise.

        Single-shot by default: this is called from inside the trading cycle,
        and a blocking retry loop there would freeze position management and
        the dashboard while it waited.  The health cycle provides the
        repetition and the healer's rate limit provides the backoff.
        """
        for i in range(max(1, attempts)):
            self.shutdown()
            if base_delay > 0:
                time.sleep(min(base_delay * (2 ** i), 30.0))
            if self.connect():
                log.info("MT5 reconnected on attempt %d", i + 1)
                return True
        return False

    # ---------------------------------------------------------------- clock --
    def _sync_clock(self) -> None:
        """Measure the trade-server offset from a live tick.

        A liquid FX symbol is preferred; we fall back to whatever is selected.
        """
        try:
            mt5 = self.mt5
            for sym in ("EURUSD", "GBPUSD", "USDJPY"):
                if mt5.symbol_select(sym, True):
                    t = mt5.symbol_info_tick(sym)
                    if t and t.time:
                        srv = dt.datetime.utcfromtimestamp(int(t.time))
                        self._clock = ServerClock.measure(srv, utcnow())
                        log.info("server clock offset = %+d min",
                                 self._clock.offset_seconds // 60)
                        return
        except Exception as exc:
            log.warning("clock sync failed: %s", exc)

    @property
    def clock(self) -> ServerClock:
        return self._clock

    def server_time(self) -> dt.datetime:
        return self._clock.utc_to_server(utcnow())

    # ------------------------------------------------------------- account --
    def account(self) -> AccountInfo:
        with self._lock:
            ai = self.mt5.account_info()
            if ai is None:
                raise NotConnected("account_info returned None")
            TRADE_MODE_DEMO = 0
            return AccountInfo(
                login=int(ai.login), server=str(ai.server),
                currency=str(ai.currency), balance=float(ai.balance),
                equity=float(ai.equity), margin=float(ai.margin),
                margin_free=float(ai.margin_free),
                margin_level=float(ai.margin_level or 0.0),
                leverage=int(ai.leverage),
                trade_allowed=bool(getattr(
                    self.mt5.terminal_info(), "trade_allowed", False)),
                is_demo=int(getattr(ai, "trade_mode", 0)) == TRADE_MODE_DEMO,
                company=str(ai.company), name=str(ai.name))

    # ------------------------------------------------------------- symbols --
    def symbols(self) -> Sequence[str]:
        with self._lock:
            infos = self.mt5.symbols_get()
            return tuple(s.name for s in (infos or ()))

    def ensure_selected(self, symbol: str) -> bool:
        with self._lock:
            try:
                return bool(self.mt5.symbol_select(symbol, True))
            except Exception:
                return False

    def spec(self, symbol: str) -> Optional[SymbolSpec]:
        now = time.time()
        cached = self._spec_cache.get(symbol)
        if cached and now - cached[0] < self._spec_ttl:
            return cached[1]
        with self._lock:
            mt5 = self.mt5
            info = mt5.symbol_info(symbol)
            if info is None:
                if not self.ensure_selected(symbol):
                    return None
                info = mt5.symbol_info(symbol)
                if info is None:
                    return None
            base, quote, _ = classify_fx(symbol)
            group = infer_group(symbol, getattr(info, "currency_base", ""),
                                getattr(info, "currency_profit", ""),
                                getattr(info, "path", ""))
            filling = []
            fill_mask = int(getattr(info, "filling_mode", 0))
            if fill_mask & 1:
                filling.append("FOK")
            if fill_mask & 2:
                filling.append("IOC")
            if not filling:
                filling = ["IOC"]
            SYMBOL_TRADE_MODE_FULL = 4
            spec = SymbolSpec(
                name=symbol, digits=int(info.digits), point=float(info.point),
                tick_size=float(info.trade_tick_size or info.point),
                tick_value=float(info.trade_tick_value or 0.0),
                contract_size=float(info.trade_contract_size or 0.0),
                volume_min=float(info.volume_min),
                volume_max=float(info.volume_max),
                volume_step=float(info.volume_step),
                stops_level_points=float(getattr(info, "trade_stops_level", 0)),
                freeze_level_points=float(getattr(info, "trade_freeze_level", 0)),
                base_currency=str(getattr(info, "currency_base", "") or base),
                profit_currency=str(getattr(info, "currency_profit", "") or quote),
                margin_currency=str(getattr(info, "currency_margin", "")),
                swap_long=float(getattr(info, "swap_long", 0.0)),
                swap_short=float(getattr(info, "swap_short", 0.0)),
                trade_allowed=int(getattr(info, "trade_mode", 0)) == SYMBOL_TRADE_MODE_FULL,
                filling_modes=tuple(filling), asset_group=group,
                typical_spread_points=float(getattr(info, "spread", 0) or 0),
            )
            self._spec_cache[symbol] = (now, spec)
            return spec

    # ---------------------------------------------------------------- data --
    def tick(self, symbol: str) -> Optional[Tick]:
        with self._lock:
            t = self.mt5.symbol_info_tick(symbol)
            if t is None or not t.time:
                return None
            ts = self._clock.server_to_utc(dt.datetime.utcfromtimestamp(int(t.time)))
            return Tick(symbol=symbol, time=ts, bid=float(t.bid),
                        ask=float(t.ask), last=float(t.last or 0.0),
                        volume=float(t.volume or 0.0))

    def bars(self, symbol: str, tf: TF, count: int,
             end: Optional[dt.datetime] = None) -> list[Bar]:
        if tf.is_synthetic:
            return self._synthetic_bars(symbol, tf, count, end)
        with self._lock:
            mt5 = self.mt5
            tf_const = getattr(mt5, _TF_MAP_NAMES[tf])
            if end is None:
                rates = mt5.copy_rates_from_pos(symbol, tf_const, 0, count)
            else:
                rates = mt5.copy_rates_from(
                    symbol, tf_const, self._clock.utc_to_server(end), count)
            if rates is None:
                return []
            out = []
            for r in rates:
                ts = self._clock.server_to_utc(
                    dt.datetime.utcfromtimestamp(int(r["time"])))
                out.append(Bar(time=ts, open=float(r["open"]),
                               high=float(r["high"]), low=float(r["low"]),
                               close=float(r["close"]),
                               tick_volume=float(r["tick_volume"]),
                               real_volume=float(r["real_volume"]),
                               spread_points=float(r["spread"])))
            return out

    def _synthetic_bars(self, symbol: str, tf: TF, count: int,
                        end: Optional[dt.datetime]) -> list[Bar]:
        """Build 5s/10s/30s bars from raw ticks for entry timing.

        MT5 exposes no sub-minute timeframe, so these are aggregated from
        ``copy_ticks_range``.  Sub-minute views are used for *timing* only; the
        decision itself never rests on them alone.
        """
        span = tf.seconds * (count + 2)
        stop = end or utcnow()
        start = stop - dt.timedelta(seconds=span)
        ticks = self.ticks_range(symbol, start, stop)
        if not ticks:
            return []
        buckets: dict[int, list[Tick]] = {}
        for t in ticks:
            buckets.setdefault(int(t.time.timestamp()) // tf.seconds, []).append(t)
        out = []
        for key in sorted(buckets):
            grp = buckets[key]
            mids = [t.mid for t in grp]
            out.append(Bar(time=dt.datetime.fromtimestamp(key * tf.seconds, tz=UTC),
                           open=mids[0], high=max(mids), low=min(mids),
                           close=mids[-1], tick_volume=float(len(grp)),
                           spread_points=0.0))
        return out[-count:]

    def ticks_range(self, symbol: str, start: dt.datetime,
                    end: dt.datetime) -> list[Tick]:
        with self._lock:
            mt5 = self.mt5
            arr = mt5.copy_ticks_range(symbol,
                                       self._clock.utc_to_server(start),
                                       self._clock.utc_to_server(end),
                                       mt5.COPY_TICKS_ALL)
            if arr is None:
                return []
            out = []
            for r in arr:
                ts = self._clock.server_to_utc(
                    dt.datetime.utcfromtimestamp(int(r["time"])))
                out.append(Tick(symbol, ts, float(r["bid"]), float(r["ask"]),
                                float(r["last"]), float(r["volume"])))
            return out

    # ----------------------------------------------------------- positions --
    def positions(self, magic: Optional[int] = None) -> list[Position]:
        with self._lock:
            raw = self.mt5.positions_get()
            if raw is None:
                return []
            POSITION_TYPE_BUY = 0
            out = []
            for p in raw:
                if magic is not None and int(p.magic) != magic:
                    continue
                out.append(Position(
                    ticket=int(p.ticket), symbol=str(p.symbol),
                    side=Side.BUY if int(p.type) == POSITION_TYPE_BUY else Side.SELL,
                    volume=float(p.volume), entry_price=float(p.price_open),
                    sl=float(p.sl), tp=float(p.tp),
                    open_time=self._clock.server_to_utc(
                        dt.datetime.utcfromtimestamp(int(p.time))),
                    profit=float(p.profit), swap=float(p.swap),
                    comment=str(p.comment), magic=int(p.magic)))
            return out

    # -------------------------------------------------------------- trading --
    def _filling_const(self, spec: SymbolSpec, requested: str):
        mt5 = self.mt5
        order = [requested] + [m for m in spec.filling_modes if m != requested]
        for mode in order:
            if mode in spec.filling_modes:
                return getattr(mt5, f"ORDER_FILLING_{mode}")
        return mt5.ORDER_FILLING_IOC

    def send(self, req: OrderRequest) -> OrderResult:
        with self._lock:
            mt5 = self.mt5
            spec = self.spec(req.symbol)
            if spec is None:
                return OrderResult(False, RetCode.REJECT.value,
                                   comment="no symbol spec",
                                   idempotency_key=req.idempotency_key)
            tick = self.tick(req.symbol)
            if tick is None:
                return OrderResult(False, RetCode.PRICE_OFF.value,
                                   comment="no tick",
                                   idempotency_key=req.idempotency_key)
            price = req.price or (tick.ask if req.side is Side.BUY else tick.bid)
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": req.symbol,
                "volume": float(spec.normalise_volume(req.volume)),
                "type": (mt5.ORDER_TYPE_BUY if req.side is Side.BUY
                         else mt5.ORDER_TYPE_SELL),
                "price": spec.normalise_price(price),
                "sl": spec.normalise_price(req.sl) if req.sl else 0.0,
                "tp": spec.normalise_price(req.tp) if req.tp else 0.0,
                "deviation": int(req.deviation_points),
                "magic": int(req.magic or self.magic),
                # The comment carries the idempotency key so that, after a crash,
                # the broker itself can tell us whether an intent was executed.
                "comment": (req.comment or req.idempotency_key)[:31],
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": self._filling_const(spec, req.filling),
            }
            if request["volume"] <= 0:
                return OrderResult(False, RetCode.INVALID_VOLUME.value,
                                   comment="volume below minimum",
                                   idempotency_key=req.idempotency_key)
            res = mt5.order_send(request)
            if res is None:
                return OrderResult(False, RetCode.CONNECTION.value,
                                   comment=str(mt5.last_error()),
                                   requested_volume=request["volume"],
                                   idempotency_key=req.idempotency_key)
            retcode = int(res.retcode)
            ok = retcode in (RetCode.DONE.value, RetCode.DONE_PARTIAL.value,
                             RetCode.PLACED.value)
            return OrderResult(
                ok=ok, retcode=retcode, ticket=int(getattr(res, "order", 0)),
                deal=int(getattr(res, "deal", 0)),
                filled_volume=float(getattr(res, "volume", 0.0)),
                price=float(getattr(res, "price", 0.0)),
                requested_volume=float(request["volume"]),
                comment=str(getattr(res, "comment", "")),
                idempotency_key=req.idempotency_key,
                raw=dict(res._asdict()) if hasattr(res, "_asdict") else {})

    def modify_stops(self, ticket: int, sl: float, tp: float) -> OrderResult:
        with self._lock:
            mt5 = self.mt5
            pos = next((p for p in self.positions() if p.ticket == ticket), None)
            if pos is None:
                return OrderResult(False, RetCode.REJECT.value,
                                   comment="position not found")
            spec = self.spec(pos.symbol)
            request = {
                "action": mt5.TRADE_ACTION_SLTP,
                "symbol": pos.symbol,
                "position": int(ticket),
                "sl": spec.normalise_price(sl) if sl else 0.0,
                "tp": spec.normalise_price(tp) if tp else 0.0,
                "magic": pos.magic or self.magic,
            }
            res = mt5.order_send(request)
            if res is None:
                return OrderResult(False, RetCode.CONNECTION.value,
                                   comment=str(mt5.last_error()), ticket=ticket)
            return OrderResult(int(res.retcode) == RetCode.DONE.value,
                               int(res.retcode), ticket=ticket,
                               comment=str(getattr(res, "comment", "")))

    def close(self, ticket: int, volume: float = 0.0,
              comment: str = "") -> OrderResult:
        with self._lock:
            mt5 = self.mt5
            pos = next((p for p in self.positions() if p.ticket == ticket), None)
            if pos is None:
                return OrderResult(False, RetCode.REJECT.value,
                                   comment="position not found")
            spec = self.spec(pos.symbol)
            tick = self.tick(pos.symbol)
            vol = spec.normalise_volume(volume) if volume > 0 else pos.volume
            vol = min(vol, pos.volume)
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": pos.symbol,
                "position": int(ticket),
                "volume": float(vol),
                "type": (mt5.ORDER_TYPE_SELL if pos.side is Side.BUY
                         else mt5.ORDER_TYPE_BUY),
                "price": (tick.bid if pos.side is Side.BUY else tick.ask),
                "deviation": 30,
                "magic": pos.magic or self.magic,
                "comment": (comment or "close")[:31],
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": self._filling_const(spec, "IOC"),
            }
            res = mt5.order_send(request)
            if res is None:
                return OrderResult(False, RetCode.CONNECTION.value,
                                   comment=str(mt5.last_error()), ticket=ticket)
            return OrderResult(int(res.retcode) in (RetCode.DONE.value,
                                                    RetCode.DONE_PARTIAL.value),
                               int(res.retcode), ticket=ticket,
                               filled_volume=float(getattr(res, "volume", 0.0)),
                               price=float(getattr(res, "price", 0.0)))

    def closed_deal(self, ticket: int) -> Optional[dict]:
        """Read the closing deals for a position from MT5's history.

        MT5 records profit, commission and swap per deal; summing them is the
        only way to know what a trade actually earned.  Reconstructing it from
        prices would silently ignore both.
        """
        with self._lock:
            try:
                mt5 = self.mt5
                # history_deals_get() has three mutually exclusive forms:
                # (from, to[, group]), (ticket=), (position=).  Passing dates
                # together with position= makes the package ignore the
                # position and return every deal in the window - which on a
                # live account was the sum of the whole week's deals booked
                # against each trade.  Ask by position alone, and then filter
                # by position id ourselves so no package quirk can do it again.
                deals = mt5.history_deals_get(position=int(ticket))
                if not deals:
                    return None
                DEAL_ENTRY_IN = 0
                closing = [d for d in deals
                           if int(getattr(d, "position_id", ticket)) == int(ticket)
                           and int(getattr(d, "entry", 0)) != DEAL_ENTRY_IN]
                if not closing:
                    return None
                volume = sum(float(d.volume) for d in closing)
                pnl = sum(float(d.profit) + float(getattr(d, "commission", 0.0))
                          + float(getattr(d, "swap", 0.0)) for d in closing)
                exit_price = (sum(float(d.price) * float(d.volume)
                                  for d in closing) / volume
                              if volume else float(closing[-1].price))
                return {"exit_price": exit_price, "pnl": pnl,
                        "volume": volume,
                        "symbol": str(getattr(closing[-1], "symbol", "") or ""),
                        "position": int(ticket),
                        "time": self._clock.server_to_utc(
                            dt.datetime.utcfromtimestamp(int(closing[-1].time))),
                        "reason": str(getattr(closing[-1], "comment", ""))}
            except Exception as exc:
                log.warning("could not read the closing deal for %s: %s",
                            ticket, exc)
                return None

    def calc_margin(self, symbol: str, side: Side, volume: float,
                    price: float) -> Optional[float]:
        """Ask MT5 for the real margin figure.

        Deriving it locally would require every cross-rate and the broker's
        own margin rules; ``order_calc_margin`` already knows them, and a
        guess here would mis-size positions on non-account-currency pairs.
        """
        with self._lock:
            try:
                mt5 = self.mt5
                order_type = (mt5.ORDER_TYPE_BUY if side is Side.BUY
                              else mt5.ORDER_TYPE_SELL)
                val = mt5.order_calc_margin(order_type, symbol, float(volume),
                                            float(price))
                return None if val is None else float(val)
            except Exception:
                return None

    # ------------------------------------------------------------ calendar --
    def calendar(self, start: dt.datetime, end: dt.datetime,
                 currencies: Sequence[str] = ()) -> list[CalendarEvent]:
        """Read MT5's native economic calendar.

        ``calendar_value_history`` timestamps are trade-server time; they are
        converted here so nothing downstream ever sees server time.
        """
        with self._lock:
            mt5 = self.mt5
            if not hasattr(mt5, "calendar_value_history"):
                raise BrokerError(
                    "this MetaTrader5 build has no calendar API; "
                    "use the stored historical calendar")
            values = mt5.calendar_value_history(
                self._clock.utc_to_server(start), self._clock.utc_to_server(end))
            if values is None:
                return []
            out: list[CalendarEvent] = []
            want = set(c.upper() for c in currencies)
            for v in values:
                ev = mt5.calendar_event_by_id(int(v.event_id))
                if ev is None:
                    continue
                ccy = str(getattr(ev, "currency", "") or "").upper()
                if want and ccy not in want:
                    continue
                out.append(CalendarEvent(
                    event_id=str(v.event_id),
                    time_utc=self._clock.server_to_utc(
                        dt.datetime.utcfromtimestamp(int(v.time))),
                    currency=ccy,
                    country=str(getattr(ev, "country_id", "")),
                    name=str(getattr(ev, "name", "")),
                    importance=int(getattr(ev, "importance", 0)),
                    actual=_num(getattr(v, "actual_value", None)),
                    forecast=_num(getattr(v, "forecast_value", None)),
                    previous=_num(getattr(v, "prev_value", None)),
                    revised_previous=_num(getattr(v, "revised_prev_value", None)),
                    source="mt5"))
            return out


def _num(x) -> Optional[float]:
    """MT5 encodes 'no value' as a huge sentinel; normalise it to ``None``."""
    if x is None:
        return None
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    if abs(f) > 1e14:
        return None
    return f
